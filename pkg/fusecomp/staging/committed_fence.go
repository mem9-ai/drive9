package staging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CommittedFence records that a staged image landed remotely while its WAL
// commit marker never became durable. It is deliberately NOT a pending entry:
// the image is published, so every live reader must see the path as clean
// (HasPending false, GetMeta absent, Count not inflated), and a later staging
// of a newer generation must not be suppressed by it.
//
// The fence only governs crash recovery:
//   - journal replay drops a pending-meta frame whose generation is at or
//     below Generation, and a data frame at or below JournalSeq (the newest WAL
//     sequence when the fence was written), so the pre-commit image is not
//     resurrected and re-uploaded against the landed revision;
//   - anything newer is replayed normally, so fsync-acknowledged writes that
//     happened after the landing are never lost.
//
// The fence file survives until settle: SettleCommittedFences appends a durable
// JournalCommit (so compaction can drop the frames the fence was masking) and
// removes the shadow the fence was protecting, and only then deletes the fence.
// A crash in between leaves the fence, so the next start retries the settle
// instead of resurrecting the landed image.
type CommittedFence struct {
	Path       string     `json:"path"`
	Generation uint64     `json:"generation"`
	ShadowGen  ContentGen `json:"shadow_gen,omitempty"`
	JournalSeq uint64     `json:"journal_seq,omitempty"`
}

// fenceFile returns the on-disk fence path for a remote path.
func (idx *PendingIndex) fenceFile(remotePath string) string {
	return filepath.Join(idx.dir, HashPath(remotePath)+".fence")
}

// loadCommittedFences scans the fence files once and returns the first scan
// error. A fence that cannot be read or decoded is NOT treated as absent: replay
// must never resurrect an image whose fence might have masked it, so the path's
// hash is recorded in fenceMask and CommittedFence reports a mask-everything
// fence for it. Callers that can surface an error (replay, disk recovery)
// return it and abort instead of continuing with a hole in the fence set.
func (idx *PendingIndex) loadCommittedFences() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.loadCommittedFencesLocked()
}

// ensureCommittedFencesLoaded is the best-effort variant for readers that
// cannot return an error. The mask recorded by a failed scan still keeps their
// answers fail-closed.
func (idx *PendingIndex) ensureCommittedFencesLoaded() {
	if idx == nil {
		return
	}
	idx.mu.RLock()
	loaded := idx.fencesLoaded
	idx.mu.RUnlock()
	if loaded {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	_ = idx.loadCommittedFencesLocked()
}

// ensureCommittedFencesLoadedLocked is the best-effort scan for callers that
// already hold idx.mu.
func (idx *PendingIndex) ensureCommittedFencesLoadedLocked() {
	_ = idx.loadCommittedFencesLocked()
}

// loadCommittedFencesLocked is the scan itself. The caller holds idx.mu.
func (idx *PendingIndex) loadCommittedFencesLocked() error {
	if idx.fencesLoaded {
		return idx.fencesErr
	}
	idx.fencesLoaded = true
	if idx.fences == nil {
		idx.fences = make(map[string]CommittedFence)
	}
	if idx.fenceMask == nil {
		idx.fenceMask = make(map[string]struct{})
	}
	entries, err := os.ReadDir(idx.dir)
	if err != nil {
		idx.fencesErr = fmt.Errorf("pending index read committed fences: %w", err)
		return idx.fencesErr
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".fence") {
			continue
		}
		hash := strings.TrimSuffix(name, ".fence")
		raw, err := os.ReadFile(filepath.Join(idx.dir, name))
		if err != nil {
			idx.fenceMask[hash] = struct{}{}
			if idx.fencesErr == nil {
				idx.fencesErr = fmt.Errorf("pending index read committed fence %s: %w", name, err)
			}
			continue
		}
		var fence CommittedFence
		if err := json.Unmarshal(raw, &fence); err != nil || fence.Path == "" {
			idx.fenceMask[hash] = struct{}{}
			if idx.fencesErr == nil {
				idx.fencesErr = fmt.Errorf("pending index decode committed fence %s: %w", name, err)
			}
			continue
		}
		idx.fences[fence.Path] = fence
		if fence.Generation > idx.nextGen.Load() {
			idx.nextGen.Store(fence.Generation)
		}
	}
	return idx.fencesErr
}

// CommittedFence returns the replay fence for a path, if one is on record. A
// fence file that could not be decoded masks every frame for its path, so a
// replay hole can never resurrect a landed image.
func (idx *PendingIndex) CommittedFence(remotePath string) (CommittedFence, bool) {
	if idx == nil {
		return CommittedFence{}, false
	}
	idx.ensureCommittedFencesLoaded()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if _, masked := idx.fenceMask[HashPath(remotePath)]; masked {
		return CommittedFence{Path: remotePath, Generation: ^uint64(0), JournalSeq: ^uint64(0)}, true
	}
	fence, ok := idx.fences[remotePath]
	return fence, ok
}

// CommittedFences snapshots every fence on record. Fences whose file could not
// be decoded are excluded (they are never settled).
func (idx *PendingIndex) CommittedFences() []CommittedFence {
	if idx == nil {
		return nil
	}
	idx.ensureCommittedFencesLoaded()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if len(idx.fences) == 0 {
		return nil
	}
	out := make([]CommittedFence, 0, len(idx.fences))
	for _, fence := range idx.fences {
		out = append(out, fence)
	}
	return out
}

// ClearCommittedFence drops the fence for a path from memory and disk, after
// the caller made the replay fence durable another way (a JournalCommit frame)
// or the path was superseded (a newer staging, an explicit removal).
func (idx *PendingIndex) ClearCommittedFence(remotePath string) {
	if idx == nil {
		return
	}
	idx.ensureCommittedFencesLoaded()
	pl := idx.acquirePathLock(remotePath)
	idx.mu.Lock()
	_, had := idx.fences[remotePath]
	delete(idx.fences, remotePath)
	idx.mu.Unlock()
	if had {
		_ = os.Remove(idx.fenceFile(remotePath))
	}
	idx.releasePathLock(remotePath, pl)
}

// clearCommittedFenceLocked drops a fence for a path whose pending entry was
// just replaced or removed. The caller holds the path lock. The file removal is
// best effort: a stale fence can only suppress frames at or below a generation
// that a newer publication already superseded, and the generation gate keeps
// replay correct.
func (idx *PendingIndex) clearCommittedFenceLocked(remotePath string) {
	if idx.fences == nil {
		return
	}
	if _, had := idx.fences[remotePath]; !had {
		return
	}
	delete(idx.fences, remotePath)
	_ = os.Remove(idx.fenceFile(remotePath))
}

// SettleCommittedFences retires every fence left by a crashed or restarted
// process. A fence can only be dropped once its purpose is served, so this
// appends a durable commit marker for the path first (letting compaction drop
// the frames the fence masked) and removes the landed image's shadow, and only
// then deletes the fence. Any failure leaves the fence in place for the next
// start.
func (idx *PendingIndex) SettleCommittedFences(j *Journal, shadows *ShadowStore) error {
	if idx == nil {
		return nil
	}
	for _, fence := range idx.CommittedFences() {
		if j != nil {
			if err := j.AppendSync(JournalEntry{Op: JournalCommit, Path: fence.Path}); err != nil {
				return fmt.Errorf("settle committed fence %s: %w", fence.Path, err)
			}
		}
		if shadows != nil && fence.ShadowGen != 0 {
			if _, err := shadows.RemoveIfGenerationDurable(fence.Path, fence.ShadowGen); err != nil {
				return fmt.Errorf("settle committed fence shadow %s: %w", fence.Path, err)
			}
		}
		idx.ClearCommittedFence(fence.Path)
	}
	return nil
}
