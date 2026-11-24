package staging

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrStagedViewLost marks a recorded loss of a staged image, so callers can
// tell it apart from other terminal failures (a standalone queue transfer
// record, for instance) when a barrier consults the loss record.
var ErrStagedViewLost = errors.New("staged image was lost")

// ViewLoss is a durable record that a staged pair was pruned: its only local
// copy of the image is gone, so a durability barrier for the path must fail
// instead of reading the missing entry as a completed upload.
//
// It lives on disk in the cache directory next to the staged pairs
// (<hash>.lost beside <hash>.dat/<hash>.meta) rather than in a bounded
// in-process map, because a bounded map can evict the FIRST report of a loss and
// turn real data loss into a silent success. A fresh staging or an explicit
// removal clears it; a landing clears it only when it landed a generation at or
// above the lost one, so an older upload finishing cannot erase the record of a
// newer image that was lost.
//
// The in-process mirror (lostGens, lostUnknown, lostDirUnreadable) is
// authoritative after construction: every marker this process writes goes
// through writeViewLoss, and loadViewLossFromDisk adopts whatever a previous
// process left. That is what keeps ViewLossCount O(1) on the syncfs path and the
// prefix helpers off the directory on every rename.
type ViewLoss struct {
	Path       string `json:"path"`
	Generation uint64 `json:"generation"`
	Error      string `json:"error,omitempty"`
}

// lostFile is the on-disk view-loss marker for a remote path.
func (c *WriteBackCache) lostFile(remotePath string) string {
	return filepath.Join(c.dir, HashPath(remotePath)+".lost")
}

// lostHash is the marker's basename fragment, the key for lostUnknown.
func lostHash(remotePath string) string { return HashPath(remotePath) }

// writeViewLoss persists a loss for the path and reminds this process about it.
// The caller holds the path lock.
func (c *WriteBackCache) writeViewLoss(remotePath string, gen uint64, err error) {
	loss := ViewLoss{Path: remotePath, Generation: gen}
	if err != nil {
		loss.Error = err.Error()
	}
	if raw, merr := json.Marshal(&loss); merr == nil {
		if werr := atomicWrite(c.lostFile(remotePath), raw); werr != nil {
			// The in-process record below still fails this process's barriers;
			// only durability across the next restart is lost.
			safeLogPrintf("writeback cache: view loss marker for %s not durable: %v", remotePath, werr)
		}
	}
	c.mu.Lock()
	c.recordViewLossLocked(remotePath, gen)
	c.mu.Unlock()
}

// recordViewLossLocked installs the in-process record for a path and drops any
// "unknown marker" bookkeeping for its hash (the marker is now readable). The
// caller holds c.mu.
func (c *WriteBackCache) recordViewLossLocked(remotePath string, gen uint64) {
	if c.lostGens == nil {
		c.lostGens = make(map[string]uint64)
	}
	c.lostGens[remotePath] = gen
	if c.lostUnknown != nil {
		delete(c.lostUnknown, lostHash(remotePath))
	}
}

// forgetViewLossLocked drops the in-process record for a path. The caller holds
// c.mu.
func (c *WriteBackCache) forgetViewLossLocked(remotePath string) {
	delete(c.lostGens, remotePath)
	if c.lostUnknown != nil {
		delete(c.lostUnknown, lostHash(remotePath))
	}
}

// markLostUnknownLocked remembers a marker that could not be attributed. The
// caller holds c.mu.
func (c *WriteBackCache) markLostUnknownLocked(hash string) {
	if c.lostUnknown == nil {
		c.lostUnknown = make(map[string]struct{})
	}
	c.lostUnknown[hash] = struct{}{}
}

// ViewLoss reports the recorded loss for a path, if any: from the in-process
// map first, then from the durable marker a previous process may have left.
func (c *WriteBackCache) ViewLoss(remotePath string) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	gen, ok := c.lostGens[remotePath]
	c.mu.Unlock()
	if ok {
		return fmt.Errorf("%w: staged image for %s (generation %d) was pruned", ErrStagedViewLost, remotePath, gen)
	}
	loss, ok := c.readViewLoss(remotePath)
	if !ok {
		return nil
	}
	if loss.Error != "" {
		return fmt.Errorf("%w: %s (%s)", ErrStagedViewLost, loss.Error, remotePath)
	}
	return fmt.Errorf("%w: staged image for %s (generation %d) was pruned", ErrStagedViewLost, remotePath, loss.Generation)
}

// readViewLoss reads a durable marker. Any error other than "not exist" reports
// a loss: a hole in the failure record must not read as "nothing was lost".
func (c *WriteBackCache) readViewLoss(remotePath string) (ViewLoss, bool) {
	raw, err := os.ReadFile(c.lostFile(remotePath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ViewLoss{}, false
		}
		return ViewLoss{Path: remotePath, Generation: ^uint64(0), Error: fmt.Sprintf("unreadable view loss marker: %v", err)}, true
	}
	var loss ViewLoss
	if err := json.Unmarshal(raw, &loss); err != nil || loss.Path == "" || loss.Generation == 0 {
		// A partial write of a previous process: unreadable, not "no loss".
		return ViewLoss{Path: remotePath, Generation: ^uint64(0), Error: "unreadable view loss marker"}, true
	}
	return loss, true
}

// ClearViewLossIfGeneration drops the loss record for a path when the caller
// landed a generation at or above the lost one. A newer loss survives: an older
// upload finishing says nothing about an image that died after it.
func (c *WriteBackCache) ClearViewLossIfGeneration(remotePath string, gen uint64) {
	if c == nil {
		return
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)
	c.clearViewLossLocked(remotePath, gen)
}

// ClearViewLoss drops any loss record for a path unconditionally. It is for
// callers that just published the path's image themselves (a queue staging, an
// inline commit, a patched commit): their image is the newest one this mount
// knows, so every earlier loss record for the name is superseded.
func (c *WriteBackCache) ClearViewLoss(remotePath string) {
	if c == nil {
		return
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)
	c.clearViewLossLocked(remotePath, ^uint64(0))
}

// clearViewLossForNewStagingLocked drops any record for the path: a fresh
// staging supersedes it. The caller holds the path lock.
func (c *WriteBackCache) clearViewLossForNewStagingLocked(remotePath string) {
	c.clearViewLossLocked(remotePath, ^uint64(0))
}

// clearViewLossLocked drops the record when the lost generation is at or below
// gen. The caller holds the path lock.
//
// The in-process entry is only removed once the disk marker is gone (or provably
// at or below gen and unlinked): dropping it first would let a concurrent drain
// observe ViewLossCount() == 0 over a marker that is still on disk.
func (c *WriteBackCache) clearViewLossLocked(remotePath string, gen uint64) {
	c.mu.Lock()
	memGen, memOK := c.lostGens[remotePath]
	_, unkOK := c.lostUnknown[lostHash(remotePath)]
	c.mu.Unlock()

	loss, diskOK := c.readViewLoss(remotePath)
	if (memOK && memGen > gen) || (diskOK && loss.Generation > gen) {
		// A newer image's loss: keep it (an older durable marker may still be the
		// only crash witness for the generation this landing covers, so it stays
		// until a landing at or above the newer generation).
		return
	}
	if !diskOK {
		// The overwhelmingly common case: this runs on every Put, and a healthy
		// mount has no markers at all, so it must not pay a directory fsync.
		if (memOK && memGen <= gen) || (unkOK && gen == ^uint64(0)) {
			c.mu.Lock()
			c.forgetViewLossLocked(remotePath)
			c.mu.Unlock()
		}
		return
	}
	if err := os.Remove(c.lostFile(remotePath)); err != nil && !errors.Is(err, os.ErrNotExist) {
		// The marker survives; keep the in-process record so this process still
		// fails closed.
		if !memOK || memGen <= gen {
			c.mu.Lock()
			c.recordViewLossLocked(remotePath, loss.Generation)
			c.mu.Unlock()
		}
		return
	}
	c.mu.Lock()
	if !memOK || memGen <= gen {
		c.forgetViewLossLocked(remotePath)
	}
	c.mu.Unlock()
	// The removal must survive a crash: a resurrected marker would fail barriers
	// on a name whose image is fine.
	_ = fsyncDir(c.dir)
}

// ClearViewLossPrefix drops the loss records of every path under prefix. It is
// for subtree removals, where the names are gone but their markers would
// otherwise keep failing barriers on re-created paths. It answers from the
// in-process mirror, which mirrors every readable marker.
func (c *WriteBackCache) ClearViewLossPrefix(prefix string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	var inMemory []string
	for path := range c.lostGens {
		if strings.HasPrefix(path, prefix) {
			inMemory = append(inMemory, path)
		}
	}
	c.mu.Unlock()
	for _, path := range inMemory {
		c.ClearViewLoss(path)
	}
}

// HasViewLossPrefix reports whether any path under prefix carries a recorded
// loss. A directory rename must consult it: ListByPrefix only sees .meta
// entries, so a pruned descendant would otherwise be moved as if it landed. A
// marker that could not be attributed (unreadable, or a directory that could not
// be read) reports true: those are exactly the cases where a descendant's image
// cannot be proven to have landed.
func (c *WriteBackCache) HasViewLossPrefix(prefix string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lostDirUnreadable || len(c.lostUnknown) > 0 {
		return true
	}
	for path := range c.lostGens {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// RekeyViewLoss moves a recorded loss from oldPath to newPath. A rename carries
// the obligation with the name: the destination now holds the unlanded image's
// identity, so dropping the record (or leaving it on the dead name) would let a
// barrier certify bytes that never landed.
//
// Both path locks are taken in a deterministic order, and the new marker is
// written before the old one is unlinked: a crash in between leaves the record
// on one of the two names, never on neither. The in-process mirror never dips to
// zero during the swap, so a concurrent drain cannot certify the mount.
func (c *WriteBackCache) RekeyViewLoss(oldPath, newPath string) {
	if c == nil || oldPath == newPath {
		return
	}
	first, second := oldPath, newPath
	if second < first {
		first, second = second, first
	}
	pf := c.AcquirePathLock(first)
	ps := c.AcquirePathLock(second)
	defer c.ReleasePathLock(second, ps)
	defer c.ReleasePathLock(first, pf)
	c.rekeyViewLoss(oldPath, newPath)
}

// rekeyViewLoss is RekeyViewLoss for a caller that already holds both sorted
// path locks (RenamePending). It never removes the old record before the new
// marker exists, and it keeps newPath failing closed when the write fails.
func (c *WriteBackCache) rekeyViewLoss(oldPath, newPath string) {
	if c == nil || oldPath == newPath {
		return
	}
	c.mu.Lock()
	gen, ok := c.lostGens[oldPath]
	c.mu.Unlock()
	if !ok {
		loss, has := c.readViewLoss(oldPath)
		if !has {
			return
		}
		gen = loss.Generation
	}
	if raw, merr := json.Marshal(&ViewLoss{Path: newPath, Generation: gen}); merr == nil {
		if werr := atomicWrite(c.lostFile(newPath), raw); werr != nil {
			// The object already moved: record it for this process on the
			// destination and keep the source's durable marker for the next one.
			safeLogPrintf("writeback cache: view loss rekey %s -> %s failed: %v", oldPath, newPath, werr)
			c.mu.Lock()
			c.recordViewLossLocked(newPath, gen)
			c.mu.Unlock()
			return
		}
	}
	// Record the destination first: the object already moved, so it must fail
	// closed even if the source marker cannot be removed.
	c.mu.Lock()
	c.recordViewLossLocked(newPath, gen)
	c.mu.Unlock()
	if err := os.Remove(c.lostFile(oldPath)); err == nil {
		_ = fsyncDir(c.dir)
	}
	if _, err := os.Stat(c.lostFile(oldPath)); errors.Is(err, os.ErrNotExist) {
		// Only forget the source once its marker is gone: a surviving file would
		// otherwise be uncounted by the mirror.
		c.mu.Lock()
		c.forgetViewLossLocked(oldPath)
		c.mu.Unlock()
	}
}

// RekeyViewLossPrefix moves every recorded loss under oldPrefix to newPrefix,
// preserving the relative path.
func (c *WriteBackCache) RekeyViewLossPrefix(oldPrefix, newPrefix string) {
	if c == nil || oldPrefix == newPrefix {
		return
	}
	c.mu.Lock()
	pairs := make(map[string]string)
	for path := range c.lostGens {
		if strings.HasPrefix(path, oldPrefix) {
			pairs[path] = newPrefix + strings.TrimPrefix(path, oldPrefix)
		}
	}
	c.mu.Unlock()
	for oldPath, newPath := range pairs {
		c.RekeyViewLoss(oldPath, newPath)
	}
}

// ViewLossCount reports how many unresolved view losses are on record. It is the
// only signal a drain has for a pruned pair whose path no longer has an open
// handle (closed, or a fresh process after a restart).
//
// It is O(1): the in-process mirror is authoritative after construction (every
// marker this process writes goes through writeViewLoss, and
// loadViewLossFromDisk adopts the ones a previous process left), and markers that
// could not be decoded are counted once at load. A directory scan per call would
// make syncfs(2) O(staged files) on a write-back mount.
func (c *WriteBackCache) ViewLossCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := len(c.lostGens) + len(c.lostUnknown)
	if c.lostDirUnreadable && count == 0 {
		count = 1
	}
	return count
}

// loadViewLossFromDisk seeds the in-process mirror (and the generation counter)
// from the markers a previous process left, so a recovered marker is honored and
// a fresh generation can never be mistaken for an older loss.
//
// A marker that cannot be read or decoded is remembered by hash instead: the
// drain must not certify the mount, and a prefix query cannot attribute it, so
// it fails closed.
func (c *WriteBackCache) loadViewLossFromDisk() {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		c.mu.Lock()
		c.lostDirUnreadable = true
		c.mu.Unlock()
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".lost") {
			continue
		}
		hash := strings.TrimSuffix(name, ".lost")
		raw, rerr := os.ReadFile(filepath.Join(c.dir, name))
		if rerr != nil {
			if !errors.Is(rerr, os.ErrNotExist) {
				c.mu.Lock()
				c.markLostUnknownLocked(hash)
				c.mu.Unlock()
			}
			continue
		}
		var loss ViewLoss
		if jerr := json.Unmarshal(raw, &loss); jerr != nil || loss.Path == "" || loss.Generation == 0 {
			c.mu.Lock()
			c.markLostUnknownLocked(hash)
			c.mu.Unlock()
			continue
		}
		c.mu.Lock()
		if c.lostGens == nil {
			c.lostGens = make(map[string]uint64)
		}
		c.lostGens[loss.Path] = loss.Generation
		c.mu.Unlock()
		c.advanceNextGen(loss.Generation)
	}
}

// LossGenerationBound returns the highest generation a landing may treat as
// superseded: the caller's own watermark raised to the recorded loss, if any. A
// landing published after this bound must clear only up to it, so a newer image
// that died while the upload was on the wire keeps its record.
func (c *WriteBackCache) LossGenerationBound(remotePath string, bound uint64) uint64 {
	if c == nil {
		return bound
	}
	c.mu.Lock()
	gen, ok := c.lostGens[remotePath]
	_, unkOK := c.lostUnknown[lostHash(remotePath)]
	dirUnreadable := c.lostDirUnreadable
	c.mu.Unlock()
	if ok && gen > bound {
		return gen
	}
	if unkOK || dirUnreadable {
		// A marker that cannot be attributed may be newer than anything this
		// landing published: do not let the landing clear it.
		if loss, has := c.readViewLoss(remotePath); has && loss.Generation > bound {
			return loss.Generation
		}
		if unkOK {
			return ^uint64(0)
		}
	}
	return bound
}
