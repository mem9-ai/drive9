package fuse

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PendingIndex is an in-memory authoritative index for pending file metadata.
// All metadata reads are served from memory (O(1), no disk I/O, no JSON parse).
// Disk writes happen on Put/Remove/Rename for durability and crash recovery.
//
// Concurrency: mutating operations hold a per-path lock across both the disk
// write and the in-memory publish/removal, so a same-path Put and a
// generation-checked RemoveIfGeneration cannot interleave disk state — e.g. a
// replacement Put's new .meta must never be deleted by a stale owner's
// remove that already passed its generation check in memory.
type PendingIndex struct {
	mu        sync.RWMutex
	items     map[string]*WriteBackMeta // path → metadata
	dir       string                    // directory for .meta persistence
	nextGen   atomic.Uint64
	pathLocks map[string]*pendingPathLock
	// journal, when set, is the durable publication channel for new pending
	// entries: one group-committed WAL fsync replaces the per-entry
	// atomicWrite (tmp fsync + dir fsync). Callers must have made the entry's
	// content durable (shadow fsync / durable snapshot) BEFORE Put, which the
	// staging paths already guarantee.
	journal *Journal
	// journalSyncOnPut controls whether Put fsyncs the WAL frame it appends.
	// Legacy (true): every close pays a group-committed fsync. Lazy staging
	// (issue #964, false): the frame lands in the kernel page cache and a
	// background SyncLoop / fsync(2) / umount make it durable.
	journalSyncOnPut bool
	// restoreMetaWrite is a narrow test seam for the cross-file layer cache
	// transaction. Production always uses atomicWrite.
	restoreMetaWrite func(string, []byte) error
}

// SetJournal wires the WAL used for durable pending-meta publication. It is
// wired once during mount init, before the first Put.
func (idx *PendingIndex) SetJournal(j *Journal) {
	if idx == nil || j == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.journal == nil {
		idx.journal = j
		idx.journalSyncOnPut = true
	}
}

// SetJournalSyncOnPut toggles per-Put WAL fsyncs (see journalSyncOnPut).
func (idx *PendingIndex) SetJournalSyncOnPut(on bool) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.journalSyncOnPut = on
	idx.mu.Unlock()
}

// pendingPathLock is a refcounted per-path mutex, mirroring WriteBackCache's
// pathLockEntry. Lock order: path lock (outer) → idx.mu (inner).
type pendingPathLock struct {
	mu      sync.Mutex
	waiters int
}

func (idx *PendingIndex) acquirePathLock(remotePath string) *pendingPathLock {
	idx.mu.Lock()
	pl, ok := idx.pathLocks[remotePath]
	if !ok {
		pl = &pendingPathLock{}
		idx.pathLocks[remotePath] = pl
	}
	pl.waiters++
	idx.mu.Unlock()

	pl.mu.Lock()
	return pl
}

func (idx *PendingIndex) releasePathLock(remotePath string, pl *pendingPathLock) {
	idx.mu.Lock()
	pl.waiters--
	if pl.waiters == 0 {
		delete(idx.pathLocks, remotePath)
	}
	idx.mu.Unlock()
	pl.mu.Unlock()
}

// acquireTwoPathLocks locks both paths in a stable (lexicographic) order so
// rename operations cannot deadlock against each other.
func (idx *PendingIndex) acquireTwoPathLocks(a, b string) (pla, plb *pendingPathLock) {
	if a == b {
		pla = idx.acquirePathLock(a)
		return pla, nil
	}
	first, second := a, b
	if second < first {
		first, second = second, first
	}
	pla = idx.acquirePathLock(first)
	plb = idx.acquirePathLock(second)
	return pla, plb
}

func (idx *PendingIndex) releaseTwoPathLocks(a, b string, pla, plb *pendingPathLock) {
	// Release against the same SORTED names used by acquireTwoPathLocks:
	// pla protects the lexicographically smaller path, plb the larger one.
	// Releasing against the original unsorted names crosses the lock/path
	// pairing for reverse-ordered renames and can delete a live lock's map
	// entry, splitting one path into two mutexes.
	first, second := a, b
	if second < first {
		first, second = second, first
	}
	if plb != nil {
		idx.releasePathLock(second, plb)
	}
	idx.releasePathLock(first, pla)
}

// NewPendingIndex creates a PendingIndex backed by the given directory.
// The directory is created if it does not exist.
func NewPendingIndex(dir string) (*PendingIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("pending index dir: %w", err)
	}
	if err := recoverLayerRestoreTransactions(dir); err != nil {
		return nil, fmt.Errorf("pending index recover layer restore transaction: %w", err)
	}
	idx := &PendingIndex{
		items:     make(map[string]*WriteBackMeta),
		dir:       dir,
		pathLocks: make(map[string]*pendingPathLock),
	}
	return idx, nil
}

func (idx *PendingIndex) writeLayerRestoreMeta(path string, data []byte) error {
	if idx.restoreMetaWrite != nil {
		return idx.restoreMetaWrite(path, data)
	}
	return atomicWrite(path, data)
}

// RecoverFromDisk scans .meta files in the directory and rebuilds in-memory
// state. Called once at startup for crash recovery.
func (idx *PendingIndex) RecoverFromDisk() error {
	entries, err := os.ReadDir(idx.dir)
	if err != nil {
		return fmt.Errorf("pending index recovery: %w", err)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	var maxGen uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".meta") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(idx.dir, name))
		if err != nil {
			continue
		}
		var meta WriteBackMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			// Corrupt meta — remove it.
			_ = os.Remove(filepath.Join(idx.dir, name))
			continue
		}
		cp := meta
		idx.items[meta.Path] = &cp
		if meta.Generation > maxGen {
			maxGen = meta.Generation
		}
	}
	idx.nextGen.Store(maxGen)
	return nil
}

// Put stores metadata for the given path in both memory and disk.
func (idx *PendingIndex) Put(remotePath string, size int64, kind PendingKind) (uint64, error) {
	return idx.PutWithBaseRev(remotePath, size, kind, 0)
}

// PutWithBaseRev stores metadata for the given path together with the base
// revision observed when the local edit session started.
func (idx *PendingIndex) PutWithBaseRev(remotePath string, size int64, kind PendingKind, baseRev int64) (uint64, error) {
	return idx.PutWithBaseRevAndMode(remotePath, size, kind, baseRev, 0, false)
}

// PutWithBaseRevAndMode stores metadata for the given path together with the
// file mode that must be applied after the pending data commits remotely.
func (idx *PendingIndex) PutWithBaseRevAndMode(remotePath string, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool) (uint64, error) {
	return idx.PutWithBaseRevAndModeAndLineage(remotePath, size, kind, baseRev, mode, hasMode, "", "", false)
}

// PutLayerCommitted stores a retained overlay reconstructed from authoritative
// layer state. Unlike Put, it publishes the committed marker in one atomic
// .meta replacement and intentionally does not append an uncommitted WAL
// frame: the remote layer is already the durable source and a crash may simply
// rebuild this local cache again.
func (idx *PendingIndex) PutLayerCommitted(remotePath string, size, baseRev int64, mode uint32, hasMode bool) (uint64, error) {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	gen := idx.nextGen.Add(1)
	meta := &WriteBackMeta{
		Path:           remotePath,
		Size:           size,
		Mtime:          time.Now(),
		CreatedAt:      time.Now(),
		Generation:     gen,
		Kind:           PendingOverwrite,
		BaseRev:        baseRev,
		LayerCommitted: true,
		Mode:           mode & posixPermissionModeMask,
		HasMode:        hasMode,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return 0, fmt.Errorf("pending index marshal committed layer: %w", err)
	}
	if err := atomicWrite(filepath.Join(idx.dir, hashPath(remotePath)+".meta"), metaBytes); err != nil {
		return 0, fmt.Errorf("pending index put committed layer: %w", err)
	}
	idx.mu.Lock()
	idx.items[remotePath] = meta
	idx.mu.Unlock()
	return gen, nil
}

// restoreLayerGenerationLocked returns the current entry when it still
// represents the committed generation observed by a layer restore. Generation
// zero means the restore observed no pending entry. Callers must hold the path
// lock for remotePath; the method takes idx.mu only for the map lookup.
func (idx *PendingIndex) restoreLayerGenerationLocked(remotePath string, expectedGeneration uint64) (*WriteBackMeta, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	meta, ok := idx.items[remotePath]
	if expectedGeneration == 0 {
		return nil, !ok
	}
	if !ok || meta.Generation != expectedGeneration || !meta.LayerCommitted {
		return nil, false
	}
	cp := cloneWriteBackMeta(meta)
	return &cp, true
}

// restoreLayerCommittedIfGeneration installs authoritative layer content only
// while the path still has the committed generation observed before the remote
// fetch. The pending path lock stays held while apply performs its shadow-store
// generation CAS, so a writer that bypasses the remote-commit lock can change
// the shadow but cannot publish matching pending metadata until this operation
// either observes that change and aborts or finishes first.
func (idx *PendingIndex) restoreLayerCommittedIfGeneration(
	remotePath string,
	expectedGeneration uint64,
	baseRev int64,
	mode uint32,
	hasMode bool,
	shadowPath string,
	apply func(tx *layerRestoreTxn, commitMeta func(size int64) error) (applied bool, err error),
) (bool, error) {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	if _, ok := idx.restoreLayerGenerationLocked(remotePath, expectedGeneration); !ok {
		return false, nil
	}
	tx, err := beginLayerRestoreTxn(idx.dir, filepath.Join(idx.dir, hashPath(remotePath)+".meta"), shadowPath)
	if err != nil {
		return false, err
	}
	committed := false
	commitMeta := func(size int64) error {
		gen := idx.nextGen.Add(1)
		now := time.Now()
		meta := &WriteBackMeta{
			Path:           remotePath,
			Size:           size,
			Mtime:          now,
			CreatedAt:      now,
			Generation:     gen,
			Kind:           PendingOverwrite,
			BaseRev:        baseRev,
			LayerCommitted: true,
			Mode:           mode & posixPermissionModeMask,
			HasMode:        hasMode,
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("pending index marshal restored layer: %w", err)
		}
		if err := idx.writeLayerRestoreMeta(filepath.Join(idx.dir, hashPath(remotePath)+".meta"), metaBytes); err != nil {
			return fmt.Errorf("pending index restore committed layer: %w", err)
		}
		idx.mu.Lock()
		idx.items[remotePath] = meta
		idx.mu.Unlock()
		committed = true
		return nil
	}
	applied, err := apply(tx, commitMeta)
	if err != nil || !applied {
		if err != nil {
			if rollbackErr := tx.rollback(); rollbackErr != nil {
				return false, errors.Join(err, rollbackErr)
			}
			return false, err
		}
		if cleanupErr := tx.commit(); cleanupErr != nil {
			return false, cleanupErr
		}
		return false, nil
	}
	if !committed {
		err := fmt.Errorf("pending index restore committed layer: shadow applied without metadata")
		if rollbackErr := tx.rollback(); rollbackErr != nil {
			return false, errors.Join(err, rollbackErr)
		}
		return false, err
	}
	if err := tx.commit(); err != nil {
		return false, err
	}
	return true, nil
}

// restoreLayerModeIfGeneration applies a peer chmod only to the committed
// retained generation that was observed by restore. A newer unfinished local
// generation is never relabeled as remotely committed.
func (idx *PendingIndex) restoreLayerModeIfGeneration(remotePath string, expectedGeneration uint64, mode uint32) (bool, error) {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	meta, ok := idx.restoreLayerGenerationLocked(remotePath, expectedGeneration)
	if !ok {
		return false, nil
	}
	if meta == nil {
		// Chmod-only layer entries can refer to a remote base file for which no
		// retained local content exists. There is no pending metadata to update.
		return true, nil
	}
	meta.Mode = mode & posixPermissionModeMask
	meta.HasMode = true
	meta.LayerCommitted = true
	meta.Generation = idx.nextGen.Add(1)
	meta.Mtime = time.Now()
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return false, fmt.Errorf("pending index marshal restored layer mode: %w", err)
	}
	if err := atomicWrite(filepath.Join(idx.dir, hashPath(remotePath)+".meta"), metaBytes); err != nil {
		return false, fmt.Errorf("pending index restore layer mode: %w", err)
	}
	idx.mu.Lock()
	idx.items[remotePath] = meta
	idx.mu.Unlock()
	return true, nil
}

// restoreLayerrenameIfGenerations applies the pending-index half of a peer
// rename only while both paths still match the committed/absent generations
// observed before any remote read. apply must perform the corresponding
// shadow-store generation CAS. Holding both pending path locks across apply
// prevents a lock-timeout writer from publishing metadata between the shadow
// CAS and this durable metadata move.
func (idx *PendingIndex) restoreLayerrenameIfGenerations(
	oldPath, newPath string,
	expectedOldGeneration, expectedNewGeneration uint64,
	baseRev int64,
	mode uint32,
	hasMode bool,
	oldShadowPath, newShadowPath string,
	apply func(tx *layerRestoreTxn, commitMeta func(size int64) error) (applied bool, err error),
) (bool, error) {
	pla, plb := idx.acquireTwoPathLocks(oldPath, newPath)
	defer idx.releaseTwoPathLocks(oldPath, newPath, pla, plb)

	oldMeta, oldOK := idx.restoreLayerGenerationLocked(oldPath, expectedOldGeneration)
	if !oldOK {
		return false, nil
	}
	if _, newOK := idx.restoreLayerGenerationLocked(newPath, expectedNewGeneration); !newOK {
		return false, nil
	}
	tx, err := beginLayerRestoreTxn(
		idx.dir,
		filepath.Join(idx.dir, hashPath(oldPath)+".meta"),
		filepath.Join(idx.dir, hashPath(newPath)+".meta"),
		oldShadowPath,
		newShadowPath,
	)
	if err != nil {
		return false, err
	}
	committed := false
	commitMeta := func(size int64) error {
		now := time.Now()
		newMeta := &WriteBackMeta{
			Path:           newPath,
			Size:           size,
			Mtime:          now,
			CreatedAt:      now,
			Kind:           PendingOverwrite,
			BaseRev:        baseRev,
			LayerCommitted: true,
			Mode:           mode & posixPermissionModeMask,
			HasMode:        hasMode,
		}
		if oldMeta != nil {
			copied := cloneWriteBackMeta(oldMeta)
			newMeta = &copied
			newMeta.Path = newPath
			newMeta.Size = size
			newMeta.Mtime = now
			newMeta.Kind = PendingOverwrite
			newMeta.LayerCommitted = true
		}
		newMeta.Generation = idx.nextGen.Add(1)
		metaBytes, err := json.Marshal(newMeta)
		if err != nil {
			return fmt.Errorf("pending index marshal restored layer rename: %w", err)
		}
		if err := idx.writeLayerRestoreMeta(filepath.Join(idx.dir, hashPath(newPath)+".meta"), metaBytes); err != nil {
			return fmt.Errorf("pending index restore layer rename: %w", err)
		}
		if oldPath != newPath {
			oldMetaPath := filepath.Join(idx.dir, hashPath(oldPath)+".meta")
			if err := os.Remove(oldMetaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("pending index remove restored layer source: %w", err)
			}
			if err := fsyncDir(idx.dir); err != nil {
				return fmt.Errorf("pending index sync restored layer rename: %w", err)
			}
		}
		idx.mu.Lock()
		if oldPath != newPath {
			delete(idx.items, oldPath)
		}
		idx.items[newPath] = newMeta
		idx.mu.Unlock()
		committed = true
		return nil
	}
	applied, err := apply(tx, commitMeta)
	if err != nil || !applied {
		if err != nil {
			if rollbackErr := tx.rollback(); rollbackErr != nil {
				return false, errors.Join(err, rollbackErr)
			}
			return false, err
		}
		if cleanupErr := tx.commit(); cleanupErr != nil {
			return false, cleanupErr
		}
		return false, nil
	}
	if !committed {
		err := fmt.Errorf("pending index restore layer rename: shadow applied without metadata")
		if rollbackErr := tx.rollback(); rollbackErr != nil {
			return false, errors.Join(err, rollbackErr)
		}
		return false, err
	}
	if err := tx.commit(); err != nil {
		return false, err
	}
	return true, nil
}

// PutWithBaseRevAndModeAndLineage attaches process-local causal identity to
// the durable metadata. The IDs are intentionally excluded from JSON; empty
// lineage is accepted but can never authorize a growth rebase.
func (idx *PendingIndex) PutWithBaseRevAndModeAndLineage(remotePath string, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, liveAncestors ...string) (uint64, error) {
	return idx.putInternal(remotePath, size, kind, baseRev, false, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, liveAncestors)
}

// PutShadowSpill is like PutWithBaseRev but marks the entry as ShadowSpill
// so that crash recovery (RecoverPending) reconstructs it with the correct
// upload path (streaming from shadow, not full-memory ReadAll).
func (idx *PendingIndex) PutShadowSpill(remotePath string, size int64, kind PendingKind, baseRev int64) (uint64, error) {
	return idx.PutShadowSpillWithMode(remotePath, size, kind, baseRev, 0, false)
}

// PutShadowSpillWithMode is like PutShadowSpill, but also persists file mode
// metadata for the eventual remote chmod.
func (idx *PendingIndex) PutShadowSpillWithMode(remotePath string, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool) (uint64, error) {
	return idx.PutShadowSpillWithModeAndLineage(remotePath, size, kind, baseRev, mode, hasMode, "", "", false)
}

// PutShadowSpillWithModeAndLineage is the spill equivalent of
// PutWithBaseRevAndModeAndLineage.
func (idx *PendingIndex) PutShadowSpillWithModeAndLineage(remotePath string, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, liveAncestors ...string) (uint64, error) {
	return idx.putInternal(remotePath, size, kind, baseRev, true, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, liveAncestors)
}

func (idx *PendingIndex) putInternal(remotePath string, size int64, kind PendingKind, baseRev int64, shadowSpill bool, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, liveAncestors []string) (uint64, error) {
	// Hold the per-path lock across the disk write AND the in-memory publish
	// so a generation-checked removal of a stale entry cannot delete this
	// fresh .meta after already passing its in-memory check.
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	gen := idx.nextGen.Add(1)
	meta := &WriteBackMeta{
		Path:             remotePath,
		Size:             size,
		Mtime:            time.Now(),
		CreatedAt:        time.Now(),
		Generation:       gen,
		Kind:             kind,
		BaseRev:          baseRev,
		ShadowSpill:      shadowSpill,
		Mode:             mode & posixPermissionModeMask,
		HasMode:          hasMode,
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapshotID,
		lineageTrusted:   lineageTrusted,
		liveAncestors:    append([]string(nil), liveAncestors...),
	}

	// Durable publication first, then the in-memory publish.
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return 0, fmt.Errorf("pending index marshal: %w", err)
	}
	idx.mu.RLock()
	journal := idx.journal
	syncOnPut := idx.journalSyncOnPut
	idx.mu.RUnlock()
	if journal != nil {
		// WAL route: one group-committed fsync covers this record (and any
		// records other closers appended concurrently). Ordering is safe —
		// callers fsync the content (shadow / durable snapshot) before Put,
		// and JournalPendingMeta replay rebuilds this entry after a crash.
		if err := journal.Append(JournalEntry{
			Op:         JournalPendingMeta,
			Path:       remotePath,
			Generation: gen,
			Timestamp:  time.Now().Unix(),
			Meta:       metaBytes,
		}); err != nil {
			return 0, fmt.Errorf("pending index journal append: %w", err)
		}
		if syncOnPut {
			if err := journal.FsyncShared(); err != nil {
				return 0, fmt.Errorf("pending index journal fsync: %w", err)
			}
		}
	} else {
		metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
		if err := atomicWrite(metaPath, metaBytes); err != nil {
			return 0, fmt.Errorf("pending index put meta: %w", err)
		}
	}

	idx.mu.Lock()
	idx.items[remotePath] = meta
	idx.mu.Unlock()

	return gen, nil
}

// publishRecoveredMeta installs a pending entry unmarshaled from a WAL frame
// during crash recovery. It is in-memory only: the WAL record remains on disk
// until compaction drops it behind the path's commit marker, so re-publishing
// the .meta file would be redundant.
//
// A layer commit persists its retained-overlay marker as a .meta file. A later
// edit to the same path is published through the WAL, so replay must prefer
// that strictly newer generation over the older .meta marker. Equal or older
// WAL generations still lose to .meta because the standalone file may carry
// metadata not present in legacy WAL records.
func (idx *PendingIndex) publishRecoveredMeta(metaBytes []byte) error {
	var meta WriteBackMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return fmt.Errorf("recovered meta unmarshal: %w", err)
	}
	if meta.Path == "" {
		return fmt.Errorf("recovered meta has empty path")
	}
	idx.mu.Lock()
	if current, ok := idx.items[meta.Path]; !ok || meta.Generation > current.Generation {
		idx.items[meta.Path] = &meta
	}
	idx.mu.Unlock()
	for current := idx.nextGen.Load(); meta.Generation > current; current = idx.nextGen.Load() {
		if idx.nextGen.CompareAndSwap(current, meta.Generation) {
			break
		}
	}
	return nil
}

// GetMeta reads metadata from memory only. O(1), no disk I/O.
func (idx *PendingIndex) GetMeta(remotePath string) (*WriteBackMeta, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	meta, ok := idx.items[remotePath]
	if !ok {
		return nil, false
	}
	cp := cloneWriteBackMeta(meta)
	return &cp, true
}

// HasPending reports whether a pending entry exists for the path.
func (idx *PendingIndex) HasPending(remotePath string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.items[remotePath]
	return ok
}

// Remove deletes metadata for the given path from memory and disk.
func (idx *PendingIndex) Remove(remotePath string) {
	pl := idx.acquirePathLock(remotePath)
	idx.mu.Lock()
	delete(idx.items, remotePath)
	idx.mu.Unlock()

	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	_ = os.Remove(metaPath)
	idx.releasePathLock(remotePath, pl)
}

// RemoveIfGeneration atomically checks that the current generation matches
// expectedGen before removing the entry. Returns true if the entry was
// removed. This prevents a stale upload from removing a fresher pending entry
// that was created (via a newer Put) while the old upload was in flight.
//
// The per-path lock is held across the generation check, the map removal AND
// the disk .meta removal, so a concurrent same-path Put cannot publish its new
// generation in between and have its fresh .meta deleted by this stale remove.
func (idx *PendingIndex) RemoveIfGeneration(remotePath string, expectedGen uint64) bool {
	pl := idx.acquirePathLock(remotePath)
	idx.mu.Lock()
	meta, ok := idx.items[remotePath]
	if !ok || meta.Generation != expectedGen {
		idx.mu.Unlock()
		idx.releasePathLock(remotePath, pl)
		return false
	}
	delete(idx.items, remotePath)
	idx.mu.Unlock()

	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	_ = os.Remove(metaPath)
	idx.releasePathLock(remotePath, pl)
	return true
}

// Generation returns the current generation for a path, or 0 if not found.
func (idx *PendingIndex) Generation(remotePath string) uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if meta, ok := idx.items[remotePath]; ok {
		return meta.Generation
	}
	return 0
}

// ReparentSnapshotIfGeneration path-compresses one exact process-local
// pending snapshot after its direct queued parent is coalesced. The generation
// and snapshot checks prevent an older queue entry from rewriting newer
// in-memory metadata. Lineage is deliberately not persisted across restart.
func (idx *PendingIndex) ReparentSnapshotIfGeneration(remotePath string, expectedGen uint64, snapshotID, expectedParentID, newParentID string) (bool, error) {
	if idx == nil || remotePath == "" || expectedGen == 0 || snapshotID == "" || expectedParentID == "" {
		return false, nil
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	meta, ok := idx.items[remotePath]
	if !ok || meta.Generation != expectedGen || meta.SnapshotID != snapshotID || meta.ParentSnapshotID != expectedParentID {
		return false, nil
	}
	meta.ParentSnapshotID = newParentID
	return true, nil
}

// RenamePending atomically moves a pending entry from oldPath to newPath.
// Returns true if there was a pending entry to rename.
func (idx *PendingIndex) RenamePending(oldPath, newPath string) bool {
	pla, plb := idx.acquireTwoPathLocks(oldPath, newPath)
	defer idx.releaseTwoPathLocks(oldPath, newPath, pla, plb)

	idx.mu.RLock()
	meta, ok := idx.items[oldPath]
	if !ok {
		idx.mu.RUnlock()
		return false
	}
	// Copy fields under read lock.
	gen := idx.nextGen.Add(1)
	newMeta := cloneWriteBackMeta(meta)
	newMeta.Path = newPath
	newMeta.Generation = gen
	idx.mu.RUnlock()

	// Persist new meta to disk BEFORE updating memory so that crash
	// recovery always has a consistent view.
	metaBytes, _ := json.Marshal(&newMeta)
	newMetaPath := filepath.Join(idx.dir, hashPath(newPath)+".meta")
	if err := atomicWrite(newMetaPath, metaBytes); err != nil {
		return false
	}

	idx.mu.Lock()
	delete(idx.items, oldPath)
	idx.items[newPath] = &newMeta
	idx.mu.Unlock()

	oldMetaPath := filepath.Join(idx.dir, hashPath(oldPath)+".meta")
	_ = os.Remove(oldMetaPath)

	return true
}

// PrepareRename is phase one of a crash-safe rename: it persists the pending
// entry under newPath on disk while oldPath stays authoritative in memory and
// on disk. With both .meta files durable, a crash leaves the data reachable
// no matter whether the shadow file has moved yet — recovery prunes whichever
// side has no shadow. Returns the prepared meta for CommitRename, nil meta
// (with nil error) when there is no pending entry for oldPath, and a non-nil
// error when persisting the prepared meta fails.
func (idx *PendingIndex) PrepareRename(oldPath, newPath string) (*WriteBackMeta, error) {
	pla, plb := idx.acquireTwoPathLocks(oldPath, newPath)
	defer idx.releaseTwoPathLocks(oldPath, newPath, pla, plb)

	idx.mu.RLock()
	meta, ok := idx.items[oldPath]
	if !ok {
		idx.mu.RUnlock()
		return nil, nil
	}
	gen := idx.nextGen.Add(1)
	newMeta := cloneWriteBackMeta(meta)
	newMeta.Path = newPath
	newMeta.Generation = gen
	idx.mu.RUnlock()

	metaBytes, err := json.Marshal(&newMeta)
	if err != nil {
		return nil, fmt.Errorf("marshal prepared meta for %s: %w", newPath, err)
	}
	if err := atomicWrite(filepath.Join(idx.dir, hashPath(newPath)+".meta"), metaBytes); err != nil {
		return nil, fmt.Errorf("persist prepared meta for %s: %w", newPath, err)
	}
	return &newMeta, nil
}

// CommitRename completes a rename prepared by PrepareRename after the shadow
// file has moved: newPath becomes authoritative in memory, oldPath is removed
// from memory and disk.
func (idx *PendingIndex) CommitRename(oldPath string, newMeta *WriteBackMeta) {
	pla, plb := idx.acquireTwoPathLocks(oldPath, newMeta.Path)
	defer idx.releaseTwoPathLocks(oldPath, newMeta.Path, pla, plb)

	idx.mu.Lock()
	delete(idx.items, oldPath)
	idx.items[newMeta.Path] = newMeta
	idx.mu.Unlock()

	_ = os.Remove(filepath.Join(idx.dir, hashPath(oldPath)+".meta"))
}

// AbortRename rolls back PrepareRename by deleting the prepared on-disk meta
// for newPath. It refuses to touch disk when newPath is live in memory, so an
// unrelated pending entry at the target path cannot be destroyed.
func (idx *PendingIndex) AbortRename(newPath string) {
	pl := idx.acquirePathLock(newPath)
	defer idx.releasePathLock(newPath, pl)

	idx.mu.RLock()
	_, live := idx.items[newPath]
	idx.mu.RUnlock()
	if live {
		return
	}
	_ = os.Remove(filepath.Join(idx.dir, hashPath(newPath)+".meta"))
}

// ListPendingPaths returns the set of remote paths that have pending entries.
func (idx *PendingIndex) ListPendingPaths() map[string]struct{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.items) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(idx.items))
	for k := range idx.items {
		result[k] = struct{}{}
	}
	return result
}

// ConflictSummary reports pending entries preserved for manual recovery after
// terminal commit failure.
func (idx *PendingIndex) ConflictSummary() (count int, bytes int64, firstPath string) {
	if idx == nil {
		return 0, 0, ""
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for path, meta := range idx.items {
		if meta == nil || meta.Kind != PendingConflict {
			continue
		}
		if firstPath == "" {
			firstPath = path
		}
		count++
		bytes += meta.Size
	}
	return count, bytes, firstPath
}

// ListByPrefix returns metadata for all paths with the given prefix.
func (idx *PendingIndex) ListByPrefix(prefix string) []*WriteBackMeta {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []*WriteBackMeta
	for p, meta := range idx.items {
		if strings.HasPrefix(p, prefix) {
			cp := cloneWriteBackMeta(meta)
			result = append(result, &cp)
		}
	}
	return result
}

// UpdateSize updates only the size field for an existing entry.
func (idx *PendingIndex) UpdateSize(remotePath string, size int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if meta, ok := idx.items[remotePath]; ok {
		meta.Size = size
		meta.Mtime = time.Now()
	}
}

// UpdateMode updates the pending mode metadata for an existing entry and
// returns the new generation. The generation is always uncommitted until the
// caller proves the corresponding remote mutation succeeded.
func (idx *PendingIndex) UpdateMode(remotePath string, mode uint32) (uint64, error) {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	meta, ok := idx.items[remotePath]
	if !ok {
		return 0, nil
	}
	updated := cloneWriteBackMeta(meta)
	updated.Mode = mode & posixPermissionModeMask
	updated.HasMode = true
	// This is a new local mutation generation. It is not remotely committed
	// merely because the preceding retained layer generation was.
	updated.LayerCommitted = false
	updated.Generation = idx.nextGen.Add(1)
	updated.Mtime = time.Now()

	metaBytes, err := json.Marshal(&updated)
	if err != nil {
		return 0, fmt.Errorf("pending index marshal mode: %w", err)
	}
	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return 0, fmt.Errorf("pending index update mode: %w", err)
	}
	cp := updated
	idx.items[remotePath] = &cp
	return updated.Generation, nil
}

// MarkLayerCommitted keeps the pending entry as a local overlay data source but
// excludes the exact uploaded generation from recovery. It returns false when
// the caller has no generation fence or a newer same-path generation replaced
// the uploaded entry before bookkeeping completed; that generation must remain
// recoverable.
func (idx *PendingIndex) MarkLayerCommitted(remotePath string, expectedGen uint64, committedRev int64) (bool, error) {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	meta, ok := idx.items[remotePath]
	if !ok {
		return true, nil
	}
	if expectedGen == 0 || meta.Generation != expectedGen {
		return false, nil
	}
	updated := cloneWriteBackMeta(meta)
	updated.Kind = PendingOverwrite
	updated.LayerCommitted = true
	if committedRev > 0 {
		updated.BaseRev = committedRev
	}
	updated.Generation = idx.nextGen.Add(1)
	updated.Mtime = time.Now()

	metaBytes, err := json.Marshal(&updated)
	if err != nil {
		return false, fmt.Errorf("pending index marshal committed: %w", err)
	}
	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return false, fmt.Errorf("pending index mark committed: %w", err)
	}
	cp := updated
	idx.items[remotePath] = &cp
	return true, nil
}

// MarkConflict marks a pending entry as conflicted so that RecoverPending
// skips it on restart. The entry is kept on disk for manual recovery.
// The in-memory state is only updated after the disk write succeeds to
// ensure crash recovery sees a consistent view.
func (idx *PendingIndex) MarkConflict(remotePath string) error {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	idx.mu.RLock()
	meta, ok := idx.items[remotePath]
	if !ok {
		idx.mu.RUnlock()
		return nil
	}
	// Build a copy with the conflict marker for disk persistence.
	conflicted := *meta
	conflicted.Kind = PendingConflict
	idx.mu.RUnlock()

	// Persist to disk first so crash recovery also sees the conflict marker.
	metaBytes, err := json.Marshal(&conflicted)
	if err != nil {
		return fmt.Errorf("pending index marshal conflict: %w", err)
	}
	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return fmt.Errorf("pending index write conflict: %w", err)
	}

	// Only update in-memory state after disk write succeeds.
	idx.mu.Lock()
	if m, exists := idx.items[remotePath]; exists {
		m.Kind = PendingConflict
	}
	idx.mu.Unlock()
	return nil
}

// MarkConflictIfGeneration is MarkConflict scoped to a specific pending-entry
// generation. It returns false without touching disk or memory when a newer
// same-path Put has already replaced the entry, so a stale commit failure does
// not poison fresher pending data.
func (idx *PendingIndex) MarkConflictIfGeneration(remotePath string, expectedGen uint64) (bool, error) {
	if expectedGen == 0 {
		return true, idx.MarkConflict(remotePath)
	}
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	idx.mu.RLock()
	meta, ok := idx.items[remotePath]
	if !ok {
		idx.mu.RUnlock()
		return true, nil
	}
	if meta.Generation != expectedGen {
		idx.mu.RUnlock()
		return false, nil
	}
	conflicted := *meta
	conflicted.Kind = PendingConflict
	idx.mu.RUnlock()

	metaBytes, err := json.Marshal(&conflicted)
	if err != nil {
		return false, fmt.Errorf("pending index marshal conflict: %w", err)
	}
	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return false, fmt.Errorf("pending index write conflict: %w", err)
	}

	idx.mu.Lock()
	if m, exists := idx.items[remotePath]; exists && m.Generation == expectedGen {
		m.Kind = PendingConflict
		idx.mu.Unlock()
		return true, nil
	}
	idx.mu.Unlock()
	return false, nil
}

// Count returns the number of pending entries.
func (idx *PendingIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.items)
}
