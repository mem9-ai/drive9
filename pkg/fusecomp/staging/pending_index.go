package staging

import (
	"encoding/json"
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
	layerPublicationMu sync.RWMutex // rollback waits for publishers before dropping caches
	mu                 sync.RWMutex
	items              map[string]*WriteBackMeta // path → metadata
	dir                string                    // directory for .meta persistence
	nextGen            atomic.Uint64
	pathLocks          map[string]*pendingPathLock
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
	shadows          *ShadowStore // wired once before staging/recovery
	layerAbandoned   bool         // prevents late publications from reviving a rolled-back overlay
}

// pendingPathLock is a refcounted per-path mutex, mirroring WriteBackCache's
// pathLockEntry. Lock order: path lock (outer) → idx.mu (inner).
type pendingPathLock struct {
	mu      sync.Mutex
	waiters int
}

// NewPendingIndex creates a PendingIndex backed by the given directory.
// The directory is created if it does not exist.
func NewPendingIndex(dir string) (*PendingIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("pending index dir: %w", err)
	}
	idx := &PendingIndex{
		items:     make(map[string]*WriteBackMeta),
		dir:       dir,
		pathLocks: make(map[string]*pendingPathLock),
	}
	return idx, nil
}

// Dir reports the index's on-disk directory (recovery and tests rebuild a
// fresh index over it).
func (idx *PendingIndex) Dir() string {
	if idx == nil {
		return ""
	}
	return idx.dir
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

// PutLayerCache publishes content already persisted in a Layer. It remains
// visible through the overlay index, but does not require another upload.
func (idx *PendingIndex) PutLayerCache(remotePath string, size int64, baseRev int64, mode uint32, hasMode bool, identity LayerCacheIdentity) (uint64, error) {
	return idx.putInternal(WriteBackMeta{Path: remotePath, Size: size, Kind: PendingOverwrite, BaseRev: baseRev, Mode: mode, HasMode: hasMode, LayerClean: true, LayerID: identity.LayerID, LayerEntrySeq: identity.EntrySeq})
}

// putLayerCacheIfAbsent never replaces a publication that arrived while a
// chmod with no initial metadata was reading or uploading its payload.

// PutWithBaseRevAndModeAndLineage attaches process-local causal identity to
// the durable metadata. The IDs are intentionally excluded from JSON; empty
// lineage is accepted but can never authorize a growth rebase.
func (idx *PendingIndex) PutWithBaseRevAndModeAndLineage(remotePath string, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, liveAncestors ...string) (uint64, error) {
	return idx.putInternal(WriteBackMeta{Path: remotePath, Size: size, Kind: kind, BaseRev: baseRev, Mode: mode, HasMode: hasMode, SnapshotID: snapshotID, ParentSnapshotID: parentSnapshotID, lineageTrusted: lineageTrusted, liveAncestors: liveAncestors})
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
	return idx.putInternal(WriteBackMeta{Path: remotePath, Size: size, Kind: kind, BaseRev: baseRev, Mode: mode, HasMode: hasMode, SnapshotID: snapshotID, ParentSnapshotID: parentSnapshotID, lineageTrusted: lineageTrusted, liveAncestors: liveAncestors, ShadowSpill: true})
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

	metaPath := filepath.Join(idx.dir, HashPath(remotePath)+".meta")
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

	metaPath := filepath.Join(idx.dir, HashPath(remotePath)+".meta")
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
	newMetaPath := filepath.Join(idx.dir, HashPath(newPath)+".meta")
	if err := atomicWrite(newMetaPath, metaBytes); err != nil {
		return false
	}

	idx.mu.Lock()
	delete(idx.items, oldPath)
	idx.items[newPath] = &newMeta
	idx.mu.Unlock()

	oldMetaPath := filepath.Join(idx.dir, HashPath(oldPath)+".meta")
	_ = os.Remove(oldMetaPath)

	return true
}

// PendingConflictTransfer is a fenced, two-phase move of a conflict-marked
// pending entry — metadata AND payload — from one pathname to another. It
// holds BOTH endpoint path locks for its whole lifetime, so no same-path
// publication (or a caller's own staging) can slip a newer generation in
// between the prepare and the payload move: whatever generation the
// destination ends up paired with is the one the transaction fenced on.
//
// Phase order is chosen so a crash at any point leaves every surviving .meta
// paired with the bytes that actually sit beside it:
//   - prepare: when the destination has no entry, its .meta is persisted
//     FIRST, while the old name keeps both its .meta and its payload;
//   - move: a LIVE destination first loses its own payload (its object
//     incarnation was already replaced by the remote rename that precedes
//     this transaction) and then receives the prepared .meta — only then is
//     the source payload renamed over it, so the prepared meta can never be
//     observed beside the dead bytes it does not describe;
//   - commit: the destination .meta is (re)written to describe the moved
//     payload, memory is published, and the old .meta is removed.
//
// A destination that already carried an entry is deliberately NOT touched at
// prepare time: its own .meta and payload stay exactly as they were until the
// move, so an abort before the move has nothing to restore and a crash leaves
// both sides describing their own bytes.
type PendingConflictTransfer struct {
	idx     *PendingIndex
	shadows *ShadowStore
	oldPath string
	newPath string
	locks   [2]*pendingPathLock
	// prepared is the source entry re-homed under newPath with a fresh
	// generation. It is only published by Commit (or PublishDestination).
	prepared *WriteBackMeta
	// destWasLive records that newPath carried an entry when the transfer
	// began; its .meta and payload stay untouched until the move.
	destWasLive bool
	// destReplaced records that MovePayload already retired the live
	// destination's own payload and persisted the prepared .meta there: from
	// that point the destination durably describes the source image, so
	// PublishDestination publishes it instead of skipping, and Abort can no
	// longer restore the dead entry.
	destReplaced bool
	// destMetaPersisted records that the prepared .meta is durable at the
	// destination path (written at prepare for an empty destination, at the
	// move for a live one).
	destMetaPersisted bool
	// payloadMoved records that the source payload was renamed to the
	// destination: the durable state already describes the committed pairing,
	// so Abort degenerates into finishing the publication.
	payloadMoved   bool
	expectedOldGen uint64
	expectedNewGen uint64
	closed         bool
}

// BeginConflictTransfer opens a fenced transfer and, when the destination is
// empty, persists its prepared .meta so a crash before the caller's payload
// move leaves both names recoverable. It reports (nil, false, nil) when either
// endpoint no longer matches the generations the caller captured, and a
// non-nil error only when the prepared metadata could not be persisted.
//
// A LIVE source entry (a queued image, not a terminal-failure marker) is
// durably flipped to PendingConflict BEFORE the destination is prepared: a
// crash between the two writes must leave recovery skipping the old name —
// preserved, never re-driven at the dead path the successful remote rename
// left behind — instead of re-uploading it there through the 404/create
// retry. The prepared destination keeps the entry's LIVE kind: the
// obligation it carries is still a landing, not a terminal failure.
func (idx *PendingIndex) BeginConflictTransfer(oldPath, newPath string, expectedOldGen, expectedNewGen uint64, shadows *ShadowStore) (*PendingConflictTransfer, bool, error) {
	if idx == nil || oldPath == newPath || expectedOldGen == 0 {
		return nil, false, nil
	}
	idx.mu.RLock()
	srcKind := PendingConflict
	if m, ok := idx.items[oldPath]; ok && m != nil {
		srcKind = m.Kind
	}
	idx.mu.RUnlock()
	if srcKind != PendingConflict {
		if _, err := idx.MarkConflictIfGeneration(oldPath, expectedOldGen); err != nil {
			return nil, false, err
		}
	}
	pla, plb := idx.acquireTwoPathLocks(oldPath, newPath)
	prepared, ok, destWasLive, err := idx.prepareConflictTransferLocked(oldPath, newPath, expectedOldGen, expectedNewGen, srcKind)
	if err != nil || !ok {
		idx.releaseTwoPathLocks(oldPath, newPath, pla, plb)
		return nil, false, err
	}
	return &PendingConflictTransfer{
		idx:               idx,
		shadows:           shadows,
		oldPath:           oldPath,
		newPath:           newPath,
		locks:             [2]*pendingPathLock{pla, plb},
		prepared:          prepared,
		destWasLive:       destWasLive,
		destMetaPersisted: !destWasLive,
		expectedOldGen:    expectedOldGen,
		expectedNewGen:    expectedNewGen,
	}, true, nil
}

// prepareConflictTransferLocked fences both endpoints and builds the prepared
// meta. The caller holds both path locks. It returns the prepared meta, ok,
// whether the destination was live, and an error when the prepared file could
// not be persisted.
func (idx *PendingIndex) prepareConflictTransferLocked(oldPath, newPath string, expectedOldGen, expectedNewGen uint64, keepKind PendingKind) (*WriteBackMeta, bool, bool, error) {
	idx.mu.RLock()
	src, hasSrc := idx.items[oldPath]
	dst, hasDst := idx.items[newPath]
	idx.mu.RUnlock()
	if !hasSrc || src == nil || src.Generation != expectedOldGen {
		return nil, false, false, nil
	}
	if hasDst {
		if dst == nil || dst.Generation != expectedNewGen {
			return nil, false, false, nil
		}
	} else if expectedNewGen != 0 {
		return nil, false, false, nil
	}
	gen := idx.nextGen.Add(1)
	clone := cloneWriteBackMeta(src)
	clone.Path = newPath
	clone.Generation = gen
	// A live source was flipped to a marker at Begin so recovery cannot
	// re-drive the old name; the prepared clone keeps the LIVE kind the
	// destination must carry.
	clone.Kind = keepKind
	if src.Kind == PendingConflict {
		// The moved marker describes the same failed image the source entry
		// described: remember that image's generation, so a landing which
		// superseded the SOURCE generation — what every retained handle and
		// pre-staged cache snapshot recorded before the move — recovers the
		// marker even though the move itself minted a newer generation.
		clone.SupersedesPendingGen = src.Generation
	}
	prepared := &clone
	if !hasDst {
		if err := idx.writePreparedWitness(prepared); err != nil {
			return nil, false, false, err
		}
	}
	return prepared, true, hasDst, nil
}

// writePreparedWitness persists the prepared entry at the destination as a
// CONFLICT-kind witness while the transaction is in flight: every crash
// point before the commit must leave the destination either empty or
// carrying a durable EIO marker — recovery preserves shadowless conflict
// entries precisely so this witness survives restart instead of being
// pruned like an orphaned live entry. The commit rewrites the entry with
// its true kind (live for a moved queued image) once the payload sits
// beside it.
func (idx *PendingIndex) writePreparedWitness(prepared *WriteBackMeta) error {
	witness := cloneWriteBackMeta(prepared)
	witness.Kind = PendingConflict
	return idx.writeMetaFile(&witness)
}

// MovePayload moves the preserved shadow bytes to the new name. It reports
// false when the source has no payload to move or the move failed; the
// transaction is then still open and must be aborted or published.
//
// For a destination that carried its own entry, the move runs in a fixed
// order — retire the dead destination payload, persist the prepared .meta,
// then rename the source payload over it — so every intermediate crash state
// keeps each surviving .meta paired with the bytes beside it: the old
// destination .meta can only ever be observed WITHOUT its bytes (pruned on
// recovery), and the prepared .meta can only ever be observed without bytes
// or beside the source's bytes, never beside the dead destination's.
func (t *PendingConflictTransfer) MovePayload() bool {
	if t == nil || t.closed || t.prepared == nil {
		return false
	}
	if t.shadows == nil {
		return true // metadata-only transfer: there are no preserved bytes
	}
	if t.destWasLive && !t.destReplaced {
		if !t.shadows.Has(t.oldPath) {
			// No preserved bytes to pair with the marker: leave the
			// destination's own entry untouched for PublishDestination.
			return false
		}
		// The destination's own pending image belongs to an object incarnation
		// the completed remote rename already replaced: retire its payload
		// BEFORE the prepared .meta can describe this path, so no crash can
		// pair that .meta with the dead bytes. Remove's cleanup ignores disk
		// errors, so the retirement must be CONFIRMED (VerifyRemoved) before
		// the relabel: with the payload still beside the path, writing the
		// prepared .meta would pair the source's size/base revision with the
		// dead destination's bytes — durably, surviving a restart, exactly
		// the mismatch this ordering exists to prevent.
		t.shadows.Remove(t.newPath)
		if verr := t.shadows.VerifyRemoved(t.newPath); verr != nil {
			return false
		}
		t.destReplaced = true
		if err := t.idx.writePreparedWitness(t.prepared); err != nil {
			return false
		}
		t.destMetaPersisted = true
	}
	if t.shadows.Rename(t.oldPath, t.newPath) {
		t.payloadMoved = true
		return true
	}
	return false
}

// Commit re-verifies both fences, publishes the destination metadata (durable
// first), and removes the old name's entry. It reports false when a fence was
// lost; nothing is changed in that case. A non-nil error means the
// destination metadata could not be persisted — the payload has already
// moved, so the caller must surface the failure.
// PreparedGeneration reports the generation the prepared destination entry
// will carry once committed: the rename flow records it so its own second
// pass can distinguish the entry IT installed from anything else published
// at the destination meanwhile.
func (t *PendingConflictTransfer) PreparedGeneration() uint64 {
	if t == nil || t.prepared == nil {
		return 0
	}
	return t.prepared.Generation
}

func (t *PendingConflictTransfer) Commit() (bool, error) {
	if t == nil || t.closed || t.prepared == nil {
		return false, nil
	}
	defer t.release()

	if ok := t.fencesHold(); !ok {
		return false, nil
	}
	if err := t.idx.writeMetaFile(t.prepared); err != nil {
		return false, err
	}
	t.idx.mu.Lock()
	cp := *t.prepared
	t.idx.items[t.newPath] = &cp
	delete(t.idx.items, t.oldPath)
	t.idx.mu.Unlock()

	_ = os.Remove(filepath.Join(t.idx.dir, HashPath(t.oldPath)+".meta"))
	safeLogPrintf("pending: conflict marker moved %s -> %s (gen %d -> %d)", t.oldPath, t.newPath, t.expectedOldGen, t.prepared.Generation)
	return true, nil
}

// Abort rolls the transaction back. Before any payload move it is exact: an
// empty destination only needs the prepared file removed, a live destination
// was never touched, and the old name keeps its entry and payload either way.
// Once the payload has moved, the durable state already describes the
// committed pairing and a rollback would strand the moved bytes without
// metadata — Abort then degenerates into finishing the publication.
func (t *PendingConflictTransfer) Abort() {
	if t == nil || t.closed {
		return
	}
	if t.payloadMoved {
		_, _ = t.Commit()
		return
	}
	defer t.release()
	if t.destReplaced {
		// The move retired the destination's own payload (its incarnation was
		// replaced by the remote rename) and may have persisted the prepared
		// .meta beside no bytes: restore silence at the destination so no
		// surviving .meta describes missing bytes. The source keeps its entry
		// and payload — the rename of the shadow never happened.
		_ = os.Remove(filepath.Join(t.idx.dir, HashPath(t.newPath)+".meta"))
		t.idx.mu.Lock()
		delete(t.idx.items, t.newPath)
		t.idx.mu.Unlock()
		return
	}
	if !t.destWasLive {
		_ = os.Remove(filepath.Join(t.idx.dir, HashPath(t.newPath)+".meta"))
	}
}

// PublishDestination installs the prepared marker at the destination WITHOUT
// a payload move: the caller's payload move failed after the pathname had
// already moved remotely, so the new name must not report durability for
// bytes that are still only under the old name. The old name keeps its own
// entry — and the payload — so the data stays recoverable when the
// destination's own entry is pruned on restart (it has no shadow).
//
// A destination that was live and was NOT yet replaced (the move was refused
// before it could start) keeps its own entry: it still describes its own
// bytes, and overwriting it would mislabel them — the caller must record the
// failure in memory instead. Once the move has replaced the destination, the
// prepared marker is (or must become) its durable state and is published.
func (t *PendingConflictTransfer) PublishDestination() error {
	if t == nil || t.closed || t.prepared == nil {
		if t != nil && !t.closed {
			t.release()
		}
		return nil
	}
	if t.destWasLive && !t.destReplaced {
		// A live destination still carries its own marker describing its own
		// bytes; overwriting it would mislabel them.
		t.release()
		return nil
	}
	defer t.release()
	if !t.destMetaPersisted {
		if err := t.idx.writeMetaFile(t.prepared); err != nil {
			return err
		}
	}
	t.idx.mu.Lock()
	cp := *t.prepared
	t.idx.items[t.newPath] = &cp
	t.idx.mu.Unlock()
	safeLogPrintf("pending: conflict marker published at %s (payload stayed at %s)", t.newPath, t.oldPath)
	return nil
}

// fencesHold re-checks both endpoints under the held locks: a same-path
// publication cannot interleave, so this is defence in depth against a
// caller that mutated the index by other means.
func (t *PendingConflictTransfer) fencesHold() bool {
	t.idx.mu.RLock()
	src, hasSrc := t.idx.items[t.oldPath]
	dst, hasDst := t.idx.items[t.newPath]
	t.idx.mu.RUnlock()
	if !hasSrc || src == nil || src.Generation != t.expectedOldGen {
		safeLogPrintf("pending: conflict transfer %s -> %s lost its source fence (generation advanced)", t.oldPath, t.newPath)
		return false
	}
	if hasDst {
		if dst == nil || dst.Generation != t.expectedNewGen {
			safeLogPrintf("pending: conflict transfer %s -> %s lost its destination fence (generation advanced)", t.oldPath, t.newPath)
			return false
		}
	} else if t.expectedNewGen != 0 {
		return false
	}
	return true
}

func (t *PendingConflictTransfer) release() {
	if t.closed {
		return
	}
	t.closed = true
	t.idx.releaseTwoPathLocks(t.oldPath, t.newPath, t.locks[0], t.locks[1])
}

// writeMetaFile persists one pending entry's .meta under the index directory.
func (idx *PendingIndex) writeMetaFile(meta *WriteBackMeta) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal pending meta for %s: %w", meta.Path, err)
	}
	if err := atomicWrite(filepath.Join(idx.dir, HashPath(meta.Path)+".meta"), raw); err != nil {
		return fmt.Errorf("persist pending meta for %s: %w", meta.Path, err)
	}
	return nil
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
	if err := atomicWrite(filepath.Join(idx.dir, HashPath(newPath)+".meta"), metaBytes); err != nil {
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

	_ = os.Remove(filepath.Join(idx.dir, HashPath(oldPath)+".meta"))
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
	_ = os.Remove(filepath.Join(idx.dir, HashPath(newPath)+".meta"))
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

// ListUncommittedPaths excludes clean Layer overlays without hiding conflicts
// or legacy records whose durability is unknown.
func (idx *PendingIndex) ListUncommittedPaths() map[string]struct{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	paths := make(map[string]struct{})
	for path, meta := range idx.items {
		if !meta.LayerClean || meta.Kind == PendingConflict {
			paths[path] = struct{}{}
		}
	}
	return paths
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

// UpdateMode updates the pending mode metadata for an existing entry.
func (idx *PendingIndex) UpdateMode(remotePath string, mode uint32) error {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	meta, ok := idx.items[remotePath]
	if !ok {
		return nil
	}
	updated := cloneWriteBackMeta(meta)
	updated.Mode = mode & posixPermissionModeMask
	updated.HasMode = true
	updated.LayerClean = false
	updated.Generation = idx.nextGen.Add(1)
	updated.Mtime = time.Now()

	metaBytes, err := json.Marshal(&updated)
	if err != nil {
		return fmt.Errorf("pending index marshal mode: %w", err)
	}
	if idx.journal != nil {
		if err := idx.journal.Append(JournalEntry{Op: JournalPendingMeta, Path: remotePath, Meta: metaBytes}); err != nil {
			return fmt.Errorf("pending index journal mode: %w", err)
		}
		if err := idx.journal.FsyncShared(); err != nil {
			return fmt.Errorf("pending index sync mode: %w", err)
		}
	} else {
		metaPath := filepath.Join(idx.dir, HashPath(remotePath)+".meta")
		if err := atomicWrite(metaPath, metaBytes); err != nil {
			return fmt.Errorf("pending index update mode: %w", err)
		}
	}
	cp := updated
	idx.items[remotePath] = &cp
	return nil
}

// MarkLayerCommittedIfGeneration marks only the uploaded publication as a
// durable overlay. An older completion cannot acknowledge a newer local edit.
func (idx *PendingIndex) MarkLayerCommittedIfGeneration(remotePath string, generation uint64, committedRev int64, mode uint32, hasMode bool, identity LayerCacheIdentity) error {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	meta, ok := idx.items[remotePath]
	if !ok || generation == 0 || meta.Generation != generation || idx.layerAbandoned || meta.Kind == PendingConflict {
		return nil
	}
	updated := cloneWriteBackMeta(meta)
	updated.Kind = PendingOverwrite
	updated.LayerClean = true
	updated.LayerID = identity.LayerID
	updated.LayerEntrySeq = identity.EntrySeq
	if hasMode {
		updated.Mode = mode & posixPermissionModeMask
		updated.HasMode = true
	}
	if committedRev > 0 {
		updated.BaseRev = committedRev
	}
	updated.Mtime = time.Now()

	metaBytes, err := json.Marshal(&updated)
	if err != nil {
		return fmt.Errorf("pending index marshal committed: %w", err)
	}
	if idx.journal != nil {
		// Publish under the same path lock as Put. A path-only commit marker
		// could otherwise hide a newer WAL publication after a crash.
		if err := idx.journal.Append(JournalEntry{Op: JournalPendingMeta, Path: remotePath, Meta: metaBytes}); err != nil {
			return fmt.Errorf("pending index mark layer committed: %w", err)
		}
		if err := idx.journal.FsyncShared(); err != nil {
			return fmt.Errorf("pending index sync layer commit: %w", err)
		}
	} else {
		metaPath := filepath.Join(idx.dir, HashPath(remotePath)+".meta")
		if err := atomicWrite(metaPath, metaBytes); err != nil {
			return fmt.Errorf("pending index mark layer committed: %w", err)
		}
	}
	cp := updated
	idx.items[remotePath] = &cp
	return nil
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
	metaPath := filepath.Join(idx.dir, HashPath(remotePath)+".meta")
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

// EnsureConflictMarker installs a shadowless conflict marker at remotePath
// when the index holds no entry there: a rename's standalone failure record
// (its unlanded image preserved under a different, now-dead name) has no
// pending entry of its own to carry the state, and recovery preserves
// shadowless conflict markers precisely so this witness survives restart —
// a fresh process answers EIO from it instead of certifying the stale
// destination image. An existing entry (any kind) is left untouched: the
// caller's own markers are richer than this minimal witness.
func (idx *PendingIndex) EnsureConflictMarker(remotePath string) error {
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)

	idx.mu.RLock()
	_, exists := idx.items[remotePath]
	idx.mu.RUnlock()
	if exists {
		return nil
	}
	meta := WriteBackMeta{
		Path:       remotePath,
		Kind:       PendingConflict,
		LayerClean: false,
	}
	gen := idx.nextGen.Add(1)
	meta.Generation = gen
	if err := idx.writeMetaFile(&meta); err != nil {
		return fmt.Errorf("pending index witness marker for %s: %w", remotePath, err)
	}
	idx.mu.Lock()
	if _, exists := idx.items[remotePath]; !exists {
		cp := meta
		idx.items[remotePath] = &cp
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
	metaPath := filepath.Join(idx.dir, HashPath(remotePath)+".meta")
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

// discardLayerCache removes only the captured clean publication. The WAL
// tombstone is serialized with Put, so restart cannot resurrect the cache or
// accidentally suppress a newer dirty publication. Newer shadow bytes survive.

// uncommittedGenerations snapshots publication identities as well as paths:
// replacing one unsent write with another is progress, even at the same path.

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

func (idx *PendingIndex) putInternal(meta WriteBackMeta) (uint64, error) {
	meta.Mode &= posixPermissionModeMask
	meta.liveAncestors = append([]string(nil), meta.liveAncestors...)
	return idx.publishMeta(&meta, false)
}

// publishMeta serializes a publication, including an optional absence check,
// with acknowledgements and rollback cache removal.
func (idx *PendingIndex) publishMeta(meta *WriteBackMeta, onlyIfAbsent bool) (uint64, error) {
	idx.layerPublicationMu.RLock()
	defer idx.layerPublicationMu.RUnlock()
	remotePath := meta.Path
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)
	idx.mu.RLock()
	abandoned := idx.layerAbandoned
	previous, exists := idx.items[remotePath]
	if exists && !meta.LayerClean {
		// Dirty staging retains the Layer snapshot it was based on. Replay
		// needs this identity to detect an external update after Flush.
		meta.LayerID, meta.LayerEntrySeq = previous.LayerID, previous.LayerEntrySeq
		// Re-staging is not conflict resolution. In particular, a first
		// write can be conflicted without ever having had a Layer identity.
		if previous.Kind == PendingConflict {
			meta.Kind = PendingConflict
		}
	}
	idx.mu.RUnlock()
	if onlyIfAbsent && exists {
		return 0, nil
	}
	if abandoned {
		if meta.LayerClean {
			return 0, errLayerRolledBack
		}
		meta.Kind = PendingConflict
	}
	meta.Mtime = time.Now()
	meta.CreatedAt = meta.Mtime
	gen := idx.nextGen.Add(1)
	meta.Generation = gen

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
			Op:        JournalPendingMeta,
			Path:      remotePath,
			Timestamp: time.Now().Unix(),
			Meta:      metaBytes,
		}); err != nil {
			return 0, fmt.Errorf("pending index journal append: %w", err)
		}
		if syncOnPut {
			if err := journal.FsyncShared(); err != nil {
				return 0, fmt.Errorf("pending index journal fsync: %w", err)
			}
		}
	} else {
		metaPath := filepath.Join(idx.dir, HashPath(remotePath)+".meta")
		if err := atomicWrite(metaPath, metaBytes); err != nil {
			return 0, fmt.Errorf("pending index put meta: %w", err)
		}
	}

	idx.mu.Lock()
	idx.items[remotePath] = meta
	idx.mu.Unlock()

	return gen, nil
}

// abandonLayer fences clean publishers already in progress and makes future
// dirty publications conflicts instead of reviving the abandoned Layer.

// publishRecoveredMeta installs a pending entry unmarshaled from a WAL frame
// during crash recovery. It is in-memory only: the WAL record remains on disk
// until compaction drops it behind the path's commit marker, so re-publishing
// the .meta file would be redundant.
func (idx *PendingIndex) publishRecoveredMeta(metaBytes []byte) error {
	var meta WriteBackMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return fmt.Errorf("recovered meta unmarshal: %w", err)
	}
	if meta.Path == "" {
		return fmt.Errorf("recovered meta has empty path")
	}
	idx.mu.Lock()
	idx.items[meta.Path] = &meta
	// Future publication must not reuse a generation restored from the WAL.
	for previous := idx.nextGen.Load(); previous < meta.Generation; previous = idx.nextGen.Load() {
		if idx.nextGen.CompareAndSwap(previous, meta.Generation) {
			break
		}
	}
	idx.mu.Unlock()
	return nil
}
