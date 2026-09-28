package fuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"syscall"
	"time"

	"github.com/pingcap/failpoint"
)

var errFtruncateBusy = fmt.Errorf("fd-truncate operation busy: %w", syscall.EAGAIN)

// A bounded immutable baseline survives the truncating fd. It owns no staging
// generations: copying content never transfers another handle's cleanup rights.
type ftruncateInheritance struct {
	ino       uint64
	path      string
	view      uint64
	seq       uint64
	revision  int64
	id        string
	data      []byte
	complete  bool
	ancestors []string // bounded immutable ancestry of this baseline
}

func (fs *Dat9FS) publishFtruncateInheritanceLocked(fh *FileHandle) {
	if fh.Unlinked || fh.UnlinkedSnapshot || fh.UnlinkedData != nil {
		return
	}
	// Even when writer inheritance is unavailable, readers must stop using
	// pre-truncate pins. An observation without an identity grants no lineage.
	event := &ftruncateInheritance{ino: fh.Ino, path: fh.Path, view: fs.mountViewGeneration.Load(), seq: fh.DirtySeq, revision: fh.BaseRev}
	defer func() { fs.openHandles.notifyFtruncateReaders(event) }()
	// SetAttr holds the writable siblings here. Independently dirty branches
	// predating this truncate remain outside the clean-sibling contract.
	for _, other := range fs.openHandles.SnapshotInode(fh.Ino) {
		if other != fh && other.Flags&syscall.O_ACCMODE != syscall.O_RDONLY && other.Ino == fh.Ino && !other.Unlinked && fs.ftruncateAliasLinked(other) && other.Dirty != nil && other.DirtySeq != 0 && other.pendingFtruncate.Load() == nil {
			return
		}
	}
	if !fh.LineageTrusted {
		clearStagedSnapshotLineageLocked(fh)
		fh.ContentSnapshotID, fh.contentAncestors, fh.ftruncateInherited = "", nil, ""
	}
	id, _ := ensureStagedSnapshotLineageLocked(fh)
	fh.LineageTrusted, fh.StagedLineageTrusted = true, true
	fh.ftruncateInherited = id
	publishStagedSnapshotLineageLocked(fh)
	event.id, event.ancestors = id, append([]string(nil), fh.contentAncestors...)
	if fh.Dirty.Size() <= maxLandedPayloadBytes && fh.Dirty.CanMaterializeFull() {
		event.data, event.complete = fh.Dirty.Bytes(), true
	}
	for _, sibling := range fs.openHandles.SnapshotInode(fh.Ino) {
		if sibling.Flags&syscall.O_ACCMODE != syscall.O_RDONLY && sibling.Ino == fh.Ino && !sibling.Unlinked && fs.ftruncateAliasLinked(sibling) && sibling.Dirty != nil {
			copy := *event
			copy.path = sibling.Path
			sibling.pendingFtruncate.Store(&copy)
		}
	}
}

// The caller holds the path fence. Match direct O_TRUNC's refusal to erase an
// uncommitted successor; do this before the existing pathname truncate effects.
func (fs *Dat9FS) checkFtruncatePathZero(ino uint64, path string) error {
	if fs.hasPendingMetadataState(path) {
		return syscall.EAGAIN
	}
	event := fs.openHandles.ftruncateInheritance(ino, path)
	if event == nil {
		return nil
	}
	for _, fh := range fs.openHandles.SnapshotPath(path) {
		if fh.Ino != ino || fh.Flags&syscall.O_ACCMODE == syscall.O_RDONLY {
			continue
		}
		if !fh.TryLock() {
			return syscall.EAGAIN
		}
		busy := fh.DirtySeq != 0 && ftruncateSnapshotID(fh) != event.id
		fh.Unlock()
		if busy {
			return syscall.EAGAIN
		}
	}
	return nil
}

// Linux can translate O_TRUNC into Open followed by pathname SetAttr. The
// existing pathname truncate resets every open buffer; those equal empty images
// must share a fresh identity rather than retain their different old histories.
func (fs *Dat9FS) resetFtruncateZeroImageLocked(fh *FileHandle, id string) {
	clearStagedSnapshotLineageLocked(fh)
	fh.ContentSnapshotID, fh.contentAncestors, fh.LineageTrusted = id, nil, true
	fh.StagedSnapshotID, fh.StagedSnapshotSeq, fh.StagedLineageTrusted = id, fh.DirtySeq, true
	fh.ftruncateInherited = id
	fh.appendSnapshot = true
	fh.BaseRev = max(fh.BaseRev, fs.latestCommittedRevision(fh.Path))
	fh.OrigSize, fh.ZeroBase = 0, true
	fh.Dirty.visibleTruncate = true
	fs.openHandles.MarkVisibleTruncate(fh, fh.DirtySeq)
	fh.pendingFtruncate.Store(&ftruncateInheritance{ino: fh.Ino, path: fh.Path,
		view: fs.mountViewGeneration.Load(), seq: fh.DirtySeq, revision: fh.BaseRev,
		id: id, complete: true})
	fs.openHandles.notifyFtruncateReaders(fh.pendingFtruncate.Load())
}

func ftruncateSnapshotID(fh *FileHandle) string {
	if fh.StagedSnapshotID != "" && fh.StagedSnapshotSeq == fh.DirtySeq {
		return fh.StagedSnapshotID
	}
	return fh.ContentSnapshotID
}

func ftruncateDescends(fh *FileHandle, id string) bool {
	return id != "" && fh.LineageTrusted && (fh.ftruncateInherited == id || ftruncateSnapshotID(fh) == id ||
		slices.Contains(fh.contentAncestors, id) || slices.Contains(fh.stagedAncestors, id) || fh.ContentSnapshotID == id)
}

func (fs *Dat9FS) ftruncateParticipates(fh *FileHandle) bool {
	return fh != nil && fh.Flags&syscall.O_ACCMODE != syscall.O_RDONLY && fh.pendingFtruncate.Load() != nil && !fh.Unlinked && !fh.UnlinkedSnapshot && fh.UnlinkedData == nil && !fs.layerEnabled()
}

// Unlike lockWritableRemoteCommitPath this never cancels another commit or
// returns an unlocked success. Callers hold fh.mu, so sibling locks use TryLock.
func (fs *Dat9FS) fenceFtruncateLocked(fh *FileHandle) error {
	if fh.RemoteCommitUnlock != nil && fh.ftruncateFence {
		return nil
	}
	fs.releaseHandleRemoteCommitPathLocked(fh)
	unlock, ok := fs.tryLockFtruncateAliases(fh)
	if !ok {
		return errFtruncateBusy
	}
	fh.RemoteCommitUnlock, fh.ftruncateFence = unlock, true
	return nil
}

// Returns a locked live descendant, if one exists. Dirty independent siblings
// are not merged. The caller releases the returned handle.
func (fs *Dat9FS) ftruncateChildLocked(fh *FileHandle) (*FileHandle, error) {
	id := ftruncateSnapshotID(fh)
	if (id == "" || fs.handleCanAdoptCommittedRevisionLocked(fh)) && fh.pendingFtruncate.Load() != nil {
		id = fh.pendingFtruncate.Load().id
	}
	for _, other := range fs.openHandles.SnapshotInode(fh.Ino) {
		if other == fh || other.Ino != fh.Ino || other.Flags&syscall.O_ACCMODE == syscall.O_RDONLY {
			continue
		}
		if !other.TryLock() {
			return nil, errFtruncateBusy
		}
		if other.Dirty != nil && !other.Unlinked && other.DirtySeq > fh.DirtySeq &&
			ftruncateSnapshotID(other) != id && ftruncateDescends(other, id) {
			return other, nil
		}
		// A newly opened copy must not create a second dirty child of the
		// same uncommitted image. An unexplained newer image also fails closed
		// when its bounded ancestry no longer proves a relationship.
		if event := fh.pendingFtruncate.Load(); event != nil && other.DirtySeq >= event.seq && other.DirtySeq > fh.DirtySeq {
			otherID := ftruncateSnapshotID(other)
			if (otherID == id && id != event.id) || (otherID != id && !ftruncateDescends(fh, otherID)) {
				other.Unlock()
				return nil, syscall.EAGAIN
			}
		}
		other.Unlock()
	}
	return nil, nil
}

// Verify a landed image before retiring a live ancestor. A watermark alone is
// not a content proof. This also handles a parent closing before B's first write.
func (fs *Dat9FS) adoptLandedFtruncateLocked(ctx context.Context, fh *FileHandle) (bool, error) {
	event := fh.pendingFtruncate.Load()
	if event == nil {
		return false, nil
	}
	base := fh.BaseRev
	if !ftruncateDescends(fh, event.id) {
		base = min(base, event.revision)
	}
	if fs.ftruncateAliasRevision(fh) <= base {
		return false, nil
	}
	reader := &CommitQueue{client: fs.client, remoteRoot: fs.remoteRoot()}
	rev, size, data, err := reader.readRemoteSnapshot(ctx, fh.Path)
	if err != nil {
		return false, err
	}
	id := ftruncateSnapshotID(fh)
	if id == "" {
		id = event.id
	}
	proof := pathCommitLandmark{}
	if fs.commitQueue != nil {
		proof = fs.ftruncateAliasProof(fh)
	}
	// The exact parent may land before its live child. Rebase only after
	// proving that the remote is that parent's complete immutable image.
	parentMatches := landedIdentityMatches(proof, rev, size, data) && ftruncateDescends(fh, proof.snapshotID)
	if event.complete && size == int64(len(event.data)) && bytes.Equal(event.data, data) {
		if !parentMatches {
			proof.snapshotID, proof.ancestors = event.id, event.ancestors
		}
		parentMatches = ftruncateDescends(fh, proof.snapshotID)
	}
	if parentMatches && fh.DirtySeq != 0 && id != proof.snapshotID {
		fh.BaseRev = rev
		return false, nil
	}
	verified := landedIdentityMatches(proof, rev, size, data) && (proof.snapshotID == id || slices.Contains(proof.ancestors, id))
	if !verified && (fh.DirtySeq == 0 || id == event.id) && event.complete && bytes.Equal(event.data, data) && size == int64(len(data)) {
		verified, proof.snapshotID, proof.ancestors = true, event.id, event.ancestors
	}
	if !verified {
		return false, syscall.EAGAIN
	}
	fs.removeHandleOwnedStagingLocked(fh)
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fs.installFtruncateImageLocked(fh, data, rev, proof.snapshotID, proof.ancestors)
	return true, nil
}

func (fs *Dat9FS) installFtruncateImageLocked(fh *FileHandle, data []byte, revision int64, id string, ancestors []string) {
	buffer := fs.newWriteBuffer(fh.Path, maxPreloadSize, 0)
	_, _ = buffer.Write(0, data) // bounded by maxLandedPayloadBytes
	buffer.ClearDirty()
	fh.Dirty, fh.DirtySeq, fh.WriteBackSeq = buffer, 0, 0
	fh.BaseRev, fh.OrigSize, fh.ZeroBase = revision, int64(len(data)), len(data) == 0
	fh.ShadowReady, fh.ShadowSpill, fh.ShadowCommitReady = false, false, false
	fh.ShadowCommitSeq = 0
	fh.WriteBackGen, fh.PendingIndexGen, fh.ShadowStageGen = 0, 0, 0
	clearStagedSnapshotLineageLocked(fh)
	fh.ContentSnapshotID, fh.contentAncestors, fh.LineageTrusted = id, ancestors, true
	if event := fh.pendingFtruncate.Load(); event != nil {
		fh.ftruncateInherited = event.id
	}
	fh.appendSnapshot = true
}

func (fs *Dat9FS) prepareFtruncateMutationLocked(ctx context.Context, fh *FileHandle) error {
	if fh != nil && !fh.Unlinked {
		for _, alias := range fs.ftruncateAliasPaths(fh.Ino) {
			if alias != fh.Path && fs.commitQueue != nil && fs.commitQueue.HasPath(alias) {
				return syscall.EAGAIN
			}
		}
	}
	if !fs.ftruncateParticipates(fh) {
		return nil
	}
	event := fh.pendingFtruncate.Load()
	if event.ino != fh.Ino || event.path != fh.Path || event.view != fs.mountViewGeneration.Load() || !fs.ftruncateAliasLinked(fh) {
		return syscall.EAGAIN
	}
	if _, err := fs.adoptLandedFtruncateLocked(ctx, fh); err != nil {
		return err
	}
	child, err := fs.ftruncateChildLocked(fh)
	if child != nil {
		child.Unlock()
		return syscall.EAGAIN
	}
	if err != nil {
		return err
	}
	// A closed successor may be newer than the immutable truncate baseline.
	// Wait for its commit proof rather than resurrecting the original image
	// or pairing path-keyed shadow bytes with another metadata generation.
	if fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(fh.Path); ok && meta.lineageTrusted &&
			(meta.SnapshotID == event.id || slices.Contains(processLocalMetaAncestors(meta), event.id)) {
			id := ftruncateSnapshotID(fh)
			if id == "" || fs.handleCanAdoptCommittedRevisionLocked(fh) {
				id = event.id
			}
			if meta.SnapshotID != id && slices.Contains(processLocalMetaAncestors(meta), id) {
				return syscall.EAGAIN
			}
			if fs.handleCanAdoptCommittedRevisionLocked(fh) && (meta.Kind == PendingConflict || meta.SnapshotID != event.id) {
				return syscall.EAGAIN
			}
		}
	}
	if ftruncateDescends(fh, event.id) {
		fh.ftruncateInherited = event.id
		return nil
	}
	if !event.complete || !fs.handleCanAdoptCommittedRevisionLocked(fh) || (fh.Streamer != nil && fh.Streamer.Started()) {
		return syscall.EAGAIN
	}
	fs.installFtruncateImageLocked(fh, event.data, event.revision, event.id, event.ancestors)
	return nil
}

// Stage and hand off a full child before allowing its parent to retire. Queue
// refusal preserves the child's exact current generation; Release can expose
// the failure through the existing durable conflict record without a legacy
// path-only upload fallback.
func (fs *Dat9FS) handoffFtruncateLocked(ctx context.Context, fh *FileHandle, strict bool) (bool, error) {
	if !fs.ftruncateParticipates(fh) || fh.DirtySeq == 0 {
		return false, nil
	}
	if adopted, err := fs.adoptLandedFtruncateLocked(ctx, fh); adopted || err != nil {
		return adopted, err
	}
	if fs.pendingIndex != nil && fs.commitQueue != nil {
		for _, alias := range fs.ftruncateAliasPaths(fh.Ino) {
			if meta, ok := fs.pendingIndex.GetMeta(alias); ok && meta.lineageTrusted &&
				meta.SnapshotID != ftruncateSnapshotID(fh) && slices.Contains(processLocalMetaAncestors(meta), ftruncateSnapshotID(fh)) {
				if fs.commitQueue.ownsFtruncateSnapshot(meta) {
					if strict {
						return true, syscall.EAGAIN
					}
					fs.removeHandleOwnedStagingLocked(fh)
					fs.clearDirtySize(fh.Ino, fh.DirtySeq)
					fh.Dirty.ClearDirty()
					fh.DirtySeq, fh.WriteBackSeq = 0, 0
					return true, nil
				}
			}
		}
	}
	child, err := fs.ftruncateChildLocked(fh)
	if err != nil || child == nil {
		return false, err
	}
	defer child.Unlock()
	if fs.commitQueue == nil || fs.shadowStore == nil || fs.pendingIndex == nil ||
		child.Dirty.Size() > maxLandedPayloadBytes || !child.Dirty.CanMaterializeFull() {
		return true, syscall.EAGAIN
	}
	if err := fs.fenceFtruncateLocked(child); err != nil {
		return true, err
	}
	defer fs.releaseHandleRemoteCommitPathLocked(child)
	if err := fs.stageShadowLocked(child, true); err != nil {
		return true, err
	}
	childSeq := child.DirtySeq
	if err := fs.enqueueStagedShadowCommitLocked(child); err != nil {
		return true, fs.retainFtruncateConflictLocked(child, err)
	}
	// This is a live sibling, not its Release: preserve accepted local reads
	// until the exact queued snapshot has landed. Staging ownership stays queued.
	child.Dirty.MarkAllDirty()
	child.DirtySeq = childSeq
	fs.restoreDirtySize(child.Ino, childSeq, child.Dirty.Size())
	if strict {
		return true, syscall.EAGAIN
	}
	fs.removeHandleOwnedStagingLocked(fh)
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.Dirty.ClearDirty()
	fh.DirtySeq, fh.WriteBackSeq = 0, 0
	return true, nil
}

func (fs *Dat9FS) retainFtruncateConflictLocked(fh *FileHandle, cause error) error {
	marked, err := fs.pendingIndex.MarkConflictIfGeneration(fh.Path, fh.PendingIndexGen)
	if err != nil {
		return err
	}
	if !marked {
		return syscall.EAGAIN
	}
	if fs.writeBack != nil {
		fs.writeBack.RemoveIfGeneration(fh.Path, fh.WriteBackGen)
	}
	fh.WriteBackGen, fh.WriteBackSeq = 0, 0
	return cause
}

// The queue owned the failed generation, so the live handle has no cleanup
// tokens after handoff. On a successful retry, use the existing exact landed
// ancestor proof under the path fence to retire that conflict, not an arbitrary
// current path generation. Pending permission changes retain their own owner.
func (fs *Dat9FS) discardFtruncateRetriedAncestorLocked(ctx context.Context, fh *FileHandle) {
	if !fs.ftruncateParticipates(fh) || fs.pendingIndex == nil || fs.shadowStore == nil || fs.commitQueue == nil {
		return
	}
	meta, ok := fs.pendingIndex.GetMeta(fh.Path)
	if !ok || meta.Kind != PendingConflict || !meta.lineageTrusted || meta.HasMode || fs.commitQueue.ownsFtruncateSnapshot(meta) {
		return
	}
	entry := &CommitEntry{Path: fh.Path, Inode: fh.Ino, SnapshotID: meta.SnapshotID,
		liveLineageProof: true, PendingIndexGen: meta.Generation, ShadowGen: fs.shadowStore.ActiveGeneration(fh.Path)}
	if fs.commitQueue.discardLandedAncestor(ctx, entry) && fs.writeBack != nil {
		if cached, ok := fs.writeBack.GetMeta(fh.Path); ok && cached.lineageTrusted && cached.SnapshotID == meta.SnapshotID {
			fs.writeBack.RemoveIfGeneration(fh.Path, cached.Generation)
		}
	}
}

func (cq *CommitQueue) ownsFtruncateSnapshot(meta *WriteBackMeta) bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	for _, entry := range cq.queue {
		if !entry.canceled && entry.Path == meta.Path && entry.SnapshotID == meta.SnapshotID && entry.PendingIndexGen == meta.Generation {
			return true
		}
	}
	return false
}

func (fs *Dat9FS) prepareFtruncateCommitLocked(ctx context.Context, fh *FileHandle, strict bool) (bool, error) {
	if !fs.ftruncateParticipates(fh) {
		return false, nil
	}
	if strict && fh.DirtySeq == 0 && fh.ftruncateInherited != "" && fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(fh.Path); ok && meta.lineageTrusted &&
			(meta.SnapshotID == ftruncateSnapshotID(fh) || slices.Contains(processLocalMetaAncestors(meta), ftruncateSnapshotID(fh))) {
			return true, syscall.EAGAIN
		}
	}
	if fh.Dirty == nil || (fh.DirtySeq == 0 && !fh.Dirty.HasDirtyParts()) {
		return false, nil
	}
	event := fh.pendingFtruncate.Load()
	if event.ino != fh.Ino || event.path != fh.Path || event.view != fs.mountViewGeneration.Load() || !fs.ftruncateAliasLinked(fh) {
		return true, syscall.EAGAIN
	}
	if handled, err := fs.handoffFtruncateLocked(ctx, fh, strict); handled || err != nil {
		return handled, err
	}
	if err := fs.fenceFtruncateLocked(fh); err != nil {
		return true, err
	}
	return false, nil
}

func (fs *Dat9FS) lockFtruncateWritableOpen(ino uint64, path string) (func(), bool, error) {
	if fs.layerEnabled() || isSQLiteDirectIOPath(path) || fs.openHandles.ftruncateInheritance(ino, path) == nil {
		return fs.lockWritableRemoteCommitPathForWritableOpen(path), false, nil
	}
	deadline := time.Now().Add(fs.opts.RemoteCommitWaitTimeout)
	for {
		remaining := time.Until(deadline)
		if fs.opts.RemoteCommitWaitTimeout > 0 && remaining <= 0 {
			return nil, true, syscall.EAGAIN
		}
		if fs.commitQueue != nil && fs.commitQueue.HasPath(path) {
			interval := 50 * time.Millisecond
			if fs.opts.RemoteCommitWaitTimeout > 0 {
				interval = min(interval, remaining)
			}
			fs.commitQueue.WaitPathTimeout(path, interval)
			continue
		}
		var unlock func()
		if fs.opts.RemoteCommitWaitTimeout <= 0 {
			unlock = fs.lockRemoteCommitPath(path)
		} else {
			var ok bool
			unlock, ok = fs.lockRemoteCommitPathTimeout(path, remaining)
			if !ok {
				return nil, true, syscall.EAGAIN
			}
		}
		if fs.commitQueue == nil || !fs.commitQueue.HasPath(path) {
			return unlock, true, nil
		}
		unlock()
	}
}

// Release cannot return EAGAIN to the kernel or abandon its only dirty image.
// Yield the handle while a participating operation owns a lock; no queued work
// is canceled and callbacks are free to finish before we retry the handoff.
func (fs *Dat9FS) prepareFtruncateReleaseLocked(ctx context.Context, fh *FileHandle) (bool, error) {
	for {
		handled, err := fs.prepareFtruncateCommitLocked(ctx, fh, false)
		if !errors.Is(err, errFtruncateBusy) {
			if err != nil && fh.Dirty != nil && fh.Dirty.HasDirtyParts() && fs.shadowStore != nil && fs.pendingIndex != nil {
				if fenceErr := fs.fenceFtruncateLocked(fh); errors.Is(fenceErr, errFtruncateBusy) {
					fh.Unlock()
					time.Sleep(samePathDirtyWaitInterval)
					fh.Lock()
					continue
				} else if fenceErr != nil {
					return true, fenceErr
				}
				if meta, ok := fs.pendingIndex.GetMeta(fh.Path); ok && meta.lineageTrusted &&
					slices.Contains(processLocalMetaAncestors(meta), ftruncateSnapshotID(fh)) {
					// A complete durable child already preserves this parent.
					return true, err
				}
				if meta, ok := fs.pendingIndex.GetMeta(fh.Path); ok && meta.Generation != fh.PendingIndexGen && !ftruncateDescends(fh, meta.SnapshotID) {
					return true, err // never replace an independent generation
				}
				if stageErr := fs.stageShadowLocked(fh, true); stageErr != nil {
					return true, stageErr
				}
				return true, fs.retainFtruncateConflictLocked(fh, err)
			}
			return handled, err
		}
		failpoint.InjectCall("ftruncateReleaseWait", fs, fh)
		fh.Unlock()
		time.Sleep(samePathDirtyWaitInterval)
		fh.Lock()
	}
}

// readReleasedFtruncateLocked covers the clean writable sibling after the last
// source marker disappears. Reads borrow immutable bytes, never cleanup tokens.
// A missing proof fails closed rather than exposing the old clean buffer.
func (fs *Dat9FS) readReleasedFtruncateLocked(fh *FileHandle, offset int64, size uint32) ([]byte, bool, error) {
	data, handled, err := fs.readReleasedFtruncateOnceLocked(fh, offset, size)
	if !errors.Is(err, errFtruncateBusy) {
		return data, handled, err
	}
	event := fh.pendingFtruncate.Load()
	deadline := time.Now().Add(samePathDirtyWaitTimeout)
	for errors.Is(err, errFtruncateBusy) && time.Now().Before(deadline) {
		// Buffered Linux reads can surface EAGAIN as EIO. Yield through a
		// short active commit, as the existing live-source reader does.
		fh.Unlock()
		time.Sleep(samePathDirtyWaitInterval)
		fh.Lock()
		data, handled, err = fs.readReleasedFtruncateOnceLocked(fh, offset, size)
		if !handled && fh.pendingFtruncate.Load() != event {
			return nil, true, syscall.EAGAIN
		}
	}
	return data, handled, err
}

func (fs *Dat9FS) readReleasedFtruncateOnceLocked(fh *FileHandle, offset int64, size uint32) ([]byte, bool, error) {
	if !fs.ftruncateParticipates(fh) || fh.Dirty == nil || fh.DirtySeq != 0 ||
		fh.Dirty.HasDirtyParts() || fs.openHandles.HasVisibleTruncate(fh.Ino) {
		return nil, false, nil
	}
	event := fh.pendingFtruncate.Load()
	if event.path != fh.Path || event.ino != fh.Ino || event.view != fs.mountViewGeneration.Load() {
		return nil, true, syscall.EAGAIN
	}
	unlock, ok := fs.tryLockRemoteCommitPath(fh.Path)
	if !ok {
		return nil, true, errFtruncateBusy
	}
	defer unlock()
	revision := fs.latestCommittedRevision(fh.Path)
	localSeq := func() uint64 {
		fs.dirtyMu.Lock()
		defer fs.dirtyMu.Unlock()
		return fs.dirtyInodes[fh.Ino].seq
	}
	seq := localSeq()
	var meta *WriteBackMeta
	if fs.pendingIndex != nil {
		meta, _ = fs.pendingIndex.GetMeta(fh.Path)
	}
	// Once landed, the ordinary revision refresh can invalidate this clean
	// buffer. An uncommitted successor must still take precedence below.
	if meta == nil && revision > event.revision {
		if seq > event.seq {
			return nil, true, syscall.EAGAIN
		}
		return nil, false, nil
	}
	data, selectedSeq := event.data, event.seq
	readShadowGen := uint64(0)
	if meta != nil {
		if !meta.lineageTrusted || (meta.SnapshotID != event.id && !slices.Contains(processLocalMetaAncestors(meta), event.id)) {
			return nil, true, syscall.EAGAIN
		}
		if meta.SnapshotID != event.id {
			// Publication binds the shadow token to this exact process-local
			// metadata snapshot, including a retained failed commit.
			shadowGen := meta.ownedStagingGens.ShadowGen
			selectedSeq = meta.MutationSeq
			if !meta.ownedStagingKnown || meta.Inode != fh.Ino {
				return nil, true, syscall.EAGAIN
			}
			if shadowGen == 0 || fs.shadowStore == nil || meta.Size > maxLandedPayloadBytes {
				return nil, true, syscall.EAGAIN
			}
			var err error
			readShadowGen = shadowGen
			data, err = fs.shadowStore.ReadAllIfGeneration(fh.Path, shadowGen)
			if err != nil || int64(len(data)) != meta.Size {
				return nil, true, syscall.EAGAIN
			}
		} else if !event.complete || int64(len(data)) != meta.Size {
			return nil, true, syscall.EAGAIN
		}
	} else if !event.complete || revision > event.revision {
		return nil, true, syscall.EAGAIN
	}
	if seq > selectedSeq {
		return nil, true, syscall.EAGAIN
	}
	if revision > event.revision {
		if selectedSeq <= fs.ftruncateCommittedSeq(fh.Ino) || (meta != nil && meta.SnapshotID == event.id) {
			return nil, false, nil
		}
		// Do not serve an older pending child over an unrelated remote commit.
		if fs.commitQueue == nil {
			return nil, true, syscall.EAGAIN
		}
		proof := fs.commitQueue.landedCommit(fh.Path)
		if proof.rev != revision || meta == nil || !slices.Contains(processLocalMetaAncestors(meta), proof.snapshotID) {
			return nil, true, syscall.EAGAIN
		}
	}
	if readShadowGen != 0 && fs.shadowStore.ActiveGeneration(fh.Path) != readShadowGen {
		return nil, true, syscall.EAGAIN
	}
	failpoint.InjectCall("releasedFtruncateBeforeReadValidate", fs, fh)
	entry, linked := fs.inodes.GetEntry(fh.Ino)
	if !linked || entry.Unlinked {
		return nil, true, syscall.EAGAIN
	}
	if _, linked = entry.Paths[fh.Path]; !linked || fh.pendingFtruncate.Load() != event ||
		event.view != fs.mountViewGeneration.Load() || revision != fs.latestCommittedRevision(fh.Path) || seq != localSeq() {
		return nil, true, syscall.EAGAIN
	}
	if fs.pendingIndex != nil {
		current, exists := fs.pendingIndex.GetMeta(fh.Path)
		if (meta == nil && exists) || (meta != nil && (!exists || current.Generation != meta.Generation || current.SnapshotID != meta.SnapshotID)) {
			return nil, true, syscall.EAGAIN
		}
	}
	if offset < 0 || offset > int64(^uint64(0)>>1)-int64(size) {
		return nil, true, syscall.EINVAL
	}
	if offset >= int64(len(data)) {
		return nil, true, nil
	}
	end := min(offset+int64(size), int64(len(data)))
	return append([]byte(nil), data[offset:end]...), true, nil
}

// A final close can race the alias child's Release between durable staging
// and queue ownership. Yield the parent handle through that short handoff;
// do not return a transient lock collision as a failed close to the kernel.
func (fs *Dat9FS) prepareFtruncateFlushLocked(ctx context.Context, fh *FileHandle) (bool, error) {
	handled, err := fs.prepareFtruncateCommitLocked(ctx, fh, false)
	if !errors.Is(err, errFtruncateBusy) || len(fs.ftruncateAliasPaths(fh.Ino)) <= 1 {
		return handled, err
	}
	deadline := time.Now().Add(samePathDirtyWaitTimeout)
	for errors.Is(err, errFtruncateBusy) && time.Now().Before(deadline) && ctx.Err() == nil {
		failpoint.InjectCall("ftruncateAliasFlushWait", fs, fh)
		fh.Unlock()
		time.Sleep(samePathDirtyWaitInterval)
		fh.Lock()
		handled, err = fs.prepareFtruncateCommitLocked(ctx, fh, false)
	}
	return handled, err
}
