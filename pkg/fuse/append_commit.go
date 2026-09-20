package fuse

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// commitAppendSnapshotLocked gives small live append snapshots the same causal
// validation and landed identity on fsync as on queued close. Caller holds
// fh.mu; queue callbacks use TryLock and cannot consume this handle mid-fsync.
// As on the existing synchronous write-back path, metadata finalization follows
// the content commit while the handle remains locked.
func (fs *Dat9FS) commitAppendSnapshotLocked(ctx context.Context, fh *FileHandle) (bool, gofuse.Status) {
	if fs.commitQueue == nil || fs.layerEnabled() || fh.Unlinked || fh.Dirty == nil ||
		(!fh.appendSnapshot && fh.Flags&uint32(syscall.O_APPEND) == 0) ||
		isSQLitePersistentJournalPath(fh.Path) || fh.Dirty.Size() > maxLandedPayloadBytes ||
		(fh.Streamer != nil && fh.Streamer.Started()) {
		return false, gofuse.OK
	}
	if !fh.Dirty.HasDirtyParts() {
		if fh.HasPendingMode {
			return true, httpToFuseStatus(fs.applyPendingModeWithTimeoutLocked(fh))
		}
		return false, gofuse.OK
	}
	if !fh.Dirty.CanMaterializeFull() {
		return false, gofuse.OK
	}
	unlock := fs.takeHandleRemoteCommitPathLocked(fh)
	ensureStagedSnapshotLineageLocked(fh)
	entry := &CommitEntry{
		Path: fh.Path, Inode: fh.Ino, MutationSeq: fh.DirtySeq,
		BaseRev: expectedRevisionForHandle(fh), Size: fh.Dirty.Size(),
		Kind: fs.pendingKindForHandle(fh),
	}
	fs.bindCommitEntryToHandleLocked(entry, fh, fh.BaseRev)
	entry.bindPayload(fh.Dirty.Bytes())
	// This upload owns its private payload, not the current path-keyed stores.
	// Clean only the handle's original generations after committing.
	entry.ShadowGen, entry.PendingIndexGen, entry.WriteBackGen = 0, 0, 0
	err := fs.commitQueue.commitNowPathLocked(ctx, entry)
	if err == nil {
		proof := fs.commitQueue.landedCommit(entry.Path)
		if proof.rev > 0 {
			fs.recordCommittedRevisionWithSize(entry.Path, proof.rev, proof.size)
		}
	}
	unlock()
	if err != nil {
		return true, httpToFuseStatus(err)
	}
	if fh.Unlinked || fh.Path != entry.Path || fh.DirtySeq != entry.MutationSeq {
		return true, gofuse.OK
	}
	fs.removeHandleOwnedStagingLocked(fh)
	fh.Dirty.ClearDirty()
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq, fh.WriteBackSeq = 0, 0
	fh.ShadowCommitReady, fh.ShadowCommitSeq = false, 0
	fh.ShadowReady, fh.ShadowSpill = false, false
	fh.appendSnapshot = false
	publishStagedSnapshotLineageLocked(fh)
	fs.adoptCommittedRevisionLocked(fh)
	proof := fs.commitQueue.landedCommit(fh.Path)
	if proof.rev == fh.BaseRev && fh.Dirty.CanMaterializeFull() && landedIdentityMatches(proof, fh.BaseRev, fh.Dirty.Size(), fh.Dirty.Bytes()) {
		fh.ContentSnapshotID, fh.contentAncestors = proof.snapshotID, proof.ancestors
	}
	if err := fs.applyPendingModeWithTimeoutLocked(fh); err != nil {
		return true, httpToFuseStatus(err)
	}
	return true, gofuse.OK
}
