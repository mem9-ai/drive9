package fuse

import (
	"context"
	"slices"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// sameLinkedInode excludes detached or replaced names from alias handoffs.
func (fs *Dat9FS) sameLinkedInode(path string, ino uint64) bool {
	if fs.inodes == nil {
		return false
	}
	current, ok := fs.inodes.GetInode(path)
	return ok && ino != 0 && current == ino
}

// publishLinkedAppendCommit shares only the queue's existing committed proof
// and watermark with names still attached to this file. Path-owned staging
// and generation cleanup remain separate for each alias.
func (fs *Dat9FS) publishLinkedAppendCommit(path string, ino uint64, revision, size int64) {
	if fs.commitQueue == nil || fs.layerEnabled() || revision <= 0 ||
		!fs.sameLinkedInode(path, ino) {
		return
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.ResourceID == "" || len(entry.Paths) < 2 {
		return
	}
	proof := fs.commitQueue.landedCommit(path)
	if proof.rev != revision {
		proof = pathCommitLandmark{}
	}
	for alias := range entry.Paths {
		if alias == path || !fs.sameLinkedInode(alias, ino) {
			continue
		}
		fs.recordCommittedRevisionWithSize(alias, revision, size)
		// Alias lookup/GetAttr must not restore an older cached length
		// after another name commits the final append image.
		fs.cacheFileForPath(alias, size, entry.Mtime, revision)
		fs.commitQueue.rememberLanded(alias, revision, size, proof.checksum, proof.snapshotID, proof.ancestors...)
		fs.invalidateReadCacheAndTargets(alias)
		fs.refreshCommittedRevisionForOpenHandlesWithSize(alias, revision, nil, size)
	}
}

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
	if fs.inodes != nil {
		if current, ok := fs.inodes.GetInode(fh.Path); ok && current != fh.Ino {
			return true, gofuse.EAGAIN
		}
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
	clearHandleShadowClaimLocked(fh)
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

// adoptLandedAppendSnapshotLocked binds the complete landed image and its
// identity together before selecting an append source. A clean handle may have
// adopted the revision without the snapshot identity; a dirty handle needs
// proof that the landed snapshot includes its exact image. Caller holds fh.mu
// and the path commit lock. Failed remote checks leave the local state intact.
func (fs *Dat9FS) adoptLandedAppendSnapshotLocked(ctx context.Context, fh *FileHandle) error {
	if fs.commitQueue == nil || fs.layerEnabled() ||
		fh.Dirty == nil || fh.Dirty.Size() > maxLandedPayloadBytes ||
		(fh.Streamer != nil && fh.Streamer.Started()) {
		return nil
	}
	if fs.inodes != nil {
		if current, ok := fs.inodes.GetInode(fh.Path); ok && current != fh.Ino {
			return syscall.EAGAIN
		}
	}
	proof := fs.commitQueue.landedCommit(fh.Path)
	if fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
		if max(proof.rev, fs.latestCommittedRevision(fh.Path)) > fh.BaseRev {
			// Recovered or unrelated dirty images cannot acknowledge more
			// appends against a superseded base. Keep them for recovery.
			if !fh.LineageTrusted || proof.snapshotID == "" {
				return syscall.EAGAIN
			}
			// The local image can already include the landed parent plus
			// additional writes which must not be retired by adoption.
			if proof.snapshotID == fh.ContentSnapshotID || slices.Contains(fh.contentAncestors, proof.snapshotID) {
				return nil
			}
		}
	}
	if !fh.LineageTrusted || proof.snapshotID == "" || proof.rev < fh.BaseRev {
		return nil
	}
	if fh.DirtySeq == 0 && !fh.Dirty.HasDirtyParts() {
		// Only a clean remote baseline can take the landed identity without
		// ancestry. Pending local snapshots must retain their own contents.
		if fh.IsNew || fh.ZeroBase || !fs.handleCanAdoptCommittedRevisionLocked(fh) ||
			proof.rev != fh.BaseRev || fh.ContentSnapshotID == proof.snapshotID {
			return nil
		}
	} else {
		if proof.rev <= fh.BaseRev {
			return nil
		}
		snapshotID, _ := ensureStagedSnapshotLineageLocked(fh)
		if snapshotID != proof.snapshotID && !slices.Contains(proof.ancestors, snapshotID) {
			return syscall.EAGAIN
		}
	}
	stat, data, err := fs.commitQueue.readRemoteSnapshotStat(ctx, fh.Path)
	if err != nil {
		return err
	}
	if entry, ok := fs.inodes.GetEntry(fh.Ino); ok && len(entry.Paths) > 1 &&
		(entry.ResourceID == "" || stat.ResourceID != entry.ResourceID) {
		return syscall.EAGAIN
	}
	revision, size := stat.Revision, stat.Size
	if !landedIdentityMatches(proof, revision, size, data) {
		return syscall.EAGAIN
	}
	next := fs.newWriteBuffer(fh.Path, max(fh.Dirty.maxSize, size), fh.Dirty.PartSize())
	if _, err := next.Write(0, data); err != nil {
		return err
	}
	next.ClearDirty()
	next.markCommittedPrefix(revision, size)
	fs.removeHandleOwnedStagingLocked(fh)
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.Dirty = next
	fh.DirtySeq, fh.WriteBackSeq = 0, 0
	clearHandleShadowClaimLocked(fh)
	clearStagedSnapshotLineageLocked(fh)
	clearReadTargetForLockedHandle(fh)
	fh.IsNew, fh.ZeroBase, fh.appendSnapshot = false, false, false
	fh.BaseRev, fh.OrigSize = revision, size
	fh.ContentSnapshotID, fh.contentAncestors = proof.snapshotID, proof.ancestors
	fs.inodes.UpdateRevision(fh.Ino, revision)
	fs.inodes.UpdateSize(fh.Ino, size)
	fs.adoptCommittedStorageClassLocked(fh, size)
	return nil
}

// tryLockAppendRemoteCommitPathLocked uses the existing append retry loop
// instead of waiting for a path fence while pinning a sibling's handle. Each
// retry also observes reservations acquired by a concurrent same-handle Flush.
// Caller holds fh.mu; on contention it must release fh.mu before retrying.
func (fs *Dat9FS) tryLockAppendRemoteCommitPathLocked(fh *FileHandle) (func(), bool) {
	if fh.RemoteCommitUnlock != nil || fh.Path == "" {
		return func() {}, true
	}
	if !fh.Unlinked && fh.ZeroBase && fh.Dirty != nil && fh.Dirty.Size() == 0 {
		_ = fs.canSupersedeQueuedPathTruncate(fh.Path)
	}
	if fs.appendCommitPending(fh) {
		return func() {}, false
	}
	unlock, ok := fs.tryLockRemoteCommitPath(fh.Path)
	if !ok {
		return func() {}, false
	}
	if fs.appendCommitPending(fh) {
		unlock()
		return func() {}, false
	}
	fh.RemoteCommitUnlock = unlock
	return func() { fs.releaseHandleRemoteCommitPathLocked(fh) }, true
}

func (fs *Dat9FS) appendCommitPending(fh *FileHandle) bool {
	if fs.commitQueue == nil {
		return false
	}
	if fs.commitQueue.HasPath(fh.Path) {
		return true
	}
	for _, path := range fs.appendReadPaths(fh.Ino) {
		if fs.sameLinkedInode(path, fh.Ino) && fs.commitQueue.HasPath(path) {
			return true
		}
	}
	return false
}
