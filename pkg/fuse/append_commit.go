package fuse

import (
	"context"
	"slices"
	"syscall"
	"time"

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
func (fs *Dat9FS) publishLinkedAppendCommit(path string, ino uint64, revision, size int64) bool {
	if fs.layerEnabled() || revision <= 0 ||
		!fs.sameLinkedInode(path, ino) {
		return false
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || len(entry.Paths) < 2 {
		return false
	}
	proof := pathCommitLandmark{}
	if entry.ResourceID != "" {
		proof = fs.landedAppendCommit(path)
	}
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
		if fs.commitQueue != nil {
			fs.commitQueue.rememberLanded(alias, revision, size, proof.checksum, proof.snapshotID, proof.ancestors...)
		} else {
			aliasProof := proof
			aliasProof.rev, aliasProof.size = revision, size
			fs.recordCommittedAppendSnapshot(alias, aliasProof)
		}
		fs.invalidateReadCacheAndTargets(alias)
		if entry.ResourceID != "" {
			fs.refreshCommittedRevisionForOpenHandlesWithSize(alias, revision, nil, size)
		}
	}
	return true
}

// refreshUnidentifiedAppendAliasesLocked resolves an advisory Link identity
// only after a locally committed alias fence makes the retained baseline old.
// All still-linked names must agree before any buffer or CAS base is changed.
func (fs *Dat9FS) refreshUnidentifiedAppendAliasesLocked(ctx context.Context, fh *FileHandle) error {
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || entry.ResourceID != "" || len(entry.Paths) < 2 || fs.latestCommittedRevision(fh.Path) <= fh.BaseRev {
		return nil
	}
	paths := fs.appendReadPaths(fh.Ino)
	resourceID := ""
	revisions := make(map[string]int64, len(paths))
	for _, path := range paths {
		if !fs.sameLinkedInode(path, fh.Ino) {
			return syscall.EAGAIN
		}
		stat, err := fs.client.StatCtx(ctx, fs.remotePath(path))
		if err != nil || stat == nil || stat.IsDir || stat.ResourceID == "" || stat.Revision < fs.latestCommittedRevision(path) {
			return syscall.EAGAIN
		}
		if resourceID != "" && resourceID != stat.ResourceID {
			return syscall.EAGAIN
		}
		resourceID = stat.ResourceID
		revisions[path] = stat.Revision
	}
	for _, path := range paths {
		if revisions[path] < fs.latestCommittedRevision(path) {
			return syscall.EAGAIN
		}
	}
	fs.inodes.mu.Lock()
	current, present := fs.inodes.byInode[fh.Ino]
	valid := present && !current.IsDir && len(current.Paths) == len(paths) && (current.ResourceID == "" || current.ResourceID == resourceID)
	if valid {
		for _, path := range paths {
			if _, member := current.Paths[path]; !member || fs.inodes.byPath[path] != fh.Ino {
				valid = false
				break
			}
		}
	}
	if valid {
		fs.inodes.setIdentityLocked(current, resourceID)
	}
	fs.inodes.mu.Unlock()
	if !valid {
		return syscall.EAGAIN
	}
	for _, path := range paths {
		proof := fs.landedAppendCommit(path)
		if proof.snapshotID != "" && proof.checksum != "" {
			fs.publishLinkedAppendCommit(path, fh.Ino, proof.rev, proof.size)
		}
	}
	return nil
}

// recordCommittedAppendSnapshot attaches the existing live lineage proof to
// the committed watermark when no queue owns that proof. The revision, size
// and immutable identity are published together; nothing survives recovery.
func (fs *Dat9FS) recordCommittedAppendSnapshot(path string, proof pathCommitLandmark) bool {
	if fs.commitQueue != nil || path == "" || proof.rev <= 0 || proof.size < 0 {
		return false
	}
	fs.committedMu.Lock()
	defer fs.committedMu.Unlock()
	if proof.rev < fs.committedRev[path] {
		return false
	}
	if fs.committedRev == nil {
		fs.committedRev = make(map[string]int64)
	}
	if fs.committedSize == nil {
		fs.committedSize = make(map[string]int64)
	}
	fs.committedRev[path], fs.committedSize[path] = proof.rev, proof.size
	if proof.snapshotID == "" || proof.checksum == "" || proof.size > maxLandedPayloadBytes {
		delete(fs.committedAppend, path)
		return true
	}
	if fs.committedAppend == nil {
		fs.committedAppend = make(map[string]pathCommitLandmark)
	}
	if len(fs.committedAppend) >= maxLandedCommitLandmarks {
		for old := range fs.committedAppend {
			delete(fs.committedAppend, old)
			break
		}
	}
	proof.ancestors = append([]string(nil), proof.ancestors[:min(len(proof.ancestors), maxLiveSnapshotAncestors)]...)
	fs.committedAppend[path] = proof
	return true
}

// landedAppendCommit uses the queue landmark when available; without a queue,
// its revision and size must still match the process-local committed watermark.
// Ancestors are copied. This lookup alone does not verify current remote bytes.
func (fs *Dat9FS) landedAppendCommit(path string) pathCommitLandmark {
	if fs.commitQueue != nil {
		return fs.commitQueue.landedCommit(path)
	}
	fs.committedMu.Lock()
	defer fs.committedMu.Unlock()
	proof := fs.committedAppend[path]
	if proof.rev != fs.committedRev[path] || proof.size != fs.committedSize[path] {
		return pathCommitLandmark{}
	}
	proof.ancestors = append([]string(nil), proof.ancestors...)
	return proof
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
	return fs.adoptLandedAppendSnapshotWithRetryBudgetLocked(ctx, fh, maxLiveSnapshotAncestors)
}

// adoptLandedAppendSnapshotWithRetryBudgetLocked requires fh.mu and the path
// fence, like its wrapper. The budget bounds no-CQ proof advancement under the
// original context; failed verification preserves acknowledged bytes and owned staging.
func (fs *Dat9FS) adoptLandedAppendSnapshotWithRetryBudgetLocked(ctx context.Context, fh *FileHandle, proofRetries int) error {
	if fs.layerEnabled() || fh.Dirty == nil {
		return nil
	}
	if fh.Dirty.Size() > maxLandedPayloadBytes {
		// A dirty oversized image cannot adopt a newer CAS token alone.
		// Preserve it and reject more writes until the content can align.
		if (fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts()) && fs.latestCommittedRevision(fh.Path) > fh.BaseRev {
			return syscall.EAGAIN
		}
		return nil
	}
	if fh.Streamer != nil && fh.Streamer.Started() {
		return nil
	}
	if fs.inodes != nil {
		if current, ok := fs.inodes.GetInode(fh.Path); ok && current != fh.Ino {
			return syscall.EAGAIN
		}
	}
	proof := fs.landedAppendCommit(fh.Path)
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
				if fs.commitQueue != nil {
					return nil
				}
				// This exact live snapshot may have landed before its sync
				// caller reacquires fh.mu. A subsequent write can retire it;
				// a different dirty child must preserve its own records.
				exact := fh.StagedSnapshotID == proof.snapshotID && fh.StagedSnapshotSeq == fh.DirtySeq &&
					fh.Dirty.CanMaterializeFull() && landedIdentityMatches(proof, proof.rev, fh.Dirty.Size(), fh.Dirty.Bytes())
				if !exact {
					return fs.rebaseLegacyAppendOntoLandedParentWithRetryBudgetLocked(ctx, fh, proof, proofRetries)
				}
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
	stat, data, err := readBoundedRemoteSnapshotStat(ctx, fs.client, fs.remotePath(fh.Path))
	if err != nil {
		return err
	}
	if entry, ok := fs.inodes.GetEntry(fh.Ino); ok && len(entry.Paths) > 1 &&
		(entry.ResourceID == "" || stat.ResourceID != entry.ResourceID) {
		return syscall.EAGAIN
	}
	revision, size := stat.Revision, stat.Size
	if !landedIdentityMatches(proof, revision, size, data) {
		current := fs.landedAppendCommit(fh.Path)
		if current.rev > proof.rev && current.snapshotID != "" && current.checksum != "" &&
			slices.Contains(current.ancestors, proof.snapshotID) {
			if fs.commitQueue == nil {
				return fs.retryLegacyAppendProofAdvanceLocked(ctx, fh, proof, proofRetries)
			}
			return errAppendRefreshBusy
		}
		return syscall.EAGAIN
	}
	next := fs.newWriteBuffer(fh.Path, max(fh.Dirty.maxSize, size), fh.Dirty.PartSize())
	if _, err := next.Write(0, data); err != nil {
		return err
	}
	next.ClearDirty()
	next.markCommittedPrefix(revision, size)
	modeMeta := fs.claimLandedAppendPendingModeLocked(fh, fh.StagedSnapshotID)
	if modeMeta != nil {
		fh.WriteBackGen = 0
	}
	fs.removeHandleOwnedStagingLocked(fh)
	if modeMeta != nil {
		fh.WriteBackGen = modeMeta.Generation
	}
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

// appendCommitPending observes every still-linked alias while the caller holds
// fh.mu. Legacy in-flight uploads remain pending through success publication;
// generation-owned live buffers may instead participate in append composition.
func (fs *Dat9FS) appendCommitPending(fh *FileHandle) bool {
	if fs.commitQueue != nil && fs.commitQueue.HasPath(fh.Path) || fs.uploader != nil && fs.uploader.hasPath(fh.Path) {
		return true
	}
	for _, path := range fs.appendReadPaths(fh.Ino) {
		if fs.sameLinkedInode(path, fh.Ino) &&
			(fs.commitQueue != nil && fs.commitQueue.HasPath(path) || fs.legacyAppendCommitPendingLocked(fh, path)) {
			return true
		}
	}
	return false
}

// Caller owns fh.mu; an unexamined busy producer remains pending.
func (fs *Dat9FS) legacyAppendCommitPendingLocked(fh *FileHandle, path string) bool {
	if fs.uploader != nil && fs.uploader.hasPath(path) {
		return true
	}
	if fs.writeBack == nil {
		return false
	}
	meta, exists := fs.writeBack.GetMeta(path)
	if !exists || meta.Kind == PendingChmod || meta.LayerClean {
		return false
	}
	for _, source := range fs.openHandles.SnapshotPath(path) {
		if source != fh && !source.TryLock() {
			return true
		}
		access := source.Flags & uint32(syscall.O_ACCMODE)
		owned := meta.Generation != 0 && !source.Unlinked && source.Path == path &&
			source.Ino == fh.Ino && (access == syscall.O_WRONLY || access == syscall.O_RDWR) &&
			source.Dirty != nil && source.DirtySeq != 0 && source.WriteBackSeq != 0 &&
			source.WriteBackGen == meta.Generation
		if source != fh {
			source.Unlock()
		}
		if owned {
			return false
		}
	}
	return true
}

var testHookBeforeLinkedUploaderWait func(*FileHandle, string)

// Wait for released legacy ancestors before submitting a live descendant.
// Drop the handle lock while waiting, as callbacks and other producers may
// need it. The caller's context owns the whole wait, including queued uploads
// which have not entered the uploader's in-flight map yet.
func (fs *Dat9FS) waitLinkedLegacyAppendUploadsLocked(ctx context.Context, fh *FileHandle) error {
	if fs.commitQueue != nil || fs.uploader == nil || fs.layerEnabled() || fh.Dirty == nil ||
		(!fh.appendSnapshot && fh.Flags&uint32(syscall.O_APPEND) == 0) {
		return nil
	}
	path, ino := fh.Path, fh.Ino
	for {
		if fh.Unlinked {
			return nil
		}
		if fh.Path != path || fh.Ino != ino {
			return syscall.EAGAIN
		}
		pending := ""
		for _, alias := range fs.appendReadPaths(ino) {
			if fs.sameLinkedInode(alias, ino) && fs.legacyAppendCommitPendingLocked(fh, alias) {
				pending = alias
				break
			}
		}
		if pending == "" {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		active := fs.uploader.hasPath(pending)
		if active && testHookBeforeLinkedUploaderWait != nil {
			testHookBeforeLinkedUploaderWait(fh, pending)
		}
		fh.Unlock()
		var err error
		if active {
			err = fs.uploader.waitPathContext(ctx, pending)
		} else {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(samePathDirtyWaitInterval):
			}
		}
		fh.Lock()
		if err != nil {
			return err
		}
	}
}
