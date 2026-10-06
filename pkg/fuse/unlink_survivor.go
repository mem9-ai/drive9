package fuse

import (
	"context"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

var testHookBeforeSurvivorOpenRegister func(*FileHandle)
var testHookAfterUnlinkSurvivorPrepare func(string, string)
var testHookAfterUnlinkFirstMark func(string)

// prepareUnlinkSurvivor transfers clean fd bindings to a verified live alias.
// Failure precedes namespace mutation; a new generation requires a fresh retry.
// Network I/O and drains precede the local, nonblocking handoff critical section.
func (fs *Dat9FS) prepareUnlinkSurvivor(ctx context.Context, old string) (string, gofuse.Status) {
	// Other policies and specialized backends keep their existing unlink path.
	if fs.mountWritePolicy() != WritePolicyWriteBack || fs.layerEnabled() || isSQLiteDirectIOPath(old) {
		return "", gofuse.OK
	}

	ino, ok := fs.inodes.GetInode(old)
	if !ok {
		return "", gofuse.OK
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.Nlink <= 1 {
		return "", gofuse.OK
	}
	for _, h := range fs.openHandles.SnapshotInode(ino) {
		h.Lock()
		eligible := h.WritePolicy == WritePolicyWriteBack && h.LocalFile == nil && h.Layer != PathLayerGitWorkspace && !h.isExtent()
		h.Unlock()
		if !eligible {
			return "", gofuse.OK
		}
	}
	survivor := ""
	for _, p := range fs.appendReadPaths(ino) {
		if isSQLiteDirectIOPath(p) {
			return "", gofuse.OK
		}
		if p != old {
			survivor = p
			break
		}
	}
	if survivor == "" || entry.ResourceID == "" {
		return "", gofuse.EIO
	}
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(survivor))
	if err != nil || stat.ResourceID != entry.ResourceID || stat.Revision <= 0 {
		return "", gofuse.EIO
	}
	if testHookBeforeUnlinkSurvivorSync != nil {
		testHookBeforeUnlinkSurvivorSync(old)
	}
	if st := fs.syncOpenSourceForHardlink(ctx, ino); st != gofuse.OK {
		return "", st
	}
	if fs.commitQueue != nil {
		if testHookBeforeUnlinkSurvivorWait != nil {
			testHookBeforeUnlinkSurvivorWait(old)
		}
		fs.commitQueue.WaitPath(old)
	}
	if fs.writeBack != nil && fs.uploader != nil {
		fs.uploader.WaitPath(old)
		if err := fs.flushPendingWriteBack(ctx, old); err != nil {
			return "", httpToFuseStatus(err)
		}
	}
	if testHookAfterUnlinkSurvivorSettle != nil {
		testHookAfterUnlinkSurvivorSettle(old)
	}
	if fs.hasPendingMetadataState(old) {
		return "", gofuse.EIO
	}
	stat, err = fs.client.StatCtx(ctx, fs.remotePath(survivor))
	if err != nil || stat.ResourceID != entry.ResourceID || stat.Revision <= 0 {
		return "", gofuse.EIO
	}
	if testHookBeforeUnlinkSurvivorBind != nil {
		testHookBeforeUnlinkSurvivorBind(old)
	}
	// Final handoff is local-only. A late stage/commit invalidates the sampled
	// baseline; reject before changing preferred path, handle state or namespace.
	handles := fs.openHandles.SnapshotInode(ino)
	ordered, locked := tryLockFileHandlesInOrder(handles)
	if !locked {
		return "", gofuse.EIO
	}
	defer unlockFileHandles(ordered)
	paths := fs.appendReadPaths(ino)
	for _, p := range paths {
		unlock, ok := fs.tryLockRemoteCommitPath(p)
		if !ok {
			return "", gofuse.EIO
		}
		defer unlock()
	}
	for _, p := range paths {
		if fs.hasPendingMetadataState(p) || fs.hasQueuedCommit(p) || fs.latestCommittedRevision(p) > stat.Revision {
			return "", gofuse.EIO
		}
	}
	for _, h := range ordered {
		if h.Ino != ino || !cleanSurvivorBindingEligible(h, stat.Revision, false) || !fs.handleCanAdoptCommittedRevisionLocked(h) {
			return "", gofuse.EIO
		}
	}
	if testHookBeforeUnlinkSurvivorIndexCheck != nil {
		testHookBeforeUnlinkSurvivorIndexCheck()
	}
	// Existing inode/index locks cover validation and path publication together.
	// No handle acquisition, network operation, or fallible apply under these locks.
	fs.inodes.mu.Lock()
	fs.openHandles.mu.Lock()
	current := fs.inodes.byInode[ino]
	valid := current != nil && current.ResourceID == entry.ResourceID && current.Revision <= stat.Revision && fs.inodes.byPath[old] == ino && fs.inodes.byPath[survivor] == ino
	captured := make(map[*FileHandle]bool, len(ordered))
	for _, h := range ordered {
		captured[h] = true
	}
	for h := range fs.openHandles.byInode[ino] {
		if !captured[h] {
			valid = false
		}
	}
	for _, h := range ordered {
		_, alive := fs.openHandles.byInode[ino][h]
		indexed, hasPath := fs.openHandles.pathByHandle[h]
		_, atPath := fs.openHandles.byPath[h.Path][h]
		if !alive || !hasPath || indexed != h.Path || !atPath {
			valid = false
		}
	}
	if !valid {
		fs.openHandles.mu.Unlock()
		fs.inodes.mu.Unlock()
		return "", gofuse.EIO
	}
	// Commit point: from here all operations are infallible local applications.
	current.Path = survivor
	for _, h := range ordered {
		if h.Path == old {
			fs.moveSurvivorBindingIndexLocked(h, survivor)
		}
	}
	fs.openHandles.mu.Unlock()
	fs.inodes.mu.Unlock()
	fs.recordCommittedRevisionWithSize(survivor, stat.Revision, stat.Size)
	for _, h := range ordered {
		if h.Path == survivor {
			fs.applySurvivorBindingLocked(h, stat.Revision, stat.Size)
		}
	}

	return survivor, gofuse.OK
}

var testHookAfterSurvivorOpenRegister func(*FileHandle)
var testHookAfterUnlinkSurvivorSettle func(string)

// Fields are inspected only while holding fh.mu. Unlinked is permitted only
// for a not-yet-exposed Open that is proved to have a surviving inode name.
func cleanSurvivorBindingEligible(fh *FileHandle, revision int64, newOpen bool) bool {
	return fh != nil && fh.DirtySeq == 0 && (fh.Dirty == nil || !fh.Dirty.HasDirtyParts()) && !fh.IsNew && !fh.ZeroBase && fh.BaseRev <= revision && (!fh.Unlinked || newOpen)
}

// Caller holds fh.mu and openHandles.mu and has completed every check.
func (fs *Dat9FS) moveSurvivorBindingIndexLocked(fh *FileHandle, target string) {
	if _, registered := fs.openHandles.byInode[fh.Ino][fh]; registered {
		if old, ok := fs.openHandles.pathByHandle[fh]; ok {
			removeHandleFromSet(fs.openHandles.byPath, old, fh)
		}
		addHandleToSet(fs.openHandles.byPath, target, fh)
		fs.openHandles.pathByHandle[fh] = target
	}
	fh.Path = target
}

// Infallible application after validation/publication; caller holds fh.mu.
func (fs *Dat9FS) applySurvivorBindingLocked(fh *FileHandle, revision, size int64) {
	fh.Unlinked = false
	fs.rollbackUnlinkedSnapshotAttachLocked(fh, 0)
	if fh.Dirty != nil {
		fh.Dirty.path = fh.Path
	}
	if fh.ShadowPinned {
		gen := fh.ShadowGen
		fh.ShadowPinned = false
		fh.ShadowGen = 0
		if fs.shadowStore != nil {
			fs.shadowStore.Unpin(gen)
		}
	}
	clearReadTargetForLockedHandle(fh)
	if fh.Prefetch != nil {
		fh.Prefetch.SetPath(fs.remotePath(fh.Path))
		fh.Prefetch.invalidateWithSize(size)
	}
	if fh.Streamer != nil {
		fh.Streamer.SetPath(fh.Path, fs.remoteRoot())
	}
	fs.adoptCleanCommittedRevisionLocked(fh, revision, size)
}

// validateOpenSurvivorBinding checks an unexposed Open before and after registration.
// Stat runs outside fh.mu; the final identity/index check precedes every mutation.
func (fs *Dat9FS) validateOpenSurvivorBinding(ctx context.Context, fh *FileHandle, resourceID string) gofuse.Status {
	if fs.layerEnabled() {
		return gofuse.OK
	}

	fh.Lock()
	if fh.WritePolicy != WritePolicyWriteBack || isSQLiteDirectIOPath(fh.Path) {
		fh.Unlock()
		return gofuse.OK
	}
	ino, old := fh.Ino, fh.Path
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		fh.Unlock()
		return gofuse.EIO
	}
	if entry.Path == old {
		fh.Unlock()
		return gofuse.OK
	}
	target := entry.Path
	fh.Unlock()
	if resourceID == "" || entry.ResourceID != resourceID || len(entry.Paths) == 0 {
		return gofuse.EIO
	}
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(target))
	if err != nil || stat.ResourceID != resourceID || stat.Revision <= 0 {
		return gofuse.EIO
	}
	if testHookAfterOpenSurvivorStat != nil {
		testHookAfterOpenSurvivorStat(fh)
	}
	fh.Lock()
	defer fh.Unlock()
	if fh.Ino != ino || (fh.Path != old && fh.Path != target) || !cleanSurvivorBindingEligible(fh, stat.Revision, true) || fs.latestCommittedRevision(target) > stat.Revision {
		return gofuse.EIO
	}
	fs.inodes.mu.RLock()
	current := fs.inodes.byInode[ino]
	if current == nil || current.ResourceID != resourceID || current.Path != target || fs.inodes.byPath[target] != ino || current.Revision > stat.Revision {
		fs.inodes.mu.RUnlock()
		return gofuse.EIO
	}
	fs.openHandles.mu.Lock()
	indexed, hasPath := fs.openHandles.pathByHandle[fh]
	_, registered := fs.openHandles.byInode[ino][fh]
	// An unlink-marked, unexposed Open may be detached from byPath; membership
	// by inode remains its lifetime proof. A mismatched existing path is invalid.
	if hasPath && indexed != fh.Path || registered && !hasPath && !fh.Unlinked {
		fs.openHandles.mu.Unlock()
		fs.inodes.mu.RUnlock()
		return gofuse.EIO
	}
	fs.moveSurvivorBindingIndexLocked(fh, target)
	fs.openHandles.mu.Unlock()
	fs.inodes.mu.RUnlock()
	fs.applySurvivorBindingLocked(fh, stat.Revision, stat.Size)
	return gofuse.OK
}

// discardUnexposedSurvivorOpen releases resources owned by a rejected Open.
func (fs *Dat9FS) discardUnexposedSurvivorOpen(fh *FileHandle) {
	fh.Lock()
	fh.Unlinked = false
	fs.rollbackUnlinkedSnapshotAttachLocked(fh, 0)
	fh.Unlock()
	fs.releaseHandleShadowPin(fh)
	if fh.Prefetch != nil {
		fh.Prefetch.Close()
	}
	if fh.Streamer != nil {
		fh.Streamer.Abort()
	}
}

var testHookBeforeUnlinkSurvivorIndexCheck func()
var testHookAfterOpenSurvivorStat func(*FileHandle)

var testHookBeforeUnlinkSurvivorSync func(string)
var testHookBeforeUnlinkSurvivorWait func(string)
var testHookBeforeUnlinkSurvivorBind func(string)
