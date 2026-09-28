package fuse

import (
	"context"
	"syscall"
	"time"

	"github.com/pingcap/failpoint"
)

// recoverFtruncateViewLocked retires a stale observation only after its content
// is committed and the current authorized identity has been checked. Caller
// holds fh.mu; resetMountView never waits for that mutex or performs this IO.
func (fs *Dat9FS) recoverFtruncateViewLocked(ctx context.Context, fh *FileHandle) error {
	event := fh.pendingFtruncate.Load()
	view := fs.mountViewGeneration.Load()
	if event == nil || event.view == view {
		return nil
	}
	if !fs.ftruncateAliasLinked(fh) || !fs.handleCanAdoptCommittedRevisionLocked(fh) || (fh.Streamer != nil && fh.Streamer.Started()) {
		return syscall.EAGAIN
	}
	if !fh.ftruncateFence || fh.RemoteCommitUnlock == nil {
		unlock, ok := fs.tryLockFtruncateAliases(fh)
		if !ok {
			return errFtruncateBusy
		}
		defer unlock()
	}
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || entry.ResourceID == "" {
		return syscall.EAGAIN
	}
	paths := fs.ftruncateAliasPaths(fh.Ino)
	fs.dirtyMu.Lock()
	committed := fs.mutationInodes[fh.Ino]
	fs.dirtyMu.Unlock()
	if committed.committedSeq < event.seq || committed.committedRevision <= 0 {
		return syscall.EAGAIN
	}
	settled := func() bool {
		current, exists := fs.inodes.GetEntry(fh.Ino)
		if !exists || current.ResourceID != entry.ResourceID {
			return false
		}
		fs.dirtyMu.Lock()
		unchanged := fs.mutationInodes[fh.Ino] == committed && fs.dirtyInodes[fh.Ino].seq <= committed.committedSeq
		fs.dirtyMu.Unlock()
		if !unchanged || fh.pendingFtruncate.Load() != event || !fs.ftruncateAliasesValid(fh.Ino, paths, view) {
			return false
		}
		for _, p := range paths {
			if fs.commitQueue != nil && fs.commitQueue.HasPath(p) {
				return false
			}
			if fs.pendingIndex != nil {
				if m, exists := fs.pendingIndex.GetMeta(p); exists && m.Kind != PendingChmod {
					return false
				}
			}
			if fs.writeBack != nil {
				if m, exists := fs.writeBack.GetMeta(p); exists && m.Kind != PendingChmod {
					return false
				}
			}
		}
		for _, other := range fs.openHandles.SnapshotInode(fh.Ino) {
			if other == fh {
				continue
			}
			if !other.TryLock() {
				return false
			}
			pending := other.DirtySeq > committed.committedSeq || (other.DirtySeq == 0 && other.Dirty != nil && other.Dirty.HasDirtyParts())
			other.Unlock()
			if pending {
				return false
			}
		}
		return true
	}
	if !settled() {
		return errFtruncateBusy
	}
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(fh.Path))
	if err != nil {
		return err
	}
	if stat == nil || stat.ResourceID != entry.ResourceID || stat.Revision < committed.committedRevision || stat.Size < 0 {
		return syscall.EAGAIN
	}
	failpoint.InjectCall("ftruncateResetVerified", fs, fh)
	if !fs.lockMountViewRead(view) {
		return syscall.EAGAIN
	}
	defer fs.mountViewMu.RUnlock()
	if !settled() {
		return syscall.EAGAIN
	}
	if !fh.pendingFtruncate.CompareAndSwap(event, nil) {
		return syscall.EAGAIN
	}
	if fh.ShadowPinned && fs.shadowStore != nil {
		fs.shadowStore.Unpin(fh.ShadowGen)
		fh.ShadowPinned, fh.ShadowGen = false, 0
	}
	clearReadTargetForLockedHandle(fh)
	if fh.Prefetch != nil {
		fh.Prefetch.invalidateWithSize(stat.Size)
	}
	fh.BaseRev, fh.OrigSize = stat.Revision, stat.Size
	clearHandleShadowClaimLocked(fh)
	// An inactive uploader belongs to the old fully loaded buffer. A lazy
	// replacement must use the normal load/patch path, never UploadAll zeros.
	if fh.Streamer != nil {
		fh.Streamer.Abort()
		fh.Streamer = nil
	}
	fs.rebindCleanWriteBufferToRemoteLocked(fh, stat.Size)
	clearStagedSnapshotLineageLocked(fh)
	fh.ContentSnapshotID, fh.ftruncateInherited = "", ""
	fh.contentAncestors = nil
	fh.LineageTrusted, fh.appendSnapshot = false, false
	fs.openHandles.UnmarkVisibleTruncate(fh)
	for _, p := range paths {
		fs.recordCommittedRevisionWithSize(p, stat.Revision, stat.Size)
	}
	fs.inodes.advanceRevision(fh.Ino, stat.Revision)
	fs.inodes.UpdateSize(fh.Ino, stat.Size)
	return nil
}

// Capture missing identity only while establishing a current-view truncate or
// its successful content commit. Ordinary commits and reset itself do no IO.
func (fs *Dat9FS) captureFtruncateIdentity(ctx context.Context, ino uint64, path string, revision int64) {
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.Unlinked || entry.ResourceID != "" || revision <= 0 {
		return
	}
	view := fs.mountViewGeneration.Load()
	observed := false
	for _, h := range fs.openHandles.SnapshotInode(ino) {
		if e := h.pendingFtruncate.Load(); e != nil && e.view == view {
			observed = true
			break
		}
	}
	if !observed {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(path))
	if err != nil || stat == nil || stat.Revision != revision || stat.ResourceID == "" || !fs.lockMountViewRead(view) {
		return
	}
	defer fs.mountViewMu.RUnlock()
	fs.inodes.mu.Lock()
	defer fs.inodes.mu.Unlock()
	if current := fs.inodes.byInode[ino]; current != nil && !current.Unlinked && current.ResourceID == "" && fs.inodes.byPath[path] == ino {
		fs.inodes.setIdentityLocked(current, stat.ResourceID)
		if stat.Nlink > 0 {
			current.Nlink = stat.Nlink
		}
	}
}
