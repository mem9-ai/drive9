package fuse

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/pingcap/failpoint"
)

// renameOwnedWriteBack keeps the exact cache/staging tuple inaccessible to
// uploaders until its new ownership is published. No handle locks or Submit
// calls are allowed in this window. Store methods below require held path locks.
func (fs *Dat9FS) renameOwnedWriteBack(oldPath, newPath string) (moved bool, err error) {
	first, second := oldPath, newPath
	if second < first {
		first, second = second, first
	}
	if fs.uploader != nil {
		unlockFirst := fs.uploader.acquirePath(first)
		defer unlockFirst()
		unlockSecond := fs.uploader.acquirePath(second)
		defer unlockSecond()
	}
	cache := fs.writeBack
	firstLock := cache.acquirePathLock(first)
	defer cache.releasePathLock(first, firstLock)
	secondLock := cache.acquirePathLock(second)
	defer cache.releasePathLock(second, secondLock)
	if fs.pendingIndex != nil {
		a, b := fs.pendingIndex.acquireTwoPathLocks(first, second)
		defer fs.pendingIndex.releaseTwoPathLocks(first, second, a, b)
	}
	if fs.shadowStore != nil {
		a, b := fs.shadowStore.acquireTwoPathLocks(first, second)
		defer fs.shadowStore.releaseTwoPathLocks(first, second, a, b)
	}
	meta, ok := cache.getMetaLocked(oldPath)
	if !ok {
		return false, nil // Uploaded while we acquired exclusion.
	}
	conflictPath, conflictGen := oldPath, meta.Generation
	defer func() {
		if err != nil {
			err = errors.Join(err, cache.markRenameConflictLocked(conflictPath, conflictGen))
		}
	}()
	if !meta.ownedStagingKnown || meta.Kind == PendingConflict {
		return false, syscall.EAGAIN
	}
	if _, exists := cache.getMetaLocked(newPath); exists {
		return false, syscall.EAGAIN
	}
	var pending *WriteBackMeta
	if fs.pendingIndex != nil {
		if _, exists := fs.pendingIndex.GetMeta(newPath); exists {
			return false, syscall.EAGAIN
		}
		pending, _ = fs.pendingIndex.GetMeta(oldPath)
	}
	if fs.shadowStore != nil && fs.shadowStore.Has(newPath) {
		return false, syscall.EAGAIN
	}
	if !cache.renamePendingLocked(oldPath, newPath) {
		return false, fmt.Errorf("rename writeback cache %s to %s: %w", oldPath, newPath, syscall.EIO)
	}
	movedMeta, _ := cache.getMetaLocked(newPath)
	conflictPath, conflictGen = newPath, movedMeta.Generation
	failpoint.InjectCall("ownedWriteBackRenameRekeyed", fs, oldPath, newPath)
	if pending == nil {
		if meta.ownedStagingGens.PendingIndexGen != 0 || meta.ownedStagingGens.ShadowGen != 0 {
			return true, syscall.EAGAIN // Missing source is not proof of ownership.
		}
		return true, nil
	}
	prepared, prepareErr := fs.pendingIndex.prepareRenameLocked(oldPath, newPath)
	if prepareErr != nil {
		return true, prepareErr
	}
	if prepared == nil {
		return true, syscall.EAGAIN
	}
	if fs.shadowStore != nil && fs.shadowStore.Has(oldPath) {
		if !fs.shadowStore.renameLocked(oldPath, newPath) {
			fs.pendingIndex.abortRenameLocked(newPath)
			return true, fmt.Errorf("rename staged shadow %s: %w", oldPath, syscall.EIO)
		}
	} else if meta.ownedStagingGens.ShadowGen != 0 {
		fs.pendingIndex.abortRenameLocked(newPath)
		return true, syscall.EAGAIN
	}
	fs.pendingIndex.commitRenameLocked(oldPath, prepared)
	// A newer independently staged image still moves with the namespace, but
	// the old cache snapshot never acquires its cleanup token.
	if pending.Generation == meta.ownedStagingGens.PendingIndexGen &&
		(pending.SnapshotID == "" || meta.SnapshotID == "" || pending.SnapshotID == meta.SnapshotID) {
		cache.mu.Lock()
		cache.metas[newPath].ownedStagingGens.PendingIndexGen = prepared.Generation
		cache.mu.Unlock()
	}
	// ShadowStore.renameLocked preserves its content generation, so the
	// original shadow token remains exact (or intentionally stale).
	return true, nil
}
