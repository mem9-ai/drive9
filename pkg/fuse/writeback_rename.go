package fuse

import (
	"errors"
	"fmt"
	"sort"
	"sync"
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
	return fs.renameOwnedWriteBackLocked(oldPath, newPath)
}

// renameOwnedWriteBackLocked requires the caller to hold both uploader path
// exclusions. Store path locks are acquired here; no handle locks or Submit.
func (fs *Dat9FS) renameOwnedWriteBackLocked(oldPath, newPath string) (moved bool, err error) {
	first, second := oldPath, newPath
	if second < first {
		first, second = second, first
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
	var pending *WriteBackMeta
	if fs.pendingIndex != nil {
		pending, _ = fs.pendingIndex.GetMeta(oldPath)
	}
	conflictPath, conflictGen := oldPath, meta.Generation
	defer func() {
		if err != nil {
			err = errors.Join(err, cache.markRenameConflictLocked(conflictPath, conflictGen))
			if pending != nil {
				err = errors.Join(err, fs.pendingIndex.markRenameConflictLocked(oldPath, pending.Generation))
			}
		}
	}()
	if !meta.ownedStagingKnown || meta.Kind == PendingConflict {
		return false, syscall.EAGAIN
	}
	if _, exists := cache.getMetaLocked(newPath); exists {
		return false, syscall.EAGAIN
	}
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

// lockOwnedRenamePaths fences existing owned descendants before the remote
// namespace changes. Recheck the capture after waiting; never wait for uploads
// or acquire handle locks inside this interval. The returned release is idempotent.
func (fs *Dat9FS) lockOwnedRenamePaths(oldP, newP string) (map[string]uint64, func(), error) {
	captured := make(map[string]uint64)
	paths := make(map[string]bool)
	if fs.writeBack != nil {
		for _, meta := range fs.writeBack.ListByPrefix(oldP + "/") {
			if !meta.ownedStagingKnown {
				continue
			}
			captured[meta.Path] = meta.Generation
			paths[meta.Path] = true
			paths[newP+meta.Path[len(oldP):]] = true
		}
	}
	failpoint.InjectCall("ownedRenamePathsCaptured", fs)
	ordered := make([]string, 0, len(paths))
	for p := range paths {
		ordered = append(ordered, p)
	}
	sort.Strings(ordered)
	var unlocks []func()
	var once sync.Once
	unlock := func() {
		once.Do(func() {
			for i := len(unlocks) - 1; i >= 0; i-- {
				unlocks[i]()
			}
		})
	}
	for _, p := range ordered {
		unlocks = append(unlocks, fs.lockRemoteCommitPath(p))
	}
	if fs.uploader != nil {
		for _, p := range ordered {
			unlocks = append(unlocks, fs.uploader.acquirePath(p))
		}
	}
	if fs.writeBack != nil {
		for _, meta := range fs.writeBack.ListByPrefix(oldP + "/") {
			if generation, covered := captured[meta.Path]; (meta.ownedStagingKnown && (!covered || generation != meta.Generation)) || (covered && !meta.ownedStagingKnown) {
				unlock()
				return nil, func() {}, syscall.EAGAIN
			}
		}
	}
	failpoint.InjectCall("ownedRenamePathsLocked", fs)
	return captured, unlock, nil
}
