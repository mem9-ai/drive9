package fuse

// An enqueue failure may already have preserved this exact full image as a
// durable conflict. It can outlive the handle, unlike an unstaged RAM image.
func (fs *Dat9FS) releasedImageHasDurableConflictLocked(fh *FileHandle) bool {
	if fh.HasPendingMode || fh.Dirty == nil || fs.shadowStore == nil || fs.pendingIndex == nil ||
		fh.PendingIndexGen == 0 || fh.ShadowStageGen == 0 || !fh.StagedLineageTrusted ||
		fh.StagedSnapshotID == "" || fh.StagedSnapshotSeq != fh.DirtySeq || !fs.ftruncateAliasLinked(fh) {
		return false
	}
	hadFence := fh.ftruncateFence
	if err := fs.fenceFtruncateLocked(fh); err != nil {
		return false
	}
	if !hadFence {
		defer fs.releaseHandleRemoteCommitPathLocked(fh)
	}
	meta, ok := fs.pendingIndex.GetMeta(fh.Path)
	return ok && meta.Kind == PendingConflict && meta.Generation == fh.PendingIndexGen && meta.lineageTrusted &&
		meta.Inode == fh.Ino && meta.MutationSeq == fh.DirtySeq && meta.Size == fh.Dirty.Size() &&
		meta.SnapshotID == fh.StagedSnapshotID && meta.ownedStagingKnown &&
		meta.ownedStagingGens.ShadowGen == fh.ShadowStageGen &&
		fs.shadowStore.ActiveGeneration(fh.Path) == fh.ShadowStageGen
}

// The caller owns fh.mu and releaseClaim and has discharged or transferred all
// obligations. Remove membership before releasing detached resources outside mu.
func (fs *Dat9FS) finalizeReleasedHandleLocked(id uint64, fh *FileHandle) func() {
	prefetch, ino, path := fh.Prefetch, fh.Ino, fh.Path
	fh.Prefetch = nil
	var shadow, unlinked uint64
	if fh.ShadowPinned {
		shadow = fh.ShadowGen
		fh.ShadowPinned, fh.ShadowGen = false, 0
	}
	unlinked, fh.UnlinkedShadowGen = fh.UnlinkedShadowGen, 0
	fs.deleteFileHandle(id, fh)
	return func() {
		if prefetch != nil {
			prefetch.Close()
		}
		if fs.shadowStore != nil {
			if shadow != 0 {
				fs.shadowStore.Unpin(shadow)
			}
			if unlinked != 0 {
				fs.shadowStore.Unpin(unlinked)
			}
		}
		fs.cleanupReleasedInode(ino, path)
	}
}
