package fuse

func (fs *Dat9FS) refreshCommittedRevisionForOpenHandles(path string, revision int64, skip *FileHandle) {
	if fs == nil || fs.openHandles == nil || path == "" || revision <= 0 {
		return
	}

	for _, fh := range fs.openHandles.SnapshotPath(path) {
		if fh == nil || fh == skip {
			continue
		}
		// This method is called from commit paths that may already hold another
		// same-path handle lock. Never block on sibling handles here: two
		// concurrent commits can otherwise deadlock by each holding one handle
		// and waiting for the other. Clean locked siblings refresh lazily in
		// Read before serving any loaded clean writable-buffer bytes.
		if !fh.TryLock() {
			continue
		}
		fs.discardSupersededMutationLocked(fh)
		if fs.handleCanAdoptCommittedRevisionLocked(fh) {
			cleanBuffer := fh.Dirty != nil
			if cleanBuffer && fs.clearRemovedCommittedShadowLocked(fh, revision, fs.committedHandleSizeLocked(fh), true) {
				fh.Unlock()
				continue
			}
			fs.adoptCleanCommittedRevisionLocked(fh, revision, fs.committedHandleSizeLocked(fh))
		}
		fh.Unlock()
	}
}

// refreshCommittedRevisionForOpenHandlesWithSize is like
// refreshCommittedRevisionForOpenHandles but uses an explicit committed
// size instead of reading from the inode table. This is needed in the
// Path 2 concurrent-write case where the inode size has already been
// updated by the concurrent Write() but the committed revision
// corresponds to the pre-write snapshot.
func (fs *Dat9FS) refreshCommittedRevisionForOpenHandlesWithSize(path string, revision int64, skip *FileHandle, committedSize int64) {
	if fs == nil || fs.openHandles == nil || path == "" || revision <= 0 {
		return
	}

	for _, fh := range fs.openHandles.SnapshotPath(path) {
		if fh == nil || fh == skip {
			continue
		}
		if !fh.TryLock() {
			continue
		}
		fs.discardSupersededMutationLocked(fh)
		if fs.handleCanAdoptCommittedRevisionLocked(fh) {
			cleanBuffer := fh.Dirty != nil
			if cleanBuffer && fs.clearRemovedCommittedShadowLocked(fh, revision, committedSize, true) {
				fh.Unlock()
				continue
			}
			fs.adoptCleanCommittedRevisionLocked(fh, revision, committedSize)
		}
		fh.Unlock()
	}
}

func (fs *Dat9FS) handleCanAdoptCommittedRevisionLocked(fh *FileHandle) bool {
	if fh == nil {
		return false
	}
	if fh.WriteBackSeq != 0 || fh.ShadowCommitReady || fh.ShadowCommitSeq != 0 {
		return false
	}
	if fs != nil && fh.PendingIndexGen != 0 && fs.pendingIndex != nil && fs.pendingIndex.Generation(fh.Path) == fh.PendingIndexGen {
		return false
	}
	if fs != nil && fh.ShadowStageGen != 0 && fs.shadowStore != nil && fs.shadowStore.ActiveGeneration(fh.Path) == fh.ShadowStageGen {
		return false
	}
	// A dirty handle's bytes were prepared against its current BaseRev.
	// Advancing only the revision token after a sibling commit would create
	// the silent-rollback shape: old payload + new CAS base. This applies to
	// SQLite sidecar journals too: before staging, a dirty app.db-wal handle
	// is still old WAL payload and must not silently become "based on" a newer
	// sibling checkpoint revision.
	if fh.DirtySeq != 0 || (fh.Dirty != nil && fh.Dirty.HasDirtyParts()) {
		return false
	}
	return true
}

// isPassiveCloseSyncHandleLocked identifies acknowledged handles which may
// still await Release but must not take ownership of a new path truncate.
func (fs *Dat9FS) isPassiveCloseSyncHandleLocked(fh *FileHandle) bool {
	return fh != nil && fh.WritePolicy == WritePolicyCloseSync &&
		!fh.IsNew && fs.handleCanAdoptCommittedRevisionLocked(fh)
}

// clearHandleShadowClaimLocked retires the handle's staging/commit claim:
// ready/spill flags and staging/commit generation/sequence tokens. The read pin
// (ShadowGen/ShadowPinned) has an independent lifetime and is not retired here.
// This does not mutate the path-keyed store, which may contain a newer writer's
// generation. Callers own generation-scoped store cleanup and buffer rebind.
func clearHandleShadowClaimLocked(fh *FileHandle) {
	fh.ShadowReady = false
	fh.ShadowSpill = false
	fh.ShadowStageGen = 0
	fh.ShadowStageSeq = 0
	fh.ShadowCommitReady = false
	fh.ShadowCommitSeq = 0
}

// adoptCleanCommittedRevisionLocked rebinds an eligible clean handle to a
// committed remote image, including its shadow claim and eviction callbacks.
func (fs *Dat9FS) adoptCleanCommittedRevisionLocked(fh *FileHandle, revision, size int64) {
	if fs == nil || fh == nil {
		return
	}
	// Read pins validate freshness independently. Leave shared staging bytes
	// to their generation-scoped owner/lifecycle rather than guessing ownership
	// from whether a sibling mutex happens to be available.
	fh.IsNew = false
	fh.BaseRev = revision
	clearHandleShadowClaimLocked(fh)
	fs.rebindCleanWriteBufferToRemoteLocked(fh, size)
	fh.appendLogAdoptCommittedBaseline(revision, size)
	if fh.Streamer != nil {
		fh.Streamer.RefreshExpectedRevision(expectedRevisionForHandle(fh))
	}
}

func (fs *Dat9FS) refreshCleanCommittedRevisionForHandleLocked(fh *FileHandle) bool {
	if fs == nil || fh == nil || fh.Dirty == nil || !fs.handleCanAdoptCommittedRevisionLocked(fh) {
		return false
	}
	revision := fs.latestCommittedRevision(fh.Path)
	if revision <= 0 || revision <= fh.BaseRev {
		return false
	}
	fs.adoptCleanCommittedRevisionLocked(fh, revision, fs.committedHandleSizeLocked(fh))
	return true
}

// syncPassivePathTruncateLocked sizes a clean close-sync handle without
// making it a second publisher. A positive revision acknowledges a remote
// commit; zero is only a local view change while another handle owns the write.
func (fs *Dat9FS) syncPassivePathTruncateLocked(fh *FileHandle, size, revision int64) error {
	fs.adoptCommittedStorageClassLocked(fh, size)
	if revision > 0 {
		fh.ZeroBase = false
		fs.adoptCleanCommittedRevisionLocked(fh, revision, size)
		return nil
	}
	if fh.Dirty.Size() != size {
		if fs.appendLogPathConfigured(fh.Path) {
			// OrigSize may shrink for routing below. Preserve the actual CAS
			// baseline before changing this uncommitted view.
			fh.appendLogRecordTruncate()
		}
		if err := fh.Dirty.Truncate(size); err != nil {
			return err
		}
		fh.Dirty.ResetSequentialState(size)
		if size == 0 || size < fh.OrigSize {
			fh.OrigSize = size
		}
	}
	fh.Dirty.ClearDirty()
	fh.ZeroBase = false
	clearHandleShadowClaimLocked(fh)
	fs.rebaselineCommittedDirtyBufferLocked(fh)
	return nil
}

// syncOpenHandlesAfterPathTruncate truncates the WriteBuffer of every open
// dirty handle for the given inode after a path-based truncate(path, size).
func (fs *Dat9FS) syncOpenHandlesAfterPathTruncate(ino uint64, newSize int64) {
	fs.syncOpenHandlesAfterPathTruncateWithCommit(ino, 0, newSize, false)
}

func (fs *Dat9FS) syncOpenHandlesAfterCommittedPathTruncate(ino uint64, callerPID uint32, newSize int64) {
	fs.syncOpenHandlesAfterPathTruncateWithCommit(ino, callerPID, newSize, true)
}

func (fs *Dat9FS) syncOpenHandlesAfterPathTruncateWithCommit(ino uint64, callerPID uint32, newSize int64, remoteCommitted bool) {
	fs.syncOpenHandlesAfterPathTruncateState(ino, callerPID, newSize, remoteCommitted, false)
}

// syncOpenHandlesAfterStagedPathTruncate updates open-handle views after a
// path truncate has been durably staged but is still owned by CommitQueue.
// Clean handles stay clean until queue completion publishes the new revision.
func (fs *Dat9FS) syncOpenHandlesAfterStagedPathTruncate(ino uint64, callerPID uint32, newSize int64) {
	fs.syncOpenHandlesAfterPathTruncateState(ino, callerPID, newSize, false, true)
}

func (fs *Dat9FS) syncOpenHandlesAfterPathTruncateState(ino uint64, callerPID uint32, newSize int64, remoteCommitted, remoteStaged bool) {
	for _, fh := range fs.fileHandlesForInode(ino) {
		fh.Lock()
		if fh.Dirty == nil {
			fh.Unlock()
			continue
		}
		// A close-sync handle whose commit already completed may still await
		// asynchronous Release. It reflects the path mutation but never becomes
		// a second publisher.
		if fs.isPassiveCloseSyncHandleLocked(fh) {
			var committedRevision int64
			if remoteCommitted || fs.appendLogPathConfigured(fh.Path) {
				if revision, size, ok := fs.latestCommittedRevisionWithSize(fh.Path); ok && size == newSize {
					committedRevision = revision
				}
			}
			if err := fs.syncPassivePathTruncateLocked(fh, newSize, committedRevision); err != nil {
				safeLogPrintf("passive path-truncate sync failed for %s: %v", fh.Path, err)
			}
			fh.Unlock()
			continue
		}

		cleanHandle := !writableHandleHasPendingContentLocked(fh)
		// A completed remote path truncate is authoritative for every clean
		// kernel-owned handle, including an older process whose Release is late.
		mayRebaseCleanHandle := fs.appendLogPathConfigured(fh.Path) ||
			(remoteCommitted && fh.OpenPID != 0)
		if cleanHandle && mayRebaseCleanHandle {
			if revision, committedSize, ok := fs.latestCommittedRevisionWithSize(fh.Path); ok && revision > 0 && committedSize == newSize {
				fs.adoptCommittedStorageClassLocked(fh, newSize)
				if fh.Dirty.Size() != newSize {
					if err := fh.Dirty.Truncate(newSize); err != nil {
						safeLogPrintf("clean path-truncate rebase failed for %s: %v", fh.Path, err)
						fh.Unlock()
						continue
					}
					fh.Dirty.ResetSequentialState(newSize)
				}
				fh.Dirty.ClearDirty()
				fh.OrigSize = newSize
				fh.ZeroBase = false
				fs.adoptCleanCommittedRevisionLocked(fh, revision, newSize)
				fh.Unlock()
				continue
			}
		}
		if cleanHandle && (remoteStaged || (remoteCommitted && fs.layerEnabled())) {
			// CommitQueue or the layer entry owns this mutation. Reflect its
			// acknowledged size without creating a hidden Release generation.
			fs.adoptCommittedStorageClassLocked(fh, newSize)
			if fh.Dirty.Size() != newSize {
				if err := fh.Dirty.Truncate(newSize); err != nil {
					safeLogPrintf("staged path-truncate sync failed for %s: %v", fh.Path, err)
					fh.Unlock()
					continue
				}
				fh.Dirty.ResetSequentialState(newSize)
			}
			fh.Dirty.ClearDirty()
			fh.OrigSize = newSize
			fh.ZeroBase = false
			fh.Unlock()
			continue
		}

		fs.adoptCommittedStorageClassLocked(fh, newSize)
		if fh.Dirty.Size() != newSize {
			if err := fh.Dirty.Truncate(newSize); err != nil {
				safeLogPrintf("path-truncate sync: dirty buffer truncate failed for %s: %v", fh.Path, err)
				fh.Unlock()
				continue
			}
			fh.Dirty.ResetSequentialState(newSize)
			fh.ZeroBase = newSize == 0
			if newSize == 0 || newSize < fh.OrigSize {
				fh.OrigSize = newSize
			}
		}
		fh.DirtySeq = fs.markDirtySize(ino, newSize)
		fh.Unlock()
	}
}
