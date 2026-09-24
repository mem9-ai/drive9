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
//
// Without this, applyRemoteTruncate updates the remote file and the inode's
// cached size, but the open handle's WriteBuffer still holds the pre-truncate
// data. A subsequent fstat(fd) calls GetAttr → dirtyHandleSize(ino), which
// returns the stale WriteBuffer size instead of the truncated size, causing
// fstat() mismatch (LTP ftest01 m_fstat case).
//
// Both shrink and grow are handled. For shrink, the buffer is truncated.
// For grow, the buffer is zero-extended so a later flush uploads the correct
// size (not stale shorter content that could shrink the file back).
func (fs *Dat9FS) syncOpenHandlesAfterPathTruncate(ino uint64, newSize int64) {
	for _, fh := range fs.fileHandlesForInode(ino) {
		fh.Lock()
		if fh.Dirty == nil {
			fh.Unlock()
			continue
		}
		if fs.isPassiveCloseSyncHandleLocked(fh) {
			var committedRevision int64
			if fs.appendLogPathConfigured(fh.Path) {
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
		if fs.appendLogPathConfigured(fh.Path) && fh.DirtySeq == 0 && !fh.Dirty.HasDirtyParts() {
			if revision, committedSize, ok := fs.latestCommittedRevisionWithSize(fh.Path); ok && revision > 0 && committedSize == newSize {
				fs.adoptCommittedStorageClassLocked(fh, newSize)
				if fh.Dirty.Size() != newSize {
					if err := fh.Dirty.Truncate(newSize); err != nil {
						safeLogPrintf("append-log path-truncate sync failed for %s: %v", fh.Path, err)
						fh.Unlock()
						continue
					}
					fh.Dirty.ResetSequentialState(newSize)
				}
				fh.Dirty.ClearDirty()
				fh.BaseRev = revision
				fh.OrigSize = newSize
				fh.ZeroBase = false
				fh.appendLogAdoptCommittedBaseline(revision, newSize)
				fh.Unlock()
				continue
			}
		}
		// Upload routing follows the truncated size. This notification can
		// precede the caller-owned handle's remote commit.
		fs.adoptCommittedStorageClassLocked(fh, newSize)
		curSize := fh.Dirty.Size()
		if curSize != newSize {
			if err := fh.Dirty.Truncate(newSize); err != nil {
				safeLogPrintf("path-truncate sync: dirty buffer truncate failed for %s: %v", fh.Path, err)
				fh.Unlock()
				continue
			}
			fh.Dirty.ResetSequentialState(newSize)
			fh.ZeroBase = newSize == 0
			// Reset OrigSize only when truncating to 0 or shrinking to
			// prevent PatchFile on db9-backed files (see B5 rationale).
			if newSize == 0 || newSize < fh.OrigSize {
				fh.OrigSize = newSize
			}
		}
		fh.DirtySeq = fs.markDirtySize(ino, newSize)
		fh.Unlock()
	}
}
