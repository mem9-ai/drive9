package fuse

import (
	"errors"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func (fs *Dat9FS) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseFsync, perfStart, status, 0) }()
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return gofuse.OK
	}
	if fh.isExtent() {
		return fs.extentFsync(fs.jfsCtx(input.Pid, input.Uid, input.Gid), fh, int(input.FsyncFlags))
	}
	ctx, cf := fs.syncDataCommitContext(cancel, fuseTimeout)
	defer cf()
	fs.observePathPolicyWithContext(ctx, fh.Path)

	start := time.Now()
	phase := "start"
	fs.debugf("fsync start path=%s fh=%d ino=%d", fh.Path, input.Fh, fh.Ino)
	lockStart := time.Now()
	fh.Lock()
	lockWait := time.Since(lockStart)
	if fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
		fs.debugf("fsync lock wait path=%s fh=%d ino=%d wait=%s", fh.Path, input.Fh, fh.Ino, lockWait)
	}
	defer fh.Unlock()
	if fs.debugEnabled() && strings.HasSuffix(fh.Path, ".db") {
		mainDBPath := fh.Path
		mainDBFsyncStarted := time.Now()
		defer func() {
			fs.debugf("append-log trace event=main_db_fsync path=%q status=%d wall_unix_nano=%d duration_ns=%d lock_wait_ns=%d", mainDBPath, status, time.Now().UnixNano(), time.Since(mainDBFsyncStarted).Nanoseconds(), lockWait.Nanoseconds())
		}()
	}
	if fs.debugEnabled() && strings.HasSuffix(fh.Path, ".db-wal") {
		walPath := fh.Path
		defer func() {
			fs.debugf("append-log trace event=wal_fsync path=%q status=%d wall_unix_nano=%d duration_ns=%d lock_wait_ns=%d", walPath, status, time.Now().UnixNano(), time.Since(start).Nanoseconds(), lockWait.Nanoseconds())
		}()
	}
	defer func() {
		if !fs.debugEnabled() {
			return
		}
		var size int64
		dirty := false
		if fh.Dirty != nil {
			size = fh.Dirty.Size()
			dirty = fh.Dirty.HasDirtyParts()
		}
		d := time.Since(start)
		if status == gofuse.OK && d < fuseDebugSlowOpThreshold {
			return
		}
		fs.debugf("fsync done path=%s fh=%d ino=%d phase=%s size=%d dirty=%t shadow_spill=%t status=%d dur=%s", fh.Path, input.Fh, fh.Ino, phase, size, dirty, fh.ShadowSpill, status, d)
	}()

	if isLocalFileHandle(fh) {
		phase = "local-sync"
		if err := syncOpenLocalFile(fh.LocalFile); err != nil {
			return localErrToFuseStatus(err)
		}
		if info, err := fh.LocalFile.Stat(); err == nil {
			fs.inodes.UpdateSize(fh.Ino, info.Size())
			fs.inodes.UpdateMtime(fh.Ino, info.ModTime())
		}
		if localPathShouldCheckpointGitState(fh.Path) && localFileHandleOpenedWritable(fh) {
			phase = "local-git-checkpoint"
			checkpointCtx, checkpointCancel := fs.syncDataCommitContext(cancel, gitCheckpointTimeout)
			defer checkpointCancel()
			if err := fs.checkpointGitStateAfterLocalWrite(checkpointCtx, fh.Path, true); err != nil {
				return httpToFuseStatus(err)
			}
		}
		return gofuse.OK
	}
	if fh.Layer == PathLayerGitWorkspace {
		phase = "git-overlay"
		flushCtx, flushCancel := fs.syncDataCommitContext(cancel, gitCheckpointTimeout)
		defer flushCancel()
		return fs.flushGitHandleLockedWithPolicy(flushCtx, fh, fs.syncMode == SyncStrict)
	}

	if st := fs.layerHandleMutationStatusLocked(fh); st != gofuse.OK {
		return st
	}

	// Unlink-while-open: never stage/enqueue/upload. Fsync can otherwise
	// re-enter commitQueue after Unlink's ordered drain and resurrect the
	// deleted path (write → unlink → fsync → close → rmdir ENOTEMPTY).
	// Keep Dirty so a successful write-after-unlink stays readable.
	if fh.Unlinked {
		phase = "unlinked-discard"
		fs.cancelUnlinkedRemotePublishLocked(fh)
		return gofuse.OK
	}
	if fs.ftruncateParticipates(fh) {
		defer fs.releaseHandleRemoteCommitPathLocked(fh)
	}
	if handled, err := fs.prepareFtruncateCommitLocked(ctx, fh, fs.syncMode == SyncStrict); handled || err != nil {
		return httpToFuseStatus(err)
	}
	if fs.discardSupersededMutationLocked(fh) {
		phase = "superseded-mutation"
		return gofuse.OK
	}
	if fs.appendLogConfiguredLocked(fh) {
		phase = "append-log-sync"
		size := int64(0)
		if fh.Dirty != nil {
			size = fh.Dirty.Size()
		}
		recordSync := fh.IsNew || (fh.Dirty != nil && fh.Dirty.HasDirtyParts())
		syncStart := time.Now()
		syncCtx, syncCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
		defer syncCancel()
		status, fullRewrite := fs.syncAppendLogHandleToRemoteLocked(syncCtx, fh, shadowUploadLocalDurable)
		if recordSync && fs.perfEnabled() {
			fs.perf.recordAppendLogFsync(fullRewrite, time.Since(syncStart))
		}
		return status
	}

	// Interactive mode: Fsync = local durable only. Shadow file + journal
	// ensure crash safety. Remote commit happens asynchronously.
	requiresRemoteSync := isSQLitePersistentJournalPath(fh.Path)
	if fs.syncMode == SyncInteractive && !requiresRemoteSync {
		if fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
			phase = "interactive-clean"
			return gofuse.OK
		}
		if fh.ShadowSpill {
			// ShadowSpill: stage shadow + journal, no writeBack snapshot.
			phase = "interactive-shadowspill-stage"
			stageStart := time.Now()
			err := fs.stageShadowForQueuedCommitLocked(fh, true)
			if errors.Is(err, syscall.EAGAIN) {
				return gofuse.EAGAIN
			}
			fs.debugDurationf(stageStart, 0, "fsync shadowspill stage done path=%s err=%v", fh.Path, err)
			if err == nil {
				if fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() {
					phase = "superseded-after-commit-fence"
					return gofuse.OK
				}
				// Journal before enqueue: the async commit appends a
				// JournalCommit marker, which must get a higher Seq than
				// this fsync frame or replay resurrects a committed path.
				if fs.journal != nil {
					entry := JournalEntry{
						Op:          JournalFsync,
						Path:        fh.Path,
						Length:      fh.Dirty.Size(),
						BaseRev:     fh.BaseRev,
						ShadowSpill: true,
					}
					_ = fs.journal.Append(entry)
					_ = fs.journal.FsyncShared()
				}
				if fs.commitQueue != nil {
					phase = "interactive-shadowspill-enqueue"
					if err := fs.enqueueStagedShadowCommitLocked(fh); err != nil {
						safeLogPrintf("fsync: enqueue staged ShadowSpill commit failed for %s: %v; deferring to Release", fh.Path, err)
						fh.ShadowCommitReady = true
						fh.ShadowCommitSeq = fh.DirtySeq
					}
				} else {
					fh.ShadowCommitReady = true
					fh.ShadowCommitSeq = fh.DirtySeq
				}
				return gofuse.OK
			}
		} else {
			phase = "interactive-stage"
			stageStart := time.Now()
			err := fs.stageShadowForQueuedCommitLocked(fh, true)
			if errors.Is(err, syscall.EAGAIN) {
				return gofuse.EAGAIN
			}
			fs.debugDurationf(stageStart, 0, "fsync stage done path=%s err=%v", fh.Path, err)
			if err == nil {
				if fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() {
					phase = "superseded-after-commit-fence"
					return gofuse.OK
				}
				// Journal before enqueue: the async commit appends a
				// JournalCommit marker, which must get a higher Seq than
				// this fsync frame or replay resurrects a committed path.
				if fs.journal != nil {
					entry := JournalEntry{
						Op:      JournalFsync,
						Path:    fh.Path,
						Length:  fh.Dirty.Size(),
						BaseRev: fh.BaseRev,
					}
					_ = fs.journal.Append(entry)
					_ = fs.journal.FsyncShared()
				}
				if fs.commitQueue != nil && fs.shadowStore != nil && fs.shadowStore.Has(fh.Path) {
					phase = "interactive-enqueue"
					if err := fs.enqueueStagedShadowCommitLocked(fh); err != nil {
						fs.releaseHandleRemoteCommitPathLocked(fh)
						safeLogPrintf("fsync: enqueue staged commit failed for %s: %v", fh.Path, err)
						return gofuse.EIO
					}
				} else {
					fs.releaseHandleRemoteCommitPathLocked(fh)
					if err := fs.snapshotWriteBackLocked(fh, true); err != nil {
						safeLogPrintf("fsync writeback snapshot failed for %s: %v", fh.Path, err)
					} else {
						// See the small-snapshot-writeback path: only claim a
						// current cache when the snapshot actually landed.
						fh.WriteBackSeq = fh.DirtySeq
					}
				}
				return gofuse.OK
			}
		}
	}

	if handled, st := fs.commitAppendSnapshotLocked(ctx, fh); handled {
		return st
	}

	// ShadowSpill strict: synchronous streaming upload from shadow.
	if fh.ShadowSpill && fs.shadowStore != nil {
		size := fh.Dirty.Size()
		uploadCtx, uploadCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
		defer uploadCancel()
		if fs.layerEnabled() {
			phase = "shadowspill-layer-sync-upload"
			uploadStart := time.Now()
			fs.debugf("fsync layer shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
			err := fs.commitLayerShadowLocked(uploadCtx, fh, true, false)
			fs.debugDurationf(uploadStart, 0, "fsync layer shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
			if err != nil {
				safeLogPrintf("fsync: layer ShadowSpill sync upload failed for %s: %v", fh.Path, err)
				return httpToFuseStatus(err)
			}
			return gofuse.OK
		}
		phase = "shadowspill-sync-upload"
		gvisorCompat := fs.gvisorCompatibilityEnabled()
		handleIsNew := fh.IsNew
		handlePath := fh.Path
		stagingGens := fs.captureHandleStagingGensLocked(fh)
		handleIno := fh.Ino
		// B4: serialize with Unlink's remote DELETE — hold the per-path
		// remoteCommitLock across the network upload while releasing fh.mu,
		// exactly like flushHandle Path 2. Without it Unlink can DELETE and
		// return while this large-file PUT is still in flight.
		unlockRemoteCommit := fs.takeHandleRemoteCommitPathLocked(fh)
		if fs.discardSupersededMutationLocked(fh) {
			fs.removeHandleOwnedStagingLocked(fh)
			unlockRemoteCommit()
			phase = "superseded-after-commit-fence"
			return gofuse.OK
		}
		expectedRevision := fs.expectedRevisionForHandleLocked(fh)
		mutationSeq := fh.DirtySeq
		uploadStart := time.Now()
		fs.debugf("fsync shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
		fh.Unlock()
		committedRev, err := uploadFromShadowRemote(uploadCtx, fs.client, fs.shadowStore, handlePath, fs.remotePath(handlePath), expectedRevision, stagingGens.ShadowGen, shadowUploadLocalDurable)
		// Record the committed revision while still holding remoteCommitLock
		// so a waiting Unlink re-check observes the upload and issues a DELETE.
		if err == nil && committedRev > 0 {
			if handleIsNew {
				fs.replaceCommittedRevision(handlePath, committedRev)
			} else {
				fs.recordCommittedRevision(handlePath, committedRev)
			}
		}
		if err == nil && committedRev == 0 {
			if syntheticRev, ok := committedRevisionFromExpectedRevision(expectedRevision); ok && syntheticRev > 0 {
				fs.recordCommittedRevision(handlePath, syntheticRev)
			}
		}
		if err == nil {
			mutationRevision := fs.resolveCommittedMutationRevision(handlePath, committedRev, expectedRevision)
			fs.recordCommittedMutation(handleIno, mutationSeq, mutationRevision, size)
			fs.captureFtruncateIdentity(ctx, handleIno, handlePath, mutationRevision)
			if mutationRevision > 0 {
				fs.refreshCommittedRevisionForOpenHandlesWithSize(handlePath, mutationRevision, fh, size)
			}
		}
		unlockRemoteCommit()
		fh.Lock()
		var uploadBytes uint64
		if size > 0 {
			uploadBytes = uint64(size)
		}
		fs.perfRecordRemote(perfRemoteWrite, uploadStart, err, uploadBytes)
		fs.debugDurationf(uploadStart, 0, "fsync shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
		if err != nil {
			safeLogPrintf("fsync: ShadowSpill sync upload failed for %s: %v", fh.Path, err)
			return gofuse.EIO
		}
		// If Unlink completed while remoteCommitLock was released (it
		// serializes DELETE after our PUT via the same lock), the path is
		// already deleted. Cancel path-keyed staging and do not re-publish
		// handle committed state. Keep Dirty for anonymous-fd reads.
		if fh.Unlinked {
			phase = "unlinked-after-shadowspill-upload"
			fs.cancelUnlinkedRemotePublishLocked(fh)
			return gofuse.OK
		}
		if gvisorCompat && (fh.Path != handlePath || fh.DirtySeq != mutationSeq) {
			return gofuse.OK
		}
		if err := fs.applyPendingModeWithTimeoutLocked(fh); err != nil {
			safeLogPrintf("fsync: ShadowSpill pending chmod failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}
		if gvisorCompat && (fh.Path != handlePath || fh.DirtySeq != mutationSeq) {
			return gofuse.OK
		}
		if fh.Dirty != nil {
			fh.Dirty.ClearDirty()
			if gvisorCompat {
				fs.clearDirtySize(handleIno, mutationSeq)
			} else {
				fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			}
			fh.DirtySeq = 0
		}
		if committedRev > 0 {
			clearReadTargetForLockedHandle(fh)
			if fs.seedReadCacheFromShadowGenerationLocked(fh.Path, size, committedRev, stagingGens.ShadowGen) {
				fs.clearReadTargetsForPathExcept(fh.Path, fh)
			} else {
				fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
			}
			fs.markHandleRemoteCommittedLocked(fh, committedRev)
			fs.removeShadowPendingStagingGenerationLocked(fh, fh.Path, stagingGens.ShadowGen, stagingGens.PendingIndexGen)
			fs.cacheFileForPath(fh.Path, size, time.Now(), committedRev)
		} else {
			clearReadTargetForLockedHandle(fh)
			fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
			fs.finalizeHandleFlushLocked(fh, expectedRevision)
			fs.cacheFileForPath(fh.Path, size, time.Now(), 0)
			fs.removeShadowPendingStagingGenerationLocked(fh, fh.Path, stagingGens.ShadowGen, stagingGens.PendingIndexGen)
		}
		fs.inodes.UpdateSize(fh.Ino, size)
		return gofuse.OK
	}

	// Strict mode: Fsync = remote durable. Upload to server before returning.
	if fs.writeBack != nil && fs.uploader != nil && fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq {
		// Snapshot matches current dirty state — safe to upload.
		phase = "writeback-upload-sync"
		size := int64(0)
		if fh.Dirty != nil {
			size = fh.Dirty.Size()
		} else if meta, ok := fs.writeBack.GetMeta(fh.Path); ok && meta != nil {
			size = meta.Size
		}
		if fs.layerEnabled() {
			if fs.commitQueue == nil || fs.shadowStore == nil || !fs.shadowStore.Has(fh.Path) {
				safeLogPrintf("fsync layer upload failed for %s: missing commit queue shadow", fh.Path)
				return gofuse.EIO
			}
			mode, hasMode := fs.modeForPendingHandle(fh)
			expectedRevision := fs.expectedRevisionForHandleLocked(fh)
			payloadBaseRev := fh.BaseRev
			entry := &CommitEntry{
				Path:        fh.Path,
				Inode:       fh.Ino,
				MutationSeq: fh.DirtySeq,
				BaseRev:     expectedRevision,
				Size:        size,
				Kind:        fs.pendingKindForHandle(fh),
				Mode:        mode,
				HasMode:     hasMode,
			}
			fs.bindCommitEntryToHandleLocked(entry, fh, payloadBaseRev)
			uploadStart := time.Now()
			fs.debugf("fsync layer upload start path=%s", fh.Path)
			err := fs.commitQueue.CommitNow(ctx, entry)
			fs.debugDurationf(uploadStart, 0, "fsync layer upload done path=%s err=%v", fh.Path, err)
			if err != nil {
				safeLogPrintf("fsync layer upload failed for %s: %v", fh.Path, err)
				return httpToFuseStatus(err)
			}
			if hasMode {
				fs.clearPendingModeForInodeGeneration(fh.Ino, fh, mode&0o777, fh.PendingModeGen)
				clearPendingModeLocked(fh)
			}
			if fh.Dirty != nil {
				fh.Dirty.ClearDirty()
				fs.clearDirtySize(fh.Ino, fh.DirtySeq)
				fh.DirtySeq = 0
			}
			// The handle is now clean. Future sibling-revision adoption will rebind
			// the writable buffer to the adopted revision before any new write can
			// use the updated BaseRev.
			fh.WriteBackSeq = 0
			return gofuse.OK
		}

		expectedRevision := fs.expectedRevisionForHandleLocked(fh)
		mutationSeq := fh.DirtySeq
		uploadStart := time.Now()
		fs.debugf("fsync writeback upload start path=%s", fh.Path)
		committedRev, err := fs.uploader.UploadSyncWithRevision(ctx, fh.Path)
		fs.debugDurationf(uploadStart, 0, "fsync writeback upload done path=%s err=%v", fh.Path, err)
		if err != nil {
			safeLogPrintf("fsync writeback upload failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}
		mutationRevision := fs.resolveCommittedMutationRevision(fh.Path, committedRev, expectedRevision)
		fs.recordCommittedMutation(fh.Ino, mutationSeq, mutationRevision, size)
		fs.captureFtruncateIdentity(ctx, fh.Ino, fh.Path, mutationRevision)
		if mutationRevision > 0 {
			fs.refreshCommittedRevisionForOpenHandlesWithSize(fh.Path, mutationRevision, fh, size)
		}
		if fh.HasPendingMode {
			ino := fh.Ino
			mode := fh.PendingMode & posixPermissionModeMask
			modeGen := fh.PendingModeGen
			clearPendingModeLocked(fh)
			fh.Unlock()
			fs.clearPendingModeForInodeGeneration(ino, fh, mode, modeGen)
			fh.Lock()
		}
		// UploadSync already persisted the data to the server. Clear
		// the dirty state so the subsequent flushHandleDebounced sees
		// !HasDirtyParts() and skips the redundant upload.
		if fh.Dirty != nil {
			fh.Dirty.ClearDirty()
			fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			fh.DirtySeq = 0
			fh.WriteBackSeq = 0
		}
		// The handle is now clean. Future sibling-revision adoption will rebind
		// the writable buffer to the adopted revision before any new write can
		// use the updated BaseRev.
		if committedRev > 0 {
			fs.markHandleRemoteCommittedLocked(fh, committedRev)
		} else {
			fs.finalizeHandleFlushLocked(fh, expectedRevision)
		}
		fs.inodes.UpdateSize(fh.Ino, size)
		fs.cacheFileForPath(fh.Path, size, time.Now(), committedRev)
	} else if fs.writeBack != nil && fh.WriteBackSeq != 0 && fh.WriteBackSeq != fh.DirtySeq {
		// Snapshot is stale — discard it so we don't upload old data.
		phase = "writeback-stale"
		if fh.WriteBackGen != 0 {
			fs.writeBack.RemoveIfGeneration(fh.Path, fh.WriteBackGen)
		}
		fh.WriteBackGen = 0
		fh.WriteBackSeq = 0
	}

	phase = "flush-debounced-force"
	return fs.flushHandleDebounced(ctx, fh, true)
}
