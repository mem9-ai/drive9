package fuse

import (
	"errors"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func (fs *Dat9FS) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseFlush, perfStart, status, 0) }()
	fh, ok := fs.fileHandles.Get(input.Fh)
	if ok && fh.isExtent() {
		// Extent handles release POSIX locks inside JuiceFS VFS.Flush.
		return fs.extentFlush(fs.jfsCtx(input.Pid, input.Uid, input.Gid), fh, input.LockOwner)
	}
	if lockOwner := fuseLockOwner(input.LockOwner, input.Pid, input.Fh); lockOwner != 0 {
		fs.locks.release(input.NodeId, lockOwner)
	}
	if !ok {
		return gofuse.OK
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	fs.observePathPolicyWithContext(ctx, fh.Path)

	start := time.Now()
	phase := "start"
	fs.debugf("flush start path=%s fh=%d ino=%d", fh.Path, input.Fh, fh.Ino)
	lockStart := time.Now()
	if !fh.LockWithTimeout(flushLockTimeout) {
		fs.debugf("flush lock timeout path=%s fh=%d ino=%d wait=%s", fh.Path, input.Fh, fh.Ino, time.Since(lockStart))
		return gofuse.EIO
	}
	if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
		fs.debugf("flush lock wait path=%s fh=%d ino=%d wait=%s", fh.Path, input.Fh, fh.Ino, lockWait)
	}
	defer fh.Unlock()
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
		fs.debugf("flush done path=%s fh=%d ino=%d phase=%s size=%d dirty=%t shadow_ready=%t shadow_spill=%t status=%d dur=%s", fh.Path, input.Fh, fh.Ino, phase, size, dirty, fh.ShadowReady, fh.ShadowSpill, status, d)
	}()

	if isLocalFileHandle(fh) {
		gitState := localPathShouldCheckpointGitState(fh.Path)
		if gitState {
			phase = "local-git-sync"
			if err := syncOpenLocalFile(fh.LocalFile); err != nil {
				return localErrToFuseStatus(err)
			}
		} else {
			phase = "local-metadata"
		}
		if info, err := fh.LocalFile.Stat(); err == nil {
			fs.inodes.UpdateSize(fh.Ino, info.Size())
			fs.inodes.UpdateMtime(fh.Ino, info.ModTime())
		}
		if gitState && localFileHandleOpenedWritable(fh) {
			phase = "local-git-checkpoint"
			checkpointCtx, checkpointCancel := fs.syncDataCommitContext(cancel, gitCheckpointTimeout)
			defer checkpointCancel()
			if err := fs.checkpointGitStateAfterLocalWrite(checkpointCtx, fh.Path, fs.syncMode == SyncStrict); err != nil {
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

	// Unlink-while-open: never re-stage write-back or upload. Doing so
	// resurrects the path after Unlink's remote DELETE and makes parent
	// rmdir return ENOTEMPTY (pjdfstest unlink/14.t). Keep Dirty so
	// write-after-unlink remains readable if unlink later retries.
	if fh.Unlinked {
		phase = "unlinked-discard"
		fs.cancelUnlinkedRemotePublishLocked(fh)
		return gofuse.OK
	}
	if handled, err := fs.prepareFtruncateCommitLocked(ctx, fh, false); handled || err != nil {
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
		syncCtx, syncCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
		defer syncCancel()
		return fs.syncHandleToRemoteLocked(syncCtx, fh, shadowUploadRemoteDurable)
	}

	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() &&
		(fh.WritePolicy == WritePolicyCloseSync || fh.WritePolicy == WritePolicyWriteSync) {
		size := fh.Dirty.Size()
		syncCtx, syncCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
		defer syncCancel()
		phase = fh.WritePolicy.String()
		return fs.syncHandleToRemoteLocked(syncCtx, fh, shadowUploadRemoteDurable)
	}

	requiresRemoteSync := isSQLitePersistentJournalPath(fh.Path)

	// Write-back path: small dirty files are persisted to local disk
	// and return immediately. The actual HTTP upload happens in Release
	// (async). This reduces Flush latency from ~100-300ms to ~1-5ms.
	//
	// IMPORTANT: We do NOT ClearDirty here. The buffer stays dirty as a
	// safety net — if the user writes more data between Flush and Release,
	// Release will see HasDirtyParts() == true and fall through to the
	// synchronous flushHandle path, uploading the latest data. The cache
	// entry is just a snapshot for the async-upload fast path.
	if !requiresRemoteSync && fs.writeBack != nil && fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		// Same generation already cached — no new writes since last Flush.
		if fh.WriteBackSeq > 0 && fh.WriteBackSeq == fh.DirtySeq {
			phase = "writeback-same-seq"
			return gofuse.OK
		}
		size := fh.Dirty.Size()
		// Only use write-back for small files that haven't started streaming.
		// A Streamer may exist (Create always attaches one) but as long as
		// no parts have been streamed, the data is still fully in the WriteBuffer.
		hasActiveStream := fh.Streamer != nil && fh.Streamer.HasStreamedParts()
		if size < writeBackThreshold && !hasActiveStream {
			// Only stage locally when the shadow/buffer represents the full
			// current file contents. Otherwise a background full-file PUT would
			// silently zero untouched remote-backed ranges.
			if fs.canStageShadowFastLocked(fh) || fh.Dirty.CanMaterializeFull() {
				if fs.shadowStore != nil && fs.pendingIndex != nil {
					phase = "small-stage-shadow"
					stageStart := time.Now()
					fs.debugf("flush stage shadow start path=%s size=%d durable=true", fh.Path, size)
					err := fs.stageShadowForQueuedCommitLocked(fh, fs.stageDurableAtClose())
					if errors.Is(err, syscall.EAGAIN) {
						return gofuse.EAGAIN
					}
					stageDur := time.Since(stageStart)
					fs.debugDurationf(stageStart, 0, "flush stage shadow done path=%s size=%d err=%v", fh.Path, size, err)
					fs.perf.recordFlushStageShadow(stageDur)
					if err != nil {
						safeLogPrintf("shadow stage failed for %s: %v, falling through", fh.Path, err)
					} else {
						if fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() {
							phase = "superseded-after-commit-fence"
							return gofuse.OK
						}
						phase = "small-snapshot-writeback"
						snapWBStart := time.Now()
						// Advance WriteBackSeq only when the snapshot actually
						// landed: the sequence gates the writeBack cache as an
						// upload/read source, so claiming freshness after a
						// skipped/failed snapshot could serve a stale .dat as
						// current. On failure the next Flush/Release re-attempts
						// the snapshot or falls back to the synchronous flush —
						// the staged shadow + pendingIndex already carry the
						// durable commit path.
						if err := fs.snapshotWriteBackLocked(fh, false); err != nil {
							safeLogPrintf("writeback snapshot failed for %s: %v", fh.Path, err)
						} else {
							fh.WriteBackSeq = fh.DirtySeq
						}
						fs.perf.recordFlushSnapshotWB(time.Since(snapWBStart))
						return gofuse.OK
					}
				}

				phase = "small-snapshot-writeback"
				snapWBStart2 := time.Now()
				if err := fs.snapshotWriteBackWithPendingLocked(fh, true, true); err != nil {
					fs.perf.recordFlushSnapshotWB(time.Since(snapWBStart2))
					safeLogPrintf("writeback cache put failed for %s: %v, falling back to sync upload", fh.Path, err)
				} else {
					fs.perf.recordFlushSnapshotWB(time.Since(snapWBStart2))
					// Snapshot the dirty sequence at cache-write time so
					// Release can detect whether new writes happened since.
					fh.WriteBackSeq = fh.DirtySeq
					return gofuse.OK
				}
			}
		}
	}

	// Large file path. Returning OK here without persisting the file would
	// break close→drop_caches→open: the kernel re-issues Lookup, which falls
	// through to a remote stat that has not yet seen the upload, returning
	// ENOENT (juicefs bench reproduces this). Staging also registers the entry
	// in pendingIndex so subsequent Lookups hit the in-memory overlay.
	//
	// Two strategies, depending on the write policy — not the sync mode. The
	// fsync tier's durability contract is fsync(2), not close(2), exactly as
	// for the small-file path above (#964); close-sync/write-sync are the
	// tiers that must pay remote durability on close(2) itself:
	//
	//   • WriteBack: stage the buffer to the local shadow store + journal and
	//     let Release pick it up via the write-back cache fast path, which
	//     enqueues the actual server upload into the CommitQueue. close(2) is
	//     fast; remote durability is async and fsync(2) is the pay-up moment.
	//
	//   • CloseSync/WriteSync (or write-back fall-through on stage failure):
	//     block in Flush until the upload completes. Use a size-proportional
	//     timeout (releaseTimeout) instead of the 30s fuseCtx — large uploads
	//     need it.
	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() && fh.Dirty.Size() >= writeBackThreshold {
		// ShadowSpill stage path: stage shadow journal + set ShadowCommitReady.
		// Does NOT use snapshotWriteBackLocked or WriteBackSeq — those assume
		// writeBack cache holds complete file data, which ShadowSpill does not.
		if !requiresRemoteSync && fh.ShadowSpill && fs.stageLargeAtCloseEnabled(fh) && fs.shadowStore != nil && fs.pendingIndex != nil {
			phase = "large-shadowspill-stage"
			size := fh.Dirty.Size()
			stageStart := time.Now()
			fs.debugf("flush shadowspill stage start path=%s size=%d durable=true", fh.Path, size)
			err := fs.stageShadowForQueuedCommitLocked(fh, fs.stageDurableAtClose())
			if errors.Is(err, syscall.EAGAIN) {
				return gofuse.EAGAIN
			}
			largeStageDur := time.Since(stageStart)
			fs.debugDurationf(stageStart, 0, "flush shadowspill stage done path=%s size=%d err=%v", fh.Path, size, err)
			fs.perf.recordFlushStageShadow(largeStageDur)
			if err != nil {
				safeLogPrintf("flush: shadow stage failed for ShadowSpill %s (size=%d): %v, falling through to sync upload", fh.Path, fh.Dirty.Size(), err)
			} else {
				if fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() {
					phase = "superseded-after-commit-fence"
					return gofuse.OK
				}
				fh.ShadowCommitReady = true
				fh.ShadowCommitSeq = fh.DirtySeq
				return gofuse.OK
			}
		}

		// ShadowSpill fall-through (close-sync/write-sync, or a failed stage):
		// synchronous streaming upload from shadow.
		if fh.ShadowSpill {
			size := fh.Dirty.Size()
			uploadCtx, uploadCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
			defer uploadCancel()
			if fs.layerEnabled() {
				phase = "large-shadowspill-layer-sync-upload"
				uploadStart := time.Now()
				fs.debugf("flush layer shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
				err := fs.commitLayerShadowLocked(uploadCtx, fh, true, false)
				fs.debugDurationf(uploadStart, 0, "flush layer shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
				if err != nil {
					safeLogPrintf("flush: layer ShadowSpill sync upload failed for %s: %v", fh.Path, err)
					return httpToFuseStatus(err)
				}
				return gofuse.OK
			}
			phase = "large-shadowspill-sync-upload"
			gvisorCompat := fs.gvisorCompatibilityEnabled()
			expectedRevision := fs.expectedRevisionForHandleLocked(fh)
			mutationSeq := fh.DirtySeq
			handleIsNew := fh.IsNew
			handlePath := fh.Path
			handleIno := fh.Ino
			stagingGens := fs.captureHandleStagingGensLocked(fh)
			// B4: serialize with Unlink's remote DELETE — hold the per-path
			// remoteCommitLock across the network upload while releasing fh.mu,
			// exactly like flushHandle Path 2. Without it Unlink can DELETE and
			// return while this large-file PUT is still in flight.
			if gvisorCompat && testHookBeforeGVisorShadowSpillFlushFence != nil {
				testHookBeforeGVisorShadowSpillFlushFence(fh.Path)
			}
			unlockRemoteCommit := fs.takeHandleRemoteCommitPathLocked(fh)
			if gvisorCompat && fs.discardSupersededMutationLocked(fh) {
				fs.removeHandleOwnedStagingLocked(fh)
				unlockRemoteCommit()
				phase = "superseded-after-commit-fence"
				return gofuse.OK
			}
			if gvisorCompat {
				expectedRevision = fs.expectedRevisionForHandleLocked(fh)
				mutationSeq = fh.DirtySeq
				handleIsNew = fh.IsNew
				handlePath = fh.Path
				handleIno = fh.Ino
				stagingGens = fs.captureHandleStagingGensLocked(fh)
			}
			uploadStart := time.Now()
			fs.debugf("flush shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
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
			var mutationRevision int64
			if err == nil && (gvisorCompat || fs.openHandles.HasVisibleTruncate(handleIno)) {
				mutationRevision = fs.resolveCommittedMutationRevision(handlePath, committedRev, expectedRevision)
				fs.recordCommittedMutation(handleIno, mutationSeq, mutationRevision, size)
				if gvisorCompat && mutationRevision > 0 {
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
			fs.debugDurationf(uploadStart, 0, "flush shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
			if err != nil {
				safeLogPrintf("flush: ShadowSpill sync upload failed for %s: %v", fh.Path, err)
				if status, matched := quotaErrToFuseStatus(err); matched {
					return status
				}
				return gofuse.EIO
			}
			if !gvisorCompat {
				mutationRevision = fs.resolveCommittedMutationRevision(fh.Path, committedRev, expectedRevision)
				if mutationRevision > 0 {
					fs.refreshCommittedRevisionForOpenHandlesWithSize(fh.Path, mutationRevision, fh, size)
				}
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
				safeLogPrintf("flush: ShadowSpill pending chmod failed for %s: %v", fh.Path, err)
				return httpToFuseStatus(err)
			}
			if gvisorCompat && (fh.Path != handlePath || fh.DirtySeq != mutationSeq) {
				return gofuse.OK
			}
			fh.Dirty.ClearDirty()
			if gvisorCompat {
				fs.clearDirtySize(handleIno, mutationSeq)
			} else {
				fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			}
			fh.DirtySeq = 0
			if committedRev > 0 {
				clearReadTargetForLockedHandle(fh)
				if fs.seedReadCacheFromShadowGenerationLocked(fh.Path, size, committedRev, stagingGens.ShadowGen) {
					fs.clearReadTargetsForPathExcept(fh.Path, fh)
				} else {
					fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
				}
				fs.markHandleRemoteCommittedLocked(fh, committedRev)
				fs.removeShadowPendingStagingGenerationLocked(fh, fh.Path, stagingGens.ShadowGen, stagingGens.PendingIndexGen)
				fs.inodes.UpdateSize(fh.Ino, size)
				fs.cacheFileForPath(fh.Path, size, time.Now(), committedRev)
				return gofuse.OK
			}
			clearReadTargetForLockedHandle(fh)
			fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
			fs.inodes.UpdateSize(fh.Ino, size)
			fs.cacheFileForPath(fh.Path, size, time.Now(), 0)
			fs.finalizeHandleFlushLocked(fh, expectedRevision)
			fs.removeShadowPendingStagingGenerationLocked(fh, fh.Path, stagingGens.ShadowGen, stagingGens.PendingIndexGen)
			return gofuse.OK
		}

		if !requiresRemoteSync && fs.stageLargeAtCloseEnabled(fh) && fs.shadowStore != nil && fs.pendingIndex != nil {
			if fs.canStageShadowFastLocked(fh) || fh.Dirty.CanMaterializeFull() {
				phase = "large-stage-shadow"
				size := fh.Dirty.Size()
				stageStart := time.Now()
				fs.debugf("flush stage shadow start path=%s size=%d durable=true", fh.Path, size)
				err := fs.stageShadowForQueuedCommitLocked(fh, fs.stageDurableAtClose())
				if errors.Is(err, syscall.EAGAIN) {
					return gofuse.EAGAIN
				}
				fs.debugDurationf(stageStart, 0, "flush stage shadow done path=%s size=%d err=%v", fh.Path, size, err)
				if err != nil {
					safeLogPrintf("flush: shadow stage failed for %s (size=%d): %v, falling through to sync upload", fh.Path, fh.Dirty.Size(), err)
				} else {
					if fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() {
						phase = "superseded-after-commit-fence"
						return gofuse.OK
					}
					phase = "large-snapshot-writeback"
					if err := fs.snapshotWriteBackLocked(fh, false); err != nil {
						safeLogPrintf("flush: writeback snapshot failed for %s: %v", fh.Path, err)
					} else {
						// See the small-snapshot-writeback path: only claim a
						// current cache when the snapshot actually landed.
						fh.WriteBackSeq = fh.DirtySeq
					}
					// If a streaming upload was already in flight, abandon it:
					// the CommitQueue (driven by Release via the cache fast
					// path) will read from the shadow file instead. Without
					// this, Release sees streamerActive and falls through to
					// a synchronous re-upload, defeating the whole point.
					if fh.Streamer != nil && fh.Streamer.Started() {
						fh.Streamer.Abort()
						fh.Streamer = nil
					}
					return gofuse.OK
				}
			}
		}

		// Strict mode (or interactive fall-through): synchronous upload with
		// a size-aware timeout. Must NOT debounce — debounce returns OK and
		// uploads asynchronously, which would re-introduce the same bug.
		size := fh.Dirty.Size()
		flushCtx, flushCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
		defer flushCancel()
		phase = "large-sync-flush"
		fs.debugf("flush sync upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
		return fs.flushHandle(flushCtx, fh)
	}

	phase = "debounced-or-sync-flush"
	// flushHandleDebounced resolves synchronously for layer mounts, SQLite
	// persistent journals, buffers over the inline threshold, and disabled
	// debounce — production paths whose upload must survive a FUSE interrupt
	// like every other sync data commit. The deferred-callback path never
	// reads this context (the callback owns its lifecycle), so the
	// policy-aware context only changes the synchronous resolutions.
	debounceCtx, debounceCancel := fs.syncDataCommitContext(cancel, fuseTimeout)
	defer debounceCancel()
	return fs.flushHandleDebounced(debounceCtx, fh, false)
}
