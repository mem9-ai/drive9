package fuse

import (
	"context"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func (fs *Dat9FS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	perfStart := fs.perfStart()
	releaseStatus := gofuse.OK
	defer func() { fs.perfRecordFuse(perfFuseRelease, perfStart, releaseStatus, 0) }()
	if lockOwner := fuseLockOwner(input.LockOwner, input.Pid, input.Fh); lockOwner != 0 {
		fs.locks.release(input.NodeId, lockOwner)
	}
	fh, ok := fs.fileHandles.Get(input.Fh)
	if ok {
		ctx, cf := fs.syncDataCommitContext(cancel, fuseTimeout)
		defer cf()
		fs.observePathPolicyWithContext(ctx, fh.Path)
		if fh.isExtent() {
			fs.extentRelease(fs.jfsCtx(input.Pid, input.Uid, input.Gid), fh)
			fs.deleteFileHandle(input.Fh, fh)
			return
		}
		flushStatus := gofuse.OK
		preservePendingModeOnReleaseFailure := false
		retryPendingModeAfterContentCommit := false
		defer func() {
			if fh.Prefetch != nil {
				fh.Prefetch.Close()
			}
			fs.deleteFileHandle(input.Fh, fh)
			fs.cleanupReleasedInode(fh.Ino, fh.Path)
		}()
		defer func() {
			fh.Lock()
			fs.releaseHandleRemoteCommitPathLocked(fh)
			fh.Unlock()
		}()
		// Apply any deferred chmod after flush completes but before cleanup.
		defer func() {
			fh.Lock()
			hasPendingMode := fh.HasPendingMode
			pendingMode := fh.PendingMode & posixPermissionModeMask
			pendingModeGen := fh.PendingModeGen
			previousMode := fh.PreviousMode
			hasPreviousMode := fh.HasPreviousMode
			previousModeKnown := fh.PreviousModeKnown
			ino := fh.Ino
			localPath := fh.Path
			layer := fh.Layer
			fh.Unlock()
			if !hasPendingMode {
				return
			}
			if layer == PathLayerGitWorkspace {
				if flushStatus != gofuse.OK {
					return
				}
				fh.Lock()
				stillCurrent := pendingModeMatchesLocked(fh, pendingMode, pendingModeGen)
				if stillCurrent {
					clearPendingModeLocked(fh)
				}
				fh.Unlock()
				if stillCurrent {
					fs.inodes.UpdateMode(ino, pendingMode)
					fs.clearPendingModeForInodeGeneration(ino, fh, pendingMode, pendingModeGen)
				}
				return
			}

			if flushStatus == gofuse.OK || retryPendingModeAfterContentCommit {
				// The pending chmod completes the Release content commit, so
				// it runs under the same interrupt-safe policy as the upload:
				// a FUSE interrupt that arrived during the detached upload
				// must not hand the chmod an already-canceled context and
				// drop the mode from an otherwise complete commit. The
				// non-layer route re-detaches inside
				// namespaceMutationCommitContext anyway; the layer route
				// (upsertLayerChmod) relies on this context being detached.
				modeCtx, modeCancel := fs.syncDataCommitContext(cancel, 30*time.Second)
				err := retryPostUploadMode(modeCtx, func() error {
					return fs.applyRemoteMode(modeCtx, localPath, pendingMode)
				})
				modeCancel()
				if err != nil {
					safeLogPrintf("release: pending chmod failed for %s: %v", localPath, err)
					fh.Lock()
					stillCurrent := pendingModeMatchesLocked(fh, pendingMode, pendingModeGen)
					fh.Unlock()
					if stillCurrent && hasPreviousMode {
						fs.inodes.SetModeState(ino, previousMode, previousModeKnown)
					}
					return
				}
				fh.Lock()
				stillCurrent := pendingModeMatchesLocked(fh, pendingMode, pendingModeGen)
				if stillCurrent {
					clearPendingModeLocked(fh)
				}
				fh.Unlock()
				if stillCurrent {
					fs.inodes.UpdateMode(ino, pendingMode)
					fs.clearPendingModeForInodeGeneration(ino, fh, pendingMode, pendingModeGen)
				}
				if retryPendingModeAfterContentCommit {
					flushStatus = gofuse.OK
					releaseStatus = gofuse.OK
				}
				return
			}

			// Flush failed — revert the in-memory mode so local GetAttr doesn't lie.
			if preservePendingModeOnReleaseFailure {
				return
			}
			fh.Lock()
			stillCurrent := pendingModeMatchesLocked(fh, pendingMode, pendingModeGen)
			if stillCurrent {
				clearPendingModeLocked(fh)
			}
			fh.Unlock()
			if stillCurrent && hasPreviousMode {
				fs.inodes.SetModeState(ino, previousMode, previousModeKnown)
			}
			if stillCurrent {
				fs.clearPendingModeForInodeGeneration(ino, fh, pendingMode, pendingModeGen)
			}
		}()

		// Look up the pin at deferred execution time. Generation reset can retire
		// and unpin it during Release; capturing the old token here would unpin it
		// a second time and invalidate another reader's retired snapshot.
		if fs.shadowStore != nil {
			defer fs.releaseHandleShadowPin(fh)
		}
		if fs.shadowStore != nil {
			fh.Lock()
			unlinkedShadowGen := fh.UnlinkedShadowGen
			fh.Unlock()
			if unlinkedShadowGen != 0 {
				defer fs.shadowStore.Unpin(unlinkedShadowGen)
			}
		}

		start := time.Now()
		phase := "start"
		defer func() { releaseStatus = flushStatus }()
		fs.debugf("release start path=%s fh=%d ino=%d", fh.Path, input.Fh, fh.Ino)
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
			if flushStatus == gofuse.OK && d < fuseDebugSlowOpThreshold {
				return
			}
			fs.debugf("release done path=%s fh=%d ino=%d phase=%s size=%d dirty=%t shadow_ready=%t shadow_spill=%t status=%d dur=%s", fh.Path, input.Fh, fh.Ino, phase, size, dirty, fh.ShadowReady, fh.ShadowSpill, flushStatus, d)
		}()

		if isLocalFileHandle(fh) {
			phase = "local-close"
			fh.Lock()
			localFile := fh.LocalFile
			openedWritable := localFileHandleOpenedWritable(fh)
			gitState := localPathShouldCheckpointGitState(fh.Path)
			localPath := fh.Path
			ino := fh.Ino
			fh.LocalFile = nil
			fh.Unlock()
			if localFile != nil {
				if gitState && openedWritable {
					phase = "local-git-sync-close"
					if err := syncOpenLocalFile(localFile); err != nil {
						flushStatus = localErrToFuseStatus(err)
					}
				}
				if info, err := localFile.Stat(); err == nil {
					fs.inodes.UpdateSize(ino, info.Size())
					fs.inodes.UpdateMtime(ino, info.ModTime())
				}
				if err := localFile.Close(); err != nil && flushStatus == gofuse.OK {
					flushStatus = localErrToFuseStatus(err)
				}
			}
			if flushStatus == gofuse.OK && gitState && openedWritable {
				phase = "local-git-checkpoint"
				checkpointCtx, checkpointCancel := fs.syncDataCommitContext(cancel, gitCheckpointTimeout)
				err := fs.checkpointGitStateAfterLocalWrite(checkpointCtx, localPath, fs.syncMode == SyncStrict)
				checkpointCancel()
				if err != nil {
					flushStatus = httpToFuseStatus(err)
				}
			}
			return
		}

		if fh.Layer == PathLayerGitWorkspace {
			phase = "git-overlay"
			lockStart := time.Now()
			fh.Lock()
			if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
				fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
			}
			var flushSize int64
			if fh.Dirty != nil {
				flushSize = fh.Dirty.Size()
			}
			flushCtx, flushCancel := fs.syncDataCommitContext(cancel, releaseTimeout(flushSize))
			flushStatus = fs.flushGitHandleLocked(flushCtx, fh)
			flushCancel()
			localFile := fh.LocalFile
			fh.LocalFile = nil
			fh.Unlock()
			if localFile != nil {
				if err := localFile.Close(); err != nil && flushStatus == gofuse.OK {
					flushStatus = localErrToFuseStatus(err)
				}
			}
			return
		}
		// Unlink-while-open: discard dirty state and skip all remote uploads.
		// Checked BEFORE the path-global debounce cancel: for an unlinked
		// handle the debounce entry (if any) is cancelled ownership-scoped
		// inside the discard helper, so a replacement file that reused the
		// pathname keeps its pending debounce.
		fh.Lock()
		fh.releasing = true
		if st := fs.layerHandleMutationStatusLocked(fh); st != gofuse.OK {
			flushStatus = st
			fh.Unlock()
			return
		}
		if fh.Unlinked {
			phase = "unlinked-discard"
			fs.discardUnlinkedHandleStateLocked(fh)
			fh.Unlock()
			return
		}
		if handled, err := fs.prepareFtruncateReleaseLocked(ctx, fh); handled || err != nil {
			if err != nil {
				flushStatus = httpToFuseStatus(err)
				releaseStatus = flushStatus
				safeLogPrintf("release: fd-truncate handoff failed for %s: %v", fh.Path, err)
			}
			fh.Unlock()
			return
		}
		if fs.ftruncateParticipates(fh) && fh.Dirty != nil && fh.Dirty.HasDirtyParts() &&
			fs.commitQueue != nil && fs.shadowStore != nil && fs.pendingIndex != nil &&
			fh.Dirty.Size() <= maxLandedPayloadBytes && fh.Dirty.CanMaterializeFull() {
			err := fs.stageShadowLocked(fh, true)
			if err == nil {
				if enqueueErr := fs.enqueueStagedShadowCommitLocked(fh); enqueueErr != nil {
					err = fs.retainFtruncateConflictLocked(fh, enqueueErr)
				}
			}
			if err != nil {
				flushStatus = httpToFuseStatus(err)
				releaseStatus = flushStatus
				safeLogPrintf("release: fd-truncate snapshot retained for %s: %v", fh.Path, err)
			}
			fh.Unlock()
			return
		}
		if fs.discardSupersededMutationLocked(fh) {
			phase = "superseded-mutation"
			fh.Unlock()
			return
		}
		if fs.appendLogConfiguredLocked(fh) {
			phase = "append-log-release-sync"
			releasePath := fh.Path
			size := int64(0)
			if fh.Dirty != nil {
				size = fh.Dirty.Size()
			}
			flushCtx, flushCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
			flushStatus = fs.syncHandleToRemoteLocked(flushCtx, fh, shadowUploadRemoteDurable)
			flushCancel()
			// The content finalizer may have succeeded before chmod failed.
			// Let the existing Release mode finalizer retry only the mode; on
			// failure it keeps the pending generation on live sibling handles.
			retryPendingModeAfterContentCommit = flushStatus != gofuse.OK &&
				fh.Path == releasePath && !fh.Unlinked && !fh.IsNew && fh.BaseRev > 0 &&
				fh.HasPendingMode && fh.DirtySeq == 0 && (fh.Dirty == nil || !fh.Dirty.HasDirtyParts())
			fh.Unlock()
			return
		}
		fh.Unlock()

		// Cancel any pending debounce for this path — Release always flushes immediately.
		phase = "cancel-debounce"
		fs.debouncer.CancelNoWait(fh.Path)

		// close-sync is primarily enforced in Flush so close(2) can receive
		// remote upload errors. Keep Release as a best-effort fallback for
		// unusual flows where dirty staged state reaches Release directly.
		if fh.WritePolicy == WritePolicyCloseSync || fh.WritePolicy == WritePolicyWriteSync {
			phase = "release-write-policy-sync"
			lockStart := time.Now()
			fh.Lock()
			if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
				fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
			}
			if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
				size := fh.Dirty.Size()
				flushCtx, flushCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
				flushStart := time.Now()
				fs.debugf("release write policy sync start path=%s size=%d policy=%s timeout=%s", fh.Path, size, fh.WritePolicy, releaseTimeout(size))
				flushStatus = fs.syncHandleToRemoteLocked(flushCtx, fh, shadowUploadRemoteDurable)
				fs.debugDurationf(flushStart, 0, "release write policy sync done path=%s size=%d status=%d", fh.Path, size, flushStatus)
				flushCancel()
			}
			fh.Unlock()
			if flushStatus != gofuse.OK {
				return
			}
		}

		// ShadowSpill Release: CommitQueue streaming from shadow, no writeBack.
		if !isSQLitePersistentJournalPath(fh.Path) && fh.ShadowSpill && fh.ShadowCommitReady && fh.ShadowCommitSeq == fh.DirtySeq && fs.commitQueue != nil && fs.shadowStore != nil {
			phase = "shadowspill-commit"
			lockStart := time.Now()
			fh.Lock()
			if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
				fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
			}
			// Unlink may have marked this handle between the earlier check
			// and this lock acquisition; discard instead of enqueueing —
			// the queued commit could land after Unlink's DELETE and
			// resurrect the path (B4 class).
			if fh.Unlinked {
				phase = "unlinked-discard"
				fs.discardUnlinkedHandleStateLocked(fh)
				fh.Unlock()
				return
			}
			unlockRemoteCommit := fs.takeHandleRemoteCommitPathLocked(fh)
			if fs.discardSupersededMutationLocked(fh) {
				fs.removeHandleOwnedStagingLocked(fh)
				unlockRemoteCommit()
				fh.Unlock()
				phase = "superseded-after-commit-fence"
				return
			}
			mutationSeq := fh.DirtySeq
			size := fh.Dirty.Size()
			mode, hasMode := fs.modeForPendingHandle(fh)
			expectedRevision := fs.expectedRevisionForHandleLocked(fh)
			payloadBaseRev := fh.BaseRev
			entry := &CommitEntry{
				Path:        fh.Path,
				Inode:       fh.Ino,
				MutationSeq: mutationSeq,
				BaseRev:     expectedRevision,
				Size:        size,
				Kind:        PendingOverwrite,
				ShadowSpill: true,
				Mode:        mode,
				HasMode:     hasMode,
			}
			if fh.IsNew {
				entry.Kind = PendingNew
			}
			fs.bindCommitEntryToHandleLocked(entry, fh, payloadBaseRev)
			fh.Dirty.ClearDirty()
			fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			fh.DirtySeq = 0
			fh.ShadowCommitReady = false
			fh.ShadowCommitSeq = 0
			fh.Unlock()

			enqueueStart := time.Now()
			fs.debugf("release commit enqueue start path=%s size=%d shadow_spill=true", fh.Path, size)
			err := fs.commitQueue.Enqueue(entry)
			fs.debugDurationf(enqueueStart, 0, "release commit enqueue done path=%s size=%d err=%v", fh.Path, size, err)
			fallbackCommittedRev := int64(0)
			if err != nil {
				// Fallback: synchronous streaming upload from shadow.
				// Do NOT use uploader.Submit — it reads from writeBack cache.
				safeLogPrintf("release: ShadowSpill commitQueue enqueue failed for %s: %v, falling back to sync upload", fh.Path, err)
				if fs.layerEnabled() {
					flushStatus = gofuse.EIO
					safeLogPrintf("release: layer mode preserves ShadowSpill pending state for %s after enqueue failure", fh.Path)
				} else {
					uploadCtx, uploadCancel := fs.syncDataCommitContext(cancel, releaseTimeout(size))
					phase = "shadowspill-sync-upload"
					uploadStart := time.Now()
					fs.debugf("release shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
					committedRev, uploadErr := uploadFromShadowRemote(uploadCtx, fs.client, fs.shadowStore, fh.Path, fs.remotePath(fh.Path), expectedRevision, entry.ShadowGen, shadowUploadLocalDurable)
					var uploadBytes uint64
					if size > 0 {
						uploadBytes = uint64(size)
					}
					fs.perfRecordRemote(perfRemoteWrite, uploadStart, uploadErr, uploadBytes)
					fs.debugDurationf(uploadStart, 0, "release shadowspill upload done path=%s size=%d err=%v", fh.Path, size, uploadErr)
					if uploadErr != nil {
						flushStatus = gofuse.EIO
						safeLogPrintf("release: ShadowSpill sync upload failed for %s: %v", fh.Path, uploadErr)
					} else {
						fallbackCommittedRev = committedRev
						fh.Lock()
						mutationRevision := fs.resolveCommittedMutationRevision(fh.Path, committedRev, expectedRevision)
						fs.recordCommittedMutation(fh.Ino, mutationSeq, mutationRevision, size)
						fs.captureFtruncateIdentity(ctx, fh.Ino, fh.Path, mutationRevision)
						if mutationRevision > 0 {
							fs.refreshCommittedRevisionForOpenHandlesWithSize(fh.Path, mutationRevision, fh, size)
						}
						if err := fs.applyPendingModeWithTimeoutLocked(fh); err != nil {
							flushStatus = httpToFuseStatus(err)
							preservePendingModeOnReleaseFailure = true
							safeLogPrintf("release: ShadowSpill pending chmod failed for %s after sync upload: %v", fh.Path, err)
						} else {
							clearReadTargetForLockedHandle(fh)
							if committedRev > 0 {
								if fs.seedReadCacheFromShadowGenerationLocked(fh.Path, size, committedRev, entry.ShadowGen) {
									fs.clearReadTargetsForPathExcept(fh.Path, fh)
								} else {
									fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
								}
								fs.markHandleRemoteCommittedLocked(fh, committedRev)
								fs.removeShadowPendingStagingGenerationLocked(fh, fh.Path, entry.ShadowGen, entry.PendingIndexGen)
							} else {
								fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
								fs.finalizeHandleFlushLocked(fh, expectedRevision)
								fs.removeShadowPendingStagingGenerationLocked(fh, fh.Path, entry.ShadowGen, entry.PendingIndexGen)
							}
							fs.inodes.UpdateSize(fh.Ino, size)
						}
						fh.Unlock()
					}
					uploadCancel()
				}
			} else if hasMode {
				fs.clearPendingModeForInode(fh.Ino)
			}
			unlockRemoteCommit()

			fs.invalidateReadCacheAndTargets(fh.Path)
			if flushStatus == gofuse.OK {
				fs.cacheFileForPath(fh.Path, size, time.Now(), fallbackCommittedRev)
			} else {
				fs.dirCache.Invalidate(parentDir(fh.Path))
			}
			// Local release — kernel already knows about this close.
			// No notifyInode needed; userspace caches are invalidated above.
			return
		}
		fh.Lock()
		if fh.ShadowCommitReady && fh.ShadowCommitSeq != 0 && fh.ShadowCommitSeq != fh.DirtySeq {
			fh.ShadowCommitReady = false
			fh.ShadowCommitSeq = 0
			fs.releaseHandleRemoteCommitPathLocked(fh)
		}
		fh.Unlock()

		// Check if Flush already wrote this file to the write-back cache
		// AND no new writes have happened since. If the DirtySeq changed,
		// the cache snapshot is stale — fall through to synchronous upload
		// which will upload the latest buffer data.
		if !isSQLitePersistentJournalPath(fh.Path) && fs.writeBack != nil && fs.uploader != nil {
			phase = "writeback-check"
			lockStart := time.Now()
			fh.Lock()
			if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
				fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
			}
			// If parts were submitted to the streaming uploader during Write,
			// they've been evicted from the WriteBuffer. The write-back /
			// commit-queue paths would miss those parts. Force the
			// synchronous flush path so FinishStreaming uploads the
			// buffered parts with the correct total size.
			streamerActive := fh.Streamer != nil && fh.Streamer.Started()
			canUseCache := !streamerActive && fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq
			fs.debugf("release writeback check path=%s streamer_active=%t writeback_seq=%d dirty_seq=%d can_use_cache=%t", fh.Path, streamerActive, fh.WriteBackSeq, fh.DirtySeq, canUseCache)
			if canUseCache {
				phase = "writeback-cache-release"
				useCommitQueue := fs.commitQueue != nil && fs.shadowStore != nil && fs.shadowStore.Has(fh.Path)
				var unlockRemoteCommit func()
				if useCommitQueue {
					unlockRemoteCommit = fs.takeHandleRemoteCommitPathLocked(fh)
					if fs.discardSupersededMutationLocked(fh) {
						fs.removeHandleOwnedStagingLocked(fh)
						unlockRemoteCommit()
						fh.Unlock()
						phase = "superseded-after-commit-fence"
						return
					}
				}
				mode, hasMode := fs.modeForPendingHandle(fh)
				expectedRevision := fs.expectedRevisionForHandleLocked(fh)
				mutationSeq := fh.DirtySeq
				commitSize := fh.Dirty.Size()
				payloadBaseRev := fh.BaseRev
				entry := &CommitEntry{
					Path:        fh.Path,
					Inode:       fh.Ino,
					MutationSeq: mutationSeq,
					BaseRev:     expectedRevision,
					Size:        commitSize,
					Kind:        PendingOverwrite,
					Mode:        mode,
					HasMode:     hasMode,
				}
				if fh.IsNew {
					entry.Kind = PendingNew
				}
				fs.bindCommitEntryToHandleLocked(entry, fh, payloadBaseRev)
				fh.Dirty.ClearDirty()
				fs.clearDirtySize(fh.Ino, fh.DirtySeq)
				fh.DirtySeq = 0
				fh.WriteBackSeq = 0
				fh.Unlock()

				// Enqueue to CommitQueue if available (P1), otherwise
				// use the legacy uploader.
				if useCommitQueue {
					enqueueStart := time.Now()
					fs.debugf("release commit enqueue start path=%s size=%d shadow_spill=false", fh.Path, entry.Size)
					err := fs.commitQueue.Enqueue(entry)
					fs.debugDurationf(enqueueStart, 0, "release commit enqueue done path=%s size=%d err=%v", fh.Path, entry.Size, err)
					if err != nil {
						if fs.layerEnabled() {
							flushStatus = gofuse.EIO
							safeLogPrintf("release: layer commitQueue enqueue failed for %s: %v", fh.Path, err)
							unlockRemoteCommit()
							return
						}
						if fs.gvisorCompatibilityEnabled() {
							uploadCtx, uploadCancel := context.WithTimeout(context.Background(), releaseTimeout(commitSize))
							commitErr := fs.commitQueue.commitNowPathLocked(uploadCtx, entry)
							uploadCancel()
							if commitErr != nil {
								flushStatus = httpToFuseStatus(commitErr)
								safeLogPrintf("release: synchronous sequence-preserving fallback failed for %s: %v", fh.Path, commitErr)
								unlockRemoteCommit()
								return
							}
							fs.writeBack.Remove(fh.Path)
						} else {
							// Preserve the legacy non-gVisor backpressure behavior.
							fs.debugf("release uploader submit fallback path=%s", fh.Path)
							fs.uploader.Submit(fh.Path)
						}
					} else {
						// CommitQueue owns the upload via shadow; remove the
						// writeBack .dat/.meta snapshot so it doesn't leak or
						// serve stale data to Lookup/Read.
						if entry.WriteBackGen != 0 {
							fs.writeBack.RemoveIfGeneration(fh.Path, entry.WriteBackGen)
						}
					}
					unlockRemoteCommit()
				} else {
					if fs.layerEnabled() {
						flushStatus = gofuse.EIO
						safeLogPrintf("release: layer mode requires commitQueue for %s", fh.Path)
						return
					}
					// Async upload — the uploader will read from cache and upload.
					fs.debugf("release uploader submit path=%s", fh.Path)
					fs.uploader.Submit(fh.Path)
				}
				if hasMode {
					fs.clearPendingModeForInode(fh.Ino)
				}

				// Invalidate caches so subsequent reads see fresh data.
				fs.invalidateReadCacheAndTargets(fh.Path)
				fs.cacheFileForPath(fh.Path, commitSize, time.Now(), 0)
				// Local release — kernel already knows about this close.
				// No notifyInode needed; userspace caches are invalidated above.
				return
			}
			// Stale cache — remove it, fall through to sync upload.
			if fh.WriteBackSeq != 0 {
				fs.debugf("release stale writeback remove path=%s writeback_seq=%d dirty_seq=%d", fh.Path, fh.WriteBackSeq, fh.DirtySeq)
				if fh.WriteBackGen != 0 {
					fs.writeBack.RemoveIfGeneration(fh.Path, fh.WriteBackGen)
				}
				fh.WriteBackGen = 0
				fh.WriteBackSeq = 0
			}
			fh.Unlock()
		}

		// Normal path: synchronous upload in Release.
		// Timeout scales with file size so large uploads don't get killed.
		phase = "sync-flush"
		lockStart := time.Now()
		fh.Lock()
		if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
			fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
		}
		var flushSize int64
		if fh.Dirty != nil {
			flushSize = fh.Dirty.Size()
		}
		flushCtx, flushCancel := fs.syncDataCommitContext(cancel, releaseTimeout(flushSize))
		flushStart := time.Now()
		fs.debugf("release sync flush start path=%s size=%d timeout=%s", fh.Path, flushSize, releaseTimeout(flushSize))
		st := fs.flushHandle(flushCtx, fh)
		fs.debugDurationf(flushStart, 0, "release sync flush done path=%s size=%d status=%d", fh.Path, flushSize, st)
		flushStatus = st
		flushCancel()
		streamer := fh.Streamer
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		fh.Unlock()

		if st != gofuse.OK && streamer != nil {
			// Flush failed — abort the streaming upload to avoid orphaned
			// multipart uploads on S3. Called without fh.mu because Abort()
			// may perform network I/O.
			streamer.Abort()
			safeLogPrintf("flush failed for %s (status %d), aborted stream upload", fh.Path, st)
		}

	}
}
