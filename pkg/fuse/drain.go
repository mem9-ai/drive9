package fuse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

type drainError struct {
	kind string
	path string
	err  error
}

const drainPendingWorkKind = "pending_work_remaining"

func (e *drainError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *drainError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func newDrainContextError(err error) *drainError {
	if errors.Is(err, context.DeadlineExceeded) {
		return &drainError{kind: "timeout", err: err}
	}
	if errors.Is(err, context.Canceled) {
		return &drainError{kind: "canceled", err: err}
	}
	return &drainError{kind: "error", err: err}
}

func newDrainStatusError(path string, status gofuse.Status) *drainError {
	kind := "fuse_status"
	if status == gofuse.Status(syscall.EAGAIN) {
		kind = "remote_timeout_or_retryable"
	}
	return &drainError{
		kind: kind,
		path: path,
		err:  fmt.Errorf("flush returned FUSE status %d", status),
	}
}

func (fs *Dat9FS) Drain(ctx context.Context) mountcontrol.DrainResponse {
	fs.drainMu.Lock()
	defer fs.drainMu.Unlock()

	startedAt := time.Now().UTC()
	resp := mountcontrol.NewDrainResponse(fs.mountPointForDrain(), startedAt)
	fs.populateNativeSyncFSSupport(&resp)
	runPhase := func(name string, fn func() error) bool {
		phaseStart := time.Now()
		err := fn()
		phase := mountcontrol.DrainPhase{
			Name:       name,
			DurationMS: time.Since(phaseStart).Milliseconds(),
		}
		if err != nil {
			phase.Error = err.Error()
		}
		resp.Phases = append(resp.Phases, phase)
		if err == nil {
			return true
		}
		var dErr *drainError
		if errors.As(err, &dErr) {
			resp.Fail(dErr.kind, dErr.path, dErr.err)
		} else {
			resp.Fail("error", "", err)
		}
		return false
	}

	if !runPhase("flush_debounced", func() error {
		if err := ctx.Err(); err != nil {
			return newDrainContextError(err)
		}
		if fs.debouncer != nil {
			fs.debouncer.FlushAll()
		}
		return nil
	}) {
		resp.Pending = fs.snapshotDrainPending()
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("flush_open_handles", func() error {
		return fs.drainOpenHandles(ctx)
	}) {
		resp.Pending = fs.snapshotDrainPending()
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("flush_extent", func() error {
		if err := ctx.Err(); err != nil {
			return newDrainContextError(err)
		}
		v := fs.extentVFS()
		if v == nil {
			return nil
		}
		if err := v.FlushAll(""); err != nil {
			return err
		}
		return nil
	}) {
		resp.Pending = fs.snapshotDrainPending()
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("git_overlay", func() error {
		if err := fs.waitGitOverlayWrites(ctx); err != nil {
			return err
		}
		fs.syncGitDirtyMirrors()
		if err := fs.waitGitOverlayWrites(ctx); err != nil {
			return err
		}
		fs.drainGitStateCheckpoints()
		fs.checkpointAllGitWorkspaces()
		if err := ctx.Err(); err != nil {
			return newDrainContextError(err)
		}
		return nil
	}) {
		resp.Pending = fs.snapshotDrainPending()
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("writeback_uploader", func() error {
		if fs.uploader == nil {
			return nil
		}
		if err := fs.uploader.WaitIdle(ctx); err != nil {
			snap := fs.uploader.Snapshot()
			return &drainError{kind: drainContextKind(err), path: snap.FirstPath, err: err}
		}
		return nil
	}) {
		resp.Pending = fs.snapshotDrainPending()
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("commit_queue", func() error {
		if fs.commitQueue == nil {
			return nil
		}
		if err := fs.commitQueue.WaitIdle(ctx); err != nil {
			snap := fs.commitQueue.Snapshot()
			return &drainError{kind: drainContextKind(err), path: snap.FirstPath, err: err}
		}
		return nil
	}) {
		resp.Pending = fs.snapshotDrainPending()
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("notifications", func() error {
		return waitGroupWithContext(ctx, &fs.notifyWg)
	}) {
		resp.Pending = fs.snapshotDrainPending()
		resp.Finish(time.Now().UTC())
		return resp
	}

	// Status/error reporting is nonblocking. At the successful Drain boundary,
	// however, an ordinary close still finishing cleanup is not dirty work:
	// wait within the original context until those uncertain snapshots settle.
	var uncertain int
	resp.Pending, uncertain = fs.snapshotDrainPendingState()
	for uncertain != 0 && resp.Pending.DirtyHandles == uncertain {
		if err := waitHandleRetry(ctx, time.Time{}); err != nil {
			resp.Fail(drainContextKind(err), "", err)
			resp.Finish(time.Now().UTC())
			return resp
		}
		resp.Pending, uncertain = fs.snapshotDrainPendingState()
	}
	if err := drainPendingWorkError(resp.Pending); err != nil {
		resp.Fail(drainPendingWorkKind, "", err)
	}
	resp.Finish(time.Now().UTC())
	return resp
}

func (fs *Dat9FS) populateNativeSyncFSSupport(resp *mountcontrol.DrainResponse) {
	if fs == nil || resp == nil || fs.server == nil {
		return
	}
	settings := fs.server.KernelSettings()
	if settings != nil {
		resp.FUSEProtocolMajor = settings.Major
		resp.FUSEProtocolMinor = settings.Minor
	}
	// Linux kernels can advertise protocol support for FUSE_SYNCFS without
	// dispatching syncfs(2) to userspace for this mount. Treat native syncfs as
	// unsupported until the daemon observes a real SyncFs request; otherwise the
	// control-plane drain response would falsely promise that `sync -f` flushes
	// open Drive9 handles.
	resp.NativeSyncFSSupported = fs.nativeSyncFSObserved()
}

func (fs *Dat9FS) nativeSyncFSObserved() bool {
	return fs != nil && fs.nativeSyncFSSeen.Load()
}

func (fs *Dat9FS) mountPointForDrain() string {
	if fs != nil && fs.opts != nil {
		return fs.opts.MountPoint
	}
	return ""
}

func drainContextKind(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "error"
}

func (fs *Dat9FS) drainOpenHandles(ctx context.Context) error {
	if fs == nil || fs.fileHandles == nil {
		return nil
	}
	handles := make(map[uint64]*FileHandle)
	fs.fileHandles.ForEach(func(id uint64, fh *FileHandle) { handles[id] = fh })
	for id, fh := range handles {
		if err := ctx.Err(); err != nil {
			return newDrainContextError(err)
		}
		if st := fs.drainFileHandle(ctx, id, fh, time.Time{}); st != gofuse.OK {
			path, _ := fs.openHandles.Path(fh)
			return newDrainStatusError(path, st)
		}
	}
	return nil
}

// Claim before mu for both live and deferred handles. Release may publish a
// deferred ID during an upload; recheck it after the helper gives back control.
func (fs *Dat9FS) drainFileHandle(ctx context.Context, id uint64, fh *FileHandle, shutdownStart time.Time) gofuse.Status {
	if current, ok := fs.fileHandles.Get(id); !ok || current != fh {
		return gofuse.OK
	}
	for !fh.releaseClaim.CompareAndSwap(false, true) {
		if current, ok := fs.fileHandles.Get(id); !ok || current != fh {
			return gofuse.OK
		}
		if err := waitHandleRetry(ctx, time.Time{}); err != nil {
			return httpToFuseStatus(err)
		}
	}
	claimed := true
	defer func() {
		if claimed {
			fh.releaseClaim.Store(false)
		}
	}()
	if current, ok := fs.fileHandles.Get(id); !ok || current != fh {
		return gofuse.OK
	}
	deadline, _ := ctx.Deadline()
	state := &handleLockState{deadline: deadline}
	if err := state.lock(ctx, fh); err != nil {
		return httpToFuseStatus(err)
	}
	defer func() {
		if state.held {
			fh.Unlock()
		}
	}()
	if current, ok := fs.fileHandles.Get(id); !ok || current != fh {
		return gofuse.OK
	}
	if !shutdownStart.IsZero() {
		var size int64
		if fh.Dirty != nil {
			size = fh.Dirty.Size()
		}
		state.deadline = shutdownStart.Add(releaseTimeout(size))
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(context.Background(), state.deadline)
		defer cancel()
	}
	status := gofuse.OK
	if fh.deferredRelease.Load() == 0 {
		// Entry is bounded for every handle, including a closing transition.
		// Keep legacy upload finalization for unrelated files; only truncate
		// participants have the exact receipt needed for an unlocked return.
		state.truncateOnly = true
		status = fs.drainHandleLocked(ctx, fh, state)
	}
	if state.held {
		unlockHandleForIO(fh, []*handleLockState{state})
	}
	if fh.deferredRelease.Load() != 0 {
		claimed = false
		fh.releaseClaim.Store(false)
		status = fs.releaseFileHandle(ctx, nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: id}, true)
		if current, ok := fs.fileHandles.Get(id); ok && current == fh && status == gofuse.OK {
			return gofuse.EAGAIN
		}
	}
	return status
}

func (fs *Dat9FS) drainHandleLocked(ctx context.Context, fh *FileHandle, lockState ...*handleLockState) gofuse.Status {
	if isLocalFileHandle(fh) {
		if localFileHandleOpenedWritable(fh) {
			if err := syncOpenLocalFile(fh.LocalFile); err != nil {
				return localErrToFuseStatus(err)
			}
		}
		if info, err := fh.LocalFile.Stat(); err == nil {
			fs.inodes.UpdateSize(fh.Ino, info.Size())
			fs.inodes.UpdateMtime(fh.Ino, info.ModTime())
		}
		if localPathShouldCheckpointGitState(fh.Path) && localFileHandleOpenedWritable(fh) {
			if err := fs.checkpointGitStateAfterLocalWrite(ctx, fh.Path, true); err != nil {
				return httpToFuseStatus(err)
			}
		}
		return gofuse.OK
	}
	if fh != nil && fh.Layer == PathLayerGitWorkspace {
		return fs.flushGitHandleLockedWithPolicy(ctx, fh, true)
	}
	if fh.isExtent() {
		jctx := fs.jfsCtx(0, 0, 0)
		if st := fs.extentFlush(jctx, fh, 0); st != gofuse.OK {
			return st
		}
		return fs.extentFsync(jctx, fh, 0)
	}
	return fs.flushHandle(ctx, fh, lockState...)
}

func (fs *Dat9FS) waitGitOverlayWrites(ctx context.Context) error {
	if fs == nil {
		return nil
	}
	start := time.Now()
	if err := waitGroupWithContext(ctx, &fs.gitOverlayWG); err != nil {
		return newDrainContextError(err)
	}
	if fs.perfEnabled() {
		fs.perf.gitOverlayDrainCount.add(1)
		fs.perf.gitOverlayDrainTotalNS.add(uint64(time.Since(start)))
	}
	return nil
}

func waitGroupWithContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return newDrainContextError(ctx.Err())
	}
}

func (fs *Dat9FS) snapshotDrainPending() mountcontrol.DrainPending {
	pending, _ := fs.snapshotDrainPendingState()
	return pending
}

func (fs *Dat9FS) snapshotDrainPendingState() (pending mountcontrol.DrainPending, uncertain int) {
	if fs == nil {
		return
	}
	if fs.fileHandles != nil {
		handles := make(map[uint64]*FileHandle)
		fs.fileHandles.ForEach(func(id uint64, fh *FileHandle) { handles[id] = fh })
		for id, fh := range handles {
			if current, ok := fs.fileHandles.Get(id); !ok || current != fh {
				continue
			}
			pending.OpenHandles++
			if fh.deferredRelease.Load() != 0 {
				pending.DirtyHandles++
				continue
			}
			if fh.releaseClaim.Load() || !fh.TryLock() {
				pending.DirtyHandles++
				uncertain++
				continue
			}
			if drainHandleHasDirtyStateLocked(fh) || fh.deferredRelease.Load() != 0 || fh.releaseClaim.Load() {
				pending.DirtyHandles++
			}
			fh.Unlock()
		}
	}
	if fs.commitQueue != nil {
		snap := fs.commitQueue.Snapshot()
		pending.CommitQueuePending = snap.Pending
		pending.CommitQueueBytes = snap.Bytes
		pending.CommitQueueInFlight = snap.InFlight
		pending.CommitQueueDelayed = snap.Delayed
		pending.CommitQueueConflicts = snap.Conflicts
	}
	if fs.uploader != nil {
		snap := fs.uploader.Snapshot()
		pending.UploaderQueued = snap.Queued
		pending.UploaderInFlight = snap.InFlight
		pending.UploaderCached = snap.Cached
		pending.UploaderCachedBytes = snap.CachedBytes
	}
	return
}

func drainPendingWorkError(p mountcontrol.DrainPending) error {
	if !drainPendingHasUndrainedWork(p) {
		return nil
	}
	return fmt.Errorf(
		"pending work remains after drain: dirty_handles=%d commit_queue_pending=%d commit_queue_in_flight=%d commit_queue_delayed=%d commit_queue_conflicts=%d uploader_queued=%d uploader_in_flight=%d uploader_cached=%d",
		p.DirtyHandles,
		p.CommitQueuePending,
		p.CommitQueueInFlight,
		p.CommitQueueDelayed,
		p.CommitQueueConflicts,
		p.UploaderQueued,
		p.UploaderInFlight,
		p.UploaderCached,
	)
}

// drainPendingHasUndrainedWork is true when drain cannot report a clean pause.
// commit_queue_conflicts is included so writeback mounts still fail drain after
// an async commit terminal-failure. Close-sync/write-sync pending-new rename
// failures return EAGAIN to the caller and do not require drain to become
// visible; leftover pending is recovered on remount.
func drainPendingHasUndrainedWork(p mountcontrol.DrainPending) bool {
	return p.DirtyHandles != 0 ||
		p.CommitQueuePending != 0 ||
		p.CommitQueueInFlight != 0 ||
		p.CommitQueueDelayed != 0 ||
		p.CommitQueueConflicts != 0 ||
		p.UploaderQueued != 0 ||
		p.UploaderInFlight != 0 ||
		p.UploaderCached != 0
}

func drainHandleHasDirtyStateLocked(fh *FileHandle) bool {
	if fh == nil {
		return false
	}
	if fh.isExtent() {
		return false
	}
	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		return true
	}
	return fh.DirtySeq != 0 || fh.ShadowCommitReady || fh.HasPendingMode
}

func (fs *Dat9FS) SyncFs(cancel <-chan struct{}, header *gofuse.InHeader) gofuse.Status {
	perfStart := fs.perfStart()
	status := gofuse.OK
	defer func() { fs.perfRecordFuse(perfFuseSyncFs, perfStart, status, 0) }()
	fs.nativeSyncFSSeen.Store(true)

	ctx, cf := fuseCtx(cancel)
	defer cf()
	resp := fs.Drain(ctx)
	if resp.OK {
		return gofuse.OK
	}
	status = drainResponseStatus(resp)
	return status
}

func drainResponseStatus(resp mountcontrol.DrainResponse) gofuse.Status {
	switch resp.ErrorKind {
	case "timeout":
		return gofuse.Status(syscall.ETIMEDOUT)
	case "remote_timeout_or_retryable":
		return gofuse.Status(syscall.EAGAIN)
	case "canceled":
		return gofuse.Status(syscall.EINTR)
	default:
		return gofuse.EIO
	}
}
