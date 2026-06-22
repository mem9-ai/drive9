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

const fuseSyncFSProtocolMinor = 34
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

	resp.Pending = fs.snapshotDrainPending()
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
	if settings == nil {
		return
	}
	resp.FUSEProtocolMajor = settings.Major
	resp.FUSEProtocolMinor = settings.Minor
	resp.NativeSyncFSSupported = settings.SupportsVersion(7, fuseSyncFSProtocolMinor)
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
	handles := fs.fileHandles.Snapshot()
	for _, fh := range handles {
		if err := ctx.Err(); err != nil {
			return newDrainContextError(err)
		}
		fh.Lock()
		path := fh.Path
		status := fs.drainHandleLocked(ctx, fh)
		fh.Unlock()
		if status != gofuse.OK {
			return newDrainStatusError(path, status)
		}
	}
	return nil
}

func (fs *Dat9FS) drainHandleLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
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
	return fs.flushHandle(ctx, fh)
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
	var pending mountcontrol.DrainPending
	if fs == nil {
		return pending
	}
	if fs.fileHandles != nil {
		handles := fs.fileHandles.Snapshot()
		pending.OpenHandles = len(handles)
		for _, fh := range handles {
			fh.Lock()
			if drainHandleHasDirtyStateLocked(fh) {
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
	return pending
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
	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		return true
	}
	return fh.DirtySeq != 0 || fh.ShadowCommitReady || fh.HasPendingMode
}

func (fs *Dat9FS) SyncFs(cancel <-chan struct{}, header *gofuse.InHeader) gofuse.Status {
	perfStart := fs.perfStart()
	status := gofuse.OK
	defer func() { fs.perfRecordFuse(perfFuseSyncFs, perfStart, status, 0) }()

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
