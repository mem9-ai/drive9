package fuse

import (
	"context"
	"errors"
	"io"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// initialSyncRequest is owned by one foreground coordinator. Independent safe
// stages share its budget; workers only return immutable failure provenance.
type initialSyncRequest struct {
	retried bool
}

type lookupStageResult struct {
	out    gofuse.EntryOut
	status gofuse.Status
}

type attrStageResult struct {
	out    gofuse.AttrOut
	status gofuse.Status
}

type directoryStageResult struct {
	entries    []DirEntry
	generation uint64
}

type readStageResult struct {
	result gofuse.ReadResult
	status gofuse.Status
}

func readRecoveryDeadline(fresh bool, deadline time.Time) []time.Time {
	if fresh {
		return []time.Time{deadline}
	}
	return nil
}

// readRetainedSource reuses this FD's authoritative backing without redoing
// open/truncate/pin maintenance. It never substitutes a newer pathname object.
func (fs *Dat9FS) readRetainedSource(fh *FileHandle, input *gofuse.ReadIn) (gofuse.ReadResult, gofuse.Status, string, int, bool) {
	fh.Lock()
	preferDirty := fh.Dirty != nil && fh.Dirty.HasDirtyParts()
	if !preferDirty {
		if data, n, ok := readUnlinkedData(fh, int64(input.Offset), input.Size); ok {
			fh.Unlock()
			return gofuse.ReadResultData(data), gofuse.OK, "retained-snapshot", n, true
		}
	}
	// Clean writable buffers defer to the private unlinked shadow first.
	if preferDirty || !fh.Unlinked || fh.UnlinkedShadowGen == 0 {
		value, st, src, n, handled := fs.readHandleBufferLocked(fh, input)
		if handled {
			return value, st, src, n, true
		}
		fh.Lock()
	}
	if fh.Unlinked {
		if !preferDirty {
			if data, n, ok := readUnlinkedData(fh, int64(input.Offset), input.Size); ok {
				fh.Unlock()
				return gofuse.ReadResultData(data), gofuse.OK, "retained-snapshot", n, true
			}
		}
		gen, size := fh.UnlinkedShadowGen, fh.UnlinkedSize
		if int64(input.Offset) >= size {
			fh.Unlock()
			return gofuse.ReadResultData(nil), gofuse.OK, "retained-unlinked-eof", 0, true
		}
		if gen == 0 || fs.shadowStore == nil {
			fh.Unlock()
			return nil, gofuse.EIO, "retained-unlinked-missing", 0, true
		}
		end := min(size, int64(input.Offset)+int64(input.Size))
		data := make([]byte, end-int64(input.Offset))
		n, err := fs.shadowStore.ReadAtGen(gen, int64(input.Offset), data)
		fh.Unlock()
		if (err != nil && !errors.Is(err, io.EOF)) || n != len(data) {
			return nil, gofuse.EIO, "retained-shadow-error", n, true
		}
		return gofuse.ReadResultData(data), gofuse.OK, "retained-shadow", n, true
	}
	if fh.Dirty != nil {
		fh.Unlock()
		return nil, gofuse.OK, "", 0, false
	}
	path, policy, baseRev := fh.Path, fh.WritePolicy, fh.BaseRev
	fh.Unlock()
	if isSQLitePersistentJournalPath(path) {
		if data, n, ok, st, src := fs.readSQLitePersistentJournalVisibleRange(path, fh, baseRev, int64(input.Offset), input.Size); ok || st != gofuse.OK {
			return gofuse.ReadResultData(data), st, src, n, true
		}
	} else if policy == WritePolicyCloseSync || policy == WritePolicyWriteSync {
		if data, n, ok, st, src := fs.readSamePathDirtyHandleVisibleRange(path, fh, int64(input.Offset), input.Size); ok || st != gofuse.OK {
			return gofuse.ReadResultData(data), st, src, n, true
		}
	} else if isSQLiteVisibleSamePathDirtyPath(path) {
		if data, n, ok, st := fs.readSamePathDirtyHandle(path, fh, int64(input.Offset), input.Size); ok || st != gofuse.OK {
			return gofuse.ReadResultData(data), st, "same-path-dirty", n, true
		}
	}
	// Read the existing pin without refreshing or acquiring another one.
	fh.Lock()
	if gen := fh.ShadowGen; gen != 0 && fs.shadowStore != nil {
		if size := fs.shadowStore.SizeGen(gen); size >= 0 {
			offset := int64(input.Offset)
			end := min(size, offset+int64(input.Size))
			if offset >= size {
				fh.Unlock()
				return gofuse.ReadResultData(nil), gofuse.OK, "retained-shadow-eof", 0, true
			}
			data := make([]byte, end-offset)
			n, err := fs.shadowStore.ReadAtGen(gen, offset, data)
			fh.Unlock()
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, gofuse.EIO, "retained-shadow-error", n, true
			}
			return gofuse.ReadResultData(data[:n]), gofuse.OK, "retained-shadow", n, true
		}
	}
	fh.Unlock()
	if fs.writeBack != nil {
		if data, ok := fs.writeBack.getView(path); ok {
			offset := min(int64(input.Offset), int64(len(data)))
			end := min(offset+int64(input.Size), int64(len(data)))
			return gofuse.ReadResultData(data[offset:end]), gofuse.OK, "retained-writeback", int(end - offset), true
		}
	}
	return nil, gofuse.OK, "", 0, false
}

func initialSyncMetadataStatus(err error) gofuse.Status {
	if errors.Is(err, context.Canceled) {
		return gofuse.EINTR
	}
	return listDirErrToFuseStatus(err)
}

func recoveryDeadline(deadlines []time.Time) time.Time {
	if len(deadlines) != 0 {
		return deadlines[0]
	}
	return time.Time{}
}

func fuseCtxRecovery(cancel <-chan struct{}, deadline time.Time) (context.Context, context.CancelFunc) {
	ctx, cf := fuseCtx(cancel)
	if deadline.IsZero() {
		return ctx, cf
	}
	bounded, stop := context.WithDeadline(ctx, deadline)
	return bounded, func() { stop(); cf() }
}

type mountViewFailure struct {
	generation uint64
	initial    bool
}

func (e *mountViewFailure) Error() string { return errMountViewChanged.Error() }
func (e *mountViewFailure) Unwrap() error { return errMountViewChanged }

// mountViewCause accepts unary wrapping, but never treats a mixed error as an
// isolated view rejection that can hide a real HTTP, context or I/O failure.
func mountViewCause(err error) (*mountViewFailure, bool) {
	for err != nil {
		if _, mixed := err.(interface{ Unwrap() []error }); mixed {
			return nil, false
		}
		if cause, ok := err.(*mountViewFailure); ok {
			return cause, true
		}
		err = errors.Unwrap(err)
	}
	return nil, false
}

// lockMountViewReadCause hands the existing RLock to the caller on success.
// Rejection captures provenance before unlocking and owns no lock on return.
func (fs *Dat9FS) lockMountViewReadCause(generation uint64) error {
	fs.mountViewMu.RLock()
	current := fs.mountViewGeneration.Load()
	if generation == current {
		return nil
	}
	tag := fs.initialSyncResetGeneration.Load()
	err := &mountViewFailure{
		generation: generation,
		initial:    tag != 0 && current == tag && generation+1 == tag,
	}
	fs.mountViewMu.RUnlock()
	return err
}

func (fs *Dat9FS) mountViewFailureFrom(generation uint64) error {
	fs.mountViewMu.RLock()
	defer fs.mountViewMu.RUnlock()
	current := fs.mountViewGeneration.Load()
	tag := fs.initialSyncResetGeneration.Load()
	return &mountViewFailure{generation: generation, initial: tag != 0 && current == tag && generation+1 == tag}
}

// executeInitialSync is the only first-reset recovery loop. The safe once unit
// owns its existing fences and cleanup. Success may hand a publication lock to
// its caller, so never inspect the view or context again after success.
func executeInitialSync[T any](fs *Dat9FS, ctx context.Context, request *initialSyncRequest, generation uint64,
	once func(context.Context, bool) (T, error),
) (T, error) {
	fresh := false
	for {
		value, err := once(ctx, fresh)
		if err == nil {
			return value, nil
		}
		cause, view := mountViewCause(err)
		if !view {
			return value, err
		}
		var zero T
		if request.retried || !cause.initial || cause.generation != generation || !fs.initialSyncViewChangedFrom(generation) {
			return zero, err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, ctxErr
		}
		request.retried = true
		fresh = true
	}
}

func initialSyncDeadline(ctx context.Context, fresh bool) time.Time {
	if fresh {
		deadline, _ := ctx.Deadline()
		return deadline
	}
	return time.Time{}
}

// capInitialSyncTimeout keeps ordinary transport parents/timeouts unchanged.
// Only a fresh recovery supplies an absolute deadline for its detached work.
func capInitialSyncTimeout(parent context.Context, timeout time.Duration, deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return context.WithTimeout(parent, timeout)
	}
	end := time.Now().Add(timeout)
	if deadline.Before(end) {
		end = deadline
	}
	return context.WithDeadline(parent, end)
}

func (fs *Dat9FS) initialSyncFlightKey(key string, generation uint64) string {
	tag := fs.initialSyncResetGeneration.Load()
	if tag != 0 && generation >= tag {
		return key + "@initial-synced"
	}
	return key
}

func (fs *Dat9FS) directoryChildrenForMutation(ctx context.Context, request *initialSyncRequest, path string, layer bool) (bool, error) {
	generation := fs.mountViewGeneration.Load()
	return executeInitialSync(fs, ctx, request, generation, func(ctx context.Context, _ bool) (bool, error) {
		var children bool
		var source uint64
		if layer {
			source = fs.mountViewGeneration.Load()
			entries, err := fs.listDir(ctx, path)
			if err != nil {
				if _, local := fs.layerDirMode(path); !local || !isNotFoundErr(err) {
					return false, err
				}
			}
			children = len(entries) != 0
		} else {
			var err error
			children, source, err = fs.remoteDirectoryHasChildren(ctx, path)
			if err != nil {
				return children, err
			}
		}
		if err := fs.lockMountViewReadCause(source); err != nil {
			return false, err
		}
		fs.mountViewMu.RUnlock()
		return children, nil
	})
}

func (fs *Dat9FS) renameStatStage(ctx context.Context, path string, request *initialSyncRequest) (*client.StatResult, error) {
	generation := fs.mountViewGeneration.Load()
	return executeInitialSync(fs, ctx, request, generation, func(ctx context.Context, fresh bool) (*client.StatResult, error) {
		return fs.renameStatWithTransientRetry(ctx, path, initialSyncDeadline(ctx, fresh))
	})
}

func (fs *Dat9FS) renameProbeViewError(ctx context.Context, generation uint64, resultErr error) error {
	// Successful/absent candidates alone can establish a pure first rejection.
	// Runtime checks and real failures retain the legacy cancellation encoding.
	if (resultErr == nil || isNotFoundErr(resultErr)) && ctx.Err() == nil {
		if cause, ok := mountViewCause(fs.mountViewFailureFrom(generation)); ok && cause.initial {
			return cause
		}
	}
	return context.Canceled
}
