package extent

// This file owns the extent runtime's lifecycle: single-flight
// construction, the in-flight metadata gate, the compaction loop, and the
// teardown that stops it and releases the JuiceFS session.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/mem9-ai/drive9/pkg/extent"
)

// errExtentTornDown is returned when an extent operation races unmount: the
// runtime has been torn down and must not be re-created.
var errExtentTornDown = errors.New("extent runtime: mount is tearing down")

// extentMetaDrainTimeout bounds closeExtentRuntime's wait for in-flight
// metadata RPCs. Timeout leaves the runtime retained for a later retry.
const extentMetaDrainTimeout = extent.MetaCallTimeout + 5*time.Second

// extentMetaHold counts in-flight metadata RPCs so teardown can wait for them
// before closing the JuiceFS session.
type extentMetaHold struct {
	mu     sync.Mutex
	cond   *sync.Cond
	active int
	closed bool
}

func newExtentMetaHold() *extentMetaHold {
	h := &extentMetaHold{}
	h.cond = sync.NewCond(&h.mu)
	return h
}

// extentTeardownOp reports whether op is one of the metadata RPCs that
// CloseSession itself flushes after the gate closes.
func extentTeardownOp(op string) bool {
	switch op {
	case jfsmeta.Drive9OpCleanStaleSession, jfsmeta.Drive9OpDeleteSlice:
		return true
	default:
		return false
	}
}

func (h *extentMetaHold) enter(op string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed && !extentTeardownOp(op) {
		return false
	}
	h.active++
	return true
}

func (h *extentMetaHold) leave() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active > 0 {
		h.active--
	}
	if h.active == 0 {
		h.cond.Broadcast()
	}
}

func (h *extentMetaHold) closeAndWait(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return h.closeAndWaitContext(ctx) == nil
}

func (h *extentMetaHold) closeAndWaitContext(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	wake := context.AfterFunc(ctx, func() {
		h.mu.Lock()
		h.cond.Broadcast()
		h.mu.Unlock()
	})
	defer wake()
	for h.active > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		h.cond.Wait()
	}
	return nil
}

// extentBuildState single-flights the unlocked runtime build.
type extentBuildState struct {
	building bool
	cond     *sync.Cond
	gen      uint64
	err      error
}

type extentRuntime struct {
	rt   *extent.Runtime
	hold *extentMetaHold
	stop context.CancelFunc
	// wg tracks the background compaction loop.
	wg sync.WaitGroup
}

// Runtime exposes the live JuiceFS runtime (nil when not built).
func (e *Extent) Runtime() *extent.Runtime {
	if er := e.rt.Load(); er != nil {
		return er.rt
	}
	return nil
}

func (e *Extent) vfs() *vfs.VFS {
	er := e.rt.Load()
	if er == nil || er.rt == nil {
		return nil
	}
	return er.rt.VFS
}

// stopRuntimeLoop marks the runtime torn down and stops and joins its
// compaction loop.
func (e *Extent) stopRuntimeLoop() {
	e.extentMu.Lock()
	e.extentTornDown = true
	if e.extentBuild.cond != nil {
		// Wake single-flight waiters: a build that is still running can no
		// longer be installed, so they must observe the teardown.
		e.extentBuild.cond.Broadcast()
	}
	er := e.rt.Load()
	e.extentMu.Unlock()
	if er == nil {
		return
	}
	if er.stop != nil {
		er.stop()
	}
	// Join the compaction loop before touching the VFS: a compaction in
	// flight is uploading a merged blob and about to CAS it into meta, and
	// returning from unmount in between would leak that blob.
	er.wg.Wait()
}

// closeRuntime confirms metadata calls have drained before releasing the
// session/store. A timeout retains the runtime for retry. A CloseSession failure
// remains sticky: repeating Stop cannot reinterpret an uncertain close as a
// successful old-view shutdown just because the runtime pointer was cleared.
func (e *Extent) closeRuntime(ctx context.Context) error {
	e.extentStopMu.Lock()
	defer e.extentStopMu.Unlock()
	if e.extentStopErr != nil {
		return e.extentStopErr
	}
	e.stopRuntimeLoop()
	er := e.rt.Load()
	if er == nil {
		return nil
	}
	if er.hold != nil {
		waitCtx, cancel := context.WithTimeout(ctx, extentMetaDrainTimeout)
		err := er.hold.closeAndWaitContext(waitCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("extent metadata drain: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// CloseSession has no context API. Once started, await its actual result;
	// cancellation cannot turn that result into permission to resume old work.
	e.extentStopErr = extent.CloseRuntime(er.rt)
	e.extentMu.Lock()
	e.rt.Store(nil)
	e.extentMu.Unlock()
	return e.extentStopErr
}

// ensureRuntime builds the JuiceFS session single-flighted.
func (e *Extent) ensureRuntime() (err error) {
	e.extentMu.Lock()
	if e.extentBuild.cond == nil {
		e.extentBuild.cond = sync.NewCond(&e.extentMu)
	}
	for {
		if e.extentTornDown {
			e.extentMu.Unlock()
			return errExtentTornDown
		}
		if e.rt.Load() != nil {
			e.extentMu.Unlock()
			return nil
		}
		if !e.extentBuild.building {
			e.extentBuild.building = true
			e.extentBuild.gen++
			e.extentBuild.err = nil
			break
		}
		gen := e.extentBuild.gen
		e.extentBuild.cond.Wait()
		if e.extentBuild.err != nil && e.extentBuild.gen == gen {
			err := e.extentBuild.err
			e.extentMu.Unlock()
			return err
		}
	}
	e.extentMu.Unlock()

	defer func() {
		e.extentMu.Lock()
		e.extentBuild.building = false
		e.extentBuild.err = err
		e.extentBuild.cond.Broadcast()
		e.extentMu.Unlock()
	}()

	if e.opts.Client == nil {
		return fmt.Errorf("extent runtime: missing client")
	}
	mintCtx, mintCancel := context.WithTimeout(context.Background(), extent.CredentialMintTimeout)
	cred, err := e.opts.Client.GetDataCredential(mintCtx)
	mintCancel()
	if err != nil {
		return fmt.Errorf("data credential: %w", err)
	}
	store, err := extent.OpenStorage(cred, e.opts.Client)
	if err != nil {
		return fmt.Errorf("extent storage: %w", err)
	}
	cacheDir := e.opts.CacheDir
	hold := newExtentMetaHold()
	transport := extent.NewHTTPTransport(e.opts.Client)
	transport.Enter = hold.enter
	transport.Leave = hold.leave
	rt, err := extent.NewRuntime(extent.RuntimeConfig{
		CacheDir:  cacheDir,
		CacheKey:  extent.CacheKeyForPrefix(cred.Prefix),
		Transport: transport,
		Storage:   store,
	})
	if err != nil {
		return err
	}
	if rt.VFS == nil {
		_ = extent.CloseRuntime(rt)
		return fmt.Errorf("extent runtime: missing juicefs VFS")
	}
	ctx, cancel := context.WithCancel(context.Background())
	er := &extentRuntime{rt: rt, hold: hold, stop: cancel}

	e.extentMu.Lock()
	if e.extentTornDown {
		e.extentMu.Unlock()
		cancel()
		_ = extent.CloseRuntime(rt)
		return errExtentTornDown
	}
	if e.rt.Load() != nil {
		e.extentMu.Unlock()
		cancel()
		_ = extent.CloseRuntime(rt)
		return nil
	}
	fmt.Fprintf(os.Stderr, "drive9: extent juicefs cache-dir=%s buffer-size=%d\n",
		rt.ChunkConf.CacheDir, rt.ChunkConf.BufferSize)
	e.rt.Store(er)
	er.wg.Add(1)
	go func() {
		defer er.wg.Done()
		e.compactLoop(ctx)
	}()
	e.extentMu.Unlock()
	return nil
}

// compactLoop claims/executes/acks compaction tasks with the 2s idle
// cadence and lease-preserving failure handling.
func (e *Extent) compactLoop(ctx context.Context) {
	idle := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		er := e.rt.Load()
		if er == nil || er.rt == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(idle):
			}
			continue
		}
		ino, indx, taskID, receipt, err := extent.ClaimNextCompact(er.rt.Transport)
		if err != nil || taskID == "" || ino == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(idle):
			}
			continue
		}
		if err := extent.ExecuteCompact(ctx, er.rt, ino, indx); err != nil {
			if ctx.Err() == nil {
				_ = extent.RequeueCompact(er.rt.Transport, taskID, receipt, err)
			}
		} else {
			_ = extent.CompleteCompact(er.rt.Transport, taskID, receipt)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
	}
}
