package fuse

// This file owns the extent runtime's lifecycle: single-flight construction,
// the in-flight metadata gate, and the teardown that stops the compaction loop
// and releases the JuiceFS session. The hot FUSE operation paths live in
// extent_jfs.go.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/extent"
)

// errExtentTornDown is returned when an extent operation races unmount: the
// runtime has been torn down and must not be re-created.
var errExtentTornDown = errors.New("extent runtime: mount is tearing down")

// extentMetaDrainTimeout bounds closeExtentRuntime's wait for in-flight
// metadata RPCs before it releases the session anyway (logged). It must exceed
// extent.MetaCallTimeout: draining cannot succeed while a bounded non-blocking
// RPC may still be running for that long.
const extentMetaDrainTimeout = extent.MetaCallTimeout + 5*time.Second

// stopExtentRuntimeLoop marks the extent runtime torn down and stops and joins
// its compaction loop. Teardown is marked under extentMu so a concurrent
// ensureExtentRuntime cannot install a runtime after the pointer is cleared.
// The runtime stays installed until closeExtentRuntime so FlushAll's VFS flush
// still sees it.
func (fs *Dat9FS) stopExtentRuntimeLoop() {
	fs.extentMu.Lock()
	fs.extentTornDown = true
	if fs.extentBuildCond != nil {
		// Wake single-flight waiters: a build that is still running can no
		// longer be installed, so they must observe the teardown.
		fs.extentBuildCond.Broadcast()
	}
	er := fs.extentRT.Load()
	fs.extentMu.Unlock()
	if er == nil {
		return
	}
	if er.stop != nil {
		er.stop()
	}
	// Join the compaction loop before touching the VFS: a compaction in flight
	// is uploading a merged blob and about to CAS it into meta, and returning
	// from unmount in between would leak that blob.
	er.wg.Wait()
}

// closeExtentRuntime releases the extent runtime's JuiceFS session and object
// store and clears the pointer, so a torn-down fs never advertises a dead
// runtime as healthy.
func (fs *Dat9FS) closeExtentRuntime() {
	fs.extentMu.Lock()
	er := fs.extentRT.Load()
	fs.extentRT.Store(nil)
	fs.extentMu.Unlock()
	if er == nil {
		return
	}
	// Refuse new metadata RPCs and wait for the in-flight ones before closing the
	// session, so a late FUSE handler cannot use a closed one (the object store is
	// refcounted separately). The session-cleanup RPC CloseRuntime issues next is
	// exempt from the gate: refusing it would leave the JuiceFS session open.
	if er.hold != nil {
		if !er.hold.closeAndWait(extentMetaDrainTimeout) {
			safeLogPrintf("extent metadata drain timed out after %s; closing the session anyway", extentMetaDrainTimeout)
		}
	}
	if err := extent.CloseRuntime(er.rt); err != nil {
		safeLogPrintf("close extent runtime: %v", err)
	}
}

// extentCleanupOp is the one metadata RPC teardown must run after the gate is
// closed: CloseRuntime's release of the JuiceFS session. Every other RPC is
// refused once teardown starts.
const extentCleanupOp = jfsmeta.Drive9OpCleanStaleSession

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

// enter reports whether a metadata RPC may run. Once teardown has closed the
// gate only the session-cleanup RPC is admitted, so a late handler is refused
// while CloseRuntime can still release the session it is protecting.
func (h *extentMetaHold) enter(op string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed && op != extentCleanupOp {
		return false
	}
	h.active++
	return true
}

func (h *extentMetaHold) leave() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active--
	if h.active == 0 {
		h.cond.Broadcast()
	}
}

// closeAndWait refuses new metadata RPCs (except the session-cleanup RPC) and
// returns once the in-flight ones finish, or false after timeout. Teardown
// proceeds either way: the process is exiting.
func (h *extentMetaHold) closeAndWait(timeout time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	deadline := time.Now().Add(timeout)
	for h.active > 0 {
		remain := time.Until(deadline)
		if remain <= 0 {
			return false
		}
		timer := time.AfterFunc(remain, func() {
			h.mu.Lock()
			h.cond.Broadcast()
			h.mu.Unlock()
		})
		h.cond.Wait()
		timer.Stop()
	}
	return true
}

type extentRuntime struct {
	rt   *extent.Runtime
	hold *extentMetaHold
	stop context.CancelFunc
	// wg tracks the background compaction loop. FlushAll cancels it and then
	// joins it, so unmount cannot return while a compaction is still
	// uploading or committing: exiting between the object PUT and the compact
	// CAS would leak the merged blob.
	wg sync.WaitGroup
}

func (fs *Dat9FS) ensureExtentRuntime() (err error) {
	fs.extentMu.Lock()
	if fs.extentBuildCond == nil {
		fs.extentBuildCond = sync.NewCond(&fs.extentMu)
	}
	for {
		if fs.extentTornDown {
			fs.extentMu.Unlock()
			return errExtentTornDown
		}
		if fs.extentRT.Load() != nil {
			fs.extentMu.Unlock()
			return nil
		}
		if !fs.extentBuilding {
			// Single-flight: only the first caller builds; the rest wait for it
			// rather than each constructing a full runtime with its own session.
			fs.extentBuilding = true
			fs.extentBuildGen++
			fs.extentBuildErr = nil
			break
		}
		gen := fs.extentBuildGen
		if fs.extentBuildWaitHook != nil {
			fs.extentBuildWaitHook()
		}
		fs.extentBuildCond.Wait()
		if fs.extentBuildErr != nil && fs.extentBuildGen == gen {
			// Share the failed build's outcome with every waiter instead of
			// letting each one serially retry a full build against a dead
			// endpoint. A later caller starts a fresh build (generation moves on).
			err := fs.extentBuildErr
			fs.extentMu.Unlock()
			return err
		}
	}
	fs.extentMu.Unlock()

	// The build runs outside extentMu (the mint and the JuiceFS
	// Init/Load/NewSession metadata calls go to the network, and teardown must
	// never wait on them). Publish the outcome and wake the waiters on every path.
	defer func() {
		fs.extentMu.Lock()
		fs.extentBuilding = false
		fs.extentBuildErr = err
		fs.extentBuildCond.Broadcast()
		fs.extentMu.Unlock()
	}()

	if fs.client == nil {
		return fmt.Errorf("extent runtime: missing client")
	}
	mintCtx, mintCancel := context.WithTimeout(context.Background(), extent.CredentialMintTimeout)
	cred, err := fs.client.GetDataCredential(mintCtx)
	mintCancel()
	if err != nil {
		return fmt.Errorf("data credential: %w", err)
	}
	store, err := extent.OpenStorage(cred, fs.client)
	if err != nil {
		return fmt.Errorf("extent storage: %w", err)
	}
	// The extent data plane keeps a read block cache under the mount-scoped
	// drive9 cache dir (JuiceFS puts it in its jfs/ subdir); chunk-store
	// staging stays off, so this directory holds only blocks that are already
	// durable in object storage. The per-tenant key keeps two tenants that
	// share --cache-dir from serving each other's blocks.
	cacheDir := fs.extentCacheDir
	if cacheDir == "" && fs.opts != nil {
		cacheDir = fs.opts.CacheDir
	}
	hold := newExtentMetaHold()
	transport := extent.NewHTTPTransport(fs.client)
	transport.Enter = hold.enter
	transport.Leave = hold.leave
	// Bound the non-blocking metadata RPCs (blocking Flock/Setlk are exempt in
	// Call). Set before NewRuntime publishes the transport to its goroutines.
	transport.Timeout = extent.MetaCallTimeout
	rt, err := extent.NewRuntime(extent.RuntimeConfig{
		CacheDir:  cacheDir,
		CacheKey:  extent.CacheKeyForPrefix(cred.Prefix),
		Transport: transport,
		Storage:   store,
	})
	if err != nil {
		// NewRuntime owns the store and released it before returning.
		return err
	}
	if rt.VFS == nil {
		_ = extent.CloseRuntime(rt)
		return fmt.Errorf("extent runtime: missing juicefs VFS")
	}
	ctx, cancel := context.WithCancel(context.Background())
	er := &extentRuntime{rt: rt, hold: hold, stop: cancel}

	fs.extentMu.Lock()
	if fs.extentTornDown {
		// Teardown started while we were building: discard what we built.
		fs.extentMu.Unlock()
		cancel()
		_ = extent.CloseRuntime(rt)
		return errExtentTornDown
	}
	if fs.extentRT.Load() != nil {
		// Another caller installed a runtime first: discard the duplicate.
		fs.extentMu.Unlock()
		cancel()
		_ = extent.CloseRuntime(rt)
		return nil
	}
	fmt.Fprintf(os.Stderr, "drive9: extent juicefs cache-dir=%s buffer-size=%d\n",
		rt.ChunkConf.CacheDir, rt.ChunkConf.BufferSize)
	fs.extentRT.Store(er)
	er.wg.Add(1)
	go func() {
		defer er.wg.Done()
		fs.extentCompactLoop(ctx)
	}()
	fs.extentMu.Unlock()
	return nil
}
