// Package core implements the drive9 remote terminal as a plain fs.Handler
// tail: there is no privileged core in the framework, this is simply the
// last handler of the chain — Do never calls next. Every operation is
// issued synchronously through pkg/client, so a successful Write has
// already been committed by the server (write-sync durability:
// Flush/Fsync/Release are no-ops by construction). Writes are whole-file
// compare-and-set uploads patched in place, with a small
// revision-conflict retry budget; reads are range requests. Efficiency
// layers compose above this terminal. Error classification and errno
// mapping live in errors.go.
package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/singleflight"
)

const (
	// defaultWriteRetries bounds compare-and-set retries on revision
	// conflicts.
	defaultWriteRetries = 3
	// Default detached stat-retry budget.
	defaultStatRetryCount   = 3
	defaultStatRetryTimeout = 2 * time.Second
	// defaultReadConcurrency bounds concurrent remote reads by default.
	defaultReadConcurrency = 24

	// readTransientRetry* bound the read-path detached retry.
	readTransientRetryCount   = 2
	readTransientRetryTimeout = 2 * time.Second
	// remoteFetchTimeout bounds one shared singleflight fetch.
	remoteFetchTimeout = 30 * time.Second

	// namespaceMutationRetryTimeout bounds each detached probe and retry
	// after a transient namespace-mutation failure.
	namespaceMutationRetryTimeout = 2 * time.Second
	// mutationCommitTimeout caps a detached namespace-mutation commit
	// commit: a kernel interrupt after the request hit the wire
	// must not abort the server-side commit into an ambiguous state.
	mutationCommitTimeout = 30 * time.Second

	// postUploadMode retries a mode application that 404s against backend
	// replica lag right after an upload.
	postUploadModeAttempts     = 5
	postUploadModeInitialDelay = 10 * time.Millisecond
)

// errReadRetriesExhausted marks a read whose transient retries are spent;
// it wraps the last error so the errno table still sees its cause.
var errReadRetriesExhausted = errors.New("read retries exhausted")

// Options configures the terminal.
type Options struct {
	// TrustLocalEvents lets the namespace cache answer Lookup/GetAttr
	// without remote revalidation (single-server mounts where every
	// mutation flows through this process). Default false: process-local
	// event bookkeeping cannot see other servers' writes, and the cached
	// row then hides foreign revisions.
	TrustLocalEvents bool
	// RemoteRoot prefixes every engine path before it reaches the client.
	RemoteRoot string

	// WriteRetries bounds compare-and-set retries on revision conflicts.
	// Zero means defaultWriteRetries.
	WriteRetries int

	// StatRetryCount bounds detached retries after transient Lookup/GetAttr
	// stat failures: 0 disables, negative takes the
	// default, positive is the count itself.
	StatRetryCount int
	// StatRetryTimeout bounds each detached retry (default 2s).
	StatRetryTimeout time.Duration

	// ReadConcurrency bounds concurrent remote reads (default 24); the
	// backend loses its backpressure without it.
	ReadConcurrency int

	// LegacyInterruptibleMutations keeps namespace-mutation commits bound
	// to the FUSE cancel channel (legacy mode). Default false: commits
	// run detached so an interrupt cannot abort them into an ambiguous
	// server-side state.
	LegacyInterruptibleMutations bool

	// LegacyDirStatFallback serves directory stats by listing the parent
	// when the backend rejects HEAD on directories (some deployments
	// 403/404 directory stats).
	LegacyDirStatFallback bool
}

// NamespaceCache answers path lookups from the mount's directory-listing
// state (implemented by the facade's listing cache; attached after
// assembly).
type NamespaceCache interface {
	LookupNamespace(parent, name string) (fs.DirEntryInfo, fs.LookupKind)
}

// Core is the remote terminal: the last handler of the chain, serving
// every op synchronously against the HTTP backend.
type Core struct {
	cl      *client.Client
	root    string
	retries int
	// trustLocalEvents mirrors Options.TrustLocalEvents (namespace-cache
	// stat shortcut).
	trustLocalEvents bool

	statRetryCount   int
	statRetryTimeout time.Duration
	dirCache         NamespaceCache
	// readFlight dedups concurrent remote reads of one range:
	// thundering-herd FUSE reads on an uncached file collapse into a
	// single HTTP fetch.
	// viewEpoch advances on every checkout/reset (a 401 or an SSE
	// reconnect). A stat dispatched before the reset must not publish its
	// negative row into the namespace cache — the facade's reply fence
	// comes too late, the pollution survives and hides the new view's file.
	// viewMu makes the epoch check and the negative publication atomic
	// against the reset (a plain Load left a window where the old 404
	// re-seeded the row after the sweep).
	viewEpoch atomic.Uint64
	viewMu    sync.RWMutex

	// moveMu serializes a remote namespace move with terminal writes: a
	// write holds the read side for its WHOLE commit (the per-handle lock
	// alone only protects the retarget that follows the move), and the
	// rename holds the write side across the remote call + retarget.
	moveMu sync.RWMutex

	// syncHandles tracks terminal handles so a rename can retarget them:
	// without it a core-only chain (no writeback) left an open fd on the
	// OLD name, so its next write recreated the old path and /moved kept
	// the stale bytes.
	syncHandles sync.Map

	// bus announces view resets (401) to the facade; resetPending coalesces
	// a burst of 401s into one asynchronous reset.
	bus          *fs.EventBus
	resetPending atomic.Bool
	readFlight   *singleflight.SingleFlight
	// readSlots bounds concurrent remote reads (default 24).
	readSlots chan struct{}

	legacyDirStatFallback        bool
	legacyInterruptibleMutations bool

	// remotePerf reports HTTP families to the facade's perf counters (v1
	// perfRecordRemote; attached after assembly, nil-safe).
	remotePerf func(name string, err error, dur time.Duration, bytes uint64)
}

// SetRemoteOpRecorder wires the facade-owned perf feed.
func (c *Core) SetRemoteOpRecorder(fn func(name string, err error, dur time.Duration, bytes uint64)) {
	c.remotePerf = fn
}

func (c *Core) recordRemote(name string, start time.Time, err error, bytes uint64) {
	if c.remotePerf != nil {
		c.remotePerf(name, err, time.Since(start), bytes)
	}
}

// New constructs the terminal. It negotiates the server's inline threshold
// once (cl.Warm) so commits route small files through conditional PUT
// instead of the threshold-0 safe default (multipart).
func New(cl *client.Client, opts Options) (*Core, error) {
	if cl == nil {
		return nil, fmt.Errorf("core: client is required")
	}
	cl.Warm(context.Background())
	retries := opts.WriteRetries
	if retries <= 0 {
		retries = defaultWriteRetries
	}
	trustLocal := opts.TrustLocalEvents
	statRetries := opts.StatRetryCount
	if statRetries < 0 {
		statRetries = defaultStatRetryCount
	}
	statTimeout := opts.StatRetryTimeout
	if statTimeout <= 0 {
		statTimeout = defaultStatRetryTimeout
	}
	readConcurrency := opts.ReadConcurrency
	if readConcurrency <= 0 {
		readConcurrency = defaultReadConcurrency
	}
	return &Core{cl: cl, root: normalizeRoot(opts.RemoteRoot), retries: retries,
		statRetryCount: statRetries, statRetryTimeout: statTimeout,
		readFlight:                   singleflight.New(),
		readSlots:                    make(chan struct{}, readConcurrency),
		legacyDirStatFallback:        opts.LegacyDirStatFallback,
		legacyInterruptibleMutations: opts.LegacyInterruptibleMutations,
		trustLocalEvents:             trustLocal}, nil
}

// AttachEvents wires the mount's event bus: an HTTP 401 means the
// credential (or the tenant it points at) changed underneath the mount, so
// the whole view is untrusted and the facade must reset it before the
// caller sees EACCES.
func (c *Core) AttachEvents(bus *fs.EventBus) {
	c.bus = bus
	if bus != nil {
		bus.On(fs.EventCheckout, func(fs.Event) {
			c.viewMu.Lock()
			c.viewEpoch.Add(1)
			c.viewMu.Unlock()
		})
	}
}

// mapErr folds backend failures into fs identities, announcing a view
// reset on 401.
//
// The announcement runs on its OWN goroutine: a 401 surfaces inside a data
// plane that may hold its own handle lock (a buffered overwrite's base
// preload reaches SyncHandle.Read under the writeback handle mutex), and
// the checkout subscribers take that same lock — a synchronous broadcast
// self-deadlocked. Coalesced so a burst of 401s emits one reset.
func (c *Core) mapErr(err error) error {
	if err != nil && c.bus != nil {
		var se *client.StatusError
		if errors.As(err, &se) && se.StatusCode == http.StatusUnauthorized && c.resetPending.CompareAndSwap(false, true) {
			go func() {
				defer c.resetPending.Store(false)
				// A revoked credential may now address a DIFFERENT
				// namespace: userspace caches AND the kernel's page/dentry
				// caches must be flushed (plain SSE resets leave pages).
				c.bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/", KernelInvalidate: true})
			}()
		}
	}
	return mapErr(err)
}

// Attach wires the facade-owned namespace cache into the terminal so
// Lookups/GetAttrs resolve from fresh listings without a remote stat.
func (c *Core) Attach(dirCache NamespaceCache) {
	c.dirCache = dirCache
}

// Do serves an operation synchronously against the backend. As the chain
// tail it never calls next.
func (c *Core) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpCreate, fs.OpMkdir, fs.OpSymlink, fs.OpLink, fs.OpRename,
		fs.OpUnlink, fs.OpRmdir, fs.OpSetAttr, fs.OpWrite,
		fs.OpLookup, fs.OpGetAttr, fs.OpList:
		// v1 interrupt-safe mutations, extended to the bounded namespace
		// reads: kernel-interrupted requests detach from the FUSE cancel
		// channel and finish under their own deadline. An abort mid-flight
		// surfaces EAGAIN to the app, which nothing retries (#1034);
		// completing the op converges the name and the kernel discards or
		// accepts the reply either way. OpRead stays interruptible —
		// streaming reads of uncached data must not outlive their caller
		// by a full detached budget.
		//
		// The detach keeps the caller's explicit budget and honors the
		// legacy interruptible mount option: a caller deadline earlier
		// than the detached budget still bounds the op, and
		// LegacyInterruptibleMutations keeps cancellation live.
		if !c.legacyInterruptibleMutations {
			mctx, mcancel := detachedNamespaceContext(ctx)
			defer mcancel()
			ctx = mctx
		}
	}
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		return c.statOp(ctx, op.Path)
	case fs.OpList:
		return c.listOp(ctx, op.Path)
	case fs.OpRead:
		return c.readOp(ctx, op.Path, op.Offset, op.Limit)
	case fs.OpCreate:
		return c.createOp(ctx, op)
	case fs.OpWrite:
		if err := c.WriteSync(ctx, op.Path, op.Offset, op.Data, op.Mode); err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Written: uint32(len(op.Data))}, nil
	case fs.OpMkdir:
		mode := op.Mode & 0o7777
		if err := c.mkdirRemote(ctx, op.Path, mode); err != nil {
			return fs.Result{}, c.mapErr(err)
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: mode}}, nil
	case fs.OpRmdir:
		if err := c.deleteRemote(ctx, op.Path, true); err != nil {
			// Tolerate 404: a stale kernel dentry (or a concurrent
			// removal) makes unlink/rmdir of an already-gone path succeed.
			if client.IsNotFound(err) {
				return fs.Result{}, nil
			}
			return fs.Result{}, c.mapErr(err)
		}
		return fs.Result{}, nil
	case fs.OpUnlink:
		// POSIX unlink-of-open-file: the descriptors keep their object.
		c.moveMu.Lock()
		rollback, serr := c.snapshotSyncHandlesForPath(ctx, op.Path)
		if serr != nil {
			c.moveMu.Unlock()
			return fs.Result{}, serr
		}
		delErr := c.deleteRemote(ctx, op.Path, false)
		if delErr != nil && !client.IsNotFound(delErr) {
			// The name survives: the descriptors must keep writing to it.
			rollback()
			c.moveMu.Unlock()
			return fs.Result{}, c.mapErr(delErr)
		}
		c.moveMu.Unlock()
		if delErr != nil {
			return fs.Result{}, nil
		}
		return fs.Result{}, nil
	case fs.OpRename:
		// The move and the retarget are ONE boundary with terminal writes:
		// an in-flight commit finishes against the old name first (the
		// rename then carries its data), and a write starting after the
		// move sees the new path.
		c.moveMu.Lock()
		// The overwritten target's descriptors keep the OLD object's bytes.
		rollback, serr := c.snapshotSyncHandlesForPath(ctx, op.Path2)
		var err error
		if serr != nil {
			err = serr
		} else {
			err = c.renameRemote(ctx, op.Path, op.Path2)
			if err == nil {
				c.retargetSyncHandles(op.Path, op.Path2)
			} else {
				// The rename failed and the target KEEPS its name: restore
				// the descriptors' writable identity (an anonymous snapshot
				// would swallow every later write).
				rollback()
			}
		}
		c.moveMu.Unlock()
		if serr != nil {
			return fs.Result{}, serr
		}
		if err != nil {
			return fs.Result{}, c.mapErr(err)
		}
		return fs.Result{}, nil
	case fs.OpSymlink:
		if err := c.symlinkRemote(ctx, op.Target, op.Path); err != nil {
			return fs.Result{}, c.mapErr(err)
		}
		// Answer from the created row (v1 stats after create): the reply's
		// mtime seeds the kernel entry, and a synthesized now() there
		// outruns the row — every later cache-expiry stat then flips the
		// served mtime backwards and git flags the link modified.
		st, err := c.cl.StatCtx(ctx, c.remotePath(op.Path))
		if err != nil {
			return fs.Result{}, c.mapErr(err)
		}
		return fs.Result{Attr: attrOf(st)}, nil
	case fs.OpReadlink:
		// drive9 stores a symlink's target as the link's file content.
		data, err := c.fetchRemote(ctx, c.remotePath(op.Path), 0, 0)
		if err != nil {
			return fs.Result{}, c.mapErr(err)
		}
		return fs.Result{Data: data}, nil
	case fs.OpLink:
		if err := c.hardlinkRemote(ctx, op.Path, op.Path2); err != nil {
			return fs.Result{}, c.mapErr(err)
		}
		// The kernel sizes the new name from this reply; a missing Attr
		// reads as an empty file.
		st, err := c.cl.StatCtx(ctx, c.remotePath(op.Path2))
		if err != nil {
			return fs.Result{}, c.mapErr(err)
		}
		return fs.Result{Attr: attrOf(st)}, nil
	case fs.OpMknod:
		if op.Mode&uint32(syscall.S_IFMT) != 0 && op.Mode&uint32(syscall.S_IFMT) != syscall.S_IFREG {
			// The terminal creates regular files only; any other node
			// type is unsupported here.
			return fs.Result{}, fs.ErrUnsupported
		}
		return c.mknodRegular(ctx, op)
	case fs.OpSetAttr:
		return c.setAttrOp(ctx, op)
	case fs.OpFlush, fs.OpFsync, fs.OpRelease:
		// Write-sync: nothing is buffered locally, so durability
		// boundaries carry no extra work.
		return fs.Result{}, nil
	}
	return fs.Result{}, fs.ErrUnsupported
}

// emitLanded publishes a terminal commit so read-side planes can refresh
// the identity of other open descriptors. The terminal is the only
// component that ALWAYS sees the commit (writeback is optional): without
// this, a core-only mount cleared its dirty bypass at fsync while readers
// still held the pre-write revision.
func (c *Core) emitLanded(path string, rev, size int64) {
	if c.bus == nil {
		return
	}
	c.bus.Emit(fs.Event{Kind: fs.EventLanded, Path: path, Rev: rev, Size: size, HasSize: true})
}

// WriteSync durably writes data at off: it returns only after the server
// has committed the whole file image (write-sync durability).
func (c *Core) WriteSync(ctx context.Context, path string, off int64, data []byte, mode uint32) error {
	rp := c.remotePath(path)
	for attempt := 0; attempt <= c.retries; attempt++ {
		st, err := c.cl.StatCtx(ctx, rp)
		if err != nil {
			if !client.IsNotFound(err) {
				return c.mapErr(err)
			}
			if off != 0 {
				// write past EOF on a missing path: materialize an empty
				// file first, then retry the stat
				if _, cerr := c.cl.CreateFileCtx(ctx, rp); cerr != nil && !errors.Is(cerr, client.ErrConflict) {
					return c.mapErr(cerr)
				}
				continue
			}
			if werr := c.cl.WriteStream(ctx, rp, bytes.NewReader(data), int64(len(data)), nil); werr != nil {
				if errors.Is(werr, client.ErrConflict) {
					continue // lost the create race; retry as an existing file
				}
				return c.mapErr(werr)
			}
			if mode != 0 {
				if err := retryPostUploadMode(ctx, func() error {
					return c.cl.ChmodCtx(ctx, rp, mode&0o777)
				}); err != nil {
					return c.mapErr(err)
				}
			}
			rev := int64(0)
			if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
				rev = st.Revision
			}
			c.emitLanded(path, rev, int64(len(data)))
			return nil
		}
		content, merr := c.mergeAt(ctx, rp, st, off, data)
		if merr != nil {
			return merr
		}
		start := time.Now()
		committed, werr := c.cl.WriteStreamConditionalWithChecksum(ctx, rp, bytes.NewReader(content), int64(len(content)), nil, st.Revision, "")
		if werr != nil {
			if errors.Is(werr, client.ErrConflict) {
				continue // revision moved under us; re-stat and re-patch
			}
			c.recordRemote("write", start, werr, uint64(len(content)))
			return c.mapErr(werr)
		}
		c.recordRemote("write", start, nil, uint64(len(content)))
		if committed != nil {
			c.emitLanded(path, committed.Revision, committed.Size)
		} else {
			c.emitLanded(path, 0, int64(len(content)))
		}
		if mode == 0 && st.HasMode && st.Mode&0o7777 != 0o644 {
			// The server resets permissions on whole-image PUTs; an
			// explicitly chmod'd file keeps its mode across writes. The
			// reapply is best-effort (v1 postUploadMode tolerated lag): a
			// failing mode restore must not fail an already-committed
			// write.
			_ = retryPostUploadMode(ctx, func() error {
				// v1 remoteChmodMode: the wire chmod carries permission
				// bits only; the local entry keeps the full 0o7777.
				return c.cl.ChmodCtx(ctx, rp, st.Mode&0o777)
			})
		}
		return nil
	}
	return &fs.StatusError{Path: path, Status: gofuse.EAGAIN}
}

// SyncHandle is the terminal data-plane handle: every Read is a range
// request through the shared read machinery and every Write a durable
// whole-file commit. Sync/Flush/Release are no-ops — nothing is buffered.
type SyncHandle struct {
	core *Core
	mu   sync.RWMutex
	path string
	// snap preserves the OLD object's bytes once its name is replaced or
	// removed (a core-only chain has no writeback snapshot plane): reads
	// serve it, and writes compose into it anonymously.
	snap *anonSnapshot
}

// anonSnapshot is a bounded in-memory or temp-file image of an object whose
// name is going away.
type anonSnapshot struct {
	mu     sync.Mutex
	data   []byte
	file   *os.File
	size   int64
	closed bool
}

func (s *anonSnapshot) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || off < 0 || off >= s.size {
		return 0, io.EOF
	}
	if s.file != nil {
		return s.file.ReadAt(p, off)
	}
	n := copy(p, s.data[off:])
	if int64(n) < int64(len(p)) && off+int64(n) >= s.size {
		return n, io.EOF
	}
	return n, nil
}

func (s *anonSnapshot) WriteAt(off int64, p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	end := off + int64(len(p))
	if s.file == nil && end > maxAnonSnapshotBytes {
		// A sparse/large write must not grow the heap without bound: move
		// the captured bytes to a temp file and continue there.
		tmp, err := os.CreateTemp("", "drive9-anon-*")
		if err != nil {
			return err
		}
		_ = os.Remove(tmp.Name())
		if len(s.data) > 0 {
			if _, werr := tmp.WriteAt(s.data, 0); werr != nil {
				_ = tmp.Close()
				return werr
			}
		}
		s.file = tmp
		s.data = nil
	}
	if s.file == nil && end > int64(len(s.data)) {
		grown := make([]byte, end)
		copy(grown, s.data)
		s.data = grown
	}
	if s.file != nil {
		if _, err := s.file.WriteAt(p, off); err != nil {
			return err
		}
	}
	if s.file == nil {
		copy(s.data[off:], p)
	}
	if end > s.size {
		s.size = end
	}
	return nil
}

func (s *anonSnapshot) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.size = 0
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
	s.data = nil
}

// detachedNamespaceDeadline bounds detached namespace ops (mutations and
// bounded reads) once they leave the FUSE cancel channel (v1's
// interrupt-safe mutations carried their own deadline).
const detachedNamespaceDeadline = 30 * time.Second

// detachedNamespaceContext detaches ctx from its cancellation while
// preserving an explicit caller deadline: the op finishes unless the
// caller's own budget expires (a raw WithoutCancel let a 100 ms GetAttr
// outlive its deadline by the full detached window).
func detachedNamespaceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(ctx)
	if dl, ok := ctx.Deadline(); ok {
		lim := time.Now().Add(detachedNamespaceDeadline)
		if dl.Before(lim) {
			return context.WithDeadline(base, dl)
		}
	}
	return context.WithTimeout(base, detachedNamespaceDeadline)
}

// RemoteNamespaceReadable marks handles whose bytes equal the ordinary
// remote namespace object at the path (core's terminal handles): the read
// the cache's block prefetcher may fetch through the remote namespace ONLY
// behind such a claim — localize/layer/Git planes own different bytes for
// the same name, and mixing them corrupted reads.
// IsRemoteNamespaceReadable reports whether reads through this handle
// equal the ordinary remote namespace (safe for remote prefetch).
func (h *SyncHandle) IsRemoteNamespaceReadable() bool { return true }

// RetargetTo rebinds the handle onto another name of the same object (the
// write plane's deferred-unlink survivor hook, #1029): the handle reads by
// path, and a name that is unlinked and later recreated belongs to a
// different object by then.
func (h *SyncHandle) RetargetTo(newPath string) {
	h.mu.Lock()
	h.path = newPath
	h.mu.Unlock()
}

// Anonymous reports that the handle's name is gone/replaced and its bytes
// come from a private snapshot: outer path-keyed caches must not answer for
// it.
func (h *SyncHandle) Anonymous() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.snap != nil
}

// Path returns the handle's current name.
func (h *SyncHandle) Path() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.path
}

// Read fills dest with content at off.
func (h *SyncHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.RLock()
	snap := h.snap
	if snap != nil {
		// Keep the read lock for the WHOLE snapshot read: a rollback closes
		// the snapshot under the write lock, so Close cannot run between
		// the pointer fetch and ReadAt (a closed snapshot used to panic on
		// a non-zero offset).
		defer h.mu.RUnlock()
		n, err := snap.ReadAt(dest, off)
		if err == io.EOF {
			err = nil
		}
		return n, err
	}
	h.mu.RUnlock()
	data, err := h.core.fetchRemote(ctx, h.core.remotePath(h.Path()), off, int64(len(dest)))
	if err != nil {
		return 0, h.core.mapErr(err)
	}
	return copy(dest, data), nil
}

// Write durably commits data at off.
//
// The handle lock is held for the WHOLE commit: a concurrent rename
// retarget waits here, so the write either completes against the old name
// before the move (which then carries the data) or starts against the new
// one. Without it a write paused mid-PUT resumed after the rename and
// recreated the old name from its retry.
func (h *SyncHandle) Write(ctx context.Context, off int64, data []byte) error {
	// Lock order: moveMu -> h.mu (the rename takes moveMu.Lock then the
	// handle lock in RetargetTo). A rename therefore waits for this whole
	// commit; a write starting after it reads the already-retargeted path.
	h.core.moveMu.RLock()
	defer h.core.moveMu.RUnlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.snap != nil {
		// The name is gone/replaced: the write updates the anonymous
		// object only (its bytes stay readable through this fd).
		return h.snap.WriteAt(off, data)
	}
	return h.core.WriteSync(ctx, h.path, off, data, 0)
}

// Sync implements fs.Handle.
func (h *SyncHandle) Sync(ctx context.Context) error { return nil }

// Flush implements fs.Handle.
func (h *SyncHandle) Flush(ctx context.Context) error { return nil }

// Release implements fs.Handle.
func (h *SyncHandle) Release(ctx context.Context) error {
	if h.core != nil {
		h.core.syncHandles.Delete(h)
	}
	h.mu.Lock()
	if h.snap != nil {
		h.snap.Close()
		h.snap = nil
	}
	h.mu.Unlock()
	return nil
}

// newSyncHandle registers a terminal handle for rename retargeting.
func (c *Core) newSyncHandle(path string) *SyncHandle {
	h := &SyncHandle{core: c, path: path}
	c.syncHandles.Store(h, struct{}{})
	return h
}

// maxAnonSnapshotBytes bounds the in-memory anonymous snapshot; larger
// objects stream to a temp file.
const maxAnonSnapshotBytes = 64 << 20

// snapshotSyncHandlesForPath preserves the object currently at path for its
// open terminal handles. A failure aborts the namespace operation: replacing
// the name without a read source would silently switch the descriptors to
// the replacement's bytes.
func (c *Core) snapshotSyncHandlesForPath(ctx context.Context, path string) (func(), error) {
	var targets []*SyncHandle
	c.syncHandles.Range(func(key, _ any) bool {
		h, ok := key.(*SyncHandle)
		// Handles that already own a snapshot are anonymous: they must not
		// make this operation stat/read the REPLACEMENT object (a failing
		// GET on an unreferenced new file blocked its own delete).
		if ok && h.Path() == path && !h.Anonymous() {
			targets = append(targets, h)
		}
		return true
	})
	noop := func() {}
	if len(targets) == 0 {
		return noop, nil
	}
	st, err := c.cl.StatCtx(ctx, c.remotePath(path))
	if err != nil {
		if client.IsNotFound(err) {
			return noop, nil
		}
		return noop, c.mapErr(err)
	}
	size := st.Size
	installed := make([]*SyncHandle, 0, len(targets))
	rollback := func() {
		for _, h := range installed {
			h.mu.Lock()
			if h.snap != nil {
				h.snap.Close()
				h.snap = nil
			}
			h.mu.Unlock()
		}
	}
	for _, h := range targets {
		snap, serr := c.captureAnonSnapshot(ctx, path, size)
		if serr != nil {
			// A partial capture must not leave earlier handles anonymous:
			// the namespace operation is about to fail and the name stays.
			rollback()
			return noop, serr
		}
		h.mu.Lock()
		if h.snap == nil {
			h.snap = snap
			installed = append(installed, h)
		} else {
			snap.Close()
		}
		h.mu.Unlock()
	}
	return rollback, nil
}

func (c *Core) captureAnonSnapshot(ctx context.Context, path string, size int64) (*anonSnapshot, error) {
	if size <= maxAnonSnapshotBytes {
		data, err := c.fetchRemote(ctx, c.remotePath(path), 0, size)
		if err != nil {
			return nil, c.mapErr(err)
		}
		if int64(len(data)) < size {
			return nil, io.ErrUnexpectedEOF
		}
		return &anonSnapshot{data: data[:size], size: size}, nil
	}
	tmp, err := os.CreateTemp("", "drive9-anon-*")
	if err != nil {
		return nil, err
	}
	_ = os.Remove(tmp.Name())
	for off := int64(0); off < size; {
		chunk := int64(4 << 20)
		if off+chunk > size {
			chunk = size - off
		}
		part, rerr := c.fetchRemote(ctx, c.remotePath(path), off, chunk)
		if rerr != nil {
			_ = tmp.Close()
			return nil, c.mapErr(rerr)
		}
		if int64(len(part)) != chunk {
			_ = tmp.Close()
			return nil, io.ErrUnexpectedEOF
		}
		if _, werr := tmp.WriteAt(part, off); werr != nil {
			_ = tmp.Close()
			return nil, werr
		}
		off += chunk
	}
	return &anonSnapshot{file: tmp, size: size}, nil
}

// retargetSyncHandles moves every open terminal handle under a renamed path
// (directory descendants included).
func (c *Core) retargetSyncHandles(oldPath, newPath string) {
	oldPrefix := strings.TrimSuffix(oldPath, "/") + "/"
	newPrefix := strings.TrimSuffix(newPath, "/") + "/"
	c.syncHandles.Range(func(key, _ any) bool {
		h, ok := key.(*SyncHandle)
		if !ok {
			return true
		}
		p := h.Path()
		switch {
		case p == oldPath:
			h.RetargetTo(newPath)
		case strings.HasPrefix(p, oldPrefix):
			h.RetargetTo(newPrefix + strings.TrimPrefix(p, oldPrefix))
		}
		return true
	})
}

// statOp serves OpLookup / OpGetAttr: the namespace cache first (positive
// and negative rows), then a remote stat with transient-retry absorption,
// with the legacy parent-listing fallback as the last resort.
func (c *Core) statOp(ctx context.Context, path string) (fs.Result, error) {
	epoch := c.viewEpoch.Load()
	if attr, ok := c.statFromNamespaceCache(path); ok {
		if attr == nil {
			// Cached-negative rows (negative entry, complete miss, session
			// miss) answer ENOENT without a remote stat. Returning a nil
			// attr as success here would render the missing name as an
			// empty file.
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{Attr: attr}, nil
	}
	st, err := c.statWithTransientRetry(ctx, path)
	if err != nil {
		if client.IsNotFound(err) {
			// v1 cacheRemoteNotFoundPath: a remote-observed miss seeds a
			// short-lived negative row (the storm escalation rides along;
			// lock files never seed).
			// The check and the row insertion share the reset's write
			// side: a reset that already swept the cache blocks here until
			// the publish finishes (and the publish sees the new epoch), so
			// a superseded 404 can never re-seed the cleared cache.
			c.viewMu.RLock()
			if c.viewEpoch.Load() != epoch {
				c.viewMu.RUnlock()
				// The 404 belongs to a view a reset replaced: seeding the
				// negative row would hide the new view's file for its TTL
				// (the facade's generation fence only rejects the reply, not
				// this cache write).
				return fs.Result{}, fs.ErrNotFound
			}
			defer c.viewMu.RUnlock()
			if rec, ok := c.dirCache.(interface {
				RecordRemoteNotFound(parent, name string) bool
			}); ok {
				if parent, name := splitDirBase(path); parent != "" && name != "" && rec.RecordRemoteNotFound(parent, name) {
					// Escalation window tripped: one parent listing
					// installs the authoritative view (v1
					// maybeListOnNegativeLookupStorm). A listing failure
					// must not fail the stat — the ENOENT stands.
					if _, lerr := c.cl.ListCtx(ctx, c.remotePath(parent)); lerr == nil {
						// The listing is installed; re-answer from it.
						if attr, ok := c.statFromNamespaceCache(path); ok && attr == nil {
							return fs.Result{}, fs.ErrNotFound
						}
					}
				}
			}
		}
		if attr, ok := c.statFromParentListing(ctx, path, err); ok {
			return fs.Result{Attr: attr}, nil
		}
		return fs.Result{}, c.mapErr(err)
	}
	return fs.Result{Attr: attrOf(st)}, nil
}

// statFromParentListing serves a stat by matching the parent listing when
// the backend cannot HEAD the path itself: some deployments 403/404
// directory stats while listings work.
func (c *Core) statFromParentListing(ctx context.Context, path string, statErr error) (*fs.Attr, bool) {
	if !c.legacyDirStatFallback {
		return nil, false
	}
	var se *client.StatusError
	if !errors.As(statErr, &se) {
		return nil, false
	}
	switch se.StatusCode {
	case http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed:
	default:
		return nil, false
	}
	parent, name := splitDirBase(path)
	if parent == "" || name == "" {
		return nil, false
	}
	infos, err := c.cl.ListCtx(ctx, c.remotePath(parent))
	if err != nil {
		return nil, false
	}
	for _, fi := range infos {
		if fi.Name != name {
			continue
		}
		kind := fs.KindFile
		mode := fi.Mode & 0o7777
		if fi.IsDir {
			kind = fs.KindDir
			if !fi.HasMode {
				mode = 0o755
			}
		} else if !fi.HasMode {
			mode = 0o644
		}
		mtime := time.Time{}
		if fi.Mtime > 0 {
			mtime = time.Unix(fi.Mtime, 0)
		}
		return &fs.Attr{Kind: kind, Mode: mode, Size: fi.Size, Rev: fi.Revision, Mtime: mtime}, true
	}
	return nil, false
}

// statFromNamespaceCache serves Lookup/GetAttr from a fresh cached listing
// (positive/negative/complete-miss rows). Lock files
// never use it: O_CREAT|O_EXCL probes must revalidate against the remote.
func (c *Core) statFromNamespaceCache(path string) (*fs.Attr, bool) {
	if c.dirCache == nil || path == "/" || path == "" {
		return nil, false
	}
	parent, name := splitDirBase(path)
	if parent == "" || name == "" || name == "." || name == ".." || strings.HasSuffix(name, ".lock") {
		return nil, false
	}
	item, kind := c.dirCache.LookupNamespace(parent, name)
	switch kind {
	case fs.LookupPositive:
		if !c.trustLocalEvents && !item.LocalMutation {
			// Listing-observed rows do not serve stats without
			// revalidation on multi-server mounts (default): only the
			// process-local trust flag — or a row this mount itself wrote
			// (LocalMutation: its own create/write/landing, which no
			// foreign server can make stale) — authorizes them. Foreign
			// writes this mount never heard of would otherwise hide
			// behind a cached size forever. Negative rows stay served:
			// they are short-TTL hints on every mount (v1).
			return nil, false
		}
		itemKind := fs.KindFile
		if item.IsDir {
			itemKind = fs.KindDir
		} else if item.HasMode && item.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
			itemKind = fs.KindSymlink
		}
		mode := item.Mode
		if !item.HasMode {
			mode = 0o644
			if item.IsDir {
				mode = 0o755
			}
		}
		if itemKind == fs.KindSymlink {
			mode = uint32(syscall.S_IFLNK) | mode&0o777
		} else {
			mode &= 0o7777
		}
		return &fs.Attr{Kind: itemKind, Mode: mode, Size: item.Size, Rev: item.Rev, Mtime: item.Mtime,
			Nlink: item.Nlink, ResourceID: item.ResourceID,
			// The extent plane routes by the ExtentIno a LOOKUP replies
			// with; projecting the cached row without it sent an extent
			// file's open to the ordinary HTTP data plane (422 there).
			ExtentIno: item.ExtentIno,
			Uid:       item.Uid, Gid: item.Gid,
			// Keep the two presence flags apart: a row written by a
			// single-field chown carries the other field's preserve
			// sentinel, which must never surface as an actual owner.
			HasOwner: item.HasUID || item.HasGID, HasUID: item.HasUID, HasGID: item.HasGID}, true
	case fs.LookupNegative, fs.LookupCompleteMiss, fs.LookupSessionMiss:
		return nil, true // known absent: ENOENT without a remote stat
	}
	return nil, false
}

// statWithTransientRetry absorbs short-lived backend jitter with detached,
// bounded retries: the initial probe honors the
// caller's context; retries do not re-bind it, so a kernel interrupt cannot
// cancel the absorb window.
func (c *Core) statWithTransientRetry(ctx context.Context, path string) (*client.StatResult, error) {
	rp := c.remotePath(path)
	start := time.Now()
	st, err := c.cl.StatCtx(ctx, rp)
	c.recordRemote("stat", start, err, 0)
	if err == nil || client.IsNotFound(err) || !isTransientStatErr(err) || c.statRetryCount <= 0 {
		return st, err
	}
	for range c.statRetryCount {
		retryCtx, cancel := context.WithTimeout(context.Background(), c.statRetryTimeout)
		start := time.Now()
		st, err = c.cl.StatCtx(retryCtx, rp)
		c.recordRemote("stat", start, err, 0)
		cancel()
		if err == nil || client.IsNotFound(err) || !isTransientStatErr(err) {
			return st, err
		}
	}
	return st, err
}

// listOp serves OpList: it lists one directory, dropping unusable rows
// (empty, dotdot, separator-bearing) and classifying symlinks by their
// mode bits.
func (c *Core) listOp(ctx context.Context, dir string) (fs.Result, error) {
	infos, err := c.cl.ListCtx(ctx, c.remotePath(dir))
	if err != nil {
		return fs.Result{}, c.mapErr(err)
	}
	entries := make([]fs.Entry, 0, len(infos))
	for _, fi := range infos {
		if fi.Name == "" || fi.Name == "." || fi.Name == ".." ||
			strings.ContainsRune(fi.Name, '/') || strings.ContainsRune(fi.Name, 0) {
			continue
		}
		attr := fs.Attr{Size: fi.Size, Rev: fi.Revision, Mode: fi.Mode & 0o7777, Mtime: time.Unix(fi.Mtime, 0), Nlink: fi.Nlink, ResourceID: fi.ResourceID, ExtentIno: fi.ExtentIno}
		if fi.IsDir {
			attr.Kind = fs.KindDir
			if !fi.HasMode {
				attr.Mode = 0o755
			}
		} else if fi.HasMode && fi.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
			attr.Kind = fs.KindSymlink
			// Keep the type bit: cached entries restore the symlink kind
			// from S_IFMT (a perms-only mask would retype the link as a
			// regular file and break follow semantics).
			attr.Mode = uint32(syscall.S_IFLNK) | 0o777
		} else if !fi.HasMode {
			attr.Mode = 0o644
		}
		entries = append(entries, fs.Entry{Name: fi.Name, Attr: attr})
	}
	return fs.Result{Entries: entries}, nil
}

// createOp serves OpCreate (open-or-create). The existence probe goes
// through statOp: the namespace cache answers paths this mount just
// created/committed, so an open racing backend replica lag reads the local
// view instead of a stale remote stat.
func (c *Core) createOp(ctx context.Context, op *fs.Op) (fs.Result, error) {
	rp := c.remotePath(op.Path)
	probe, statErr := c.statOp(ctx, op.Path)
	exists := statErr == nil && probe.Attr != nil
	if statErr != nil && statErr != fs.ErrNotFound {
		return fs.Result{}, statErr
	}
	var st *client.StatResult
	if exists {
		st = &client.StatResult{
			IsDir: probe.Attr.IsDir(), Size: probe.Attr.Size,
			Revision: probe.Attr.Rev, Mtime: probe.Attr.Mtime,
			Nlink: probe.Attr.Nlink,
		}
	}
	flags := op.Flags
	if flags&uint32(syscall.O_CREAT) != 0 {
		if exists && flags&uint32(syscall.O_EXCL) != 0 {
			return fs.Result{}, syscall.EEXIST
		}
		if !exists {
			commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
			_, err := c.cl.CreateFileCtx(commitCtx, rp)
			cancel()
			if err != nil {
				// A conflict against a path this mount believes gone is
				// backend replica lag (create-after-unlink/rename races a
				// stale replica): re-probe detached and retry once before
				// surfacing EEXIST (v1 createFile conflict tolerance).
				if errors.Is(err, client.ErrConflict) {
					// Bounded backoff: cross-pod replica lag on this
					// backend outlasts sub-second ladders — budget a few
					// seconds before surfacing EEXIST (v1's retry ladder).
					for range 12 {
						if _, serr := c.remoteStatDetached(rp); serr != nil && client.IsNotFound(serr) {
							retryCtx, rcancel := c.namespaceMutationCommitContext(ctx)
							_, err = c.cl.CreateFileCtx(retryCtx, rp)
							rcancel()
							if err == nil {
								st, statErr := c.cl.StatCtx(ctx, rp)
								if statErr != nil {
									return fs.Result{}, c.mapErr(statErr)
								}
								return fs.Result{Attr: attrOf(st), Handle: c.newSyncHandle(op.Path)}, nil
							}
							if !errors.Is(err, client.ErrConflict) {
								break
							}
						}
						time.Sleep(250 * time.Millisecond)
					}
				}
				return fs.Result{}, c.mapErr(err)
			}
			// An explicit create mode is always meaningful (mode 0 must be
			// applied, not skipped: pjdfstest creates 00-mode files); the
			// full 0o7777 range keeps SUID/SGID at create (chmod/12).
			if mode := op.Mode & 0o7777; mode != 0o644 {
				if err := retryPostUploadMode(ctx, func() error {
					// v1 masks the create-mode chmod to 0o777 at the
					// wire (remoteChmodMode); SUID/SGID stays local.
					return c.cl.ChmodCtx(ctx, rp, mode&0o777)
				}); err != nil {
					return fs.Result{}, c.mapErr(err)
				}
			}
			st, statErr = c.cl.StatCtx(ctx, rp)
			if statErr != nil {
				return fs.Result{}, c.mapErr(statErr)
			}
		}
	} else if !exists {
		return fs.Result{}, fs.ErrNotFound
	}
	// open(O_TRUNC) truncates locally only: truncation is published
	// at a commit boundary above the terminal — PUTting the remote here
	// would change crash semantics (an open that crashes before any write
	// must leave the remote intact).
	return fs.Result{Attr: attrOf(st), Handle: c.newSyncHandle(op.Path)}, nil
}

// setAttrOp serves OpSetAttr. The probe comes first: it yields the kind (directory chmod goes to the
// trailing-slash form), the truncate base revision, and the reply
// attributes — no extra remote stat follows the mutations.
func (c *Core) setAttrOp(ctx context.Context, op *fs.Op) (fs.Result, error) {
	probe, statErr := c.statOp(ctx, op.Path)
	if statErr != nil {
		return fs.Result{}, statErr
	}
	if probe.Attr == nil {
		return fs.Result{}, fs.ErrNotFound
	}
	attr := *probe.Attr
	rp := c.remotePath(op.Path)
	if op.HasMode {
		mode := op.Mode & 0o7777
		target := rp
		if attr.IsDir() {
			target = remoteDirectoryPath(rp)
		}
		commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
		// retryPostUploadMode (v1 Setattr): a chmod can 404 while this
		// mount's own async create/upload is still landing (writeback's
		// RELEASE races ahead of the queue) — retry across that window
		// before failing, exactly like v1 did.
		err := retryPostUploadMode(commitCtx, func() error {
			// v1 remoteChmodMode: permission bits only over the wire.
			return c.cl.ChmodCtx(commitCtx, target, mode&0o777)
		})
		cancel()
		if err != nil && !c.chmodNotFoundCanStayLocal(rp, target, err) {
			return fs.Result{}, c.mapErr(err)
		}
		attr.Mode = mode
		// The chmod bumps the remote revision; the probed (cached) rev is
		// stale now. Refresh it so a follow-up conditional write (truncate)
		// does not CAS against the pre-chmod revision and 409.
		if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
			attr.Rev = st.Revision
		}
	}
	if op.Size >= 0 && attr.Size != op.Size {
		if op.Size > maxTruncateImageSize {
			// Whole-image truncate materializes the target; absurd sizes
			// (the kernel can forward near-2^63 lengths) must fail fast
			// instead of attempting the allocation.
			return fs.Result{}, fs.StatusErrorOf(gofuse.Status(syscall.EFBIG))
		}
		// Truncate (v1): read only the kept prefix — min(newSize, oldSize)
		// — and zero-extend to the target, so a shrink or a truncate-to-
		// zero never downloads the discarded tail. The commit CASes on the
		// probed revision directly: the buffered-write path would re-read
		// the whole old image for a shrink (mergeAt), defeating the prefix
		// read.
		keep := op.Size
		if attr.Size < keep {
			keep = attr.Size
		}
		var old []byte
		if keep > 0 {
			var err error
			old, err = c.cl.ReadAtCtx(ctx, rp, 0, keep)
			if err != nil {
				return fs.Result{}, c.mapErr(err)
			}
		}
		next := make([]byte, op.Size)
		copy(next, old)
		// A cached namespace row may carry no revision (Rev 0); a
		// conditional commit with revision 0 tells the server "expect the
		// path to be absent" and fails with a conflict. Take a fresh
		// detached stat before CASing.
		if attr.Rev <= 0 {
			if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
				attr.Rev = st.Revision
			}
		}
		// v1 write-sync: commits go through the streaming upload path,
		// which routes payloads over the server's inline threshold through
		// the multipart protocol with part checksums — a direct conditional
		// PUT is rejected there with "missing X-Dat9-Part-Checksums".
		// The truncate CASes on the probed revision; a just-landed async
		// create can leave the probe's replica view stale (409 → EIO,
		// truncate/05). Re-stat and re-CAS like the commit paths (v1).
		var werr error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
					attr.Rev = st.Revision
				}
			}
			werr = c.cl.WriteStreamConditional(ctx, rp, bytes.NewReader(next), int64(len(next)), nil, attr.Rev)
			if werr == nil {
				break
			}
			if !errors.Is(werr, client.ErrConflict) {
				return fs.Result{}, c.mapErr(werr)
			}
		}
		if werr != nil {
			return fs.Result{}, c.mapErr(werr)
		}
		attr.Size = op.Size
		if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
			attr.Rev = st.Revision
		}
		c.emitLanded(op.Path, attr.Rev, op.Size)
	}
	// op.Mtime: the backend derives mtimes from writes and has no mtime
	// verb, so timestamp changes are accepted as no-ops rather than lying
	// with an error to every utimens(2).
	return fs.Result{Attr: &attr}, nil
}

// mknodRegular creates a regular file: create-on-missing with EEXIST on
// collision, a conditional chmod for the requested permissions, and a
// rollback delete when the mode application fails.
func (c *Core) mknodRegular(ctx context.Context, op *fs.Op) (fs.Result, error) {
	rp := c.remotePath(op.Path)
	// The existence probe goes through statOp so this mount's own negative
	// rows answer first: a recreate right after this mount deleted the name
	// must not race backend replica lag on the stale remote stat.
	_, statErr := c.statOp(ctx, op.Path)
	if statErr == nil {
		return fs.Result{}, syscall.EEXIST
	} else if !fs.IsNotFound(statErr) {
		return fs.Result{}, statErr
	}
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	_, createErr := c.cl.CreateFileCtx(commitCtx, rp)
	cancel()
	if createErr != nil {
		return fs.Result{}, c.mapErr(createErr)
	}
	// mknod carries the caller's permission bits explicitly:
	// mknod(S_IFREG|0000) must create an inaccessible file, not silently
	// widen it to 0644.
	mode := op.Mode & 0o777
	rollback := func() { _ = c.cl.DeleteFileCtx(context.WithoutCancel(ctx), rp) }
	if mode != 0o644 || op.Mode&0o777 == 0 {
		if err := retryPostUploadMode(ctx, func() error {
			return c.cl.ChmodCtx(ctx, rp, mode)
		}); err != nil {
			rollback()
			return fs.Result{}, c.mapErr(err)
		}
	}
	st, err := c.cl.StatCtx(ctx, rp)
	if err != nil {
		rollback()
		return fs.Result{}, c.mapErr(err)
	}
	return fs.Result{Attr: attrOf(st)}, nil
}

// readOp serves OpRead: a range (limit>0) or whole-file fetch through the
// shared read machinery, slicing whole-file results by the offset.
func (c *Core) readOp(ctx context.Context, path string, off, limit int64) (fs.Result, error) {
	data, err := c.fetchRemote(ctx, c.remotePath(path), off, limit)
	if err != nil {
		return fs.Result{}, c.mapErr(err)
	}
	if limit <= 0 && off > 0 {
		if off >= int64(len(data)) {
			data = nil
		} else {
			data = data[off:]
		}
	}
	return fs.Result{Data: data}, nil
}

// fetchRemote fetches content through the mount-wide singleflight and slot
// limiter with bounded transient retries (request dedup, mount-wide
// backpressure, and detached retry loops). limit>0 reads one range;
// limit<=0 reads the whole file.
func (c *Core) fetchRemote(ctx context.Context, rp string, off, limit int64) ([]byte, error) {
	key := rp + "@" + strconv.FormatInt(off, 10) + "@" + strconv.FormatInt(limit, 10)
	data, err, _ := c.readFlight.Do(ctx, key, func() ([]byte, error) {
		// The shared fetch runs detached from the owner's context:
		// cancelling one piggybacking FUSE request must not fail the
		// fetch every other waiter shares; a fresh bound keeps it finite.
		fctx, cancel := context.WithTimeout(context.Background(), remoteFetchTimeout)
		defer cancel()
		data, err := c.readOnce(fctx, rp, off, limit)
		if err == nil || !isTransientReadErr(err) {
			return data, err
		}
		var lastErr = err
		for range readTransientRetryCount {
			retryCtx, retryCancel := context.WithTimeout(context.Background(), readTransientRetryTimeout)
			data, err = c.readOnce(retryCtx, rp, off, limit)
			retryCancel()
			if err == nil {
				return data, nil
			}
			if !isTransientReadErr(err) {
				return nil, err
			}
			lastErr = err
		}
		return nil, fmt.Errorf("%w: %s: %v", errReadRetriesExhausted, rp, lastErr)
	})
	return data, err
}

// readOnce performs one bounded fetch attempt under the slot limiter
// request: wait a concurrency slot before the HTTP fetch so the
// backend is not stampeded.
func (c *Core) readOnce(ctx context.Context, rp string, off, limit int64) ([]byte, error) {
	select {
	case c.readSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.readSlots }()
	start := time.Now()
	var (
		data []byte
		err  error
	)
	if limit > 0 {
		data, err = c.cl.ReadAtCtx(ctx, rp, off, limit)
	} else {
		data, err = c.cl.ReadCtx(ctx, rp)
	}
	c.recordRemote("read", start, err, uint64(len(data)))
	return data, err
}

// mergeAt builds the whole-file image for a write at off. The overwrite
// fast path skips the read when the write covers the whole file.
func (c *Core) mergeAt(ctx context.Context, rp string, st *client.StatResult, off int64, data []byte) ([]byte, error) {
	if off == 0 && int64(len(data)) >= st.Size {
		return data, nil
	}
	old, err := c.cl.ReadCtx(ctx, rp)
	if err != nil {
		return nil, c.mapErr(err)
	}
	end := off + int64(len(data))
	if end > int64(len(old)) {
		grown := make([]byte, end)
		copy(grown, old)
		old = grown
	}
	copy(old[off:], data)
	return old, nil
}

// namespaceMutationCommitContext detaches a namespace-mutation commit from
// the FUSE cancel channel: a kernel interrupt after the request hit
// the wire must not abort the server-side commit into an ambiguous state.
// The mount-wide timeout stays clamped to the caller's own deadline; the
// legacy-interruptible mode keeps the caller's context.
func (c *Core) namespaceMutationCommitContext(parent context.Context) (context.Context, context.CancelFunc) {
	if c.legacyInterruptibleMutations {
		return parent, func() {}
	}
	timeout := mutationCommitTimeout
	if deadline, ok := parent.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}

// dirNotEmpty retry knobs: the lag window between a child deletion and a
// parent rmdir is short; a few quick probes cover it.
const (
	dirNotEmptyRetryCount = 8
	dirNotEmptyRetryDelay = 100 * time.Millisecond
)

// maxTruncateImageSize bounds a whole-image truncate: 1 TiB matches the
// StatFs capacity the facade advertises.
const maxTruncateImageSize = 1 << 40

// remoteDirectoryPath returns the directory form of a remote path:
// directories carry a trailing slash on the wire.
func remoteDirectoryPath(p string) string {
	if p == "/" || strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

// remoteStatDetached stats a remote path detached from any request
// context, bounded by the mutation retry timeout.
func (c *Core) remoteStatDetached(rp string) (*client.StatResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	defer cancel()
	return c.cl.StatCtx(ctx, rp)
}

// remoteDirExistsDetached reports whether a directory landed remotely.
func (c *Core) remoteDirExistsDetached(path string) bool {
	st, err := c.remoteStatDetached(c.remotePath(path))
	return err == nil && st.IsDir
}

// remotePathGoneDetached reports whether a path is already gone remotely.
func (c *Core) remotePathGoneDetached(path string) bool {
	_, err := c.remoteStatDetached(c.remotePath(path))
	return client.IsNotFound(err)
}

// remoteRenameCommittedDetached reports whether a rename landed: target
// visible AND source gone — target visibility alone is not enough because
// server-side rename replaces an existing target.
func (c *Core) remoteRenameCommittedDetached(oldP, newP string) bool {
	if _, err := c.remoteStatDetached(c.remotePath(newP)); err != nil {
		return false
	}
	_, err := c.remoteStatDetached(c.remotePath(oldP))
	return client.IsNotFound(err)
}

// remoteHardlinkCommittedDetached reports whether a hardlink landed: both
// ends exist as files sharing one resource identity.
func (c *Core) remoteHardlinkCommittedDetached(srcP, dstP string) bool {
	dstStat, err := c.remoteStatDetached(c.remotePath(dstP))
	if err != nil || dstStat == nil || dstStat.IsDir || dstStat.ResourceID == "" {
		return false
	}
	srcStat, err := c.remoteStatDetached(c.remotePath(srcP))
	if err != nil || srcStat == nil || srcStat.IsDir || srcStat.ResourceID == "" {
		return false
	}
	return srcStat.ResourceID == dstStat.ResourceID
}

// mkdirRemote commits a directory creation with transient recovery: a
// transient failure first probes detached whether the directory actually
// landed (an interrupted or blipped request may have committed), then
// retries with conflict shortcuts.
func (c *Core) mkdirRemote(ctx context.Context, path string, mode uint32) error {
	rp := c.remotePath(path)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.MkdirCtx(commitCtx, rp, mode)
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if c.remoteDirExistsDetached(path) {
		return nil
	}
	if c.statRetryCount <= 0 {
		return err
	}
	lastErr := err
	for range c.statRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		err = c.cl.MkdirCtx(retryCtx, rp, mode)
		retryCancel()
		if err == nil {
			return nil
		}
		if isConflictErr(err) && c.remoteDirExistsDetached(path) {
			return nil
		}
		if !isTransientMutationErr(err) {
			return err
		}
		if c.remoteDirExistsDetached(path) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// deleteRemote commits a deletion with interrupt recovery: after a
// transient failure it probes detached whether the path is already gone
// and reports success; it deliberately does NOT retry — another actor may
// have recreated the name after the ambiguous first delete.
func (c *Core) deleteRemote(ctx context.Context, path string, dir bool) error {
	rp := c.remotePath(path)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	var err error
	if dir {
		err = c.cl.DeleteDirCtx(commitCtx, rp)
	} else {
		err = c.cl.DeleteFileCtx(commitCtx, rp)
	}
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		if dir && isDirNotEmptyErr(err) {
			// A just-issued child deletion may not be visible yet on the
			// pod serving this rmdir (replica lag): the deferred-unlink
			// flush issues delete-then-rmdir back to back. Bounded retry
			// before surfacing ENOTEMPTY.
			for range dirNotEmptyRetryCount {
				time.Sleep(dirNotEmptyRetryDelay)
				dctx, dcancel := c.namespaceMutationCommitContext(ctx)
				retry := c.cl.DeleteDirCtx(dctx, rp)
				dcancel()
				if retry == nil {
					return nil
				}
				if !isDirNotEmptyErr(retry) {
					return retry
				}
			}
		}
		return err
	}
	if c.remotePathGoneDetached(path) {
		return nil
	}
	return err
}

// isDirNotEmptyErr matches the backend's directory-not-empty conflict.
func isDirNotEmptyErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.Contains(err.Error(), "not empty")
	var se *client.StatusError
	if errors.As(err, &se) {
		return se.StatusCode == http.StatusConflict && msg
	}
	return msg
}

// renameRemote commits a rename with transient recovery: a transient
// failure probes whether the rename committed, then retries with
// not-found and transient shortcuts.
func (c *Core) renameRemote(ctx context.Context, oldP, newP string) error {
	oldRemote, newRemote := c.remotePath(oldP), c.remotePath(newP)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.RenameCtx(commitCtx, oldRemote, newRemote)
	cancel()
	if err != nil && client.IsNotFound(err) {
		// v1 renameRemoteFileToMissingTargetFallback: some backends 404 a
		// rename whose source is a zero-byte single-link file landing on a
		// missing target; create the target and delete the source instead
		// of surfacing ENOENT.
		if handled, ferr := c.renameMissingTargetFallback(ctx, oldP, newP); handled {
			return ferr
		}
	}
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if c.remoteRenameCommittedDetached(oldP, newP) {
		return nil
	}
	if c.statRetryCount <= 0 {
		return err
	}
	lastErr := err
	for range c.statRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		err = c.cl.RenameCtx(retryCtx, oldRemote, newRemote)
		retryCancel()
		if err == nil {
			return nil
		}
		if client.IsNotFound(err) && c.remoteRenameCommittedDetached(oldP, newP) {
			return nil
		}
		if !isTransientMutationErr(err) {
			return err
		}
		if c.remoteRenameCommittedDetached(oldP, newP) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// renameMissingTargetFallback ports v1 renameRemoteFileToMissingTargetFallback:
// after the rename itself 404s, it applies only when the source is a
// zero-byte, single-link regular file and the target is absent — create the
// target (create-POST, falling back to a conditional empty write when the
// action is unsupported), then delete the source. handled=false leaves the
// original 404 to surface through the caller.
func (c *Core) renameMissingTargetFallback(ctx context.Context, oldP, newP string) (handled bool, err error) {
	st, serr := c.remoteStatDetached(c.remotePath(oldP))
	if serr != nil || st == nil || st.IsDir || st.Size != 0 || st.Nlink > 1 {
		return false, nil
	}
	if _, terr := c.remoteStatDetached(c.remotePath(newP)); terr == nil {
		return false, nil // target landed after all; surface the original 404
	}
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	defer cancel()
	_, createErr := c.cl.CreateFileCtx(commitCtx, c.remotePath(newP))
	if createErr != nil {
		if isCreateActionUnsupportedErr(createErr) {
			_, createErr = c.cl.WriteCtxConditionalWithRevision(commitCtx, c.remotePath(newP), nil, 0)
		}
		if createErr != nil {
			return false, nil
		}
	}
	if derr := c.deleteRemote(ctx, oldP, false); derr != nil && !client.IsNotFound(derr) {
		// v1: the create already committed; surface the delete failure
		// instead of silently leaving the source behind.
		return true, c.mapErr(derr)
	}
	return true, nil
}

// hardlinkRemote commits a hardlink with transient recovery: a transient
// failure probes whether the link landed (shared resource identity), then
// retries with conflict shortcuts.
func (c *Core) hardlinkRemote(ctx context.Context, srcP, dstP string) error {
	srcRemote, dstRemote := c.remotePath(srcP), c.remotePath(dstP)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.HardlinkCtx(commitCtx, srcRemote, dstRemote)
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if c.remoteHardlinkCommittedDetached(srcP, dstP) {
		return nil
	}
	if c.statRetryCount <= 0 {
		return err
	}
	lastErr := err
	for range c.statRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		err = c.cl.HardlinkCtx(retryCtx, srcRemote, dstRemote)
		retryCancel()
		if err == nil {
			return nil
		}
		if isConflictErr(err) && c.remoteHardlinkCommittedDetached(srcP, dstP) {
			return nil
		}
		if !isTransientMutationErr(err) {
			return err
		}
		if c.remoteHardlinkCommittedDetached(srcP, dstP) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// symlinkRemote commits a symlink with committed-probe recovery only: a
// transient failure probes detached whether the link landed and really is
// a symlink; there is no retry loop.
func (c *Core) symlinkRemote(ctx context.Context, target, path string) error {
	rp := c.remotePath(path)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.SymlinkCtx(commitCtx, target, rp)
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if st, probeErr := c.remoteStatDetached(rp); probeErr == nil && st != nil && st.HasMode &&
		st.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
		return nil
	}
	return err
}

// chmodNotFoundCanStayLocal tolerates a chmod 404: it is accepted when a
// detached stat still sees the target
// (backend replica lag, or the directory trailing-slash form), keeping the
// mode change mount-local instead of failing setattr.
func (c *Core) chmodNotFoundCanStayLocal(rp, chmodTarget string, chmodErr error) bool {
	if !client.IsNotFound(chmodErr) {
		return false
	}
	if _, err := c.remoteStatDetached(rp); err == nil {
		return true
	}
	if strings.HasSuffix(chmodTarget, "/") {
		withoutSlash := strings.TrimRight(chmodTarget, "/")
		if withoutSlash != "" && withoutSlash != chmodTarget {
			if _, err := c.remoteStatDetached(withoutSlash); err == nil {
				return true
			}
		}
	}
	return false
}

// retryPostUploadMode retries a mode application that can 404 against
// backend replica lag right after an upload, with a short bounded backoff
// before failing.
func retryPostUploadMode(ctx context.Context, apply func() error) error {
	var lastErr error
	for attempt := 0; attempt < postUploadModeAttempts; attempt++ {
		lastErr = apply()
		if lastErr == nil {
			return nil
		}
		if !client.IsNotFound(lastErr) || attempt == postUploadModeAttempts-1 {
			return lastErr
		}
		timer := time.NewTimer(postUploadModeInitialDelay << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

// normalizeRoot canonicalizes the configured remote root to "/" or a
// "/"-prefixed path without a trailing slash.
func normalizeRoot(root string) string {
	root = strings.TrimSpace(root)
	if root == "" || root == "/" {
		return "/"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return strings.TrimSuffix(root, "/")
}

// remotePath prefixes a mount path with the remote root.
func (c *Core) remotePath(p string) string {
	if c.root == "/" {
		return p
	}
	if p == "/" {
		return c.root
	}
	return c.root + p
}

// splitDirBase splits an absolute path into its parent directory and base
// name; the root parent keeps its slash.
func splitDirBase(p string) (dir, base string) {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return "", p
	}
	if i == 0 {
		return "/", p[1:]
	}
	return p[:i], p[i+1:]
}

// attrOf converts a backend stat into the framework attribute vocabulary.
func attrOf(st *client.StatResult) *fs.Attr {
	kind := fs.KindFile
	if st.IsDir {
		kind = fs.KindDir
	} else if st.HasMode && st.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
		kind = fs.KindSymlink
	}
	mode := st.Mode & 0o7777
	if !st.HasMode {
		mode = 0o644
		if kind == fs.KindDir {
			mode = 0o755
		}
	}
	return &fs.Attr{Kind: kind, Mode: mode, Size: st.Size, Rev: st.Revision, Mtime: st.Mtime, Nlink: st.Nlink, ResourceID: st.ResourceID, ExtentIno: st.ExtentIno}
}
