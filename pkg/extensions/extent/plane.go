package extent

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathfilter"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// Options configures the engine plane.
type Options struct {
	// Engine is the constructed bridge+store+vfs bundle.
	Engine *Engine
	// Threshold is the fresh-create claim cutoff in bytes. 0 claims every
	// create (full JuiceFS mode); a positive value only claims existing
	// extent files, leaving creation to the classic plane's promotion.
	// The mount wires hybrid mode: fresh creates ride the classic write
	// plane, which promotes large images to extent layout at the inline
	// cutoff.
	Threshold int64
	// Patterns are explicit path globs (--extent / the profile [extent]
	// section; the builtin "extent" profile uses ["*"]): fresh creates of
	// matching paths ride the engine plane from byte zero even in hybrid
	// mode.
	Patterns []string
	// Events, when set, drops the bridge's cached mappings when names
	// vanish or change underneath the engine.
	Events *fs.EventBus
	// WriteSync enforces remote publication before Write returns (--durability write-sync).
	WriteSync bool
}

// Handler routes extent files onto the engine plane.
type Handler struct {
	opts Options

	mu        sync.Mutex
	handles   map[string][]*engineHandle
	pathLocks sync.Map // mount-local path -> *sync.Mutex

	// patterns (compiled once from opts.Patterns) — explicit
	// --extent /[extent] routing for fresh creates.
	patterns []pathfilter.Pattern

	// onSentinelWait is an optional unit-test hook invoked once when
	// waitClosingSentinelsInDir observes an active/dropping sentinel and enters
	// its wait path.
	onSentinelWait func()
	// flushGrace overrides sentinelFlushDispatchGrace in unit tests when > 0.
	flushGrace time.Duration
}

// New builds the engine-plane handler.
func New(opts Options) *Handler {
	h := &Handler{opts: opts, handles: make(map[string][]*engineHandle)}
	if opts.Events != nil {
		// A rename rides EventWrite for the OLD name with RenamedTo set
		// (pkg/fuse mutatedRename), followed by a plain EventDelete for
		// the old name. Open engine handles must follow the file: retarget
		// handle bookkeeping and the bridge mapping on the rename event;
		// the trailing EventDelete then finds the handles already moved
		// and only forgets the (now unmapped) old path.
		opts.Events.On(fs.EventWrite, func(ev fs.Event) {
			if ev.Path != "" && ev.RenamedTo != "" {
				h.retarget(ev.Path, ev.RenamedTo)
			}
		})
		opts.Events.On(fs.EventDelete, func(ev fs.Event) {
			if ev.Path == "" || ev.RenamedTo != "" {
				return
			}
			h.mu.Lock()
			_, open := h.handles[ev.Path]
			h.mu.Unlock()
			if !open {
				h.drop(ev.Path)
			}
		})
		opts.Events.On(fs.EventInvalidated, func(ev fs.Event) {
			if ev.Path == "" {
				return
			}
			h.mu.Lock()
			_, open := h.handles[ev.Path]
			h.mu.Unlock()
			if !open {
				h.drop(ev.Path)
			}
		})
	}
	return h
}

// lockPaths takes the per-path serialization locks for the given paths (a
// rename touches two), in a fixed order. Path-addressed plane operations —
// claiming an open, truncating, unlink retention, rename-over retention —
// must not interleave for the same name: an open straddling a retention
// boundary would attach a descriptor for the retained file onto the
// replaceable name, and nothing afterwards can tell that descriptor from
// the arriving file's own.
func (h *Handler) lockPaths(paths ...string) func() {
	uniq := make([]string, 0, len(paths))
	for _, p := range paths {
		if p != "" {
			uniq = append(uniq, p)
		}
	}
	sort.Strings(uniq)
	var held []*sync.Mutex
	for _, p := range uniq {
		any, _ := h.pathLocks.LoadOrStore(p, &sync.Mutex{})
		mu := any.(*sync.Mutex)
		mu.Lock()
		held = append(held, mu)
	}
	return func() {
		for i := len(held) - 1; i >= 0; i-- {
			held[i].Unlock()
		}
	}
}

// retarget moves every open handle (and the bridge mapping) from the old
// name to the renamed one. A directory rename arrives as ONE event for the
// directory, so every open DESCENDANT follows too — otherwise a later
// create at the old child name registers next to (and aliases with) the
// retained descriptor.
func (h *Handler) retarget(oldPath, newPath string) {
	h.retargetHandles(oldPath, newPath)
	h.opts.Engine.Bridge.RemapLocal(oldPath, newPath)
}

// retargetHandles moves only the handle bookkeeping (see retarget); used
// where the caller manages the bridge mappings itself.
func (h *Handler) retargetHandles(oldPath, newPath string) {
	type moved struct{ from, to string }
	var moves []moved
	h.mu.Lock()
	for p := range h.handles {
		switch {
		case p == oldPath:
			moves = append(moves, moved{p, newPath})
		case strings.HasPrefix(p, oldPath+"/"):
			moves = append(moves, moved{p, newPath + p[len(oldPath):]})
		}
	}
	for _, m := range moves {
		list := h.handles[m.from]
		delete(h.handles, m.from)
		for _, eh := range list {
			eh.path = m.to
		}
		h.handles[m.to] = append(h.handles[m.to], list...)
	}
	h.mu.Unlock()
}

func (h *Handler) drop(path string) {
	h.mu.Lock()
	delete(h.handles, path)
	h.mu.Unlock()
	h.opts.Engine.Bridge.ForgetPath(path)
}

// claimFinalClose reserves the right to publish the file's final state for
// this path, returning true to exactly ONE handle even when closes overlap.
//
// A plain "is any other handle registered?" check is racy: two releases can
// each observe the other still registered and both decline, leaving the file
// with no owner of its final publication. Marking the caller FIRST and then
// counting the still-unreleased handles gives a total order — the caller
// that observes zero is the one whose mark made the set empty, and every
// earlier caller observed itself excluded.
func (h *Handler) claimFinalClose(e *engineHandle) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	e.released = true
	for _, x := range h.handles[e.path] {
		if x != e && !x.released {
			return false
		}
	}
	return true
}

// release removes one handle for a path; the bridge mapping is only
// forgotten when the LAST handle for the path closes.
func (h *Handler) release(e *engineHandle) {
	h.mu.Lock()
	list := h.handles[e.path]
	for i, x := range list {
		if x == e {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) > 0 {
		h.handles[e.path] = list
		h.mu.Unlock()
		return
	}
	delete(h.handles, e.path)
	h.mu.Unlock()
	// Advisory locks are NOT dropped here. Each descriptor's own FLUSH and
	// RELEASE go through JuiceFS's VFS, which records which owner a handle
	// holds locks for and unlocks exactly that owner (h.locks/h.ofdOwner/
	// flockOwner in vfs/handle.go). An inode-wide sweep at the last close of
	// one PATH would be both redundant and wrong: on a hardlink, the path
	// closing is not the inode going away, and the sweep would release a live
	// descriptor's locks — letting a second writer in while the first still
	// believes it holds the file.
	h.opts.Engine.Bridge.ForgetPath(e.path)
	h.opts.Engine.Bridge.DropDeletedOpen(e.path)
}

func vfsCtx(c fs.Caller) vfs.LogContext {
	return vfs.NewLogContext(meta.NewContext(c.Pid, c.Uid, []uint32{c.Gid}))
}

func vfsCtxWith(ctx context.Context, c fs.Caller) vfs.LogContext {
	if ctx == nil {
		return vfsCtx(c)
	}
	return vfs.NewLogContext(meta.WrapWithoutCancel(ctx, c.Pid, c.Uid, []uint32{c.Gid}))
}

// openExistingLocked opens an engine handle for a path ALREADY known to hold
// extent layout. The caller holds the path lock.
func (h *Handler) openExistingLocked(ctx context.Context, path string, flags uint32, c fs.Caller) (fs.Handle, *fs.Attr, syscall.Errno) {
	ino, errno := h.opts.Engine.ResolvePath(ctx, path)
	if errno != 0 {
		return nil, nil, errno
	}
	if flags&uint32(syscall.O_TRUNC) != 0 {
		if errno := h.opts.Engine.VFS.Truncate(vfsCtx(c), meta.Ino(ino), 0, 0, nil); errno != 0 {
			return nil, nil, errno
		}
		if err := h.opts.Engine.Bridge.TruncateLocal(ctx, path, 0); err != nil {
			if e, ok := err.(syscall.Errno); ok {
				return nil, nil, e
			}
			return nil, nil, syscall.EIO
		}
	}
	_, fh, errno := h.opts.Engine.VFS.Open(vfsCtx(c), meta.Ino(ino), flags)
	if errno != 0 {
		return nil, nil, errno
	}
	handle := h.register(path, ino, fh, c)
	// Creation attributes let the facade populate its namespace cache (a
	// handle-only reply leaves the entry uncached).
	attr := &fs.Attr{Kind: fs.KindFile, Nlink: 1}
	if st, serr := h.opts.Engine.Bridge.StatLocal(ctx, path); serr == nil {
		attr.Size = st.Size
		// The revision is the compare-and-set base a write needs. Dropping
		// it made every later overwrite carry baseRev 0 ("unknown"), and a
		// base-0 overwrite is refused outright — the write-back plane then
		// never publishes and the file stays at its opened size. An empty
		// database file hit exactly that.
		attr.Rev = st.Revision
		if st.HasMode {
			attr.Mode = st.Mode
		}
	}
	if flags&uint32(syscall.O_TRUNC) != 0 {
		attr.Size = 0
	}
	return handle, attr, 0
}

// OpenExisting hands an engine handle to a caller that promoted the file to
// extent layout AFTER its descriptor was opened on another plane — the write
// back plane's promotion does exactly that, and without this an open
// descriptor keeps re-uploading its whole image at every fsync because its
// classic handle never learns the remote is now extent.
//
// Reports ok=false (with a nil error) when the path is not extent layout or
// holds a different revision than the caller expects, so the caller keeps
// its current plane rather than adopting someone else's object.
// The returned handle has no kernel fd; it is driven through fs.Handle only.
//
// LOCKING: this deliberately does NOT take h.lockPaths. Its only caller is
// the write-back plane's promotion migration, which holds ITS per-path lock
// across the call; taking the extent path lock here would create the cycle
//
//	migration:  writeback path lock -> extent path lock
//	rename:     extent path lock    -> writeback path lock (via next -> flush)
//
// which is an ABBA deadlock over two mutexes (no RPC deadline can break it).
// The order that IS allowed is extent -> writeback, because rename needs to
// flush the source writer while it holds the namespace lock. Everything this
// function does is therefore lock-free with respect to the extent plane; the
// revision check is what keeps a rename/replacement from being adopted, and
// the caller re-checks the handle's state under its own lock before
// installing the result.
func (h *Handler) OpenExisting(ctx context.Context, path string, flags uint32, expectResourceID string, expectRevision int64, c fs.Caller) (fs.Handle, bool, error) {
	if h == nil || h.opts.Engine == nil {
		return nil, false, nil
	}
	st, err := h.opts.Engine.Bridge.StatLocal(ctx, path)
	if err != nil {
		if client.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if st.ContentLayout != client.ContentLayoutExtent {
		return nil, false, nil
	}
	// The promotion must still describe the object at this name: a
	// replacement landed since is a different file, and migrating onto it
	// would silently adopt its bytes. The RESOURCE IDENTITY is the proof —
	// it names the object the commit produced. Revision equality is only a
	// fallback for servers that do not echo one, and it is a weak proxy: a
	// revision can advance for reasons that have nothing to do with who
	// wrote the bytes.
	if expectResourceID != "" {
		if st.ResourceID != expectResourceID {
			return nil, false, nil
		}
	} else if expectRevision > 0 && st.Revision != expectRevision {
		return nil, false, nil
	}
	// The descriptor was opened BEFORE the promotion and every write it made
	// is already in the staged image the promotion just published. Replaying
	// the open's truncate/create flags here would discard that image (O_TRUNC)
	// or re-run create semantics this plane must not repeat. Only the access
	// mode and append flag carry over.
	handle, _, errno := h.openExistingLocked(ctx, path, migrateOpenFlags(flags), c)
	if errno != 0 {
		return nil, false, errno
	}
	return handle, true, nil
}

// migrateOpenFlags reduces an original open's flags to the ones that describe
// ONGOING access: the mode bits (read/write) plus append. Create and truncate
// intent was already satisfied by the original open, and re-applying it to a
// live object is destructive.
func migrateOpenFlags(flags uint32) uint32 {
	return flags & uint32(syscall.O_ACCMODE|syscall.O_APPEND)
}

// Do claims extent files (and, in full mode, fresh creates); everything
// else passes through.
func (h *Handler) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpList:
		res, err := next(ctx, op)
		if err != nil || len(res.Entries) == 0 || h.opts.Engine == nil || h.opts.Engine.Bridge == nil {
			return res, err
		}
		br := h.opts.Engine.Bridge
		filtered := res.Entries[:0]
		for _, entry := range res.Entries {
			if strings.Contains(entry.Name, ".d9open-") && br.IsSentinel(childPath(op.Path, entry.Name)) {
				continue
			}
			filtered = append(filtered, entry)
		}
		res.Entries = filtered
		return res, nil
	case fs.OpUnlink, fs.OpRmdir:
		if op.Kind == fs.OpRmdir {
			h.waitClosingSentinelsInDir(ctx, op.Path)
		}
		// Unlink of a file with live engine handles: plans and commits
		// resolve through the pathname, so a plain delete would break the
		// open descriptor (and a later flush would resurrect the name).
		// Silly-rename instead: move the remote name to a hidden sentinel
		// the bridge keeps using, and delete the sentinel when the last
		// handle closes (the plane's unlink never reaches the chain).
		// Serialized against concurrent claims of the same name.
		unlock := h.lockPaths(op.Path)
		h.mu.Lock()
		_, open := h.handles[op.Path]
		h.mu.Unlock()
		if !open {
			unlock()
			break
		}
		sentinel, err := h.opts.Engine.Bridge.KeepDeletedOpen(op.Path)
		if err != nil {
			unlock()
			return fs.Result{}, err
		}
		if sentinel != "" {
			// Follow the descriptor to the sentinel so the last
			// release can delete the registered keep-alive name.
			h.retarget(op.Path, sentinel)
		}
		unlock()
		return fs.Result{}, nil
	case fs.OpLookup:
		// A fresh engine create exists only in the bridge until its first
		// commit; the classic lookup would answer (and negatively cache)
		// ENOENT for a successfully created, still-open file.
		if attr, ok := h.lookupLocalOnly(ctx, op.Path); ok {
			return fs.Result{Attr: attr}, nil
		}
	case fs.OpCreate:
		res, nextOp, err, claimed := h.tryClaimOpen(ctx, op)
		if claimed {
			return res, err
		}
		if nextOp != nil {
			op = nextOp
		}
	case fs.OpLink:
		if res, err, handled := h.linkAfterLanding(ctx, op, next); handled {
			return res, err
		}
	case fs.OpRename:
		if res, err, handled := h.renameOverOpen(ctx, op, next); handled {
			return res, err
		}
	case fs.OpSetAttr:
		if op.HasSize {
			if res, err, handled := h.tryTruncate(ctx, op, next); handled {
				return res, err
			}
		}
		if res, handled := h.setAttrLocalOnly(ctx, op); handled {
			return res, nil
		}
	case fs.OpFallocate:
		// VFS owns this op end to end: it validates against the kernel's
		// limits, takes the handle's write lock, and drives Meta.Fallocate
		// (plus the writer/reader length bookkeeping) itself. Without this
		// case the op fell through to the chain terminal as ENOSYS — an
		// extent file answered "not supported" to an operation the engine
		// underneath fully implements.
		if res, err, handled := h.engineHandleOp(op, func(e *engineHandle) syscall.Errno {
			return h.opts.Engine.VFS.Fallocate(e.vctx, e.ino, uint8(op.Flags), op.Offset, op.Size, e.fh)
		}); handled {
			return res, err
		}
	case fs.OpCopyFileRange:
		if res, err, handled := h.copyFileRangeOverEngine(ctx, op); handled {
			return res, err
		}
	case fs.OpGetAttr:
		h.mu.Lock()
		_, open := h.handles[op.Path]
		h.mu.Unlock()
		if open {
			if attr, ok := h.lookupLocalOnly(ctx, op.Path); ok {
				return fs.Result{Attr: attr}, nil
			}
		}
		res, err := next(ctx, op)
		if err == nil && res.Attr != nil {
			h.mu.Lock()
			_, open := h.handles[op.Path]
			h.mu.Unlock()
			if open {
				if size, ok := h.opts.Engine.LocalAttr(ctx, op.Path); ok && size > res.Attr.Size {
					res.Attr.Size = size
				}
			}
		}
		return res, err
	}
	return next(ctx, op)
}

// engineHandleOp runs a descriptor-scoped operation against the engine handle
// that opened the op's fh, for ops JuiceFS's VFS already implements.
//
// The rule this encodes: the VFS is the implementation, this plane only
// ROUTES. Anything VFS exposes for an open file belongs there — it knows the
// handle's writer, reader and lock state, and it maintains them as a side
// effect of the call. Reimplementing such an op against the bridge (or, worse,
// letting it fall through to the chain terminal as ENOSYS) desynchronises that
// state from what the engine believes.
//
// The fh must match EXACTLY. Batch operations act on the object the kernel
// opened, and another descriptor at the same path may be a different
// incarnation (replaced, renamed, or opened read-only) — acting through it
// would apply the operation to the wrong open file.
//
// handled=false when this plane owns no such handle, so the op keeps its
// normal route (classic files are none of this plane's business).
func resolveEngineHandle(raw any) *engineHandle {
	cur := raw
	for cur != nil {
		if eh, ok := cur.(*engineHandle); ok {
			return eh
		}
		switch u := cur.(type) {
		case interface{ UnwrapHandle() fs.Handle }:
			next := u.UnwrapHandle()
			if next == nil || next == cur {
				return nil
			}
			cur = next
		case interface{ Unwrap() fs.Handle }:
			next := u.Unwrap()
			if next == nil || next == cur {
				return nil
			}
			cur = next
		default:
			return nil
		}
	}
	return nil
}

func (h *Handler) engineHandleOp(op *fs.Op, call func(*engineHandle) syscall.Errno) (fs.Result, error, bool) {
	if h.opts.Engine == nil {
		return fs.Result{}, nil, false
	}
	var live *engineHandle
	if op.Handle != nil {
		if eh := resolveEngineHandle(op.Handle); eh != nil && eh.plane == h {
			live = eh
		}
	} else if op.Fh != 0 {
		h.mu.Lock()
		live = h.handleForLocked(op.Path, op.Fh)
		h.mu.Unlock()
	}
	if live == nil {
		return fs.Result{}, nil, false
	}
	if errno := call(live); errno != 0 {
		return fs.Result{}, errno, true
	}
	return fs.Result{}, nil, true
}

// copyFileRangeOverEngine copies bytes between two open engine handles on this
// mount via their VFS reader/writer paths.
func (h *Handler) copyFileRangeOverEngine(ctx context.Context, op *fs.Op) (fs.Result, error, bool) {
	if h.opts.Engine == nil {
		return fs.Result{}, nil, false
	}
	var src, dst *engineHandle
	if op.Handle != nil {
		if eh := resolveEngineHandle(op.Handle); eh != nil && eh.plane == h {
			src = eh
		}
	} else if op.Fh != 0 {
		h.mu.Lock()
		src = h.handleForLocked(op.Path, op.Fh)
		h.mu.Unlock()
	}
	if op.Handle2 != nil {
		if eh := resolveEngineHandle(op.Handle2); eh != nil && eh.plane == h {
			dst = eh
		}
	} else if op.Fh2 != 0 && op.Path2 != "" {
		h.mu.Lock()
		dst = h.handleForLocked(op.Path2, op.Fh2)
		h.mu.Unlock()
	}
	if src == nil || dst == nil {
		return fs.Result{}, nil, false
	}
	if op.Flags != 0 {
		return fs.Result{}, syscall.EINVAL, true
	}
	if op.Limit < 0 || op.Offset < 0 || op.Offset2 < 0 {
		return fs.Result{}, syscall.EINVAL, true
	}
	if op.Limit == 0 {
		return fs.Result{Written: 0}, nil, true
	}
	if src.ino == dst.ino {
		inEnd := op.Offset + op.Limit
		outEnd := op.Offset2 + op.Limit
		if op.Offset < outEnd && op.Offset2 < inEnd {
			return fs.Result{}, syscall.EINVAL, true
		}
	}
	const maxChunk = 1 << 20
	bufLen := op.Limit
	if bufLen > maxChunk {
		bufLen = maxChunk
	}
	buf := make([]byte, bufLen)
	var total int64
	for total < op.Limit {
		step := op.Limit - total
		if step > int64(len(buf)) {
			step = int64(len(buf))
		}
		n, rerr := src.Read(ctx, op.Offset+total, buf[:step])
		if rerr != nil {
			if total > 0 {
				break
			}
			return fs.Result{}, rerr, true
		}
		if n == 0 {
			break
		}
		if errno := h.opts.Engine.VFS.Write(dst.vctx, dst.ino, buf[:n], uint64(op.Offset2+total), dst.fh); errno != 0 {
			if total > 0 {
				break
			}
			return fs.Result{}, errno, true
		}
		h.opts.Engine.Bridge.noteBufferedLength(dst.ino, op.Offset2+total+int64(n))
		total += int64(n)
		if int64(n) < step {
			break
		}
	}
	if total > 0 && h.opts.WriteSync {
		if err := dst.Sync(ctx); err != nil {
			return fs.Result{}, err, true
		}
	}
	return fs.Result{Written: uint32(total)}, nil, true
}

// handleForLocked resolves the live engine handle for path and fh. Caller
// holds h.mu.
func (h *Handler) handleForLocked(path string, fh uint64) *engineHandle {
	for _, e := range h.handles[path] {
		if e.fh == fh {
			return e
		}
	}
	return nil
}

// TryClaimExistingOpen claims an open when the target ALREADY exists locally
// in the bridge or remotely with extent layout (used when an inner plane
// finished waiting on an asynchronous promotion and re-checks layout).
func (h *Handler) TryClaimExistingOpen(ctx context.Context, op *fs.Op) (fs.Result, bool, error) {
	if h == nil || h.opts.Engine == nil || op == nil {
		return fs.Result{}, false, nil
	}
	defer h.lockPaths(op.Path)()
	res, claimed, _, _, err := h.tryClaimExistingLocked(ctx, op, false)
	return res, claimed, err
}

// tryClaimOpen claims the op when the target is (or, in full/pattern-matched
// mode, will become) an engine file. When an existing remote classic/multipart
// occupant is discovered behind a kernel CREATE, nextOp carries its observed
// attributes so downstream planes open/truncate the occupant instead of
// treating the path as fresh.
func (h *Handler) tryClaimOpen(ctx context.Context, op *fs.Op) (fs.Result, *fs.Op, error, bool) {
	if h.opts.Engine == nil {
		return fs.Result{}, nil, nil, false
	}
	// Serialized against retention for this name (unlink or rename-over):
	// a claim straddling a retention boundary would attach a descriptor
	// for the retained file onto the replaceable name. The layout decision
	// is (re)made under the lock too — the file at the name may have been
	// replaced while this claim waited, and a classic replacement must
	// pass through to the classic plane, never ride the engine.
	defer h.lockPaths(op.Path)()
	claimsFresh := op.Flags&uint32(syscall.O_CREAT) != 0 &&
		(h.opts.Threshold == 0 || h.claimsPattern(op.Path))
	// Bulk extraction of ordinary non-matching files (unzip, tar, cp) passes
	// O_CREAT|O_TRUNC without O_EXCL and stays on the classic plane, so this
	// plane skips its remote StatLocal there. Whenever this plane WOULD claim a
	// fresh create on the engine (full extent mode or an explicit/profile
	// pattern such as SQLite), or on non-truncating / O_EXCL creates, we must
	// probe remote state first so an occupant created behind a cached negative
	// lookup keeps its true layout and CAS identity.
	skipRemoteStat := op.KernelCreate() && op.Flags&uint32(syscall.O_CREAT) != 0 &&
		op.Flags&uint32(syscall.O_EXCL) == 0 && op.Flags&uint32(syscall.O_TRUNC) != 0 &&
		!claimsFresh
	res, claimed, remoteClassic, nextOp, err := h.tryClaimExistingLocked(ctx, op, skipRemoteStat)
	if claimed {
		return res, nil, err, true
	}
	if remoteClassic {
		// An existing classic/multipart file must remain on the classic
		// plane even when opened with O_CREAT (e.g. sqlite3 O_RDWR|O_CREAT,
		// O_CREAT|O_TRUNC behind a cached negative lookup, or --profile extent).
		return fs.Result{}, nextOp, nil, false
	}
	// Fresh path: claim creates in full JuiceFS mode, or when the path
	// matches an explicit --extent / [extent] pattern; otherwise creation
	// stays on the classic plane (hybrid promotion).
	if !claimsFresh {
		return fs.Result{}, nil, nil, false
	}
	res, err = h.createOnEngine(ctx, op)
	return res, nil, err, true
}

func attrFromClassicStat(st *client.StatResult) fs.Attr {
	mode := st.Mode & 0o7777
	kind := fs.KindFile
	if st.IsDir {
		kind = fs.KindDir
		if !st.HasMode {
			mode = 0o755
		}
	} else if st.HasMode && st.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
		kind = fs.KindSymlink
		mode = uint32(syscall.S_IFLNK) | 0o777
	} else if !st.HasMode {
		mode = 0o644
	}
	return fs.Attr{
		Kind:       kind,
		Mode:       mode,
		Size:       st.Size,
		Rev:        st.Revision,
		Mtime:      st.Mtime,
		Nlink:      st.Nlink,
		ResourceID: st.ResourceID,
		ExtentIno:  st.ExtentIno,
	}
}

// tryClaimExistingLocked claims opens for files that already exist locally in
// the bridge or remotely with extent layout. Caller holds h.lockPaths(op.Path).
func (h *Handler) tryClaimExistingLocked(ctx context.Context, op *fs.Op, skipRemoteStat bool) (fs.Result, bool, bool, *fs.Op, error) {
	if !skipRemoteStat {
		st, err := h.opts.Engine.Bridge.StatLocal(ctx, op.Path)
		if err == nil {
			if st.ContentLayout != client.ContentLayoutExtent {
				// This bridge's own zero-write materialization occupies the
				// name (a claimed create's final close with no bytes): the
				// engine-created name stays on the engine plane. The claim
				// re-verifies the occupant is still the empty classic file;
				// the descriptor rides the engine as the fresh create it
				// semantically is, and its first commit replaces the
				// placeholder with an extent create (see Bridge.commit).
				// O_EXCL declines instead: the name exists, and the classic
				// plane's server-arbitrated EEXIST is the honest answer.
				if st.Size == 0 && op.Flags&uint32(syscall.O_EXCL) == 0 &&
					h.opts.Engine.Bridge.MatchesEmptyPlaceholder(op.Path, st) {
					if _, local := h.opts.Engine.Bridge.LocalResolve(op.Path); !local {
						res, err := h.createOnEngine(ctx, op)
						return res, true, false, nil, err
					}
					// An open engine handle already owns this placeholder in the
					// bridge (e.g. MaterializeEmpty during SQLite WAL startup);
					// fall through to the LocalResolve branch below so the
					// second handle joins the existing inode without resetting
					// freshCreate.
				} else {
					var nextOp *fs.Op
					if op.KernelCreate() {
						if h.opts.Events != nil {
							h.opts.Events.Emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
						}
						if !st.IsDir && st.Revision > 0 {
							cp := *op
							attr := attrFromClassicStat(st)
							cp.OpenAttr = &attr
							nextOp = &cp
						}
					}
					return fs.Result{}, false, true, nextOp, nil
				}
			} else {
				if op.Flags&uint32(syscall.O_CREAT) != 0 && op.Flags&uint32(syscall.O_EXCL) != 0 {
					return fs.Result{}, true, false, nil, syscall.EEXIST
				}
				if op.KernelCreate() && h.opts.Events != nil {
					h.opts.Events.Emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
				}
				handle, attr, errno := h.openExistingLocked(ctx, op.Path, op.Flags, op.Caller)
				if errno != 0 {
					return fs.Result{}, true, false, nil, errno
				}
				return fs.Result{Handle: handle, Attr: attr}, true, false, nil, nil
			}
		}
		if !client.IsNotFound(err) {
			return fs.Result{}, false, false, nil, nil
		}
	}
	// A file that exists ONLY in the bridge (a fresh engine create whose
	// remote materialization is pending) opens through the engine even
	// without O_CREAT — and a concurrent kernel CREATE (including O_TRUNC)
	// must join/truncate that local inode rather than allocating a second
	// unbound fresh create or falling through to the classic plane.
	if ino, local := h.opts.Engine.Bridge.LocalResolve(op.Path); local {
		if op.Flags&syscall.O_CREAT != 0 && op.Flags&syscall.O_EXCL != 0 {
			// The folded OpCreate contract includes create/open flags: an
			// exclusive create of an existing (still-local) file is EEXIST.
			return fs.Result{}, true, false, nil, syscall.EEXIST
		}
		if op.Flags&syscall.O_TRUNC != 0 {
			if errno := h.opts.Engine.VFS.Truncate(vfsCtx(op.Caller), ino, 0, 0, nil); errno != 0 {
				return fs.Result{}, true, false, nil, errno
			}
			if err := h.opts.Engine.Bridge.TruncateLocal(ctx, op.Path, 0); err != nil {
				return fs.Result{}, true, false, nil, err
			}
		}
		_, fh, errno := h.opts.Engine.VFS.Open(vfsCtx(op.Caller), ino, op.Flags)
		if errno != 0 {
			return fs.Result{}, true, false, nil, errno
		}
		handle := h.register(op.Path, uint64(ino), fh, op.Caller)
		attr := &fs.Attr{Kind: fs.KindFile, Nlink: 1}
		if size, mode, ok := h.opts.Engine.Bridge.LocalView(ino); ok {
			attr.Size = size
			attr.Mode = mode
		}
		if op.Flags&syscall.O_TRUNC != 0 {
			attr.Size = 0
		}
		// The revision is the compare-and-set base for later writes.
		if !skipRemoteStat {
			if st, serr := h.opts.Engine.Bridge.StatLocal(ctx, op.Path); serr == nil {
				attr.Rev = st.Revision
			}
		}
		return fs.Result{Handle: handle, Attr: attr}, true, false, nil, nil
	}
	return fs.Result{}, false, false, nil, nil
}

// createOnEngine claims the name as a fresh engine create: the bridge
// registers a local mapping (the server sees nothing until the first
// commit) and the descriptor must establish an attributable identity at
// its first publication. Shared by the ordinary pattern-matched create and
// the reclaim of this bridge's own zero-byte classic placeholder.
func (h *Handler) createOnEngine(ctx context.Context, op *fs.Op) (fs.Result, error) {
	parent, name := SplitParent(op.Path)
	parentIno, errno := h.opts.Engine.ResolvePath(ctx, parent)
	if errno != 0 {
		return fs.Result{}, errno
	}
	mode := op.Mode
	if !op.HasMode {
		mode = 0o644
	}
	_, fh, errno := h.opts.Engine.VFS.Create(vfsCtx(op.Caller), meta.Ino(parentIno), name, uint16(mode&0o7777), 0, op.Flags)
	if errno != 0 {
		return fs.Result{}, errno
	}
	ino, errno := h.opts.Engine.ResolvePath(ctx, op.Path)
	if errno != 0 {
		// The bridge registered the mapping during Create; resolve again
		// defensively (fresh files resolve locally).
		return fs.Result{}, errno
	}
	// This descriptor CREATED the name: its first publication must
	// establish an attributable identity before writing any occupant.
	h.opts.Engine.Bridge.MarkFreshCreate(meta.Ino(ino))
	handle := h.register(op.Path, ino, fh, op.Caller)
	return fs.Result{Handle: handle, Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Nlink: 1}}, nil
}

// lookupLocalOnly answers namespace operations for files that exist ONLY
// in the bridge: fresh engine creates claimed from byte zero do not
// materialize server-side until their first commit. Committed files answer
// the classic plane themselves.
func (h *Handler) lookupLocalOnly(ctx context.Context, path string) (*fs.Attr, bool) {
	if h.opts.Engine == nil {
		return nil, false
	}
	br := h.opts.Engine.Bridge
	ino, ok := br.LocalResolve(path)
	if !ok {
		return nil, false
	}
	if st, err := br.StatLocal(ctx, path); err == nil {
		_ = st
		return nil, false // remotely materialized: the classic plane answers
	} else if !client.IsNotFound(err) {
		return nil, false // undecided: do not shadow the classic plane
	}
	size, _, _ := br.LocalView(ino)
	mode := uint32(0o644)
	if m, ok := br.ModeOf(ino); ok {
		mode = m // a recorded mode is authoritative, including 0000
	}
	attr := &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size, Nlink: 1}
	if mt, ok := br.MtimeOf(ino); ok && !mt.IsZero() {
		attr.Mtime = mt
	}
	return attr, true
}

// setAttrLocalOnly absorbs mode/mtime updates on a fresh engine file that has
// not yet materialized server-side, preventing fchmod/futimens before close()
// from falling through to the classic terminal and returning ENOENT.
func (h *Handler) setAttrLocalOnly(ctx context.Context, op *fs.Op) (fs.Result, bool) {
	if h.opts.Engine == nil {
		return fs.Result{}, false
	}
	h.mu.Lock()
	_, open := h.handles[op.Path]
	h.mu.Unlock()
	if !open {
		return fs.Result{}, false
	}
	br := h.opts.Engine.Bridge
	ino, ok := br.LocalResolve(op.Path)
	if !ok {
		return fs.Result{}, false
	}
	if _, err := br.StatLocal(ctx, op.Path); err == nil || !client.IsNotFound(err) {
		return fs.Result{}, false
	}
	if op.HasMode {
		br.SetModeLocal(ino, op.Mode&0o7777)
	}
	if !op.Mtime.IsZero() {
		br.SetMtimeLocal(ino, op.Mtime)
	}
	size, _, _ := br.LocalView(ino)
	mode := uint32(0o644)
	if m, ok := br.ModeOf(ino); ok {
		mode = m
	}
	attr := &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size, Nlink: 1}
	if mt, ok := br.MtimeOf(ino); ok && !mt.IsZero() {
		attr.Mtime = mt
	}
	return fs.Result{Attr: attr}, true
}

// claimsPattern reports whether a fresh create of the path is explicitly
// routed to the engine plane (--extent / the profile [extent] section).
func (h *Handler) claimsPattern(path string) bool {
	if len(h.opts.Patterns) == 0 {
		return false
	}
	if h.patterns == nil {
		h.patterns = pathfilter.CompileAll(h.opts.Patterns)
	}
	cleaned, err := pathutil.Canonicalize(path)
	if err != nil {
		return false
	}
	cleaned = strings.TrimPrefix(cleaned, "/")
	for _, p := range h.patterns {
		if p.MatchCanonical(cleaned) {
			return true
		}
	}
	// A single-segment pattern (no '/') also claims by BASENAME at any
	// depth, agreeing with the write plane's promotion rule: the builtin
	// "extent" profile supplies "*", which must match mix/a.db, and
	// "*.db" must claim nested creates the same way the queue promotes
	// them.
	for _, raw := range h.opts.Patterns {
		raw = strings.TrimSpace(raw)
		if raw == "" || strings.Contains(raw, "/") {
			continue
		}
		if ok, _ := filepath.Match(raw, pathutil.BaseName(cleaned)); ok {
			return true
		}
	}
	return false
}

// linkAfterLanding publishes the SOURCE before the remote hardlink is issued.
//
// The server's hardlink resolves the source BY NAME, so the name has to exist
// remotely first. An extent create exists only in this bridge until its first
// commit, so an immediate `link` after `create(2)` — pjdfstest link/06, link/07
// and unlink/00 do exactly that — used to answer ENOENT even though create(2)
// had already succeeded. The write-back plane has the same rule for its own
// handles (writeback.Do, OpLink), but it cannot see engine handles: those live
// in this plane, so the flush has to happen here.
//
// handled=false leaves unrelated links to the chain.
func (h *Handler) linkAfterLanding(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error, bool) {
	if h.opts.Engine == nil || op.Path2 == "" {
		return fs.Result{}, nil, false
	}
	unlock := h.lockPaths(op.Path)
	h.mu.Lock()
	list := append([]*engineHandle(nil), h.handles[op.Path]...)
	h.mu.Unlock()
	if len(list) == 0 {
		unlock()
		return fs.Result{}, nil, false
	}
	// Land every descriptor on the source, in TWO steps:
	//
	//  1. VFS.Flush pushes the JuiceFS writer's buffered bytes down to the
	//     bridge. Without it the bytes (and the new length) can still live
	//     only in the community writer: the bridge then sees size 0 and no
	//     pending slice, materializes an EMPTY classic source, and the new
	//     alias reads EOF despite the successful write that preceded it.
	//  2. FlushFile publishes what the bridge now holds, and MaterializeEmpty
	//     gives a genuinely empty file its name (the engine materializes an
	//     empty name only at the last close).
	for _, e := range list {
		// The lock owner is 0 on purpose here: this flush lands the source's
		// bytes for a link, it is not that descriptor's close(2), so it must
		// not drop the file's locks. (engineHandle.Flush, which IS a close
		// boundary, passes the real owner.)
		if errno := h.opts.Engine.VFS.Flush(e.vctx, e.ino, e.fh, 0); errno != 0 {
			unlock()
			return fs.Result{}, errno, true
		}
		if err := h.opts.Engine.FlushFile(ctx, uint64(e.ino)); err != nil {
			unlock()
			return fs.Result{}, err, true
		}
		if err := h.opts.Engine.Bridge.MaterializeEmpty(ctx, e.ino); err != nil {
			unlock()
			return fs.Result{}, err, true
		}
	}
	unlock()
	res, err := next(ctx, op)
	return res, err, true
}

// renameOverOpen runs the rename, first preserving any OPEN engine file the// renameOverOpen runs the rename, first preserving any OPEN engine file the
// rename is about to displace (see tryRenameOverOpen). handled=false only for
// renames the plane does not touch (no engine, no destination name).
//
// No durable intent is recorded: the extent plane keeps no local fsync
// barrier, so there is no staged record whose name a rename could strand.
// The rename itself is the remote operation the caller asked for.
func (h *Handler) renameOverOpen(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error, bool) {
	if h.opts.Engine == nil || op.Path2 == "" || op.Path2 == op.Path {
		return fs.Result{}, nil, false
	}
	res, err, handled := h.tryRenameOverOpen(ctx, op, next)
	if !handled {
		// The helper declined to run the rename (e.g. the destination has an
		// open handle that is still local-only, so there is no remote alias
		// to retain). Run the chain HERE so this wrapper observes the
		// outcome; a not-handled result means only that the helper did
		// nothing, never that the rename happened.
		res, err = next(ctx, op)
	}
	return res, err, true
}

// tryRenameOverOpen preserves an OPEN engine file that a rename is about
// to replace. The server's rename overwrites the destination object while
// the displaced descriptors keep addressing the name, so their next commit
// would PATCH the file just moved into it. RetainOpenAlias adds a HARDLINK
// of the displaced file under a fresh sentinel name first (non-replacing;
// the file never leaves its name) and its descriptors are retargeted to
// the alias; the chain's rename then replaces only the dentry while the
// alias keeps the displaced object alive. The alias is one extra reference
// under every outcome and drops at the displaced file's own last close, so
// NOTHING is ever put back, deleted, or decided from an unreserved
// observation: after an error the only action is the purely observational
// check whether the source actually landed (a committed rename can lose
// its response; core's probes can be inconclusive) — if so, the source's
// descriptors follow it; in every other case the displaced descriptor
// simply keeps using its alias.
func (h *Handler) tryRenameOverOpen(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error, bool) {
	if h.opts.Engine == nil || op.Path2 == "" || op.Path2 == op.Path {
		return fs.Result{}, nil, false
	}
	// Serialized against concurrent claims, truncates, unlinks and other
	// renames touching either name, across the whole operation INCLUDING
	// the destination-open check: an Open that starts while the rename is
	// pending must not register a descriptor on the destination before the
	// replacement lands — taken before the check, the lock holds for the
	// no-open-destination branch too, so that Open waits for the outcome.
	defer h.lockPaths(op.Path, op.Path2)()
	br := h.opts.Engine.Bridge
	// The source's identity, to recognize a rename that committed without
	// a response.
	srcID := resourceIDOf(ctx, br, op.Path)
	h.mu.Lock()
	_, destOpen := h.handles[op.Path2]
	h.mu.Unlock()
	if !destOpen {
		// No handle to protect, but the lock must still cover the chain's
		// rename itself: an Open of the destination starting now waits for
		// the outcome instead of registering a descriptor on the name
		// about to be replaced. (Returning "not handled" would drop the
		// lock before the chain runs.)
		res, nerr := next(ctx, op)
		return res, nerr, true
	}
	sentinel, err := br.RetainOpenAlias(op.Path2)
	if err != nil {
		// Retention failed: refuse the rename rather than risk the open
		// file (nothing has changed yet).
		return fs.Result{}, err, true
	}
	if sentinel == "" {
		return fs.Result{}, nil, false
	}
	h.retargetHandles(op.Path2, sentinel)
	res, err := next(ctx, op)
	if err == nil {
		return res, nil, true
	}
	if h.destHoldsSource(op.Path2, srcID) {
		// The rename committed after all: the source lives at the name.
		// Follow it so its descriptors commit to where its bytes are; the
		// displaced file keeps its alias until its own last close.
		h.retarget(op.Path, op.Path2)
	}
	return res, err, true
}

// resourceIDOf stats a path for its server-side identity ("" when absent
// or unreachable).
func resourceIDOf(ctx context.Context, br *Bridge, local string) string {
	if st, err := br.StatLocal(ctx, local); err == nil {
		return st.ResourceID
	}
	return ""
}

// destHoldsSourceProbes paces the detached probes after a failed rename
// (the error that got us here was likely transient).
var destHoldsSourceProbes = []time.Duration{0, 200 * time.Millisecond, 500 * time.Millisecond}

// destHoldsSource reports — detached from the failed request's context —
// whether the destination now holds the SOURCE's identity, i.e. the
// rename committed after all. An observation, never a reservation: the
// caller must only use it for actions on the source's own descriptors.
func (h *Handler) destHoldsSource(dest, srcID string) bool {
	if srcID == "" {
		return false
	}
	for _, delay := range destHoldsSourceProbes {
		time.Sleep(delay)
		pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		st, err := h.opts.Engine.Bridge.StatLocal(pctx, dest)
		cancel()
		if err == nil && st.ResourceID == srcID {
			return true
		}
	}
	return false
}

// tryTruncate routes a size change on an open engine file into the bridge;
// other setattr facets fall through to the classic plane.
func (h *Handler) tryTruncate(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error, bool) {
	unlock := h.lockPaths(op.Path)
	defer unlock()
	h.mu.Lock()
	_, open := h.handles[op.Path]
	h.mu.Unlock()
	if !open {
		return fs.Result{}, nil, false
	}
	// Apply the size change to the open engine handle as well: until the
	// remote commit, reads must not expose the shrunk tail and appends
	// must see the new EOF.
	h.mu.Lock()
	list := h.handles[op.Path]
	var live *engineHandle
	if len(list) > 0 {
		live = list[0]
	}
	h.mu.Unlock()
	if live != nil {
		if errno := h.opts.Engine.VFS.Truncate(live.vctx, live.ino, op.Size, 0, nil); errno != 0 {
			return fs.Result{}, errno, true
		}
	}
	if err := h.opts.Engine.Bridge.TruncateLocal(ctx, op.Path, op.Size); err != nil {
		return fs.Result{}, err, true
	}
	if !op.HasMode && op.SetUID == 0 && op.SetGID == 0 && op.Mtime.IsZero() {
		// The facade treats this attribute as authoritative (it re-applies
		// the mode and publishes to the caches); a size-only attribute
		// would publish mode 0000. Return the complete current set.
		size, _ := h.opts.Engine.LocalAttr(ctx, op.Path)
		attr := &fs.Attr{Kind: fs.KindFile, Nlink: 1, Size: size}
		if live != nil {
			if st, err := h.opts.Engine.Bridge.StatLocal(ctx, live.path); err == nil && st.HasMode {
				attr.Mode = st.Mode
			} else if m, ok := h.opts.Engine.Bridge.ModeOf(live.ino); ok && m != 0 {
				attr.Mode = m
			}
		}
		return fs.Result{Attr: attr}, nil, true
	}
	// Mixed setattr: if still local-only, apply locally; otherwise forward rest.
	rest := *op
	rest.HasSize = false
	if res, localHandled := h.setAttrLocalOnly(ctx, &rest); localHandled {
		return res, nil, true
	}
	res, err := next(ctx, &rest)
	if err == nil && res.Attr != nil {
		if size, ok := h.opts.Engine.LocalAttr(ctx, op.Path); ok && size > res.Attr.Size {
			res.Attr.Size = size
		}
	}
	return res, err, true
}

func (h *Handler) register(path string, ino uint64, fh uint64, c fs.Caller) *engineHandle {
	eh := &engineHandle{plane: h, path: path, ino: meta.Ino(ino), fh: fh, caller: c, vctx: vfsCtx(c)}
	h.mu.Lock()
	h.handles[path] = append(h.handles[path], eh)
	h.mu.Unlock()
	return eh
}

// engineHandle adapts one open engine file to the chain's data plane.
type engineHandle struct {
	plane  *Handler
	path   string
	ino    meta.Ino
	fh     uint64
	caller fs.Caller
	vctx   vfs.LogContext
	// released is set (under Handler.mu) by the Release that is retiring
	// this handle. It makes the "am I the last close" decision atomic: two
	// concurrent releases each mark themselves and then count the handles
	// still unreleased, so exactly one of them observes zero.
	released bool
	// flushedAtNano records the UnixNano timestamp of the most recent
	// FUSE_FLUSH on this descriptor (0 if never flushed). Used to bound the
	// kernel FUSE_FLUSH -> FUSE_RELEASE dispatch grace window without stalling
	// rmdir when a duplicated fd closed only one reference.
	flushedAtNano atomic.Int64
}

const sentinelFlushDispatchGrace = 25 * time.Millisecond

// waitClosingSentinelsInDir waits before OpRmdir when a directory contains
// internal .d9open-* sentinels that are actively being dropped by FUSE_RELEASE
// (up to 2s) or are within the brief kernel dispatch grace window immediately
// after FUSE_FLUSH (up to 25ms). Descriptors that remain open past the flush
// grace (e.g. a dup'd fd where only one reference closed) return without
// further delay.
func (h *Handler) waitClosingSentinelsInDir(ctx context.Context, dir string) {
	if h == nil || h.opts.Engine == nil || h.opts.Engine.Bridge == nil {
		return
	}
	br := h.opts.Engine.Bridge
	grace := sentinelFlushDispatchGrace
	if h.flushGrace > 0 {
		grace = h.flushGrace
	}
	releaseDeadline := time.Now().Add(2 * time.Second)
	notifiedWait := false
	for {
		active, dropping := br.SentinelsInDir(dir)
		if len(active) == 0 && len(dropping) == 0 {
			return
		}
		now := time.Now()
		hasConfirmedRelease := len(dropping) > 0
		hasFlushGraceOnly := false
		if len(active) > 0 {
			allClosingOrFlushed := true
			h.mu.Lock()
			for _, s := range active {
				handles := h.handles[s]
				if len(handles) == 0 {
					// The last handle already retired from h.handles and is
					// entering DropDeletedOpen; wait for sentinel deletion.
					hasConfirmedRelease = true
					continue
				}
				for _, eh := range handles {
					if eh.released {
						hasConfirmedRelease = true
						continue
					}
					flushedAt := eh.flushedAtNano.Load()
					if flushedAt != 0 && now.Sub(time.Unix(0, flushedAt)) <= grace {
						hasFlushGraceOnly = true
						continue
					}
					allClosingOrFlushed = false
					break
				}
				if !allClosingOrFlushed {
					break
				}
			}
			h.mu.Unlock()
			if !allClosingOrFlushed {
				return
			}
		}
		if (!hasConfirmedRelease && !hasFlushGraceOnly) || now.After(releaseDeadline) || ctx.Err() != nil {
			return
		}
		if !notifiedWait && h.onSentinelWait != nil {
			notifiedWait = true
			h.onSentinelWait()
		}
		waitCh := br.SentinelChanged()
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-waitCh:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
		timer.Stop()
	}
}

func (e *engineHandle) GetLk(ctx context.Context, lk fs.FileLock) (fs.FileLock, error) {
	start, end := fuseRangeToLock(lk.Start, lk.End)
	typ, pid := lk.Type, lk.Pid
	if errno := e.plane.opts.Engine.VFS.Getlk(vfsCtxWith(ctx, e.caller), e.ino, e.fh, lk.Owner, &start, &end, &typ, &pid); errno != 0 {
		return fs.FileLock{}, errno
	}
	out := lk
	out.Type = typ
	out.Start = int64(start)
	out.End = lockRangeToFuseEnd(end)
	if typ == syscall.F_UNLCK {
		out.Pid = 0
	} else {
		out.Pid = pid
	}
	return out, nil
}

func (e *engineHandle) SetLk(ctx context.Context, lk fs.FileLock, blocking bool) error {
	lctx := vfsCtxWith(ctx, e.caller)
	if lk.Flock {
		if errno := e.plane.opts.Engine.VFS.Flock(lctx, e.ino, e.fh, lk.Owner, lk.Type, blocking); errno != 0 {
			return errno
		}
		return nil
	}
	start, end := fuseRangeToLock(lk.Start, lk.End)
	if errno := e.plane.opts.Engine.VFS.Setlk(lctx, e.ino, e.fh, lk.Owner, start, end, lk.Type, lk.Pid, blocking); errno != 0 {
		return errno
	}
	return nil
}

// fuseRangeToLock converts an inclusive FUSE lock range into the bridge's
// internal form. FUSE encodes EOF as OFFSET_MAX ((1<<63)-1 = lockVFSUnlockEOF),
// whereas [0, 0] is a legitimate 1-byte lock on byte zero.
func fuseRangeToLock(start, end int64) (uint64, uint64) {
	if start < 0 {
		start = 0
	}
	if end < 0 || uint64(end) >= lockVFSUnlockEOF {
		return uint64(start), lockEOF
	}
	return uint64(start), uint64(end)
}

// lockRangeToFuseEnd converts the internal EOF sentinel back to FUSE OFFSET_MAX.
func lockRangeToFuseEnd(end uint64) int64 {
	if end >= lockVFSUnlockEOF {
		return int64(lockVFSUnlockEOF)
	}
	return int64(end)
}

func (e *engineHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	n, errno := e.plane.opts.Engine.VFS.Read(e.vctx, e.ino, dest, uint64(off), e.fh)
	if errno != 0 {
		return n, errno
	}
	return n, nil
}

func (e *engineHandle) Write(ctx context.Context, off int64, data []byte) error {
	if errno := e.plane.opts.Engine.VFS.Write(e.vctx, e.ino, data, uint64(off), e.fh); errno != 0 {
		return errno
	}
	// Record the end offset immediately: these bytes are visible to the
	// caller from now on, even though the engine has not published them yet
	// (it buffers until a chunk fills or the file is flushed). A stat in
	// between MUST report the grown length — POSIX grows the file on
	// write(2), and answering "0 bytes" makes the kernel reset its own
	// i_size and serve every following read as EOF.
	e.plane.opts.Engine.Bridge.noteBufferedLength(e.ino, off+int64(len(data)))
	if e.plane.opts.WriteSync {
		return e.Sync(ctx)
	}
	return nil
}

func (e *engineHandle) Sync(ctx context.Context) error {
	e.plane.opts.Engine.Bridge.kickPendingFlush()
	errno := e.plane.opts.Engine.VFS.Fsync(e.vctx, e.ino, 0, e.fh)
	if errno != 0 && !e.plane.opts.Engine.Bridge.HasPending(e.ino) {
		return errno
	}
	if err := e.plane.opts.Engine.FlushFile(ctx, uint64(e.ino)); err != nil {
		if errno != 0 {
			return errno // the VFS-level failure still stands
		}
		return err
	}
	return e.plane.opts.Engine.Bridge.MaterializeEmpty(ctx, e.ino)
}

func (e *engineHandle) Flush(ctx context.Context) error {
	defer func() {
		e.flushedAtNano.Store(time.Now().UnixNano())
	}()
	e.plane.opts.Engine.Bridge.kickPendingFlush()
	if errno := e.plane.opts.Engine.VFS.Flush(e.vctx, e.ino, e.fh, fs.FlushOwnerOf(ctx)); errno != 0 {
		return errno
	}
	return e.plane.opts.Engine.FlushFile(ctx, uint64(e.ino))
}

func (e *engineHandle) Release(ctx context.Context) error {
	// VFS.Release flushes the engine writer into the bridge (its pending
	// slices), but landing is the meta side's job: the bridge's Close does
	// it, and a handle that never reaches Close (FUSE Release is the last
	// callback) would otherwise stay unlanded. VFS.Release reports no
	// error, so the bridge-side landing is the only failure channel here.
	e.plane.opts.Engine.VFS.Release(e.vctx, e.ino, e.fh)
	// Only the LAST close may materialize an empty file's name. Doing it on
	// every release would create an unattributed classic placeholder while
	// another descriptor for the same inode is still open — and that
	// descriptor's next write would then be refused by the very ownership
	// guard this ordering exists to satisfy.
	//
	// Serialize with handle ADMISSION too, not only with retirement: an Open
	// of a still-local name registers through tryClaimOpen under the same
	// per-path lock, so taking it here means a new handle cannot slip in
	// between the final-close decision and the publication it gates.
	unlockPath := e.plane.lockPaths(e.path)
	// claimFinalClose makes the selection atomic, so overlapping closes still
	// produce exactly one owner of the final publication.
	finalClose := e.plane.claimFinalClose(e)
	defer unlockPath()
	var landErr error
	if finalClose {
		landErr = e.plane.opts.Engine.FlushFileFinal(ctx, uint64(e.ino))
	} else {
		landErr = e.plane.opts.Engine.FlushFile(ctx, uint64(e.ino))
	}
	e.plane.release(e)
	return landErr
}
