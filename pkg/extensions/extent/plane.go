package extent

import (
	"context"
	"sort"
	"strings"
	"sync"
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
	// SyncPolicy is the mount's --durability write policy. "write-sync"
	// makes every Write remote-durable before returning (engine flush +
	// bridge commit per write); the other policies commit at Flush/Release
	// like the classic plane.
	SyncPolicy string
	// SyncMode is the mount's resolved fsync semantic: "interactive" makes
	// fsync a LOCAL durability barrier (pending blocks spilled under the
	// engine's staging dir; the remote commit lands at flush/close or the
	// next mount's replay) exactly like the classic plane's staging
	// ledger; "strict" (or empty) pays the remote commit synchronously.
	SyncMode string
	// Events, when set, drops the bridge's cached mappings when names
	// vanish or change underneath the engine.
	Events *fs.EventBus
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
}

// New builds the engine-plane handler.
func New(opts Options) *Handler {
	h := &Handler{opts: opts, handles: make(map[string][]*engineHandle)}
	if opts.SyncMode == "interactive" && opts.Engine != nil {
		// Interactive durability: fsync is a local barrier; commits defer
		// to the flush/close boundary (or the next mount's replay).
		opts.Engine.Bridge.EnableDeferredCommits()
	}
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
	h.opts.Engine.Bridge.ForgetPath(e.path)
	h.opts.Engine.Bridge.DropDeletedOpen(e.path)
}

func vfsCtx(c fs.Caller) vfs.LogContext {
	return vfs.NewLogContext(meta.NewContext(c.Pid, c.Uid, []uint32{c.Gid}))
}

// Do claims extent files (and, in full mode, fresh creates); everything
// else passes through.
func (h *Handler) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpUnlink, fs.OpRmdir:
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
		if res, err, claimed := h.tryClaimOpen(ctx, op); claimed {
			return res, err
		}
	case fs.OpRename:
		if res, err, handled := h.tryRenameOverOpen(ctx, op, next); handled {
			return res, err
		}
	case fs.OpSetAttr:
		if op.HasSize {
			if res, err, handled := h.tryTruncate(ctx, op, next); handled {
				return res, err
			}
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

// tryClaimOpen claims the op when the target is (or, in full mode, will
// become) an engine file.
func (h *Handler) tryClaimOpen(ctx context.Context, op *fs.Op) (fs.Result, error, bool) {
	if h.opts.Engine == nil {
		return fs.Result{}, nil, false
	}
	// Serialized against retention for this name (unlink or rename-over):
	// a claim straddling a retention boundary would attach a descriptor
	// for the retained file onto the replaceable name. The layout decision
	// is (re)made under the lock too — the file at the name may have been
	// replaced while this claim waited, and a classic replacement must
	// pass through to the classic plane, never ride the engine.
	defer h.lockPaths(op.Path)()
	st, err := h.opts.Engine.Bridge.StatLocal(ctx, op.Path)
	if err == nil {
		if st.ContentLayout != client.ContentLayoutExtent {
			return fs.Result{}, nil, false
		}
		ino, errno := h.opts.Engine.ResolvePath(ctx, op.Path)
		if errno != 0 {
			return fs.Result{}, errno, false
		}
		_, fh, errno := h.opts.Engine.VFS.Open(vfsCtx(op.Caller), meta.Ino(ino), op.Flags)
		if errno != 0 {
			return fs.Result{}, errno, true
		}
		handle := h.register(op.Path, ino, fh, op.Flags, op.Caller)
		// Creation attributes let the facade populate its namespace cache
		// (a handle-only reply leaves the entry uncached).
		attr := &fs.Attr{Kind: fs.KindFile, Nlink: 1}
		if st2, serr := h.opts.Engine.Bridge.StatLocal(ctx, op.Path); serr == nil {
			attr.Size = st2.Size
			if st2.HasMode {
				attr.Mode = st2.Mode
			}
		}
		return fs.Result{Handle: handle, Attr: attr}, nil, true
	}
	if !client.IsNotFound(err) {
		return fs.Result{}, nil, false
	}
	// A file that exists ONLY in the bridge (a fresh engine create whose
	// remote materialization is pending) opens through the engine even
	// without O_CREAT — the classic plane would answer ENOENT for a
	// successfully created, still-open file.
	if ino, local := h.opts.Engine.Bridge.LocalResolve(op.Path); local {
		if op.Flags&syscall.O_CREAT != 0 && op.Flags&syscall.O_EXCL != 0 {
			// The folded OpCreate contract includes create/open flags: an
			// exclusive create of an existing (still-local) file is EEXIST.
			return fs.Result{}, syscall.EEXIST, true
		}
		if op.Flags&syscall.O_TRUNC != 0 {
			if errno := h.opts.Engine.VFS.Truncate(vfsCtx(op.Caller), ino, 0, 0, nil); errno != 0 {
				return fs.Result{}, errno, true
			}
			if err := h.opts.Engine.Bridge.TruncateLocal(ctx, op.Path, 0); err != nil {
				return fs.Result{}, err, true
			}
		}
		_, fh, errno := h.opts.Engine.VFS.Open(vfsCtx(op.Caller), ino, op.Flags)
		if errno != 0 {
			return fs.Result{}, errno, true
		}
		handle := h.register(op.Path, uint64(ino), fh, op.Flags, op.Caller)
		attr := &fs.Attr{Kind: fs.KindFile, Nlink: 1}
		if size, mode, ok := h.opts.Engine.Bridge.LocalView(ino); ok {
			attr.Size = size
			attr.Mode = mode
		}
		return fs.Result{Handle: handle, Attr: attr}, nil, true
	}
	// Fresh path: claim creates in full JuiceJS mode, or when the path
	// matches an explicit --extent / [extent] pattern; otherwise creation
	// stays on the classic plane (hybrid promotion).
	if op.Flags&syscall.O_CREAT == 0 || (h.opts.Threshold != 0 && !h.claimsPattern(op.Path)) {
		return fs.Result{}, nil, false
	}
	parent, name := SplitParent(op.Path)
	parentIno, errno := h.opts.Engine.ResolvePath(ctx, parent)
	if errno != 0 {
		return fs.Result{}, errno, true
	}
	mode := op.Mode
	if !op.HasMode {
		mode = 0o644
	}
	_, fh, errno := h.opts.Engine.VFS.Create(vfsCtx(op.Caller), meta.Ino(parentIno), name, uint16(mode&0o7777), 0, op.Flags)
	if errno != 0 {
		return fs.Result{}, errno, true
	}
	ino, errno := h.opts.Engine.ResolvePath(ctx, op.Path)
	if errno != 0 {
		// The bridge registered the mapping during Create; resolve again
		// defensively (fresh files resolve locally).
		return fs.Result{}, errno, true
	}
	handle := h.register(op.Path, ino, fh, op.Flags, op.Caller)
	return fs.Result{Handle: handle, Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Nlink: 1}}, nil, true
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
	return &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size, Nlink: 1}, true
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
	return false
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
	// Mixed setattr: the size landed in the bridge, the rest goes classic.
	rest := *op
	rest.HasSize = false
	res, err := next(ctx, &rest)
	if err == nil && res.Attr != nil {
		if size, ok := h.opts.Engine.LocalAttr(ctx, op.Path); ok && size > res.Attr.Size {
			res.Attr.Size = size
		}
	}
	return res, err, true
}

func (h *Handler) register(path string, ino uint64, fh uint64, flags uint32, c fs.Caller) *engineHandle {
	eh := &engineHandle{plane: h, path: path, ino: meta.Ino(ino), fh: fh, flags: flags, vctx: vfsCtx(c)}
	h.mu.Lock()
	h.handles[path] = append(h.handles[path], eh)
	h.mu.Unlock()
	return eh
}

// engineHandle adapts one open engine file to the chain's data plane.
type engineHandle struct {
	plane *Handler
	path  string
	ino   meta.Ino
	fh    uint64
	flags uint32 // open flags, for re-opening after a writer recovery
	vctx  vfs.LogContext
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
	if e.plane.opts.SyncPolicy == "write-sync" {
		// --durability write-sync: each Write is remote-durable before it
		// returns, mirroring the classic plane's per-Write commit.
		if errno := e.plane.opts.Engine.VFS.Flush(e.vctx, e.ino, e.fh, 0); errno != 0 {
			return errno
		}
		return e.plane.opts.Engine.FlushFile(ctx, uint64(e.ino))
	}
	return nil
}

func (e *engineHandle) Sync(ctx context.Context) error {
	errno := e.plane.opts.Engine.VFS.Fsync(e.vctx, e.ino, 0, e.fh)
	if errno != 0 && !e.plane.opts.Engine.Bridge.HasPending(e.ino) {
		return errno
	}
	// A failed VFS-level flush may itself be a failed COMMIT attempt
	// whose state the bridge preserved: FlushFile below retries it, and
	// success resolves the fsync. (With nothing pending the errno above
	// already returned.)
	if errno == 0 && e.plane.opts.SyncMode == "interactive" {
		// fsync is a local durability barrier on RTT-bound mounts, exactly
		// like the classic plane's staging ledger: spill the pending blocks
		// durably and return; the remote commit lands at the flush/close
		// boundary or the next mount's replay of the staged record.
		return e.plane.opts.Engine.Bridge.StagePendingLocal(e.ino)
	}
	if err := e.plane.opts.Engine.FlushFile(ctx, uint64(e.ino)); err != nil {
		if errno != 0 {
			return errno // the VFS-level failure still stands
		}
		return err
	}
	if errno != 0 {
		// The bridge landed what the community writer recorded as failed:
		// that writer keeps returning its recorded error forever (v1.4.1
		// fileWriter.err), so replace the handle with a fresh one — same
		// inode, same flags — or every later Write/Flush on this descriptor
		// would keep failing after the reported recovery.
		e.plane.opts.Engine.VFS.Release(e.vctx, e.ino, e.fh)
		_, fh, nerr := e.plane.opts.Engine.VFS.Open(e.vctx, e.ino, e.flags)
		if nerr != 0 {
			return errno
		}
		e.fh = fh
	}
	return nil
}

func (e *engineHandle) Flush(ctx context.Context) error {
	if errno := e.plane.opts.Engine.VFS.Flush(e.vctx, e.ino, e.fh, 0); errno != 0 {
		return errno
	}
	return e.plane.opts.Engine.FlushFile(ctx, uint64(e.ino))
}

func (e *engineHandle) Release(ctx context.Context) error {
	e.plane.opts.Engine.VFS.Release(e.vctx, e.ino, e.fh)
	e.plane.release(e)
	return nil
}
