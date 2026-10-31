package extent

import (
	"context"
	"strings"
	"sync"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
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
	// SyncPolicy is the mount's --durability write policy. "write-sync"
	// makes every Write remote-durable before returning (engine flush +
	// bridge commit per write); the other policies commit at Flush/Release
	// like the classic plane.
	SyncPolicy string
	// Events, when set, drops the bridge's cached mappings when names
	// vanish or change underneath the engine.
	Events *fs.EventBus
}

// Handler routes extent files onto the engine plane.
type Handler struct {
	opts Options

	mu      sync.Mutex
	handles map[string][]*engineHandle
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

// retarget moves every open handle (and the bridge mapping) from the old
// name to the renamed one. A directory rename arrives as ONE event for the
// directory, so every open DESCENDANT follows too — otherwise a later
// create at the old child name registers next to (and aliases with) the
// retained descriptor.
func (h *Handler) retarget(oldPath, newPath string) {
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
	h.opts.Engine.Bridge.RemapLocal(oldPath, newPath)
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
		h.mu.Lock()
		_, open := h.handles[op.Path]
		h.mu.Unlock()
		if open {
			sentinel, err := h.opts.Engine.Bridge.KeepDeletedOpen(op.Path)
			if err != nil {
				return fs.Result{}, err
			}
			if sentinel != "" {
				// Follow the descriptor to the sentinel so the last
				// release can delete the registered keep-alive name.
				h.retarget(op.Path, sentinel)
			}
			return fs.Result{}, nil
		}
	case fs.OpCreate:
		if res, err, claimed := h.tryClaimOpen(ctx, op); claimed {
			return res, err
		}
	case fs.OpSetAttr:
		if op.HasSize {
			if res, err, handled := h.tryTruncate(ctx, op, next); handled {
				return res, err
			}
		}
	case fs.OpGetAttr:
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
		handle := h.register(op.Path, ino, fh, op.Caller)
		return fs.Result{Handle: handle}, nil, true
	}
	if !client.IsNotFound(err) {
		return fs.Result{}, nil, false
	}
	// Fresh path: claim only in full JuiceFS mode, and only for creates.
	if h.opts.Threshold != 0 || op.Flags&syscall.O_CREAT == 0 {
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
	handle := h.register(op.Path, ino, fh, op.Caller)
	return fs.Result{Handle: handle}, nil, true
}

// tryTruncate routes a size change on an open engine file into the bridge;
// other setattr facets fall through to the classic plane.
func (h *Handler) tryTruncate(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error, bool) {
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
		size, _ := h.opts.Engine.LocalAttr(ctx, op.Path)
		return fs.Result{Attr: &fs.Attr{Size: size}}, nil, true
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

func (h *Handler) register(path string, ino uint64, fh uint64, c fs.Caller) *engineHandle {
	eh := &engineHandle{plane: h, path: path, ino: meta.Ino(ino), fh: fh, vctx: vfsCtx(c)}
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
	if errno := e.plane.opts.Engine.VFS.Fsync(e.vctx, e.ino, 0, e.fh); errno != 0 {
		return errno
	}
	return e.plane.opts.Engine.FlushFile(ctx, uint64(e.ino))
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
