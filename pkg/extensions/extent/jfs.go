package extent

// VFS operation core: jfs contexts (including the FUSE
// interrupt-cancel semantics), the open/read/write/flush bodies, and the
// supplementary-group resolution that feeds jfsMetaCtx.

import (
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	drive9extent "github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// processSupplementaryGroups resolves the caller's groups from /proc so
// jfsMetaCtx enforces group permissions like the kernel.
func processSupplementaryGroups(pid uint32) []uint32 {
	if pid == 0 {
		return nil
	}
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(string(status), "\n") {
		if !strings.HasPrefix(line, "Groups:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "Groups:"))
		out := make([]uint32, 0, len(fields))
		for _, field := range fields {
			if v, err := strconv.ParseUint(field, 10, 32); err == nil {
				out = append(out, uint32(v))
			}
		}
		return out
	}
	return nil
}

func (e *Extent) jfsMetaCtx(caller fs.Caller) jfsmeta.Context {
	gids := []uint32{caller.Gid}
	seen := map[uint32]struct{}{caller.Gid: {}}
	for _, extra := range processSupplementaryGroups(caller.Pid) {
		if _, ok := seen[extra]; ok {
			continue
		}
		seen[extra] = struct{}{}
		gids = append(gids, extra)
	}
	return jfsmeta.NewContext(caller.Pid, caller.Uid, gids)
}

func (e *Extent) jfsCtx(caller fs.Caller) vfs.LogContext {
	return vfs.NewLogContext(e.jfsMetaCtx(caller))
}

// jfsCtxWithKeyedPath builds a LogContext whose meta context carries the
// drive9 projection path (jfsmeta.Drive9PathKey): the engine's mknod/mkdir
// transactions publish the file_nodes projection row server-side (without
// it, newly created extent files are invisible to listings, other mounts,
// and re-mounts).
func (e *Extent) jfsCtxWithKeyedPath(caller fs.Caller, remotePath string) vfs.LogContext {
	return vfs.NewLogContext(e.jfsMetaCtx(caller).WithValue(jfsmeta.Drive9PathKey, remotePath))
}

// jfsFuseContext carries the FUSE-interrupt semantics: Canceled() ignores
// the FUSE interrupt channel for the first second, then polls it — JuiceFS
// only returns EINTR between lock retries, never mid-txn. cancel is the
// kernel-request context's Done channel; lockBlock marks blocking lock
// requests, which poll immediately AND surface the block hint to the
// transport so the server loops like sql_lock instead of one EAGAIN RPC
// every 10ms.
type jfsFuseContext struct {
	vfs.LogContext
	cancel    <-chan struct{}
	lockBlock bool
	started   time.Time
}

// Value surfaces the block hint for the extent transport (v1: the context
// answered LockBlockCtxKey=true, injecting "block":true server-side).
func (c *jfsFuseContext) Value(key any) any {
	if key == drive9extent.LockBlockCtxKey && c.lockBlock {
		return true
	}
	return c.LogContext.Value(key)
}

func (c *jfsFuseContext) Canceled() bool {
	if c == nil {
		return false
	}
	if c.lockBlock {
		// Blocking lock retries poll the interrupt immediately: JuiceFS
		// only returns EINTR between retries.
		select {
		case <-c.cancel:
			return true
		default:
			return false
		}
	}
	// JuiceFS only consults the interrupt after the first second, so
	// mid-transaction cancels do not abort; the check itself never blocks.
	if time.Since(c.started) < time.Second {
		return false
	}
	select {
	case <-c.cancel:
		return true
	default:
		return false
	}
}

// extentOpenFlags maps JuiceFS close-to-open invalidation: the inode keeps
// its cache only when the server said so.
func extentOpenFlags(keepCache bool) uint32 {
	if keepCache {
		// The protocol constant is FOPEN_KEEP_CACHE (1<<1): 0x20 is
		// FOPEN_NOFLUSH, which made the kernel skip close-time FLUSH and
		// defer upload errors to a Release that discards them.
		return gofuse.FOPEN_KEEP_CACHE
	}
	// JuiceFS says the page cache must NOT survive the close: carry the
	// facade-reserved marker so it stops ORing FOPEN_KEEP_CACHE back on
	// (a plain 0 was indistinguishable from "plane had no opinion" and
	// cancelled the close-to-open invalidation between mounts).
	return fs.OpenFlagDropKernelCache
}

// juiceWriteOffset bumps O_APPEND kernel writes (which can arrive with
// off=0) to the VFS length.
func juiceWriteOffset(off, length uint64, appendMode bool) uint64 {
	if !appendMode || off != 0 {
		return off
	}
	if length > off {
		return length
	}
	return off
}

// extentHandle is the terminal data-plane handle: Read/Write/Flush go to the
// JuiceFS VFS through the pinned (ino, fh) pair.
type extentHandle struct {
	e        *Extent
	path     string
	ino      jfsmeta.Ino
	fh       uint64
	flags    uint32
	mu       sync.Mutex
	closed   bool
	openFlag uint32
	// caller is the opener's credentials: JuiceFS VFS operations must run
	// as the requester, not as root (the private context key is never
	// populated by the facade's plain fuseCtx contexts).
	caller fs.Caller
}

// callerOf resolves the request's caller: a context-carried caller when
// present (namespace ops), else the handle's opener (handle I/O arrives
// with a bare fuseCtx context).
func (h *extentHandle) callerOf(ctx context.Context) fs.Caller {
	if c, ok := ctx.Value(callerKey{}).(fs.Caller); ok && (c.Uid != 0 || c.Gid != 0 || c.Pid != 0) {
		return c
	}
	return h.caller
}

func (h *extentHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	v := h.e.vfs()
	if v == nil {
		return 0, syscall.EIO
	}
	n, errn := v.Read(h.e.jfsCtx(h.callerOf(ctx)), h.ino, dest, uint64(off), h.fh)
	if errn != 0 {
		return n, errno(errn)
	}
	return n, nil
}

func (h *extentHandle) Write(ctx context.Context, off int64, data []byte) error {
	v := h.e.vfs()
	if v == nil {
		return syscall.EIO
	}
	jctx := h.e.jfsCtx(h.callerOf(ctx))
	if h.flags&uint32(syscall.O_APPEND) != 0 {
		if entry, errn := v.GetAttr(jctx, h.ino, 1); errn == 0 && entry != nil && entry.Attr != nil {
			v.UpdateLength(h.ino, entry.Attr)
			off = int64(juiceWriteOffset(uint64(off), entry.Attr.Length, true))
		}
	}
	if errn := v.Write(jctx, h.ino, data, uint64(off), h.fh); errn != 0 {
		// Ported tolerance: ENOENT means the meta no longer has this inode
		// (unlinked) — nothing to write to; EBADF is a handle bug and must
		// surface instead of reporting a write that never touched data.
		if errn == syscall.ENOENT {
			return nil
		}
		return errno(errn)
	}
	// v1: write-sync mounts pay remote durability per Write — flush through
	// the VFS so write(2) returning means the data is on the server.
	if h.e.policy == "write-sync" {
		if ferrn := v.Flush(jctx, h.ino, h.fh, 0); ferrn != 0 {
			return errno(ferrn)
		}
	}
	// A write by a non-root caller drops set-user-ID/set-group-ID.
	if uid := h.callerOf(ctx).Uid; uid != 0 {
		h.e.clearSetidAfterWrite(ctx, h.ino)
	}
	return nil
}

func (h *extentHandle) Sync(ctx context.Context) error {
	v := h.e.vfs()
	if v == nil {
		return syscall.EIO
	}
	if errn := v.Flush(h.e.jfsCtx(h.callerOf(ctx)), h.ino, h.fh, 0); errn != 0 {
		return errno(errn)
	}
	return nil
}

func (h *extentHandle) Flush(ctx context.Context) error {
	// Flush releases the FUSE lock owner and pushes the VFS flush: the
	// owner the kernel named in FLUSH (a close of one descriptor of a
	// dup'ed fd) must reach VFS.Flush — JuiceFS only issues the close-time
	// F_UNLCK for the owner it is given, and a hardcoded 0 left the
	// original process's record locks held after close.
	v := h.e.vfs()
	if v == nil {
		return nil
	}
	owner := fs.FlushOwnerOf(ctx)
	if errn := v.Flush(h.e.jfsCtx(h.callerOf(ctx)), h.ino, h.fh, owner); errn != 0 {
		return errno(errn)
	}
	return nil
}

// GetLk defers POSIX lock probes to the JuiceFS VFS.
func (h *extentHandle) GetLk(ctx context.Context, lk fs.FileLock) (fs.FileLock, error) {
	v := h.e.vfs()
	if v == nil {
		return fs.FileLock{}, syscall.EIO
	}
	start, end, ltype, pid := uint64(lk.Start), uint64(lk.End), lk.Type, lk.Pid
	jctx := &jfsFuseContext{LogContext: h.e.jfsCtx(h.callerOf(ctx)), cancel: ctx.Done(), started: time.Now()}
	if errn := v.Getlk(jctx, vfs.Ino(h.ino), h.fh, lk.Owner, &start, &end, &ltype, &pid); errn != 0 {
		return fs.FileLock{}, errno(errn)
	}
	return fs.FileLock{Start: int64(start), End: int64(end), Type: ltype, Pid: pid}, nil
}

// SetLk defers POSIX lock ownership to the JuiceFS VFS. The owner passes
// through as-is: the pid/fh fallback of the facade table would break
// unlock matching in the VFS.
func (h *extentHandle) SetLk(ctx context.Context, lk fs.FileLock, blocking bool) error {
	v := h.e.vfs()
	if v == nil {
		return syscall.EIO
	}
	// cancel wires the kernel-request context: a blocking lock becomes
	// interruptible (EINTR), and lockBlock surfaces the transport's
	// block:true hint so the server loops instead of 10ms EAGAIN polls.
	jctx := &jfsFuseContext{LogContext: h.e.jfsCtx(h.callerOf(ctx)), cancel: ctx.Done(), lockBlock: true, started: time.Now()}
	if lk.Flock {
		return errno(v.Flock(jctx, vfs.Ino(h.ino), h.fh, lk.Owner, lk.Type, blocking))
	}
	return errno(v.Setlk(jctx, vfs.Ino(h.ino), h.fh, lk.Owner, uint64(lk.Start), uint64(lk.End), lk.Type, lk.Pid, blocking))
}

func (h *extentHandle) Release(ctx context.Context) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()
	h.e.untrackLiveHandle(h)
	v := h.e.vfs()
	if v == nil {
		return nil
	}
	v.Release(h.e.jfsCtx(h.callerOf(ctx)), vfs.Ino(h.ino), h.fh)
	return nil
}

func callerFrom(ctx context.Context) fs.Caller {
	if c, ok := ctx.Value(callerKey{}).(fs.Caller); ok {
		return c
	}
	return fs.Caller{}
}

type callerKey struct{}

// contextWithCaller plants the request's credentials where callerFrom
// finds them: the namespace mutation paths (unlink/rmdir/truncate/rename)
// construct JuiceFS contexts as the requester, not as root (main passed
// the request's credentials into these VFS operations).
func contextWithCaller(ctx context.Context, caller fs.Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// openExtent ensures the runtime, VFS.Opens the pinned inode, updates the
// length cache, and returns the terminal handle.
func (e *Extent) openExtent(ctx context.Context, path string, ino uint64, flags uint32, caller fs.Caller) (fs.Handle, uint32, *fs.Attr, error) {
	if err := e.ensureRuntime(); err != nil {
		return nil, 0, nil, syscall.EIO
	}
	v := e.vfs()
	if v == nil {
		return nil, 0, nil, syscall.EIO
	}
	jctx := e.jfsCtx(caller)
	jino := jfsmeta.Ino(ino)
	entry, vfsFh, errn := v.Open(jctx, jino, flags)
	if errn != 0 {
		return nil, 0, nil, errno(errn)
	}
	length := uint64(0)
	keepCache := false
	if entry != nil && entry.Attr != nil {
		v.UpdateLength(jino, entry.Attr)
		length = entry.Attr.Length
		keepCache = entry.Attr.KeepCache
	}
	e.rememberExtentIno(path, ino)
	attr := &fs.Attr{Kind: fs.KindFile, Size: int64(length)}
	if entry != nil && entry.Attr != nil {
		attr.Mode = juiceTypeToStatMode(entry.Attr.Typ, entry.Attr.Mode)
		attr.Rdev = entry.Attr.Rdev
		attr.Mtime = time.Unix(int64(entry.Attr.Atime), int64(entry.Attr.Atimensec))
		attr.Nlink = entry.Attr.Nlink
		attr.Uid = entry.Attr.Uid
		attr.Gid = entry.Attr.Gid
		attr.HasOwner = true
	}
	h := &extentHandle{e: e, path: path, ino: jino, fh: vfsFh, flags: flags, openFlag: extentOpenFlags(keepCache), caller: caller}
	e.trackLiveHandle(h)
	return h, extentOpenFlags(keepCache), attr, nil
}

// adoptCreatedHandle builds a handle around the fh VFS.Create already
// allocated (re-opening through Open would leak that reference).
func (e *Extent) adoptCreatedHandle(path string, ino uint64, fh uint64, a *jfsmeta.Attr, flags uint32, caller fs.Caller) (fs.Handle, uint32, *fs.Attr, error) {
	jino := jfsmeta.Ino(ino)
	attr := &fs.Attr{Kind: fs.KindFile, Mode: 0o644}
	keepCache := false
	if a != nil {
		e.vfs().UpdateLength(jino, a)
		attr.Mode = juiceTypeToStatMode(a.Typ, a.Mode)
		attr.Rdev = a.Rdev
		attr.Size = int64(a.Length)
		attr.Mtime = time.Unix(int64(a.Mtime), int64(a.Mtimensec))
		attr.Nlink = a.Nlink
		attr.Uid, attr.Gid, attr.HasOwner = a.Uid, a.Gid, true
		keepCache = a.KeepCache
	}
	h := &extentHandle{e: e, path: path, ino: jino, fh: fh, flags: flags, openFlag: extentOpenFlags(keepCache), caller: caller}
	e.trackLiveHandle(h)
	return h, extentOpenFlags(keepCache), attr, nil
}

// extentHandleOf walks decorator wrappers to the extent's own handle
// (FD-named setattr must reach the descriptor's BOUND inode — after a
// rename replaced the path, the path-resolved inode is the replacement
// file, and truncating it corrupted the new object).
func extentHandleOf(h any) *extentHandle {
	for range 8 {
		if eh, ok := h.(*extentHandle); ok {
			return eh
		}
		u, ok := h.(interface{ UnwrapHandle() fs.Handle })
		if !ok {
			return nil
		}
		inner := u.UnwrapHandle()
		if inner == nil {
			return nil
		}
		h = inner
	}
	return nil
}

func errno(e syscall.Errno) error {
	if e == 0 {
		return nil
	}
	return e
}
