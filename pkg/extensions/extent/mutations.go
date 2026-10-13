// Mutations on claimed paths: truncate through the VFS SetAttr, unlink and
// rmdir against the resolved parent, the rename RPC that moves both the
// meta edge and the server-side projection in one transaction — and the
// staging-claim bookkeeping that keeps unlinks and renames of
// never-committed buffered writers local.
package extent

import (
	"context"
	"syscall"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// truncateIno applies a size change through the VFS.
func (e *Extent) truncateIno(ctx context.Context, path string, ino uint64, size int64) error {
	if err := e.ensureRuntime(); err != nil {
		return syscall.EIO
	}
	v := e.vfs()
	if v == nil {
		return syscall.EIO
	}
	entry, errn := v.SetAttr(e.jfsCtx(callerFrom(ctx)), jfsmeta.Ino(ino), int(jfsmeta.SetAttrSize), 0, 0, 0, 0, 0, 0, 0, 0, uint64(size))
	if errn != 0 {
		return errno(errn)
	}
	if entry != nil && entry.Attr != nil {
		e.paths.rememberIno(path, ino)
	}
	return nil
}

// unlinkIno removes the meta edge through the VFS, carrying the
// still-open flag so the server keeps the projection for live handles.
func (e *Extent) unlinkIno(ctx context.Context, path string, ino uint64) error {
	if err := e.ensureRuntime(); err != nil {
		return syscall.EIO
	}
	v := e.vfs()
	if v == nil {
		return syscall.EIO
	}
	caller := callerFrom(ctx)
	metaCtx := e.jfsMetaCtx(caller).
		WithValue(jfsmeta.Drive9OpenedKey, e.paths.hasLive(path)).
		WithValue(jfsmeta.Drive9PathKey, e.remotePath(path))
	parent, errn := e.ensureParent(e.jfsCtx(caller), pathutil.DirOf(path))
	if errn != 0 {
		return errno(errn)
	}
	if errn := v.Unlink(vfs.NewLogContext(metaCtx), parent, pathutil.BaseName(path)); errn != 0 && errn != syscall.ENOENT {
		return errno(errn)
	}
	e.paths.forgetIno(path)
	return nil
}

// removeDirIno removes an empty extent directory.
func (e *Extent) removeDirIno(ctx context.Context, path string, ino uint64) error {
	if err := e.ensureRuntime(); err != nil {
		return syscall.EIO
	}
	v := e.vfs()
	if v == nil {
		return syscall.EIO
	}
	caller := callerFrom(ctx)
	parent, errn := e.ensureParent(e.jfsCtx(caller), pathutil.DirOf(path))
	if errn != 0 {
		return errno(errn)
	}
	if errn := v.Rmdir(e.jfsCtx(caller), parent, pathutil.BaseName(path)); errn != 0 && errn != syscall.ENOENT {
		return errno(errn)
	}
	e.paths.forgetIno(path)
	return nil
}

// renameIno moves an extent inode: one RPC moves the meta edge and the
// server-side projection together, with the still-open flag of the
// destination so a live handle there keeps its bytes.
func (e *Extent) renameIno(ctx context.Context, op *fs.Op, ino uint64) error {
	if err := e.ensureRuntime(); err != nil {
		return syscall.EIO
	}
	er := e.rt.Load()
	if er == nil || er.rt == nil {
		return syscall.EIO
	}
	jctx := e.jfsCtx(op.Caller)
	srcParent, errn := e.ensureParent(jctx, pathutil.DirOf(op.Path))
	if errn != 0 {
		return errno(errn)
	}
	dstParent, errn := e.ensureParent(jctx, pathutil.DirOf(op.Path2))
	if errn != 0 {
		return errno(errn)
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	call := er.rt.Transport.Call(e.jfsMetaCtx(op.Caller), jfsmeta.Drive9OpRename, map[string]any{
		"src_parent": srcParent,
		"src_name":   pathutil.BaseName(op.Path),
		"dst_parent": dstParent,
		"dst_name":   pathutil.BaseName(op.Path2),
		"flags":      op.Flags,
		"src_path":   e.remotePath(op.Path),
		"dst_path":   e.remotePath(op.Path2),
		"dst_opened": e.paths.hasLive(op.Path2),
		"sid":        er.rt.SessionID,
	}, &resp)
	if call != 0 {
		return errno(call)
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	e.paths.forgetIno(op.Path)
	e.paths.forgetIno(op.Path2)
	e.paths.rememberIno(op.Path2, ino)
	e.paths.retargetLive(op.Path, op.Path2)
	return nil
}

// mutateStaging serves unlink, rmdir and rename for paths the large-file
// takeover buffers: the op is forwarded so the plane below drops its own
// pending state for the path, then the claim's handles go anonymous (or
// move with a rename). A never-committed claim has nothing remote to
// delete — the op succeeds locally when a live claim owns the path and the
// plane below knew nothing of the name either.
//
// The leading stat is load-bearing, not bookkeeping: it settles the
// window after close(2) in which the write plane's queued background
// upload of the just-closed image is still landing (its WaitPath covers
// in-flight work only). Without it, a rename that follows close
// immediately can double-upload the staged image and CAS a stale
// revision — git's lock→config cycle failed deterministically until the
// stat was restored. Removing it needs the write plane to gain
// queued-aware path waits first.
func (e *Extent) mutateStaging(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if _, statErr := e.opts.Client.StatCtx(ctx, e.remotePath(op.Path)); statErr != nil {
		if !client.IsNotFound(statErr) {
			return fs.Result{}, statErr
		}
		res, err := next(ctx, op)
		if err == nil {
			e.finishStagingMutation(ctx, op)
			return res, nil
		}
		if !fs.IsNotFound(err) {
			return res, err
		}
		if !e.paths.hasLiveStagingAt(op.Path, op.Kind == fs.OpRmdir) {
			return fs.Result{}, fs.ErrNotFound
		}
		e.finishStagingMutation(ctx, op)
		return fs.Result{}, nil
	}
	res, err := next(ctx, op)
	if err != nil {
		return res, err
	}
	e.finishStagingMutation(ctx, op)
	return res, nil
}

// finishStagingMutation retargets a rename's claims and flags an unlink's
// (or removed directory's) claims anonymous.
func (e *Extent) finishStagingMutation(ctx context.Context, op *fs.Op) {
	if op.Kind == fs.OpRename {
		// The rename replaced the destination: its open claims go
		// anonymous before the source claims move onto that path — an
		// un-anonymized destination claim would keep answering lookups
		// from its stale image and its boundary push would clobber the
		// renamed-in file.
		e.paths.markStagingUnlinked(op.Path2, false)
		e.paths.retargetStaging(ctx, op.Path, op.Path2)
		return
	}
	e.paths.markStagingUnlinked(op.Path, op.Kind == fs.OpRmdir)
}

// setUIDPermissionBit / setGIDPermissionBit are the privilege bits a
// non-root write clears.
const (
	setUIDPermissionBit uint32 = 0o4000
	setGIDPermissionBit uint32 = 0o2000
)

// clearSetidAfterWrite drops set-user-ID/set-group-ID after a write by a
// non-root caller.
func (e *Extent) clearSetidAfterWrite(ctx context.Context, ino jfsmeta.Ino) {
	v := e.vfs()
	if v == nil {
		return
	}
	jctx := e.jfsCtx(callerFrom(ctx))
	entry, errn := v.GetAttr(jctx, ino, 1)
	if errn != 0 || entry == nil || entry.Attr == nil {
		return
	}
	mode := uint32(entry.Attr.Mode)
	if mode&(setUIDPermissionBit|setGIDPermissionBit) == 0 {
		return
	}
	mode &^= setUIDPermissionBit | setGIDPermissionBit
	_, _ = v.SetAttr(jctx, ino, int(jfsmeta.SetAttrMode), 0, mode, 0, 0, 0, 0, 0, 0, 0)
}
