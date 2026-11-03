package extent

// VFS stat/create helpers: attribute projection through the JuiceFS
// session and creation under the extent plane's parent inode.

import (
	"context"
	"path"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// statViaVFS projects one VFS.GetAttr on the resolved inode, mapped
// through juiceTypeToStatMode.
func (e *Extent) statViaVFS(ctx context.Context, path string, ino uint64) (fs.Result, error) {
	v := e.vfs()
	if v == nil {
		return fs.Result{}, syscall.EIO
	}
	entry, errn := v.GetAttr(e.jfsCtx(callerFrom(ctx)), jfsmeta.Ino(ino), 0)
	if errn != 0 || entry == nil || entry.Attr == nil {
		if errn == syscall.ENOENT {
			e.missRemember(path)
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{}, errno(errn)
	}
	attr := attrFromJuice(entry.Attr)
	e.keepOpenWALIndexSize(path, &attr)
	e.rememberExtentIno(path, ino)
	return fs.Result{Attr: &attr}, nil
}

// sqliteWALIndexMinSize is the size a still-open sqlite -shm file's mapping
// needs: an open shared-memory index reports 0 in the metadata but the
// kernel mapping requires the full index, so stat it at this floor.
const sqliteWALIndexMinSize int64 = 32768

// keepOpenWALIndexSize gives a still-open sqlite -shm file the size its
// mapping needs. See sqliteWALIndexMinSize.
func (e *Extent) keepOpenWALIndexSize(path string, attr *fs.Attr) {
	if attr.Size > 0 || !isSQLiteWALIndexPath(path) {
		return
	}
	if !e.hasLive(path) {
		return
	}
	attr.Size = sqliteWALIndexMinSize
}

// isSQLiteWALIndexPath reports a "<db>-shm" path (never a bare "-shm").
func isSQLiteWALIndexPath(localPath string) bool {
	name := path.Base(localPath)
	if name == "-shm" || !strings.HasSuffix(name, "-shm") {
		return false
	}
	return strings.TrimSuffix(name, "-shm") != ""
}

func attrFromJuice(a *jfsmeta.Attr) fs.Attr {
	attr := fs.Attr{
		// The FULL mode survives (type bits for FIFO/device/socket nodes,
		// SUID/SGID/sticky): truncating to 0777 retyped special nodes as
		// regular files and dropped privilege bits from chmod replies.
		Mode:     juiceTypeToStatMode(a.Typ, a.Mode),
		Rdev:     a.Rdev,
		Size:     int64(a.Length),
		Mtime:    time.Unix(int64(a.Mtime), int64(a.Mtimensec)),
		Nlink:    a.Nlink,
		Uid:      a.Uid,
		Gid:      a.Gid,
		HasOwner: true,
	}
	switch a.Typ {
	case jfsmeta.TypeDirectory:
		attr.Kind = fs.KindDir
	case jfsmeta.TypeSymlink:
		attr.Kind = fs.KindSymlink
	default:
		attr.Kind = fs.KindFile
	}
	return attr
}

// createInVFS resolves the parent chain in the JuiceFS namespace (the
// mount root maps to the tenant's extent subtree), creates the file with
// the caller's mode, and returns its inode. The create context carries the
// drive9 projection path so the engine publishes the file_nodes row (v1:
// without it, the new extent file is invisible to listings, other mounts,
// and re-mounts).
func (e *Extent) createInVFS(ctx context.Context, op *fs.Op) (uint64, uint64, *jfsmeta.Attr, syscall.Errno, bool) {
	v := e.vfs()
	if v == nil {
		return 0, 0, nil, syscall.EIO, false
	}
	jctx := e.jfsCtxWithKeyedPath(op.Caller, e.remotePath(op.Path))
	parent, errn := e.ensureParent(jctx, pathutil.DirOf(op.Path))
	if errn != 0 {
		return 0, 0, nil, errn, false
	}
	mode := uint16(op.Mode & 0o777)
	if mode == 0 {
		mode = 0o644
	}
	// The VFS fh returned here is REUSED by the handle: dropping it and
	// re-opening through openExtent leaked the first VFS file reference
	// forever (Release only closed the second fh).
	entry, fh, errn := v.Create(jctx, parent, pathutil.BaseName(op.Path), mode, 0, op.Flags)
	if errn != 0 || entry == nil {
		return 0, 0, nil, errn, false
	}
	ino := uint64(entry.Inode)
	e.rememberExtentIno(op.Path, ino)
	return ino, fh, entry.Attr, 0, true
}

// ensureParent walks from the extent mount root, creating missing
// directories (mkdir -p semantics in the JuiceFS namespace), and returns
// the parent inode. Each created directory carries its drive9 projection
// path (v1 extentEnsureParent) and inherits the drive9-side directory's
// mode (v1 juiceDirCreateMode: sticky/permission bits must not collapse to
// a blanket 0755).
func (e *Extent) ensureParent(jctx vfs.LogContext, dirPath string) (jfsmeta.Ino, syscall.Errno) {
	v := e.vfs()
	if v == nil {
		return 0, syscall.EIO
	}
	root := jfsmeta.RootInode
	// The walk starts from the TENANT root and follows the remote path
	// (RemoteRoot prefix included): projecting the local path directly
	// made every subdirectory mount resolve "/" to the tenant RootInode,
	// so two mounts created/opened the SAME (parent=1, name) node.
	dirPath = e.remotePath(dirPath)
	if dirPath == "/" || dirPath == "" {
		return root, 0
	}
	caller := callerFromLogCtx(jctx)
	segs := splitPath(dirPath)
	cur := root
	acc := ""
	for _, seg := range segs {
		acc += "/" + seg
		entry, errn := v.Lookup(jctx, cur, seg)
		if errn == syscall.ENOENT {
			// acc is already TENANT-absolute (the walk translated through
			// remotePath above): trimming the mount root here made the
			// mode lookup view-relative and re-prefixing doubled the
			// RemoteRoot (the projection missed the real parent row).
			mode := uint16(e.mirroredDirMode(acc))
			mkdirCtx := vfs.NewLogContext(e.jfsMetaCtx(caller).WithValue(jfsmeta.Drive9PathKey, e.remotePath(acc)+"/"))
			newEntry, mkErrn := v.Mkdir(mkdirCtx, cur, seg, mode, 0)
			if mkErrn != 0 && mkErrn != syscall.EEXIST {
				return 0, mkErrn
			}
			if mkErrn == 0 && newEntry != nil {
				cur = newEntry.Inode
				continue
			}
			entry, errn = v.Lookup(jctx, cur, seg)
			if errn != 0 || entry == nil {
				return 0, errn
			}
		} else if errn != 0 || entry == nil {
			return 0, errn
		}
		cur = entry.Inode
	}
	return cur, 0
}

// mirroredDirMode resolves the mode a newly mirrored jfs parent directory
// takes: the drive9-side directory's mode when the server knows it (sticky
// and permission bits survive), 0755 otherwise (v1 juiceDirCreateMode's
// fallback).
func (e *Extent) mirroredDirMode(dirPath string) uint32 {
	if e.opts.Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// dirPath arrives TENANT-ABSOLUTE (the parent walk already
		// translated it): remotePath would double the RemoteRoot prefix
		// and the projection stat would 404/mis-associate file_nodes.
		if st, err := e.opts.Client.StatCtx(ctx, dirPath); err == nil && st != nil && st.IsDir && st.HasMode {
			return st.Mode & 0o7777
		}
	}
	return 0o755
}

// callerFromLogCtx recovers the caller identity from a LogContext's meta
// values (nil/unknown callers fall back to root, matching engine-side
// permission checks for internal walks).
func callerFromLogCtx(jctx vfs.LogContext) fs.Caller {
	if jctx == nil {
		return fs.Caller{}
	}
	return fs.Caller{Pid: jctx.Pid(), Uid: jctx.Uid(), Gid: jctx.Gid()}
}

// metadataOnlySpecialMode matches v1: FIFO/CHR/BLK/SOCK creations (no data
// plane beyond the metadata row).
func metadataOnlySpecialMode(mode uint32) bool {
	switch mode & syscall.S_IFMT {
	case syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK:
		return true
	}
	return false
}

// mirrorMkdir ports v1 extentMkdir: inside an already-mirrored subtree the
// new directory also lands in the JuiceFS namespace with its projection
// row. Only mirrored parents participate (v1 juiceParentForOp resolved
// bound inodes; an unmirrored parent means this subtree is classic).
func (e *Extent) mirrorMkdir(ctx context.Context, op *fs.Op) error {
	parentPath := pathutil.DirOf(op.Path)
	if _, ok := e.cachedIno(parentPath); !ok {
		if !e.subtreeMirrored(parentPath) {
			return nil
		}
	}
	if err := e.ensureRuntime(); err != nil {
		return err
	}
	v := e.vfs()
	if v == nil {
		return syscall.EIO
	}
	jctx := e.jfsCtxWithKeyedPath(op.Caller, e.remotePath(op.Path)+"/")
	parent, errn := e.ensureParent(jctx, parentPath)
	if errn != 0 {
		return errno(errn)
	}
	mode := uint16(op.Mode & 0o7777)
	if mode == 0 {
		mode = 0o755
	}
	entry, errn := v.Mkdir(jctx, parent, pathutil.BaseName(op.Path), mode, 0)
	if errn == syscall.EEXIST {
		entry, errn = v.Lookup(jctx, parent, pathutil.BaseName(op.Path))
	}
	if errn != 0 {
		return errno(errn)
	}
	if entry != nil {
		e.rememberExtentIno(op.Path, uint64(entry.Inode))
	}
	return nil
}

// subtreeMirrored reports whether any ancestor of dirPath is bound to an
// extent inode (the mirrored-subtree gate for mirror ops).
func (e *Extent) subtreeMirrored(dirPath string) bool {
	for dirPath != "" && dirPath != "/" {
		if _, ok := e.cachedIno(dirPath); ok {
			return true
		}
		dirPath = pathutil.DirOf(dirPath)
	}
	return false
}

// mknodSpecialInVFS ports v1 extentMknod: metadata-only specials on extent
// mounts become persistent JuiceFS nodes (projection row included), so
// they survive restarts and are visible cross-mount.
func (e *Extent) mknodSpecialInVFS(ctx context.Context, op *fs.Op) (fs.Result, error) {
	if err := e.ensureRuntime(); err != nil {
		return fs.Result{}, err
	}
	v := e.vfs()
	if v == nil {
		return fs.Result{}, syscall.EIO
	}
	jctx := e.jfsCtxWithKeyedPath(op.Caller, e.remotePath(op.Path))
	parent, errn := e.ensureParent(jctx, pathutil.DirOf(op.Path))
	if errn != 0 {
		return fs.Result{}, errno(errn)
	}
	// The FULL mode rides into the VFS (S_IFMT included): JuiceFS's
	// get_filetype defaults a typeless mode to a regular file, so masking
	// the type bits turned mkfifo/mknod into plain-file creates.
	mode := uint16(op.Mode)
	entry, errn := v.Mknod(jctx, parent, pathutil.BaseName(op.Path), mode, 0, op.Rdev)
	if errn == syscall.EEXIST {
		return fs.Result{}, syscall.EEXIST
	}
	if errn != 0 {
		return fs.Result{}, errno(errn)
	}
	if entry == nil {
		return fs.Result{}, syscall.EIO
	}
	ino := uint64(entry.Inode)
	e.rememberExtentIno(op.Path, ino)
	attr := fs.Attr{Mode: op.Mode, Rdev: op.Rdev, Nlink: 1}
	if entry.Attr != nil {
		attr = attrFromJuice(entry.Attr)
		attr.Rdev = op.Rdev
	}
	return fs.Result{Attr: &attr}, nil
}

// mirrorSymlink ports v1 extentSymlink: a symlink inside a mirrored
// subtree also lands in the JuiceFS namespace; the classic plane still
// creates the drive9-side row.
func (e *Extent) mirrorSymlink(ctx context.Context, op *fs.Op) error {
	parentPath := pathutil.DirOf(op.Path)
	if _, ok := e.cachedIno(parentPath); !ok {
		if !e.subtreeMirrored(parentPath) {
			return nil
		}
	}
	if err := e.ensureRuntime(); err != nil {
		return err
	}
	v := e.vfs()
	if v == nil {
		return syscall.EIO
	}
	jctx := e.jfsCtxWithKeyedPath(op.Caller, e.remotePath(op.Path))
	parent, errn := e.ensureParent(jctx, parentPath)
	if errn != 0 {
		return errno(errn)
	}
	entry, errn := v.Symlink(jctx, op.Target, parent, pathutil.BaseName(op.Path))
	if errn == syscall.EEXIST {
		return nil
	}
	if errn != 0 {
		return errno(errn)
	}
	if entry != nil {
		e.rememberExtentIno(op.Path, uint64(entry.Inode))
	}
	return nil
}

// readlinkIno serves a symlink's target from the JuiceFS namespace (v1
// extentReadlink).
func (e *Extent) readlinkIno(ctx context.Context, path string, ino uint64, caller fs.Caller) (string, error) {
	v := e.vfs()
	if v == nil {
		return "", syscall.EIO
	}
	target, errn := v.Readlink(e.jfsCtx(caller), jfsmeta.Ino(ino))
	if errn != 0 {
		return "", errno(errn)
	}
	return string(target), nil
}

// setAttrViaVFS ports v1 extentSetAttr's non-size fields: one VFS SetAttr
// applies mode/owner/mtime so the jfs inode (the stat authority for extent
// files) reflects the change.
func (e *Extent) setAttrViaVFS(ctx context.Context, op *fs.Op, ino uint64, vfsFh uint64) (fs.Result, error) {
	v := e.vfs()
	if v == nil {
		return fs.Result{}, syscall.EIO
	}
	set := 0
	mode, uid, gid := uint32(0), uint32(0), uint32(0)
	atime, mtime := int64(0), int64(0)
	atimens, mtimens := uint32(0), uint32(0)
	size := uint64(0)
	if op.Size >= 0 {
		// Size rides the SAME SetAttr call: an early-return truncate
		// dropped same-request chmod/chown/mtime fields while still
		// reporting success.
		set |= int(gofuse.FATTR_SIZE)
		size = uint64(op.Size)
	}
	if op.HasMode {
		set |= int(gofuse.FATTR_MODE)
		mode = op.Mode & 0o7777
	}
	if op.HasSetUID {
		set |= int(gofuse.FATTR_UID)
		uid = op.SetUID
	}
	if op.HasSetGID {
		set |= int(gofuse.FATTR_GID)
		gid = op.SetGID
	}
	if !op.Mtime.IsZero() {
		set |= int(gofuse.FATTR_MTIME)
		mtime = op.Mtime.Unix()
		mtimens = uint32(op.Mtime.Nanosecond())
	}
	if set == 0 {
		// Nothing VFS-side to apply (times are mount-local on the classic
		// plane): answer from the current jfs attrs.
		return e.statViaVFS(ctx, op.Path, ino)
	}
	jctx := e.jfsCtx(op.Caller)
	entry, errn := v.SetAttr(jctx, jfsmeta.Ino(ino), set, vfsFh, mode, uid, gid, atime, mtime, atimens, mtimens, size)
	if errn != 0 {
		return fs.Result{}, errno(errn)
	}
	attr := fs.Attr{}
	if entry != nil && entry.Attr != nil {
		attr = attrFromJuice(entry.Attr)
	}
	return fs.Result{Attr: &attr}, nil
}

func splitPath(p string) []string {
	var out []string
	for _, seg := range splitSlash(p) {
		if seg != "" {
			out = append(out, seg)
		}
	}
	return out
}

func splitSlash(p string) []string {
	var out []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			if i > start {
				out = append(out, p[start:i])
			}
			start = i + 1
		}
	}
	if start < len(p) {
		out = append(out, p[start:])
	}
	return out
}
