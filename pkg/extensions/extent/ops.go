// VFS namespace helpers: attribute projection through the JuiceFS session,
// creation under the extent plane's parent inode (with the drive9
// projection path riding the context so the server dual-writes the classic
// rows in the same transaction), and the create-op orchestration that
// routes each open to its plane.
package extent

import (
	"context"
	"path"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// statViaVFS projects one VFS.GetAttr on the resolved inode, mapped
// through juiceTypeToStatMode. A VFS error (including ENOENT) is reported
// to the caller, which then falls back to the classic plane below.
func (e *Extent) statViaVFS(ctx context.Context, path string, ino uint64) (fs.Result, error) {
	v := e.vfs()
	if v == nil {
		return fs.Result{}, syscall.EIO
	}
	entry, errn := v.GetAttr(e.jfsCtx(callerFrom(ctx)), jfsmeta.Ino(ino), 0)
	if errn != 0 || entry == nil || entry.Attr == nil {
		if errn == syscall.ENOENT {
			e.paths.missRemember(path)
		}
		return fs.Result{}, errno(errn)
	}
	attr := attrFromJuice(entry.Attr)
	e.keepOpenWALIndexSize(path, &attr)
	e.paths.rememberIno(path, ino)
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
	if !e.paths.hasLive(path) {
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
// the caller's mode, and returns its inode with the VFS fh Create already
// allocated (re-opening through Open would leak that file reference —
// Release only closes the second fh) and the created attributes. The drive9
// projection path rides the meta context (Drive9PathKey) and every mirrored
// parent mkdir carries its own, so the server writes the JuiceFS rows and
// the classic file_nodes/contents projection in one transaction — without
// the key the extent file would be invisible to the classic plane.
func (e *Extent) createInVFS(ctx context.Context, op *fs.Op) (uint64, uint64, *jfsmeta.Attr, syscall.Errno) {
	v := e.vfs()
	if v == nil {
		return 0, 0, nil, syscall.EIO
	}
	jctx := e.jfsCtx(op.Caller)
	parent, errn := e.ensureParent(jctx, pathutil.DirOf(op.Path))
	if errn != 0 {
		return 0, 0, nil, errn
	}
	mode := uint16(op.Mode & 0o7777)
	if mode == 0 && !op.HasMode {
		// Absent mode information defaults; an explicit mode 000 (a
		// promoted claim, which always knows its create mode) is honored.
		mode = 0o644
	}
	entry, fh, errn := v.Create(e.jfsCtxWithKeyedPath(op.Caller, e.remotePath(op.Path)), parent, pathutil.BaseName(op.Path), mode, 0, op.Flags&^uint32(syscall.O_TRUNC))
	if errn != 0 || entry == nil {
		return 0, 0, nil, errn
	}
	ino := uint64(entry.Inode)
	e.paths.rememberIno(op.Path, ino)
	return ino, fh, entry.Attr, 0
}

// ensureParent walks from the extent mount root, creating missing
// directories (mkdir -p semantics in the JuiceFS namespace) and returning
// the parent inode. Mirrored parents carry their projection path so the
// server stamps file_nodes.extent_ino — that stamp is what lets a later
// recursive delete find and drop the subtree.
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
	// Each created directory carries its drive9 projection path (v1
	// extentEnsureParent) and inherits the drive9-side directory's mode (v1
	// juiceDirCreateMode: sticky/permission bits must not collapse to a
	// blanket 0755). acc stays TENANT-absolute below (the walk translated
	// through remotePath above): re-prefixing would double the RemoteRoot
	// and the projection would miss the real parent row.
	caller := callerFromLogCtx(jctx)
	acc := ""
	cur := root
	for _, seg := range splitPath(dirPath) {
		acc += "/" + seg
		entry, errn := v.Lookup(jctx, cur, seg)
		if errn == syscall.ENOENT {
			mode := uint16(e.mirroredDirMode(acc))
			mkdirCtx := vfs.NewLogContext(e.jfsMetaCtx(caller).
				WithValue(jfsmeta.Drive9PathKey, acc+"/"))
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

// createOp routes one open-or-create to its plane:
//
//  1. a declared path (the mount's extent globs) is created on the extent
//     plane from byte zero — a runtime that cannot build fails closed;
//  2. an extent-layout file (resolved inode or the claim stat's
//     ContentLayout) opens through the VFS;
//  3. a fresh truncate-writer under the large-file takeover is claimed and
//     buffered, promoting to the extent plane at the threshold and
//     materializing classically below otherwise;
//  4. everything else passes through.
func (e *Extent) createOp(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	// A live claim owns the file's current image: a read-only re-open
	// serves those bytes — falling through to the plane below would read
	// the not-yet-pushed remote state (a checksum spanning the landing
	// mixes the old and new images). A writer first lands the claim.
	if op.Flags&uint32(syscall.O_CREAT) == 0 {
		if hit := e.paths.stagingAt(op.Path); hit != nil {
			if attr, ok := hit.overlayAttr(); ok {
				if !writeIntent(op.Flags) {
					return fs.Result{Attr: &attr, Handle: &claimReadHandle{claim: hit}}, nil
				}
				_ = hit.Flush(ctx)
			}
		}
	}
	// (1) declared creation.
	if ShouldUseExtentPath(e.opts.ExtentPaths, op.Path) {
		if err := e.ensureRuntime(); err != nil {
			return fs.Result{}, err
		}
		ino, fh, cattr, errn := e.createInVFS(ctx, op)
		if errn == syscall.EEXIST {
			if op.Flags&uint32(syscall.O_EXCL) != 0 {
				return fs.Result{}, syscall.EEXIST
			}
			// The path already lives on the extent plane: open it through
			// the VFS instead of creating.
			if ino == 0 {
				ino, _ = e.resolveIno(ctx, op.Path)
			}
			if ino != 0 {
				handle, openFlags, attr, oerr := e.openExtent(ctx, op.Path, ino, op.Flags, op.Caller)
				if oerr != nil {
					return fs.Result{}, oerr
				}
				return fs.Result{Handle: handle, OpenFlags: openFlags, Attr: attr}, nil
			}
		}
		if errn != 0 || ino == 0 {
			// The VFS refused for a reason other than existence (ENOSPC,
			// EACCES, ...): a declared path never silently lands on the
			// classic plane — a failed SELECTED extent create is a REAL
			// failure, and converting it to "not claimed" would let the
			// classic plane create the file under a different layout and
			// hide the error.
			if errn == 0 {
				errn = syscall.EIO
			}
			return fs.Result{}, errno(errn)
		}
		if fh != 0 {
			// Reuse the fh VFS.Create allocated (a second Open leaked the
			// first VFS file reference forever).
			handle, openFlags, attr, aerr := e.adoptCreatedHandle(op.Path, ino, fh, cattr, op.Flags&^uint32(syscall.O_TRUNC), op.Caller)
			if aerr != nil {
				return fs.Result{}, aerr
			}
			return fs.Result{Handle: handle, OpenFlags: openFlags, Attr: attr}, nil
		}
		handle, openFlags, attr, err := e.openExtent(ctx, op.Path, ino, op.Flags&^uint32(syscall.O_TRUNC), op.Caller)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Handle: handle, OpenFlags: openFlags, Attr: attr}, nil
	}

	// (2) resolved extent layout.
	if ino, ok := e.resolveIno(ctx, op.Path); ok {
		if err := e.ensureRuntime(); err != nil {
			return next(ctx, op)
		}
		handle, openFlags, attr, err := e.openExtent(ctx, op.Path, ino, op.Flags, op.Caller)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Handle: handle, OpenFlags: openFlags, Attr: attr}, nil
	}

	// (3) the large-file takeover claim.
	if e.claimsLargeFile(op) {
		return e.claimStaging(ctx, op, next)
	}
	return next(ctx, op)
}

// claimStaging takes a creating writer: it stats the path once (the
// existence answer the boundary push and the layout the routing both
// need), then wraps the below session in a buffering handle that promotes
// to the extent plane at the inline cutoff. Existing files only stay
// claimed for a truncation rewrite (the image is built from byte zero);
// a plain re-open or an append of an existing file keeps its bytes on the
// classic plane below — legacy multipart/S3 files keep updating through
// the presign link, and layouts never convert.
func (e *Extent) claimStaging(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	st, statErr := e.opts.Client.StatCtx(ctx, e.remotePath(op.Path))
	if statErr != nil && !client.IsNotFound(statErr) {
		return fs.Result{}, statErr
	}
	if statErr == nil && st != nil && st.ContentLayout == client.ContentLayoutExtent && st.ExtentIno != 0 {
		// The claim stat itself discovered an extent layout (takeover-only
		// mounts do not run the probe path before the first promotion).
		e.paths.rememberIno(op.Path, st.ExtentIno)
		if err := e.ensureRuntime(); err != nil {
			return next(ctx, op)
		}
		handle, openFlags, attr, err := e.openExtent(ctx, op.Path, st.ExtentIno, op.Flags, op.Caller)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Handle: handle, OpenFlags: openFlags, Attr: attr}, nil
	}
	if statErr == nil && op.Flags&uint32(syscall.O_TRUNC) == 0 {
		// An existing file opened WITHOUT truncation keeps its bytes on the
		// plane below: the claim's image is built from byte zero, so it can
		// only own a whole-file rewrite. A plain re-open or an append of a
		// legacy multipart/S3 file serves and updates through the classic
		// plane (the presign link) exactly as before.
		return next(ctx, op)
	}
	h := &stagingHandle{
		e: e, path: op.Path, caller: op.Caller,
		mode:  op.Mode & 0o777,
		below: fs.HandlerFilesystem(next),
	}
	h.openFlags = op.Flags&^uint32(syscall.O_TRUNC|syscall.O_EXCL) | uint32(syscall.O_CREAT)
	innerOp := *op
	innerOp.Flags &^= uint32(syscall.O_TRUNC)
	if statErr == nil {
		if op.Flags&uint32(syscall.O_EXCL) != 0 {
			return fs.Result{}, syscall.EEXIST
		}
		// Existing classic file under a truncation rewrite: open it plainly
		// below (no create/truncate flags — the write plane takes its
		// existing-file branch with a real revision base) and this handle
		// commits the truncated image against it. The truncation is pending
		// until the first boundary pushes the (shorter) image; an existing
		// layout never converts.
		innerOp.Flags &^= uint32(syscall.O_CREAT)
		h.pushed = st.Size
		h.dirty = st.Size > 0
	} else {
		// Fresh path: promotable to the extent plane at the inline cutoff
		// (a fresh O_APPEND create included — the claim owns the file from
		// byte zero), and O_CREAT kept so the plane below answers with its
		// lazy create (no remote effect until the boundary); even
		// `: > newfile`, which closes without a single write, materializes
		// at Release.
		h.promotable = true
	}
	res, err := next(ctx, &innerOp)
	if err != nil {
		return res, err
	}
	if res.Handle == nil {
		return res, nil
	}
	h.inner = res.Handle
	e.paths.trackStaging(h)
	res.Handle = h
	return res, nil
}

// openExtent ensures the runtime, VFS.Opens the pinned inode, updates the
// length cache, and returns the terminal handle. An O_TRUNC open of an
// existing inode truncates through the VFS before the handle serves: the
// kernel sends the truncation with the create, not as a separate setattr.
func (e *Extent) openExtent(ctx context.Context, path string, ino uint64, flags uint32, caller fs.Caller) (*extentHandle, uint32, *fs.Attr, error) {
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
		if flags&uint32(syscall.O_TRUNC) != 0 && length != 0 {
			if _, terrn := v.SetAttr(jctx, jino, int(jfsmeta.SetAttrSize), 0, 0, 0, 0, 0, 0, 0, 0, 0); terrn != 0 {
				v.Release(jctx, jino, vfsFh)
				return nil, 0, nil, errno(terrn)
			}
			length = 0
		}
	}
	e.paths.rememberIno(path, ino)
	attr := &fs.Attr{Kind: fs.KindFile, Size: int64(length)}
	if entry != nil && entry.Attr != nil {
		attr.Mode = juiceTypeToStatMode(entry.Attr.Typ, entry.Attr.Mode)
		attr.Rdev = entry.Attr.Rdev
		attr.Mtime = time.Unix(int64(entry.Attr.Mtime), int64(entry.Attr.Mtimensec))
		attr.Nlink = entry.Attr.Nlink
		attr.Uid = entry.Attr.Uid
		attr.Gid = entry.Attr.Gid
		attr.HasOwner = true
	}
	h := &extentHandle{e: e, path: path, ino: jino, fh: vfsFh, flags: flags, openFlag: extentOpenFlags(keepCache), caller: caller}
	e.paths.trackLiveHandle(h)
	return h, extentOpenFlags(keepCache), attr, nil
}

// adoptCreatedHandle builds a handle around the fh VFS.Create already
// allocated (re-opening through Open would leak that reference).
func (e *Extent) adoptCreatedHandle(path string, ino uint64, fh uint64, a *jfsmeta.Attr, flags uint32, caller fs.Caller) (*extentHandle, uint32, *fs.Attr, error) {
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
	e.paths.trackLiveHandle(h)
	return h, extentOpenFlags(keepCache), attr, nil
}

// readViaVFS serves a path-addressed read through a transient VFS session.
func (e *Extent) readViaVFS(ctx context.Context, path string, ino uint64, off, limit int64) ([]byte, error) {
	h, _, attr, err := e.openExtent(ctx, path, ino, 0, callerFrom(ctx))
	if err != nil {
		return nil, err
	}
	defer func() { _ = h.Release(ctx) }()
	if limit <= 0 {
		// Whole-file read: size it from the inode the open just probed.
		limit = 0
		if attr != nil {
			limit = attr.Size - off
		}
		if limit < 0 {
			limit = 0
		}
	}
	// The facade re-issues reads for larger ranges, so a huge Limit must
	// not become one allocation.
	if limit > maxPathReadBytes {
		limit = maxPathReadBytes
	}
	buf := make([]byte, limit)
	n, rerr := h.Read(ctx, off, buf)
	if rerr != nil {
		return nil, rerr
	}
	return buf[:n], nil
}

// maxPathReadBytes caps one path-addressed read.
const maxPathReadBytes = 4 << 20

// promotedAttr refreshes the reply attributes of a promoted staging claim.
func (e *Extent) promotedAttr(ctx context.Context, h *extentHandle) (fs.Attr, error) {
	v := e.vfs()
	if v == nil {
		return fs.Attr{}, syscall.EIO
	}
	entry, errn := v.GetAttr(e.jfsCtx(callerFrom(ctx)), h.ino, 0)
	if errn != 0 || entry == nil || entry.Attr == nil {
		return fs.Attr{}, errno(errn)
	}
	v.UpdateLength(h.ino, entry.Attr)
	return attrFromJuice(entry.Attr), nil
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
// row. Only mirrored parents participate (an unmirrored parent means this
// subtree is classic).
func (e *Extent) mirrorMkdir(ctx context.Context, op *fs.Op) error {
	parentPath := pathutil.DirOf(op.Path)
	if _, ok := e.paths.cachedIno(parentPath); !ok {
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
		e.paths.rememberIno(op.Path, uint64(entry.Inode))
	}
	return nil
}

// subtreeMirrored reports whether any ancestor of dirPath is bound to an
// extent inode (the mirrored-subtree gate for mirror ops).
func (e *Extent) subtreeMirrored(dirPath string) bool {
	for dirPath != "" && dirPath != "/" {
		if _, ok := e.paths.cachedIno(dirPath); ok {
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
	e.paths.rememberIno(op.Path, ino)
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
	if _, ok := e.paths.cachedIno(parentPath); !ok {
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
		e.paths.rememberIno(op.Path, uint64(entry.Inode))
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

// setAttrViaVFS ports v1 extentSetAttr: one VFS SetAttr applies
// mode/owner/mtime — and the size when the request carries one — so the jfs
// inode (the stat authority for extent files) reflects the change. vfsFh is
// the descriptor's own fh for FD-named setattr (0 for path setattr): fh=0
// makes JuiceFS re-check permissions against the CURRENT mode (a legal
// ftruncate after a revoking chmod would answer EACCES).
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
		// reporting success (the kernel merges kill-priv mode changes
		// into truncates).
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
