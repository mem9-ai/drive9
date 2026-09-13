package fuse

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

type extentRuntime struct {
	rt   *extent.Runtime
	meta jfsmeta.Meta
	stop context.CancelFunc
	// wg tracks the background compaction loop. FlushAll cancels it and then
	// joins it, so unmount cannot return while a compaction is still
	// uploading or committing: exiting between the object PUT and the compact
	// CAS would leak the merged blob.
	wg sync.WaitGroup
}

func (fs *Dat9FS) ensureExtentRuntime() error {
	fs.extentMu.Lock()
	defer fs.extentMu.Unlock()
	if fs.extentRT != nil {
		return nil
	}
	if fs.client == nil {
		return fmt.Errorf("extent runtime: missing client")
	}
	cred, err := fs.client.GetDataCredential(context.Background())
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
	rt, err := extent.NewRuntime(extent.RuntimeConfig{
		CacheDir:  cacheDir,
		CacheKey:  extent.CacheKeyForPrefix(cred.Prefix),
		Transport: extent.NewHTTPTransport(fs.client),
		Storage:   store,
	})
	if err != nil {
		return err
	}
	if rt.VFS == nil {
		return fmt.Errorf("extent runtime: missing juicefs VFS")
	}
	fmt.Fprintf(os.Stderr, "drive9: extent juicefs cache-dir=%s buffer-size=%d\n",
		rt.ChunkConf.CacheDir, rt.ChunkConf.BufferSize)
	ctx, cancel := context.WithCancel(context.Background())
	er := &extentRuntime{
		rt:   rt,
		meta: rt.Meta,
		stop: cancel,
	}
	fs.extentRT = er
	er.wg.Add(1)
	go func() {
		defer er.wg.Done()
		fs.extentCompactLoop(ctx)
	}()
	return nil
}

func (fs *Dat9FS) extentVFS() *vfs.VFS {
	if fs.extentRT == nil || fs.extentRT.rt == nil {
		return nil
	}
	return fs.extentRT.rt.VFS
}

func (fs *Dat9FS) jfsMetaCtx(pid, uid, gid uint32) jfsmeta.Context {
	gids := []uint32{gid}
	seen := map[uint32]struct{}{gid: {}}
	for _, extra := range processSupplementaryGroups(pid) {
		if _, ok := seen[extra]; ok {
			continue
		}
		seen[extra] = struct{}{}
		gids = append(gids, extra)
	}
	return jfsmeta.NewContext(pid, uid, gids)
}

func (fs *Dat9FS) jfsCtx(pid, uid, gid uint32) vfs.LogContext {
	return vfs.NewLogContext(fs.jfsMetaCtx(pid, uid, gid))
}

// jfsFuseContext is JuiceFS pkg/fuse/context.go fuseContext: Canceled()
// ignores the FUSE interrupt channel for the first second, then polls it.
// JuiceFS does not cancel the in-flight meta txn via context.Done() — it
// only returns EINTR between lock retries. Wrapping a WithCancel context
// aborted HTTP setlk as EIO and broke blocking SetLkw.
type jfsFuseContext struct {
	vfs.LogContext
	cancel    <-chan struct{}
	lockBlock bool
}

func (c *jfsFuseContext) Canceled() bool {
	if c == nil || c.LogContext == nil {
		return false
	}
	if c.Duration() < time.Second {
		return false
	}
	if c.cancel == nil {
		return false
	}
	select {
	case <-c.cancel:
		return true
	default:
		return false
	}
}

func (c *jfsFuseContext) Value(key any) any {
	if c != nil && c.lockBlock && key == extent.LockBlockCtxKey {
		return true
	}
	if c == nil || c.LogContext == nil {
		return nil
	}
	return c.LogContext.Value(key)
}

// jfsCtxCancel is JuiceFS fuse newContext for blocking VFS ops (SetLkw).
func (fs *Dat9FS) jfsCtxCancel(cancel <-chan struct{}, pid, uid, gid uint32) (vfs.LogContext, context.CancelFunc) {
	return fs.jfsCtxCancelLock(cancel, pid, uid, gid, false)
}

func (fs *Dat9FS) jfsCtxCancelLock(cancel <-chan struct{}, pid, uid, gid uint32, block bool) (vfs.LogContext, context.CancelFunc) {
	return &jfsFuseContext{LogContext: fs.jfsCtx(pid, uid, gid), cancel: cancel, lockBlock: block}, func() {}
}

func jfsLogCtx(metaCtx jfsmeta.Context) vfs.LogContext {
	return vfs.NewLogContext(metaCtx)
}

func (fs *Dat9FS) juiceDirCreateMode(localDir string, defaultMode uint16, defaultUID, defaultGID uint32) (uint16, uint32, uint32) {
	mode, uid, gid := defaultMode, defaultUID, defaultGID
	if fs.inodes == nil {
		return mode, uid, gid
	}
	candidates := []string{localDir}
	if localDir != "/" {
		trimmed := strings.TrimSuffix(localDir, "/")
		candidates = append(candidates, trimmed, trimmed+"/")
	}
	for _, p := range candidates {
		ino, ok := fs.inodes.GetInode(p)
		if !ok {
			continue
		}
		e, ok := fs.inodes.GetEntry(ino)
		if !ok || !e.IsDir {
			continue
		}
		if e.HasMode {
			mode = uint16(e.Mode & posixPermissionModeMask)
		}
		if e.HasUID {
			uid = e.Uid
		}
		if e.HasGID {
			gid = e.Gid
		}
		return mode, uid, gid
	}
	return mode, uid, gid
}

func (fs *Dat9FS) bindJuiceDirIno(localDir string, jfsIno jfsmeta.Ino) {
	if fs.inodes == nil || jfsIno == 0 {
		return
	}
	candidates := []string{localDir}
	if localDir != "/" {
		trimmed := strings.TrimSuffix(localDir, "/")
		candidates = append(candidates, trimmed, trimmed+"/")
	}
	for _, p := range candidates {
		if ino, ok := fs.inodes.GetInode(p); ok {
			fs.inodes.SetExtentIno(ino, uint64(jfsIno))
			return
		}
	}
}

// juiceDirInoFromFuseNode resolves a FUSE *directory* node to its JuiceFS
// inode. The only caller is juiceParentForOp, which is handed the kernel's
// parent nodeid, so a non-directory entry is not a valid answer: the meta
// engine only re-resolves a parent that does not exist, so a file inode
// accepted here would become the parent of a create/rename and put children
// under a file. A directory whose routing is stale is rejected too, and the
// caller's path walk re-resolves it.
func (fs *Dat9FS) juiceDirInoFromFuseNode(nodeId uint64) (jfsmeta.Ino, bool) {
	if fs.inodes == nil || nodeId == 0 {
		return 0, false
	}
	e, ok := fs.inodes.GetEntry(nodeId)
	if !ok || !e.IsDir || e.ExtentIno == 0 {
		return 0, false
	}
	return jfsmeta.Ino(e.ExtentIno), true
}

// juiceParentForOp is JuiceFS fuse Create/Unlink: the kernel already has
// the parent inode. A path walk (Lookup per component) made every DELETE
// journal create/unlink extra HTTP.
func (fs *Dat9FS) juiceParentForOp(ctx vfs.LogContext, fuseParent uint64, parentPath string) (jfsmeta.Ino, syscall.Errno) {
	if ino, ok := fs.juiceDirInoFromFuseNode(fuseParent); ok {
		return ino, 0
	}
	if ino, ok := fs.juiceParentIno(parentPath); ok {
		return ino, 0
	}
	return fs.extentEnsureParent(ctx, parentPath)
}

// extentParentReadOnly resolves the JuiceFS inode of a directory on a read
// path (Opendir, readdirplus, a by-name extent probe) without creating one.
//
// Only a create or another mutation may mirror a directory into JuiceFS: the
// JuiceFS directory tree exists so name operations have a parent to attach to,
// and the create path that needs it (extentEnsureParent) also writes extent_ino
// back onto that directory's projection row. Creating nodes from a readdir
// leaked a JuiceFS directory for every directory a mount ever listed —
// including local-only ones such as .git, which has no drive9 projection row
// and therefore no owner to remove it. The leaked subtree then made rmdir of
// an ancestor fail with ENOTEMPTY forever while `ls` (projection-based) showed
// an empty directory, so `rm -rf` of a git working tree could not finish.
func (fs *Dat9FS) extentParentReadOnly(dirPath string) (jfsmeta.Ino, bool) {
	return fs.juiceParentIno(dirPath)
}

func (fs *Dat9FS) juiceParentIno(localDir string) (jfsmeta.Ino, bool) {
	if fs.inodes == nil {
		return 0, false
	}
	if localDir == "" || localDir == "/" {
		if e, ok := fs.inodes.GetEntry(1); ok && e.IsDir && e.ExtentIno != 0 {
			return jfsmeta.Ino(e.ExtentIno), true
		}
		if fs.juiceMountRootIsTenantRoot() {
			return jfsmeta.RootInode, true
		}
		// Subtree mount: the drive9 root is a directory inside the tenant
		// JuiceFS tree, not the JuiceFS root. Answering RootInode here put
		// every subtree mount's root-level entries at the tenant root, so
		// identical relative names (e.g. fsx.bin) collided across mounts and
		// one mount wrote into another mount's inode. Return false so the
		// caller mirrors the remote path via extentEnsureParent.
		return 0, false
	}
	for _, p := range []string{localDir, strings.TrimSuffix(localDir, "/") + "/", strings.TrimSuffix(localDir, "/")} {
		if ino, ok := fs.inodes.GetInode(p); ok {
			// Only a directory may be a parent: the same stale-routing case as
			// juiceDirInoFromFuseNode reaches this lookup.
			if e, ok := fs.inodes.GetEntry(ino); ok && e.IsDir && e.ExtentIno != 0 {
				return jfsmeta.Ino(e.ExtentIno), true
			}
		}
	}
	return 0, false
}

// juiceMountRootIsTenantRoot reports whether the drive9 mount root is the
// tenant root (RemoteRoot "/"), where drive9 paths and JuiceFS paths are the
// same tree.
func (fs *Dat9FS) juiceMountRootIsTenantRoot() bool {
	if fs == nil {
		return true
	}
	return fs.remoteRoot() == "/"
}

func (fs *Dat9FS) extentEnsureParent(ctx vfs.LogContext, dirPath string) (jfsmeta.Ino, syscall.Errno) {
	v := fs.extentVFS()
	if v == nil {
		return 0, syscall.EIO
	}
	// Walk the remote path so subtree mounts (RemoteRoot != "/") share the
	// tenant JuiceFS tree and file_nodes projection with a full-root mount.
	remoteDir := fs.remotePath(dirPath)
	if remoteDir == "/" || remoteDir == "" {
		fs.bindJuiceDirIno("/", jfsmeta.RootInode)
		return jfsmeta.RootInode, 0
	}
	parent := jfsmeta.RootInode
	if fs.juiceMountRootIsTenantRoot() {
		// Only a tenant-root mount maps drive9 "/" onto the JuiceFS root.
		// A subtree mount must bind "/" to the mirrored remote root instead
		// (done by the walk below); binding it to the JuiceFS root here made
		// concurrent juiceParentIno("/") callers create at the tenant root.
		fs.bindJuiceDirIno("/", jfsmeta.RootInode)
	}
	rel := strings.Trim(remoteDir, "/")
	acc := ""
	for _, part := range strings.Split(rel, "/") {
		if part == "" {
			continue
		}
		acc += "/" + part
		entry, st := v.Lookup(ctx, parent, part)
		if st == syscall.ENOENT {
			mode, uid, gid := fs.juiceDirCreateMode(acc, 0755, ctx.Uid(), ctx.Gid())
			mkdirCtx := jfsLogCtx(fs.jfsMetaCtx(ctx.Pid(), uid, gid).WithValue(jfsmeta.Drive9PathKey, acc+"/"))
			entry, st = v.Mkdir(mkdirCtx, parent, part, mode, 0)
			if st == syscall.EEXIST {
				entry, st = v.Lookup(ctx, parent, part)
			}
		}
		if st != 0 {
			return 0, st
		}
		if entry == nil {
			return 0, syscall.EIO
		}
		parent = entry.Inode
		if local, ok := fs.localPath(acc); ok {
			fs.bindJuiceDirIno(local, parent)
		}
	}
	return parent, 0
}

func (fs *Dat9FS) extentMkdir(input *gofuse.MkdirIn, name, childP string, mode uint32) gofuse.Status {
	if !fs.extentEnabled() {
		return gofuse.OK
	}
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	metaCtx := fs.jfsMetaCtx(input.Pid, input.Uid, input.Gid)
	ctx := jfsLogCtx(metaCtx)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.juiceParentForOp(ctx, input.NodeId, parentPath)
	if st != 0 {
		return gofuse.Status(st)
	}
	remote := fs.remotePath(childP)
	if !strings.HasSuffix(remote, "/") {
		remote += "/"
	}
	ctx = jfsLogCtx(metaCtx.WithValue(jfsmeta.Drive9PathKey, remote))
	entry, errn := v.Mkdir(ctx, parentIno, name, uint16(mode&posixPermissionModeMask), 0)
	if errn == syscall.EEXIST {
		entry, errn = v.Lookup(ctx, parentIno, name)
	}
	if errn != 0 {
		return gofuse.Status(errn)
	}
	if entry != nil {
		fs.bindJuiceDirIno(childP, entry.Inode)
	}
	return gofuse.OK
}

func (fs *Dat9FS) extentMknod(cancel <-chan struct{}, input *gofuse.MknodIn, name, childP string, mode uint32, out *gofuse.EntryOut) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	_ = cancel
	metaCtx := fs.jfsMetaCtx(input.Pid, input.Uid, input.Gid)
	ctx := jfsLogCtx(metaCtx)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.juiceParentForOp(ctx, input.NodeId, parentPath)
	if st != 0 {
		return gofuse.Status(st)
	}
	ctx = jfsLogCtx(metaCtx.WithValue(jfsmeta.Drive9PathKey, fs.remotePath(childP)))
	entry, errn := v.Mknod(ctx, parentIno, name, uint16(mode), uint16(input.Umask), input.Rdev)
	if errn != 0 {
		return gofuse.Status(errn)
	}
	now := time.Now()
	driveIno := fs.inodes.Lookup(childP, false, 0, now)
	if entry != nil {
		fs.inodes.SetExtentIno(driveIno, uint64(entry.Inode))
	}
	fs.inodes.UpdateMode(driveIno, mode)
	fs.inodes.UpdateRdev(driveIno, input.Rdev)
	fs.inodes.UpdateOwner(driveIno, input.Uid, input.Gid, true, true)
	if e, ok := fs.inodes.GetEntry(driveIno); ok {
		if entry != nil && entry.Attr != nil {
			fs.applyJuiceFSAttr(e, entry.Attr)
		}
		parentPath, _ = fs.inodes.GetPath(input.NodeId)
		fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(name, e))
		fs.touchDirectoryChangeTime(parentPath, now)
		fs.fillEntryOut(e, out)
		return gofuse.OK
	}
	return gofuse.EIO
}

func (fs *Dat9FS) extentSymlink(cancel <-chan struct{}, header *gofuse.InHeader, pointedTo, linkName, childP string, out *gofuse.EntryOut) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	_ = cancel
	metaCtx := fs.jfsMetaCtx(header.Pid, header.Uid, header.Gid)
	ctx := jfsLogCtx(metaCtx)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.juiceParentForOp(ctx, header.NodeId, parentPath)
	if st != 0 {
		return gofuse.Status(st)
	}
	ctx = jfsLogCtx(metaCtx.WithValue(jfsmeta.Drive9PathKey, fs.remotePath(childP)))
	entry, errn := v.Symlink(ctx, pointedTo, parentIno, linkName)
	if errn != 0 {
		return gofuse.Status(errn)
	}
	now := time.Now()
	mode := symlinkMode()
	driveIno := fs.inodes.Lookup(childP, false, int64(len(pointedTo)), now)
	if entry != nil {
		fs.inodes.SetExtentIno(driveIno, uint64(entry.Inode))
	}
	fs.inodes.UpdateMode(driveIno, mode)
	fs.inodes.UpdateOwner(driveIno, header.Uid, header.Gid, true, true)
	if e, ok := fs.inodes.GetEntry(driveIno); ok {
		if entry != nil && entry.Attr != nil {
			fs.applyJuiceFSAttr(e, entry.Attr)
		}
		parentPath, _ = fs.inodes.GetPath(header.NodeId)
		fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(linkName, e))
		fs.touchDirectoryChangeTime(parentPath, now)
		fs.invalidateReadCacheAndTargets(childP)
		fs.fillEntryOut(e, out)
		return gofuse.OK
	}
	return gofuse.EIO
}

func (fs *Dat9FS) extentReadlink(entry *InodeEntry) ([]byte, gofuse.Status) {
	if entry == nil || entry.ExtentIno == 0 {
		return nil, gofuse.ENOENT
	}
	if err := fs.ensureExtentRuntime(); err != nil {
		return nil, gofuse.EIO
	}
	if fs.extentRT == nil || fs.extentRT.rt == nil || fs.extentRT.rt.Transport == nil {
		return nil, gofuse.EIO
	}
	var resp struct {
		Errno  int    `json:"errno"`
		Target []byte `json:"target"`
	}
	ctx := fs.jfsCtx(0, 0, 0)
	if call := fs.extentRT.rt.Transport.Call(ctx, "readlink", map[string]any{"inode": entry.ExtentIno}, &resp); call != 0 {
		return nil, gofuse.Status(call)
	}
	if resp.Errno != 0 {
		return nil, gofuse.Status(syscall.Errno(resp.Errno))
	}
	return resp.Target, gofuse.OK
}

func (fs *Dat9FS) extentRmdir(header *gofuse.InHeader, name, childP string) gofuse.Status {
	if !fs.extentEnabled() {
		return gofuse.OK
	}
	if fs.extentRT == nil {
		return gofuse.OK
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.OK
	}
	ctx := fs.jfsCtx(header.Pid, header.Uid, header.Gid)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.juiceParentForOp(ctx, header.NodeId, parentPath)
	if st == syscall.ENOENT {
		return gofuse.OK
	}
	if st != 0 {
		return gofuse.Status(st)
	}
	if errn := v.Rmdir(ctx, parentIno, name); errn != 0 && errn != syscall.ENOENT {
		return gofuse.Status(errn)
	}
	return gofuse.OK
}

// extentLookupChild is JuiceFS fuse Lookup: VFS.Lookup(parent, name).
func (fs *Dat9FS) extentLookupChild(ctx vfs.LogContext, childP string) (jfsmeta.Ino, bool) {
	v := fs.extentVFS()
	if v == nil {
		return 0, false
	}
	parentPath := pathutil.ParentPath(childP)
	name := path.Base(childP)
	// Read-only probe: consult only directories that are already mirrored.
	// The projection is the source of truth for whether a file is extent (it
	// carries content_layout and extent_ino), and a create that needs a parent
	// mirrors it then.
	parentJfs, ok := fs.extentParentReadOnly(parentPath)
	if !ok {
		return 0, false
	}
	entry, errn := v.Lookup(ctx, parentJfs, name)
	if errn != 0 || entry == nil {
		return 0, false
	}
	return entry.Inode, true
}

func (fs *Dat9FS) extentCreate(cancel <-chan struct{}, input *gofuse.CreateIn, name, childP string, out *gofuse.CreateOut) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	metaCtx := fs.jfsMetaCtx(input.Pid, input.Uid, input.Gid)
	ctx := jfsLogCtx(metaCtx)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.juiceParentForOp(ctx, input.NodeId, parentPath)
	if st != 0 {
		return gofuse.Status(st)
	}
	ctx = jfsLogCtx(metaCtx.WithValue(jfsmeta.Drive9PathKey, fs.remotePath(childP)))
	mode := uint16(input.Mode)
	if mode == 0 {
		mode = 0644
	}
	entry, vfsFh, errn := v.Create(ctx, parentIno, name, mode, 0, input.Flags)
	if errn != 0 {
		return gofuse.Status(errn)
	}
	length := uint64(0)
	if entry != nil && entry.Attr != nil {
		v.UpdateLength(entry.Inode, entry.Attr)
		length = entry.Attr.Length
	}
	driveIno := fs.inodes.Lookup(childP, false, int64(length), time.Now())
	fs.inodes.SetExtentIno(driveIno, uint64(entry.Inode))
	if e, ok := fs.inodes.GetEntry(driveIno); ok {
		if entry != nil && entry.Attr != nil {
			fs.applyJuiceFSAttr(e, entry.Attr)
		} else {
			fs.inodes.UpdateMode(driveIno, uint32(mode))
			fs.inodes.UpdateOwner(driveIno, input.Uid, input.Gid, true, true)
		}
	}
	fh := &FileHandle{
		Ino:       driveIno,
		Path:      childP,
		Flags:     input.Flags,
		OpenPID:   input.Pid,
		OrigSize:  int64(length),
		extentIno: entry.Inode,
		extentFh:  vfsFh,
	}
	// This path is now an extent file, so any cached "not an extent file"
	// answer for it is wrong.
	fs.extentMissForget(childP)
	out.Fh = fs.allocateFileHandle(fh)
	out.OpenFlags = extentOpenFlags(false)
	if e, ok := fs.inodes.GetEntry(driveIno); ok {
		fs.fillEntryOut(e, &out.EntryOut)
	}
	parentPath, _ = fs.inodes.GetPath(input.NodeId)
	if e, ok := fs.inodes.GetEntry(driveIno); ok {
		fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(name, e))
	}
	fs.touchDirectoryChangeTime(parentPath, time.Now())
	return gofuse.OK
}

func (fs *Dat9FS) extentOpen(cancel <-chan struct{}, input *gofuse.OpenIn, p string, extentIno uint64, out *gofuse.OpenOut) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	ctx := fs.jfsCtx(input.Pid, input.Uid, input.Gid)
	ino := jfsmeta.Ino(extentIno)
	entry, vfsFh, errn := v.Open(ctx, ino, input.Flags)
	if errn != 0 {
		if errn == syscall.ENOENT {
			return gofuse.ENOENT
		}
		return gofuse.Status(errn)
	}
	length := uint64(0)
	keepCache := false
	if entry != nil && entry.Attr != nil {
		v.UpdateLength(ino, entry.Attr)
		length = entry.Attr.Length
		keepCache = entry.Attr.KeepCache
	}
	fs.inodes.SetExtentIno(input.NodeId, uint64(ino))
	fs.inodes.UpdateSize(input.NodeId, int64(length))
	fh := &FileHandle{
		Ino:       input.NodeId,
		Path:      p,
		Flags:     input.Flags,
		OpenPID:   input.Pid,
		OrigSize:  int64(length),
		extentIno: ino,
		extentFh:  vfsFh,
	}
	out.Fh = fs.allocateFileHandle(fh)
	out.OpenFlags = extentOpenFlags(keepCache)
	if out.OpenFlags == 0 {
		// JuiceFS fuse Open: InodeNotify when !KeepCache (close-to-open).
		fs.notifyInode(input.NodeId)
	}
	return gofuse.OK
}

func extentOpenFlags(keepCache bool) uint32 {
	if keepCache {
		return gofuse.FOPEN_KEEP_CACHE
	}
	return 0
}

// sqliteWALIndexMinSize is one WAL-index region (unixShmRegionPerMap).
// MAP_SHARED dirty pages at i_size 0 make close() return EIO without a
// FUSE WRITE (unixShmPurge SQLITE_IOERR_CLOSE).
//
// This is the one application-shaped rule in this layer. It exists because
// SQLite's close-time check is unconditional rather than because a filesystem
// is allowed to invent a size, and it is confined here so the exception is
// visible: the two call sites (GetAttr and SetAttrSize) are the only places
// permitted to widen it, and a second per-application special case should
// become a policy table instead of another branch beside this one.
const sqliteWALIndexMinSize int64 = 32768

// extentKeepOpenWALIndexSize gives a still-open sqlite -shm file the size its
// mapping needs. See sqliteWALIndexMinSize.
func (fs *Dat9FS) extentKeepOpenWALIndexSize(entry *InodeEntry) {
	if fs == nil || entry == nil || entry.Size > 0 {
		return
	}
	if !isSQLiteWALIndexPath(entry.Path) {
		return
	}
	if fs.openHandles == nil || !fs.openHandles.Has(0, entry.Path) {
		return
	}
	entry.Size = sqliteWALIndexMinSize
	if fs.inodes != nil && entry.Ino != 0 {
		fs.inodes.UpdateSize(entry.Ino, entry.Size)
	}
}

func (fs *Dat9FS) extentUnlink(cancel <-chan struct{}, header *gofuse.InHeader, name, childP string) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	opened := false
	if fs.openHandles != nil {
		opened = len(fs.openHandles.SnapshotPath(childP)) > 0
	}
	std, cf := fuseCtx(cancel)
	defer cf()
	marked, preserveOpen, _ := fs.markOpenHandlesUnlinked(std, childP, false)
	opened = opened || preserveOpen
	metaCtx := fs.jfsMetaCtx(header.Pid, header.Uid, header.Gid).
		WithValue(jfsmeta.Drive9OpenedKey, opened).
		WithValue(jfsmeta.Drive9PathKey, fs.remotePath(childP))
	ctx := jfsLogCtx(metaCtx)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.juiceParentForOp(ctx, header.NodeId, parentPath)
	if st != 0 {
		fs.unmarkOpenHandlesAfterFailedUnlink(childP, marked)
		return gofuse.Status(st)
	}
	if errn := v.Unlink(ctx, parentIno, name); errn != 0 && errn != syscall.ENOENT {
		fs.unmarkOpenHandlesAfterFailedUnlink(childP, marked)
		return gofuse.Status(errn)
	}
	// The meta unlink above already removed the edge, the node and the
	// projection in one transaction; a second HTTP DELETE would be a redundant
	// RPC and could delete a file that re-created the same path in between.
	if opened {
		fs.inodes.RemoveLinkPreserve(childP)
	} else {
		fs.inodes.RemoveLink(childP)
	}
	parentPath, _ = fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Remove(parentPath, name)
	fs.touchDirectoryChangeTime(parentPath, time.Now())
	fs.cacheNegativePath(childP)
	fs.extentMissForget(childP)
	return gofuse.OK
}

func (fs *Dat9FS) extentRead(ctx vfs.LogContext, fh *FileHandle, off uint64, buf []byte) (int, gofuse.Status) {
	v := fs.extentVFS()
	if v == nil || fh == nil || fh.extentFh == 0 {
		return 0, gofuse.EBADF
	}
	n, errn := v.Read(ctx, fh.extentIno, buf, off, fh.extentFh)
	if errn != 0 {
		return n, gofuse.Status(errn)
	}
	return n, gofuse.OK
}
func juiceAppendOff(off, length uint64) uint64 {
	if length > off {
		return length
	}
	return off
}

// juiceWriteOffset is JuiceFS fuse Write for O_APPEND: kernel syscall writes
// can arrive with off=0, so bump to the VFS length.
func juiceWriteOffset(off, length uint64, append bool) uint64 {
	if !append || off != 0 {
		return off
	}
	return juiceAppendOff(off, length)
}

func (fs *Dat9FS) extentWrite(ctx vfs.LogContext, fh *FileHandle, off uint64, data []byte) gofuse.Status {
	v := fs.extentVFS()
	if v == nil || fh == nil || fh.extentFh == 0 {
		return gofuse.EBADF
	}
	if fh.Flags&uint32(syscall.O_APPEND) != 0 {
		if entry, errn := v.GetAttr(ctx, fh.extentIno, 1); errn == 0 && entry != nil && entry.Attr != nil {
			v.UpdateLength(fh.extentIno, entry.Attr)
			off = juiceWriteOffset(off, entry.Attr.Length, true)
		}
	}
	if errn := v.Write(ctx, fh.extentIno, data, off, fh.extentFh); errn != 0 {
		fmt.Fprintf(os.Stderr, "drive9: extent write %s ino=%d off=%d n=%d err=%v\n", fh.Path, fh.extentIno, off, len(data), errn)
		// Only ENOENT is tolerated: the meta no longer has this inode (the
		// file was unlinked), so there is nothing to write to. EBADF means a
		// handle bug and must reach the caller, otherwise a write or an fsync
		// reports success without touching any data.
		if errn == syscall.ENOENT {
			return gofuse.OK
		}
		return gofuse.Status(errn)
	}
	end := int64(off + uint64(len(data)))
	if e, ok := fs.inodes.GetEntry(fh.Ino); ok && end > e.Size {
		fs.inodes.UpdateSize(fh.Ino, end)
	}
	// write-sync's contract is "remote-durable when write() returns", so the
	// block upload and the metadata commit have to happen before this reply.
	// fsync/close-sync are covered by Fsync/Flush/Release.
	if fs.mountWritePolicy() == WritePolicyWriteSync {
		if errn := v.Flush(ctx, fh.extentIno, fh.extentFh, 0); errn != 0 {
			fmt.Fprintf(os.Stderr, "drive9: extent write-sync flush %s ino=%d err=%v\n", fh.Path, fh.extentIno, errn)
			return gofuse.Status(errn)
		}
	}
	return gofuse.OK
}

// extentClearSetidAfterWrite is Linux file_remove_privs: a non-root write
// clears SUID/SGID so fstat reports 0777 after writing a 04777 file.
func (fs *Dat9FS) extentClearSetidAfterWrite(fh *FileHandle, callerUID uint32) {
	if callerUID == 0 || fh == nil || fh.extentIno == 0 {
		return
	}
	v := fs.extentVFS()
	if v == nil {
		return
	}
	// A write can only drop set-user-ID/set-group-ID while the file still has
	// them, and the inode already knows its mode on any path that reached a
	// handle. JuiceFS invalidates its open-file attr cache after each committed
	// meta write, so the GetAttr below costs one HTTP meta op per 4 KiB write —
	// the dominant per-write cost of the cap-off extent path.
	entry, hasEntry := fs.inodes.GetEntry(fh.Ino)
	if hasEntry && entry.HasMode && entry.Mode&(setUIDPermissionBit|setGIDPermissionBit) == 0 {
		return
	}
	ctx := fs.jfsCtx(0, 0, 0)
	jentry, errn := v.GetAttr(ctx, fh.extentIno, 1)
	if errn != 0 || jentry == nil || jentry.Attr == nil {
		return
	}
	mode := uint32(jentry.Attr.Mode)
	if mode&(setUIDPermissionBit|setGIDPermissionBit) == 0 {
		// Remember the clean mode so later writes skip the GetAttr entirely.
		if hasEntry {
			fs.inodes.SetModeState(fh.Ino, juiceTypeToStatMode(jentry.Attr.Typ, uint16(mode)), true)
		}
		return
	}
	mode &^= setUIDPermissionBit | setGIDPermissionBit
	jentry, errn = v.SetAttr(ctx, fh.extentIno, int(jfsmeta.SetAttrMode), fh.extentFh, mode, 0, 0, 0, 0, 0, 0, 0)
	if errn != 0 || jentry == nil || jentry.Attr == nil {
		return
	}
	if e, ok := fs.inodes.GetEntry(fh.Ino); ok {
		fs.applyJuiceFSAttr(e, jentry.Attr)
	}
}

func (fs *Dat9FS) extentFlush(ctx vfs.LogContext, fh *FileHandle, lockOwner uint64) gofuse.Status {
	v := fs.extentVFS()
	if v == nil || fh == nil || fh.extentFh == 0 {
		return gofuse.OK
	}
	start := time.Now()
	if errn := v.Flush(ctx, fh.extentIno, fh.extentFh, lockOwner); errn != 0 {
		fmt.Fprintf(os.Stderr, "drive9: extent flush %s ino=%d err=%v dur=%s\n", fh.Path, fh.extentIno, errn, time.Since(start))
		// Only ENOENT is tolerated: the meta no longer has this inode (the
		// file was unlinked), so there is nothing to write to. EBADF means a
		// handle bug and must reach the caller, otherwise a write or an fsync
		// reports success without touching any data.
		if errn == syscall.ENOENT {
			return gofuse.OK
		}
		return gofuse.Status(errn)
	}
	if d := time.Since(start); d >= 200*time.Millisecond {
		fmt.Fprintf(os.Stderr, "drive9: extent flush %s ino=%d dur=%s\n", fh.Path, fh.extentIno, d)
	}
	return gofuse.OK
}

func (fs *Dat9FS) extentFsync(ctx vfs.LogContext, fh *FileHandle, datasync int) gofuse.Status {
	v := fs.extentVFS()
	if v == nil || fh == nil || fh.extentFh == 0 {
		return gofuse.OK
	}
	start := time.Now()
	if errn := v.Fsync(ctx, fh.extentIno, datasync, fh.extentFh); errn != 0 {
		fmt.Fprintf(os.Stderr, "drive9: extent fsync %s ino=%d err=%v dur=%s\n", fh.Path, fh.extentIno, errn, time.Since(start))
		// Only ENOENT is tolerated: the meta no longer has this inode (the
		// file was unlinked), so there is nothing to write to. EBADF means a
		// handle bug and must reach the caller, otherwise a write or an fsync
		// reports success without touching any data.
		if errn == syscall.ENOENT {
			return gofuse.OK
		}
		return gofuse.Status(errn)
	}
	if d := time.Since(start); d >= 200*time.Millisecond {
		fmt.Fprintf(os.Stderr, "drive9: extent fsync %s ino=%d dur=%s\n", fh.Path, fh.extentIno, d)
	}
	return gofuse.OK
}

func (fs *Dat9FS) extentRelease(ctx vfs.LogContext, fh *FileHandle) {
	v := fs.extentVFS()
	if v == nil || fh == nil || fh.extentFh == 0 {
		return
	}
	v.Release(ctx, fh.extentIno, fh.extentFh)
}

func (fs *Dat9FS) extentAttachDirHandle(dh *DirHandle) {
	if dh == nil {
		return
	}
	v := fs.extentVFS()
	if v == nil {
		if fs.opts == nil || len(fs.opts.ExtentPaths) == 0 {
			return
		}
		if err := fs.ensureExtentRuntime(); err != nil {
			return
		}
		v = fs.extentVFS()
		if v == nil {
			return
		}
	}
	ctx := fs.jfsCtx(0, 0, 0)
	parent, ok := fs.extentParentReadOnly(dh.Path)
	if !ok {
		return
	}
	fh, errn := v.Opendir(ctx, parent, 0)
	if errn != 0 {
		return
	}
	dh.extentIno = parent
	dh.extentFh = fh
}

func (fs *Dat9FS) extentReleaseDir(dh *DirHandle) {
	if dh == nil || dh.extentFh == 0 {
		return
	}
	v := fs.extentVFS()
	if v == nil {
		dh.extentFh = 0
		return
	}
	v.Releasedir(fs.jfsCtx(0, 0, 0), dh.extentIno, dh.extentFh)
	dh.extentFh = 0
}
