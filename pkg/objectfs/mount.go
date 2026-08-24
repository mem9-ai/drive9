//go:build !windows

package objectfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// Mount blocks until the filesystem is unmounted.
func Mount(opts Options) error {
	// rclone S3 stores this context and uses it for backend Command("set")
	// when refreshing STS. It must outlive the mount, not the initial open.
	fsCtx, stopFs := context.WithCancel(context.Background())
	defer stopFs()
	f, fileLeaf, err := OpenFsWithSession(fsCtx, opts.Location, opts.Session)
	if err != nil {
		return err
	}
	if fileLeaf != "" {
		return fmt.Errorf("object-store mount requires a directory prefix, not a file key %q", fileLeaf)
	}
	if opts.Mint != nil {
		stopRefresh := startSessionRefresh(fsCtx, f, opts.Mint, opts.SessionExpiry)
		defer stopRefresh()
	}
	vopt := vfscommon.Opt
	vopt.CacheMode = vfscommon.CacheModeWrites
	vopt.ReadOnly = opts.ReadOnly
	cacheDir := resolveObjectCacheDir(opts.CacheDir, opts.Location.Raw)
	unlock, err := lockObjectCacheDir(cacheDir)
	if err != nil {
		return err
	}
	defer unlock()
	if cacheDir != "" {
		_ = config.SetCacheDir(cacheDir)
	}
	v := vfs.New(f, &vopt)
	var pidFile string
	defer func() {
		shutdownObjectVFS(v, cacheDir)
		if pidFile != "" {
			_ = os.Remove(pidFile)
		}
	}()

	if err := os.MkdirAll(opts.MountPoint, 0o755); err != nil {
		return err
	}

	ofs := newRcloneFUSE(v, opts.ReadOnly)
	fuseOpts := &gofuse.MountOptions{
		FsName:     opts.Location.Raw,
		Name:       "drive9-object",
		Debug:      opts.Debug,
		AllowOther: opts.AllowOther,
	}
	if runtime.GOOS == "darwin" {
		fuseOpts.Options = append(fuseOpts.Options, "defer_permissions", "local")
	}
	if opts.ReadOnly {
		fuseOpts.Options = append(fuseOpts.Options, "ro")
	}

	server, err := gofuse.NewServer(ofs, opts.MountPoint, fuseOpts)
	if err != nil {
		return err
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return err
	}

	pidFile, err = writeObjectMountState(opts.MountPoint, opts.Location.Raw, opts.Supervised)
	if err != nil {
		_ = server.Unmount()
		return err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = server.Unmount()
	}()

	server.Wait()
	return nil
}

// resolveObjectCacheDir uses --cache-dir when set, otherwise
// ~/.cache/drive9/object so rclone VFS data stays off the Dat9FS tree.
func resolveObjectCacheDir(user, rawURI string) string {
	base := strings.TrimSpace(user)
	if base != "" {
		if abs, err := filepath.Abs(base); err == nil {
			base = abs
		}
	} else if home, err := os.UserHomeDir(); err == nil && home != "" {
		base = filepath.Join(home, ".cache", "drive9", "object")
	} else {
		return ""
	}
	sum := sha256.Sum256([]byte(rawURI))
	return filepath.Join(base, hex.EncodeToString(sum[:8]))
}

func lockObjectCacheDir(dir string) (func(), error) {
	if dir == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("object cache %s is already in use by another mount", dir)
	}
	return func() {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
	}, nil
}

func shutdownObjectVFS(v *vfs.VFS, cacheDir string) {
	v.WaitForWriters(30 * time.Second)
	// Do not CleanUp(): that RemoveAlls the VFS cache, including dirty
	// files that did not finish uploading. Leave them for the next mount
	// of the same URI to reload and resume.
	if objectCacheHasPending(v) && cacheDir != "" {
		logger.Warn(context.Background(), "unfinished object writes remain; remount the same URI to resume",
			zap.String("cache_dir", cacheDir))
	}
	v.Shutdown()
}

func objectCacheHasPending(v *vfs.VFS) bool {
	if v == nil {
		return false
	}
	dc, ok := v.Stats()["diskCache"].(rc.Params)
	if !ok {
		return false
	}
	return intFromStat(dc["uploadsQueued"]) > 0 || intFromStat(dc["uploadsInProgress"]) > 0
}

func intFromStat(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func writeObjectMountState(mountPoint, remote string, supervised bool) (string, error) {
	stateMountPoint := mountPoint
	if abs, err := filepath.Abs(stateMountPoint); err == nil {
		stateMountPoint = abs
	}
	if resolved, err := filepath.EvalSymlinks(stateMountPoint); err == nil {
		stateMountPoint = resolved
	}
	creation, _ := mountstate.ProcessCreationTime(os.Getpid())
	return mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID:          os.Getpid(),
		CreationTime: creation,
		Component:    "drive9-object",
		MountKind:    mountstate.MountKindObject,
		MountPoint:   stateMountPoint,
		RemoteRoot:   remote,
		StartedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Role:         mountstate.RoleWorker,
		Supervise:    supervised,
	})
}

type rcloneFUSE struct {
	gofuse.RawFileSystem
	v        *vfs.VFS
	readOnly bool
	uid, gid uint32

	mu           sync.Mutex
	inodePath    map[uint64]string
	inodeLookups map[uint64]uint64
	handles      map[uint64]vfs.Handle
	handleSeq    uint64
}

func newRcloneFUSE(v *vfs.VFS, readOnly bool) *rcloneFUSE {
	return &rcloneFUSE{
		RawFileSystem: gofuse.NewDefaultRawFileSystem(),
		v:             v,
		readOnly:      readOnly,
		uid:           uint32(os.Geteuid()),
		gid:           uint32(os.Getegid()),
		inodePath:     map[uint64]string{},
		inodeLookups:  map[uint64]uint64{},
		handles:       map[uint64]vfs.Handle{},
	}
}

func (fs *rcloneFUSE) rel(parent string, name string) string {
	parent = strings.Trim(parent, "/")
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

func (fs *rcloneFUSE) Lookup(cancel <-chan struct{}, header *gofuse.InHeader, name string, out *gofuse.EntryOut) gofuse.Status {
	parent, status := fs.pathOf(header.NodeId)
	if status != gofuse.OK {
		return status
	}
	node, err := fs.v.Stat(fs.rel(parent, name))
	if err != nil {
		return mapVFSErr(err)
	}
	fs.fillEntry(out, node)
	return gofuse.OK
}

func (fs *rcloneFUSE) GetAttr(cancel <-chan struct{}, input *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	p, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	node, err := fs.v.Stat(p)
	if err != nil {
		return mapVFSErr(err)
	}
	fs.fillAttr(&out.Attr, node)
	return gofuse.OK
}

func (fs *rcloneFUSE) OpenDir(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	return gofuse.OK
}

func (fs *rcloneFUSE) ReadDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	return fs.readDir(input, out, false)
}

func (fs *rcloneFUSE) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	return fs.readDir(input, out, true)
}

func (fs *rcloneFUSE) readDir(input *gofuse.ReadIn, out *gofuse.DirEntryList, plus bool) gofuse.Status {
	p, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	node, err := fs.v.Stat(p)
	if err != nil {
		return mapVFSErr(err)
	}
	dir, ok := node.(*vfs.Dir)
	if !ok {
		return gofuse.Status(syscall.ENOTDIR)
	}
	items, err := dir.ReadDirAll()
	if err != nil {
		return mapVFSErr(err)
	}
	var idx uint64 = 1
	for _, item := range items {
		idx++
		if uint64(input.Offset) >= idx {
			continue
		}
		mode := uint32(syscall.S_IFREG)
		if item.IsDir() {
			mode = syscall.S_IFDIR
		}
		ent := gofuse.DirEntry{
			Mode: mode,
			Name: item.Name(),
			Ino:  item.Inode(),
			Off:  idx,
		}
		if plus {
			eo := out.AddDirLookupEntry(ent)
			if eo == nil {
				break
			}
			fs.fillEntry(eo, item)
			continue
		}
		fs.remember(item.Inode(), item.Path(), false)
		if !out.AddDirEntry(ent) {
			break
		}
	}
	return gofuse.OK
}

func fuseOpenFlags(raw uint32) int {
	const accmode = 0x3 // O_RDONLY|O_WRONLY|O_RDWR
	return int(raw) & (accmode | os.O_APPEND | os.O_CREATE | os.O_EXCL | os.O_TRUNC | os.O_SYNC)
}

func (fs *rcloneFUSE) Access(cancel <-chan struct{}, input *gofuse.AccessIn) gofuse.Status {
	if fs.readOnly && input.Mask&(uint32(unixAccessWOK())) != 0 {
		return gofuse.EROFS
	}
	return gofuse.OK
}

func unixAccessWOK() uint32 { return 0x2 } // W_OK

func (fs *rcloneFUSE) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	if fs.readOnly && writeFlags(input.Flags) {
		return gofuse.EROFS
	}
	p, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	node, err := fs.v.Stat(p)
	if err != nil {
		return mapVFSErr(err)
	}
	fh, err := node.Open(fuseOpenFlags(input.Flags))
	if err != nil {
		return mapVFSErr(err)
	}
	out.Fh = fs.storeHandle(fh)
	return gofuse.OK
}

func (fs *rcloneFUSE) Create(cancel <-chan struct{}, input *gofuse.CreateIn, name string, out *gofuse.CreateOut) gofuse.Status {
	if fs.readOnly {
		return gofuse.EROFS
	}
	parent, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	p := fs.rel(parent, name)
	fh, err := fs.v.Create(p)
	if err != nil {
		logger.Error(context.Background(), "object mount create failed", zap.String("path", p), zap.Error(err))
		return mapVFSErr(err)
	}
	if node, statErr := fs.v.Stat(p); statErr == nil {
		fs.fillEntry(&out.EntryOut, node)
	}
	out.Fh = fs.storeHandle(fh)
	return gofuse.OK
}

func (fs *rcloneFUSE) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	h := fs.getHandle(input.Fh)
	if h == nil {
		return nil, gofuse.EBADF
	}
	n, err := h.ReadAt(buf, int64(input.Offset))
	if err != nil && err != io.EOF {
		return nil, mapVFSErr(err)
	}
	return gofuse.ReadResultData(buf[:n]), gofuse.OK
}

func (fs *rcloneFUSE) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (uint32, gofuse.Status) {
	if fs.readOnly {
		return 0, gofuse.EROFS
	}
	h := fs.getHandle(input.Fh)
	if h == nil {
		return 0, gofuse.EBADF
	}
	n, err := h.WriteAt(data, int64(input.Offset))
	if err != nil {
		return uint32(n), mapVFSErr(err)
	}
	return uint32(n), gofuse.OK
}

func (fs *rcloneFUSE) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) gofuse.Status {
	h := fs.getHandle(input.Fh)
	if h == nil {
		return gofuse.OK
	}
	if st := mapVFSErr(h.Flush()); st != gofuse.OK {
		return st
	}
	return mapVFSErr(h.Sync())
}

func (fs *rcloneFUSE) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	h := fs.getHandle(input.Fh)
	if h == nil {
		return gofuse.OK
	}
	return mapVFSErr(h.Sync())
}

func (fs *rcloneFUSE) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	if h := fs.takeHandle(input.Fh); h != nil {
		if err := h.Close(); err != nil {
			logger.Error(context.Background(), "object mount release close failed", zap.Error(err))
		}
	}
}

func (fs *rcloneFUSE) SetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	if fs.readOnly {
		return gofuse.EROFS
	}
	p, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	node, err := fs.v.Stat(p)
	if err != nil {
		return mapVFSErr(err)
	}
	if input.Valid&(gofuse.FATTR_MODE|gofuse.FATTR_UID|gofuse.FATTR_GID) != 0 {
		return gofuse.ENOSYS
	}
	if size, ok := input.GetSize(); ok {
		if file, isFile := node.(*vfs.File); isFile {
			if err := file.Truncate(int64(size)); err != nil {
				return mapVFSErr(err)
			}
		}
	}
	if mtime, ok := input.GetMTime(); ok {
		if err := node.SetModTime(mtime); err != nil {
			return mapVFSErr(err)
		}
	}
	fs.fillAttr(&out.Attr, node)
	return gofuse.OK
}

func (fs *rcloneFUSE) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) gofuse.Status {
	if fs.readOnly {
		return gofuse.EROFS
	}
	parent, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	p := fs.rel(parent, name)
	if err := fs.v.Mkdir(p, os.FileMode(input.Mode)); err != nil {
		return mapVFSErr(err)
	}
	node, err := fs.v.Stat(p)
	if err != nil {
		return mapVFSErr(err)
	}
	fs.fillEntry(out, node)
	return gofuse.OK
}

func (fs *rcloneFUSE) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	if fs.readOnly {
		return gofuse.EROFS
	}
	parent, status := fs.pathOf(header.NodeId)
	if status != gofuse.OK {
		return status
	}
	return mapVFSErr(fs.v.Remove(fs.rel(parent, name)))
}

func (fs *rcloneFUSE) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	return fs.Unlink(cancel, header, name)
}

func (fs *rcloneFUSE) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) gofuse.Status {
	if fs.readOnly {
		return gofuse.EROFS
	}
	oldParent, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	newParent, status := fs.pathOf(input.Newdir)
	if status != gofuse.OK {
		return status
	}
	oldPath := fs.rel(oldParent, oldName)
	newPath := fs.rel(newParent, newName)
	if err := fs.v.Rename(oldPath, newPath); err != nil {
		return mapVFSErr(err)
	}
	fs.mu.Lock()
	for ino, p := range fs.inodePath {
		if p == oldPath || strings.HasPrefix(p, oldPath+"/") {
			fs.inodePath[ino] = newPath + strings.TrimPrefix(p, oldPath)
		}
	}
	fs.mu.Unlock()
	return gofuse.OK
}

func (fs *rcloneFUSE) StatFs(cancel <-chan struct{}, header *gofuse.InHeader, out *gofuse.StatfsOut) gofuse.Status {
	out.Bsize = 4096
	out.NameLen = 1024
	return gofuse.OK
}

func (fs *rcloneFUSE) Forget(nodeid, nlookup uint64) {
	if nodeid == 1 {
		return
	}
	fs.mu.Lock()
	if fs.inodeLookups[nodeid] > nlookup {
		fs.inodeLookups[nodeid] -= nlookup
	} else {
		delete(fs.inodeLookups, nodeid)
		delete(fs.inodePath, nodeid)
	}
	fs.mu.Unlock()
}

func (fs *rcloneFUSE) pathOf(ino uint64) (string, gofuse.Status) {
	if ino == 1 {
		return "", gofuse.OK
	}
	fs.mu.Lock()
	p, ok := fs.inodePath[ino]
	fs.mu.Unlock()
	if !ok {
		return "", gofuse.ENOENT
	}
	return p, gofuse.OK
}

func (fs *rcloneFUSE) fillEntry(out *gofuse.EntryOut, node vfs.Node) {
	fs.fillAttr(&out.Attr, node)
	out.NodeId = node.Inode()
	out.Generation = 1
	fs.remember(node.Inode(), node.Path(), true)
}

func (fs *rcloneFUSE) fillAttr(out *gofuse.Attr, node vfs.Node) {
	out.Ino = node.Inode()
	out.Size = uint64(node.Size())
	out.Uid = fs.uid
	out.Gid = fs.gid
	if node.IsDir() {
		out.Mode = syscall.S_IFDIR | 0o755
		out.Nlink = 2
	} else {
		out.Mode = syscall.S_IFREG | 0o644
		out.Nlink = 1
	}
	if fs.readOnly {
		out.Mode &^= 0o222
	}
	if mt := node.ModTime(); !mt.IsZero() {
		out.Mtime = uint64(mt.Unix())
	}
}

func (fs *rcloneFUSE) remember(ino uint64, p string, lookup bool) {
	fs.mu.Lock()
	fs.inodePath[ino] = strings.Trim(p, "/")
	if lookup {
		fs.inodeLookups[ino]++
	}
	fs.mu.Unlock()
}

func (fs *rcloneFUSE) storeHandle(h vfs.Handle) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.handleSeq++
	id := fs.handleSeq
	fs.handles[id] = h
	return id
}

func (fs *rcloneFUSE) getHandle(id uint64) vfs.Handle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.handles[id]
}

func (fs *rcloneFUSE) takeHandle(id uint64) vfs.Handle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	h := fs.handles[id]
	delete(fs.handles, id)
	return h
}

func writeFlags(flags uint32) bool {
	return flags&uint32(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE) != 0
}

func mapVFSErr(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	if os.IsNotExist(err) {
		return gofuse.ENOENT
	}
	if os.IsExist(err) {
		return gofuse.Status(syscall.EEXIST)
	}
	if os.IsPermission(err) {
		return gofuse.EACCES
	}
	return gofuse.EIO
}
