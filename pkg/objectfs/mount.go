//go:build !windows

package objectfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
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
	// Upload on Close (WriteBack=0). Flush for write handles Close+reopens
	// so close/fsync can return PUT errors while the kernel fd stays usable.
	vopt.WriteBack = 0
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
	if runtime.GOOS == "darwin" && !opts.AllowOther {
		fuseOpts.Options = append(fuseOpts.Options, "defer_permissions", "local")
	}
	if opts.AllowOther {
		fuseOpts.Options = append(fuseOpts.Options, "default_permissions")
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

const fuseOpTimeout = 5 * time.Minute

type fuseHandle struct {
	mu    sync.Mutex
	id    uint64
	h     vfs.Handle
	path  string
	flags int
	write bool
}

type rcloneFUSE struct {
	gofuse.RawFileSystem
	v        *vfs.VFS
	readOnly bool
	uid, gid uint32

	mu           sync.Mutex
	publishMu    sync.Mutex // handle publication vs rename path rewrite
	inodePath    map[uint64]string
	inodeLookups map[uint64]uint64
	handles      map[uint64]*fuseHandle
	handleSeq    uint64

	// afterRemoteOpen runs after a successful Open/Create remote call and
	// before the handle is published. Tests use it to interleave Rename.
	afterRemoteOpen func()
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
		handles:       map[uint64]*fuseHandle{},
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
	var node vfs.Node
	if st := fs.withOp(cancel, func(context.Context) error {
		var err error
		node, err = fs.v.Stat(fs.rel(parent, name))
		return err
	}); st != gofuse.OK {
		return st
	}
	fs.fillEntry(out, node)
	return gofuse.OK
}

func (fs *rcloneFUSE) GetAttr(cancel <-chan struct{}, input *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	p, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	var node vfs.Node
	if st := fs.withOp(cancel, func(context.Context) error {
		var err error
		node, err = fs.v.Stat(p)
		return err
	}); st != gofuse.OK {
		return st
	}
	fs.fillAttr(&out.Attr, node)
	return gofuse.OK
}

func (fs *rcloneFUSE) OpenDir(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	return gofuse.OK
}

func (fs *rcloneFUSE) ReadDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	return fs.readDir(cancel, input, out, false)
}

func (fs *rcloneFUSE) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	return fs.readDir(cancel, input, out, true)
}

func (fs *rcloneFUSE) readDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList, plus bool) gofuse.Status {
	p, status := fs.pathOf(input.NodeId)
	if status != gofuse.OK {
		return status
	}
	off := input.Offset
	var items vfs.Nodes
	if st := fs.withOp(cancel, func(context.Context) error {
		node, err := fs.v.Stat(p)
		if err != nil {
			return err
		}
		dir, ok := node.(*vfs.Dir)
		if !ok {
			return syscall.ENOTDIR
		}
		items, err = dir.ReadDirAll()
		return err
	}); st != gofuse.OK {
		return st
	}
	var idx uint64 = 1
	for _, item := range items {
		idx++
		if uint64(off) >= idx {
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
	flags := fuseOpenFlags(input.Flags)
	wantWrite := writeFlags(input.Flags)
	var fh vfs.Handle
	if st := fs.withMutatingOp(cancel, func(context.Context) error {
		node, err := fs.v.Stat(p)
		if err != nil {
			return err
		}
		fh, err = node.Open(flags)
		return err
	}); st != gofuse.OK {
		return st
	}
	if hook := fs.afterRemoteOpen; hook != nil {
		hook()
	}
	out.Fh = fs.publishHandle(input.NodeId, p, fh, flags, wantWrite)
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
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	var fh vfs.Handle
	var node vfs.Node
	if st := fs.withMutatingOp(cancel, func(context.Context) error {
		var err error
		fh, err = fs.v.Create(p)
		if err != nil {
			logger.Error(context.Background(), "object mount create failed", zap.String("path", p), zap.Error(err))
			return err
		}
		node, _ = fs.v.Stat(p)
		return nil
	}); st != gofuse.OK {
		return st
	}
	if hook := fs.afterRemoteOpen; hook != nil {
		hook()
	}
	fs.publishMu.Lock()
	defer fs.publishMu.Unlock()
	path := p
	if node != nil {
		if np := strings.Trim(node.Path(), "/"); np != "" {
			path = np
		}
		fs.fillEntry(&out.EntryOut, node)
		if cur, st := fs.pathOf(node.Inode()); st == gofuse.OK {
			path = cur
		}
	}
	out.Fh = fs.storeHandle(fh, path, flags, true)
	return gofuse.OK
}

func (fs *rcloneFUSE) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	dst := make([]byte, len(buf))
	off := int64(input.Offset)
	n := new(int)
	st := fs.withHandle(cancel, input.Fh, false, func(ent *fuseHandle) error {
		var err error
		*n, err = ent.h.ReadAt(dst, off)
		if err == io.EOF {
			return nil
		}
		return err
	})
	if st != gofuse.OK {
		return nil, st
	}
	return gofuse.ReadResultData(dst[:*n]), gofuse.OK
}

func (fs *rcloneFUSE) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (uint32, gofuse.Status) {
	if fs.readOnly {
		return 0, gofuse.EROFS
	}
	payload := append([]byte(nil), data...)
	off := int64(input.Offset)
	n := new(int)
	st := fs.withHandle(cancel, input.Fh, true, func(ent *fuseHandle) error {
		var err error
		*n, err = ent.h.WriteAt(payload, off)
		return err
	})
	if st != gofuse.OK {
		return 0, st
	}
	return uint32(*n), gofuse.OK
}

func (fs *rcloneFUSE) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) gofuse.Status {
	return fs.flushHandle(cancel, input.Fh)
}

func (fs *rcloneFUSE) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	return fs.flushHandle(cancel, input.Fh)
}

func (fs *rcloneFUSE) flushHandle(cancel <-chan struct{}, fh uint64) gofuse.Status {
	ent := fs.getHandle(fh)
	if ent == nil {
		return gofuse.OK
	}
	return fs.withMutatingOp(cancel, func(context.Context) error {
		ent.mu.Lock()
		defer ent.mu.Unlock()
		if ent.h == nil {
			return nil
		}
		if err := ent.h.Flush(); err != nil {
			return err
		}
		if err := ent.h.Sync(); err != nil {
			return err
		}
		if !ent.write {
			return nil
		}
		closeErr := ent.h.Close()
		reopenFlags := ent.flags &^ (os.O_TRUNC | os.O_EXCL | os.O_CREATE)
		if reopenFlags&0x3 == 0 {
			reopenFlags |= os.O_RDWR
		}
		node, err := fs.v.Stat(ent.path)
		if err != nil {
			ent.h = nil
			if closeErr != nil {
				return closeErr
			}
			return err
		}
		next, err := node.Open(reopenFlags)
		if err != nil {
			ent.h = nil
			if closeErr != nil {
				return closeErr
			}
			return err
		}
		ent.h = next
		return closeErr
	})
}

func (fs *rcloneFUSE) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	ent := fs.takeHandle(input.Fh)
	if ent == nil {
		return
	}
	if st := fs.withMutatingOp(cancel, func(context.Context) error {
		ent.mu.Lock()
		defer ent.mu.Unlock()
		if ent.h == nil {
			return nil
		}
		err := ent.h.Close()
		ent.h = nil
		return err
	}); st != gofuse.OK {
		logger.Error(context.Background(), "object mount release close failed", zap.Int("status", int(st)))
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
	if input.Valid&(gofuse.FATTR_MODE|gofuse.FATTR_UID|gofuse.FATTR_GID) != 0 {
		return gofuse.ENOSYS
	}
	size, hasSize := input.GetSize()
	mtime, hasMTime := input.GetMTime()
	var node vfs.Node
	if st := fs.withMutatingOp(cancel, func(context.Context) error {
		var err error
		node, err = fs.v.Stat(p)
		if err != nil {
			return err
		}
		if hasSize {
			if file, isFile := node.(*vfs.File); isFile {
				if err := file.Truncate(int64(size)); err != nil {
					return err
				}
			}
		}
		if hasMTime {
			if err := node.SetModTime(mtime); err != nil {
				return err
			}
		}
		return nil
	}); st != gofuse.OK {
		return st
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
	mode := os.FileMode(input.Mode)
	var node vfs.Node
	if st := fs.withMutatingOp(cancel, func(context.Context) error {
		if err := fs.v.Mkdir(p, mode); err != nil {
			return err
		}
		var err error
		node, err = fs.v.Stat(p)
		return err
	}); st != gofuse.OK {
		return st
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
	return fs.withMutatingOp(cancel, func(context.Context) error {
		return fs.v.Remove(fs.rel(parent, name))
	})
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
	return fs.withMutatingOp(cancel, func(context.Context) error {
		fs.publishMu.Lock()
		defer fs.publishMu.Unlock()
		locked := fs.lockHandlesForPath(oldPath)
		defer unlockHandles(locked)
		if err := fs.v.Rename(oldPath, newPath); err != nil {
			return err
		}
		fs.mu.Lock()
		for ino, p := range fs.inodePath {
			if p == oldPath || strings.HasPrefix(p, oldPath+"/") {
				fs.inodePath[ino] = newPath + strings.TrimPrefix(p, oldPath)
			}
		}
		for _, ent := range locked {
			if ent.path == oldPath || strings.HasPrefix(ent.path, oldPath+"/") {
				ent.path = newPath + strings.TrimPrefix(ent.path, oldPath)
			}
		}
		fs.mu.Unlock()
		return nil
	})
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

func (fs *rcloneFUSE) publishHandle(nodeID uint64, fallback string, h vfs.Handle, flags int, write bool) uint64 {
	fs.publishMu.Lock()
	defer fs.publishMu.Unlock()
	path := fallback
	if cur, st := fs.pathOf(nodeID); st == gofuse.OK {
		path = cur
	}
	return fs.storeHandle(h, path, flags, write)
}

func (fs *rcloneFUSE) storeHandle(h vfs.Handle, path string, flags int, write bool) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.handleSeq++
	id := fs.handleSeq
	fs.handles[id] = &fuseHandle{id: id, h: h, path: strings.Trim(path, "/"), flags: flags, write: write}
	return id
}

func (fs *rcloneFUSE) getHandle(id uint64) *fuseHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.handles[id]
}

func (fs *rcloneFUSE) takeHandle(id uint64) *fuseHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	h := fs.handles[id]
	delete(fs.handles, id)
	return h
}

func (fs *rcloneFUSE) lockHandlesForPath(path string) []*fuseHandle {
	fs.mu.Lock()
	var locked []*fuseHandle
	for _, ent := range fs.handles {
		if ent.path == path || strings.HasPrefix(ent.path, path+"/") {
			locked = append(locked, ent)
		}
	}
	fs.mu.Unlock()
	sort.Slice(locked, func(i, j int) bool { return locked[i].id < locked[j].id })
	for _, ent := range locked {
		ent.mu.Lock()
	}
	return locked
}

func unlockHandles(ents []*fuseHandle) {
	for i := len(ents) - 1; i >= 0; i-- {
		ents[i].mu.Unlock()
	}
}

func (fs *rcloneFUSE) withHandle(cancel <-chan struct{}, fh uint64, settle bool, fn func(*fuseHandle) error) gofuse.Status {
	ent := fs.getHandle(fh)
	if ent == nil {
		return gofuse.EBADF
	}
	op := fs.withOp
	if settle {
		op = fs.withMutatingOp
	}
	return op(cancel, func(context.Context) error {
		ent.mu.Lock()
		defer ent.mu.Unlock()
		if ent.h == nil {
			return syscall.EBADF
		}
		return fn(ent)
	})
}

// withOp bounds a non-mutating FUSE request. Kernel cancel returns EINTR
// immediately; the deadline returns ETIMEDOUT. The callback may keep
// running: rclone VFS does not abort in-flight object HTTP. Callers must
// copy go-fuse request buffers and input fields before fn uses them.
func (fs *rcloneFUSE) withOp(cancel <-chan struct{}, fn func(context.Context) error) gofuse.Status {
	return fs.runOp(cancel, fn, false)
}

// withMutatingOp runs a mutating FUSE request and does not return until fn
// finishes. Cancel/timeout still cancel ctx, but a successful mutation is
// reported as OK so the caller never sees failure followed by a delayed commit.
func (fs *rcloneFUSE) withMutatingOp(cancel <-chan struct{}, fn func(context.Context) error) gofuse.Status {
	return fs.runOp(cancel, fn, true)
}

func (fs *rcloneFUSE) runOp(cancel <-chan struct{}, fn func(context.Context) error, settle bool) gofuse.Status {
	if canceled(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	ctx, stop := context.WithTimeout(context.Background(), fuseOpTimeout)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	if settle {
		return waitSettled(cancel, stop, ctx, done)
	}
	select {
	case err := <-done:
		return mapVFSErr(err)
	case <-cancelOrNil(cancel):
		return gofuse.Status(syscall.EINTR)
	case <-ctx.Done():
		select {
		case err := <-done:
			return mapVFSErr(err)
		default:
			if canceled(cancel) {
				return gofuse.Status(syscall.EINTR)
			}
			return gofuse.Status(syscall.ETIMEDOUT)
		}
	}
}

func waitSettled(cancel <-chan struct{}, stop context.CancelFunc, ctx context.Context, done <-chan error) gofuse.Status {
	select {
	case err := <-done:
		return mapVFSErr(err)
	case <-cancelOrNil(cancel):
		stop()
	case <-ctx.Done():
		stop()
	}
	err := <-done
	if err == nil {
		return gofuse.OK
	}
	if canceled(cancel) && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return gofuse.Status(syscall.EINTR)
	}
	return mapVFSErr(err)
}

func canceled(cancel <-chan struct{}) bool {
	select {
	case <-cancelOrNil(cancel):
		return cancel != nil
	default:
		return false
	}
}

func cancelOrNil(cancel <-chan struct{}) <-chan struct{} {
	if cancel != nil {
		return cancel
	}
	return nil
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
	if errors.Is(err, context.DeadlineExceeded) {
		return gofuse.Status(syscall.ETIMEDOUT)
	}
	if errors.Is(err, context.Canceled) {
		return gofuse.Status(syscall.EINTR)
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return gofuse.Status(errno)
	}
	return gofuse.EIO
}
