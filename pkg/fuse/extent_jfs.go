package fuse

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/chunk"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

type extentRuntime struct {
	meta   jfsmeta.Meta
	reader vfs.DataReader
	writer vfs.DataWriter
}

type extentMetaTransport struct {
	client *client.Client
}

func (t *extentMetaTransport) Call(ctx jfsmeta.Context, op string, req, resp any) syscall.Errno {
	raw, err := t.client.ExtentMeta(ctx, op, req)
	if err != nil {
		return syscall.EIO
	}
	if err := json.Unmarshal(raw, resp); err != nil {
		return syscall.EIO
	}
	return 0
}

func (fs *Dat9FS) ensureExtentRuntime() error {
	fs.extentMu.Lock()
	defer fs.extentMu.Unlock()
	if fs.extentRT != nil {
		return nil
	}
	conf := jfsmeta.DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	conf.Heartbeat = 12 * time.Second
	m := jfsmeta.NewDrive9Meta(conf, &extentMetaTransport{client: fs.client})
	format := &jfsmeta.Format{
		Name:      "drive9",
		UUID:      "drive9-extent",
		Storage:   "file",
		BlockSize: 4 << 20,
		TrashDays: 0,
	}
	if err := m.Init(format, false); err != nil {
		_ = m.Init(format, true)
	}
	if _, err := m.Load(false); err != nil {
		return fmt.Errorf("load extent format: %w", err)
	}
	m.OnMsg(jfsmeta.CompactChunk, func(...interface{}) error { return jfsmeta.ErrCompactDelegated })
	m.OnMsg(jfsmeta.DeleteSlice, func(...interface{}) error { return nil })
	if err := m.NewSession(true); err != nil {
		return fmt.Errorf("extent session: %w", err)
	}
	chunkConf := chunk.Config{
		BlockSize:   4 << 20,
		CacheSize:   1 << 30,
		CacheDir:    "memory",
		MaxUpload:   20,
		MaxDownload: 200,
		BufferSize:  32 << 20,
		GetTimeout:  time.Minute,
		PutTimeout:  time.Minute,
		AutoCreate:  true,
	}
	if fs.opts != nil && fs.opts.CacheDir != "" {
		chunkConf.CacheDir = path.Join(fs.opts.CacheDir, "jfs")
		chunkConf.Writeback = true
	}
	loaded := m.GetFormat()
	chunkConf.SelfCheck(loaded.UUID)
	store := chunk.NewCachedStore(&drive9BlockStore{client: fs.client}, chunkConf, nil)
	vfsConf := &vfs.Config{Meta: conf, Format: loaded, Chunk: &chunkConf}
	reader := vfs.NewDataReader(vfsConf, m, store)
	writer := vfs.NewDataWriter(vfsConf, m, store, reader)
	fs.extentRT = &extentRuntime{meta: m, reader: reader, writer: writer}
	return nil
}

func (fs *Dat9FS) jfsCtx(pid, uid, gid uint32) jfsmeta.Context {
	gids := []uint32{gid}
	if gid == 0 {
		gids = []uint32{0}
	}
	return jfsmeta.NewContext(pid, uid, gids)
}

func (fs *Dat9FS) extentEnsureParent(ctx jfsmeta.Context, dirPath string) (jfsmeta.Ino, syscall.Errno) {
	if dirPath == "/" || dirPath == "" {
		return jfsmeta.RootInode, 0
	}
	parent := jfsmeta.RootInode
	rel := strings.Trim(dirPath, "/")
	acc := ""
	for _, part := range strings.Split(rel, "/") {
		if part == "" {
			continue
		}
		acc += "/" + part
		var ino jfsmeta.Ino
		var attr jfsmeta.Attr
		st := fs.extentRT.meta.Lookup(ctx, parent, part, &ino, &attr, false)
		if st == syscall.ENOENT {
			ctx2 := ctx.WithValue(jfsmeta.Drive9PathKey, acc+"/")
			st = fs.extentRT.meta.Mkdir(ctx2, parent, part, 0755, 0, 0, &ino, &attr)
		}
		if st != 0 {
			return 0, st
		}
		parent = ino
	}
	return parent, 0
}

func (fs *Dat9FS) extentCreate(cancel <-chan struct{}, input *gofuse.CreateIn, name, childP string, out *gofuse.CreateOut) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	ctx := fs.jfsCtx(input.Pid, input.Uid, input.Gid)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.extentEnsureParent(ctx, parentPath)
	if st != 0 {
		return gofuse.Status(st)
	}
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, childP)
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	mode := uint16(input.Mode)
	if mode == 0 {
		mode = 0644
	}
	st = fs.extentRT.meta.Create(ctx, parentIno, name, mode, 0, 0, &ino, &attr)
	if st != 0 {
		return gofuse.Status(st)
	}
	driveIno := fs.inodes.Lookup(childP, false, int64(attr.Length), time.Now())
	fs.inodes.UpdateMode(driveIno, uint32(mode))
	w := fs.extentRT.writer.Open(ino, attr.Length, 0)
	r := fs.extentRT.reader.Open(ino, attr.Length)
	fh := &FileHandle{
		Ino:       driveIno,
		Path:      childP,
		Flags:     input.Flags,
		OpenPID:   input.Pid,
		OrigSize:  int64(attr.Length),
		extentIno: ino,
		extentW:   w,
		extentR:   r,
	}
	out.Fh = fs.allocateFileHandle(fh)
	out.OpenFlags = 0
	if entry, ok := fs.inodes.GetEntry(driveIno); ok {
		fs.fillEntryOut(entry, &out.EntryOut)
	}
	parentPath, _ = fs.inodes.GetPath(input.NodeId)
	if entry, ok := fs.inodes.GetEntry(driveIno); ok {
		fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(name, entry))
	}
	fs.touchDirectoryChangeTime(parentPath, time.Now())
	return gofuse.OK
}

func (fs *Dat9FS) extentOpen(cancel <-chan struct{}, input *gofuse.OpenIn, p string, extentIno uint64, out *gofuse.OpenOut) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	ctx := fs.jfsCtx(input.Pid, 0, 0)
	var attr jfsmeta.Attr
	ino := jfsmeta.Ino(extentIno)
	if st := fs.extentRT.meta.Open(ctx, ino, input.Flags, &attr); st != 0 {
		if st == syscall.ENOENT {
			return gofuse.ENOENT
		}
		return gofuse.Status(st)
	}
	w := fs.extentRT.writer.Open(ino, attr.Length, 0)
	r := fs.extentRT.reader.Open(ino, attr.Length)
	fh := &FileHandle{
		Ino:       input.NodeId,
		Path:      p,
		Flags:     input.Flags,
		OpenPID:   input.Pid,
		OrigSize:  int64(attr.Length),
		extentIno: ino,
		extentW:   w,
		extentR:   r,
	}
	out.Fh = fs.allocateFileHandle(fh)
	out.OpenFlags = 0
	return gofuse.OK
}

func (fs *Dat9FS) extentUnlink(cancel <-chan struct{}, header *gofuse.InHeader, name, childP string) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	ctx := fs.jfsCtx(header.Pid, header.Uid, header.Gid)
	opened := false
	if fs.openHandles != nil {
		opened = len(fs.openHandles.SnapshotPath(childP)) > 0
	}
	ctx = ctx.WithValue(jfsmeta.Drive9OpenedKey, opened)
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, childP)
	parentPath := pathutil.ParentPath(childP)
	parentIno, st := fs.extentEnsureParent(ctx, parentPath)
	if st != 0 {
		return gofuse.Status(st)
	}
	if st := fs.extentRT.meta.Unlink(ctx, parentIno, name); st != 0 {
		return gofuse.Status(st)
	}
	if opened {
		fs.inodes.RemoveLinkPreserve(childP)
	} else {
		fs.inodes.RemoveLink(childP)
	}
	parentPath, _ = fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Remove(parentPath, name)
	fs.touchDirectoryChangeTime(parentPath, time.Now())
	fs.cacheNegativePath(childP)
	return gofuse.OK
}

func (fs *Dat9FS) extentRead(ctx jfsmeta.Context, fh *FileHandle, off uint64, buf []byte) (int, gofuse.Status) {
	if fh.extentW != nil {
		if st := fh.extentW.Flush(ctx); st != 0 {
			return 0, gofuse.Status(st)
		}
	}
	if fh.extentR == nil {
		return 0, gofuse.EBADF
	}
	n, st := fh.extentR.Read(ctx, off, buf)
	if st != 0 {
		return n, gofuse.Status(st)
	}
	return n, gofuse.OK
}

func (fs *Dat9FS) extentWrite(ctx jfsmeta.Context, fh *FileHandle, off uint64, data []byte) gofuse.Status {
	if fh.extentW == nil {
		return gofuse.EBADF
	}
	st := fh.extentW.Write(ctx, off, data)
	if st != 0 {
		return gofuse.Status(st)
	}
	fs.inodes.UpdateSize(fh.Ino, int64(fh.extentW.GetLength()))
	return gofuse.OK
}

func (fs *Dat9FS) extentFlush(ctx jfsmeta.Context, fh *FileHandle) gofuse.Status {
	if fh.extentW == nil {
		return gofuse.OK
	}
	st := fh.extentW.Flush(ctx)
	if st != 0 {
		return gofuse.Status(st)
	}
	fs.inodes.UpdateSize(fh.Ino, int64(fh.extentW.GetLength()))
	return gofuse.OK
}

func (fs *Dat9FS) extentRelease(ctx jfsmeta.Context, fh *FileHandle) {
	if fh.extentW != nil {
		_ = fh.extentW.Flush(ctx)
		_ = fh.extentW.Close(ctx)
	}
	if fh.extentR != nil {
		fh.extentR.Close(ctx)
	}
	_ = fs.extentRT.meta.Close(ctx, fh.extentIno)
}
