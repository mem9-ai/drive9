package fuse

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

// Dat9FS implements the go-fuse RawFileSystem interface, bridging FUSE
// operations to the dat9 HTTP API via the Go SDK client.
type Dat9FS struct {
	gofuse.RawFileSystem

	client      *client.Client
	inodes      *InodeToPath
	fileHandles *HandleTable[*FileHandle]
	dirHandles  *HandleTable[*DirHandle]
	readCache   *ReadCache
	dirCache    *DirCache
	uid         uint32
	gid         uint32
	opts        *MountOptions
}

// NewDat9FS creates a new FUSE filesystem backed by the given dat9 client.
func NewDat9FS(c *client.Client, opts *MountOptions) *Dat9FS {
	return &Dat9FS{
		RawFileSystem: gofuse.NewDefaultRawFileSystem(),
		client:        c,
		inodes:        NewInodeToPath(),
		fileHandles:   NewHandleTable[*FileHandle](),
		dirHandles:    NewHandleTable[*DirHandle](),
		readCache:     NewReadCache(opts.CacheSize, 0),
		dirCache:      NewDirCache(opts.DirTTL),
		uid:           uint32(os.Getuid()),
		gid:           uint32(os.Getgid()),
		opts:          opts,
	}
}

// --- helpers -----------------------------------------------------------------

func (fs *Dat9FS) childPath(parentIno uint64, name string) (string, gofuse.Status) {
	parentPath, ok := fs.inodes.GetPath(parentIno)
	if !ok {
		return "", gofuse.ENOENT
	}
	if parentPath == "/" {
		return "/" + name, gofuse.OK
	}
	return parentPath + "/" + name, gofuse.OK
}

func parentDir(p string) string {
	d := path.Dir(p)
	if d == "." {
		return "/"
	}
	return d
}

func (fs *Dat9FS) fillAttr(entry *InodeEntry, out *gofuse.Attr) {
	out.Ino = entry.Ino
	out.Size = uint64(entry.Size)
	out.Blocks = (uint64(entry.Size) + 511) / 512
	out.Uid = fs.uid
	out.Gid = fs.gid

	mtime := entry.Mtime
	if mtime.IsZero() {
		mtime = time.Now()
	}
	out.SetTimes(&mtime, &mtime, &mtime)

	if entry.IsDir {
		out.Mode = syscall.S_IFDIR | 0755
		out.Nlink = 2
	} else {
		out.Mode = syscall.S_IFREG | 0644
		out.Nlink = 1
	}
}

func (fs *Dat9FS) fillEntryOut(entry *InodeEntry, out *gofuse.EntryOut) {
	out.NodeId = entry.Ino
	out.Generation = 1
	fs.fillAttr(entry, &out.Attr)
	out.SetEntryTimeout(fs.opts.EntryTTL)
	out.SetAttrTimeout(fs.opts.AttrTTL)
}

func httpToFuseStatus(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found") || strings.Contains(msg, "HTTP 404"):
		return gofuse.ENOENT
	case strings.Contains(msg, "HTTP 409") || strings.Contains(msg, "already exists"):
		return gofuse.Status(syscall.EEXIST)
	case strings.Contains(msg, "HTTP 403"):
		return gofuse.EACCES
	case strings.Contains(msg, "HTTP 413"):
		return gofuse.Status(syscall.EFBIG)
	default:
		return gofuse.EIO
	}
}

// --- RawFileSystem methods ---------------------------------------------------

func (fs *Dat9FS) Init(server *gofuse.Server) {
	// nothing special needed
}

func (fs *Dat9FS) Lookup(cancel <-chan struct{}, header *gofuse.InHeader, name string, out *gofuse.EntryOut) gofuse.Status {
	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	stat, err := fs.client.Stat(childP)
	if err != nil {
		return httpToFuseStatus(err)
	}

	ino := fs.inodes.Lookup(childP, stat.IsDir, stat.Size, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}
	fs.fillEntryOut(entry, out)
	return gofuse.OK
}

func (fs *Dat9FS) Forget(nodeId uint64, nlookup uint64) {
	fs.inodes.Forget(nodeId, nlookup)
}

func (fs *Dat9FS) GetAttr(cancel <-chan struct{}, input *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	entry, ok := fs.inodes.GetEntry(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}

	// For non-root, refresh from server
	if input.NodeId != 1 {
		stat, err := fs.client.Stat(entry.Path)
		if err != nil {
			return httpToFuseStatus(err)
		}
		entry.Size = stat.Size
		entry.IsDir = stat.IsDir
		fs.inodes.UpdateSize(input.NodeId, stat.Size)
	}

	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(fs.opts.AttrTTL)
	return gofuse.OK
}

func (fs *Dat9FS) SetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	entry, ok := fs.inodes.GetEntry(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}

	// Handle truncate
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		newSize := int64(input.Size)

		// If there's an open file handle with a write buffer, truncate it
		if input.Valid&gofuse.FATTR_FH != 0 {
			fh, ok := fs.fileHandles.Get(input.Fh)
			if ok && fh.Dirty != nil {
				fh.Lock()
				if err := fh.Dirty.Truncate(newSize); err != nil {
					fh.Unlock()
					return gofuse.Status(syscall.EFBIG)
				}
				fh.Unlock()
			}
		}
		entry.Size = newSize
		fs.inodes.UpdateSize(input.NodeId, newSize)
	}

	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(fs.opts.AttrTTL)
	return gofuse.OK
}

// --- Directory operations ----------------------------------------------------

func (fs *Dat9FS) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) gofuse.Status {
	childP, st := fs.childPath(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	if err := fs.client.Mkdir(childP); err != nil {
		return httpToFuseStatus(err)
	}

	ino := fs.inodes.Lookup(childP, true, 0, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}

	parentPath, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Invalidate(parentPath)

	fs.fillEntryOut(entry, out)
	return gofuse.OK
}

func (fs *Dat9FS) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	if err := fs.client.Delete(childP); err != nil {
		return httpToFuseStatus(err)
	}

	fs.inodes.Remove(childP)
	fs.readCache.Invalidate(childP)

	parentPath, _ := fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Invalidate(parentPath)
	return gofuse.OK
}

func (fs *Dat9FS) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	if err := fs.client.Delete(childP); err != nil {
		return httpToFuseStatus(err)
	}

	fs.inodes.Remove(childP)
	fs.dirCache.Invalidate(childP)
	fs.readCache.InvalidatePrefix(childP + "/")

	parentPath, _ := fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Invalidate(parentPath)
	return gofuse.OK
}

func (fs *Dat9FS) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) gofuse.Status {
	oldP, st := fs.childPath(input.NodeId, oldName)
	if st != gofuse.OK {
		return st
	}
	newP, st := fs.childPath(input.Newdir, newName)
	if st != gofuse.OK {
		return st
	}

	if err := fs.client.Rename(oldP, newP); err != nil {
		return httpToFuseStatus(err)
	}

	fs.inodes.Rename(oldP, newP)
	fs.readCache.Invalidate(oldP)
	fs.readCache.InvalidatePrefix(oldP + "/")

	oldParent, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Invalidate(oldParent)
	if input.Newdir != input.NodeId {
		newParent, _ := fs.inodes.GetPath(input.Newdir)
		fs.dirCache.Invalidate(newParent)
	}
	return gofuse.OK
}

func (fs *Dat9FS) OpenDir(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	p, ok := fs.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}

	dh := &DirHandle{
		Ino:  input.NodeId,
		Path: p,
	}
	out.Fh = fs.dirHandles.Allocate(dh)
	out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
	return gofuse.OK
}

func (fs *Dat9FS) ReadDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	dh, ok := fs.dirHandles.Get(input.Fh)
	if !ok {
		return gofuse.ENOENT
	}

	// Populate entries if not already done
	if dh.Entries == nil {
		entries, err := fs.listDir(dh.Path)
		if err != nil {
			return httpToFuseStatus(err)
		}
		dh.Entries = entries
	}

	for i := int(input.Offset); i < len(dh.Entries); i++ {
		e := dh.Entries[i]
		if !out.AddDirEntry(gofuse.DirEntry{
			Name: e.Name,
			Ino:  e.Ino,
			Mode: e.Mode,
			Off:  uint64(i + 1),
		}) {
			break
		}
	}
	return gofuse.OK
}

func (fs *Dat9FS) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	dh, ok := fs.dirHandles.Get(input.Fh)
	if !ok {
		return gofuse.ENOENT
	}

	if dh.Entries == nil {
		entries, err := fs.listDir(dh.Path)
		if err != nil {
			return httpToFuseStatus(err)
		}
		dh.Entries = entries
	}

	for i := int(input.Offset); i < len(dh.Entries); i++ {
		e := dh.Entries[i]
		entryOut := out.AddDirLookupEntry(gofuse.DirEntry{
			Name: e.Name,
			Ino:  e.Ino,
			Mode: e.Mode,
			Off:  uint64(i + 1),
		})
		if entryOut == nil {
			break
		}

		// Fill in the entry attributes
		inoEntry, found := fs.inodes.GetEntry(e.Ino)
		if found {
			fs.fillEntryOut(inoEntry, entryOut)
		}
	}
	return gofuse.OK
}

func (fs *Dat9FS) ReleaseDir(input *gofuse.ReleaseIn) {
	fs.dirHandles.Delete(input.Fh)
}

func (fs *Dat9FS) listDir(dirPath string) ([]DirEntry, error) {
	// Check dir cache first
	if cached, ok := fs.dirCache.Get(dirPath); ok {
		return fs.cachedToDirEntries(dirPath, cached), nil
	}

	items, err := fs.client.List(dirPath)
	if err != nil {
		return nil, err
	}

	// Store in dir cache
	cached := make([]CachedFileInfo, len(items))
	for i, item := range items {
		cached[i] = CachedFileInfo{
			Name:  item.Name,
			Size:  item.Size,
			IsDir: item.IsDir,
		}
	}
	fs.dirCache.Put(dirPath, cached)

	return fs.cachedToDirEntries(dirPath, cached), nil
}

func (fs *Dat9FS) cachedToDirEntries(dirPath string, items []CachedFileInfo) []DirEntry {
	entries := make([]DirEntry, 0, len(items))
	for _, item := range items {
		var childP string
		if dirPath == "/" {
			childP = "/" + item.Name
		} else {
			childP = dirPath + "/" + item.Name
		}

		ino := fs.inodes.Lookup(childP, item.IsDir, item.Size, time.Now())

		var mode uint32
		if item.IsDir {
			mode = syscall.S_IFDIR
		} else {
			mode = syscall.S_IFREG
		}
		entries = append(entries, DirEntry{
			Name: item.Name,
			Ino:  ino,
			Mode: mode,
		})
	}
	return entries
}

// --- File operations ---------------------------------------------------------

func (fs *Dat9FS) Create(cancel <-chan struct{}, input *gofuse.CreateIn, name string, out *gofuse.CreateOut) gofuse.Status {
	if fs.opts.ReadOnly {
		return gofuse.EROFS
	}

	childP, st := fs.childPath(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	ino := fs.inodes.Lookup(childP, false, 0, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}

	wb := NewWriteBuffer(childP, 0)
	wb.touched = true
	fh := &FileHandle{
		Ino:   ino,
		Path:  childP,
		Flags: input.Flags,
		Dirty: wb,
	}
	out.Fh = fs.fileHandles.Allocate(fh)
	out.OpenFlags = gofuse.FOPEN_DIRECT_IO // bypass kernel page cache for writes
	fs.fillEntryOut(entry, &out.EntryOut)

	parentPath, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Invalidate(parentPath)
	return gofuse.OK
}

func (fs *Dat9FS) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	p, ok := fs.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}

	fh := &FileHandle{
		Ino:   input.NodeId,
		Path:  p,
		Flags: input.Flags,
	}

	// Allocate write buffer for writable opens
	accMode := input.Flags & syscall.O_ACCMODE
	if accMode == syscall.O_WRONLY || accMode == syscall.O_RDWR {
		if fs.opts.ReadOnly {
			return gofuse.EROFS
		}
		fh.Dirty = NewWriteBuffer(p, 0)

		// Preload existing content for non-truncating opens so that
		// random writes don't discard the original file data.
		if input.Flags&syscall.O_TRUNC == 0 {
			stat, err := fs.client.Stat(p)
			if err == nil && stat.Size > 0 {
				fh.OrigSize = stat.Size

				bufMax := stat.Size + 16<<20
				if bufMax < defaultWriteBufferMaxSize {
					bufMax = defaultWriteBufferMaxSize
				}
				fh.Dirty = NewWriteBuffer(p, bufMax)

				if stat.Size <= smallFileThreshold {
					if existing, err := fs.client.Read(p); err == nil && len(existing) > 0 {
						_, _ = fh.Dirty.Write(0, existing)
						fh.Dirty.ClearDirty()
					}
				} else {
					rc, err := fs.client.ReadStream(context.Background(), p)
					if err == nil {
						data, err := io.ReadAll(rc)
						_ = rc.Close()
						if err == nil && len(data) > 0 {
							if _, werr := fh.Dirty.Write(0, data); werr != nil {
								fh.Dirty = NewWriteBuffer(p, int64(len(data))+16<<20)
								_, _ = fh.Dirty.Write(0, data)
							}
							fh.Dirty.dirtyParts = nil
						}
					}
				}
			}
		}
	}

	out.Fh = fs.fileHandles.Allocate(fh)
	if fh.Dirty != nil {
		out.OpenFlags = gofuse.FOPEN_DIRECT_IO
	} else {
		out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
	}
	return gofuse.OK
}

func (fs *Dat9FS) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return nil, gofuse.ENOENT
	}

	fh.Lock()
	// If there's a dirty buffer, read from it
	if fh.Dirty != nil && fh.Dirty.Size() > 0 {
		data := fh.Dirty.Bytes()
		offset := int64(input.Offset)
		if offset >= int64(len(data)) {
			fh.Unlock()
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		result := make([]byte, end-offset)
		copy(result, data[offset:end])
		fh.Unlock()
		return gofuse.ReadResultData(result), gofuse.OK
	}
	fh.Unlock()

	p := fh.Path

	// Try read cache for small files
	entry, _ := fs.inodes.GetEntry(fh.Ino)
	if entry != nil && entry.Size <= smallFileThreshold && entry.Size > 0 {
		// Check read cache
		stat, _ := fs.client.Stat(p)
		var rev int64
		if stat != nil {
			rev = stat.Revision
		}
		if data, ok := fs.readCache.Get(p, rev); ok {
			offset := int64(input.Offset)
			if offset >= int64(len(data)) {
				return gofuse.ReadResultData(nil), gofuse.OK
			}
			end := offset + int64(input.Size)
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			return gofuse.ReadResultData(data[offset:end]), gofuse.OK
		}

		// Small file: read entirely into cache
		data, err := fs.client.Read(p)
		if err != nil {
			return nil, httpToFuseStatus(err)
		}
		if stat != nil {
			fs.readCache.Put(p, data, stat.Revision)
		}
		offset := int64(input.Offset)
		if offset >= int64(len(data)) {
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		return gofuse.ReadResultData(data[offset:end]), gofuse.OK
	}

	// Large file or unknown size: stream read
	rc, err := fs.client.ReadStream(context.Background(), p)
	if err != nil {
		return nil, httpToFuseStatus(err)
	}
	defer func() { _ = rc.Close() }()

	// Skip to offset
	if input.Offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, int64(input.Offset)); err != nil {
			if err == io.EOF {
				return gofuse.ReadResultData(nil), gofuse.OK
			}
			return nil, gofuse.EIO
		}
	}

	data := make([]byte, input.Size)
	n, err := io.ReadFull(rc, data)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, gofuse.EIO
	}
	return gofuse.ReadResultData(data[:n]), gofuse.OK
}

func (fs *Dat9FS) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (uint32, gofuse.Status) {
	if fs.opts.ReadOnly {
		return 0, gofuse.EROFS
	}

	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return 0, gofuse.ENOENT
	}

	fh.Lock()
	defer fh.Unlock()

	if fh.Dirty == nil {
		fh.Dirty = NewWriteBuffer(fh.Path, 0)
	}

	n, err := fh.Dirty.Write(int64(input.Offset), data)
	if err != nil {
		return 0, gofuse.Status(syscall.EFBIG)
	}

	fs.inodes.UpdateSize(fh.Ino, fh.Dirty.Size())
	return n, gofuse.OK
}

func (fs *Dat9FS) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) gofuse.Status {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return gofuse.OK
	}

	fh.Lock()
	defer fh.Unlock()

	return fs.flushHandle(fh)
}

func (fs *Dat9FS) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return gofuse.OK
	}

	fh.Lock()
	defer fh.Unlock()

	return fs.flushHandle(fh)
}

func (fs *Dat9FS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if ok {
		fh.Lock()
		fs.flushHandle(fh)
		fh.Unlock()
	}
	fs.fileHandles.Delete(input.Fh)
}

// flushHandle uploads buffered data to the server. Caller must hold fh.mu.
func (fs *Dat9FS) flushHandle(fh *FileHandle) gofuse.Status {
	if fh.Dirty == nil {
		return gofuse.OK
	}
	if !fh.Dirty.HasDirtyParts() {
		return gofuse.OK
	}

	data := fh.Dirty.Bytes()
	size := int64(len(data))

	var err error
	if size < smallFileThreshold {
		// Small file: direct PUT.
		err = fs.client.Write(fh.Path, data)
	} else if fh.OrigSize >= smallFileThreshold {
		dirtyParts := fh.Dirty.DirtyPartNumbers()
		if len(dirtyParts) > 0 {
			partSnapshots := make(map[int][]byte, len(dirtyParts))
			for _, pn := range dirtyParts {
				src := fh.Dirty.PartData(pn)
				if src != nil {
					cp := make([]byte, len(src))
					copy(cp, src)
					partSnapshots[pn] = cp
				}
			}
			err = fs.client.PatchFile(
				context.Background(),
				fh.Path,
				size,
				dirtyParts,
				func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
					if d, ok := partSnapshots[partNumber]; ok {
						return d, nil
					}
					return origData, nil
				},
				nil,
			)
		}
		// If no dirty parts, nothing changed — skip upload.
	} else {
		// New large file or small-to-large growth: full upload via multipart.
		err = fs.client.WriteStream(
			context.Background(),
			fh.Path,
			bytes.NewReader(data),
			size,
			nil,
		)
	}
	if err != nil {
		return httpToFuseStatus(err)
	}

	fh.Dirty.ClearDirty()
	fs.readCache.Invalidate(fh.Path)
	fs.dirCache.Invalidate(parentDir(fh.Path))
	fs.inodes.UpdateSize(fh.Ino, size)
	return gofuse.OK
}

// FlushAll flushes all open file handles. Used during graceful shutdown.
func (fs *Dat9FS) FlushAll() {
	// Snapshot handles outside the lock to avoid deadlocking with
	// concurrent FUSE operations that need HandleTable access.
	type entry struct {
		id uint64
		fh *FileHandle
	}
	var handles []entry
	fs.fileHandles.ForEach(func(fhID uint64, fh *FileHandle) {
		handles = append(handles, entry{fhID, fh})
	})
	for _, e := range handles {
		e.fh.Lock()
		fs.flushHandle(e.fh)
		e.fh.Unlock()
	}
}

func (fs *Dat9FS) String() string {
	return "dat9"
}
