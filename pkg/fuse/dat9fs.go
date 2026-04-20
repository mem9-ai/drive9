package fuse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/s3client"
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
	dirtyMu     sync.Mutex
	dirtyInodes map[uint64]dirtyInodeState
	dirtySeq    uint64
	uid         uint32
	gid         uint32
	opts        *MountOptions
	debouncer   *flushDebouncer

	// Write-back cache: Flush writes small files to local disk, Release
	// triggers async upload. Nil when CacheDir is not configured.
	writeBack *WriteBackCache
	uploader  *WriteBackUploader

	// pendingIndex is the in-memory authoritative metadata index.
	// All metadata reads (GetMeta) are served from memory (O(1)).
	// Nil when CacheDir is not configured.
	pendingIndex *PendingIndex

	// shadowStore manages per-path local shadow files for extent-based
	// writes. Flush writes only dirty parts via pwrite. Nil when not configured.
	shadowStore *ShadowStore

	// syncMode is the resolved sync mode (interactive or strict).
	syncMode SyncMode

	// journal is the append-only WAL for crash recovery (P1).
	journal *Journal

	// commitQueue is the ordered background remote commit queue (P1).
	commitQueue *CommitQueue

	// server is the go-fuse server, set during Init(). Used to send
	// kernel cache invalidation notifications (EntryNotify, InodeNotify)
	// so that long TTLs don't serve stale data after local mutations.
	server *gofuse.Server

	// notifyWg tracks inflight asynchronous kernel notification goroutines
	// (EntryNotify, InodeNotify). FlushAll waits on this to ensure all
	// notifications complete before shutdown.
	notifyWg sync.WaitGroup
}

type dirtyInodeState struct {
	size int64
	seq  uint64
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
		dirtyInodes:   make(map[uint64]dirtyInodeState),
		uid:           uint32(os.Getuid()),
		gid:           uint32(os.Getgid()),
		opts:          opts,
		debouncer:     newFlushDebouncer(opts.FlushDebounce),
	}
}

// SetWriteBack configures the write-back cache and uploader on the filesystem.
// Must be called before the filesystem starts serving requests.
func (fs *Dat9FS) SetWriteBack(cache *WriteBackCache, uploader *WriteBackUploader) {
	fs.writeBack = cache
	fs.uploader = uploader
}

// flushPendingWriteBack synchronously uploads a pending write-back cache
// entry for the given path, if one exists. This must be called before remote
// operations (Rename, Unlink) that depend on the file existing on the server.
func (fs *Dat9FS) flushPendingWriteBack(ctx context.Context, remotePath string) error {
	if fs.writeBack == nil || fs.uploader == nil {
		return nil
	}
	return fs.uploader.UploadSync(ctx, remotePath)
}

// fuseTimeout is the default timeout for FUSE operations that make HTTP calls.
// This prevents slow/dead servers from blocking the FUSE event loop forever.
const fuseTimeout = 30 * time.Second

// fuseCtx converts a FUSE cancel channel into a context.Context with a timeout.
// The context is cancelled when either the FUSE operation is interrupted or the
// timeout expires. This ensures HTTP calls never block indefinitely.
func fuseCtx(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cf := context.WithTimeout(context.Background(), fuseTimeout)
	go func() {
		select {
		case <-cancel:
			cf()
		case <-ctx.Done():
		}
	}()
	return ctx, cf
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

func (fs *Dat9FS) dirtyHandleSize(ino uint64) (int64, bool) {
	fs.dirtyMu.Lock()
	defer fs.dirtyMu.Unlock()

	state, ok := fs.dirtyInodes[ino]
	if !ok {
		return 0, false
	}
	return state.size, true
}

func (fs *Dat9FS) markDirtySize(ino uint64, size int64) uint64 {
	fs.dirtyMu.Lock()
	defer fs.dirtyMu.Unlock()

	fs.dirtySeq++
	seq := fs.dirtySeq
	fs.dirtyInodes[ino] = dirtyInodeState{size: size, seq: seq}
	return seq
}

func (fs *Dat9FS) clearDirtySize(ino uint64, seq uint64) {
	if seq == 0 {
		return
	}

	fs.dirtyMu.Lock()
	defer fs.dirtyMu.Unlock()

	state, ok := fs.dirtyInodes[ino]
	if ok && state.seq == seq {
		delete(fs.dirtyInodes, ino)
	}
}

func shouldRefreshHandleAfterPathTruncate(fh *FileHandle) bool {
	if fh == nil || fh.Dirty == nil {
		return false
	}
	if fh.ZeroBase {
		return true
	}
	return fh.Flags&syscall.O_TRUNC != 0
}

func shouldAdoptSingleHandlePathTruncate(fh *FileHandle, callerPID uint32, matchCount int) bool {
	if fh == nil || fh.Dirty == nil {
		return false
	}
	if callerPID == 0 || matchCount != 1 {
		return false
	}
	return fh.OpenPID == callerPID
}

func (fs *Dat9FS) truncateWritableHandleLocked(fh *FileHandle, newSize int64) error {
	if fh == nil || fh.Dirty == nil {
		return nil
	}
	if err := fh.Dirty.Truncate(newSize); err != nil {
		return err
	}
	if fs.shadowStore != nil && fs.pendingIndex != nil {
		if fh.ShadowReady || fh.IsNew || newSize == 0 {
			if err := fs.shadowStore.Truncate(fh.Path, newSize, fh.BaseRev); err != nil {
				log.Printf("shadow truncate failed for %s: %v", fh.Path, err)
				fs.shadowStore.Remove(fh.Path)
				fh.ShadowReady = false
			} else {
				fh.ShadowReady = true
			}
		}
	}
	// Reset sequential write tracking after truncate so that
	// subsequent writes starting at the new size are not
	// misdetected as back-writes (appendCursor may be stale).
	fh.Dirty.ResetSequentialState(newSize)
	fh.ZeroBase = newSize == 0
	fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
	return nil
}

func (fs *Dat9FS) updateOpenHandleBaseRevision(remotePath string, revision int64, callerPID uint32) {
	if revision <= 0 {
		return
	}

	var matching []*FileHandle
	for _, fh := range fs.fileHandles.Snapshot() {
		if fh != nil && fh.Path == remotePath && fh.Dirty != nil {
			matching = append(matching, fh)
		}
	}

	for _, fh := range matching {
		fh.Lock()
		if shouldAdoptSingleHandlePathTruncate(fh, callerPID, len(matching)) {
			if err := fs.truncateWritableHandleLocked(fh, 0); err != nil {
				log.Printf("handle truncate sync failed for %s: %v", fh.Path, err)
				fh.Unlock()
				continue
			}
		}
		if !shouldRefreshHandleAfterPathTruncate(fh) {
			fh.Unlock()
			continue
		}
		fh.BaseRev = revision
		if fh.Streamer != nil {
			fh.Streamer.RefreshExpectedRevision(expectedRevisionForHandle(fh))
		}
		if fh.ShadowReady && fs.shadowStore != nil {
			size := int64(0)
			if fh.Dirty != nil {
				size = fh.Dirty.Size()
			}
			if err := fs.shadowStore.Ensure(fh.Path, size, revision); err != nil {
				log.Printf("shadow base revision refresh failed for %s: %v", fh.Path, err)
			}
		}
		fh.Unlock()
	}
}

func (fs *Dat9FS) preloadWritableHandle(ctx context.Context, fh *FileHandle) gofuse.Status {
	stat, err := fs.client.StatCtx(ctx, fh.Path)
	if err != nil {
		return httpToFuseStatus(err)
	}
	fh.OrigSize = stat.Size
	fh.BaseRev = stat.Revision
	if stat.Size == 0 {
		return gofuse.OK
	}

	partSize := s3client.CalcAdaptivePartSize(stat.Size)
	// Allow growth up to 2x original size or at least 1GB, whichever is larger.
	// Lazy loading means memory usage is O(touched_parts), not O(bufMax).
	bufMax := stat.Size * 2
	if bufMax < maxPreloadSize {
		bufMax = maxPreloadSize
	}
	fh.Dirty = NewWriteBuffer(fh.Path, bufMax, partSize)

	// Lazy preload for all sizes — only Stat() now, load parts on demand.
	// Set totalSize so the buffer knows the file extent, but don't load data.
	// Set remoteSize so ensurePart() knows which parts exist on the server.
	// For small files (≤50KB), the single part is loaded on first Read/Write
	// via ensurePart → LoadPart. This reduces Open latency from 2 RTTs to 1.
	fh.Dirty.totalSize = stat.Size
	fh.Dirty.remoteSize = stat.Size

	// Install lazy loader: loads a single part from the server via range read.
	// Uses a bounded timeout so a stalled server cannot block the FUSE handler
	// (and its held fh.mu) indefinitely.
	c := fs.client
	filePath := fh.Path
	fh.Dirty.LoadPart = func(partNum int) ([]byte, error) {
		partIdx := partNum - 1
		offset := int64(partIdx) * partSize
		length := partSize
		if offset+length > stat.Size {
			length = stat.Size - offset
		}
		if length <= 0 {
			return nil, nil
		}

		lpCtx, lpCf := context.WithTimeout(context.Background(), fuseTimeout)
		defer lpCf()

		rc, err := c.ReadStreamRange(lpCtx, filePath, offset, length)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rc.Close() }()

		data, err := io.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		return data, nil
	}

	return gofuse.OK
}

func (fs *Dat9FS) pendingKindForHandle(fh *FileHandle) PendingKind {
	if fh.IsNew {
		return PendingNew
	}
	return PendingOverwrite
}

func expectedRevisionForHandle(fh *FileHandle) int64 {
	if fh == nil {
		return -1
	}
	if fh.IsNew {
		return 0
	}
	if fh.BaseRev > 0 {
		return fh.BaseRev
	}
	return -1
}

func committedRevisionFromExpectedRevision(expectedRevision int64) (int64, bool) {
	if expectedRevision < 0 {
		return 0, false
	}
	return expectedRevision + 1, true
}

// finalizeHandleFlushLocked updates the live handle and inode cache after a
// successful upload using the exact CAS revision that completed, when known.
// Callers must hold fh.mu.
func (fs *Dat9FS) finalizeHandleFlushLocked(fh *FileHandle, expectedRevision int64) {
	if fh == nil {
		return
	}

	fh.IsNew = false
	if revision, ok := committedRevisionFromExpectedRevision(expectedRevision); ok {
		fh.BaseRev = revision
		fs.inodes.UpdateRevision(fh.Ino, revision)
	} else {
		// The flush succeeded, but it was unconditional, so the precise
		// post-commit revision is unknown. Clear the cached revision instead of
		// keeping a known-stale positive value.
		fh.BaseRev = 0
		fs.inodes.UpdateRevision(fh.Ino, 0)
	}
	if fh.Streamer != nil {
		fh.Streamer.ResetForNextWrite(expectedRevisionForHandle(fh))
	}
	if fh.ZeroBase && fh.Dirty != nil && fh.Dirty.Size() > 0 {
		fh.ZeroBase = false
	}
}

func (fs *Dat9FS) canStageShadowFastLocked(fh *FileHandle) bool {
	if fs.shadowStore == nil || fs.pendingIndex == nil || fh == nil || fh.Dirty == nil {
		return false
	}
	if fh.ShadowReady || fh.IsNew {
		return true
	}
	return fh.Dirty.CanMaterializeFull()
}

func (fs *Dat9FS) stageShadowLocked(fh *FileHandle, durable bool) error {
	if !fs.canStageShadowFastLocked(fh) {
		return syscall.ENOTSUP
	}

	size := fh.Dirty.Size()
	if fh.ShadowReady {
		if err := fs.shadowStore.Truncate(fh.Path, size, fh.BaseRev); err != nil {
			return err
		}
	} else {
		if err := fs.shadowStore.WriteFull(fh.Path, fh.Dirty.Bytes(), fh.BaseRev); err != nil {
			return err
		}
		fh.ShadowReady = true
	}

	if durable {
		if err := fs.shadowStore.Sync(fh.Path); err != nil {
			return err
		}
	}
	if _, err := fs.pendingIndex.PutWithBaseRev(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev); err != nil {
		log.Printf("pending index put failed for %s: %v", fh.Path, err)
	}
	return nil
}

func (fs *Dat9FS) snapshotWriteBackLocked(fh *FileHandle) error {
	if fs.writeBack == nil {
		return nil
	}
	if fh.Dirty == nil {
		return nil
	}
	if !fh.ShadowReady && !fh.IsNew && !fh.Dirty.CanMaterializeFull() {
		return syscall.ENOTSUP
	}
	return fs.writeBack.PutWithBaseRev(
		fh.Path,
		fh.Dirty.Bytes(),
		fh.Dirty.Size(),
		fs.pendingKindForHandle(fh),
		fh.BaseRev,
	)
}

func (fs *Dat9FS) loadWritableHandleFromShadowLocked(fh *FileHandle, meta *WriteBackMeta) error {
	if fs.shadowStore == nil || fh == nil || meta == nil {
		return syscall.ENOENT
	}

	data, err := fs.shadowStore.ReadAll(fh.Path)
	if err != nil {
		return err
	}

	wb := NewWriteBuffer(fh.Path, maxPreloadSize, 0)
	if len(data) > 0 {
		if _, err := wb.Write(0, data); err != nil {
			return err
		}
		wb.ClearDirty()
	} else {
		wb.totalSize = meta.Size
	}

	fh.Dirty = wb
	fh.ShadowReady = true
	fh.IsNew = meta.Kind == PendingNew
	fh.OrigSize = meta.Size
	if meta.BaseRev > 0 {
		fh.BaseRev = meta.BaseRev
	} else if rev := fs.shadowStore.BaseRev(fh.Path); rev > 0 {
		fh.BaseRev = rev
	}
	return nil
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

	// Prefer typed StatusError so we map by status code even when the
	// server returns a JSON error body that doesn't contain "HTTP NNN".
	var se *client.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusNotFound:
			return gofuse.ENOENT
		case http.StatusConflict:
			if strings.Contains(strings.ToLower(se.Message), "already exists") {
				return gofuse.Status(syscall.EEXIST)
			}
			return gofuse.EIO
		case http.StatusForbidden:
			return gofuse.EACCES
		case http.StatusRequestEntityTooLarge:
			return gofuse.Status(syscall.EFBIG)
		case http.StatusPreconditionFailed:
			return gofuse.Status(syscall.ESTALE)
		case http.StatusBadRequest:
			return gofuse.Status(syscall.EINVAL)
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable:
			return gofuse.Status(syscall.EAGAIN)
		default:
			return gofuse.EIO
		}
	}

	// Fallback to string matching for non-StatusError errors.
	msg := err.Error()
	lowerMsg := strings.ToLower(msg)
	switch {
	case strings.Contains(lowerMsg, "not found") || strings.Contains(msg, "HTTP 404"):
		return gofuse.ENOENT
	case strings.Contains(lowerMsg, "already exists"):
		return gofuse.Status(syscall.EEXIST)
	case strings.Contains(msg, "HTTP 403"):
		return gofuse.EACCES
	case strings.Contains(msg, "HTTP 413"):
		return gofuse.Status(syscall.EFBIG)
	case strings.Contains(msg, "HTTP 412"):
		return gofuse.Status(syscall.ESTALE)
	case strings.Contains(msg, "HTTP 400"):
		return gofuse.Status(syscall.EINVAL)
	case strings.Contains(msg, "HTTP 500") ||
		strings.Contains(msg, "HTTP 502") ||
		strings.Contains(msg, "HTTP 503"):
		return gofuse.Status(syscall.EAGAIN)
	default:
		return gofuse.EIO
	}
}

func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not found") || strings.Contains(msg, "HTTP 404")
}

// --- RawFileSystem methods ---------------------------------------------------

func (fs *Dat9FS) Init(server *gofuse.Server) {
	fs.server = server
}

// notifyEntry tells the kernel to invalidate a directory entry cache.
// Safe to call even if the server is not yet initialized (e.g., during tests).
func (fs *Dat9FS) notifyEntry(parentIno uint64, name string) {
	if fs.server == nil {
		return
	}
	// Run asynchronously to avoid deadlock on macOS: EntryNotify can
	// trigger synchronous Lookup/GetAttr back into our handlers, which
	// needs a free go-fuse worker thread. If called from within a handler,
	// the calling worker is blocked, potentially exhausting the pool.
	fs.notifyWg.Add(1)
	go func() {
		defer fs.notifyWg.Done()
		_ = fs.server.EntryNotify(parentIno, name)
	}()
}

// notifyInode tells the kernel to invalidate cached attributes and data
// for an inode. off=0, sz=0 means invalidate all cached data.
func (fs *Dat9FS) notifyInode(ino uint64) {
	if fs.server == nil {
		return
	}
	fs.notifyWg.Add(1)
	go func() {
		defer fs.notifyWg.Done()
		_ = fs.server.InodeNotify(ino, 0, 0)
	}()
}

func (fs *Dat9FS) Lookup(cancel <-chan struct{}, header *gofuse.InHeader, name string, out *gofuse.EntryOut) gofuse.Status {
	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	// Pending namespace overlay: check in-memory PendingIndex first (O(1)),
	// then fall back to the write-back cache GetMeta for backward compat.
	if fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(childP); ok {
			mtime := meta.Mtime
			if mtime.IsZero() {
				mtime = time.Now()
			}
			ino := fs.inodes.Lookup(childP, false, meta.Size, mtime)
			entry, ok := fs.inodes.GetEntry(ino)
			if !ok {
				return gofuse.EIO
			}
			fs.fillEntryOut(entry, out)
			return gofuse.OK
		}
	} else if fs.writeBack != nil {
		if meta, ok := fs.writeBack.GetMeta(childP); ok {
			mtime := meta.Mtime
			if mtime.IsZero() {
				mtime = time.Now()
			}
			ino := fs.inodes.Lookup(childP, false, meta.Size, mtime)
			entry, ok := fs.inodes.GetEntry(ino)
			if !ok {
				return gofuse.EIO
			}
			fs.fillEntryOut(entry, out)
			return gofuse.OK
		}
	}

	ctx, cf := fuseCtx(cancel)
	defer cf()

	stat, err := fs.client.StatCtx(ctx, childP)
	if err != nil {
		if !isNotFoundErr(err) {
			return httpToFuseStatus(err)
		}

		// Some deployments do not support stat/HEAD on directories.
		// Fall back to listing the parent and matching by name.
		parentPath, ok := fs.inodes.GetPath(header.NodeId)
		if !ok {
			return gofuse.ENOENT
		}
		items, listErr := fs.client.ListCtx(ctx, parentPath)
		if listErr != nil {
			return httpToFuseStatus(listErr)
		}
		for _, item := range items {
			if item.Name != name {
				continue
			}
			mtime := time.Now()
			if item.Mtime > 0 {
				mtime = time.Unix(item.Mtime, 0)
			}
			ino := fs.inodes.Lookup(childP, item.IsDir, item.Size, mtime)
			entry, ok := fs.inodes.GetEntry(ino)
			if !ok {
				return gofuse.EIO
			}
			fs.fillEntryOut(entry, out)
			return gofuse.OK
		}
		// Cache negative lookup: tell kernel this entry doesn't exist
		// for NegativeEntryTTL so it doesn't re-ask immediately.
		out.NodeId = 0
		out.SetEntryTimeout(fs.opts.NegativeEntryTTL)
		return gofuse.ENOENT
	}

	mtime := time.Now()
	if !stat.Mtime.IsZero() {
		mtime = stat.Mtime
	}
	ino := fs.inodes.Lookup(childP, stat.IsDir, stat.Size, mtime)
	// Store server revision for cache validation.
	if stat.Revision > 0 {
		fs.inodes.UpdateRevision(ino, stat.Revision)
	}
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

	// Prefer unflushed writable state over the remote object size.
	if size, ok := fs.dirtyHandleSize(input.NodeId); ok {
		entry.Size = size
	} else if fs.writeBack != nil && !entry.IsDir {
		// Check pending index first (in-memory, O(1)), then fall back
		// to old GetMeta for backward compatibility.
		pendingFound := false
		if fs.pendingIndex != nil {
			if meta, ok := fs.pendingIndex.GetMeta(entry.Path); ok {
				entry.Size = meta.Size
				if !meta.Mtime.IsZero() {
					entry.Mtime = meta.Mtime
				}
				pendingFound = true
			}
		}
		if !pendingFound {
			if meta, ok := fs.writeBack.GetMeta(entry.Path); ok {
				entry.Size = meta.Size
				if !meta.Mtime.IsZero() {
					entry.Mtime = meta.Mtime
				}
				pendingFound = true
			}
		}
		if !pendingFound && input.NodeId != 1 {
			ctx, cf := fuseCtx(cancel)
			defer cf()
			stat, err := fs.client.StatCtx(ctx, entry.Path)
			if err != nil {
				return httpToFuseStatus(err)
			}
			entry.Size = stat.Size
			entry.IsDir = stat.IsDir
			fs.inodes.UpdateSize(input.NodeId, stat.Size)
			if stat.Revision > 0 {
				fs.inodes.UpdateRevision(input.NodeId, stat.Revision)
			}
			if !stat.Mtime.IsZero() {
				entry.Mtime = stat.Mtime
				fs.inodes.UpdateMtime(input.NodeId, stat.Mtime)
			}
		}
	} else if input.NodeId != 1 {
		// Some deployments do not support HEAD/stat on directories.
		// Keep directory attrs from inode map and only refresh regular files.
		if !entry.IsDir {
			ctx, cf := fuseCtx(cancel)
			defer cf()
			stat, err := fs.client.StatCtx(ctx, entry.Path)
			if err != nil {
				return httpToFuseStatus(err)
			}
			entry.Size = stat.Size
			entry.IsDir = stat.IsDir
			fs.inodes.UpdateSize(input.NodeId, stat.Size)
			if stat.Revision > 0 {
				fs.inodes.UpdateRevision(input.NodeId, stat.Revision)
			}
			if !stat.Mtime.IsZero() {
				entry.Mtime = stat.Mtime
				fs.inodes.UpdateMtime(input.NodeId, stat.Mtime)
			}
		}
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

	// Handle mtime updates
	if mtime, ok := input.GetMTime(); ok {
		entry.Mtime = mtime
		fs.inodes.UpdateMtime(input.NodeId, mtime)
	}

	// Handle truncate
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		newSize := int64(input.Size)

		if input.Valid&gofuse.FATTR_FH != 0 {
			// ftruncate(fd, size): truncate the open write buffer.
			fh, ok := fs.fileHandles.Get(input.Fh)
			if ok && fh.Dirty != nil {
				fh.Lock()
				if err := fs.truncateWritableHandleLocked(fh, newSize); err != nil {
					fh.Unlock()
					return gofuse.Status(syscall.EFBIG)
				}
				fh.Unlock()
			}
		} else {
			// truncate(path, size): no open file handle — must persist
			// to the server. We only support truncate-to-zero, which is
			// the common case (e.g. shell "> file").
			if newSize == 0 {
				ctx, cf := fuseCtx(cancel)
				defer cf()
				if err := fs.client.WriteCtx(ctx, entry.Path, nil); err != nil {
					return httpToFuseStatus(err)
				}
				// Refresh the inode revision after the server-side truncate so a
				// subsequent writable open does not reuse the stale pre-truncate
				// base revision and conflict with its own zero-byte write.
				stat, statErr := fs.client.StatCtx(ctx, entry.Path)
				if statErr != nil {
					log.Printf("post-truncate stat refresh failed for %s (inode=%d): %v (revision may be stale)", entry.Path, input.NodeId, statErr)
				} else if stat != nil {
					if stat.Revision > 0 {
						entry.Revision = stat.Revision
						fs.inodes.UpdateRevision(input.NodeId, stat.Revision)
						fs.updateOpenHandleBaseRevision(entry.Path, stat.Revision, input.Pid)
					}
					if !stat.Mtime.IsZero() {
						entry.Mtime = stat.Mtime
						fs.inodes.UpdateMtime(input.NodeId, stat.Mtime)
					}
				}
				fs.readCache.Invalidate(entry.Path)
				fs.dirCache.Invalidate(parentDir(entry.Path))
			} else if newSize != entry.Size {
				// Arbitrary truncate without an open handle is not
				// supported — dat9 has no server-side truncate API.
				return gofuse.Status(syscall.ENOTSUP)
			}
		}
		entry.Size = newSize
		fs.inodes.UpdateSize(input.NodeId, newSize)
		// Invalidate kernel attr cache for the truncated inode.
		fs.notifyInode(input.NodeId)
	}

	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(fs.opts.AttrTTL)
	return gofuse.OK
}

// --- Directory operations ----------------------------------------------------

func (fs *Dat9FS) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) gofuse.Status {
	ctx, cf := fuseCtx(cancel)
	defer cf()

	childP, st := fs.childPath(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	if err := fs.client.MkdirCtx(ctx, childP); err != nil {
		return httpToFuseStatus(err)
	}

	ino := fs.inodes.Lookup(childP, true, 0, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}

	parentPath, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Invalidate(parentPath)
	fs.notifyEntry(input.NodeId, name)
	// Invalidate kernel's cached directory listing for parent.
	fs.notifyInode(input.NodeId)

	fs.fillEntryOut(entry, out)
	return gofuse.OK
}

func (fs *Dat9FS) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	pendingNew := false
	if fs.writeBack != nil && fs.uploader != nil {
		// Wait for any in-flight upload to finish so it doesn't "revive"
		// the file on the server after we delete it.
		fs.uploader.WaitPath(childP)
		// Check if the file was created locally and never uploaded.
		if meta, ok := fs.writeBack.GetMeta(childP); ok && meta.Kind == PendingNew {
			pendingNew = true
		}
		// Remove pending cache entry to prevent future background uploads.
		fs.writeBack.Remove(childP)
	}
	// Also check pendingIndex for the pending-new flag before clearing.
	if !pendingNew && fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(childP); ok && meta.Kind == PendingNew {
			pendingNew = true
		}
	}
	// Wait for any in-flight commitQueue upload and cancel it so the
	// background commit cannot resurrect the deleted file.
	if fs.commitQueue != nil {
		fs.commitQueue.WaitPath(childP)
		fs.commitQueue.CancelPath(childP)
	} else {
		// Clean up shadow and pending index directly when no commit queue.
		if fs.pendingIndex != nil {
			fs.pendingIndex.Remove(childP)
		}
		if fs.shadowStore != nil {
			fs.shadowStore.Remove(childP)
		}
	}

	if !pendingNew {
		ctx, cf := fuseCtx(cancel)
		defer cf()

		// File existed on server (or unknown) — issue remote DELETE.
		// Tolerate 404 in case it was already deleted.
		if err := fs.client.DeleteCtx(ctx, childP); err != nil {
			if !isNotFoundErr(err) {
				return httpToFuseStatus(err)
			}
		}
	}

	fs.inodes.Remove(childP)
	fs.readCache.Invalidate(childP)

	parentPath, _ := fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Invalidate(parentPath)
	// Tell kernel the entry no longer exists and parent dir changed.
	fs.notifyEntry(header.NodeId, name)
	fs.notifyInode(header.NodeId)
	return gofuse.OK
}

func (fs *Dat9FS) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	ctx, cf := fuseCtx(cancel)
	defer cf()

	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	if err := fs.client.DeleteCtx(ctx, childP); err != nil {
		return httpToFuseStatus(err)
	}

	// Clean write-back entries for files under the removed directory.
	// Without this, the uploader would try to PUT to paths under a deleted dir.
	prefix := childP + "/"
	if fs.writeBack != nil {
		for p := range fs.writeBack.ListPendingPaths() {
			if strings.HasPrefix(p, prefix) {
				if fs.uploader != nil {
					fs.uploader.WaitPath(p)
				}
				fs.writeBack.Remove(p)
			}
		}
	}
	// Cancel commitQueue entries for the subtree so background commits
	// cannot resurrect deleted files. CancelPrefix handles shadow+index cleanup.
	if fs.commitQueue != nil {
		fs.commitQueue.CancelPrefix(prefix)
	} else {
		// Clean pendingIndex and shadowStore directly when no commit queue.
		if fs.pendingIndex != nil {
			for _, meta := range fs.pendingIndex.ListByPrefix(prefix) {
				if fs.shadowStore != nil {
					fs.shadowStore.Remove(meta.Path)
				}
				fs.pendingIndex.Remove(meta.Path)
			}
		}
	}

	fs.inodes.Remove(childP)
	fs.dirCache.Invalidate(childP)
	fs.readCache.InvalidatePrefix(childP + "/")

	parentPath, _ := fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Invalidate(parentPath)
	// Tell kernel the entry no longer exists and parent dir changed.
	fs.notifyEntry(header.NodeId, name)
	fs.notifyInode(header.NodeId)
	return gofuse.OK
}

func (fs *Dat9FS) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) gofuse.Status {
	ctx, cf := fuseCtx(cancel)
	defer cf()

	oldP, st := fs.childPath(input.NodeId, oldName)
	if st != gofuse.OK {
		return st
	}
	newP, st := fs.childPath(input.Newdir, newName)
	if st != gofuse.OK {
		return st
	}

	// Wait for any in-flight commitQueue uploads for both paths (and
	// descendants) so a background commit cannot PUT to stale paths.
	if fs.commitQueue != nil {
		fs.commitQueue.WaitPath(oldP)
		fs.commitQueue.WaitPath(newP)
		fs.commitQueue.WaitPrefix(oldP + "/")
	}

	if fs.writeBack != nil && fs.uploader != nil {
		// Wait for any in-flight uploads for both paths. This prevents a
		// background worker from PUT-ing to oldP after we rename away from it.
		fs.uploader.WaitPath(oldP)
		fs.uploader.WaitPath(newP)

		// Fast path (vim :w): if oldP is a pending-new file (created locally,
		// never existed on the server), rename it locally. The background
		// uploader will upload to newP instead. This avoids a synchronous
		// upload on the vim :w critical path.
		//
		// Pending-overwrite files (edits to existing remote files) must NOT
		// use this path — the original remote file still exists at oldP and
		// requires a server-side rename after the upload completes.
		isPendingNew := false
		if meta, ok := fs.writeBack.GetMeta(oldP); ok && meta.Kind == PendingNew {
			fs.writeBack.RenamePending(oldP, newP)
			fs.uploader.Submit(newP)
			isPendingNew = true
		}
		// Also check pendingIndex for files handed to commitQueue after Release.
		if !isPendingNew && fs.pendingIndex != nil {
			if meta, ok := fs.pendingIndex.GetMeta(oldP); ok && meta.Kind == PendingNew {
				if fs.shadowStore != nil {
					fs.shadowStore.Rename(oldP, newP)
				}
				fs.pendingIndex.RenamePending(oldP, newP)
				isPendingNew = true
			}
		}
		if isPendingNew {
			fs.inodes.Rename(oldP, newP)
			fs.readCache.Invalidate(oldP)
			fs.readCache.InvalidatePrefix(oldP + "/")

			oldParent, _ := fs.inodes.GetPath(input.NodeId)
			fs.dirCache.Invalidate(oldParent)
			fs.notifyEntry(input.NodeId, oldName)
			fs.notifyInode(input.NodeId)
			if input.Newdir != input.NodeId {
				newParent, _ := fs.inodes.GetPath(input.Newdir)
				fs.dirCache.Invalidate(newParent)
				fs.notifyEntry(input.Newdir, newName)
				fs.notifyInode(input.Newdir)
			}
			return gofuse.OK
		}

		// Slow path: either oldP is not pending, or it's a pending-overwrite
		// (existing remote file edited locally). Flush both sides first, then
		// do a server-side rename.
		if err := fs.flushPendingWriteBack(ctx, oldP); err != nil {
			log.Printf("rename: flush pending write-back for %s: %v", oldP, err)
			return httpToFuseStatus(err)
		}
		if err := fs.flushPendingWriteBack(ctx, newP); err != nil {
			log.Printf("rename: flush pending write-back for %s: %v", newP, err)
			return httpToFuseStatus(err)
		}
	}

	if err := fs.client.RenameCtx(ctx, oldP, newP); err != nil {
		return httpToFuseStatus(err)
	}

	// After server-side rename, migrate pending descendants.
	// If oldP is a directory, pending children under oldP+"/", must be
	// re-keyed to newP+"/". Without this the uploader would PUT to stale paths.
	prefix := oldP + "/"
	if fs.writeBack != nil {
		for p := range fs.writeBack.ListPendingPaths() {
			if strings.HasPrefix(p, prefix) {
				newChild := newP + p[len(oldP):]
				if fs.uploader != nil {
					fs.uploader.WaitPath(p)
				}
				fs.writeBack.RenamePending(p, newChild)
				if fs.uploader != nil {
					fs.uploader.Submit(newChild)
				}
			}
		}
	}
	// Also migrate pendingIndex and shadowStore entries for descendants.
	if fs.pendingIndex != nil {
		for _, meta := range fs.pendingIndex.ListByPrefix(prefix) {
			newChild := newP + meta.Path[len(oldP):]
			if fs.shadowStore != nil {
				fs.shadowStore.Rename(meta.Path, newChild)
			}
			fs.pendingIndex.RenamePending(meta.Path, newChild)
		}
	}

	fs.inodes.Rename(oldP, newP)
	fs.readCache.Invalidate(oldP)
	fs.readCache.InvalidatePrefix(oldP + "/")

	oldParent, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Invalidate(oldParent)
	// Tell kernel old entry is gone and old parent dir changed.
	fs.notifyEntry(input.NodeId, oldName)
	fs.notifyInode(input.NodeId)
	if input.Newdir != input.NodeId {
		newParent, _ := fs.inodes.GetPath(input.Newdir)
		fs.dirCache.Invalidate(newParent)
		// Tell kernel new parent dir changed too.
		fs.notifyEntry(input.Newdir, newName)
		fs.notifyInode(input.Newdir)
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
		ctx, cf := fuseCtx(cancel)
		defer cf()
		entries, err := fs.listDir(ctx, dh.Path)
		if err != nil {
			log.Printf("list dir failed for %s: %v", dh.Path, err)
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
		ctx, cf := fuseCtx(cancel)
		defer cf()
		entries, err := fs.listDir(ctx, dh.Path)
		if err != nil {
			log.Printf("list dir plus failed for %s: %v", dh.Path, err)
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
		if !fs.inodes.IncrementLookup(e.Ino) {
			return gofuse.EIO
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

func (fs *Dat9FS) listDir(ctx context.Context, dirPath string) ([]DirEntry, error) {
	// Check dir cache first
	if cached, ok := fs.dirCache.Get(dirPath); ok {
		entries := fs.cachedToDirEntries(dirPath, cached)
		return fs.mergePendingDirEntries(dirPath, entries), nil
	}

	items, err := fs.client.ListCtx(ctx, dirPath)
	if err != nil {
		return nil, err
	}

	// Store in dir cache
	cached := make([]CachedFileInfo, len(items))
	for i, item := range items {
		var mtime time.Time
		if item.Mtime > 0 {
			mtime = time.Unix(item.Mtime, 0)
		}
		cached[i] = CachedFileInfo{
			Name:  item.Name,
			Size:  item.Size,
			IsDir: item.IsDir,
			Mtime: mtime,
		}
	}
	fs.dirCache.Put(dirPath, cached)

	entries := fs.cachedToDirEntries(dirPath, cached)
	return fs.mergePendingDirEntries(dirPath, entries), nil
}

// mergePendingDirEntries overlays pending write-back entries onto a directory
// listing. Files that exist in the write-back cache or pendingIndex (commit
// queue backed) but are not yet on the server are added to the listing so
// that ls, tab-completion, and editors can see them.
func (fs *Dat9FS) mergePendingDirEntries(dirPath string, entries []DirEntry) []DirEntry {
	// Build set of already-listed names for dedup.
	existing := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		existing[e.Name] = struct{}{}
	}

	// Overlay write-back cache entries.
	if fs.writeBack != nil {
		for p := range fs.writeBack.ListPendingPaths() {
			if parentDir(p) != dirPath {
				continue
			}
			name := path.Base(p)
			if _, ok := existing[name]; ok {
				continue
			}
			meta, ok := fs.writeBack.GetMeta(p)
			if !ok {
				continue
			}
			mtime := meta.Mtime
			if mtime.IsZero() {
				mtime = time.Now()
			}
			ino := fs.inodes.EnsureInode(p, false, meta.Size, mtime)
			entries = append(entries, DirEntry{
				Name: name,
				Ino:  ino,
				Mode: syscall.S_IFREG,
			})
			existing[name] = struct{}{}
		}
	}

	// Overlay commit-queue-backed entries from pendingIndex. These are files
	// handed to commitQueue after Release but not yet uploaded to the server.
	if fs.pendingIndex != nil {
		prefix := dirPath
		if prefix != "/" {
			prefix += "/"
		} else {
			prefix = "/"
		}
		for _, meta := range fs.pendingIndex.ListByPrefix(prefix) {
			if parentDir(meta.Path) != dirPath {
				continue
			}
			name := path.Base(meta.Path)
			if _, ok := existing[name]; ok {
				continue // already listed from writeBack or remote
			}
			mtime := meta.Mtime
			if mtime.IsZero() {
				mtime = time.Now()
			}
			ino := fs.inodes.EnsureInode(meta.Path, false, meta.Size, mtime)
			entries = append(entries, DirEntry{
				Name: name,
				Ino:  ino,
				Mode: syscall.S_IFREG,
			})
			existing[name] = struct{}{}
		}
	}

	return entries
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

		mtime := item.Mtime
		if mtime.IsZero() {
			mtime = time.Now()
		}
		ino := fs.inodes.EnsureInode(childP, item.IsDir, item.Size, mtime)

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

	wb := NewWriteBuffer(childP, streamingWriteMaxSize, 0)
	wb.touched = true
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)
	fh := &FileHandle{
		Ino:         ino,
		Path:        childP,
		Flags:       input.Flags,
		Dirty:       wb,
		IsNew:       true,
		ShadowReady: false,
	}

	if fs.shadowStore != nil && fs.pendingIndex != nil {
		if err := fs.shadowStore.Ensure(childP, 0, 0); err != nil {
			log.Printf("shadow ensure failed for create %s: %v", childP, err)
		} else {
			fh.ShadowReady = true
		}
	}

	// Attach a streaming uploader for sequential write streaming.
	// For pure sequential writes (cp, dd, ffmpeg), parts are uploaded
	// as they fill during Write() and memory is released immediately.
	// For non-sequential writes, falls back to flush-time UploadAll.
	fh.Streamer = NewStreamUploader(fs.client, childP, expectedRevisionForHandle(fh))

	// Wire up the OnPartFull callback: when a sequential write fills
	// a part, submit it for background upload and evict after completion.
	streamer := fh.Streamer
	wb.OnPartFull = func(partIdx int, data []byte) {
		partNum := partIdx + 1
		if err := streamer.SubmitPart(context.Background(), partNum, data, func(pn int) {
			fh.Lock()
			fh.Dirty.EvictPart(pn - 1) // 0-based
			fh.Unlock()
		}); err != nil {
			log.Printf("streaming submit part %d failed for %s: %v", partNum, childP, err)
		}
	}

	fh.DirtySeq = fs.markDirtySize(ino, 0)
	out.Fh = fs.fileHandles.Allocate(fh)
	// Use cached I/O for small/interactive files. Kernel coalesces writes
	// and serves reads from page cache after first access.
	// Keep DIRECT_IO for O_TRUNC streaming files.
	out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
	fs.fillEntryOut(entry, &out.EntryOut)

	parentPath, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Invalidate(parentPath)
	fs.notifyEntry(input.NodeId, name)
	// Invalidate kernel's cached directory listing for parent.
	fs.notifyInode(input.NodeId)
	return gofuse.OK
}

func (fs *Dat9FS) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	ctx, cf := fuseCtx(cancel)
	defer cf()

	p, ok := fs.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}

	fh := &FileHandle{
		Ino:     input.NodeId,
		Path:    p,
		Flags:   input.Flags,
		OpenPID: input.Pid,
	}
	entry, _ := fs.inodes.GetEntry(input.NodeId)
	if entry != nil {
		fh.OrigSize = entry.Size
		fh.BaseRev = entry.Revision
	}

	// Allocate write buffer for writable opens
	accMode := input.Flags & syscall.O_ACCMODE
	if accMode == syscall.O_WRONLY || accMode == syscall.O_RDWR {
		if fs.opts.ReadOnly {
			return gofuse.EROFS
		}

		// If BaseRev is 0 (e.g. inode came from readdir without revision),
		// fetch the authoritative revision so CAS uploads work correctly.
		if fh.BaseRev == 0 && !fh.IsNew {
			if stat, err := fs.client.StatCtx(ctx, p); err == nil && stat != nil {
				fh.BaseRev = stat.Revision
				fs.inodes.UpdateRevision(input.NodeId, stat.Revision)
			}
		}

		fh.Dirty = NewWriteBuffer(p, maxPreloadSize, 0)

		// Preload existing content for non-truncating opens so that
		// random writes don't discard the original file data.
		if input.Flags&syscall.O_TRUNC == 0 {
			preloaded := false
			if fs.pendingIndex != nil && fs.shadowStore != nil {
				if meta, ok := fs.pendingIndex.GetMeta(p); ok && fs.shadowStore.Has(p) {
					if err := fs.loadWritableHandleFromShadowLocked(fh, meta); err == nil {
						preloaded = true
					} else {
						log.Printf("shadow preload failed for %s: %v", p, err)
					}
				}
			}
			// Prefer write-back cache over remote — handles the case where
			// a previous close is still uploading asynchronously.
			if !preloaded && fs.writeBack != nil {
				if wbData, ok := fs.writeBack.Get(p); ok {
					if _, err := fh.Dirty.Write(0, wbData); err != nil {
						return gofuse.Status(syscall.EFBIG)
					}
					// Keep dirty parts so Read sees the data. The content hasn't
					// been persisted to the server yet (pending upload), so
					// marking it dirty is semantically correct.
					fh.OrigSize = int64(len(wbData))
					fh.DirtySeq = fs.markDirtySize(fh.Ino, int64(len(wbData)))
					preloaded = true
				}
			}
			if !preloaded {
				if st := fs.preloadWritableHandle(ctx, fh); st != gofuse.OK {
					return st
				}
			}
		} else {
			// O_TRUNC: mark buffer as dirty so that close() without any
			// writes still persists the truncation (POSIX semantics).
			fh.Dirty.maxSize = streamingWriteMaxSize
			fh.Dirty.sequential = true
			fh.Dirty.uploadedParts = make(map[int]bool)
			_ = fh.Dirty.Truncate(0)
			fh.ZeroBase = true
			fh.DirtySeq = fs.markDirtySize(fh.Ino, 0)
			fs.inodes.UpdateSize(fh.Ino, 0)
			if fs.shadowStore != nil && fs.pendingIndex != nil {
				if err := fs.shadowStore.Ensure(p, 0, fh.BaseRev); err != nil {
					log.Printf("shadow ensure failed for truncate-open %s: %v", p, err)
				} else {
					fh.ShadowReady = true
				}
			}

			// Attach streaming uploader with OnPartFull wiring.
			fh.Streamer = NewStreamUploader(fs.client, p, expectedRevisionForHandle(fh))
			streamer := fh.Streamer
			filePath := p
			fh.Dirty.OnPartFull = func(partIdx int, data []byte) {
				partNum := partIdx + 1
				if err := streamer.SubmitPart(context.Background(), partNum, data, func(pn int) {
					fh.Lock()
					fh.Dirty.EvictPart(pn - 1)
					fh.Unlock()
				}); err != nil {
					log.Printf("streaming submit part %d failed for %s: %v", partNum, filePath, err)
				}
			}
		}
	}

	// Set up read prefetcher for read-only opens on large files.
	if fh.Dirty == nil {
		entry, _ := fs.inodes.GetEntry(input.NodeId)
		if entry != nil && entry.Size > smallFileThreshold {
			fh.Prefetch = NewPrefetcher(fs.client, p, entry.Size)
		}
	}

	out.Fh = fs.fileHandles.Allocate(fh)
	if fh.Dirty != nil {
		// Use cached I/O for small/interactive files (< 64MB, no O_TRUNC).
		// Kernel coalesces writes and serves reads from page cache.
		// Keep DIRECT_IO for O_TRUNC or large streaming files.
		if input.Flags&syscall.O_TRUNC != 0 || fh.OrigSize >= smallFileShadowThreshold {
			out.OpenFlags = gofuse.FOPEN_DIRECT_IO
		} else {
			out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
		}
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
	// If there's a dirty buffer (even empty — e.g. after Create or truncate-to-zero),
	// read from it so we don't go back to the server and see stale/non-existent data.
	// Uses ReadAt to avoid materializing the entire sparse buffer.
	//
	// However, if the handle has evicted (streaming-uploaded) parts, we cannot
	// serve reads from those ranges — the data is on S3 but not in memory.
	// For such ranges we fall through to the server read path.
	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}

		// Check if the read range touches any evicted part.
		// If so, we cannot serve this read from memory — fall through to server.
		touchesEvicted := false
		if evicted := fh.Dirty.StreamedPartIndices(); len(evicted) > 0 {
			ps := fh.Dirty.PartSize()
			firstPart := int(offset / ps)
			lastPart := int((end - 1) / ps)
			for p := firstPart; p <= lastPart; p++ {
				if evicted[p] && !fh.Dirty.IsPartLoaded(p) {
					touchesEvicted = true
					break
				}
			}
		}

		if !touchesEvicted {
			// Ensure parts touched by this read are loaded from the server
			// before calling ReadAt. Without this, ReadAt returns zeros for
			// unloaded parts in lazily-loaded files.
			ps := fh.Dirty.PartSize()
			firstPart := int(offset / ps)
			lastPart := int((end - 1) / ps)
			for p := firstPart; p <= lastPart; p++ {
				if !fh.Dirty.IsPartLoaded(p) {
					if err := fh.Dirty.EnsureLoaded(p); err != nil {
						fh.Unlock()
						return nil, gofuse.EIO
					}
				}
			}

			result := make([]byte, end-offset)
			fh.Dirty.ReadAt(offset, result)
			fh.Unlock()
			return gofuse.ReadResultData(result), gofuse.OK
		}
		// touchesEvicted: for new files (remoteSize == 0), the multipart
		// upload has not been completed yet — the object doesn't exist on the
		// server, so ReadStreamRange would fail. Return EIO; sequential writers
		// (cp, dd, ffmpeg) never read back evicted data in practice.
		if fh.Dirty.remoteSize == 0 {
			fh.Unlock()
			return nil, gofuse.EIO
		}
		// Existing file with evicted parts: the original object still exists
		// on the server, so fall through to ReadStreamRange.
		fh.Unlock()
	} else if fh.Dirty != nil && fh.ShadowReady {
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}
		result := make([]byte, end-offset)
		fh.Dirty.ReadAt(offset, result)
		fh.Unlock()
		return gofuse.ReadResultData(result), gofuse.OK
	} else if fh.Dirty != nil && fh.Dirty.Size() > 0 && !fh.Dirty.HasDirtyParts() {
		// Writable handle with lazy-loaded buffer (no dirty parts yet) —
		// read directly from the server for unloaded parts.
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		fh.Unlock()
		// Fall through to server read below
	} else {
		fh.Unlock()
	}

	// Read path priority for pending files:
	// 1. ShadowStore.ReadAt (local SSD) — for files staged by Flush
	// 2. WriteBackCache.Get (local disk, full file) — legacy path
	if fh.Dirty == nil && fs.shadowStore != nil && fs.shadowStore.Has(fh.Path) {
		offset := int64(input.Offset)
		sz := fs.shadowStore.Size(fh.Path)
		if sz >= 0 {
			if offset >= sz {
				return gofuse.ReadResultData(nil), gofuse.OK
			}
			end := offset + int64(input.Size)
			if end > sz {
				end = sz
			}
			buf := make([]byte, end-offset)
			n, err := fs.shadowStore.ReadAt(fh.Path, offset, buf)
			if err == nil && n > 0 {
				return gofuse.ReadResultData(buf[:n]), gofuse.OK
			}
		}
	}
	// Close-to-open consistency: if a previous handle wrote data to the
	// write-back cache (async upload still in progress), serve reads from
	// that cached data instead of going to the server (which has stale data).
	if fh.Dirty == nil && fs.writeBack != nil {
		if wbData, ok := fs.writeBack.Get(fh.Path); ok {
			offset := int64(input.Offset)
			if offset >= int64(len(wbData)) {
				return gofuse.ReadResultData(nil), gofuse.OK
			}
			end := offset + int64(input.Size)
			if end > int64(len(wbData)) {
				end = int64(len(wbData))
			}
			return gofuse.ReadResultData(wbData[offset:end]), gofuse.OK
		}
	}

	ctx, cf := fuseCtx(cancel)
	defer cf()

	p := fh.Path

	// Try prefetcher for large read-only files
	if fh.Prefetch != nil {
		offset := int64(input.Offset)
		size := int(input.Size)
		if data, ok := fh.Prefetch.Get(offset, size); ok {
			// Trigger next prefetch
			fh.Prefetch.OnRead(offset, len(data))
			return gofuse.ReadResultData(data), gofuse.OK
		}
		// Cache miss — fall through to direct read, then trigger prefetch
		defer func() {
			fh.Prefetch.OnRead(offset, size)
		}()
	}

	// Try read cache for small files. Use revision-aware cache: if the
	// InodeEntry has a stored revision from the last Lookup/GetAttr, pass
	// it to the cache for validation. Cache hit only if revision matches.
	entry, _ := fs.inodes.GetEntry(fh.Ino)
	if entry != nil && entry.Size <= smallFileThreshold && entry.Size > 0 {
		cacheRev := entry.Revision // use revision from last Stat/Lookup
		// Fast path: serve from cache without any HTTP call.
		if data, ok := fs.readCache.Get(p, cacheRev); ok {
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

		// Cache miss: read the file and store it. No separate Stat needed —
		// ReadCtx fetches the data in one round-trip.
		data, err := fs.client.ReadCtx(ctx, p)
		if err != nil {
			return nil, httpToFuseStatus(err)
		}
		// Store with the revision from the prior Stat/Lookup.
		fs.readCache.Put(p, data, cacheRev)
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

	// Large file or unknown size: range read (avoids O(offset) discard)
	rc, err := fs.client.ReadStreamRange(ctx, p, int64(input.Offset), int64(input.Size))
	if err != nil {
		return nil, httpToFuseStatus(err)
	}
	defer func() { _ = rc.Close() }()

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
		fh.Dirty = NewWriteBuffer(fh.Path, 0, 0)
	}

	n, err := fh.Dirty.Write(int64(input.Offset), data)
	if err != nil {
		return 0, gofuse.Status(syscall.EFBIG)
	}
	if fh.ShadowReady && fs.shadowStore != nil {
		if _, err := fs.shadowStore.WriteAt(fh.Path, int64(input.Offset), data, fh.BaseRev); err != nil {
			log.Printf("shadow write-through failed for %s: %v", fh.Path, err)
			fs.shadowStore.Remove(fh.Path)
			fh.ShadowReady = false
		}
	}
	fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
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

	// Write-back path: small dirty files are persisted to local disk
	// and return immediately. The actual HTTP upload happens in Release
	// (async). This reduces Flush latency from ~100-300ms to ~1-5ms.
	//
	// IMPORTANT: We do NOT ClearDirty here. The buffer stays dirty as a
	// safety net — if the user writes more data between Flush and Release,
	// Release will see HasDirtyParts() == true and fall through to the
	// synchronous flushHandle path, uploading the latest data. The cache
	// entry is just a snapshot for the async-upload fast path.
	if fs.writeBack != nil && fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		// Same generation already cached — no new writes since last Flush.
		if fh.WriteBackSeq > 0 && fh.WriteBackSeq == fh.DirtySeq {
			return gofuse.OK
		}
		size := fh.Dirty.Size()
		// Only use write-back for small files that haven't started streaming.
		// A Streamer may exist (Create always attaches one) but as long as
		// no parts have been streamed, the data is still fully in the WriteBuffer.
		hasActiveStream := fh.Streamer != nil && fh.Streamer.HasStreamedParts()
		if size < writeBackThreshold && !hasActiveStream {
			// Only stage locally when the shadow/buffer represents the full
			// current file contents. Otherwise a background full-file PUT would
			// silently zero untouched remote-backed ranges.
			if fs.canStageShadowFastLocked(fh) || fh.Dirty.CanMaterializeFull() {
				if fs.shadowStore != nil && fs.pendingIndex != nil {
					if err := fs.stageShadowLocked(fh, true); err != nil {
						log.Printf("shadow stage failed for %s: %v, falling through", fh.Path, err)
					} else {
						if err := fs.snapshotWriteBackLocked(fh); err != nil && fs.writeBack != nil {
							log.Printf("writeback snapshot failed for %s: %v", fh.Path, err)
						}
						fh.WriteBackSeq = fh.DirtySeq
						return gofuse.OK
					}
				}

				if err := fs.snapshotWriteBackLocked(fh); err != nil {
					log.Printf("writeback cache put failed for %s: %v, falling back to sync upload", fh.Path, err)
				} else {
					if fs.pendingIndex != nil {
						_, _ = fs.pendingIndex.PutWithBaseRev(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev)
					}
					// Snapshot the dirty sequence at cache-write time so
					// Release can detect whether new writes happened since.
					fh.WriteBackSeq = fh.DirtySeq
					return gofuse.OK
				}
			}
		}
	}

	ctx, cf := fuseCtx(cancel)
	defer cf()

	return fs.flushHandleDebounced(ctx, fh, false)
}

func (fs *Dat9FS) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return gofuse.OK
	}

	fh.Lock()
	defer fh.Unlock()

	// Interactive mode: Fsync = local durable only. Shadow file + journal
	// ensure crash safety. Remote commit happens asynchronously.
	if fs.syncMode == SyncInteractive {
		if fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
			return gofuse.OK
		}
		if err := fs.stageShadowLocked(fh, true); err == nil {
			if err := fs.snapshotWriteBackLocked(fh); err != nil && fs.writeBack != nil {
				log.Printf("fsync writeback snapshot failed for %s: %v", fh.Path, err)
			}
			fh.WriteBackSeq = fh.DirtySeq
			// Journal fsync for local durability (when journal is available).
			if fs.journal != nil {
				entry := JournalEntry{
					Op:      JournalFsync,
					Path:    fh.Path,
					BaseRev: fh.BaseRev,
				}
				_ = fs.journal.Append(entry)
				_ = fs.journal.Fsync()
			}
			return gofuse.OK
		}
	}

	// Strict mode: Fsync = remote durable. Upload to server before returning.
	ctx, cf := fuseCtx(cancel)
	defer cf()

	if fs.writeBack != nil && fs.uploader != nil && fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq {
		// Snapshot matches current dirty state — safe to upload.
		if err := fs.uploader.UploadSync(ctx, fh.Path); err != nil {
			log.Printf("fsync writeback upload failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}
		// UploadSync already persisted the data to the server. Clear
		// the dirty state so the subsequent flushHandleDebounced sees
		// !HasDirtyParts() and skips the redundant upload.
		if fh.Dirty != nil {
			fh.Dirty.ClearDirty()
			fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			fh.WriteBackSeq = 0
		}
	} else if fs.writeBack != nil && fh.WriteBackSeq != 0 && fh.WriteBackSeq != fh.DirtySeq {
		// Snapshot is stale — discard it so we don't upload old data.
		fs.writeBack.Remove(fh.Path)
		fh.WriteBackSeq = 0
	}

	return fs.flushHandleDebounced(ctx, fh, true)
}

func (fs *Dat9FS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if ok {
		// Cancel any pending debounce for this path — Release always flushes immediately.
		fs.debouncer.Cancel(fh.Path)

		// Check if Flush already wrote this file to the write-back cache
		// AND no new writes have happened since. If the DirtySeq changed,
		// the cache snapshot is stale — fall through to synchronous upload
		// which will upload the latest buffer data.
		if fs.writeBack != nil && fs.uploader != nil {
			fh.Lock()
			canUseCache := fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq
			if canUseCache {
				fh.Dirty.ClearDirty()
				fs.clearDirtySize(fh.Ino, fh.DirtySeq)
				fh.DirtySeq = 0
				fh.WriteBackSeq = 0
				fh.Unlock()

				// Enqueue to CommitQueue if available (P1), otherwise
				// use the legacy uploader.
				if fs.commitQueue != nil && fs.shadowStore != nil && fs.shadowStore.Has(fh.Path) {
					entry := &CommitEntry{
						Path:    fh.Path,
						Inode:   fh.Ino,
						BaseRev: fh.BaseRev,
						Size:    fh.Dirty.Size(),
						Kind:    PendingOverwrite,
					}
					if fh.IsNew {
						entry.Kind = PendingNew
					}
					if err := fs.commitQueue.Enqueue(entry); err != nil {
						// Backpressure — fall back to legacy uploader.
						fs.uploader.Submit(fh.Path)
					} else {
						// CommitQueue owns the upload via shadow; remove the
						// writeBack .dat/.meta snapshot so it doesn't leak or
						// serve stale data to Lookup/Read.
						fs.writeBack.Remove(fh.Path)
					}
				} else {
					// Async upload — the uploader will read from cache and upload.
					fs.uploader.Submit(fh.Path)
				}

				// Invalidate caches so subsequent reads see fresh data.
				fs.readCache.Invalidate(fh.Path)
				fs.dirCache.Invalidate(parentDir(fh.Path))
				fs.notifyInode(fh.Ino)
				parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
				fs.notifyInode(parentIno)

				// Close prefetcher to cancel inflight goroutines.
				if fh.Prefetch != nil {
					fh.Prefetch.Close()
				}
				fs.fileHandles.Delete(input.Fh)
				return
			}
			// Stale cache — remove it, fall through to sync upload.
			if fh.WriteBackSeq != 0 {
				fs.writeBack.Remove(fh.Path)
				fh.WriteBackSeq = 0
			}
			fh.Unlock()
		}

		// Normal path: synchronous upload in Release.
		// Release uses a generous timeout since it must persist data.
		ctx, cf := context.WithTimeout(context.Background(), 60*time.Second)
		defer cf()

		fh.Lock()
		st := fs.flushHandle(ctx, fh)
		streamer := fh.Streamer
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		fh.Unlock()

		if st != gofuse.OK && streamer != nil {
			// Flush failed — abort the streaming upload to avoid orphaned
			// multipart uploads on S3. Called without fh.mu to avoid deadlock
			// with inflight SubmitPart goroutines that need fh.Lock() in onDone.
			streamer.Abort()
			log.Printf("flush failed for %s (status %d), aborted stream upload", fh.Path, st)
		}

		// Close prefetcher to cancel inflight goroutines.
		if fh.Prefetch != nil {
			fh.Prefetch.Close()
		}
	}
	fs.fileHandles.Delete(input.Fh)
}

// flushHandleDebounced wraps flushHandle with optional debouncing for small files.
// When force is false and the file is small, the upload may be deferred.
// Caller must hold fh.mu.
func (fs *Dat9FS) flushHandleDebounced(ctx context.Context, fh *FileHandle, force bool) gofuse.Status {
	if force || fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		return fs.flushHandle(ctx, fh)
	}

	size := fh.Dirty.Size()
	if size >= smallFileThreshold || fs.debouncer.delay <= 0 {
		return fs.flushHandle(ctx, fh)
	}

	// Small file: schedule a deferred upload.
	// Snapshot the data so the deferred upload sees a consistent copy.
	data := make([]byte, len(fh.Dirty.Bytes()))
	copy(data, fh.Dirty.Bytes())
	filePath := fh.Path
	ino := fh.Ino
	handle := fh               // capture for goroutine
	snapshotSeq := fh.DirtySeq // capture current dirty sequence
	expectedRevision := expectedRevisionForHandle(fh)

	fs.debouncer.Schedule(filePath, func() {
		dCtx, dCf := context.WithTimeout(context.Background(), fuseTimeout)
		defer dCf()
		if err := fs.client.WriteCtxConditional(dCtx, filePath, data, expectedRevision); err != nil {
			log.Printf("debounced flush failed for %s: %v", filePath, err)
			return
		}
		// Only clear dirty if no writes occurred since the snapshot was taken.
		// If DirtySeq changed, the buffer has new data that wasn't uploaded.
		handle.Lock()
		fs.finalizeHandleFlushLocked(handle, expectedRevision)
		if handle.Dirty != nil && handle.DirtySeq == snapshotSeq {
			handle.Dirty.ClearDirty()
		}
		handle.Unlock()
		fs.readCache.Invalidate(filePath)
		fs.dirCache.Invalidate(parentDir(filePath))
		fs.inodes.UpdateSize(ino, int64(len(data)))
		fs.notifyInode(ino)
		parentIno, _ := fs.inodes.GetInode(parentDir(filePath))
		fs.notifyInode(parentIno)
	})

	// Do NOT ClearDirty here — the buffer stays dirty as a safety net.
	// If Release fires before the debouncer, it cancels the timer and
	// flushHandle will upload from the still-dirty buffer.
	// If the debouncer fires first, its callback clears dirty state.
	return gofuse.OK
}

// flushHandle uploads buffered data to the server. Caller must hold fh.mu.
// NOTE: This method temporarily releases fh.mu during network calls
// (FinishStreaming, UploadAll) to avoid deadlock with streaming upload
// callbacks. The lock is re-acquired before modifying handle state.
func (fs *Dat9FS) flushHandle(ctx context.Context, fh *FileHandle) gofuse.Status {
	if fh.Dirty == nil {
		return gofuse.OK
	}
	if !fh.Dirty.HasDirtyParts() {
		return gofuse.OK
	}

	size := fh.Dirty.Size()

	var err error

	// Path 1a: Streaming mode — some parts already uploaded during Write().
	// This path is used for large sequential writes (cp, dd, ffmpeg).
	// Only the final partial part and any dirty (back-written) parts need uploading.
	if fh.Streamer != nil && fh.Streamer.HasStreamedParts() {
		expectedRevision := fh.Streamer.ExpectedRevision()
		partSize := fh.Dirty.PartSize()
		numParts := int((size + partSize - 1) / partSize)
		lastPartNum := numParts // 1-based

		// Determine data for the last part.
		// If the file size is an exact multiple of partSize, the last part was
		// already fully streamed and evicted — pass nil so FinishStreaming
		// does not re-upload it with empty/zero data.
		var lastCp []byte
		if size%partSize != 0 {
			// Last part is partial — it's still in the WriteBuffer
			lastPartData := fh.Dirty.PartData(lastPartNum)
			if lastPartData != nil {
				lastCp = make([]byte, len(lastPartData))
				copy(lastCp, lastPartData)
			}
		}

		// Collect dirty parts that need re-upload (back-written after eviction)
		dirtyParts := fh.Dirty.DirtyStreamedParts()

		streamer := fh.Streamer

		// Release fh.mu before network calls. FinishStreaming calls
		// inflightWg.Wait() which blocks until SubmitPart goroutines finish.
		// Those goroutines call onDone → fh.Lock(), so holding fh.mu here
		// would deadlock.
		fh.Unlock()
		err = streamer.FinishStreaming(ctx, size,
			lastPartNum, lastCp, dirtyParts)
		fh.Lock()

		if err != nil {
			log.Printf("finish streaming failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}

		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		fs.readCache.Invalidate(fh.Path)
		fs.dirCache.Invalidate(parentDir(fh.Path))
		fs.inodes.UpdateSize(fh.Ino, size)
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
		fs.notifyInode(fh.Ino)
		parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
		fs.notifyInode(parentIno)
		return gofuse.OK
	}

	// Path 1b: Large new file with streaming uploader but no streaming parts
	// (non-sequential writes) — upload all parts in parallel at flush time.
	if fh.Streamer != nil && size >= smallFileThreshold {
		expectedRevision := fh.Streamer.ExpectedRevision()
		numParts := int((size + fh.Dirty.PartSize() - 1) / fh.Dirty.PartSize())
		partSnapshots := make(map[int][]byte, numParts)
		for pn := 1; pn <= numParts; pn++ {
			src := fh.Dirty.PartData(pn)
			if src != nil {
				cp := make([]byte, len(src))
				copy(cp, src)
				partSnapshots[pn] = cp
			}
		}

		streamer := fh.Streamer

		// Release fh.mu during network call (same deadlock avoidance as Path 1a).
		fh.Unlock()
		err = streamer.UploadAll(ctx, size, partSnapshots)
		fh.Lock()

		if err != nil {
			log.Printf("upload all parts failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}

		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		fs.readCache.Invalidate(fh.Path)
		fs.dirCache.Invalidate(parentDir(fh.Path))
		fs.inodes.UpdateSize(fh.Ino, size)
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
		fs.notifyInode(fh.Ino)
		parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
		fs.notifyInode(parentIno)
		return gofuse.OK
	}

	// Path 2: No streaming uploader or small file — materialize all data for upload.
	data := fh.Dirty.Bytes()
	expectedRevision := expectedRevisionForHandle(fh)

	if size < smallFileThreshold {
		// Small file: direct PUT.
		err = fs.client.WriteCtxConditional(ctx, fh.Path, data, expectedRevision)
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
				ctx,
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
				client.WithPartSize(fh.Dirty.PartSize()),
				client.WithExpectedRevision(expectedRevision),
			)
		}
		// If no dirty parts, nothing changed — skip upload.
	} else {
		// New large file or small-to-large growth: full upload via multipart.
		err = fs.client.WriteStreamConditional(
			ctx,
			fh.Path,
			bytes.NewReader(data),
			size,
			nil,
			expectedRevision,
		)
	}
	if err != nil {
		log.Printf("flush upload failed for %s: %v", fh.Path, err)
		return httpToFuseStatus(err)
	}

	fh.Dirty.ClearDirty()
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	fs.readCache.Invalidate(fh.Path)
	fs.dirCache.Invalidate(parentDir(fh.Path))
	fs.inodes.UpdateSize(fh.Ino, size)
	fs.finalizeHandleFlushLocked(fh, expectedRevision)
	// Invalidate kernel attr/data cache for this inode and parent dir listing.
	fs.notifyInode(fh.Ino)
	parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
	fs.notifyInode(parentIno)
	return gofuse.OK
}

// FlushAll flushes all open file handles, drains pending debounced uploads,
// drains the write-back uploader, and waits for inflight async kernel
// notifications to complete. Used during graceful shutdown.
func (fs *Dat9FS) FlushAll() {
	// Drain all pending debounced uploads first.
	fs.debouncer.FlushAll()

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
		// Per-handle timeout so each flush gets a full 60s regardless of
		// how many handles precede it.
		ctx, cf := context.WithTimeout(context.Background(), 60*time.Second)
		e.fh.Lock()
		fs.flushHandle(ctx, e.fh)
		e.fh.Unlock()
		cf()
	}

	// Drain all pending write-back uploads before shutting down.
	if fs.uploader != nil {
		fs.uploader.DrainAll()
	}

	// Drain CommitQueue (P1).
	if fs.commitQueue != nil {
		fs.commitQueue.DrainAll()
	}

	// Close journal.
	if fs.journal != nil {
		_ = fs.journal.Close()
	}

	// Close shadow store.
	if fs.shadowStore != nil {
		fs.shadowStore.Close()
	}

	// Wait for any inflight async kernel notifications to complete.
	fs.notifyWg.Wait()
}

// StatFs reports a generous virtual capacity so that apps (Obsidian, Finder)
// see free space and allow writes. The actual limit is server-side.
func (fs *Dat9FS) StatFs(cancel <-chan struct{}, header *gofuse.InHeader, out *gofuse.StatfsOut) gofuse.Status {
	const blockSize = 4096
	const totalBlocks = (1 << 40) / blockSize // 1 TiB
	out.Blocks = totalBlocks
	out.Bfree = totalBlocks - 1
	out.Bavail = totalBlocks - 1
	out.Bsize = blockSize
	out.NameLen = 255
	out.Frsize = blockSize
	return gofuse.OK
}

// --- Xattr stubs (macOS Finder/Spotlight compatibility) ----------------------

func (fs *Dat9FS) GetXAttr(cancel <-chan struct{}, header *gofuse.InHeader, attr string, dest []byte) (uint32, gofuse.Status) {
	return 0, gofuse.ENOATTR
}

func (fs *Dat9FS) ListXAttr(cancel <-chan struct{}, header *gofuse.InHeader, dest []byte) (uint32, gofuse.Status) {
	return 0, gofuse.OK
}

func (fs *Dat9FS) SetXAttr(cancel <-chan struct{}, input *gofuse.SetXAttrIn, attr string, data []byte) gofuse.Status {
	return gofuse.OK
}

func (fs *Dat9FS) RemoveXAttr(cancel <-chan struct{}, header *gofuse.InHeader, attr string) gofuse.Status {
	return gofuse.ENOATTR
}

func (fs *Dat9FS) String() string {
	return "dat9"
}
