package fuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
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

	// lookupStatRetry* counters track only the Lookup->Stat retry path so
	// operators can distinguish absorbed interrupt noise from exhausted retries
	// on the primary probe route. GetAttr and list-fallback retries intentionally
	// reuse the retry logic without contributing to these counters.
	lookupStatRetryTotal     atomic.Uint64
	lookupStatRetrySuccess   atomic.Uint64
	lookupStatRetryExhausted atomic.Uint64
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

const (
	// lookupTransientRetryCount is the number of detached retries after the
	// initial Lookup StatCtx attempt fails with a transient error.
	lookupTransientRetryCount = 2

	// lookupTransientRetryTimeout keeps each detached retry short so interrupted
	// lookups do not block the caller for long.
	lookupTransientRetryTimeout = 250 * time.Millisecond

	// lookupRetrySuccessLogEvery controls how often successful retry recovery is
	// logged, to avoid noisy logs on hot lookup paths.
	lookupRetrySuccessLogEvery uint64 = 200

	// readTransientRetryCount is the number of detached retries after the
	// initial remote Read attempt fails with a transient error (context
	// canceled, deadline exceeded, network timeout, HTTP 5xx).
	readTransientRetryCount = 2

	// readTransientRetryTimeout keeps each detached read retry bounded.
	// Each retry reads at most max_read (1 MiB), so 2s is generous.
	readTransientRetryTimeout = 2 * time.Second
)

// releaseTimeout computes a generous timeout for synchronous uploads in
// Release / FlushAll based on file size. The formula is:
//
//	max(60s, size / 5 MB/s)   capped at 15 min
//
// 5 MB/s is a conservative floor for cross-region S3 uploads. A 1 GiB file
// gets ~205s, which is comfortably reachable on typical home broadband.
func releaseTimeout(size int64) time.Duration {
	const (
		minTimeout = 60 * time.Second
		maxTimeout = 15 * time.Minute
		bandwidth  = 5 << 20 // 5 MB/s
	)
	t := time.Duration(size/bandwidth) * time.Second
	if t < minTimeout {
		t = minTimeout
	}
	if t > maxTimeout {
		t = maxTimeout
	}
	return t
}

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

		loadStart := time.Now()
		fs.debugf("dirty load part start path=%s part=%d off=%d len=%d", filePath, partNum, offset, length)
		rc, err := c.ReadStreamRange(lpCtx, filePath, offset, length)
		if err != nil {
			fs.debugDurationf(loadStart, 0, "dirty load part open failed path=%s part=%d off=%d len=%d err=%v", filePath, partNum, offset, length, err)
			return nil, err
		}
		defer func() { _ = rc.Close() }()

		data, err := io.ReadAll(rc)
		if err != nil {
			fs.debugDurationf(loadStart, 0, "dirty load part read failed path=%s part=%d off=%d len=%d got=%d err=%v", filePath, partNum, offset, length, len(data), err)
			return nil, err
		}
		fs.debugDurationf(loadStart, 0, "dirty load part done path=%s part=%d off=%d len=%d got=%d err=<nil>", filePath, partNum, offset, length, len(data))
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
	if fh.ShadowSpill {
		if _, err := fs.pendingIndex.PutShadowSpill(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev); err != nil {
			log.Printf("pending index put failed for %s: %v", fh.Path, err)
		}
	} else {
		if _, err := fs.pendingIndex.PutWithBaseRev(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev); err != nil {
			log.Printf("pending index put failed for %s: %v", fh.Path, err)
		}
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
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return gofuse.Status(syscall.EAGAIN)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return gofuse.Status(syscall.EAGAIN)
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
		// Keep status mapping aligned with isTransientLookupErr so retry-exhausted
		// timeout paths remain retryable to callers instead of regressing to EIO.
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
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
		strings.Contains(msg, "HTTP 503") ||
		strings.Contains(msg, "HTTP 504"):
		return gofuse.Status(syscall.EAGAIN)
	default:
		return gofuse.EIO
	}
}

func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var se *client.StatusError
	if errors.As(err, &se) && se.StatusCode == http.StatusNotFound {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "not found") || strings.Contains(msg, "HTTP 404")
}

func isTransientLookupErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var se *client.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// errReadRetriesExhausted is a sentinel indicating all detached read retries
// failed with transient errors. Callers should map this to EIO.
var errReadRetriesExhausted = errors.New("read retries exhausted")

// isTransientReadErr classifies errors for Read-path detached retry.
// Same classification as isTransientLookupErr — kept as a separate function
// so Read and Lookup retry policies can diverge independently if needed.
func isTransientReadErr(err error) bool {
	return isTransientLookupErr(err)
}

// readSmallFileWithRetry reads a small file via ReadCtx with bounded detached
// retry on transient failures. The first attempt uses the caller-provided ctx
// (which honors FUSE interrupt). On transient failure, up to
// readTransientRetryCount detached retries are attempted with short timeouts.
// Returns EIO (never EAGAIN) when all retries are exhausted.
func (fs *Dat9FS) readSmallFileWithRetry(ctx context.Context, path string) ([]byte, error) {
	data, err := fs.client.ReadCtx(ctx, path)
	if err == nil || !isTransientReadErr(err) {
		return data, err
	}

	lastErr := err
	for range readTransientRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), readTransientRetryTimeout)
		data, err = fs.client.ReadCtx(retryCtx, path)
		retryCancel()
		if err == nil {
			return data, nil
		}
		if !isTransientReadErr(err) {
			return nil, err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("%w: %s: %v", errReadRetriesExhausted, path, lastErr)
}

// readStreamRangeWithRetry performs a range read with bounded detached retry
// on transient failures. Wraps both the ReadStreamRange open and io.ReadFull
// body read as a single retriable unit. On body-stage transient failure, the
// stream is reopened from scratch on retry.
// Returns (data, nil) on success. On exhausted retries, the returned error
// is a wrapped sentinel so the caller can map it to EIO.
func (fs *Dat9FS) readStreamRangeWithRetry(ctx context.Context, path string, offset, size int64) ([]byte, int, error) {
	data, n, err := fs.doRangeRead(ctx, path, offset, size)
	if err == nil || !isTransientReadErr(err) {
		return data, n, err
	}

	lastErr := err
	for range readTransientRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), readTransientRetryTimeout)
		data, n, err = fs.doRangeRead(retryCtx, path, offset, size)
		retryCancel()
		if err == nil {
			return data, n, nil
		}
		if !isTransientReadErr(err) {
			return nil, 0, err
		}
		lastErr = err
	}
	return nil, 0, fmt.Errorf("%w: %s: %v", errReadRetriesExhausted, path, lastErr)
}

// doRangeRead performs a single range read attempt: open stream + read body.
// Body read errors (other than EOF/ErrUnexpectedEOF) are returned as-is so
// the caller can classify them for retry.
func (fs *Dat9FS) doRangeRead(ctx context.Context, path string, offset, size int64) ([]byte, int, error) {
	rc, err := fs.client.ReadStreamRange(ctx, path, offset, size)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rc.Close() }()

	// Use LimitReader + ReadAll instead of ReadFull so that:
	//   - Clean EOF (server sent full response) → (data, nil) — success
	//   - Truncated body (connection drop) → (partial, err) — surfaces
	//     to retry helper for transient classification
	// io.ReadFull swallows io.ErrUnexpectedEOF, hiding truncation.
	lr := io.LimitReader(rc, size)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, len(data), err
	}
	return data, len(data), nil
}

func (fs *Dat9FS) lookupStatRetryCount() int {
	if fs != nil && fs.opts != nil {
		if fs.opts.LookupRetryCount == 0 {
			return 0
		}
		if fs.opts.LookupRetryCount > 0 {
			return fs.opts.LookupRetryCount
		}
	}
	return lookupTransientRetryCount
}

func (fs *Dat9FS) lookupStatRetryTimeout() time.Duration {
	if fs != nil && fs.opts != nil {
		if fs.opts.LookupRetryTimeout > 0 {
			return fs.opts.LookupRetryTimeout
		}
	}
	return lookupTransientRetryTimeout
}

func (fs *Dat9FS) lookupRetryStats() (total, success, exhausted uint64) {
	if fs == nil {
		return 0, 0, 0
	}
	return fs.lookupStatRetryTotal.Load(), fs.lookupStatRetrySuccess.Load(), fs.lookupStatRetryExhausted.Load()
}

func (fs *Dat9FS) statWithTransientRetry(cancel <-chan struct{}, remotePath string, trackLookupMetrics bool) (*client.StatResult, error) {
	ctx, cf := fuseCtx(cancel)
	stat, err := fs.client.StatCtx(ctx, remotePath)
	cf()
	if err == nil || isNotFoundErr(err) || !isTransientLookupErr(err) {
		return stat, err
	}

	// Mapping interruption to a retryable errno alone is insufficient for callers
	// that treat open/stat failures as terminal and never retry in user space.
	// Absorb short-lived metadata probe interruptions here before returning to
	// the kernel-facing path.
	retryCount := fs.lookupStatRetryCount()
	if retryCount <= 0 {
		return nil, err
	}
	if trackLookupMetrics {
		fs.lookupStatRetryTotal.Add(1)
	}

	lastErr := err
	for range retryCount {
		// Retry attempts intentionally use a detached context instead of
		// fuseCtx(cancel): the initial probe already honored FUSE interrupt, and
		// these retries exist to absorb interrupt races plus short backend jitter.
		// Rebinding cancel here would cancel retries immediately and re-expose
		// transient failures. Keep this detached+bounded behavior unless retry
		// semantics are redesigned.
		retryCtx, retryCancel := context.WithTimeout(context.Background(), fs.lookupStatRetryTimeout())
		stat, err = fs.client.StatCtx(retryCtx, remotePath)
		retryCancel()
		if err == nil {
			if trackLookupMetrics {
				successCount := fs.lookupStatRetrySuccess.Add(1)
				if successCount <= 3 || successCount%lookupRetrySuccessLogEvery == 0 {
					log.Printf("lookup stat retry recovered for %s (success_count=%d)", remotePath, successCount)
				}
			}
			return stat, nil
		}
		if isNotFoundErr(err) || !isTransientLookupErr(err) {
			return nil, err
		}
		lastErr = err
	}

	if trackLookupMetrics {
		fs.lookupStatRetryExhausted.Add(1)
		log.Printf("lookup stat retries exhausted for %s: %v", remotePath, lastErr)
	}
	return nil, lastErr
}

func (fs *Dat9FS) lookupStatWithRetry(cancel <-chan struct{}, childP string) (*client.StatResult, error) {
	return fs.statWithTransientRetry(cancel, childP, true)
}

func (fs *Dat9FS) getAttrStatWithRetry(cancel <-chan struct{}, remotePath string) (*client.StatResult, error) {
	// Keep GetAttr retries out of lookupStatRetry* so that those counters retain
	// a single meaning: Lookup path retry behavior.
	return fs.statWithTransientRetry(cancel, remotePath, false)
}

func (fs *Dat9FS) lookupListWithRetry(cancel <-chan struct{}, parentPath string) ([]client.FileInfo, error) {
	// list-fallback retries are intentionally not counted in lookupStatRetry*;
	// those counters remain scoped to the primary Lookup->Stat path.
	ctx, cf := fuseCtx(cancel)
	items, err := fs.client.ListCtx(ctx, parentPath)
	cf()
	if err == nil || !isTransientLookupErr(err) {
		return items, err
	}
	retryCount := fs.lookupStatRetryCount()
	if retryCount <= 0 {
		return nil, err
	}

	lastErr := err
	for range retryCount {
		// Keep list fallback retries detached from FUSE cancel for the same reason
		// as stat retries above: this path is a compatibility probe after HEAD
		// said "not found", and cancel-coupled retries would collapse to the
		// original transient failure immediately.
		retryCtx, retryCancel := context.WithTimeout(context.Background(), fs.lookupStatRetryTimeout())
		items, err = fs.client.ListCtx(retryCtx, parentPath)
		retryCancel()
		if err == nil || !isTransientLookupErr(err) {
			return items, err
		}
		lastErr = err
	}
	return nil, lastErr
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

	stat, err := fs.lookupStatWithRetry(cancel, childP)
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
		items, listErr := fs.lookupListWithRetry(cancel, parentPath)
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
			stat, err := fs.getAttrStatWithRetry(cancel, entry.Path)
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
			stat, err := fs.getAttrStatWithRetry(cancel, entry.Path)
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
	start := time.Now()
	status := gofuse.OK
	fs.debugf("unlink start path=%s parent_ino=%d name=%s", childP, header.NodeId, name)
	defer func() {
		fs.debugf("unlink done path=%s status=%d dur=%s", childP, status, time.Since(start))
	}()

	pendingNew := false
	if fs.writeBack != nil && fs.uploader != nil {
		// Wait for any in-flight upload to finish so it doesn't "revive"
		// the file on the server after we delete it.
		waitStart := time.Now()
		fs.debugf("unlink wait writeback start path=%s", childP)
		fs.uploader.WaitPath(childP)
		fs.debugDurationf(waitStart, 0, "unlink wait writeback done path=%s", childP)
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
		waitStart := time.Now()
		fs.debugf("unlink wait commit start path=%s", childP)
		fs.commitQueue.WaitPath(childP)
		fs.debugDurationf(waitStart, 0, "unlink wait commit done path=%s", childP)
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
		deleteStart := time.Now()
		fs.debugf("unlink remote delete start path=%s", childP)
		err := fs.client.DeleteCtx(ctx, childP)
		fs.debugDurationf(deleteStart, 0, "unlink remote delete done path=%s err=%v", childP, err)
		if err != nil {
			if !isNotFoundErr(err) {
				status = httpToFuseStatus(err)
				return status
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
	start := time.Now()
	status := gofuse.OK
	fs.debugf("rmdir start path=%s parent_ino=%d name=%s", childP, header.NodeId, name)
	defer func() {
		fs.debugf("rmdir done path=%s status=%d dur=%s", childP, status, time.Since(start))
	}()

	deleteStart := time.Now()
	fs.debugf("rmdir remote delete start path=%s", childP)
	err := fs.client.DeleteCtx(ctx, childP)
	fs.debugDurationf(deleteStart, 0, "rmdir remote delete done path=%s err=%v", childP, err)
	if err != nil {
		status = httpToFuseStatus(err)
		return status
	}

	// Clean write-back entries for files under the removed directory.
	// Without this, the uploader would try to PUT to paths under a deleted dir.
	prefix := childP + "/"
	if fs.writeBack != nil {
		for p := range fs.writeBack.ListPendingPaths() {
			if strings.HasPrefix(p, prefix) {
				if fs.uploader != nil {
					waitStart := time.Now()
					fs.debugf("rmdir wait writeback start path=%s child=%s", childP, p)
					fs.uploader.WaitPath(p)
					fs.debugDurationf(waitStart, 0, "rmdir wait writeback done path=%s child=%s", childP, p)
				}
				fs.writeBack.Remove(p)
			}
		}
	}
	// Cancel commitQueue entries for the subtree so background commits
	// cannot resurrect deleted files. CancelPrefix handles shadow+index cleanup.
	if fs.commitQueue != nil {
		cancelStart := time.Now()
		fs.debugf("rmdir cancel commit prefix start path=%s prefix=%s", childP, prefix)
		fs.commitQueue.CancelPrefix(prefix)
		fs.debugDurationf(cancelStart, 0, "rmdir cancel commit prefix done path=%s prefix=%s", childP, prefix)
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
			fh.ShadowSpill = true
		}
	}

	if fh.ShadowSpill {
		// ShadowSpill mode: shadow is the authoritative data source.
		// OnPartFull evicts memory immediately — no StreamUploader needed.
		wb.OnPartFull = func(partIdx int, data []byte) {
			wb.EvictPart(partIdx)
		}
	} else {
		// Normal mode: attach streaming uploader for sequential write streaming.
		fh.Streamer = NewStreamUploader(fs.client, childP, expectedRevisionForHandle(fh))
		streamer := fh.Streamer
		wb.OnPartFull = func(partIdx int, data []byte) {
			partNum := partIdx + 1
			if err := streamer.SubmitPart(context.Background(), partNum, data, nil); err != nil {
				log.Printf("streaming submit part %d failed for %s: %v", partNum, childP, err)
			}
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
					fh.ShadowSpill = true
					// Pin shadow so commit queue cleanup doesn't delete it while
					// this handle is reading.
					fh.ShadowGen = fs.shadowStore.Pin(p)
					fh.ShadowPinned = true
				}
			}

			if fh.ShadowSpill {
				// ShadowSpill mode: shadow is the authoritative data source.
				wb := fh.Dirty
				wb.OnPartFull = func(partIdx int, data []byte) {
					wb.EvictPart(partIdx)
				}
			} else {
				// Normal mode: attach streaming uploader with OnPartFull wiring.
				fh.Streamer = NewStreamUploader(fs.client, p, expectedRevisionForHandle(fh))
				streamer := fh.Streamer
				filePath := p
				fh.Dirty.OnPartFull = func(partIdx int, data []byte) {
					partNum := partIdx + 1
					if err := streamer.SubmitPart(context.Background(), partNum, data, nil); err != nil {
						log.Printf("streaming submit part %d failed for %s: %v", partNum, filePath, err)
					}
				}
			}
		}
	}

	// Set up read prefetcher for read-only opens on large files.
	if fh.Dirty == nil {
		entry, _ := fs.inodes.GetEntry(input.NodeId)
		if entry != nil && entry.Size > smallFileThreshold {
			fh.Prefetch = NewPrefetcher(fs.client, p, entry.Size, fs.debugEnabled())
		}
		// Atomically pin shadow for read-only opens so commit queue cleanup
		// doesn't delete the shadow file while this handle is reading from it.
		// PinIfExists avoids a TOCTOU race between Has() and Pin().
		if !fh.ShadowPinned && fs.shadowStore != nil {
			if gen, ok := fs.shadowStore.PinIfExists(p); ok {
				fh.ShadowGen = gen
				fh.ShadowPinned = true
			}
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
	} else if fh.Prefetch != nil {
		// Large read-only files with prefetcher: use DIRECT_IO so every
		// read goes through our Read handler (no kernel page cache).
		// The prefetcher provides its own caching layer.
		out.OpenFlags = gofuse.FOPEN_DIRECT_IO
	} else {
		out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
	}
	fs.debugf("open path=%s fh=%d ino=%d flags=0x%x open_flags=%d dirty=%t prefetch=%t orig_size=%d base_rev=%d shadow_ready=%t shadow_spill=%t", p, out.Fh, fh.Ino, input.Flags, out.OpenFlags, fh.Dirty != nil, fh.Prefetch != nil, fh.OrigSize, fh.BaseRev, fh.ShadowReady, fh.ShadowSpill)
	return gofuse.OK
}

func (fs *Dat9FS) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (result gofuse.ReadResult, status gofuse.Status) {
	start := time.Now()
	logPath := ""
	var logIno uint64
	source := "unknown"
	bytesRead := -1
	defer func() {
		if !fs.debugEnabled() {
			return
		}
		d := time.Since(start)
		if status == gofuse.OK && d < fuseDebugSlowReadThreshold {
			return
		}
		fs.debugf("read path=%s fh=%d ino=%d off=%d req=%d got=%d source=%s status=%d dur=%s", logPath, input.Fh, logIno, input.Offset, input.Size, bytesRead, source, status, d)
	}()

	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		source = "missing-handle"
		return nil, gofuse.ENOENT
	}
	logPath = fh.Path
	logIno = fh.Ino

	lockStart := time.Now()
	fh.Lock()
	if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
		fs.debugf("read lock wait path=%s fh=%d ino=%d wait=%s", fh.Path, input.Fh, fh.Ino, lockWait)
	}

	// ShadowSpill: read from shadow file (the authoritative data source).
	// Dirty has evicted parts so ReadAt would return incomplete data.
	if fh.ShadowSpill && fs.shadowStore != nil {
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			source = "shadow-spill-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}
		fh.Unlock()
		result := make([]byte, end-offset)
		n, err := fs.shadowStore.ReadAt(fh.Path, offset, result)
		if err != nil {
			source = "shadow-spill-error"
			return nil, gofuse.EIO
		}
		source = "shadow-spill"
		bytesRead = n
		return gofuse.ReadResultData(result[:n]), gofuse.OK
	}

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
			source = "dirty-eof"
			bytesRead = 0
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
						source = "dirty-load-error"
						return nil, gofuse.EIO
					}
				}
			}

			result := make([]byte, end-offset)
			fh.Dirty.ReadAt(offset, result)
			fh.Unlock()
			source = "dirty-buffer"
			bytesRead = len(result)
			return gofuse.ReadResultData(result), gofuse.OK
		}
		// touchesEvicted: for new files (remoteSize == 0), the multipart
		// upload has not been completed yet — the object doesn't exist on the
		// server, so ReadStreamRange would fail. Return EIO; sequential writers
		// (cp, dd, ffmpeg) never read back evicted data in practice.
		if fh.Dirty.remoteSize == 0 {
			fh.Unlock()
			source = "dirty-evicted-new"
			return nil, gofuse.EIO
		}
		// Existing file with evicted parts: the original object still exists
		// on the server, so fall through to ReadStreamRange.
		source = "dirty-evicted-remote"
		fh.Unlock()
	} else if fh.Dirty != nil && fh.ShadowReady {
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			source = "dirty-shadow-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}
		result := make([]byte, end-offset)
		fh.Dirty.ReadAt(offset, result)
		fh.Unlock()
		source = "dirty-shadow"
		bytesRead = len(result)
		return gofuse.ReadResultData(result), gofuse.OK
	} else if fh.Dirty != nil && fh.Dirty.Size() > 0 && !fh.Dirty.HasDirtyParts() {
		// Writable handle with lazy-loaded buffer (no dirty parts yet) —
		// read directly from the server for unloaded parts.
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			source = "dirty-clean-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		source = "dirty-clean-remote"
		fh.Unlock()
		// Fall through to server read below
	} else {
		fh.Unlock()
	}

	// Read path priority for pending files:
	// 1. ShadowStore (local SSD) — for files staged by Flush
	// 2. WriteBackCache.Get (local disk, full file) — legacy path
	//
	// If the handle holds a generation token (ShadowGen != 0), try the
	// generation-based read first — this works even after the shadow has
	// been retired by commit queue cleanup. Otherwise use path-based ReadAt.
	if fh.Dirty == nil && fs.shadowStore != nil {
		var sz int64 = -1
		var useGen bool
		if fh.ShadowGen != 0 {
			sz = fs.shadowStore.SizeGen(fh.ShadowGen)
			useGen = sz >= 0
		}
		if !useGen {
			if fs.shadowStore.Has(fh.Path) {
				sz = fs.shadowStore.Size(fh.Path)
			}
		}
		if sz >= 0 {
			offset := int64(input.Offset)
			if offset >= sz {
				source = "shadow-store-eof"
				bytesRead = 0
				return gofuse.ReadResultData(nil), gofuse.OK
			}
			end := offset + int64(input.Size)
			if end > sz {
				end = sz
			}
			buf := make([]byte, end-offset)
			var n int
			var err error
			if useGen {
				n, err = fs.shadowStore.ReadAtGen(fh.ShadowGen, offset, buf)
			} else {
				n, err = fs.shadowStore.ReadAt(fh.Path, offset, buf)
			}
			if err == nil && n > 0 {
				source = "shadow-store"
				bytesRead = n
				return gofuse.ReadResultData(buf[:n]), gofuse.OK
			}
			if err != nil {
				fs.debugf("read shadow-store miss path=%s off=%d req=%d gen=%d err=%v", fh.Path, input.Offset, input.Size, fh.ShadowGen, err)
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
				source = "writeback-cache-eof"
				bytesRead = 0
				return gofuse.ReadResultData(nil), gofuse.OK
			}
			end := offset + int64(input.Size)
			if end > int64(len(wbData)) {
				end = int64(len(wbData))
			}
			source = "writeback-cache"
			bytesRead = int(end - offset)
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
			source = "prefetch-hit"
			bytesRead = len(data)
			return gofuse.ReadResultData(data), gofuse.OK
		}
		// Cache miss — fall through to direct read. Prefetch is triggered
		// only after a successful read (see below), not unconditionally.
		source = "prefetch-miss-range"
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
				source = "read-cache-eof"
				bytesRead = 0
				return gofuse.ReadResultData(nil), gofuse.OK
			}
			end := offset + int64(input.Size)
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			source = "read-cache-hit"
			bytesRead = int(end - offset)
			return gofuse.ReadResultData(data[offset:end]), gofuse.OK
		}

		// Cache miss: read the file and store it. No separate Stat needed —
		// ReadCtx fetches the data in one round-trip. Uses detached retry
		// so a single FUSE interrupt doesn't permanently return EAGAIN.
		data, err := fs.readSmallFileWithRetry(ctx, p)
		if err != nil {
			source = "small-read-error"
			if errors.Is(err, errReadRetriesExhausted) {
				return nil, gofuse.EIO
			}
			return nil, httpToFuseStatus(err)
		}
		// Store with the revision from the prior Stat/Lookup.
		fs.readCache.Put(p, data, cacheRev)
		offset := int64(input.Offset)
		if offset >= int64(len(data)) {
			source = "small-read-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		source = "small-read"
		bytesRead = int(end - offset)
		return gofuse.ReadResultData(data[offset:end]), gofuse.OK
	}

	// Large file or unknown size: range read (avoids O(offset) discard).
	// Uses detached retry so a single FUSE interrupt / transient error
	// doesn't permanently return EAGAIN to the caller.
	if source == "unknown" {
		source = "range-read"
	}
	rangeStart := time.Now()
	data, n, err := fs.readStreamRangeWithRetry(ctx, p, int64(input.Offset), int64(input.Size))
	if err != nil {
		fs.debugf("read range error path=%s off=%d req=%d got=%d source=%s dur=%s err=%v", p, input.Offset, input.Size, n, source, time.Since(rangeStart), err)
		if errors.Is(err, errReadRetriesExhausted) {
			return nil, gofuse.EIO
		}
		return nil, httpToFuseStatus(err)
	}
	if fs.debugEnabled() && time.Since(rangeStart) >= fuseDebugSlowReadThreshold {
		fs.debugf("read range done path=%s off=%d req=%d got=%d source=%s dur=%s", p, input.Offset, input.Size, n, source, time.Since(rangeStart))
	}
	bytesRead = n
	if fh.Prefetch != nil {
		fh.Prefetch.OnRead(int64(input.Offset), n)
	}
	return gofuse.ReadResultData(data), gofuse.OK
}

func (fs *Dat9FS) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (written uint32, status gofuse.Status) {
	start := time.Now()
	logPath := ""
	var logIno uint64
	source := "start"
	defer func() {
		if !fs.debugEnabled() {
			return
		}
		d := time.Since(start)
		if status == gofuse.OK && d < fuseDebugSlowOpThreshold {
			return
		}
		fs.debugf("write path=%s fh=%d ino=%d off=%d req=%d wrote=%d source=%s status=%d dur=%s", logPath, input.Fh, logIno, input.Offset, len(data), written, source, status, d)
	}()
	if fs.opts.ReadOnly {
		source = "readonly"
		return 0, gofuse.EROFS
	}

	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		source = "missing-handle"
		return 0, gofuse.ENOENT
	}
	logPath = fh.Path
	logIno = fh.Ino

	lockStart := time.Now()
	fh.Lock()
	if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
		fs.debugf("write lock wait path=%s fh=%d ino=%d wait=%s", fh.Path, input.Fh, fh.Ino, lockWait)
	}
	defer fh.Unlock()

	if fh.Dirty == nil {
		source = "new-dirty-buffer"
		fh.Dirty = NewWriteBuffer(fh.Path, 0, 0)
	}

	// ShadowSpill: write shadow FIRST, before Dirty. If shadow fails, return
	// EIO without touching Dirty — OnPartFull may evict the part, so writing
	// Dirty first could lose data if shadow then fails.
	if fh.ShadowSpill && fh.ShadowReady && fs.shadowStore != nil {
		if !fs.shadowStore.CheckDiskSpaceThrottled() {
			log.Printf("shadow write rejected for %s: disk space below watermark", fh.Path)
			source = "shadow-spill-nospace"
			return 0, gofuse.Status(syscall.ENOSPC)
		}
		shadowStart := time.Now()
		if _, err := fs.shadowStore.WriteAt(fh.Path, int64(input.Offset), data, fh.BaseRev); err != nil {
			log.Printf("shadow write failed for ShadowSpill %s: %v", fh.Path, err)
			source = "shadow-spill-error"
			return 0, gofuse.EIO
		}
		if fs.debugEnabled() && time.Since(shadowStart) >= fuseDebugSlowOpThreshold {
			fs.debugf("write shadow-spill done path=%s off=%d size=%d dur=%s", fh.Path, input.Offset, len(data), time.Since(shadowStart))
		}
		source = "shadow-spill"
	}

	n, err := fh.Dirty.Write(int64(input.Offset), data)
	if err != nil {
		source = "dirty-write-error"
		return 0, gofuse.Status(syscall.EFBIG)
	}
	written = n

	// Non-ShadowSpill: write-through to shadow after Dirty (best-effort).
	if !fh.ShadowSpill && fh.ShadowReady && fs.shadowStore != nil {
		shadowStart := time.Now()
		if _, err := fs.shadowStore.WriteAt(fh.Path, int64(input.Offset), data, fh.BaseRev); err != nil {
			log.Printf("shadow write-through failed for %s: %v", fh.Path, err)
			fs.shadowStore.Remove(fh.Path)
			fh.ShadowReady = false
			source = "shadow-through-error"
		} else {
			source = "shadow-through"
		}
		if fs.debugEnabled() && time.Since(shadowStart) >= fuseDebugSlowOpThreshold {
			fs.debugf("write shadow-through done path=%s off=%d size=%d dur=%s", fh.Path, input.Offset, len(data), time.Since(shadowStart))
		}
	}
	if source == "start" || source == "new-dirty-buffer" {
		source = "dirty-buffer"
	}
	fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
	fs.inodes.UpdateSize(fh.Ino, fh.Dirty.Size())
	return n, gofuse.OK
}

func (fs *Dat9FS) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) (status gofuse.Status) {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return gofuse.OK
	}

	start := time.Now()
	phase := "start"
	fs.debugf("flush start path=%s fh=%d ino=%d", fh.Path, input.Fh, fh.Ino)
	lockStart := time.Now()
	fh.Lock()
	if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
		fs.debugf("flush lock wait path=%s fh=%d ino=%d wait=%s", fh.Path, input.Fh, fh.Ino, lockWait)
	}
	defer fh.Unlock()
	defer func() {
		if !fs.debugEnabled() {
			return
		}
		var size int64
		dirty := false
		if fh.Dirty != nil {
			size = fh.Dirty.Size()
			dirty = fh.Dirty.HasDirtyParts()
		}
		d := time.Since(start)
		if status == gofuse.OK && d < fuseDebugSlowOpThreshold {
			return
		}
		fs.debugf("flush done path=%s fh=%d ino=%d phase=%s size=%d dirty=%t shadow_ready=%t shadow_spill=%t status=%d dur=%s", fh.Path, input.Fh, fh.Ino, phase, size, dirty, fh.ShadowReady, fh.ShadowSpill, status, d)
	}()

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
			phase = "writeback-same-seq"
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
					phase = "small-stage-shadow"
					stageStart := time.Now()
					fs.debugf("flush stage shadow start path=%s size=%d durable=true", fh.Path, size)
					err := fs.stageShadowLocked(fh, true)
					fs.debugDurationf(stageStart, 0, "flush stage shadow done path=%s size=%d err=%v", fh.Path, size, err)
					if err != nil {
						log.Printf("shadow stage failed for %s: %v, falling through", fh.Path, err)
					} else {
						phase = "small-snapshot-writeback"
						if err := fs.snapshotWriteBackLocked(fh); err != nil && fs.writeBack != nil {
							log.Printf("writeback snapshot failed for %s: %v", fh.Path, err)
						}
						fh.WriteBackSeq = fh.DirtySeq
						return gofuse.OK
					}
				}

				phase = "small-snapshot-writeback"
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

	// Large file path. Returning OK here without persisting the file would
	// break close→drop_caches→open: the kernel re-issues Lookup, which falls
	// through to a remote stat that has not yet seen the upload, returning
	// ENOENT (juicefs bench reproduces this).
	//
	// Two strategies, depending on sync mode:
	//
	//   • SyncInteractive: stage the buffer to the local shadow store + journal
	//     (durable on the host) and register it in pendingIndex so subsequent
	//     Lookups hit the in-memory overlay. Release will pick this up via
	//     its write-back cache fast path and enqueue the actual server upload
	//     into the CommitQueue. close(2) is fast; remote durability is async.
	//
	//   • SyncStrict (or interactive fall-through on stage failure): block in
	//     Flush until the upload completes. Use a size-proportional timeout
	//     (releaseTimeout) instead of the 30s fuseCtx — large uploads need it.
	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() && fh.Dirty.Size() >= writeBackThreshold {
		// ShadowSpill interactive path: stage shadow journal + set ShadowCommitReady.
		// Does NOT use snapshotWriteBackLocked or WriteBackSeq — those assume
		// writeBack cache holds complete file data, which ShadowSpill does not.
		if fh.ShadowSpill && fs.syncMode == SyncInteractive && fs.shadowStore != nil && fs.pendingIndex != nil {
			phase = "large-shadowspill-stage"
			size := fh.Dirty.Size()
			stageStart := time.Now()
			fs.debugf("flush shadowspill stage start path=%s size=%d durable=true", fh.Path, size)
			err := fs.stageShadowLocked(fh, true)
			fs.debugDurationf(stageStart, 0, "flush shadowspill stage done path=%s size=%d err=%v", fh.Path, size, err)
			if err != nil {
				log.Printf("flush: shadow stage failed for ShadowSpill %s (size=%d): %v, falling through to sync upload", fh.Path, fh.Dirty.Size(), err)
			} else {
				fh.ShadowCommitReady = true
				return gofuse.OK
			}
		}

		// ShadowSpill strict path: synchronous streaming upload from shadow.
		if fh.ShadowSpill {
			size := fh.Dirty.Size()
			ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(size))
			defer cf()
			phase = "large-shadowspill-sync-upload"
			uploadStart := time.Now()
			fs.debugf("flush shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
			fh.Unlock()
			err := uploadFromShadow(ctx, fs.client, fs.shadowStore, fh.Path, expectedRevisionForHandle(fh))
			fh.Lock()
			fs.debugDurationf(uploadStart, 0, "flush shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
			if err != nil {
				log.Printf("flush: ShadowSpill sync upload failed for %s: %v", fh.Path, err)
				return gofuse.EIO
			}
			fh.Dirty.ClearDirty()
			fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			fh.DirtySeq = 0
			return gofuse.OK
		}

		if fs.syncMode == SyncInteractive && fs.shadowStore != nil && fs.pendingIndex != nil {
			if fs.canStageShadowFastLocked(fh) || fh.Dirty.CanMaterializeFull() {
				phase = "large-stage-shadow"
				size := fh.Dirty.Size()
				stageStart := time.Now()
				fs.debugf("flush stage shadow start path=%s size=%d durable=true", fh.Path, size)
				err := fs.stageShadowLocked(fh, true)
				fs.debugDurationf(stageStart, 0, "flush stage shadow done path=%s size=%d err=%v", fh.Path, size, err)
				if err != nil {
					log.Printf("flush: shadow stage failed for %s (size=%d): %v, falling through to sync upload", fh.Path, fh.Dirty.Size(), err)
				} else {
					phase = "large-snapshot-writeback"
					if err := fs.snapshotWriteBackLocked(fh); err != nil && fs.writeBack != nil {
						log.Printf("flush: writeback snapshot failed for %s: %v", fh.Path, err)
					}
					fh.WriteBackSeq = fh.DirtySeq
					// If a streaming upload was already in flight, abandon it:
					// the CommitQueue (driven by Release via the cache fast
					// path) will read from the shadow file instead. Without
					// this, Release sees streamerActive and falls through to
					// a synchronous re-upload, defeating the whole point.
					if fh.Streamer != nil && fh.Streamer.Started() {
						fh.Streamer.Abort()
						fh.Streamer = nil
					}
					return gofuse.OK
				}
			}
		}

		// Strict mode (or interactive fall-through): synchronous upload with
		// a size-aware timeout. Must NOT debounce — debounce returns OK and
		// uploads asynchronously, which would re-introduce the same bug.
		size := fh.Dirty.Size()
		ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(size))
		defer cf()
		phase = "large-sync-flush"
		fs.debugf("flush sync upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
		return fs.flushHandle(ctx, fh)
	}

	ctx, cf := fuseCtx(cancel)
	defer cf()

	phase = "debounced-or-sync-flush"
	return fs.flushHandleDebounced(ctx, fh, false)
}

func (fs *Dat9FS) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) (status gofuse.Status) {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return gofuse.OK
	}

	start := time.Now()
	phase := "start"
	fs.debugf("fsync start path=%s fh=%d ino=%d", fh.Path, input.Fh, fh.Ino)
	lockStart := time.Now()
	fh.Lock()
	if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
		fs.debugf("fsync lock wait path=%s fh=%d ino=%d wait=%s", fh.Path, input.Fh, fh.Ino, lockWait)
	}
	defer fh.Unlock()
	defer func() {
		if !fs.debugEnabled() {
			return
		}
		var size int64
		dirty := false
		if fh.Dirty != nil {
			size = fh.Dirty.Size()
			dirty = fh.Dirty.HasDirtyParts()
		}
		d := time.Since(start)
		if status == gofuse.OK && d < fuseDebugSlowOpThreshold {
			return
		}
		fs.debugf("fsync done path=%s fh=%d ino=%d phase=%s size=%d dirty=%t shadow_spill=%t status=%d dur=%s", fh.Path, input.Fh, fh.Ino, phase, size, dirty, fh.ShadowSpill, status, d)
	}()

	// Interactive mode: Fsync = local durable only. Shadow file + journal
	// ensure crash safety. Remote commit happens asynchronously.
	if fs.syncMode == SyncInteractive {
		if fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
			phase = "interactive-clean"
			return gofuse.OK
		}
		if fh.ShadowSpill {
			// ShadowSpill: stage shadow + journal, no writeBack snapshot.
			phase = "interactive-shadowspill-stage"
			stageStart := time.Now()
			err := fs.stageShadowLocked(fh, true)
			fs.debugDurationf(stageStart, 0, "fsync shadowspill stage done path=%s err=%v", fh.Path, err)
			if err == nil {
				fh.ShadowCommitReady = true
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
		} else {
			phase = "interactive-stage"
			stageStart := time.Now()
			err := fs.stageShadowLocked(fh, true)
			fs.debugDurationf(stageStart, 0, "fsync stage done path=%s err=%v", fh.Path, err)
			if err == nil {
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
	}

	// ShadowSpill strict: synchronous streaming upload from shadow.
	if fh.ShadowSpill && fs.shadowStore != nil {
		size := fh.Dirty.Size()
		ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(size))
		defer cf()
		phase = "shadowspill-sync-upload"
		uploadStart := time.Now()
		fs.debugf("fsync shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
		fh.Unlock()
		err := uploadFromShadow(ctx, fs.client, fs.shadowStore, fh.Path, expectedRevisionForHandle(fh))
		fh.Lock()
		fs.debugDurationf(uploadStart, 0, "fsync shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
		if err != nil {
			log.Printf("fsync: ShadowSpill sync upload failed for %s: %v", fh.Path, err)
			return gofuse.EIO
		}
		if fh.Dirty != nil {
			fh.Dirty.ClearDirty()
			fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			fh.DirtySeq = 0
		}
		return gofuse.OK
	}

	// Strict mode: Fsync = remote durable. Upload to server before returning.
	ctx, cf := fuseCtx(cancel)
	defer cf()

	if fs.writeBack != nil && fs.uploader != nil && fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq {
		// Snapshot matches current dirty state — safe to upload.
		phase = "writeback-upload-sync"
		uploadStart := time.Now()
		fs.debugf("fsync writeback upload start path=%s", fh.Path)
		err := fs.uploader.UploadSync(ctx, fh.Path)
		fs.debugDurationf(uploadStart, 0, "fsync writeback upload done path=%s err=%v", fh.Path, err)
		if err != nil {
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
		phase = "writeback-stale"
		fs.writeBack.Remove(fh.Path)
		fh.WriteBackSeq = 0
	}

	phase = "flush-debounced-force"
	return fs.flushHandleDebounced(ctx, fh, true)
}

func (fs *Dat9FS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if ok {
		// Unpin shadow if this handle pinned it, so deferred removals can proceed.
		if fh.ShadowPinned && fs.shadowStore != nil {
			defer fs.shadowStore.Unpin(fh.ShadowGen)
		}

		start := time.Now()
		phase := "start"
		flushStatus := gofuse.OK
		fs.debugf("release start path=%s fh=%d ino=%d", fh.Path, input.Fh, fh.Ino)
		defer func() {
			if !fs.debugEnabled() {
				return
			}
			var size int64
			dirty := false
			if fh.Dirty != nil {
				size = fh.Dirty.Size()
				dirty = fh.Dirty.HasDirtyParts()
			}
			d := time.Since(start)
			if flushStatus == gofuse.OK && d < fuseDebugSlowOpThreshold {
				return
			}
			fs.debugf("release done path=%s fh=%d ino=%d phase=%s size=%d dirty=%t shadow_ready=%t shadow_spill=%t status=%d dur=%s", fh.Path, input.Fh, fh.Ino, phase, size, dirty, fh.ShadowReady, fh.ShadowSpill, flushStatus, d)
		}()
		// Cancel any pending debounce for this path — Release always flushes immediately.
		phase = "cancel-debounce"
		fs.debouncer.Cancel(fh.Path)

		// ShadowSpill Release: CommitQueue streaming from shadow, no writeBack.
		if fh.ShadowSpill && fh.ShadowCommitReady && fs.commitQueue != nil && fs.shadowStore != nil {
			phase = "shadowspill-commit"
			lockStart := time.Now()
			fh.Lock()
			if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
				fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
			}
			fh.Dirty.ClearDirty()
			fs.clearDirtySize(fh.Ino, fh.DirtySeq)
			size := fh.Dirty.Size()
			fh.DirtySeq = 0
			fh.ShadowCommitReady = false
			fh.Unlock()

			entry := &CommitEntry{
				Path:        fh.Path,
				Inode:       fh.Ino,
				BaseRev:     fh.BaseRev,
				Size:        size,
				Kind:        PendingOverwrite,
				ShadowSpill: true,
			}
			if fh.IsNew {
				entry.Kind = PendingNew
			}
			enqueueStart := time.Now()
			fs.debugf("release commit enqueue start path=%s size=%d shadow_spill=true", fh.Path, size)
			err := fs.commitQueue.Enqueue(entry)
			fs.debugDurationf(enqueueStart, 0, "release commit enqueue done path=%s size=%d err=%v", fh.Path, size, err)
			if err != nil {
				// Fallback: synchronous streaming upload from shadow.
				// Do NOT use uploader.Submit — it reads from writeBack cache.
				log.Printf("release: ShadowSpill commitQueue enqueue failed for %s: %v, falling back to sync upload", fh.Path, err)
				ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(size))
				phase = "shadowspill-sync-upload"
				uploadStart := time.Now()
				fs.debugf("release shadowspill upload start path=%s size=%d timeout=%s", fh.Path, size, releaseTimeout(size))
				uploadErr := uploadFromShadow(ctx, fs.client, fs.shadowStore, fh.Path, expectedRevisionForHandle(fh))
				fs.debugDurationf(uploadStart, 0, "release shadowspill upload done path=%s size=%d err=%v", fh.Path, size, uploadErr)
				if uploadErr != nil {
					flushStatus = gofuse.EIO
					log.Printf("release: ShadowSpill sync upload failed for %s: %v", fh.Path, uploadErr)
				}
				cf()
			}

			fs.readCache.Invalidate(fh.Path)
			fs.dirCache.Invalidate(parentDir(fh.Path))
			fs.notifyInode(fh.Ino)
			parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
			fs.notifyInode(parentIno)
			if fh.Prefetch != nil {
				fh.Prefetch.Close()
			}
			fs.fileHandles.Delete(input.Fh)
			return
		}

		// Check if Flush already wrote this file to the write-back cache
		// AND no new writes have happened since. If the DirtySeq changed,
		// the cache snapshot is stale — fall through to synchronous upload
		// which will upload the latest buffer data.
		if fs.writeBack != nil && fs.uploader != nil {
			phase = "writeback-check"
			lockStart := time.Now()
			fh.Lock()
			if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
				fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
			}
			// If parts were submitted to the streaming uploader during Write,
			// they've been evicted from the WriteBuffer. The write-back /
			// commit-queue paths would miss those parts. Force the
			// synchronous flush path so FinishStreaming uploads the
			// buffered parts with the correct total size.
			streamerActive := fh.Streamer != nil && fh.Streamer.Started()
			canUseCache := !streamerActive && fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq
			fs.debugf("release writeback check path=%s streamer_active=%t writeback_seq=%d dirty_seq=%d can_use_cache=%t", fh.Path, streamerActive, fh.WriteBackSeq, fh.DirtySeq, canUseCache)
			if canUseCache {
				phase = "writeback-cache-release"
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
					enqueueStart := time.Now()
					fs.debugf("release commit enqueue start path=%s size=%d shadow_spill=false", fh.Path, entry.Size)
					err := fs.commitQueue.Enqueue(entry)
					fs.debugDurationf(enqueueStart, 0, "release commit enqueue done path=%s size=%d err=%v", fh.Path, entry.Size, err)
					if err != nil {
						// Backpressure — fall back to legacy uploader.
						fs.debugf("release uploader submit fallback path=%s", fh.Path)
						fs.uploader.Submit(fh.Path)
					} else {
						// CommitQueue owns the upload via shadow; remove the
						// writeBack .dat/.meta snapshot so it doesn't leak or
						// serve stale data to Lookup/Read.
						fs.writeBack.Remove(fh.Path)
					}
				} else {
					// Async upload — the uploader will read from cache and upload.
					fs.debugf("release uploader submit path=%s", fh.Path)
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
				fs.debugf("release stale writeback remove path=%s writeback_seq=%d dirty_seq=%d", fh.Path, fh.WriteBackSeq, fh.DirtySeq)
				fs.writeBack.Remove(fh.Path)
				fh.WriteBackSeq = 0
			}
			fh.Unlock()
		}

		// Normal path: synchronous upload in Release.
		// Timeout scales with file size so large uploads don't get killed.
		phase = "sync-flush"
		lockStart := time.Now()
		fh.Lock()
		if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
			fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
		}
		var flushSize int64
		if fh.Dirty != nil {
			flushSize = fh.Dirty.Size()
		}
		ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(flushSize))
		flushStart := time.Now()
		fs.debugf("release sync flush start path=%s size=%d timeout=%s", fh.Path, flushSize, releaseTimeout(flushSize))
		st := fs.flushHandle(ctx, fh)
		fs.debugDurationf(flushStart, 0, "release sync flush done path=%s size=%d status=%d", fh.Path, flushSize, st)
		flushStatus = st
		cf()
		streamer := fh.Streamer
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		fh.Unlock()

		if st != gofuse.OK && streamer != nil {
			// Flush failed — abort the streaming upload to avoid orphaned
			// multipart uploads on S3. Called without fh.mu because Abort()
			// may perform network I/O.
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
		handle.Lock()
		if handle.Dirty == nil || handle.DirtySeq != snapshotSeq {
			handle.Unlock()
			return
		}

		dCtx, dCf := context.WithTimeout(context.Background(), fuseTimeout)
		committedRev, err := fs.client.WriteCtxConditionalWithRevision(dCtx, filePath, data, expectedRevision)
		dCf()
		if err != nil {
			handle.Unlock()
			log.Printf("debounced flush failed for %s: %v", filePath, err)
			return
		}
		// The handle stays locked across upload + finalize so concurrent writes
		// cannot advance live state for data outside this committed snapshot.
		if committedRev > 0 {
			handle.IsNew = false
			handle.BaseRev = committedRev
			fs.inodes.UpdateRevision(ino, committedRev)
			if handle.ZeroBase && handle.Dirty != nil && handle.Dirty.Size() > 0 {
				handle.ZeroBase = false
			}
		} else {
			fs.finalizeHandleFlushLocked(handle, expectedRevision)
		}
		if handle.Dirty != nil && handle.DirtySeq == snapshotSeq {
			handle.Dirty.ClearDirty()
		}
		handle.Unlock()
		if committedRev > 0 {
			fs.readCache.Put(filePath, data, committedRev)
		} else {
			fs.readCache.Invalidate(filePath)
		}
		fs.dirCache.Invalidate(parentDir(filePath))
		fs.inodes.UpdateSize(ino, int64(len(data)))
		fs.notifyInode(ino)
		parentIno, _ := fs.inodes.GetInode(parentDir(filePath))
		fs.notifyEntry(parentIno, path.Base(filePath))
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
func (fs *Dat9FS) flushHandle(ctx context.Context, fh *FileHandle) (status gofuse.Status) {
	start := time.Now()
	phase := "start"
	defer func() {
		if !fs.debugEnabled() {
			return
		}
		var size int64
		dirty := false
		if fh != nil && fh.Dirty != nil {
			size = fh.Dirty.Size()
			dirty = fh.Dirty.HasDirtyParts()
		}
		d := time.Since(start)
		if status == gofuse.OK && d < fuseDebugSlowOpThreshold {
			return
		}
		pathForLog := ""
		var ino uint64
		if fh != nil {
			pathForLog = fh.Path
			ino = fh.Ino
		}
		fs.debugf("flushHandle done path=%s ino=%d phase=%s size=%d dirty=%t status=%d dur=%s", pathForLog, ino, phase, size, dirty, status, d)
	}()
	if fh.Dirty == nil {
		phase = "no-dirty-buffer"
		return gofuse.OK
	}
	if !fh.Dirty.HasDirtyParts() {
		phase = "no-dirty-parts"
		return gofuse.OK
	}

	size := fh.Dirty.Size()

	var err error

	// Path 1a: Streaming mode — parts were submitted during Write() and are
	// buffered in the StreamUploader's pendingParts. We must finalize via
	// FinishStreaming (which initiates the server upload with the actual total
	// size and uploads from pendingParts), not Path 1b's UploadAll.
	if fh.Streamer != nil && fh.Streamer.Started() {
		phase = "finish-streaming"
		expectedRevision := fh.Streamer.ExpectedRevision()
		partSize := fh.Dirty.PartSize()
		numParts := int((size + partSize - 1) / partSize)
		lastPartNum := numParts // 1-based

		// Determine data for the last part.
		// If the file size is an exact multiple of partSize, the last part
		// was already submitted via SubmitPart — pass nil so FinishStreaming
		// uses the buffered copy from pendingParts.
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

		// Release fh.mu before network calls — FinishStreaming does
		// synchronous uploads that may take minutes.
		uploadStart := time.Now()
		fs.debugf("flushHandle finish streaming start path=%s size=%d part_size=%d parts=%d dirty_parts=%d expected_rev=%d", fh.Path, size, partSize, numParts, len(dirtyParts), expectedRevision)
		fh.Unlock()
		err = streamer.FinishStreaming(ctx, size,
			lastPartNum, lastCp, dirtyParts)
		fh.Lock()
		fs.debugDurationf(uploadStart, 0, "flushHandle finish streaming done path=%s size=%d err=%v", fh.Path, size, err)

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
		// Remove stale shadow so subsequent read-only opens don't serve
		// the empty placeholder created at Create/Open time.
		if fs.shadowStore != nil {
			fs.shadowStore.Remove(fh.Path)
		}
		if fs.pendingIndex != nil {
			fs.pendingIndex.Remove(fh.Path)
		}
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
		fs.notifyInode(fh.Ino)
		parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
		fs.notifyInode(parentIno)
		return gofuse.OK
	}

	// Path 1b: Large new file with streaming uploader but no streaming parts
	// (non-sequential writes) — upload all parts in parallel at flush time.
	if fh.Streamer != nil && size >= smallFileThreshold {
		phase = "upload-all"
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
		uploadStart := time.Now()
		fs.debugf("flushHandle upload all start path=%s size=%d parts=%d expected_rev=%d", fh.Path, size, len(partSnapshots), expectedRevision)
		fh.Unlock()
		err = streamer.UploadAll(ctx, size, partSnapshots)
		fh.Lock()
		fs.debugDurationf(uploadStart, 0, "flushHandle upload all done path=%s size=%d parts=%d err=%v", fh.Path, size, len(partSnapshots), err)

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
		// Remove stale shadow (same reason as Path 1a above).
		if fs.shadowStore != nil {
			fs.shadowStore.Remove(fh.Path)
		}
		if fs.pendingIndex != nil {
			fs.pendingIndex.Remove(fh.Path)
		}
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
		fs.notifyInode(fh.Ino)
		parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
		fs.notifyInode(parentIno)
		return gofuse.OK
	}

	// Path 2: No streaming uploader or small file — materialize all data for upload.
	data := fh.Dirty.Bytes()
	expectedRevision := expectedRevisionForHandle(fh)
	var committedRev int64

	if size < smallFileThreshold {
		// Small file: direct PUT with revision return for freshness seeding.
		phase = "small-write"
		writeStart := time.Now()
		fs.debugf("flushHandle small write start path=%s size=%d expected_rev=%d", fh.Path, size, expectedRevision)
		committedRev, err = fs.client.WriteCtxConditionalWithRevision(ctx, fh.Path, data, expectedRevision)
		fs.debugDurationf(writeStart, 0, "flushHandle small write done path=%s size=%d committed_rev=%d err=%v", fh.Path, size, committedRev, err)
	} else if fh.OrigSize >= smallFileThreshold {
		phase = "patch-file"
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
			patchStart := time.Now()
			fs.debugf("flushHandle patch start path=%s size=%d dirty_parts=%d expected_rev=%d", fh.Path, size, len(dirtyParts), expectedRevision)
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
			fs.debugDurationf(patchStart, 0, "flushHandle patch done path=%s size=%d dirty_parts=%d err=%v", fh.Path, size, len(dirtyParts), err)
		}
		// If no dirty parts, nothing changed — skip upload.
	} else {
		// New large file or small-to-large growth: full upload via multipart.
		phase = "write-stream"
		writeStart := time.Now()
		fs.debugf("flushHandle write stream start path=%s size=%d expected_rev=%d", fh.Path, size, expectedRevision)
		err = fs.client.WriteStreamConditional(
			ctx,
			fh.Path,
			bytes.NewReader(data),
			size,
			nil,
			expectedRevision,
		)
		fs.debugDurationf(writeStart, 0, "flushHandle write stream done path=%s size=%d err=%v", fh.Path, size, err)
	}
	if err != nil {
		log.Printf("flush upload failed for %s: %v", fh.Path, err)
		return httpToFuseStatus(err)
	}

	fh.Dirty.ClearDirty()
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	if committedRev > 0 {
		fs.readCache.Put(fh.Path, data, committedRev)
		fh.IsNew = false
		fh.BaseRev = committedRev
		fs.inodes.UpdateRevision(fh.Ino, committedRev)
		if fh.ZeroBase && fh.Dirty != nil && fh.Dirty.Size() > 0 {
			fh.ZeroBase = false
		}
	} else {
		fs.readCache.Invalidate(fh.Path)
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
	}
	fs.dirCache.Invalidate(parentDir(fh.Path))
	fs.inodes.UpdateSize(fh.Ino, size)
	// Invalidate kernel attr/data cache for this inode and parent dir listing.
	fs.notifyInode(fh.Ino)
	parentIno, _ := fs.inodes.GetInode(parentDir(fh.Path))
	fs.notifyEntry(parentIno, path.Base(fh.Path))
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
		// Per-handle timeout scaled by file size so large uploads complete.
		e.fh.Lock()
		var sz int64
		if e.fh.Dirty != nil {
			sz = e.fh.Dirty.Size()
		}
		ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(sz))
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

// onCommitQueueSuccess is called by the commit queue after a successful upload.
// It seeds readCache and updates inode revision when committedRev is available,
// or invalidates the cache otherwise.
func (fs *Dat9FS) onCommitQueueSuccess(entry *CommitEntry, committedRev int64) {
	if committedRev > 0 && entry.Inode > 0 {
		// Seed readCache from shadow data before the shadow file is removed.
		// Only attempt for files under the readCache size limit.
		if entry.Size < int64(smallFileThreshold) && fs.shadowStore != nil {
			if data, err := fs.shadowStore.ReadAll(entry.Path); err == nil {
				fs.readCache.Put(entry.Path, data, committedRev)
			}
		}
		fs.inodes.UpdateRevision(entry.Inode, committedRev)
		fs.inodes.UpdateSize(entry.Inode, entry.Size)
		fs.dirCache.Invalidate(parentDir(entry.Path))
		fs.notifyInode(entry.Inode)
		parentIno, _ := fs.inodes.GetInode(parentDir(entry.Path))
		fs.notifyEntry(parentIno, path.Base(entry.Path))
		fs.notifyInode(parentIno)
	} else {
		fs.readCache.Invalidate(entry.Path)
		fs.dirCache.Invalidate(parentDir(entry.Path))
	}
}

func (fs *Dat9FS) String() string {
	return "dat9"
}
