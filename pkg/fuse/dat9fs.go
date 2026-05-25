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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountpath"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

// Dat9FS implements the go-fuse RawFileSystem interface, bridging FUSE
// operations to the dat9 HTTP API via the Go SDK client.
type Dat9FS struct {
	gofuse.RawFileSystem

	client      *client.Client
	inodes      *InodeToPath
	fileHandles *HandleTable[*FileHandle]
	openHandles *OpenHandleIndex
	dirHandles  *HandleTable[*DirHandle]
	readCache   *ReadCache
	dirCache    *DirCache
	readSlots   chan struct{}
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
	// for external/SSE-driven changes that the kernel doesn't know about.
	// Local FUSE mutations avoid server notify because handlers can be running
	// on the same worker pool that services notify-triggered revalidation.
	server *gofuse.Server

	// notifyWg tracks inflight asynchronous kernel notification goroutines
	// (EntryNotify, InodeNotify). FlushAll waits on this to ensure all
	// notifications complete before shutdown.
	notifyWg sync.WaitGroup

	// notifyCount tracks total kernel notify calls (EntryNotify + InodeNotify)
	// for observability and testing. Incremented even when fs.server is nil.
	notifyCount atomic.Int64

	// lookupStatRetry* counters track only the Lookup->Stat retry path so
	// operators can distinguish absorbed interrupt noise from exhausted retries
	// on the primary probe route. GetAttr and list-fallback retries intentionally
	// reuse the retry logic without contributing to these counters.
	lookupStatRetryTotal     atomic.Uint64
	lookupStatRetrySuccess   atomic.Uint64
	lookupStatRetryExhausted atomic.Uint64

	// perf contains optional mount-level counters. Nil when disabled.
	perf *fusePerfCounters

	// smallFileMax mirrors the server's inline_threshold (fetched lazily via
	// the dat9 client). When 0, defaultSmallFileThreshold is used. Use the
	// inlineThreshold() accessor; do not read directly.
	smallFileMax atomic.Int64
}

// newWriteBuffer constructs a WriteBuffer with the small-file fast-path
// cutoff aligned to the negotiated server inline_threshold.
func (fs *Dat9FS) newWriteBuffer(path string, maxSize, partSize int64) *WriteBuffer {
	wb := NewWriteBuffer(path, maxSize, partSize)
	wb.SetSmallFileMax(fs.inlineThreshold())
	return wb
}

// inlineThreshold returns a small-file cutoff suitable for performance
// heuristics (read-cache prefetch sizing, debounce flush timing, write-
// buffer fast-path bounds). It mirrors the server's inline_threshold once
// /v1/status is observed, and falls back to defaultSmallFileThreshold so
// the heuristics still work in unit tests and pre-warm.
//
// Do NOT use this for the simple-PUT vs V2-multipart upload decision —
// callers there must use negotiatedInlineThreshold() so a missing server
// value forces multipart instead of guessing 50KB (which would break when
// the server is configured below 50KB).
func (fs *Dat9FS) inlineThreshold() int64 {
	if v := fs.negotiatedInlineThreshold(); v > 0 {
		return v
	}
	return defaultSmallFileThreshold
}

// negotiatedInlineThreshold returns the server-advertised inline_threshold
// or 0 when no value has been observed yet. Hot-path readers must not
// trigger network I/O; warmInlineThreshold is responsible for populating
// the cache before first use.
//
// Returns 0 means "unknown — caller must fall back to a multipart-safe
// behavior". The historical 50KB fallback is unsafe here: when the server
// is configured with DRIVE9_INLINE_THRESHOLD < 50000, files in
// [server_threshold, 50000) would be direct-PUT and rejected at the
// server's IsLargeFile gate with `missing X-Dat9-Part-Checksums`.
func (fs *Dat9FS) negotiatedInlineThreshold() int64 {
	if v := fs.smallFileMax.Load(); v > 0 {
		return v
	}
	if fs.client != nil {
		if t := fs.client.CachedSmallFileThreshold(); t > 0 {
			fs.smallFileMax.Store(t)
			return t
		}
	}
	return 0
}

// warmInlineThreshold triggers a one-shot /v1/status fetch via the client
// to populate the cached server inline_threshold. Idempotent and safe to
// call from FUSE startup; failures (e.g. older server) cache as zero so
// subsequent reads fall back to defaultSmallFileThreshold without retrying.
func (fs *Dat9FS) warmInlineThreshold(ctx context.Context) {
	if fs.client == nil {
		return
	}
	if t := fs.client.SmallFileThreshold(ctx); t > 0 {
		fs.smallFileMax.Store(t)
	}
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
		openHandles:   NewOpenHandleIndex(),
		dirHandles:    NewHandleTable[*DirHandle](),
		readCache:     NewReadCacheWithMaxFileSize(opts.CacheSize, 0, opts.ReadCacheMaxFileBytes),
		dirCache:      NewNamespaceCache(opts.DirTTL, opts.NegativeEntryTTL, defaultNamespaceCacheMaxEntries),
		readSlots:     make(chan struct{}, readConcurrencyOrDefault(opts.ReadConcurrency)),
		dirtyInodes:   make(map[uint64]dirtyInodeState),
		uid:           uint32(os.Getuid()),
		gid:           uint32(os.Getgid()),
		opts:          opts,
		debouncer:     newFlushDebouncer(opts.FlushDebounce),
		perf:          newFusePerfCounters(opts.PerfCounters),
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
	// Raised from 2 to 3 to tolerate E2B/high-latency environments where
	// concurrent git checkout triggers kernel FUSE interrupts on Lookup.
	lookupTransientRetryCount = 3

	// lookupTransientRetryTimeout is the timeout per detached retry. Set to
	// 2s (up from 250ms) so that retries can complete a full HTTP round-trip
	// in high-latency environments (E2B sandbox → drive9 server can be
	// 100-200ms RTT). The previous 250ms was barely above one RTT, leaving
	// almost no margin for server processing or network jitter.
	lookupTransientRetryTimeout = 2 * time.Second

	// lookupRetrySuccessLogEvery controls how often successful retry recovery is
	// logged, to avoid noisy logs on hot lookup paths.
	lookupRetrySuccessLogEvery uint64 = 200

	// namespaceMutationRetryTimeout bounds detached retries for idempotent-ish
	// namespace mutations after a FUSE interrupt or transient backend error.
	namespaceMutationRetryTimeout = 2 * time.Second

	// readTransientRetryCount is the number of detached retries after the
	// initial remote Read attempt fails with a transient error (context
	// canceled, deadline exceeded, network timeout, HTTP 5xx).
	readTransientRetryCount = 2

	// readTransientRetryTimeout keeps each detached read retry bounded.
	// Each retry reads at most max_read (1 MiB), so 2s is generous.
	readTransientRetryTimeout = 2 * time.Second

	// defaultRemoteReadConcurrency bounds backend read fan-out from one FUSE
	// mount. This protects Drive9/S3/TiDB from read floods; it does not reserve
	// kernel FUSE MaxBackground slots before go-fuse dispatches a request.
	defaultRemoteReadConcurrency = 24
)

func readConcurrencyOrDefault(n int) int {
	if n <= 0 {
		return defaultRemoteReadConcurrency
	}
	return n
}

func (fs *Dat9FS) acquireRemoteReadSlot(ctx context.Context) (func(), error) {
	if fs == nil || fs.readSlots == nil {
		return func() {}, nil
	}
	select {
	case fs.readSlots <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() { <-fs.readSlots })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

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
	if cancel == nil {
		return ctx, cf
	}
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

func (fs *Dat9FS) remoteRoot() string {
	if fs == nil || fs.opts == nil || fs.opts.RemoteRoot == "" {
		return "/"
	}
	return fs.opts.RemoteRoot
}

func (fs *Dat9FS) remotePath(localPath string) string {
	return mountpath.ToRemote(fs.remoteRoot(), localPath)
}

func (fs *Dat9FS) writePolicyForOpen(flags uint32) WritePolicy {
	policy := WritePolicyWriteBack
	if fs != nil && fs.opts != nil {
		policy = fs.opts.WritePolicy
	}
	if hasSyncOpenFlag(flags) {
		return WritePolicyWriteSync
	}
	return policy
}

func isGitLooseObjectFinalPath(p string) bool {
	const marker = "/.git/objects/"

	idx := strings.LastIndex(p, marker)
	if idx < 0 {
		return false
	}
	rel := p[idx+len(marker):]
	parts := strings.Split(rel, "/")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 38 {
		return false
	}
	for _, s := range parts {
		for _, r := range s {
			if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
				return false
			}
		}
	}
	return true
}

func (fs *Dat9FS) localPath(remotePath string) (string, bool) {
	return mountpath.ToLocal(fs.remoteRoot(), remotePath)
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
	if fh.WritePolicy != WritePolicyWriteSync && fs.shadowStore != nil && fs.pendingIndex != nil {
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
	for _, fh := range fs.openHandles.SnapshotPath(remotePath) {
		if fh == nil {
			continue
		}
		fh.Lock()
		dirty := fh.Dirty != nil
		fh.Unlock()
		if dirty {
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
	statStart := fs.perfStart()
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(fh.Path))
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
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
	fh.Dirty = fs.newWriteBuffer(fh.Path, bufMax, partSize)

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
	fh.Dirty.LoadPart = func(partNum int) ([]byte, error) {
		// WriteBuffer callers hold fh.mu. Resolve the path at load time so an
		// open handle renamed before lazy loading reads from the new remote path.
		filePath := fh.Path
		remoteFilePath := fs.remotePath(filePath)
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
		releaseReadSlot, err := fs.acquireRemoteReadSlot(lpCtx)
		if err != nil {
			return nil, err
		}
		defer releaseReadSlot()

		loadStart := time.Now()
		fs.debugf("dirty load part start path=%s part=%d off=%d len=%d", filePath, partNum, offset, length)
		rc, err := c.ReadStreamRange(lpCtx, remoteFilePath, offset, length)
		if err != nil {
			fs.perfRecordRemote(perfRemoteRead, loadStart, err, 0)
			fs.debugDurationf(loadStart, 0, "dirty load part open failed path=%s part=%d off=%d len=%d err=%v", filePath, partNum, offset, length, err)
			return nil, err
		}
		defer func() { _ = rc.Close() }()

		data, err := io.ReadAll(rc)
		fs.perfRecordRemote(perfRemoteRead, loadStart, err, uint64(len(data)))
		if err != nil {
			fs.debugDurationf(loadStart, 0, "dirty load part read failed path=%s part=%d off=%d len=%d got=%d err=%v", filePath, partNum, offset, length, len(data), err)
			return nil, err
		}
		fs.debugDurationf(loadStart, 0, "dirty load part done path=%s part=%d off=%d len=%d got=%d err=<nil>", filePath, partNum, offset, length, len(data))
		return data, nil
	}

	return gofuse.OK
}

func (fs *Dat9FS) preloadWritableHandleFromReadCacheLocked(fh *FileHandle) bool {
	if fs.readCache == nil || fh == nil || fh.Dirty == nil {
		return false
	}
	if fh.OrigSize <= 0 || fh.OrigSize > fs.readCache.MaxFileSize() || fh.BaseRev <= 0 {
		return false
	}

	data, ok := fs.readCache.Get(fh.Path, fh.BaseRev)
	if !ok {
		return false
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		log.Printf("read-cache writable preload failed for %s: %v", fh.Path, err)
		return false
	}
	fh.Dirty.ClearDirty()
	return true
}

func (fs *Dat9FS) pendingKindForHandle(fh *FileHandle) PendingKind {
	if fh.IsNew {
		return PendingNew
	}
	return PendingOverwrite
}

func createInputMode(inputMode uint32) (uint32, bool) {
	mode := inputMode & 0o777
	return mode, mode != defaultRegularFileMode
}

// modeForPendingHandle returns the mode that still needs to be committed to
// the server. Callers must hold fh.mu unless the handle has not been published.
func (fs *Dat9FS) modeForPendingHandle(fh *FileHandle) (uint32, bool) {
	if fh == nil {
		return 0, false
	}
	if fh.HasPendingMode {
		return fh.PendingMode & 0o777, true
	}
	return 0, false
}

func (fs *Dat9FS) fileHandlesForInode(ino uint64) []*FileHandle {
	var handles []*FileHandle
	fs.fileHandles.ForEach(func(_ uint64, h *FileHandle) {
		if h.Ino == ino {
			handles = append(handles, h)
		}
	})
	return handles
}

func (fs *Dat9FS) setPendingMetadataMode(path string, mode uint32) {
	mode &= 0o777
	if fs.pendingIndex != nil {
		if err := fs.pendingIndex.UpdateMode(path, mode); err != nil {
			log.Printf("pending index mode update failed for %s: %v", path, err)
		}
	}
	if fs.writeBack != nil {
		if err := fs.writeBack.UpdateMode(path, mode); err != nil {
			log.Printf("writeback mode update failed for %s: %v", path, err)
		}
	}
}

func (fs *Dat9FS) clearPendingModeForInode(ino uint64) {
	fs.clearPendingModeForInodeExcept(ino, nil)
}

func (fs *Dat9FS) clearPendingModeForInodeExcept(ino uint64, skip *FileHandle) {
	for _, h := range fs.fileHandlesForInode(ino) {
		if h == skip {
			continue
		}
		h.Lock()
		h.HasPendingMode = false
		h.HasPreviousMode = false
		h.PreviousModeKnown = false
		h.Unlock()
	}
}

func (fs *Dat9FS) applyRemoteMode(ctx context.Context, localPath string, mode uint32) error {
	mode &= 0o777
	start := fs.perfStart()
	err := fs.client.ChmodCtx(ctx, fs.remotePath(localPath), mode)
	fs.perfRecordRemote(perfRemoteMutation, start, err, 0)
	if err != nil {
		return fmt.Errorf("chmod %s to %o: %w", localPath, mode, err)
	}
	return nil
}

// applyPendingModeForHandleLocked applies a deferred chmod after the data
// upload has succeeded. Caller must hold fh.mu; this method drops it around
// the network request and re-acquires it before returning.
func (fs *Dat9FS) applyPendingModeForHandleLocked(ctx context.Context, fh *FileHandle) error {
	mode, hasMode := fs.modeForPendingHandle(fh)
	if !hasMode {
		return nil
	}
	localPath := fh.Path
	ino := fh.Ino
	previousMode := fh.PreviousMode
	hasPreviousMode := fh.HasPreviousMode
	previousModeKnown := fh.PreviousModeKnown
	if !shouldApplyRemoteMode(fs.pendingKindForHandle(fh), hasMode, mode) {
		fh.HasPendingMode = false
		fh.HasPreviousMode = false
		fh.PreviousModeKnown = false
		fh.Unlock()
		fs.clearPendingModeForInodeExcept(ino, fh)
		fh.Lock()
		return nil
	}

	fh.Unlock()
	err := retryPostUploadMode(ctx, func() error {
		return fs.applyRemoteMode(ctx, localPath, mode)
	})
	fh.Lock()
	if err != nil {
		if hasPreviousMode {
			fs.inodes.SetModeState(ino, previousMode, previousModeKnown)
		}
		return err
	}
	fh.HasPendingMode = false
	fh.HasPreviousMode = false
	fh.PreviousModeKnown = false
	fh.Unlock()
	fs.clearPendingModeForInodeExcept(ino, fh)
	fh.Lock()
	return nil
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
		if err := fs.shadowStore.WriteFull(fh.Path, fh.Dirty.bytesView(), fh.BaseRev); err != nil {
			return err
		}
		fh.ShadowReady = true
	}

	if durable {
		if err := fs.shadowStore.Sync(fh.Path); err != nil {
			return err
		}
	}
	mode, hasMode := fs.modeForPendingHandle(fh)
	if fh.ShadowSpill {
		if _, err := fs.pendingIndex.PutShadowSpillWithMode(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev, mode, hasMode); err != nil {
			log.Printf("pending index put failed for %s: %v", fh.Path, err)
		}
	} else {
		if _, err := fs.pendingIndex.PutWithBaseRevAndMode(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev, mode, hasMode); err != nil {
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
	mode, hasMode := fs.modeForPendingHandle(fh)
	return fs.writeBack.PutWithBaseRevAndMode(
		fh.Path,
		fh.Dirty.bytesView(),
		fh.Dirty.Size(),
		fs.pendingKindForHandle(fh),
		fh.BaseRev,
		mode,
		hasMode,
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

	wb := fs.newWriteBuffer(fh.Path, maxPreloadSize, 0)
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
	if meta.HasMode {
		mode := meta.Mode & 0o777
		fh.PendingMode = mode
		fh.HasPendingMode = true
		fs.inodes.UpdateMode(fh.Ino, mode)
	}
	return nil
}

func (fs *Dat9FS) loadWritableHandleFromWriteBackLocked(fh *FileHandle) bool {
	if fs.writeBack == nil || fh == nil || fh.Dirty == nil {
		return false
	}

	meta, ok := fs.writeBack.GetMeta(fh.Path)
	if !ok {
		return false
	}
	data, ok := fs.writeBack.getView(fh.Path)
	if !ok {
		return false
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		log.Printf("writeback preload failed for %s: %v", fh.Path, err)
		return false
	}

	fh.IsNew = meta.Kind == PendingNew
	fh.OrigSize = int64(len(data))
	if meta.BaseRev > 0 {
		fh.BaseRev = meta.BaseRev
		fs.inodes.UpdateRevision(fh.Ino, meta.BaseRev)
	}
	if meta.HasMode {
		mode := meta.Mode & 0o777
		fh.PendingMode = mode
		fh.HasPendingMode = true
		fs.inodes.UpdateMode(fh.Ino, mode)
	}
	fh.DirtySeq = fs.markDirtySize(fh.Ino, int64(len(data)))
	fh.WriteBackSeq = fh.DirtySeq
	return true
}

func (fs *Dat9FS) loadWritableHandleFromOpenHandleLocked(fh *FileHandle) bool {
	if fs.openHandles == nil || fh == nil || fh.Dirty == nil {
		return false
	}

	type candidate struct {
		src          *FileHandle
		size         int64
		origSize     int64
		baseRev      int64
		dirtySeq     uint64
		isNew        bool
		zeroBase     bool
		shadowSource bool
		canMemory    bool
	}

	var candidates []candidate
	for _, src := range fs.openHandles.SnapshotPath(fh.Path) {
		if src == nil || src == fh {
			continue
		}

		src.Lock()
		if src.Dirty == nil {
			src.Unlock()
			continue
		}
		c := candidate{
			src:          src,
			size:         src.Dirty.Size(),
			origSize:     src.OrigSize,
			baseRev:      src.BaseRev,
			dirtySeq:     src.DirtySeq,
			isNew:        src.IsNew,
			zeroBase:     src.ZeroBase,
			shadowSource: fs.shadowStore != nil && (src.ShadowReady || src.ShadowSpill),
			canMemory:    src.Dirty.CanMaterializeFull(),
		}
		src.Unlock()
		candidates = append(candidates, c)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.dirtySeq != b.dirtySeq {
			return a.dirtySeq > b.dirtySeq
		}
		if a.shadowSource != b.shadowSource {
			return a.shadowSource
		}
		if a.size != b.size {
			return a.size > b.size
		}
		return a.baseRev > b.baseRev
	})

	for _, c := range candidates {
		var data []byte
		size := c.size
		haveData := size == 0

		// Shadow-backed handles are authoritative in the shadow store. Their
		// dirty buffer may have evicted parts and would materialize zeros.
		if !haveData && c.shadowSource {
			var err error
			data, err = fs.shadowStore.ReadAll(fh.Path)
			if err != nil {
				log.Printf("open-handle preload shadow read failed for %s: %v", fh.Path, err)
			} else {
				size = int64(len(data))
				haveData = true
			}
		}

		if !haveData && c.canMemory {
			c.src.Lock()
			if c.src.Dirty != nil {
				size = c.src.Dirty.Size()
				c.origSize = c.src.OrigSize
				c.baseRev = c.src.BaseRev
				c.isNew = c.src.IsNew
				c.zeroBase = c.src.ZeroBase
				if size == 0 {
					haveData = true
				} else if c.src.Dirty.CanMaterializeFull() {
					data = c.src.Dirty.Bytes()
					haveData = true
				}
			}
			c.src.Unlock()
		}

		if !haveData {
			continue
		}

		if len(data) > 0 {
			if _, err := fh.Dirty.Write(0, data); err != nil {
				log.Printf("open-handle preload failed for %s: %v", fh.Path, err)
				continue
			}
		} else if err := fh.Dirty.Truncate(size); err != nil {
			log.Printf("open-handle preload truncate failed for %s: %v", fh.Path, err)
			continue
		}
		fh.Dirty.ClearDirty()
		fh.IsNew = c.isNew
		fh.ZeroBase = c.zeroBase || (c.isNew && c.baseRev == 0)
		fh.BaseRev = c.baseRev
		if c.isNew {
			fh.OrigSize = 0
		} else {
			fh.OrigSize = c.origSize
		}
		fs.inodes.UpdateSize(fh.Ino, size)
		if c.baseRev > 0 {
			fs.inodes.UpdateRevision(fh.Ino, c.baseRev)
		}
		return true
	}
	return false
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
		mode := entry.Mode
		if !entry.HasMode {
			mode = 0755
		}
		out.Mode = syscall.S_IFDIR | (mode & 0o777)
		out.Nlink = 2
	} else if entryIsSymlink(entry) {
		mode := entry.Mode & 0o777
		if !entry.HasMode {
			mode = 0o777
		}
		out.Mode = uint32(syscall.S_IFLNK) | mode
		out.Nlink = 1
	} else {
		mode := entry.Mode
		if !entry.HasMode {
			mode = 0644
		}
		out.Mode = syscall.S_IFREG | (mode & 0o777)
		out.Nlink = 1
	}
}

func isSymlinkMode(mode uint32) bool {
	return mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK)
}

func entryIsSymlink(entry *InodeEntry) bool {
	return entry != nil && !entry.IsDir && entry.HasMode && isSymlinkMode(entry.Mode)
}

func dirEntryMode(isDir, hasMode bool, mode uint32) uint32 {
	if isDir {
		perm := uint32(0o755)
		if hasMode {
			perm = mode & 0o777
		}
		return uint32(syscall.S_IFDIR) | perm
	}
	perm := uint32(0o644)
	if hasMode {
		perm = mode & 0o777
	}
	if hasMode && isSymlinkMode(mode) {
		return uint32(syscall.S_IFLNK) | perm
	}
	return uint32(syscall.S_IFREG) | perm
}

func symlinkMode() uint32 {
	return uint32(syscall.S_IFLNK) | 0o777
}

func (fs *Dat9FS) fillEntryOut(entry *InodeEntry, out *gofuse.EntryOut) {
	out.NodeId = entry.Ino
	out.Generation = 1
	fs.fillAttr(entry, &out.Attr)
	entryTTL := fs.opts.EntryTTL
	if isLockFilePath(entry.Path) {
		entryTTL = 0
	}
	out.SetEntryTimeout(entryTTL)
	out.SetAttrTimeout(fs.opts.AttrTTL)
}

func isLockFilePath(p string) bool {
	return strings.HasSuffix(path.Base(p), ".lock")
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
			if strings.Contains(strings.ToLower(se.Message), "revision conflict") {
				return gofuse.EIO
			}
			return gofuse.Status(syscall.EEXIST)
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
		// 499 (Client Closed Request) is emitted by tenantAuthMiddleware when
		// the request context is canceled before auth completes; treat it
		// identically to context.Canceled so the FUSE caller sees a
		// retryable EAGAIN rather than an opaque EIO.
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, statusClientClosedRequest:
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
	case strings.Contains(lowerMsg, "already exists") || strings.Contains(msg, "HTTP 409"):
		return gofuse.Status(syscall.EEXIST)
	case strings.Contains(msg, "HTTP 403"):
		return gofuse.EACCES
	case strings.Contains(msg, "HTTP 413"):
		return gofuse.Status(syscall.EFBIG)
	case strings.Contains(msg, "HTTP 412"):
		return gofuse.Status(syscall.ESTALE)
	case strings.Contains(msg, "HTTP 400"):
		return gofuse.Status(syscall.EINVAL)
	case strings.Contains(msg, "HTTP 499") ||
		strings.Contains(msg, "HTTP 500") ||
		strings.Contains(msg, "HTTP 502") ||
		strings.Contains(msg, "HTTP 503") ||
		strings.Contains(msg, "HTTP 504"):
		return gofuse.Status(syscall.EAGAIN)
	default:
		return gofuse.EIO
	}
}

// statusClientClosedRequest mirrors the non-standard 499 status emitted by the
// server's auth middleware when the client cancels mid-auth. Tracked here so
// httpToFuseStatus / isTransientLookupErr stay aligned with the server contract
// without taking a server package dependency.
const statusClientClosedRequest = 499

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
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, statusClientClosedRequest:
			return true
		}
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isConflictErr(err error) bool {
	if err == nil {
		return false
	}
	var se *client.StatusError
	return errors.As(err, &se) && se.StatusCode == http.StatusConflict
}

func isCreateActionUnsupportedErr(err error) bool {
	var se *client.StatusError
	if !errors.As(err, &se) {
		return false
	}
	if se.StatusCode == http.StatusNotFound {
		return true
	}
	if se.StatusCode != http.StatusBadRequest && se.StatusCode != http.StatusMethodNotAllowed {
		return false
	}
	msg := strings.ToLower(se.Message)
	return strings.Contains(msg, "unknown post action") || strings.Contains(msg, "method not allowed")
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
	remotePath := fs.remotePath(path)
	readStart := fs.perfStart()
	data, err := fs.client.ReadCtx(ctx, remotePath)
	fs.perfRecordRemote(perfRemoteRead, readStart, err, uint64(len(data)))
	if err == nil || !isTransientReadErr(err) {
		return data, err
	}

	if fs.perf != nil {
		fs.perf.readRetryTotal.add(1)
	}
	lastErr := err
	for range readTransientRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), readTransientRetryTimeout)
		readStart = fs.perfStart()
		data, err = fs.client.ReadCtx(retryCtx, remotePath)
		retryCancel()
		fs.perfRecordRemote(perfRemoteRead, readStart, err, uint64(len(data)))
		if err == nil {
			if fs.perf != nil {
				fs.perf.readRetrySuccess.add(1)
			}
			return data, nil
		}
		if !isTransientReadErr(err) {
			return nil, err
		}
		lastErr = err
	}
	if fs.perf != nil {
		fs.perf.readRetryExhausted.add(1)
	}
	return nil, fmt.Errorf("%w: %s: %v", errReadRetriesExhausted, path, lastErr)
}

func (fs *Dat9FS) readTargetForHandle(ctx context.Context, fh *FileHandle) *client.ReadTarget {
	if fs == nil || fs.client == nil || fh == nil || fh.Dirty != nil {
		return nil
	}
	fh.Lock()
	target := fh.ReadTarget
	handlePath := fh.Path
	remotePath := fs.remotePath(handlePath)
	fh.Unlock()
	if target != nil {
		return target
	}

	resolved, err := fs.client.ResolveReadTarget(ctx, remotePath)
	if err != nil {
		// Fall back to the ordinary read path. This preserves inline-file and
		// transient-error behavior while still optimizing the common S3 case.
		return nil
	}

	fh.Lock()
	if fh.Path == handlePath && fh.Dirty == nil && fh.ReadTarget == nil {
		fh.ReadTarget = resolved
		if fh.Prefetch != nil {
			fh.Prefetch.SetReadTarget(resolved)
		}
	}
	target = fh.ReadTarget
	fh.Unlock()
	return target
}

func clearReadTargetForHandle(fh *FileHandle) {
	if fh == nil {
		return
	}
	fh.Lock()
	fh.ReadTarget = nil
	if fh.Prefetch != nil {
		fh.Prefetch.SetReadTarget(nil)
	}
	fh.Unlock()
}

func (fs *Dat9FS) clearReadTargetsForPath(p string) {
	fs.clearReadTargetsForPathExcept(p, nil)
}

func (fs *Dat9FS) clearReadTargetsForPathExcept(p string, skip *FileHandle) {
	if fs == nil || fs.openHandles == nil || p == "" {
		return
	}
	for _, fh := range fs.openHandles.SnapshotPath(p) {
		if fh == skip {
			continue
		}
		clearReadTargetForHandle(fh)
	}
}

func clearReadTargetForLockedHandle(fh *FileHandle) {
	if fh == nil {
		return
	}
	fh.ReadTarget = nil
	if fh.Prefetch != nil {
		fh.Prefetch.SetReadTarget(nil)
	}
}

func (fs *Dat9FS) clearAllReadTargets() {
	if fs == nil || fs.fileHandles == nil {
		return
	}
	fs.fileHandles.ForEach(func(_ uint64, fh *FileHandle) {
		clearReadTargetForHandle(fh)
	})
}

func (fs *Dat9FS) invalidateReadCacheAndTargets(p string) {
	fs.invalidateReadCacheAndTargetsExcept(p, nil)
}

func (fs *Dat9FS) invalidateReadCacheAndTargetsExcept(p string, skip *FileHandle) {
	if fs == nil {
		return
	}
	if fs.readCache != nil {
		fs.readCache.Invalidate(p)
	}
	fs.clearReadTargetsForPathExcept(p, skip)
}

// readStreamRangeWithRetry performs a range read with bounded detached retry
// on transient failures. Wraps both the ReadStreamRange open and io.ReadFull
// body read as a single retriable unit. On body-stage transient failure, the
// stream is reopened from scratch on retry.
// Returns (data, nil) on success. On exhausted retries, the returned error
// is a wrapped sentinel so the caller can map it to EIO.
func (fs *Dat9FS) readStreamRangeWithRetry(ctx context.Context, path string, fh *FileHandle, offset, size int64) ([]byte, int, error) {
	target := fs.readTargetForHandle(ctx, fh)
	data, n, err := fs.doRangeRead(ctx, path, target, offset, size)
	if client.IsPresignExpired(err) {
		clearReadTargetForHandle(fh)
		retryCtx, retryCancel := context.WithTimeout(context.Background(), readTransientRetryTimeout)
		target = fs.readTargetForHandle(retryCtx, fh)
		data, n, err = fs.doRangeRead(retryCtx, path, target, offset, size)
		retryCancel()
		if err == nil && fs.perf != nil {
			fs.perf.readRetrySuccess.add(1)
		}
	}
	if err == nil || !isTransientReadErr(err) {
		return data, n, err
	}

	if fs.perf != nil {
		fs.perf.readRetryTotal.add(1)
	}
	lastErr := err
	for range readTransientRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), readTransientRetryTimeout)
		data, n, err = fs.doRangeRead(retryCtx, path, target, offset, size)
		if client.IsPresignExpired(err) {
			clearReadTargetForHandle(fh)
			target = fs.readTargetForHandle(retryCtx, fh)
			data, n, err = fs.doRangeRead(retryCtx, path, target, offset, size)
		}
		retryCancel()
		if err == nil {
			if fs.perf != nil {
				fs.perf.readRetrySuccess.add(1)
			}
			return data, n, nil
		}
		if !isTransientReadErr(err) {
			return nil, 0, err
		}
		lastErr = err
	}
	if fs.perf != nil {
		fs.perf.readRetryExhausted.add(1)
	}
	return nil, 0, fmt.Errorf("%w: %s: %v", errReadRetriesExhausted, path, lastErr)
}

// doRangeRead performs a single range read attempt: open stream + read body.
// All body read errors (including truncation) are returned as-is so the
// caller can classify them for retry.
func (fs *Dat9FS) doRangeRead(ctx context.Context, path string, target *client.ReadTarget, offset, size int64) ([]byte, int, error) {
	readStart := fs.perfStart()
	var rc io.ReadCloser
	var err error
	if target != nil {
		rc, err = fs.client.ReadObjectRange(ctx, target, offset, size)
	} else {
		rc, err = fs.client.ReadStreamRange(ctx, fs.remotePath(path), offset, size)
	}
	if err != nil {
		fs.perfRecordRemote(perfRemoteRead, readStart, err, 0)
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
	fs.perfRecordRemote(perfRemoteRead, readStart, err, uint64(len(data)))
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

func (fs *Dat9FS) statWithTransientRetry(cancel <-chan struct{}, localPath string, trackLookupMetrics bool) (*client.StatResult, error) {
	apiPath := fs.remotePath(localPath)
	ctx, cf := fuseCtx(cancel)
	statStart := fs.perfStart()
	stat, err := fs.client.StatCtx(ctx, apiPath)
	cf()
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
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
		if fs.perf != nil {
			fs.perf.lookupRetryTotal.add(1)
		}
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
		statStart = fs.perfStart()
		stat, err = fs.client.StatCtx(retryCtx, apiPath)
		retryCancel()
		fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
		if err == nil {
			if trackLookupMetrics {
				successCount := fs.lookupStatRetrySuccess.Add(1)
				if fs.perf != nil {
					fs.perf.lookupRetrySuccess.add(1)
				}
				if successCount <= 3 || successCount%lookupRetrySuccessLogEvery == 0 {
					log.Printf("lookup stat retry recovered for %s (success_count=%d)", localPath, successCount)
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
		if fs.perf != nil {
			fs.perf.lookupRetryExhausted.add(1)
		}
		log.Printf("lookup stat retries exhausted for %s: %v", localPath, lastErr)
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

func cachedFileInfos(items []client.FileInfo) []CachedFileInfo {
	cached := make([]CachedFileInfo, len(items))
	for i, item := range items {
		var mtime time.Time
		if item.Mtime > 0 {
			mtime = time.Unix(item.Mtime, 0)
		}
		cached[i] = CachedFileInfo{
			Name:    item.Name,
			Size:    item.Size,
			IsDir:   item.IsDir,
			Mtime:   mtime,
			Mode:    item.Mode,
			HasMode: item.HasMode,
		}
	}
	return cached
}

func (fs *Dat9FS) lookupListWithRetry(cancel <-chan struct{}, parentPath string) ([]client.FileInfo, error) {
	// list-fallback retries are intentionally not counted in lookupStatRetry*;
	// those counters remain scoped to the primary Lookup->Stat path.
	ctx, cf := fuseCtx(cancel)
	apiPath := fs.remotePath(parentPath)
	listStart := fs.perfStart()
	items, err := fs.client.ListCtx(ctx, apiPath)
	cf()
	fs.perfRecordRemote(perfRemoteList, listStart, err, 0)
	if err == nil {
		fs.dirCache.Put(parentPath, cachedFileInfos(items))
		return items, nil
	}
	if !isTransientLookupErr(err) {
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
		listStart = fs.perfStart()
		items, err = fs.client.ListCtx(retryCtx, apiPath)
		retryCancel()
		fs.perfRecordRemote(perfRemoteList, listStart, err, 0)
		if err == nil {
			fs.dirCache.Put(parentPath, cachedFileInfos(items))
			return items, nil
		}
		if !isTransientLookupErr(err) {
			return items, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func cachedInfoFromEntry(name string, entry *InodeEntry) CachedFileInfo {
	if entry == nil {
		return CachedFileInfo{Name: name}
	}
	mtime := entry.Mtime
	if mtime.IsZero() {
		mtime = time.Now()
	}
	return CachedFileInfo{
		Name:     name,
		Size:     entry.Size,
		IsDir:    entry.IsDir,
		Mtime:    mtime,
		Revision: entry.Revision,
		Mode:     entry.Mode,
		HasMode:  entry.HasMode,
	}
}

func cachedInfoFromStat(name string, stat *client.StatResult) CachedFileInfo {
	mtime := time.Now()
	if stat != nil && !stat.Mtime.IsZero() {
		mtime = stat.Mtime
	}
	item := CachedFileInfo{Name: name, Mtime: mtime}
	if stat != nil {
		item.Size = stat.Size
		item.IsDir = stat.IsDir
		item.Revision = stat.Revision
		item.HasMode = stat.HasMode
		item.Mode = stat.Mode
	}
	return item
}

func cachedInfoFromWriteBackMeta(name string, meta *WriteBackMeta) CachedFileInfo {
	if meta == nil {
		return CachedFileInfo{Name: name}
	}
	mtime := meta.Mtime
	if mtime.IsZero() {
		mtime = time.Now()
	}
	return CachedFileInfo{
		Name:     name,
		Size:     meta.Size,
		IsDir:    false,
		Mtime:    mtime,
		Revision: meta.BaseRev,
		Mode:     meta.Mode,
		HasMode:  meta.HasMode,
	}
}

func (fs *Dat9FS) cacheFileForPath(p string, size int64, mtime time.Time, revision int64) {
	if fs == nil || fs.dirCache == nil || p == "/" {
		return
	}
	if mtime.IsZero() {
		mtime = time.Now()
	}
	parentPath, name := cacheParentName(p)
	fs.dirCache.Upsert(parentPath, CachedFileInfo{
		Name:     name,
		Size:     size,
		IsDir:    false,
		Mtime:    mtime,
		Revision: revision,
	})
}

func (fs *Dat9FS) cacheNegativePath(p string) {
	if fs == nil || fs.dirCache == nil || p == "/" || isLockFilePath(p) {
		return
	}
	parentPath, name := cacheParentName(p)
	fs.dirCache.MarkNegative(parentPath, name)
}

func (fs *Dat9FS) negativeEntryTTL(p string) time.Duration {
	if isLockFilePath(p) {
		return 0
	}
	return fs.opts.NegativeEntryTTL
}

func (fs *Dat9FS) remoteDirExistsDetached(localPath string) bool {
	retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	defer retryCancel()
	statStart := fs.perfStart()
	stat, err := fs.client.StatCtx(retryCtx, fs.remotePath(localPath))
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	return err == nil && stat.IsDir
}

func (fs *Dat9FS) remoteRenameCommittedDetached(oldP, newP string) bool {
	targetCtx, targetCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	statStart := fs.perfStart()
	_, targetErr := fs.client.StatCtx(targetCtx, fs.remotePath(newP))
	targetCancel()
	fs.perfRecordRemote(perfRemoteStat, statStart, targetErr, 0)
	if targetErr != nil {
		return false
	}

	sourceCtx, sourceCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	statStart = fs.perfStart()
	_, sourceErr := fs.client.StatCtx(sourceCtx, fs.remotePath(oldP))
	sourceCancel()
	fs.perfRecordRemote(perfRemoteStat, statStart, sourceErr, 0)
	return isNotFoundErr(sourceErr)
}

func (fs *Dat9FS) remotePathGoneDetached(localPath string) bool {
	retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	defer retryCancel()
	statStart := fs.perfStart()
	_, err := fs.client.StatCtx(retryCtx, fs.remotePath(localPath))
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	return isNotFoundErr(err)
}

func (fs *Dat9FS) remotePathExistsDetached(localPath string) (bool, error) {
	retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	defer retryCancel()
	statStart := fs.perfStart()
	_, err := fs.client.StatCtx(retryCtx, fs.remotePath(localPath))
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	if err == nil {
		return true, nil
	}
	if isNotFoundErr(err) {
		return false, nil
	}
	return false, err
}

func (fs *Dat9FS) remotePathStatDetached(localPath string) (*client.StatResult, error) {
	retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	defer retryCancel()
	statStart := fs.perfStart()
	stat, err := fs.client.StatCtx(retryCtx, fs.remotePath(localPath))
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	return stat, err
}

func (fs *Dat9FS) mkdirRemoteWithTransientRetry(cancel <-chan struct{}, localPath string, mode uint32) error {
	apiPath := fs.remotePath(localPath)
	ctx, cf := fuseCtx(cancel)
	mutationStart := fs.perfStart()
	err := fs.client.MkdirCtx(ctx, apiPath, mode)
	cf()
	fs.perfRecordRemote(perfRemoteMutation, mutationStart, err, 0)
	if err == nil || !isTransientLookupErr(err) {
		return err
	}

	// The first request honored the FUSE interrupt. If it was canceled after the
	// server committed the directory, a detached stat lets us return success
	// instead of surfacing EAGAIN to checkout-like callers that will not retry.
	if fs.remoteDirExistsDetached(localPath) {
		return nil
	}

	retryCount := fs.lookupStatRetryCount()
	if retryCount <= 0 {
		return err
	}

	lastErr := err
	for range retryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		mutationStart = fs.perfStart()
		err = fs.client.MkdirCtx(retryCtx, apiPath, mode)
		retryCancel()
		fs.perfRecordRemote(perfRemoteMutation, mutationStart, err, 0)
		if err == nil {
			return nil
		}
		if isConflictErr(err) && fs.remoteDirExistsDetached(localPath) {
			return nil
		}
		if !isTransientLookupErr(err) {
			return err
		}
		if fs.remoteDirExistsDetached(localPath) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

type deleteKind string

const (
	deleteKindFile deleteKind = "file"
	deleteKindDir  deleteKind = "dir"
)

func (fs *Dat9FS) deleteRemoteFileWithInterruptRecovery(ctx context.Context, localPath string) error {
	return fs.deleteRemotePathWithInterruptRecovery(ctx, localPath, deleteKindFile)
}

func (fs *Dat9FS) deleteRemoteDirWithInterruptRecovery(ctx context.Context, localPath string) error {
	return fs.deleteRemotePathWithInterruptRecovery(ctx, localPath, deleteKindDir)
}

func (fs *Dat9FS) deleteRemotePathWithInterruptRecovery(ctx context.Context, localPath string, kind deleteKind) error {
	mutationStart := fs.perfStart()
	remotePath := fs.remotePath(localPath)
	var err error
	switch kind {
	case deleteKindFile:
		err = fs.client.DeleteFileCtx(ctx, remotePath)
	case deleteKindDir:
		err = fs.client.DeleteDirCtx(ctx, remotePath)
	default:
		err = fmt.Errorf("unsupported delete kind %q", kind)
	}
	fs.perfRecordRemote(perfRemoteMutation, mutationStart, err, 0)
	if err == nil || !isTransientLookupErr(err) {
		return err
	}

	// If the FUSE request was interrupted after the server committed the delete,
	// the path is already gone. Confirm that detached from the canceled request.
	// If the path still exists, do not retry a path-only DELETE: another actor
	// may have recreated the same name after the ambiguous first delete.
	if fs.remotePathGoneDetached(localPath) {
		return nil
	}
	return err
}

func (fs *Dat9FS) renameRemoteWithTransientRetry(ctx context.Context, oldP, newP string) error {
	oldRemote := fs.remotePath(oldP)
	newRemote := fs.remotePath(newP)
	mutationStart := fs.perfStart()
	err := fs.client.RenameCtx(ctx, oldRemote, newRemote)
	fs.perfRecordRemote(perfRemoteMutation, mutationStart, err, 0)
	if err == nil || !isTransientLookupErr(err) {
		return err
	}

	// If the caller interrupted after the server committed the rename but before
	// the response reached us, the target is visible and the source is gone.
	// Target visibility alone is not enough because server-side rename supports
	// replacing an existing target such as .git/config.
	if fs.remoteRenameCommittedDetached(oldP, newP) {
		return nil
	}

	retryCount := fs.lookupStatRetryCount()
	if retryCount <= 0 {
		return err
	}

	lastErr := err
	for range retryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		mutationStart = fs.perfStart()
		err = fs.client.RenameCtx(retryCtx, oldRemote, newRemote)
		retryCancel()
		fs.perfRecordRemote(perfRemoteMutation, mutationStart, err, 0)
		if err == nil {
			return nil
		}
		if isNotFoundErr(err) && fs.remoteRenameCommittedDetached(oldP, newP) {
			return nil
		}
		if !isTransientLookupErr(err) {
			return err
		}
		if fs.remoteRenameCommittedDetached(oldP, newP) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func (fs *Dat9FS) lookupFromDirCache(parentPath, childP, name string, out *gofuse.EntryOut) (bool, gofuse.Status) {
	result := fs.dirCache.Lookup(parentPath, name)
	switch result.kind {
	case namespaceLookupPositive:
		if fs.perf != nil {
			fs.perf.dirCacheHit.add(1)
			fs.perf.namespacePositiveHit.add(1)
		}
		item := result.item
		mtime := item.Mtime
		if mtime.IsZero() {
			mtime = time.Now()
		}
		ino := fs.inodes.Lookup(childP, item.IsDir, item.Size, mtime)
		if item.Revision > 0 {
			fs.inodes.UpdateRevision(ino, item.Revision)
		}
		if item.HasMode {
			fs.inodes.UpdateMode(ino, item.Mode)
		}
		entry, ok := fs.inodes.GetEntry(ino)
		if !ok {
			return true, gofuse.EIO
		}
		fs.fillEntryOut(entry, out)
		return true, gofuse.OK
	case namespaceLookupNegative, namespaceLookupCompleteMiss, namespaceLookupSessionMiss:
		if isLockFilePath(childP) {
			if fs.perf != nil {
				fs.perf.dirCacheMiss.add(1)
			}
			return false, gofuse.OK
		}
		if fs.perf != nil {
			fs.perf.dirCacheHit.add(1)
			switch result.kind {
			case namespaceLookupNegative:
				fs.perf.namespaceNegativeHit.add(1)
			case namespaceLookupCompleteMiss:
				fs.perf.namespaceCompleteMiss.add(1)
			case namespaceLookupSessionMiss:
				fs.perf.namespaceSessionMiss.add(1)
			}
		}
		out.NodeId = 0
		out.SetEntryTimeout(fs.negativeEntryTTL(childP))
		return true, gofuse.ENOENT
	case namespaceLookupPartialMiss:
		if fs.perf != nil {
			fs.perf.dirCacheMiss.add(1)
			fs.perf.namespacePartialMiss.add(1)
		}
		return false, gofuse.OK
	default:
		if fs.perf != nil {
			fs.perf.dirCacheMiss.add(1)
		}
		return false, gofuse.OK
	}
}

// --- RawFileSystem methods ---------------------------------------------------

func (fs *Dat9FS) Init(server *gofuse.Server) {
	fs.server = server
	// Synchronously warm the server-advertised inline_threshold so the very
	// first Create/Write/Flush/commit-queue decision after mount sees the
	// negotiated value. Bound by a short timeout so an unreachable server
	// can't stall mount; on timeout/failure we fall back to
	// defaultSmallFileThreshold (50KB), matching old-server behavior.
	ctx, cancel := context.WithTimeout(context.Background(), inlineThresholdWarmTimeout)
	defer cancel()
	fs.warmInlineThreshold(ctx)
}

// inlineThresholdWarmTimeout caps the synchronous status fetch on Init.
// 5s leaves margin for cold TLS handshake + cross-region RTT while staying
// under typical mount-readiness expectations. Declared as a var so tests
// can shrink it; production callers must not mutate.
var inlineThresholdWarmTimeout = 5 * time.Second

// notifyEntry tells the kernel to invalidate a directory entry cache.
// Safe to call even if the server is not yet initialized (e.g., during tests).
func (fs *Dat9FS) notifyEntry(parentIno uint64, name string) {
	fs.notifyCount.Add(1)
	if fs.perf != nil {
		fs.perf.notifyEntry.add(1)
	}
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
	fs.notifyCount.Add(1)
	if fs.perf != nil {
		fs.perf.notifyInode.add(1)
	}
	if fs.server == nil {
		return
	}
	fs.notifyWg.Add(1)
	go func() {
		defer fs.notifyWg.Done()
		_ = fs.server.InodeNotify(ino, 0, 0)
	}()
}

func (fs *Dat9FS) Lookup(cancel <-chan struct{}, header *gofuse.InHeader, name string, out *gofuse.EntryOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseLookup, perfStart, status, 0) }()
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
			if meta.HasMode {
				fs.inodes.UpdateMode(ino, meta.Mode)
			}
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
			if meta.HasMode {
				fs.inodes.UpdateMode(ino, meta.Mode)
			}
			entry, ok := fs.inodes.GetEntry(ino)
			if !ok {
				return gofuse.EIO
			}
			fs.fillEntryOut(entry, out)
			return gofuse.OK
		}
	}

	// Open-created namespace overlay: a file may be open and dirty before
	// Flush has staged it into PendingIndex. Git's lock-file path can chmod
	// such a file by path while the kernel has already forgotten the original
	// lookup ref; resolving from the open handle keeps POSIX create->chmod
	// sequences from seeing a false ENOENT.
	if entry, ok := fs.openHandleEntry(childP); ok {
		fs.fillEntryOut(entry, out)
		return gofuse.OK
	}

	parentPath, ok := fs.inodes.GetPath(header.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	if handled, cacheStatus := fs.lookupFromDirCache(parentPath, childP, name, out); handled {
		return cacheStatus
	}

	stat, err := fs.lookupStatWithRetry(cancel, childP)
	if err != nil {
		if !isNotFoundErr(err) {
			return httpToFuseStatus(err)
		}

		if fs.opts.LegacyDirStatFallback {
			// Compatibility path for private legacy servers that do not support
			// stat/HEAD on directories: list the parent and match by name.
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
				if item.HasMode {
					fs.inodes.UpdateMode(ino, item.Mode)
				}
				entry, ok := fs.inodes.GetEntry(ino)
				if !ok {
					return gofuse.EIO
				}
				fs.fillEntryOut(entry, out)
				return gofuse.OK
			}
		}
		// Cache negative lookup: tell kernel this entry doesn't exist
		// for NegativeEntryTTL so it doesn't re-ask immediately.
		fs.cacheNegativePath(childP)
		out.NodeId = 0
		out.SetEntryTimeout(fs.negativeEntryTTL(childP))
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
	if stat.HasMode {
		fs.inodes.UpdateMode(ino, stat.Mode)
	}
	fs.dirCache.Upsert(parentPath, cachedInfoFromStat(name, stat))
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}
	fs.fillEntryOut(entry, out)
	return gofuse.OK
}

func (fs *Dat9FS) Forget(nodeId uint64, nlookup uint64) {
	entry, ok := fs.inodes.GetEntry(nodeId)
	if ok && !entry.IsDir && fs.shouldPreserveForgottenInode(entry) {
		fs.inodes.ForgetKeepMapping(nodeId, nlookup)
		return
	}
	fs.inodes.Forget(nodeId, nlookup)
}

func (fs *Dat9FS) shouldPreserveForgottenInode(entry *InodeEntry) bool {
	if entry == nil {
		return false
	}
	if fs.hasOpenHandle(entry.Ino, entry.Path) {
		return true
	}
	return fs.hasPendingLocalState(entry.Path) || fs.hasQueuedCommit(entry.Path)
}

func (fs *Dat9FS) hasOpenHandle(ino uint64, p string) bool {
	return fs.openHandles.Has(ino, p)
}

func (fs *Dat9FS) hasPendingLocalState(p string) bool {
	if fs.pendingIndex != nil {
		if _, ok := fs.pendingIndex.GetMeta(p); ok {
			return true
		}
	}
	if fs.writeBack != nil {
		if _, ok := fs.writeBack.GetMeta(p); ok {
			return true
		}
	}
	if fs.shadowStore != nil && fs.shadowStore.Has(p) {
		return true
	}
	return false
}

func (fs *Dat9FS) hasQueuedCommit(p string) bool {
	return fs.commitQueue != nil && fs.commitQueue.HasPath(p)
}

func (fs *Dat9FS) openHandleEntry(p string) (*InodeEntry, bool) {
	for _, fh := range fs.openHandles.SnapshotPath(p) {
		if fh == nil {
			continue
		}
		fh.Lock()
		if fh.Dirty == nil {
			fh.Unlock()
			continue
		}
		size := fh.Dirty.Size()
		fh.Unlock()
		// This path reconstructs a dentry for an already-open writable file
		// after the kernel dropped its lookup ref. Reuse the handle's inode
		// instead of path Lookup so a stale/missing path map cannot allocate a
		// second inode for the same open file.
		if !fs.inodes.IncrementLookup(fh.Ino) {
			return nil, false
		}
		fs.inodes.UpdateSize(fh.Ino, size)
		fs.inodes.UpdateMtime(fh.Ino, time.Now())
		return fs.inodes.GetEntry(fh.Ino)
	}
	return nil, false
}

func (fs *Dat9FS) cleanupReleasedInode(ino uint64, p string) {
	if fs.hasOpenHandle(ino, p) || fs.hasPendingLocalState(p) || fs.hasQueuedCommit(p) {
		return
	}
	// Concurrent Release/commit cleanup for the same path is safe here:
	// RemoveFileIfUnreferenced holds the inode map lock and is idempotent when
	// another goroutine already removed the forgotten regular-file mapping.
	fs.inodes.RemoveFileIfUnreferenced(ino)
}

func (fs *Dat9FS) cleanupCommittedInode(ino uint64, p string) {
	if fs.hasOpenHandle(ino, p) || fs.hasPendingLocalState(p) {
		return
	}
	fs.inodes.RemoveFileIfUnreferenced(ino)
}

func (fs *Dat9FS) GetAttr(cancel <-chan struct{}, input *gofuse.GetAttrIn, out *gofuse.AttrOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseGetAttr, perfStart, status, 0) }()
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
			if stat.HasMode {
				entry.Mode = stat.Mode
				entry.HasMode = true
				fs.inodes.UpdateMode(input.NodeId, stat.Mode)
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
			if stat.HasMode {
				entry.Mode = stat.Mode
				entry.HasMode = true
				fs.inodes.UpdateMode(input.NodeId, stat.Mode)
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

func (fs *Dat9FS) SetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, out *gofuse.AttrOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseSetAttr, perfStart, status, 0) }()
	entry, ok := fs.inodes.GetEntry(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}

	// Handle mtime updates
	if mtime, ok := input.GetMTime(); ok {
		entry.Mtime = mtime
		fs.inodes.UpdateMtime(input.NodeId, mtime)
	}

	// Handle mode (chmod)
	if input.Valid&gofuse.FATTR_MODE != 0 {
		mode := input.Mode & 0777
		entryMode := mode
		if entryIsSymlink(entry) {
			entryMode |= uint32(syscall.S_IFLNK)
		}
		// If the file has an open dirty handle, update mode locally without
		// consulting the remote server. The mode will be synced on Flush.
		hasDirtyHandle := false
		for _, h := range fs.fileHandlesForInode(input.NodeId) {
			h.Lock()
			if h.Dirty != nil {
				hasDirtyHandle = true
				h.PendingMode = mode
				h.HasPendingMode = true
				if !h.HasPreviousMode {
					if entry.HasMode {
						h.PreviousMode = entry.Mode
					} else {
						h.PreviousMode = 0
					}
					h.HasPreviousMode = true
					h.PreviousModeKnown = entry.HasMode
				}
			}
			h.Unlock()
		}
		if hasDirtyHandle {
			fs.setPendingMetadataMode(entry.Path, mode)
		}
		if !hasDirtyHandle {
			ctx, cf := fuseCtx(cancel)
			defer cf()
			if err := fs.client.ChmodCtx(ctx, fs.remotePath(entry.Path), mode); err != nil {
				return httpToFuseStatus(err)
			}
		}
		entry.Mode = entryMode
		fs.inodes.UpdateMode(input.NodeId, entryMode)
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
				apiPath := fs.remotePath(entry.Path)
				writeStart := fs.perfStart()
				err := fs.client.WriteCtx(ctx, apiPath, nil)
				fs.perfRecordRemote(perfRemoteWrite, writeStart, err, 0)
				if err != nil {
					return httpToFuseStatus(err)
				}
				// Refresh the inode revision after the server-side truncate so a
				// subsequent writable open does not reuse the stale pre-truncate
				// base revision and conflict with its own zero-byte write.
				var refreshedRevision int64
				var refreshedMtime time.Time
				statStart := fs.perfStart()
				stat, statErr := fs.client.StatCtx(ctx, apiPath)
				fs.perfRecordRemote(perfRemoteStat, statStart, statErr, 0)
				if statErr != nil {
					log.Printf("post-truncate stat refresh failed for %s (inode=%d): %v (revision may be stale)", entry.Path, input.NodeId, statErr)
				} else if stat != nil {
					if stat.Revision > 0 {
						refreshedRevision = stat.Revision
						entry.Revision = stat.Revision
						fs.inodes.UpdateRevision(input.NodeId, stat.Revision)
						fs.updateOpenHandleBaseRevision(entry.Path, stat.Revision, input.Pid)
					}
					if !stat.Mtime.IsZero() {
						refreshedMtime = stat.Mtime
						entry.Mtime = stat.Mtime
						fs.inodes.UpdateMtime(input.NodeId, stat.Mtime)
					}
				}
				fs.invalidateReadCacheAndTargets(entry.Path)
				fs.cacheFileForPath(entry.Path, 0, refreshedMtime, refreshedRevision)
			} else if newSize != entry.Size {
				// Arbitrary truncate without an open handle is not
				// supported — dat9 has no server-side truncate API.
				return gofuse.Status(syscall.ENOTSUP)
			}
		}
		entry.Size = newSize
		fs.inodes.UpdateSize(input.NodeId, newSize)
		// Kernel already receives updated attrs via the SetAttr reply —
		// no need for an explicit notifyInode here.
	}

	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(fs.opts.AttrTTL)
	return gofuse.OK
}

func (fs *Dat9FS) Readlink(cancel <-chan struct{}, header *gofuse.InHeader) (out []byte, status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseReadlink, perfStart, status, uint64(len(out))) }()

	entry, ok := fs.inodes.GetEntry(header.NodeId)
	if !ok {
		return nil, gofuse.ENOENT
	}
	if !entryIsSymlink(entry) {
		return nil, gofuse.Status(syscall.EINVAL)
	}

	ctx, cf := fuseCtx(cancel)
	defer cf()
	target, err := fs.readSmallFileWithRetry(ctx, entry.Path)
	if err != nil {
		if errors.Is(err, errReadRetriesExhausted) {
			return nil, gofuse.EIO
		}
		return nil, httpToFuseStatus(err)
	}
	return target, gofuse.OK
}

// --- Directory operations ----------------------------------------------------

func (fs *Dat9FS) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseMkdir, perfStart, status, 0) }()
	childP, st := fs.childPath(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	mode := input.Mode & 0o777
	if err := fs.mkdirRemoteWithTransientRetry(cancel, childP, mode); err != nil {
		return httpToFuseStatus(err)
	}

	ino := fs.inodes.Lookup(childP, true, 0, time.Now())
	fs.inodes.UpdateMode(ino, mode)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}

	parentPath, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(name, entry))
	fs.dirCache.MarkSessionCreatedDir(childP)
	// Kernel already receives the new entry via the Mkdir reply —
	// no need for notifyEntry/notifyInode here.

	fs.fillEntryOut(entry, out)
	return gofuse.OK
}

func (fs *Dat9FS) Symlink(cancel <-chan struct{}, header *gofuse.InHeader, pointedTo string, linkName string, out *gofuse.EntryOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseSymlink, perfStart, status, 0) }()
	if fs.opts.ReadOnly {
		return gofuse.EROFS
	}

	childP, st := fs.childPath(header.NodeId, linkName)
	if st != gofuse.OK {
		return st
	}

	ctx, cf := fuseCtx(cancel)
	defer cf()
	mutationStart := fs.perfStart()
	err := fs.client.SymlinkCtx(ctx, pointedTo, fs.remotePath(childP))
	fs.perfRecordRemote(perfRemoteMutation, mutationStart, err, 0)
	if err != nil {
		if isTransientLookupErr(err) {
			stat, probeErr := fs.remotePathStatDetached(childP)
			if probeErr == nil && stat != nil && stat.HasMode && isSymlinkMode(stat.Mode) {
				err = nil
			} else if probeErr != nil {
				log.Printf("symlink: probe created path %s failed: %v", childP, probeErr)
			} else if stat != nil {
				log.Printf("symlink: recovered path %s is not a symlink (hasMode=%t mode=%o)", childP, stat.HasMode, stat.Mode)
			}
		}
	}
	if err != nil {
		return httpToFuseStatus(err)
	}

	mode := symlinkMode()
	ino := fs.inodes.Lookup(childP, false, int64(len(pointedTo)), time.Now())
	fs.inodes.UpdateMode(ino, mode)
	if stat, err := fs.client.StatCtx(ctx, fs.remotePath(childP)); err == nil && stat != nil {
		if stat.Revision > 0 {
			fs.inodes.UpdateRevision(ino, stat.Revision)
		}
		if !stat.Mtime.IsZero() {
			fs.inodes.UpdateMtime(ino, stat.Mtime)
		}
		if stat.HasMode {
			fs.inodes.UpdateMode(ino, stat.Mode)
		}
	} else if err != nil && !isNotFoundErr(err) {
		log.Printf("post-symlink stat refresh failed for %s: %v", childP, err)
	}

	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}
	parentPath, _ := fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(linkName, entry))
	fs.invalidateReadCacheAndTargets(childP)
	fs.fillEntryOut(entry, out)
	return gofuse.OK
}

func (fs *Dat9FS) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseUnlink, perfStart, status, 0) }()
	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	start := time.Now()
	status = gofuse.OK
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
	// Wait for any in-flight commitQueue upload and cancel it so the
	// background commit cannot resurrect the deleted file.
	if fs.commitQueue != nil {
		waitStart := time.Now()
		fs.debugf("unlink wait commit start path=%s", childP)
		fs.commitQueue.WaitPath(childP)
		fs.debugDurationf(waitStart, 0, "unlink wait commit done path=%s", childP)

		// Re-check pendingIndex after WaitPath but before CancelPath. On a
		// successful commit, commitQueue removes pendingIndex before taking the
		// entry out of inFlight/queue, so after WaitPath this distinguishes:
		// still PendingNew => never uploaded; missing => uploaded or remote file.
		if fs.pendingIndex != nil {
			if meta, ok := fs.pendingIndex.GetMeta(childP); ok {
				pendingNew = meta.Kind == PendingNew
			} else {
				pendingNew = false
			}
		}
		fs.commitQueue.CancelPath(childP)
	} else {
		// Also check pendingIndex for the pending-new flag before clearing.
		if !pendingNew && fs.pendingIndex != nil {
			if meta, ok := fs.pendingIndex.GetMeta(childP); ok && meta.Kind == PendingNew {
				pendingNew = true
			}
		}
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
		err := fs.deleteRemoteFileWithInterruptRecovery(ctx, childP)
		fs.debugDurationf(deleteStart, 0, "unlink remote delete done path=%s err=%v", childP, err)
		if err != nil {
			if !isNotFoundErr(err) {
				status = httpToFuseStatus(err)
				return status
			}
		}
	}

	fs.inodes.Remove(childP)
	fs.invalidateReadCacheAndTargets(childP)

	parentPath, _ := fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Remove(parentPath, name)
	fs.cacheNegativePath(childP)
	// Kernel initiated the unlink and receives OK — it already
	// removes the dentry. No notifyEntry/notifyInode needed.
	return gofuse.OK
}

func (fs *Dat9FS) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseRmdir, perfStart, status, 0) }()
	ctx, cf := fuseCtx(cancel)
	defer cf()

	childP, st := fs.childPath(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	start := time.Now()
	status = gofuse.OK
	fs.debugf("rmdir start path=%s parent_ino=%d name=%s", childP, header.NodeId, name)
	defer func() {
		fs.debugf("rmdir done path=%s status=%d dur=%s", childP, status, time.Since(start))
	}()

	deleteStart := time.Now()
	fs.debugf("rmdir remote delete start path=%s", childP)
	err := fs.deleteRemoteDirWithInterruptRecovery(ctx, childP)
	fs.debugDurationf(deleteStart, 0, "rmdir remote delete done path=%s err=%v", childP, err)
	if err != nil {
		status = httpToFuseStatus(err)
		return status
	}

	// Clean write-back entries for files under the removed directory.
	// Without this, the uploader would try to PUT to paths under a deleted dir.
	prefix := childP + "/"
	if fs.writeBack != nil {
		for _, meta := range fs.writeBack.ListByPrefix(prefix) {
			if fs.uploader != nil {
				waitStart := time.Now()
				fs.debugf("rmdir wait writeback start path=%s child=%s", childP, meta.Path)
				fs.uploader.WaitPath(meta.Path)
				fs.debugDurationf(waitStart, 0, "rmdir wait writeback done path=%s child=%s", childP, meta.Path)
			}
			fs.writeBack.Remove(meta.Path)
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
	fs.dirCache.InvalidatePrefix(childP)
	fs.readCache.InvalidatePrefix(childP + "/")

	parentPath, _ := fs.inodes.GetPath(header.NodeId)
	fs.dirCache.Remove(parentPath, name)
	fs.cacheNegativePath(childP)
	// Kernel initiated the rmdir and receives OK — it already
	// removes the dentry. No notifyEntry/notifyInode needed.
	return gofuse.OK
}

type pendingRenameResult int

const (
	pendingRenameNotApplicable pendingRenameResult = iota
	pendingRenameRemoteFallback
	pendingRenameRemoteFallbackCleanupOld
	pendingRenameHandled
)

func (fs *Dat9FS) renamePendingNewCommit(ctx context.Context, input *gofuse.RenameIn, oldP, newP string) (pendingRenameResult, error) {
	if fs.pendingIndex == nil {
		return pendingRenameNotApplicable, nil
	}
	meta, ok := fs.pendingIndex.GetMeta(oldP)
	if !ok || meta.Kind != PendingNew {
		return pendingRenameNotApplicable, nil
	}

	// Only use the local fast path when the final path is truly absent.
	// Git lockfile replacement (for example config.lock -> config) must keep
	// the old server-side rename semantics so the existing target is replaced
	// atomically after the temp file upload completes.
	if fs.commitQueue != nil {
		fs.commitQueue.WaitPath(newP)
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	targetExists, err := fs.pendingRenameTargetExists(probeCtx, newP)
	probeCancel()
	if err != nil {
		log.Printf("rename: probe final pending-new target %s failed, using remote fallback: %v", newP, err)
		return pendingRenameRemoteFallback, nil
	}
	if targetExists {
		return pendingRenameRemoteFallback, nil
	}

	if fs.commitQueue != nil {
		fs.commitQueue.CancelPathPreserveLocal(oldP)
		fs.commitQueue.WaitPath(oldP)

		// The cancel may have raced with a successful upload. If so, the old
		// path now exists remotely and the normal server-side rename is correct.
		oldRemoteExists, err := fs.remotePathExistsDetached(oldP)
		if err != nil {
			log.Printf("rename: probe old pending-new source %s failed, using remote fallback: %v", oldP, err)
			return pendingRenameRemoteFallback, nil
		}
		if oldRemoteExists {
			// The caller still needs to execute the remote rename. Keep local
			// state until that succeeds so a remote rename failure can be
			// retried from the preserved shadow/pending entry.
			return pendingRenameRemoteFallbackCleanupOld, nil
		}
		meta, ok = fs.pendingIndex.GetMeta(oldP)
		if !ok || meta.Kind != PendingNew {
			return pendingRenameRemoteFallback, nil
		}
		oldExistsRemote, oldExistsErr := fs.remotePathExistsDetached(oldP)
		if oldExistsErr != nil {
			return pendingRenameHandled, oldExistsErr
		}
		if oldExistsRemote {
			if err := fs.renameRemoteWithTransientRetry(ctx, oldP, newP); err != nil {
				return pendingRenameHandled, err
			}
			if fs.shadowStore != nil {
				fs.shadowStore.Remove(oldP)
			}
			fs.pendingIndex.Remove(oldP)
			fs.finishLocalRename(input, oldP, newP)
			return pendingRenameHandled, nil
		}
	}

	if fs.shadowStore != nil {
		if !fs.shadowStore.Rename(oldP, newP) {
			return pendingRenameHandled, fmt.Errorf("move pending shadow %s -> %s failed", oldP, newP)
		}
	}
	if !fs.pendingIndex.RenamePending(oldP, newP) {
		if fs.shadowStore != nil {
			_ = fs.shadowStore.Rename(newP, oldP)
		}
		return pendingRenameHandled, fmt.Errorf("move pending index %s -> %s failed", oldP, newP)
	}

	fs.finishLocalRename(input, oldP, newP)

	if fs.commitQueue != nil {
		ino, _ := fs.inodes.GetInode(newP)
		entry := &CommitEntry{
			Path:        newP,
			Inode:       ino,
			BaseRev:     meta.BaseRev,
			Size:        meta.Size,
			Kind:        meta.Kind,
			ShadowSpill: meta.ShadowSpill,
			Mode:        meta.Mode,
			HasMode:     meta.HasMode,
		}
		if isGitLooseObjectFinalPath(newP) {
			// Git treats a successful tmp_obj_* -> <sha> rename as making the
			// object database complete. Do not acknowledge that rename while the
			// content-addressed object is only queued for best-effort upload.
			if commitErr := fs.commitQueue.CommitNow(ctx, entry); commitErr != nil {
				return pendingRenameHandled, fmt.Errorf("sync commit git loose object rename %s: %w", newP, commitErr)
			}
		} else if err := fs.commitQueue.Enqueue(entry); err != nil {
			log.Printf("rename: enqueue pending-new commit for %s failed, falling back to sync commit: %v", newP, err)
			if commitErr := fs.commitQueue.CommitNow(ctx, entry); commitErr != nil {
				return pendingRenameHandled, fmt.Errorf("sync commit pending-new rename %s: %w", newP, commitErr)
			}
		}
	}
	return pendingRenameHandled, nil
}

func (fs *Dat9FS) pendingRenameTargetExists(ctx context.Context, p string) (bool, error) {
	if fs.pendingIndex != nil {
		if _, ok := fs.pendingIndex.GetMeta(p); ok {
			return true, nil
		}
	}
	if fs.writeBack != nil {
		if _, ok := fs.writeBack.GetMeta(p); ok {
			return true, nil
		}
	}
	if fs.commitQueue != nil && fs.commitQueue.HasPath(p) {
		return true, nil
	}
	statStart := fs.perfStart()
	_, err := fs.client.StatCtx(ctx, fs.remotePath(p))
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	if err == nil {
		return true, nil
	}
	if isNotFoundErr(err) {
		return false, nil
	}
	return false, err
}

func (fs *Dat9FS) finishLocalRename(input *gofuse.RenameIn, oldP, newP string) {
	var oldEntry *InodeEntry
	oldEntryOK := false
	if oldIno, ok := fs.inodes.GetInode(oldP); ok {
		oldEntry, oldEntryOK = fs.inodes.GetEntry(oldIno)
	}
	fs.inodes.Rename(oldP, newP)
	fs.invalidateReadCacheAndTargets(oldP)
	fs.invalidateReadCacheAndTargets(newP)
	fs.readCache.InvalidatePrefix(oldP + "/")
	fs.readCache.InvalidatePrefix(newP + "/")

	oldParent, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Remove(oldParent, path.Base(oldP))
	fs.cacheNegativePath(oldP)
	fs.dirCache.InvalidatePrefix(oldP)
	fs.dirCache.InvalidatePrefix(newP)
	newParent := oldParent
	if input.Newdir != input.NodeId {
		newParent, _ = fs.inodes.GetPath(input.Newdir)
	}
	if oldEntryOK {
		fs.dirCache.Upsert(newParent, cachedInfoFromEntry(path.Base(newP), oldEntry))
	}
	fs.retargetOpenHandlesForRename(oldP, newP)
}

func (fs *Dat9FS) retargetOpenHandlesForRename(oldP, newP string) {
	for fh, currentPath := range fs.openHandles.RenamePathPrefix(oldP, newP) {
		if fh == nil {
			continue
		}
		fh.Lock()
		fh.Path = currentPath
		fh.ReadTarget = nil
		if fh.Dirty != nil {
			fh.Dirty.path = currentPath
		}
		if fh.Streamer != nil {
			fh.Streamer.SetPath(currentPath, fs.remoteRoot())
		}
		if fh.Prefetch != nil {
			fh.Prefetch.SetPath(fs.remotePath(currentPath))
			fh.Prefetch.SetReadTarget(nil)
		}
		fh.Unlock()
	}
}

func (fs *Dat9FS) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseRename, perfStart, status, 0) }()
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
	if oldP == newP {
		return gofuse.OK
	}

	pendingRename, err := fs.renamePendingNewCommit(ctx, input, oldP, newP)
	if err != nil {
		log.Printf("rename: pending-new local rename %s -> %s failed: %v", oldP, newP, err)
		return httpToFuseStatus(err)
	}
	if pendingRename == pendingRenameHandled {
		return gofuse.OK
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
		if !isPendingNew {
			pendingRename, err := fs.renamePendingNewCommit(ctx, input, oldP, newP)
			if err != nil {
				log.Printf("rename: pending-new local rename %s -> %s failed: %v", oldP, newP, err)
				return httpToFuseStatus(err)
			}
			if pendingRename == pendingRenameHandled {
				return gofuse.OK
			}
		}
		if isPendingNew {
			fs.finishLocalRename(input, oldP, newP)
			// Kernel initiated the rename and receives OK. Lock files opt out
			// of entry caching at create/lookup time, so the next O_CREAT|O_EXCL
			// revalidates without a synchronous EntryNotify from this handler.
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

	if err := fs.renameRemoteWithTransientRetry(ctx, oldP, newP); err != nil {
		return httpToFuseStatus(err)
	}
	if pendingRename == pendingRenameRemoteFallbackCleanupOld {
		if fs.shadowStore != nil {
			fs.shadowStore.Remove(oldP)
		}
		if fs.pendingIndex != nil {
			fs.pendingIndex.Remove(oldP)
		}
	}

	// After server-side rename, migrate pending descendants.
	// If oldP is a directory, pending children under oldP+"/", must be
	// re-keyed to newP+"/". Without this the uploader would PUT to stale paths.
	prefix := oldP + "/"
	if fs.writeBack != nil {
		for _, meta := range fs.writeBack.ListByPrefix(prefix) {
			newChild := newP + meta.Path[len(oldP):]
			if fs.uploader != nil {
				fs.uploader.WaitPath(meta.Path)
			}
			fs.writeBack.RenamePending(meta.Path, newChild)
			if fs.uploader != nil {
				fs.uploader.Submit(newChild)
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

	fs.finishLocalRename(input, oldP, newP)
	return gofuse.OK
}

func (fs *Dat9FS) OpenDir(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseOpenDir, perfStart, status, 0) }()
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

func (fs *Dat9FS) ReadDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseReadDir, perfStart, status, 0) }()
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

func (fs *Dat9FS) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseReadDirPlus, perfStart, status, 0) }()
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
		if _, ok := fs.inodes.GetEntry(e.Ino); !ok {
			e.Ino = fs.recreateDirEntryInode(dh.Path, e)
			dh.Entries[i].Ino = e.Ino
		}
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

func (fs *Dat9FS) recreateDirEntryInode(dirPath string, e DirEntry) uint64 {
	childP := dirEntryChildPath(dirPath, e.Name)
	if ino, ok := fs.inodes.GetInode(childP); ok {
		return ino
	}

	isDir := e.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFDIR)
	size := int64(0)
	mtime := time.Now()
	var revision int64
	var mode uint32
	hasMode := false

	if e.HasMetadata {
		isDir = e.IsDir
		size = e.Size
		if !e.Mtime.IsZero() {
			mtime = e.Mtime
		}
		revision = e.Revision
		mode = e.AttrMode
		hasMode = e.HasMode
	} else if item, ok := fs.dirEntryMetadata(dirPath, childP, e.Name); ok {
		isDir = item.IsDir
		size = item.Size
		if !item.Mtime.IsZero() {
			mtime = item.Mtime
		}
		revision = item.Revision
		mode = item.Mode
		hasMode = item.HasMode
	}

	ino := fs.inodes.EnsureInodeNoUpdate(childP, isDir, size, mtime)
	if revision > 0 {
		fs.inodes.UpdateRevision(ino, revision)
	}
	if hasMode {
		fs.inodes.UpdateMode(ino, mode)
	}
	return ino
}

func (fs *Dat9FS) dirEntryMetadata(dirPath, childP, name string) (CachedFileInfo, bool) {
	if fs.writeBack != nil {
		if meta, ok := fs.writeBack.GetMeta(childP); ok {
			return cachedInfoFromWriteBackMeta(name, meta), true
		}
	}
	if fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(childP); ok {
			return cachedInfoFromWriteBackMeta(name, meta), true
		}
	}
	if fs.dirCache != nil {
		result := fs.dirCache.Lookup(dirPath, name)
		if result.kind == namespaceLookupPositive {
			return result.item, true
		}
	}
	return CachedFileInfo{}, false
}

func dirEntryFromCachedInfo(item CachedFileInfo, ino uint64) DirEntry {
	return DirEntry{
		Name:        item.Name,
		Ino:         ino,
		Mode:        dirEntryMode(item.IsDir, item.HasMode, item.Mode),
		Size:        item.Size,
		Mtime:       item.Mtime,
		Revision:    item.Revision,
		AttrMode:    item.Mode,
		HasMode:     item.HasMode,
		IsDir:       item.IsDir,
		HasMetadata: true,
	}
}

func (fs *Dat9FS) ReleaseDir(input *gofuse.ReleaseIn) {
	fs.dirHandles.Delete(input.Fh)
}

func (fs *Dat9FS) listDir(ctx context.Context, dirPath string) ([]DirEntry, error) {
	// Check dir cache first
	if cached, ok := fs.dirCache.Get(dirPath); ok {
		if fs.perf != nil {
			fs.perf.dirCacheHit.add(1)
		}
		entries := fs.cachedToDirEntries(dirPath, cached)
		return fs.mergePendingDirEntries(dirPath, entries), nil
	}
	if fs.perf != nil {
		fs.perf.dirCacheMiss.add(1)
	}

	listStart := fs.perfStart()
	items, err := fs.client.ListCtx(ctx, fs.remotePath(dirPath))
	fs.perfRecordRemote(perfRemoteList, listStart, err, 0)
	if err != nil {
		return nil, err
	}

	// Store in dir cache
	cached := cachedFileInfos(items)
	fs.applyBatchStats(ctx, dirPath, cached)
	fs.dirCache.Put(dirPath, cached)
	fs.prefetchReadCacheForDir(ctx, dirPath, cached)

	entries := fs.cachedToDirEntries(dirPath, cached)
	return fs.mergePendingDirEntries(dirPath, entries), nil
}

func (fs *Dat9FS) applyBatchStats(ctx context.Context, dirPath string, items []CachedFileInfo) {
	if len(items) == 0 {
		return
	}
	for start := 0; start < len(items); start += client.MaxBatchStatPaths {
		end := start + client.MaxBatchStatPaths
		if end > len(items) {
			end = len(items)
		}
		paths := make([]string, end-start)
		for i := start; i < end; i++ {
			paths[i-start] = fs.remotePath(dirEntryChildPath(dirPath, items[i].Name))
		}
		results, err := fs.client.BatchStatCtx(ctx, paths)
		if err != nil {
			log.Printf("batch stat failed for %s entries %d-%d: %v", dirPath, start, end, err)
			return
		}
		for i, result := range results {
			if !result.OK() {
				continue
			}
			item := &items[start+i]
			item.Size = result.Size
			item.IsDir = result.IsDir
			if result.Mtime > 0 {
				item.Mtime = time.Unix(result.Mtime, 0)
			}
			item.Revision = result.Revision
			item.HasMode = result.HasMode
			item.Mode = result.Mode
		}
	}
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
		prefix := dirPath
		if prefix != "/" {
			prefix += "/"
		}
		for _, meta := range fs.writeBack.ListByPrefix(prefix) {
			if parentDir(meta.Path) != dirPath {
				continue
			}
			name := path.Base(meta.Path)
			if _, ok := existing[name]; ok {
				continue
			}
			mtime := meta.Mtime
			if mtime.IsZero() {
				mtime = time.Now()
			}
			ino := fs.inodes.EnsureInode(meta.Path, false, meta.Size, mtime)
			if meta.HasMode {
				fs.inodes.UpdateMode(ino, meta.Mode)
			}
			entries = append(entries, DirEntry{
				Name:        name,
				Ino:         ino,
				Mode:        dirEntryMode(false, meta.HasMode, meta.Mode),
				Size:        meta.Size,
				Mtime:       mtime,
				AttrMode:    meta.Mode,
				HasMode:     meta.HasMode,
				IsDir:       false,
				HasMetadata: true,
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
			if meta.HasMode {
				fs.inodes.UpdateMode(ino, meta.Mode)
			}
			entries = append(entries, DirEntry{
				Name:        name,
				Ino:         ino,
				Mode:        dirEntryMode(false, meta.HasMode, meta.Mode),
				Size:        meta.Size,
				Mtime:       mtime,
				AttrMode:    meta.Mode,
				HasMode:     meta.HasMode,
				IsDir:       false,
				HasMetadata: true,
			})
			existing[name] = struct{}{}
		}
	}

	return entries
}

func (fs *Dat9FS) cachedToDirEntries(dirPath string, items []CachedFileInfo) []DirEntry {
	entries := make([]DirEntry, 0, len(items))
	for _, item := range items {
		childP := dirEntryChildPath(dirPath, item.Name)

		mtime := item.Mtime
		if mtime.IsZero() {
			mtime = time.Now()
		}
		ino := fs.inodes.EnsureInode(childP, item.IsDir, item.Size, mtime)
		if item.Revision > 0 {
			fs.inodes.UpdateRevision(ino, item.Revision)
		}
		if item.HasMode {
			fs.inodes.UpdateMode(ino, item.Mode)
		}

		entries = append(entries, dirEntryFromCachedInfo(item, ino))
	}
	return entries
}

func dirEntryChildPath(dirPath, name string) string {
	if dirPath == "/" {
		return "/" + name
	}
	return dirPath + "/" + name
}

// --- File operations ---------------------------------------------------------

func (fs *Dat9FS) Create(cancel <-chan struct{}, input *gofuse.CreateIn, name string, out *gofuse.CreateOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseCreate, perfStart, status, 0) }()
	if fs.opts.ReadOnly {
		return gofuse.EROFS
	}

	childP, st := fs.childPath(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}

	mode, hasRemoteMode := createInputMode(input.Mode)
	ino := fs.inodes.Lookup(childP, false, 0, time.Now())
	fs.inodes.UpdateMode(ino, mode)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return gofuse.EIO
	}

	wb := fs.newWriteBuffer(childP, streamingWriteMaxSize, 0)
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
		WritePolicy: fs.writePolicyForOpen(input.Flags),
	}
	if hasRemoteMode {
		fh.PendingMode = mode
		fh.HasPendingMode = true
	}

	if fh.WritePolicy != WritePolicyWriteSync && fs.shadowStore != nil && fs.pendingIndex != nil {
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
	} else if fh.WritePolicy != WritePolicyWriteSync {
		// Normal mode: attach streaming uploader for sequential write streaming.
		fh.Streamer = NewStreamUploader(fs.client, childP, expectedRevisionForHandle(fh), fs.remoteRoot())
		streamer := fh.Streamer
		wb.OnPartFull = func(partIdx int, data []byte) {
			partNum := partIdx + 1
			if err := streamer.SubmitPart(context.Background(), partNum, data, nil); err != nil {
				log.Printf("streaming submit part %d failed for %s: %v", partNum, childP, err)
			}
		}
	}

	fh.DirtySeq = fs.markDirtySize(ino, 0)
	out.Fh = fs.allocateFileHandle(fh)
	// Use cached I/O for small/interactive files. Kernel coalesces writes
	// and serves reads from page cache after first access.
	// Keep DIRECT_IO for O_TRUNC streaming files.
	out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
	fs.fillEntryOut(entry, &out.EntryOut)

	parentPath, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(name, entry))
	// Kernel initiated the create and receives the new entry via reply —
	// no need for notifyEntry/notifyInode here.
	return gofuse.OK
}

func (fs *Dat9FS) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseOpen, perfStart, status, 0) }()
	ctx, cf := fuseCtx(cancel)
	defer cf()

	p, ok := fs.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}

	fh := &FileHandle{
		Ino:         input.NodeId,
		Path:        p,
		Flags:       input.Flags,
		OpenPID:     input.Pid,
		WritePolicy: fs.writePolicyForOpen(input.Flags),
	}
	entry, _ := fs.inodes.GetEntry(input.NodeId)
	if entry != nil {
		if entryIsSymlink(entry) {
			return gofuse.Status(syscall.ELOOP)
		}
		fh.OrigSize = entry.Size
		fh.BaseRev = entry.Revision
	}

	// Allocate write buffer for writable opens
	accMode := input.Flags & syscall.O_ACCMODE
	if accMode == syscall.O_WRONLY || accMode == syscall.O_RDWR {
		if fs.opts.ReadOnly {
			return gofuse.EROFS
		}

		fh.Dirty = fs.newWriteBuffer(p, maxPreloadSize, 0)

		// Preload existing content for non-truncating opens so that
		// random writes don't discard the original file data.
		if input.Flags&syscall.O_TRUNC == 0 {
			preloaded := false
			if fs.loadWritableHandleFromOpenHandleLocked(fh) {
				preloaded = true
			}
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
			if !preloaded && fs.loadWritableHandleFromWriteBackLocked(fh) {
				preloaded = true
			}

			// If BaseRev is still 0 (e.g. inode came from readdir without
			// revision, or a legacy pending overwrite lacks baseRev), fetch the
			// authoritative revision so CAS uploads work correctly.
			if fh.BaseRev == 0 && !fh.IsNew {
				statStart := fs.perfStart()
				stat, err := fs.client.StatCtx(ctx, fs.remotePath(p))
				fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
				if err == nil && stat != nil {
					fh.BaseRev = stat.Revision
					fs.inodes.UpdateRevision(input.NodeId, stat.Revision)
				}
			}
			if !preloaded && fs.preloadWritableHandleFromReadCacheLocked(fh) {
				preloaded = true
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
			if fh.WritePolicy != WritePolicyWriteSync && fs.shadowStore != nil && fs.pendingIndex != nil {
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
			} else if fh.WritePolicy != WritePolicyWriteSync {
				// Normal mode: attach streaming uploader with OnPartFull wiring.
				fh.Streamer = NewStreamUploader(fs.client, p, expectedRevisionForHandle(fh), fs.remoteRoot())
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
		if entry != nil && entry.Size > fs.readCache.MaxFileSize() {
			fh.Prefetch = NewPrefetcher(fs.client, fs.remotePath(p), entry.Size, fs.debugEnabled())
			fh.Prefetch.SetPerfCounters(fs.perf)
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

	out.Fh = fs.allocateFileHandle(fh)
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
		// Read-only opens need kernel caching so mmap-based readers (notably
		// git pack access) do not SIGBUS on macFUSE. Prefetch-backed reads
		// still populate the userspace prefetcher on cache misses.
		out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
	}
	fs.debugf("open path=%s fh=%d ino=%d flags=0x%x open_flags=%d dirty=%t prefetch=%t orig_size=%d base_rev=%d shadow_ready=%t shadow_spill=%t write_policy=%s", p, out.Fh, fh.Ino, input.Flags, out.OpenFlags, fh.Dirty != nil, fh.Prefetch != nil, fh.OrigSize, fh.BaseRev, fh.ShadowReady, fh.ShadowSpill, fh.WritePolicy)
	return gofuse.OK
}

func (fs *Dat9FS) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (result gofuse.ReadResult, status gofuse.Status) {
	start := time.Now()
	logPath := ""
	var logIno uint64
	source := "unknown"
	bytesRead := -1
	defer func() {
		var perfBytes uint64
		if bytesRead > 0 {
			perfBytes = uint64(bytesRead)
		}
		fs.perfRecordFuse(perfFuseRead, start, status, perfBytes)
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
		// serve already-loaded ranges from memory and fall back to the server
		// only when the requested range still has unloaded parts.
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			source = "dirty-clean-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}
		if end <= offset {
			fh.Unlock()
			source = "dirty-clean-empty"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK
		}
		ps := fh.Dirty.PartSize()
		firstPart := int(offset / ps)
		lastPart := int((end - 1) / ps)
		fullyLoaded := true
		for p := firstPart; p <= lastPart; p++ {
			if !fh.Dirty.IsPartLoaded(p) {
				fullyLoaded = false
				break
			}
		}
		if fullyLoaded {
			result := make([]byte, end-offset)
			fh.Dirty.ReadAt(offset, result)
			fh.Unlock()
			source = "dirty-clean-buffer"
			bytesRead = len(result)
			return gofuse.ReadResultData(result), gofuse.OK
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
		if wbData, ok := fs.writeBack.getView(fh.Path); ok {
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
			if fs.perf != nil {
				fs.perf.prefetchHit.add(1)
			}
			// Trigger next prefetch
			fh.Prefetch.OnRead(offset, len(data))
			source = "prefetch-hit"
			bytesRead = len(data)
			return gofuse.ReadResultData(data), gofuse.OK
		}
		if fs.perf != nil {
			fs.perf.prefetchMiss.add(1)
		}
		// Cache miss — fall through to direct read. Prefetch is triggered
		// only after a successful read (see below), not unconditionally.
		source = "prefetch-miss-range"
	}

	// Try read cache for small files. Use revision-aware cache: if the
	// InodeEntry has a stored revision from the last Lookup/GetAttr, pass
	// it to the cache for validation. Cache hit only if revision matches.
	entry, _ := fs.inodes.GetEntry(fh.Ino)
	if entry != nil && entry.Size <= fs.readCache.MaxFileSize() && entry.Size > 0 {
		cacheRev := entry.Revision // use revision from last Stat/Lookup
		// Fast path: serve from cache without any HTTP call.
		if data, ok := fs.readCache.Get(p, cacheRev); ok {
			if fs.perf != nil {
				fs.perf.readCacheHit.add(1)
			}
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
		if fs.perf != nil {
			fs.perf.readCacheMiss.add(1)
		}

		// Cache miss: read the file and store it. No separate Stat needed —
		// ReadCtx fetches the data in one round-trip. Uses detached retry
		// so a single FUSE interrupt doesn't permanently return EAGAIN.
		data, err := func() ([]byte, error) {
			releaseReadSlot, err := fs.acquireRemoteReadSlot(ctx)
			if err != nil {
				return nil, err
			}
			defer releaseReadSlot()
			return fs.readSmallFileWithRetry(ctx, p)
		}()
		if err != nil {
			source = "small-read-error"
			if errors.Is(err, errReadRetriesExhausted) {
				return nil, gofuse.EIO
			}
			return nil, httpToFuseStatus(err)
		}
		// Store with the revision from the prior Stat/Lookup.
		fs.readCache.PutOwned(p, data, cacheRev)
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
	data, n, err := func() ([]byte, int, error) {
		releaseReadSlot, err := fs.acquireRemoteReadSlot(ctx)
		if err != nil {
			return nil, 0, err
		}
		defer releaseReadSlot()
		return fs.readStreamRangeWithRetry(ctx, p, fh, int64(input.Offset), int64(input.Size))
	}()
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
		fs.perfRecordFuse(perfFuseWrite, start, status, uint64(written))
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
		fh.Dirty = fs.newWriteBuffer(fh.Path, 0, 0)
	}
	writeSyncSnapshot := (*writeBufferSnapshot)(nil)
	if fh.WritePolicy == WritePolicyWriteSync {
		writeSyncSnapshot = fh.Dirty.snapshot()
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
	if fh.WritePolicy == WritePolicyWriteSync {
		size := fh.Dirty.Size()
		ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(size))
		defer cf()
		source = "write-sync"
		st := fs.syncWriteHandleToRemoteLocked(ctx, fh)
		if st != gofuse.OK {
			fs.restoreFailedWriteSyncLocked(fh, writeSyncSnapshot)
			return 0, st
		}
	}
	return n, gofuse.OK
}

func (fs *Dat9FS) restoreFailedWriteSyncLocked(fh *FileHandle, snapshot *writeBufferSnapshot) {
	if fh == nil {
		return
	}
	if fh.DirtySeq != 0 {
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
	}
	if snapshot == nil {
		fh.Dirty = nil
	} else {
		if fh.Dirty == nil {
			fh.Dirty = fs.newWriteBuffer(fh.Path, snapshot.maxSize, snapshot.partSize)
		}
		fh.Dirty.restore(snapshot)
		if fh.Dirty.HasDirtyParts() {
			fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
		}
		fs.inodes.UpdateSize(fh.Ino, fh.Dirty.Size())
	}
	fh.WriteBackSeq = 0
	clearReadTargetForLockedHandle(fh)
	if fs.shadowStore != nil && fh.ShadowReady {
		fs.shadowStore.Remove(fh.Path)
		fh.ShadowReady = false
		fh.ShadowSpill = false
		fh.ShadowCommitReady = false
	}
}

// syncWriteHandleToRemoteLocked makes the current handle contents
// remote-durable for a write-sync Write call. Caller must hold fh.mu and this
// method keeps it held for the whole upload so no same-handle write can mutate
// the dirty buffer before the syscall returns.
func (fs *Dat9FS) syncWriteHandleToRemoteLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
	if fh == nil || fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		return gofuse.OK
	}

	size := fh.Dirty.Size()
	expectedRevision := expectedRevisionForHandle(fh)
	var (
		data         []byte
		committedRev int64
		err          error
	)

	threshold := fs.negotiatedInlineThreshold()
	useDirectPUT := size == 0 || (threshold > 0 && size < threshold)
	if useDirectPUT || fh.OrigSize < threshold {
		data = fh.Dirty.bytesView()
	}

	if useDirectPUT {
		writeStart := time.Now()
		if size == 0 && fh.IsNew {
			fs.debugf("write-sync empty create start path=%s expected_rev=%d", fh.Path, expectedRevision)
			committedRev, err = fs.client.CreateFileCtx(ctx, fs.remotePath(fh.Path))
			if isCreateActionUnsupportedErr(err) {
				fs.debugf("write-sync empty create unsupported path=%s fallback=small-write err=%v", fh.Path, err)
				committedRev, err = fs.client.WriteCtxConditionalWithRevision(ctx, fs.remotePath(fh.Path), data, expectedRevision)
			}
			fs.perfRecordRemote(perfRemoteWrite, writeStart, err, 0)
			fs.debugDurationf(writeStart, 0, "write-sync empty create done path=%s committed_rev=%d err=%v", fh.Path, committedRev, err)
		} else {
			fs.debugf("write-sync small write start path=%s size=%d expected_rev=%d", fh.Path, size, expectedRevision)
			committedRev, err = fs.client.WriteCtxConditionalWithRevision(ctx, fs.remotePath(fh.Path), data, expectedRevision)
			fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(len(data)))
			fs.debugDurationf(writeStart, 0, "write-sync small write done path=%s size=%d committed_rev=%d err=%v", fh.Path, size, committedRev, err)
		}
	} else if fh.OrigSize >= threshold {
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
			fs.debugf("write-sync patch start path=%s size=%d dirty_parts=%d expected_rev=%d", fh.Path, size, len(dirtyParts), expectedRevision)
			err = fs.client.PatchFile(
				ctx,
				fs.remotePath(fh.Path),
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
			var patchBytes uint64
			if size > 0 {
				patchBytes = uint64(size)
			}
			fs.perfRecordRemote(perfRemoteWrite, patchStart, err, patchBytes)
			fs.debugDurationf(patchStart, 0, "write-sync patch done path=%s size=%d dirty_parts=%d err=%v", fh.Path, size, len(dirtyParts), err)
		}
	} else {
		if data == nil && !fh.Dirty.CanMaterializeFull() {
			log.Printf("write-sync cannot materialize full file for %s", fh.Path)
			return gofuse.EIO
		}
		if data == nil {
			data = fh.Dirty.bytesView()
		}
		writeStart := time.Now()
		fs.debugf("write-sync stream start path=%s size=%d expected_rev=%d", fh.Path, size, expectedRevision)
		err = fs.client.WriteStreamConditional(
			ctx,
			fs.remotePath(fh.Path),
			bytes.NewReader(data),
			size,
			nil,
			expectedRevision,
		)
		fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(len(data)))
		fs.debugDurationf(writeStart, 0, "write-sync stream done path=%s size=%d err=%v", fh.Path, size, err)
	}
	if err != nil {
		log.Printf("write-sync upload failed for %s: %v", fh.Path, err)
		return httpToFuseStatus(err)
	}
	if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
		log.Printf("write-sync pending chmod failed for %s: %v", fh.Path, err)
		return httpToFuseStatus(err)
	}

	fh.Dirty.ClearDirty()
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	if committedRev > 0 {
		clearReadTargetForLockedHandle(fh)
		fs.clearReadTargetsForPathExcept(fh.Path, fh)
		fs.readCache.Put(fh.Path, data, committedRev)
		fh.IsNew = false
		fh.BaseRev = committedRev
		fs.inodes.UpdateRevision(fh.Ino, committedRev)
		if fh.ZeroBase && fh.Dirty != nil && fh.Dirty.Size() > 0 {
			fh.ZeroBase = false
		}
	} else {
		clearReadTargetForLockedHandle(fh)
		fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
	}
	fs.inodes.UpdateSize(fh.Ino, size)
	fs.cacheFileForPath(fh.Path, size, time.Now(), committedRev)
	return gofuse.OK
}

// syncHandleToRemoteLocked makes the current dirty handle remote-durable.
// Caller must hold fh.mu. The method may temporarily release fh.mu around
// network I/O, matching flushHandle's locking contract.
func (fs *Dat9FS) syncHandleToRemoteLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
	if fh == nil || fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		return gofuse.OK
	}

	size := fh.Dirty.Size()
	if fh.ShadowSpill && fs.shadowStore != nil {
		expectedRevision := expectedRevisionForHandle(fh)
		uploadStart := time.Now()
		fs.debugf("sync handle shadowspill upload start path=%s size=%d expected_rev=%d", fh.Path, size, expectedRevision)
		fh.Unlock()
		err := uploadFromShadowRemote(ctx, fs.client, fs.shadowStore, fh.Path, fs.remotePath(fh.Path), expectedRevision)
		fh.Lock()
		var uploadBytes uint64
		if size > 0 {
			uploadBytes = uint64(size)
		}
		fs.perfRecordRemote(perfRemoteWrite, uploadStart, err, uploadBytes)
		fs.debugDurationf(uploadStart, 0, "sync handle shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
		if err != nil {
			log.Printf("sync handle shadowspill upload failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}
		if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
			log.Printf("sync handle shadowspill pending chmod failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}
		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		fh.ShadowCommitReady = false
		clearReadTargetForLockedHandle(fh)
		fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
		fs.inodes.UpdateSize(fh.Ino, size)
		fs.cacheFileForPath(fh.Path, size, time.Now(), 0)
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
		if fs.shadowStore != nil {
			fs.shadowStore.Remove(fh.Path)
			fh.ShadowReady = false
			fh.ShadowSpill = false
			fh.ShadowCommitReady = false
		}
		if fs.pendingIndex != nil {
			fs.pendingIndex.Remove(fh.Path)
		}
		return gofuse.OK
	}

	return fs.flushHandle(ctx, fh)
}

func (fs *Dat9FS) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) (status gofuse.Status) {
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseFlush, perfStart, status, 0) }()
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

	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() &&
		(fh.WritePolicy == WritePolicyCloseSync || fh.WritePolicy == WritePolicyWriteSync) {
		size := fh.Dirty.Size()
		ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(size))
		defer cf()
		phase = fh.WritePolicy.String()
		return fs.syncHandleToRemoteLocked(ctx, fh)
	}

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
			err := uploadFromShadowRemote(ctx, fs.client, fs.shadowStore, fh.Path, fs.remotePath(fh.Path), expectedRevisionForHandle(fh))
			fh.Lock()
			var uploadBytes uint64
			if size > 0 {
				uploadBytes = uint64(size)
			}
			fs.perfRecordRemote(perfRemoteWrite, uploadStart, err, uploadBytes)
			fs.debugDurationf(uploadStart, 0, "flush shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
			if err != nil {
				log.Printf("flush: ShadowSpill sync upload failed for %s: %v", fh.Path, err)
				return gofuse.EIO
			}
			if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
				log.Printf("flush: ShadowSpill pending chmod failed for %s: %v", fh.Path, err)
				return httpToFuseStatus(err)
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
	perfStart := fs.perfStart()
	defer func() { fs.perfRecordFuse(perfFuseFsync, perfStart, status, 0) }()
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
		err := uploadFromShadowRemote(ctx, fs.client, fs.shadowStore, fh.Path, fs.remotePath(fh.Path), expectedRevisionForHandle(fh))
		fh.Lock()
		var uploadBytes uint64
		if size > 0 {
			uploadBytes = uint64(size)
		}
		fs.perfRecordRemote(perfRemoteWrite, uploadStart, err, uploadBytes)
		fs.debugDurationf(uploadStart, 0, "fsync shadowspill upload done path=%s size=%d err=%v", fh.Path, size, err)
		if err != nil {
			log.Printf("fsync: ShadowSpill sync upload failed for %s: %v", fh.Path, err)
			return gofuse.EIO
		}
		if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
			log.Printf("fsync: ShadowSpill pending chmod failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
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
		if fh.HasPendingMode {
			ino := fh.Ino
			fh.HasPendingMode = false
			fh.HasPreviousMode = false
			fh.PreviousModeKnown = false
			fh.Unlock()
			fs.clearPendingModeForInodeExcept(ino, fh)
			fh.Lock()
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
	perfStart := fs.perfStart()
	releaseStatus := gofuse.OK
	defer func() { fs.perfRecordFuse(perfFuseRelease, perfStart, releaseStatus, 0) }()
	fh, ok := fs.fileHandles.Get(input.Fh)
	if ok {
		flushStatus := gofuse.OK
		defer func() {
			if fh.Prefetch != nil {
				fh.Prefetch.Close()
			}
			fs.deleteFileHandle(input.Fh, fh)
			fs.cleanupReleasedInode(fh.Ino, fh.Path)
		}()
		// Apply any deferred chmod after flush completes but before cleanup.
		defer func() {
			fh.Lock()
			hasPendingMode := fh.HasPendingMode
			pendingMode := fh.PendingMode & 0o777
			previousMode := fh.PreviousMode
			hasPreviousMode := fh.HasPreviousMode
			previousModeKnown := fh.PreviousModeKnown
			ino := fh.Ino
			localPath := fh.Path
			fh.Unlock()
			if !hasPendingMode {
				return
			}

			if flushStatus == gofuse.OK {
				ctx, cf := context.WithTimeout(context.Background(), 30*time.Second)
				err := retryPostUploadMode(ctx, func() error {
					return fs.applyRemoteMode(ctx, localPath, pendingMode)
				})
				cf()
				if err != nil {
					log.Printf("release: pending chmod failed for %s: %v", localPath, err)
					if hasPreviousMode {
						fs.inodes.SetModeState(ino, previousMode, previousModeKnown)
					}
					return
				}
				fs.inodes.UpdateMode(ino, pendingMode)
				fs.clearPendingModeForInode(ino)
				return
			}

			// Flush failed — revert the in-memory mode so local GetAttr doesn't lie.
			if hasPreviousMode {
				fs.inodes.SetModeState(ino, previousMode, previousModeKnown)
			}
			fs.clearPendingModeForInode(ino)
		}()

		// Unpin shadow if this handle pinned it, so deferred removals can proceed.
		if fh.ShadowPinned && fs.shadowStore != nil {
			defer fs.shadowStore.Unpin(fh.ShadowGen)
		}

		start := time.Now()
		phase := "start"
		defer func() { releaseStatus = flushStatus }()
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

		// close-sync is primarily enforced in Flush so close(2) can receive
		// remote upload errors. Keep Release as a best-effort fallback for
		// unusual flows where dirty staged state reaches Release directly.
		if fh.WritePolicy == WritePolicyCloseSync || fh.WritePolicy == WritePolicyWriteSync {
			phase = "release-write-policy-sync"
			lockStart := time.Now()
			fh.Lock()
			if lockWait := time.Since(lockStart); fs.debugEnabled() && lockWait >= fuseDebugSlowOpThreshold {
				fs.debugf("release lock wait path=%s fh=%d ino=%d phase=%s wait=%s", fh.Path, input.Fh, fh.Ino, phase, lockWait)
			}
			if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
				size := fh.Dirty.Size()
				ctx, cf := context.WithTimeout(context.Background(), releaseTimeout(size))
				flushStart := time.Now()
				fs.debugf("release write policy sync start path=%s size=%d policy=%s timeout=%s", fh.Path, size, fh.WritePolicy, releaseTimeout(size))
				flushStatus = fs.syncHandleToRemoteLocked(ctx, fh)
				fs.debugDurationf(flushStart, 0, "release write policy sync done path=%s size=%d status=%d", fh.Path, size, flushStatus)
				cf()
			}
			fh.Unlock()
			if flushStatus != gofuse.OK {
				return
			}
		}

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
			mode, hasMode := fs.modeForPendingHandle(fh)
			fh.Unlock()

			entry := &CommitEntry{
				Path:        fh.Path,
				Inode:       fh.Ino,
				BaseRev:     fh.BaseRev,
				Size:        size,
				Kind:        PendingOverwrite,
				ShadowSpill: true,
				Mode:        mode,
				HasMode:     hasMode,
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
				uploadErr := uploadFromShadowRemote(ctx, fs.client, fs.shadowStore, fh.Path, fs.remotePath(fh.Path), expectedRevisionForHandle(fh))
				var uploadBytes uint64
				if size > 0 {
					uploadBytes = uint64(size)
				}
				fs.perfRecordRemote(perfRemoteWrite, uploadStart, uploadErr, uploadBytes)
				fs.debugDurationf(uploadStart, 0, "release shadowspill upload done path=%s size=%d err=%v", fh.Path, size, uploadErr)
				if uploadErr != nil {
					flushStatus = gofuse.EIO
					log.Printf("release: ShadowSpill sync upload failed for %s: %v", fh.Path, uploadErr)
				}
				cf()
			} else if hasMode {
				fs.clearPendingModeForInode(fh.Ino)
			}

			fs.invalidateReadCacheAndTargets(fh.Path)
			if flushStatus == gofuse.OK {
				fs.cacheFileForPath(fh.Path, size, time.Now(), 0)
			} else {
				fs.dirCache.Invalidate(parentDir(fh.Path))
			}
			// Local release — kernel already knows about this close.
			// No notifyInode needed; userspace caches are invalidated above.
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
				mode, hasMode := fs.modeForPendingHandle(fh)
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
						Mode:    mode,
						HasMode: hasMode,
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
				if hasMode {
					fs.clearPendingModeForInode(fh.Ino)
				}

				// Invalidate caches so subsequent reads see fresh data.
				fs.invalidateReadCacheAndTargets(fh.Path)
				fs.cacheFileForPath(fh.Path, fh.Dirty.Size(), time.Now(), 0)
				// Local release — kernel already knows about this close.
				// No notifyInode needed; userspace caches are invalidated above.
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

	}
}

// flushHandleDebounced wraps flushHandle with optional debouncing for small files.
// When force is false and the file is small, the upload may be deferred.
// Caller must hold fh.mu.
func (fs *Dat9FS) flushHandleDebounced(ctx context.Context, fh *FileHandle, force bool) gofuse.Status {
	if force || fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		return fs.flushHandle(ctx, fh)
	}

	size := fh.Dirty.Size()
	if size >= fs.inlineThreshold() || fs.debouncer.delay <= 0 {
		return fs.flushHandle(ctx, fh)
	}

	// Small file: schedule a deferred upload.
	// Snapshot the data so the deferred upload sees a consistent copy.
	snapshot := fh.Dirty.bytesView()
	data := make([]byte, len(snapshot))
	copy(data, snapshot)
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
		writeStart := fs.perfStart()
		committedRev, err := fs.client.WriteCtxConditionalWithRevision(dCtx, fs.remotePath(filePath), data, expectedRevision)
		dCf()
		fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(len(data)))
		if err != nil {
			handle.Unlock()
			log.Printf("debounced flush failed for %s: %v", filePath, err)
			return
		}
		modeCtx, modeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		modeErr := fs.applyPendingModeForHandleLocked(modeCtx, handle)
		modeCancel()
		if modeErr != nil {
			handle.Unlock()
			log.Printf("debounced flush pending chmod failed for %s: %v", filePath, modeErr)
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
			fs.clearReadTargetsForPath(filePath)
			fs.readCache.PutOwned(filePath, data, committedRev)
		} else {
			fs.invalidateReadCacheAndTargets(filePath)
		}
		fs.inodes.UpdateSize(ino, int64(len(data)))
		fs.cacheFileForPath(filePath, int64(len(data)), time.Now(), committedRev)
		// Local debounced flush — kernel is not aware of this async
		// operation and does not need notify. Userspace caches are
		// updated above; kernel will pick up new attrs on next getattr.
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
		perfUploadStart := fs.perfStart()
		err = streamer.FinishStreaming(ctx, size,
			lastPartNum, lastCp, dirtyParts)
		fh.Lock()
		var perfUploadBytes uint64
		if size > 0 {
			perfUploadBytes = uint64(size)
		}
		fs.perfRecordRemote(perfRemoteWrite, perfUploadStart, err, perfUploadBytes)
		fs.debugDurationf(uploadStart, 0, "flushHandle finish streaming done path=%s size=%d err=%v", fh.Path, size, err)

		if err != nil {
			log.Printf("finish streaming failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}
		if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
			log.Printf("finish streaming pending chmod failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}

		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		clearReadTargetForLockedHandle(fh)
		fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
		fs.inodes.UpdateSize(fh.Ino, size)
		fs.cacheFileForPath(fh.Path, size, time.Now(), 0)
		// Remove stale shadow so subsequent read-only opens don't serve
		// the empty placeholder created at Create/Open time.
		if fs.shadowStore != nil {
			fs.shadowStore.Remove(fh.Path)
		}
		if fs.pendingIndex != nil {
			fs.pendingIndex.Remove(fh.Path)
		}
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
		// Local flush — kernel receives the Flush reply with status.
		// No notifyInode needed; kernel will refresh attrs on next access.
		return gofuse.OK
	}

	// Path 1b: Large new file with streaming uploader but no streaming parts
	// (non-sequential writes) — upload all parts in parallel at flush time.
	if fh.Streamer != nil && size >= fs.inlineThreshold() {
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
		perfUploadStart := fs.perfStart()
		err = streamer.UploadAll(ctx, size, partSnapshots)
		fh.Lock()
		var perfUploadBytes uint64
		if size > 0 {
			perfUploadBytes = uint64(size)
		}
		fs.perfRecordRemote(perfRemoteWrite, perfUploadStart, err, perfUploadBytes)
		fs.debugDurationf(uploadStart, 0, "flushHandle upload all done path=%s size=%d parts=%d err=%v", fh.Path, size, len(partSnapshots), err)

		if err != nil {
			log.Printf("upload all parts failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}
		if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
			log.Printf("upload all pending chmod failed for %s: %v", fh.Path, err)
			return httpToFuseStatus(err)
		}

		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		fh.DirtySeq = 0
		clearReadTargetForLockedHandle(fh)
		fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
		fs.inodes.UpdateSize(fh.Ino, size)
		fs.cacheFileForPath(fh.Path, size, time.Now(), 0)
		// Remove stale shadow (same reason as Path 1a above).
		if fs.shadowStore != nil {
			fs.shadowStore.Remove(fh.Path)
		}
		if fs.pendingIndex != nil {
			fs.pendingIndex.Remove(fh.Path)
		}
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
		// Local flush — kernel receives the Flush reply with status.
		// No notifyInode needed; kernel will refresh attrs on next access.
		return gofuse.OK
	}

	// Path 2: No streaming uploader or small file — materialize all data for upload.
	data := fh.Dirty.bytesView()
	expectedRevision := expectedRevisionForHandle(fh)
	var committedRev int64

	// Use the negotiated server threshold (not the heuristic-only inline
	// fallback): when /v1/status hasn't been observed we must force
	// multipart for non-empty writes. The server's IsLargeFile gate would
	// otherwise reject a direct PUT with `missing X-Dat9-Part-Checksums`
	// whenever the server is configured below the historical 50KB
	// default. Zero-byte files keep direct PUT because V2 initiate
	// rejects total_size=0.
	threshold := fs.negotiatedInlineThreshold()
	useDirectPUT := size == 0 || (threshold > 0 && size < threshold)
	if useDirectPUT {
		if size == 0 && fh.IsNew {
			phase = "empty-create"
			writeStart := time.Now()
			fs.debugf("flushHandle empty create start path=%s expected_rev=%d", fh.Path, expectedRevision)
			committedRev, err = fs.client.CreateFileCtx(ctx, fs.remotePath(fh.Path))
			if isCreateActionUnsupportedErr(err) {
				fs.debugf("flushHandle empty create unsupported path=%s fallback=small-write err=%v", fh.Path, err)
				committedRev, err = fs.client.WriteCtxConditionalWithRevision(ctx, fs.remotePath(fh.Path), data, expectedRevision)
			}
			fs.perfRecordRemote(perfRemoteWrite, writeStart, err, 0)
			fs.debugDurationf(writeStart, 0, "flushHandle empty create done path=%s committed_rev=%d err=%v", fh.Path, committedRev, err)
		} else {
			// Small file: direct PUT with revision return for freshness seeding.
			phase = "small-write"
			writeStart := time.Now()
			fs.debugf("flushHandle small write start path=%s size=%d expected_rev=%d", fh.Path, size, expectedRevision)
			committedRev, err = fs.client.WriteCtxConditionalWithRevision(ctx, fs.remotePath(fh.Path), data, expectedRevision)
			fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(len(data)))
			fs.debugDurationf(writeStart, 0, "flushHandle small write done path=%s size=%d committed_rev=%d err=%v", fh.Path, size, committedRev, err)
		}
	} else if fh.OrigSize >= threshold {
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
				fs.remotePath(fh.Path),
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
			var patchBytes uint64
			if size > 0 {
				patchBytes = uint64(size)
			}
			fs.perfRecordRemote(perfRemoteWrite, patchStart, err, patchBytes)
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
			fs.remotePath(fh.Path),
			bytes.NewReader(data),
			size,
			nil,
			expectedRevision,
		)
		fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(len(data)))
		fs.debugDurationf(writeStart, 0, "flushHandle write stream done path=%s size=%d err=%v", fh.Path, size, err)
	}
	if err != nil {
		log.Printf("flush upload failed for %s: %v", fh.Path, err)
		return httpToFuseStatus(err)
	}
	if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
		log.Printf("flush pending chmod failed for %s: %v", fh.Path, err)
		return httpToFuseStatus(err)
	}

	fh.Dirty.ClearDirty()
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	if committedRev > 0 {
		clearReadTargetForLockedHandle(fh)
		fs.clearReadTargetsForPathExcept(fh.Path, fh)
		fs.readCache.Put(fh.Path, data, committedRev)
		fh.IsNew = false
		fh.BaseRev = committedRev
		fs.inodes.UpdateRevision(fh.Ino, committedRev)
		if fh.ZeroBase && fh.Dirty != nil && fh.Dirty.Size() > 0 {
			fh.ZeroBase = false
		}
	} else {
		clearReadTargetForLockedHandle(fh)
		fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
		fs.finalizeHandleFlushLocked(fh, expectedRevision)
	}
	fs.inodes.UpdateSize(fh.Ino, size)
	fs.cacheFileForPath(fh.Path, size, time.Now(), committedRev)
	// Local flush — kernel receives the Flush reply with status.
	// No notifyInode/notifyEntry needed; userspace caches are updated
	// above and kernel will refresh attrs on next getattr/lookup.
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

	if fs.perf != nil {
		fs.perf.printSummary(os.Stderr)
	}
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
		fs.clearReadTargetsForPath(entry.Path)
		// Seed readCache from shadow data before the shadow file is removed.
		// Only attempt for files under the readCache size limit.
		if entry.Size <= fs.readCache.MaxFileSize() && fs.shadowStore != nil {
			if data, err := fs.shadowStore.ReadAll(entry.Path); err == nil {
				fs.readCache.PutOwned(entry.Path, data, committedRev)
			}
		}
		fs.inodes.UpdateRevision(entry.Inode, committedRev)
		fs.inodes.UpdateSize(entry.Inode, entry.Size)
		if entry.HasMode {
			fs.inodes.UpdateMode(entry.Inode, entry.Mode&0o777)
		}
		fs.cacheFileForPath(entry.Path, entry.Size, time.Now(), committedRev)
		// Local async commit completion — this is not an external change.
		// Kernel does not need notify; userspace caches and inode state
		// are updated above. Kernel will see new attrs on next access.
	} else {
		fs.invalidateReadCacheAndTargets(entry.Path)
		if entry.HasMode && entry.Inode > 0 {
			fs.inodes.UpdateMode(entry.Inode, entry.Mode&0o777)
		}
		fs.cacheFileForPath(entry.Path, entry.Size, time.Now(), 0)
	}
}

func (fs *Dat9FS) onCommitQueueCleanup(entry *CommitEntry) {
	if entry == nil || entry.Inode == 0 {
		return
	}
	fs.cleanupCommittedInode(entry.Inode, entry.Path)
}

func (fs *Dat9FS) String() string {
	return "dat9"
}
