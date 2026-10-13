// Package writeback is the write-plane extension. The write buffer
// (writebuffer.WriteBuffer) and the staging ledger (staging.ShadowStore /
// staging.PendingIndex / staging.Journal) back its handles, and the commit
// queue drains them. It implements the three durability policies around a
// buffered handle — write-sync commits on every Write, close-sync commits
// on Flush/Release, and writeback defers commits to the queue's own
// scheduling, bounded by flush debounce and drain.
package writeback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	flushdebounce "github.com/mem9-ai/drive9/pkg/fusecomp/debounce"
	"github.com/mem9-ai/drive9/pkg/fusecomp/httperr"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

const (
	// PolicyWriteBack buffers writes and commits asynchronously (staged
	// now, async queue pending).
	PolicyWriteBack Policy = iota
	// PolicyCloseSync commits the buffer when the handle closes.
	PolicyCloseSync
	// PolicyWriteSync commits on every Write before returning.
	PolicyWriteSync
)

const (
	commitRetries        = 3
	bufferedMaxTotal     = 10 << 30 // streamingWriteMaxSize, ported default
	writeBackThresholdIO = 0        // buffer every writable handle
	// commitInlineStreamThreshold is v1's writeBackThreshold: images at or
	// above it never materialize whole (Bytes()/LoadMissingParts) — a sparse
	// 2GiB pwrite (open/25) would OOM small hosts. They stream chunk-by-chunk
	// through a part-loading reader instead, committing inline.
	commitInlineStreamThreshold = 10 << 20
	// defaultUploadConcurrency ports the uploader worker count.
	defaultUploadConcurrency = 16
)

// Policy selects when a buffered handle commits.
type Policy int

// Options configures the extension.
type Options struct {
	Policy     Policy
	Client     *client.Client
	RemoteRoot string
	Events     *fs.EventBus
	// staging.Staging wires the async commit queue (writeback policy): staged
	// bytes and metadata land in the ledger and workers commit them.
	Staging *staging.Staging
	// FlushDebounce coalesces rapid flushes of one path under the
	// writeback policy (FlushDebounce; 0 disables, <0 = 2s default).
	FlushDebounce time.Duration
	// PreloadLimit caps how much existing content is preloaded into the
	// write buffer at open; larger files open unbuffered. Zero uses the
	// ported writebuffer.MaxPreloadSize.
	PreloadLimit int64

	// UploadConcurrency bounds the commit queue's workers (default 16).
	UploadConcurrency int
	// CommitQueueMaxPending bounds queued entries (default 500; 0 =
	// ported default).
	CommitQueueMaxPending int
	// Batch window knobs (WriteBackBatch*): rapid small commits merge
	// into one upload. Zero leaves batching off.
	BatchWindow   time.Duration
	BatchMaxFiles int
	BatchMaxBytes int64

	// RemoteCommitWaitTimeout bounds metadata-op waits for a path's pending
	// commit (v1 lockWritableRemoteCommitPath budget): 0 uses the 5s
	// default, negative disables the bound (wait unconditionally).
	RemoteCommitWaitTimeout time.Duration

	// InterruptibleCommits keeps commit contexts bound to the FUSE cancel
	// channel (--legacy-interruptible-mutations; default off — commits
	// detach so a kernel interrupt cannot cancel a server-side write that
	// already applied).
	InterruptibleCommits bool

	// SyncMode selects fsync durability: "strict" (default) waits the
	// remote commit; "interactive" (RTT > 10ms mounts) serves fsync
	// from the local staging ledger — stage + async enqueue, no queue wait.
	SyncMode string

	// LazyStaging ports v1 --writeback-lazy-staging (issue #964): close
	// stages into the page cache and the background syncer bounds the
	// power-loss window, instead of fsyncing the shadow and every WAL
	// frame at each staging boundary. SyncWindow (v1
	// --writeback-sync-window) bounds that syncer's interval; 0 disables
	// the syncer entirely.
	LazyStaging bool
	SyncWindow  time.Duration

	// WriteCacheDir arms the small-file writeback track: flushes stage
	// .dat/.meta images on disk and a 16-worker WriteBackUploader lands
	// them asynchronously.
	WriteCacheDir       string
	WriteCacheSizeMB    int64
	WriteCacheFreeRatio float64
	// WriteCache injects an existing cache instance (tests); it wins over
	// WriteCacheDir.
	WriteCache *staging.WriteBackCache
}

// Writeback is the fs.Handler middleware installing buffered handles.
type Writeback struct {
	// rolledBack fences every inline commit after a layer rollback.
	rolledBack atomic.Bool
	opts       Options
	// interactive mirrors SyncMode == "interactive".
	interactive bool
	debounce    *flushdebounce.FlushDebouncer
	root        string
	preload     int64
	commitMu    sync.Mutex
	queue       *CommitQueue
	// bypass arms kernel-cache DIRECT_IO windows after truncate-first
	// commits (attached by the facade).
	bypass fs.CacheBypassArmer
	// survivorOf resolves a surviving hardlink alias for unlink-survivor
	// (attached by the facade; nil disables).
	survivorOf func(path string) (string, bool)
	// inodes is the facade's identity table (alias-truncate sync; optional).
	inodes fs.InodeTable

	// wbCache/uploader form the small-file writeback track (.dat/.meta
	// disk cache + 16-worker uploader), active when WriteCacheDir is set.
	wbCache  *staging.WriteBackCache
	uploader *WriteBackUploader
	// pathLocks serializes queue callbacks against same-path staging.
	pathLocks stripedLocks
	// live maps path → *bufferingHandle for open writers (Setattr deferral
	// looks the current writer up by path).
	live sync.Map
	// liveSet tracks every open bufferingHandle so unlink/rename can walk
	// them; keeps this per-handle on FileHandle.
	liveSet sync.Map
	// landedMu/landed is the recently-landed table (landedAt).
	landedMu sync.Mutex
	landed   map[string]landedInfo
	// perfShim forwards the queue/uploader remote-op observations to the
	// facade's perf counters (v1 perfRecordRemote).
	perfShim *fusePerfCounters
	// unlinkedMu/unlinkedShadow track per-path unlink snapshots staged into
	// the shadow for anonymous clean fds (v1
	// snapshotUnlinkedRemoteToShadow).
	unlinkedMu     sync.Mutex
	unlinkedShadow map[string]struct{}
	// unlinkedShadowGen fences each snapshot's drop by the shadow
	// generation it captured: a re-created path stages new bytes at a new
	// generation, and the old object's last-fd cleanup must not remove
	// them (#918's replacement-detach, ported).
	unlinkedShadowGen map[string]uint64
	// lazy mirrors Options.LazyStaging; syncerCancel stops the background
	// WAL syncer that bounds the lazy staging power-loss window.
	lazy         bool
	syncerCancel context.CancelFunc
	// commitSeeder caches the just-committed image (readCache.Put at
	// commit settle) so reads racing backend replica lag see this mount's
	// own writes; nil disables seeding.
	commitSeeder  func(path string, data []byte, rev int64)
	pendingSeeder func(path string, data []byte)
	// baseReader serves a write-side base preload from the read cache (
	// preloaded through the read-cache path): a just-committed image answers
	// locally instead of racing backend replica lag with a remote GET.
	baseReader func(path string) ([]byte, bool)
}

type stripedLocks struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

// bufferingHandle accumulates writes in a writebuffer.WriteBuffer (read-your-writes
// without round trips) and commits whole images per policy. New files are
// created lazily: the remote sees nothing until the commit boundary, and
// reads of never-written regions return zeros, matching the terminal.
type bufferingHandle struct {
	w    *Writeback
	path string
	// isNew marks O_CREAT claims that have not reached the remote yet.
	isNew bool
	// pendingImage is an immutable captured pending image: reads of a
	// pending-overlay handle serve exactly these bytes.
	pendingImage []byte
	// flags is the open mode: O_APPEND writes track the buffer tail.
	flags uint32
	// pendingMode defers a chmod past a dirty handle to its commit (
	// pendingMode: applying it remotely mid-buffer would race the upload).
	pendingMode    uint32
	hasPendingMode bool
	// baseRev is the remote revision the buffer was opened against.
	baseRev int64
	// inner is nil for isNew handles until commit adopts a terminal.
	inner    fs.Handle
	baseSize int64

	mu    sync.Mutex
	buf   *writebuffer.WriteBuffer
	dirty bool
	// landed records that a commit has already published this handle's
	// image remotely (guards Release's empty materialization).
	landed bool
	// stagedToCache records that Flush snapshotted the image into the
	// .dat/.meta track; Release owes the uploader a Submit for it (v1: the
	// write-back cache fast path — Flush persists locally, the upload is
	// enqueued once, at the close boundary).
	stagedToCache bool
	// unlinked marks the handle anonymous (fh.Unlinked): the buffer
	// keeps serving the fd, commits are discarded.
	unlinked bool
	// trunc records an open(O_TRUNC): the buffer starts empty and dirty so
	// a close with no writes still publishes size 0 (ZeroBase).
	trunc bool
	// stream is the StreamUploader for large sequential writes: parts
	// upload in parallel while the write stream continues, releasing
	// memory per part.
	stream *StreamUploader
	// streamArmed guards one-time streaming setup on the first write.
	streamArmed bool
	// forceSync marks O_SYNC/O_DSYNC opens: every Write commits inline
	// regardless of the mount policy (writePolicyForOpen).
	forceSync bool
	// storageClass is the observed server-side storage class of the open
	// object, learned from stats and commit outcomes. Empty means unknown
	// — a patch is still attempted (the server's rejection is the
	// authoritative answer) and remembered from then on.
	storageClass client.StorageType
	// Staged-snapshot lineage (v1 ensureStagedSnapshotLineageLocked /
	// publishStagedSnapshotLineageLocked): consecutive stagings of one
	// handle chain snapshot IDs — the queue's direct-parent merge and
	// growth-rebase decisions need real lineage instead of fail-closed
	// empties.
	stageSeq          int64
	stagedSnapshotID  string
	stagedParentID    string
	stagedSnapshotSeq int64
	contentSnapshotID string
}

// lineageSeq mints process-unique snapshot IDs (v1 generateMountID): the
// queue only needs uniqueness within its landed table and pending index.
var lineageSeq atomic.Uint64

func newSnapshotID() string {
	return fmt.Sprintf("snap-%d-%d", os.Getpid(), lineageSeq.Add(1))
}

// New constructs the write plane.
func New(opts Options) (*Writeback, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("writeback: client is required")
	}
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	preload := opts.PreloadLimit
	if preload <= 0 {
		preload = writebuffer.MaxPreloadSize
	}
	w := &Writeback{opts: opts, root: root, preload: preload, interactive: opts.SyncMode == "interactive",
		perfShim: &fusePerfCounters{}}
	w.perfShim.enabled.Store(true)
	// A view checkout (layer restore/switch, SSE reset) retargets the base:
	// open handles with CLEAN buffers would keep serving the abandoned
	// view's bytes forever. Drop clean buffers so the next read reloads
	// from the new base; DIRTY buffers keep their local writes (they are
	// this mount's own acknowledged data and still commit) — review.
	if opts.Events != nil {
		opts.Events.On(fs.EventCheckout, func(e fs.Event) {
			w.liveSet.Range(func(_, v any) bool {
				if h, ok := v.(*bufferingHandle); ok {
					h.checkoutDropCleanBuffer()
				}
				return true
			})
		})
	}
	// FlushDebounce semantics: 0 disables, negative means unset (2s
	// default — the CLI's default -1 must land on the default, not on 0).
	debounce := opts.FlushDebounce
	if debounce < 0 {
		debounce = flushdebounce.DefaultFlushDebounce
	}
	if debounce != 0 {
		w.debounce = flushdebounce.NewFlushDebouncer(debounce)
	}
	if opts.WriteCache != nil {
		w.wbCache = opts.WriteCache
	}
	if w.wbCache == nil {
		if dir := strings.TrimSpace(opts.WriteCacheDir); dir != "" {
			cache, cerr := staging.NewWriteBackCache(dir)
			if cerr != nil {
				return nil, fmt.Errorf("writeback: write cache: %w", cerr)
			}
			w.wbCache = cache
		}
	}
	if w.wbCache != nil {
		workers := opts.UploadConcurrency
		if workers <= 0 {
			workers = defaultUploadConcurrency
		}
		up := NewWriteBackUploader(opts.Client, w.wbCache, workers, root)
		up.OnSuccess = func(meta staging.WriteBackMeta, committedRev int64, gens StagingGens) {
			// v1 onWriteBackUploadSuccess: seed the read cache with the
			// landed image (same replica-lag reasoning as the queue path).
			if committedRev > 0 {
				w.landedAt(w.localPathOf(meta.Path), committedRev, meta.Size)
			}
			if w.commitSeeder != nil && committedRev > 0 && meta.Size > 0 {
				if data, derr := w.wbCache.Get(meta.Path); derr && int64(len(data)) == meta.Size {
					w.commitSeeder(w.localPathOf(meta.Path), data, committedRev)
				}
			}
			if opts.Events != nil {
				// a local commit settle updates
				// the committed entry in place (inode size + dir cache via
				// Landed carrying the size) — never the parent listing, the
				// kernel view, or unrelated cached data.
				local := w.localPathOf(meta.Path)
				opts.Events.Emit(fs.Event{Kind: fs.EventLanded, Path: local, Rev: committedRev, Size: meta.Size})
			}
		}
		w.uploader = up
		up.SetPerfCounters(w.perfShim)
	}
	if opts.Staging != nil {
		workers := opts.UploadConcurrency
		if workers <= 0 {
			workers = defaultUploadConcurrency
		}
		q := NewCommitQueue(opts.Client, opts.Staging.Shadow, opts.Staging.Pending, opts.Staging.Journal, workers, opts.CommitQueueMaxPending, root)
		q.SetPerfCounters(w.perfShim)
		if opts.BatchWindow > 0 || opts.BatchMaxFiles > 0 || opts.BatchMaxBytes > 0 {
			q.ConfigureBatchWrite(opts.BatchWindow, opts.BatchMaxFiles, opts.BatchMaxBytes)
		}
		// The commit callbacks collapse into events here:
		// success settles the committed entry in place (Landed carries the
		// size for the inode/dir-cache refresh, onCommitQueueSuccess);
		// PathLock keeps queue cleanup serialized with same-path staging.
		q.OnSuccess = func(entry *CommitEntry, committedRev int64) {
			if w.opts.Policy == PolicyWriteSync || w.opts.Policy == PolicyCloseSync {
				w.armBypass(entry.Path, committedRev, entry.Size, "sync-commit")
			}
			// v1 onCommitQueueSuccess seeds the read cache from the shadow
			// image before cleanup removes it: reads racing the just-landed
			// revision (row updated, backend replicas still converging) then
			// serve this mount's own bytes instead of a lagging replica.
			if committedRev > 0 {
				w.landedAt(entry.Path, committedRev, entry.Size)
			}
			if w.commitSeeder != nil && committedRev > 0 && entry.Size > 0 {
				if data, derr := opts.Staging.Shadow.ReadAll(entry.Path); derr == nil && int64(len(data)) == entry.Size {
					w.commitSeeder(entry.Path, data, committedRev)
				}
			}
			if opts.Events != nil {
				// a local async commit completion is not an external
				// change — no kernel notify, no listing invalidation.
				opts.Events.Emit(fs.Event{Kind: fs.EventLanded, Path: entry.Path, Rev: committedRev, Size: entry.Size})
			}
		}
		q.PathLock = w.pathLocks.lock
		w.queue = q
		// v1 lazy-staging wiring (issue #964): WAL frames stop fsyncing on
		// Put and a background syncer bounds the power-loss window instead.
		w.lazy = opts.LazyStaging
		if opts.LazyStaging {
			opts.Staging.Pending.SetJournalSyncOnPut(false)
			if opts.SyncWindow > 0 {
				syncerCtx, cancel := context.WithCancel(context.Background())
				w.syncerCancel = cancel
				go opts.Staging.Journal.SyncLoop(syncerCtx, opts.SyncWindow)
			}
		}
	}
	return w, nil
}

// String returns the policy name.
func (p Policy) String() string {
	switch p {
	case PolicyCloseSync:
		return "close-sync"
	case PolicyWriteSync:
		return "write-sync"
	default:
		return "writeback"
	}
}

// SetCommitSeeder wires the read-cache seeding hook (the cache extension's
// SeedCommit); called by the mount when both extensions are present.
// AbandonLayerForRollback fences the whole write plane after the mounted
// layer rolled back: the commit queue halts (in-flight uploads fail and
// their staged bytes stay for manual recovery) and later inline commits
// fail with the rolled-back error instead of publishing into the base.
func (w *Writeback) AbandonLayerForRollback() {
	if w == nil {
		return
	}
	w.rolledBack.Store(true)
	if w.queue != nil {
		w.queue.AbandonLayer()
	}
}

func (w *Writeback) SetCommitSeeder(fn func(path string, data []byte, rev int64)) {
	w.commitSeeder = fn
}

// SetPendingSeeder wires the flush-time pending-seed hook (the cache
// extension's SeedPending).
func (w *Writeback) SetPendingSeeder(fn func(path string, data []byte)) {
	w.pendingSeeder = fn
}

// SetBaseReader wires the read-cache base-preload hook; nil keeps the
// direct remote preload.
func (w *Writeback) SetBaseReader(fn func(path string) ([]byte, bool)) {
	w.baseReader = fn
}

// Lifecycle: the facade chain starts/drains/stops the queue with the mount.
// Start ports the v1 crash-recovery wiring: rebuild the pending index from
// its .meta files, then re-enqueue persisted pending commits. When the
// commit queue is absent (write-sync/close-sync stacks) the uploader owns
// recovery of the .dat/.meta track instead (v1: "Recover pending uploads
// only when the newer commit queue is unavailable").
func (w *Writeback) Start(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if w.opts.Staging != nil && w.opts.Staging.Pending != nil {
		if err := w.opts.Staging.Pending.RecoverFromDisk(); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: pending index recovery: %v\n", err)
		}
	}
	if w.queue != nil {
		w.queue.RecoverPending()
	} else if w.uploader != nil {
		w.uploader.RecoverPending()
	}
	return nil
}

// WaitCommitQueueIdle waits the async commit queue — the separate
// commit_queue drain phase; idempotent after Drain.
func (w *Writeback) WaitCommitQueueIdle(ctx context.Context) error {
	if w.queue != nil {
		return w.queue.WaitIdle(ctx)
	}
	return nil
}

// Drain quiesces the flush debouncer, the uploader, and the commit queue.
func (w *Writeback) Drain(ctx context.Context) error {
	// v1 FlushAll: an OPEN handle has no Release boundary coming, so its
	// buffered image must publish now. The walk happens here — inside the
	// plane that owns the buffering handles — because the facade sees only
	// the outermost decorator (the cache extension's handle) and a plain
	// Flush on it merely snapshots locally under the write-back policy.
	w.liveSet.Range(func(key, _ any) bool {
		if h, ok := key.(*bufferingHandle); ok {
			hctx, cf := context.WithTimeout(ctx, 30*time.Second)
			_ = h.DrainFlush(hctx)
			cf()
		}
		return true
	})
	// drain order: flush debounced uploads first, then the uploader and
	// the commit queue.
	if w.debounce != nil {
		w.debounce.FlushAll()
	}
	if w.uploader != nil {
		if err := w.uploader.WaitIdle(ctx); err != nil {
			return err
		}
	}
	if w.queue != nil {
		return w.queue.WaitIdle(ctx)
	}
	return nil
}

// Stop tears the extension down after Drain.
func (w *Writeback) Stop(ctx context.Context) error {
	if w.syncerCancel != nil {
		w.syncerCancel()
	}
	if w.uploader != nil {
		w.uploader.DrainAll()
	}
	if w.queue != nil {
		w.queue.DrainAll()
	}
	return nil
}

// MarkPathUnlinked marks every live handle of p anonymous (survivor
// bookkeeping) without touching the remote: the facade defers the actual
// deletion to the last close of an open file.
func (w *Writeback) MarkPathUnlinked(p string) {
	w.markUnlinked(p, false)
	// The name is going away NOW (the syscall already returned OK): the
	// pending overlays must stop answering lookups for it, or a
	// re-creator racing the deferred remote DELETE reads the old entry
	// (unlink/14 row 4's EEXIST against the resurrected name). The open
	// fds keep their image: the staged bytes move to the unlink
	// snapshot before the pending entries die.
	if w.opts.Staging != nil && w.queue != nil {
		if meta, ok := w.pendingMeta(p); ok && meta != nil {
			if data, kok := readShadowGenerationFenced(w.opts.Staging.Shadow, p, meta.Size); kok {
				if err := w.opts.Staging.Shadow.WriteFull(p, data, 0); err == nil {
					w.recordUnlinkedSnapshot(p, w.opts.Staging.Shadow.ActiveGeneration(p))
				}
			}
		}
		w.queue.CancelPath(p)
	}
	if w.wbCache != nil {
		w.wbCache.Remove(p)
	}
	// The landed table's rescue window must not resurrect it either.
	w.forgetLanded(p)
}

// SetSurvivorLookup wires the facade's hardlink-alias probe.
func (w *Writeback) SetSurvivorLookup(fn func(path string) (string, bool)) { w.survivorOf = fn }

// SetInodeTable wires the facade's identity table (alias-truncate sync).
func (w *Writeback) SetInodeTable(t fs.InodeTable) { w.inodes = t }

// SetRemoteOpRecorder wires the facade-owned remote-op perf feed: the
// commit queue's and uploader's HTTP observations flow into the mount
// counters (v1 perfRecordRemote).
func (w *Writeback) SetRemoteOpRecorder(fn func(name string, err error, dur time.Duration, bytes uint64)) {
	w.perfShim.setForward(fn)
}

// smallFileShadowThreshold is the cached-I/O cutoff (v1): handles of bases
// at or above it open DIRECT_IO so the kernel never page-caches a file the
// shadow plane owns.
const smallFileShadowThreshold = 64 << 20

// isSQLiteDirectIOPath approximates v1's predicate: a sqlite main database
// (a same-path sidecar is open) or a persistent journal path opens
// DIRECT_IO — the kernel must not cache pages of a file whose sidecar
// traffic can change the image underneath.
func (w *Writeback) isSQLiteDirectIOPath(path string) bool {
	base := baseNameOf(path)
	if strings.HasSuffix(base, "-journal") {
		return true
	}
	stem := strings.TrimSuffix(base, extOf(base))
	if stem == "" {
		return false
	}
	dir := strings.TrimSuffix(path, base)
	found := false
	w.liveSet.Range(func(key, _ any) bool {
		h, ok := key.(*bufferingHandle)
		if !ok {
			return true
		}
		hp := h.path
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if hp == dir+stem+suffix {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

func baseNameOf(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

func extOf(base string) string {
	if i := strings.IndexByte(base, '.'); i > 0 {
		return base[i:]
	}
	return ""
}

// openDirectIO reports whether a freshly opened handle should reply
// FOPEN_DIRECT_IO (v1 remoteOpenFlagsForHandle): sqlite-coherent paths,
// shadow-backed staged images, and bases at or above the 64MB threshold.
func (w *Writeback) openDirectIO(path string, baseSize int64) bool {
	if w.isSQLiteDirectIOPath(path) {
		return true
	}
	if w.opts.Staging != nil && w.opts.Staging.Pending.HasPending(path) {
		return true
	}
	return baseSize >= smallFileShadowThreshold
}

// syncAliasHandlesAfterTruncate ports v1 syncOpenHandlesAfterPathTruncate:
// a truncate through one hardlink alias rebases every OTHER open handle of
// the same inode — their buffers keep the stale size otherwise and their
// next flush republishes the pre-truncation tail (LTP ftest01's fstat).
func (w *Writeback) syncAliasHandlesAfterTruncate(ino uint64, path string, newSize int64) {
	aliaser, ok := w.inodes.(interface{ Aliases(ino uint64) []string })
	if !ok || ino == 0 {
		return
	}
	for _, alias := range aliaser.Aliases(ino) {
		if alias == path {
			continue
		}
		h := w.handleFor(alias)
		if h == nil {
			continue
		}
		h.mu.Lock()
		if h.buf != nil && !h.unlinked && h.buf.Size() != newSize {
			if err := h.buf.Truncate(newSize); err == nil {
				h.buf.ResetSequentialState(newSize)
				h.dirty = true
			}
		}
		h.mu.Unlock()
	}
}

// Queue exposes the commit queue for the mount's queue-stats probe.
func (w *Writeback) Queue() *CommitQueue { return w.queue }

// UploaderSnapshot reports the .dat/.meta track depths for the
// continuous-perf queue sample (nil-safe).
func (w *Writeback) UploaderSnapshot() (queued, inFlight int) {
	if w.uploader != nil {
		snap := w.uploader.Snapshot()
		return snap.Queued, snap.InFlight
	}
	return 0, 0
}

// DirtyHandleCount reports open buffering handles with uncommitted images
// (v1 dirty-inode accounting for the continuous-perf queue sample).
func (w *Writeback) DirtyHandleCount() int {
	n := 0
	w.liveSet.Range(func(_, v any) bool {
		if h, ok := v.(*bufferingHandle); ok {
			h.mu.Lock()
			if h.dirty {
				n++
			}
			h.mu.Unlock()
		}
		return true
	})
	return n
}

// SetBypassArmer wires the facade-owned bypass table (Context.Bypass).
func (w *Writeback) SetBypassArmer(a fs.CacheBypassArmer) { w.bypass = a }

// ReexecRefusal ports the v1 writeback re-exec gates: queued/in-flight/
// delayed/conflicted commits, uploader work, staged pending entries, and
// resident shadow bytes all hold state a re-exec would lose. (The v1
// journal uncommitted-frame gate is subsumed by the pending-index count —
// the index is the reconciled view of the replayed journal.)
func (w *Writeback) ReexecRefusal() (string, int) {
	if w == nil {
		return "", 0
	}
	if w.queue != nil {
		if snap := w.queue.Snapshot(); snap.Pending > 0 || snap.InFlight > 0 || snap.Delayed > 0 || snap.Conflicts > 0 {
			return "commit_queue_pending", snap.Pending + snap.InFlight + snap.Delayed + snap.Conflicts
		}
	}
	if w.uploader != nil {
		if snap := w.uploader.Snapshot(); snap.Queued > 0 || snap.InFlight > 0 || snap.Cached > 0 {
			return "uploader_queued", snap.Queued + snap.InFlight + snap.Cached
		}
	}
	if w.opts.Staging != nil {
		if w.opts.Staging.Pending != nil {
			if n := w.opts.Staging.Pending.Count(); n > 0 {
				return "pending_index", n
			}
		}
		if w.opts.Staging.Shadow != nil {
			if n := w.opts.Staging.Shadow.PendingBytes(); n > 0 {
				return "shadow_store", int(n)
			}
		}
	}
	return "", 0
}

// Do implements fs.Handler.
func (w *Writeback) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		// pendingIndex overlay: a lazily created file exists from Create
		// until its commit boundary, so path resolutions against the open
		// file (git's chmod on .git/config.lock, a competing stat) answer
		// locally instead of ENOENT against a remote that has not seen it.
		if h := w.handleFor(op.Path); h != nil {
			h.mu.Lock()
			isNew := h.isNew
			var size int64
			if h.buf != nil {
				size = h.buf.Size()
			}
			mode, hasMode := h.pendingMode, h.hasPendingMode
			h.mu.Unlock()
			if isNew {
				if !hasMode {
					mode = 0o644
				}
				return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size}}, nil
			}
		}
		// A staged-but-not-landed image is newer than anything the remote can
		// serve: answer with its pending size/mode. A PendingOverwrite without
		// a pending mode must NOT answer — the remote still owns the mode, and
		// a default 0644 reply would clobber it (chmod/12: chmod 0777, write,
		// fstat → 0644).
		if attr, ok := w.pendingAttr(op.Path); ok {
			return fs.Result{Attr: &attr}, nil
		}
		res, err := next(ctx, op)
		if err == nil || !fs.IsNotFound(err) {
			return res, err
		}
		// A just-landed commit our own replica lag can hide: the chain stat
		// 404'd against a converging replica. Rescue from the landed table
		// instead of letting the miss poison the namespace cache with a
		// negative row (close/write-sync's ENOENT source). Rescue-only — a
		// successful remote stat always wins, and paths deleted after their
		// landing drop out of the table at unlink/rename.
		if attr, ok := w.landedAttr(op.Path); ok {
			return fs.Result{Attr: &attr}, nil
		}
		return res, err
	case fs.OpList:
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		// v1/#918 resolve-dirty-and-pending-before-publication: a listing
		// row describes the remote image, but a file whose newest image is
		// still staged or live in a writer's buffer must not publish that
		// stale size/mode through READDIRPLUS — the stat path answers from
		// pendingAttr, and a listing that races a cache invalidation would
		// otherwise serve the pre-write image while the commit is in
		// flight.
		res.Entries = w.overlayPendingListAttrs(op.Path, res.Entries)
		return res, nil
	case fs.OpCreate:
		flags := op.Flags
		if flags&uint32(syscall.O_CREAT) == 0 {
			// v1 close-to-open: a read-only re-open of a path whose image
			// has not landed serves the pending bytes — the kernel's LOOKUP
			// already answered from the pending overlay and v1's open never
			// re-stat the remote. Waiting out the async landing instead
			// turns a slow upload into an open failure (open/25: a 2GiB
			// spill re-opened before the stream lands).
			if flags&uint32(syscall.O_ACCMODE) == uint32(syscall.O_RDONLY) {
				if attr, ok := w.pendingAttr(op.Path); ok {
					h := &bufferingHandle{w: w, path: op.Path, flags: op.Flags,
						baseSize: attr.Size, forceSync: hasSyncOpenFlag(op.Flags),
						pendingImage: w.capturePendingImage(op.Path)}
					if pb, ok := w.pendingBaseRevOf(op.Path); ok {
						h.baseRev = pb
					}
					w.track(h)
					attrCopy := attr
					return fs.Result{Attr: &attrCopy, Handle: h}, nil
				}
				// An overwrite's pending image owns the path too: its
				// .dat/shadow bytes are what a re-open must read — pinning
				// the remote instead races the async landing (a checksum
				// spanning the landing mixes the old and new images; the
				// tier-transition 8MiB smoke row). pendingAttr declines
				// mode-less overwrites for LOOKUP replies; reads do not
				// need the mode.
				if w.hasPendingLocalState(op.Path) {
					size := int64(0)
					if meta, ok := w.pendingMeta(op.Path); ok && meta != nil {
						size = meta.Size
					}
					if image := w.capturePendingImage(op.Path); image != nil {
						h := &bufferingHandle{w: w, path: op.Path, flags: op.Flags,
							baseSize: size, forceSync: hasSyncOpenFlag(op.Flags),
							pendingImage: image}
						if pb, ok := w.pendingBaseRevOf(op.Path); ok {
							h.baseRev = pb
						}
						w.track(h)
						return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: 0o644, Size: size}, Handle: h}, nil
					}
				}
			}
			// Read-visibility across fds (v1 flushPendingWriteBack): a
			// live writer's image exists only in its buffer — a second
			// opener would wrap the stale remote base and read past the
			// writer's bytes (git's index-pack preads the tmp_pack a
			// concurrent fd just wrote). Flush the writer first so the
			// pending serve below owns the path.
			if h := w.handleFor(op.Path); h != nil && op.Flags&uint32(syscall.O_ACCMODE) == uint32(syscall.O_RDONLY) {
				if ferr := h.Flush(ctx); ferr != nil {
					// The flush is what makes the writer's newest bytes
					// visible to this read; continuing would serve the
					// stale remote image as success (ENOSPC staging, a
					// failed journal append, an upload error).
					return fs.Result{}, ferr
				}
			}
			// pending-cache visibility (plain opens too): a path flushed
			// into the async queue but not yet landed waits for its own
			// image before the chain below stats it.
			if w.queue != nil && w.queue.HasPath(op.Path) {
				if _, serr := w.opts.Client.StatCtx(ctx, w.remotePath(op.Path)); serr != nil && client.IsNotFound(serr) {
					w.waitPathCommit(op.Path)
				}
			}
			// Same visibility for the .dat/.meta track: a staged image is
			// this mount's own file — land it instead of letting the chain
			// below stat a remote that has not seen it (v1: read-only
			// re-opens of a lagging close read the pending bytes, never a
			// 404).
			if w.wbCache != nil && w.uploader != nil {
				if _, ok := w.wbCache.GetMeta(op.Path); ok {
					if _, serr := w.opts.Client.StatCtx(ctx, w.remotePath(op.Path)); serr != nil && client.IsNotFound(serr) {
						if err := w.uploader.UploadSync(ctx, op.Path); err == nil {
							// Landed; fall through so the chain below opens
							// the fresh remote image.
							_ = serr
						}
					}
				}
			}
			// open of an existing file: the chain below serves it, wrap
			// the returned handle with buffering on first write
			res, err := next(ctx, op)
			if err != nil || res.Handle == nil {
				return res, err
			}
			// open(O_TRUNC) without O_CREAT truncates locally.
			trunc := flags&uint32(syscall.O_TRUNC) != 0 &&
				flags&(uint32(syscall.O_WRONLY)|uint32(syscall.O_RDWR)) != 0
			h := &bufferingHandle{w: w, path: op.Path, inner: res.Handle, flags: op.Flags,
				baseSize: attrSize(res), baseRev: attrRev(res), trunc: trunc,
				forceSync: hasSyncOpenFlag(op.Flags)}
			if h.baseRev == 0 {
				// A pending image (not yet landed) is this mount's own base:
				// adopt its CAS lineage so the eventual overwrite commits.
				if pb, ok := w.pendingBaseRevOf(op.Path); ok {
					h.baseRev = pb
				}
			}
			w.track(h)
			res.Handle = h
			if w.openDirectIO(op.Path, h.baseSize) {
				// v1 remoteOpenFlagsForHandle: shadow-backed/sqlite/large
				// bases reply DIRECT_IO; the facade keeps the flag and skips
				// KEEP_CACHE.
				res.OpenFlags |= gofuse.FOPEN_DIRECT_IO
			}
			if trunc && res.Attr != nil {
				res.Attr.Size = 0
			}
			if trunc {
				w.armBypass(op.Path, attrRev(res), 0, "open-truncate")
			}
			if res.Attr != nil && w.hasPendingLocalState(op.Path) {
				// The authoritative image is local staging, not the remote
				// row this stat came from: a stale rev would let read caches
				// serve the previous revision's bytes (the tier-B overwrite
				// mixed 64 blocks of the old image into the readback).
				res.Attr.Rev = 0
			}
			return res, nil
		}
		// IsNew is existence, not the O_CREAT flag: an O_CREAT open of
		// an existing path (bash `>>` passes O_CREAT|O_APPEND) buffers over
		// the real base so appends and read-modify-writes keep existing
		// content. Only a genuinely missing path is claimed for lazy
		// creation — nothing reaches the remote until the policy's commit
		// boundary; O_EXCL must still see the backend's existing entry.
		// The existence probe goes through the chain (dircache negative rows
		// answer locally): a raw remote stat races backend replica lag right
		// after this mount's own DELETE and resurrects the name as EEXIST
		// (unlink/14's re-create).
		st, statErr := w.probeExisting(ctx, next, op.Path)
		if statErr != nil && fs.IsNotFound(statErr) && w.queue != nil && w.queue.HasPath(op.Path) {
			// pending-cache visibility: the path was flushed into the
			// async queue but has not landed yet. Wait for OUR entry (it is
			// this mount's own data) instead of claiming a fresh empty
			// create that would race the queued image.
			if w.waitPathCommit(op.Path) {
				st, statErr = w.probeExisting(ctx, next, op.Path)
			}
		}
		if statErr != nil && client.IsNotFound(statErr) && w.wbCache != nil && w.uploader != nil {
			// Same pending-cache visibility for the .dat/.meta track: a
			// staged image is this mount's own file — land it instead of
			// claiming a fresh create that would double-own the name.
			if _, ok := w.wbCache.GetMeta(op.Path); ok {
				if err := w.uploader.UploadSync(ctx, op.Path); err == nil {
					st, statErr = w.probeExisting(ctx, next, op.Path)
				}
			}
		}
		if statErr == nil && flags&uint32(syscall.O_EXCL) != 0 {
			return fs.Result{}, syscall.EEXIST
		}
		if statErr != nil && !fs.IsNotFound(statErr) {
			return fs.Result{}, statErr
		}
		if statErr == nil {
			res, err := next(ctx, op)
			if err != nil || res.Handle == nil {
				return res, err
			}
			// open(O_TRUNC) truncates locally: the buffer rebases to
			// empty and the commit boundary publishes the truncation — the
			// remote keeps the pre-truncation image until then.
			trunc := flags&uint32(syscall.O_TRUNC) != 0 &&
				flags&(uint32(syscall.O_WRONLY)|uint32(syscall.O_RDWR)) != 0
			h := &bufferingHandle{w: w, path: op.Path, inner: res.Handle, flags: op.Flags,
				baseSize: st.Size, baseRev: st.Rev, trunc: trunc,
				forceSync: hasSyncOpenFlag(op.Flags)}
			if h.baseRev == 0 {
				if pb, ok := w.pendingBaseRevOf(op.Path); ok {
					h.baseRev = pb
				}
			}
			w.track(h)
			res.Handle = h
			if w.openDirectIO(op.Path, h.baseSize) {
				res.OpenFlags |= gofuse.FOPEN_DIRECT_IO
			}
			if trunc && res.Attr != nil {
				res.Attr.Size = 0
			}
			if trunc {
				w.armBypass(op.Path, st.Rev, 0, "open-truncate")
			}
			return res, nil
		}
		// v1 createInputMode: a create mode other than the server default
		// (0644) rides as a pending chmod from birth, so the commit boundary
		// publishes it. The full 0o7777 range keeps SUID/SGID at create
		// (chmod/12: 04777); the kernel's killpriv clears them on write.
		// Mode 0 is a legitimate create (open/26) — never coerce it.
		mode := op.Mode & 0o7777
		h := &bufferingHandle{w: w, path: op.Path, flags: op.Flags, isNew: true, baseRev: 0,
			forceSync: hasSyncOpenFlag(op.Flags)}
		if mode != 0o644 {
			h.pendingMode = mode
			h.hasPendingMode = true
		}
		w.track(h)
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode}, Handle: h}, nil
	case fs.OpUnlink, fs.OpRmdir:
		// unlink orchestration: drain in-flight commits for the path,
		// mark open handles anonymous (the fd keeps serving its buffer but
		// Release must not resurrect the name), DELETE remotely, then
		// cancel anything still queued. A failed DELETE rolls the anonymous
		// marking back — a transient error must not silently drop buffered
		// writes — and only a genuine 404 skips the remote call.
		// under the writeback policy, an unlink of
		// a hardlinked file with live writers rebinds them onto a verified
		// surviving alias (after flushing their pending image) instead of
		// discarding the buffer — the fd keeps writing to the survivor.
		if op.Kind == fs.OpUnlink && w.opts.Policy == PolicyWriteBack && w.survivorOf != nil {
			if survivor, ok := w.survivorOf(op.Path); ok && survivor != "" {
				if h := w.handleFor(op.Path); h != nil {
					// Synchronous publish (flushPendingWriteBack): the
					// debounced Flush path would leave the image unsent
					// when the preferred path flips.
					_ = h.commitLocked(ctx)
					if w.queue != nil {
						w.waitPathCommit(op.Path)
					}
					w.retarget(op.Path, survivor)
					if _, err := next(ctx, op); err != nil {
						// DELETE failed: bind back so nothing is lost.
						w.retarget(survivor, op.Path)
						return fs.Result{}, err
					}
					return fs.Result{}, nil
				}
			}
		}
		under := op.Kind == fs.OpRmdir
		prefix := strings.TrimSuffix(op.Path, "/") + "/"
		if w.queue != nil {
			w.waitPathCommit(op.Path)
			if under {
				w.queue.WaitPrefixTimeout(prefix, 10*time.Second)
			}
		}
		// An in-flight .dat/.meta upload must land before the DELETE: it
		// would otherwise resurrect the path after the unlink (unlink/14:
		// re-create hits EEXIST against the resurrected name).
		if w.uploader != nil {
			w.uploader.WaitPath(op.Path)
		}
		// writeBack.Remove() / CancelPrefix: staged entries die with their
		// path (prefix-wide for directory removal). The queue cancel runs
		// BEFORE the survivor snapshot: CancelPath also removes the shadow
		// file, which would wipe the image an anonymous clean fd reads
		// after the delete.
		if w.wbCache != nil {
			if under {
				for _, entry := range w.wbCache.ListByPrefix(prefix) {
					w.wbCache.Remove(entry.Path)
				}
			}
			w.wbCache.Remove(op.Path)
		}
		if w.queue != nil {
			w.queue.CancelPath(op.Path)
			if under {
				w.queue.CancelPrefix(prefix)
			}
		}
		w.markUnlinked(op.Path, under)
		w.forgetLanded(op.Path)
		rollback := func() {
			w.markLinked(op.Path, under)
			w.dropUnlinkedShadow(op.Path)
		}
		_, statErr := w.opts.Client.StatCtx(ctx, w.remotePath(op.Path))
		if statErr != nil && !client.IsNotFound(statErr) {
			rollback()
			return fs.Result{}, mapErr(statErr)
		}
		if statErr != nil && w.everLanded(op.Path) {
			// We landed this path ourselves moments ago; a 404 here is a
			// lagging replica, not truth (close/write-sync land constantly,
			// so this race is their dominant EEXIST source: the skipped
			// DELETE resurrects on re-create). Issue the DELETE anyway —
			// next()'s terminal tolerates a genuine 404.
			statErr = nil
		}
		if statErr != nil {
			// nothing remote; a lazy-created path never landed. Local
			// discard is enough, but queued entries must still die.
			if w.queue != nil {
				w.queue.CancelPath(op.Path)
				if under {
					w.queue.CancelPrefix(prefix)
				}
			}
			return fs.Result{}, nil
		}
		res, err := next(ctx, op)
		if err != nil {
			rollback()
			return res, err
		}
		return res, nil
	case fs.OpLink:
		// v1 Link: the hardlink commit needs the source's pending data landed
		// first — flush a live writer, wait the queued commit, land a staged
		// .dat/.meta image — otherwise the remote link ENOENTs against a
		// path the server has not seen yet (link/00: create, write, close,
		// link).
		if h := w.handleFor(op.Path); h != nil {
			_ = h.Flush(ctx)
		}
		if w.queue != nil {
			w.waitPathCommit(op.Path)
			w.waitPathCommit(op.Path2)
		}
		if w.wbCache != nil && w.uploader != nil {
			if _, ok := w.wbCache.GetMeta(op.Path); ok {
				_ = w.uploader.UploadSync(ctx, op.Path)
			}
		}
		return next(ctx, op)

	case fs.OpRename:
		// A staged .dat/.meta image for the old path lands there first,
		// then the remote rename moves it. In-flight uploads drain, the
		// image lands synchronously, duplicates drain again, and the old
		// name's staged entry dies: a straggler landing after the remote
		// rename resurrects the old name (git's lock→config rename left
		// config.lock behind and the next lock create EEXISTs).
		if w.uploader != nil {
			w.uploader.WaitPath(op.Path)
			w.uploader.WaitPath(op.Path2)
		}
		if w.wbCache != nil && w.uploader != nil {
			if meta, ok := w.wbCache.GetMeta(op.Path); ok {
				if err := w.uploader.UploadSync(ctx, op.Path); err != nil {
					// The staged image is the acknowledged content of the
					// old name; losing it to a failed upload and letting
					// the rename move the stale remote image would drop
					// the write with no retry source. Fail the rename —
					// the staging stays for the retry.
					return fs.Result{}, fmt.Errorf("land staged image before rename %s: %w", op.Path, err)
				}
				w.uploader.WaitPath(op.Path)
				// Retire only the generation that uploaded: a fresher
				// staging published during the upload still needs its own
				// landing (or the retargeted writer's flush).
				w.wbCache.RemoveIfGeneration(op.Path, meta.Generation)
			}
		}
		// renames only after the old path's pending data landed:
		// flush live writers (staging + enqueue), wait the queue out for
		// the old path, then rename remotely — otherwise a queued commit
		// would resurrect the old name after the rename.
		if h := w.handleFor(op.Path); h != nil {
			_ = h.Flush(ctx)
		}
		if w.queue != nil {
			w.waitPathCommit(op.Path)
			w.waitPathCommit(op.Path2)
		}
		// A lazy-created writer that never landed has no remote entry to
		// rename: its buffer simply moves (PrepareRename's local
		// phase). Otherwise the remote rename runs first and the open
		// writers follow it onto the new path (fh.Path + SetPath).
		if _, statErr := w.opts.Client.StatCtx(ctx, w.remotePath(op.Path)); statErr != nil {
			if !client.IsNotFound(statErr) {
				return fs.Result{}, mapErr(statErr)
			}
			if w.handleFor(op.Path) == nil {
				return fs.Result{}, fs.ErrNotFound
			}
			w.retarget(op.Path, op.Path2)
			return fs.Result{}, nil
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		w.forgetLanded(op.Path)
		w.retarget(op.Path, op.Path2)
		// staged descendants migrate to the new
		// prefix alongside the remote rename.
		if w.wbCache != nil {
			prefix := strings.TrimSuffix(op.Path, "/") + "/"
			for _, entry := range w.wbCache.ListByPrefix(prefix) {
				_ = w.wbCache.RenamePending(entry.Path, strings.Replace(entry.Path, op.Path, op.Path2, 1))
			}
		}
		return res, nil

	case fs.OpSetAttr:
		if op.HasMode {
			// A chmod on a dirty path is deferred to the handle's commit
			// boundary (pendingMode); clean paths apply immediately. The
			// deferral must answer with the REQUESTED mode (v1 settles
			// entry.Mode locally): an empty reply makes the facade re-stat
			// the chain, which reads the not-yet-published remote mode and
			// settles the stale value into the kernel's 30s attr cache
			// (killpriv SETATTR 0777 replying 04777 — chmod/12).
			if h := w.handleFor(op.Path); h != nil {
				h.mu.Lock()
				h.pendingMode = op.Mode & 0o7777
				h.hasPendingMode = true
				size := h.baseSize
				if h.buf != nil {
					size = h.buf.Size()
				}
				h.mu.Unlock()
				// v1 setPendingMetadataMode: journal the mode onto the staged
				// ledger meta too — the queue entry snapshotted its mode at
				// enqueue, and a chmod newer than that snapshot (killpriv on
				// an open handle) must win the post-landing application
				// (chmod/12: 0777 vs the create-mode 04777).
				if w.opts.Staging != nil {
					_ = w.opts.Staging.Pending.UpdateMode(op.Path, op.Mode&0o7777)
				}
				return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: op.Mode & 0o7777, Size: size}}, nil
			}
			// v1 Setattr: a chmod on a queued path first waits for its own
			// commit to land — the queue entry snapshotted its mode at
			// enqueue, so journaling past an in-flight upload would be lost
			// and a remote chmod would 404 a path the server has not seen.
			if w.queue != nil && w.queue.HasPath(op.Path) {
				if !w.waitPathCommit(op.Path) {
					return fs.Result{}, fs.StatusErrorOf(gofuse.Status(syscall.EAGAIN))
				}
			}
			// A staged .dat/.meta image journals the mode onto the pending
			// meta (setPendingMetadataMode): the uploader applies it
			// post-landing. The reply carries the requested mode for the
			// same attr-cache reason as the live-handle deferral above.
			if w.wbCache != nil {
				if meta, ok := w.wbCache.GetMeta(op.Path); ok {
					if err := w.wbCache.UpdateMode(w.remotePath(op.Path), op.Mode&0o7777); err == nil {
						return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: op.Mode & 0o7777, Size: meta.Size}}, nil
					}
				}
			}
			res, err := next(ctx, op)
			if err != nil && fs.IsNotFound(err) && w.queue != nil && w.queue.HasPath(op.Path) {
				// The chmod raced the path's own enqueue (RELEASE is
				// processed asynchronously, so the queue entry can appear
				// after the checks above): wait for the landing and retry
				// the remote chmod once.
				if w.waitPathCommit(op.Path) {
					res, err = next(ctx, op)
				}
			}
			if err == nil {
				w.invalidate(op.Path, 0)
			}
			return res, err
		}
		if op.Size >= 0 {
			// A size change on a path with a live writer is local:
			// the kernel delivers open(O_TRUNC) on an existing file as
			// OPEN + SETATTR(size=0, fh) — the handle's buffer must
			// rebase to the new size and the commit publishes it. Sending
			// it to the remote now would leave the handle preloading the
			// pre-truncation base and republish it at close.
			if h := w.handleFor(op.Path); h != nil {
				h.mu.Lock()
				unlinked := h.unlinked
				mode, hasMode := h.pendingMode, h.hasPendingMode
				h.mu.Unlock()
				if !unlinked {
					if err := h.truncateLocal(ctx, op.Size); err == nil {
						// Every hardlink alias's open handle rebases with
						// this truncate (v1
						// syncOpenHandlesAfterPathTruncate).
						w.syncAliasHandlesAfterTruncate(op.Ino, op.Path, op.Size)
						// Publish the new size (UpdateSize +
						// cacheFileForPath at setattr-on-handle): the FUSE
						// SETATTR reply reflects it, and the kernel drops
						// its stale tail pages instead of stitching them
						// into the next writeback. The reply must carry the
						// permission bits — a Mode-less settle poisons the
						// kernel's 30s attr cache with 0000 and every later
						// open EACCESes under default_permissions.
						if !hasMode {
							mode = 0o644
						}
						w.invalidate(op.Path, op.Size)
						return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: op.Size}}, nil
					}
				}
			}
			// A clean-path truncate must not race the path's own pending
			// landings: a queued commit or a staged .dat/.meta image landing
			// mid-truncate 409s the conditional write (EIO) or resurrects the
			// pre-truncation size after it. Land everything first, then let
			// the terminal truncate a settled base.
			if w.queue != nil && w.queue.HasPath(op.Path) {
				if !w.waitPathCommit(op.Path) {
					return fs.Result{}, fs.StatusErrorOf(gofuse.Status(syscall.EAGAIN))
				}
			}
			if w.wbCache != nil && w.uploader != nil {
				if _, ok := w.wbCache.GetMeta(op.Path); ok {
					if err := w.uploader.UploadSync(ctx, op.Path); err != nil {
						return fs.Result{}, mapErr(err)
					}
				}
			}
			// A clean-path truncate beyond the in-memory threshold streams
			// remotely (v1 applyRemoteTruncate's >64MB branch): the kept
			// prefix is fetched lazily part-by-part through a ReaderAt, so
			// truncating a huge file never materializes the image.
			if op.Size > maxPathTruncateInMemoryBytes {
				if attr, ok := w.statForTruncate(ctx, op.Path); ok {
					if err := uploadRemoteTruncateFile(ctx, w.opts.Client, w.remotePath(op.Path), attr.Size, op.Size, -1); err != nil {
						return fs.Result{}, mapErr(err)
					}
					w.invalidate(op.Path, op.Size)
					return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Size: op.Size}}, nil
				}
				// No remote entry (lazy-created or already gone): fall
				// through and let the terminal answer.
			}
			res, err := next(ctx, op)
			if err == nil {
				w.invalidate(op.Path, op.Size)
			}
			return res, err
		}
	}
	return next(ctx, op)
}

// KeepForgotten reports live writers or queued commits for a path (// hasPendingLocalState || hasQueuedCommit): their inode mapping must
// survive a kernel FORGET until the state drains.
func (w *Writeback) KeepForgotten(path string) bool {
	if w.handleFor(path) != nil {
		return true
	}
	return w.queue != nil && w.queue.HasPath(path)
}

// Read implements fs.Handle.
func (h *bufferingHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	if h.pendingImage != nil {
		if off < 0 || off >= int64(len(h.pendingImage)) {
			h.mu.Unlock()
			return 0, nil
		}
		n := copy(dest, h.pendingImage[off:])
		h.mu.Unlock()
		return n, nil
	}
	if h.buf == nil {
		isNew := h.isNew
		if !isNew {
			// OpenWritable preload: the first touch materializes the staged
			// .dat/.meta image (newer than the remote) instead of serving the
			// remote's stale bytes.
			if h.w.wbCache != nil {
				if _, ok := h.w.wbCache.GetMeta(h.path); ok {
					if err := h.ensureBufferLocked(ctx); err == nil && h.buf != nil {
						n := h.buf.ReadAt(off, dest)
						if end := h.buf.Size() - off; end < int64(len(dest)) {
							if end < 0 {
								end = 0
							}
							n = int(end)
						}
						h.mu.Unlock()
						return n, nil
					}
				}
			}
		}
		h.mu.Unlock()
		if isNew {
			return 0, nil // nothing written yet
		}
		// Read-your-writes across fds (v1 readSamePathDirtyHandleVisibleRange):
		// an unflushed dirty buffer is POSIX-visible to every fd of the
		// file — the second open reads what the first wrote, not the stale
		// remote image.
		if n, ok := h.readLiveWriter(ctx, off, dest); ok {
			return n, nil
		}
		// Read-your-staged-writes (v1 pendingIndex overlay): a released handle's
		// image lives in the shadow store until the queue lands it — the remote
		// still 404s inside that window and the .dat/.meta entry is gone (the
		// queue owns the upload), so serve the staged bytes.
		if h.w.opts.Staging != nil && h.w.opts.Staging.Pending.HasPending(h.path) {
			if data, kok := readShadowGenerationFenced(h.w.opts.Staging.Shadow, h.path, h.baseSize); kok && data != nil {
				if off >= int64(len(data)) {
					return 0, nil
				}
				return copy(dest, data[off:]), nil
			}
		}
		// An anonymous clean fd of a deleted name reads its unlink snapshot
		// (v1 survivor): the remote is about to lose the object at last
		// close, and the fd must keep serving the pre-delete image.
		if h.w.opts.Staging != nil && h.w.unlinkedShadowServed(h.path) {
			if data, rerr := h.w.opts.Staging.Shadow.ReadAll(h.path); rerr == nil && data != nil {
				if off >= int64(len(data)) {
					return 0, nil
				}
				return copy(dest, data[off:]), nil
			}
		}
		// A pending-overlay handle (read-only re-open of a not-yet-landed
		// image) has no inner terminal; the pending tracks above were its
		// only sources.
		if h.inner == nil {
			return 0, fs.ErrNotFound
		}
		return h.inner.Read(ctx, off, dest)
	}
	// Load every part the read touches before serving: ReadAt zero-fills
	// unloaded parts, which is only correct for regions beyond the remote
	// base (EnsureLoaded-before-read).
	if err := h.buf.EnsurePartsForWrite(off, int64(len(dest))); err != nil {
		h.mu.Unlock()
		return 0, err
	}
	n := h.buf.ReadAt(off, dest)
	if end := h.buf.Size() - off; end < int64(len(dest)) {
		if end < 0 {
			end = 0
		}
		n = int(end)
	}
	h.mu.Unlock()
	return n, nil
}

// readLiveWriter serves a clean handle's read from the live writer's dirty
// buffer (v1 readSamePathDirtyHandleVisibleRange): POSIX makes an unflushed
// write visible to every fd of the file. ok=false when no live writer holds
// a dirty buffer (fall through to the staged/remote tiers).
func (h *bufferingHandle) readLiveWriter(ctx context.Context, off int64, dest []byte) (int, bool) {
	v, ok := h.w.live.Load(h.path)
	if !ok {
		return 0, false
	}
	writer, ok := v.(*bufferingHandle)
	if !ok || writer == h {
		return 0, false
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if !writer.dirty || writer.buf == nil {
		return 0, false
	}
	if err := writer.ensureBufferLocked(ctx); err != nil || writer.buf == nil {
		return 0, false
	}
	n := writer.buf.ReadAt(off, dest)
	if end := writer.buf.Size() - off; end < int64(len(dest)) {
		if end < 0 {
			end = 0
		}
		n = int(end)
	}
	return n, true
}

// Write implements fs.Handle.
func (h *bufferingHandle) Write(ctx context.Context, off int64, data []byte) error {
	h.mu.Lock()
	if err := h.ensureBufferLocked(ctx); err != nil {
		h.mu.Unlock()
		return err
	}
	// O_APPEND: the kernel may deliver off=0; the write lands at the
	// buffer tail (append semantics for buffered handles).
	if h.flags&uint32(syscall.O_APPEND) != 0 && off == 0 {
		off = h.buf.Size()
	}
	if err := h.maybeArmStreaming(); err != nil {
		h.mu.Unlock()
		return err
	}
	if _, err := h.buf.Write(off, data); err != nil {
		h.mu.Unlock()
		return err
	}
	h.dirty = true
	policy := h.w.opts.Policy
	forceSync := h.forceSync
	h.mu.Unlock()

	if policy == PolicyWriteSync || forceSync {
		return h.commitLocked(ctx)
	}
	return nil
}

// DrainFlush publishes the buffered image through the durable track for a
// mount-drain of an OPEN handle (v1 FlushAll): unlike the kernel Flush —
// which only snapshots locally and lets Release own the upload — no close
// boundary is coming, so the image stages into the ledger now and the
// facade's chain drain waits for the landing.
func (h *bufferingHandle) DrainFlush(ctx context.Context) error {
	h.mu.Lock()
	staged, unlinked := h.stagedToCache, h.unlinked
	h.stagedToCache = false
	h.mu.Unlock()
	if staged && !unlinked && h.w.uploader != nil && h.w.wbCache != nil {
		if _, ok := h.w.wbCache.GetMeta(h.path); ok {
			// v1 FlushAll over the write-cache track: the Flush snapshot
			// owns the upload — land it synchronously, exactly once. A
			// commitLocked here would publish a second image that races
			// the uploader's CAS on the same base.
			if err := h.w.uploader.UploadSync(ctx, h.path); err != nil {
				return err
			}
			h.mu.Lock()
			h.dirty = false
			if h.buf != nil {
				h.buf.ClearDirty()
			}
			h.landed = true
			h.mu.Unlock()
			return nil
		}
	}
	if err := h.commitLocked(ctx); err != nil {
		return err
	}
	if !h.w.waitPathCommit(h.path) {
		return fs.StatusErrorOf(gofuse.Status(syscall.EAGAIN))
	}
	return nil
}

// Sync implements fs.Handle.
func (h *bufferingHandle) Sync(ctx context.Context) error {
	// interactive mode: fsync is a durability barrier served by
	// the local staging ledger — stage + async enqueue, and the queue lands the
	// remote commit; no full-queue wait (WAN mounts would otherwise block
	// every fsync behind unrelated uploads). Without a ledger nothing is
	// locally durable, so strict applies.
	if h.w.interactive && h.w.opts.Policy == PolicyWriteBack && h.w.queue != nil && h.w.opts.Staging != nil {
		// v1 interactive fsync: the staging is unconditionally durable and
		// a JournalFsync frame fences the async commit's marker ordering.
		return h.stageAndEnqueueOpt(ctx, true, true)
	}
	if err := h.commitLocked(ctx); err != nil {
		return err
	}
	// fsync is a durability barrier: under the async queue policy the image
	// is staged+enqueued — wait for THIS path to land, not the whole queue
	// (a foreign path's slow upload must not hold this fsync hostage).
	if h.w.queue != nil {
		if !h.w.queue.WaitPathDeadline(h.path, uploadTimeout) {
			return fs.StatusErrorOf(gofuse.Status(syscall.EAGAIN))
		}
	}
	// A staged .dat/.meta snapshot is only locally durable; strict fsync
	// pays the remote landing synchronously (v1 fsync on the write-back
	// cache fast path).
	h.mu.Lock()
	staged := h.stagedToCache
	h.mu.Unlock()
	if staged && h.w.uploader != nil {
		rev, err := h.w.uploader.UploadSyncWithRevision(ctx, h.path)
		if err != nil {
			return err
		}
		h.mu.Lock()
		h.stagedToCache = false
		h.mu.Unlock()
		h.markLanded(rev)
	}
	return nil
}

// Flush implements fs.Handle.
func (h *bufferingHandle) Flush(ctx context.Context) error {
	// Inline-path writeback coalesces rapid flushes of one path into one
	// upload (flushHandleDebounced): without a staging ledger every
	// Flush would otherwise PUT synchronously. Release cancels the pending
	// callback and commits synchronously, so close(2) semantics hold.
	if h.w.opts.Policy == PolicyCloseSync {
		h.mu.Lock()
		dirty, unlinked, truncPending := h.dirty, h.unlinked, h.trunc && h.buf == nil
		isNew, landed, buffered := h.isNew, h.landed, h.buf != nil
		h.mu.Unlock()
		// v1 createEmptyHandleRemote: an O_CREAT claim that never wrote
		// still materializes at close(2) — deferring it to Release lets
		// the next syscall (chmod/link/unlink/rmdir preflight) answer
		// from a remote that has not seen the file yet.
		emptyCreate := isNew && !landed && !buffered
		if (!dirty && !truncPending && !emptyCreate) || unlinked {
			return nil
		}
		size := int64(0)
		if buffered {
			h.mu.Lock()
			if h.buf != nil {
				size = h.buf.Size()
			}
			h.mu.Unlock()
		}
		if size >= commitInlineStreamThreshold && h.w.queue != nil && h.w.opts.Staging != nil {
			// v1 close-sync large files (uploadCloseSyncShadow): stage
			// through the shadow spill path — bounded memory, no whole-image
			// materialization — and land synchronously through the queue's
			// streaming upload, waiting for THIS path only.
			if err := h.stageAndEnqueueOpt(ctx, true, false); err != nil {
				return err
			}
			if !h.w.queue.WaitPathDeadline(h.path, uploadTimeout) {
				return fs.StatusErrorOf(gofuse.Status(syscall.EAGAIN))
			}
			return nil
		}
		// close-sync owes remote durability at close(2) itself (its
		// defining contract): the .dat snapshot + a synchronous landing.
		// Deferring to Release would leave remote listings lagging closes,
		// and rmdir/rename preflights then read stale emptiness (461
		// pjdfstest rows in close-sync mode). The commit context detaches
		// from the FUSE cancel channel under interrupt-safe mutations (v1
		// syncDataCommitContext): a kernel interrupt after the PUT hit the
		// wire must not cancel it into an ambiguous server-side state.
		if h.w.uploader != nil && h.w.wbCache != nil {
			if !h.w.opts.InterruptibleCommits {
				var detachCancel context.CancelFunc
				ctx, detachCancel = h.w.detachCommitContext(ctx, 0)
				defer detachCancel()
			}
			isNew := func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.isNew }()
			if err := h.stageToWriteCache(ctx, isNew); err != nil {
				return err
			}
			h.mu.Lock()
			h.stagedToCache = true
			h.mu.Unlock()
			rev, err := h.w.uploader.UploadSyncWithRevision(ctx, h.path)
			if err != nil {
				return err
			}
			h.mu.Lock()
			h.stagedToCache = false
			// The image is on the remote now — for every create shape, not
			// only the empty one: Release's materialize path keys on
			// landed, and re-materializing a published image would PUT a
			// second copy at the open-time base (close-sync double-upload).
			h.dirty = false
			h.mu.Unlock()
			// The landed revision also retires isNew: a later write on
			// this handle must stage as an overwrite of that revision.
			h.markLanded(rev)
			return nil
		}
		if emptyCreate {
			// No write-cache track: materialize the empty buffer so the
			// inline commit below publishes it.
			if err := h.ensureBuffer(ctx); err != nil {
				return err
			}
			h.mu.Lock()
			h.dirty = true
			h.mu.Unlock()
		}
		// No write-cache track: fall through to the inline commit below.
	}
	if h.w.opts.Policy == PolicyWriteBack && h.w.uploader != nil && h.w.wbCache != nil {
		// small-file writeback track: the flush stages a durable
		// .dat/.meta image on disk and the uploader lands it async; the
		// commit queue keeps serving the large-file/shadow track.
		h.mu.Lock()
		dirty, unlinked, isNew := h.dirty, h.unlinked, h.isNew
		size := int64(0)
		if h.buf != nil {
			size = h.buf.Size()
		}
		h.mu.Unlock()
		if !dirty || unlinked {
			return nil
		}
		if size < commitInlineStreamThreshold {
			if err := h.stageToWriteCache(ctx, isNew); err != nil {
				return err
			}
			// v1 small-file contract (write-back policy): the Flush
			// snapshot is local-only. Uploading here would publish transient
			// mid-stream states as their own commits (an O_TRUNC open
			// delivers OPEN + SETATTR(size=0) + FLUSH before the first
			// WRITE: the empty image landed first and the final image's CAS
			// at the open-time base revision then conflicts forever —
			// fuse-smoke's overwrite step). Release submits the final
			// snapshot exactly once.
			h.mu.Lock()
			h.stagedToCache = true
			h.mu.Unlock()
			return nil
		}
		// v1 writeBackThreshold: large images skip the whole-image .dat/.meta
		// track and stage through the shadow spill path instead — bounded
		// memory, local durability at close(2), and the queue streams the
		// upload asynchronously (the re-open read path serves the staged
		// image from the shadow pending overlay).
		if h.w.queue != nil && h.w.opts.Staging != nil {
			return h.stageAndEnqueue(ctx)
		}
		return h.commitInline(ctx)
	}
	if h.w.opts.Policy == PolicyWriteBack && h.w.debounce != nil && h.w.queue != nil && h.w.opts.Staging != nil {
		// flush-debounce coalescing applies ONLY with a locally durable
		// staging ledger: flush stages synchronously and the debounced
		// callback only coalesces the remote landing. Without local
		// durability a debounced flush would return before the data exists
		// anywhere — close(2) semantics break and reads inside the window
		// see nothing (or race backend replica lag with 404s).
		h.mu.Lock()
		dirty, unlinked := h.dirty, h.unlinked
		h.mu.Unlock()
		if !dirty || unlinked {
			return nil
		}
		h.w.debounce.ScheduleWithOwner(h.path, h, func() {
			bgctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
			defer cancel()
			_ = h.commitLocked(bgctx)
		})
		return nil
	}
	if h.w.opts.Policy == PolicyWriteSync || h.forceSync {
		h.mu.Lock()
		dirty, unlinked := h.dirty, h.unlinked
		isNew, landed, buffered := h.isNew, h.landed, h.buf != nil
		h.mu.Unlock()
		if unlinked {
			return nil
		}
		// An O_CREAT claim that never wrote materializes at the flush
		// boundary like v1's createEmptyHandleRemote — same contract as
		// close-sync: close(2) returns with the name existing remotely.
		if !dirty && (!isNew || landed || buffered) {
			return nil // already durable per Write
		}
		if !dirty {
			if err := h.ensureBuffer(ctx); err != nil {
				return err
			}
			h.mu.Lock()
			h.dirty = true
			h.mu.Unlock()
		}
		// A truncation-at-open with no writes is the only other dirty
		// state the write path leaves behind; publish it at the flush
		// boundary.
	}
	return h.commitLocked(ctx)
}

// Release implements fs.Handle.
func (h *bufferingHandle) Release(ctx context.Context) error {
	h.w.untrack(h)
	if h.w.debounce != nil {
		h.w.debounce.Cancel(h.path)
	}
	// The last handle of an unlinked name releases its snapshot (v1
	// survivor cleanup).
	if h.w.unlinkedShadowServed(h.path) && h.w.handleFor(h.path) == nil {
		h.w.dropUnlinkedShadow(h.path)
	}
	// An O_CREAT claim that never landed must still materialize as a remote
	// file (stages it in pendingIndex at Create): git probes the
	// filesystem by creating and unlinking `.git/ä` with no writes, and a
	// vanished create breaks the unlink and every later step. Keyed on
	// landed, not dirty: write-sync commits mid-stream and clears dirty,
	// and re-materializing then would clobber the committed image with an
	// empty buffer.
	h.mu.Lock()
	materialize := h.isNew && !h.landed && !h.unlinked
	staged, unlinked := h.stagedToCache, h.unlinked
	dirty := h.dirty
	isNew := h.isNew
	h.stagedToCache = false
	h.mu.Unlock()
	if staged && !unlinked && h.w.uploader != nil && h.w.wbCache != nil {
		if _, ok := h.w.wbCache.GetMeta(h.path); ok {
			if dirty {
				// A write landed between Flush and Release: the staged
				// snapshot is stale — refresh it from the current buffer
				// before submitting (v1: the latest image always wins;
				// submitting the stale snapshot loses the newer write).
				if err := h.stageToWriteCache(ctx, isNew); err != nil {
					return err
				}
			}
			// v1 write-back cache fast path at Release: the Flush snapshot
			// owns the close upload — submit it exactly once instead of
			// re-committing (commitLocked would publish a second image and
			// race the uploader's CAS on the same base).
			h.w.uploader.Submit(h.path)
			h.mu.Lock()
			h.dirty = false
			if h.buf != nil {
				h.buf.ClearDirty()
			}
			h.landed = true
			h.mu.Unlock()
			if h.inner != nil {
				_ = h.inner.Release(ctx)
			}
			return nil
		}
	}
	if materialize {
		if err := h.ensureBuffer(ctx); err == nil {
			h.mu.Lock()
			h.dirty = true
			h.mu.Unlock()
		}
	}
	err := h.commitLocked(ctx)
	if h.inner != nil {
		if relErr := h.inner.Release(ctx); err == nil {
			err = relErr
		}
	}
	return err
}

// pendingServeMaterializeBytes bounds the images a pending-overlay open
// materializes into its own buffer: the fd then survives the staged
// entry's removal (unlink) like any buffered handle. Larger images stay
// views over the shared staged bytes (a 2GiB spill must not allocate).
const pendingServeMaterializeBytes = 16 << 20

// capturePendingImage grabs the pending image as an immutable slice. The
// serve handle reads ONLY this slice — routing it through the write
// buffer let EnsurePartsForWrite re-fetch "missing" parts from the remote
// base (still the pre-landing image), and an unfenced shadow read raced
// concurrent restages (the tier-transition reader served the PREVIOUS
// image under the fresh staging).
func (w *Writeback) capturePendingImage(path string) []byte {
	size := int64(0)
	if meta, ok := w.pendingMeta(path); ok && meta != nil {
		size = meta.Size
	}
	if size <= 0 || size > pendingServeMaterializeBytes {
		return nil
	}
	if w.wbCache != nil {
		if data, ok := w.wbCache.Get(path); ok && int64(len(data)) == size {
			return data
		}
	}
	if w.opts.Staging != nil && w.opts.Staging.Pending.HasPending(path) {
		if data, ok := readShadowGenerationFenced(w.opts.Staging.Shadow, path, size); ok {
			return data
		}
	}
	return nil
}

// readShadowGenerationFenced reads the shadow only while its content
// generation is stable across the read, retrying once when a concurrent
// restage moved it.
func readShadowGenerationFenced(shadows *staging.ShadowStore, path string, size int64) ([]byte, bool) {
	if shadows == nil {
		return nil, false
	}
	for attempt := 0; attempt < 2; attempt++ {
		gen := shadows.ActiveGeneration(path)
		if gen == 0 {
			return nil, false
		}
		data, err := shadows.ReadAllIfGeneration(path, gen)
		if err == nil && int64(len(data)) == size {
			return data, true
		}
	}
	return nil, false
}

// markLanded records a successful remote landing for this handle: the
// name exists now, so later stagings are overwrites carrying the landed
// revision as their CAS base (v1 cleared fh.IsNew at every commit settle
// — a stale isNew staged a create-CAS against the just-landed name and
// the rewrite conflicted forever).
// checkoutDropCleanBuffer retires a CLEAN buffered image at a view
// checkout: the buffer was only a read cache of the abandoned base, and
// serving it forever would hide the new view's bytes. A dirty buffer (or a
// pending local truncation) is this mount's own data and survives; the
// next read reloads from the new base through the terminal.
func (h *bufferingHandle) checkoutDropCleanBuffer() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unlinked || h.dirty || h.pendingImage != nil {
		return
	}
	h.buf = nil
}

func (h *bufferingHandle) markLanded(rev int64) {
	h.mu.Lock()
	h.landed = true
	h.isNew = false
	if rev > 0 {
		h.baseRev = rev
	}
	h.mu.Unlock()
}

func attrSize(res fs.Result) int64 {
	if res.Attr != nil {
		return res.Attr.Size
	}
	return 0
}

// pendingAttr answers a lookup/getattr from the write-side pending state —
// the staging pendingIndex (commit-queue track, keyed by local path) and the
// .dat/.meta cache (uploader track, keyed by remote path). The pendingIndex
// entry wins: both tracks can hold the same path transiently (Flush staged
// the cache image, then a strict fsync staged the ledger image), and the
// queue's landing is the authoritative one.
func (w *Writeback) pendingAttr(path string) (fs.Attr, bool) {
	if meta, ok := w.pendingMeta(path); ok {
		return attrFromPendingMeta(meta)
	}
	return fs.Attr{}, false
}

// pendingMeta returns the staged image's metadata from either pending
// track (ledger first, mirroring pendingAttr's precedence).
func (w *Writeback) pendingMeta(path string) (*staging.WriteBackMeta, bool) {
	if w.opts.Staging != nil {
		if meta, ok := w.opts.Staging.Pending.GetMeta(path); ok {
			return meta, true
		}
	}
	if w.wbCache != nil {
		if meta, ok := w.wbCache.GetMeta(path); ok {
			return meta, true
		}
	}
	return nil, false
}

// attrFromPendingMeta builds the overlay reply for a staged image. Only a
// PendingNew (the server default 0644 applies at create) or an entry with an
// explicit pending mode can answer — an overwrite without a mode leaves the
// remote's mode authoritative and must fall through to the chain.
func attrFromPendingMeta(meta *staging.WriteBackMeta) (fs.Attr, bool) {
	if meta == nil {
		return fs.Attr{}, false
	}
	if !meta.HasMode && meta.Kind != staging.PendingNew {
		return fs.Attr{}, false
	}
	mode := uint32(0o644)
	if meta.HasMode {
		mode = meta.Mode & 0o7777
	}
	return fs.Attr{Kind: fs.KindFile, Mode: mode, Size: meta.Size}, true
}

// overlayPendingListAttrs rewrites and completes listing rows with this
// mount's pending write state before the caller publishes them (the OpList
// half of the LOOKUP/GetAttr pending overlay above). Size is authoritative
// from the pending image — a staged image is newer than anything the remote
// can serve, including a smaller one — while mode follows attrFromPendingMeta's
// rule: only a PendingNew (server default 0644) or an explicit pending
// mode answers, so a mode-less overwrite keeps the row's remote mode.
//
// Names are completed too: a fresh listing after a cache invalidation
// cannot carry a file whose commit has not landed, and readdir must not
// hide it for the landing's duration (the v1/#918 resolve-before-directory-
// conversion contract) — staged tracks and live writers both contribute.
func (w *Writeback) overlayPendingListAttrs(dir string, entries []fs.Entry) []fs.Entry {
	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		e := &entries[i]
		seen[e.Name] = struct{}{}
		if e.Attr.IsDir() {
			continue
		}
		if size, mode, hasMode, ok := w.pendingListAttr(pathutil.JoinPath(dir, e.Name)); ok {
			e.Attr.Size = size
			if hasMode {
				e.Attr.Mode = mode
			}
		}
	}
	for _, name := range w.pendingNamesUnder(dir, seen) {
		size, mode, _, ok := w.pendingListAttr(pathutil.JoinPath(dir, name))
		if !ok {
			continue
		}
		entries = append(entries, fs.Entry{Name: name, Attr: fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size}})
	}
	return entries
}

// pendingListAttr resolves the newest write-plane state for one path: a
// staged image's metadata, overridden by a live writer's buffer (its bytes
// may not have reached staging yet — v1 snapshotted published write-back
// metadata per listing).
func (w *Writeback) pendingListAttr(p string) (size int64, mode uint32, hasMode, ok bool) {
	size = -1
	if meta, mok := w.pendingMeta(p); mok && meta != nil {
		size = meta.Size
		if meta.HasMode {
			mode, hasMode = meta.Mode&0o7777, true
		} else if meta.Kind == staging.PendingNew {
			mode, hasMode = 0o644, true
		}
	}
	if h := w.handleFor(p); h != nil && !h.unlinked {
		h.mu.Lock()
		if h.buf != nil {
			size = h.buf.Size()
		}
		if h.hasPendingMode {
			mode, hasMode = h.pendingMode&0o7777, true
		} else if h.isNew && !hasMode {
			mode, hasMode = 0o644, true
		}
		h.mu.Unlock()
	}
	if size < 0 {
		return 0, 0, false, false
	}
	return size, mode, hasMode, true
}

// pendingNamesUnder collects the direct children of dir the write plane
// still owns that the listing did not carry, in deterministic order.
func (w *Writeback) pendingNamesUnder(dir string, seen map[string]struct{}) []string {
	var names []string
	add := func(p string) {
		if pathutil.ParentPath(p) != dir {
			return
		}
		name := pathutil.BaseName(p)
		if _, dup := seen[name]; dup {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	if w.wbCache != nil {
		for p := range w.wbCache.ListPendingPaths() {
			add(p)
		}
	}
	if w.opts.Staging != nil && w.opts.Staging.Pending != nil {
		for p := range w.opts.Staging.Pending.ListPendingPaths() {
			add(p)
		}
	}
	w.live.Range(func(key, _ any) bool {
		if p, ok := key.(string); ok {
			add(p)
		}
		return true
	})
	sort.Strings(names)
	return names
}

func attrRev(res fs.Result) int64 {
	if res.Attr != nil {
		return res.Attr.Rev
	}
	return 0
}

// mapErr keeps the errno table (httperr.HTTPStatusOf): 404 → fs.ErrNotFound,
// revision-conflict 409 → EIO, quota 507 → EDQUOT/EFBIG, 5xx/499 → EAGAIN.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var se *client.StatusError
	if !errors.As(err, &se) {
		return err
	}
	if se.StatusCode == http.StatusNotFound {
		return fs.ErrNotFound
	}
	return &fs.StatusError{Status: httperr.HTTPStatusOf(err)}
}

// statForTruncate resolves a path's remote size for the streaming-truncate
// claim. ok=false (missing remote entry or a lookup error other than
// not-found defers the decision) lets the op continue down the chain.
func (w *Writeback) statForTruncate(ctx context.Context, path string) (fs.Attr, bool) {
	st, err := w.opts.Client.StatCtx(ctx, w.remotePath(path))
	if err != nil || st == nil {
		return fs.Attr{}, false
	}
	return fs.Attr{Kind: fs.KindFile, Size: st.Size}, true
}

func (w *Writeback) armBypass(path string, rev, size int64, reason string) {
	if w.bypass != nil {
		w.bypass.ArmBypass(path, rev, size, reason)
	}
}

func (w *Writeback) remotePath(p string) string {
	return pathutil.WithRemoteRoot(w.root, p)
}

// localPathOf maps a remote (uploader/cache) path back onto the view.
func (w *Writeback) localPathOf(rp string) string {
	if w.root == "/" || rp == w.root {
		return "/"
	}
	return strings.TrimPrefix(rp, w.root)
}

// track registers an open writer by path (newest wins, matching 's
// per-path Setattr deferral) and in the walk set for unlink/rename.
func (w *Writeback) track(h *bufferingHandle) {
	w.live.Store(h.path, h)
	w.liveSet.Store(h, struct{}{})
}

func (w *Writeback) untrack(h *bufferingHandle) {
	w.live.Delete(h.path)
	w.liveSet.Delete(h)
}

// liveHandles tracks open buffering handles so SetAttr/unlink can find the
// writer that owns a path.
func (w *Writeback) handleFor(path string) *bufferingHandle {
	h, ok := w.live.Load(path)
	if !ok {
		return nil
	}
	return h.(*bufferingHandle)
}

// markUnlinked flags live handles at p (or, for a removed directory, under
// unlinkedSnapshotMaxBytes caps the clean-fd unlink snapshot (v1 read the
// whole image; larger files keep relying on their buffer, and a
// multi-gigabyte read-only fd must not OOM the mount at unlink).
const unlinkedSnapshotMaxBytes = 64 << 20

// markUnlinked makes every live writer of p (or, under, its subtree)
// anonymous: the buffer keeps serving the fd, but commits are discarded
// so Release cannot resurrect the path. Per-handle, so a re-created p stays
// committable by its new writer (fh.Unlinked).
func (w *Writeback) markUnlinked(p string, under bool) {
	prefix := strings.TrimSuffix(p, "/") + "/"
	var clean []*bufferingHandle
	w.liveSet.Range(func(key, _ any) bool {
		h := key.(*bufferingHandle)
		h.mu.Lock()
		if h.path == p || (under && strings.HasPrefix(h.path, prefix)) {
			h.unlinked = true
			if h.buf == nil && !h.isNew && !h.dirty {
				clean = append(clean, h)
			}
		}
		h.mu.Unlock()
		return true
	})
	if w.opts.Staging != nil {
		for _, h := range clean {
			// v1 snapshotUnlinkedRemoteToShadow: a clean read-only fd of
			// the deleted name keeps a snapshot in the shadow — its reads
			// must survive the remote DELETE at last close. The snapshot
			// carries no pending entry, so nothing ever re-uploads it.
			w.snapshotUnlinkedClean(h)
		}
	}
}

// snapshotUnlinkedClean stages one clean handle's remote image into the
// shadow for post-delete reads.
func (w *Writeback) snapshotUnlinkedClean(h *bufferingHandle) {
	st := w.opts.Staging
	if st.Pending.HasPending(h.path) {
		return // a staged image already serves post-delete reads
	}
	if h.inner == nil {
		// A pending-overlay handle (read-only re-open of a not-yet-landed
		// image) has no terminal to fetch from; its post-delete reads are
		// served by the pending tracks above.
		return
	}
	size := h.baseSize
	if size <= 0 || size > unlinkedSnapshotMaxBytes {
		return
	}
	buf := make([]byte, size)
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()
	n, err := h.inner.Read(ctx, 0, buf)
	if err != nil || int64(n) != size {
		return
	}
	if err := st.Shadow.WriteFull(h.path, buf, h.baseRev); err != nil {
		return
	}
	w.recordUnlinkedSnapshot(h.path, st.Shadow.ActiveGeneration(h.path))
}

// recordUnlinkedSnapshot installs the unlink-snapshot marker with the
// shadow generation the snapshot captured, so a later drop only removes
// bytes that are still the snapshot's own.
func (w *Writeback) recordUnlinkedSnapshot(path string, gen uint64) {
	w.unlinkedMu.Lock()
	defer w.unlinkedMu.Unlock()
	if w.unlinkedShadow == nil {
		w.unlinkedShadow = map[string]struct{}{}
	}
	w.unlinkedShadow[path] = struct{}{}
	if gen != 0 {
		if w.unlinkedShadowGen == nil {
			w.unlinkedShadowGen = map[string]uint64{}
		}
		w.unlinkedShadowGen[path] = gen
	}
}

// unlinkedShadowServed reports whether a path has an unlink snapshot in
// the shadow (the read fallback for anonymous clean fds).
func (w *Writeback) unlinkedShadowServed(path string) bool {
	w.unlinkedMu.Lock()
	defer w.unlinkedMu.Unlock()
	_, ok := w.unlinkedShadow[path]
	return ok
}

// dropUnlinkedShadow releases a path's unlink snapshot once its last
// handle is gone.
func (w *Writeback) dropUnlinkedShadow(path string) {
	w.unlinkedMu.Lock()
	delete(w.unlinkedShadow, path)
	gen := w.unlinkedShadowGen[path]
	delete(w.unlinkedShadowGen, path)
	w.unlinkedMu.Unlock()
	if w.opts.Staging == nil {
		return
	}
	// Generation-fenced removal: when the path was re-created after the
	// unlink, its new staging replaced the snapshot's generation, and the
	// old object's cleanup must leave the new bytes for their own commit
	// (a plain Remove here rejected the replacement's upload as a stale
	// payload and terminally conflicted it).
	if gen != 0 {
		if w.opts.Staging.Shadow.RemoveIfGeneration(path, gen) {
			return
		}
		// A newer generation owns the shadow now; the snapshot's bytes are
		// gone with the overwrite — nothing of the old object remains.
		return
	}
	w.opts.Staging.Shadow.Remove(path)
}

// DrainPending reports undrained write-side work after a drain (v1
// drainPendingHasUndrainedWork): conflicted queue entries, uploader-cached
// snapshots, and dirty open handles each fail the drain — syncfs(2) and
// `drive9 mount drain` must not report success over abandoned data.
func (w *Writeback) DrainPending() (string, int) {
	if w.queue != nil {
		if snap := w.queue.Snapshot(); snap.Conflicts > 0 {
			return "commit_queue_conflicts", snap.Conflicts
		}
	}
	if w.uploader != nil {
		if snap := w.uploader.Snapshot(); snap.Cached > 0 {
			return "uploader_cached", snap.Cached
		}
	}
	if n := w.DirtyHandleCount(); n > 0 {
		return "dirty_handles", n
	}
	return "", 0
}

// markLinked is markUnlinked's rollback: a failed remote DELETE must not
// leave live writers anonymous, or their buffers would be discarded at the
// next commit boundary (the unmark pass).
func (w *Writeback) markLinked(p string, under bool) {
	prefix := strings.TrimSuffix(p, "/") + "/"
	w.liveSet.Range(func(key, _ any) bool {
		h := key.(*bufferingHandle)
		h.mu.Lock()
		if h.path == p || (under && strings.HasPrefix(h.path, prefix)) {
			h.unlinked = false
		}
		h.mu.Unlock()
		return true
	})
}

// retarget moves live handles onto the renamed path (renames fh.Path and
// retargets its streamer), including handles nested under a renamed
// directory, and re-keys the per-path writer index.
func (w *Writeback) retarget(oldPath, newPath string) {
	prefix := strings.TrimSuffix(oldPath, "/") + "/"
	w.liveSet.Range(func(key, _ any) bool {
		h := key.(*bufferingHandle)
		h.mu.Lock()
		switch {
		case h.path == oldPath:
			if cur, ok := w.live.Load(oldPath); ok && cur == h {
				w.live.Delete(oldPath)
				w.live.Store(newPath, h)
			}
			h.path = newPath
		case strings.HasPrefix(h.path, prefix):
			next := newPath + strings.TrimPrefix(h.path, oldPath)
			if cur, ok := w.live.Load(h.path); ok && cur == h {
				w.live.Delete(h.path)
				w.live.Store(next, h)
			}
			h.path = next
		}
		h.mu.Unlock()
		return true
	})
}

// probeExisting resolves a path's existence through the chain — the
// namespace cache answers both "exists with rev" and "this mount deleted
// the name" locally, where a raw remote stat races backend replica lag
// (unlink/14's re-create after a just-committed DELETE sees the stale
// replica and answers EEXIST).
func (w *Writeback) probeExisting(ctx context.Context, next fs.Next, path string) (fs.Attr, error) {
	res, err := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: path})
	if err != nil {
		return fs.Attr{}, err
	}
	if res.Attr == nil {
		return fs.Attr{}, fs.ErrNotFound
	}
	return *res.Attr, nil
}

// hasPendingLocalState reports whether a not-yet-landed image owns the path
// on either staging track.
func (w *Writeback) hasPendingLocalState(path string) bool {
	if w.queue != nil && w.queue.HasPath(path) {
		return true
	}
	if w.wbCache != nil {
		if _, ok := w.wbCache.GetMeta(path); ok {
			return true
		}
	}
	return w.opts.Staging != nil && w.opts.Staging.Pending.HasPending(path)
}

// landedAt records this mount's own just-landed commits (queue/uploader
// success): path → revision+size with a settle time. Lookups inside the
// window answer from local knowledge — the backend replicas converge
// asynchronously, and a chain stat racing the landing reads the stale
// replica and poisons the namespace cache with a negative row (the
// close/write-sync modes land on every close, so this seam dominates
// their ENOENT counts).
func (w *Writeback) landedAt(path string, rev, size int64) {
	if rev <= 0 {
		return
	}
	w.landedMu.Lock()
	if w.landed == nil {
		w.landed = map[string]landedInfo{}
	}
	w.landed[path] = landedInfo{rev: rev, size: size, at: time.Now()}
	w.landedMu.Unlock()
}

func (w *Writeback) forgetLanded(path string) {
	w.landedMu.Lock()
	delete(w.landed, path)
	w.landedMu.Unlock()
}

func (w *Writeback) everLanded(path string) bool {
	w.landedMu.Lock()
	_, ok := w.landed[path]
	w.landedMu.Unlock()
	return ok
}

// landedAttr answers a lookup from the recently-landed table (30s window).
func (w *Writeback) landedAttr(path string) (fs.Attr, bool) {
	w.landedMu.Lock()
	info, ok := w.landed[path]
	w.landedMu.Unlock()
	if !ok || time.Since(info.at) > 30*time.Second {
		return fs.Attr{}, false
	}
	return fs.Attr{Kind: fs.KindFile, Mode: 0o644, Size: info.size, Rev: info.rev}, true
}

type landedInfo struct {
	rev  int64
	size int64
	at   time.Time
}

// pendingBaseRevOf resolves the CAS lineage for opening a path whose image
// is still pending locally (staged ledger or .dat/.meta): the eventual
// overwrite must CAS against the pending image's base revision, not 0 — the
// uploader refuses base-less overwrites and the write would never land
// ("writeback upload skipped: base revision required", chmod/12).
func (w *Writeback) pendingBaseRevOf(path string) (int64, bool) {
	if w.opts.Staging != nil {
		if meta, ok := w.opts.Staging.Pending.GetMeta(path); ok {
			return meta.BaseRev, true
		}
	}
	if w.wbCache != nil {
		if meta, ok := w.wbCache.GetMeta(path); ok {
			return meta.BaseRev, true
		}
	}
	return 0, false
}

// waitPathCommit waits for a path's pending commits under the configured
// RemoteCommitWaitTimeout budget. It reports false only when the bounded
// wait expired with the path still busy (the caller surfaces EAGAIN); an
// unbounded budget (negative) never reports false.
func (w *Writeback) waitPathCommit(path string) bool {
	if w.queue == nil {
		return true // no async queue: nothing to wait for (nil-safe)
	}
	switch budget := w.opts.RemoteCommitWaitTimeout; {
	case budget < 0:
		w.queue.WaitPath(path)
		return true
	case budget == 0:
		return w.queue.WaitPathDeadline(path, 5*time.Second)
	default:
		return w.queue.WaitPathDeadline(path, budget)
	}
}

func (w *Writeback) invalidate(path string, size int64) {
	w.invalidateRev(path, 0, size)
}

func (w *Writeback) invalidateRev(path string, rev, size int64) {
	if w.opts.Events != nil {
		// A local commit refreshes views (Landed): the size rides along so
		// listing caches update the entry (cacheFileForPath) instead of
		// serving the pre-commit image's attributes; the committed revision
		// keys the read-cache seed (rev>0 commits settle in place).
		w.opts.Events.Emit(fs.Event{Kind: fs.EventLanded, Path: path, Rev: rev, Size: size})
	}
}

// bufferReader streams a WriteBuffer's image chunk-by-chunk: each Read
// loads only the parts it touches (EnsurePartsForWrite + ReadAt), so
// committing a sparse multi-GiB image never materializes the whole image
// in memory (open/25's pwrite at 2^31 OOM-killed small hosts).
type bufferReader struct {
	h    *bufferingHandle
	off  int64
	size int64
}

func (r *bufferReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.off)
	if n > 0 {
		r.off += int64(n)
	}
	return n, err
}

// ReadAt satisfies the client's large-upload contract: multipart uploads
// type-assert io.ReaderAt and seek per part — a plain sequential Reader is
// rejected with "large uploads require an io.ReaderAt".
func (r *bufferReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if off+n > r.size {
		n = r.size - off
	}
	if n <= 0 {
		return 0, io.EOF
	}
	r.h.mu.Lock()
	defer r.h.mu.Unlock()
	if r.h.buf == nil {
		return 0, fs.ErrUnsupported
	}
	if err := r.h.buf.EnsurePartsForWrite(off, n); err != nil {
		return 0, err
	}
	got := r.h.buf.ReadAt(off, p[:n])
	if got <= 0 {
		return 0, io.EOF
	}
	return got, nil
}

// commitInline publishes the buffer without the staging ledger, streaming
// images at or above commitInlineStreamThreshold instead of materializing
// them. Used by the sync policies' close path and by the async policy when
// the image is too large for the whole-image tracks (v1's large-file
// bypass of the .dat/.meta track).
func (h *bufferingHandle) commitInline(ctx context.Context) error {
	h.mu.Lock()
	size := int64(0)
	if h.buf != nil {
		size = h.buf.Size()
	}
	isNew := h.isNew
	var newReader func() io.Reader
	var image []byte
	if size >= commitInlineStreamThreshold {
		newReader = func() io.Reader { return &bufferReader{h: h, size: size} }
	} else {
		if err := h.buf.LoadMissingParts(); err != nil {
			h.mu.Unlock()
			return err
		}
		image = h.buf.Bytes()
		newReader = func() io.Reader { return bytes.NewReader(image) }
	}
	h.mu.Unlock()

	committedRev, err := h.w.commitReaderOpt(ctx, h.path, newReader, size, isNew && size == 0)
	if err == nil {
		if h.w.commitSeeder != nil && image != nil {
			h.w.commitSeeder(h.path, image, committedRev)
		}
		h.mu.Lock()
		h.dirty = false
		if h.buf != nil {
			h.buf.ClearDirty()
		}
		h.landed = true
		h.isNew = false
		if committedRev > 0 {
			h.baseRev = committedRev
		}
		h.mu.Unlock()
		if h.w.opts.Policy == PolicyWriteSync || h.w.opts.Policy == PolicyCloseSync {
			h.w.armBypass(h.path, 0, size, "sync-commit")
		}
		h.w.invalidateRev(h.path, committedRev, size)
		err = h.applyPendingMode(ctx)
	}
	return err
}

// detachCommitContext ports v1 syncDataCommitContext: under interrupt-safe
// mutations the commit detaches from the FUSE cancel channel and keeps a
// size-proportional budget; the legacy mode stays bound to the caller.
func (w *Writeback) detachCommitContext(parent context.Context, size int64) (context.Context, context.CancelFunc) {
	if w.opts.InterruptibleCommits {
		return parent, func() {}
	}
	budget := releaseTimeout(size)
	if deadline, ok := parent.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < budget {
			budget = remaining
		}
	}
	if budget <= 0 {
		budget = time.Nanosecond
	}
	return context.WithTimeout(context.WithoutCancel(parent), budget)
}

// commitReaderOpt is commitReader with the empty-create hint: a brand-new
// zero-byte file lands through the create endpoint first (v1 flushHandle's
// CreateFileCtx), falling back to the conditional write — the two endpoints
// differ server-side in permissions and event fan-out.
func (w *Writeback) commitReaderOpt(ctx context.Context, path string, newReader func() io.Reader, size int64, emptyCreate bool) (int64, error) {
	rp := w.remotePath(path)
	// syncDataCommitContext + releaseTimeout: commits detach from the
	// FUSE cancel channel (a kernel interrupt must not fail a server-side
	// write that already applied) and get a size-proportional budget.
	if !w.opts.InterruptibleCommits {
		detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout(size))
		defer cancel()
		ctx = detached
	}
	w.commitMu.Lock()
	defer w.commitMu.Unlock()
	if emptyCreate {
		// v1 fallback order: create endpoint first; any failure (conflict
		// against a raced create included) falls back to the conditional
		// write below.
		if rev, err := w.opts.Client.CreateFileCtx(ctx, rp); err == nil {
			return rev, nil
		}
	}
	// Upload through the size-aware client path (commitWhole): inline
	// PUTs below the threshold, multipart above it — the server rejects
	// large plain PUTs with "missing X-Dat9-Part-Checksums".
	for attempt := 0; attempt <= commitRetries; attempt++ {
		expected := int64(0)
		if st, err := w.opts.Client.StatCtx(ctx, rp); err == nil {
			expected = st.Revision
		} else if !client.IsNotFound(err) {
			return 0, mapErr(err)
		}
		st, werr := w.opts.Client.WriteStreamConditionalWithChecksum(ctx,
			rp, newReader(), size, nil, expected, "")
		if werr != nil {
			if errors.Is(werr, client.ErrConflict) {
				continue
			}
			return 0, mapErr(werr)
		}
		rev := int64(0)
		if st != nil {
			rev = st.Revision
		}
		return rev, nil
	}
	return 0, &fs.StatusError{Path: path, Status: gofuse.EAGAIN}
}

// truncateLocal rebases a live writer onto a new size: the buffer loads the
// current base (so a growing truncate zero-fills and a shrinking one drops
// the tail), then truncates locally; the commit boundary publishes it (// Setattr-on-open-handle semantics).
func (h *bufferingHandle) truncateLocal(ctx context.Context, size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ensureBufferLocked(ctx); err != nil {
		return err
	}
	// v1: a truncate invalidates everything the streamer already uploaded —
	// reset the streamed-part bookkeeping and abandon the active stream so
	// finishStreaming cannot publish pre-truncation bytes past the new size.
	h.stream = nil
	h.buf.ResetStreamingState()
	if err := h.buf.Truncate(size); err != nil {
		return err
	}
	h.dirty = true
	h.w.armBypass(h.path, h.baseRev, size, "setattr-size")
	return nil
}

func (h *bufferingHandle) ensureBuffer(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ensureBufferLocked(ctx)
}

func (h *bufferingHandle) ensureBufferLocked(ctx context.Context) error {
	if h.buf != nil {
		return nil
	}
	h.buf = writebuffer.NewWriteBuffer(h.path, bufferedMaxTotal, 0)
	if h.unlinked && h.w.opts.Staging != nil && h.w.unlinkedShadowServed(h.path) {
		// v1 survivor: an anonymous fd's base is its unlink snapshot, not
		// the deleted remote — post-unlink writes keep building on the
		// pre-delete image (the commit boundary discards them with the
		// name).
		if data, rerr := h.w.opts.Staging.Shadow.ReadAll(h.path); rerr == nil && data != nil {
			if _, err := h.buf.Write(0, data); err != nil {
				return err
			}
			h.buf.ClearDirty()
			return nil
		}
	}
	if !h.isNew && h.w.wbCache != nil {
		if data, ok := h.w.wbCache.Get(h.w.remotePath(h.path)); ok && int64(len(data)) > 0 {
			// The staged image is newer than the remote (it has not
			// landed yet): preload from the cache so read-your-writes and
			// the next commit see the pending bytes (OpenWritable
			// preload).
			if _, err := h.buf.Write(0, data); err != nil {
				return err
			}
			h.buf.ClearDirty()
			return nil
		}
	}
	if h.trunc {
		// O_TRUNC: empty base, marked dirty so a write-less close still
		// commits the truncation (ZeroBase + commit at boundary).
		h.dirty = true
		return nil
	}
	if (h.isNew || h.inner == nil) && h.w.opts.Staging != nil {
		// A pending image owns this path (queue track): preload the
		// staged bytes so the handle reads its own image. Size-gated —
		// a spilled multi-GiB image stays a view over the shadow.
		if meta, ok := h.w.opts.Staging.Pending.GetMeta(h.path); ok && meta != nil &&
			meta.Size > 0 && meta.Size <= pendingServeMaterializeBytes {
			if data, err := h.w.opts.Staging.Shadow.ReadAll(h.path); err == nil && int64(len(data)) == meta.Size {
				if _, werr := h.buf.Write(0, data); werr == nil {
					h.buf.ClearDirty()
					return nil
				}
			}
		}
	}
	if h.isNew || h.inner == nil || h.baseSize <= 0 {
		return nil
	}
	if h.baseSize <= h.w.preload {
		// Preload the base cache-first (read its preload through the
		// read cache): a just-committed image answers locally, so the
		// materialization cannot 404 inside backend replica lag.
		if h.w.baseReader != nil {
			data, ok := h.w.baseReader(h.path)
			if ok && int64(len(data)) >= h.baseSize {
				if _, err := h.buf.Write(0, data); err != nil {
					return err
				}
				h.buf.ClearDirty()
				return nil
			}
		}
		// preload the remote base so reads and commits see full images
		base := make([]byte, h.baseSize)
		if _, err := h.inner.Read(ctx, 0, base); err != nil {
			return err
		}
		if _, err := h.buf.Write(0, base); err != nil {
			return err
		}
		h.buf.ClearDirty()
		return nil
	}
	// Large bases stay lazy (preloadWritableHandle): parts load from the
	// remote on first touch, so memory stays O(touched parts) and untouched
	// regions are served from the remote image instead of zero-filled holes.
	partSize := h.buf.PartSize()
	remoteSize := h.baseSize
	inner := h.inner
	h.buf.LoadPart = func(partNum int) ([]byte, error) {
		start := int64(partNum-1) * partSize
		end := start + partSize
		if end > remoteSize {
			end = remoteSize
		}
		if end <= start {
			return nil, nil
		}
		part := make([]byte, end-start)
		if _, err := inner.Read(ctx, start, part); err != nil {
			return nil, err
		}
		return part, nil
	}
	// Seed the base extent: Size() answers the base size (reads and
	// O_APPEND tails see the full file) while parts stay unloaded.
	h.buf.SeedRemote(remoteSize)
	return nil
}

// stageAndEnqueue publishes the buffered image through the staging ledger
// and hands it to the async commit queue (the true writeback path). The
// close boundary stages durably unless lazy staging is on (v1).
func (h *bufferingHandle) stageAndEnqueue(ctx context.Context) error {
	return h.stageAndEnqueueOpt(ctx, !h.w.lazy, false)
}

// stageAndEnqueueOpt is stageAndEnqueue with explicit durability controls:
// durable forces the shadow fsync even under lazy staging, and journalFsync
// appends a JournalFsync frame after staging so the async commit's
// JournalCommit marker lands at a higher Seq — replay then cannot
// resurrect a path the commit settled (v1 interactive fsync ordering).
func (h *bufferingHandle) stageAndEnqueueOpt(ctx context.Context, durable, journalFsync bool) error {
	h.mu.Lock()
	if !h.dirty || h.buf == nil {
		h.mu.Unlock()
		return nil
	}
	size := h.buf.Size()
	spill := size >= commitInlineStreamThreshold
	if !spill {
		// Materialize every remote-backed part first: Bytes zero-fills
		// unloaded parts, which would publish a corrupted image.
		if err := h.buf.LoadMissingParts(); err != nil {
			h.mu.Unlock()
			return err
		}
	}
	isNew, baseRev := h.isNew, h.baseRev
	// A zero-based image (fresh create, or O_TRUNC discarding the base)
	// holds only zeros beyond its written parts — its spill stays sparse.
	zeroBase := isNew || h.trunc
	// The deferred chmod (or a create mode ≠ 0644) rides the staging entry:
	// the queue snapshots Mode at enqueue and applies it post-landing
	// (onCommitSuccess). Consume it here — a later staging must not re-apply
	// a stale mode. Dropping it entirely (the old behavior) lost every chmod
	// deferred onto an open writeback handle (chmod/12: 0777 → 0644).
	mode, hasMode := h.pendingMode, h.hasPendingMode
	h.pendingMode, h.hasPendingMode = 0, false
	// Lineage (v1 ensureStagedSnapshotLineageLocked): a fresh snapshot ID
	// per staging, parented on this handle's current content snapshot.
	if h.stagedSnapshotID == "" || h.stagedSnapshotSeq != h.stageSeq {
		h.stagedSnapshotID = newSnapshotID()
		h.stagedParentID = h.contentSnapshotID
		h.stagedSnapshotSeq = h.stageSeq
	}
	h.stageSeq++
	snapshotID, parentSnapshotID := h.stagedSnapshotID, h.stagedParentID
	buf := h.buf
	h.mu.Unlock()

	w := h.w
	st := w.opts.Staging
	kind := staging.PendingOverwrite
	if isNew {
		kind = staging.PendingNew
		baseRev = 0
	}
	// staging.Staging keys are LOCAL paths; the queue maps them to API paths via
	// its own remoteRoot (same convention as the terminal).
	unlock := w.pathLocks.lock(h.path)
	defer unlock()
	var image []byte
	if spill {
		// v1 stageLargeAtClose + shadow-spill staging: bounded memory — each
		// part is written to the shadow and evicted from the in-memory
		// buffer as it completes; RestorePart re-reads evicted parts from
		// the shadow (v1 bindShadowSpillEvictionLocked /
		// restoreWriteBufferPartFromShadow), so the handle stays usable and
		// later sequential writes keep spilling through OnPartFull. The
		// enqueued entry carries ShadowSpill and the queue streams the
		// upload asynchronously: close(2) returns after the LOCAL durable
		// stage (the write-back policy's defining contract), not after the
		// network transfer.
		if err := h.spillToShadow(st, buf, baseRev, zeroBase); err != nil {
			return err
		}
		if durable {
			if err := st.Shadow.Sync(h.path); err != nil {
				return err
			}
		}
	} else {
		image = buf.Bytes()
		if err := st.Shadow.WriteFull(h.path, image, baseRev); err != nil {
			return err
		}
		if w.pendingSeeder != nil {
			w.pendingSeeder(h.path, image)
		}
		if durable {
			// v1 stageShadowLocked(durable): the staged content is fsynced
			// BEFORE the pending meta is published — Put's WAL record promises
			// the bytes are durable (journal replay rebuilds the pending entry
			// and the commit queue re-uploads from the shadow).
			if err := st.Shadow.Sync(h.path); err != nil {
				return err
			}
		}
	}
	shadowGen := st.Shadow.ActiveGeneration(h.path)
	var pendingGen uint64
	var err error
	if spill {
		pendingGen, err = st.Pending.PutShadowSpillWithModeAndLineage(h.path, size, kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, true)
	} else {
		pendingGen, err = st.Pending.PutWithBaseRevAndModeAndLineage(h.path, int64(len(image)), kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, true)
	}
	if err != nil {
		return err
	}
	if journalFsync && st.Journal != nil {
		_ = st.Journal.Append(staging.JournalEntry{
			Op:      staging.JournalFsync,
			Path:    h.path,
			Length:  size,
			BaseRev: baseRev,
		})
		_ = st.Journal.FsyncShared()
	}
	// The queue now owns this image: drop a same-image .dat/.meta snapshot
	// the Flush left, or Release's Submit double-publishes it as a second
	// track and one side always 409s (the create case: queue lands the
	// PendingNew, the uploader's expect-absent CAS then conflicts forever).
	if w.wbCache != nil {
		w.wbCache.Remove(h.path)
	}
	entry := &CommitEntry{
		Path:             h.path,
		Size:             size,
		Kind:             kind,
		BaseRev:          baseRev,
		Mode:             mode,
		HasMode:          hasMode,
		ShadowGen:        shadowGen,
		PendingIndexGen:  pendingGen,
		ShadowSpill:      spill,
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapshotID,
	}
	if w.opts.Events != nil {
		w.opts.Events.Emit(fs.Event{Kind: fs.EventStaged, Path: h.path, Size: entry.Size})
	}
	if err := w.queue.Enqueue(entry); err != nil {
		return err
	}
	h.mu.Lock()
	h.dirty = false
	h.buf.ClearDirty()
	h.landed = true
	// v1 publishStagedSnapshotLineageLocked: the staged snapshot becomes
	// this handle's content identity — the next staging parents on it.
	h.contentSnapshotID = snapshotID
	h.mu.Unlock()
	return nil
}

// spillToShadow stages the buffer's complete image into the shadow store
// part-by-part with bounded memory (v1 large-file close staging). Parts are
// loaded, written, and evicted; RestorePart and OnPartFull keep the
// shadow as the buffer's authoritative backing from here on.
func (h *bufferingHandle) spillToShadow(st *staging.Staging, buf *writebuffer.WriteBuffer, baseRev int64, zeroBase bool) error {
	partSize := buf.PartSize()
	if partSize <= 0 {
		return fmt.Errorf("writeback: spill without part size: %s", h.path)
	}
	size := buf.Size()
	// The shadow becomes the buffer's authoritative backing: evicted parts
	// restore from it, and future sequential writes spill through it.
	buf.RestorePart = func(partNum int) ([]byte, error) {
		return restoreShadowPart(st.Shadow, h.path, buf, partNum)
	}
	buf.OnPartFull = func(partIdx int, data []byte) {
		if _, err := st.Shadow.WriteAt(h.path, int64(partIdx)*partSize, data, baseRev); err != nil {
			safeLogPrintf("writeback: shadow spill of part %d for %s failed: %v", partIdx, h.path, err)
			return
		}
		buf.EvictPart(partIdx)
	}
	// v1's spill wrote only the parts the writer touched: a one-byte
	// write at 2GiB+1 never materialized 2GiB of zeros on the disk, and
	// the shadow byte quota rejected the port's full-image rewrite
	// (open/25). For a zero-based image, parts that are neither dirty
	// nor in memory are zero holes (or bytes a prior spill already put in
	// the shadow) — the sparse shadow plus the final Truncate serves
	// them.
	var dirty map[int]bool
	if zeroBase {
		dirty = make(map[int]bool)
		for _, pn := range buf.DirtyPartNumbers() {
			dirty[pn] = true
		}
	}
	for start := int64(0); start < size; start += partSize {
		partNum := int(start/partSize) + 1
		if zeroBase && !dirty[partNum] && !buf.IsPartLoaded(partNum-1) {
			continue
		}
		end := start + partSize
		if end > size {
			end = size
		}
		if err := buf.EnsurePartsForWrite(start, end-start); err != nil {
			return err
		}
		data := buf.PartData(partNum)
		if int64(len(data)) < end-start {
			return fmt.Errorf("writeback: spill short part %d of %s", partNum, h.path)
		}
		if _, err := st.Shadow.WriteAt(h.path, start, data[:end-start], baseRev); err != nil {
			return err
		}
		buf.EvictPart(int(start / partSize))
	}
	if err := st.Shadow.Truncate(h.path, size, baseRev); err != nil {
		return err
	}
	return nil
}

// restoreShadowPart re-reads one evicted buffer part from the shadow (v1
// restoreWriteBufferPartFromShadow). partNum is 1-based.
func restoreShadowPart(store *staging.ShadowStore, path string, wb *writebuffer.WriteBuffer, partNum int) ([]byte, error) {
	partSize := wb.PartSize()
	if store == nil || wb == nil || partNum < 1 || partSize <= 0 {
		return nil, fmt.Errorf("invalid shadow part restore")
	}
	start := int64(partNum-1) * partSize
	end := start + partSize
	if total := wb.Size(); total > 0 && end > total {
		end = total
	}
	if end <= start {
		return nil, fmt.Errorf("empty shadow part %d", partNum)
	}
	buf := make([]byte, end-start)
	n, err := store.ReadAt(path, start, buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if int64(n) != end-start {
		return nil, fmt.Errorf("shadow part %d short read n=%d want=%d", partNum, n, end-start)
	}
	return buf, nil
}

// commitLocked publishes the buffered image per policy: with a staging
// ledger and the async policy it stages and enqueues (returning before
// the remote commit); otherwise it uploads inline with compare-and-set
// and revision-conflict retries. An unlinked handle never commits — the
// anonymous fd's data dies at Release (semantics).
func (h *bufferingHandle) commitLocked(ctx context.Context) error {
	if h.w.rolledBack.Load() {
		return errLayerRolledBack
	}
	h.mu.Lock()
	unlinked := h.unlinked
	h.mu.Unlock()
	if unlinked {
		h.mu.Lock()
		h.dirty = false
		if h.buf != nil {
			h.buf.ClearDirty()
		}
		h.mu.Unlock()
		return nil
	}
	// A truncation-at-open materializes its empty dirty buffer lazily; the
	// write-less close still publishes size 0 (ZeroBase).
	h.mu.Lock()
	truncPending := h.trunc && h.buf == nil
	h.mu.Unlock()
	if truncPending {
		if err := h.ensureBuffer(ctx); err != nil {
			return err
		}
	}
	h.mu.Lock()
	if !h.dirty || h.buf == nil {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()
	if err := h.finishStreaming(ctx); err != nil {
		return err
	}
	if h.w.opts.Policy == PolicyWriteBack && h.w.queue != nil && h.w.opts.Staging != nil {
		// Large images stage through the shadow spill path (bounded memory)
		// and land asynchronously — v1 never materialized a large image for
		// the whole-image tracks (a sparse 2GiB pwrite OOM-killed small
		// hosts), and close(2) returned after the local durable stage.
		return h.stageAndEnqueue(ctx)
	}
	// Commit dispatch: a partial edit of an existing large file patches
	// only the dirty parts (the server copies the rest from the current
	// object); everything else falls through to the full-write path.
	if _, attempted, perr := h.commitPatched(ctx); attempted {
		if perr != nil {
			return perr
		}
		h.mu.Lock()
		h.dirty = false
		h.buf.ClearDirty()
		h.landed = true
		h.isNew = false
		size := h.buf.Size()
		h.mu.Unlock()
		// only sync-durability policies arm the post-commit bypass —
		// writeback's async commits never did, and arming them turns every
		// following clean reader DIRECT_IO.
		if h.w.opts.Policy == PolicyWriteSync || h.w.opts.Policy == PolicyCloseSync {
			h.w.armBypass(h.path, 0, size, "sync-commit")
		}
		// A patch reply carries no revision; caches invalidate without a
		// read-cache seed.
		h.w.invalidateRev(h.path, 0, size)
		return h.applyPendingMode(ctx)
	}
	return h.commitInline(ctx)
}

// commitPatched lands a partial edit of an existing large file as a
// part-level patch: only the dirty parts transfer, the server copies the
// rest from the current object. It reports attempted=false when the
// dispatch does not apply — a small file (direct PUT lands it inline), a
// fresh create or a growth past the original (no object to copy from), an
// active part stream (its own finish path), or an observed non-patchable
// storage class — leaving the caller the full-write path.
func (h *bufferingHandle) commitPatched(ctx context.Context) (rev int64, attempted bool, err error) {
	threshold := h.w.opts.Client.CachedSmallFileThreshold()
	h.mu.Lock()
	size := h.buf.Size()
	directPUT := size == 0 || (threshold > 0 && size < threshold)
	applicable := !directPUT && threshold > 0 && !h.isNew &&
		h.baseSize >= threshold && size <= h.baseSize &&
		h.storageClass != client.StorageTypeDB9 &&
		(h.stream == nil || !h.stream.Started())
	var dirty []int
	var snapshots map[int][]byte
	var partSize int64
	if applicable {
		dirty = h.buf.DirtyPartNumbers()
		if len(dirty) > 0 {
			applicable = true
			snapshots = make(map[int][]byte, len(dirty))
			for _, pn := range dirty {
				if src := h.buf.PartData(pn); src != nil {
					cp := make([]byte, len(src))
					copy(cp, src)
					snapshots[pn] = cp
				}
			}
			partSize = h.buf.PartSize()
		} else {
			applicable = false
		}
	}
	h.mu.Unlock()
	if !applicable {
		return 0, false, nil
	}
	rp := h.w.remotePath(h.path)
	expected := int64(0)
	if st, serr := h.w.opts.Client.StatCtx(ctx, rp); serr == nil {
		expected = st.Revision
		if st.StorageType != "" {
			h.mu.Lock()
			h.storageClass = st.StorageType
			h.mu.Unlock()
			if st.StorageType == client.StorageTypeDB9 {
				return 0, false, nil
			}
		}
	} else if client.IsNotFound(serr) {
		// The object vanished; the full-write path recreates it.
		return 0, false, nil
	} else {
		return 0, false, mapErr(serr)
	}
	perr := h.w.opts.Client.PatchFile(ctx, rp, size, dirty,
		func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
			if d, ok := snapshots[partNumber]; ok {
				return d, nil
			}
			return origData, nil
		},
		nil,
		client.WithPartSize(partSize),
		client.WithExpectedRevision(expected),
	)
	if perr != nil {
		if client.IsUnsupportedStorageTargetErr(perr) {
			// Observed: this object does not accept partial updates;
			// remember the class and land the edit as a full write.
			h.mu.Lock()
			h.storageClass = client.StorageTypeDB9
			h.mu.Unlock()
			return 0, false, nil
		}
		return 0, true, mapErr(perr)
	}
	// A patch keeps the object on part storage.
	h.mu.Lock()
	h.storageClass = client.StorageTypeS3
	h.mu.Unlock()
	return 0, true, nil
}

// maybeArmStreaming ports the streaming-upload arming: sequential writes
// on large existing files (beyond the preload cap, no local base) upload
// parts in parallel while the write stream continues. Parts flow through
// WriteBuffer.OnPartFull; FinishStreaming publishes at the commit boundary.
func (h *bufferingHandle) maybeArmStreaming() error {
	if h.streamArmed || h.isNew || h.w.opts.Policy == PolicyWriteSync || h.forceSync {
		return nil
	}
	h.streamArmed = true
	if h.baseSize <= h.w.preload {
		return nil // small files buffer whole
	}
	h.stream = NewStreamUploader(h.w.opts.Client, h.path, h.baseRev, h.w.root)
	h.buf.OnPartFull = func(partIdx int, data []byte) {
		if h.stream != nil {
			// onDone releases the buffered part once the parallel upload
			// finishes (memory model: O(in-flight parts)).
			part := partIdx
			onDone := func(done int) { h.buf.EvictPart(done) }
			ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
			defer cancel()
			_ = h.stream.SubmitPart(ctx, part+1, data, onDone)
		}
	}
	return nil
}

// finishStreaming publishes the streamed multipart image (// FinishStreaming at the commit boundary); non-streamed handles fall back
// to the whole-image path.
func (h *bufferingHandle) finishStreaming(ctx context.Context) error {
	h.mu.Lock()
	stream := h.stream
	h.stream = nil
	if h.buf != nil {
		h.buf.OnPartFull = nil
	}
	h.mu.Unlock()
	if stream == nil || !stream.Started() {
		return nil
	}
	h.mu.Lock()
	size := int64(0)
	var dirtyParts map[int][]byte
	if h.buf != nil {
		size = h.buf.Size()
		dirtyParts = h.buf.DirtyStreamedParts()
	}
	h.mu.Unlock()
	return stream.FinishStreaming(ctx, size, 0, nil, dirtyParts)
}

// stageToWriteCache writes the buffered image into the .dat/.meta cache
// (small-stage) and clears the in-memory buffer's dirty state.
func (h *bufferingHandle) stageToWriteCache(ctx context.Context, isNew bool) error {
	h.mu.Lock()
	if h.buf == nil {
		h.mu.Unlock()
		if !isNew {
			return nil
		}
		// An O_CREAT claim that never wrote stages as a zero-byte image
		// (v1 createEmptyHandleRemote): its existence must land at the
		// durability boundary, not only at Release.
		if err := h.ensureBuffer(ctx); err != nil {
			return err
		}
		h.mu.Lock()
		if h.buf == nil {
			h.mu.Unlock()
			return nil
		}
	}
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	image := h.buf.Bytes()
	baseRev := h.baseRev
	mode, hasMode := h.pendingMode, h.hasPendingMode
	h.mu.Unlock()
	if isNew {
		baseRev = 0
	} else {
		// The CAS base must be the file's real current revision. The
		// open-time stat can be stale either way — a row whose revision a
		// streaming landing never reported (committedRev=0), or a row left
		// at the pre-overwrite revision by the previous handle's async
		// upload. Refresh from the source of truth before staging.
		if st, serr := h.w.opts.Client.StatCtx(ctx, h.w.remotePath(h.path)); serr == nil && st != nil {
			h.mu.Lock()
			h.baseRev = st.Revision
			h.mu.Unlock()
			baseRev = st.Revision
		} else if baseRev <= 0 {
			// Unreachable remotely (stat failed): keep whatever lineage we
			// have rather than staging a base-less overwrite.
			_ = serr
		}
	}
	kind := staging.PendingOverwrite
	if isNew {
		kind = staging.PendingNew
	}
	// The write cache is LOCAL-path keyed; the uploader maps to the
	// remote name itself (a remote key here made every subtree mount's
	// upload silently no-op — the git-ops clone's config.lock never
	// landed).
	if err := h.w.wbCache.PutWithBaseRevAndMode(h.path, image, int64(len(image)), kind, baseRev, mode, hasMode); err != nil {
		return err
	}
	// v1 seeded the read cache with the staged image at the flush
	// boundary: readers during the async landing serve these bytes instead
	// of racing the staging's publication against the stale remote.
	if h.w.pendingSeeder != nil {
		h.w.pendingSeeder(h.path, image)
	}
	h.mu.Lock()
	h.dirty = false
	h.buf.ClearDirty()
	h.mu.Unlock()
	return nil
}

// applyPendingMode publishes a deferred chmod once the image it describes
// has landed (flush-time mode application). The server may briefly 404 a
// path chmod right after its create upload — retries (postUploadMode).
func (h *bufferingHandle) applyPendingMode(ctx context.Context) error {
	h.mu.Lock()
	mode, has := h.pendingMode, h.hasPendingMode
	h.pendingMode, h.hasPendingMode = 0, false
	h.mu.Unlock()
	if !has || mode == 0 {
		return nil
	}
	err := retryPostUploadMode(ctx, func() error {
		// v1 remoteChmodMode: the wire chmod carries permission bits
		// only; the full 0o7777 stays in the local view.
		return h.w.opts.Client.ChmodCtx(ctx, h.w.remotePath(h.path), mode&0o777)
	})
	if err != nil && client.IsNotFound(err) {
		// the server can 404 a path
		// chmod while the node is visible by stat; keep the mode local.
		if _, serr := h.w.opts.Client.StatCtx(ctx, h.w.remotePath(h.path)); serr == nil {
			return nil
		}
	}
	return err
}

func (l *stripedLocks) lock(path string) func() {
	l.mu.Lock()
	if l.m == nil {
		l.m = map[string]*sync.Mutex{}
	}
	m := l.m[path]
	if m == nil {
		m = &sync.Mutex{}
		l.m[path] = m
	}
	l.mu.Unlock()
	m.Lock()
	return m.Unlock
}
