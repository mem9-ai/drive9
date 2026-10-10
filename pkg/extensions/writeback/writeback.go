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
	// commitSeeder caches the just-committed image (readCache.Put at
	// commit settle) so reads racing backend replica lag see this mount's
	// own writes; nil disables seeding.
	commitSeeder func(path string, data []byte, rev int64)
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
	w := &Writeback{opts: opts, root: root, preload: preload, interactive: opts.SyncMode == "interactive"}
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
	}
	if opts.Staging != nil {
		workers := opts.UploadConcurrency
		if workers <= 0 {
			workers = defaultUploadConcurrency
		}
		q := NewCommitQueue(opts.Client, opts.Staging.Shadow, opts.Staging.Pending, opts.Staging.Journal, workers, opts.CommitQueueMaxPending, root)
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
			if opts.Events != nil {
				// a local async commit completion is not an external
				// change — no kernel notify, no listing invalidation.
				opts.Events.Emit(fs.Event{Kind: fs.EventLanded, Path: entry.Path, Rev: committedRev, Size: entry.Size})
			}
		}
		q.PathLock = w.pathLocks.lock
		w.queue = q
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

// SetBaseReader wires the read-cache base-preload hook; nil keeps the
// direct remote preload.
func (w *Writeback) SetBaseReader(fn func(path string) ([]byte, bool)) {
	w.baseReader = fn
}

// Lifecycle: the facade chain starts/drains/stops the queue with the mount.
func (w *Writeback) Start(ctx context.Context) error { return nil }

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
}

// SetSurvivorLookup wires the facade's hardlink-alias probe.
func (w *Writeback) SetSurvivorLookup(fn func(path string) (string, bool)) { w.survivorOf = fn }

// Queue exposes the commit queue for the mount's queue-stats probe.
func (w *Writeback) Queue() *CommitQueue { return w.queue }

// SetBypassArmer wires the facade-owned bypass table (Context.Bypass).
func (w *Writeback) SetBypassArmer(a fs.CacheBypassArmer) { w.bypass = a }

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
		return next(ctx, op)
	case fs.OpCreate:
		flags := op.Flags
		if flags&uint32(syscall.O_CREAT) == 0 {
			// pending-cache visibility (plain opens too): a path flushed
			// into the async queue but not yet landed waits for its own
			// image before the chain below stats it.
			if w.queue != nil && w.queue.HasPath(op.Path) {
				if _, serr := w.opts.Client.StatCtx(ctx, w.remotePath(op.Path)); serr != nil && client.IsNotFound(serr) {
					w.waitPathCommit(op.Path)
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
			if trunc && res.Attr != nil {
				res.Attr.Size = 0
			}
			if trunc {
				w.armBypass(op.Path, attrRev(res), 0, "open-truncate")
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
			if _, ok := w.wbCache.GetMeta(w.remotePath(op.Path)); ok {
				if err := w.uploader.UploadSync(ctx, w.remotePath(op.Path)); err == nil {
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
		// path (prefix-wide for directory removal).
		if w.wbCache != nil {
			if under {
				for _, entry := range w.wbCache.ListByPrefix(prefix) {
					w.wbCache.Remove(entry.Path)
				}
			}
			w.wbCache.Remove(w.remotePath(op.Path))
		}
		w.markUnlinked(op.Path, under)
		rollback := func() { w.markLinked(op.Path, under) }
		_, statErr := w.opts.Client.StatCtx(ctx, w.remotePath(op.Path))
		if statErr != nil && !client.IsNotFound(statErr) {
			rollback()
			return fs.Result{}, mapErr(statErr)
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
		if w.queue != nil {
			w.queue.CancelPath(op.Path)
			if under {
				w.queue.CancelPrefix(prefix)
			}
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
			if _, ok := w.wbCache.GetMeta(w.remotePath(op.Path)); ok {
				_ = w.uploader.UploadSync(ctx, w.remotePath(op.Path))
			}
		}
		return next(ctx, op)

	case fs.OpRename:
		// a staged .dat/.meta image for the old
		// path uploads there first, then the remote rename moves it.
		if w.wbCache != nil && w.uploader != nil {
			if _, ok := w.wbCache.GetMeta(w.remotePath(op.Path)); ok {
				_ = w.uploader.UploadSync(ctx, w.remotePath(op.Path))
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
				println("DBG wb-rename statErr path=" + op.Path + " err=" + statErr.Error())
				return fs.Result{}, mapErr(statErr)
			}
			if w.handleFor(op.Path) == nil {
				println("DBG wb-rename NotFound-nolive path=" + op.Path)
				return fs.Result{}, fs.ErrNotFound
			}
			w.retarget(op.Path, op.Path2)
			return fs.Result{}, nil
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
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
		if op.Mode != 0 {
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
				if meta, ok := w.wbCache.GetMeta(w.remotePath(op.Path)); ok {
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
				h.mu.Unlock()
				if !unlinked {
					if err := h.truncateLocal(ctx, op.Size); err == nil {
						// Publish the new size (UpdateSize +
						// cacheFileForPath at setattr-on-handle): the FUSE
						// SETATTR reply reflects it, and the kernel drops
						// its stale tail pages instead of stitching them
						// into the next writeback.
						w.invalidate(op.Path, op.Size)
						return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Size: op.Size}}, nil
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
				if _, ok := w.wbCache.GetMeta(w.remotePath(op.Path)); ok {
					if err := w.uploader.UploadSync(ctx, w.remotePath(op.Path)); err != nil {
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
	defer h.mu.Unlock()
	if h.buf == nil {
		if h.isNew {
			return 0, nil // nothing written yet
		}
		// OpenWritable preload: the first touch materializes the staged
		// .dat/.meta image (newer than the remote) instead of serving the
		// remote's stale bytes.
		if h.w.wbCache != nil {
			if _, ok := h.w.wbCache.GetMeta(h.w.remotePath(h.path)); ok {
				if err := h.ensureBufferLocked(ctx); err == nil && h.buf != nil {
					n := h.buf.ReadAt(off, dest)
					if end := h.buf.Size() - off; end < int64(len(dest)) {
						if end < 0 {
							end = 0
						}
						n = int(end)
					}
					return n, nil
				}
			}
		}
		return h.inner.Read(ctx, off, dest)
	}
	// Load every part the read touches before serving: ReadAt zero-fills
	// unloaded parts, which is only correct for regions beyond the remote
	// base (EnsureLoaded-before-read).
	if err := h.buf.EnsurePartsForWrite(off, int64(len(dest))); err != nil {
		return 0, err
	}
	n := h.buf.ReadAt(off, dest)
	if end := h.buf.Size() - off; end < int64(len(dest)) {
		if end < 0 {
			end = 0
		}
		n = int(end)
	}
	return n, nil
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

// Sync implements fs.Handle.
func (h *bufferingHandle) Sync(ctx context.Context) error {
	// interactive mode: fsync is a durability barrier served by
	// the local staging ledger — stage + async enqueue, and the queue lands the
	// remote commit; no full-queue wait (WAN mounts would otherwise block
	// every fsync behind unrelated uploads). Without a ledger nothing is
	// locally durable, so strict applies.
	if h.w.interactive && h.w.opts.Policy == PolicyWriteBack && h.w.queue != nil && h.w.opts.Staging != nil {
		return h.stageAndEnqueue(ctx)
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
	return nil
}

// Flush implements fs.Handle.
func (h *bufferingHandle) Flush(ctx context.Context) error {
	// Inline-path writeback coalesces rapid flushes of one path into one
	// upload (flushHandleDebounced): without a staging ledger every
	// Flush would otherwise PUT synchronously. Release cancels the pending
	// callback and commits synchronously, so close(2) semantics hold.
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
			h.w.uploader.Submit(h.path)
			return nil
		}
		// v1 writeBackThreshold: large images skip the whole-image
		// .dat/.meta track, and the staging ledger has no live spill
		// path — the close commits synchronously by streaming. After
		// close(2) the data must exist somewhere beyond the handle's
		// memory (open/25's re-open read EIO'd against a remote that
		// never saw the write).
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
		dirty := h.dirty
		h.mu.Unlock()
		if !dirty {
			return nil // already durable per Write
		}
		// A truncation-at-open with no writes is the only dirty state the
		// write path leaves behind; publish it at the flush boundary.
	}
	return h.commitLocked(ctx)
}

// Release implements fs.Handle.
func (h *bufferingHandle) Release(ctx context.Context) error {
	h.w.untrack(h)
	if h.w.debounce != nil {
		h.w.debounce.Cancel(h.path)
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
	h.mu.Unlock()
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
	if w.opts.Staging != nil {
		if meta, ok := w.opts.Staging.Pending.GetMeta(path); ok {
			return attrFromPendingMeta(meta)
		}
	}
	if w.wbCache != nil {
		if meta, ok := w.wbCache.GetMeta(w.remotePath(path)); ok {
			return attrFromPendingMeta(meta)
		}
	}
	return fs.Attr{}, false
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
	if w.root == "/" {
		return p
	}
	if p == "/" {
		return w.root
	}
	return w.root + p
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
// it) anonymous: the buffer keeps serving the fd, but commits are discarded
// so Release cannot resurrect the path. Per-handle, so a re-created p stays
// committable by its new writer (fh.Unlinked).
func (w *Writeback) markUnlinked(p string, under bool) {
	prefix := strings.TrimSuffix(p, "/") + "/"
	w.liveSet.Range(func(key, _ any) bool {
		h := key.(*bufferingHandle)
		h.mu.Lock()
		if h.path == p || (under && strings.HasPrefix(h.path, prefix)) {
			h.unlinked = true
		}
		h.mu.Unlock()
		return true
	})
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
		if meta, ok := w.wbCache.GetMeta(w.remotePath(path)); ok {
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

func (w *Writeback) commit(ctx context.Context, path string, image []byte) (int64, error) {
	return w.commitReader(ctx, path, func() io.Reader { return bytes.NewReader(image) }, int64(len(image)))
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

	committedRev, err := h.w.commitReader(ctx, h.path, newReader, size)
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
		h.mu.Unlock()
		if h.w.opts.Policy == PolicyWriteSync || h.w.opts.Policy == PolicyCloseSync {
			h.w.armBypass(h.path, 0, size, "sync-commit")
		}
		h.w.invalidateRev(h.path, committedRev, size)
		err = h.applyPendingMode(ctx)
	}
	return err
}

func (w *Writeback) commitReader(ctx context.Context, path string, newReader func() io.Reader, size int64) (int64, error) {
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
// and hands it to the async commit queue (the true writeback path).
func (h *bufferingHandle) stageAndEnqueue(ctx context.Context) error {
	h.mu.Lock()
	if !h.dirty || h.buf == nil {
		h.mu.Unlock()
		return nil
	}
	// Materialize every remote-backed part first: Bytes zero-fills
	// unloaded parts, which would publish a corrupted image.
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	image := h.buf.Bytes()
	isNew, baseRev := h.isNew, h.baseRev
	// The deferred chmod (or a create mode ≠ 0644) rides the staging entry:
	// the queue snapshots Mode at enqueue and applies it post-landing
	// (onCommitSuccess). Consume it here — a later staging must not re-apply
	// a stale mode. Dropping it entirely (the old behavior) lost every chmod
	// deferred onto an open writeback handle (chmod/12: 0777 → 0644).
	mode, hasMode := h.pendingMode, h.hasPendingMode
	h.pendingMode, h.hasPendingMode = 0, false
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
	if err := st.Shadow.WriteFull(h.path, image, baseRev); err != nil {
		return err
	}
	shadowGen := st.Shadow.ActiveGeneration(h.path)
	pendingGen, err := st.Pending.PutWithBaseRevAndMode(h.path, int64(len(image)), kind, baseRev, mode, hasMode)
	if err != nil {
		return err
	}
	entry := &CommitEntry{
		Path:            h.path,
		Size:            int64(len(image)),
		Kind:            kind,
		BaseRev:         baseRev,
		Mode:            mode,
		HasMode:         hasMode,
		ShadowGen:       shadowGen,
		PendingIndexGen: pendingGen,
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
	h.mu.Unlock()
	return nil
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
		h.mu.Lock()
		size := int64(0)
		if h.buf != nil {
			size = h.buf.Size()
		}
		h.mu.Unlock()
		if size >= commitInlineStreamThreshold {
			// v1 writeBackThreshold: large images never materialize for the
			// whole-image tracks — stream the commit inline instead (a
			// sparse 2GiB pwrite OOM-killed small hosts).
			return h.commitInline(ctx)
		}
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
		!(h.stream != nil && h.stream.Started())
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
		return nil
	}
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	image := h.buf.Bytes()
	baseRev := h.baseRev
	mode, hasMode := h.pendingMode, h.hasPendingMode
	if isNew {
		baseRev = 0
	}
	h.mu.Unlock()
	kind := staging.PendingOverwrite
	if isNew {
		kind = staging.PendingNew
	}
	rp := h.w.remotePath(h.path)
	if err := h.w.wbCache.PutWithBaseRevAndMode(rp, image, int64(len(image)), kind, baseRev, mode, hasMode); err != nil {
		return err
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
		return h.w.opts.Client.ChmodCtx(ctx, h.w.remotePath(h.path), mode)
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
