package writeback

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/mountpath"
)

// Exponential backoff constants for upload retries.
const (
	uploadBackoffBase = 200 * time.Millisecond
	uploadBackoffMax  = 30 * time.Second
	uploadMaxRetries  = 3
)

var errWriteBackBaseRevisionRequired = errors.New("writeback base revision required")

// WriteBackSuccessFunc observes committed data or a completed PendingChmod entry.
type WriteBackSuccessFunc func(meta staging.WriteBackMeta, committedRev int64, gens StagingGens)

// StagingGens captures the generations of the writeBack, pendingIndex and
// shadowStore entries associated with a writeBack entry at the moment it is
// read for upload. This lets the OnSuccess callback perform generation-guarded
// cleanup so a stale upload does not remove a fresher same-path staging entry
// that was created by a concurrent write while the upload was in flight.
// TerminalFailureSeq snapshots the commit queue's terminal-failure stamp for
// the path: OnSuccess may only clear that exact failure, never one recorded
// after the upload started.
type StagingGens struct {
	WriteBackGen       uint64
	PendingIndexGen    uint64
	ShadowGen          staging.ContentGen
	TerminalFailureSeq uint64
}

// SnapshotStagingGensFunc snapshots the pendingIndex and shadowStore
// generations plus the commit queue's terminal-failure stamp for a path. If a
// store is absent or has no entry, the corresponding gen is 0 (which never
// matches, so the guarded remove is skipped). Optional — if nil, OnSuccess
// receives zero-value gens.
type SnapshotStagingGensFunc func(remotePath string) StagingGens

// WriteBackUploader consumes pending write-back cache entries and uploads
// them to the server in the background. It runs a fixed number of worker
// goroutines that drain a deduplicated FIFO queue whose on-disk footprint is
// bounded by the write-back cache quota.
//
// Per-path serialization: at most one upload per local namespace path is in-flight at
// any time. WaitPath allows Rename/Unlink/Fsync to block until the current
// upload for a path finishes.
type WriteBackUploader struct {
	client     *client.Client
	cache      *staging.WriteBackCache
	remoteRoot string
	queueMu     sync.Mutex
	queueCond   *sync.Cond
	queue       []string
	queueHead   int
	queuedSet   map[string]struct{}
	activePaths map[string]int
	wg          sync.WaitGroup
	stopOnce   sync.Once
	stopped    atomic.Bool
	active     atomic.Int64

	// Per-path in-flight tracking.
	inflightMu sync.Mutex
	inflight   map[string]*pathState

	batchMu       sync.Mutex
	batchWindow   time.Duration
	batchMaxItems int
	batchMaxBytes int64
	batchDisabled atomic.Bool
	batchSem      chan struct{}

	perf      *fusePerfCounters
	OnSuccess WriteBackSuccessFunc
	// SnapshotStagingGens optionally captures the pendingIndex/shadowStore
	// generations for a path so OnSuccess can do generation-guarded cleanup.
	SnapshotStagingGens SnapshotStagingGensFunc
	// ResolveConflict optionally repairs a conflicted entry in place:
	// given the staged meta, the payload that was read for upload, and the
	// base its upload just conflicted on, it returns a newer same-origin
	// base to rebase onto and retry. Wired by the writeback plane to its
	// landed records — the payload this mount itself uploaded — so a
	// duplicate-commit race (the kernel's asynchronous RELEASE racing a
	// handoff flush of one handle) heals without a remote stat, while a
	// foreign advance or a divergent image stays conflicted.
	ResolveConflict func(localPath string, meta *staging.WriteBackMeta, data []byte, conflictedBase int64) (int64, bool)
	// LedgerOwned optionally reports that the staging ledger owns a path's
	// current image (pending index entry). The synchronous cache landing
	// REFUSES such paths: the .dat/.meta pair beneath the ledger entry may
	// be a superseded image, and publishing it — including the
	// expect-absent create a PendingNew pair would issue against an absent
	// row — hands the backend bytes the queue's own recovery is about to
	// contradict. Callers fall back to the queue track.
	LedgerOwned func(localPath string) bool
	// RecordLanding observes every successful upload with EXACTLY the
	// bytes that were published (both the background worker and the sync
	// path), read under the same generation gate as the upload's meta
	// snapshot. Wired by the writeback plane to its landed records, whose
	// payload identity must describe the uploaded generation — capturing
	// it outside this gate can pair one generation's identity with
	// another's revision.
	RecordLanding func(localPath string, data []byte, committedRev int64, meta *staging.WriteBackMeta)
}

// resolveConflict consults ResolveConflict and rebases the staged entry's
// base when the proof holds; ok reports a retryable new base.
func (u *WriteBackUploader) resolveConflict(localPath string, meta *staging.WriteBackMeta, data []byte, conflictedBase int64) (int64, bool) {
	if u.ResolveConflict == nil {
		return 0, false
	}
	return u.ResolveConflict(localPath, meta, data, conflictedBase)
}

// recordLanding consults RecordLanding when wired and only for a revision
// the upload itself reported: a fabricated (expected+1) or absent revision
// must never become a same-origin proof.
func (u *WriteBackUploader) recordLanding(localPath string, data []byte, committedRev int64, meta *staging.WriteBackMeta) {
	if u.RecordLanding != nil && committedRev > 0 {
		u.RecordLanding(localPath, data, committedRev, meta)
	}
}

// WriteBackUploaderSnapshot is one uploader-depth observation.
type WriteBackUploaderSnapshot struct {
	Queued      int
	InFlight    int
	Cached      int
	CachedBytes int64
	FirstPath   string
}

// pathState tracks per-path in-flight upload state so that callers can wait
// for completion and concurrent uploads of the same path are serialized.
type pathState struct {
	done chan struct{} // closed when the current upload finishes
}

// NewWriteBackUploader creates and starts a background uploader with
// numWorkers goroutines. Typical value: 4.
func NewWriteBackUploader(c *client.Client, cache *staging.WriteBackCache, numWorkers int, remoteRoot ...string) *WriteBackUploader {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	root := "/"
	if len(remoteRoot) > 0 && remoteRoot[0] != "" {
		root = remoteRoot[0]
	}
	u := &WriteBackUploader{
		client:      c,
		cache:       cache,
		remoteRoot:  root,
		queuedSet:   make(map[string]struct{}),
		activePaths: make(map[string]int),
		inflight:    make(map[string]*pathState),
		batchSem:    make(chan struct{}, 4),
	}
	u.queueCond = sync.NewCond(&u.queueMu)
	for i := 0; i < numWorkers; i++ {
		u.wg.Add(1)
		go u.worker()
	}
	return u
}

// SetPerfCounters attaches the uploader counters shim.
func (u *WriteBackUploader) SetPerfCounters(perf *fusePerfCounters) {
	u.perf = perf
}

// ConfigureBatchWrite enables explicit writeback small-file batching parameters.
func (u *WriteBackUploader) ConfigureBatchWrite(window time.Duration, maxItems int, maxBytes int64) {
	if u == nil {
		return
	}
	if maxItems <= 0 {
		maxItems = defaultCommitBatchMaxItems
	}
	if maxItems > client.MaxBatchWriteItems {
		maxItems = client.MaxBatchWriteItems
	}
	if maxBytes <= 0 {
		maxBytes = defaultCommitBatchMaxBytes
	}
	if maxBytes > client.MaxBatchWriteBytes {
		maxBytes = client.MaxBatchWriteBytes
	}
	u.batchMu.Lock()
	defer u.batchMu.Unlock()
	if window <= 0 || maxItems <= 1 || maxBytes <= 0 {
		u.batchWindow = 0
		u.batchMaxItems = 0
		u.batchMaxBytes = 0
		return
	}
	u.batchWindow = window
	u.batchMaxItems = maxItems
	u.batchMaxBytes = maxBytes
}

// InFlight reports whether localPath currently has an upload in progress.
// The flag stays set through OnSuccess, so a pending image seen while it is
// true may already have a published revision that its deferred cleanup has
// not acknowledged yet.
func (u *WriteBackUploader) InFlight(localPath string) bool {
	if u == nil {
		return false
	}
	u.inflightMu.Lock()
	_, ok := u.inflight[localPath]
	u.inflightMu.Unlock()
	return ok
}

// QueuedOrActive reports whether localPath is currently queued for background
// upload, popped by an active worker, or holding the per-path upload gate.
func (u *WriteBackUploader) QueuedOrActive(localPath string) bool {
	if u == nil {
		return false
	}
	u.queueMu.Lock()
	_, queued := u.queuedSet[localPath]
	active := u.activePaths[localPath] > 0
	u.queueMu.Unlock()
	if queued || active {
		return true
	}
	return u.InFlight(localPath)
}

// PendingStats returns queued and in-flight upload counts for observability.
func (u *WriteBackUploader) PendingStats() (queued int, inFlight int) {
	if u == nil {
		return 0, 0
	}
	u.queueMu.Lock()
	queued = len(u.queue) - u.queueHead
	active := int(u.active.Load())
	u.queueMu.Unlock()

	u.inflightMu.Lock()
	inFlight = len(u.inflight)
	u.inflightMu.Unlock()
	if active > inFlight {
		inFlight = active
	}
	return queued, inFlight
}

// Snapshot captures queued and in-flight depths.
func (u *WriteBackUploader) Snapshot() WriteBackUploaderSnapshot {
	if u == nil {
		return WriteBackUploaderSnapshot{}
	}
	u.queueMu.Lock()
	queued := len(u.queue) - u.queueHead
	active := int(u.active.Load())
	u.queueMu.Unlock()

	snap := WriteBackUploaderSnapshot{Queued: queued}
	u.inflightMu.Lock()
	snap.InFlight = len(u.inflight)
	for path := range u.inflight {
		snap.FirstPath = path
		break
	}
	u.inflightMu.Unlock()
	if active > snap.InFlight {
		snap.InFlight = active
	}
	if u.cache != nil {
		var firstCachedPath string
		snap.Cached, snap.CachedBytes, firstCachedPath = u.cache.PendingSummary()
		if snap.FirstPath == "" {
			snap.FirstPath = firstCachedPath
		}
	}
	return snap
}

// WaitIdle blocks until the upload queue drains.
func (u *WriteBackUploader) WaitIdle(ctx context.Context) error {
	if u == nil {
		return nil
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		queued, inFlight := u.PendingStats()
		if queued == 0 && inFlight == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Submit enqueues a local namespace path for background upload without blocking
// foreground FUSE_RELEASE callers. Paths already queued (not yet popped by a
// worker) are deduplicated in place; if called after DrainAll has stopped the
// uploader, it falls back to synchronous upload so data is never dropped.
func (u *WriteBackUploader) Submit(localPath string) {
	if u.perf != nil {
		u.perf.uploaderSubmit.add(1)
	}
	u.queueMu.Lock()
	if u.stopped.Load() {
		u.queueMu.Unlock()
		safeLogPrintf("writeback uploader: already stopped, uploading synchronously for %s", localPath)
		if u.perf != nil {
			u.perf.uploaderSyncFallback.add(1)
		}
		u.uploadOne(localPath)
		return
	}
	if _, dup := u.queuedSet[localPath]; !dup {
		u.queuedSet[localPath] = struct{}{}
		u.queue = append(u.queue, localPath)
		u.queueCond.Signal()
	}
	u.queueMu.Unlock()
}

// WaitPath blocks until any in-flight upload for localPath completes.
// Returns immediately if no upload is in progress for this path.
// This must be called by Rename/Unlink before operating on the local path
// to prevent the background worker from re-creating a renamed/deleted file.
func (u *WriteBackUploader) WaitPath(localPath string) {
	u.inflightMu.Lock()
	ps, ok := u.inflight[localPath]
	u.inflightMu.Unlock()
	if ok {
		<-ps.done
	}
}

// DrainAll stops accepting queued uploads and waits for all queued and
// in-flight uploads to complete. Called during graceful shutdown.
func (u *WriteBackUploader) DrainAll() {
	start := time.Now()
	defer func() {
		if u.perf != nil {
			u.perf.uploaderDrainCount.add(1)
			u.perf.uploaderDrainTotal.add(uint64(time.Since(start)))
		}
	}()
	u.stopOnce.Do(func() {
		u.queueMu.Lock()
		u.stopped.Store(true)
		u.queueCond.Broadcast()
		u.queueMu.Unlock()
	})
	u.wg.Wait()
}

// RecoverPending scans the cache directory for leftover entries from a
// previous session and submits them for upload.
func (u *WriteBackUploader) RecoverPending() {
	u.RecoverPendingFilter(nil)
}

// RecoverPendingFilter is RecoverPending with a skip predicate: paths for
// which skip reports true belong to another track's recovery (the staging
// ledger's pending index) and are left alone. It enumerates metadata only
// — the payloads are loaded one at a time by the workers, so recovering a
// large backlog never materializes its total bytes.
func (u *WriteBackUploader) RecoverPendingFilter(skip func(localPath string) bool) {
	metas := u.cache.ListPendingMetas()
	skippedLegacyOverwrites := 0
	for i := range metas {
		path, meta := metas[i].Path, &metas[i]
		if skip != nil && skip(path) {
			continue
		}
		if meta.Kind == staging.PendingOverwrite && meta.BaseRev <= 0 {
			skippedLegacyOverwrites++
			safeLogPrintf("writeback: skipping legacy overwrite without base revision for %s", path)
			continue
		}
		safeLogPrintf("writeback: recovering pending upload for %s (%d bytes)", path, meta.Size)
		u.Submit(path)
	}
	if skippedLegacyOverwrites > 0 {
		safeLogPrintf("writeback: skipped %d legacy overwrite entries without base revision during recovery", skippedLegacyOverwrites)
	}
}

// errLedgerOwned reports a synchronous landing refused because the staging
// ledger owns the path's image (see WriteBackUploader.LedgerOwned).
var errLedgerOwned = errors.New("writeback: path owned by the staging ledger")

// IsLedgerOwnedError reports whether err is the ledger-ownership refusal.
func IsLedgerOwnedError(err error) bool { return errors.Is(err, errLedgerOwned) }

// IsLedgerOwnedError on the uploader forwards to the package-level check.
func (u *WriteBackUploader) IsLedgerOwnedError(err error) bool { return IsLedgerOwnedError(err) }

// UploadSync synchronously uploads a single path from the cache to the server.
// Serializes with other uploads of the same path, then uploads with
// generation protection. Used by Fsync/Rename which require the data to be
// persisted remotely before returning. Ledger-owned paths are refused.
func (u *WriteBackUploader) UploadSync(ctx context.Context, localPath string) error {
	_, err := u.UploadSyncWithRevision(ctx, localPath)
	return err
}

// ledgerOwnedRefused reports whether this path's sync landing must be
// refused (the ledger owns its image).
func (u *WriteBackUploader) ledgerOwnedRefused(localPath string) bool {
	return u.LedgerOwned != nil && u.LedgerOwned(localPath)
}

// UploadSyncWithRevision uploads one path synchronously and reports
// the committed revision.
func (u *WriteBackUploader) UploadSyncWithRevision(ctx context.Context, localPath string) (int64, error) {
	if u.ledgerOwnedRefused(localPath) {
		return 0, errLedgerOwned
	}
	rev, _, err := u.uploadSyncWithMetadata(ctx, localPath)
	return rev, err
}

// Metadata identifies the completed cache operation; nil means no entry was consumed.
func (u *WriteBackUploader) uploadSyncWithMetadata(ctx context.Context, localPath string) (int64, *staging.WriteBackMeta, error) {
	if u.ledgerOwnedRefused(localPath) {
		return 0, nil, errLedgerOwned
	}
	// Own the gate through cache cleanup, just like uploadOne. A wait alone
	// leaves a gap in which a background or second synchronous upload can
	// publish the same generation and trigger a false revision conflict.
	release := u.acquirePath(localPath)
	defer release()

	// staging.PendingChmod entries have no .dat file (data already uploaded, only
	// chmod remains). Read meta first so we can handle chmod without loading
	// data — GetMetaAndView would prune the entry when the .dat is missing.
	chmodMeta, chmodOK := u.cache.GetMeta(localPath)
	if !chmodOK {
		return 0, nil, nil // not in cache (may have been uploaded by the background worker we just waited for)
	}
	if chmodMeta.Kind == staging.PendingChmod {
		if err := u.applyPendingChmod(ctx, localPath, chmodMeta, chmodMeta.Generation, true); err != nil {
			return 0, nil, err
		}
		return 0, chmodMeta, nil
	}

	// Atomically read meta + data under one path lock so they are guaranteed
	// to be from the same generation.
	meta, data, ok := u.cache.GetMetaAndView(localPath)
	if !ok {
		return 0, nil, nil // not in cache (removed between GetMeta and GetMetaAndView)
	}
	gen := meta.Generation

	expectedRevision, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		if errors.Is(err, errWriteBackBaseRevisionRequired) && meta.Kind == staging.PendingOverwrite {
			// Backward compatibility for pre-CAS writeback entries: preserve
			// the historical UploadSync behaviour so fsync/rename do not fail
			// with EIO on mounts that still have legacy pending overwrites.
			safeLogPrintf("writeback uploadsync: legacy overwrite without base revision for %s, falling back to unconditional write", localPath)
			expectedRevision = -1
		} else {
			return 0, nil, fmt.Errorf("resolve expected revision for %s: %w", localPath, err)
		}
	}

	var committedRev int64
	threshold := u.directPutThreshold()
	useDirectPUT := meta.Size == 0 || (threshold > 0 && meta.Size < threshold)
	put := func(ctx context.Context, rev int64) (int64, error) {
		if useDirectPUT {
			return u.client.WriteCtxConditionalWithRevision(ctx, u.remotePath(localPath), data, rev)
		}
		return uploadBufferedRemoteFileWithRevision(ctx, u.client, u.remotePath(localPath), data, rev)
	}
	uploadStart := time.Now()
	committedRev, err = put(ctx, expectedRevision)
	if errors.Is(err, client.ErrConflict) {
		// Same-origin repair (see uploadOne): this mount's own landing may
		// have won a duplicate-commit race under this staging — rebase and
		// retry once so the flush that routed here (close-sync's boundary,
		// the claim handoff inside an open) does not surface EIO for a
		// race the mount itself can prove.
		if rev, ok := u.resolveConflict(localPath, meta, data, expectedRevision); ok {
			expectedRevision = rev
			committedRev, err = put(ctx, expectedRevision)
		}
	}
	if u.perf != nil {
		u.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(uploadStart), uint64(len(data)))
	}
	if err != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		return 0, nil, err
	}
	// The landed record is written before the expected+1 fabrication below
	// applies: a landed record may only carry a revision the upload itself
	// reported (streaming landings report 0 and record nothing).
	u.recordLanding(localPath, data, committedRev, meta)
	if u.OnSuccess != nil && meta.LineageTrusted() && meta.SnapshotID != "" && meta.Size == int64(len(data)) && len(data) <= 1<<20 {
		meta.LiveChecksum = payloadChecksum(data)
	}
	committedRev = committedRevisionForExpectedRevision(expectedRevision, committedRev)
	chmodCtx, chmodCancel := context.WithTimeout(ctx, 30*time.Second)
	err = u.applyMode(chmodCtx, meta)
	chmodCancel()
	if err != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		if _, markErr := u.cache.MarkChmodPending(localPath, gen); markErr != nil {
			return 0, nil, fmt.Errorf("%w; mark chmod pending: %v", err, markErr)
		}
		if u.OnSuccess != nil {
			u.OnSuccess(*meta, committedRev, StagingGens{WriteBackGen: gen})
		}
		return 0, nil, err
	}

	// Atomically remove only if generation matches — a concurrent Put may
	// have written newer data between our read and this point.
	u.cache.RemoveIfGeneration(localPath, gen)
	if u.perf != nil {
		u.perf.uploaderSuccess.add(1)
	}
	if u.OnSuccess != nil {
		u.OnSuccess(*meta, committedRev, StagingGens{WriteBackGen: gen})
	}
	return committedRev, meta, nil
}

func expectedRevisionForWriteBack(meta *staging.WriteBackMeta) (int64, error) {
	if meta == nil {
		return -1, fmt.Errorf("missing writeback metadata")
	}
	if meta.Kind == staging.PendingChmod {
		return -1, fmt.Errorf("data already uploaded for %s; chmod pending", meta.Path)
	}
	if meta.Kind == staging.PendingNew {
		return 0, nil
	}
	if meta.BaseRev <= 0 {
		return -1, errWriteBackBaseRevisionRequired
	}
	return meta.BaseRev, nil
}

func (u *WriteBackUploader) remotePath(localPath string) string {
	root := "/"
	if u != nil && u.remoteRoot != "" {
		root = u.remoteRoot
	}
	return mountpath.ToRemote(root, localPath)
}

func (u *WriteBackUploader) applyMode(ctx context.Context, meta *staging.WriteBackMeta) error {
	if meta == nil || !shouldApplyRemoteMode(meta.Kind, meta.HasMode, meta.Mode) {
		return nil
	}
	mode := meta.Mode & posixPermissionModeMask
	remoteMode := remoteChmodMode(mode)
	err := retryPostUploadMode(ctx, func() error {
		start := time.Now()
		applyErr := u.client.ChmodCtx(ctx, u.remotePath(meta.Path), remoteMode)
		if u.perf != nil {
			u.perf.recordRemoteOp(perfRemoteMutation, applyErr, time.Since(start), 0)
		}
		return applyErr
	})
	if err != nil {
		// The same rule as the commit queue: the content is already uploaded, and a token
		// that may not chmod (an fs_scoped mount) can never succeed here, so retrying
		// would keep the mode pending for ever. Drop it with a warning instead.
		if client.IsForbidden(err) {
			safeLogPrintf("writeback upload: chmod denied for %s (token may not chmod); proceeding without mode update", meta.Path)
			return nil
		}
		return fmt.Errorf("writeback upload chmod %s to %o: %w", meta.Path, mode, err)
	}
	return nil
}

func (u *WriteBackUploader) directPutThreshold() int64 {
	if u != nil && u.client != nil {
		return u.client.CachedSmallFileThreshold()
	}
	return 0
}

func (u *WriteBackUploader) popOneLocked() (string, bool) {
	if u.queueHead >= len(u.queue) {
		return "", false
	}
	p := u.queue[u.queueHead]
	u.queue[u.queueHead] = ""
	u.queueHead++
	if u.queueHead == len(u.queue) {
		u.queue = u.queue[:0]
		u.queueHead = 0
	} else if u.queueHead >= 1024 && u.queueHead*2 >= len(u.queue) {
		rem := len(u.queue) - u.queueHead
		copy(u.queue[:rem], u.queue[u.queueHead:])
		clear(u.queue[rem:])
		u.queue = u.queue[:rem]
		u.queueHead = 0
	}
	delete(u.queuedSet, p)
	u.activePaths[p]++
	return p, true
}

func (u *WriteBackUploader) finishActivePaths(paths ...string) {
	if len(paths) == 0 {
		return
	}
	u.queueMu.Lock()
	for _, p := range paths {
		if n := u.activePaths[p] - 1; n > 0 {
			u.activePaths[p] = n
		} else {
			delete(u.activePaths, p)
		}
	}
	u.queueMu.Unlock()
}

func (u *WriteBackUploader) popNext() (string, bool) {
	u.queueMu.Lock()
	for u.queueHead >= len(u.queue) && !u.stopped.Load() {
		u.queueCond.Wait()
	}
	p, ok := u.popOneLocked()
	if ok {
		u.active.Add(1)
	}
	u.queueMu.Unlock()
	return p, ok
}

func (u *WriteBackUploader) popBatchExtras(limit int, window time.Duration) []string {
	if limit <= 0 {
		return nil
	}
	extras := make([]string, 0, limit)
	u.queueMu.Lock()
	for len(extras) < limit {
		p, ok := u.popOneLocked()
		if !ok {
			break
		}
		extras = append(extras, p)
	}
	if len(extras) < limit && window > 0 && !u.stopped.Load() {
		var expired bool
		timer := time.AfterFunc(window, func() {
			u.queueMu.Lock()
			expired = true
			u.queueCond.Broadcast()
			u.queueMu.Unlock()
		})
		for len(extras) < limit && !expired && !u.stopped.Load() {
			if p, ok := u.popOneLocked(); ok {
				extras = append(extras, p)
				continue
			}
			u.queueCond.Wait()
		}
		timer.Stop()
	}
	u.queueMu.Unlock()
	return extras
}

func (u *WriteBackUploader) worker() {
	defer u.wg.Done()
	for {
		localPath, ok := u.popNext()
		if !ok {
			return
		}
		func() {
			defer u.active.Add(-1)
			defer u.finishActivePaths(localPath)
			if u.tryUploadBatch(localPath) {
				return
			}
			u.uploadOne(localPath)
		}()
	}
}

type uploaderBatchCandidate struct {
	path        string
	meta        *staging.WriteBackMeta
	data        []byte
	gen         uint64
	expectedRev int64
	stagingGens StagingGens
	release     func()
}

func (u *WriteBackUploader) batchParams() (enabled bool, window time.Duration, maxItems int, maxBytes int64) {
	if u == nil || u.client == nil || u.cache == nil || u.batchDisabled.Load() {
		return false, 0, 0, 0
	}
	u.batchMu.Lock()
	win, items, bytesLim := u.batchWindow, u.batchMaxItems, u.batchMaxBytes
	u.batchMu.Unlock()
	if win > 0 && items > 1 && bytesLim > 0 {
		return true, win, items, bytesLim
	}
	return false, 0, 0, 0
}

func (u *WriteBackUploader) tryUploadBatch(firstPath string) bool {
	enabled, window, maxItems, maxBytes := u.batchParams()
	if !enabled {
		return false
	}

	counts := map[string]int{firstPath: 1}
	distinct := make([]string, 0, maxItems)
	distinct = append(distinct, firstPath)
	extras := u.popBatchExtras(maxItems-1, window)
	defer u.finishActivePaths(extras...)
	for _, p := range extras {
		if counts[p] == 0 {
			distinct = append(distinct, p)
		}
		counts[p]++
	}

	if len(distinct) < 2 {
		return false
	}

	// Lock distinct paths in sorted order so concurrent batch workers can
	// never deadlock on overlapping path gates.
	sort.Strings(distinct)
	releases := make([]func(), len(distinct))
	for i, p := range distinct {
		releases[i] = u.acquirePath(p)
	}

	threshold := u.directPutThreshold()
	cands := make([]uploaderBatchCandidate, 0, len(distinct))
	fallback := make([]string, 0)
	var totalBytes int64

	for i, localPath := range distinct {
		rel := releases[i]
		chmodMeta, ok := u.cache.GetMeta(localPath)
		if !ok {
			rel()
			continue
		}
		if chmodMeta.Kind == staging.PendingChmod {
			rel()
			fallback = append(fallback, localPath)
			continue
		}
		var stagingGens StagingGens
		meta, data, ok := u.cache.GetMetaAndViewWithCallback(localPath, func() {
			if u.SnapshotStagingGens != nil {
				stagingGens = u.SnapshotStagingGens(localPath)
			}
		})
		if !ok {
			rel()
			continue
		}
		stagingGens.WriteBackGen = meta.Generation
		expectedRev, err := expectedRevisionForWriteBack(meta)
		size := int64(len(data))
		inlineOK := size == 0 || (threshold > 0 && size < threshold) || (threshold == 0 && size <= 48<<10)
		if err != nil || !inlineOK || (meta.Kind != staging.PendingNew && meta.Kind != staging.PendingOverwrite) || totalBytes+size > maxBytes {
			rel()
			fallback = append(fallback, localPath)
			continue
		}
		totalBytes += size
		cands = append(cands, uploaderBatchCandidate{
			path:        localPath,
			meta:        meta,
			data:        data,
			gen:         meta.Generation,
			expectedRev: expectedRev,
			stagingGens: stagingGens,
			release:     rel,
		})
	}

	runFollowups := func(paths []string) {
		for _, p := range paths {
			u.uploadOne(p)
		}
		for _, p := range distinct {
			if counts[p] > 1 {
				u.uploadOne(p)
			}
		}
	}

	if len(cands) < 2 {
		for i := len(cands) - 1; i >= 0; i-- {
			cands[i].release()
		}
		for _, c := range cands {
			u.uploadOne(c.path)
		}
		runFollowups(fallback)
		return true
	}

	modeSupported := u.client.CachedBatchWriteModeSupported()
	remoteItems := make([]client.BatchWriteItem, len(cands))
	for i, c := range cands {
		item := client.BatchWriteItem{
			Path:             u.remotePath(c.path),
			ExpectedRevision: c.expectedRev,
			Data:             c.data,
		}
		if modeSupported && shouldApplyRemoteMode(c.meta.Kind, c.meta.HasMode, c.meta.Mode) {
			item.Mode = remoteChmodMode(c.meta.Mode & posixPermissionModeMask)
			item.HasMode = true
		}
		remoteItems[i] = item
	}

	if u.batchSem != nil {
		u.batchSem <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*uploadTimeout)
	uploadStart := time.Now()
	results, err := u.client.BatchWriteCtx(ctx, remoteItems)
	cancel()
	if u.batchSem != nil {
		<-u.batchSem
	}
	if u.perf != nil {
		u.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(uploadStart), uint64(totalBytes))
	}

	if err != nil || len(results) != len(cands) {
		if err != nil && isBatchWriteUnsupported(err) {
			u.batchDisabled.Store(true)
		}
		for i := len(cands) - 1; i >= 0; i-- {
			cands[i].release()
		}
		for _, c := range cands {
			u.uploadOne(c.path)
		}
		runFollowups(fallback)
		return true
	}

	retryFailed := make([]string, 0)
	for i, res := range results {
		c := cands[i]
		if !res.OK() {
			c.release()
			retryFailed = append(retryFailed, c.path)
			continue
		}
		committedRev := res.Revision
		u.recordLanding(c.path, c.data, committedRev, c.meta)
		if u.OnSuccess != nil && c.meta.LineageTrusted() && c.meta.SnapshotID != "" && c.meta.Size == int64(len(c.data)) && len(c.data) <= 1<<20 {
			c.meta.LiveChecksum = payloadChecksum(c.data)
		}
		committedRev = committedRevisionForExpectedRevision(c.expectedRev, committedRev)
		if !remoteItems[i].HasMode {
			chmodCtx, chmodCancel := context.WithTimeout(context.Background(), 30*time.Second)
			modeErr := u.applyMode(chmodCtx, c.meta)
			chmodCancel()
			if modeErr != nil {
				if u.perf != nil {
					u.perf.uploaderFailure.add(1)
				}
				if updated, merr := u.cache.MarkChmodPending(c.path, c.gen); merr != nil {
					safeLogPrintf("writeback upload chmod-pending meta update failed for %s: %v", c.path, merr)
					c.release()
					continue
				} else if updated {
					safeLogPrintf("writeback upload data committed for %s; chmod remains pending", c.path)
				}
				if u.OnSuccess != nil {
					u.OnSuccess(*c.meta, committedRev, c.stagingGens)
				}
				c.release()
				continue
			}
		}
		u.cache.RemoveIfGeneration(c.path, c.gen)
		if u.perf != nil {
			u.perf.uploaderSuccess.add(1)
		}
		if u.OnSuccess != nil {
			u.OnSuccess(*c.meta, committedRev, c.stagingGens)
		}
		c.release()
	}

	for _, p := range retryFailed {
		u.uploadOne(p)
	}
	runFollowups(fallback)
	return true
}

// acquirePath registers an in-flight upload for localPath. If another upload
// for the same path is already in progress, it waits for that to finish first,
// ensuring per-path serialization. Returns a release function that the caller
// must call when the upload is done.
func (u *WriteBackUploader) acquirePath(localPath string) func() {
	for {
		u.inflightMu.Lock()
		ps, ok := u.inflight[localPath]
		if !ok {
			// No in-flight upload — register ours.
			ps = &pathState{done: make(chan struct{})}
			u.inflight[localPath] = ps
			u.inflightMu.Unlock()
			return func() {
				u.inflightMu.Lock()
				// Only delete if it's still our state (not replaced by a new upload).
				if cur, ok := u.inflight[localPath]; ok && cur == ps {
					delete(u.inflight, localPath)
				}
				u.inflightMu.Unlock()
				close(ps.done)
			}
		}
		u.inflightMu.Unlock()
		// Wait for existing upload to finish, then retry.
		<-ps.done
	}
}

func (u *WriteBackUploader) uploadOne(localPath string) {
	release := u.acquirePath(localPath)
	defer release()

	// staging.PendingChmod entries have no .dat file (data already uploaded, only
	// chmod remains). Read meta first so we can handle chmod without loading
	// data — GetMetaAndView would prune the entry when the .dat is missing.
	chmodMeta, chmodOK := u.cache.GetMeta(localPath)
	if !chmodOK {
		return // Already uploaded or removed.
	}
	if chmodMeta.Kind == staging.PendingChmod {
		_ = u.applyPendingChmod(context.Background(), localPath, chmodMeta, chmodMeta.Generation, false)
		return
	}

	// Atomically read meta + data under one path lock so they are guaranteed
	// to be from the same generation. Without this, a concurrent Put could
	// replace the data between GetMeta and getView, causing the uploader to
	// upload new data with old baseRev/generation. The onLocked callback
	// captures the staging-store generations under the same path-lock window so
	// the OnSuccess cleanup is generation-guarded consistently — a concurrent
	// same-path write cannot race the snapshot and leave a stale upload holding
	// newer generations it is not responsible for.
	var stagingGens StagingGens
	meta, data, ok := u.cache.GetMetaAndViewWithCallback(localPath, func() {
		if u.SnapshotStagingGens != nil {
			stagingGens = u.SnapshotStagingGens(localPath)
		}
	})
	if !ok {
		return // Already uploaded or removed.
	}
	gen := meta.Generation
	// Use the writeBack generation we just read (authoritative) rather than
	// a re-read from the snapshot callback, which could race with a concurrent
	// Put between GetMetaAndView and the callback.
	stagingGens.WriteBackGen = gen

	expectedRevision, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		// TODO: add a migration or cleanup path for legacy overwrite entries
		// without base revisions so they do not remain pending forever when
		// they only flow through Flush -> Release -> background uploadOne.
		safeLogPrintf("writeback upload skipped for %s: %v", localPath, err)
		return
	}

	// Retry with exponential backoff.
	var (
		lastErr      error
		committedRev int64
		repaired     bool
	)
	for attempt := 0; attempt <= uploadMaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(float64(uploadBackoffBase) * math.Pow(2, float64(attempt-1)))
			if delay > uploadBackoffMax {
				delay = uploadBackoffMax
			}
			time.Sleep(delay)
		}

		ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
		uploadStart := time.Now()
		committedRev, lastErr = uploadBufferedRemoteFileWithRevision(ctx, u.client, u.remotePath(localPath), data, expectedRevision)
		cancel()
		if u.perf != nil {
			u.perf.recordRemoteOp(perfRemoteWrite, lastErr, time.Since(uploadStart), uint64(len(data)))
		}

		if lastErr == nil {
			break
		}
		if errors.Is(lastErr, client.ErrConflict) {
			// Same-origin repair first: if this mount itself landed this
			// entry's image after it staged (a duplicate-commit race — the
			// kernel's asynchronous RELEASE racing a handoff flush of the
			// same handle), rebase onto the winner's base and retry
			// instead of staying conflicted forever. Exactly ONE repair
			// per upload: successive conflicts can keep finding eligible
			// newer duplicates while this worker holds the path gate, and
			// re-repairing on the extra attempt would extend the gate
			// beyond the single promised retry (the sync path repairs once).
			if !repaired {
				if rev, ok := u.resolveConflict(localPath, meta, data, expectedRevision); ok {
					expectedRevision = rev
					repaired = true
					if attempt == uploadMaxRetries {
						// The repair earned its own attempt: refund this
						// slot so the rebased PUT runs even when the
						// conflict hit the last ordinary attempt.
						attempt--
					}
					continue
				}
			}
			break
		}
		safeLogPrintf("writeback upload attempt %d/%d failed for %s: %v", attempt+1, uploadMaxRetries+1, localPath, lastErr)
	}

	if lastErr != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		if errors.Is(lastErr, client.ErrConflict) {
			// TODO: persist a conflict marker or resolution flow so these
			// entries do not remain pending forever across mounts.
			safeLogPrintf("writeback upload conflict for %s at base revision %d (will keep local pending data): %v", localPath, meta.BaseRev, lastErr)
			return
		}
		safeLogPrintf("writeback upload failed for %s after %d attempts: %v (will retry on next mount)", localPath, uploadMaxRetries+1, lastErr)
		return
	}
	u.recordLanding(localPath, data, committedRev, meta)
	if u.OnSuccess != nil && meta.LineageTrusted() && meta.SnapshotID != "" && meta.Size == int64(len(data)) && len(data) <= 1<<20 {
		// The proof identifies the data PUT even when its later chmod fails.
		meta.LiveChecksum = payloadChecksum(data)
	}
	committedRev = committedRevisionForExpectedRevision(expectedRevision, committedRev)
	chmodCtx, chmodCancel := context.WithTimeout(context.Background(), 30*time.Second)
	modeErr := u.applyMode(chmodCtx, meta)
	chmodCancel()
	if modeErr != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		if updated, err := u.cache.MarkChmodPending(localPath, gen); err != nil {
			safeLogPrintf("writeback upload chmod-pending meta update failed for %s: %v", localPath, err)
			safeLogPrintf("%v (will retry on next mount)", modeErr)
			return
		} else if updated {
			safeLogPrintf("writeback upload data committed for %s; chmod remains pending", localPath)
		}
		if u.OnSuccess != nil {
			u.OnSuccess(*meta, committedRev, stagingGens)
		}
		safeLogPrintf("%v (will retry on next mount)", modeErr)
		return
	}

	// Atomically remove from cache only if the generation hasn't changed.
	// If a newer Put happened while we were uploading, the cache now holds
	// fresher data that must not be discarded. RemoveIfGeneration ensures the
	// check and delete are under the same lock acquisition.
	u.cache.RemoveIfGeneration(localPath, gen)
	if u.perf != nil {
		u.perf.uploaderSuccess.add(1)
	}
	if u.OnSuccess != nil {
		u.OnSuccess(*meta, committedRev, stagingGens)
	}
}

func (u *WriteBackUploader) applyPendingChmod(ctx context.Context, localPath string, meta *staging.WriteBackMeta, gen uint64, sync bool) error {
	chmodCtx, chmodCancel := context.WithTimeout(ctx, 30*time.Second)
	err := u.applyMode(chmodCtx, meta)
	chmodCancel()
	if err != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		if sync {
			return err
		}
		safeLogPrintf("%v (chmod remains pending)", err)
		return nil
	}

	u.cache.RemoveIfGeneration(localPath, gen)
	if u.perf != nil {
		u.perf.uploaderSuccess.add(1)
	}
	if u.OnSuccess != nil {
		u.OnSuccess(*meta, 0, StagingGens{WriteBackGen: gen})
	}
	return nil
}
