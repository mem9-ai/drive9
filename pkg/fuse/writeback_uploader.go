package fuse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
)

// Exponential backoff constants for upload retries.
const (
	uploadBackoffBase = 200 * time.Millisecond
	uploadBackoffMax  = 30 * time.Second
	uploadMaxRetries  = 3
)

// uploadTimeout is the per-file timeout for background write-back uploads.
const uploadTimeout = 60 * time.Second

// submitTimeout is the maximum time Submit will block waiting for channel space
// before falling back to a synchronous upload in the calling goroutine.
const submitTimeout = 5 * time.Second

var errWriteBackBaseRevisionRequired = errors.New("writeback base revision required")

// pathState tracks per-path in-flight upload state so that callers can wait
// for completion and concurrent uploads of the same path are serialized.
type pathState struct {
	done chan struct{} // closed when the current upload finishes
}

type WriteBackSuccessFunc func(meta WriteBackMeta, committedRev int64, gens StagingGens)

// StagingGens captures the generations of the writeBack, pendingIndex and
// shadowStore entries associated with a writeBack entry at the moment it is
// read for upload. This lets the OnSuccess callback perform generation-guarded
// cleanup so a stale upload does not remove a fresher same-path staging entry
// that was created by a concurrent write while the upload was in flight.
type StagingGens struct {
	WriteBackGen    uint64
	PendingIndexGen uint64
	ShadowGen       uint64
}

// SnapshotStagingGensFunc snapshots the pendingIndex and shadowStore generations
// for a path. If either store is absent or has no entry, the corresponding gen
// is 0 (which never matches, so the guarded remove is skipped). Optional — if
// nil, OnSuccess receives zero-value gens.
type SnapshotStagingGensFunc func(remotePath string) StagingGens

// WriteBackUploader consumes pending write-back cache entries and uploads
// them to the server in the background. It runs a fixed number of worker
// goroutines that read from a shared channel.
//
// Per-path serialization: at most one upload per local namespace path is in-flight at
// any time. WaitPath() allows Rename/Unlink/Fsync to block until the current
// upload for a path finishes.
type WriteBackUploader struct {
	client     *client.Client
	cache      *WriteBackCache
	remoteRoot string
	uploadCh   chan string // local namespace paths to upload
	wg         sync.WaitGroup
	stopOnce   sync.Once
	stopped    atomic.Bool
	active     atomic.Int64
	stopCh     chan struct{}

	// Per-path in-flight tracking.
	inflightMu sync.Mutex
	inflight   map[string]*pathState

	perf      *fusePerfCounters
	OnSuccess WriteBackSuccessFunc
	// SnapshotStagingGens optionally captures the pendingIndex/shadowStore
	// generations for a path so OnSuccess can do generation-guarded cleanup.
	SnapshotStagingGens SnapshotStagingGensFunc
}

// NewWriteBackUploader creates and starts a background uploader with
// numWorkers goroutines. Typical value: 4.
func NewWriteBackUploader(c *client.Client, cache *WriteBackCache, numWorkers int, remoteRoot ...string) *WriteBackUploader {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	root := "/"
	if len(remoteRoot) > 0 && remoteRoot[0] != "" {
		root = remoteRoot[0]
	}
	u := &WriteBackUploader{
		client:     c,
		cache:      cache,
		remoteRoot: root,
		uploadCh:   make(chan string, 256),
		stopCh:     make(chan struct{}),
		inflight:   make(map[string]*pathState),
	}
	for i := 0; i < numWorkers; i++ {
		u.wg.Add(1)
		go u.worker()
	}
	return u
}

func (u *WriteBackUploader) remotePath(localPath string) string {
	root := "/"
	if u != nil && u.remoteRoot != "" {
		root = u.remoteRoot
	}
	return mountpath.ToRemote(root, localPath)
}

func (u *WriteBackUploader) applyMode(ctx context.Context, meta *WriteBackMeta) error {
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
		return fmt.Errorf("writeback upload chmod %s to %o: %w", meta.Path, mode, err)
	}
	return nil
}

func (u *WriteBackUploader) SetPerfCounters(perf *fusePerfCounters) {
	u.perf = perf
}

// PendingStats returns queued and in-flight upload counts for observability.
func (u *WriteBackUploader) PendingStats() (queued int, inFlight int) {
	if u == nil {
		return 0, 0
	}
	queued = len(u.uploadCh)
	u.inflightMu.Lock()
	inFlight = len(u.inflight)
	u.inflightMu.Unlock()
	if active := int(u.active.Load()); active > inFlight {
		inFlight = active
	}
	return queued, inFlight
}

type WriteBackUploaderSnapshot struct {
	Queued      int
	InFlight    int
	Cached      int
	CachedBytes int64
	FirstPath   string
}

func (u *WriteBackUploader) Snapshot() WriteBackUploaderSnapshot {
	if u == nil {
		return WriteBackUploaderSnapshot{}
	}
	snap := WriteBackUploaderSnapshot{Queued: len(u.uploadCh)}
	u.inflightMu.Lock()
	snap.InFlight = len(u.inflight)
	for path := range u.inflight {
		snap.FirstPath = path
		break
	}
	u.inflightMu.Unlock()
	if active := int(u.active.Load()); active > snap.InFlight {
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

func (u *WriteBackUploader) WaitIdle(ctx context.Context) error {
	if u == nil {
		return nil
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap := u.Snapshot()
		if snap.Queued == 0 && snap.InFlight == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Submit enqueues a local namespace path for background upload. Blocks up to 5s if the
// channel is full; on timeout, falls back to synchronous upload in the current
// goroutine so data is never silently dropped.
func (u *WriteBackUploader) Submit(localPath string) {
	if u.perf != nil {
		u.perf.uploaderSubmit.add(1)
	}
	if u.stopped.Load() {
		log.Printf("writeback uploader: already stopped, uploading synchronously for %s", localPath)
		if u.perf != nil {
			u.perf.uploaderSyncFallback.add(1)
		}
		u.uploadOne(localPath)
		return
	}
	select {
	case u.uploadCh <- localPath:
	default:
		// Channel full — block with timeout, then fallback to sync upload.
		timer := time.NewTimer(submitTimeout)
		defer timer.Stop()
		select {
		case u.uploadCh <- localPath:
		case <-timer.C:
			log.Printf("writeback uploader: channel full after %v, uploading synchronously for %s", submitTimeout, localPath)
			if u.perf != nil {
				u.perf.uploaderSyncFallback.add(1)
			}
			u.uploadOne(localPath)
		}
	}
}

func (u *WriteBackUploader) directPutThreshold() int64 {
	if u != nil && u.client != nil {
		return u.client.CachedSmallFileThreshold()
	}
	return 0
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

// DrainAll closes the upload channel and waits for all inflight uploads to
// complete. Called during graceful shutdown.
func (u *WriteBackUploader) DrainAll() {
	start := time.Now()
	defer func() {
		if u.perf != nil {
			u.perf.uploaderDrainCount.add(1)
			u.perf.uploaderDrainTotal.add(uint64(time.Since(start)))
		}
	}()
	u.stopOnce.Do(func() {
		u.stopped.Store(true)
		close(u.uploadCh)
	})
	u.wg.Wait()
}

// RecoverPending scans the cache directory for leftover entries from a
// previous session and submits them for upload.
func (u *WriteBackUploader) RecoverPending() {
	entries := u.cache.ListPending()
	skippedLegacyOverwrites := 0
	for _, e := range entries {
		if e.Meta.Kind == PendingOverwrite && e.Meta.BaseRev <= 0 {
			skippedLegacyOverwrites++
			log.Printf("writeback: skipping legacy overwrite without base revision for %s", e.Meta.Path)
			continue
		}
		log.Printf("writeback: recovering pending upload for %s (%d bytes)", e.Meta.Path, e.Meta.Size)
		u.Submit(e.Meta.Path)
	}
	if skippedLegacyOverwrites > 0 {
		log.Printf("writeback: skipped %d legacy overwrite entries without base revision during recovery", skippedLegacyOverwrites)
	}
}

func expectedRevisionForWriteBack(meta *WriteBackMeta) (int64, error) {
	if meta == nil {
		return -1, fmt.Errorf("missing writeback metadata")
	}
	if meta.Kind == PendingChmod {
		return -1, fmt.Errorf("data already uploaded for %s; chmod pending", meta.Path)
	}
	if meta.Kind == PendingNew {
		return 0, nil
	}
	if meta.BaseRev <= 0 {
		return -1, errWriteBackBaseRevisionRequired
	}
	return meta.BaseRev, nil
}

func (u *WriteBackUploader) worker() {
	defer u.wg.Done()
	for localPath := range u.uploadCh {
		u.active.Add(1)
		func() {
			defer u.active.Add(-1)
			u.uploadOne(localPath)
		}()
	}
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

	// PendingChmod entries have no .dat file (data already uploaded, only
	// chmod remains). Read meta first so we can handle chmod without loading
	// data — GetMetaAndView would prune the entry when the .dat is missing.
	chmodMeta, chmodOK := u.cache.GetMeta(localPath)
	if !chmodOK {
		return // Already uploaded or removed.
	}
	if chmodMeta.Kind == PendingChmod {
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
		log.Printf("writeback upload skipped for %s: %v", localPath, err)
		return
	}

	// Retry with exponential backoff.
	var (
		lastErr      error
		committedRev int64
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
			break
		}
		log.Printf("writeback upload attempt %d/%d failed for %s: %v", attempt+1, uploadMaxRetries+1, localPath, lastErr)
	}

	if lastErr != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		if errors.Is(lastErr, client.ErrConflict) {
			// TODO: persist a conflict marker or resolution flow so these
			// entries do not remain pending forever across mounts.
			log.Printf("writeback upload conflict for %s at base revision %d (will keep local pending data)", localPath, meta.BaseRev)
			return
		}
		log.Printf("writeback upload failed for %s after %d attempts: %v (will retry on next mount)", localPath, uploadMaxRetries+1, lastErr)
		return
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
			log.Printf("writeback upload chmod-pending meta update failed for %s: %v", localPath, err)
		} else if updated {
			log.Printf("writeback upload data committed for %s; chmod remains pending", localPath)
		}
		log.Printf("%v (will retry on next mount)", modeErr)
		return
	}

	// Atomically remove from cache only if the generation hasn't changed.
	// If a newer Put() happened while we were uploading, the cache now holds
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

// UploadSync synchronously uploads a single path from the cache to the server.
// Waits for any in-flight background upload to finish first, then uploads with
// generation protection. Used by Fsync/Rename which require the data to be
// persisted remotely before returning.
func (u *WriteBackUploader) UploadSync(ctx context.Context, localPath string) error {
	_, err := u.UploadSyncWithRevision(ctx, localPath)
	return err
}

func (u *WriteBackUploader) UploadSyncWithRevision(ctx context.Context, localPath string) (int64, error) {
	// Wait for any in-flight background upload to complete first.
	u.WaitPath(localPath)

	// PendingChmod entries have no .dat file (data already uploaded, only
	// chmod remains). Read meta first so we can handle chmod without loading
	// data — GetMetaAndView would prune the entry when the .dat is missing.
	chmodMeta, chmodOK := u.cache.GetMeta(localPath)
	if !chmodOK {
		return 0, nil // not in cache (may have been uploaded by the background worker we just waited for)
	}
	if chmodMeta.Kind == PendingChmod {
		return 0, u.applyPendingChmod(ctx, localPath, chmodMeta, chmodMeta.Generation, true)
	}

	// Atomically read meta + data under one path lock so they are guaranteed
	// to be from the same generation.
	meta, data, ok := u.cache.GetMetaAndView(localPath)
	if !ok {
		return 0, nil // not in cache (removed between GetMeta and GetMetaAndView)
	}
	gen := meta.Generation

	expectedRevision, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		if errors.Is(err, errWriteBackBaseRevisionRequired) && meta.Kind == PendingOverwrite {
			// Backward compatibility for pre-CAS writeback entries: preserve
			// the historical UploadSync behaviour so fsync/rename do not fail
			// with EIO on mounts that still have legacy pending overwrites.
			log.Printf("writeback uploadsync: legacy overwrite without base revision for %s, falling back to unconditional write", localPath)
			expectedRevision = -1
		} else {
			return 0, fmt.Errorf("resolve expected revision for %s: %w", localPath, err)
		}
	}

	var committedRev int64
	uploadStart := time.Now()
	threshold := u.directPutThreshold()
	useDirectPUT := meta.Size == 0 || (threshold > 0 && meta.Size < threshold)
	if useDirectPUT {
		committedRev, err = u.client.WriteCtxConditionalWithRevision(ctx, u.remotePath(localPath), data, expectedRevision)
	} else {
		committedRev, err = uploadBufferedRemoteFileWithRevision(ctx, u.client, u.remotePath(localPath), data, expectedRevision)
	}
	if u.perf != nil {
		u.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(uploadStart), uint64(len(data)))
	}
	if err != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		return 0, err
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
			return 0, fmt.Errorf("%w; mark chmod pending: %v", err, markErr)
		}
		return 0, err
	}

	// Atomically remove only if generation matches — a concurrent Put() may
	// have written newer data between our read and this point.
	u.cache.RemoveIfGeneration(localPath, gen)
	if u.perf != nil {
		u.perf.uploaderSuccess.add(1)
	}
	return committedRev, nil
}

func (u *WriteBackUploader) applyPendingChmod(ctx context.Context, localPath string, meta *WriteBackMeta, gen uint64, sync bool) error {
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
		log.Printf("%v (chmod remains pending)", err)
		return nil
	}

	u.cache.RemoveIfGeneration(localPath, gen)
	if u.perf != nil {
		u.perf.uploaderSuccess.add(1)
	}
	return nil
}
