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

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountpath"
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
	stopCh     chan struct{}

	// Per-path in-flight tracking.
	inflightMu sync.Mutex
	inflight   map[string]*pathState

	perf *fusePerfCounters
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

func (u *WriteBackUploader) SetPerfCounters(perf *fusePerfCounters) {
	u.perf = perf
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
		u.uploadOne(localPath)
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

	// Read data and remember the generation so we can detect concurrent overwrites.
	meta, ok := u.cache.GetMeta(localPath)
	if !ok {
		return // Already uploaded or removed.
	}
	gen := meta.Generation

	data, ok := u.cache.Get(localPath)
	if !ok {
		return
	}

	expectedRevision, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		// TODO: add a migration or cleanup path for legacy overwrite entries
		// without base revisions so they do not remain pending forever when
		// they only flow through Flush -> Release -> background uploadOne.
		log.Printf("writeback upload skipped for %s: %v", localPath, err)
		return
	}

	// Retry with exponential backoff.
	var lastErr error
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
		lastErr = uploadBufferedRemoteFile(ctx, u.client, u.remotePath(localPath), data, expectedRevision)
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

	// Only remove from cache if the generation hasn't changed. If a newer
	// Put() happened while we were uploading, the cache now holds fresher
	// data that must not be discarded.
	curMeta, ok := u.cache.GetMeta(localPath)
	if ok && curMeta.Generation == gen {
		u.cache.Remove(localPath)
	}
	if u.perf != nil {
		u.perf.uploaderSuccess.add(1)
	}
}

// UploadSync synchronously uploads a single path from the cache to the server.
// Waits for any in-flight background upload to finish first, then uploads with
// generation protection. Used by Fsync/Rename which require the data to be
// persisted remotely before returning.
func (u *WriteBackUploader) UploadSync(ctx context.Context, localPath string) error {
	// Wait for any in-flight background upload to complete first.
	u.WaitPath(localPath)

	meta, ok := u.cache.GetMeta(localPath)
	if !ok {
		return nil // not in cache (may have been uploaded by the background worker we just waited for)
	}
	gen := meta.Generation

	data, ok := u.cache.Get(localPath)
	if !ok {
		return nil
	}

	expectedRevision, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		if errors.Is(err, errWriteBackBaseRevisionRequired) && meta.Kind == PendingOverwrite {
			// Backward compatibility for pre-CAS writeback entries: preserve
			// the historical UploadSync behaviour so fsync/rename do not fail
			// with EIO on mounts that still have legacy pending overwrites.
			log.Printf("writeback uploadsync: legacy overwrite without base revision for %s, falling back to unconditional write", localPath)
			expectedRevision = -1
		} else {
			return fmt.Errorf("resolve expected revision for %s: %w", localPath, err)
		}
	}

	uploadStart := time.Now()
	err = uploadBufferedRemoteFile(ctx, u.client, u.remotePath(localPath), data, expectedRevision)
	if u.perf != nil {
		u.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(uploadStart), uint64(len(data)))
	}
	if err != nil {
		if u.perf != nil {
			u.perf.uploaderFailure.add(1)
		}
		return err
	}

	// Only remove if generation matches — a concurrent Put() may have written newer data.
	curMeta, ok := u.cache.GetMeta(localPath)
	if ok && curMeta.Generation == gen {
		u.cache.Remove(localPath)
	}
	if u.perf != nil {
		u.perf.uploaderSuccess.add(1)
	}
	return nil
}
