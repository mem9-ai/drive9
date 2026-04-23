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
// Per-path serialization: at most one upload per remote path is in-flight at
// any time. WaitPath() allows Rename/Unlink/Fsync to block until the current
// upload for a path finishes.
type WriteBackUploader struct {
	client    *client.Client
	cache     *WriteBackCache
	uploadCh  chan string // remote paths to upload
	wg        sync.WaitGroup
	stopOnce  sync.Once
	stopped   atomic.Bool
	stopCh    chan struct{}
	onSuccess func(remotePath string, committedRevision int64)

	// Per-path in-flight tracking.
	inflightMu sync.Mutex
	inflight   map[string]*pathState
}

// NewWriteBackUploader creates and starts a background uploader with
// numWorkers goroutines. Typical value: 4.
func NewWriteBackUploader(c *client.Client, cache *WriteBackCache, numWorkers int) *WriteBackUploader {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	u := &WriteBackUploader{
		client:   c,
		cache:    cache,
		uploadCh: make(chan string, 256),
		stopCh:   make(chan struct{}),
		inflight: make(map[string]*pathState),
	}
	for i := 0; i < numWorkers; i++ {
		u.wg.Add(1)
		go u.worker()
	}
	return u
}

// SetSuccessCallback installs a hook that runs after a cached upload commits.
// committedRevision is the post-commit remote revision when it can be derived
// from the cached metadata. Callers should set the hook before starting uploads.
func (u *WriteBackUploader) SetSuccessCallback(fn func(remotePath string, committedRevision int64)) {
	u.onSuccess = fn
}

// Submit enqueues a remote path for background upload. Blocks up to 5s if the
// channel is full; on timeout, falls back to synchronous upload in the current
// goroutine so data is never silently dropped.
func (u *WriteBackUploader) Submit(remotePath string) {
	if u.stopped.Load() {
		log.Printf("writeback uploader: already stopped, uploading synchronously for %s", remotePath)
		u.uploadOne(remotePath)
		return
	}
	select {
	case u.uploadCh <- remotePath:
	default:
		// Channel full — block with timeout, then fallback to sync upload.
		timer := time.NewTimer(submitTimeout)
		defer timer.Stop()
		select {
		case u.uploadCh <- remotePath:
		case <-timer.C:
			log.Printf("writeback uploader: channel full after %v, uploading synchronously for %s", submitTimeout, remotePath)
			u.uploadOne(remotePath)
		}
	}
}

// WaitPath blocks until any in-flight upload for remotePath completes.
// Returns immediately if no upload is in progress for this path.
// This must be called by Rename/Unlink before operating on the remote path
// to prevent the background worker from re-creating a renamed/deleted file.
func (u *WriteBackUploader) WaitPath(remotePath string) {
	u.inflightMu.Lock()
	ps, ok := u.inflight[remotePath]
	u.inflightMu.Unlock()
	if ok {
		<-ps.done
	}
}

// DrainAll closes the upload channel and waits for all inflight uploads to
// complete. Called during graceful shutdown.
func (u *WriteBackUploader) DrainAll() {
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
	for remotePath := range u.uploadCh {
		u.uploadOne(remotePath)
	}
}

// acquirePath registers an in-flight upload for remotePath. If another upload
// for the same path is already in progress, it waits for that to finish first,
// ensuring per-path serialization. Returns a release function that the caller
// must call when the upload is done.
func (u *WriteBackUploader) acquirePath(remotePath string) func() {
	for {
		u.inflightMu.Lock()
		ps, ok := u.inflight[remotePath]
		if !ok {
			// No in-flight upload — register ours.
			ps = &pathState{done: make(chan struct{})}
			u.inflight[remotePath] = ps
			u.inflightMu.Unlock()
			return func() {
				u.inflightMu.Lock()
				// Only delete if it's still our state (not replaced by a new upload).
				if cur, ok := u.inflight[remotePath]; ok && cur == ps {
					delete(u.inflight, remotePath)
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

func (u *WriteBackUploader) uploadOne(remotePath string) {
	release := u.acquirePath(remotePath)
	defer release()

	// Read data and remember the generation so we can detect concurrent overwrites.
	meta, ok := u.cache.GetMeta(remotePath)
	if !ok {
		return // Already uploaded or removed.
	}
	gen := meta.Generation

	data, ok := u.cache.Get(remotePath)
	if !ok {
		return
	}

	expectedRevision, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		// TODO: add a migration or cleanup path for legacy overwrite entries
		// without base revisions so they do not remain pending forever when
		// they only flow through Flush -> Release -> background uploadOne.
		log.Printf("writeback upload skipped for %s: %v", remotePath, err)
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
		lastErr = uploadBufferedRemoteFile(ctx, u.client, remotePath, data, expectedRevision)
		cancel()

		if lastErr == nil {
			break
		}
		if errors.Is(lastErr, client.ErrConflict) {
			break
		}
		log.Printf("writeback upload attempt %d/%d failed for %s: %v", attempt+1, uploadMaxRetries+1, remotePath, lastErr)
	}

	if lastErr != nil {
		if errors.Is(lastErr, client.ErrConflict) {
			// TODO: persist a conflict marker or resolution flow so these
			// entries do not remain pending forever across mounts.
			log.Printf("writeback upload conflict for %s at base revision %d (will keep local pending data)", remotePath, meta.BaseRev)
			return
		}
		log.Printf("writeback upload failed for %s after %d attempts: %v (will retry on next mount)", remotePath, uploadMaxRetries+1, lastErr)
		return
	}

	// Only remove from cache if the generation hasn't changed. If a newer
	// Put() happened while we were uploading, the cache now holds fresher
	// data that must not be discarded.
	curMeta, ok := u.cache.GetMeta(remotePath)
	if ok && curMeta.Generation == gen {
		u.cache.Remove(remotePath)
	}
	if committedRev, ok := committedRevisionForPending(meta.Kind, meta.BaseRev); ok {
		if u.onSuccess != nil {
			u.onSuccess(remotePath, committedRev)
		}
		log.Printf("writeback upload success for %s: base_rev=%d committed_rev=%d kind=%d; refreshed FUSE revision state", remotePath, meta.BaseRev, committedRev, meta.Kind)
	} else {
		log.Printf("writeback upload success for %s: base_rev=%d kind=%d; committed revision unknown, FUSE revision state unchanged", remotePath, meta.BaseRev, meta.Kind)
	}
}

// UploadSync synchronously uploads a single path from the cache to the server.
// Waits for any in-flight background upload to finish first, then uploads with
// generation protection. Used by Fsync/Rename which require the data to be
// persisted remotely before returning.
func (u *WriteBackUploader) UploadSync(ctx context.Context, remotePath string) error {
	// Wait for any in-flight background upload to complete first.
	u.WaitPath(remotePath)

	meta, ok := u.cache.GetMeta(remotePath)
	if !ok {
		return nil // not in cache (may have been uploaded by the background worker we just waited for)
	}
	gen := meta.Generation

	data, ok := u.cache.Get(remotePath)
	if !ok {
		return nil
	}

	expectedRevision, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		if errors.Is(err, errWriteBackBaseRevisionRequired) && meta.Kind == PendingOverwrite {
			// Backward compatibility for pre-CAS writeback entries: preserve
			// the historical UploadSync behaviour so fsync/rename do not fail
			// with EIO on mounts that still have legacy pending overwrites.
			log.Printf("writeback uploadsync: legacy overwrite without base revision for %s, falling back to unconditional write", remotePath)
			expectedRevision = -1
		} else {
			return fmt.Errorf("resolve expected revision for %s: %w", remotePath, err)
		}
	}

	if err := uploadBufferedRemoteFile(ctx, u.client, remotePath, data, expectedRevision); err != nil {
		return err
	}

	// Only remove if generation matches — a concurrent Put() may have written newer data.
	curMeta, ok := u.cache.GetMeta(remotePath)
	if ok && curMeta.Generation == gen {
		u.cache.Remove(remotePath)
	}
	return nil
}
