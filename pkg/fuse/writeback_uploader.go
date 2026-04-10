package fuse

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// uploadTimeout is the per-file timeout for background write-back uploads.
const uploadTimeout = 60 * time.Second

// submitTimeout is the maximum time Submit will block waiting for channel space
// before falling back to a synchronous upload in the calling goroutine.
const submitTimeout = 5 * time.Second

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
	client   *client.Client
	cache    *WriteBackCache
	uploadCh chan string // remote paths to upload
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopped  atomic.Bool
	stopCh   chan struct{}

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
	for _, e := range entries {
		log.Printf("writeback: recovering pending upload for %s (%d bytes)", e.Meta.Path, e.Meta.Size)
		u.Submit(e.Meta.Path)
	}
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

	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	if err := u.client.WriteCtx(ctx, remotePath, data); err != nil {
		log.Printf("writeback upload failed for %s: %v (will retry on next mount)", remotePath, err)
		return
	}

	// Only remove from cache if the generation hasn't changed. If a newer
	// Put() happened while we were uploading, the cache now holds fresher
	// data that must not be discarded.
	curMeta, ok := u.cache.GetMeta(remotePath)
	if ok && curMeta.Generation == gen {
		u.cache.Remove(remotePath)
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

	if err := u.client.WriteCtx(ctx, remotePath, data); err != nil {
		return err
	}

	// Only remove if generation matches — a concurrent Put() may have written newer data.
	curMeta, ok := u.cache.GetMeta(remotePath)
	if ok && curMeta.Generation == gen {
		u.cache.Remove(remotePath)
	}
	return nil
}
