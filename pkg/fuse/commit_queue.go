package fuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountpath"
)

var errCommitPostUpload = errors.New("commit post-upload step failed")

// directPutThreshold returns the size limit below which commit queue workers
// use direct PUT (WriteCtxConditionalWithRevision) instead of multipart
// upload. Must match the server's inline_threshold — the server rejects
// simple PUTs for files at or above the threshold on S3-configured backends
// by requiring X-Dat9-Part-Checksums (multipart protocol). Read from the
// client's cached value to avoid surprise GET /v1/status calls in the hot
// commit path; the FS layer is expected to have warmed the cache via the
// startup inlineThreshold() call.
//
// Returns 0 when no server value has been negotiated yet. Callers that
// route on the result must treat 0 as "force multipart": the server may
// be configured below the historical 50KB default, and a fixed fallback
// would direct-PUT files the server then rejects. Multipart is always
// accepted.
func (cq *CommitQueue) directPutThreshold() int64 {
	if cq.client != nil {
		return cq.client.CachedSmallFileThreshold()
	}
	return 0
}

// CommitEntry represents a pending remote commit.
type CommitEntry struct {
	Path         string
	Inode        uint64
	BaseRev      int64 // revision when we started editing
	Size         int64
	Kind         PendingKind
	ShadowSpill  bool // true when data is only in shadow file (auto-resolve would OOM)
	Mode         uint32
	HasMode      bool
	canceled     bool
	cancelCommit context.CancelFunc
	cancelUpload context.CancelFunc
}

// CommitSuccessFunc is called after a commit queue entry is successfully
// uploaded. committedRev is the server-returned revision (>0 for direct PUT,
// 0 for multipart where the revision is not returned inline).
type CommitSuccessFunc func(entry *CommitEntry, committedRev int64)

// CommitCleanupFunc is called after a successful commit's local shadow/index
// state has been removed but before the queue entry is dequeued.
type CommitCleanupFunc func(entry *CommitEntry)

// CommitQueue manages ordered background remote commits with baseRev tracking.
// It provides backpressure when the queue exceeds maxPending items.
type CommitQueue struct {
	mu           sync.Mutex
	queue        []*CommitEntry
	queuedByPath map[string]map[*CommitEntry]struct{}
	inFlight     map[string]*CommitEntry // paths currently being processed by workers
	maxPending   int
	client       *client.Client
	remoteRoot   string
	shadows      *ShadowStore
	index        *PendingIndex
	journal      *Journal
	wg           sync.WaitGroup
	stopped      bool

	// OnSuccess is called after successful upload with the committed
	// revision. Used by dat9fs to seed readCache and update inode revision.
	OnSuccess CommitSuccessFunc

	// OnCleanup is called after local commit state has been removed.
	OnCleanup CommitCleanupFunc

	// workCh dispatches entries to upload workers. The buffer is always
	// larger than maxPending so Enqueue never blocks.
	workCh chan *CommitEntry

	perf *fusePerfCounters
}

// NewCommitQueue creates a CommitQueue with background workers.
func NewCommitQueue(c *client.Client, shadows *ShadowStore, index *PendingIndex, journal *Journal, numWorkers int, maxPending int, remoteRoot ...string) *CommitQueue {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	if maxPending <= 0 {
		maxPending = maxCommitQueuePending
	}
	root := "/"
	if len(remoteRoot) > 0 && remoteRoot[0] != "" {
		root = remoteRoot[0]
	}
	// Buffer must be > maxPending so Enqueue's send never blocks.
	bufSize := maxPending * 2
	if bufSize < 256 {
		bufSize = 256
	}
	cq := &CommitQueue{
		maxPending:   maxPending,
		client:       c,
		remoteRoot:   root,
		shadows:      shadows,
		index:        index,
		journal:      journal,
		inFlight:     make(map[string]*CommitEntry),
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		workCh:       make(chan *CommitEntry, bufSize),
	}
	for i := 0; i < numWorkers; i++ {
		cq.wg.Add(1)
		go cq.worker()
	}
	return cq
}

func (cq *CommitQueue) SetPerfCounters(perf *fusePerfCounters) {
	cq.perf = perf
}

func (cq *CommitQueue) remotePath(localPath string) string {
	root := "/"
	if cq != nil && cq.remoteRoot != "" {
		root = cq.remoteRoot
	}
	return mountpath.ToRemote(root, localPath)
}

// Enqueue adds a commit entry to the queue. Returns an error if the queue
// is full (backpressure).
func (cq *CommitQueue) Enqueue(entry *CommitEntry) error {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if cq.stopped {
		if cq.perf != nil {
			cq.perf.commitEnqueueError.add(1)
		}
		return fmt.Errorf("commit queue stopped")
	}
	if len(cq.queue) >= cq.maxPending {
		if cq.perf != nil {
			cq.perf.commitEnqueueError.add(1)
		}
		return fmt.Errorf("commit queue full (%d pending)", cq.maxPending)
	}
	cq.queue = append(cq.queue, entry)
	cq.addQueuedLocked(entry)
	if cq.perf != nil {
		cq.perf.commitEnqueue.add(1)
	}

	// Send to workers while holding the lock. The channel buffer is always
	// > maxPending, so this will not block as long as the backpressure
	// check above holds. Holding the lock prevents DrainAll from closing
	// workCh between our check and the send.
	cq.workCh <- entry
	return nil
}

// Pending returns the number of pending commits.
func (cq *CommitQueue) Pending() int {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return len(cq.queue)
}

// PendingStats returns the number of unique pending entries and the sum
// of their sizes. Used by unmount to display drain progress.
//
// Subtleties:
//  1. In-flight entries remain in cq.queue until the worker calls
//     removeFromQueue() *after* cleanup (see line 692). The same
//     *CommitEntry pointer is stored in both cq.queue and cq.inFlight
//     during that window.
//  2. Enqueue does NOT dedupe by Path (line 153 just appends). A path
//     written + closed twice yields two distinct entries with the same
//     Path string but different pointers.
//
// We dedupe by entry pointer: collapses (1) without collapsing (2).
func (cq *CommitQueue) PendingStats() (count int, bytes int64) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	seen := make(map[*CommitEntry]struct{}, len(cq.queue))
	for _, e := range cq.queue {
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		count++
		bytes += e.Size
	}
	for _, e := range cq.inFlight {
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		count++
		bytes += e.Size
	}
	return
}

// IsFull reports whether the queue has reached its backpressure limit.
func (cq *CommitQueue) IsFull() bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return len(cq.queue) >= cq.maxPending
}

// DrainAll stops accepting new entries and waits for all workers to finish.
func (cq *CommitQueue) DrainAll() {
	start := time.Now()
	defer func() {
		if cq.perf != nil {
			cq.perf.commitDrainCount.add(1)
			cq.perf.commitDrainTotalNS.add(uint64(time.Since(start)))
		}
	}()
	cq.mu.Lock()
	if cq.stopped {
		cq.mu.Unlock()
		cq.wg.Wait()
		return
	}
	cq.stopped = true
	// Close workCh under the lock so no concurrent Enqueue can send after close.
	close(cq.workCh)
	cq.mu.Unlock()
	cq.wg.Wait()
}

// RecoverPending re-enqueues any locally persisted pending commits on startup.
func (cq *CommitQueue) RecoverPending() {
	if cq.index == nil {
		return
	}
	for path := range cq.index.ListPendingPaths() {
		meta, ok := cq.index.GetMeta(path)
		if !ok {
			continue
		}
		if cq.shadows != nil && !cq.shadows.Has(path) {
			// Shadow file missing — prune orphaned pending index entry so
			// Lookup/GetAttr don't serve stale metadata.
			log.Printf("commit queue: pruning orphaned pending entry for %s (shadow missing)", path)
			cq.index.Remove(path)
			continue
		}
		if meta.Kind == PendingConflict {
			log.Printf("commit queue: skipping conflicted entry for %s (preserved for manual recovery)", path)
			continue
		}
		if meta.Kind == PendingOverwrite && meta.BaseRev <= 0 {
			log.Printf("commit queue: skip legacy pending overwrite without base revision for %s", path)
			continue
		}
		entry := &CommitEntry{
			Path:        path,
			BaseRev:     meta.BaseRev,
			Size:        meta.Size,
			Kind:        meta.Kind,
			ShadowSpill: meta.ShadowSpill,
			Mode:        meta.Mode,
			HasMode:     meta.HasMode,
		}
		if err := cq.Enqueue(entry); err != nil {
			log.Printf("commit queue: recover enqueue failed for %s: %v", path, err)
		}
	}
}

// WaitPath blocks until any in-flight or queued commit for the given path
// completes (including post-commit cleanup). This prevents namespace
// operations from racing with background commits.
func (cq *CommitQueue) WaitPath(path string) {
	for {
		cq.mu.Lock()
		_, inflight := cq.inFlight[path]
		queued := cq.hasQueuedPathLocked(path)
		cq.mu.Unlock()
		if !inflight && !queued {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// HasPath reports whether a path is queued or currently in flight.
func (cq *CommitQueue) HasPath(path string) bool {
	if cq == nil {
		return false
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if _, inflight := cq.inFlight[path]; inflight {
		return true
	}
	return cq.hasQueuedPathLocked(path)
}

// WaitPrefix blocks until all in-flight or queued commits under the given
// prefix complete. Used by Rename to wait for descendant commits.
func (cq *CommitQueue) WaitPrefix(prefix string) {
	for {
		cq.mu.Lock()
		found := false
		for p := range cq.inFlight {
			if strings.HasPrefix(p, prefix) {
				found = true
				break
			}
		}
		if !found {
			for _, e := range cq.queue {
				if strings.HasPrefix(e.Path, prefix) {
					found = true
					break
				}
			}
		}
		cq.mu.Unlock()
		if !found {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// CancelPath marks currently queued or in-flight entries for path as canceled,
// removes queued entries, and cleans up shadow/index state. Cancellation is
// entry-scoped so future files that reuse the same path (for example git's
// config.lock) are not poisoned by an old cancellation.
func (cq *CommitQueue) CancelPath(path string) {
	cq.cancelPath(path, false)
}

// CancelPathPreserveLocal cancels queued or in-flight uploads for path without
// removing the local shadow/index state. Rename uses this for Git's loose-object
// temp files: the temp upload must stop, but the bytes must survive while the
// pending entry moves to the content-addressed final path.
func (cq *CommitQueue) CancelPathPreserveLocal(path string) {
	cq.cancelPath(path, true)
}

func (cq *CommitQueue) cancelPath(path string, preserveLocal bool) {
	var cancels []context.CancelFunc
	seen := make(map[*CommitEntry]struct{})
	markCanceled := func(e *CommitEntry) {
		if e == nil {
			return
		}
		e.canceled = true
		if _, ok := seen[e]; ok {
			return
		}
		seen[e] = struct{}{}
		if e.cancelCommit != nil {
			cancels = append(cancels, e.cancelCommit)
		}
		if e.cancelUpload != nil {
			cancels = append(cancels, e.cancelUpload)
		}
	}

	cq.mu.Lock()
	if e, ok := cq.inFlight[path]; ok {
		markCanceled(e)
	}
	if cq.queuedByPath != nil {
		for e := range cq.queuedByPath[path] {
			markCanceled(e)
		}
	}
	if cq.queuedByPath == nil {
		remaining := cq.queue[:0]
		for _, e := range cq.queue {
			if e.Path == path {
				markCanceled(e)
				continue
			}
			remaining = append(remaining, e)
		}
		cq.queue = remaining
	} else if len(cq.queuedByPath[path]) > 0 {
		remaining := cq.queue[:0]
		for _, e := range cq.queue {
			if e.Path != path {
				remaining = append(remaining, e)
			}
		}
		cq.queue = remaining
		delete(cq.queuedByPath, path)
	}
	cq.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	if preserveLocal {
		return
	}
	if cq.shadows != nil {
		cq.shadows.Remove(path)
	}
	if cq.index != nil {
		cq.index.Remove(path)
	}
}

// CancelPrefix marks queued or in-flight entries under prefix as canceled,
// removes queued entries, and cleans up their shadow/index state. Cancellation
// is entry-scoped, so future entries under the same prefix are unaffected.
func (cq *CommitQueue) CancelPrefix(prefix string) {
	cq.mu.Lock()
	var remaining []*CommitEntry
	var cancelled []string
	var cancels []context.CancelFunc
	seen := make(map[*CommitEntry]struct{})
	markCanceled := func(e *CommitEntry) {
		if e == nil {
			return
		}
		e.canceled = true
		if _, ok := seen[e]; ok {
			return
		}
		seen[e] = struct{}{}
		if e.cancelCommit != nil {
			cancels = append(cancels, e.cancelCommit)
		}
		if e.cancelUpload != nil {
			cancels = append(cancels, e.cancelUpload)
		}
	}
	for p, e := range cq.inFlight {
		if strings.HasPrefix(p, prefix) {
			markCanceled(e)
			cancelled = append(cancelled, p)
		}
	}
	for _, e := range cq.queue {
		if strings.HasPrefix(e.Path, prefix) {
			markCanceled(e)
			cancelled = append(cancelled, e.Path)
		} else {
			remaining = append(remaining, e)
		}
	}
	cq.queue = remaining
	if cq.queuedByPath != nil {
		cq.rebuildQueuedIndexLocked()
	}
	cq.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	for _, p := range cancelled {
		if cq.shadows != nil {
			cq.shadows.Remove(p)
		}
		if cq.index != nil {
			cq.index.Remove(p)
		}
	}
}

// isEntryCanceled checks whether this specific entry was canceled by
// Unlink/Rmdir. It intentionally does not key by path; git repeatedly reuses
// config.lock, and an old cancellation must not affect a newer entry.
func (cq *CommitQueue) isEntryCanceled(entry *CommitEntry) bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if entry == nil {
		return false
	}
	return entry.canceled
}

func (cq *CommitQueue) worker() {
	defer cq.wg.Done()
	for entry := range cq.workCh {
		// Check if this entry was canceled while buffered in workCh.
		if cq.isEntryCanceled(entry) {
			cq.removeFromQueue(entry)
			log.Printf("commit queue: skipping canceled entry for %s", entry.Path)
			continue
		}

		// Mark as in-flight so WaitPath blocks until cleanup finishes.
		cq.mu.Lock()
		cq.inFlight[entry.Path] = entry
		cq.mu.Unlock()

		cq.commitOne(entry)

		// Clear in-flight after all cleanup is done.
		cq.mu.Lock()
		if cq.inFlight[entry.Path] == entry {
			delete(cq.inFlight, entry.Path)
		}
		cq.mu.Unlock()
	}
}

// commitOne uploads a single entry to the server with exponential backoff.
func (cq *CommitQueue) commitOne(entry *CommitEntry) {
	const maxRetries = 5
	const baseDelay = 200 * time.Millisecond
	const maxDelay = 30 * time.Second

	entryCtx, entryCancel := context.WithCancel(context.Background())
	cq.mu.Lock()
	if entry.canceled {
		cq.mu.Unlock()
		entryCancel()
		cq.removeFromQueue(entry)
		log.Printf("commit queue: entry for %s was canceled before retry loop", entry.Path)
		return
	}
	entry.cancelCommit = entryCancel
	cq.mu.Unlock()
	defer func() {
		cq.mu.Lock()
		entry.cancelCommit = nil
		entry.cancelUpload = nil
		cq.mu.Unlock()
		entryCancel()
	}()

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Re-check cancelation between retries.
		if cq.isEntryCanceled(entry) {
			cq.removeFromQueue(entry)
			log.Printf("commit queue: entry for %s was canceled during retry", entry.Path)
			return
		}

		if attempt > 0 {
			if cq.perf != nil {
				cq.perf.commitRetry.add(1)
			}
			delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt-1)))
			if delay > maxDelay {
				delay = maxDelay
			}
			if !sleepWithCancel(entryCtx, delay) {
				cq.removeFromQueue(entry)
				log.Printf("commit queue: entry for %s was canceled during retry backoff", entry.Path)
				return
			}
		}

		timeout := uploadTimeout
		if entry.ShadowSpill {
			timeout = releaseTimeout(entry.Size)
		}
		ctx, cancel := context.WithTimeout(entryCtx, timeout)
		cq.mu.Lock()
		if entry.canceled {
			cq.mu.Unlock()
			cancel()
			cq.removeFromQueue(entry)
			log.Printf("commit queue: entry for %s was canceled before upload", entry.Path)
			return
		}
		entry.cancelUpload = cancel
		cq.mu.Unlock()

		committedRev, err := cq.uploadEntry(ctx, entry)
		cq.mu.Lock()
		entry.cancelUpload = nil
		cq.mu.Unlock()
		cancel()

		if err == nil {
			if err := cq.onCommitSuccess(entry, committedRev); err == nil {
				return
			} else {
				log.Printf("commit queue: post-upload attempt %d/%d failed for %s: %v", attempt+1, maxRetries, entry.Path, err)
				cq.onCommitPostUploadFailure(entry, err)
				return
			}
		}
		if cq.isEntryCanceled(entry) {
			cq.removeFromQueue(entry)
			log.Printf("commit queue: entry for %s was canceled during upload", entry.Path)
			return
		}
		if errors.Is(err, client.ErrConflict) {
			log.Printf("commit queue: conflict committing %s at base revision %d, attempting auto-resolve", entry.Path, entry.BaseRev)
			cq.tryAutoResolveConflict(entry)
			return
		}
		lastErr = err
		log.Printf("commit queue: upload attempt %d/%d failed for %s: %v", attempt+1, maxRetries, entry.Path, err)
	}

	log.Printf("commit queue: giving up on %s after %d retries: %v", entry.Path, maxRetries, lastErr)
	if errors.Is(lastErr, errCommitPostUpload) {
		cq.onCommitPostUploadFailure(entry, lastErr)
		return
	}
	cq.onCommitTerminalFailure(entry)
}

func sleepWithCancel(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// CommitNow uploads an entry synchronously through the same commit path used
// by workers. It is used as a fallback when the async queue rejects an entry
// after local state has already moved to the final path.
func (cq *CommitQueue) CommitNow(ctx context.Context, entry *CommitEntry) error {
	committedRev, err := cq.uploadEntry(ctx, entry)
	if err != nil {
		if cq.perf != nil {
			cq.perf.commitFailure.add(1)
		}
		return err
	}
	if err := cq.onCommitSuccess(entry, committedRev); err != nil {
		if cq.perf != nil {
			cq.perf.commitFailure.add(1)
		}
		return err
	}
	return nil
}

// uploadEntry uploads entry data to the server. Returns (committedRev, error).
// committedRev > 0 when direct PUT is used (server returns revision inline);
// committedRev == 0 for multipart uploads or ShadowSpill streams.
func (cq *CommitQueue) uploadEntry(ctx context.Context, entry *CommitEntry) (int64, error) {
	if cq.shadows == nil {
		return 0, fmt.Errorf("no shadow store")
	}

	expectedRevision := entry.BaseRev
	if entry.Kind == PendingOverwrite && expectedRevision <= 0 {
		return 0, fmt.Errorf("missing base revision for overwrite: %s", entry.Path)
	}
	apiPath := cq.remotePath(entry.Path)

	// ShadowSpill entries: stream directly from shadow file to avoid loading
	// multi-GiB files into memory. Uses io.SectionReader over the shadow fd.
	if entry.ShadowSpill {
		start := time.Now()
		committedRev, err := uploadFromShadowRemoteWithRevision(ctx, cq.client, cq.shadows, entry.Path, apiPath, expectedRevision)
		if cq.perf != nil {
			var bytes uint64
			if entry.Size > 0 {
				bytes = uint64(entry.Size)
			}
			cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), bytes)
		}
		return committedRev, err
	}

	// Non-ShadowSpill: read full content into memory.
	data, err := cq.shadows.ReadAll(entry.Path)
	if err != nil {
		return 0, fmt.Errorf("read shadow: %w", err)
	}

	// Route based on entry.Size (metadata at enqueue time), NOT len(data).
	// Files under directPutThreshold() use direct PUT to skip the multipart
	// initiate/presign/complete/finalize overhead (~440ms). When threshold
	// is 0 (no server value cached) we deliberately do not direct-PUT
	// non-empty files: the server may be configured below 50KB and would
	// reject the simple PUT. Zero-byte files keep direct PUT because V2
	// initiate rejects total_size=0.
	threshold := cq.directPutThreshold()
	useDirectPUT := entry.Size == 0 || (threshold > 0 && entry.Size < threshold)
	if useDirectPUT {
		start := time.Now()
		committedRev, err := cq.client.WriteCtxConditionalWithRevision(ctx, apiPath, data, expectedRevision)
		if cq.perf != nil {
			cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), uint64(len(data)))
		}
		return committedRev, err
	}

	// Larger non-ShadowSpill files: multipart upload.
	start := time.Now()
	err = uploadBufferedRemoteFile(ctx, cq.client, apiPath, data, expectedRevision)
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), uint64(len(data)))
	}
	return 0, err
}

func (cq *CommitQueue) removeFromQueue(entry *CommitEntry) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	for i, e := range cq.queue {
		if e == entry {
			cq.queue = append(cq.queue[:i], cq.queue[i+1:]...)
			cq.removeQueuedLocked(entry)
			return
		}
	}
}

func (cq *CommitQueue) addQueuedLocked(entry *CommitEntry) {
	if cq.queuedByPath == nil || entry == nil {
		return
	}
	entries := cq.queuedByPath[entry.Path]
	if entries == nil {
		entries = make(map[*CommitEntry]struct{})
		cq.queuedByPath[entry.Path] = entries
	}
	entries[entry] = struct{}{}
}

func (cq *CommitQueue) removeQueuedLocked(entry *CommitEntry) {
	if cq.queuedByPath == nil || entry == nil {
		return
	}
	entries := cq.queuedByPath[entry.Path]
	if entries == nil {
		return
	}
	delete(entries, entry)
	if len(entries) == 0 {
		delete(cq.queuedByPath, entry.Path)
	}
}

func (cq *CommitQueue) hasQueuedPathLocked(path string) bool {
	if cq.queuedByPath != nil {
		return len(cq.queuedByPath[path]) > 0
	}
	for _, e := range cq.queue {
		if e.Path == path {
			return true
		}
	}
	return false
}

func (cq *CommitQueue) rebuildQueuedIndexLocked() {
	if cq.queuedByPath == nil {
		return
	}
	cq.queuedByPath = make(map[string]map[*CommitEntry]struct{}, len(cq.queue))
	for _, e := range cq.queue {
		cq.addQueuedLocked(e)
	}
}

func (cq *CommitQueue) onCommitSuccess(entry *CommitEntry, committedRev int64) error {
	if shouldApplyRemoteMode(entry.Kind, entry.HasMode, entry.Mode) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		var err error
		mode := entry.Mode & posixPermissionModeMask
		remoteMode := remoteChmodMode(mode)
		err = retryPostUploadMode(ctx, func() error {
			start := time.Now()
			applyErr := cq.client.ChmodCtx(ctx, cq.remotePath(entry.Path), remoteMode)
			if cq.perf != nil {
				cq.perf.recordRemoteOp(perfRemoteMutation, applyErr, time.Since(start), 0)
			}
			return applyErr
		})
		cancel()
		if err != nil {
			return fmt.Errorf("%w: chmod %s to %o: %w", errCommitPostUpload, entry.Path, mode, err)
		}
	}

	// Write durable commit record BEFORE cleaning up local state so that
	// crash recovery never re-uploads an already committed entry.
	if cq.journal != nil {
		if err := cq.journal.Append(JournalEntry{
			Op:   JournalCommit,
			Path: entry.Path,
		}); err != nil {
			log.Printf("commit queue: journal commit marker failed for %s: %v (keeping local state)", entry.Path, err)
			cq.removeFromQueue(entry)
			return nil
		}
	}

	// Notify dat9fs to seed readCache + update inode revision before
	// cleaning up shadow (which is the data source for the cache seed).
	if cq.OnSuccess != nil {
		cq.OnSuccess(entry, committedRev)
	}

	// Clean up shadow and pending index.
	if cq.shadows != nil {
		cq.shadows.Remove(entry.Path)
	}
	if cq.index != nil {
		cq.index.Remove(entry.Path)
	}
	if cq.OnCleanup != nil {
		cq.OnCleanup(entry)
	}

	// Remove from queue AFTER all cleanup so WaitPath sees the entry
	// until bookkeeping is complete.
	cq.removeFromQueue(entry)

	if cq.perf != nil {
		cq.perf.commitSuccess.add(1)
	}
	log.Printf("commit queue: successfully uploaded %s (%d bytes, rev=%d)", entry.Path, entry.Size, committedRev)
	return nil
}

// tryAutoResolveConflict attempts to resolve a 409 conflict automatically.
// It fetches the server's current revision and content, then:
//   - If local shadow matches server content → idempotent (mark success)
//   - If local shadow differs → LWW re-upload with the new revision
//   - If re-upload also 409s or any step fails → fall back to terminal failure
//
// This covers ~80% of agent conflict scenarios (whole-file overwrites) without
// requiring 3-way merge. Max 1 retry to avoid write amplification.
func (cq *CommitQueue) tryAutoResolveConflict(entry *CommitEntry) {
	// Bail early if the file was deleted locally while queued.
	if cq.isEntryCanceled(entry) {
		cq.removeFromQueue(entry)
		log.Printf("commit queue: auto-resolve skipped for %s (canceled)", entry.Path)
		return
	}

	// ShadowSpill large files: auto-resolve requires full-memory ReadAll +
	// bytes.Equal which would OOM for multi-GiB files. Terminal failure instead.
	if entry.ShadowSpill {
		log.Printf("commit queue: auto-resolve skipped for ShadowSpill %s (would OOM), terminal failure", entry.Path)
		cq.onCommitTerminalFailure(entry)
		return
	}

	if cq.shadows == nil {
		cq.onCommitTerminalFailure(entry)
		return
	}

	// Read local shadow content.
	localData, err := cq.shadows.ReadAll(entry.Path)
	if err != nil {
		// Shadow may have been removed by a concurrent CancelPath/CancelPrefix
		// (Unlink/Rmdir). Treat as canceled rather than a true conflict.
		if cq.isEntryCanceled(entry) {
			cq.removeFromQueue(entry)
			log.Printf("commit queue: auto-resolve skipped for %s (canceled mid-read)", entry.Path)
			return
		}
		log.Printf("commit queue: auto-resolve failed for %s: read shadow: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}
	apiPath := cq.remotePath(entry.Path)

	// Fetch server's current state: revision + content.
	// Use per-RPC timeouts so that a slow Read doesn't starve the Upload budget.
	statCtx, statCancel := context.WithTimeout(context.Background(), 10*time.Second)
	statStart := time.Now()
	stat, err := cq.client.StatCtx(statCtx, apiPath)
	statCancel()
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteStat, err, time.Since(statStart), 0)
	}
	if err != nil {
		log.Printf("commit queue: auto-resolve failed for %s: stat: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}
	serverRev := stat.Revision

	readCtx, readCancel := context.WithTimeout(context.Background(), uploadTimeout)
	readStart := time.Now()
	serverData, err := cq.client.ReadCtx(readCtx, apiPath)
	readCancel()
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteRead, err, time.Since(readStart), uint64(len(serverData)))
	}
	if err != nil {
		log.Printf("commit queue: auto-resolve failed for %s: read server: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}

	// Branch 1: idempotent — content already matches server.
	if bytes.Equal(localData, serverData) {
		log.Printf("commit queue: auto-resolved conflict for %s (idempotent, content matches server rev %d)", entry.Path, serverRev)
		if err := cq.onCommitSuccess(entry, 0); err != nil {
			cq.onCommitPostUploadFailure(entry, err)
		}
		return
	}

	// Branch 2: LWW — re-upload local shadow with new base revision.
	// Re-check cancelation before the potentially expensive upload.
	if cq.isEntryCanceled(entry) {
		cq.removeFromQueue(entry)
		log.Printf("commit queue: auto-resolve aborted for %s before LWW upload (canceled)", entry.Path)
		return
	}
	log.Printf("commit queue: auto-resolving conflict for %s via LWW (base rev %d → server rev %d)", entry.Path, entry.BaseRev, serverRev)
	uploadCtx, uploadCancel := context.WithTimeout(context.Background(), releaseTimeout(int64(len(localData))))
	uploadStart := time.Now()
	err = uploadBufferedRemoteFile(uploadCtx, cq.client, apiPath, localData, serverRev)
	uploadCancel()
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(uploadStart), uint64(len(localData)))
	}
	if err != nil {
		log.Printf("commit queue: auto-resolve LWW re-upload failed for %s: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}

	log.Printf("commit queue: auto-resolved conflict for %s via LWW (overwrote rev %d → new upload based on rev %d)", entry.Path, entry.BaseRev, serverRev)
	if err := cq.onCommitSuccess(entry, 0); err != nil {
		cq.onCommitPostUploadFailure(entry, err)
	}
}

func (cq *CommitQueue) onCommitPostUploadFailure(entry *CommitEntry, err error) {
	if cq.perf != nil {
		cq.perf.commitFailure.add(1)
	}
	cq.removeFromQueue(entry)
	log.Printf("commit queue: post-upload failure for %s; local pending state preserved for retry: %v", entry.Path, err)
}

func (cq *CommitQueue) onCommitTerminalFailure(entry *CommitEntry) {
	if cq.perf != nil {
		cq.perf.commitFailure.add(1)
	}
	// Mark the entry as conflicted in the pending index so that crash
	// recovery (RecoverPending) skips it instead of retrying forever.
	// Preserve both the shadow file and the pending metadata so the user
	// can recover their local edits manually — deleting them here would
	// silently discard the only durable copy of unsynchronised data.
	//
	// The conflict marker MUST be durable before we journal or dequeue;
	// otherwise a restart could re-enqueue the same upload.
	if cq.index != nil {
		if err := cq.index.MarkConflict(entry.Path); err != nil {
			// Conflict marker not durable — leave the entry queued so
			// RecoverPending can retry on next startup rather than
			// silently dropping it.
			log.Printf("commit queue: failed to mark conflict for %s: %v (entry remains queued)", entry.Path, err)
			cq.removeFromQueue(entry)
			return
		}
	}
	if cq.journal != nil {
		_ = cq.journal.Append(JournalEntry{
			Op:   JournalCommit, // treated as "done" so recovery won't re-enqueue
			Path: entry.Path,
		})
	}

	// Remove from queue AFTER all cleanup so WaitPath sees the entry
	// until bookkeeping is complete.
	cq.removeFromQueue(entry)

	log.Printf("commit queue: terminal failure for %s — local data preserved for manual recovery", entry.Path)
}
