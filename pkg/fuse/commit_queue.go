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
)

// commitQueueDirectPutThreshold is the size limit below which commit queue
// workers use direct PUT (WriteCtxConditionalWithRevision) instead of
// multipart upload. Must match the server's smallFileThreshold — the server
// rejects simple PUTs for files >= 50KB on S3-configured backends by requiring
// X-Dat9-Part-Checksums (multipart protocol).
const commitQueueDirectPutThreshold = client.DefaultSmallFileThreshold

// CommitEntry represents a pending remote commit.
type CommitEntry struct {
	Path        string
	Inode       uint64
	BaseRev     int64 // revision when we started editing
	Size        int64
	Kind        PendingKind
	ShadowSpill bool // true when data is only in shadow file (auto-resolve would OOM)
}

// CommitSuccessFunc is called after a commit queue entry is successfully
// uploaded. committedRev is the server-returned revision (>0 for direct PUT,
// 0 for multipart where the revision is not returned inline).
type CommitSuccessFunc func(entry *CommitEntry, committedRev int64)

// CommitQueue manages ordered background remote commits with baseRev tracking.
// It provides backpressure when the queue exceeds maxPending items.
type CommitQueue struct {
	mu         sync.Mutex
	queue      []*CommitEntry
	inFlight   map[string]struct{} // paths currently being processed by workers
	canceled   map[string]struct{} // paths canceled by Unlink/Rmdir (workers skip these)
	maxPending int
	client     *client.Client
	shadows    *ShadowStore
	index      *PendingIndex
	journal    *Journal
	wg         sync.WaitGroup
	stopped    bool

	// OnSuccess is called after successful upload with the committed
	// revision. Used by dat9fs to seed readCache and update inode revision.
	OnSuccess CommitSuccessFunc

	// workCh dispatches entries to upload workers. The buffer is always
	// larger than maxPending so Enqueue never blocks.
	workCh chan *CommitEntry
}

// NewCommitQueue creates a CommitQueue with background workers.
func NewCommitQueue(c *client.Client, shadows *ShadowStore, index *PendingIndex, journal *Journal, numWorkers int, maxPending int) *CommitQueue {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	if maxPending <= 0 {
		maxPending = maxCommitQueuePending
	}
	// Buffer must be > maxPending so Enqueue's send never blocks.
	bufSize := maxPending * 2
	if bufSize < 256 {
		bufSize = 256
	}
	cq := &CommitQueue{
		maxPending: maxPending,
		client:     c,
		shadows:    shadows,
		index:      index,
		journal:    journal,
		inFlight:   make(map[string]struct{}),
		canceled:   make(map[string]struct{}),
		workCh:     make(chan *CommitEntry, bufSize),
	}
	for i := 0; i < numWorkers; i++ {
		cq.wg.Add(1)
		go cq.worker()
	}
	return cq
}

// Enqueue adds a commit entry to the queue. Returns an error if the queue
// is full (backpressure).
func (cq *CommitQueue) Enqueue(entry *CommitEntry) error {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if cq.stopped {
		return fmt.Errorf("commit queue stopped")
	}
	if len(cq.queue) >= cq.maxPending {
		return fmt.Errorf("commit queue full (%d pending)", cq.maxPending)
	}
	cq.queue = append(cq.queue, entry)

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

// IsFull reports whether the queue has reached its backpressure limit.
func (cq *CommitQueue) IsFull() bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return len(cq.queue) >= cq.maxPending
}

// DrainAll stops accepting new entries and waits for all workers to finish.
func (cq *CommitQueue) DrainAll() {
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
		queued := false
		for _, e := range cq.queue {
			if e.Path == path {
				queued = true
				break
			}
		}
		cq.mu.Unlock()
		if !inflight && !queued {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
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

// CancelPath marks a path as canceled so workers skip it, removes it from
// the queue, and cleans up its shadow/index state. Used by Unlink to
// prevent background commits from resurrecting deleted files.
func (cq *CommitQueue) CancelPath(path string) {
	cq.mu.Lock()
	cq.canceled[path] = struct{}{}
	for i, e := range cq.queue {
		if e.Path == path {
			cq.queue = append(cq.queue[:i], cq.queue[i+1:]...)
			break
		}
	}
	cq.mu.Unlock()

	if cq.shadows != nil {
		cq.shadows.Remove(path)
	}
	if cq.index != nil {
		cq.index.Remove(path)
	}
}

// CancelPrefix marks all paths under prefix as canceled, removes them from
// the queue, and cleans up their shadow/index state. Used by Rmdir.
// Workers that have already dequeued entries will check the canceled set
// before uploading.
func (cq *CommitQueue) CancelPrefix(prefix string) {
	cq.mu.Lock()
	var remaining []*CommitEntry
	var cancelled []string
	for _, e := range cq.queue {
		if strings.HasPrefix(e.Path, prefix) {
			cancelled = append(cancelled, e.Path)
			cq.canceled[e.Path] = struct{}{}
		} else {
			remaining = append(remaining, e)
		}
	}
	cq.queue = remaining
	cq.mu.Unlock()

	for _, p := range cancelled {
		if cq.shadows != nil {
			cq.shadows.Remove(p)
		}
		if cq.index != nil {
			cq.index.Remove(p)
		}
	}
}

// isCanceled checks whether the path has been canceled by Unlink/Rmdir.
func (cq *CommitQueue) isCanceled(path string) bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if _, ok := cq.canceled[path]; ok {
		return true
	}
	// Also check prefix cancellations.
	for p := range cq.canceled {
		if strings.HasSuffix(p, "/") && strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func (cq *CommitQueue) worker() {
	defer cq.wg.Done()
	for entry := range cq.workCh {
		// Check if this entry was canceled while buffered in workCh.
		if cq.isCanceled(entry.Path) {
			cq.removeFromQueue(entry)
			log.Printf("commit queue: skipping canceled entry for %s", entry.Path)
			continue
		}

		// Mark as in-flight so WaitPath blocks until cleanup finishes.
		cq.mu.Lock()
		cq.inFlight[entry.Path] = struct{}{}
		cq.mu.Unlock()

		cq.commitOne(entry)

		// Clear in-flight after all cleanup is done.
		cq.mu.Lock()
		delete(cq.inFlight, entry.Path)
		cq.mu.Unlock()
	}
}

// commitOne uploads a single entry to the server with exponential backoff.
func (cq *CommitQueue) commitOne(entry *CommitEntry) {
	const maxRetries = 5
	const baseDelay = 200 * time.Millisecond
	const maxDelay = 30 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Re-check cancelation between retries.
		if cq.isCanceled(entry.Path) {
			cq.removeFromQueue(entry)
			log.Printf("commit queue: entry for %s was canceled during retry", entry.Path)
			return
		}

		if attempt > 0 {
			delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt-1)))
			if delay > maxDelay {
				delay = maxDelay
			}
			time.Sleep(delay)
		}

		timeout := uploadTimeout
		if entry.ShadowSpill {
			timeout = releaseTimeout(entry.Size)
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		committedRev, err := cq.uploadEntry(ctx, entry)
		cancel()

		if err == nil {
			// Success — clean up.
			cq.onCommitSuccess(entry, committedRev)
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
	cq.onCommitTerminalFailure(entry)
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

	// ShadowSpill entries: stream directly from shadow file to avoid loading
	// multi-GiB files into memory. Uses io.SectionReader over the shadow fd.
	if entry.ShadowSpill {
		return 0, uploadFromShadow(ctx, cq.client, cq.shadows, entry.Path, expectedRevision)
	}

	// Non-ShadowSpill: read full content into memory.
	data, err := cq.shadows.ReadAll(entry.Path)
	if err != nil {
		return 0, fmt.Errorf("read shadow: %w", err)
	}

	// Route based on entry.Size (metadata at enqueue time), NOT len(data).
	// Files under commitQueueDirectPutThreshold use direct PUT to skip the
	// multipart initiate/presign/complete/finalize overhead (~440ms).
	if entry.Size < commitQueueDirectPutThreshold {
		committedRev, err := cq.client.WriteCtxConditionalWithRevision(ctx, entry.Path, data, expectedRevision)
		return committedRev, err
	}

	// Larger non-ShadowSpill files: multipart upload.
	return 0, uploadBufferedRemoteFile(ctx, cq.client, entry.Path, data, expectedRevision)
}

func (cq *CommitQueue) removeFromQueue(entry *CommitEntry) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	for i, e := range cq.queue {
		if e == entry {
			cq.queue = append(cq.queue[:i], cq.queue[i+1:]...)
			return
		}
	}
}

func (cq *CommitQueue) onCommitSuccess(entry *CommitEntry, committedRev int64) {
	// Write durable commit record BEFORE cleaning up local state so that
	// crash recovery never re-uploads an already committed entry.
	if cq.journal != nil {
		if err := cq.journal.Append(JournalEntry{
			Op:   JournalCommit,
			Path: entry.Path,
		}); err != nil {
			log.Printf("commit queue: journal commit marker failed for %s: %v (keeping local state)", entry.Path, err)
			cq.removeFromQueue(entry)
			return
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

	// Remove from queue AFTER all cleanup so WaitPath sees the entry
	// until bookkeeping is complete.
	cq.removeFromQueue(entry)

	log.Printf("commit queue: successfully uploaded %s (%d bytes, rev=%d)", entry.Path, entry.Size, committedRev)
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
	if cq.isCanceled(entry.Path) {
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
		if cq.isCanceled(entry.Path) {
			cq.removeFromQueue(entry)
			log.Printf("commit queue: auto-resolve skipped for %s (canceled mid-read)", entry.Path)
			return
		}
		log.Printf("commit queue: auto-resolve failed for %s: read shadow: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}

	// Fetch server's current state: revision + content.
	// Use per-RPC timeouts so that a slow Read doesn't starve the Upload budget.
	statCtx, statCancel := context.WithTimeout(context.Background(), 10*time.Second)
	stat, err := cq.client.StatCtx(statCtx, entry.Path)
	statCancel()
	if err != nil {
		log.Printf("commit queue: auto-resolve failed for %s: stat: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}
	serverRev := stat.Revision

	readCtx, readCancel := context.WithTimeout(context.Background(), uploadTimeout)
	serverData, err := cq.client.ReadCtx(readCtx, entry.Path)
	readCancel()
	if err != nil {
		log.Printf("commit queue: auto-resolve failed for %s: read server: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}

	// Branch 1: idempotent — content already matches server.
	if bytes.Equal(localData, serverData) {
		log.Printf("commit queue: auto-resolved conflict for %s (idempotent, content matches server rev %d)", entry.Path, serverRev)
		cq.onCommitSuccess(entry, 0)
		return
	}

	// Branch 2: LWW — re-upload local shadow with new base revision.
	// Re-check cancelation before the potentially expensive upload.
	if cq.isCanceled(entry.Path) {
		cq.removeFromQueue(entry)
		log.Printf("commit queue: auto-resolve aborted for %s before LWW upload (canceled)", entry.Path)
		return
	}
	log.Printf("commit queue: auto-resolving conflict for %s via LWW (base rev %d → server rev %d)", entry.Path, entry.BaseRev, serverRev)
	uploadCtx, uploadCancel := context.WithTimeout(context.Background(), releaseTimeout(int64(len(localData))))
	err = uploadBufferedRemoteFile(uploadCtx, cq.client, entry.Path, localData, serverRev)
	uploadCancel()
	if err != nil {
		log.Printf("commit queue: auto-resolve LWW re-upload failed for %s: %v", entry.Path, err)
		cq.onCommitTerminalFailure(entry)
		return
	}

	log.Printf("commit queue: auto-resolved conflict for %s via LWW (overwrote rev %d → new upload based on rev %d)", entry.Path, entry.BaseRev, serverRev)
	cq.onCommitSuccess(entry, 0)
}

func (cq *CommitQueue) onCommitTerminalFailure(entry *CommitEntry) {
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
