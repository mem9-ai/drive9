package fuse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// CommitEntry represents a pending remote commit.
type CommitEntry struct {
	Path    string
	Inode   uint64
	BaseRev int64 // revision when we started editing
	Size    int64
	Kind    PendingKind
}

// CommitQueue manages ordered background remote commits with baseRev tracking.
// It provides backpressure when the queue exceeds maxPending items.
type CommitQueue struct {
	mu         sync.Mutex
	queue      []*CommitEntry
	maxPending int
	client     *client.Client
	shadows    *ShadowStore
	index      *PendingIndex
	journal    *Journal
	wg         sync.WaitGroup
	stopped    bool

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
		if meta.Kind == PendingOverwrite && meta.BaseRev <= 0 {
			log.Printf("commit queue: skip legacy pending overwrite without base revision for %s", path)
			continue
		}
		entry := &CommitEntry{
			Path:    path,
			BaseRev: meta.BaseRev,
			Size:    meta.Size,
			Kind:    meta.Kind,
		}
		if err := cq.Enqueue(entry); err != nil {
			log.Printf("commit queue: recover enqueue failed for %s: %v", path, err)
		}
	}
}

func (cq *CommitQueue) worker() {
	defer cq.wg.Done()
	for entry := range cq.workCh {
		cq.commitOne(entry)
	}
}

// commitOne uploads a single entry to the server with exponential backoff.
func (cq *CommitQueue) commitOne(entry *CommitEntry) {
	const maxRetries = 5
	const baseDelay = 200 * time.Millisecond
	const maxDelay = 30 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt-1)))
			if delay > maxDelay {
				delay = maxDelay
			}
			time.Sleep(delay)
		}

		ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
		err := cq.uploadEntry(ctx, entry)
		cancel()

		if err == nil {
			// Success — clean up.
			cq.onCommitSuccess(entry)
			return
		}
		if errors.Is(err, client.ErrConflict) {
			log.Printf("commit queue: conflict committing %s at base revision %d", entry.Path, entry.BaseRev)
			cq.onCommitTerminalFailure(entry)
			return
		}
		lastErr = err
		log.Printf("commit queue: upload attempt %d/%d failed for %s: %v", attempt+1, maxRetries, entry.Path, err)
	}

	log.Printf("commit queue: giving up on %s after %d retries: %v", entry.Path, maxRetries, lastErr)
	cq.onCommitTerminalFailure(entry)
}

func (cq *CommitQueue) uploadEntry(ctx context.Context, entry *CommitEntry) error {
	// Read data from shadow store.
	if cq.shadows == nil {
		return fmt.Errorf("no shadow store")
	}
	data, err := cq.shadows.ReadAll(entry.Path)
	if err != nil {
		return fmt.Errorf("read shadow: %w", err)
	}

	expectedRevision := entry.BaseRev
	if entry.Kind == PendingOverwrite && expectedRevision <= 0 {
		return fmt.Errorf("missing base revision for overwrite: %s", entry.Path)
	}

	// Upload to server with a revision gate.
	if err := cq.client.WriteCtxConditional(ctx, entry.Path, data, expectedRevision); err != nil {
		return err
	}

	return nil
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

func (cq *CommitQueue) onCommitSuccess(entry *CommitEntry) {
	cq.removeFromQueue(entry)

	// Write durable commit record BEFORE cleaning up local state so that
	// crash recovery never re-uploads an already committed entry.
	if cq.journal != nil {
		if err := cq.journal.Append(JournalEntry{
			Op:   JournalCommit,
			Path: entry.Path,
		}); err != nil {
			log.Printf("commit queue: journal commit marker failed for %s: %v (keeping local state)", entry.Path, err)
			return
		}
	}

	// Clean up shadow and pending index.
	if cq.shadows != nil {
		cq.shadows.Remove(entry.Path)
	}
	if cq.index != nil {
		cq.index.Remove(entry.Path)
	}

	log.Printf("commit queue: successfully uploaded %s (%d bytes)", entry.Path, entry.Size)
}

func (cq *CommitQueue) onCommitTerminalFailure(entry *CommitEntry) {
	cq.removeFromQueue(entry)

	// Clean up local state to prevent infinite retry loop on restart.
	// The data is either stale (conflict) or unrecoverable (max retries).
	if cq.shadows != nil {
		cq.shadows.Remove(entry.Path)
	}
	if cq.index != nil {
		cq.index.Remove(entry.Path)
	}
	log.Printf("commit queue: terminal failure for %s, local state cleaned up", entry.Path)
}
