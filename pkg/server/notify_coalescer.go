package server

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

// defaultTenantNotifyFlushInterval is how often the coalescer flushes merged
// work signals to tenant_notify_outbox.
const defaultTenantNotifyFlushInterval = 200 * time.Millisecond

// tenantNotifyFlushRetryBackoff is the delay before a failed batch flush is
// retried once.
const tenantNotifyFlushRetryBackoff = 100 * time.Millisecond

// tenantNotifyCoalescer merges per-tenant tenant_notify_outbox signals in
// process and flushes them in one multi-row INSERT per flush window, instead
// of one single-row INSERT per event (46M inserts/12h under stress).
//
// Semantics of the merge:
//   - Signals for the same tenant are OR-merged into one row. This is
//     lossless: consumers dispatch by work_mask bit and the resulting kicks
//     are idempotent, so delivering a merged mask once is equivalent to
//     delivering every original signal.
//   - Coalescing adds at most one flush interval (200ms) of cross-pod
//     delivery latency on top of the existing 200ms outbox poller tick.
//     Same-pod delivery is unaffected: publishEvent still calls
//     bus.Publish() directly and the write path still kicks the in-process
//     tenant worker synchronously.
//   - Poller cursor semantics (id > cursor ORDER BY id) are unaffected
//     because each batch INSERT commits all of its rows atomically.
//   - A failed flush retries the batch once, then falls back to independent
//     per-row inserts; a row that still fails is logged and dropped. The
//     5-minute safety-net scan backstops only semantic/file_gc work — it
//     never reads tenant_notify_outbox — so SSE cross-pod delivery relies on
//     this retry → per-row fallback. Residual signal loss occurs only under
//     sustained metadb failure, the same failure class in which the
//     pre-coalescer single-row path lost events too.
type tenantNotifyCoalescer struct {
	insertBatch   func(ctx context.Context, entries []meta.TenantNotifyEntry) error
	insertSingle  func(ctx context.Context, tenantID string, workMask int) error
	flushInterval time.Duration

	// stopped is set by stop under mu; add checks it under the same mu, so a
	// signal can never slip into pending between stop's final flush and its
	// stopped-store (an entry added after the last flush would never be
	// flushed). insertTenantNotify reads the Server's coalescer field
	// unsynchronized from the write path, so the field is never cleared —
	// this flag is what keeps post-stop signals from being accepted into a
	// batch that will never flush.
	stopped bool

	mu      sync.Mutex
	pending map[string]int // tenantID → OR-merged work_mask

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newTenantNotifyCoalescer(insertBatch func(ctx context.Context, entries []meta.TenantNotifyEntry) error, insertSingle func(ctx context.Context, tenantID string, workMask int) error, flushInterval time.Duration) *tenantNotifyCoalescer {
	if flushInterval <= 0 {
		flushInterval = defaultTenantNotifyFlushInterval
	}
	return &tenantNotifyCoalescer{
		insertBatch:   insertBatch,
		insertSingle:  insertSingle,
		flushInterval: flushInterval,
		pending:       make(map[string]int),
	}
}

// start launches the periodic flush loop on a context derived from ctx.
func (c *tenantNotifyCoalescer) start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.wg.Add(1)
	go c.run(runCtx)
}

// add OR-merges one work signal into the pending batch. After stop it is a
// no-op: the flush loop is gone, so accepting the signal would drop it
// silently anyway. The stopped check runs under mu so it is serialized
// against stop's stopped-store and the final flush's map swap.
func (c *tenantNotifyCoalescer) add(tenantID string, workMask int) {
	if tenantID == "" || workMask == 0 {
		return
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.pending[tenantID] |= workMask
	pending := len(c.pending)
	c.mu.Unlock()
	metrics.RecordNotifyCoalescerPending(pending)
}

func (c *tenantNotifyCoalescer) run(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flush(ctx)
		}
	}
}

// stop marks the coalescer stopped (under mu, so racing adds either land in
// the pending map before the final flush or are rejected after it), cancels
// the flush loop, waits for it to exit, then performs a final flush on a
// non-cancelled context so pending signals are not dropped at shutdown.
func (c *tenantNotifyCoalescer) stop() {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.flush(ctx)
}

// flush swaps out the pending map and writes it as one batch INSERT. An
// empty map is a no-op (no statement issued). On batch failure the batch is
// retried once after a short backoff; if that also fails, each row is
// inserted individually, so a metadb hiccup degrades to roughly the
// pre-coalescer per-event behavior instead of dropping the whole window.
func (c *tenantNotifyCoalescer) flush(ctx context.Context) {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.pending
	c.pending = make(map[string]int)
	c.mu.Unlock()
	metrics.RecordNotifyCoalescerPending(0)

	entries := make([]meta.TenantNotifyEntry, 0, len(batch))
	for tenantID, workMask := range batch {
		entries = append(entries, meta.TenantNotifyEntry{TenantID: tenantID, WorkMask: workMask})
	}
	if err := c.insertBatch(ctx, entries); err == nil {
		metrics.RecordNotifyCoalescerFlush("ok", len(entries))
		return
	} else {
		logger.Warn(ctx, "tenant_notify_coalescer_flush_failed",
			zap.Int("tenants", len(entries)),
			zap.Error(err))
	}
	select {
	case <-ctx.Done():
		// No room for the batch retry; go straight to per-row inserts (they
		// fail fast on the cancelled context and are dropped per row).
	case <-time.After(tenantNotifyFlushRetryBackoff):
		if err := c.insertBatch(ctx, entries); err == nil {
			metrics.RecordNotifyCoalescerFlush("retry_ok", len(entries))
			return
		} else {
			logger.Warn(ctx, "tenant_notify_coalescer_flush_retry_failed",
				zap.Int("tenants", len(entries)),
				zap.Error(err))
		}
	}
	metrics.RecordNotifyCoalescerFlush("fallback", len(entries))
	c.insertPerRow(ctx, entries)
}

// insertPerRow delivers each entry independently; a failing row is logged
// and dropped without affecting the rest.
func (c *tenantNotifyCoalescer) insertPerRow(ctx context.Context, entries []meta.TenantNotifyEntry) {
	for _, e := range entries {
		if err := c.insertSingle(ctx, e.TenantID, e.WorkMask); err != nil {
			metrics.RecordNotifyCoalescerPerRowFallback("error")
			logger.Warn(ctx, "tenant_notify_coalescer_row_insert_failed",
				zap.String("tenant_id", e.TenantID),
				zap.Int("work_mask", e.WorkMask),
				zap.Error(err))
		} else {
			metrics.RecordNotifyCoalescerPerRowFallback("ok")
		}
	}
}
