package server

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
)

// defaultTenantNotifyFlushInterval is how often the coalescer flushes merged
// work signals to tenant_notify_outbox.
const defaultTenantNotifyFlushInterval = 200 * time.Millisecond

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
//   - Flush failures are logged and the batch is DROPPED: the outbox is
//     already best-effort and the 5-minute safety-net scan is the backstop
//     for any lost signal.
type tenantNotifyCoalescer struct {
	insert        func(ctx context.Context, entries []meta.TenantNotifyEntry) error
	flushInterval time.Duration

	mu      sync.Mutex
	pending map[string]int // tenantID → OR-merged work_mask

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newTenantNotifyCoalescer(insert func(ctx context.Context, entries []meta.TenantNotifyEntry) error, flushInterval time.Duration) *tenantNotifyCoalescer {
	if flushInterval <= 0 {
		flushInterval = defaultTenantNotifyFlushInterval
	}
	return &tenantNotifyCoalescer{
		insert:        insert,
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

// add OR-merges one work signal into the pending batch.
func (c *tenantNotifyCoalescer) add(tenantID string, workMask int) {
	if tenantID == "" || workMask == 0 {
		return
	}
	c.mu.Lock()
	c.pending[tenantID] |= workMask
	c.mu.Unlock()
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

// stop cancels the flush loop, waits for it to exit, then performs a final
// flush on a non-cancelled context so pending signals are not dropped at
// shutdown.
func (c *tenantNotifyCoalescer) stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.flush(ctx)
}

// flush swaps out the pending map and writes it as one batch INSERT. An
// empty map is a no-op (no statement issued). Failures are logged and the
// batch is dropped — the safety-net scan recovers the work.
func (c *tenantNotifyCoalescer) flush(ctx context.Context) {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.pending
	c.pending = make(map[string]int)
	c.mu.Unlock()

	entries := make([]meta.TenantNotifyEntry, 0, len(batch))
	for tenantID, workMask := range batch {
		entries = append(entries, meta.TenantNotifyEntry{TenantID: tenantID, WorkMask: workMask})
	}
	if err := c.insert(ctx, entries); err != nil {
		logger.Warn(ctx, "tenant_notify_coalescer_flush_failed",
			zap.Int("tenants", len(entries)),
			zap.Error(err))
	}
}
