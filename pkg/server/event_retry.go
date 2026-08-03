package server

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	// eventRetryGlobalCap bounds the total buffered entries per pod. One
	// tenant's sustained DB outage must not fill the shared buffer and cause
	// drop-oldest to discard healthy tenants' events, so the per-tenant
	// sub-cap below applies first.
	eventRetryGlobalCap = 10000
	// eventRetryTenantCap bounds buffered entries per tenant.
	eventRetryTenantCap = 1000
	// eventRetryScanInterval is how often the flush goroutine scans for due
	// entries.
	eventRetryScanInterval = 1 * time.Second
	// eventRetryBaseBackoff is the initial per-entry retry delay; it doubles
	// per attempt up to eventRetryMaxBackoff.
	eventRetryBaseBackoff = 1 * time.Second
	// eventRetryMaxBackoff caps the per-entry retry delay.
	eventRetryMaxBackoff = 5 * time.Minute
	// eventRetryMinMaxAge / eventRetryMaxMaxAge bound the retry buffer's
	// max-entry age: the server passes the fs_events retention and the
	// constructor clamps it into [1h, 24h]. The floor keeps a short-retention
	// deployment from dropping salvageable events within minutes; the cap
	// keeps a 7-day retention from pinning multi-day garbage in memory.
	eventRetryMinMaxAge = 1 * time.Hour
	eventRetryMaxMaxAge = 24 * time.Hour
	// eventRetryFlushTimeout bounds one entry's durable insert attempt so a
	// hung tenant-DB connection cannot block the flush goroutine — and
	// thereby wg.Wait() in stop() and the whole server Close — forever.
	eventRetryFlushTimeout = 30 * time.Second
	// eventRetryShutdownFlushTimeout bounds the final best-effort flush at
	// server shutdown. A typical 30s termination grace period leaves room
	// for several serial per-entry flushes (each bounded by
	// eventRetryFlushTimeout) plus the notify coalescer's own final flush.
	eventRetryShutdownFlushTimeout = 10 * time.Second
)

// fsEventInserter is the durable-insert dependency of the retry buffer,
// satisfied by *datastore.Store. Kept small and unexported so unit tests can
// substitute fakes without a database.
type fsEventInserter interface {
	InsertFSEvent(ctx context.Context, path, op, actor string, ts int64) (int64, error)
}

// eventRetryEntry is one buffered filesystem event awaiting a durable insert.
type eventRetryEntry struct {
	tenantID   string
	orgID      string
	bus        *EventBus
	path       string
	op         string
	actor      string
	ts         int64
	enqueuedAt time.Time
	attempts   int
	nextRetry  time.Time
}

// eventRetryBuffer is a per-pod bounded in-memory buffer for events whose
// fs_events INSERT failed. A background goroutine flushes due entries with
// per-entry exponential backoff. The buffer never blocks the mutation path:
// overflow drops the oldest entry (per tenant first, then globally) and counts
// it as hard loss via drive9_sse_event_retry_dropped_total.
//
// A successful flush performs the mandatory second wake — insertTenantNotify
// (cross-pod) and bus.Publish (same-pod) — exactly like a fresh publishEvent,
// because subscribers that already saw an empty poll otherwise believe they
// are caught up.
//
// Ordering note: seq is assigned at INSERT success, so a retried event is
// sequenced after events that persisted while it was buffered. Consumers
// observe bounded reorder around insert failures and must treat events as
// idempotent hints (see docs/design/sse-event-log-retention.md).
type eventRetryBuffer struct {
	// insertNotify delivers the cross-pod second wake (Server.insertTenantNotify).
	insertNotify func(tenantID string, workMask int)
	// loadStore resolves the tenant's current store from the entry's bus.
	// Defaults to the bus's atomic store pointer; tests substitute fakes.
	loadStore func(bus *EventBus) fsEventInserter
	// maxAge is how long an entry may sit in the buffer before it is dropped
	// (counted as hard loss). Set from the fs_events retention, clamped to
	// [eventRetryMinMaxAge, eventRetryMaxMaxAge].
	maxAge time.Duration

	// stopped is set by stop under mu; enqueue checks it under the same mu,
	// so an event can never slip into the buffer after the final flush (it
	// would never be retried — count it as dropped instead). Mirrors the
	// notify coalescer's stopped flag; the field is never cleared because
	// publishEvent reads the buffer pointer unsynchronized from the write path.
	stopped bool

	mu           sync.Mutex
	entries      []*eventRetryEntry // append-only; index 0 is the globally oldest
	tenantCounts map[string]int

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newEventRetryBuffer creates a buffer whose second-wake notify goes through
// insertNotify (Server.insertTenantNotify in production). maxAge is the
// per-entry drop age, clamped to [eventRetryMinMaxAge, eventRetryMaxMaxAge];
// the server passes its fs_events retention so a longer replay window is
// actually reachable by retried events.
func newEventRetryBuffer(insertNotify func(tenantID string, workMask int), maxAge time.Duration) *eventRetryBuffer {
	if maxAge < eventRetryMinMaxAge {
		maxAge = eventRetryMinMaxAge
	}
	if maxAge > eventRetryMaxMaxAge {
		maxAge = eventRetryMaxMaxAge
	}
	return &eventRetryBuffer{
		insertNotify: insertNotify,
		loadStore: func(bus *EventBus) fsEventInserter {
			if store := bus.store.Load(); store != nil {
				return store
			}
			return nil
		},
		maxAge:       maxAge,
		tenantCounts: make(map[string]int),
	}
}

// start launches the flush goroutine on a context derived from ctx. Runs on
// every pod (not leader-gated), like the outbox poller.
func (b *eventRetryBuffer) start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.wg.Add(1)
	go b.run(runCtx)
}

// stop marks the buffer stopped (under mu, so racing enqueues either land in
// the buffer before the final flush or are rejected and counted as dropped
// after it), cancels the flush goroutine, waits for it to exit, then attempts
// one final best-effort flush with a bounded timeout so buffered entries get
// a last chance to land before the process exits. All entries are made due
// for this flush regardless of their backoff slot.
func (b *eventRetryBuffer) stop() {
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
	b.mu.Lock()
	for _, e := range b.entries {
		e.nextRetry = time.Time{}
	}
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), eventRetryShutdownFlushTimeout)
	defer cancel()
	b.flushDue(ctx)
}

// run scans for due entries every eventRetryScanInterval until ctx is done.
func (b *eventRetryBuffer) run(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(eventRetryScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushDue(ctx)
		}
	}
}

// enqueue adds one failed event to the buffer, evicting the tenant's oldest
// entry at the per-tenant cap and the globally oldest entry at the global cap.
// Every eviction is counted per tenant as hard loss. After stop, enqueue does
// NOT buffer: the flush goroutine is gone, so the event would sit forever —
// it is counted as dropped (reason "stopped") instead.
func (b *eventRetryBuffer) enqueue(bus *EventBus, path, op, actor string, ts int64) {
	now := time.Now()
	entry := &eventRetryEntry{
		tenantID:   bus.tenantID,
		orgID:      bus.TiDBCloudOrgID(),
		bus:        bus,
		path:       path,
		op:         op,
		actor:      actor,
		ts:         ts,
		enqueuedAt: now,
		nextRetry:  now.Add(eventRetryBaseBackoff),
	}
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		metrics.RecordSSEEventRetryDropped(entry.tenantID, entry.orgID, "stopped")
		return
	}
	if b.tenantCounts[entry.tenantID] >= eventRetryTenantCap {
		b.dropOldestForTenantLocked(entry.tenantID, "tenant_cap")
	}
	if len(b.entries) >= eventRetryGlobalCap {
		b.dropOldestLocked("global_cap")
	}
	b.entries = append(b.entries, entry)
	b.tenantCounts[entry.tenantID]++
	depth := len(b.entries)
	b.mu.Unlock()
	metrics.RecordSSEEventRetryBufferDepth(depth)
}

// flushDue attempts every entry whose nextRetry has passed and drops entries
// older than the buffer's maxAge (counted as hard loss). Entries that fail
// (or have no store yet) are requeued with per-entry exponential backoff.
func (b *eventRetryBuffer) flushDue(ctx context.Context) {
	now := time.Now()
	b.mu.Lock()
	kept := b.entries[:0]
	var due []*eventRetryEntry
	for _, e := range b.entries {
		switch {
		case now.Sub(e.enqueuedAt) > b.maxAge:
			b.removeForTenantLocked(e.tenantID)
			metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, "expired")
		case !e.nextRetry.After(now):
			b.removeForTenantLocked(e.tenantID)
			due = append(due, e)
		default:
			kept = append(kept, e)
		}
	}
	b.entries = kept
	b.mu.Unlock()

	for _, e := range due {
		if ctx.Err() != nil {
			// Shutdown or scan interruption: requeue the rest untouched.
			b.requeue(e, now)
			continue
		}
		if !b.flushEntry(ctx, e) {
			b.requeue(e, now)
		}
	}

	b.mu.Lock()
	depth := len(b.entries)
	b.mu.Unlock()
	metrics.RecordSSEEventRetryBufferDepth(depth)
}

// flushEntry attempts the durable insert for one entry. On success it runs the
// mandatory second wake (insertTenantNotify + bus.Publish) and returns true.
// A nil store or an insert error returns false so the caller requeues. The
// insert runs under eventRetryFlushTimeout so a hung tenant-DB connection
// cannot block the flush goroutine — and thereby stop()'s wg.Wait() — forever.
func (b *eventRetryBuffer) flushEntry(ctx context.Context, e *eventRetryEntry) bool {
	inserter := b.loadStore(e.bus)
	if inserter == nil {
		// Store not (yet) available: requeue with backoff.
		return false
	}
	insertCtx, cancel := context.WithTimeout(ctx, eventRetryFlushTimeout)
	defer cancel()
	seq, err := inserter.InsertFSEvent(insertCtx, e.path, e.op, e.actor, e.ts)
	if err != nil {
		metrics.RecordTenantOperationWithOrg(e.tenantID, e.orgID, "event_bus", "retry_flush", metrics.ResultForError(err), 0)
		logger.Warn(ctx, "sse_event_retry_flush_failed",
			zap.String("tenant_id", e.tenantID),
			zap.String("path", e.path),
			zap.String("op", e.op),
			zap.Int("attempts", e.attempts+1),
			zap.Error(err))
		return false
	}
	metrics.RecordTenantOperationWithOrg(e.tenantID, e.orgID, "event_bus", "retry_flush", "ok", 0)
	logger.Info(ctx, "sse_event_retry_flush_ok",
		zap.String("tenant_id", e.tenantID),
		zap.String("path", e.path),
		zap.String("op", e.op),
		zap.Int64("seq", seq),
		zap.Int("attempts", e.attempts+1))
	// Mandatory second wake: without insertTenantNotify cross-pod subscribers
	// sleep until the next write; without bus.Publish same-pod connections
	// that already saw an empty poll believe they are caught up.
	if b.insertNotify != nil {
		b.insertNotify(e.tenantID, WorkSSE)
	}
	e.bus.Publish()
	return true
}

// requeue returns an entry to the buffer with the next backoff slot, unless it
// aged out while the flush was in flight (then it counts as hard loss).
func (b *eventRetryBuffer) requeue(e *eventRetryEntry, now time.Time) {
	e.attempts++
	backoff := eventRetryBaseBackoff << min(e.attempts, 10)
	if backoff > eventRetryMaxBackoff {
		backoff = eventRetryMaxBackoff
	}
	e.nextRetry = now.Add(backoff)
	b.mu.Lock()
	if now.Sub(e.enqueuedAt) > b.maxAge {
		b.mu.Unlock()
		metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, "expired")
		return
	}
	// Caps can be hit by concurrent enqueues while the flush was in flight;
	// apply the same eviction policy as enqueue.
	if b.tenantCounts[e.tenantID] >= eventRetryTenantCap {
		b.dropOldestForTenantLocked(e.tenantID, "tenant_cap")
	}
	if len(b.entries) >= eventRetryGlobalCap {
		b.dropOldestLocked("global_cap")
	}
	b.entries = append(b.entries, e)
	b.tenantCounts[e.tenantID]++
	b.mu.Unlock()
}

// dropOldestForTenantLocked removes the tenant's oldest entry and counts the
// drop. Caller must hold b.mu.
func (b *eventRetryBuffer) dropOldestForTenantLocked(tenantID, reason string) {
	for i, e := range b.entries {
		if e.tenantID == tenantID {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			b.removeForTenantLocked(tenantID)
			metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, reason)
			return
		}
	}
}

// dropOldestLocked removes the globally oldest entry and counts the drop.
// Caller must hold b.mu.
func (b *eventRetryBuffer) dropOldestLocked(reason string) {
	if len(b.entries) == 0 {
		return
	}
	e := b.entries[0]
	b.entries = b.entries[1:]
	b.removeForTenantLocked(e.tenantID)
	metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, reason)
}

// removeForTenantLocked decrements the per-tenant count, clearing the map
// entry at zero. Caller must hold b.mu.
func (b *eventRetryBuffer) removeForTenantLocked(tenantID string) {
	if n := b.tenantCounts[tenantID]; n <= 1 {
		delete(b.tenantCounts, tenantID)
	} else {
		b.tenantCounts[tenantID] = n - 1
	}
}
