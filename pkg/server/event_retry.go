package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
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
	// hung tenant-DB connection cannot block its tenant's flusher — and
	// thereby wg.Wait() in stop() and the whole server Close — forever.
	eventRetryFlushTimeout = 30 * time.Second
	// eventRetryFlushConcurrency bounds how many tenant flushers run at once.
	// Per-tenant workers keep one tenant's hung DB from delaying other
	// tenants' flushes (no cross-tenant head-of-line blocking).
	eventRetryFlushConcurrency = 8
	// eventRetryShutdownFlushTimeout bounds the final best-effort flush at
	// server shutdown. A typical 30s termination grace period leaves room
	// for several serial per-entry flushes (each bounded by
	// eventRetryFlushTimeout) plus the notify coalescer's own final flush.
	eventRetryShutdownFlushTimeout = 10 * time.Second
)

// errTenantGone is returned by the store resolver when the tenant no longer
// exists (or is leaving active service), so its buffered events are dropped
// with reason tenant_gone instead of spinning forever.
var errTenantGone = errors.New("retry store: tenant gone")

// fsEventInserter is the durable-insert dependency of the retry buffer,
// satisfied by *datastore.Store. Kept small and unexported so unit tests can
// substitute fakes without a database.
type fsEventInserter interface {
	InsertFSEvent(ctx context.Context, path, op, actor string, ts int64) (int64, error)
}

// eventRetryEntry is one buffered filesystem event awaiting a durable insert.
// Entries live in the queue (counted against the caps and the depth gauge)
// for their entire residency — INCLUDING while flushing — so in-flight work
// can neither evade the caps nor be evicted from under its own flusher.
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
	// flushing marks an entry currently held by its tenant's flusher.
	// Eviction (tenant/global cap) skips flushing entries: the oldest
	// non-flushing entry is evicted instead.
	flushing bool
}

// eventRetryBuffer is a per-pod bounded in-memory buffer for events whose
// fs_events INSERT failed. Flush work is sharded PER TENANT: one FIFO
// flusher per active tenant, bounded globally by eventRetryFlushConcurrency,
// so a tenant with a hung DB cannot delay other tenants' flushes. The buffer
// never blocks the mutation path: overflow drops the oldest non-flushing
// entry (per tenant first, then globally) and counts it as hard loss via
// drive9_sse_event_retry_dropped_total.
//
// The queue stays enqueuedAt-ordered by construction: new entries append at
// the tail, failed entries update their backoff in place (they do NOT move),
// so "oldest" is always the first non-flushing match — no age-order
// inversion between requeued and fresh entries.
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
	// loadStore resolves the tenant's cached store from the entry's bus
	// (first try). Tests substitute fakes.
	loadStore func(bus *EventBus) fsEventInserter
	// resolveStore re-resolves the tenant's store via the server's normal
	// backend acquisition path when the cached pointer is nil or its DB is
	// closed (Server.resolveRetryStore in production; fakes in tests). It
	// refreshes the bus pointer as a side effect and returns errTenantGone
	// when the tenant no longer exists. Nil disables re-resolution.
	resolveStore func(ctx context.Context, tenantID string, bus *EventBus) (fsEventInserter, error)
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
	entries      []*eventRetryEntry // enqueuedAt-ordered; index 0 is the globally oldest
	tenantCounts map[string]int     // true residency per tenant, including in-flight entries
	tenantActive map[string]bool    // a flusher is currently draining this tenant
	flushSlots   chan struct{}      // global flusher concurrency cap

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newEventRetryBuffer creates a buffer whose second-wake notify goes through
// insertNotify (Server.insertTenantNotify in production). maxAge is the
// per-entry drop age, clamped to [eventRetryMinMaxAge, eventRetryMaxMaxAge];
// the server passes its fs_events retention so a longer replay window is
// actually reachable by retried events. resolveStore re-resolves stores for
// idle-closed tenants (see eventRetryBuffer.resolveStore).
func newEventRetryBuffer(insertNotify func(tenantID string, workMask int), maxAge time.Duration, resolveStore func(context.Context, string, *EventBus) (fsEventInserter, error)) *eventRetryBuffer {
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
		resolveStore: resolveStore,
		maxAge:       maxAge,
		tenantCounts: make(map[string]int),
		tenantActive: make(map[string]bool),
		flushSlots:   make(chan struct{}, eventRetryFlushConcurrency),
	}
}

// start launches the scan goroutine on a context derived from ctx. Runs on
// every pod (not leader-gated), like the outbox poller.
func (b *eventRetryBuffer) start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.wg.Add(1)
	go b.run(runCtx)
}

// stop marks the buffer stopped (under mu, so racing enqueues either land in
// the buffer before the final flush or are rejected and counted as dropped
// after it), cancels the scan goroutine, waits for it and any in-flight
// flushers to exit, then runs one final best-effort flush under the shutdown
// budget with all entries made due. Entries still buffered after the budget
// runs out are hard loss: each is counted as dropped (reason "shutdown") and
// the remaining depth is warn-logged.
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
	// flushDue spawned the final flushers with the budgeted ctx; wait for
	// them (the ctx bounds their total runtime, not the per-scan timeout).
	b.wg.Wait()
	b.mu.Lock()
	leftover := len(b.entries)
	for _, e := range b.entries {
		metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, "shutdown")
	}
	b.entries = nil
	b.tenantCounts = make(map[string]int)
	b.mu.Unlock()
	if leftover > 0 {
		logger.Warn(context.Background(), "sse_event_retry_shutdown_leftover",
			zap.Int("dropped", leftover))
	}
	metrics.RecordSSEEventRetryBufferDepth(0)
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
// non-flushing entry at the per-tenant cap and the globally oldest
// non-flushing entry at the global cap. Every eviction is counted per tenant
// as hard loss. After stop, enqueue does NOT buffer: the flush goroutine is
// gone, so the event would sit forever — it is counted as dropped (reason
// "stopped") instead.
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

// flushDue drops expired non-flushing entries, groups due work by tenant, and
// spawns one flusher per newly-active tenant while a global slot is free.
// Tenants that lose the slot race are picked up by the next scan.
func (b *eventRetryBuffer) flushDue(ctx context.Context) {
	now := time.Now()
	b.mu.Lock()
	kept := b.entries[:0]
	for _, e := range b.entries {
		if !e.flushing && now.Sub(e.enqueuedAt) > b.maxAge {
			b.removeForTenantLocked(e.tenantID)
			metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, "expired")
			continue
		}
		kept = append(kept, e)
	}
	b.entries = kept
	var tenants []string
	seen := make(map[string]bool)
	for _, e := range b.entries {
		if e.flushing || e.nextRetry.After(now) || seen[e.tenantID] || b.tenantActive[e.tenantID] {
			continue
		}
		seen[e.tenantID] = true
		tenants = append(tenants, e.tenantID)
	}
	for _, tenantID := range tenants {
		select {
		case b.flushSlots <- struct{}{}:
			b.tenantActive[tenantID] = true
			b.wg.Add(1)
			go b.flushTenant(ctx, tenantID)
		default:
			// All slots busy: the next scan picks this tenant up.
		}
	}
	depth := len(b.entries)
	b.mu.Unlock()
	metrics.RecordSSEEventRetryBufferDepth(depth)
}

// flushTenant drains one tenant's due entries oldest-first (FIFO within the
// tenant) until none remain due, then releases the tenant's active flag and
// the global slot. It is the ONLY writer of the tenant's entries' flushing
// flags, and it never runs concurrently with itself (tenantActive gate).
func (b *eventRetryBuffer) flushTenant(ctx context.Context, tenantID string) {
	defer b.wg.Done()
	defer func() { <-b.flushSlots }()
	defer func() {
		b.mu.Lock()
		delete(b.tenantActive, tenantID)
		b.mu.Unlock()
	}()
	for {
		entry := b.takeDueForTenant(tenantID)
		if entry == nil {
			return
		}
		if ctx.Err() != nil {
			// Shutdown or scan interruption: hand the entry back untouched.
			b.mu.Lock()
			entry.flushing = false
			b.mu.Unlock()
			return
		}
		b.flushOne(ctx, entry)
	}
}

// takeDueForTenant returns the tenant's oldest DUE entry (enqueuedAt order,
// skipping in-flight entries) marked flushing, or nil. Expired entries found
// along the way are dropped (counted as hard loss).
func (b *eventRetryBuffer) takeDueForTenant(tenantID string) *eventRetryEntry {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range b.entries {
		if e.tenantID != tenantID || e.flushing {
			continue
		}
		if now.Sub(e.enqueuedAt) > b.maxAge {
			b.dropEntryLocked(e, "expired")
			continue
		}
		if e.nextRetry.After(now) {
			continue
		}
		e.flushing = true
		return e
	}
	return nil
}

// flushOutcome classifies one flush attempt.
type flushOutcome int

const (
	flushRetry    flushOutcome = iota // transient failure: backoff and retry later
	flushOK                           // durable insert + second wake done
	flushDropGone                     // tenant is gone: drop the entry (hard loss)
)

// flushOne attempts one entry and reconciles it in place: on success the
// entry is removed; on transient failure its backoff is updated in place
// (keeping its queue position — the queue stays enqueuedAt-ordered); on
// tenant-gone it is dropped with a distinct reason.
func (b *eventRetryBuffer) flushOne(ctx context.Context, e *eventRetryEntry) {
	outcome := b.flushEntry(ctx, e)
	now := time.Now() // fresh clock: the attempt may have taken up to 30s
	b.mu.Lock()
	defer b.mu.Unlock()
	e.flushing = false
	switch outcome {
	case flushOK:
		b.removeEntryLocked(e)
	case flushDropGone:
		b.dropEntryLocked(e, "tenant_gone")
	default:
		e.attempts++
		// The shift clamps at 9: 1s<<9 (512s) already exceeds the 5min cap,
		// so larger shifts change nothing (and stay far from any overflow).
		backoff := eventRetryBaseBackoff << min(e.attempts, 9)
		if backoff > eventRetryMaxBackoff {
			backoff = eventRetryMaxBackoff
		}
		e.nextRetry = now.Add(backoff)
		if now.Sub(e.enqueuedAt) > b.maxAge {
			b.dropEntryLocked(e, "expired")
		}
	}
}

// flushEntry performs the durable insert for one entry under the per-entry
// timeout. Store resolution: the bus's cached pointer first; when it is nil
// (tenant idle-closed) or the insert fails with a closed-DB error, the
// server resolver re-acquires the store once (refreshing the bus pointer as
// a side effect). On success it runs the mandatory second wake
// (insertTenantNotify + bus.Publish) and returns flushOK.
func (b *eventRetryBuffer) flushEntry(ctx context.Context, e *eventRetryEntry) flushOutcome {
	inserter := b.loadStore(e.bus)
	if inserter == nil {
		var err error
		if inserter, err = b.resolve(ctx, e); err != nil {
			if errors.Is(err, errTenantGone) {
				return flushDropGone
			}
			return flushRetry
		}
	}
	if inserter == nil {
		// Store not (yet) available: requeue with backoff.
		return flushRetry
	}
	insertCtx, cancel := context.WithTimeout(ctx, eventRetryFlushTimeout)
	defer cancel()
	seq, err := inserter.InsertFSEvent(insertCtx, e.path, e.op, e.actor, e.ts)
	if err != nil && isClosedDBError(err) && b.resolveStore != nil {
		// The cached store was idle-closed by the pool: re-resolve once and
		// retry with the fresh handle.
		if alt, rerr := b.resolve(ctx, e); rerr == nil && alt != nil {
			seq, err = alt.InsertFSEvent(insertCtx, e.path, e.op, e.actor, e.ts)
		}
	}
	if err != nil {
		metrics.RecordTenantOperationWithOrg(e.tenantID, e.orgID, "event_bus", "retry_flush", metrics.ResultForError(err), 0)
		logger.Warn(ctx, "sse_event_retry_flush_failed",
			zap.String("tenant_id", e.tenantID),
			zap.String("path", e.path),
			zap.String("op", e.op),
			zap.Int("attempts", e.attempts+1),
			zap.Error(err))
		return flushRetry
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
	return flushOK
}

// resolve calls the injected store resolver, tolerating a nil resolver.
func (b *eventRetryBuffer) resolve(ctx context.Context, e *eventRetryEntry) (fsEventInserter, error) {
	if b.resolveStore == nil {
		return nil, nil
	}
	return b.resolveStore(ctx, e.tenantID, e.bus)
}

// isClosedDBError reports whether err indicates an idle-closed *sql.DB
// (sql.ErrConnDone or the driver's "database is closed"), i.e. the cached
// store is dead but the tenant itself may be fine.
func isClosedDBError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, sql.ErrConnDone) || strings.Contains(err.Error(), "database is closed")
}

// dropOldestForTenantLocked removes the tenant's oldest NON-FLUSHING entry
// and counts the drop. Caller must hold b.mu.
func (b *eventRetryBuffer) dropOldestForTenantLocked(tenantID, reason string) {
	for i, e := range b.entries {
		if e.tenantID == tenantID && !e.flushing {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			b.removeForTenantLocked(tenantID)
			metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, reason)
			return
		}
	}
}

// dropOldestLocked removes the globally oldest NON-FLUSHING entry and counts
// the drop. Caller must hold b.mu.
func (b *eventRetryBuffer) dropOldestLocked(reason string) {
	for i, e := range b.entries {
		if !e.flushing {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			b.removeForTenantLocked(e.tenantID)
			metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, reason)
			return
		}
	}
}

// dropEntryLocked removes a specific entry and counts the drop. Caller must
// hold b.mu.
func (b *eventRetryBuffer) dropEntryLocked(target *eventRetryEntry, reason string) {
	for i, e := range b.entries {
		if e == target {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			b.removeForTenantLocked(e.tenantID)
			metrics.RecordSSEEventRetryDropped(e.tenantID, e.orgID, reason)
			return
		}
	}
}

// removeEntryLocked removes a specific entry WITHOUT counting a drop (the
// entry landed durably). Caller must hold b.mu.
func (b *eventRetryBuffer) removeEntryLocked(target *eventRetryEntry) {
	for i, e := range b.entries {
		if e == target {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			b.removeForTenantLocked(e.tenantID)
			return
		}
	}
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

// resolveRetryStore is the production store resolver for the retry buffer:
// it re-acquires the tenant's backend through the server's normal path (the
// same pool.Acquire the request path uses) and refreshes the bus's cached
// store pointer as a side effect. Single-tenant mode resolves to the
// fallback store. A tenant that no longer exists (or is no longer active)
// yields errTenantGone so its buffered events drop instead of spinning.
func (s *Server) resolveRetryStore(ctx context.Context, tenantID string, bus *EventBus) (fsEventInserter, error) {
	if s.fallback != nil {
		return s.fallback.Store(), nil
	}
	if s.meta == nil || s.pool == nil || tenantID == "" {
		return nil, nil
	}
	t, err := s.meta.GetTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", errTenantGone, tenantID)
		}
		return nil, fmt.Errorf("get tenant %s: %w", tenantID, err)
	}
	if t.Status != meta.TenantActive {
		return nil, fmt.Errorf("%w: %s (status %s)", errTenantGone, tenantID, t.Status)
	}
	backend, release, err := s.pool.Acquire(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("acquire tenant backend %s: %w", tenantID, err)
	}
	defer release()
	store := backend.Store()
	if store == nil {
		return nil, nil
	}
	if bus != nil {
		// Refresh the cached pointer as a side effect so subsequent flushes
		// (and the live SSE path) use the fresh handle.
		bus.SetStore(store)
	}
	return store, nil
}
