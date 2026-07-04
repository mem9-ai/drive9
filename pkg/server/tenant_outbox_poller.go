package server

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

// outboxKicker is the interface the poller uses to dispatch sharded work
// (semantic/file_gc/quota) to the unified worker. The worker deduplicates
// kicks by tenant and OR-accumulates the work_mask.
type outboxKicker interface {
	Kick(tenantID string, workMask int)
}

const (
	// defaultTenantOutboxPollInterval is the poller's tick interval. At 200ms,
	// cross-pod work delivery is bounded to ~200ms. The target is the
	// always-provisioned meta DB, so 5 QPS per pod is negligible.
	defaultTenantOutboxPollInterval = 200 * time.Millisecond
	// tenantOutboxBatchSize is the max rows read per ListTenantNotifySince call.
	// If a batch is full, the poller immediately reads the next batch (drain
	// mode) so a burst of writes is drained quickly.
	tenantOutboxBatchSize = 1000
	// defaultTenantOutboxCursorFlushInterval is how often the poller persists
	// its in-memory cursor to tenant_outbox_cursor. At 5s, a restart loses at
	// most 5s of work signals — the safety-net scan recovers any expired
	// leases that were missed.
	defaultTenantOutboxCursorFlushInterval = 5 * time.Second
)

// tenantOutboxPoller is the single per-pod goroutine that reads the unified
// tenant_notify_outbox table (in the always-provisioned meta DB) and
// dispatches work by work_mask:
//   - SSE bit → wake the local EventBus (broadcast: all pods with subscribers)
//   - Semantic/FileGC/Quota bits → kick the unified worker if this pod's shard
//     resolver owns the tenant (sharded: shard owner only)
//
// This replaces notify_poller.go (SSE-only) and eliminates all periodic
// per-tenant TiDB scanning. Idle tenant TiDBs can scale to zero because the
// poller never touches them — it only reads the central meta DB.
type tenantOutboxPoller struct {
	metaStore        *meta.Store
	buses            *eventBuses
	worker           outboxKicker
	shardFn          func(string) bool
	podID            string
	interval         time.Duration
	cursorFlushEvery time.Duration

	lastID uint64
}

// newTenantOutboxPoller creates a tenantOutboxPoller. metaStore must be non-nil.
// shardFn returns true when this pod owns sharded work for the tenant. When
// shardFn is nil, this pod owns all sharded work (single-pod mode).
func newTenantOutboxPoller(
	metaStore *meta.Store,
	buses *eventBuses,
	worker outboxKicker,
	shardFn func(string) bool,
	podID string,
	interval time.Duration,
	cursorFlushEvery time.Duration,
) *tenantOutboxPoller {
	if interval <= 0 {
		interval = defaultTenantOutboxPollInterval
	}
	if cursorFlushEvery <= 0 {
		cursorFlushEvery = defaultTenantOutboxCursorFlushInterval
	}
	if shardFn == nil {
		shardFn = func(string) bool { return true }
	}
	return &tenantOutboxPoller{
		metaStore:        metaStore,
		buses:            buses,
		worker:           worker,
		shardFn:          shardFn,
		podID:            podID,
		interval:         interval,
		cursorFlushEvery: cursorFlushEvery,
	}
}

// initCursor synchronously initializes the poller's lastID before run starts
// and before the server accepts live traffic. On restart, the cursor is
// recovered from tenant_outbox_cursor so no work is skipped. On first launch
// (no cursor row), the cursor is set to MAX(id) so historical rows are skipped
// — the pod never owned work before its first start.
func (p *tenantOutboxPoller) initCursor(ctx context.Context) {
	if p.podID != "" {
		cursor, err := p.metaStore.GetTenantOutboxCursor(ctx, p.podID)
		if err == nil && cursor != nil {
			p.lastID = cursor.LastID
			logger.Info(ctx, "tenant_outbox_poller_cursor_recovered",
				zap.String("pod_id", p.podID),
				zap.Uint64("cursor", p.lastID))
			return
		}
		if errors.Is(err, meta.ErrNotFound) {
			// First launch — no prior cursor. Fall through to MAX(id) below.
		} else if err != nil {
			// Non-ErrNotFound error: do NOT fall through to MAX(id), which would
			// skip historical rows. Start from 0 instead so all unpruned rows are
			// re-read; duplicate kicks are harmless (work is idempotent/deduped).
			logger.Warn(ctx, "tenant_outbox_poller_cursor_read_failed",
				zap.String("pod_id", p.podID),
				zap.Error(err))
			p.lastID = 0
			logger.Info(ctx, "tenant_outbox_poller_cursor_initialized",
				zap.Uint64("cursor", p.lastID))
			return
		}
	}
	maxID, err := p.metaStore.MaxTenantNotifyID(ctx)
	if err != nil {
		logger.Warn(ctx, "tenant_outbox_poller_init_cursor_failed", zap.Error(err))
		maxID = 0
	}
	p.lastID = maxID
	logger.Info(ctx, "tenant_outbox_poller_cursor_initialized",
		zap.Uint64("cursor", p.lastID))
}

// run starts the poller goroutine. It blocks until ctx is cancelled. The cursor
// (lastID) must already be initialized by initCursor before run is called.
func (p *tenantOutboxPoller) run(ctx context.Context) {
	logger.Info(ctx, "tenant_outbox_poller_started",
		zap.Duration("interval", p.interval),
		zap.Uint64("cursor", p.lastID),
		zap.Duration("cursor_flush_every", p.cursorFlushEvery))

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	flushTicker := time.NewTicker(p.cursorFlushEvery)
	defer flushTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Flush cursor on shutdown using a fresh non-cancelled context so
			// the final cursor position is persisted. Using the already-cancelled
			// ctx would cause UpsertTenantOutboxCursor to fail with
			// context.Canceled and the cursor would be lost — a pod that shuts
			// down before its first periodic flush would restart at MAX(id),
			// skipping unprocessed rows.
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			p.flushCursor(shutdownCtx)
			shutdownCancel()
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		case <-flushTicker.C:
			p.flushCursor(ctx)
		}
	}
}

// pollOnce reads new outbox rows and dispatches by work_mask. If a full batch
// is returned, it immediately reads the next batch (drain mode) so a burst of
// events is processed without waiting for the next tick.
func (p *tenantOutboxPoller) pollOnce(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		rows, err := p.metaStore.ListTenantNotifySince(ctx, p.lastID, tenantOutboxBatchSize)
		if err != nil {
			logger.Warn(ctx, "tenant_outbox_poller_list_failed",
				zap.Uint64("after_id", p.lastID),
				zap.Error(err))
			// List failure: the meta-DB read that drives kick dispatch failed.
			// Record as a user_db_access path-level signal (the poller itself
			// only reads the meta DB, but a list failure delays all downstream
			// tenant-DB work). Avoids the previous abuse of putting the pod
			// name in the tenant_id label of drive9_event_bus_poll_failures_total.
			metrics.RecordOperation("user_db_access", "outbox_poll_list", "error", 0)
			return
		}
		for _, row := range rows {
			p.dispatch(ctx, row)
			p.lastID = row.ID
		}
		// If we got fewer rows than the batch size, we're caught up — wait for
		// the next tick. Otherwise drain the remaining rows immediately.
		if len(rows) < tenantOutboxBatchSize {
			return
		}
	}
}

// dispatch sends a single outbox row to the right consumer based on work_mask.
func (p *tenantOutboxPoller) dispatch(ctx context.Context, row meta.TenantNotifyRow) {
	if row.WorkMask&WorkSSE != 0 {
		// SSE is broadcast: wake any local bus with subscribers for this tenant.
		// If no bus exists, skip — we never touch the tenant's TiDB.
		if bus := p.buses.getIfExists(row.TenantID); bus != nil {
			bus.Publish()
		}
	}
	shardedMask := row.WorkMask & (WorkSemantic | WorkFileGC)
	if shardedMask != 0 && p.worker != nil {
		// Sharded work: only the shard owner processes it. Other pods skip;
		// the safety-net scan recovers any work whose kick was lost.
		if p.shardFn(row.TenantID) {
			p.worker.Kick(row.TenantID, shardedMask)
			// Record the kick that will trigger a tenant-DB access via the
			// worker. This gives a baseline rate of "kicks dispatched" to
			// compare against actual tenant_worker_acquire — a divergence
			// (kicks without acquires) points to lost work.
			metrics.RecordTenantOperationCount(row.TenantID, "user_db_access", "outbox_dispatch_kick", "ok")
		}
	}
}

// flushCursor persists the current in-memory cursor to tenant_outbox_cursor
// so a restart resumes from the last processed row. Best-effort: errors are
// logged and the in-memory cursor remains valid.
func (p *tenantOutboxPoller) flushCursor(ctx context.Context) {
	if p.podID == "" {
		return
	}
	if err := p.metaStore.UpsertTenantOutboxCursor(ctx, p.podID, p.lastID); err != nil {
		if ctx.Err() == nil {
			logger.Warn(ctx, "tenant_outbox_poller_cursor_flush_failed",
				zap.String("pod_id", p.podID),
				zap.Uint64("cursor", p.lastID),
				zap.Error(err))
		}
	}
}
