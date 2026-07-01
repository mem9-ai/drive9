package server

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/meta"
)

// semanticOutboxPoller is a per-pod goroutine that reads the central
// semantic_notify_outbox table (in the always-provisioned meta DB) to discover
// which tenants have pending semantic tasks. For each outbox row belonging to
// this pod's shard, it kicks the semantic worker so the worker opens the
// tenant's TiDB and claims the task. Rows for tenants owned by other pods are
// silently skipped — the tenant's TiDB is never touched, allowing it to
// scale to zero.
//
// Unlike the SSE notify poller (which initializes its cursor to MAX(id) on
// every restart, relying on SSE reconnect replay as a fallback), the semantic
// outbox poller persists its cursor in semantic_outbox_cursor so a restart
// resumes from the last processed row. This is necessary because semantic
// tasks have no replay path — a skipped outbox row means a task stays stuck if
// the tenant does not write again.
type semanticOutboxPoller struct {
	metaStore *meta.Store
	worker    *semanticWorkerManager
	shardFn   func(tenantID string) bool
	podID     string

	interval          time.Duration
	cursorFlushEvery time.Duration

	mu        sync.Mutex
	lastID    uint64
	lastFlush time.Time
}

const (
	defaultSemanticOutboxPollInterval  = 200 * time.Millisecond
	semanticOutboxBatchSize             = 1000
	defaultSemanticOutboxCursorFlush    = 5 * time.Second
)

// newSemanticOutboxPoller creates a semantic outbox poller. metaStore must be
// non-nil in multi-tenant mode. shardFn determines whether a tenant belongs
// to this pod. podID is used to persist the cursor.
func newSemanticOutboxPoller(
	metaStore *meta.Store,
	worker *semanticWorkerManager,
	shardFn func(string) bool,
	podID string,
	interval time.Duration,
	cursorFlushEvery time.Duration,
) *semanticOutboxPoller {
	if interval <= 0 {
		interval = defaultSemanticOutboxPollInterval
	}
	if cursorFlushEvery <= 0 {
		cursorFlushEvery = defaultSemanticOutboxCursorFlush
	}
	return &semanticOutboxPoller{
		metaStore:         metaStore,
		worker:            worker,
		shardFn:           shardFn,
		podID:             podID,
		interval:          interval,
		cursorFlushEvery:  cursorFlushEvery,
	}
}

// initCursor synchronously initializes the poller's lastID. On restart it
// recovers from semantic_outbox_cursor; on first launch (no cursor row) it
// skips to MAX(id) — safe because the pod has never owned work before. This
// MUST be called before run() starts and before the server accepts live
// traffic.
func (p *semanticOutboxPoller) initCursor(ctx context.Context) {
	if p == nil || p.metaStore == nil {
		return
	}
	lastID, err := p.metaStore.GetSemanticOutboxCursor(ctx, p.podID)
	if err != nil {
		logger.Warn(ctx, "semantic_outbox_poller_cursor_load_failed",
			zap.String("pod_id", p.podID), zap.Error(err))
		// Fall back to MAX(id) — better to skip historical rows than block.
		lastID = 0
	}
	if lastID == 0 {
		// First launch: skip historical rows. This pod has never owned work,
		// so historical signals belong to other (previous) pods.
		maxID, err := p.metaStore.MaxSemanticNotifyID(ctx)
		if err != nil {
			logger.Warn(ctx, "semantic_outbox_poller_max_id_failed", zap.Error(err))
			maxID = 0
		}
		lastID = maxID
	}
	p.mu.Lock()
	p.lastID = lastID
	p.lastFlush = time.Now()
	p.mu.Unlock()
	logger.Info(ctx, "semantic_outbox_poller_cursor_initialized",
		zap.Uint64("cursor", lastID))
}

// run starts the poller goroutine. It blocks until ctx is cancelled. The
// cursor must already be initialized by initCursor before run is called.
func (p *semanticOutboxPoller) run(ctx context.Context) {
	if p == nil || p.metaStore == nil {
		return
	}
	logger.Info(ctx, "semantic_outbox_poller_started",
		zap.Duration("interval", p.interval),
		zap.Duration("cursor_flush", p.cursorFlushEvery))

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Best-effort final cursor flush on shutdown.
			p.flushCursor(context.Background())
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

// pollOnce reads new outbox rows and kicks the worker for matching tenants.
// If a full batch is returned, it immediately reads the next batch (drain
// mode) so a burst of enqueues is processed without waiting for the next tick.
func (p *semanticOutboxPoller) pollOnce(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		p.mu.Lock()
		afterID := p.lastID
		p.mu.Unlock()

		rows, err := p.metaStore.ListSemanticNotifySince(ctx, afterID, semanticOutboxBatchSize)
		if err != nil {
			logger.Warn(ctx, "semantic_outbox_poller_list_failed",
				zap.Uint64("after_id", afterID), zap.Error(err))
			metrics.RecordEventBusPollFailure("semantic_outbox_poller")
			return
		}
		for _, row := range rows {
			// Advance cursor for every row, including those owned by other pods,
			// so they are never re-read.
			p.mu.Lock()
			p.lastID = row.ID
			p.mu.Unlock()
			// Only kick the worker for tenants this pod owns. Other pods'
			// pollers will do the same and skip rows they don't own.
			if p.shardFn != nil && p.shardFn(row.TenantID) {
				p.worker.Kick(row.TenantID)
			}
		}
		// Maybe flush cursor.
		p.maybeFlushCursor(ctx)

		if len(rows) < semanticOutboxBatchSize {
			return
		}
	}
}

// maybeFlushCursor persists the cursor to semantic_outbox_cursor at most once
// per cursorFlushEvery interval. This reduces meta DB write QPS from
// 1/tick (5 QPS at 200ms) to 1/flushInterval (0.2 QPS at 5s). On crash, at
// most cursorFlushEvery of rows are replayed — duplicate kicks are harmless.
func (p *semanticOutboxPoller) maybeFlushCursor(ctx context.Context) {
	if p == nil || p.metaStore == nil {
		return
	}
	p.mu.Lock()
	now := time.Now()
	if now.Sub(p.lastFlush) < p.cursorFlushEvery {
		p.mu.Unlock()
		return
	}
	p.lastFlush = now
	lastID := p.lastID
	p.mu.Unlock()

	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.metaStore.UpsertSemanticOutboxCursor(flushCtx, p.podID, lastID); err != nil {
		logger.Warn(ctx, "semantic_outbox_poller_cursor_flush_failed",
			zap.Uint64("last_id", lastID), zap.Error(err))
	}
}

// flushCursor persists the cursor unconditionally. Used on shutdown.
func (p *semanticOutboxPoller) flushCursor(ctx context.Context) {
	if p == nil || p.metaStore == nil {
		return
	}
	p.mu.Lock()
	lastID := p.lastID
	p.mu.Unlock()
	flushCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := p.metaStore.UpsertSemanticOutboxCursor(flushCtx, p.podID, lastID); err != nil {
		logger.Warn(ctx, "semantic_outbox_poller_cursor_flush_shutdown_failed",
			zap.Uint64("last_id", lastID), zap.Error(err))
	}
}