package server

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/meta"
)

// notifyPoller is a single per-pod goroutine that reads the central
// sse_notify_outbox table (in the always-provisioned meta DB) to discover
// which tenants have new fs_events rows written by other pods. For each
// notification row, it looks up the local EventBus (if one exists with active
// subscribers) and calls Publish() to wake SSE handlers. If no local bus
// exists for the tenant, the notification is silently skipped — the tenant's
// TiDB is never queried.
//
// This replaces the old per-bus pollLoop (one 1s goroutine per tenant) with a
// single 200ms goroutine targeting the central meta DB. With ~100k tenants the
// old scheme generated ~100k QPS against 100k separate serverless TiDBs,
// preventing scale-to-zero. The new scheme generates 1 QPS per pod against a
// single always-provisioned meta DB, and idle tenant TiDBs can sleep.
//
// The poller is the fallback path. The primary cross-pod path is the podNotifier
// (direct HTTP push, <10ms). If the push is lost or the notifier is disabled,
// the poller catches the notification within its tick interval (200ms default).
type notifyPoller struct {
	metaStore *meta.Store
	buses     *eventBuses
	interval  time.Duration
	lastID    uint64
}

const (
	// defaultNotifyPollInterval is the fallback poller's tick interval. At
	// 200ms, cross-pod delivery via the fallback path is bounded to ~200ms
	// (vs ~10ms via the push path). The target is the always-provisioned meta
	// DB, so 5 QPS per pod is negligible.
	defaultNotifyPollInterval = 200 * time.Millisecond
	// notifyPollBatchSize is the max rows read per ListSSENotifySince call.
	// If a batch is full, the poller immediately reads the next batch without
	// waiting for the next tick, so a burst of writes is drained quickly.
	notifyPollBatchSize = 1000
)

// newNotifyPoller creates a notifyPoller. metaStore must be non-nil. The
// poller initializes its cursor to the current max outbox id so it skips
// historical rows (SSE client reconnect replay covers those).
func newNotifyPoller(metaStore *meta.Store, buses *eventBuses, interval time.Duration) *notifyPoller {
	if interval <= 0 {
		interval = defaultNotifyPollInterval
	}
	return &notifyPoller{
		metaStore: metaStore,
		buses:     buses,
		interval:  interval,
	}
}

// initCursor synchronously initializes the poller's lastID to the current max
// outbox id. This MUST be called before run() starts and before the server
// accepts live traffic, so that a write between poller construction and the
// first poll tick is not skipped. If the init query fails, lastID stays 0
// (reads some historical rows but Publish is a no-op for tenants without
// subscribers — not harmful).
func (np *notifyPoller) initCursor(ctx context.Context) {
	maxID, err := np.metaStore.MaxSSENotifyID(ctx)
	if err != nil {
		logger.Warn(ctx, "notify_poller_init_cursor_failed",
			zap.Error(err))
		maxID = 0
	}
	np.lastID = maxID
	logger.Info(ctx, "notify_poller_cursor_initialized",
		zap.Uint64("cursor", np.lastID))
}

// run starts the poller goroutine. It blocks until ctx is cancelled.
// The cursor (lastID) must already be initialized by initCursor before run
// is called, so that no outbox rows are skipped between construction and
// the first poll tick.
func (np *notifyPoller) run(ctx context.Context) {
	logger.Info(ctx, "notify_poller_started",
		zap.Duration("interval", np.interval),
		zap.Uint64("cursor", np.lastID))

	ticker := time.NewTicker(np.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			np.pollOnce(ctx)
		}
	}
}

// pollOnce reads new outbox rows and wakes matching local buses. If a full
// batch is returned, it immediately reads the next batch (drain mode) so a
// burst of events is processed without waiting for the next tick.
func (np *notifyPoller) pollOnce(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		rows, err := np.metaStore.ListSSENotifySince(ctx, np.lastID, notifyPollBatchSize)
		if err != nil {
			logger.Warn(ctx, "notify_poller_list_failed",
				zap.Uint64("after_id", np.lastID),
				zap.Error(err))
			metrics.RecordEventBusPollFailure("notify_poller")
			return
		}
		for _, row := range rows {
			// Only wake buses that exist (have local SSE subscribers). If no
			// bus exists for this tenant, skip — we never touch the tenant's
			// TiDB, allowing it to scale to zero.
			if bus := np.buses.getIfExists(row.TenantID); bus != nil {
				bus.Publish()
			}
			np.lastID = row.ID
		}
		// If we got fewer rows than the batch size, we're caught up — wait
		// for the next tick. Otherwise drain the remaining rows immediately.
		if len(rows) < notifyPollBatchSize {
			return
		}
	}
}