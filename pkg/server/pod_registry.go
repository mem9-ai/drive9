package server

import (
	"context"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"go.uber.org/zap"
)

// podRegistry manages this pod's presence in the central pod_registry and
// pod_subscriptions tables. It runs two background loops:
//
//  1. heartbeatLoop: upserts this pod's row every ~10s so the leader's stale-pod
//     sweeper knows the pod is alive.
//
//  2. subscriptionLoop: periodically reports the set of tenant IDs for which
//     this pod has active SSE subscribers, and prunes tenants that are no
//     longer active. Writers consult this table (via the podNotifier's route
//     cache) to push notifications only to pods that care about each tenant.
//
// The leader additionally runs stalePodSweepLoop to mark pods with expired
// heartbeats as stale and clean up their subscription rows.
//
// All goroutines are tracked by wg so shutdown (Stop) waits for in-flight DB
// calls to complete before returning, preventing use-after-close races on the
// shared meta store.
type podRegistry struct {
	metaStore *meta.Store
	podID     string
	addr      string
	buses     *eventBuses

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

const (
	// podHeartbeatInterval is how often this pod refreshes its pod_registry row.
	podHeartbeatInterval = 10 * time.Second
	// podSubscriptionRefreshInterval is how often this pod reports its SSE
	// subscriber tenant set to pod_subscriptions.
	podSubscriptionRefreshInterval = 5 * time.Second
	// podStaleThreshold is the age after which a pod's heartbeat is considered
	// stale. The leader marks such pods as inactive so writers stop pushing to
	// them. 3× the heartbeat interval gives tolerance for brief GC pauses.
	podStaleThreshold = 30 * time.Second
	// stalePodSweepInterval is how often the leader marks stale pods and
	// cleans up their subscription rows.
	stalePodSweepInterval = 30 * time.Second
)

// newPodRegistry creates a podRegistry. podID is the unique pod identifier;
// addr is the internally reachable address (host:port) for peer push.
func newPodRegistry(metaStore *meta.Store, podID, addr string, buses *eventBuses) *podRegistry {
	return &podRegistry{
		metaStore: metaStore,
		podID:     podID,
		addr:      addr,
		buses:     buses,
	}
}

// Start launches the subscription-reporting and heartbeat goroutines. All
// goroutines are tracked by wg so Stop can wait for them. Runs on every pod
// (not leader-gated). Blocks until ctx is cancelled (via Stop).
func (pr *podRegistry) Start(ctx context.Context) {
	pr.ctx, pr.cancel = context.WithCancel(ctx)
	pr.wg.Add(2)
	go func() {
		defer pr.wg.Done()
		pr.subscriptionLoop(pr.ctx)
	}()
	if pr.addr != "" {
		go func() {
			defer pr.wg.Done()
			pr.heartbeatLoop(pr.ctx)
		}()
	} else {
		// addr is empty — no heartbeat. Still need to decrement wg for balance.
		pr.wg.Done()
	}
}

// RegisterBeforeStart performs the initial synchronous heartbeat so this pod
// appears in pod_registry immediately (before any background goroutine). This
// lets callers (and tests) observe the pod in ListActivePods without waiting
// for the first ticker. Returns an error if the registration fails so callers
// can decide whether to retry or fail. Skip if addr is empty.
func (pr *podRegistry) RegisterBeforeStart(ctx context.Context) error {
	if pr.addr == "" {
		return nil
	}
	if err := pr.metaStore.UpsertPod(ctx, pr.podID, pr.addr); err != nil {
		logger.Warn(ctx, "pod_registry_initial_heartbeat_failed",
			zap.String("pod_id", pr.podID),
			zap.Error(err))
		return err
	}
	return nil
}

// Stop cancels the context and waits for all registry goroutines (heartbeat,
// subscription) to exit. This ensures in-flight DB calls complete before the
// caller tears down the meta store.
func (pr *podRegistry) Stop() {
	if pr.cancel != nil {
		pr.cancel()
	}
	pr.wg.Wait()
}

// heartbeatLoop upserts this pod's row in pod_registry at a fixed interval.
// Skips if addr is empty (pod has no reachable address yet).
func (pr *podRegistry) heartbeatLoop(ctx context.Context) {
	if pr.addr == "" {
		// No address to register — don't poll. The subscription loop still
		// runs so this pod reports its subscriber set.
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(podHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := pr.metaStore.UpsertPod(ctx, pr.podID, pr.addr); err != nil {
				logger.Warn(ctx, "pod_registry_heartbeat_failed",
					zap.String("pod_id", pr.podID),
					zap.Error(err))
			}
		}
	}
}

// subscriptionLoop periodically reports this pod's active SSE subscriber
// tenant set to pod_subscriptions and prunes tenants that no longer have
// subscribers. This lets the podNotifier build an accurate tenant→peer route
// table for push targeting.
func (pr *podRegistry) subscriptionLoop(ctx context.Context) {
	ticker := time.NewTicker(podSubscriptionRefreshInterval)
	defer ticker.Stop()
	pr.reportSubscriptions(ctx) // initial report
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pr.reportSubscriptions(ctx)
		}
	}
}

// reportSubscriptions upserts the current active tenant set and prunes stale
// entries. Best-effort: errors are logged and retried on the next tick.
func (pr *podRegistry) reportSubscriptions(ctx context.Context) {
	tenantIDs := pr.buses.activeTenantIDs()
	if err := pr.metaStore.UpsertPodSubscriptions(ctx, pr.podID, tenantIDs); err != nil {
		logger.Warn(ctx, "pod_registry_upsert_subscriptions_failed",
			zap.String("pod_id", pr.podID),
			zap.Int("tenant_count", len(tenantIDs)),
			zap.Error(err))
		return
	}
	n, err := pr.metaStore.PrunePodSubscriptions(ctx, pr.podID, tenantIDs)
	if err != nil {
		logger.Warn(ctx, "pod_registry_prune_subscriptions_failed",
			zap.String("pod_id", pr.podID),
			zap.Error(err))
		return
	}
	if n > 0 {
		logger.Debug(ctx, "pod_registry_pruned_subscriptions",
			zap.String("pod_id", pr.podID),
			zap.Int64("pruned", n))
	}
}

// SweepStalePods marks pods with expired heartbeats as stale and atomically
// cleans up subscription rows for currently-stale pods (via a conditional
// delete join, avoiding a TOCTOU race where a pod recovers between marking and
// deleting). Leader-gated: only the leader runs this to avoid concurrent sweeps.
// Best-effort: errors are logged and retried next sweep.
func (pr *podRegistry) SweepStalePods(ctx context.Context) {
	before := time.Now().Add(-podStaleThreshold)
	n, err := pr.metaStore.MarkStalePods(ctx, before)
	if err != nil {
		logger.Warn(ctx, "pod_registry_mark_stale_failed", zap.Error(err))
		return
	}
	if n > 0 {
		logger.Info(ctx, "pod_registry_marked_stale",
			zap.Int64("stale_pods", n))
	}
	// Atomically delete subscriptions for pods that are currently stale.
	// The conditional join ensures we only delete for pods that are still
	// stale at delete time — a pod that recovered between MarkStalePods and
	// this call retains its subscriptions.
	deleted, err := pr.metaStore.DeleteSubscriptionsForStalePods(ctx)
	if err != nil {
		logger.Warn(ctx, "pod_registry_delete_stale_subscriptions_failed", zap.Error(err))
		return
	}
	if deleted > 0 {
		logger.Info(ctx, "pod_registry_deleted_stale_subscriptions",
			zap.Int64("deleted_rows", deleted))
	}
	// Clean up outbox cursor rows for stale pods. Without this, a dead pod's
	// stale cursor holds back outbox pruning (DeleteTenantNotifyBefore uses
	// MIN(last_id) across all cursors). Best-effort: errors are logged and
	// retried next sweep.
	stalePods, err := pr.metaStore.ListStalePods(ctx)
	if err != nil {
		logger.Warn(ctx, "pod_registry_list_stale_pods_failed", zap.Error(err))
		return
	}
	for _, podID := range stalePods {
		if err := pr.metaStore.DeleteTenantOutboxCursor(ctx, podID); err != nil {
			logger.Warn(ctx, "pod_registry_delete_stale_cursor_failed",
				zap.String("pod_id", podID), zap.Error(err))
			continue
		}
	}
	if len(stalePods) > 0 {
		logger.Info(ctx, "pod_registry_deleted_stale_cursors",
			zap.Int("count", len(stalePods)))
	}
}
