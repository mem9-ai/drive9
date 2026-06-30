package server

import (
	"context"
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
type podRegistry struct {
	metaStore *meta.Store
	podID     string
	addr      string
	buses     *eventBuses
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

// Start launches the heartbeat and subscription-reporting goroutines. Both
// run on every pod (not leader-gated). They block until ctx is cancelled.
func (pr *podRegistry) Start(ctx context.Context) {
	// Initial heartbeat so this pod appears in pod_registry immediately.
	if err := pr.metaStore.UpsertPod(ctx, pr.podID, pr.addr); err != nil {
		logger.Warn(ctx, "pod_registry_initial_heartbeat_failed",
			zap.String("pod_id", pr.podID),
			zap.Error(err))
	}
	go pr.heartbeatLoop(ctx)
	go pr.subscriptionLoop(ctx)
}

// heartbeatLoop upserts this pod's row in pod_registry at a fixed interval.
func (pr *podRegistry) heartbeatLoop(ctx context.Context) {
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

// SweepStalePods marks pods with expired heartbeats as stale and cleans up
// their subscription rows. Leader-gated: only the leader runs this to avoid
// concurrent sweeps. Best-effort: errors are logged and retried next sweep.
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
	// Clean up subscription rows for stale pods so writers stop routing pushes
	// to dead pods.
	stalePods, err := pr.metaStore.ListStalePods(ctx)
	if err != nil {
		logger.Warn(ctx, "pod_registry_list_stale_failed", zap.Error(err))
		return
	}
	for _, podID := range stalePods {
		if ctx.Err() != nil {
			return
		}
		if err := pr.metaStore.DeletePodSubscriptions(ctx, podID); err != nil {
			logger.Warn(ctx, "pod_registry_delete_stale_subscriptions_failed",
				zap.String("pod_id", podID),
				zap.Error(err))
		}
	}
}