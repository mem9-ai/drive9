package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"go.uber.org/zap"
)

// podRouteTable is a snapshot of the pod-to-pod push routing table. It maps
// tenant IDs to the set of peer pod addresses that have SSE subscribers for
// that tenant. The notifier refreshes this from the central pod_subscriptions
// table periodically so writers can target push notifications precisely.
type podRouteTable struct {
	// tenantToPods maps tenant_id → list of peer pod base URLs that have
	// subscribers for that tenant. A tenant with no subscribers in any peer
	// is absent from the map; pushing to peers that don't care is wasteful.
	tenantToPods map[string][]string
	// allPeers is the full list of active peer addresses. Used for logging.
	allPeers []string
}

// podNotifier sends cross-pod SSE push notifications via HTTP. When a local
// FS mutation writes an event, the notifier asynchronously POSTs a lightweight
// {tenant_id, seq} body to each peer pod that has SSE subscribers for that
// tenant. The push is fire-and-forget (no retry, no response waiting) — the
// central outbox + notifyPoller is the durable fallback that catches any lost
// pushes within 200ms.
//
// This provides <10ms cross-pod latency in the normal case while keeping the
// cost model unchanged: idle tenant TiDBs are never queried (push is
// demand-driven; the receiving pod only calls Publish() which wakes local SSE
// handlers, which then call EventsSince against the tenant TiDB only if the
// tenant actually has new events).
type podNotifier struct {
	metaStore *meta.Store
	client    *http.Client
	selfID    string
	secret    []byte

	// routeCache is the current podRouteTable. Refreshed by refreshLoop.
	routeCache atomic.Pointer[podRouteTable]

	// Lifecycle: Start spawns the refresh goroutine; Stop cancels it.
	refreshCtx    context.Context
	refreshCancel context.CancelFunc
	wg            sync.WaitGroup

	// routeRefreshInterval is how often the route table is refreshed from
	// the central pod_subscriptions table. Stale routes are harmless: an
	// erroneous push to a pod with no subscribers is a no-op (getIfExists
	// returns nil); a missed push is caught by the 200ms notifyPoller.
	routeRefreshInterval time.Duration

	// pushTimeout is the per-HTTP-request timeout. Fire-and-forget: we don't
	// retry on failure, but we don't want to block goroutines indefinitely on
	// a dead peer.
	pushTimeout time.Duration
}

const (
	// defaultRouteRefreshInterval is the route table refresh cadence. At 5s,
	// a newly subscribed tenant's pod becomes visible to writers within 5s.
	// Before that, pushes for the new subscriber are missed but the 200ms
	// notifyPoller catches them.
	defaultRouteRefreshInterval = 5 * time.Second
	// defaultPushTimeout bounds each fire-and-forget HTTP POST so a dead peer
	// doesn't leak goroutines.
	defaultPushTimeout = 2 * time.Second
)

// newPodNotifier creates a podNotifier. selfID is this pod's identifier;
// secret is the shared bearer token for internal endpoint auth.
func newPodNotifier(metaStore *meta.Store, selfID string, secret []byte) *podNotifier {
	return &podNotifier{
		metaStore:            metaStore,
		client:               &http.Client{Timeout: defaultPushTimeout},
		selfID:               selfID,
		secret:               secret,
		routeRefreshInterval: defaultRouteRefreshInterval,
		pushTimeout:          defaultPushTimeout,
	}
}

// Start launches the route table refresh goroutine.
func (pn *podNotifier) Start(ctx context.Context) {
	pn.refreshCtx, pn.refreshCancel = context.WithCancel(ctx)
	pn.wg.Add(1)
	go func() {
		defer pn.wg.Done()
		pn.refreshLoop(pn.refreshCtx)
	}()
}

// Stop cancels the refresh goroutine and waits for it to exit.
func (pn *podNotifier) Stop() {
	if pn.refreshCancel != nil {
		pn.refreshCancel()
	}
	pn.wg.Wait()
}

// refreshLoop periodically reads the central pod_registry and
// pod_subscriptions tables to build a fresh route table. The table is stored
// in routeCache as an atomic pointer so Notify reads it lock-free.
func (pn *podNotifier) refreshLoop(ctx context.Context) {
	// Initial refresh before the first tick so Notify has a route table
	// immediately on startup.
	pn.refresh(ctx)
	ticker := time.NewTicker(pn.routeRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pn.refresh(ctx)
		}
	}
}

// refresh reads pod_registry (active peers) and pod_subscriptions (which
// tenants each pod cares about) from the central meta DB and builds a
// tenant→peer-address reverse index. The result is published atomically.
func (pn *podNotifier) refresh(ctx context.Context) {
	pods, err := pn.metaStore.ListActivePods(ctx, pn.selfID)
	if err != nil {
		logger.Warn(ctx, "pod_notifier_refresh_pods_failed", zap.Error(err))
		return
	}
	if len(pods) == 0 {
		// No peers — publish an empty table so Notify is a no-op.
		pn.routeCache.Store(&podRouteTable{
			tenantToPods: make(map[string][]string),
		})
		return
	}
	// Build podID → addr lookup for active peers.
	podAddr := make(map[string]string, len(pods))
	for _, p := range pods {
		podAddr[p.PodID] = p.Addr
	}
	// Read all pod_subscriptions and build tenant → []peerAddr reverse index.
	subs, err := pn.metaStore.ListAllPodSubscriptions(ctx)
	if err != nil {
		logger.Warn(ctx, "pod_notifier_refresh_subs_failed", zap.Error(err))
		return
	}
	tenantToPods := make(map[string][]string)
	for _, s := range subs {
		addr, ok := podAddr[s.PodID]
		if !ok {
			// Subscription references a pod that's not active (stale or self).
			// Skip — pushing to an inactive pod is wasteful.
			continue
		}
		tenantToPods[s.TenantID] = append(tenantToPods[s.TenantID], addr)
	}
	allAddrs := make([]string, 0, len(pods))
	for _, p := range pods {
		allAddrs = append(allAddrs, p.Addr)
	}
	pn.routeCache.Store(&podRouteTable{
		tenantToPods: tenantToPods,
		allPeers:     allAddrs,
	})
	logger.Debug(ctx, "pod_notifier_route_refreshed",
		zap.Int("peers", len(allAddrs)),
		zap.Int("tenant_routes", len(tenantToPods)))
}

// notifyPushRequest is the JSON body for POST /v1/internal/sse-notify.
type notifyPushRequest struct {
	TenantID string `json:"tenant_id"`
	Seq      uint64 `json:"seq"`
}

// Notify sends a fire-and-forget HTTP push to all peer pods that have SSE
// subscribers for the given tenant. It never blocks the caller — each push
// is dispatched in a goroutine. If the route table is empty or the tenant has
// no peer subscribers, Notify is a no-op (the notifyPoller fallback covers it).
func (pn *podNotifier) Notify(tenantID string, seq uint64) {
	rt := pn.routeCache.Load()
	if rt == nil {
		return
	}
	targets, ok := rt.tenantToPods[tenantID]
	if !ok || len(targets) == 0 {
		// No peer pod has subscribers for this tenant — skip. The notifyPoller
		// will still catch this event for any peer that starts subscribing
		// before the next 200ms tick.
		return
	}
	payload, err := json.Marshal(notifyPushRequest{
		TenantID: tenantID,
		Seq:      seq,
	})
	if err != nil {
		logger.Warn(context.Background(), "pod_notifier_marshal_failed",
			zap.String("tenant_id", tenantID),
			zap.Error(err))
		return
	}
	for _, addr := range targets {
		pn.dispatchPush(addr, payload)
	}
}

// dispatchPush sends a single fire-and-forget POST to a peer. It runs in its
// own goroutine so the caller is never blocked by a slow/dead peer. Errors
// are logged at debug level (the notifyPoller is the durable fallback).
func (pn *podNotifier) dispatchPush(addr string, payload []byte) {
	go func() {
		url := addr + sseNotifyInternalRoute
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if len(pn.secret) > 0 {
			req.Header.Set("Authorization", "Bearer "+string(pn.secret))
		}
		resp, err := pn.client.Do(req)
		if err != nil {
			logger.Debug(context.Background(), "pod_notifier_push_failed",
				zap.String("peer", addr),
				zap.Error(err))
			return
		}
		_ = resp.Body.Close()
	}()
}