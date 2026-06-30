package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

// ChangeEvent represents a single filesystem mutation event.
type ChangeEvent struct {
	Seq   uint64 `json:"seq"`             // monotonic per-tenant seq (fs_events.seq AUTO_INCREMENT)
	Path  string `json:"path"`            // affected path
	Op    string `json:"op"`              // "write" | "delete" | "rename" | "mkdir" | "copy" | "upload_complete"
	Actor string `json:"actor,omitempty"` // X-Dat9-Actor header value (per-mount ID)
	Ts    int64  `json:"ts"`              // unix milliseconds
}

const (
	eventBusListenerChanSize = 1 // signal-only channel
)

// EventBus is a per-tenant event hub backed by the durable fs_events table.
// Events are stored in the shared tenant database so that they propagate across
// all pods. The local notify channel provides instant push to same-pod SSE
// clients; cross-pod events are discovered via the central sse_notify_outbox
// table in the meta DB (polled by a single global notifyPoller per pod, not a
// per-bus goroutine) and optionally via direct pod-to-pod HTTP push.
//
// The store field is an atomic pointer so it can be safely refreshed by
// eventBuses.get (when the pool recreates a backend) while SSE handlers read it.
//
// Design note: prior to the outbox redesign, each EventBus ran its own 1s poll
// goroutine querying the tenant's fs_events table. With ~100k tenants this
// kept every serverless tenant TiDB awake (RCU cost). Now cross-pod discovery is
// centralized: a single notifyPoller reads the lightweight sse_notify_outbox
// (in the always-provisioned meta DB) and calls Publish() on matching buses.
// A podNotifier additionally pushes notifications to peers for <10ms latency.
// Neither path touches a tenant TiDB unless that tenant actually has new events.
type EventBus struct {
	tenantID string
	store    atomic.Pointer[datastore.Store]
	mu       sync.Mutex
	listeners map[uint64]chan struct{}
	nextID    uint64
}

// NewEventBus creates a new EventBus backed by the given tenant store.
func NewEventBus(tenantID string, store *datastore.Store) *EventBus {
	eb := &EventBus{
		tenantID:  tenantID,
		listeners: make(map[uint64]chan struct{}),
	}
	eb.store.Store(store)
	return eb
}

// TenantID returns the tenant identifier for this bus.
func (eb *EventBus) TenantID() string {
	return eb.tenantID
}

// SetStore atomically updates the backing store. Called by eventBuses.get
// when the pool invalidates and recreates a backend (closing the old store
// and opening a new one).
func (eb *EventBus) SetStore(store *datastore.Store) {
	eb.store.Store(store)
}

// HasListeners returns true if this bus currently has at least one subscriber.
// Used by the notify poller and pod notifier to skip tenants with no local SSE
// connections (avoiding unnecessary wakeups).
func (eb *EventBus) HasListeners() bool {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	return len(eb.listeners) > 0
}

// Publish signals all local subscribers that a new event is available.
// The actual event row is inserted into fs_events by the caller (publishEvent)
// before calling Publish; this method only wakes same-pod SSE clients for instant
// delivery. Cross-pod clients discover new rows via the central notifyPoller
// or pod-to-pod HTTP push.
func (eb *EventBus) Publish() {
	eb.mu.Lock()
	for _, ch := range eb.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	eb.mu.Unlock()
}

// Subscribe registers a new listener. Returns a unique ID and a signal channel.
// The channel receives a signal whenever new events are published locally or
// discovered by the central notifyPoller / received via pod-to-pod push.
func (eb *EventBus) Subscribe() (uint64, chan struct{}) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	id := eb.nextID
	eb.nextID++
	ch := make(chan struct{}, eventBusListenerChanSize)
	eb.listeners[id] = ch
	metrics.RecordSSEInFlight(eb.tenantID, float64(len(eb.listeners)))
	return id, ch
}

// Unsubscribe removes a listener and closes its channel.
func (eb *EventBus) Unsubscribe(id uint64) {
	eb.mu.Lock()
	if ch, ok := eb.listeners[id]; ok {
		delete(eb.listeners, id)
		close(ch)
	}
	metrics.RecordSSEInFlight(eb.tenantID, float64(len(eb.listeners)))
	eb.mu.Unlock()
}

// Seq returns the current maximum fs_events seq, or 0 if no events exist.
func (eb *EventBus) Seq(ctx context.Context) uint64 {
	return uint64(clampNonNegative(latestSeqSafeWithMetric(ctx, eb.store.Load(), eb.tenantID)))
}

// EventsSince returns all events with seq > since and the current head seq.
// ok=false signals the caller to send a reset (cursor is stale, table missing,
// or initial sync). The reset reasons mirror the old ring buffer semantics:
//   - since == 0: initial_sync
//   - since < oldestSeq: events pruned between client cursor and retained window
//   - since > headSeq: server_restart (client cursor ahead of head)
func (eb *EventBus) EventsSince(ctx context.Context, since uint64) (events []ChangeEvent, headSeq uint64, ok bool) {
	store := eb.store.Load()
	if store == nil {
		return nil, 0, false
	}
	if since == 0 {
		// Initial sync: send reset so client starts fresh.
		headSeq = uint64(clampNonNegative(latestSeqSafeWithMetric(ctx, store, eb.tenantID)))
		return nil, headSeq, false
	}
	queryStart := time.Now()
	rows, err := store.ListFSEventsSince(ctx, int64(since), 1000)
	metrics.RecordEventBusQuery(eb.tenantID, "events_since", queryResult(err), time.Since(queryStart))
	if err != nil {
		// Table missing or query failed: don't send reset on every poll —
		// that would cause continuous full-cache invalidation. Instead, return
		// ok=true with empty events (caught up). The FUSE client's TTL/HEAD
		// revalidation provides correctness without SSE. Only since=0 (initial
		// connection) sends a reset. Log the error and emit a metric counter so
		// operators can detect and alert on persistent table-missing or DB
		// connectivity issues.
		logger.Warn(ctx, "event_bus_query_failed",
			zap.String("tenant_id", eb.tenantID),
			zap.Uint64("since", since),
			zap.Float64("query_ms", float64(time.Since(queryStart).Microseconds())/1000),
			zap.Error(err))
		metrics.RecordTenantOperation(eb.tenantID, "event_bus", "query", metrics.ResultForError(err), 0)
		headSeq = since // keep the client's cursor unchanged
		return nil, headSeq, true
	}
	events = make([]ChangeEvent, 0, len(rows))
	for _, r := range rows {
		events = append(events, ChangeEvent{
			Seq:   r.Seq,
			Path:  r.Path,
			Op:    r.Op,
			Actor: r.Actor,
			Ts:    r.Ts,
		})
	}
	if len(events) > 0 {
		// Got rows: check for a gap between the client's cursor and the
		// first retained event. If since+1 < first event seq, events were
		// pruned in between → reset to avoid silently missing them.
		firstSeq := events[0].Seq
		if since+1 < firstSeq {
			headSeq = events[len(events)-1].Seq
			return nil, headSeq, false // gap detected → reset
		}
		headSeq = events[len(events)-1].Seq
		return events, headSeq, true
	}
	// No rows after since. Check the actual table state to determine whether
	// the client's cursor is stale (events pruned) or simply caught up.
	latestStart := time.Now()
	latest := latestSeqSafeWithMetric(ctx, store, eb.tenantID)
	latestQueryMs := float64(time.Since(latestStart).Microseconds()) / 1000
	oldestStart := time.Now()
	oldest := oldestSeqSafeWithMetric(ctx, store, eb.tenantID)
	oldestQueryMs := float64(time.Since(oldestStart).Microseconds()) / 1000
	if latest == 0 {
		// Table is empty (all events pruned): cursor is stale → reset.
		return nil, 0, false
	}
	headSeq = uint64(latest)
	if int64(since) > latest {
		// Client cursor is ahead of head (server_restart): reset.
		return nil, headSeq, false
	}
	if int64(since)+1 < oldest {
		// Client cursor is behind the oldest retained event: events between
		// since+1 and oldest-1 were pruned, so the client has a gap → reset
		// to avoid silently missing those events.
		// Log the gap with table bounds so operators can detect table growth
		// without direct DB access (oldest-seq - since delta indicates how
		// much was pruned).
		logger.Info(ctx, "event_bus_cursor_gap",
			zap.String("tenant_id", eb.tenantID),
			zap.Uint64("since", since),
			zap.Int64("oldest", oldest),
			zap.Int64("latest", latest),
			zap.Float64("latest_query_ms", latestQueryMs),
			zap.Float64("oldest_query_ms", oldestQueryMs))
		return nil, headSeq, false
	}
	// Client is caught up: no new events after since, cursor within range.
	return nil, headSeq, true
}

// latestSeqSafeWithMetric is latestSeqSafe plus a per-query duration metric.
func latestSeqSafeWithMetric(ctx context.Context, store *datastore.Store, tenantID string) int64 {
	if store == nil {
		return 0
	}
	start := time.Now()
	seq, err := store.LatestFSEventSeq(ctx)
	metrics.RecordEventBusQuery(tenantID, "latest", queryResult(err), time.Since(start))
	if err != nil {
		return 0
	}
	return seq
}

// oldestSeqSafeWithMetric is oldestSeqSafe plus a per-query duration metric.
func oldestSeqSafeWithMetric(ctx context.Context, store *datastore.Store, tenantID string) int64 {
	if store == nil {
		return 0
	}
	start := time.Now()
	seq, err := store.OldestFSEventSeq(ctx)
	metrics.RecordEventBusQuery(tenantID, "oldest", queryResult(err), time.Since(start))
	if err != nil {
		return 0
	}
	return seq
}

// queryResult maps a query error to a metric result label.
func queryResult(err error) string {
	return metrics.ResultForError(err)
}

// clampNonNegative returns v if v >= 0, else 0.
func clampNonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
