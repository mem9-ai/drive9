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
	Op    string `json:"op"`             // "write" | "delete" | "rename" | "mkdir" | "copy" | "upload_complete"
	Actor string `json:"actor,omitempty"` // X-Dat9-Actor header value (per-mount ID)
	Ts    int64  `json:"ts"`             // unix milliseconds
}

const (
	eventBusListenerChanSize = 1 // signal-only channel
	// eventBusPollInterval is how often the per-bus poll goroutine queries
	// fs_events for cross-pod events. Override via ssePollInterval in sse.go.
	eventBusPollInterval = 1 * time.Second
)

// EventBus is a per-tenant event hub backed by the durable fs_events table.
// Events are stored in the shared tenant database so that they propagate across
// all pods. The local notify channel provides instant push to same-pod SSE clients;
// cross-pod events are discovered via a single per-bus poll goroutine (not
// per-connection) to minimize TiDB RCU consumption.
// The store field is an atomic pointer so it can be safely refreshed by
// eventBuses.get (when the pool recreates a backend) while SSE handlers read it.
type EventBus struct {
	store     atomic.Pointer[datastore.Store]
	mu        sync.Mutex
	listeners map[uint64]chan struct{}
	nextID    uint64

	// poll goroutine lifecycle: starts on first Subscribe, stops on last Unsubscribe.
	pollCancel context.CancelFunc
	pollWG     sync.WaitGroup
	pollLast   uint64 // last seq seen by the poll goroutine
}

// NewEventBus creates a new EventBus backed by the given tenant store.
func NewEventBus(store *datastore.Store) *EventBus {
	eb := &EventBus{
		listeners: make(map[uint64]chan struct{}),
	}
	eb.store.Store(store)
	return eb
}

// SetStore atomically updates the backing store. Called by eventBuses.get
// when the pool invalidates and recreates a backend (closing the old store
// and opening a new one).
func (eb *EventBus) SetStore(store *datastore.Store) {
	eb.store.Store(store)
}

// Publish signals all local subscribers that a new event is available.
// The actual event row is inserted into fs_events by the caller (publishEvent)
// before calling Publish; this method only wakes same-pod SSE clients for instant
// delivery. Cross-pod clients discover new rows via periodic polling.
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
// discovered by the per-bus cross-pod poll goroutine.
// On the first Subscribe, the poll goroutine is started to discover cross-pod
// events. On the last Unsubscribe, it is stopped to release DB polling load.
func (eb *EventBus) Subscribe() (uint64, chan struct{}) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	id := eb.nextID
	eb.nextID++
	ch := make(chan struct{}, eventBusListenerChanSize)
	eb.listeners[id] = ch

	// Start the cross-pod poll goroutine on first subscriber.
	if eb.pollCancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		eb.pollCancel = cancel
		eb.pollWG.Add(1)
		go eb.pollLoop(ctx)
	}
	return id, ch
}

// Unsubscribe removes a listener and closes its channel.
// On the last Unsubscribe, the poll goroutine is stopped.
func (eb *EventBus) Unsubscribe(id uint64) {
	eb.mu.Lock()
	if ch, ok := eb.listeners[id]; ok {
		delete(eb.listeners, id)
		close(ch)
	}
	// Stop the poll goroutine when no subscribers remain.
	if len(eb.listeners) == 0 && eb.pollCancel != nil {
		eb.pollCancel()
		eb.pollCancel = nil
	}
	eb.mu.Unlock()

	// Wait for poll goroutine outside the lock to avoid deadlock.
	eb.pollWG.Wait()
}

// pollLoop is the per-bus cross-pod poll goroutine. It periodically queries
// fs_events for new rows (written by other pods) and signals all local
// subscribers via Publish(). This replaces per-SSE-connection polling, reducing
// idle RCU from N-mounts×QPS to 1-poll-per-bus.
func (eb *EventBus) pollLoop(ctx context.Context) {
	defer eb.pollWG.Done()

	// Initialize pollLast to the current head so we don't replay old events
	// on first poll.
	eb.mu.Lock()
	eb.pollLast = eb.unsafeSeq(ctx)
	eb.mu.Unlock()

	ticker := time.NewTicker(eventBusPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			eb.pollOnce(ctx)
		}
	}
}

// pollOnce queries fs_events for rows after pollLast and signals subscribers
// if any new rows are found.
func (eb *EventBus) pollOnce(ctx context.Context) {
	store := eb.store.Load()
	if store == nil {
		return
	}
	eb.mu.Lock()
	since := eb.pollLast
	eb.mu.Unlock()

	rows, err := store.ListFSEventsSince(ctx, int64(since), 1000)
	if err != nil {
		// Don't log on every tick — only on persistent failures.
		return
	}
	if len(rows) == 0 {
		return
	}
	// Update pollLast to the last row's seq and signal subscribers.
	eb.mu.Lock()
	eb.pollLast = rows[len(rows)-1].Seq
	eb.mu.Unlock()
	eb.Publish()
}

// unsafeSeq returns the current maximum fs_events seq. Caller must hold eb.mu.
func (eb *EventBus) unsafeSeq(ctx context.Context) uint64 {
	store := eb.store.Load()
	if store == nil {
		return 0
	}
	seq, err := store.LatestFSEventSeq(ctx)
	if err != nil {
		return 0
	}
	if seq < 0 {
		return 0
	}
	return uint64(seq)
}

// Seq returns the current maximum fs_events seq, or 0 if no events exist.
func (eb *EventBus) Seq(ctx context.Context) uint64 {
	store := eb.store.Load()
	if store == nil {
		return 0
	}
	seq, err := store.LatestFSEventSeq(ctx)
	if err != nil {
		return 0
	}
	if seq < 0 {
		return 0
	}
	return uint64(seq)
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
		headSeq = uint64(clampNonNegative(latestSeqSafe(ctx, store)))
		return nil, headSeq, false
	}
	rows, err := store.ListFSEventsSince(ctx, int64(since), 1000)
	if err != nil {
		// Table missing or query failed: don't send reset on every poll —
		// that would cause continuous full-cache invalidation. Instead, return
		// ok=true with empty events (caught up). The FUSE client's TTL/HEAD
		// revalidation provides correctness without SSE. Only since=0 (initial
		// connection) sends a reset. Log the error and emit a metric counter so
		// operators can detect and alert on persistent table-missing or DB
		// connectivity issues.
		logger.Warn(ctx, "event_bus_query_failed",
			zap.Uint64("since", since),
			zap.Error(err))
		metrics.RecordOperation("event_bus", "query", "error", 0)
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
	latest := latestSeqSafe(ctx, store)
	oldest := oldestSeqSafe(ctx, store)
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
		return nil, headSeq, false
	}
	// Client is caught up: no new events after since, cursor within range.
	return nil, headSeq, true
}

// latestSeqSafe returns LatestFSEventSeq, or 0 on error.
func latestSeqSafe(ctx context.Context, store *datastore.Store) int64 {
	if store == nil {
		return 0
	}
	seq, err := store.LatestFSEventSeq(ctx)
	if err != nil {
		return 0
	}
	return seq
}

// oldestSeqSafe returns OldestFSEventSeq, or 0 on error.
func oldestSeqSafe(ctx context.Context, store *datastore.Store) int64 {
	if store == nil {
		return 0
	}
	seq, err := store.OldestFSEventSeq(ctx)
	if err != nil {
		return 0
	}
	return seq
}

// clampNonNegative returns v if v >= 0, else 0.
func clampNonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}