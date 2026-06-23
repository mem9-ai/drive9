package server

import (
	"context"
	"sync"

	"github.com/mem9-ai/drive9/pkg/datastore"
)

// ChangeEvent represents a single filesystem mutation event.
type ChangeEvent struct {
	Seq   uint64 `json:"seq"`              // monotonic per-tenant seq (fs_events.seq AUTO_INCREMENT)
	Path  string `json:"path"`             // affected path
	Op    string `json:"op"`               // "write" | "delete" | "rename" | "mkdir" | "copy" | "upload_complete"
	Actor string `json:"actor,omitempty"`  // X-Dat9-Actor header value (per-mount ID)
	Ts    int64  `json:"ts"`               // unix milliseconds
}

const (
	eventBusListenerChanSize = 1 // signal-only channel
)

// EventBus is a per-tenant event hub backed by the durable fs_events table.
// Events are stored in the shared tenant database so that they propagate across
// all pods. The local notify channel provides instant push to same-pod SSE clients;
// cross-pod events are discovered via periodic polling of the fs_events table.
type EventBus struct {
	store     *datastore.Store
	mu        sync.Mutex
	listeners map[uint64]chan struct{}
	nextID    uint64
}

// NewEventBus creates a new EventBus backed by the given tenant store.
func NewEventBus(store *datastore.Store) *EventBus {
	return &EventBus{
		store:     store,
		listeners: make(map[uint64]chan struct{}),
	}
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
// The channel receives a signal whenever new events are published locally.
// Call Unsubscribe with the returned ID to clean up.
func (eb *EventBus) Subscribe() (uint64, chan struct{}) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	id := eb.nextID
	eb.nextID++
	ch := make(chan struct{}, eventBusListenerChanSize)
	eb.listeners[id] = ch
	return id, ch
}

// Unsubscribe removes a listener and closes its channel.
func (eb *EventBus) Unsubscribe(id uint64) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if ch, ok := eb.listeners[id]; ok {
		delete(eb.listeners, id)
		close(ch)
	}
}

// Seq returns the current maximum fs_events seq, or 0 if no events exist.
func (eb *EventBus) Seq(ctx context.Context) uint64 {
	if eb.store == nil {
		return 0
	}
	seq, err := eb.store.LatestFSEventSeq(ctx)
	if err != nil {
		return 0
	}
	return uint64(seq)
}

// EventsSince returns all events with seq > since and the current head seq.
// If since is 0 (initial sync) or the query fails, ok is false and the caller
// should send a reset using the returned headSeq.
func (eb *EventBus) EventsSince(ctx context.Context, since uint64) (events []ChangeEvent, headSeq uint64, ok bool) {
	if eb.store == nil {
		return nil, 0, false
	}
	if since == 0 {
		// Initial sync: send reset so client starts fresh.
		headSeq = eb.Seq(ctx)
		return nil, headSeq, false
	}
	rows, err := eb.store.ListFSEventsSince(ctx, int64(since), 1000)
	if err != nil {
		// Table missing or query failed: don't send reset on every poll —
		// that would cause continuous full-cache invalidation. Instead, return
		// ok=true with empty events (caught up). The FUSE client's TTL/HEAD
		// revalidation provides correctness without SSE. Only since=0 (initial
		// connection) sends a reset.
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
	headSeq = eb.Seq(ctx)
	// If no new events but the table is empty (all events pruned) and since > 0,
	// the client's cursor is stale → send reset (like the old ring buffer's
	// "since > newestSeq" case). If the table has events but none after since,
	// the client is caught up → ok=true with empty slice.
	if len(events) == 0 && headSeq == 0 && since > 0 {
		return nil, headSeq, false
	}
	return events, headSeq, true
}