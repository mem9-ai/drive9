package server

import (
	"sync"
	"time"
)

// ChangeEvent represents a single filesystem mutation event.
type ChangeEvent struct {
	Seq   uint64 `json:"seq"`              // monotonic per-bus sequence number
	Path  string `json:"path"`             // affected path
	Op    string `json:"op"`               // "write" | "delete" | "rename" | "mkdir" | "copy" | "upload_complete"
	Actor string `json:"actor,omitempty"`  // X-Dat9-Actor header value (per-mount ID)
	Ts    int64  `json:"ts"`               // unix milliseconds
}

const (
	eventBusRingSize         = 10000
	eventBusListenerChanSize = 1 // signal-only channel
)

// EventBus is a per-tenant in-memory event hub backed by a fixed-size ring buffer.
// Single-instance only — does not survive restarts or replicate across processes.
type EventBus struct {
	mu        sync.Mutex
	seq       uint64 // monotonic counter, protected by mu
	ring      [eventBusRingSize]ChangeEvent
	head      int // next write position
	count     int // entries currently stored (max eventBusRingSize)
	listeners map[uint64]chan struct{}
	nextID    uint64
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		listeners: make(map[uint64]chan struct{}),
	}
}

// Publish appends a new event to the ring buffer and wakes all subscribers.
func (eb *EventBus) Publish(path, op, actor string) {
	eb.mu.Lock()
	eb.seq++
	ev := ChangeEvent{
		Seq:   eb.seq,
		Path:  path,
		Op:    op,
		Actor: actor,
		Ts:    time.Now().UnixMilli(),
	}
	eb.ring[eb.head] = ev
	eb.head = (eb.head + 1) % eventBusRingSize
	if eb.count < eventBusRingSize {
		eb.count++
	}

	// Wake all listeners (non-blocking send on signal channel).
	for _, ch := range eb.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	eb.mu.Unlock()
}

// Subscribe registers a new listener. Returns a unique ID and a signal channel.
// The channel receives a signal whenever new events are published.
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

// Seq returns the current sequence number (0 if no events published).
func (eb *EventBus) Seq() uint64 {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	return eb.seq
}

// EventsSince returns all events with seq > since and the current head seq.
// If since is too old (ring wrapped), from the future, or zero, ok is false
// and the caller should send a reset using the returned headSeq.
//
// headSeq is always returned from the same lock acquisition as the event
// scan, guaranteeing a consistent snapshot for reset cursor positioning.
func (eb *EventBus) EventsSince(since uint64) (events []ChangeEvent, headSeq uint64, ok bool) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	headSeq = eb.seq

	if since == 0 {
		return nil, headSeq, false
	}

	if eb.count == 0 {
		// No events ever published. since > 0 means stale client.
		return nil, headSeq, false
	}

	// Find the oldest seq in the ring.
	oldestIdx := (eb.head - eb.count + eventBusRingSize) % eventBusRingSize
	oldestSeq := eb.ring[oldestIdx].Seq
	newestSeq := eb.ring[(eb.head-1+eventBusRingSize)%eventBusRingSize].Seq

	if since+1 < oldestSeq {
		// Ring has wrapped past the client's position.
		// since+1 is the first seq the client needs; if it's older than
		// the oldest retained event, we can't replay.
		return nil, headSeq, false
	}
	if since > newestSeq {
		// Client is ahead of us (e.g. server restarted). Send reset.
		return nil, headSeq, false
	}
	if since == newestSeq {
		// Client is caught up, no new events.
		return nil, headSeq, true
	}

	// Walk the ring from oldest to newest, collecting events with seq > since.
	result := make([]ChangeEvent, 0, 64)
	for i := 0; i < eb.count; i++ {
		idx := (oldestIdx + i) % eventBusRingSize
		ev := eb.ring[idx]
		if ev.Seq > since {
			result = append(result, ev)
		}
	}
	return result, headSeq, true
}
