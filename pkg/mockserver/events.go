package mockserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// eventHub is the mock's per-tenant change bus: every mutating handler
// publishes a file_changed event, and every /v1/events subscriber receives
// it. The real server's mounts rely on these events to invalidate read
// caches — without them a second read of a just-written file serves the
// pre-write (possibly empty) cached entry, which is exactly the
// write-then-read-empty class the simulator caught.
type eventHub struct {
	mu     sync.Mutex
	seq    uint64
	subs   map[chan string]struct{}
}

func newEventHub() *eventHub {
	return &eventHub{subs: map[chan string]struct{}{}}
}

func (h *eventHub) subscribe() chan string {
	ch := make(chan string, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *eventHub) unsubscribe(ch chan string) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

// publish bumps the seq and fans the event out to every subscriber. The
// actor comes from the mutating request's X-Dat9-Actor header: subscribers
// filter their own actor, so a mount's own writes must NOT invalidate its
// own pending/commit seeds (an unattributed event reads as a remote change
// and makes the mount drop its own just-committed content).
func (h *eventHub) publish(path, op, actor string) {
	h.mu.Lock()
	h.seq++
	seq := h.seq
	data, _ := json.Marshal(map[string]any{
		"seq":   seq,
		"path":  path,
		"op":    op,
		"actor": actor,
		"ts":    time.Now().UnixMilli(),
	})
	line := fmt.Sprintf("event: file_changed\ndata: %s\n\n", data)
	for ch := range h.subs {
		select {
		case ch <- line:
		default: // slow subscriber: drop rather than block writes
		}
	}
	h.mu.Unlock()
}

func (h *eventHub) head() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

// handleEvents streams the tenant's change events (spec §9): initial
// reset(initial_sync) at the current head, then file_changed events and
// heartbeats carrying the head seq.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	tdb := tenantFrom(r)
	hub := s.hubFor(tdb.id)
	flusher, ok := w.(http.Flusher)
	if !ok {
		errJSON(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "event: reset\ndata: {\"seq\":%d,\"reason\":\"initial_sync\"}\n\n", hub.head())
	// immediate heartbeat: the client treats the stream as current only
	// once it sees one (OnCurrent fires on heartbeats, not resets)
	_, _ = fmt.Fprintf(w, "event: heartbeat\ndata: {\"seq\":%d}\n\n", hub.head())
	flusher.Flush()
	ch := hub.subscribe()
	defer hub.unsubscribe(ch)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case line := <-ch:
			_, _ = fmt.Fprint(w, line)
			_, _ = fmt.Fprintf(w, "event: heartbeat\ndata: {\"seq\":%d}\n\n", hub.head())
			flusher.Flush()
		case <-heartbeat.C:
			_, _ = fmt.Fprintf(w, "event: heartbeat\ndata: {\"seq\":%d}\n\n", hub.head())
			flusher.Flush()
		}
	}
}
