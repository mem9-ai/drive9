package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
)

const (
	sseHeartbeatInterval = 30 * time.Second
)

// eventBuses manages per-tenant EventBus instances. For single-tenant mode
// (fallback backend), the empty-string key is used.
type eventBuses struct {
	mu    sync.RWMutex
	buses map[string]*EventBus
}

func newEventBuses() *eventBuses {
	return &eventBuses{
		buses: make(map[string]*EventBus),
	}
}

func (ebs *eventBuses) get(tenantID string) *EventBus {
	ebs.mu.RLock()
	bus, ok := ebs.buses[tenantID]
	ebs.mu.RUnlock()
	if ok {
		return bus
	}

	ebs.mu.Lock()
	defer ebs.mu.Unlock()
	// Double-check after acquiring write lock.
	if bus, ok = ebs.buses[tenantID]; ok {
		return bus
	}
	bus = NewEventBus()
	ebs.buses[tenantID] = bus
	return bus
}

func (s *Server) tenantEventBus(r *http.Request) *EventBus {
	scope := ScopeFromContext(r.Context())
	if scope != nil {
		return s.events.get(scope.TenantID)
	}
	// Single-tenant / fallback mode.
	return s.events.get("")
}

func (s *Server) publishEvent(r *http.Request, path, op string) {
	actor := r.Header.Get("X-Dat9-Actor")
	bus := s.tenantEventBus(r)
	bus.Publish(path, op, actor)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		errJSON(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	sinceStr := r.URL.Query().Get("since")
	var since uint64
	if sinceStr != "" {
		v, err := strconv.ParseUint(sinceStr, 10, 64)
		if err != nil {
			errJSON(w, http.StatusBadRequest, "invalid since parameter")
			return
		}
		since = v
	}

	bus := s.tenantEventBus(r)
	subID, notify := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()

	// Phase 1: Replay or Reset.
	lastSeen := since
	if since == 0 {
		// Initial connection: send reset with current seq.
		sendSSEReset(w, bus.Seq(), "initial_sync")
		lastSeen = bus.Seq()
		flusher.Flush()
	} else {
		events, ok := bus.EventsSince(since)
		if !ok {
			// Gap detected: send reset.
			currentSeq := bus.Seq()
			reason := "seq_too_old"
			if since > currentSeq {
				reason = "server_restart"
			}
			sendSSEReset(w, currentSeq, reason)
			lastSeen = currentSeq
		} else {
			for _, ev := range events {
				sendSSEEvent(w, ev)
				lastSeen = ev.Seq
			}
		}
		flusher.Flush()
	}

	// Phase 2: Live stream.
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			sendSSEHeartbeat(w, bus.Seq())
			flusher.Flush()
		case _, open := <-notify:
			if !open {
				return
			}
			events, ok := bus.EventsSince(lastSeen)
			if !ok {
				sendSSEReset(w, bus.Seq(), "seq_too_old")
				lastSeen = bus.Seq()
			} else {
				for _, ev := range events {
					sendSSEEvent(w, ev)
					lastSeen = ev.Seq
				}
			}
			flusher.Flush()
		}
	}
}

func sendSSEEvent(w http.ResponseWriter, ev ChangeEvent) {
	data, err := json.Marshal(ev)
	if err != nil {
		logger.Error(nil, "sse_marshal_event_failed")
		return
	}
	fmt.Fprintf(w, "event: file_changed\ndata: %s\n\n", data)
}

func sendSSEReset(w http.ResponseWriter, seq uint64, reason string) {
	data, _ := json.Marshal(map[string]interface{}{
		"seq":    seq,
		"reason": reason,
	})
	fmt.Fprintf(w, "event: reset\ndata: %s\n\n", data)
}

func sendSSEHeartbeat(w http.ResponseWriter, seq uint64) {
	data, _ := json.Marshal(map[string]interface{}{
		"seq": seq,
	})
	fmt.Fprintf(w, "event: heartbeat\ndata: %s\n\n", data)
}
