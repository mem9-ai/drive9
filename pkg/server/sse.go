package server

import (
	"bufio"
	"context"
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
	sseFlushBatchSize    = 10
	sseFlushMaxDelay     = 5 * time.Millisecond
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

	// Wrap the response writer with a buffered writer to reduce syscalls
	// when sending many small SSE events in rapid succession.
	bw := newSSEBufferedWriter(w)

	// flush sends any buffered data to the client. It flushes the bufio
	// buffer first, then the HTTP flusher.
	flush := func() {
		_ = bw.Flush()
		flusher.Flush()
	}

	// Phase 1: Replay or Reset.
	// EventsSince returns headSeq from the same lock acquisition as the
	// ring scan, so reset cursor and ring state are a consistent snapshot.
	events, headSeq, ok := bus.EventsSince(since)
	lastSeen := since
	if !ok {
		reason := "initial_sync"
		if since > 0 {
			reason = "seq_too_old"
			if since > headSeq {
				reason = "server_restart"
			}
		}
		sendSSEReset(bw, headSeq, reason)
		lastSeen = headSeq
	} else {
		for _, ev := range events {
			sendSSEEvent(bw, ev)
			lastSeen = ev.Seq
		}
	}
	flush()

	// Phase 2: Live stream with micro-batching.
	// Instead of flushing after every single notify, we accumulate events
	// and flush at heartbeat boundaries or when the batch size is reached.
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	pending := 0
	flushTimer := time.NewTimer(0)
	flushTimer.Stop()
	defer flushTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			sendSSEHeartbeat(bw, lastSeen)
			flush()
			pending = 0
		case <-flushTimer.C:
			if pending > 0 {
				flush()
				pending = 0
			}
		case _, open := <-notify:
			if !open {
				return
			}
			liveEvents, liveHead, liveOK := bus.EventsSince(lastSeen)
			if !liveOK {
				sendSSEReset(bw, liveHead, "seq_too_old")
				lastSeen = liveHead
			} else {
				for _, ev := range liveEvents {
					sendSSEEvent(bw, ev)
					lastSeen = ev.Seq
					pending++
				}
			}
			// Batch flush: either when we have enough events or after a
			// short delay to allow more events to arrive.
			if pending >= sseFlushBatchSize {
				flush()
				pending = 0
				flushTimer.Stop()
			} else {
				// Restart the micro-delay timer.
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flushTimer.Reset(sseFlushMaxDelay)
			}
		}
	}
}

// isStructuralOp returns true for operations that change namespace structure
// (rename, delete, mkdir, copy). These ops require a full reset on the client
// because targeted invalidation cannot reliably cover old paths, subtrees,
// and parent directory caches.
func isStructuralOp(op string) bool {
	switch op {
	case "rename", "delete", "mkdir", "copy":
		return true
	}
	return false
}

func sendSSEEvent(w http.ResponseWriter, ev ChangeEvent) {
	if isStructuralOp(ev.Op) {
		// Structural ops are sent as reset events per the accepted design.
		sendSSEReset(w, ev.Seq, "structural_change")
		return
	}
	data, err := json.Marshal(ev)
	if err != nil {
		logger.Error(context.TODO(), "sse_marshal_event_failed")
		return
	}
	_, _ = fmt.Fprintf(w, "event: file_changed\ndata: %s\n\n", data)
}

func sendSSEReset(w http.ResponseWriter, seq uint64, reason string) {
	data, _ := json.Marshal(map[string]interface{}{
		"seq":    seq,
		"reason": reason,
	})
	_, _ = fmt.Fprintf(w, "event: reset\ndata: %s\n\n", data)
}

func sendSSEHeartbeat(w http.ResponseWriter, seq uint64) {
	data, _ := json.Marshal(map[string]interface{}{
		"seq": seq,
	})
	_, _ = fmt.Fprintf(w, "event: heartbeat\ndata: %s\n\n", data)
}

// sseBufferedWriter wraps http.ResponseWriter with a bufio.Writer to reduce
// syscalls when sending many small SSE events in rapid succession.
type sseBufferedWriter struct {
	http.ResponseWriter
	buf *bufio.Writer
}

func newSSEBufferedWriter(w http.ResponseWriter) *sseBufferedWriter {
	return &sseBufferedWriter{
		ResponseWriter: w,
		buf:            bufio.NewWriterSize(w, 4096),
	}
}

func (bw *sseBufferedWriter) Write(p []byte) (int, error) {
	return bw.buf.Write(p)
}

func (bw *sseBufferedWriter) Flush() error {
	return bw.buf.Flush()
}
