package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
)

const (
	sseHeartbeatInterval     = 30 * time.Second
	sseFlushBatchSize        = 10
	sseFlushMaxDelay         = 1 * time.Millisecond
	ssePersistentReplayLimit = eventBusRingSize
	ssePersistentRetention   = eventBusRingSize * 10
)

var sseActiveConnections atomic.Int64

// stopTimer drains a timer's channel after stopping it to prevent spurious
// ticks. Returns true if the timer was stopped before it fired.
func stopTimer(t *time.Timer) bool {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
		return false
	}
	return true
}

// sseBufferedWriter wraps http.ResponseWriter with a bufio.Writer to batch
// small SSE writes and reduce syscalls. Flushing follows two rules:
//  1. Batch size: flush after sseFlushBatchSize events.
//  2. Max delay: flush at most sseFlushMaxDelay after the first buffered
//     event.
type sseBufferedWriter struct {
	rw      http.ResponseWriter
	w       *bufio.Writer
	count   int
	first   time.Time
	flusher http.Flusher
}

// flusherWriter wraps an http.Flusher so that any write to the underlying
// ResponseWriter also triggers Flush(), ensuring data reaches the client
// immediately rather than being buffered by net/http or reverse proxies.
type flusherWriter struct {
	rw      http.ResponseWriter
	flusher http.Flusher
}

func (fw *flusherWriter) Write(p []byte) (int, error) {
	n, err := fw.rw.Write(p)
	if err != nil {
		return n, err
	}
	fw.flusher.Flush()
	return n, nil
}

func newSSEBufferedWriter(rw http.ResponseWriter, flusher http.Flusher) *sseBufferedWriter {
	// Use a 64 KiB buffer — large enough for a batch of 10 events while
	// small enough to avoid excessive memory per connection.
	fw := &flusherWriter{rw: rw, flusher: flusher}
	return &sseBufferedWriter{
		rw:      rw,
		w:       bufio.NewWriterSize(fw, 64*1024),
		flusher: flusher,
	}
}

func (bw *sseBufferedWriter) Write(p []byte) (int, error) {
	return bw.w.Write(p)
}

func (bw *sseBufferedWriter) Flush() error {
	if err := bw.w.Flush(); err != nil {
		return err
	}
	bw.count = 0
	bw.first = time.Time{}
	return nil
}

func (bw *sseBufferedWriter) shouldFlush() bool {
	if bw.count == 0 {
		return false
	}
	if bw.count >= sseFlushBatchSize {
		return true
	}
	if !bw.first.IsZero() && time.Since(bw.first) >= sseFlushMaxDelay {
		return true
	}
	return false
}

func (bw *sseBufferedWriter) recordWrite() {
	if bw.count == 0 {
		bw.first = time.Now()
	}
	bw.count++
}

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
	start := time.Now()
	if b := backendFromRequest(r); b != nil && b.Store() != nil {
		ev, err := b.Store().InsertFSEvent(r.Context(), path, op, actor)
		if err == nil {
			recordSSEOperation("persist", "ok", start)
			if ev.Seq > ssePersistentRetention {
				if _, pruneErr := b.Store().PruneFSEventsBefore(r.Context(), ev.Seq-ssePersistentRetention+1); pruneErr != nil {
					recordSSEOperation("retention_sweep", "error", start)
				} else {
					recordSSEOperation("retention_sweep", "ok", start)
				}
			}
			bus.PublishEvent(ChangeEvent{
				Seq:   ev.Seq,
				Path:  ev.Path,
				Op:    ev.Op,
				Actor: ev.Actor,
				Ts:    ev.Ts,
			})
			recordSSEOperation("publish", "ok", start)
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "sse_persist_failed", "path", path, "op", op, "error", err)...)
		recordSSEOperation("persist", "error", start)
	}
	bus.Publish(path, op, actor)
	recordSSEOperation("publish", "volatile", start)
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
	metrics.RecordGauge("sse", "active_connections", float64(sseActiveConnections.Add(1)))
	defer metrics.RecordGauge("sse", "active_connections", float64(sseActiveConnections.Add(-1)))

	ctx := r.Context()

	// Phase 1: Replay or Reset.
	// Prefer the durable event log when tenant storage is available. The
	// in-memory ring remains the live fan-out path and fallback for tests /
	// single-tenant setups without a scoped backend.
	events, headSeq, ok, replayReason := s.eventsSince(r.Context(), bStoreFromRequest(r), bus, since)
	lastSeen := since

	bw := newSSEBufferedWriter(w, flusher)

	if !ok {
		sendSSEReset(bw, headSeq, replayReason)
		recordSSEOperation("reset", replayReason, time.Time{})
		lastSeen = headSeq
	} else {
		for _, ev := range events {
			sendSSEEvent(bw, ev)
			lastSeen = ev.Seq
		}
	}
	// End the initial replay/reset phase with an immediate heartbeat so
	// clients have an explicit stream-current marker. This lets caches that
	// were marked unverified on disconnect become verified without waiting
	// for the periodic heartbeat.
	sendSSEHeartbeat(bw, lastSeen)
	// Flush initial replay/reset immediately so the client receives the
	// cursor position without waiting for the periodic heartbeat.
	if err := bw.Flush(); err != nil {
		return
	}

	// Phase 2: Live stream with micro-batching.
	// Instead of flushing after every single event, we accumulate events
	// and flush at heartbeat boundaries or when the batch size is reached.
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	// Use a nil timer that we allocate on first need. Starting with
	// time.NewTimer(0) and immediately stopping can leave a stale tick
	// in the channel that fires spuriously on later Reset calls.
	var flushTimer *time.Timer
	var flushC <-chan time.Time
	defer func() {
		if flushTimer != nil {
			stopTimer(flushTimer)
			flushC = nil
		}
	}()
	for {
		select {
		case <-ctx.Done():
			recordSSEOperation("disconnect", "client_cancelled", time.Time{})
			return
		case <-heartbeat.C:
			sendSSEHeartbeat(bw, lastSeen)
			if err := bw.Flush(); err != nil {
				return
			}
			if flushTimer != nil {
				stopTimer(flushTimer)
				flushC = nil
			}
		case <-flushC:
			if bw.count > 0 {
				if err := bw.Flush(); err != nil {
					return
				}
			}
			flushC = nil
		case _, open := <-notify:
			if !open {
				recordSSEOperation("disconnect", "server_closed", time.Time{})
				return
			}
			liveEvents, liveHead, liveOK := bus.EventsSince(lastSeen)
			if !liveOK {
				// Reset must be sent immediately; buffering it would stall
				// the client until the next heartbeat or unrelated event.
				sendSSEReset(bw, liveHead, "seq_too_old")
				lastSeen = liveHead
				if err := bw.Flush(); err != nil {
					return
				}
				if flushTimer != nil {
					stopTimer(flushTimer)
					flushC = nil
				}
			} else {
				for _, ev := range liveEvents {
					sendSSEEvent(bw, ev)
					lastSeen = ev.Seq
				}
				if bw.count > 0 {
					if bw.shouldFlush() {
						if err := bw.Flush(); err != nil {
							return
						}
						if flushTimer != nil {
							stopTimer(flushTimer)
							flushC = nil
						}
					} else if flushC == nil {
						// Arm the max-delay timer for the first
						// buffered event since the last flush. Use
						// flushC (not bw.count) so coalesced bursts
						// of N>1 still arm the timer.
						if flushTimer == nil {
							flushTimer = time.NewTimer(sseFlushMaxDelay)
						} else {
							stopTimer(flushTimer)
							flushTimer.Reset(sseFlushMaxDelay)
						}
						flushC = flushTimer.C
					}
				}
			}
		}
	}
}

func bStoreFromRequest(r *http.Request) *datastore.Store {
	b := backendFromRequest(r)
	if b == nil {
		return nil
	}
	return b.Store()
}

func (s *Server) eventsSince(ctx context.Context, store *datastore.Store, bus *EventBus, since uint64) ([]ChangeEvent, uint64, bool, string) {
	if store != nil {
		events, headSeq, ok, reason, err := persistentEventsSince(ctx, store, since)
		if err == nil {
			return events, headSeq, ok, reason
		}
		logger.Error(ctx, "server_event", eventFields(ctx, "sse_replay_persistent_failed", "error", err)...)
	}
	events, headSeq, ok := bus.EventsSince(since)
	if !ok {
		reason := "initial_sync"
		if since > 0 {
			reason = "seq_too_old"
			if since > headSeq {
				reason = "server_restart"
			}
		}
		return nil, headSeq, false, reason
	}
	return events, headSeq, true, ""
}

func persistentEventsSince(ctx context.Context, store *datastore.Store, since uint64) ([]ChangeEvent, uint64, bool, string, error) {
	start := time.Now()
	oldestSeq, headSeq, count, err := store.FSEventBounds(ctx)
	if err != nil {
		recordSSEOperation("replay", "error", start)
		return nil, 0, false, "", err
	}

	if since == 0 {
		recordSSEOperation("replay", "initial_sync", start)
		return nil, headSeq, false, "initial_sync", nil
	}
	if count == 0 {
		recordSSEOperation("replay", "server_restart", start)
		return nil, headSeq, false, "server_restart", nil
	}
	if since > headSeq {
		recordSSEOperation("replay", "server_restart", start)
		return nil, headSeq, false, "server_restart", nil
	}
	if oldestSeq > 0 && since+1 < oldestSeq {
		recordSSEOperation("replay", "seq_too_old", start)
		return nil, headSeq, false, "seq_too_old", nil
	}
	if since == headSeq {
		recordSSEOperation("replay", "ok", start)
		return nil, headSeq, true, "", nil
	}

	events, err := store.ListFSEventsSince(ctx, since, ssePersistentReplayLimit+1)
	if err != nil {
		recordSSEOperation("replay", "error", start)
		return nil, 0, false, "", err
	}
	if len(events) > ssePersistentReplayLimit {
		recordSSEOperation("replay", "seq_too_old", start)
		return nil, headSeq, false, "seq_too_old", nil
	}
	out := make([]ChangeEvent, 0, len(events))
	for _, ev := range events {
		out = append(out, ChangeEvent{
			Seq:   ev.Seq,
			Path:  ev.Path,
			Op:    ev.Op,
			Actor: ev.Actor,
			Ts:    ev.Ts,
		})
	}
	recordSSEOperation("replay", "ok", start)
	return out, headSeq, true, "", nil
}

func recordSSEOperation(operation, result string, start time.Time) {
	var d time.Duration
	if !start.IsZero() {
		d = time.Since(start)
	}
	metrics.RecordOperation("sse", operation, result, d)
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

func sendSSEEvent(w *sseBufferedWriter, ev ChangeEvent) {
	if isStructuralOp(ev.Op) {
		// Structural ops are sent as reset events per the accepted design.
		sendSSEStructuralReset(w, ev)
		return
	}
	data, err := json.Marshal(ev)
	if err != nil {
		logger.Error(context.TODO(), "sse_marshal_event_failed")
		return
	}
	if _, err := fmt.Fprintf(w, "event: file_changed\ndata: %s\n\n", data); err == nil {
		w.recordWrite()
	}
}

type sseResetPayload struct {
	Seq    uint64 `json:"seq"`
	Reason string `json:"reason"`
	Path   string `json:"path,omitempty"`
	Op     string `json:"op,omitempty"`
	Actor  string `json:"actor,omitempty"`
}

func sendSSEStructuralReset(w *sseBufferedWriter, ev ChangeEvent) {
	data, _ := json.Marshal(sseResetPayload{
		Seq:    ev.Seq,
		Reason: "structural_change",
		Path:   ev.Path,
		Op:     ev.Op,
		Actor:  ev.Actor,
	})
	if _, err := fmt.Fprintf(w, "event: reset\ndata: %s\n\n", data); err == nil {
		w.recordWrite()
	}
}

func sendSSEReset(w *sseBufferedWriter, seq uint64, reason string) {
	data, _ := json.Marshal(sseResetPayload{
		Seq:    seq,
		Reason: reason,
	})
	if _, err := fmt.Fprintf(w, "event: reset\ndata: %s\n\n", data); err == nil {
		w.recordWrite()
	}
}

func sendSSEHeartbeat(w *sseBufferedWriter, seq uint64) {
	data, _ := json.Marshal(map[string]interface{}{
		"seq": seq,
	})
	if _, err := fmt.Fprintf(w, "event: heartbeat\ndata: %s\n\n", data); err == nil {
		w.recordWrite()
	}
}
