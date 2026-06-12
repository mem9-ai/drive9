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
	"go.uber.org/zap"
)

const (
	maxUint64                   = ^uint64(0)
	defaultSSEHeartbeatInterval = 15 * time.Second
	sseFlushBatchSize           = 10
	sseFlushMaxDelay            = 1 * time.Millisecond
	sseRetentionSweepTimeout    = 5 * time.Second
	sseRetentionSweepEvery      = time.Minute
	ssePersistentReplayLimit    = eventBusRingSize
	ssePersistentRetention      = eventBusRingSize * 10
)

var sseActiveConnections atomic.Int64

type sseOperationResult string

const (
	sseResultOK              sseOperationResult = "ok"
	sseResultError           sseOperationResult = "error"
	sseResultInitialSync     sseOperationResult = "initial_sync"
	sseResultSeqTooOld       sseOperationResult = "seq_too_old"
	sseResultServerRestart   sseOperationResult = "server_restart"
	sseResultNoHistory       sseOperationResult = "no_history"
	sseResultStructural      sseOperationResult = "structural_change"
	sseResultClientCancelled sseOperationResult = "client_cancelled"
	sseResultServerClosed    sseOperationResult = "server_closed"
)

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
	return s.events.get(s.sseTenantKey(r))
}

func (s *Server) sseTenantKey(r *http.Request) string {
	scope := ScopeFromContext(r.Context())
	if scope != nil {
		if scope.TenantID == "" || (s != nil && s.fallback != nil && scope.Backend == s.fallback) {
			return ""
		}
		return scope.TenantID
	}
	// Single-tenant / fallback mode.
	return ""
}

func eventMutationContext(r *http.Request) (context.Context, *datastore.FSEventCollector) {
	collector := datastore.NewFSEventCollector()
	ctx := datastore.WithFSEventActor(r.Context(), r.Header.Get("X-Dat9-Actor"))
	ctx = datastore.WithFSEventCollector(ctx, collector)
	return ctx, collector
}

func (s *Server) publishCollectedEvents(r *http.Request, collector *datastore.FSEventCollector) {
	if collector == nil {
		return
	}
	for _, ev := range collector.Events() {
		recordSSEOperation("persist", sseResultOK, time.Time{})
		s.publishDurableEvent(r, ev)
	}
}

func (s *Server) publishDurableEvent(r *http.Request, ev datastore.FSEvent) {
	publishStart := time.Now()
	s.tenantEventBus(r).PublishEvent(ChangeEvent{
		Seq:   ev.Seq,
		Path:  ev.Path,
		Op:    ev.Op,
		Actor: ev.Actor,
		Ts:    ev.Ts,
	})
	recordSSEOperation("publish", sseResultOK, publishStart)
	if b := backendFromRequest(r); b != nil && b.Store() != nil && s.sseRetention != nil {
		s.sseRetention.MaybeSweep(r.Context(), sseRetentionTenantKey(r), b.Store(), ev.Seq)
	}
}

type sseRetentionSweeper struct {
	mu          sync.Mutex
	last        map[string]time.Time
	running     map[string]bool
	retention   uint64
	minInterval time.Duration
	timeout     time.Duration
	now         func() time.Time
}

func newSSERetentionSweeper() *sseRetentionSweeper {
	return &sseRetentionSweeper{
		last:        make(map[string]time.Time),
		running:     make(map[string]bool),
		retention:   ssePersistentRetention,
		minInterval: sseRetentionSweepEvery,
		timeout:     sseRetentionSweepTimeout,
		now:         time.Now,
	}
}

func sseRetentionTenantKey(r *http.Request) string {
	if scope := ScopeFromContext(r.Context()); scope != nil {
		return scope.TenantID
	}
	return ""
}

func (w *sseRetentionSweeper) MaybeSweep(ctx context.Context, tenantKey string, store *datastore.Store, headSeq uint64) {
	if w == nil || store == nil || w.retention == 0 || headSeq <= w.retention {
		return
	}
	now := w.now()

	w.mu.Lock()
	if w.running[tenantKey] {
		w.mu.Unlock()
		return
	}
	if last := w.last[tenantKey]; !last.IsZero() && now.Sub(last) < w.minInterval {
		w.mu.Unlock()
		return
	}
	w.running[tenantKey] = true
	w.last[tenantKey] = now
	w.mu.Unlock()

	go func() {
		defer func() {
			w.mu.Lock()
			w.running[tenantKey] = false
			w.mu.Unlock()
		}()
		sweepCtx, cancel := context.WithTimeout(backgroundWithTrace(ctx), w.timeout)
		defer cancel()
		w.sweepHead(sweepCtx, store, headSeq)
	}()
}

func (w *sseRetentionSweeper) sweepHead(ctx context.Context, store *datastore.Store, headSeq uint64) {
	if w == nil || w.retention == 0 || headSeq <= w.retention {
		return
	}
	w.sweepStore(ctx, store, headSeq-w.retention+1)
}

func (w *sseRetentionSweeper) sweepStore(ctx context.Context, store *datastore.Store, keepFromSeq uint64) {
	if store == nil || keepFromSeq == 0 {
		return
	}
	start := time.Now()
	if _, err := store.PruneFSEventsBefore(ctx, keepFromSeq); err != nil {
		recordSSEOperation("retention_sweep", sseResultError, start)
		logger.Warn(ctx, "sse_retention_sweep_failed", zap.Uint64("keep_from_seq", keepFromSeq), zap.Error(err))
		return
	}
	recordSSEOperation("retention_sweep", sseResultOK, start)
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
	tenantKey := s.sseTenantKey(r)
	s.sseCatchup.Register(tenantKey, bus, since)
	defer func() {
		bus.Unsubscribe(subID)
		s.sseCatchup.Unregister(tenantKey, bus)
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	recordSSEOperation("connect", sseResultOK, time.Time{})
	metrics.RecordGauge("sse", "active_connections", float64(sseActiveConnections.Add(1)))
	defer metrics.RecordGauge("sse", "active_connections", float64(sseActiveConnections.Add(-1)))

	ctx := r.Context()

	// Phase 1: Replay or Reset.
	// Prefer the durable event log when tenant storage is available. The
	// in-memory ring remains the live fan-out path and fallback for tests /
	// single-tenant setups without a scoped backend.
	events, headSeq, ok, replayReason := s.eventsSince(r.Context(), bStoreFromRequest(r), bus, since)
	bus.AdvanceSeq(headSeq)
	s.sseCatchup.AdvanceCursor(tenantKey, headSeq)
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
		recordSSEOperation("disconnect", sseResultClientCancelled, time.Time{})
		return
	}

	// Phase 2: Live stream with micro-batching.
	// Instead of flushing after every single event, we accumulate events
	// and flush at heartbeat boundaries or when the batch size is reached.
	heartbeatInterval := s.sseHeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultSSEHeartbeatInterval
	}
	heartbeat := time.NewTicker(heartbeatInterval)
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
			recordSSEOperation("disconnect", sseResultClientCancelled, time.Time{})
			return
		case <-heartbeat.C:
			sendSSEHeartbeat(bw, lastSeen)
			if err := bw.Flush(); err != nil {
				recordSSEOperation("disconnect", sseResultClientCancelled, time.Time{})
				return
			}
			if flushTimer != nil {
				stopTimer(flushTimer)
				flushC = nil
			}
		case <-flushC:
			if bw.count > 0 {
				if err := bw.Flush(); err != nil {
					recordSSEOperation("disconnect", sseResultClientCancelled, time.Time{})
					return
				}
			}
			flushC = nil
		case _, open := <-notify:
			if !open {
				recordSSEOperation("disconnect", sseResultServerClosed, time.Time{})
				return
			}
			liveEvents, liveHead, liveOK := bus.LiveEventsSince(lastSeen)
			if !liveOK {
				// Reset must be sent immediately; buffering it would stall
				// the client until the next heartbeat or unrelated event.
				sendSSEReset(bw, liveHead, sseResultSeqTooOld)
				lastSeen = liveHead
				if err := bw.Flush(); err != nil {
					recordSSEOperation("disconnect", sseResultClientCancelled, time.Time{})
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
							recordSSEOperation("disconnect", sseResultClientCancelled, time.Time{})
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

func (s *Server) eventsSince(ctx context.Context, store *datastore.Store, bus *EventBus, since uint64) ([]ChangeEvent, uint64, bool, sseOperationResult) {
	if store != nil {
		events, headSeq, ok, reason, err := persistentEventsSince(ctx, store, since)
		if err == nil {
			return events, headSeq, ok, reason
		}
		logger.Error(ctx, "server_event", eventFields(ctx, "sse_replay_persistent_failed", "error", err)...)
	}
	events, headSeq, ok := bus.EventsSince(since)
	if !ok {
		reason := sseResultInitialSync
		if since > 0 {
			reason = sseResultSeqTooOld
			if since > headSeq {
				reason = sseResultServerRestart
			}
		}
		return nil, headSeq, false, reason
	}
	return events, headSeq, true, ""
}

func persistentEventsSince(ctx context.Context, store *datastore.Store, since uint64) ([]ChangeEvent, uint64, bool, sseOperationResult, error) {
	start := time.Now()
	_, headSeq, count, err := store.FSEventBounds(ctx)
	if err != nil {
		recordSSEOperation("replay", sseResultError, start)
		return nil, 0, false, "", err
	}

	if since == 0 {
		recordSSEOperation("replay", sseResultInitialSync, start)
		return nil, headSeq, false, sseResultInitialSync, nil
	}
	if count == 0 {
		recordSSEOperation("replay", sseResultNoHistory, start)
		return nil, headSeq, false, sseResultNoHistory, nil
	}
	if since > headSeq {
		recordSSEOperation("replay", sseResultServerRestart, start)
		return nil, headSeq, false, sseResultServerRestart, nil
	}
	if since == maxUint64 {
		recordSSEOperation("replay", sseResultServerRestart, start)
		return nil, headSeq, false, sseResultServerRestart, nil
	}
	if since == headSeq {
		recordSSEOperation("replay", sseResultOK, start)
		return nil, headSeq, true, "", nil
	}

	events, err := store.ListFSEventsSince(ctx, since, ssePersistentReplayLimit+1)
	if err != nil {
		recordSSEOperation("replay", sseResultError, start)
		return nil, 0, false, "", err
	}
	if len(events) > ssePersistentReplayLimit {
		recordSSEOperation("replay", sseResultSeqTooOld, start)
		return nil, headSeq, false, sseResultSeqTooOld, nil
	}
	if len(events) == 0 {
		recordSSEOperation("replay", sseResultSeqTooOld, start)
		return nil, headSeq, false, sseResultSeqTooOld, nil
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
	recordSSEOperation("replay", sseResultOK, start)
	return out, headSeq, true, "", nil
}

func recordSSEOperation(operation string, result sseOperationResult, start time.Time) {
	var d time.Duration
	if !start.IsZero() {
		d = time.Since(start)
	}
	metrics.RecordOperation("sse", operation, string(result), d)
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
	if ev.Op == eventBusForceResetOp {
		sendSSEReset(w, ev.Seq, sseResultSeqTooOld)
		return
	}
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
		Reason: string(sseResultStructural),
		Path:   ev.Path,
		Op:     ev.Op,
		Actor:  ev.Actor,
	})
	if _, err := fmt.Fprintf(w, "event: reset\ndata: %s\n\n", data); err == nil {
		w.recordWrite()
	}
}

func sendSSEReset(w *sseBufferedWriter, seq uint64, reason sseOperationResult) {
	data, _ := json.Marshal(sseResetPayload{
		Seq:    seq,
		Reason: string(reason),
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
