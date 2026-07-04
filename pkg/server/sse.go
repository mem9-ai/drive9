package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	sseHeartbeatInterval = 30 * time.Second
	sseFlushBatchSize    = 10
	sseFlushMaxDelay     = 1 * time.Millisecond

	// sseEventsRoute is the SSE change-notification stream endpoint. It is the
	// only SSE route today; observe uses this constant (plus the
	// sseStreamEstablished context flag) to distinguish real SSE connection
	// lifetimes from bounded error responses on the same route.
	sseEventsRoute = "/v1/events"

	// sseNotifyInternalRoute is the pod-to-pod SSE push endpoint. It receives
	// a lightweight {tenant_id, seq} POST from a peer pod that just wrote an
	// fs_events row, so this pod can wake its local SSE subscribers for that
	// tenant without waiting for the 200ms notifyPoller fallback. Registered
	// directly on the mux (not through the tenant auth middleware chain) and
	// authenticated via a shared internal bearer secret.
	sseNotifyInternalRoute = "/v1/internal/sse-notify"
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

func (ebs *eventBuses) get(tenantID string, store *datastore.Store) *EventBus {
	ebs.mu.Lock()
	defer ebs.mu.Unlock()
	if bus, ok := ebs.buses[tenantID]; ok {
		// Refresh the store reference if a non-nil store is provided: the pool
		// may have invalidated and recreated the backend (closing the old store
		// and opening a new one), so the cached bus's store could be stale/closed.
		// Don't overwrite with nil — that would break a bus that already has a
		// valid store (e.g. when tenantEventBus can't resolve a backend but the
		// bus was previously initialized with one).
		if store != nil {
			bus.SetStore(store)
		}
		return bus
	}
	bus := NewEventBus(tenantID, store)
	ebs.buses[tenantID] = bus
	return bus
}

// getIfExists returns the EventBus for tenantID if one already exists, or nil
// if no bus has been created for that tenant. Unlike get, it does NOT create a
// new bus. Used by the notifyPoller and pod notifier to wake only tenants that
// have local SSE subscribers — skipping idle tenants avoids touching their TiDB.
func (ebs *eventBuses) getIfExists(tenantID string) *EventBus {
	ebs.mu.RLock()
	defer ebs.mu.RUnlock()
	return ebs.buses[tenantID]
}

// activeTenantIDs returns the tenant IDs for which this pod has EventBus
// instances (and thus potentially SSE subscribers). Used by the pod registry
// to periodically report this pod's subscription set to the central DB so
// writers can route push notifications to the right pods.
func (ebs *eventBuses) activeTenantIDs() []string {
	ebs.mu.RLock()
	defer ebs.mu.RUnlock()
	ids := make([]string, 0, len(ebs.buses))
	for id, bus := range ebs.buses {
		// Only report buses that currently have listeners. A bus without
		// listeners means all SSE connections for that tenant disconnected;
		// reporting it would cause peers to push notifications to a pod that
		// has no one to deliver them to.
		if bus.HasListeners() {
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *Server) tenantEventBus(r *http.Request) *EventBus {
	scope := ScopeFromContext(r.Context())
	if scope != nil && scope.Backend != nil {
		return s.events.get(scope.TenantID, scope.Backend.Store())
	}
	// Single-tenant / fallback mode.
	var store *datastore.Store
	if s.fallback != nil {
		store = s.fallback.Store()
	}
	return s.events.get("", store)
}

func (s *Server) publishEvent(r *http.Request, path, op string) {
	actor := r.Header.Get("X-Dat9-Actor")
	bus := s.tenantEventBus(r)
	ctx := r.Context()
	// Step 1: Insert the durable event row into the per-tenant fs_events table.
	// This is the authoritative event content (path/op/actor/ts) and the source
	// of the monotonic seq cursor. Best-effort: if the INSERT fails (network
	// blip, table missing pre-migration, conn pool exhaustion), the event is
	// lost for cross-pod SSE clients — they won't see it via the outbox or push
	// since there's no DB row. However, local SSE clients still get instant
	// delivery via the notify channel below (their EventsSince will find no new
	// rows but the notify signal wakes them to re-check, returning empty =
	// caught up). FUSE correctness is maintained by HEAD revalidation regardless.
	// For existing tenants without the fs_events table (pre-migration), the
	// INSERT will fail until EnsureTiDBSchemaForAutoEmbeddingProfile creates the
	// table (triggered automatically by the CRC32 schema version bump).
	var seq int64
	store := bus.store.Load()
	if store != nil {
		var err error
		seq, err = store.InsertFSEvent(ctx, path, op, actor, time.Now().UnixMilli())
		if err != nil {
			logger.Warn(ctx, "sse_publish_fs_event_insert_failed",
				zap.String("tenant_id", bus.tenantID),
				zap.String("path", path),
				zap.String("op", op),
				zap.Error(err))
			metrics.RecordTenantOperation(bus.tenantID, "event_bus", "publish", metrics.ResultForError(err), 0)
			metrics.RecordEventBusPublishError(bus.tenantID)
		}
	}

	// Step 2: Write a lightweight pointer to the central tenant_notify_outbox
	// table (in the always-provisioned meta DB) with the SSE work bit set. This
	// lets other pods discover that tenant T has new fs_events rows via the
	// 200ms unified outbox poller, without polling the tenant's own TiDB.
	// Best-effort: if this fails, SSE client reconnect replay is the ultimate
	// fallback. Only write if we got a valid seq from step 1. The helper uses
	// a non-cancelable background context so a client disconnect after the
	// fs_events write doesn't abort the outbox pointer.
	if seq > 0 {
		s.insertTenantNotify(bus.tenantID, WorkSSE)
	}

	// Step 3: Wake same-pod SSE subscribers instantly (in-memory, sub-ms).
	bus.Publish()
}

// notifyPushRequest is the JSON body for POST /v1/internal/sse-notify. This
// legacy pod-to-pod push endpoint is retained for backward compatibility; the
// unified outbox poller is the primary cross-pod delivery path.
type notifyPushRequest struct {
	TenantID string `json:"tenant_id"`
	Seq      uint64 `json:"seq"`
}

// handleInternalSSENotify receives a pod-to-pod push notification from a
// peer pod. The peer just wrote an fs_events row for tenant_id and is
// notifying this pod (which has SSE subscribers for that tenant) to wake them
// immediately rather than waiting for the 200ms unified outbox poller.
//
// Authenticated via a shared internal bearer secret (PodNotifySecret) compared
// in constant time. Registered directly on the mux, NOT through the tenant auth
// middleware — this endpoint has no tenant scope.
//
// Fire-and-forget semantics: the peer does not wait for a response. If this
// pod's bus for the tenant no longer exists (all subscribers disconnected),
// Publish is a no-op. EventsSince on the next wake reads the actual event
// content from the tenant's fs_events table.
func (s *Server) handleInternalSSENotify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	// Validate the shared internal secret. If no secret is configured, the
	// internal endpoint is disabled (404-equivalent: reject all requests).
	if len(s.podNotifySecret) == 0 {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	tok := bearerToken(r)
	if tok == "" || subtle.ConstantTimeCompare([]byte(tok), s.podNotifySecret) != 1 {
		errJSON(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req notifyPushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.TenantID == "" {
		errJSON(w, http.StatusBadRequest, "missing tenant_id")
		return
	}
	// Wake local SSE subscribers for this tenant. If no bus exists (no local
	// subscribers), this is a no-op — the peer's route table was slightly stale.
	// The tenant's TiDB is NOT queried here; the SSE handler will call
	// EventsSince only when it actually has a subscriber to deliver to.
	if bus := s.events.getIfExists(req.TenantID); bus != nil {
		bus.Publish()
	}
	w.WriteHeader(http.StatusNoContent)
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
	tenantID := bus.tenantID
	connStart := time.Now()
	// Track SSE inflight and connection lifetime. The inflight count is
	// derived from the EventBus listener set (adjusted in Subscribe/
	// Unsubscribe); here we record the connection lifecycle into the
	// dedicated SSE metrics (NOT the HTTP duration histogram — see the
	// route guard in observe).
	defer func() {
		metrics.RecordSSEConnection(tenantID, "closed", time.Since(connStart))
	}()

	subID, notify := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	// Mark the stream as established so observe treats this as a real SSE
	// connection lifetime (skip HTTP duration histogram, record SSE metrics
	// instead of normal HTTP/tenant durations). Bounded error responses that
	// return before this point are still recorded as normal HTTP requests.
	markSSEStreamEstablished(r.Context())

	ctx := r.Context()

	// Phase 1: Replay or Reset.
	// EventsSince reads from the durable fs_events table, so events written
	// by other pods are visible here (cross-pod propagation).
	phase1Start := time.Now()
	events, headSeq, ok := bus.EventsSince(ctx, since)
	lastSeen := since

	bw := newSSEBufferedWriter(w, flusher)

	if !ok {
		reason := "initial_sync"
		if since > 0 {
			reason = "seq_too_old"
			if since > headSeq {
				reason = "server_restart"
			}
		}
		sendSSEReset(bw, headSeq, reason)
		metrics.RecordSSEResetSent(tenantID, reason)
		lastSeen = headSeq
	} else {
		for _, ev := range events {
			sendSSEEvent(bw, ev)
			if isStructuralOp(ev.Op) {
				// Structural ops are emitted as reset events (see sendSSEEvent),
				// so count them as resets, not file_changed deliveries.
				metrics.RecordSSEResetSent(tenantID, "structural_change")
			} else {
				metrics.RecordSSEEventSent(tenantID, ev.Op)
			}
			lastSeen = ev.Seq
		}
	}
	// End the initial replay/reset phase with an immediate heartbeat so
	// clients have an explicit stream-current marker. This lets caches that
	// were marked unverified on disconnect become verified without waiting
	// for the periodic heartbeat.
	sendSSEHeartbeat(bw, lastSeen)
	metrics.RecordSSEHeartbeatSent(tenantID)
	// Flush initial replay/reset immediately so the client receives the
	// cursor position without waiting for the periodic heartbeat.
	if err := bw.Flush(); err != nil {
		return
	}
	metrics.RecordSSEPhase1(tenantID, time.Since(phase1Start))

	// Phase 2: Live stream with micro-batching.
	// The notify channel catches same-pod events instantly. A 1s poll ticker
	// catches cross-pod events (writes on other pods that inserted into the
	// shared fs_events table). The EventBus's per-bus poll goroutine handles
	// cross-pod discovery and signals via the notify channel — no per-connection
	// poll ticker needed (P1-6 optimization).
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

	// pollAndSend queries fs_events for new rows since lastSeen and streams them.
	// Returns false if the stream should terminate (write error or reset sent).
	pollAndSend := func() bool {
		liveEvents, liveHead, liveOK := bus.EventsSince(ctx, lastSeen)
		if !liveOK {
			sendSSEReset(bw, liveHead, "seq_too_old")
			metrics.RecordSSEResetSent(tenantID, "seq_too_old")
			lastSeen = liveHead
			if err := bw.Flush(); err != nil {
				return false
			}
			if flushTimer != nil {
				stopTimer(flushTimer)
				flushC = nil
			}
			return true
		}
		for _, ev := range liveEvents {
			sendSSEEvent(bw, ev)
			if isStructuralOp(ev.Op) {
				metrics.RecordSSEResetSent(tenantID, "structural_change")
			} else {
				metrics.RecordSSEEventSent(tenantID, ev.Op)
			}
			lastSeen = ev.Seq
		}
		if bw.count > 0 {
			if bw.shouldFlush() {
				if err := bw.Flush(); err != nil {
					return false
				}
				if flushTimer != nil {
					stopTimer(flushTimer)
					flushC = nil
				}
			} else if flushC == nil {
				if flushTimer == nil {
					flushTimer = time.NewTimer(sseFlushMaxDelay)
				} else {
					stopTimer(flushTimer)
					flushTimer.Reset(sseFlushMaxDelay)
				}
				flushC = flushTimer.C
			}
		}
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			sendSSEHeartbeat(bw, lastSeen)
			metrics.RecordSSEHeartbeatSent(tenantID)
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
				return
			}
			if !pollAndSend() {
				return
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
