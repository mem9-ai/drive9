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

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	sseHeartbeatInterval = 30 * time.Second
	sseFlushBatchSize    = 10
	sseFlushMaxDelay     = 1 * time.Millisecond

	// sseReplayHeartbeatInterval bounds how long a Phase-1 backlog drain may
	// stream without an interleaved heartbeat; LB and client idle timeouts
	// would otherwise kill multi-minute replays.
	sseReplayHeartbeatInterval = 5 * time.Second
	// sseCatchupPollDelay is the re-arm delay of the Phase-2 catch-up timer:
	// when one wake hits the full-page cap, the stream flushes and polls again
	// after this delay instead of waiting for the next notify signal.
	sseCatchupPollDelay = 10 * time.Millisecond

	// fsEventsSweepInterval throttles the lazy fs_events retention sweep on
	// the write path to at most once per hour per tenant.
	fsEventsSweepInterval = 1 * time.Hour
	// fsEventsSweepBatchSize caps rows deleted per DELETE statement in one
	// retention sweep; fsEventsSweepMaxBatches caps statements per sweep.
	// Leftover rows drain on the next sweep.
	fsEventsSweepBatchSize  = 5000
	fsEventsSweepMaxBatches = 20
	// fsEventsLazySweepTimeout bounds one detached lazy sweep so a slow
	// tenant DB cannot leak the goroutine.
	fsEventsLazySweepTimeout = 5 * time.Minute

	// sseEventsRoute is the SSE change-notification stream endpoint. It is the
	// only SSE route today; observe uses this constant (plus the
	// sseStreamEstablished context flag) to distinguish real SSE connection
	// lifetimes from bounded error responses on the same route.
	sseEventsRoute = "/v1/events"
)

var (
	// sseMaxFullPagesPerWake caps how many consecutive full event pages a
	// single Phase-2 wake drains before flushing and re-arming the catch-up
	// timer, so heartbeats and disconnects are serviced between bursts. A var
	// (not a const) so tests can exercise the cap with a small page size.
	sseMaxFullPagesPerWake = 10

	// eventsSinceE is the Phase-1 drain's event source. A package var so
	// tests can inject a mid-drain query error deterministically (mirrors the
	// tenantWorkerUsesTiDBAutoEmbedding seam).
	eventsSinceE = (*EventBus).EventsSinceE
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
	return ebs.getWithOrg(tenantID, "", store)
}

func (ebs *eventBuses) getWithOrg(tenantID, tidbCloudOrgID string, store *datastore.Store) *EventBus {
	ebs.mu.Lock()
	defer ebs.mu.Unlock()
	if bus, ok := ebs.buses[tenantID]; ok {
		bus.SetMetricOrgID(tidbCloudOrgID)
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
	bus := NewEventBusWithOrg(tenantID, tidbCloudOrgID, store)
	ebs.buses[tenantID] = bus
	return bus
}

// getIfExists returns the EventBus for tenantID if one already exists, or nil
// if no bus has been created for that tenant. Unlike get, it does NOT create a
// new bus. Used by the notifyPoller to wake only tenants that have local SSE
// subscribers — skipping idle tenants avoids touching their TiDB.
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
		tidbCloudOrgID := scope.TiDBCloudOrgID
		if tidbCloudOrgID == "" {
			tidbCloudOrgID = scope.Backend.TiDBCloudOrgID()
		}
		return s.events.getWithOrg(scope.TenantID, tidbCloudOrgID, scope.Backend.Store())
	}
	// Single-tenant / fallback mode.
	var store *datastore.Store
	if s.fallback != nil {
		store = s.fallback.Store()
	}
	return s.events.getWithOrg("", "", store)
}

func (s *Server) publishEvent(r *http.Request, path, op string) {
	actor := r.Header.Get("X-Dat9-Actor")
	bus := s.tenantEventBus(r)
	ctx := r.Context()
	// Step 1: Insert the durable event row into the per-tenant fs_events table.
	// This is the authoritative event content (path/op/actor/ts) and the source
	// of the monotonic seq cursor. Best-effort: a failed INSERT never fails the
	// enclosing mutation — the event is enqueued into the per-pod retry buffer
	// (event_retry.go) and flushed with backoff; the buffer's second wake then
	// delivers it to SSE clients. Buffer overflow is counted per tenant as hard
	// loss. Local SSE clients are NOT durably served on this path: the notify
	// channel below wakes them once, their poll finds no new row (caught up),
	// and actual delivery waits for the retry buffer's second wake
	// (bus.Publish after a successful flush). FUSE correctness is maintained
	// by HEAD revalidation regardless.
	// For existing tenants without the fs_events table (pre-migration), the
	// INSERT will fail until EnsureTiDBSchemaForAutoEmbeddingProfile creates the
	// table (triggered automatically by the CRC32 schema version bump).
	ts := time.Now().UnixMilli()
	var seq int64
	store := bus.store.Load()
	if store != nil {
		var err error
		seq, err = store.InsertFSEvent(ctx, path, op, actor, ts)
		if err != nil {
			logger.Warn(ctx, "sse_publish_fs_event_insert_failed",
				zap.String("tenant_id", bus.tenantID),
				zap.String("path", path),
				zap.String("op", op),
				zap.Error(err))
			metrics.RecordTenantOperationWithOrg(bus.tenantID, bus.TiDBCloudOrgID(), "event_bus", "publish", metrics.ResultForError(err), 0)
			if s.eventRetry != nil {
				s.eventRetry.enqueue(bus, path, op, actor, ts)
			}
		} else {
			// Successful insert: lazily sweep expired rows, throttled per
			// tenant. Writes are the only operation that both creates rows and
			// guarantees the tenant DB is already awake, so the sweep rides the
			// write path instead of a periodic task.
			s.maybeSweepFSEvents(ctx, bus, store)
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

// maybeSweepFSEvents runs the fs_events retention sweep for the tenant at most
// once per fsEventsSweepInterval, on a detached background goroutine so it
// never adds mutation latency. Over-retention has no correctness cost (a
// consumer whose cursor is older than the retention simply gets a longer
// replay), so leftover rows and failures are fine: failures are metrics + warn
// logs only, and rows beyond the batch cap drain on the next sweep.
func (s *Server) maybeSweepFSEvents(ctx context.Context, bus *EventBus, store *datastore.Store) {
	now := time.Now().Unix()
	last := bus.lastSweepUnix.Load()
	if now-last < int64(fsEventsSweepInterval/time.Second) {
		return
	}
	if !bus.lastSweepUnix.CompareAndSwap(last, now) {
		return
	}
	retention := s.fsEventsRetention
	if retention <= 0 {
		// Hand-constructed servers (tests) may skip NewWithConfig defaults.
		retention = defaultFSEventsRetention
	}
	go func() {
		// Detach from the request context: a client disconnect must not abort
		// the sweep, but the sweep must not outlive a slow tenant DB either.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fsEventsLazySweepTimeout)
		defer cancel()
		deleted, hasMore, err := store.DeleteFSEventsBefore(ctx, time.Now().Add(-retention), fsEventsSweepBatchSize, fsEventsSweepMaxBatches)
		if err != nil {
			logger.Warn(ctx, "fs_events_lazy_sweep_failed",
				zap.String("tenant_id", bus.tenantID),
				zap.Error(err))
			metrics.RecordTenantOperationWithOrg(bus.tenantID, bus.TiDBCloudOrgID(), "event_bus", "retention_sweep", metrics.ResultForError(err), 0)
			return
		}
		metrics.RecordTenantOperationWithOrg(bus.tenantID, bus.TiDBCloudOrgID(), "event_bus", "retention_sweep", "ok", 0)
		metrics.RecordFSEventsPruned(deleted)
		if hasMore {
			// Batch cap hit with leftover rows: they drain on the next
			// write-path sweep or tenant-worker maintenance cycle.
			logger.Info(ctx, "fs_events_lazy_sweep_has_more",
				zap.String("tenant_id", bus.tenantID),
				zap.Int64("deleted", deleted))
		}
	}()
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
	tidbCloudOrgID := bus.TiDBCloudOrgID()
	connStart := time.Now()
	// Track SSE inflight and connection lifetime. The inflight count is
	// derived from the EventBus listener set (adjusted in Subscribe/
	// Unsubscribe); here we record the connection lifecycle into the
	// dedicated SSE metrics (NOT the HTTP duration histogram — see the
	// route guard in observe).
	defer func() {
		metrics.RecordSSEConnectionWithOrg(tenantID, tidbCloudOrgID, "closed", time.Since(connStart))
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
	// The FIRST call uses the tolerated wrapper (query error → caught-up
	// fallback): pre-migration tenants without the fs_events table must not
	// reconnect-loop, and FUSE HEAD revalidation provides correctness without
	// SSE. Mid-drain calls below use eventsSinceE instead, because a masked
	// error there would falsely mark a still-behind client as caught up.
	phase1Start := time.Now()
	events, headSeq, ok := bus.EventsSince(ctx, since)
	lastSeen := since
	// lastSuccessfulPoll tracks the last EventsSince that returned ok=true;
	// the optional Phase-2 liveness poll uses it to detect signal loss.
	lastSuccessfulPoll := time.Now()
	lastHeartbeat := lastSuccessfulPoll

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
		metrics.RecordSSEResetSentWithOrg(tenantID, tidbCloudOrgID, reason)
		lastSeen = headSeq
	} else {
		// Drain the replay backlog: one EventsSince call returns at most
		// eventPageSize rows, so loop until a short page signals the end.
		// Flush after each full batch and interleave a heartbeat when the
		// drain runs long so LB/client idle timeouts don't kill the replay.
		for {
			for _, ev := range events {
				sendSSEEvent(bw, ev)
				if isStructuralOp(ev.Op) {
					// Structural ops are emitted as reset events (see sendSSEEvent),
					// so count them as resets, not file_changed deliveries.
					metrics.RecordSSEResetSentWithOrg(tenantID, tidbCloudOrgID, "structural_change")
				} else {
					metrics.RecordSSEEventSentWithOrg(tenantID, tidbCloudOrgID, ev.Op)
				}
				lastSeen = ev.Seq
			}
			if len(events) < eventPageSize {
				break // backlog drained
			}
			if err := bw.Flush(); err != nil {
				return
			}
			if time.Since(lastHeartbeat) > sseReplayHeartbeatInterval {
				sendSSEHeartbeat(bw, lastSeen)
				metrics.RecordSSEHeartbeatSentWithOrg(tenantID, tidbCloudOrgID)
				lastHeartbeat = time.Now()
				if err := bw.Flush(); err != nil {
					return
				}
			}
			var qErr error
			events, headSeq, ok, qErr = eventsSinceE(bus, ctx, lastSeen)
			if qErr != nil {
				// Mid-drain query error: the tolerated "caught up" fallback
				// would be a LIE here — the client already consumed earlier
				// pages and is still behind. Terminate the stream WITHOUT the
				// end-of-replay heartbeat; the client reconnects and resumes
				// from its durable cursor (the last seq it already saw).
				logger.Warn(ctx, "sse_phase1_drain_query_failed",
					zap.String("tenant_id", tenantID),
					zap.Uint64("last_seen", lastSeen),
					zap.Error(qErr))
				metrics.RecordTenantOperationWithOrg(tenantID, tidbCloudOrgID, "event_bus", "phase1_drain", metrics.ResultForError(qErr), 0)
				return
			}
			if !ok {
				// The cursor went stale mid-drain (a retention sweep raced the
				// replay): same reset contract as the initial ok=false path.
				sendSSEReset(bw, headSeq, "seq_too_old")
				metrics.RecordSSEResetSentWithOrg(tenantID, tidbCloudOrgID, "seq_too_old")
				lastSeen = headSeq
				break
			}
			lastSuccessfulPoll = time.Now()
		}
	}
	// End the initial replay/reset phase with an immediate heartbeat so
	// clients have an explicit stream-current marker. This lets caches that
	// were marked unverified on disconnect become verified without waiting
	// for the periodic heartbeat.
	sendSSEHeartbeat(bw, lastSeen)
	metrics.RecordSSEHeartbeatSentWithOrg(tenantID, tidbCloudOrgID)
	// Flush initial replay/reset immediately so the client receives the
	// cursor position without waiting for the periodic heartbeat.
	if err := bw.Flush(); err != nil {
		return
	}
	metrics.RecordSSEPhase1WithOrg(tenantID, tidbCloudOrgID, time.Since(phase1Start))

	// Phase 2: Live stream with micro-batching.
	// The notify channel catches same-pod events instantly; cross-pod events
	// are discovered by the tenantOutboxPoller (meta DB only) and delivered via
	// the same channel — no per-connection poll ticker needed. Delivery of a
	// full event page re-polls immediately (event-driven drain) so a backlog
	// larger than one page cannot stall until the next write.
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	// Optional liveness poll (default off): covers signal loss — e.g. a
	// meta-DB outage in which outbox rows are never written — for connected
	// clients only. When the ticker fires and no EventsSince has succeeded
	// within the interval, poll once. It never touches tenant DBs of tenants
	// without a live SSE connection.
	var livenessC <-chan time.Time
	var livenessTicker *time.Ticker
	if s.sseLivenessPollInterval > 0 {
		livenessTicker = time.NewTicker(s.sseLivenessPollInterval)
		livenessC = livenessTicker.C
		defer livenessTicker.Stop()
	}

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

	// Catch-up timer for the event-driven drain: when one wake hits the
	// full-page cap, pollAndSend flushes and re-arms this short timer instead
	// of waiting for the next notify signal (the outbox coalescer emits only
	// one signal per tenant per 200ms window, so a >cap burst would otherwise
	// stall its tail until the next write). Only a short page disarms it.
	var catchupTimer *time.Timer
	var catchupC <-chan time.Time
	defer func() {
		if catchupTimer != nil {
			stopTimer(catchupTimer)
			catchupC = nil
		}
	}()
	armCatchup := func() {
		if catchupTimer == nil {
			catchupTimer = time.NewTimer(sseCatchupPollDelay)
		} else {
			stopTimer(catchupTimer)
			catchupTimer.Reset(sseCatchupPollDelay)
		}
		catchupC = catchupTimer.C
	}
	disarmCatchup := func() {
		if catchupTimer != nil {
			stopTimer(catchupTimer)
		}
		catchupC = nil
	}

	// pollAndSend queries fs_events for new rows since lastSeen and streams them.
	// A full page (eventPageSize events) means the backlog likely continues, so
	// it polls again immediately — up to sseMaxFullPagesPerWake consecutive full
	// pages per wake, then flushes and re-arms the catch-up timer; a short page
	// disarms. Returns false if the stream should terminate (write error).
	pollAndSend := func() bool {
		capped := false
		for fullPages := 0; ; {
			liveEvents, liveHead, liveOK := bus.EventsSince(ctx, lastSeen)
			if !liveOK {
				sendSSEReset(bw, liveHead, "seq_too_old")
				metrics.RecordSSEResetSentWithOrg(tenantID, tidbCloudOrgID, "seq_too_old")
				lastSeen = liveHead
				if err := bw.Flush(); err != nil {
					return false
				}
				if flushTimer != nil {
					stopTimer(flushTimer)
					flushC = nil
				}
				// A reset jumps the cursor to head: no backlog left to chase.
				disarmCatchup()
				return true
			}
			lastSuccessfulPoll = time.Now()
			for _, ev := range liveEvents {
				sendSSEEvent(bw, ev)
				if isStructuralOp(ev.Op) {
					metrics.RecordSSEResetSentWithOrg(tenantID, tidbCloudOrgID, "structural_change")
				} else {
					metrics.RecordSSEEventSentWithOrg(tenantID, tidbCloudOrgID, ev.Op)
				}
				lastSeen = ev.Seq
			}
			if len(liveEvents) == eventPageSize {
				fullPages++
				if fullPages < sseMaxFullPagesPerWake {
					// More rows likely pending: flush what is buffered and poll
					// again immediately instead of waiting for the next notify.
					if bw.count > 0 {
						if err := bw.Flush(); err != nil {
							return false
						}
						if flushTimer != nil {
							stopTimer(flushTimer)
							flushC = nil
						}
					}
					continue
				}
				// Per-wake cap reached: fall through to the normal flush path,
				// then re-arm the catch-up timer to resume the drain.
				capped = true
			}
			break
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
		if capped {
			armCatchup()
		} else {
			disarmCatchup()
		}
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			sendSSEHeartbeat(bw, lastSeen)
			metrics.RecordSSEHeartbeatSentWithOrg(tenantID, tidbCloudOrgID)
			if err := bw.Flush(); err != nil {
				return
			}
			if flushTimer != nil {
				stopTimer(flushTimer)
				flushC = nil
			}
		case <-livenessC:
			if time.Since(lastSuccessfulPoll) >= s.sseLivenessPollInterval {
				if !pollAndSend() {
					return
				}
			}
		case <-catchupC:
			// A previous wake hit the full-page cap: resume the drain. The
			// select loop serviced heartbeats and disconnects in between.
			catchupC = nil
			if !pollAndSend() {
				return
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
