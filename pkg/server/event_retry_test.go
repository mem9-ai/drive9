package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

// fakeFSEventInserter implements fsEventInserter for unit tests.
type fakeFSEventInserter struct {
	mu    sync.Mutex
	calls []fakeInsertCall
	err   error
	seq   int64
}

type fakeInsertCall struct {
	path  string
	op    string
	actor string
	ts    int64
}

func (f *fakeFSEventInserter) InsertFSEvent(_ context.Context, path, op, actor string, ts int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeInsertCall{path: path, op: op, actor: actor, ts: ts})
	if f.err != nil {
		return 0, f.err
	}
	f.seq++
	return f.seq, nil
}

// blockingFSEventInserter blocks inside InsertFSEvent until released, so
// tests can hold an entry in-flight deterministically.
type blockingFSEventInserter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingFSEventInserter() *blockingFSEventInserter {
	return &blockingFSEventInserter{started: make(chan struct{}, 1), release: make(chan struct{})}
}

func (f *blockingFSEventInserter) InsertFSEvent(_ context.Context, _, _, _ string, _ int64) (int64, error) {
	f.once.Do(func() { f.started <- struct{}{} })
	<-f.release
	return 1, nil
}

// notifyRecorder is a thread-safe record of second-wake notify calls.
type notifyRecorder struct {
	mu    sync.Mutex
	calls []notifyCall
}

type notifyCall struct {
	tenantID string
	workMask int
}

func (r *notifyRecorder) add(tenantID string, workMask int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, notifyCall{tenantID: tenantID, workMask: workMask})
}

func (r *notifyRecorder) forTenant(tenantID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.calls {
		if c.tenantID == tenantID {
			n++
		}
	}
	return n
}

func (r *notifyRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func newTestEventRetryBuffer() (*eventRetryBuffer, *notifyRecorder) {
	rec := &notifyRecorder{}
	buf := newEventRetryBuffer(rec.add, time.Hour, nil)
	return buf, rec
}

// markAllDue forces every buffered entry to be due for the next flushDue scan.
func markAllDue(buf *eventRetryBuffer) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	for _, e := range buf.entries {
		e.nextRetry = time.Time{}
	}
}

func bufferDepth(buf *eventRetryBuffer) int {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	return len(buf.entries)
}

// flushSync drives one scan and waits for the flushers it spawned (tests
// never start the run goroutine, so wg only counts those workers).
func flushSync(buf *eventRetryBuffer) {
	buf.flushDue(context.Background())
	buf.wg.Wait()
}

// TestEventRetryTenantCapDropsOldest verifies that hitting the per-tenant cap
// evicts that tenant's oldest entry and counts the drop per tenant.
func TestEventRetryTenantCapDropsOldest(t *testing.T) {
	buf, _ := newTestEventRetryBuffer()
	bus := NewEventBus("tenant-cap-tenant", nil)

	for i := 0; i < eventRetryTenantCap+1; i++ {
		buf.enqueue(bus, "/f.txt", "write", "", int64(i))
	}

	if got := bufferDepth(buf); got != eventRetryTenantCap {
		t.Fatalf("depth = %d, want %d", got, eventRetryTenantCap)
	}
	// The first enqueued entry (ts=0) must have been evicted; the oldest
	// remaining entry is ts=1.
	buf.mu.Lock()
	oldest := buf.entries[0]
	tenantCount := buf.tenantCounts["tenant-cap-tenant"]
	buf.mu.Unlock()
	if oldest.ts != 1 {
		t.Fatalf("oldest remaining entry ts = %d, want 1 (tenant's oldest dropped)", oldest.ts)
	}
	if tenantCount != eventRetryTenantCap {
		t.Fatalf("tenant count = %d, want %d", tenantCount, eventRetryTenantCap)
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_sse_event_retry_dropped_total{reason="tenant_cap",tenant_id="tenant-cap-tenant",tidbcloud_org_id="guest"} 1`) {
		t.Fatalf("expected per-tenant drop counter series:\n%s", rec.Body.String())
	}
}

// TestEventRetryGlobalCapDropsGloballyOldest verifies that hitting the global
// cap evicts the globally oldest entry, even when it belongs to a different
// tenant than the one being enqueued.
func TestEventRetryGlobalCapDropsGloballyOldest(t *testing.T) {
	buf, _ := newTestEventRetryBuffer()

	// Fill: 11 tenants × the per-tenant cap exceeds the global cap by 1000.
	const tenants = 11
	buses := make([]*EventBus, tenants)
	for i := 0; i < tenants; i++ {
		buses[i] = NewEventBus("global-cap-tenant-"+string(rune('a'+i)), nil)
		for j := 0; j < eventRetryTenantCap; j++ {
			buf.enqueue(buses[i], "/f.txt", "write", "", int64(j))
		}
	}

	if got := bufferDepth(buf); got != eventRetryGlobalCap {
		t.Fatalf("depth = %d, want %d", got, eventRetryGlobalCap)
	}
	buf.mu.Lock()
	t0Count := buf.tenantCounts["global-cap-tenant-a"]
	lastTenantCount := buf.tenantCounts["global-cap-tenant-k"]
	oldest := buf.entries[0]
	buf.mu.Unlock()
	// Tenant 'a' enqueued first, so its 1000 entries were the global-oldest
	// and were evicted one by one as tenant 'k' filled the buffer.
	if t0Count != 0 {
		t.Fatalf("tenant-a count = %d, want 0 (globally oldest evicted)", t0Count)
	}
	if lastTenantCount != eventRetryTenantCap {
		t.Fatalf("tenant-k count = %d, want %d", lastTenantCount, eventRetryTenantCap)
	}
	if oldest.tenantID != "global-cap-tenant-b" {
		t.Fatalf("oldest remaining entry tenant = %q, want global-cap-tenant-b", oldest.tenantID)
	}
}

// TestEventRetryInFlightResidencyAndEvictionSkip verifies that an in-flight
// entry stays counted against the caps (no cap evasion) and that eviction
// skips it: with the oldest entry blocked in-flight, hitting the per-tenant
// cap evicts the oldest NON-FLUSHING entry instead of the in-flight one.
func TestEventRetryInFlightResidencyAndEvictionSkip(t *testing.T) {
	buf, _ := newTestEventRetryBuffer()
	bus := NewEventBus("inflight-tenant", nil)
	blk := newBlockingFSEventInserter()
	buf.loadStore = func(*EventBus) fsEventInserter { return blk }

	for i := 0; i < eventRetryTenantCap; i++ {
		buf.enqueue(bus, "/f.txt", "write", "", int64(i))
	}
	// Make ONLY the oldest entry due; the flusher grabs it and blocks.
	buf.mu.Lock()
	buf.entries[0].nextRetry = time.Time{}
	buf.mu.Unlock()
	buf.flushDue(context.Background())
	<-blk.started // entry 0 is now in-flight

	// Enqueue one more: the tenant is at cap INCLUDING the in-flight entry,
	// so eviction must drop entry ts=1 (oldest non-flushing), not ts=0.
	buf.enqueue(bus, "/f.txt", "write", "", 1000)

	buf.mu.Lock()
	depth := len(buf.entries)
	first := buf.entries[0]
	sawTS1 := false
	for _, e := range buf.entries {
		if e.ts == 1 {
			sawTS1 = true
		}
	}
	buf.mu.Unlock()
	if depth != eventRetryTenantCap {
		t.Fatalf("depth = %d, want %d (in-flight entry must stay counted)", depth, eventRetryTenantCap)
	}
	if first.ts != 0 || !first.flushing {
		t.Fatalf("oldest entry = (ts=%d, flushing=%v), want (0, true) retained in-flight", first.ts, first.flushing)
	}
	if sawTS1 {
		t.Fatal("ts=1 should have been evicted as the oldest NON-FLUSHING entry")
	}

	// Unblock: the in-flight entry lands and is removed.
	close(blk.release)
	buf.wg.Wait()
	if got := bufferDepth(buf); got != eventRetryTenantCap-1 {
		t.Fatalf("depth after flush = %d, want %d", got, eventRetryTenantCap-1)
	}
}

// TestEventRetryPerTenantIsolation verifies that one tenant's hung flusher
// does not delay another tenant's flush (per-tenant workers, no cross-tenant
// head-of-line blocking).
func TestEventRetryPerTenantIsolation(t *testing.T) {
	buf, rec := newTestEventRetryBuffer()
	busA := NewEventBus("iso-tenant-a", nil)
	busB := NewEventBus("iso-tenant-b", nil)
	blk := newBlockingFSEventInserter()
	fast := &fakeFSEventInserter{}
	buf.loadStore = func(bus *EventBus) fsEventInserter {
		if bus.tenantID == "iso-tenant-a" {
			return blk
		}
		return fast
	}

	buf.enqueue(busA, "/a.txt", "write", "", 1)
	buf.enqueue(busB, "/b.txt", "write", "", 2)
	markAllDue(buf)
	buf.flushDue(context.Background())
	<-blk.started // tenant A's flusher is now blocked in-flight

	// Tenant B must flush promptly without waiting for A.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rec.forTenant("iso-tenant-b") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.forTenant("iso-tenant-b") != 1 {
		t.Fatal("healthy tenant B did not flush while tenant A was blocked (cross-tenant HOL)")
	}

	close(blk.release)
	buf.wg.Wait()
	if rec.forTenant("iso-tenant-a") != 1 {
		t.Fatal("tenant A did not flush after release")
	}
	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth = %d, want 0 after both tenants flushed", got)
	}
}

// TestEventRetryNilStoreRequeues verifies that an entry with no store (and no
// resolver) is requeued with backoff instead of being dropped or attempted.
func TestEventRetryNilStoreRequeues(t *testing.T) {
	buf, rec := newTestEventRetryBuffer()
	bus := NewEventBus("nil-store-tenant", nil) // default loadStore → nil

	buf.enqueue(bus, "/a.txt", "write", "actor1", 100)
	markAllDue(buf)
	flushSync(buf)

	if got := bufferDepth(buf); got != 1 {
		t.Fatalf("depth = %d, want 1 (entry requeued)", got)
	}
	buf.mu.Lock()
	e := buf.entries[0]
	buf.mu.Unlock()
	if e.attempts != 1 {
		t.Fatalf("attempts = %d, want 1", e.attempts)
	}
	if !e.nextRetry.After(time.Now()) {
		t.Fatalf("nextRetry = %v, want a future time (backoff)", e.nextRetry)
	}
	if rec.total() != 0 {
		t.Fatalf("notify calls = %d, want none on requeue", rec.total())
	}
}

// TestEventRetryFailedFlushRequeuesWithBackoff verifies that an insert error
// requeues the entry IN PLACE (queue position kept) with doubled backoff.
func TestEventRetryFailedFlushRequeuesWithBackoff(t *testing.T) {
	buf, rec := newTestEventRetryBuffer()
	fake := &fakeFSEventInserter{err: errors.New("db down")}
	buf.loadStore = func(bus *EventBus) fsEventInserter { return fake }
	bus := NewEventBus("fail-flush-tenant", nil)

	buf.enqueue(bus, "/a.txt", "write", "actor1", 100)

	for wantAttempts := 1; wantAttempts <= 2; wantAttempts++ {
		markAllDue(buf)
		before := time.Now()
		flushSync(buf)
		if got := bufferDepth(buf); got != 1 {
			t.Fatalf("depth after failed flush = %d, want 1", got)
		}
		buf.mu.Lock()
		e := buf.entries[0]
		buf.mu.Unlock()
		if e.attempts != wantAttempts {
			t.Fatalf("attempts = %d, want %d", e.attempts, wantAttempts)
		}
		wantBackoff := eventRetryBaseBackoff << wantAttempts
		if e.nextRetry.Before(before.Add(wantBackoff)) {
			t.Fatalf("nextRetry backoff too small after attempt %d: %v (base %v)", wantAttempts, e.nextRetry, before)
		}
	}
	if len(fake.calls) != 2 {
		t.Fatalf("insert attempts = %d, want 2", len(fake.calls))
	}
	if rec.total() != 0 {
		t.Fatalf("notify calls = %d, want none while flush fails", rec.total())
	}
}

// TestEventRetryFlushSuccessSecondWake verifies that a successful flush
// performs the mandatory second wake: insertTenantNotify(WorkSSE) and
// bus.Publish(), exactly like a fresh publishEvent.
func TestEventRetryFlushSuccessSecondWake(t *testing.T) {
	buf, rec := newTestEventRetryBuffer()
	fake := &fakeFSEventInserter{}
	buf.loadStore = func(bus *EventBus) fsEventInserter { return fake }
	bus := NewEventBus("flush-ok-tenant", nil)
	subID, notify := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	buf.enqueue(bus, "/a.txt", "write", "actor1", 100)
	markAllDue(buf)
	flushSync(buf)

	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth after successful flush = %d, want 0", got)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("insert calls = %d, want 1", len(fake.calls))
	}
	c := fake.calls[0]
	if c.path != "/a.txt" || c.op != "write" || c.actor != "actor1" || c.ts != 100 {
		t.Fatalf("insert call = %+v, want /a.txt write actor1 ts=100", c)
	}
	if rec.forTenant("flush-ok-tenant") != 1 {
		t.Fatalf("notify calls for tenant = %d, want 1 (WorkSSE second wake)", rec.forTenant("flush-ok-tenant"))
	}
	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("notify channel closed unexpectedly")
		}
	default:
		t.Fatal("expected bus.Publish signal after successful flush")
	}
}

// TestEventRetryExpiredEntryDropped verifies that entries older than the
// buffer's maxAge are dropped (counted as hard loss) without an insert
// attempt.
func TestEventRetryExpiredEntryDropped(t *testing.T) {
	buf, rec := newTestEventRetryBuffer()
	fake := &fakeFSEventInserter{}
	buf.loadStore = func(bus *EventBus) fsEventInserter { return fake }
	bus := NewEventBus("expired-tenant", nil)

	buf.enqueue(bus, "/a.txt", "write", "", 100)
	buf.mu.Lock()
	buf.entries[0].enqueuedAt = time.Now().Add(-2 * buf.maxAge)
	buf.entries[0].nextRetry = time.Time{}
	buf.mu.Unlock()

	flushSync(buf)

	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth = %d, want 0 (expired entry dropped)", got)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("insert calls = %d, want 0 for expired entry", len(fake.calls))
	}
	if rec.total() != 0 {
		t.Fatalf("notify calls = %d, want none for expired entry", rec.total())
	}

	mrec := httptest.NewRecorder()
	metrics.WritePrometheus(mrec)
	if !strings.Contains(mrec.Body.String(), `drive9_sse_event_retry_dropped_total{reason="expired",tenant_id="expired-tenant",tidbcloud_org_id="guest"} 1`) {
		t.Fatalf("expected expired drop counter series:\n%s", mrec.Body.String())
	}
}

// TestEventRetryEnqueueAfterStopDropped verifies the stopped gate: an event
// enqueued after stop must NOT be buffered (the flush goroutine is gone, so
// it would never be retried) — it is counted as dropped with reason stopped.
// Entries still buffered when stop's flush budget runs out are discarded and
// counted with reason shutdown.
func TestEventRetryEnqueueAfterStopDropped(t *testing.T) {
	buf, _ := newTestEventRetryBuffer()
	bus := NewEventBus("stopped-tenant", nil)

	// Buffer one entry, then stop (never started: wg.Wait returns immediately;
	// the final flush fails against the nil store and the leftover is
	// discarded as shutdown loss).
	buf.enqueue(bus, "/kept.txt", "write", "", 100)
	buf.stop()

	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth after stop = %d, want 0 (leftover discarded as shutdown loss)", got)
	}

	buf.enqueue(bus, "/dropped.txt", "write", "", 200)

	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth = %d, want 0 (post-stop enqueue must not buffer)", got)
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_sse_event_retry_dropped_total{reason="stopped",tenant_id="stopped-tenant",tidbcloud_org_id="guest"} 1`) {
		t.Fatalf("expected stopped drop counter series:\n%s", text)
	}
	if !strings.Contains(text, `drive9_sse_event_retry_dropped_total{reason="shutdown",tenant_id="stopped-tenant",tidbcloud_org_id="guest"} 1`) {
		t.Fatalf("expected shutdown drop counter series:\n%s", text)
	}
}

// TestEventRetryMaxAgeClamped verifies the constructor clamps the per-entry
// drop age into [eventRetryMinMaxAge, eventRetryMaxMaxAge] so the server's
// fs_events retention maps to min(retention, 24h) floored at 1h.
func TestEventRetryMaxAgeClamped(t *testing.T) {
	noop := func(string, int) {}
	if got := newEventRetryBuffer(noop, time.Minute, nil).maxAge; got != eventRetryMinMaxAge {
		t.Fatalf("maxAge for 1m retention = %v, want floor %v", got, eventRetryMinMaxAge)
	}
	if got := newEventRetryBuffer(noop, 168*time.Hour, nil).maxAge; got != eventRetryMaxMaxAge {
		t.Fatalf("maxAge for 168h retention = %v, want cap %v", got, eventRetryMaxMaxAge)
	}
	if got := newEventRetryBuffer(noop, 6*time.Hour, nil).maxAge; got != 6*time.Hour {
		t.Fatalf("maxAge for 6h retention = %v, want 6h", got)
	}
}

// TestEventRetryResolverHealsClosedStore verifies the store-resolver path:
// the cached store fails with a closed-DB error, the resolver re-acquires a
// working store, and the retry still lands.
func TestEventRetryResolverHealsClosedStore(t *testing.T) {
	buf, rec := newTestEventRetryBuffer()
	bus := NewEventBus("resolver-tenant", nil)
	closed := &fakeFSEventInserter{err: sql.ErrConnDone}
	good := &fakeFSEventInserter{}
	buf.loadStore = func(*EventBus) fsEventInserter { return closed }
	var resolveCalls atomic.Int32
	buf.resolveStore = func(_ context.Context, tenantID string, bus *EventBus) (fsEventInserter, error) {
		resolveCalls.Add(1)
		if tenantID != "resolver-tenant" {
			t.Errorf("resolve tenantID = %q, want resolver-tenant", tenantID)
		}
		return good, nil
	}

	buf.enqueue(bus, "/a.txt", "write", "", 100)
	markAllDue(buf)
	flushSync(buf)

	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth = %d, want 0 (resolver healed the closed store)", got)
	}
	if resolveCalls.Load() < 1 {
		t.Fatal("resolver was not called after the closed-DB error")
	}
	if len(good.calls) != 1 {
		t.Fatalf("inserts on resolved store = %d, want 1", len(good.calls))
	}
	if rec.forTenant("resolver-tenant") != 1 {
		t.Fatalf("notify calls = %d, want 1 after resolved flush", rec.forTenant("resolver-tenant"))
	}
}

// TestEventRetryTenantGoneDropped verifies that a resolver reporting the
// tenant as gone drops the entry with the distinct tenant_gone reason
// instead of spinning forever.
func TestEventRetryTenantGoneDropped(t *testing.T) {
	buf, rec := newTestEventRetryBuffer()
	bus := NewEventBus("gone-tenant", nil) // default loadStore → nil → resolver consulted
	buf.resolveStore = func(_ context.Context, _ string, _ *EventBus) (fsEventInserter, error) {
		return nil, errTenantGone
	}

	buf.enqueue(bus, "/a.txt", "write", "", 100)
	markAllDue(buf)
	flushSync(buf)

	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth = %d, want 0 (gone tenant's entry dropped)", got)
	}
	if rec.total() != 0 {
		t.Fatalf("notify calls = %d, want none for dropped entry", rec.total())
	}
	m := httptest.NewRecorder()
	metrics.WritePrometheus(m)
	if !strings.Contains(m.Body.String(), `drive9_sse_event_retry_dropped_total{reason="tenant_gone",tenant_id="gone-tenant",tidbcloud_org_id="guest"} 1`) {
		t.Fatalf("expected tenant_gone drop counter series:\n%s", m.Body.String())
	}
}
