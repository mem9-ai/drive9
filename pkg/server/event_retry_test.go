package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
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

func newTestEventRetryBuffer() (*eventRetryBuffer, *[]notifyCall) {
	calls := &[]notifyCall{}
	buf := newEventRetryBuffer(func(tenantID string, workMask int) {
		*calls = append(*calls, notifyCall{tenantID: tenantID, workMask: workMask})
	}, time.Hour)
	return buf, calls
}

type notifyCall struct {
	tenantID string
	workMask int
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

// TestEventRetryNilStoreRequeues verifies that an entry whose bus has no store
// yet is requeued with backoff instead of being dropped or attempted.
func TestEventRetryNilStoreRequeues(t *testing.T) {
	buf, calls := newTestEventRetryBuffer()
	bus := NewEventBus("nil-store-tenant", nil) // default loadStore → nil

	buf.enqueue(bus, "/a.txt", "write", "actor1", 100)
	markAllDue(buf)
	buf.flushDue(context.Background())

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
	if len(*calls) != 0 {
		t.Fatalf("notify calls = %v, want none on requeue", *calls)
	}
}

// TestEventRetryFailedFlushRequeuesWithBackoff verifies that an insert error
// requeues the entry and that the second attempt doubles the backoff.
func TestEventRetryFailedFlushRequeuesWithBackoff(t *testing.T) {
	buf, calls := newTestEventRetryBuffer()
	fake := &fakeFSEventInserter{err: errors.New("db down")}
	buf.loadStore = func(bus *EventBus) fsEventInserter { return fake }
	bus := NewEventBus("fail-flush-tenant", nil)

	buf.enqueue(bus, "/a.txt", "write", "actor1", 100)

	for wantAttempts := 1; wantAttempts <= 2; wantAttempts++ {
		markAllDue(buf)
		before := time.Now()
		buf.flushDue(context.Background())
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
	if len(*calls) != 0 {
		t.Fatalf("notify calls = %v, want none while flush fails", *calls)
	}
}

// TestEventRetryFlushSuccessSecondWake verifies that a successful flush
// performs the mandatory second wake: insertTenantNotify(WorkSSE) and
// bus.Publish(), exactly like a fresh publishEvent.
func TestEventRetryFlushSuccessSecondWake(t *testing.T) {
	buf, calls := newTestEventRetryBuffer()
	fake := &fakeFSEventInserter{}
	buf.loadStore = func(bus *EventBus) fsEventInserter { return fake }
	bus := NewEventBus("flush-ok-tenant", nil)
	subID, notify := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	buf.enqueue(bus, "/a.txt", "write", "actor1", 100)
	markAllDue(buf)
	buf.flushDue(context.Background())

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
	if len(*calls) != 1 || (*calls)[0].tenantID != "flush-ok-tenant" || (*calls)[0].workMask != WorkSSE {
		t.Fatalf("notify calls = %+v, want one WorkSSE call for flush-ok-tenant", *calls)
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
	buf, calls := newTestEventRetryBuffer()
	fake := &fakeFSEventInserter{}
	buf.loadStore = func(bus *EventBus) fsEventInserter { return fake }
	bus := NewEventBus("expired-tenant", nil)

	buf.enqueue(bus, "/a.txt", "write", "", 100)
	buf.mu.Lock()
	buf.entries[0].enqueuedAt = time.Now().Add(-2 * buf.maxAge)
	buf.entries[0].nextRetry = time.Time{}
	buf.mu.Unlock()

	buf.flushDue(context.Background())

	if got := bufferDepth(buf); got != 0 {
		t.Fatalf("depth = %d, want 0 (expired entry dropped)", got)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("insert calls = %d, want 0 for expired entry", len(fake.calls))
	}
	if len(*calls) != 0 {
		t.Fatalf("notify calls = %v, want none for expired entry", *calls)
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_sse_event_retry_dropped_total{reason="expired",tenant_id="expired-tenant",tidbcloud_org_id="guest"} 1`) {
		t.Fatalf("expected expired drop counter series:\n%s", rec.Body.String())
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
	if got := newEventRetryBuffer(noop, time.Minute).maxAge; got != eventRetryMinMaxAge {
		t.Fatalf("maxAge for 1m retention = %v, want floor %v", got, eventRetryMinMaxAge)
	}
	if got := newEventRetryBuffer(noop, 168*time.Hour).maxAge; got != eventRetryMaxMaxAge {
		t.Fatalf("maxAge for 168h retention = %v, want cap %v", got, eventRetryMaxMaxAge)
	}
	if got := newEventRetryBuffer(noop, 6*time.Hour).maxAge; got != 6*time.Hour {
		t.Fatalf("maxAge for 6h retention = %v, want 6h", got)
	}
}
