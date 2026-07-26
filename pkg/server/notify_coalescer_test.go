package server

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

type recordingNotifyInserter struct {
	mu          sync.Mutex
	calls       [][]meta.TenantNotifyEntry
	singleCalls []meta.TenantNotifyEntry
	err         error
	singleErr   map[string]error // per-tenant error injected on the single-row path
}

func (r *recordingNotifyInserter) insert(ctx context.Context, entries []meta.TenantNotifyEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := append([]meta.TenantNotifyEntry(nil), entries...)
	r.calls = append(r.calls, cp)
	return r.err
}

func (r *recordingNotifyInserter) insertSingle(ctx context.Context, tenantID string, workMask int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.singleCalls = append(r.singleCalls, meta.TenantNotifyEntry{TenantID: tenantID, WorkMask: workMask})
	return r.singleErr[tenantID]
}

func (r *recordingNotifyInserter) recorded() [][]meta.TenantNotifyEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]meta.TenantNotifyEntry(nil), r.calls...)
}

func (r *recordingNotifyInserter) recordedSingle() []meta.TenantNotifyEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]meta.TenantNotifyEntry(nil), r.singleCalls...)
}

// flakyBatchInserter fails the first len(errs) batch calls with errs in
// order, then succeeds.
type flakyBatchInserter struct {
	mu    sync.Mutex
	errs  []error
	calls [][]meta.TenantNotifyEntry
}

func (f *flakyBatchInserter) insert(ctx context.Context, entries []meta.TenantNotifyEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]meta.TenantNotifyEntry(nil), entries...))
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return nil
}

func (f *flakyBatchInserter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestTenantNotifyCoalescerORMergesWorkMasks(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, time.Minute)

	c.add("tenant-a", 1)
	c.add("tenant-a", 4)
	c.add("tenant-a", 1) // duplicate bit stays set exactly once
	c.add("tenant-b", 2)
	c.flush(context.Background())

	calls := rec.recorded()
	if len(calls) != 1 {
		t.Fatalf("insert calls = %d, want 1 (single batch)", len(calls))
	}
	got := make(map[string]int)
	for _, e := range calls[0] {
		got[e.TenantID] = e.WorkMask
	}
	if len(got) != 2 || got["tenant-a"] != 5 || got["tenant-b"] != 2 {
		t.Fatalf("merged entries = %v, want tenant-a=5 (1|4) and tenant-b=2", got)
	}
}

func TestTenantNotifyCoalescerFlushSwapsMap(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, time.Minute)

	c.add("tenant-a", 1)
	c.flush(context.Background())
	// The first flush swapped the map out: a second flush with no new adds
	// must not re-insert anything.
	c.flush(context.Background())

	if calls := rec.recorded(); len(calls) != 1 {
		t.Fatalf("insert calls = %d, want 1 (empty flush is a no-op)", len(calls))
	}

	c.add("tenant-a", 2)
	c.flush(context.Background())
	calls := rec.recorded()
	if len(calls) != 2 {
		t.Fatalf("insert calls = %d, want 2", len(calls))
	}
	if len(calls[1]) != 1 || calls[1][0].WorkMask != 2 {
		t.Fatalf("second batch = %v, want tenant-a mask 2 only (no replay of mask 1)", calls[1])
	}
}

func TestTenantNotifyCoalescerEmptyFlushIsNoOp(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, time.Minute)
	c.flush(context.Background())
	c.flush(context.Background())
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("insert calls = %d, want 0", len(calls))
	}
}

// The batch path fails once, the retry succeeds: nothing is dropped and the
// per-row fallback is never used.
func TestTenantNotifyCoalescerFlushRetriesBatchOnce(t *testing.T) {
	batch := &flakyBatchInserter{errs: []error{errors.New("transient meta error")}}
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(batch.insert, rec.insertSingle, time.Minute)

	c.add("tenant-a", 1)
	c.flush(context.Background())

	if got := batch.callCount(); got != 2 {
		t.Fatalf("batch insert calls = %d, want 2 (initial + one retry)", got)
	}
	if singles := rec.recordedSingle(); len(singles) != 0 {
		t.Fatalf("single-row inserts = %d, want 0 (batch retry succeeded)", len(singles))
	}

	// The retry delivered the batch: nothing is pending for the next flush.
	c.flush(context.Background())
	if got := batch.callCount(); got != 2 {
		t.Fatalf("batch insert calls after successful flush = %d, want 2", got)
	}
}

// The batch path fails twice: every entry is delivered through independent
// per-row inserts, and a failing row is dropped without affecting the rest.
func TestTenantNotifyCoalescerFlushFailureFallsBackToPerRow(t *testing.T) {
	rec := &recordingNotifyInserter{
		err:       errors.New("meta down"),
		singleErr: map[string]error{"tenant-b": errors.New("row conflict")},
	}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, time.Minute)

	c.add("tenant-a", 1)
	c.add("tenant-b", 2)
	c.flush(context.Background()) // batch fails twice -> per-row fallback
	rec.err = nil
	c.flush(context.Background()) // nothing pending anymore

	calls := rec.recorded()
	if len(calls) != 2 {
		t.Fatalf("batch insert calls = %d, want 2 (initial + one retry)", len(calls))
	}
	singles := rec.recordedSingle()
	if len(singles) != 2 {
		t.Fatalf("single-row inserts = %d, want 2 (per-row fallback for both tenants)", len(singles))
	}
	got := make(map[string]int)
	for _, e := range singles {
		got[e.TenantID] = e.WorkMask
	}
	if got["tenant-a"] != 1 || got["tenant-b"] != 2 {
		t.Fatalf("per-row entries = %v, want tenant-a=1 and tenant-b=2", got)
	}
	if calls := rec.recorded(); len(calls) != 2 {
		t.Fatalf("batch insert calls after fallback = %d, want 2 (dropped rows are not replayed)", len(calls))
	}
	metricRec := httptest.NewRecorder()
	metrics.WritePrometheus(metricRec)
	metricText := metricRec.Body.String()
	for _, want := range []string{
		`drive9_notify_coalescer_flush_total{result="fallback"}`,
		`drive9_notify_coalescer_per_row_fallback_total{result="ok"}`,
		`drive9_notify_coalescer_per_row_fallback_total{result="error"}`,
		`drive9_notify_coalescer_pending 0.000000`,
	} {
		if !strings.Contains(metricText, want) {
			t.Fatalf("missing notify coalescer metric %q: %s", want, metricText)
		}
	}
}

func TestTenantNotifyCoalescerStopFlushesPending(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, time.Hour) // interval must not fire during the test
	c.start(context.Background())

	c.add("tenant-a", 1)
	c.add("tenant-b", 2)
	c.stop()

	calls := rec.recorded()
	if len(calls) != 1 {
		t.Fatalf("insert calls = %d, want 1 (final flush on stop)", len(calls))
	}
	got := make(map[string]int)
	for _, e := range calls[0] {
		got[e.TenantID] = e.WorkMask
	}
	if len(got) != 2 || got["tenant-a"] != 1 || got["tenant-b"] != 2 {
		t.Fatalf("final flush entries = %v, want tenant-a=1 and tenant-b=2", got)
	}

	// stop must be safe to call with nothing pending too.
	c.stop()
}

func TestTenantNotifyCoalescerAddAfterStopIsNoOp(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, time.Hour)
	c.start(context.Background())
	c.stop()

	// Signals racing with shutdown are dropped instead of being accepted
	// into a batch that will never flush.
	c.add("tenant-a", 1)
	c.flush(context.Background())
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("insert calls after stop = %d, want 0 (add after stop is a no-op)", len(calls))
	}
}

func TestTenantNotifyCoalescerPeriodicFlush(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, 10*time.Millisecond)
	c.start(context.Background())
	defer c.stop()

	c.add("tenant-a", 1)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.recorded()) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("periodic flush did not fire within 2s")
}

// TestTenantNotifyCoalescerConcurrentAddStop hammers add from several
// goroutines while stop runs. After stop returns, no signal may sit in
// pending unflushed — a signal accepted after the final flush would be
// dropped silently, which is exactly the add/stop race the mutex-ordered
// stopped check closes.
func TestTenantNotifyCoalescerConcurrentAddStop(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, rec.insertSingle, time.Millisecond)
	c.start(context.Background())

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			tenantID := fmt.Sprintf("tenant-%d", g)
			for i := 0; i < 500; i++ {
				c.add(tenantID, WorkSSE)
			}
		}(g)
	}
	time.Sleep(5 * time.Millisecond) // let the stream race the stop
	c.stop()
	wg.Wait()

	c.mu.Lock()
	remaining := len(c.pending)
	c.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("pending not empty after stop: %d entries would have been silently dropped", remaining)
	}
}
