package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
)

type recordingNotifyInserter struct {
	mu    sync.Mutex
	calls [][]meta.TenantNotifyEntry
	err   error
}

func (r *recordingNotifyInserter) insert(ctx context.Context, entries []meta.TenantNotifyEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := append([]meta.TenantNotifyEntry(nil), entries...)
	r.calls = append(r.calls, cp)
	return r.err
}

func (r *recordingNotifyInserter) recorded() [][]meta.TenantNotifyEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]meta.TenantNotifyEntry(nil), r.calls...)
}

func TestTenantNotifyCoalescerORMergesWorkMasks(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, time.Minute)

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
	c := newTenantNotifyCoalescer(rec.insert, time.Minute)

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
	c := newTenantNotifyCoalescer(rec.insert, time.Minute)
	c.flush(context.Background())
	c.flush(context.Background())
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("insert calls = %d, want 0", len(calls))
	}
}

func TestTenantNotifyCoalescerFlushFailureDropsBatch(t *testing.T) {
	rec := &recordingNotifyInserter{err: errors.New("meta down")}
	c := newTenantNotifyCoalescer(rec.insert, time.Minute)

	c.add("tenant-a", 1)
	c.flush(context.Background()) // must not panic; batch is dropped
	rec.err = nil
	c.flush(context.Background()) // nothing pending anymore

	calls := rec.recorded()
	if len(calls) != 1 {
		t.Fatalf("insert calls = %d, want 1 (dropped batch is not retried)", len(calls))
	}
}

func TestTenantNotifyCoalescerStopFlushesPending(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, time.Hour) // interval must not fire during the test
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

func TestTenantNotifyCoalescerPeriodicFlush(t *testing.T) {
	rec := &recordingNotifyInserter{}
	c := newTenantNotifyCoalescer(rec.insert, 10*time.Millisecond)
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
