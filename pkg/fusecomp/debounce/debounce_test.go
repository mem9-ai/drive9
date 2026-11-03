package debounce

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDebounceFiresAfterDelay(t *testing.T) {
	d := NewFlushDebouncer(20 * time.Millisecond)
	var ran atomic.Int32
	d.Schedule("/a", func() { ran.Add(1) })
	if ran.Load() != 0 {
		t.Fatal("callback ran before the delay elapsed")
	}
	deadline := time.Now().Add(time.Second)
	for ran.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if ran.Load() != 1 {
		t.Fatalf("callback ran %d times, want 1", ran.Load())
	}
}

func TestDebounceCoalescesRapidSchedules(t *testing.T) {
	d := NewFlushDebouncer(50 * time.Millisecond)
	var first, latest atomic.Int32
	d.Schedule("/a", func() { first.Add(1) })
	for range 10 {
		time.Sleep(5 * time.Millisecond) // inside the window
		d.Schedule("/a", func() { latest.Add(1) })
	}
	time.Sleep(150 * time.Millisecond)
	if first.Load() != 0 {
		t.Fatalf("superseded callback ran %d times", first.Load())
	}
	if latest.Load() != 1 {
		t.Fatalf("latest callback ran %d times, want 1", latest.Load())
	}
}

func TestCancelPreventsExecution(t *testing.T) {
	d := NewFlushDebouncer(20 * time.Millisecond)
	var ran atomic.Int32
	d.Schedule("/a", func() { ran.Add(1) })
	d.Cancel("/a")
	time.Sleep(80 * time.Millisecond)
	if ran.Load() != 0 {
		t.Fatalf("canceled callback ran %d times", ran.Load())
	}
}

func TestCancelNoWaitIfOwnerScopesToOwner(t *testing.T) {
	ownerA, ownerB := "handle-a", "handle-b"

	// A cancel from a different owner must not touch the pending entry.
	d := NewFlushDebouncer(20 * time.Millisecond)
	var ran atomic.Int32
	d.ScheduleWithOwner("/a", ownerA, func() { ran.Add(1) })
	d.CancelNoWaitIfOwner("/a", ownerB)
	time.Sleep(100 * time.Millisecond)
	if ran.Load() != 1 {
		t.Fatalf("foreign-owner cancel suppressed the upload (ran=%d)", ran.Load())
	}

	// The owning handle's cancel does.
	d2 := NewFlushDebouncer(20 * time.Millisecond)
	var ran2 atomic.Int32
	d2.ScheduleWithOwner("/a", ownerA, func() { ran2.Add(1) })
	d2.CancelNoWaitIfOwner("/a", ownerA)
	time.Sleep(100 * time.Millisecond)
	if ran2.Load() != 0 {
		t.Fatalf("owner cancel did not suppress the upload (ran=%d)", ran2.Load())
	}
}

func TestFlushAllRunsPendingImmediately(t *testing.T) {
	d := NewFlushDebounce_LongDelay(t) // 10s: only FlushAll can fire these
	var a, b, c atomic.Int32
	d.Schedule("/a", func() { a.Add(1) })
	d.Schedule("/b", func() { b.Add(1) })
	d.Schedule("/c", func() { c.Add(1) })
	done := make(chan struct{})
	go func() { d.FlushAll(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("FlushAll did not run pending uploads promptly")
	}
	if a.Load() != 1 || b.Load() != 1 || c.Load() != 1 {
		t.Fatalf("flush counts a=%d b=%d c=%d", a.Load(), b.Load(), c.Load())
	}
	// The pending map is drained: a later Cancel is a no-op and the old
	// timers (stopped by FlushAll) never re-fire.
	d.Cancel("/a")
	time.Sleep(30 * time.Millisecond)
	if a.Load() != 1 {
		t.Fatalf("stale timer re-fired after FlushAll (a=%d)", a.Load())
	}
}

func TestFlushAllWaitsForInflightPerPath(t *testing.T) {
	d := NewFlushDebouncer(0) // fire immediately
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	ran := map[string]int{}
	record := func(p string) func() {
		return func() {
			mu.Lock()
			ran[p]++
			mu.Unlock()
		}
	}
	d.Schedule("/p", func() {
		close(started)
		<-release
		record("/p")()
	})
	<-started // first callback is now in flight and blocked

	// A second schedule lands in pending while /p is executing.
	d.Schedule("/p", record("/p"))

	flushed := make(chan struct{})
	go func() { d.FlushAll(); close(flushed) }()

	select {
	case <-flushed:
		t.Fatal("FlushAll returned while a same-path callback was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-flushed:
	case <-time.After(time.Second):
		t.Fatal("FlushAll did not finish after the in-flight callback drained")
	}
	mu.Lock()
	defer mu.Unlock()
	if ran["/p"] != 2 {
		t.Fatalf("/p ran %d times, want 2 (serialized)", ran["/p"])
	}
}

func TestNegativeDelayClampsToZero(t *testing.T) {
	d := NewFlushDebouncer(-1)
	if d.Delay() != 0 {
		t.Fatalf("delay = %v, want 0", d.Delay())
	}
	var ran atomic.Int32
	d.Schedule("/a", func() { ran.Add(1) })
	deadline := time.Now().Add(time.Second)
	for ran.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ran.Load() != 1 {
		t.Fatal("zero-delay callback did not fire")
	}
}

// NewFlushDebounce_LongDelay builds a debouncer whose window is far longer
// than any test timeout, so only the explicit FlushAll can fire callbacks.
func NewFlushDebounce_LongDelay(t *testing.T) *FlushDebouncer {
	t.Helper()
	return NewFlushDebouncer(10 * time.Second)
}
