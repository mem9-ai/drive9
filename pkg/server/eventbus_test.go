package server

import (
	"sync"
	"testing"
)

func TestEventBusPublishAndSeq(t *testing.T) {
	bus := NewEventBus()
	if bus.Seq() != 0 {
		t.Fatalf("initial seq=%d, want 0", bus.Seq())
	}

	bus.Publish("/a.txt", "write", "actor1")
	if bus.Seq() != 1 {
		t.Fatalf("seq after 1 publish=%d, want 1", bus.Seq())
	}

	bus.Publish("/b.txt", "delete", "actor2")
	if bus.Seq() != 2 {
		t.Fatalf("seq after 2 publishes=%d, want 2", bus.Seq())
	}
}

func TestEventBusSince0ReturnsNotOK(t *testing.T) {
	bus := NewEventBus()
	bus.Publish("/a.txt", "write", "")

	events, headSeq, ok := bus.EventsSince(0)
	if ok {
		t.Fatal("EventsSince(0) should return ok=false (initial sync → reset)")
	}
	if events != nil {
		t.Fatalf("expected nil events, got %d", len(events))
	}
	if headSeq != 1 {
		t.Fatalf("headSeq=%d, want 1", headSeq)
	}
}

func TestEventBusReplay(t *testing.T) {
	bus := NewEventBus()
	bus.Publish("/a.txt", "write", "")
	bus.Publish("/b.txt", "write", "")
	bus.Publish("/c.txt", "delete", "")

	events, _, ok := bus.EventsSince(1) // since seq=1, want seq=2,3
	if !ok {
		t.Fatal("EventsSince(1) returned not ok")
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Seq != 2 || events[0].Path != "/b.txt" {
		t.Errorf("event[0] = %+v", events[0])
	}
	if events[1].Seq != 3 || events[1].Path != "/c.txt" {
		t.Errorf("event[1] = %+v", events[1])
	}
}

func TestEventBusCaughtUp(t *testing.T) {
	bus := NewEventBus()
	bus.Publish("/a.txt", "write", "")

	events, _, ok := bus.EventsSince(1)
	if !ok {
		t.Fatal("expected ok=true when caught up")
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events when caught up, got %d", len(events))
	}
}

func TestEventBusRingOverflow(t *testing.T) {
	bus := NewEventBus()
	// Fill ring + 1 to trigger wrap.
	for i := 0; i < eventBusRingSize+1; i++ {
		bus.Publish("/file.txt", "write", "")
	}

	// Seq 1 should be evicted from the ring.
	_, _, ok := bus.EventsSince(1)
	if ok {
		t.Fatal("expected ok=false for seq that's been overwritten")
	}

	// Most recent seq should still be reachable.
	events, _, ok := bus.EventsSince(uint64(eventBusRingSize))
	if !ok {
		t.Fatal("expected ok=true for recent seq")
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestEventBusFutureSeq(t *testing.T) {
	bus := NewEventBus()
	bus.Publish("/a.txt", "write", "")

	// Client has seq=999 but server only has seq=1 (e.g. server restarted).
	_, _, ok := bus.EventsSince(999)
	if ok {
		t.Fatal("expected ok=false for future seq (server restart)")
	}
}

func TestEventBusSubscribeNotify(t *testing.T) {
	bus := NewEventBus()
	id, ch := bus.Subscribe()
	defer bus.Unsubscribe(id)

	bus.Publish("/a.txt", "write", "")

	select {
	case <-ch:
		// OK — received signal.
	default:
		t.Fatal("expected signal on subscriber channel after publish")
	}
}

func TestEventBusUnsubscribeCloses(t *testing.T) {
	bus := NewEventBus()
	id, ch := bus.Subscribe()
	bus.Unsubscribe(id)

	_, open := <-ch
	if open {
		t.Fatal("expected channel to be closed after unsubscribe")
	}
}

func TestEventBusConcurrentPublish(t *testing.T) {
	bus := NewEventBus()
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			bus.Publish("/file.txt", "write", "")
		}()
	}
	wg.Wait()

	if bus.Seq() != uint64(n) {
		t.Fatalf("seq=%d, want %d", bus.Seq(), n)
	}
}

func TestEventBusSinceBoundaryNoFalseReset(t *testing.T) {
	bus := NewEventBus()
	// Fill ring so oldestSeq > 1: publish eventBusRingSize+2 events
	// so ring contains seqs [3, 4, ..., ringSize+2] with oldestSeq=3.
	for i := 0; i < eventBusRingSize+2; i++ {
		bus.Publish("/file.txt", "write", "")
	}
	// since=2 means client needs seq 3+, which is exactly oldestSeq.
	// This should NOT trigger a reset — all needed events are in the ring.
	events, _, ok := bus.EventsSince(2)
	if !ok {
		t.Fatal("expected ok=true for since=oldestSeq-1 (all needed events are in the ring)")
	}
	if len(events) != eventBusRingSize {
		t.Fatalf("expected %d events, got %d", eventBusRingSize, len(events))
	}
	if events[0].Seq != 3 {
		t.Fatalf("first event seq=%d, want 3", events[0].Seq)
	}
}

func TestEventBusEmptyRingSincePositive(t *testing.T) {
	bus := NewEventBus()
	// No events published; client has since=5 (stale from previous server life).
	_, _, ok := bus.EventsSince(5)
	if ok {
		t.Fatal("expected ok=false when ring is empty and since > 0")
	}
}
