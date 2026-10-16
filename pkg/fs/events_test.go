package fs

import (
	"testing"
	"time"
)

func TestEventBus(t *testing.T) {
	bus := NewEventBus()
	var landed []Event
	off := bus.On(EventLanded, func(ev Event) { landed = append(landed, ev) })
	bus.Emit(Event{Kind: EventStaged, Path: "/a"})
	if len(landed) != 0 {
		t.Fatalf("other-kind event delivered: %v", landed)
	}
	bus.Emit(Event{Kind: EventLanded, Path: "/a", Rev: 7, Size: 9, Epoch: 3})
	if len(landed) != 1 || landed[0].Path != "/a" || landed[0].Rev != 7 || landed[0].Size != 9 || landed[0].Epoch != 3 {
		t.Fatalf("landed = %+v", landed)
	}
	off()
	bus.Emit(Event{Kind: EventLanded, Path: "/b"})
	if len(landed) != 1 {
		t.Fatal("unsubscribe did not take effect")
	}
}

func TestEventBusEmitDuringHandler(t *testing.T) {
	bus := NewEventBus()
	done := make(chan struct{}, 1)
	bus.On(EventStaged, func(ev Event) {
		bus.Emit(Event{Kind: EventLanded, Path: ev.Path})
	})
	bus.On(EventLanded, func(ev Event) {
		if ev.Path == "/chain" {
			done <- struct{}{}
		}
	})
	bus.Emit(Event{Kind: EventStaged, Path: "/chain"})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emit from inside a handler deadlocked or was dropped")
	}
}
