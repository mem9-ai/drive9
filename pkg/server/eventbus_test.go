package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

func newTestStoreForEventBus(t *testing.T) *datastore.Store {
	t.Helper()
	inst, err := testmysql.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	store, err := datastore.Open(inst.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	testmysql.ResetDB(t, store.DB())
	// Ensure fs_events table exists.
	if _, err := store.DB().Exec(`CREATE TABLE IF NOT EXISTS fs_events (
		seq        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		path       VARCHAR(512) NOT NULL,
		op         VARCHAR(64) NOT NULL,
		actor      VARCHAR(255),
		ts         BIGINT NOT NULL,
		created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
	)`); err != nil {
		t.Fatal(err)
	}
	// Create index, ignoring "duplicate key" error for idempotency (MySQL has no IF NOT EXISTS for indexes).
	if _, err := store.DB().Exec(`CREATE INDEX idx_fs_events_created ON fs_events(created_at)`); err != nil && !strings.Contains(err.Error(), "Duplicate key") {
		t.Fatal(err)
	}
	return store
}

func TestEventBusSubscribeAndNotify(t *testing.T) {
	store := newTestStoreForEventBus(t)
	bus := NewEventBus(store)

	subID, notify := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	// No signal yet.
	select {
	case <-notify:
		t.Fatal("unexpected signal before publish")
	case <-time.After(50 * time.Millisecond):
	}

	// Insert an event and signal.
	if _, err := store.InsertFSEvent(context.Background(), "/a.txt", "write", "actor1", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	bus.Publish()

	// Should receive a signal.
	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("channel closed unexpectedly")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func TestEventBusEventsSinceZeroReturnsReset(t *testing.T) {
	store := newTestStoreForEventBus(t)
	bus := NewEventBus(store)

	if _, err := store.InsertFSEvent(context.Background(), "/a.txt", "write", "", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}

	events, headSeq, ok := bus.EventsSince(context.Background(), 0)
	if ok {
		t.Fatalf("EventsSince(0) should return ok=false for initial sync, got ok=true with %d events", len(events))
	}
	if headSeq == 0 {
		t.Fatal("headSeq should be > 0 after insert")
	}
}

func TestEventBusEventsSinceReplays(t *testing.T) {
	store := newTestStoreForEventBus(t)
	bus := NewEventBus(store)

	// Insert two events.
	seq1, err := store.InsertFSEvent(context.Background(), "/a.txt", "write", "actor1", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := store.InsertFSEvent(context.Background(), "/b.txt", "delete", "actor2", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}

	// Query events since seq1 (should get event 2).
	events, headSeq, ok := bus.EventsSince(context.Background(), uint64(seq1))
	if !ok {
		t.Fatal("EventsSince should succeed")
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Path != "/b.txt" || events[0].Op != "delete" {
		t.Fatalf("unexpected event: %+v", events[0])
	}
	if headSeq < uint64(seq2) {
		t.Fatalf("headSeq=%d, want >= %d", headSeq, seq2)
	}
}

func TestEventBusSeq(t *testing.T) {
	store := newTestStoreForEventBus(t)
	bus := NewEventBus(store)

	// Reset AUTO_INCREMENT so seq starts at 1.
	if _, err := store.DB().Exec(`ALTER TABLE fs_events AUTO_INCREMENT = 1`); err != nil {
		t.Fatal(err)
	}

	if bus.Seq(context.Background()) != 0 {
		t.Fatal("initial seq should be 0")
	}

	seq, err := store.InsertFSEvent(context.Background(), "/a.txt", "write", "", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("insert returned seq=%d, want 1", seq)
	}
	if bus.Seq(context.Background()) != 1 {
		t.Fatalf("seq after 1 insert = %d, want 1", bus.Seq(context.Background()))
	}
}

func TestEventBusSubscribeUnsubscribe(t *testing.T) {
	store := newTestStoreForEventBus(t)
	bus := NewEventBus(store)

	subID, _ := bus.Subscribe()
	bus.Unsubscribe(subID)

	// After unsubscribe, a new subscribe should work.
	subID2, notify := bus.Subscribe()
	defer bus.Unsubscribe(subID2)

	bus.Publish()
	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("channel closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func TestEventBusMultipleSubscribers(t *testing.T) {
	store := newTestStoreForEventBus(t)
	bus := NewEventBus(store)

	var wg sync.WaitGroup
	const n = 5
	for i := 0; i < n; i++ {
		wg.Add(1)
		subID, notify := bus.Subscribe()
		go func(id uint64, ch chan struct{}) {
			defer wg.Done()
			select {
			case _, open := <-ch:
				if open {
					bus.Unsubscribe(id)
				}
			case <-time.After(2 * time.Second):
			}
		}(subID, notify)
	}

	bus.Publish()
	wg.Wait()
}