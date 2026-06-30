package server

import (
	"context"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/meta"
)

// newTestMetaStoreForNotify opens the shared test DSN as a meta store and
// resets the SSE-notify-related tables for isolation between tests.
func newTestMetaStoreForNotify(t *testing.T) *meta.Store {
	t.Helper()
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	// Clean up SSE notify tables (ResetMetaDB may not know about new tables).
	// Fail on error so stale rows don't leak between tests.
	ctx := context.Background()
	for _, table := range []string{"sse_notify_outbox", "pod_subscriptions", "pod_registry"} {
		if _, err := metaStore.DB().ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("clean up %s: %v", table, err)
		}
	}
	return metaStore
}

// TestNotifyPollerDiscoversCrossPodEvents verifies that the notify poller
// reads new sse_notify_outbox rows and wakes matching local EventBus
// subscribers via Publish. This replaces the old per-bus pollLoop test.
func TestNotifyPollerDiscoversCrossPodEvents(t *testing.T) {
	metaStore := newTestMetaStoreForNotify(t)
	store := newTestStoreForEventBus(t)
	buses := newEventBuses()
	// Create a bus for tenant "T" with a subscriber.
	bus := buses.get("T", store)
	id, notify := bus.Subscribe()
	defer bus.Unsubscribe(id)

	np := newNotifyPoller(metaStore, buses, 50*time.Millisecond)
	// Initialize cursor to 0 so we don't skip the row we're about to insert
	// (run() would set it to MaxSSENotifyID which races with the insert).
	np.lastID = 0
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go np.run(ctx)

	// Simulate a cross-pod write: insert an outbox row (as a peer pod would).
	// The poller should discover it within ~100ms and Publish to wake notify.
	if err := metaStore.InsertSSENotify(context.Background(), "T", 1); err != nil {
		t.Fatal(err)
	}

	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("notify channel closed")
		}
		// Signal received — poller discovered the cross-pod outbox row.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cross-pod event signal from notify poller")
	}
}

// TestNotifyPollerSkipsTenantsWithoutSubscribers verifies that outbox rows for
// tenants with no local bus (no SSE subscribers) are silently skipped — they
// don't panic and don't touch the tenant's TiDB. This is the key property that
// allows idle tenant TiDBs to scale to zero.
func TestNotifyPollerSkipsTenantsWithoutSubscribers(t *testing.T) {
	metaStore := newTestMetaStoreForNotify(t)
	store := newTestStoreForEventBus(t)
	buses := newEventBuses()
	// Create a bus for tenant "T1" with a subscriber.
	bus1 := buses.get("T1", store)
	id1, notify1 := bus1.Subscribe()
	defer bus1.Unsubscribe(id1)

	np := newNotifyPoller(metaStore, buses, 50*time.Millisecond)
	np.lastID = 0 // start from beginning to avoid cursor race with insert
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go np.run(ctx)

	// Insert outbox rows for T1 (has subscriber) and T2 (no subscriber).
	if err := metaStore.InsertSSENotify(context.Background(), "T1", 1); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertSSENotify(context.Background(), "T2", 1); err != nil {
		t.Fatal(err)
	}

	// T1's subscriber should be woken.
	select {
	case _, open := <-notify1:
		if !open {
			t.Fatal("notify channel closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for T1 event signal")
	}

	// Verify the poller advanced past both rows (T2 was skipped without error).
	// After a short delay, the poller's lastID should be >= the T2 row id.
	// We check indirectly: insert another T1 row and confirm it's discovered
	// (proving the cursor advanced past T2 without getting stuck).
	if err := metaStore.InsertSSENotify(context.Background(), "T1", 2); err != nil {
		t.Fatal(err)
	}
	select {
	case _, open := <-notify1:
		if !open {
			t.Fatal("notify channel closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second T1 event — poller may have stalled on T2")
	}
}

// TestNotifyPollerBatchDrain verifies that when a full batch is returned, the
// poller immediately reads the next batch without waiting for the next tick,
// so a burst of outbox rows is processed quickly.
func TestNotifyPollerBatchDrain(t *testing.T) {
	metaStore := newTestMetaStoreForNotify(t)
	store := newTestStoreForEventBus(t)
	buses := newEventBuses()
	bus := buses.get("drain-tenant", store)
	id, notify := bus.Subscribe()
	defer bus.Unsubscribe(id)

	// Insert more rows than the batch size (1000) so the poller must drain
	// multiple batches in a single pollOnce.
	const burstSize = notifyPollBatchSize + 500
	for i := 0; i < burstSize; i++ {
		if err := metaStore.InsertSSENotify(context.Background(), "drain-tenant", uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}

	np := newNotifyPoller(metaStore, buses, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// Manually call pollOnce once to drain all batches synchronously.
	np.lastID = 0 // start from the beginning
	np.pollOnce(ctx)

	// All rows should have been consumed. Verify by checking that the poller's
	// cursor advanced past all rows: a subsequent pollOnce should find 0 rows.
	maxID, err := metaStore.MaxSSENotifyID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if np.lastID < maxID {
		t.Fatalf("poller cursor %d did not drain to max %d", np.lastID, maxID)
	}

	// The subscriber should have been woken at least once by the synchronous
	// pollOnce call (which calls Publish for each row; the coalescing channel
	// buffer size 1 means the first signal is retained).
	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("notify channel closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for notify signal after batch drain")
	}
}