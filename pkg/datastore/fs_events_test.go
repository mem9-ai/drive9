package datastore

import (
	"context"
	"testing"
	"time"
)

// TestDeleteFSEventsBeforeBatched exercises the batched retention sweep:
// hasMore is true when the batch cap is hit with rows left over, and false
// after a short final batch.
func TestDeleteFSEventsBeforeBatched(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Insert 7 old rows and 2 fresh rows.
	old := time.Now().Add(-2 * time.Hour)
	for i := 0; i < 9; i++ {
		seq, err := store.InsertFSEvent(ctx, "/f.txt", "write", "tester", int64(100+i))
		if err != nil {
			t.Fatalf("InsertFSEvent %d: %v", i, err)
		}
		if i < 7 {
			if _, err := store.DB().ExecContext(ctx, `UPDATE fs_events SET created_at = ? WHERE seq = ?`, old, seq); err != nil {
				t.Fatalf("age row %d: %v", i, err)
			}
		}
	}

	cutoff := time.Now().Add(-time.Hour)

	// First sweep: batchSize 3, maxBatches 2 → 6 of the 7 old rows deleted,
	// hasMore=true because the cap was hit with one old row left.
	deleted, hasMore, err := store.DeleteFSEventsBefore(ctx, cutoff, 3, 2)
	if err != nil {
		t.Fatalf("DeleteFSEventsBefore sweep 1: %v", err)
	}
	if deleted != 6 {
		t.Fatalf("sweep 1 deleted = %d, want 6", deleted)
	}
	if !hasMore {
		t.Fatal("sweep 1 hasMore = false, want true (batch cap hit with leftover)")
	}

	// Second sweep: the one remaining old row is a short final batch.
	deleted, hasMore, err = store.DeleteFSEventsBefore(ctx, cutoff, 3, 2)
	if err != nil {
		t.Fatalf("DeleteFSEventsBefore sweep 2: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("sweep 2 deleted = %d, want 1", deleted)
	}
	if hasMore {
		t.Fatal("sweep 2 hasMore = true, want false after short final batch")
	}

	// Only the 2 fresh rows remain.
	count, err := store.CountFSEvents(ctx)
	if err != nil {
		t.Fatalf("CountFSEvents: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2 (fresh rows must survive)", count)
	}

	// Nothing left to delete: zero rows, no hasMore.
	deleted, hasMore, err = store.DeleteFSEventsBefore(ctx, time.Now().Add(time.Hour), 3, 2)
	if err != nil {
		t.Fatalf("DeleteFSEventsBefore sweep 3: %v", err)
	}
	if deleted != 2 || hasMore {
		// cutoff now covers the fresh rows too: both deleted in one short batch.
		t.Fatalf("sweep 3 = (%d, %v), want (2, false)", deleted, hasMore)
	}

	// Invalid batch parameters are rejected.
	if _, _, err := store.DeleteFSEventsBefore(ctx, cutoff, 0, 1); err == nil {
		t.Fatal("batchSize 0 should return an error")
	}
	if _, _, err := store.DeleteFSEventsBefore(ctx, cutoff, 1, 0); err == nil {
		t.Fatal("maxBatches 0 should return an error")
	}
}

// TestCountFSEvents verifies the capped count is exact below the cap.
func TestCountFSEvents(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	count, err := store.CountFSEvents(ctx)
	if err != nil {
		t.Fatalf("CountFSEvents empty: %v", err)
	}
	if count != 0 {
		t.Fatalf("empty count = %d, want 0", count)
	}

	for i := 0; i < 3; i++ {
		if _, err := store.InsertFSEvent(ctx, "/f.txt", "write", "", int64(i)); err != nil {
			t.Fatalf("InsertFSEvent %d: %v", i, err)
		}
	}
	count, err = store.CountFSEvents(ctx)
	if err != nil {
		t.Fatalf("CountFSEvents: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}

// TestDeleteSharedFSEventsBeforeRefusesStandalone verifies the shared-pool
// sweep refuses to run on a non-shared (dedicated tenant) store: an unscoped
// DELETE there would be a catastrophic bug.
func TestDeleteSharedFSEventsBeforeRefusesStandalone(t *testing.T) {
	store := newTestStore(t)
	if _, _, err := store.DeleteSharedFSEventsBefore(context.Background(), time.Now(), 100, 10); err == nil {
		t.Fatal("DeleteSharedFSEventsBefore on a standalone store should return an error")
	}
	// Invalid parameters are rejected (on a shared store these would be;
	// here the shape check fires first, which is also fine).
	if _, _, err := store.DeleteSharedFSEventsBefore(context.Background(), time.Now(), 0, 10); err == nil {
		t.Fatal("batchSize 0 should return an error")
	}
}
