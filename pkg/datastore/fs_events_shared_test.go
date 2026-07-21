package datastore

import (
	"context"
	"testing"
	"time"
)

// TestFSEventsSharedShapeParity exercises insert / list-since / seq helpers /
// retention against the shared (fs_id) schema shape.
func TestFSEventsSharedShapeParity(t *testing.T) {
	installSharedCoreFSSchema(t)
	const fsID int64 = 4400010
	store := newSharedStore(t, fsID)
	ctx := context.Background()

	seq1, err := store.InsertFSEvent(ctx, "/a.txt", "create", "tester", 100)
	if err != nil {
		t.Fatalf("InsertFSEvent 1: %v", err)
	}
	seq2, err := store.InsertFSEvent(ctx, "/b.txt", "delete", "", 101)
	if err != nil {
		t.Fatalf("InsertFSEvent 2: %v", err)
	}
	if seq2 <= seq1 {
		t.Fatalf("seq2 = %d, want > seq1 = %d", seq2, seq1)
	}

	events, err := store.ListFSEventsSince(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListFSEventsSince: %v", err)
	}
	if len(events) != 2 || events[0].Path != "/a.txt" || events[1].Path != "/b.txt" {
		t.Fatalf("events = %#v, want /a.txt then /b.txt", events)
	}
	if events[0].Actor != "tester" || events[1].Actor != "" {
		t.Fatalf("actors = %q, %q; want tester, \"\"", events[0].Actor, events[1].Actor)
	}
	events, err = store.ListFSEventsSince(ctx, seq1, 10)
	if err != nil {
		t.Fatalf("ListFSEventsSince seq1: %v", err)
	}
	if len(events) != 1 || events[0].Seq != uint64(seq2) {
		t.Fatalf("events since seq1 = %#v, want only seq %d", events, seq2)
	}

	latest, err := store.LatestFSEventSeq(ctx)
	if err != nil {
		t.Fatalf("LatestFSEventSeq: %v", err)
	}
	if latest != seq2 {
		t.Fatalf("latest = %d, want %d", latest, seq2)
	}
	oldest, err := store.OldestFSEventSeq(ctx)
	if err != nil {
		t.Fatalf("OldestFSEventSeq: %v", err)
	}
	if oldest != seq1 {
		t.Fatalf("oldest = %d, want %d", oldest, seq1)
	}
	count, err := store.CountFSEvents(ctx)
	if err != nil {
		t.Fatalf("CountFSEvents: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	// Retention: age the first row, sweep, and only that row is deleted.
	old := time.Now().Add(-2 * time.Hour)
	if _, err := store.DB().Exec(`UPDATE fs_events SET created_at = ? WHERE seq = ?`, old, seq1); err != nil {
		t.Fatalf("age row: %v", err)
	}
	deleted, err := store.DeleteFSEventsBefore(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("DeleteFSEventsBefore: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	events, err = store.ListFSEventsSince(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListFSEventsSince after sweep: %v", err)
	}
	if len(events) != 1 || events[0].Seq != uint64(seq2) {
		t.Fatalf("events after sweep = %#v, want only seq %d", events, seq2)
	}
}

// TestFSEventsSharedShapeInterleaving appends events from two tenants
// alternately and asserts each store sees only its own events in seq order.
// Because seq is a table-global AUTO_INCREMENT in shared shape, each tenant's
// seq stream has holes where the other tenant's events landed — per-tenant
// gaps are expected; the SSE gap/reset handling for them lives in pkg/server
// eventbus (separate change).
func TestFSEventsSharedShapeInterleaving(t *testing.T) {
	installSharedCoreFSSchema(t)
	const fsA, fsB int64 = 4400011, 4400012
	storeA := newSharedStore(t, fsA)
	storeB := newSharedStore(t, fsB)
	ctx := context.Background()

	var seqsA []int64
	for i, tc := range []struct {
		store *Store
		path  string
	}{
		{storeA, "/a/1"}, {storeB, "/b/1"},
		{storeA, "/a/2"}, {storeB, "/b/2"},
		{storeA, "/a/3"},
	} {
		seq, err := tc.store.InsertFSEvent(ctx, tc.path, "write", "tester", int64(200+i))
		if err != nil {
			t.Fatalf("InsertFSEvent %s: %v", tc.path, err)
		}
		if tc.store == storeA {
			seqsA = append(seqsA, seq)
		}
	}

	events, err := storeA.ListFSEventsSince(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListFSEventsSince A: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("A sees %d events, want 3 (cross-tenant leak): %#v", len(events), events)
	}
	for i, ev := range events {
		wantPath := []string{"/a/1", "/a/2", "/a/3"}[i]
		if ev.Path != wantPath || ev.Seq != uint64(seqsA[i]) {
			t.Fatalf("A event %d = (%d, %s), want (%d, %s)", i, ev.Seq, ev.Path, seqsA[i], wantPath)
		}
	}
	// B's interleaved seqs must leave holes in A's stream.
	if events[2].Seq-events[0].Seq == 2 {
		t.Fatalf("expected seq holes from interleaved tenant, got contiguous: %#v", events)
	}

	countB, err := storeB.CountFSEvents(ctx)
	if err != nil {
		t.Fatalf("CountFSEvents B: %v", err)
	}
	if countB != 2 {
		t.Fatalf("B count = %d, want 2", countB)
	}

	// A's retention sweep must delete only A's rows.
	deleted, err := storeA.DeleteFSEventsBefore(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("DeleteFSEventsBefore A: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("A deleted = %d, want 3", deleted)
	}
	eventsB, err := storeB.ListFSEventsSince(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListFSEventsSince B: %v", err)
	}
	if len(eventsB) != 2 || eventsB[0].Path != "/b/1" || eventsB[1].Path != "/b/2" {
		t.Fatalf("B events after A's sweep = %#v, want /b/1 and /b/2", eventsB)
	}
	var total int64
	if err := storeA.DB().QueryRow(`SELECT COUNT(*) FROM fs_events`).Scan(&total); err != nil {
		t.Fatalf("count all fs_events: %v", err)
	}
	if total != 2 {
		t.Fatalf("total fs_events rows = %d, want 2 (only B's rows survive)", total)
	}
}
