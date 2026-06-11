package datastore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

var errRollbackForTest = errors.New("rollback for test")

func TestFSEventLogInsertBoundsAndReplay(t *testing.T) {
	s := newTestStore(t)
	clearFSEventsForTest(t, s)
	ctx := context.Background()

	ev1, err := s.InsertFSEvent(ctx, "/a.txt", "write", "actor-a")
	if err != nil {
		t.Fatal(err)
	}
	ev2, err := s.InsertFSEvent(ctx, "/b.txt", "chmod", "")
	if err != nil {
		t.Fatal(err)
	}
	if ev1.Seq == 0 || ev2.Seq != ev1.Seq+1 {
		t.Fatalf("seqs = %d, %d; want consecutive positive seqs", ev1.Seq, ev2.Seq)
	}

	oldest, head, count, err := s.FSEventBounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if oldest != ev1.Seq || head != ev2.Seq || count != 1 {
		t.Fatalf("bounds oldest=%d head=%d count=%d; want %d %d non-empty", oldest, head, count, ev1.Seq, ev2.Seq)
	}

	events, err := s.ListFSEventsSince(ctx, ev1.Seq, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events))
	}
	if got := events[0]; got.Seq != ev2.Seq || got.Path != "/b.txt" || got.Op != "chmod" || got.Actor != "" {
		t.Fatalf("event = %+v, want second chmod event", got)
	}
}

func TestFSEventLogPrune(t *testing.T) {
	s := newTestStore(t)
	clearFSEventsForTest(t, s)
	ctx := context.Background()

	ev1, err := s.InsertFSEvent(ctx, "/a.txt", "write", "")
	if err != nil {
		t.Fatal(err)
	}
	ev2, err := s.InsertFSEvent(ctx, "/b.txt", "write", "")
	if err != nil {
		t.Fatal(err)
	}

	deleted, err := s.PruneFSEventsBefore(ctx, ev2.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	oldest, head, count, err := s.FSEventBounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if oldest != ev2.Seq || head != ev2.Seq || count != 1 {
		t.Fatalf("bounds after prune oldest=%d head=%d count=%d; want %d %d 1", oldest, head, count, ev2.Seq, ev2.Seq)
	}
	events, err := s.ListFSEventsSince(ctx, ev1.Seq-1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Seq != ev2.Seq {
		t.Fatalf("events after prune = %+v, want only second event", events)
	}
}

func TestAppendFSEventTxRollbackDoesNotConsumeSeq(t *testing.T) {
	s := newTestStore(t)
	clearFSEventsForTest(t, s)
	ctx := context.Background()

	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		if _, err := s.AppendFSEventTx(ctx, tx, "/rolled-back.txt", "write", ""); err != nil {
			return err
		}
		return errRollbackForTest
	}); err != errRollbackForTest {
		t.Fatalf("rollback tx error = %v, want %v", err, errRollbackForTest)
	}

	ev, err := s.InsertFSEvent(ctx, "/committed.txt", "write", "")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Seq != 1 {
		t.Fatalf("seq after rollback = %d, want 1", ev.Seq)
	}
}

func TestNoopDirectoryMutationsDoNotEmitFSEvents(t *testing.T) {
	s := newTestStore(t)
	clearFSEventsForTest(t, s)
	ctx := context.Background()

	count, err := s.RenameDir(ctx, "/missing/", "/renamed/")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("RenameDir count = %d, want 0", count)
	}
	orphaned, err := s.DeleteDirRecursive(ctx, "/missing/")
	if err != nil {
		t.Fatal(err)
	}
	if len(orphaned) != 0 {
		t.Fatalf("DeleteDirRecursive orphaned len = %d, want 0", len(orphaned))
	}

	events, err := s.ListFSEventsSince(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("events after no-op directory mutations = %+v, want none", events)
	}

	ev, err := s.InsertFSEvent(ctx, "/committed.txt", "write", "")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Seq != 1 {
		t.Fatalf("seq after no-op directory mutations = %d, want 1", ev.Seq)
	}
}

func clearFSEventsForTest(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.DB().Exec(`DELETE FROM fs_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`DELETE FROM fs_event_seq`); err != nil {
		t.Fatal(err)
	}
}
