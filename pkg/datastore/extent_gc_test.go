package datastore

import (
	"context"
	"testing"
	"time"
)

func TestDeleteOldSliceCommitOpsKeepsRecent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	inode := "inode-ops-gc"
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	insert := `INSERT INTO slice_commit_ops (op_id, inode_id, generation_after, revision_after, size_after, created_at) VALUES (?, ?, 1, 2, 8, ?)`
	for _, row := range []struct {
		op string
		at time.Time
	}{
		{"op-old-1", old},
		{"op-old-2", old.Add(time.Minute)},
		{"op-new", now},
	} {
		if _, err := s.db.ExecContext(ctx, insert, row.op, inode, row.at); err != nil {
			t.Fatalf("insert %s: %v", row.op, err)
		}
	}
	if err := s.DeleteOldSliceCommitOps(ctx, inode, 1, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM slice_commit_ops WHERE inode_id = ?`, inode).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("kept %d rows, want 1", n)
	}
	var opID string
	if err := s.db.QueryRowContext(ctx, `SELECT op_id FROM slice_commit_ops WHERE inode_id = ?`, inode).Scan(&opID); err != nil {
		t.Fatal(err)
	}
	if opID != "op-new" {
		t.Fatalf("kept %q, want op-new", opID)
	}
}
