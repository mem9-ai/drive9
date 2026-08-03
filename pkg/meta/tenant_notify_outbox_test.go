package meta

import (
	"context"
	"testing"
	"time"
)

// resetTenantOutboxTables clears the outbox tables; ResetMetaDB does not know
// about them, so clean them explicitly between cases.
func resetTenantOutboxTables(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"tenant_notify_outbox", "tenant_outbox_cursor"} {
		if _, err := s.DB().ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("clean up %s: %v", table, err)
		}
	}
}

// insertOldTenantNotify inserts one outbox row with an old created_at and
// returns its id.
func insertOldTenantNotify(t *testing.T, s *Store, tenantID string, age time.Duration) uint64 {
	t.Helper()
	res, err := s.DB().ExecContext(context.Background(),
		`INSERT INTO tenant_notify_outbox (tenant_id, work_mask, created_at) VALUES (?, ?, ?)`,
		tenantID, 1, time.Now().Add(-age))
	if err != nil {
		t.Fatalf("insert old outbox row: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return uint64(id)
}

func listTenantNotifyIDs(t *testing.T, s *Store) map[uint64]bool {
	t.Helper()
	rows, err := s.ListTenantNotifySince(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("ListTenantNotifySince: %v", err)
	}
	ids := make(map[uint64]bool, len(rows))
	for _, r := range rows {
		ids[r.ID] = true
	}
	return ids
}

// TestDeleteTenantNotifyBeforeFreshFloor verifies the pruning floor: rows are
// pruned up to MIN(last_id) across fresh cursors, stale cursors are ignored,
// and age-only pruning applies when no fresh cursors remain.
func TestDeleteTenantNotifyBeforeFreshFloor(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	cutoff := func() time.Time { return time.Now().Add(-time.Hour) }

	// Case A: a fresh cursor holds the floor — rows above its last_id survive
	// even when older than the retention.
	resetTenantOutboxTables(t, s)
	id1 := insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	id2 := insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	id3 := insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	if err := s.UpsertTenantOutboxCursor(ctx, "pod-fresh", id2); err != nil {
		t.Fatal(err)
	}
	n, err := s.DeleteTenantNotifyBefore(ctx, cutoff())
	if err != nil {
		t.Fatalf("DeleteTenantNotifyBefore case A: %v", err)
	}
	if n != 2 {
		t.Fatalf("case A deleted = %d, want 2 (floor at cursor last_id)", n)
	}
	ids := listTenantNotifyIDs(t, s)
	if ids[id1] || ids[id2] {
		t.Fatalf("case A: rows <= floor should be pruned: %v", ids)
	}
	if !ids[id3] {
		t.Fatalf("case A: row above fresh cursor floor must survive: %v", ids)
	}

	// Case B: the only cursor is stale (updated_at older than the freshness
	// bound) — it is ignored, so pruning falls back to age alone.
	resetTenantOutboxTables(t, s)
	id1 = insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	id2 = insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	id3 = insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	if err := s.UpsertTenantOutboxCursor(ctx, "pod-stalled", id1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE tenant_outbox_cursor SET updated_at = ? WHERE pod_id = ?`,
		time.Now().Add(-2*tenantOutboxCursorFreshnessBound), "pod-stalled"); err != nil {
		t.Fatal(err)
	}
	n, err = s.DeleteTenantNotifyBefore(ctx, cutoff())
	if err != nil {
		t.Fatalf("DeleteTenantNotifyBefore case B: %v", err)
	}
	if n != 3 {
		t.Fatalf("case B deleted = %d, want 3 (stale cursor ignored, age-only prune)", n)
	}
	if ids = listTenantNotifyIDs(t, s); ids[id1] || ids[id2] || ids[id3] {
		t.Fatalf("case B: all old rows should be pruned: %v", ids)
	}

	// Case C: a stale cursor with a low last_id alongside a fresh cursor with
	// a high last_id — the floor comes from the fresh cursor only.
	resetTenantOutboxTables(t, s)
	id1 = insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	id2 = insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	id3 = insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	if err := s.UpsertTenantOutboxCursor(ctx, "pod-stalled", id1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE tenant_outbox_cursor SET updated_at = ? WHERE pod_id = ?`,
		time.Now().Add(-2*tenantOutboxCursorFreshnessBound), "pod-stalled"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTenantOutboxCursor(ctx, "pod-fresh", id2); err != nil {
		t.Fatal(err)
	}
	n, err = s.DeleteTenantNotifyBefore(ctx, cutoff())
	if err != nil {
		t.Fatalf("DeleteTenantNotifyBefore case C: %v", err)
	}
	if n != 2 {
		t.Fatalf("case C deleted = %d, want 2 (floor from fresh cursor)", n)
	}
	if ids = listTenantNotifyIDs(t, s); ids[id1] || ids[id2] || !ids[id3] {
		t.Fatalf("case C: want id1,id2 pruned and id3 retained: %v", ids)
	}

	// Case D: no cursors at all — plain age-only pruning, fresh rows survive.
	resetTenantOutboxTables(t, s)
	insertOldTenantNotify(t, s, "tenant-a", 2*time.Hour)
	if err := s.InsertTenantNotify(ctx, "tenant-a", 1); err != nil {
		t.Fatal(err)
	}
	n, err = s.DeleteTenantNotifyBefore(ctx, cutoff())
	if err != nil {
		t.Fatalf("DeleteTenantNotifyBefore case D: %v", err)
	}
	if n != 1 {
		t.Fatalf("case D deleted = %d, want 1", n)
	}
	if got := len(listTenantNotifyIDs(t, s)); got != 1 {
		t.Fatalf("case D: fresh row must survive, remaining = %d", got)
	}
}

// TestUpsertTenantOutboxCursorRefreshesUpdatedAt verifies the cursor's
// updated_at is refreshed on every upsert — the freshness bound in
// DeleteTenantNotifyBefore depends on it.
func TestUpsertTenantOutboxCursorRefreshesUpdatedAt(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	resetTenantOutboxTables(t, s)

	if err := s.UpsertTenantOutboxCursor(ctx, "pod-a", 10); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-2 * tenantOutboxCursorFreshnessBound)
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE tenant_outbox_cursor SET updated_at = ? WHERE pod_id = ?`, stale, "pod-a"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTenantOutboxCursor(ctx, "pod-a", 20); err != nil {
		t.Fatal(err)
	}
	cursor, err := s.GetTenantOutboxCursor(ctx, "pod-a")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.LastID != 20 {
		t.Fatalf("last_id = %d, want 20", cursor.LastID)
	}
	if time.Since(cursor.UpdatedAt) > tenantOutboxCursorFreshnessBound {
		t.Fatalf("updated_at = %v not refreshed by upsert", cursor.UpdatedAt)
	}
}
