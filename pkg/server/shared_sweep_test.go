package server

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/meta"
)

// sharedFSEventsDDL mirrors the shared-shape fs_events table from
// pkg/tenant/schema/tidb_shared.go (fs_id discriminator + shared indexes).
const sharedFSEventsDDL = `CREATE TABLE IF NOT EXISTS fs_events (
	fs_id      BIGINT       NOT NULL,
	seq        BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	path       TEXT         NOT NULL,
	op         VARCHAR(64)  NOT NULL,
	actor      VARCHAR(255),
	ts         BIGINT       NOT NULL,
	created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	KEY idx_fs_events_created (created_at),
	KEY idx_fs_events_fs_seq (fs_id, seq),
	KEY idx_fs_events_fs_created (fs_id, created_at)
)`

const sharedMaintenanceStateDDL = `CREATE TABLE IF NOT EXISTS shared_maintenance_state (
	name        VARCHAR(64) NOT NULL,
	last_run_at DATETIME(3) NOT NULL,
	PRIMARY KEY (name)
)`

// installSharedFSEventsTable swaps fs_events to the shared (fs_id) shape for
// the duration of the test. The cleanup DROPS the table: every SSE test
// helper recreates the standalone shape via CREATE TABLE IF NOT EXISTS.
// Package tests run sequentially, so the swap is safe (same pattern as
// pkg/datastore's installSharedTables).
func installSharedFSEventsTable(t *testing.T) {
	t.Helper()
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("DROP TABLE IF EXISTS fs_events"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db, err := sql.Open("mysql", testDSN)
		if err != nil {
			t.Errorf("reopen test db for fs_events restore: %v", err)
			return
		}
		defer func() { _ = db.Close() }()
		if _, err := db.Exec("DROP TABLE IF EXISTS fs_events"); err != nil {
			t.Errorf("restore fs_events: %v", err)
		}
	})
	if _, err := db.Exec(sharedFSEventsDDL); err != nil {
		t.Fatal(err)
	}
}

func newSharedSweepStore(t *testing.T, fsID int64) *datastore.Store {
	t.Helper()
	store, err := datastore.OpenForTenantScoped(context.Background(), testDSN, "", "", datastore.SharedScope(fsID))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func insertAgedSharedEvent(t *testing.T, store *datastore.Store, path string) {
	t.Helper()
	ctx := context.Background()
	seq, err := store.InsertFSEvent(ctx, path, "write", "", 1)
	if err != nil {
		t.Fatalf("InsertFSEvent %s: %v", path, err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE fs_events SET created_at = DATE_SUB(NOW(3), INTERVAL 2 HOUR) WHERE seq = ?`, seq); err != nil {
		t.Fatalf("age row %s: %v", path, err)
	}
}

func sharedFSEventCount(t *testing.T, store *datastore.Store) int64 {
	t.Helper()
	var n int64
	if err := store.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM fs_events`).Scan(&n); err != nil {
		t.Fatalf("count fs_events: %v", err)
	}
	return n
}

// waitForSharedFSEventCount polls until the whole (shared) fs_events table
// reaches want rows or the deadline passes. The sweep runs on a detached
// goroutine, so positive assertions must be eventual.
func waitForSharedFSEventCount(t *testing.T, store *datastore.Store, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sharedFSEventCount(t, store) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fs_events count = %d, want %d after %v", sharedFSEventCount(t, store), want, timeout)
}

// TestMaybeSweepSharedFSEventsOrchestration exercises the pre-filter →
// cluster claim → delete orchestration of the shared-pool sweep with a real
// meta DB and a real shared-shape store:
// (a) claim won → the delete runs across all tenants;
// (c) a second call inside the interval is blocked by the per-pod pre-filter
// (the winnable claim is NOT re-attempted);
// (b) claim lost → no delete;
// (d) claim error → the pre-filter is restored, so a later call retries.
func TestMaybeSweepSharedFSEventsOrchestration(t *testing.T) {
	ctx := context.Background()
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	cleanMaintenanceState := func() {
		t.Helper()
		if _, err := metaStore.DB().ExecContext(ctx, `DELETE FROM shared_maintenance_state`); err != nil {
			t.Fatalf("clean shared_maintenance_state: %v", err)
		}
	}
	cleanMaintenanceState()
	t.Cleanup(cleanMaintenanceState)

	installSharedFSEventsTable(t)
	storeA := newSharedSweepStore(t, 7700001)
	storeB := newSharedSweepStore(t, 7700002)

	insertAgedSharedEvent(t, storeA, "/a/1")
	insertAgedSharedEvent(t, storeB, "/b/1")

	// (a) Claim won: the detached goroutine claims and deletes BOTH tenants'
	// aged rows (the sweep is unscoped).
	s1 := &Server{meta: metaStore}
	s1.maybeSweepSharedFSEvents(storeA, "")
	waitForSharedFSEventCount(t, storeA, 0, 3*time.Second)

	// (c) Pre-filter: s1 already swept moments ago. Make the cluster claim
	// winnable again (aged last_run_at) and add another aged row — the second
	// call must still be blocked by the per-pod pre-filter: no claim attempt
	// (last_run_at stays aged) and no delete.
	insertAgedSharedEvent(t, storeA, "/a/2")
	if _, err := metaStore.DB().ExecContext(ctx,
		`UPDATE shared_maintenance_state SET last_run_at = DATE_SUB(NOW(3), INTERVAL 1 HOUR) WHERE name = ?`,
		sharedFSEventsSweepClaimName); err != nil {
		t.Fatal(err)
	}
	s1.maybeSweepSharedFSEvents(storeA, "")
	time.Sleep(500 * time.Millisecond) // negative margin; a local claim+delete takes ms
	if got := sharedFSEventCount(t, storeA); got != 1 {
		t.Fatalf("pre-filter: count = %d, want 1 (second call within the interval must not sweep)", got)
	}
	var lastRun time.Time
	if err := metaStore.DB().QueryRowContext(ctx,
		`SELECT last_run_at FROM shared_maintenance_state WHERE name = ?`,
		sharedFSEventsSweepClaimName).Scan(&lastRun); err != nil {
		t.Fatal(err)
	}
	if time.Since(lastRun) < 30*time.Minute {
		t.Fatalf("pre-filter: claim was re-attempted (last_run_at = %v) despite the per-pod throttle", lastRun)
	}

	// (b) Claim lost: a fresh pod (no local pre-filter) whose claim loses to a
	// fresh last_run_at must NOT delete.
	s2 := &Server{meta: metaStore}
	if _, err := metaStore.DB().ExecContext(ctx,
		`UPDATE shared_maintenance_state SET last_run_at = NOW(3) WHERE name = ?`,
		sharedFSEventsSweepClaimName); err != nil {
		t.Fatal(err)
	}
	s2.maybeSweepSharedFSEvents(storeA, "")
	time.Sleep(500 * time.Millisecond) // negative margin
	if got := sharedFSEventCount(t, storeA); got != 1 {
		t.Fatalf("claim lost: count = %d, want 1 (lost claim must not delete)", got)
	}

	// (d) Claim error: with the table dropped the claim fails; the pre-filter
	// must be restored so the NEXT trigger retries (and then wins + deletes).
	s3 := &Server{meta: metaStore}
	if _, err := metaStore.DB().ExecContext(ctx, `DROP TABLE shared_maintenance_state`); err != nil {
		t.Fatal(err)
	}
	s3.maybeSweepSharedFSEvents(storeA, "")
	time.Sleep(500 * time.Millisecond) // let the erroring goroutine finish + restore
	if _, err := metaStore.DB().ExecContext(ctx, sharedMaintenanceStateDDL); err != nil {
		t.Fatal(err)
	}
	s3.maybeSweepSharedFSEvents(storeA, "")
	waitForSharedFSEventCount(t, storeA, 0, 3*time.Second)
}
