package leader

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
)

func TestLeaderDisabledAlwaysLeader(t *testing.T) {
	var leadCount atomic.Int32
	m := NewManager(nil, WithDisabled(), WithCallbacks(
		func() { leadCount.Add(1) },
		nil,
	))
	m.Start(context.Background())
	t.Cleanup(func() { m.Stop() })

	if !m.IsLeader() {
		t.Fatal("disabled manager should report IsLeader=true")
	}
	if leadCount.Load() != 1 {
		t.Fatalf("onLead should fire once, got %d", leadCount.Load())
	}
}

func TestLeaderNilDBIsDisabled(t *testing.T) {
	m := NewManager(nil)
	if !m.IsLeader() {
		t.Fatal("nil DB should make manager disabled (always leader)")
	}
}

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := testmysql.OpenDB(t, testDSN)
	// Leader election is connection-scoped (GET_LOCK/IS_USED_LOCK/RELEASE_LOCK)
	// and does not touch any tables, but reset keeps the package consistent with
	// the repo's shared-MySQL convention.
	testmysql.ResetDB(t, db)
	return db
}

func TestLeaderAcquireAndLose(t *testing.T) {
	db := newTestDB(t)

	var mu sync.Mutex
	leadCount := 0
	loseCount := 0

	m1 := NewManager(db,
		WithLockName("drive9:test:acquire-lose"),
		WithHeartbeatInterval(100*time.Millisecond),
		WithCallbacks(
			func() { mu.Lock(); leadCount++; mu.Unlock() },
			func() { mu.Lock(); loseCount++; mu.Unlock() },
		),
	)
	m1.Start(context.Background())
	t.Cleanup(func() { m1.Stop() })

	// Wait for m1 to become leader.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m1.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !m1.IsLeader() {
		t.Fatal("m1 should become leader")
	}

	// m2 tries to acquire the same lock — should not become leader.
	m2 := NewManager(db,
		WithLockName("drive9:test:acquire-lose"),
		WithHeartbeatInterval(100*time.Millisecond),
	)
	m2.Start(context.Background())
	t.Cleanup(func() { m2.Stop() })

	time.Sleep(300 * time.Millisecond)
	if m2.IsLeader() {
		t.Fatal("m2 should not be leader while m1 holds the lock")
	}

	// Stop m1 — releases the lock. m2 should acquire it.
	m1.Stop()

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m2.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !m2.IsLeader() {
		t.Fatal("m2 should become leader after m1 releases the lock")
	}

	mu.Lock()
	if leadCount != 1 {
		t.Fatalf("m1 onLead should fire once, got %d", leadCount)
	}
	if loseCount != 1 {
		t.Fatalf("m1 onLose should fire once, got %d", loseCount)
	}
	mu.Unlock()
}

func TestLeaderExclusive(t *testing.T) {
	db := newTestDB(t)

	m1 := NewManager(db,
		WithLockName("drive9:test:exclusive"),
		WithHeartbeatInterval(100*time.Millisecond),
	)
	m2 := NewManager(db,
		WithLockName("drive9:test:exclusive"),
		WithHeartbeatInterval(100*time.Millisecond),
	)
	m1.Start(context.Background())
	m2.Start(context.Background())
	t.Cleanup(func() { m2.Stop() })
	t.Cleanup(func() { m1.Stop() })

	// Wait a bit for both to settle.
	time.Sleep(500 * time.Millisecond)

	// Exactly one should be leader.
	l1, l2 := m1.IsLeader(), m2.IsLeader()
	if l1 && l2 {
		t.Fatal("both managers should not be leader simultaneously")
	}
	if !l1 && !l2 {
		t.Fatal("at least one manager should be leader")
	}
}

func TestLeaderCheckerInterface(t *testing.T) {
	var _ LeaderChecker = (*Manager)(nil)

	// A disabled manager satisfies LeaderChecker and always returns true.
	m := NewManager(nil, WithDisabled())
	var checker LeaderChecker = m
	if !checker.IsLeader() {
		t.Fatal("LeaderChecker.IsLeader should return true for disabled manager")
	}
}

func TestLeaderDirectDBCheck(t *testing.T) {
	// Verify the GET_LOCK / RELEASE_LOCK primitives work as expected.
	db := newTestDB(t)

	ctx := context.Background()
	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn1.Close() })

	var got sql.NullInt64
	if err := conn1.QueryRowContext(ctx, getLockSQL, "drive9:test:primitive").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Valid || got.Int64 != 1 {
		t.Fatalf("GET_LOCK should return 1, got %+v", got)
	}

	// The owning connection ID should match what IS_USED_LOCK reports.
	var connID int64
	if err := conn1.QueryRowContext(ctx, connectionIDSQL).Scan(&connID); err != nil {
		t.Fatal(err)
	}
	var ownerID sql.NullInt64
	if err := conn1.QueryRowContext(ctx, isUsedLockSQL, "drive9:test:primitive").Scan(&ownerID); err != nil {
		t.Fatal(err)
	}
	if !ownerID.Valid || ownerID.Int64 != connID {
		t.Fatalf("IS_USED_LOCK owner %d should match CONNECTION_ID() %d", ownerID.Int64, connID)
	}

	// A second connection should fail to acquire the same lock.
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn2.Close() })

	var got2 sql.NullInt64
	if err := conn2.QueryRowContext(ctx, getLockSQL, "drive9:test:primitive").Scan(&got2); err != nil {
		t.Fatal(err)
	}
	if got2.Valid && got2.Int64 == 1 {
		t.Fatal("second GET_LOCK should not acquire an already-held lock")
	}

	// Release.
	var released sql.NullInt64
	if err := conn1.QueryRowContext(ctx, releaseLockSQL, "drive9:test:primitive").Scan(&released); err != nil {
		t.Fatal(err)
	}
	if !released.Valid || released.Int64 != 1 {
		t.Fatalf("RELEASE_LOCK should return 1, got %+v", released)
	}
}

// TestLeaderCtxCancellation verifies that cancelling the parent context passed
// to Start stops the election loop (the loop now derives workerCtx from ctx
// rather than context.Background()).
func TestLeaderCtxCancellation(t *testing.T) {
	db := newTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	m := NewManager(db,
		WithLockName("drive9:test:ctx-cancel"),
		WithHeartbeatInterval(100*time.Millisecond),
	)
	m.Start(ctx)
	t.Cleanup(func() { m.Stop() })

	// Wait for m to become leader.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !m.IsLeader() {
		t.Fatal("m should become leader")
	}

	// Cancel the parent context; the election loop should exit and the manager
	// should stop being leader once holdLeadership returns.
	cancel()

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !m.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if m.IsLeader() {
		t.Fatal("m should lose leadership after parent context cancellation")
	}
}