package tenant

import (
	"context"
	"sync/atomic"
	"testing"
)

// fakeLeaderChecker is a controllable LeaderChecker for FileGC leader-gating tests.
type fakeLeaderChecker struct {
	leader atomic.Bool
}

func (f *fakeLeaderChecker) IsLeader() bool { return f.leader.Load() }

// TestPoolFileGCStartsOnLeaderGainDuringBackendCreation simulates the race
// where a tenant backend is created while this pod is a standby, and the pod
// gains leadership before the backend is inserted into the pool. The backend
// must end up with FileGC running, because fileGCEnabled is set under p.mu by
// StartAllFileGC and read under p.mu at insertion — so the post-transition
// (leader) state wins rather than the moment-in-time standby snapshot.
func TestPoolFileGCStartsOnLeaderGainDuringBackendCreation(t *testing.T) {
	lc := &fakeLeaderChecker{}
	// Start as standby: FileGC disabled.
	lc.leader.Store(false)
	pool, tnt := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:    2,
		LeaderChecker: lc,
	}, "tenant-leader-gain")
	ctx := context.Background()

	// Simulate a leadership gain that happens while the backend is being
	// created (pool empty): StartAllFileGC only flips fileGCEnabled=true here
	// since no backends are cached yet — the transition "wins" the race.
	// The fake checker is left as standby to prove the outcome is driven by
	// fileGCEnabled (pool-owned state), not by a moment-in-time IsLeader().
	pool.StartAllFileGC()

	// Now create+insert the backend. Insertion reads fileGCEnabled (true) under
	// p.mu and starts FileGC, even though this pod was a standby at create time.
	b, err := pool.Get(ctx, tnt)
	if err != nil {
		t.Fatal(err)
	}
	if !b.FileGCWorkerRunning() {
		t.Fatal("FileGC worker should be running on a backend inserted after a leadership gain that raced with creation")
	}

	// Subsequent cached Get must keep FileGC running (no re-evaluation on hits).
	b2, err := pool.Get(ctx, tnt)
	if err != nil {
		t.Fatal(err)
	}
	if b2 != b {
		t.Fatal("second Get should hit the cache")
	}
	if !b2.FileGCWorkerRunning() {
		t.Fatal("FileGC worker should still be running on a cached hit")
	}
}

// TestPoolFileGCSkipsOnLeaderLossDuringBackendCreation simulates the race
// where a tenant backend is created while this pod is the leader, and the pod
// loses leadership before the backend is inserted into the pool. The backend
// must end up WITHOUT FileGC running, because fileGCEnabled is set to false
// under p.mu by StopAllFileGC and read under p.mu at insertion — so the
// post-transition (standby) state wins rather than the moment-in-time leader
// snapshot.
func TestPoolFileGCSkipsOnLeaderLossDuringBackendCreation(t *testing.T) {
	lc := &fakeLeaderChecker{}
	// Start as leader: FileGC enabled.
	lc.leader.Store(true)
	pool, tnt := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:    2,
		LeaderChecker: lc,
	}, "tenant-leader-loss")
	ctx := context.Background()

	// Simulate a leadership loss that happens while the backend is being
	// created (pool empty): StopAllFileGC only flips fileGCEnabled=false here
	// since no backends are cached yet — the transition "wins" the race.
	pool.StopAllFileGC()

	// Now create+insert the backend. Insertion reads fileGCEnabled (false) under
	// p.mu and does NOT start FileGC, even though this pod was leader at create.
	b, err := pool.Get(ctx, tnt)
	if err != nil {
		t.Fatal(err)
	}
	if b.FileGCWorkerRunning() {
		t.Fatal("FileGC worker should NOT be running on a backend inserted after a leadership loss that raced with creation")
	}

	// A later leadership gain must start FileGC on the cached backend.
	pool.StartAllFileGC()
	if !b.FileGCWorkerRunning() {
		t.Fatal("StartAllFileGC should start FileGC on the cached backend after a leadership regain")
	}
}

// TestPoolFileGCNoLeaderCheckerAlwaysEnabled verifies single-pod mode (nil
// LeaderChecker): fileGCEnabled defaults to true and every backend starts
// FileGC regardless of transitions.
func TestPoolFileGCNoLeaderCheckerAlwaysEnabled(t *testing.T) {
	pool, tnt := newTestPoolAndTenant(t, 2, "tenant-no-leader")
	ctx := context.Background()

	b, err := pool.Get(ctx, tnt)
	if err != nil {
		t.Fatal(err)
	}
	if !b.FileGCWorkerRunning() {
		t.Fatal("FileGC worker should run in single-pod mode (nil LeaderChecker)")
	}

	// StartAllFileGC / StopAllFileGC are no-ops in single-pod mode in practice
	// (the server only calls them when a Leader is configured); verify the
	// default is enabled.
}