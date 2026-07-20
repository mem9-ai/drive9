package server

import (
	"testing"
)

func TestTenantWorkerKickDedup(t *testing.T) {
	t.Parallel()
	// A nil manager (no fallback, no pool) is not viable.
	m := newTenantWorkerManager(nil, nil, nil, nil, TenantWorkerOptions{}, 0)
	if m != nil {
		t.Fatal("expected nil manager with no backend")
	}
}

func TestTenantWorkerOptionsNormalize(t *testing.T) {
	t.Parallel()
	opts := TenantWorkerOptions{}
	opts.normalize()
	if opts.Workers != defaultTenantWorkers {
		t.Fatalf("expected default workers %d, got %d", defaultTenantWorkers, opts.Workers)
	}
	if opts.LeaseDuration != defaultTenantLeaseDuration {
		t.Fatalf("expected default lease %v, got %v", defaultTenantLeaseDuration, opts.LeaseDuration)
	}
	if opts.MaintenanceInterval != defaultTenantMaintenanceInterval {
		t.Fatalf("expected default maintenance %v, got %v", defaultTenantMaintenanceInterval, opts.MaintenanceInterval)
	}
}

func TestTenantWorkerKickAccumulation(t *testing.T) {
	t.Parallel()
	// Create a viable manager with a fallback backend that supports extract.
	// We can't easily create a real backend here without MySQL, so test the
	// kick dedup logic directly with a manually constructed manager.
	m := &tenantWorkerManager{
		inflight:    make(map[string]int),
		kickPending: make(map[string]pendingKick),
		kicks:       make(chan kickMsg, tenantKickQueueCapacity),
	}
	// First kick should be queued.
	m.KickWithOrg("t1", "org-t1", WorkSemantic)
	if len(m.kicks) != 1 {
		t.Fatalf("expected 1 queued kick, got %d", len(m.kicks))
	}
	msg := <-m.kicks
	if msg.tidbCloudOrgID != "org-t1" {
		t.Fatalf("queued kick org = %q, want org-t1", msg.tidbCloudOrgID)
	}
	m.kicks <- msg
	// Second kick for same tenant should coalesce (OR-accumulate mask).
	m.Kick("t1", WorkFileGC)
	// kickPending should have the accumulated mask.
	m.mu.Lock()
	pending := m.kickPending["t1"].workMask
	orgID := m.kickPending["t1"].tidbCloudOrgID
	m.mu.Unlock()
	if pending != WorkSemantic|WorkFileGC {
		t.Fatalf("expected accumulated mask %d, got %d", WorkSemantic|WorkFileGC, pending)
	}
	if orgID != "org-t1" {
		t.Fatalf("pending kick org = %q, want org-t1", orgID)
	}
	// Only one kick should be in the channel (coalesced, not duplicated).
	if len(m.kicks) != 1 {
		t.Fatalf("expected 1 queued kick after coalesce, got %d", len(m.kicks))
	}
}

func TestTenantWorkerKickEmptyIgnored(t *testing.T) {
	t.Parallel()
	m := &tenantWorkerManager{
		inflight:    make(map[string]int),
		kickPending: make(map[string]pendingKick),
		kicks:       make(chan kickMsg, tenantKickQueueCapacity),
	}
	m.Kick("t1", 0)
	if len(m.kicks) != 0 {
		t.Fatal("zero mask kick should be ignored")
	}
	m.Kick("", WorkSemantic)
	if len(m.kicks) != 0 {
		t.Fatal("empty tenant kick should be ignored")
	}
}

func TestTenantWorkerTakePendingWorkMask(t *testing.T) {
	t.Parallel()
	m := &tenantWorkerManager{
		inflight:    make(map[string]int),
		kickPending: make(map[string]pendingKick),
		kicks:       make(chan kickMsg, tenantKickQueueCapacity),
	}
	m.kickPending["t1"] = pendingKick{workMask: WorkSemantic | WorkFileGC, tidbCloudOrgID: "org-t1"}
	mask := m.takePendingWorkMask("t1")
	if mask != WorkSemantic|WorkFileGC {
		t.Fatalf("expected mask %d, got %d", WorkSemantic|WorkFileGC, mask)
	}
	// Second take should return 0 (cleared).
	mask2 := m.takePendingWorkMask("t1")
	if mask2 != 0 {
		t.Fatalf("expected 0 after take, got %d", mask2)
	}
}

func TestTenantWorkerTakePendingKickIncludesOrg(t *testing.T) {
	t.Parallel()
	m := &tenantWorkerManager{
		inflight:    make(map[string]int),
		kickPending: make(map[string]pendingKick),
		kicks:       make(chan kickMsg, tenantKickQueueCapacity),
	}
	m.kickPending["t1"] = pendingKick{workMask: WorkSemantic, tidbCloudOrgID: "org-t1"}

	pending := m.takePendingKick("t1")
	if pending.workMask != WorkSemantic {
		t.Fatalf("pending mask = %d, want %d", pending.workMask, WorkSemantic)
	}
	if pending.tidbCloudOrgID != "org-t1" {
		t.Fatalf("pending org = %q, want org-t1", pending.tidbCloudOrgID)
	}
	if _, ok := m.kickPending["t1"]; ok {
		t.Fatal("pending kick should be cleared")
	}
}
