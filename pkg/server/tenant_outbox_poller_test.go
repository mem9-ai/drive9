package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

// mockKicker records kicks for deterministic testing of the poller.
type mockKicker struct {
	kicks []kickMsg
}

func (m *mockKicker) KickWithOrg(tenantID, tidbCloudOrgID string, workMask int) {
	m.kicks = append(m.kicks, kickMsg{tenantID: tenantID, tidbCloudOrgID: tidbCloudOrgID, workMask: workMask})
}

func TestTenantOutboxPollerShardFilter(t *testing.T) {
	t.Parallel()
	// In single-pod mode (shardFn nil → owns everything), all sharded kicks pass.
	buses := newEventBuses()
	k := &mockKicker{}
	p := newTenantOutboxPoller(nil, buses, k, nil, "pod1", 0, 0)
	row := meta.TenantNotifyRow{ID: 1, TenantID: "t1", WorkMask: WorkSemantic | WorkSSE, CreatedAt: time.Now()}
	p.dispatch(context.Background(), row)
	if len(k.kicks) != 1 {
		t.Fatalf("expected 1 kick, got %d", len(k.kicks))
	}
	if k.kicks[0].workMask != WorkSemantic {
		t.Fatalf("expected WorkSemantic mask, got %d", k.kicks[0].workMask)
	}
}

func TestTenantOutboxPollerPassesOrgToWorker(t *testing.T) {
	buses := newEventBuses()
	k := &mockKicker{}
	p := newTenantOutboxPoller(nil, buses, k, nil, "pod1", 0, 0)
	row := meta.TenantNotifyRow{ID: 1, TenantID: "tenant-outbox-org-metric", TiDBCloudOrgID: "org-outbox-metric", WorkMask: WorkFileGC, CreatedAt: time.Now()}
	p.dispatch(context.Background(), row)
	if len(k.kicks) != 1 {
		t.Fatalf("expected 1 kick, got %d", len(k.kicks))
	}
	if k.kicks[0].tidbCloudOrgID != "org-outbox-metric" {
		t.Fatalf("kick org = %q, want org-outbox-metric", k.kicks[0].tidbCloudOrgID)
	}
	recorder := httptest.NewRecorder()
	metrics.WritePrometheus(recorder)
	want := `drive9_service_operations_total{component="user_db_access",operation="outbox_dispatch_kick",result="ok"}`
	if !strings.Contains(recorder.Body.String(), want) {
		t.Fatalf("missing aggregate outbox dispatch metric %q", want)
	}
	if strings.Contains(recorder.Body.String(), `drive9_service_operations_total{component="user_db_access",operation="outbox_dispatch_kick",result="ok",tenant_id=`) {
		t.Fatalf("successful outbox dispatch metric unexpectedly carries tenant labels: %s", recorder.Body.String())
	}
}

func TestTenantOutboxPollerSSEOnlyNoKick(t *testing.T) {
	t.Parallel()
	buses := newEventBuses()
	k := &mockKicker{}
	p := newTenantOutboxPoller(nil, buses, k, nil, "pod1", 0, 0)
	// SSE-only row: should wake bus but NOT kick the worker.
	row := meta.TenantNotifyRow{ID: 1, TenantID: "t1", WorkMask: WorkSSE, CreatedAt: time.Now()}
	p.dispatch(context.Background(), row)
	if len(k.kicks) != 0 {
		t.Fatalf("SSE-only row should not kick worker, got %d kicks", len(k.kicks))
	}
}

func TestTenantOutboxPollerMetricsCleanupIsBroadcast(t *testing.T) {
	tenantID := "tenant-outbox-metrics-cleanup"
	orgID := "org-outbox-metrics-cleanup"
	metrics.RecordTenantOperationWithOrg(tenantID, orgID, "event_bus", "publish", "error", 0)
	metrics.RecordTenantGaugeWithOrg(tenantID, orgID, "semantic_worker", "queue_lag_seconds", 10)

	buses := newEventBuses()
	k := &mockKicker{}
	// A shard-rejected pod must still clean its local metric registry because
	// cleanup is broadcast, unlike semantic and file-GC work.
	p := newTenantOutboxPoller(nil, buses, k, func(string) bool { return false }, "pod1", 0, 0)
	p.dispatch(context.Background(), meta.TenantNotifyRow{
		ID:        1,
		TenantID:  tenantID,
		WorkMask:  WorkMetricsCleanup,
		CreatedAt: time.Now(),
	})

	recorder := httptest.NewRecorder()
	metrics.WritePrometheus(recorder)
	if strings.Contains(recorder.Body.String(), tenantID) {
		t.Fatalf("tenant metrics remain after broadcast cleanup: %s", recorder.Body.String())
	}
	if len(k.kicks) != 0 {
		t.Fatalf("metrics cleanup should not kick sharded work, got %d kicks", len(k.kicks))
	}
}

func TestTenantOutboxPollerShardRejects(t *testing.T) {
	t.Parallel()
	buses := newEventBuses()
	k := &mockKicker{}
	// shardFn always returns false → this pod doesn't own the tenant.
	shardFn := func(string) bool { return false }
	p := newTenantOutboxPoller(nil, buses, k, shardFn, "pod1", 0, 0)
	row := meta.TenantNotifyRow{ID: 1, TenantID: "t1", WorkMask: WorkSemantic, CreatedAt: time.Now()}
	p.dispatch(context.Background(), row)
	if len(k.kicks) != 0 {
		t.Fatalf("shard-rejected tenant should not be kicked")
	}
}

func TestTenantOutboxPollerDefaults(t *testing.T) {
	t.Parallel()
	p := newTenantOutboxPoller(nil, nil, nil, nil, "", 0, 0)
	if p.interval != defaultTenantOutboxPollInterval {
		t.Fatalf("expected default interval, got %v", p.interval)
	}
	if p.cursorFlushEvery != defaultTenantOutboxCursorFlushInterval {
		t.Fatalf("expected default cursor flush, got %v", p.cursorFlushEvery)
	}
}
