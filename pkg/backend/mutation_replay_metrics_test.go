package backend

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

func TestMutationReplayWorkerRecordsPendingBacklogGauges(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.mutations = []fakeMutationRecord{
		{tenantID: "tenant-backlog", id: 1, typ: "file_create", status: "pending", data: []byte(`{}`), createdAt: time.Now().Add(-2 * time.Second)},
		{tenantID: "tenant-backlog", id: 2, typ: "file_create", status: "pending", data: []byte(`{}`), createdAt: time.Now().Add(-time.Second)},
	}

	w := &MutationReplayWorker{store: fake}
	w.recordPendingBacklog(context.Background())

	metricsText := readBackendMetrics()
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="pending_mutations",tenant_id="tenant-backlog",tidbcloud_org_id="guest"} 2`) {
		t.Errorf("metrics missing pending mutation backlog gauge: %s", metricsText)
	}
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="oldest_pending_age_seconds",tenant_id="tenant-backlog",tidbcloud_org_id="guest"}`) {
		t.Errorf("metrics missing oldest pending age gauge: %s", metricsText)
	}

	fake.mu.Lock()
	fake.mutations[0].status = "applied"
	fake.mutations[1].status = "applied"
	fake.mu.Unlock()
	w.recordPendingBacklog(context.Background())

	metricsText = readBackendMetrics()
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="pending_mutations",tenant_id="tenant-backlog",tidbcloud_org_id="guest"} 0`) {
		t.Errorf("metrics did not clear pending mutation backlog gauge: %s", metricsText)
	}
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="oldest_pending_age_seconds",tenant_id="tenant-backlog",tidbcloud_org_id="guest"} 0`) {
		t.Errorf("metrics did not clear oldest pending age gauge: %s", metricsText)
	}
}

func TestMutationReplayWorkerClearsPendingBacklogGaugesOnStop(t *testing.T) {
	w := &MutationReplayWorker{
		observedBacklogTenants: map[string]string{
			"tenant-stop": "guest",
		},
	}
	metrics.RecordTenantGauge("tenant-stop", "mutation_replay", "pending_mutations", 3)
	metrics.RecordTenantGauge("tenant-stop", "mutation_replay", "oldest_pending_age_seconds", 9)

	w.clearPendingBacklogGauges()

	metricsText := readBackendMetrics()
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="pending_mutations",tenant_id="tenant-stop",tidbcloud_org_id="guest"} 0`) {
		t.Errorf("metrics did not clear pending mutation backlog gauge on stop: %s", metricsText)
	}
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="oldest_pending_age_seconds",tenant_id="tenant-stop",tidbcloud_org_id="guest"} 0`) {
		t.Errorf("metrics did not clear oldest pending age gauge on stop: %s", metricsText)
	}
	if len(w.observedBacklogTenants) != 0 {
		t.Errorf("observed backlog tenants = %d, want 0", len(w.observedBacklogTenants))
	}
}

func TestMutationReplayWorkerRecordsPendingBacklogIfDueThrottle(t *testing.T) {
	t.Setenv("DRIVE9_QUOTA_REPLAY_OBSERVE_MS", "10000")
	fake := newFakeMetaQuotaStore()
	fake.mutations = []fakeMutationRecord{
		{tenantID: "tenant-throttle", id: 1, typ: "file_create", status: "pending", data: []byte(`{}`), createdAt: time.Now().Add(-time.Second)},
	}
	w := &MutationReplayWorker{store: fake}

	w.recordPendingBacklogIfDue(context.Background())
	w.recordPendingBacklogIfDue(context.Background())
	fake.mu.Lock()
	calls := fake.observePendingCalls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("ObservePendingMutations calls within throttle window = %d, want 1", calls)
	}

	w.lastBacklogObservation = time.Now().Add(-replayObserveEvery() - time.Millisecond)
	w.recordPendingBacklogIfDue(context.Background())
	fake.mu.Lock()
	calls = fake.observePendingCalls
	fake.mu.Unlock()
	if calls != 2 {
		t.Fatalf("ObservePendingMutations calls after throttle window = %d, want 2", calls)
	}
}

func TestMutationReplayWorkerRunDoesNotClearPendingBacklogGaugesOnFatalExit(t *testing.T) {
	t.Setenv("DRIVE9_QUOTA_REPLAY_POLL_MS", "1")
	store := &fatalListMetaQuotaStore{fakeMetaQuotaStore: newFakeMetaQuotaStore()}
	w := &MutationReplayWorker{
		store: store,
		done:  make(chan struct{}),
		observedBacklogTenants: map[string]string{
			"tenant-fatal-run": "guest",
		},
	}
	metrics.RecordTenantGauge("tenant-fatal-run", "mutation_replay", "pending_mutations", 5)
	metrics.RecordTenantGauge("tenant-fatal-run", "mutation_replay", "oldest_pending_age_seconds", 13)

	w.run(context.Background())

	metricsText := readBackendMetrics()
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="pending_mutations",tenant_id="tenant-fatal-run",tidbcloud_org_id="guest"} 5`) {
		t.Errorf("fatal run should keep pending mutation backlog gauge: %s", metricsText)
	}
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="oldest_pending_age_seconds",tenant_id="tenant-fatal-run",tidbcloud_org_id="guest"} 13`) {
		t.Errorf("fatal run should keep oldest pending age gauge: %s", metricsText)
	}
}

type fatalListMetaQuotaStore struct {
	*fakeMetaQuotaStore
}

func (f *fatalListMetaQuotaStore) ListPendingMutations(ctx context.Context, minAge time.Duration, limit int) ([]MutationLogView, error) {
	return nil, errors.New("database is closed")
}
