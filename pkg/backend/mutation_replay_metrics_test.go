package backend

import (
	"context"
	"strings"
	"testing"
	"time"
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
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="pending_mutations",tenant_id="tenant-backlog"} 2`) {
		t.Fatalf("metrics missing pending mutation backlog gauge: %s", metricsText)
	}
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="oldest_pending_age_seconds",tenant_id="tenant-backlog"}`) {
		t.Fatalf("metrics missing oldest pending age gauge: %s", metricsText)
	}

	fake.mu.Lock()
	fake.mutations[0].status = "applied"
	fake.mutations[1].status = "applied"
	fake.mu.Unlock()
	w.recordPendingBacklog(context.Background())

	metricsText = readBackendMetrics()
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="pending_mutations",tenant_id="tenant-backlog"} 0`) {
		t.Fatalf("metrics did not clear pending mutation backlog gauge: %s", metricsText)
	}
	if !strings.Contains(metricsText, `drive9_service_gauge{component="mutation_replay",name="oldest_pending_age_seconds",tenant_id="tenant-backlog"} 0`) {
		t.Fatalf("metrics did not clear oldest pending age gauge: %s", metricsText)
	}
}
