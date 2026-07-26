package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

func TestRecordSemanticWorkerObservationExportsOnlyAbnormalState(t *testing.T) {
	const tenantID = "tenant-semantic-observation-lifecycle"
	const orgID = "org-semantic-observation-lifecycle"
	now := time.Now().UTC()
	oldest := now.Add(-10 * time.Minute)

	recordSemanticWorkerObservation(tenantID, orgID, &datastore.SemanticTaskObservation{
		DeadLettered: 2, OldestClaimableAvailableAt: &oldest,
	}, now)
	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_service_gauge{component="semantic_worker",name="dead_lettered",tenant_id="`+tenantID+`",tidbcloud_org_id="`+orgID+`"} 2`) {
		t.Fatalf("dead-letter gauge missing in abnormal state:\n%s", text)
	}
	if !strings.Contains(text, `drive9_service_gauge{component="semantic_worker",name="queue_lag_seconds",tenant_id="`+tenantID+`",tidbcloud_org_id="`+orgID+`"} 600`) {
		t.Fatalf("queue-lag gauge missing in abnormal state:\n%s", text)
	}

	recordSemanticWorkerObservation(tenantID, orgID, &datastore.SemanticTaskObservation{}, now)
	rec = httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text = rec.Body.String()
	for _, name := range []string{"dead_lettered", "queue_lag_seconds"} {
		if strings.Contains(text, `drive9_service_gauge{component="semantic_worker",name="`+name+`",tenant_id="`+tenantID+`"`) {
			t.Fatalf("healthy semantic gauge %s remains exported:\n%s", name, text)
		}
	}
}

func TestTenantWorkerSemanticObservationDeletesPreviousOrgSeries(t *testing.T) {
	const tenantID = "tenant-semantic-observation-org-change"
	now := time.Now().UTC()
	oldest := now.Add(-time.Minute)
	m := &tenantWorkerManager{semanticMetricOrg: make(map[string]string)}
	m.recordSemanticWorkerObservation(tenantID, "org-old", &datastore.SemanticTaskObservation{
		DeadLettered: 1, OldestClaimableAvailableAt: &oldest,
	}, now)
	m.recordSemanticWorkerObservation(tenantID, "org-new", &datastore.SemanticTaskObservation{
		DeadLettered: 2, OldestClaimableAvailableAt: &oldest,
	}, now)

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text := rec.Body.String()
	if strings.Contains(text, `tenant_id="`+tenantID+`",tidbcloud_org_id="org-old"`) {
		t.Fatalf("previous-org semantic gauge remains exported:\n%s", text)
	}
	if !strings.Contains(text, `component="semantic_worker",name="dead_lettered",tenant_id="`+tenantID+`",tidbcloud_org_id="org-new"`) {
		t.Fatalf("new-org semantic gauge missing:\n%s", text)
	}
}

func TestTenantWorkerStartDoesNotExportUnmaintainedGlobalQueueGauges(t *testing.T) {
	for _, name := range []string{"queued", "processing", "dead_lettered", "queue_lag_seconds"} {
		metrics.DeleteTenantGauge("", "semantic_worker", name)
	}
	m := &tenantWorkerManager{
		opts:            TenantWorkerOptions{Workers: 1, PollInterval: time.Hour},
		inflight:        make(map[string]int),
		kickPending:     make(map[string]pendingKick),
		lastMaintenance: make(map[string]time.Time),
		kicks:           make(chan kickMsg, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	defer m.Stop()

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text := rec.Body.String()
	for _, name := range []string{"queued", "processing", "dead_lettered", "queue_lag_seconds"} {
		if strings.Contains(text, `drive9_service_gauge{component="semantic_worker",name="`+name+`",tenant_id=""`) {
			t.Fatalf("unmaintained global semantic gauge %s was exported:\n%s", name, text)
		}
	}
}
