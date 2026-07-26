package server

import (
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
