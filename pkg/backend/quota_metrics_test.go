package backend

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

func TestRecordTenantQuotaSnapshotReplacesOldLimitsAndOrgLabels(t *testing.T) {
	const tenantID = "tenant-quota-snapshot-replace"

	recordTenantQuotaSnapshot(tenantID, "org-old", &QuotaUsageView{
		StorageBytes: 10, MediaFileCount: 2, VideoFileCount: 1,
	}, &QuotaConfigView{
		MaxStorageBytes: 100, MaxMediaLLMFiles: 20, MaxVideoLLMFiles: 10,
	})
	recordTenantQuotaSnapshot(tenantID, "org-new", &QuotaUsageView{
		StorageBytes: 11, MediaFileCount: 3, VideoFileCount: 2,
	}, &QuotaConfigView{})

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text := rec.Body.String()
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, `tenant_id="`+tenantID+`"`) {
			continue
		}
		if strings.Contains(line, `tidbcloud_org_id="org-old"`) {
			t.Fatalf("old org quota series remains exported: %s", line)
		}
		if strings.Contains(line, `state="limit"`) {
			t.Fatalf("removed quota limit remains exported: %s", line)
		}
	}
	if !strings.Contains(text, `drive9_tenant_storage_bytes{state="confirmed",tenant_id="`+tenantID+`",tidbcloud_org_id="org-new"} 11.000000`) {
		t.Fatalf("new quota usage snapshot missing:\n%s", text)
	}
}
