package metrics

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestResultForError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: "ok"},
		{name: "context canceled", err: context.Canceled, want: "canceled"},
		{name: "context deadline", err: fmt.Errorf("query: %w", context.DeadlineExceeded), want: "deadline_exceeded"},
		{name: "bad conn sentinel", err: fmt.Errorf("query: %w", driver.ErrBadConn), want: "bad_conn"},
		{name: "invalid connection string", err: errors.New("driver: invalid connection"), want: "bad_conn"},
		{name: "invalid connection mixed case", err: errors.New("driver: Invalid Connection"), want: "bad_conn"},
		{name: "generic", err: errors.New("quota outbox lease mismatch"), want: "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResultForError(tt.err); got != tt.want {
				t.Fatalf("ResultForError(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestRecordTenantOperationCountDoesNotRecordDuration(t *testing.T) {
	const component = "counter_only_test_component"
	const operation = "counter_only_test_operation"

	RecordTenantOperationCount("tenant-a", component, operation, "ok")

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_service_operations_total{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="tenant-a"} 1`) {
		t.Fatalf("missing counter-only operation total:\n%s", text)
	}
	if strings.Contains(text, `drive9_service_operation_duration_seconds_count{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="tenant-a"}`) {
		t.Fatalf("counter-only operation unexpectedly recorded a duration:\n%s", text)
	}
}

func TestRecordTenantPoolMetadataResumeWaitIncludesPoolAndOrganizationID(t *testing.T) {
	const poolID = "pool-metadata-resume-metrics-test"
	const organizationID = "org-metadata-resume-metrics-test"

	RecordTenantPoolMetadataResumeWait(poolID, organizationID, "group", "ok", 10*time.Minute)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_tenant_pool_metadata_resume_wait_total{organization_id="`+organizationID+`",pool_id="`+poolID+`",result="ok",scope="group"} 1`) {
		t.Fatalf("missing tenant pool metadata resume wait total with pool_id and organization_id:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_metadata_resume_wait_duration_seconds_count{organization_id="`+organizationID+`",pool_id="`+poolID+`",result="ok",scope="group"} 1`) {
		t.Fatalf("missing tenant pool metadata resume wait duration with pool_id and organization_id:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_metadata_resume_wait_duration_seconds_bucket{organization_id="`+organizationID+`",pool_id="`+poolID+`",result="ok",scope="group",le="480"} 0`) {
		t.Fatalf("missing 480s tenant pool metadata resume wait duration bucket:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_metadata_resume_wait_duration_seconds_bucket{organization_id="`+organizationID+`",pool_id="`+poolID+`",result="ok",scope="group",le="600"} 1`) {
		t.Fatalf("missing 600s tenant pool metadata resume wait duration bucket:\n%s", text)
	}
}

func TestRecordHTTPRequestIncludesLongWriteLatencyBuckets(t *testing.T) {
	const route = "/v1/fs/http-long-bucket-test"

	RecordHTTPRequest("POST", route, 200, 25*time.Second)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_http_request_duration_seconds_bucket{method="POST",route="`+route+`",le="20"} 0`) {
		t.Fatalf("missing 20s HTTP duration bucket:\n%s", text)
	}
	if !strings.Contains(text, `drive9_http_request_duration_seconds_bucket{method="POST",route="`+route+`",le="30"} 1`) {
		t.Fatalf("missing 30s HTTP duration bucket:\n%s", text)
	}
}

func TestRecordTenantPoolCapacityIncludesPoolOrganizationAndState(t *testing.T) {
	const poolID = "pool-capacity-metrics-test"
	const organizationID = "org-capacity-metrics-test"

	RecordTenantPoolCapacity(poolID, organizationID, "size", 10)
	RecordTenantPoolCapacity(poolID, organizationID, "free", 1)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_tenant_pool_capacity{organization_id="`+organizationID+`",pool_id="`+poolID+`",state="size"} 10`) {
		t.Fatalf("missing tenant pool size capacity gauge:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_capacity{organization_id="`+organizationID+`",pool_id="`+poolID+`",state="free"} 1`) {
		t.Fatalf("missing tenant pool free capacity gauge:\n%s", text)
	}
}

func TestRecordDBOperationOmitsTenantID(t *testing.T) {
	const tenantID = "tenant-db-operation-test"

	RecordDBOperation("user", "query", "ok", 0)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_db_operations_total{operation="query",result="ok",role="user"} 1`) {
		t.Fatalf("missing db operation total:\n%s", text)
	}
	if strings.Contains(text, `tenant_id="`+tenantID+`"`) {
		t.Fatalf("db operation metrics must not carry tenant_id:\n%s", text)
	}
}
