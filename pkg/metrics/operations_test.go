package metrics

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
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

	RecordTenantPoolMetadataResumeWait(poolID, organizationID, "group", "ok", 0)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_tenant_pool_metadata_resume_wait_total{organization_id="`+organizationID+`",pool_id="`+poolID+`",result="ok",scope="group"} 1`) {
		t.Fatalf("missing tenant pool metadata resume wait total with pool_id and organization_id:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_metadata_resume_wait_duration_seconds_count{organization_id="`+organizationID+`",pool_id="`+poolID+`",result="ok",scope="group"} 1`) {
		t.Fatalf("missing tenant pool metadata resume wait duration with pool_id and organization_id:\n%s", text)
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
