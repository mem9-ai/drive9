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

func hasMetricLineWith(text, metric string, fragments ...string) bool {
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, metric+"{") && !strings.HasPrefix(line, metric+" ") {
			continue
		}
		matches := true
		for _, fragment := range fragments {
			if !strings.Contains(line, fragment) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

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

	RecordTenantOperationCountWithOrg("tenant-a", "org-counter-only", component, operation, "ok")

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_service_operations_total{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="tenant-a",tidbcloud_org_id="org-counter-only"} 1`) {
		t.Fatalf("missing counter-only operation total:\n%s", text)
	}
	if strings.Contains(text, `drive9_service_operation_duration_seconds_count{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="tenant-a"}`) {
		t.Fatalf("counter-only operation unexpectedly recorded a duration:\n%s", text)
	}
}

func TestRecordTenantOperationZeroDurationDoesNotRecordDuration(t *testing.T) {
	const component = "zero_duration_test_component"
	const operation = "zero_duration_test_operation"

	RecordTenantOperationWithOrg("tenant-zero", "org-zero-duration", component, operation, "ok", 0)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_service_operations_total{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="tenant-zero",tidbcloud_org_id="org-zero-duration"} 1`) {
		t.Fatalf("missing zero-duration operation total:\n%s", text)
	}
	if strings.Contains(text, `drive9_service_operation_duration_seconds_count{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="tenant-zero"}`) {
		t.Fatalf("zero-duration operation unexpectedly recorded a duration:\n%s", text)
	}
}

func TestRecordTenantOperationOmitsTenantFromDurationHistograms(t *testing.T) {
	const operation = "duration_tenant_omit_test_operation"

	for _, component := range []string{"backend", "central_quota", "file_gc", "quota_config_cache", "server_quota", "user_db_access", "vault"} {
		t.Run(component, func(t *testing.T) {
			tenantID := "tenant-duration-omit-" + component
			RecordTenantOperationWithOrg(tenantID, "org-"+component, component, operation, "ok", time.Second)

			rec := httptest.NewRecorder()
			WritePrometheus(rec)
			text := rec.Body.String()
			if !strings.Contains(text, `drive9_service_operations_total{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="`+tenantID+`",tidbcloud_org_id="org-`+component+`"} 1`) {
				t.Fatalf("missing tenant-scoped operation total:\n%s", text)
			}
			if !strings.Contains(text, `drive9_service_operation_duration_seconds_count{component="`+component+`",operation="`+operation+`",result="ok"} 1`) {
				t.Fatalf("missing tenant-omitted duration histogram:\n%s", text)
			}
			if strings.Contains(text, `drive9_service_operation_duration_seconds_count{component="`+component+`",operation="`+operation+`",result="ok",tenant_id="`+tenantID+`"}`) {
				t.Fatalf("duration histogram unexpectedly carried tenant_id:\n%s", text)
			}
		})
	}
}

func TestRecordTenantRequestOmitsHighCardinalityLabels(t *testing.T) {
	const (
		tenantID = "tenant-request-duration-omit"
		surface  = "request_duration_surface"
		action   = "request_duration_action"
	)

	RecordTenantRequestWithOrg(tenantID, "org-request-duration", surface, action, "ok", 201, time.Second)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_tenant_requests_total{action="`+action+`",result="ok",status_class="2xx",surface="`+surface+`",tenant_id="`+tenantID+`",tidbcloud_org_id="org-request-duration"} 1`) {
		t.Errorf("missing tenant-scoped request total:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_request_duration_seconds_count{status_class="2xx",surface="`+surface+`"} 1`) {
		t.Errorf("missing reduced request duration histogram:\n%s", text)
	}
	if hasMetricLineWith(text, "drive9_tenant_requests_total", `status="201"`) {
		t.Errorf("tenant request total unexpectedly carried raw status label:\n%s", text)
	}
	for _, unexpected := range []string{
		`action="` + action + `"`,
		`result="ok"`,
		`tenant_id="` + tenantID + `"`,
		`tidbcloud_org_id="org-request-duration"`,
	} {
		if hasMetricLineWith(text, "drive9_tenant_request_duration_seconds_count", unexpected) {
			t.Errorf("tenant request duration unexpectedly carried label %q:\n%s", unexpected, text)
		}
	}
}

func TestRecordTenantRequestZeroDurationDoesNotRecordDuration(t *testing.T) {
	const tenantID = "tenant-zero-request"

	RecordTenantRequest(tenantID, "zero_surface", "zero_action", "ok", 200, 0)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_tenant_requests_total{action="zero_action",result="ok",status_class="2xx",surface="zero_surface",tenant_id="`+tenantID+`",tidbcloud_org_id="guest"} 1`) {
		t.Errorf("missing zero-duration tenant request total:\n%s", text)
	}
	if hasMetricLineWith(text, "drive9_tenant_request_duration_seconds_count", `surface="zero_surface"`) {
		t.Errorf("zero-duration tenant request unexpectedly recorded a duration:\n%s", text)
	}
}

func TestRecordSSEConnectionZeroDurationDoesNotRecordDuration(t *testing.T) {
	const tenantID = "tenant-zero-sse-connection"

	RecordSSEConnectionWithOrg(tenantID, "org-sse-zero", "closed", 0)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_sse_connections_total{reason="closed",tenant_id="`+tenantID+`",tidbcloud_org_id="org-sse-zero"} 1`) {
		t.Fatalf("missing zero-duration SSE connection total:\n%s", text)
	}
	if hasMetricLineWith(text, "drive9_sse_connection_duration_seconds_count", `tenant_id="`+tenantID+`"`) {
		t.Fatalf("zero-duration SSE connection unexpectedly recorded a duration:\n%s", text)
	}
}

func TestRecordTenantDurationMetricsSkipZeroDuration(t *testing.T) {
	const (
		phaseTenant    = "tenant-zero-sse-phase1"
		eventBusTenant = "tenant-zero-event-bus"
	)

	RecordSSEPhase1(phaseTenant, 0)
	RecordEventBusQuery(eventBusTenant, "events_since", "ok", 0)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if hasMetricLineWith(text, "drive9_sse_phase1_duration_seconds_count", `tenant_id="`+phaseTenant+`"`) {
		t.Fatalf("zero-duration SSE phase1 unexpectedly recorded a duration:\n%s", text)
	}
	if hasMetricLineWith(text, "drive9_event_bus_query_duration_seconds_count", `operation="events_since"`, `result="ok"`) {
		t.Fatalf("zero-duration event bus query unexpectedly recorded a duration:\n%s", text)
	}
}

func TestRecordEventBusQueryDurationOmitsTenant(t *testing.T) {
	const tenantID = "tenant-event-bus-duration-omit"

	RecordEventBusQuery(tenantID, "event_bus_duration_omit", "ok", time.Second)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_event_bus_query_duration_seconds_count{operation="event_bus_duration_omit",result="ok"} 1`) {
		t.Errorf("missing tenant-omitted event bus duration:\n%s", text)
	}
	if strings.Contains(text, `drive9_event_bus_query_duration_seconds_count{operation="event_bus_duration_omit",result="ok",tenant_id="`+tenantID+`"}`) {
		t.Errorf("event bus duration unexpectedly carried tenant_id:\n%s", text)
	}
}

func TestRecordTenantInFlightDeletesZeroValue(t *testing.T) {
	const (
		tenantID = "tenant-inflight-delete"
		surface  = "inflight_delete_surface"
		action   = "inflight_delete_action"
	)

	RecordTenantInFlightWithOrg(tenantID, "org-inflight-delete", surface, action, 1)
	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_tenant_inflight_requests{action="`+action+`",surface="`+surface+`",tenant_id="`+tenantID+`",tidbcloud_org_id="org-inflight-delete"} 1.000000`) {
		t.Errorf("missing tenant in-flight gauge:\n%s", text)
	}

	RecordTenantInFlightWithOrg(tenantID, "org-inflight-delete", surface, action, 0)
	rec = httptest.NewRecorder()
	WritePrometheus(rec)
	text = rec.Body.String()
	if strings.Contains(text, `drive9_tenant_inflight_requests{action="`+action+`",surface="`+surface+`",tenant_id="`+tenantID+`",tidbcloud_org_id="org-inflight-delete"}`) {
		t.Errorf("tenant in-flight gauge should be deleted at zero:\n%s", text)
	}
}

func TestTenantIDMetricsIncludeTiDBCloudOrgID(t *testing.T) {
	const (
		tenantID = "tenant-org-label-test"
		orgID    = "org-label-test"
	)

	RecordTenantGaugeWithOrg(tenantID, orgID, "component_org_label_test", "gauge", 1)
	RecordTenantEventWithOrg(tenantID, orgID, "event_org_label_test", "result", "ok")
	RecordTenantRequestCountWithOrg(tenantID, orgID, "surface_org_label_test", "action", "ok", 200)
	RecordTenantHTTPBytesWithOrg(tenantID, orgID, "surface_org_label_test", "ignored", "request", 10)
	RecordTenantFileBytesWithOrg(tenantID, orgID, "surface_org_label_test", "write", "out", 20)
	RecordTenantStorageBytesWithOrg(tenantID, orgID, "confirmed", 30)
	RecordTenantMediaFilesWithOrg(tenantID, orgID, "confirmed", 40)
	RecordTenantVideoFilesWithOrg(tenantID, orgID, "limit", 50)
	RecordSSEInFlightWithOrg(tenantID, orgID, 1)
	RecordSSEPhase1WithOrg(tenantID, orgID, time.Second)
	RecordSSEEventSentWithOrg(tenantID, orgID, "write")
	RecordSSEResetSentWithOrg(tenantID, orgID, "seq_too_old")
	RecordSSEHeartbeatSentWithOrg(tenantID, orgID)
	RecordEventBusPollFailureWithOrg(tenantID, orgID)
	RecordEventBusPublishErrorWithOrg(tenantID, orgID)
	RecordFSEventsRowsWithOrg(tenantID, orgID, 50)
	RecordFSEventsPrunedWithOrg(tenantID, orgID, 60)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	for _, want := range []string{
		`drive9_service_gauge{component="component_org_label_test",name="gauge",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1.000000`,
		`drive9_business_events_total{event="event_org_label_test",result="ok",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_tenant_requests_total{action="action",result="ok",status_class="2xx",surface="surface_org_label_test",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_tenant_http_bytes_total{direction="request",surface="surface_org_label_test",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 10`,
		`drive9_tenant_file_bytes_total{action="write",direction="out",surface="surface_org_label_test",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 20`,
		`drive9_tenant_storage_bytes{state="confirmed",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 30.000000`,
		`drive9_tenant_media_files{state="confirmed",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 40.000000`,
		`drive9_tenant_video_files{state="limit",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 50.000000`,
		`drive9_sse_inflight_connections{tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1.000000`,
		`drive9_sse_phase1_duration_seconds_count{tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_sse_events_sent_total{op="write",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_sse_resets_sent_total{reason="seq_too_old",tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_sse_heartbeats_sent_total{tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_event_bus_poll_failures_total{tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_event_bus_publish_errors_total{tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 1`,
		`drive9_fs_events_rows{tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 50.000000`,
		`drive9_fs_events_pruned_total{tenant_id="` + tenantID + `",tidbcloud_org_id="` + orgID + `"} 60`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing org-scoped tenant metric %q:\n%s", want, text)
		}
	}
}

func TestRecordTenantCountUsesStatus(t *testing.T) {
	RecordTenantCount("pending_test", 1)
	RecordTenantCount("provisioning_test", 2)
	RecordTenantCount("active_test", 3)
	RecordTenantCount("failed_test", 4)
	RecordTenantCount("suspended_test", 5)
	RecordTenantCount("deleting_test", 6)
	RecordTenantCount("deleted_test", 7)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	for _, want := range []string{
		`drive9_tenant_count{status="pending_test"} 1.000000`,
		`drive9_tenant_count{status="provisioning_test"} 2.000000`,
		`drive9_tenant_count{status="active_test"} 3.000000`,
		`drive9_tenant_count{status="failed_test"} 4.000000`,
		`drive9_tenant_count{status="suspended_test"} 5.000000`,
		`drive9_tenant_count{status="deleting_test"} 6.000000`,
		`drive9_tenant_count{status="deleted_test"} 7.000000`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing tenant count metric %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, `drive9_tenant_non_deleted_count`) || strings.Contains(text, `total_non_deleted`) {
		t.Fatalf("tenant count metrics must only use real status values:\n%s", text)
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

func TestRecordOperationIncludesLongAdminTenantPoolUpdateBuckets(t *testing.T) {
	RecordOperation("admin_tenant_pool", "update", "ok", 5*time.Minute)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_service_operation_duration_seconds_bucket{component="admin_tenant_pool",operation="update",result="ok",le="300"} 1`) {
		t.Fatalf("missing 300s service operation duration bucket:\n%s", text)
	}
	if !strings.Contains(text, `drive9_service_operation_duration_seconds_bucket{component="admin_tenant_pool",operation="update",result="ok",le="600"} 1`) {
		t.Fatalf("missing 600s service operation duration bucket:\n%s", text)
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

func TestRecordTenantPoolBindingsIncludesPoolTiDBCloudOrgAndStatus(t *testing.T) {
	const poolID = "pool-binding-metrics-test"
	const orgID = "org-binding-metrics-test"

	RecordTenantPoolBindings(poolID, orgID, "free", 3)
	RecordTenantPoolBindings(poolID, orgID, "used", 2)
	RecordTenantPoolBindings(poolID+"-guest", "", "free", 1)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_tenant_pool_bindings{pool_id="`+poolID+`",status="free",tidbcloud_org_id="`+orgID+`"} 3`) {
		t.Fatalf("missing tenant pool free binding gauge:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_bindings{pool_id="`+poolID+`",status="used",tidbcloud_org_id="`+orgID+`"} 2`) {
		t.Fatalf("missing tenant pool used binding gauge:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_bindings{pool_id="`+poolID+`-guest",status="free",tidbcloud_org_id="guest"} 1`) {
		t.Fatalf("missing tenant pool guest org binding gauge:\n%s", text)
	}
	if strings.Contains(text, `pool_status=`) {
		t.Fatalf("tenant pool binding metrics must use status label, not pool_status:\n%s", text)
	}

	DeleteTenantPoolBindings(poolID, orgID, "free")

	rec = httptest.NewRecorder()
	WritePrometheus(rec)
	text = rec.Body.String()
	if strings.Contains(text, `drive9_tenant_pool_bindings{pool_id="`+poolID+`",status="free",tidbcloud_org_id="`+orgID+`"}`) {
		t.Fatalf("tenant pool free binding gauge was not deleted:\n%s", text)
	}
	if !strings.Contains(text, `drive9_tenant_pool_bindings{pool_id="`+poolID+`",status="used",tidbcloud_org_id="`+orgID+`"} 2`) {
		t.Fatalf("tenant pool used binding gauge should remain after deleting free gauge:\n%s", text)
	}
}

func TestTiDBCloudQuotaMetricsOmitTenantID(t *testing.T) {
	RecordTiDBCloudRBACCacheRequest("quota_get", "cluster", "hit")
	RecordTiDBCloudOpenAPIRequest("admin_tenant_get", "list_managed_clusters", "ok")
	RecordTiDBCloudSpendingLimitSync("quota_get", "updated")
	RecordTiDBCloudSpendingLimitMissing("admin_tenant_list")

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	for _, want := range []string{
		`drive9_tidbcloud_rbac_cache_requests_total{path="quota_get",result="hit",scope="cluster"} 1`,
		`drive9_tidbcloud_openapi_requests_total{operation="list_managed_clusters",path="admin_tenant_get",result="ok"} 1`,
		`drive9_tidbcloud_spending_limit_sync_total{result="updated",source="quota_get"} 1`,
		`drive9_tidbcloud_spending_limit_missing_total{path="admin_tenant_list"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing TiDB Cloud quota metric %q:\n%s", want, text)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "drive9_tidbcloud_") && strings.Contains(line, `tenant_id=`) {
			t.Fatalf("TiDB Cloud quota metrics must not carry tenant_id:\n%s", text)
		}
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

func TestDeleteTenantCounters(t *testing.T) {
	const tenantID = "tenant-delete-counter-test"
	const tidbCloudOrgID = "org-delete-counter"

	RecordTenantOperationCountWithOrg(tenantID, tidbCloudOrgID, "cmp", "op", "ok")
	RecordTenantRequestCountWithOrg(tenantID, tidbCloudOrgID, "api", "read", "ok", 200)
	RecordTenantHTTPBytesWithOrg(tenantID, tidbCloudOrgID, "api", "", "upload", 1024)
	RecordSSEConnectionWithOrg(tenantID, tidbCloudOrgID, "mount", time.Second)
	RecordSSEEventSentWithOrg(tenantID, tidbCloudOrgID, "write")
	RecordTenantInFlightWithOrg(tenantID, tidbCloudOrgID, "api", "read", 3)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `tenant_id="`+tenantID+`"`) {
		t.Fatalf("tenant_id not found before delete:\n%s", text)
	}

	DeleteTenantCounters(tenantID)

	rec2 := httptest.NewRecorder()
	WritePrometheus(rec2)
	text2 := rec2.Body.String()

	counterNames := []string{
		"drive9_service_operations_total",
		"drive9_tenant_requests_total",
		"drive9_tenant_http_bytes_total",
		"drive9_sse_connections_total",
		"drive9_sse_events_sent_total",
	}
	histogramNames := []string{
		"drive9_sse_connection_duration_seconds_bucket",
		"drive9_sse_connection_duration_seconds_count",
		"drive9_sse_connection_duration_seconds_sum",
	}
	gaugeNames := []string{
		"drive9_tenant_inflight_requests",
	}
	var failures int
	for _, line := range strings.Split(text2, "\n") {
		if !strings.Contains(line, `tenant_id="`+tenantID+`"`) {
			continue
		}
		for _, name := range counterNames {
			if strings.HasPrefix(line, name) {
				t.Errorf("counter line with tenant_id still present: %s", line)
				failures++
			}
		}
		for _, name := range histogramNames {
			if strings.HasPrefix(line, name) {
				t.Errorf("histogram line with tenant_id still present: %s", line)
				failures++
			}
		}
		for _, name := range gaugeNames {
			if strings.HasPrefix(line, name) {
				t.Errorf("gauge line with tenant_id still present: %s", line)
				failures++
			}
		}
	}
	if failures > 0 {
		t.Fatalf("%d tenant-scoped lines leaked after DeleteTenantCounters", failures)
	}
}

func TestDeleteTenantCountersEmptyIsNoop(t *testing.T) {
	DeleteTenantCounters("")
}
