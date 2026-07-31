package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

func TestRequestRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want string
	}{
		{path: "/healthz", want: "/healthz"},
		{path: "/metrics", want: "/metrics"},
		{path: "/v1/provision", want: "/v1/provision"},
		{path: "/v1/status", want: "/v1/status"},
		{path: "/v1/tokens", want: "/v1/tokens/*"},
		{path: "/v1/tokens/key1", want: "/v1/tokens/*"},
		{path: "/v1/sql", want: "/v1/sql"},
		{path: "/v1/events", want: "/v1/events"},
		{path: "/v1/fs/doc.txt", want: "/v1/fs/*"},
		{path: "/v1/fs:batch-stat", want: "/v1/fs:batch-stat"},
		{path: "/v1/fs:batch-read-small", want: "/v1/fs:batch-read-small"},
		{path: "/v1/fs:batch-write", want: "/v1/fs:batch-write"},
		{path: "/v1/uploads", want: "/v1/uploads"},
		{path: "/v1/uploads/u1/complete", want: "/v1/uploads/*"},
		{path: "/v2/uploads/u1/parts", want: "/v2/uploads/*"},
		{path: "/v1/vault/secrets", want: "/v1/vault/secrets/*"},
		{path: "/v1/vault/secrets/db-prod", want: "/v1/vault/secrets/*"},
		{path: "/v1/vault/tokens", want: "/v1/vault/tokens/*"},
		{path: "/v1/vault/grants/g1", want: "/v1/vault/grants/*"},
		{path: "/v1/vault/audit", want: "/v1/vault/audit"},
		{path: "/v1/vault/read", want: "/v1/vault/read/*"},
		{path: "/v1/vault/read/secret/path", want: "/v1/vault/read/*"},
		{path: "/s3/tenant-a/object", want: "/s3/*"},
		{path: "/unknown", want: "other"},
	}

	for _, tc := range tests {
		if got := requestRoute(tc.path); got != tc.want {
			t.Fatalf("requestRoute(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestClassifyTenantRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method string
		path   string
		want   tenantRequestClass
	}{
		{method: http.MethodGet, path: "/v1/fs/doc.txt", want: tenantRequestClass{surface: "fs", action: "read"}},
		{method: http.MethodGet, path: "/v1/fs/dir/?list=1", want: tenantRequestClass{surface: "fs", action: "list"}},
		{method: http.MethodGet, path: "/v1/fs:batch-stat", want: tenantRequestClass{surface: "fs", action: "batch_stat"}},
		{method: http.MethodPost, path: "/v1/fs:batch-read-small", want: tenantRequestClass{surface: "fs", action: "batch_read_small"}},
		{method: http.MethodPost, path: "/v1/fs:batch-write", want: tenantRequestClass{surface: "fs", action: "batch_write"}},
		{method: http.MethodPost, path: "/v1/fs/large.bin?append=1", want: tenantRequestClass{surface: "fs", action: "append"}},
		{method: http.MethodPost, path: "/v1/uploads", want: tenantRequestClass{surface: "upload", action: "initiate"}},
		{method: http.MethodGet, path: "/v1/uploads", want: tenantRequestClass{surface: "upload", action: "list"}},
		{method: http.MethodPost, path: "/v1/uploads/u1/complete", want: tenantRequestClass{surface: "upload", action: "complete"}},
		{method: http.MethodDelete, path: "/v1/uploads/u1", want: tenantRequestClass{surface: "upload", action: "abort"}},
		{method: http.MethodPost, path: "/v1/uploads/u1/random-user-input", want: tenantRequestClass{surface: "upload", action: "other"}},
		{method: http.MethodPost, path: "/v2/uploads/initiate", want: tenantRequestClass{surface: "upload", action: "initiate"}},
		{method: http.MethodPost, path: "/v2/uploads/u1/presign-batch", want: tenantRequestClass{surface: "upload", action: "presign_batch"}},
		{method: http.MethodPost, path: "/v2/uploads/u1/random-user-input", want: tenantRequestClass{surface: "upload", action: "other"}},
		{method: http.MethodPost, path: "/v1/tokens", want: tenantRequestClass{surface: "tokens", action: "issue"}},
		{method: http.MethodPost, path: "/v1/tokens/revoke", want: tenantRequestClass{surface: "tokens", action: "revoke_by_key"}},
		{method: http.MethodDelete, path: "/v1/tokens/tok_123", want: tenantRequestClass{surface: "tokens", action: "revoke"}},
		{method: http.MethodGet, path: "/v1/vault/read/db/password", want: tenantRequestClass{surface: "vault", action: "read"}},
		{method: http.MethodPut, path: "/s3/local/upload/u1/1", want: tenantRequestClass{surface: "object_store", action: "upload_part"}},
		{method: http.MethodGet, path: "/s3/local/objects/blob", want: tenantRequestClass{surface: "object_store", action: "get_object"}},
		{method: http.MethodPost, path: "/v1/sql", want: tenantRequestClass{surface: "sql", action: "post"}},
	}

	for _, tc := range tests {
		req, err := http.NewRequest(tc.method, "http://example.test"+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		got := classifyTenantRequest(req)
		if got != tc.want {
			t.Fatalf("classifyTenantRequest(%s %s) = %#v, want %#v", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestRequestTenantID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want string
	}{
		{path: "/s3/local/upload/u1/1", want: "local"},
		{path: "/s3/upload/u1/1", want: "local"},
		{path: "/s3/objects/blob", want: "local"},
		{path: "/s3/tenant-a/upload/u1/1", want: ""},
		{path: "/s3/tenant-a/objects/blob", want: ""},
		{path: "/v1/fs/doc.txt", want: ""},
	}

	for _, tc := range tests {
		req, err := http.NewRequest(http.MethodGet, "http://example.test"+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := requestTenantID(req); got != tc.want {
			t.Fatalf("requestTenantID(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.test/s3/tenant-a/objects/blob", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := withRequestMetricState(req.Context(), &requestMetricState{})
	req = req.WithContext(ctx)
	setRequestMetricTenant(req.Context(), "tenant-a", "", "", "org-a", tenantRequestClass{surface: "object_store", action: "get_object"})
	if got := requestTenantID(req); got != "tenant-a" {
		t.Fatalf("requestTenantID with verified scope = %q, want tenant-a", got)
	}
}

func TestSetRequestMetricTenantMovesInFlightLabel(t *testing.T) {
	m := newServerMetrics()
	state := &requestMetricState{}
	ctx := withMetrics(context.Background(), m)
	ctx = withRequestMetricState(ctx, state)
	class := tenantRequestClass{surface: "object_store", action: "upload_part"}

	setRequestMetricTenant(ctx, "local", "", "", "", class)
	if got := m.tenantInFlight[tenantInFlightKey("local", defaultTenantMetricTiDBCloudOrgID, "object_store")]; got != 1 {
		t.Fatalf("local in-flight = %d, want 1", got)
	}

	setRequestMetricTenant(ctx, "tenant-a", "", "", "org-a", class)
	if got := m.tenantInFlight[tenantInFlightKey("local", defaultTenantMetricTiDBCloudOrgID, "object_store")]; got != 0 {
		t.Fatalf("local in-flight after move = %d, want 0", got)
	}
	if got := m.tenantInFlight[tenantInFlightKey("tenant-a", "org-a", "object_store")]; got != 1 {
		t.Fatalf("tenant-a in-flight = %d, want 1", got)
	}

	finishRequestMetricTenant(ctx)
	if got := len(m.tenantInFlight); got != 0 {
		t.Fatalf("tenant in-flight map size after finish = %d, want 0", got)
	}
}

func TestSetRequestMetricTenantForAuthStatusOnlyScopesActiveTenants(t *testing.T) {
	for _, status := range []meta.TenantStatus{meta.TenantDeleting, meta.TenantDeleted, meta.TenantSuspended} {
		t.Run(string(status), func(t *testing.T) {
			const tenantID = "tenant-auth-status-metrics"
			metrics.DeleteTenantCounters(tenantID)
			t.Cleanup(func() { metrics.DeleteTenantCounters(tenantID) })
			ctx := withRequestMetricState(context.Background(), &requestMetricState{})
			setRequestMetricTenantForAuthStatus(ctx, tenantID, "key", "db9", "org", status, tenantRequestClass{surface: "api", action: "read"})
			if tenantID, _, _, _ := requestMetricScope(ctx); tenantID != "" {
				t.Fatalf("tenant metric scope = %q for status %q, want empty", tenantID, status)
			}
			req := httptest.NewRequest(http.MethodGet, "http://example.test/v1/fs/stale", nil).WithContext(ctx)
			recordTenantHTTPRequest(req, http.StatusForbidden, 0, 0)
			recorder := httptest.NewRecorder()
			metrics.WritePrometheus(recorder)
			if strings.Contains(recorder.Body.String(), `tenant_id="`+tenantID+`"`) {
				t.Fatalf("rejected %s request recreated tenant metrics:\n%s", status, recorder.Body.String())
			}
		})
	}

	ctx := withRequestMetricState(context.Background(), &requestMetricState{})
	setRequestMetricTenantForAuthStatus(ctx, "tenant-active", "key", "db9", "org", meta.TenantActive, tenantRequestClass{surface: "api", action: "read"})
	if tenantID, _, _, _ := requestMetricScope(ctx); tenantID != "tenant-active" {
		t.Fatalf("tenant metric scope = %q, want tenant-active", tenantID)
	}
}

func TestRecordTenantHTTPRequestAtCompletionDoesNotRecreateCleanedCounter(t *testing.T) {
	const tenantID = "tenant-request-completion-cleanup"
	metrics.DeleteTenantCounters(tenantID)
	t.Cleanup(func() { metrics.DeleteTenantCounters(tenantID) })

	ctx := withRequestMetricState(context.Background(), &requestMetricState{})
	setRequestTenantHTTPRecorder(ctx, func() {
		metrics.RecordTenantRequestCountWithOrg(tenantID, "org-completion-cleanup", "fs", "read", http.StatusOK)
	})

	// The auth middleware runs this before it releases the final pool reference.
	recordTenantHTTPRequestAtCompletion(ctx)
	metrics.DeleteTenantRequestCounters(tenantID)

	// The outer observe fallback must not recreate a counter after closeEntry.
	recordTenantHTTPRequestAtCompletion(ctx)
	recorder := httptest.NewRecorder()
	metrics.WritePrometheus(recorder)
	if strings.Contains(recorder.Body.String(), `drive9_tenant_requests_total{`) &&
		strings.Contains(recorder.Body.String(), `tenant_id="`+tenantID+`"`) {
		t.Fatalf("tenant request counter recreated after cleanup:\n%s", recorder.Body.String())
	}
}

func TestAdjustTenantInFlightAggregatesActionsForSameSurface(t *testing.T) {
	m := newServerMetrics()
	if got := m.adjustTenantInFlight("tenant-inflight-actions", "org-inflight-actions", "fs", 1); got != 1 {
		t.Fatalf("read increment = %d, want 1", got)
	}
	if got := m.adjustTenantInFlight("tenant-inflight-actions", "org-inflight-actions", "fs", 1); got != 2 {
		t.Fatalf("write increment = %d, want aggregate 2", got)
	}
	if got := m.adjustTenantInFlight("tenant-inflight-actions", "org-inflight-actions", "fs", -1); got != 1 {
		t.Fatalf("read decrement = %d, want aggregate 1", got)
	}
	if got := m.adjustTenantInFlight("tenant-inflight-actions", "org-inflight-actions", "fs", -1); got != 0 {
		t.Fatalf("write decrement = %d, want 0", got)
	}
}

func TestEventFieldsDoesNotDuplicateExplicitTiDBCloudOrgID(t *testing.T) {
	ctx := withRequestMetricState(context.Background(), &requestMetricState{})
	setRequestMetricTenant(ctx, "tenant-a", "", "", "org-request", tenantRequestClass{surface: "status", action: "get"})

	fields := eventFields(ctx, "test_event", "tidbcloud_org_id", "org-explicit")
	count := 0
	for _, field := range fields {
		if field.Key != "tidbcloud_org_id" {
			continue
		}
		count++
		if field.String != "org-explicit" {
			t.Fatalf("tidbcloud_org_id = %q, want org-explicit", field.String)
		}
	}
	if count != 1 {
		t.Fatalf("tidbcloud_org_id field count = %d, want 1", count)
	}
}

func TestRequestTenantMetricScopeFallbackLeavesOrgEmpty(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.test/s3/objects/blob", nil)
	if err != nil {
		t.Fatal(err)
	}
	req = req.WithContext(withRequestMetricState(req.Context(), &requestMetricState{}))

	tenantID, tidbCloudOrgID := requestTenantMetricScope(req)
	if tenantID != "local" {
		t.Fatalf("tenant id = %q, want local", tenantID)
	}
	if tidbCloudOrgID != "" {
		t.Fatalf("tidb cloud org id = %q, want empty", tidbCloudOrgID)
	}
}

type flushRecorder struct {
	header  http.Header
	flushed bool
}

type plainRecorder struct {
	header http.Header
}

func (r *flushRecorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func (r *flushRecorder) Write(p []byte) (int, error) { return len(p), nil }

func (r *flushRecorder) WriteHeader(_ int) {}

func (r *flushRecorder) Flush() { r.flushed = true }

func (r *plainRecorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func (r *plainRecorder) Write(p []byte) (int, error) { return len(p), nil }

func (r *plainRecorder) WriteHeader(_ int) {}

func TestObservedResponseWriterDoesNotAdvertiseFlush(t *testing.T) {
	ow := &observedResponseWriter{ResponseWriter: &plainRecorder{}}

	if _, ok := interface{}(ow).(http.Flusher); ok {
		t.Fatal("observedResponseWriter should not implement http.Flusher")
	}
}

func TestFlusherResponseWriterDelegatesFlush(t *testing.T) {
	rec := &flushRecorder{}
	fw := &flusherResponseWriter{
		observedResponseWriter: &observedResponseWriter{ResponseWriter: rec},
		flusher:                rec,
	}

	if _, ok := interface{}(fw).(http.Flusher); !ok {
		t.Fatal("flusherResponseWriter should implement http.Flusher")
	}

	fw.Flush()
	if !rec.flushed {
		t.Fatal("expected Flush to delegate to wrapped writer")
	}
}

// TestSSEEstablishmentDuration verifies that markSSEStreamEstablished records
// the establishment timestamp so observe can measure the time-to-first-byte
// (request → 200 headers + flush) rather than the full SSE connection
// lifetime. This is the core invariant that keeps /v1/events in the HTTP p99
// latency alert on a bounded, meaningful basis without the select-loop
// hold-open time polluting the histogram.
func TestSSEEstablishmentDuration(t *testing.T) {
	state := &requestMetricState{}
	ctx := withRequestMetricState(context.Background(), state)

	// Before establishment: flag is false and duration is unavailable.
	if sseStreamEstablished(ctx) {
		t.Fatal("sseStreamEstablished should be false before markSSEStreamEstablished")
	}
	start := time.Now()
	if _, ok := sseEstablishmentDuration(ctx, start); ok {
		t.Fatal("sseEstablishmentDuration should return false before establishment")
	}

	// Simulate the establishment point (handleEvents writing 200 headers).
	time.Sleep(2 * time.Millisecond)
	markSSEStreamEstablished(ctx)

	if !sseStreamEstablished(ctx) {
		t.Fatal("sseStreamEstablished should be true after markSSEStreamEstablished")
	}

	estDur, ok := sseEstablishmentDuration(ctx, start)
	if !ok {
		t.Fatal("sseEstablishmentDuration should return true after establishment")
	}
	if estDur < time.Millisecond {
		t.Fatalf("establishment duration = %v, want >= 1ms", estDur)
	}

	// The establishment duration must be bounded and significantly shorter
	// than a simulated connection lifetime. observe uses estDur (not
	// time.Since(start)) for the HTTP duration histogram when the stream is
	// established.
	connLifetime := time.Since(start)
	if estDur > connLifetime {
		t.Fatalf("establishment duration %v should not exceed connection lifetime %v", estDur, connLifetime)
	}
}
