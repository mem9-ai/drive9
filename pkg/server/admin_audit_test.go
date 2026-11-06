package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The action table is the audit trail's coverage list: a mutating control-plane
// route that is missing here is silently unaudited, so pin the destructive ones.
func TestAdminAuditActionCoversDestructiveRoutes(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodDelete, "/v1/admin/tenants/t1", "tenant.delete"},
		{http.MethodDelete, "/v1/admin/tenants/t1/", "tenant.delete"},
		{http.MethodPost, "/v1/admin/tenants/t1/quota", "tenant.quota.set"},
		{http.MethodPut, "/v1/admin/tenants/t1/object-namespace", "tenant.object_namespace.update"},
		{http.MethodDelete, "/v1/admin/tenants/t1/object-namespace", "tenant.object_namespace.delete"},
		{http.MethodPost, "/v1/admin/tenants", "tenant.create"},
		{http.MethodPost, "/v1/provision", "tenant.create"},
		{http.MethodPost, "/v1/quota", "quota.set"},
		{http.MethodDelete, "/v1/tenant", "tenant.delete_self"},
		{http.MethodDelete, "/v1/fork", "fork.delete"},
		{http.MethodPost, "/v1/admin/tenant-pool", "tenant_pool.create"},
		{http.MethodDelete, "/v1/admin/tenant-pool", "tenant_pool.delete"},
		{http.MethodDelete, "/v1/admin/object-backends/ob1", "object_backend.delete"},

		// Reads are not audited.
		{http.MethodGet, "/v1/admin/tenants", ""},
		{http.MethodGet, "/v1/admin/tenants/t1", ""},
		{http.MethodGet, "/healthz", ""},
	}
	for _, tt := range tests {
		if got := adminAuditAction(tt.method, tt.path); got != tt.want {
			t.Errorf("adminAuditAction(%s %s) = %q, want %q", tt.method, tt.path, got, tt.want)
		}
	}
}

// A refused mutation must still produce a record: the attempt is what an audit
// trail is for.
func TestAdminAuditMiddlewareRecordsRefusedMutation(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	previous := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(previous) })

	s := &Server{}
	handler := s.adminAuditMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/tenant-1", nil)
	req.Header.Set(quotaPublicKeyHeader, "PUBLICKEY")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var entries []observer.LoggedEntry
	for _, entry := range recorded.All() {
		if entry.Message == "admin_audit" {
			entries = append(entries, entry)
		}
	}
	if len(entries) != 1 {
		t.Fatalf("admin_audit records = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["action"] != "tenant.delete" {
		t.Fatalf("action = %v, want tenant.delete", fields["action"])
	}
	if fields["result"] != "failed" {
		t.Fatalf("result = %v, want failed", fields["result"])
	}
	if fields["status"] != int64(http.StatusForbidden) {
		t.Fatalf("status = %v, want %d", fields["status"], http.StatusForbidden)
	}
	if fields["actor_kind"] != "tidbcloud_api_key" || fields["actor_public_key"] != "PUBLICKEY" {
		t.Fatalf("actor = %v/%v, want the presented public key", fields["actor_kind"], fields["actor_public_key"])
	}
}

// Reads are not audited.
func TestAdminAuditMiddlewareIgnoresReads(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	previous := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(previous) })

	s := &Server{}
	handler := s.adminAuditMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/admin/tenants", nil))

	for _, entry := range recorded.All() {
		if entry.Message == "admin_audit" {
			t.Fatal("a read produced an audit record")
		}
	}
}
