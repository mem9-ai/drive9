package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestAdminTenantCreateIdempotencyReplaysPoolClaim(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{Clusters: []tenant.CloudClusterInfo{{ClusterID: "cluster-free-idempotent", OrganizationID: "org-1"}}}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{PoolID: "pool-idempotent", OrganizationID: "org-1", Size: 1, Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	password, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatal(err)
	}
	const tenantID = "pool-tenant-idempotent"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{ID: tenantID, Status: meta.TenantActive, DBHost: "db.example.com", DBPort: 4000, DBUser: "u.root", DBPasswordCipher: password, DBName: "tidbcloud_fs", DBTLS: true, Provider: tenant.ProviderTiDBCloudNative, ClusterID: "cluster-free-idempotent", SchemaVersion: 1, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{TenantID: tenantID, OrganizationID: "org-1", ClusterID: "cluster-free-idempotent", PoolID: "pool-idempotent", PoolStatus: meta.TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(rt.server)
	defer ts.Close()
	first, firstStatus := createAdminTenantForIdempotencyTest(t, ts.URL, "manus-user:user-1")
	second, secondStatus := createAdminTenantForIdempotencyTest(t, ts.URL, "manus-user:user-1")
	if firstStatus != http.StatusAccepted || secondStatus != http.StatusOK {
		t.Fatalf("statuses = %d, %d; want 202, 200", firstStatus, secondStatus)
	}
	if first.TenantID != tenantID || second.TenantID != tenantID || first.APIKey == "" || first.APIKey != second.APIKey {
		t.Fatalf("idempotent responses differ: first=%+v second=%+v", first, second)
	}
	if got := rt.prov.markPoolUsedCalls.Load(); got != 1 {
		t.Fatalf("mark pool used calls = %d, want 1", got)
	}
	binding, err := rt.meta.GetExternalBinding(ctx, adminTenantCreateProvider, adminTenantIdempotencySubject("org-1", "manus-user:user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if binding.TenantID != tenantID {
		t.Fatalf("binding tenant = %q, want %q", binding.TenantID, tenantID)
	}
	_, credentialConflictStatus := createAdminTenantWithBodyForIdempotencyTest(
		t,
		ts.URL,
		"manus-user:user-1",
		`{"public_key":"public-2","private_key":"private-2"}`,
	)
	if credentialConflictStatus != http.StatusConflict {
		t.Fatalf("different credential replay status = %d, want 409", credentialConflictStatus)
	}
	_, conflictStatus := createAdminTenantWithBodyForIdempotencyTest(
		t,
		ts.URL,
		"manus-user:user-1",
		`{"public_key":"public-1","private_key":"private-1","max_file_count":10}`,
	)
	if conflictStatus != http.StatusConflict {
		t.Fatalf("changed replay status = %d, want 409", conflictStatus)
	}
}

func TestAdminTenantCreateIdempotencyFailsClosedWithoutPoolCapacity(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	_, status := createAdminTenantForIdempotencyTest(t, ts.URL, "manus-user:no-capacity")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	if _, err := rt.meta.GetExternalBinding(context.Background(), adminTenantCreateProvider, adminTenantIdempotencySubject("org-1", "manus-user:no-capacity")); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("external binding error = %v, want not found", err)
	}
}

func TestAdminTenantCreateIdempotencyFailsClosedWithMissingOwnerKey(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	now := time.Now().UTC()
	password, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatal(err)
	}
	const tenantID = "pool-tenant-recover-key"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID: tenantID, Status: meta.TenantActive, Provider: tenant.ProviderTiDBCloudNative,
		DBHost: "db.example.com", DBPort: 4000, DBUser: "u.root", DBPasswordCipher: password,
		DBName: "tidbcloud_fs", DBTLS: true,
		SchemaVersion: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	metadata, err := adminTenantCreateMetadata("public-1", "org-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	subjectKey := adminTenantIdempotencySubject("org-1", "manus-user:recover-key")
	if err := rt.meta.InsertExternalBinding(ctx, &meta.ExternalBinding{
		Provider: adminTenantCreateProvider, SubjectKey: subjectKey,
		TenantID: tenantID, MetadataJSON: metadata, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(rt.server)
	defer ts.Close()
	out, status := createAdminTenantForIdempotencyTest(t, ts.URL, "manus-user:recover-key")
	if status != http.StatusConflict || out.APIKey != "" {
		t.Fatalf("fail-closed response = %+v status=%d", out, status)
	}
	if _, err := rt.meta.GetActiveAPIKeyByIssuer(ctx, tenantID, adminTenantCreateProvider, subjectKey); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("owner key error = %v, want not found", err)
	}
}

func TestAdminTenantIdempotencySubjectScopesOrganization(t *testing.T) {
	key := "manus-user:user-1"
	first := adminTenantIdempotencySubject("org-1", key)
	second := adminTenantIdempotencySubject("org-2", key)
	if first == second {
		t.Fatalf("organization-scoped subjects collide: %q", first)
	}
}

func createAdminTenantForIdempotencyTest(t *testing.T, baseURL, key string) (adminTenantCreateResponse, int) {
	t.Helper()
	return createAdminTenantWithBodyForIdempotencyTest(t, baseURL, key, `{"public_key":"public-1","private_key":"private-1"}`)
}

func createAdminTenantWithBodyForIdempotencyTest(t *testing.T, baseURL, key, body string) (adminTenantCreateResponse, int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/admin/tenants", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out adminTenantCreateResponse
	if resp.StatusCode < http.StatusBadRequest {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	} else {
		body, _ := io.ReadAll(resp.Body)
		t.Logf("error response: %s", body)
	}
	return out, resp.StatusCode
}
