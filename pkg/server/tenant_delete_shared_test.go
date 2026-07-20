package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

// TestSharedTenantDeleteRetriesAfterPlacementRemoval covers the shared-delete
// retry window: an earlier attempt purged the tenant's rows and removed the
// placement but failed before enqueueing cleanup. The retried delete must not
// fall back to the standalone/provider path (no cluster deprovision) — the
// persisted provider routes it back to the shared path, which skips the
// finished purge and completes the delete.
func TestSharedTenantDeleteRetriesAfterPlacementRemoval(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	if _, err := rt.meta.EnsureFsID(ctx, rt.tenantID); err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := rt.meta.UpdateTenantStatus(ctx, rt.tenantID, meta.TenantDeleting); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	// The standalone cluster-deprovision path must never run for a shared
	// tenant, even when its placement row is already gone.
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleted) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleted)
	}
}

// TestSharedTenantDeleteWithCleanupJobShortCircuits mirrors the standalone
// retry short-circuit: a deleting shared tenant with an existing cleanup job
// returns the deleting status immediately — without purging again and without
// touching the placement row.
func TestSharedTenantDeleteWithCleanupJobShortCircuits(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	fsID, err := rt.meta.EnsureFsID(ctx, rt.tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		OrgID:          "org-shared-delete",
		Host:           "shared.example.com",
		Port:           4000,
		User:           "root",
		PasswordCipher: []byte("cipher"),
		Name:           "shared_db",
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID:        fsID,
		DbID:        dbID,
		Placement:   meta.PlacementShared,
		SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}
	if err := rt.meta.UpdateTenantStatus(ctx, rt.tenantID, meta.TenantDeleting); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnqueueTenantDeleteJob(ctx, &meta.TenantDeleteJob{
		TenantID:    rt.tenantID,
		NamespaceID: rt.tenantID,
		Backend:     "local",
		Prefix:      rt.tenantID + "/",
	}); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	// The short-circuit must leave the placement row alone: no purge ran.
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("placement after short-circuit: %v", err)
	}
}

// TestSharedTenantDeleteWithoutFsIDFails proves the shared delete path never
// allocates identity: a shared tenant without an fs_registry row fails the
// delete instead of minting a phantom fs_id (ResolveFsID, not EnsureFsID).
func TestSharedTenantDeleteWithoutFsIDFails(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	// InsertTenant allocated an fs_id at provision time; simulate a tenant
	// whose registry row was reaped out of band — the delete must fail, not
	// mint a new one.
	if _, err := rt.meta.DB().ExecContext(ctx, "DELETE FROM fs_registry WHERE tenant_id = ?", rt.tenantID); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	if _, err := rt.meta.ResolveFsID(ctx, rt.tenantID); err == nil {
		t.Fatal("delete allocated an fs_id for the tenant being deleted")
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
}

// TestSharedTenantDeleteFullPurgePath runs the complete owner delete on a
// shared tenant: drain, purge, placement removal, and final delete — the
// provider-routed dispatch never calls the cluster deprovisioner.
func TestSharedTenantDeleteFullPurgePath(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	fsID, err := rt.meta.EnsureFsID(ctx, rt.tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		OrgID:          "org-shared-delete",
		Host:           "shared.example.com",
		Port:           4000,
		User:           "root",
		PasswordCipher: []byte("cipher"),
		Name:           "shared_db",
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID:        fsID,
		DbID:        dbID,
		Placement:   meta.PlacementShared,
		SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	// The purge needs a reachable shared DB, which "shared.example.com" is
	// not — the delete must fail there WITHOUT deprovisioning any cluster and
	// leave the tenant in deleting state for a later retry.
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d (unreachable shared db)", resp.StatusCode, http.StatusInternalServerError)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleting) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleting)
	}
	// The placement row must survive the failed purge so the retry can find
	// the shared DB again.
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("placement after failed purge: %v", err)
	}
}

// TestAdminTenantCreateRejectsSharedPoolOrg proves the admin tenant API fails
// closed when the caller's org matches a registered shared pool: no claim, no
// provision, no tenant row — instead of creating a shared tenant with no
// valid admin lifecycle.
func TestAdminTenantCreateRejectsSharedPoolOrg(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	rt.prov.listPages = []*tenant.ManagedClusterListResult{
		{Clusters: []tenant.CloudClusterInfo{{OrganizationID: "org-shared-1"}}},
	}
	if _, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		OrgID:          "org-shared-1",
		Host:           "shared.example.com",
		Port:           4000,
		User:           "root",
		PasswordCipher: []byte("cipher"),
		Name:           "shared_db",
	}); err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}

	body, err := json.Marshal(map[string]string{"public_key": "pk", "private_key": "sk"})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/admin/tenants", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	var tenantCount int
	if err := rt.meta.DB().QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	// Only the pre-existing quota runtime tenant may exist.
	if tenantCount != 1 {
		t.Fatalf("tenants = %d, want 1 (no admin-created tenant)", tenantCount)
	}
}

// TestAdminTenantGetRejectsSharedProvider documents the admin/shared
// ownership contract from the other side: a shared-schema tenant (provider
// tidb_cloud_native_shared) is rejected by admin authorization instead of
// being managed as a dedicated cluster.
func TestAdminTenantGetRejectsSharedProvider(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	// The server runs the cloud-native provisioner; this tenant is placed on
	// a shared pool, so its persisted provider is the shared one.
	if err := rt.meta.UpdateTenantProvider(context.Background(), rt.tenantID, tenant.ProviderTiDBCloudNativeShared); err != nil {
		t.Fatalf("UpdateTenantProvider: %v", err)
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-TiDBCloud-Public-Key", "pk")
	req.Header.Set("X-TiDBCloud-Private-Key", "sk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
}
