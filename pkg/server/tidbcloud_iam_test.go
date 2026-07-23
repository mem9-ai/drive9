package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestTiDBCloudOrganizationMatchesRequiresExplicitOrganization(t *testing.T) {
	if !tiDBCloudOrganizationMatches("org-1", "org-1") {
		t.Fatal("matching organizations should be authorized")
	}
	for _, resourceOrganizationID := range []string{"", "*", "org-2"} {
		if tiDBCloudOrganizationMatches("org-1", resourceOrganizationID) {
			t.Fatalf("resource organization %q should not be authorized", resourceOrganizationID)
		}
	}
}

func TestAuthorizeNativeTenantCredentialsPreservesMetaBackendError(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	tenantRow, err := rt.meta.GetTenant(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = rt.server.authorizeNativeTenantCredentials(ctx, tenantRow,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, "test")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled backend error", err)
	}
}

func TestAuthorizeSharedQuotaCredentialsPreservesMetaBackendError(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNativeShared)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rt.server.authorizeSharedQuotaCredentials(ctx, &meta.Tenant{ID: rt.tenantID},
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, "test")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled backend error", err)
	}
}

func TestAuthorizedAdminSharedTenantMapsPhysicalLookupBackendErrorToServerError(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpdateTenantProvider(ctx, rt.tenantID, tenant.ProviderTiDBCloudNativeShared); err != nil {
		t.Fatal(err)
	}
	fsID, err := rt.meta.EnsureFsID(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-1", Host: "shared.example.com", Port: 4000,
		User: "root", PasswordCipher: []byte("cipher"), Name: "shared_db", MaxTenants: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatal(err)
	}

	const unavailableTable = "db_pool_admin_lookup_error"
	if _, err := rt.meta.DB().ExecContext(ctx, "RENAME TABLE db_pool TO "+unavailableTable); err != nil {
		t.Fatal(err)
	}
	renamed := true
	t.Cleanup(func() {
		if renamed {
			_, _ = rt.meta.DB().ExecContext(context.Background(), "RENAME TABLE "+unavailableTable+" TO db_pool")
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/tenants/"+rt.tenantID, nil)
	recorder := httptest.NewRecorder()
	_, _, ok := rt.server.authorizedAdminTenant(recorder, req, rt.tenantID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, true, false)

	if _, err := rt.meta.DB().ExecContext(ctx, "RENAME TABLE "+unavailableTable+" TO db_pool"); err != nil {
		t.Fatal(err)
	}
	renamed = false
	if ok {
		t.Fatal("authorization unexpectedly succeeded while shared DB lookup failed")
	}
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
}
