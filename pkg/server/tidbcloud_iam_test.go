package server

import (
	"context"
	"errors"
	"testing"

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
