package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestFirstManagedOrganizationUsesIAMIdentityCache(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.iamIdentities = []*tenant.TiDBCloudAPIKeyIdentity{
		{OrganizationID: "org-cached-1", Role: tenant.TiDBCloudRoleOrgOwner},
		{OrganizationID: "org-cached-2", Role: tenant.TiDBCloudRoleProjectOwner},
	}
	ctx := context.Background()
	cred := tenant.CredentialProvisionRequest{PublicKey: "pk", PrivateKey: "sk"}

	org, err := rt.server.firstManagedOrganization(ctx, cred)
	if err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if org != "org-cached-1" {
		t.Fatalf("org = %q, want org-cached-1", org)
	}
	org, err = rt.server.firstManagedOrganization(ctx, cred)
	if err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	if org != "org-cached-1" {
		t.Fatalf("cached org = %q, want org-cached-1", org)
	}
	if got := rt.prov.iamCalls.Load(); got != 1 {
		t.Fatalf("IAM calls = %d, want 1", got)
	}

	secondCred := tenant.CredentialProvisionRequest{PublicKey: "pk-2", PrivateKey: "sk-2"}
	org, err = rt.server.firstManagedOrganization(ctx, secondCred)
	if err != nil {
		t.Fatalf("lookup with second credentials: %v", err)
	}
	if org != "org-cached-2" {
		t.Fatalf("org with second credentials = %q, want org-cached-2", org)
	}
	if got := rt.prov.iamCalls.Load(); got != 2 {
		t.Fatalf("IAM calls after second credentials = %d, want 2", got)
	}
}

func TestResolveTiDBCloudIdentityRejectsInsufficientRole(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.iamErr = tenant.ErrTiDBCloudRoleInsufficient
	_, err := rt.server.resolveTiDBCloudIdentity(context.Background(), tenant.CredentialProvisionRequest{
		PublicKey: "pk", PrivateKey: "sk",
	}, "test_role")
	if err == nil || !isTiDBCloudRoleInsufficient(err) {
		t.Fatalf("error = %v, want insufficient role", err)
	}
}

func TestAdminTenantAPIEnabledForSharedProvider(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNativeShared)
	if !rt.server.adminTenantAPIEnabled() {
		t.Fatal("admin tenant API must be enabled for tidb_cloud_native_shared")
	}
}

func TestAdminTenantHTTPRejectsInsufficientIAMRoleWithoutLeakingDetails(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.iamErr = fmt.Errorf("%w: org:viewer SENSITIVE_IAM_DETAIL", tenant.ErrTiDBCloudRoleInsufficient)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "VIEWERFIXTURE1")
	req.Header.Set(quotaPrivateKeyHeader, "fixture-private")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), tenant.ErrTiDBCloudRoleInsufficient.Error()) {
		t.Fatalf("body = %q, want stable insufficient-role error", body)
	}
	for _, sensitive := range []string{"SENSITIVE_IAM_DETAIL", "VIEWERFIXTURE1", "fixture-private"} {
		if strings.Contains(string(body), sensitive) {
			t.Fatalf("body leaked %q: %s", sensitive, body)
		}
	}
}
