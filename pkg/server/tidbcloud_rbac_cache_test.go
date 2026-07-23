package server

import (
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestTiDBCloudRBACCacheStoresOnlyCredentialIdentity(t *testing.T) {
	cache := newTiDBCloudRBACCache(time.Hour)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	want := tenant.TiDBCloudAPIKeyIdentity{OrganizationID: "org-1", Role: tenant.TiDBCloudRoleOrgOwner}

	if got, ok := cache.getIdentity(cred); ok {
		t.Fatalf("initial identity cache hit = %+v", got)
	}
	cache.rememberIdentity(cred, want)
	got, ok := cache.getIdentity(cred)
	if !ok || got != want {
		t.Fatalf("identity cache = %+v, ok=%v, want %+v", got, ok, want)
	}
}
