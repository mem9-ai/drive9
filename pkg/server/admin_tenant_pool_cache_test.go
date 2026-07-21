package server

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

// TestFirstManagedOrganizationUsesRBACCache proves org resolution for
// provisioning reuses the RBAC cluster-list cache: repeated lookups for the
// same credential cost one TiDB Cloud list call, and forgetting the cache
// forces a fresh fetch.
func TestFirstManagedOrganizationUsesRBACCache(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{
		{Clusters: []tenant.CloudClusterInfo{{ClusterID: "c1", OrganizationID: "org-cached-1"}}},
		{Clusters: []tenant.CloudClusterInfo{{ClusterID: "c2", OrganizationID: "org-cached-2"}}},
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
	// The second lookup for the same credential must hit the cache: no
	// additional list call, and the org from the cached page is returned
	// (not the second scripted page).
	org, err = rt.server.firstManagedOrganization(ctx, cred)
	if err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	if org != "org-cached-1" {
		t.Fatalf("cached org = %q, want org-cached-1", org)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}

	// Forgetting the list forces a fresh API fetch (second scripted page).
	rt.server.forgetTiDBCloudRBACList(cred)
	org, err = rt.server.firstManagedOrganization(ctx, cred)
	if err != nil {
		t.Fatalf("lookup after forget: %v", err)
	}
	if org != "org-cached-2" {
		t.Fatalf("org after forget = %q, want org-cached-2", org)
	}
	if got := rt.prov.listCalls.Load(); got != 2 {
		t.Fatalf("list calls after forget = %d, want 2", got)
	}
}

func TestListAllManagedClustersUsesRBACCache(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{ClusterID: "c1", OrganizationID: "org-1"}},
	}}
	ctx := context.Background()
	cred := tenant.CredentialProvisionRequest{PublicKey: "pk", PrivateKey: "sk"}

	clusters, err := rt.server.listAllManagedClusters(ctx, cred, "", "test_managed_cluster_list")
	if err != nil {
		t.Fatalf("first list: %v", err)
	}
	if len(clusters) != 1 || clusters[0].ClusterID != "c1" || clusters[0].OrganizationID != "org-1" {
		t.Fatalf("first clusters = %#v", clusters)
	}

	clusters, err = rt.server.listAllManagedClusters(ctx, cred, "", "test_managed_cluster_list")
	if err != nil {
		t.Fatalf("cached list: %v", err)
	}
	if len(clusters) != 1 || clusters[0].ClusterID != "c1" || clusters[0].OrganizationID != "org-1" {
		t.Fatalf("cached clusters = %#v", clusters)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
}
