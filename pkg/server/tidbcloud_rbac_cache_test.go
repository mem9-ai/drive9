package server

import (
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestTiDBCloudRBACCacheDoesNotRetainEmptyListAndSupportsInvalidation(t *testing.T) {
	cache := newTiDBCloudRBACCache(time.Hour)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}

	cache.rememberClusterList(cred, nil)
	if clusters, ok := cache.getClusterList(cred); ok {
		t.Fatalf("empty list cache hit = %#v, want miss", clusters)
	}

	cache.rememberClusterList(cred, []tenant.CloudClusterInfo{{
		ClusterID:      "cluster-1",
		OrganizationID: "org-1",
	}})
	clusters, ok := cache.getClusterList(cred)
	if !ok {
		t.Fatal("list cache miss, want hit")
	}
	if len(clusters) != 1 || clusters[0].ClusterID != "cluster-1" || clusters[0].OrganizationID != "org-1" {
		t.Fatalf("clusters = %#v", clusters)
	}

	cache.forgetClusterList(cred)
	if clusters, ok := cache.getClusterList(cred); ok {
		t.Fatalf("list cache hit after invalidation = %#v, want miss", clusters)
	}
	if cluster, ok := cache.getCluster(cred, "cluster-1"); !ok || cluster.OrganizationID != "org-1" {
		t.Fatalf("cluster cache after list invalidation = %#v, %v; want cluster auth retained", cluster, ok)
	}
}
