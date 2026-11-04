package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

// forkCleanupProvisioner records branch deletes while keeping the cluster
// deprovision counters of the embedded fake.
type forkCleanupProvisioner struct {
	*quotaTestProvisioner
	mu       sync.Mutex
	branches []string
}

func (f *forkCleanupProvisioner) DeleteBranch(_ context.Context, clusterID, branchID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.branches = append(f.branches, clusterID+"/"+branchID)
	return nil
}

func (f *forkCleanupProvisioner) DeleteBranchWithCredentials(ctx context.Context, clusterID, branchID string, _ tenant.CredentialProvisionRequest) error {
	return f.DeleteBranch(ctx, clusterID, branchID)
}

// The remaining methods only exist so the fake satisfies
// tenant.CredentialBranchProvisioner; cleanup never creates a branch.
func (f *forkCleanupProvisioner) CreateBranchWithCredentials(context.Context, string, *tenant.ClusterInfo, tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	return nil, errors.New("not implemented")
}

func (f *forkCleanupProvisioner) WaitForBranchActiveWithCredentials(context.Context, *tenant.ClusterInfo, tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	return nil, errors.New("not implemented")
}

func (f *forkCleanupProvisioner) WaitForBranchUserWithCredentials(context.Context, string, string, tenant.CredentialProvisionRequest) (string, error) {
	return "", errors.New("not implemented")
}

func (f *forkCleanupProvisioner) deletedBranches() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.branches...)
}

// A failed fork stores the SOURCE tenant's cluster_id plus its own branch_id.
// Cleaning it up through the cluster path would deprovision the live parent and
// every sibling fork, so only the branch may be removed.
func TestCleanupFailedForkTenantDeletesBranchNotParentCluster(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	old := time.Now().UTC().Add(-time.Hour)

	forkID := token.NewID()
	if err := rt.meta.InsertTenant(context.Background(), &meta.Tenant{
		ID:                 forkID,
		Status:             meta.TenantFailed,
		Kind:               meta.TenantKindFork,
		Provider:           tenant.ProviderTiDBCloudNative,
		ClusterID:          "cluster-parent",
		BranchID:           "branch-fork-1",
		StorageNamespaceID: "ns-parent",
		DBPasswordCipher:   []byte{},
		SchemaVersion:      1,
		CreatedAt:          old,
		UpdatedAt:          old,
	}); err != nil {
		t.Fatalf("insert fork tenant: %v", err)
	}
	upsertFailedCleanupNativeBinding(t, rt, forkID, "cluster-parent", "", meta.TenantPoolBindingUsed)

	fp := &forkCleanupProvisioner{quotaTestProvisioner: rt.prov}
	rt.server.provisioner = fp

	rt.server.cleanupFailedOrganizationTenants(context.Background(), failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	if got := fp.deletedBranches(); len(got) != 1 || got[0] != "cluster-parent/branch-fork-1" {
		t.Fatalf("deleted branches = %v, want [cluster-parent/branch-fork-1]", got)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("cluster deprovision calls = %d, want 0 (a fork must never deprovision its parent cluster)", got)
	}
	got, err := rt.meta.GetTenant(context.Background(), forkID)
	if err != nil {
		t.Fatalf("get fork tenant: %v", err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("fork status = %s, want %s", got.Status, meta.TenantDeleted)
	}
}

// Defense in depth: even a direct caller cannot turn a fork row into a cluster
// deprovision.
func TestDeprovisionTenantClusterRejectsForkRow(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	err := rt.server.deprovisionTenantCluster(context.Background(), &meta.Tenant{
		ID:        "fork-1",
		Kind:      meta.TenantKindFork,
		Provider:  tenant.ProviderTiDBCloudNative,
		ClusterID: "cluster-parent",
		BranchID:  "branch-fork-1",
	}, tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})
	if err == nil {
		t.Fatal("deprovisionTenantCluster accepted a fork tenant")
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("cluster deprovision calls = %d, want 0", got)
	}
}
