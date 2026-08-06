package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"github.com/mem9-ai/drive9/pkg/traceid"
)

const failedCleanupTestOrganizationID = "org-failed-cleanup"

func TestCleanupFailedOrganizationTenantsNativePoolFree(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "cluster-pool-free", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-pool-free", "pool-native", meta.TenantPoolBindingFree)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID, cred)

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantDeleted)
	if _, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, rt.tenantID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("native binding after cleanup error = %v, want %v", err, meta.ErrNotFound)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	cluster := rt.prov.lastDeprovisionSnapshot()
	if cluster == nil || cluster.ClusterID != "cluster-pool-free" {
		t.Fatalf("deprovision cluster = %#v", cluster)
	}
	if got := rt.prov.lastCredentialsSnapshot(); got != cred {
		t.Fatalf("deprovision credentials = %#v, want %#v", got, cred)
	}
	assertNoActiveFailedCleanupKeys(t, rt, rt.tenantID)
}

func TestCleanupFailedOrganizationTenantsNativeDirectBinding(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "cluster-direct", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-direct", "", meta.TenantPoolBindingUsed)

	rt.server.cleanupFailedOrganizationTenants(context.Background(), failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantDeleted)
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
}

func TestCleanupFailedOrganizationTenantsNativeUsesBindingClusterFallback(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-binding-fallback", "pool-native", meta.TenantPoolBindingFree)

	rt.server.cleanupFailedOrganizationTenants(context.Background(), failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantDeleted)
	cluster := rt.prov.lastDeprovisionSnapshot()
	if cluster == nil || cluster.ClusterID != "cluster-binding-fallback" {
		t.Fatalf("deprovision cluster = %#v, want binding cluster fallback", cluster)
	}
}

func TestCleanupFailedOrganizationTenantsNativeDeprovisionFailureRestoresCooldown(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "cluster-fails", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-fails", "pool-native", meta.TenantPoolBindingFree)
	rt.prov.deprovisionErr = errors.New("cloud unavailable")

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	failed := assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
	if !failed.UpdatedAt.After(old) {
		t.Fatalf("updated_at = %s, want after %s", failed.UpdatedAt, old)
	}
	if _, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, rt.tenantID); err != nil {
		t.Fatalf("native binding after failed deprovision: %v", err)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls after immediate rerun = %d, want cooldown to keep 1", got)
	}
}

func TestCleanupFailedOrganizationTenantsNativeRevokeFailureStillFinalizes(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "cluster-revoke-failure", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-revoke-failure", "pool-native", meta.TenantPoolBindingFree)
	cutoff := time.Now().UTC()
	candidates, err := rt.meta.ListFailedNativeTenantCleanupCandidates(
		ctx, failedCleanupTestOrganizationID, cutoff, 1)
	if err != nil {
		t.Fatalf("list native cleanup candidate: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("native cleanup candidates = %d, want 1", len(candidates))
	}
	revokeErr := errors.New("revoke metadata unavailable")
	revokeCalls := 0

	owned, cleanupErr := rt.server.cleanupFailedNativeTenantWithDependencies(
		ctx, failedCleanupTestOrganizationID, cutoff,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
		&candidates[0],
		func(context.Context, string) error {
			revokeCalls++
			return revokeErr
		},
		rt.meta.UpdateTenantStatusIf,
	)

	if !owned {
		t.Fatal("native cleanup owned = false, want true")
	}
	if cleanupErr != nil {
		t.Fatalf("native cleanup error = %v, want nil despite revoke failure", cleanupErr)
	}
	if revokeCalls != 1 {
		t.Fatalf("revoke calls = %d, want 1", revokeCalls)
	}
	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantDeleted)
	if _, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, rt.tenantID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("native binding after revoke failure error = %v, want %v", err, meta.ErrNotFound)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls after second pass = %d, want 1", got)
	}
}

func TestCleanupFailedOrganizationTenantsRestoreIgnoresCallerCancellationAndPreservesTrace(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpdateTenantStatus(ctx, rt.tenantID, meta.TenantDeleting); err != nil {
		t.Fatalf("mark tenant deleting: %v", err)
	}
	const wantTraceID = "failed-cleanup-restore-trace"
	canceledCtx, cancel := context.WithCancel(traceid.With(ctx, wantTraceID))
	cancel()
	var (
		updaterContextErr error
		gotTraceID        string
	)

	rt.server.restoreFailedTenantAfterCleanupWithUpdater(
		canceledCtx, rt.tenantID, tenant.ProviderTiDBCloudNative,
		failedCleanupTestOrganizationID, errors.New("cleanup failed"),
		func(updateCtx context.Context, tenantID string, from, to meta.TenantStatus) (bool, error) {
			updaterContextErr = updateCtx.Err()
			gotTraceID = traceid.FromContext(updateCtx)
			return rt.meta.UpdateTenantStatusIf(updateCtx, tenantID, from, to)
		},
	)

	if updaterContextErr != nil {
		t.Fatalf("restore updater context error = %v, want nil despite canceled caller", updaterContextErr)
	}
	if gotTraceID != wantTraceID {
		t.Fatalf("restore trace_id = %q, want %q", gotTraceID, wantTraceID)
	}
	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
}

func TestCleanupFailedOrganizationTenantsRestoreTimeoutPreservesCleanupError(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "cluster-restore-timeout", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-restore-timeout", "pool-native", meta.TenantPoolBindingFree)
	cutoff := time.Now().UTC()
	candidates, err := rt.meta.ListFailedNativeTenantCleanupCandidates(
		ctx, failedCleanupTestOrganizationID, cutoff, 1)
	if err != nil {
		t.Fatalf("list native cleanup candidate: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("native cleanup candidates = %d, want 1", len(candidates))
	}
	originalTimeout := tenantFailedCleanupRestoreTimeout
	tenantFailedCleanupRestoreTimeout = 25 * time.Millisecond
	t.Cleanup(func() { tenantFailedCleanupRestoreTimeout = originalTimeout })
	cleanupCause := errors.New("cloud deprovision failed")
	rt.prov.deprovisionErr = cleanupCause
	var updaterErr error
	started := time.Now()

	owned, cleanupErr := rt.server.cleanupFailedNativeTenantWithDependencies(
		ctx, failedCleanupTestOrganizationID, cutoff,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
		&candidates[0], rt.meta.RevokeTenantAPIKeys,
		func(updateCtx context.Context, _ string, _, _ meta.TenantStatus) (bool, error) {
			<-updateCtx.Done()
			updaterErr = updateCtx.Err()
			return false, updaterErr
		},
	)
	elapsed := time.Since(started)

	if !owned {
		t.Fatal("native cleanup owned = false, want true")
	}
	if !errors.Is(cleanupErr, cleanupCause) {
		t.Fatalf("cleanup error = %v, want original cause %v", cleanupErr, cleanupCause)
	}
	if !errors.Is(updaterErr, context.DeadlineExceeded) {
		t.Fatalf("restore updater error = %v, want %v", updaterErr, context.DeadlineExceeded)
	}
	if elapsed > time.Second {
		t.Fatalf("blocked restore returned after %s, want within 1s", elapsed)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
}

func TestCleanupFailedOrganizationTenantsSkipsClaimedNativeBinding(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "cluster-claimed", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-claimed", "pool-native", meta.TenantPoolBindingUsed)

	rt.server.cleanupFailedOrganizationTenants(context.Background(), failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
}

func setFailedCleanupTenant(t *testing.T, rt *quotaRuntime, tenantID, provider, clusterID, namespaceID string, updatedAt time.Time) {
	t.Helper()
	if _, err := rt.meta.DB().ExecContext(context.Background(), `UPDATE tenants
		SET status = ?, provider = ?, cluster_id = NULLIF(?, ''), storage_namespace_id = ?, updated_at = ?
		WHERE id = ?`, meta.TenantFailed, provider, clusterID, namespaceID, updatedAt.UTC(), tenantID); err != nil {
		t.Fatalf("configure failed cleanup tenant %s: %v", tenantID, err)
	}
}

func insertFailedCleanupTenant(t *testing.T, rt *quotaRuntime, provider, clusterID, namespaceID string, updatedAt time.Time) string {
	t.Helper()
	tenantID := token.NewID()
	if err := rt.meta.InsertTenant(context.Background(), &meta.Tenant{
		ID: tenantID, Status: meta.TenantFailed, Kind: meta.TenantKindLive,
		Provider: provider, ClusterID: clusterID, StorageNamespaceID: namespaceID,
		DBPasswordCipher: []byte{}, SchemaVersion: 1, CreatedAt: updatedAt, UpdatedAt: updatedAt,
	}); err != nil {
		t.Fatalf("insert failed cleanup tenant: %v", err)
	}
	return tenantID
}

func upsertFailedCleanupNativeBinding(t *testing.T, rt *quotaRuntime, tenantID, clusterID, poolID string, status meta.TenantPoolBindingStatus) {
	t.Helper()
	now := time.Now().UTC()
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(context.Background(), &meta.TenantTiDBCloudOrgBinding{
		TenantID: tenantID, OrganizationID: failedCleanupTestOrganizationID, ClusterID: clusterID,
		PoolID: poolID, PoolStatus: status, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert native cleanup binding: %v", err)
	}
}

func assertFailedCleanupTenantStatus(t *testing.T, rt *quotaRuntime, tenantID string, want meta.TenantStatus) *meta.Tenant {
	t.Helper()
	got, err := rt.meta.GetTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("get tenant %s: %v", tenantID, err)
	}
	if got.Status != want {
		t.Fatalf("tenant %s status = %s, want %s", tenantID, got.Status, want)
	}
	return got
}

func assertNoActiveFailedCleanupKeys(t *testing.T, rt *quotaRuntime, tenantID string) {
	t.Helper()
	var active int
	if err := rt.meta.DB().QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?",
		tenantID, meta.APIKeyActive).Scan(&active); err != nil {
		t.Fatalf("count active tenant keys: %v", err)
	}
	if active != 0 {
		t.Fatalf("active tenant keys = %d, want 0", active)
	}
}
