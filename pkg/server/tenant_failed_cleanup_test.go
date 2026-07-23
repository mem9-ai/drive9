package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	tenantschema "github.com/mem9-ai/drive9/pkg/tenant/schema"
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

func TestCleanupFailedOrganizationTenantsSharedFreeMemberUnreadyDB(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared", meta.TenantPoolBindingFree)

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertSharedCleanupCompleted(t, rt, rt.tenantID, fsID, dbID)
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("shared cleanup deprovision calls = %d, want 0", got)
	}
}

func TestCleanupFailedOrganizationTenantsSharedDirectUnreadyDB(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertSharedCleanupCompleted(t, rt, rt.tenantID, fsID, dbID)
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("shared cleanup deprovision calls = %d, want 0", got)
	}
}

func TestCleanupFailedOrganizationTenantsSharedNamespaceGuardRestoresState(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "namespace-present", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared", meta.TenantPoolBindingFree)

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
	assertSharedCleanupStatePreserved(t, rt, rt.tenantID, fsID, dbID)
}

func TestCleanupFailedOrganizationTenantsSharedMissingPlacementRestoresState(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared", meta.TenantPoolBindingFree)
	if err := rt.meta.DeleteTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("delete placement for missing-placement setup: %v", err)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
	if _, err := rt.meta.GetTenantPoolMembership(ctx, rt.tenantID); err != nil {
		t.Fatalf("shared membership after missing placement: %v", err)
	}
	db, err := rt.meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("get shared db: %v", err)
	}
	if db.TenantCount != 1 {
		t.Fatalf("shared tenant count = %d, want 1", db.TenantCount)
	}
}

func TestCleanupFailedOrganizationTenantsContinuesAcrossProviders(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	nativeID := rt.tenantID
	setFailedCleanupTenant(t, rt, nativeID, tenant.ProviderTiDBCloudNative, "cluster-native-fails", "", old)
	upsertFailedCleanupNativeBinding(t, rt, nativeID, "cluster-native-fails", "pool-native", meta.TenantPoolBindingFree)
	sharedID := insertFailedCleanupTenant(t, rt, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, sharedID, false)
	upsertFailedCleanupSharedMembership(t, rt, sharedID, "pool-shared", meta.TenantPoolBindingFree)
	rt.prov.deprovisionErr = errors.New("native cloud failure")

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertFailedCleanupTenantStatus(t, rt, nativeID, meta.TenantFailed)
	assertSharedCleanupCompleted(t, rt, sharedID, fsID, dbID)
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want native-only 1", got)
	}
}

func TestCleanupFailedOrganizationTenantsSharedActiveDBPurgesScopedData(t *testing.T) {
	rt, db := newFailedCleanupRuntimeWithoutWorkers(t)
	ctx := context.Background()
	installFailedCleanupSharedSchema(t, rt.meta.DB())
	old := time.Now().UTC().Add(-time.Hour)
	tenantID := insertFailedCleanupTenant(t, rt, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	rt.tenantID = tenantID
	fsID, err := rt.meta.ResolveFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("resolve fs_id: %v", err)
	}
	passwordCipher, err := rt.server.pool.Encrypt(ctx, []byte(db.DBPass))
	if err != nil {
		t.Fatalf("encrypt shared db password: %v", err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: failedCleanupTestOrganizationID,
		Host:                    db.DBHost, Port: db.DBPort, User: db.DBUser, PasswordCipher: passwordCipher,
		Name: db.DBName, MaxTenants: 10, Status: meta.SharedDBStatusActive,
	})
	if err != nil {
		t.Fatalf("register reachable shared db: %v", err)
	}
	t.Cleanup(func() {
		if err := rt.server.pool.InvalidateSharedDB(dbID); err != nil {
			t.Errorf("invalidate reachable shared db: %v", err)
		}
	})
	if _, err := rt.meta.DB().ExecContext(ctx,
		"UPDATE db_pool SET tenant_count = 1, schema_version = ? WHERE db_id = ?",
		tenantschema.CurrentSharedTiDBSchemaVersion, dbID); err != nil {
		t.Fatalf("mark shared db schema ready: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("upsert reachable shared placement: %v", err)
	}
	upsertFailedCleanupSharedMembership(t, rt, tenantID, "pool-shared-reachable", meta.TenantPoolBindingFree)
	const otherFsID int64 = 987654321
	if _, err := rt.meta.DB().ExecContext(ctx, `INSERT INTO inodes
		(fs_id, inode_id, size_bytes, revision, status) VALUES
		(?, 'cleanup-target', 10, 1, 'CONFIRMED'),
		(?, 'cleanup-other', 20, 1, 'CONFIRMED')`, fsID, otherFsID); err != nil {
		t.Fatalf("seed shared-schema inode rows: %v", err)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertSharedCleanupCompleted(t, rt, tenantID, fsID, dbID)
	assertFailedCleanupFSRowCount(t, rt.meta.DB(), "inodes", fsID, 0)
	assertFailedCleanupFSRowCount(t, rt.meta.DB(), "inodes", otherFsID, 1)
}

func TestCleanupFailedOrganizationTenantsSharedActiveDBPurgeFailureRestoresState(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, true)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared-unreachable", meta.TenantPoolBindingFree)

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	failed := assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
	if !failed.UpdatedAt.After(old) {
		t.Fatalf("updated_at = %s, want after %s", failed.UpdatedAt, old)
	}
	assertSharedCleanupStatePreserved(t, rt, rt.tenantID, fsID, dbID)
}

func TestCleanupFailedOrganizationTenantsSharedActiveIncompleteDBSkipsPurge(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared-incomplete", meta.TenantPoolBindingFree)
	if _, err := rt.meta.DB().ExecContext(ctx,
		"UPDATE db_pool SET status = ?, db_password = X'' WHERE db_id = ?",
		meta.SharedDBStatusActive, dbID); err != nil {
		t.Fatalf("make active shared db incomplete: %v", err)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertSharedCleanupCompleted(t, rt, rt.tenantID, fsID, dbID)
}

func TestCleanupFailedOrganizationTenantsAbortsActiveAndCompletingReservations(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared-reservations", meta.TenantPoolBindingFree)
	for _, reservation := range []*meta.UploadReservation{
		{TenantID: rt.tenantID, UploadID: "upload-active", ReservedBytes: 40, FileCountDelta: 1, TargetPath: "/active.bin", ExpiresAt: time.Now().Add(time.Hour)},
		{TenantID: rt.tenantID, UploadID: "upload-completing", ReservedBytes: 60, FileCountDelta: 1, TargetPath: "/completing.bin", ExpiresAt: time.Now().Add(time.Hour)},
	} {
		if err := rt.meta.AtomicReserveAndInsertUpload(ctx, reservation); err != nil {
			t.Fatalf("reserve upload %s: %v", reservation.UploadID, err)
		}
	}
	if err := rt.meta.UpdateUploadReservationStatus(ctx, rt.tenantID, "upload-completing", "completing"); err != nil {
		t.Fatalf("mark reservation completing: %v", err)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertSharedCleanupCompleted(t, rt, rt.tenantID, fsID, dbID)
	for _, uploadID := range []string{"upload-active", "upload-completing"} {
		reservation, err := rt.meta.GetUploadReservation(ctx, rt.tenantID, uploadID)
		if err != nil {
			t.Fatalf("get reservation %s: %v", uploadID, err)
		}
		if reservation.Status != "aborted" {
			t.Fatalf("reservation %s status = %s, want aborted", uploadID, reservation.Status)
		}
	}
	usage, err := rt.meta.GetQuotaUsage(ctx, rt.tenantID)
	if err != nil {
		t.Fatalf("get quota usage: %v", err)
	}
	if usage.ReservedBytes != 0 || usage.FileCount != 0 {
		t.Fatalf("quota usage after abort = %+v, want reserved_bytes=0 file_count=0", usage)
	}
}

func TestCleanupFailedOrganizationTenantsReservationAbortFailureStillFinalizes(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared-abort-failure", meta.TenantPoolBindingFree)
	if err := rt.meta.InsertUploadReservation(ctx, &meta.UploadReservation{
		TenantID: rt.tenantID, UploadID: "upload-missing-usage", ReservedBytes: 80, FileCountDelta: 1,
		TargetPath: "/missing-usage.bin", ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("insert reservation without quota usage: %v", err)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertSharedCleanupCompleted(t, rt, rt.tenantID, fsID, dbID)
	reservation, err := rt.meta.GetUploadReservation(ctx, rt.tenantID, "upload-missing-usage")
	if err != nil {
		t.Fatalf("get reservation after best-effort abort failure: %v", err)
	}
	if reservation.Status != "active" {
		t.Fatalf("reservation status = %s, want active after rolled-back abort", reservation.Status)
	}
}

func TestCleanupFailedOrganizationTenantsResolveFsIDDoesNotCreateRegistry(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared-no-fsid", meta.TenantPoolBindingFree)
	if _, err := rt.meta.DB().ExecContext(ctx, "DELETE FROM fs_registry WHERE tenant_id = ?", rt.tenantID); err != nil {
		t.Fatalf("delete fs_registry fixture row: %v", err)
	}

	rt.server.cleanupFailedOrganizationTenants(ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
	if _, err := rt.meta.GetTenantPoolMembership(ctx, rt.tenantID); err != nil {
		t.Fatalf("shared membership after fs_id resolution failure: %v", err)
	}
	var registryRows int
	if err := rt.meta.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM fs_registry WHERE tenant_id = ?", rt.tenantID).Scan(&registryRows); err != nil {
		t.Fatalf("count fs_registry rows: %v", err)
	}
	if registryRows != 0 {
		t.Fatalf("fs_registry rows = %d, want 0 (cleanup must not allocate fs_id)", registryRows)
	}
}

func TestCleanupFailedOrganizationTenantsNativeListFailureContinuesShared(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNativeShared, "", "", old)
	fsID, dbID := setFailedCleanupSharedPlacement(t, rt, rt.tenantID, false)
	upsertFailedCleanupSharedMembership(t, rt, rt.tenantID, "pool-shared-native-list-failure", meta.TenantPoolBindingFree)
	sentinel := errors.New("native list unavailable")
	nativeLoader := func(context.Context, string, time.Time, int) ([]meta.TenantWithTiDBCloudOrgBinding, error) {
		return nil, sentinel
	}
	sharedLoader := func(ctx context.Context, organizationID string, cutoff time.Time, limit int) ([]meta.Tenant, error) {
		return rt.meta.ListFailedSharedTenantCleanupCandidates(ctx, organizationID, cutoff, limit)
	}

	rt.server.cleanupFailedOrganizationTenantsWithLoaders(
		ctx, failedCleanupTestOrganizationID,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
		nativeLoader, sharedLoader)

	assertSharedCleanupCompleted(t, rt, rt.tenantID, fsID, dbID)
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

func upsertFailedCleanupSharedMembership(t *testing.T, rt *quotaRuntime, tenantID, poolID string, status meta.TenantPoolBindingStatus) {
	t.Helper()
	now := time.Now().UTC()
	if err := rt.meta.UpsertTenantPoolMembership(context.Background(), &meta.TenantPoolMembership{
		TenantID: tenantID, TiDBCloudOrganizationID: failedCleanupTestOrganizationID,
		PoolID: poolID, PoolStatus: status, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert shared cleanup membership: %v", err)
	}
}

func setFailedCleanupSharedPlacement(t *testing.T, rt *quotaRuntime, tenantID string, ready bool) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	fsID, err := rt.meta.ResolveFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("resolve fs_id: %v", err)
	}
	shared := &meta.SharedDB{
		TiDBCloudOrganizationID: failedCleanupTestOrganizationID,
		Host:                    fmt.Sprintf("unready-%s.example.com", tenantID),
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher"),
		Name:                    fmt.Sprintf("shared_%s", tenantID),
		MaxTenants:              10,
		Status:                  meta.SharedDBStatusProvisioning,
	}
	if ready {
		passwordCipher, err := rt.server.pool.Encrypt(ctx, []byte("unreachable-password"))
		if err != nil {
			t.Fatalf("encrypt unreachable shared db password: %v", err)
		}
		shared.Host = "127.0.0.1"
		shared.Port = 1
		shared.PasswordCipher = passwordCipher
		shared.Status = meta.SharedDBStatusActive
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, shared)
	if err != nil {
		t.Fatalf("register shared db: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("upsert shared placement: %v", err)
	}
	if _, err := rt.meta.DB().ExecContext(ctx, "UPDATE db_pool SET tenant_count = 1 WHERE db_id = ?", dbID); err != nil {
		t.Fatalf("seed shared tenant count: %v", err)
	}
	return fsID, dbID
}

func newFailedCleanupRuntimeWithoutWorkers(t *testing.T) (*quotaRuntime, *testDBInfo) {
	t.Helper()
	db := newTenantDeleteDBInfo(t)
	testmysql.ResetMetaDB(t, db.Meta.DB())
	testmysql.ResetDB(t, db.Meta.DB())
	t.Cleanup(func() {
		testmysql.ResetMetaDB(t, db.Meta.DB())
		testmysql.ResetDB(t, db.Meta.DB())
	})
	prov := &quotaTestProvisioner{provider: tenant.ProviderTiDBCloudNative}
	server := &Server{
		meta: db.Meta, pool: db.Pool, provisioner: prov,
		sharedDBReopenRatio: DefaultSharedDBReopenRatio,
	}
	return &quotaRuntime{meta: db.Meta, prov: prov, server: server}, db
}

var failedCleanupSharedTables = []string{
	"journal_entry_subjects", "journal_entries", "journal_append_requests", "journal_labels", "journals",
	"vault_audit_log", "vault_grants", "vault_tokens", "vault_secret_fields", "vault_secrets", "vault_deks",
	"git_workspace_object_packs", "git_workspace_overlay", "git_workspace_git_state", "git_workspace_tree_nodes", "git_workspaces",
	"fs_layer_checkpoints", "fs_layer_events", "fs_layer_tags", "fs_layer_entries", "fs_layers",
	"fs_events", "semantic_tasks", "file_gc_tasks", "uploads", "file_tags", "file_nodes", "semantic", "contents", "inodes",
}

func installFailedCleanupSharedSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	t.Cleanup(func() {
		dropFailedCleanupSharedTables(t, db)
		initServerTenantSchema(t, testDSN)
	})
	dropFailedCleanupSharedTables(t, db)
	if err := tenantschema.InitSharedSchema(ctx, testDSN); err != nil {
		t.Fatalf("initialize shared schema: %v", err)
	}
}

func dropFailedCleanupSharedTables(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, table := range failedCleanupSharedTables {
		if _, err := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop shared table %s: %v", table, err)
		}
	}
}

func assertFailedCleanupFSRowCount(t *testing.T, db *sql.DB, table string, fsID int64, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM "+table+" WHERE fs_id = ?", fsID).Scan(&got); err != nil {
		t.Fatalf("count %s rows for fs_id %d: %v", table, fsID, err)
	}
	if got != want {
		t.Fatalf("%s rows for fs_id %d = %d, want %d", table, fsID, got, want)
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

func assertSharedCleanupCompleted(t *testing.T, rt *quotaRuntime, tenantID string, fsID, dbID int64) {
	t.Helper()
	ctx := context.Background()
	assertFailedCleanupTenantStatus(t, rt, tenantID, meta.TenantDeleted)
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("shared placement after cleanup error = %v, want %v", err, meta.ErrNotFound)
	}
	if _, err := rt.meta.GetTenantPoolMembership(ctx, tenantID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("shared membership after cleanup error = %v, want %v", err, meta.ErrNotFound)
	}
	db, err := rt.meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("get shared db: %v", err)
	}
	if db.TenantCount != 0 {
		t.Fatalf("shared tenant count = %d, want 0", db.TenantCount)
	}
	assertNoActiveFailedCleanupKeys(t, rt, tenantID)
	if exists, err := rt.meta.TenantDeleteJobExists(ctx, tenantID); err != nil {
		t.Fatalf("check tenant delete job: %v", err)
	} else if exists {
		t.Fatal("shared failed cleanup enqueued a tenant delete job")
	}
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

func assertSharedCleanupStatePreserved(t *testing.T, rt *quotaRuntime, tenantID string, fsID, dbID int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("shared placement after failed cleanup: %v", err)
	}
	if _, err := rt.meta.GetTenantPoolMembership(ctx, tenantID); err != nil {
		t.Fatalf("shared membership after failed cleanup: %v", err)
	}
	db, err := rt.meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("get shared db: %v", err)
	}
	if db.TenantCount != 1 {
		t.Fatalf("shared tenant count = %d, want 1", db.TenantCount)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("shared cleanup deprovision calls = %d, want 0", got)
	}
}
