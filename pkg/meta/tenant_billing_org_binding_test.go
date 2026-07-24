package meta

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTenantBillingOrgBindingSchemaAndCRUD(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	var tableCount, indexCount int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = 'tenant_billing_org_bindings'`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'tenant_billing_org_bindings'
			AND index_name = 'idx_billing_org_tenant'`).Scan(&indexCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 1 || indexCount != 2 {
		t.Fatalf("table/index count = %d/%d, want 1/2 index columns", tableCount, indexCount)
	}

	insertBillingTestTenant(t, s, "billing-crud", TenantPending, tidbCloudNativeProvider)
	if err := s.UpsertTenantBillingOrgBinding(ctx, " billing-crud ", " 1440002 "); err != nil {
		t.Fatal(err)
	}
	binding, err := s.GetTenantBillingOrgBinding(ctx, "billing-crud")
	if err != nil {
		t.Fatal(err)
	}
	if binding.TenantID != "billing-crud" || binding.TiDBCloudOrganizationID != "1440002" {
		t.Fatalf("binding = %+v", binding)
	}
}

func TestCountTiDBCloudFreeTenantsUsesExplicitZeroAndNonDeletedStatus(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	zero, positive := int64(0), int64(100)
	tests := []struct {
		id       string
		status   TenantStatus
		org      string
		spending *int64
		want     bool
	}{
		{id: "pending-zero", status: TenantPending, org: "org-a", spending: &zero, want: true},
		{id: "active-zero", status: TenantActive, org: "org-a", spending: &zero, want: true},
		{id: "failed-zero", status: TenantFailed, org: "org-a", spending: &zero, want: true},
		{id: "deleting-zero", status: TenantDeleting, org: "org-a", spending: &zero, want: true},
		{id: "deleted-zero", status: TenantDeleted, org: "org-a", spending: &zero},
		{id: "active-positive", status: TenantActive, org: "org-a", spending: &positive},
		{id: "active-null", status: TenantActive, org: "org-a"},
		{id: "other-org-zero", status: TenantActive, org: "org-b", spending: &zero},
	}
	want := 0
	for _, tt := range tests {
		insertBillingTestTenant(t, s, tt.id, tt.status, tidbCloudNativeProvider)
		if err := s.UpsertTenantBillingOrgBinding(ctx, tt.id, tt.org); err != nil {
			t.Fatal(err)
		}
		if tt.spending != nil {
			if err := s.SetQuotaConfigPatch(ctx, tt.id, QuotaConfigPatch{TiDBCloudSpendingLimit: tt.spending}); err != nil {
				t.Fatal(err)
			}
		} else if err := s.SetQuotaConfigPatch(ctx, tt.id, QuotaConfigPatch{MaxFileCount: &positive}); err != nil {
			t.Fatal(err)
		}
		if tt.want {
			want++
		}
	}
	got, err := s.CountTiDBCloudFreeTenants(ctx, "org-a")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("free tenant count = %d, want %d", got, want)
	}
}

func TestBackfillTenantBillingOrgBindingsFromReliableSources(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	insertBillingTestTenant(t, s, "backfill-native", TenantActive, tidbCloudNativeProvider)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "backfill-native", OrganizationID: "org-native", ClusterID: "cluster-native",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	insertSharedTenantPlacementForOrgTest(t, s, "backfill-placement", "org-placement")
	insertBillingTestTenant(t, s, "backfill-membership", TenantActive, tidbCloudNativeSharedProvider)
	if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID: "backfill-membership", PoolID: "pool-membership",
		TiDBCloudOrganizationID: "org-membership", PoolStatus: TenantPoolBindingUsed,
	}); err != nil {
		t.Fatal(err)
	}

	if err := backfillTenantBillingOrgBindings(ctx, s.DB()); err != nil {
		t.Fatal(err)
	}
	fixedUpdatedAt := time.Date(2024, time.January, 2, 3, 4, 5, 123000000, time.UTC)
	if _, err := s.DB().ExecContext(ctx, `UPDATE tenant_billing_org_bindings SET updated_at = ?`, fixedUpdatedAt); err != nil {
		t.Fatal(err)
	}
	if err := backfillTenantBillingOrgBindings(ctx, s.DB()); err != nil {
		t.Fatalf("idempotent backfill: %v", err)
	}
	for tenantID, wantOrg := range map[string]string{
		"backfill-native":     "org-native",
		"backfill-placement":  "org-placement",
		"backfill-membership": "org-membership",
	} {
		binding, err := s.GetTenantBillingOrgBinding(ctx, tenantID)
		if err != nil || binding.TiDBCloudOrganizationID != wantOrg {
			t.Fatalf("binding %s = %+v/%v, want %s", tenantID, binding, err, wantOrg)
		}
		if !binding.UpdatedAt.Equal(fixedUpdatedAt) {
			t.Fatalf("binding %s updated_at = %v, want unchanged %v", tenantID, binding.UpdatedAt, fixedUpdatedAt)
		}
	}
}

func TestBackfillTenantBillingOrgBindingsExcludesFreePoolInventory(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	insertBillingTestTenant(t, s, "backfill-native-free", TenantActive, tidbCloudNativeProvider)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "backfill-native-free", OrganizationID: "org-native-free", ClusterID: "cluster-native-free",
		PoolID: "pool-native-free", PoolStatus: TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	insertSharedTenantPlacementForOrgTest(t, s, "backfill-shared-free", "org-shared-free")
	if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID: "backfill-shared-free", PoolID: "pool-shared-free",
		TiDBCloudOrganizationID: "org-shared-free", PoolStatus: TenantPoolBindingFree,
	}); err != nil {
		t.Fatal(err)
	}

	if err := backfillTenantBillingOrgBindings(ctx, s.DB()); err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"backfill-native-free", "backfill-shared-free"} {
		if _, err := s.GetTenantBillingOrgBinding(ctx, tenantID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("free pool inventory %s billing binding error = %v, want not found", tenantID, err)
		}
	}
}

func TestBackfillTenantBillingOrgBindingsRejectsConflicts(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	insertSharedTenantPlacementForOrgTest(t, s, "backfill-conflict", "org-placement")
	if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID: "backfill-conflict", PoolID: "pool-conflict",
		TiDBCloudOrganizationID: "org-membership", PoolStatus: TenantPoolBindingUsed,
	}); err != nil {
		t.Fatal(err)
	}
	if err := backfillTenantBillingOrgBindings(ctx, s.DB()); err == nil {
		t.Fatal("conflicting organization sources did not fail")
	}
}

func TestWithTiDBCloudFreeQuotaLockSerializesAndReturnsBusy(t *testing.T) {
	s := newControlStore(t)
	originalTimeout := tidbCloudFreeQuotaLockTimeoutSeconds
	tidbCloudFreeQuotaLockTimeoutSeconds = 0
	t.Cleanup(func() { tidbCloudFreeQuotaLockTimeoutSeconds = originalTimeout })

	acquired := make(chan struct{})
	release := make(chan struct{})
	var firstErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		firstErr = s.WithTiDBCloudFreeQuotaLock(context.Background(), "org-lock", func(context.Context) error {
			close(acquired)
			<-release
			return nil
		})
	}()
	<-acquired
	err := s.WithTiDBCloudFreeQuotaLock(context.Background(), "org-lock", func(context.Context) error {
		t.Fatal("busy lock callback executed")
		return nil
	})
	close(release)
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("first lock: %v", firstErr)
	}
	if !errors.Is(err, ErrTiDBCloudFreeQuotaBusy) {
		t.Fatalf("second lock error = %v, want busy", err)
	}
}

func TestReserveTiDBCloudFreeTenantPersistsAtomicReservationAndEnforcesLimit(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	zero := int64(0)
	storage, fileSize, fileCount := int64(3<<30), int64(300<<20), int64(1000)
	quota := QuotaConfigPatch{
		MaxStorageBytes:        &storage,
		MaxFileSizeBytes:       &fileSize,
		MaxFileCount:           &fileCount,
		TiDBCloudSpendingLimit: &zero,
	}
	for _, tenantID := range []string{"free-reservation-1", "free-reservation-2"} {
		if err := s.ReserveTiDBCloudFreeTenant(ctx, newBillingTestTenant(tenantID), "org-reservation", 2, quota); err != nil {
			t.Fatalf("ReserveTiDBCloudFreeTenant(%s): %v", tenantID, err)
		}
	}
	if err := s.ReserveTiDBCloudFreeTenant(ctx, newBillingTestTenant("free-reservation-3"), "org-reservation", 2, quota); !errors.Is(err, ErrTiDBCloudFreeTenantLimitReached) {
		t.Fatalf("third reservation error = %v, want tenant limit", err)
	}
	if _, err := s.GetTenant(ctx, "free-reservation-3"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("third tenant lookup = %v, want not found", err)
	}
	binding, err := s.GetTenantBillingOrgBinding(ctx, "free-reservation-1")
	if err != nil || binding.TiDBCloudOrganizationID != "org-reservation" {
		t.Fatalf("binding = %+v, err=%v", binding, err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "free-reservation-1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != storage || cfg.MaxFileSizeBytes != fileSize || cfg.MaxFileCount != fileCount || cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != 0 {
		t.Fatalf("quota = %+v", cfg)
	}
}

func TestInsertTiDBCloudTenantReservationRollsBackEveryRow(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	const tenantID = "reservation-rollback"
	if err := s.UpsertTenantBillingOrgBinding(ctx, tenantID, "preexisting-org"); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	storage, fileSize, fileCount := int64(3<<30), int64(300<<20), int64(1000)
	err := s.InsertTiDBCloudTenantReservation(ctx, newBillingTestTenant(tenantID), "new-org", &QuotaConfigPatch{
		MaxStorageBytes:        &storage,
		MaxFileSizeBytes:       &fileSize,
		MaxFileCount:           &fileCount,
		TiDBCloudSpendingLimit: &zero,
	})
	if err == nil {
		t.Fatal("reservation with duplicate binding succeeded")
	}
	if _, err := s.GetTenant(ctx, tenantID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant lookup after rollback = %v, want not found", err)
	}
	var quotaRows int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_quota_config WHERE tenant_id = ?`, tenantID).Scan(&quotaRows); err != nil {
		t.Fatal(err)
	}
	if quotaRows != 0 {
		t.Fatalf("quota rows after rollback = %d, want 0", quotaRows)
	}
}

func TestReserveTiDBCloudFreeTenantSerializesAcrossStores(t *testing.T) {
	s1 := newControlStore(t)
	s2, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	zero := int64(0)
	storage, fileSize, fileCount := int64(3<<30), int64(300<<20), int64(1000)
	quota := QuotaConfigPatch{
		MaxStorageBytes:        &storage,
		MaxFileSizeBytes:       &fileSize,
		MaxFileCount:           &fileCount,
		TiDBCloudSpendingLimit: &zero,
	}
	stores := []*Store{s1, s2}
	var successes atomic.Int32
	errs := make(chan error, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		wg.Add(1)
		go func(i int, store *Store) {
			defer wg.Done()
			err := store.ReserveTiDBCloudFreeTenant(context.Background(), newBillingTestTenant(fmt.Sprintf("concurrent-free-%d", i)), "org-concurrent", 1, quota)
			if err == nil {
				successes.Add(1)
			}
			errs <- err
		}(i, store)
	}
	wg.Wait()
	close(errs)
	limits := 0
	for err := range errs {
		if errors.Is(err, ErrTiDBCloudFreeTenantLimitReached) {
			limits++
		} else if err != nil {
			t.Fatalf("reservation error = %v", err)
		}
	}
	if successes.Load() != 1 || limits != 1 {
		t.Fatalf("successes/limits = %d/%d, want 1/1", successes.Load(), limits)
	}
}

func TestDeleteStaleTiDBCloudFreeReservationRequiresReservationOnlyShape(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-time.Minute)

	insertReservation := func(tenantID, provider string) {
		t.Helper()
		insertBillingTestTenant(t, s, tenantID, TenantPending, provider)
		if err := s.UpsertTenantBillingOrgBinding(ctx, tenantID, "org-reservation-cleanup"); err != nil {
			t.Fatal(err)
		}
		zero := int64(0)
		if err := s.SetQuotaConfigPatch(ctx, tenantID, QuotaConfigPatch{TiDBCloudSpendingLimit: &zero}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB().ExecContext(ctx, `UPDATE tenants SET updated_at = ? WHERE id = ?`, cutoff.Add(-time.Minute), tenantID); err != nil {
			t.Fatal(err)
		}
	}
	assertNotDeleted := func(tenantID string, wantStatus TenantStatus) {
		t.Helper()
		deleted, err := s.DeleteStaleTiDBCloudFreeReservation(ctx, tenantID, cutoff)
		if err != nil {
			t.Fatal(err)
		}
		if deleted {
			t.Fatalf("tenant %s was deleted despite reservation disqualifier", tenantID)
		}
		got, err := s.GetTenant(ctx, tenantID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != wantStatus {
			t.Fatalf("tenant %s status = %s, want %s", tenantID, got.Status, wantStatus)
		}
	}

	insertReservation("reservation-only", tidbCloudNativeProvider)
	deleted, err := s.DeleteStaleTiDBCloudFreeReservation(ctx, "reservation-only", cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("reservation-only tenant was not deleted")
	}
	reservation, err := s.GetTenant(ctx, "reservation-only")
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Status != TenantDeleted {
		t.Fatalf("reservation-only status = %s, want %s", reservation.Status, TenantDeleted)
	}

	insertReservation("reservation-namespace", tidbCloudNativeProvider)
	if _, err := s.DB().ExecContext(ctx, `UPDATE tenants SET storage_namespace_id = 'namespace-1', updated_at = ?
		WHERE id = 'reservation-namespace'`, cutoff.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-namespace", TenantPending)

	insertReservation("reservation-api-key", tidbCloudNativeProvider)
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenant_api_keys
		(id, tenant_id, key_name, jwt_ciphertext, jwt_hash, status)
		VALUES ('key-reservation', 'reservation-api-key', 'default', X'01', 'hash-reservation', 'revoked')`); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-api-key", TenantPending)

	insertReservation("reservation-cluster", tidbCloudNativeProvider)
	if _, err := s.DB().ExecContext(ctx, `UPDATE tenants SET cluster_id = 'cluster-reservation', updated_at = ?
		WHERE id = 'reservation-cluster'`, cutoff.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-cluster", TenantPending)

	insertReservation("reservation-native-binding", tidbCloudNativeProvider)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "reservation-native-binding", OrganizationID: "org-reservation-cleanup", ClusterID: "cluster-native-binding",
	}); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-native-binding", TenantPending)

	insertReservation("reservation-placement", tidbCloudNativeProvider)
	fsID, err := s.EnsureFsID(ctx, "reservation-placement")
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-reservation-cleanup", Host: "reservation-placement.example", Port: 4000,
		User: "root", PasswordCipher: []byte("cipher"), Name: "reservation_placement",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTenantPlacement(ctx, &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-placement", TenantPending)

	insertReservation("reservation-membership", tidbCloudNativeSharedProvider)
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenant_pool_memberships
		(tenant_id, pool_id, pool_status) VALUES ('reservation-membership', 'pool-reservation', 'free')`); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-membership", TenantPending)

	insertReservation("reservation-fresh", tidbCloudNativeProvider)
	if _, err := s.DB().ExecContext(ctx, `UPDATE tenants SET updated_at = ? WHERE id = 'reservation-fresh'`, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-fresh", TenantPending)

	insertReservation("reservation-status-changed", tidbCloudNativeProvider)
	if _, err := s.DB().ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = 'reservation-status-changed'`, TenantProvisioning, cutoff.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertNotDeleted("reservation-status-changed", TenantProvisioning)
}

func TestHasTenantPoolOwnershipUsesExplicitOwnershipMetadata(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	insertBillingTestTenant(t, s, "ownership-none", TenantPending, tidbCloudNativeProvider)
	owned, err := s.HasTenantPoolOwnership(ctx, "ownership-none")
	if err != nil || owned {
		t.Fatalf("ownership-none = %v, err=%v, want false", owned, err)
	}

	insertBillingTestTenant(t, s, "ownership-native-binding-only", TenantPending, tidbCloudNativeProvider)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "ownership-native-binding-only", OrganizationID: "org-binding-only", ClusterID: "cluster-binding-only",
	}); err != nil {
		t.Fatal(err)
	}
	owned, err = s.HasTenantPoolOwnership(ctx, "ownership-native-binding-only")
	if err != nil || owned {
		t.Fatalf("native binding without pool = %v, err=%v, want false", owned, err)
	}

	insertBillingTestTenant(t, s, "ownership-native-pool", TenantPending, tidbCloudNativeProvider)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "ownership-native-pool", OrganizationID: "org-native-pool", ClusterID: "cluster-native-pool", PoolID: "pool-native",
	}); err != nil {
		t.Fatal(err)
	}
	owned, err = s.HasTenantPoolOwnership(ctx, "ownership-native-pool")
	if err != nil || !owned {
		t.Fatalf("native pool ownership = %v, err=%v, want true", owned, err)
	}

	insertBillingTestTenant(t, s, "ownership-membership", TenantPending, tidbCloudNativeSharedProvider)
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenant_pool_memberships
		(tenant_id, pool_id, pool_status) VALUES ('ownership-membership', 'pool-membership', 'free')`); err != nil {
		t.Fatal(err)
	}
	owned, err = s.HasTenantPoolOwnership(ctx, "ownership-membership")
	if err != nil || !owned {
		t.Fatalf("membership ownership = %v, err=%v, want true", owned, err)
	}

	insertBillingTestTenant(t, s, "ownership-placement", TenantPending, tidbCloudNativeSharedProvider)
	fsID, err := s.EnsureFsID(ctx, "ownership-placement")
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-ownership-placement", Host: "ownership-placement.example", Port: 4000,
		User: "root", PasswordCipher: []byte("cipher"), Name: "ownership_placement",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTenantPlacement(ctx, &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}); err != nil {
		t.Fatal(err)
	}
	owned, err = s.HasTenantPoolOwnership(ctx, "ownership-placement")
	if err != nil || !owned {
		t.Fatalf("placement ownership = %v, err=%v, want true", owned, err)
	}
}

func insertBillingTestTenant(t *testing.T, s *Store, tenantID string, status TenantStatus, provider string) {
	t.Helper()
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID: tenantID, Status: status, Kind: TenantKindLive, Provider: provider,
		DBPasswordCipher: []byte{}, DBTLS: true, SchemaVersion: 1,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
}

func newBillingTestTenant(tenantID string) *Tenant {
	now := time.Now().UTC()
	return &Tenant{
		ID: tenantID, Status: TenantPending, Kind: TenantKindLive, Provider: tidbCloudNativeProvider,
		DBPasswordCipher: []byte{}, DBTLS: true, SchemaVersion: 1,
		CreatedAt: now, UpdatedAt: now,
	}
}
