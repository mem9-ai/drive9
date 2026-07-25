package meta

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCountTiDBCloudFreeTenantsUsesExplicitZeroAndNonDeletedStatus(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	zero, positive := int64(0), int64(100)
	tests := []struct {
		id         string
		provider   string
		status     TenantStatus
		org        string
		spending   *int64
		poolStatus TenantPoolBindingStatus
		want       bool
	}{
		{id: "native-pending-zero", provider: tidbCloudNativeProvider, status: TenantPending, org: "org-a", spending: &zero, want: true},
		{id: "native-active-zero", provider: tidbCloudNativeProvider, status: TenantActive, org: "org-a", spending: &zero, want: true},
		{id: "shared-failed-zero", provider: tidbCloudNativeSharedProvider, status: TenantFailed, org: "org-a", spending: &zero, want: true},
		{id: "shared-deleting-zero", provider: tidbCloudNativeSharedProvider, status: TenantDeleting, org: "org-a", spending: &zero, want: true},
		{id: "native-deleted-zero", provider: tidbCloudNativeProvider, status: TenantDeleted, org: "org-a", spending: &zero},
		{id: "shared-active-positive", provider: tidbCloudNativeSharedProvider, status: TenantActive, org: "org-a", spending: &positive},
		{id: "native-active-null", provider: tidbCloudNativeProvider, status: TenantActive, org: "org-a"},
		{id: "shared-other-org-zero", provider: tidbCloudNativeSharedProvider, status: TenantActive, org: "org-b", spending: &zero},
		{id: "native-free-pool-inventory", provider: tidbCloudNativeProvider, status: TenantActive, org: "org-a", spending: &zero, poolStatus: TenantPoolBindingFree},
		{id: "shared-free-pool-inventory", provider: tidbCloudNativeSharedProvider, status: TenantActive, org: "org-a", spending: &zero, poolStatus: TenantPoolBindingFree},
	}
	want := 0
	for _, tt := range tests {
		insertFreeQuotaTestTenant(t, s, tt.id, tt.status, tt.provider)
		switch tt.provider {
		case tidbCloudNativeProvider:
			binding := &TenantTiDBCloudOrgBinding{
				TenantID: tt.id, OrganizationID: tt.org, ClusterID: "cluster-" + tt.id,
			}
			if tt.poolStatus != "" {
				binding.PoolID = "pool-" + tt.id
				binding.PoolStatus = tt.poolStatus
			}
			if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, binding); err != nil {
				t.Fatal(err)
			}
		case tidbCloudNativeSharedProvider:
			insertSharedTenantPlacementForExistingTenantTest(t, s, tt.id, tt.org)
			if tt.poolStatus != "" {
				if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
					TenantID: tt.id, PoolID: "pool-" + tt.id,
					TiDBCloudOrganizationID: tt.org, PoolStatus: tt.poolStatus,
				}); err != nil {
					t.Fatal(err)
				}
			}
		default:
			t.Fatalf("unsupported provider %q", tt.provider)
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

func insertSharedTenantPlacementForExistingTenantTest(t *testing.T, s *Store, tenantID, organizationID string) {
	t.Helper()
	ctx := context.Background()
	fsID, err := s.EnsureFsID(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: organizationID,
		Host:                    tenantID + ".shared.example.com",
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher"),
		Name:                    "shared_" + tenantID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTenantPlacement(ctx, &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}); err != nil {
		t.Fatal(err)
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

func TestInsertTiDBCloudFreeTenantReservationPersistsExplicitQuota(t *testing.T) {
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
	const tenantID = "free-reservation"
	if err := s.InsertTiDBCloudFreeTenantReservation(ctx, newFreeQuotaTestTenant(tenantID), quota); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != storage || cfg.MaxFileSizeBytes != fileSize || cfg.MaxFileCount != fileCount || cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != 0 {
		t.Fatalf("quota = %+v", cfg)
	}
}

func TestInsertTiDBCloudFreeTenantReservationRollsBackEveryRow(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	const tenantID = "reservation-rollback"
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenant_quota_config (tenant_id) VALUES (?)`, tenantID); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	storage, fileSize, fileCount := int64(3<<30), int64(300<<20), int64(1000)
	err := s.InsertTiDBCloudFreeTenantReservation(ctx, newFreeQuotaTestTenant(tenantID), QuotaConfigPatch{
		MaxStorageBytes:        &storage,
		MaxFileSizeBytes:       &fileSize,
		MaxFileCount:           &fileCount,
		TiDBCloudSpendingLimit: &zero,
	})
	if err == nil {
		t.Fatal("reservation with duplicate quota row succeeded")
	}
	if _, err := s.GetTenant(ctx, tenantID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant lookup after rollback = %v, want not found", err)
	}
	var quotaRows int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_quota_config WHERE tenant_id = ?`, tenantID).Scan(&quotaRows); err != nil {
		t.Fatal(err)
	}
	if quotaRows != 1 {
		t.Fatalf("quota rows after rollback = %d, want preexisting row only", quotaRows)
	}
}

func TestDeleteStaleTiDBCloudFreeReservationRequiresReservationOnlyShape(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-time.Minute)

	insertReservation := func(tenantID, provider string) {
		t.Helper()
		insertFreeQuotaTestTenant(t, s, tenantID, TenantPending, provider)
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

	insertFreeQuotaTestTenant(t, s, "ownership-none", TenantPending, tidbCloudNativeProvider)
	owned, err := s.HasTenantPoolOwnership(ctx, "ownership-none")
	if err != nil || owned {
		t.Fatalf("ownership-none = %v, err=%v, want false", owned, err)
	}

	insertFreeQuotaTestTenant(t, s, "ownership-native-binding-only", TenantPending, tidbCloudNativeProvider)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "ownership-native-binding-only", OrganizationID: "org-binding-only", ClusterID: "cluster-binding-only",
	}); err != nil {
		t.Fatal(err)
	}
	owned, err = s.HasTenantPoolOwnership(ctx, "ownership-native-binding-only")
	if err != nil || owned {
		t.Fatalf("native binding without pool = %v, err=%v, want false", owned, err)
	}

	insertFreeQuotaTestTenant(t, s, "ownership-native-pool", TenantPending, tidbCloudNativeProvider)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "ownership-native-pool", OrganizationID: "org-native-pool", ClusterID: "cluster-native-pool", PoolID: "pool-native",
	}); err != nil {
		t.Fatal(err)
	}
	owned, err = s.HasTenantPoolOwnership(ctx, "ownership-native-pool")
	if err != nil || !owned {
		t.Fatalf("native pool ownership = %v, err=%v, want true", owned, err)
	}

	insertFreeQuotaTestTenant(t, s, "ownership-membership", TenantPending, tidbCloudNativeSharedProvider)
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenant_pool_memberships
		(tenant_id, pool_id, pool_status) VALUES ('ownership-membership', 'pool-membership', 'free')`); err != nil {
		t.Fatal(err)
	}
	owned, err = s.HasTenantPoolOwnership(ctx, "ownership-membership")
	if err != nil || !owned {
		t.Fatalf("membership ownership = %v, err=%v, want true", owned, err)
	}

	insertFreeQuotaTestTenant(t, s, "ownership-placement", TenantPending, tidbCloudNativeSharedProvider)
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

func insertFreeQuotaTestTenant(t *testing.T, s *Store, tenantID string, status TenantStatus, provider string) {
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

func newFreeQuotaTestTenant(tenantID string) *Tenant {
	now := time.Now().UTC()
	return &Tenant{
		ID: tenantID, Status: TenantPending, Kind: TenantKindLive, Provider: tidbCloudNativeProvider,
		DBPasswordCipher: []byte{}, DBTLS: true, SchemaVersion: 1,
		CreatedAt: now, UpdatedAt: now,
	}
}
