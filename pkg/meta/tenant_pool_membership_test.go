package meta

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTenantPoolMembershipRoundTripAndClaimCAS(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	insertTenantForPoolMembershipTest(t, s, "shared-member-1", TenantActive, now)

	if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID:                "shared-member-1",
		TiDBCloudOrganizationID: "org-1",
		PoolID:                  "pool-1",
		PoolStatus:              TenantPoolBindingFree,
		CreatedAt:               now,
		UpdatedAt:               now,
	}); err != nil {
		t.Fatalf("UpsertTenantPoolMembership: %v", err)
	}

	got, err := s.GetTenantPoolMembership(ctx, "shared-member-1")
	if err != nil {
		t.Fatalf("GetTenantPoolMembership: %v", err)
	}
	if got.TenantID != "shared-member-1" || got.TiDBCloudOrganizationID != "org-1" || got.PoolID != "pool-1" || got.PoolStatus != TenantPoolBindingFree || got.UsedAt != nil {
		t.Fatalf("membership = %+v", got)
	}
	conflict := *got
	conflict.TiDBCloudOrganizationID = "org-2"
	if err := s.UpsertTenantPoolMembership(ctx, &conflict); err == nil {
		t.Fatal("conflicting organization update succeeded")
	}
	got, err = s.GetTenantPoolMembership(ctx, "shared-member-1")
	if err != nil {
		t.Fatalf("GetTenantPoolMembership after conflict: %v", err)
	}
	if got.TiDBCloudOrganizationID != "org-1" {
		t.Fatalf("organization after conflict = %q, want org-1", got.TiDBCloudOrganizationID)
	}

	claimedAt := now.Add(time.Second)
	claimed, err := s.ClaimTenantPoolMembership(ctx, "shared-member-1", "pool-1", claimedAt)
	if err != nil {
		t.Fatalf("ClaimTenantPoolMembership: %v", err)
	}
	if !claimed {
		t.Fatal("first claim lost CAS")
	}
	claimed, err = s.ClaimTenantPoolMembership(ctx, "shared-member-1", "pool-1", claimedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("second ClaimTenantPoolMembership: %v", err)
	}
	if claimed {
		t.Fatal("second claim won CAS")
	}
	got, err = s.GetTenantPoolMembership(ctx, "shared-member-1")
	if err != nil {
		t.Fatalf("GetTenantPoolMembership after claim: %v", err)
	}
	if got.PoolStatus != TenantPoolBindingUsed || got.UsedAt == nil || !got.UsedAt.Equal(claimedAt) {
		t.Fatalf("claimed membership = %+v", got)
	}
}

func TestOldestFreeTenantPoolMembershipRequiresActiveTenant(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	insertTenantForPoolMembershipTest(t, s, "shared-member-provisioning", TenantProvisioning, now)
	insertTenantForPoolMembershipTest(t, s, "shared-member-active", TenantActive, now.Add(time.Second))
	for i, tenantID := range []string{"shared-member-provisioning", "shared-member-active"} {
		createdAt := now.Add(time.Duration(i) * time.Second)
		if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
			TenantID: tenantID, TiDBCloudOrganizationID: "org-1", PoolID: "pool-1",
			PoolStatus: TenantPoolBindingFree, CreatedAt: createdAt, UpdatedAt: createdAt,
		}); err != nil {
			t.Fatalf("UpsertTenantPoolMembership(%s): %v", tenantID, err)
		}
	}

	got, err := s.GetOldestFreeTenantPoolMembership(ctx, "pool-1")
	if err != nil {
		t.Fatalf("GetOldestFreeTenantPoolMembership: %v", err)
	}
	if got.Tenant.ID != "shared-member-active" {
		t.Fatalf("oldest eligible tenant = %q, want shared-member-active", got.Tenant.ID)
	}
}

func TestTenantPoolMembershipRejectsNativeTenant(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.InsertTenant(ctx, &Tenant{
		ID: "native-member-rejected", Status: TenantActive, Provider: tidbCloudNativeProvider,
		DBPasswordCipher: []byte{}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID: "native-member-rejected", TiDBCloudOrganizationID: "org-1", PoolID: "pool-1",
		PoolStatus: TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now,
	})
	if err == nil {
		t.Fatal("native tenant was inserted into shared membership table")
	}
	if _, getErr := s.GetTenantPoolMembership(ctx, "native-member-rejected"); !errors.Is(getErr, ErrNotFound) {
		t.Fatalf("membership after rejected write = %v, want ErrNotFound", getErr)
	}
}

func TestUpdateTenantPoolMembershipOrganizationFillsOnce(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	insertTenantForPoolMembershipTest(t, s, "shared-member-org-fill", TenantProvisioning, now)
	if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID: "shared-member-org-fill", PoolID: "pool-org-fill",
		PoolStatus: TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTenantPoolMembership: %v", err)
	}

	if err := s.UpdateTenantPoolMembershipOrganization(ctx, "pool-org-fill", "org-1"); err != nil {
		t.Fatalf("initial organization fill: %v", err)
	}
	got, err := s.GetTenantPoolMembership(ctx, "shared-member-org-fill")
	if err != nil {
		t.Fatalf("GetTenantPoolMembership after fill: %v", err)
	}
	if got.TiDBCloudOrganizationID != "org-1" {
		t.Fatalf("organization after fill = %q, want org-1", got.TiDBCloudOrganizationID)
	}

	if err := s.UpdateTenantPoolMembershipOrganization(ctx, "pool-org-fill", "org-2"); err == nil {
		t.Fatal("conflicting organization fill succeeded")
	}
	got, err = s.GetTenantPoolMembership(ctx, "shared-member-org-fill")
	if err != nil {
		t.Fatalf("GetTenantPoolMembership after conflict: %v", err)
	}
	if got.TiDBCloudOrganizationID != "org-1" {
		t.Fatalf("organization after conflict = %q, want org-1", got.TiDBCloudOrganizationID)
	}
}

func TestDeleteTenantPoolAndDetachUsedMembersHandlesBothProviders(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.CreateTenantPool(ctx, &TenantPool{
		PoolID: "pool-delete-mixed", OrganizationID: "org-delete-mixed", Size: 2,
		Status: TenantPoolDeleting, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTenantPool: %v", err)
	}
	if err := s.InsertTenant(ctx, &Tenant{
		ID: "native-used", Status: TenantActive, Provider: tidbCloudNativeProvider,
		DBPasswordCipher: []byte{}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("InsertTenant native-used: %v", err)
	}
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "native-used", OrganizationID: "org-delete-mixed", ClusterID: "cluster-native-used",
		PoolID: "pool-delete-mixed", PoolStatus: TenantPoolBindingUsed, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTenantTiDBCloudOrgBinding: %v", err)
	}
	insertTenantForPoolMembershipTest(t, s, "shared-used", TenantActive, now)
	if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID: "shared-used", TiDBCloudOrganizationID: "org-delete-mixed",
		PoolID: "pool-delete-mixed", PoolStatus: TenantPoolBindingUsed, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTenantPoolMembership: %v", err)
	}
	insertTenantForPoolMembershipTest(t, s, "shared-free", TenantActive, now)
	if err := s.UpsertTenantPoolMembership(ctx, &TenantPoolMembership{
		TenantID: "shared-free", TiDBCloudOrganizationID: "org-delete-mixed",
		PoolID: "pool-delete-mixed", PoolStatus: TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTenantPoolMembership shared-free: %v", err)
	}

	if err := s.DeleteTenantPoolAndDetachUsedMembers(ctx, "pool-delete-mixed"); err == nil {
		t.Fatal("pool deletion succeeded while a free member remained")
	}
	pool, err := s.GetTenantPoolByID(ctx, "pool-delete-mixed")
	if err != nil || pool.PoolID != "pool-delete-mixed" {
		t.Fatalf("pool after rejected delete = %+v, %v", pool, err)
	}
	nativeBinding, err := s.GetTenantTiDBCloudOrgBinding(ctx, "native-used")
	if err != nil {
		t.Fatalf("GetTenantTiDBCloudOrgBinding after rejected delete: %v", err)
	}
	if nativeBinding.PoolID != "pool-delete-mixed" {
		t.Fatalf("native binding detached by rejected delete: %+v", nativeBinding)
	}
	if _, err := s.GetTenantPoolMembership(ctx, "shared-used"); err != nil {
		t.Fatalf("shared used membership removed by rejected delete: %v", err)
	}
	if err := s.DeleteTenantPoolMembership(ctx, "shared-free"); err != nil {
		t.Fatalf("DeleteTenantPoolMembership shared-free: %v", err)
	}

	if err := s.DeleteTenantPoolAndDetachUsedMembers(ctx, "pool-delete-mixed"); err != nil {
		t.Fatalf("DeleteTenantPoolAndDetachUsedMembers: %v", err)
	}
	if _, err := s.GetTenantPoolByID(ctx, "pool-delete-mixed"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTenantPoolByID after delete = %v, want ErrNotFound", err)
	}
	nativeBinding, err = s.GetTenantTiDBCloudOrgBinding(ctx, "native-used")
	if err != nil {
		t.Fatalf("GetTenantTiDBCloudOrgBinding: %v", err)
	}
	if nativeBinding.PoolID != "" || nativeBinding.PoolStatus != TenantPoolBindingUsed {
		t.Fatalf("native binding after detach = %+v", nativeBinding)
	}
	if _, err := s.GetTenantPoolMembership(ctx, "shared-used"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("shared membership after detach = %v, want ErrNotFound", err)
	}
}

func insertTenantForPoolMembershipTest(t *testing.T, s *Store, tenantID string, status TenantStatus, now time.Time) {
	t.Helper()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID: tenantID, Status: status, DBHost: "", DBPort: 0, DBUser: "", DBPasswordCipher: []byte{},
		DBName: "", Provider: "tidb_cloud_native_shared", SchemaVersion: 0, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("InsertTenant(%s): %v", tenantID, err)
	}
}
