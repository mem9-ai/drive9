package meta

import (
	"context"
	"errors"
	"sync"
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
