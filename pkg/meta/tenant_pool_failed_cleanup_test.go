package meta

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestListFailedNativeTenantCleanupCandidatesUsesOrganizationEligibility(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	cutoff := now.Add(-30 * time.Minute)

	seedNativeCleanupBinding(t, s, "native-free", "org-native", tidbCloudNativeProvider, TenantFailed,
		"pool-native", TenantPoolBindingFree, now.Add(-4*time.Hour))
	seedNativeCleanupBinding(t, s, "native-direct", "org-native", tidbCloudNativeProvider, TenantFailed,
		"", TenantPoolBindingUsed, now.Add(-3*time.Hour))
	seedNativeCleanupBinding(t, s, "native-claimed", "org-native", tidbCloudNativeProvider, TenantFailed,
		"pool-native", TenantPoolBindingUsed, now.Add(-5*time.Hour))
	seedNativeCleanupBinding(t, s, "native-wrong-org", "org-other", tidbCloudNativeProvider, TenantFailed,
		"", TenantPoolBindingUsed, now.Add(-5*time.Hour))
	seedNativeCleanupBinding(t, s, "native-recent", "org-native", tidbCloudNativeProvider, TenantFailed,
		"", TenantPoolBindingUsed, now.Add(-5*time.Minute))
	seedNativeCleanupBinding(t, s, "native-wrong-provider", "org-native", "tidb_zero", TenantFailed,
		"", TenantPoolBindingUsed, now.Add(-5*time.Hour))
	seedNativeCleanupBinding(t, s, "native-active", "org-native", tidbCloudNativeProvider, TenantActive,
		"", TenantPoolBindingUsed, now.Add(-5*time.Hour))

	got, err := s.ListFailedNativeTenantCleanupCandidates(ctx, "org-native", cutoff, 0)
	if err != nil {
		t.Fatalf("ListFailedNativeTenantCleanupCandidates default limit: %v", err)
	}
	if ids := tenantBindingIDs(got); fmt.Sprint(ids) != "[native-free]" {
		t.Fatalf("default native candidates = %v, want oldest eligible tenant", ids)
	}

	got, err = s.ListFailedNativeTenantCleanupCandidates(ctx, "org-native", cutoff, 10)
	if err != nil {
		t.Fatalf("ListFailedNativeTenantCleanupCandidates: %v", err)
	}
	if ids := tenantBindingIDs(got); fmt.Sprint(ids) != "[native-free native-direct]" {
		t.Fatalf("native cleanup candidates = %v, want free pool and direct tenants only", ids)
	}
}

func TestListFailedSharedTenantCleanupCandidatesUsesMembershipOrPlacementOrganization(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	cutoff := now.Add(-30 * time.Minute)

	seedSharedCleanupMembership(t, s, "shared-free", "org-shared", tidbCloudNativeSharedProvider,
		TenantFailed, TenantPoolBindingFree, now.Add(-4*time.Hour))
	seedSharedCleanupPlacement(t, s, "shared-direct", "org-shared", tidbCloudNativeSharedProvider,
		TenantFailed, now.Add(-3*time.Hour))
	for i := 0; i < 9; i++ {
		seedSharedCleanupMembership(t, s, fmt.Sprintf("shared-extra-%02d", i), "org-shared",
			tidbCloudNativeSharedProvider, TenantFailed, TenantPoolBindingFree, now.Add(-2*time.Hour))
	}
	seedSharedCleanupMembership(t, s, "shared-claimed", "org-shared", tidbCloudNativeSharedProvider,
		TenantFailed, TenantPoolBindingUsed, now.Add(-5*time.Hour))
	seedSharedCleanupPlacementForExistingTenant(t, s, "shared-claimed", "org-shared")
	seedSharedCleanupMembership(t, s, "shared-wrong-membership-org", "org-other", tidbCloudNativeSharedProvider,
		TenantFailed, TenantPoolBindingFree, now.Add(-5*time.Hour))
	seedSharedCleanupPlacementForExistingTenant(t, s, "shared-wrong-membership-org", "org-shared")
	seedSharedCleanupPlacement(t, s, "shared-wrong-placement-org", "org-other", tidbCloudNativeSharedProvider,
		TenantFailed, now.Add(-5*time.Hour))
	seedSharedCleanupPlacement(t, s, "shared-recent", "org-shared", tidbCloudNativeSharedProvider,
		TenantFailed, now.Add(-5*time.Minute))
	seedSharedCleanupMembership(t, s, "shared-wrong-provider", "org-shared", "tidb_zero",
		TenantFailed, TenantPoolBindingFree, now.Add(-5*time.Hour))
	seedSharedCleanupMembership(t, s, "shared-active", "org-shared", tidbCloudNativeSharedProvider,
		TenantActive, TenantPoolBindingFree, now.Add(-5*time.Hour))

	got, err := s.ListFailedSharedTenantCleanupCandidates(ctx, "org-shared", cutoff, 0)
	if err != nil {
		t.Fatalf("ListFailedSharedTenantCleanupCandidates default limit: %v", err)
	}
	wantDefault := []string{
		"shared-free", "shared-direct", "shared-extra-00", "shared-extra-01", "shared-extra-02",
		"shared-extra-03", "shared-extra-04", "shared-extra-05", "shared-extra-06", "shared-extra-07",
	}
	if ids := cleanupTenantIDs(got); fmt.Sprint(ids) != fmt.Sprint(wantDefault) {
		t.Fatalf("default shared candidates = %v, want %v", ids, wantDefault)
	}

	got, err = s.ListFailedSharedTenantCleanupCandidates(ctx, "org-shared", cutoff, 20)
	if err != nil {
		t.Fatalf("ListFailedSharedTenantCleanupCandidates: %v", err)
	}
	wantAll := append(append([]string{}, wantDefault...), "shared-extra-08")
	if ids := cleanupTenantIDs(got); fmt.Sprint(ids) != fmt.Sprint(wantAll) {
		t.Fatalf("shared cleanup candidates = %v, want free-membership and direct-placement tenants %v", ids, wantAll)
	}
}

func TestFailedTenantCleanupCooldownRestartsAfterTenantUpdate(t *testing.T) {
	tests := []struct {
		name string
		seed func(*testing.T, *Store, string, time.Time)
		list func(context.Context, *Store, time.Time) (int, error)
	}{
		{
			name: "native",
			seed: func(t *testing.T, s *Store, tenantID string, updatedAt time.Time) {
				seedNativeCleanupBinding(t, s, tenantID, "org-cooldown", tidbCloudNativeProvider,
					TenantFailed, "", TenantPoolBindingUsed, updatedAt)
			},
			list: func(ctx context.Context, s *Store, cutoff time.Time) (int, error) {
				got, err := s.ListFailedNativeTenantCleanupCandidates(ctx, "org-cooldown", cutoff, 10)
				return len(got), err
			},
		},
		{
			name: "shared",
			seed: func(t *testing.T, s *Store, tenantID string, updatedAt time.Time) {
				seedSharedCleanupPlacement(t, s, tenantID, "org-cooldown", tidbCloudNativeSharedProvider,
					TenantFailed, updatedAt)
			},
			list: func(ctx context.Context, s *Store, cutoff time.Time) (int, error) {
				got, err := s.ListFailedSharedTenantCleanupCandidates(ctx, "org-cooldown", cutoff, 10)
				return len(got), err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newControlStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Millisecond)
			cutoff := now.Add(-30 * time.Minute)
			tenantID := tt.name + "-cooldown"
			tt.seed(t, s, tenantID, now.Add(-2*time.Hour))

			if count, err := tt.list(ctx, s, cutoff); err != nil || count != 1 {
				t.Fatalf("list before tenant update = %d, %v; want one eligible candidate", count, err)
			}
			if _, err := s.DB().ExecContext(ctx, `UPDATE tenants SET schema_version = schema_version + 1 WHERE id = ?`, tenantID); err != nil {
				t.Fatalf("unrelated tenant update: %v", err)
			}
			if count, err := tt.list(ctx, s, cutoff); err != nil || count != 0 {
				t.Fatalf("list after tenant update = %d, %v; want cooldown restarted", count, err)
			}
		})
	}
}

func TestMarkFailedTenantDeletingHasOneConcurrentWinnerForPoolAndDirectCandidates(t *testing.T) {
	tests := []struct {
		name string
		seed func(*testing.T, *Store, string, time.Time)
		mark func(context.Context, *Store, string, time.Time) (bool, error)
	}{
		{
			name: "native-free",
			seed: func(t *testing.T, s *Store, tenantID string, updatedAt time.Time) {
				seedNativeCleanupBinding(t, s, tenantID, "org-race", tidbCloudNativeProvider,
					TenantFailed, "pool-race", TenantPoolBindingFree, updatedAt)
			},
			mark: func(ctx context.Context, s *Store, tenantID string, cutoff time.Time) (bool, error) {
				return s.MarkFailedNativeTenantDeleting(ctx, tenantID, "org-race", cutoff)
			},
		},
		{
			name: "native-direct",
			seed: func(t *testing.T, s *Store, tenantID string, updatedAt time.Time) {
				seedNativeCleanupBinding(t, s, tenantID, "org-race", tidbCloudNativeProvider,
					TenantFailed, "", TenantPoolBindingUsed, updatedAt)
			},
			mark: func(ctx context.Context, s *Store, tenantID string, cutoff time.Time) (bool, error) {
				return s.MarkFailedNativeTenantDeleting(ctx, tenantID, "org-race", cutoff)
			},
		},
		{
			name: "shared-free",
			seed: func(t *testing.T, s *Store, tenantID string, updatedAt time.Time) {
				seedSharedCleanupMembership(t, s, tenantID, "org-race", tidbCloudNativeSharedProvider,
					TenantFailed, TenantPoolBindingFree, updatedAt)
			},
			mark: func(ctx context.Context, s *Store, tenantID string, cutoff time.Time) (bool, error) {
				return s.MarkFailedSharedTenantDeleting(ctx, tenantID, "org-race", cutoff)
			},
		},
		{
			name: "shared-direct",
			seed: func(t *testing.T, s *Store, tenantID string, updatedAt time.Time) {
				seedSharedCleanupPlacement(t, s, tenantID, "org-race", tidbCloudNativeSharedProvider,
					TenantFailed, updatedAt)
			},
			mark: func(ctx context.Context, s *Store, tenantID string, cutoff time.Time) (bool, error) {
				return s.MarkFailedSharedTenantDeleting(ctx, tenantID, "org-race", cutoff)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newControlStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Millisecond)
			cutoff := now.Add(-30 * time.Minute)
			tenantID := "race-" + tt.name
			tt.seed(t, s, tenantID, now.Add(-2*time.Hour))

			start := make(chan struct{})
			results := make(chan bool, 2)
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					won, err := tt.mark(ctx, s, tenantID, cutoff)
					results <- won
					errs <- err
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			close(errs)

			winners := 0
			for won := range results {
				if won {
					winners++
				}
			}
			for err := range errs {
				if err != nil {
					t.Fatalf("concurrent mark: %v", err)
				}
			}
			if winners != 1 {
				t.Fatalf("concurrent winners = %d, want exactly 1", winners)
			}
			tenant, err := s.GetTenant(ctx, tenantID)
			if err != nil || tenant.Status != TenantDeleting {
				t.Fatalf("tenant after concurrent mark = %+v, %v; want deleting", tenant, err)
			}
		})
	}
}

func TestMarkFailedTenantDeletingRefusesClaimedWrongOrganizationAndRecent(t *testing.T) {
	tests := []struct {
		name string
		seed func(*testing.T, *Store, string, string, time.Time)
		mark func(context.Context, *Store, string, time.Time) (bool, error)
	}{
		{
			name: "native-claimed",
			seed: func(t *testing.T, s *Store, tenantID, _ string, updatedAt time.Time) {
				seedNativeCleanupBinding(t, s, tenantID, "org-refuse", tidbCloudNativeProvider,
					TenantFailed, "pool-refuse", TenantPoolBindingUsed, updatedAt)
			},
			mark: markNativeCleanupForOrg("org-refuse"),
		},
		{
			name: "native-wrong-org",
			seed: func(t *testing.T, s *Store, tenantID, _ string, updatedAt time.Time) {
				seedNativeCleanupBinding(t, s, tenantID, "org-other", tidbCloudNativeProvider,
					TenantFailed, "", TenantPoolBindingUsed, updatedAt)
			},
			mark: markNativeCleanupForOrg("org-refuse"),
		},
		{
			name: "native-recent",
			seed: func(t *testing.T, s *Store, tenantID, _ string, updatedAt time.Time) {
				seedNativeCleanupBinding(t, s, tenantID, "org-refuse", tidbCloudNativeProvider,
					TenantFailed, "", TenantPoolBindingUsed, updatedAt)
			},
			mark: markNativeCleanupForOrg("org-refuse"),
		},
		{
			name: "shared-claimed",
			seed: func(t *testing.T, s *Store, tenantID, _ string, updatedAt time.Time) {
				seedSharedCleanupMembership(t, s, tenantID, "org-refuse", tidbCloudNativeSharedProvider,
					TenantFailed, TenantPoolBindingUsed, updatedAt)
			},
			mark: markSharedCleanupForOrg("org-refuse"),
		},
		{
			name: "shared-wrong-org",
			seed: func(t *testing.T, s *Store, tenantID, _ string, updatedAt time.Time) {
				seedSharedCleanupPlacement(t, s, tenantID, "org-other", tidbCloudNativeSharedProvider,
					TenantFailed, updatedAt)
			},
			mark: markSharedCleanupForOrg("org-refuse"),
		},
		{
			name: "shared-recent",
			seed: func(t *testing.T, s *Store, tenantID, _ string, updatedAt time.Time) {
				seedSharedCleanupPlacement(t, s, tenantID, "org-refuse", tidbCloudNativeSharedProvider,
					TenantFailed, updatedAt)
			},
			mark: markSharedCleanupForOrg("org-refuse"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newControlStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Millisecond)
			tenantID := "refuse-" + tt.name
			updatedAt := now.Add(-2 * time.Hour)
			if tt.name == "native-recent" || tt.name == "shared-recent" {
				updatedAt = now.Add(-5 * time.Minute)
			}
			tt.seed(t, s, tenantID, "org-refuse", updatedAt)
			won, err := tt.mark(ctx, s, tenantID, now.Add(-30*time.Minute))
			if err != nil {
				t.Fatalf("mark refused candidate: %v", err)
			}
			if won {
				t.Fatal("mark won for ineligible tenant")
			}
			tenant, err := s.GetTenant(ctx, tenantID)
			if err != nil || tenant.Status != TenantFailed {
				t.Fatalf("tenant after refused mark = %+v, %v; want failed", tenant, err)
			}
		})
	}
}

func markNativeCleanupForOrg(organizationID string) func(context.Context, *Store, string, time.Time) (bool, error) {
	return func(ctx context.Context, s *Store, tenantID string, cutoff time.Time) (bool, error) {
		return s.MarkFailedNativeTenantDeleting(ctx, tenantID, organizationID, cutoff)
	}
}

func markSharedCleanupForOrg(organizationID string) func(context.Context, *Store, string, time.Time) (bool, error) {
	return func(ctx context.Context, s *Store, tenantID string, cutoff time.Time) (bool, error) {
		return s.MarkFailedSharedTenantDeleting(ctx, tenantID, organizationID, cutoff)
	}
}

func seedNativeCleanupBinding(t *testing.T, s *Store, tenantID, organizationID, provider string, status TenantStatus, poolID string, poolStatus TenantPoolBindingStatus, updatedAt time.Time) {
	t.Helper()
	insertCleanupTenant(t, s, tenantID, provider, status, updatedAt)
	if err := s.UpsertTenantTiDBCloudOrgBinding(context.Background(), &TenantTiDBCloudOrgBinding{
		TenantID: tenantID, OrganizationID: organizationID, ClusterID: "cluster-" + tenantID,
		PoolID: poolID, PoolStatus: poolStatus, CreatedAt: updatedAt, UpdatedAt: updatedAt,
	}); err != nil {
		t.Fatalf("UpsertTenantTiDBCloudOrgBinding(%s): %v", tenantID, err)
	}
}

func seedSharedCleanupMembership(t *testing.T, s *Store, tenantID, organizationID, provider string, status TenantStatus, poolStatus TenantPoolBindingStatus, updatedAt time.Time) {
	t.Helper()
	insertCleanupTenant(t, s, tenantID, provider, status, updatedAt)
	if provider == tidbCloudNativeSharedProvider {
		if err := s.UpsertTenantPoolMembership(context.Background(), &TenantPoolMembership{
			TenantID: tenantID, TiDBCloudOrganizationID: organizationID, PoolID: "pool-" + organizationID,
			PoolStatus: poolStatus, CreatedAt: updatedAt, UpdatedAt: updatedAt,
		}); err != nil {
			t.Fatalf("UpsertTenantPoolMembership(%s): %v", tenantID, err)
		}
		return
	}
	if _, err := s.DB().ExecContext(context.Background(), `INSERT INTO tenant_pool_memberships
		(tenant_id, tidbcloud_organization_id, pool_id, pool_status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`, tenantID, organizationID, "pool-"+organizationID,
		poolStatus, updatedAt, updatedAt); err != nil {
		t.Fatalf("insert mismatched tenant pool membership %s: %v", tenantID, err)
	}
}

func seedSharedCleanupPlacement(t *testing.T, s *Store, tenantID, organizationID, provider string, status TenantStatus, updatedAt time.Time) {
	t.Helper()
	insertCleanupTenant(t, s, tenantID, provider, status, updatedAt)
	seedSharedCleanupPlacementForExistingTenant(t, s, tenantID, organizationID)
}

func seedSharedCleanupPlacementForExistingTenant(t *testing.T, s *Store, tenantID, organizationID string) {
	t.Helper()
	dbID, err := s.RegisterSharedDB(context.Background(), &SharedDB{
		TiDBCloudOrganizationID: organizationID, Host: "host-" + tenantID, Port: 4000,
		User: "root", PasswordCipher: []byte("cipher"), Name: "db_" + tenantID,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB(%s): %v", tenantID, err)
	}
	fsID, err := s.ResolveFsID(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("ResolveFsID(%s): %v", tenantID, err)
	}
	if err := s.UpsertTenantPlacement(context.Background(), &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement(%s): %v", tenantID, err)
	}
}

func insertCleanupTenant(t *testing.T, s *Store, tenantID, provider string, status TenantStatus, updatedAt time.Time) {
	t.Helper()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID: tenantID, Status: status, Kind: TenantKindLive, DBHost: "db.example.com", DBPort: 4000,
		DBUser: "root", DBPasswordCipher: []byte("cipher"), DBName: "tidbcloud_fs", DBTLS: true,
		Provider: provider, ClusterID: "cluster-" + tenantID, SchemaVersion: 1,
		CreatedAt: updatedAt, UpdatedAt: updatedAt,
	}); err != nil {
		t.Fatalf("InsertTenant(%s): %v", tenantID, err)
	}
}

func tenantBindingIDs(rows []TenantWithTiDBCloudOrgBinding) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.Tenant.ID)
	}
	return ids
}

func cleanupTenantIDs(rows []Tenant) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	return ids
}
