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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newControlStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Millisecond)
			tenantID := "refuse-" + tt.name
			updatedAt := now.Add(-2 * time.Hour)
			if tt.name == "native-recent" {
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
