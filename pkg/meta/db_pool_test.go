package meta

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSharedDBCapacityBounds(t *testing.T) {
	tests := []struct {
		name       string
		softCap    int
		ratio      float64
		wantHard   int
		wantReopen int
		wantErr    bool
	}{
		{name: "default", softCap: 100, ratio: 1.2, wantHard: 120, wantReopen: 80},
		{name: "ceil fractional", softCap: 101, ratio: 1.1, wantHard: 112, wantReopen: 80},
		{name: "ratio below one", softCap: 100, ratio: 0.9, wantErr: true},
		{name: "nan ratio", softCap: 100, ratio: math.NaN(), wantErr: true},
		{name: "infinite ratio", softCap: 100, ratio: math.Inf(1), wantErr: true},
		{name: "non-positive soft cap", softCap: 0, ratio: 1.2, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hard, err := SharedDBHardCap(tt.softCap, tt.ratio)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("SharedDBHardCap(%d, %v) error = nil, want error", tt.softCap, tt.ratio)
				}
				return
			}
			if err != nil {
				t.Fatalf("SharedDBHardCap(%d, %v): %v", tt.softCap, tt.ratio, err)
			}
			if hard != tt.wantHard {
				t.Fatalf("hard cap = %d, want %d", hard, tt.wantHard)
			}
			reopen, reopenErr := SharedDBReopenThresholdForRatio(tt.softCap, 0.8)
			if reopenErr != nil {
				t.Fatalf("reopen threshold: %v", reopenErr)
			}
			if reopen != tt.wantReopen {
				t.Fatalf("reopen threshold = %d, want %d", reopen, tt.wantReopen)
			}
		})
	}
}

func TestSharedDBReopenThresholdForRatio(t *testing.T) {
	got, err := SharedDBReopenThresholdForRatio(10, 0.65)
	if err != nil {
		t.Fatalf("SharedDBReopenThresholdForRatio: %v", err)
	}
	if got != 6 {
		t.Fatalf("reopen threshold = %d, want 6", got)
	}
	for _, ratio := range []float64{0, 1, math.NaN(), math.Inf(1)} {
		if _, err := SharedDBReopenThresholdForRatio(10, ratio); err == nil {
			t.Fatalf("ratio %v error = nil, want error", ratio)
		}
	}
}

func registerTestSharedDB(t *testing.T, s *Store, orgID, host, name string) int64 {
	t.Helper()
	id, err := s.RegisterSharedDB(context.Background(), &SharedDB{
		TiDBCloudOrganizationID: orgID,
		Host:                    host,
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher-" + name),
		Name:                    name,
		TLSMode:                 "skip-verify",
		MaxTenants:              100,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB %s/%s: %v", orgID, name, err)
	}
	if id <= 0 {
		t.Fatalf("RegisterSharedDB %s/%s returned db_id = %d, want positive", orgID, name, id)
	}
	return id
}

func TestRegisterSharedDBRoundTrip(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	id := registerTestSharedDB(t, s, "org-a", "shared-a.example.com", "shared_db_a")

	got, err := s.GetSharedDB(ctx, id)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if got.ID != id {
		t.Fatalf("ID = %d, want %d", got.ID, id)
	}
	if _, err := uuid.Parse(got.UUID); err != nil {
		t.Fatalf("UUID = %q: %v", got.UUID, err)
	}
	if got.TiDBCloudOrganizationID != "org-a" {
		t.Fatalf("TiDBCloudOrganizationID = %q, want org-a", got.TiDBCloudOrganizationID)
	}
	if got.Role != SharedDBRoleShared {
		t.Fatalf("Role = %q, want %q", got.Role, SharedDBRoleShared)
	}
	if got.Host != "shared-a.example.com" {
		t.Fatalf("Host = %q, want shared-a.example.com", got.Host)
	}
	if got.Port != 4000 {
		t.Fatalf("Port = %d, want 4000", got.Port)
	}
	if got.User != "root" {
		t.Fatalf("User = %q, want root", got.User)
	}
	if string(got.PasswordCipher) != "cipher-shared_db_a" {
		t.Fatalf("PasswordCipher = %q, want cipher-shared_db_a", got.PasswordCipher)
	}
	if got.Name != "shared_db_a" {
		t.Fatalf("Name = %q, want shared_db_a", got.Name)
	}
	if got.TLSMode != "skip-verify" {
		t.Fatalf("TLSMode = %q, want skip-verify", got.TLSMode)
	}
	if got.MaxTenants != 100 {
		t.Fatalf("MaxTenants = %d, want 100", got.MaxTenants)
	}
	if got.TenantCount != 0 {
		t.Fatalf("TenantCount = %d, want 0", got.TenantCount)
	}
	if got.Status != sharedDBStatusActive {
		t.Fatalf("Status = %q, want active", got.Status)
	}

	if _, err := s.GetSharedDB(ctx, id+100000); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSharedDB unknown id error = %v, want ErrNotFound", err)
	}
}

func TestCreateManagedSharedDBPoolPersistsDurableProvisioningPlan(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	provisioningKey := make([]byte, 32)
	for i := range provisioningKey {
		provisioningKey[i] = byte(i + 1)
	}

	id, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-managed",
		ProvisioningKey:         provisioningKey,
		CloudProvider:           "aws",
		Region:                  "us-east-1",
		MaxTenants:              100,
		SpendingLimit:           &spendingLimit,
		PasswordCipher:          []byte("durable-root-cipher"),
		Name:                    "tidbcloud_fs",
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if id <= 0 {
		t.Fatalf("managed db pool id = %d, want positive", id)
	}

	got, err := s.GetSharedDB(ctx, id)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if got.ID != id || got.TiDBCloudOrganizationID != "org-managed" || got.CloudProvider != "aws" || got.Region != "us-east-1" {
		t.Fatalf("managed db pool identity = %+v", got)
	}
	if _, err := uuid.Parse(got.UUID); err != nil {
		t.Fatalf("managed db pool UUID = %q: %v", got.UUID, err)
	}
	if got.Status != SharedDBStatusProvisioning || got.MaxTenants != 100 || got.SpendingLimit == nil || *got.SpendingLimit != spendingLimit {
		t.Fatalf("managed db pool policy = %+v", got)
	}
	if got.Host != "" || got.Port != 0 || got.User != "" || got.Name != "tidbcloud_fs" || string(got.PasswordCipher) != "durable-root-cipher" {
		t.Fatalf("provisional pool unexpectedly has connection metadata: %+v", got)
	}
	if string(got.ProvisioningKey) != string(provisioningKey) {
		t.Fatalf("provisioning key = %x, want %x", got.ProvisioningKey, provisioningKey)
	}
}

func TestCreateManagedSharedDBPoolRejectsUnboundedOrInvalidPolicy(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	valid := func() *SharedDB {
		spendingLimit := MaxTiDBCloudSpendingLimit
		return &SharedDB{
			TiDBCloudOrganizationID: "org-managed", ProvisioningKey: make([]byte, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
		}
	}
	tests := map[string]func(*SharedDB){
		"zero capacity":       func(in *SharedDB) { in.MaxTenants = 0 },
		"missing fingerprint": func(in *SharedDB) { in.ProvisioningKey = nil },
		"missing cloud":       func(in *SharedDB) { in.CloudProvider = "" },
		"missing region":      func(in *SharedDB) { in.Region = "" },
		"missing spending":    func(in *SharedDB) { in.SpendingLimit = nil },
		"physical limit below fixed maximum": func(in *SharedDB) {
			value := MaxTiDBCloudSpendingLimit - 1
			in.SpendingLimit = &value
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			in := valid()
			mutate(in)
			if _, err := s.CreateManagedSharedDBPool(ctx, in); err == nil {
				t.Fatal("CreateManagedSharedDBPool succeeded, want validation error")
			}
		})
	}
}

func TestMarkSharedDBPoolFailedRejectsPoolWithTenants(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	dbID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-failed-with-tenants", ProvisioningKey: bytes.Repeat([]byte{7}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 10, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET tenant_count = 1 WHERE db_id = ?`, dbID); err != nil {
		t.Fatalf("seed tenant count: %v", err)
	}

	if err := s.MarkSharedDBPoolFailed(ctx, dbID); err == nil {
		t.Fatal("MarkSharedDBPoolFailed succeeded for a pool with tenants")
	}
	got, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if got.Status != SharedDBStatusProvisioning {
		t.Fatalf("status = %q, want provisioning", got.Status)
	}
}

func TestRegisterSharedDBUpsertKeepsIDAndTenantCount(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	first := registerTestSharedDB(t, s, "org-a", "shared-a.example.com", "shared_db_a")
	firstRecord, err := s.GetSharedDB(ctx, first)
	if err != nil {
		t.Fatalf("GetSharedDB before upsert: %v", err)
	}
	if err := s.IncrSharedDBTenantCount(ctx, first, 3); err != nil {
		t.Fatalf("IncrSharedDBTenantCount: %v", err)
	}

	second, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-a",
		Host:                    "shared-a.example.com",
		Port:                    5000,
		User:                    "app",
		PasswordCipher:          []byte("rotated-cipher"),
		Name:                    "shared_db_a",
		TLSMode:                 "true",
		MaxTenants:              200,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB upsert: %v", err)
	}
	if second != first {
		t.Fatalf("upsert allocated new db_id %d, want existing %d", second, first)
	}

	got, err := s.GetSharedDB(ctx, first)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if got.Port != 5000 || got.User != "app" || string(got.PasswordCipher) != "rotated-cipher" ||
		got.MaxTenants != 200 || got.TLSMode != "true" {
		t.Fatalf("upsert did not refresh connection fields: %+v", got)
	}
	if got.TenantCount != 3 {
		t.Fatalf("TenantCount = %d, want preserved 3", got.TenantCount)
	}
	if got.UUID != firstRecord.UUID {
		t.Fatalf("UUID = %q after upsert, want preserved %q", got.UUID, firstRecord.UUID)
	}
}

func TestRegisterSharedDBValidation(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	valid := func() *SharedDB {
		return &SharedDB{
			TiDBCloudOrganizationID: "org-a",
			Host:                    "h",
			Port:                    4000,
			User:                    "u",
			PasswordCipher:          []byte("c"),
			Name:                    "n",
		}
	}
	cases := map[string]func(*SharedDB){
		"missing host":      func(d *SharedDB) { d.Host = "" },
		"non-positive port": func(d *SharedDB) { d.Port = 0 },
		"missing user":      func(d *SharedDB) { d.User = "" },
		"missing password":  func(d *SharedDB) { d.PasswordCipher = nil },
		"missing name":      func(d *SharedDB) { d.Name = "" },
		"negative max":      func(d *SharedDB) { d.MaxTenants = -1 },
		"reserved role":     func(d *SharedDB) { d.Role = "dedicated" },
	}
	for name, mutate := range cases {
		in := valid()
		mutate(in)
		if _, err := s.RegisterSharedDB(ctx, in); err == nil {
			t.Fatalf("%s: expected error, got nil", name)
		}
	}
}

func TestFindSharedDBForOrgExactThenWildcard(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	orgID := registerTestSharedDB(t, s, "org-exact", "shared-exact.example.com", "shared_db_exact")
	wildID := registerTestSharedDB(t, s, SharedDBOrgWildcard, "shared-wild.example.com", "shared_db_wild")

	got, err := s.FindSharedDBForOrg(ctx, "org-exact")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg exact: %v", err)
	}
	if got.ID != orgID {
		t.Fatalf("exact match db_id = %d, want %d", got.ID, orgID)
	}

	got, err = s.FindSharedDBForOrg(ctx, "org-other")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg fallback: %v", err)
	}
	if got.ID != wildID {
		t.Fatalf("wildcard fallback db_id = %d, want %d", got.ID, wildID)
	}

	got, err = s.FindSharedDBForOrg(ctx, "")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg empty org: %v", err)
	}
	if got.ID != wildID {
		t.Fatalf("empty org db_id = %d, want wildcard %d", got.ID, wildID)
	}
}

func TestFindSharedDBForOrgNotFound(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	registerTestSharedDB(t, s, "org-exact", "shared-exact.example.com", "shared_db_exact")

	if _, err := s.FindSharedDBForOrg(ctx, "org-missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindSharedDBForOrg miss error = %v, want ErrNotFound", err)
	}
	if _, err := s.FindSharedDBForOrg(ctx, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindSharedDBForOrg empty org error = %v, want ErrNotFound", err)
	}
}

func TestListSharedDBs(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	first := registerTestSharedDB(t, s, "org-a", "shared-a.example.com", "shared_db_a")
	second := registerTestSharedDB(t, s, SharedDBOrgWildcard, "shared-wild.example.com", "shared_db_wild")
	// A non-active row must not appear in the list.
	if _, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-b",
		Host:                    "shared-draining.example.com",
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher"),
		Name:                    "shared_db_draining",
		Status:                  "draining",
	}); err != nil {
		t.Fatalf("RegisterSharedDB draining: %v", err)
	}

	list, err := s.ListSharedDBs(ctx)
	if err != nil {
		t.Fatalf("ListSharedDBs: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListSharedDBs returned %d rows, want 2", len(list))
	}
	if list[0].ID != first || list[1].ID != second {
		t.Fatalf("ListSharedDBs order = [%d %d], want [%d %d]",
			list[0].ID, list[1].ID, first, second)
	}
}

func TestIncrSharedDBTenantCountClampsAtZero(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	id := registerTestSharedDB(t, s, "org-a", "shared-a.example.com", "shared_db_a")

	wantCount := func(want int) {
		t.Helper()
		got, err := s.GetSharedDB(ctx, id)
		if err != nil {
			t.Fatalf("GetSharedDB: %v", err)
		}
		if got.TenantCount != want {
			t.Fatalf("TenantCount = %d, want %d", got.TenantCount, want)
		}
	}

	if err := s.IncrSharedDBTenantCount(ctx, id, 5); err != nil {
		t.Fatalf("IncrSharedDBTenantCount +5: %v", err)
	}
	wantCount(5)
	if err := s.IncrSharedDBTenantCount(ctx, id, -2); err != nil {
		t.Fatalf("IncrSharedDBTenantCount -2: %v", err)
	}
	wantCount(3)
	if err := s.IncrSharedDBTenantCount(ctx, id, -10); err != nil {
		t.Fatalf("IncrSharedDBTenantCount -10: %v", err)
	}
	wantCount(0)

	if err := s.IncrSharedDBTenantCount(ctx, id+100000, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("IncrSharedDBTenantCount unknown db error = %v, want ErrNotFound", err)
	}
}

func TestTenantPlacementUpsertGetDelete(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	p := &TenantPlacement{
		FsID:        42,
		DbID:        7,
		Placement:   PlacementShared,
		SchemaShape: SchemaShapeShared,
	}
	if err := s.UpsertTenantPlacement(ctx, p); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}

	got, err := s.GetTenantPlacement(ctx, 42)
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	if got.FsID != 42 || got.DbID != 7 || got.Placement != PlacementShared ||
		got.SchemaShape != SchemaShapeShared {
		t.Fatalf("placement mismatch: %+v", got)
	}
	if got.Status != sharedDBStatusActive {
		t.Fatalf("Status = %q, want active", got.Status)
	}
	if got.TargetDbID != nil {
		t.Fatalf("TargetDbID = %v, want nil", *got.TargetDbID)
	}
	if got.Epoch != 1 {
		t.Fatalf("Epoch = %d, want 1", got.Epoch)
	}

	// Re-upsert replaces the mutable fields; the epoch stays at 1.
	target := int64(9)
	if err := s.UpsertTenantPlacement(ctx, &TenantPlacement{
		FsID:        42,
		DbID:        8,
		Placement:   PlacementDedicated,
		SchemaShape: SchemaShapeStandalone,
		TargetDbID:  &target,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement replace: %v", err)
	}
	got, err = s.GetTenantPlacement(ctx, 42)
	if err != nil {
		t.Fatalf("GetTenantPlacement after replace: %v", err)
	}
	if got.DbID != 8 || got.Placement != PlacementDedicated || got.SchemaShape != SchemaShapeStandalone {
		t.Fatalf("replaced placement mismatch: %+v", got)
	}
	if got.TargetDbID == nil || *got.TargetDbID != 9 {
		t.Fatalf("TargetDbID = %v, want 9", got.TargetDbID)
	}
	if got.Epoch != 1 {
		t.Fatalf("Epoch after replace = %d, want 1", got.Epoch)
	}

	if err := s.DeleteTenantPlacement(ctx, 42); err != nil {
		t.Fatalf("DeleteTenantPlacement: %v", err)
	}
	if _, err := s.GetTenantPlacement(ctx, 42); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTenantPlacement after delete error = %v, want ErrNotFound", err)
	}
	// Delete is idempotent.
	if err := s.DeleteTenantPlacement(ctx, 42); err != nil {
		t.Fatalf("DeleteTenantPlacement again: %v", err)
	}
}

func TestTenantPlacementValidation(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	valid := func() *TenantPlacement {
		return &TenantPlacement{
			FsID:        1,
			DbID:        1,
			Placement:   PlacementShared,
			SchemaShape: SchemaShapeShared,
		}
	}
	cases := map[string]func(*TenantPlacement){
		"non-positive fs_id":   func(p *TenantPlacement) { p.FsID = 0 },
		"non-positive db_id":   func(p *TenantPlacement) { p.DbID = 0 },
		"unknown placement":    func(p *TenantPlacement) { p.Placement = "bogus" },
		"unknown schema shape": func(p *TenantPlacement) { p.SchemaShape = "bogus" },
		"unknown status":       func(p *TenantPlacement) { p.Status = "bogus" },
	}
	for name, mutate := range cases {
		p := valid()
		mutate(p)
		if err := s.UpsertTenantPlacement(ctx, p); err == nil {
			t.Fatalf("%s: expected error, got nil", name)
		}
	}
}

func TestFindSharedDBForOrgSkipsFullPools(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	// The exact-org pool is capped at 1 tenant; once full, selection must
	// fall through to the wildcard pool rather than overfill it.
	fullID := registerTestSharedDB(t, s, "org-capped", "shared-capped.example.com", "shared_db_capped")
	if _, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-capped",
		Host:                    "shared-capped.example.com",
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher"),
		Name:                    "shared_db_capped",
		MaxTenants:              1,
	}); err != nil {
		t.Fatalf("cap pool at 1: %v", err)
	}
	wildID := registerTestSharedDB(t, s, SharedDBOrgWildcard, "shared-wild.example.com", "shared_db_wild")

	got, err := s.FindSharedDBForOrg(ctx, "org-capped")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg before full: %v", err)
	}
	if got.ID != fullID {
		t.Fatalf("before full db_id = %d, want exact pool %d", got.ID, fullID)
	}

	if err := s.IncrSharedDBTenantCount(ctx, fullID, 1); err != nil {
		t.Fatalf("fill pool: %v", err)
	}
	got, err = s.FindSharedDBForOrg(ctx, "org-capped")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg after full: %v", err)
	}
	if got.ID != wildID {
		t.Fatalf("after full db_id = %d, want wildcard %d", got.ID, wildID)
	}

	// With no wildcard fallback, a full pool reads as not found.
	if _, err := s.db.Exec(`DELETE FROM db_pool WHERE db_id = ?`, wildID); err != nil {
		t.Fatalf("remove wildcard: %v", err)
	}
	if _, err := s.FindSharedDBForOrg(ctx, "org-capped"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("full pool without fallback error = %v, want ErrNotFound", err)
	}
}

func TestGetTenantPlacementNotFound(t *testing.T) {
	s := newControlStore(t)
	if _, err := s.GetTenantPlacement(context.Background(), 123456); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTenantPlacement error = %v, want ErrNotFound", err)
	}
}

func seedPendingTenant(t *testing.T, s *Store, tenantID string) {
	t.Helper()
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID: tenantID, Status: TenantPending, Provider: "tidb_cloud_native",
		DBPasswordCipher: []byte{}, SchemaVersion: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
}

func testOwnerKey(tenantID string) *APIKey {
	now := time.Now().UTC()
	return &APIKey{
		ID: "key-" + tenantID, TenantID: tenantID, KeyName: "default",
		JWTCiphertext: []byte("cipher-" + tenantID), JWTHash: "hash-" + tenantID,
		TokenVersion: 1, Status: APIKeyActive, ScopeKind: APIKeyScopeKindOwner,
		IssuedAt: now, CreatedAt: now, UpdatedAt: now,
	}
}

func completeTestSharedTenant(t *testing.T, s *Store, dbID int64, tenantID string, emergencyHardCap int) int64 {
	t.Helper()
	ctx := context.Background()
	seedPendingTenant(t, s, tenantID)
	fsID, err := s.EnsureFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID %s: %v", tenantID, err)
	}
	virtualLimit := int64(100)
	if err := s.SetQuotaConfigPatch(ctx, tenantID, QuotaConfigPatch{TiDBCloudSpendingLimit: &virtualLimit}); err != nil {
		t.Fatalf("SetQuotaConfigPatch %s: %v", tenantID, err)
	}
	placement := &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}
	if emergencyHardCap > 0 {
		err = s.CompleteSharedTenantProvisionEmergency(ctx, tenantID, "tidb_cloud_native_shared", placement, testOwnerKey(tenantID), emergencyHardCap)
	} else {
		err = s.CompleteSharedTenantProvision(ctx, tenantID, "tidb_cloud_native_shared", placement, testOwnerKey(tenantID))
	}
	if err != nil {
		t.Fatalf("complete shared tenant %s: %v", tenantID, err)
	}
	return fsID
}

func TestCompleteSharedTenantProvisionSoftHardCapacityAndHysteresis(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	dbID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-hysteresis", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 2, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET status = ?, cluster_id = 'cluster-hysteresis', db_host = 'h', db_port = 4000,
		db_user = 'u', db_password = 'c', db_name = 'hysteresis_db' WHERE db_id = ?`, SharedDBStatusActive, dbID); err != nil {
		t.Fatalf("activate managed pool: %v", err)
	}
	fs1 := completeTestSharedTenant(t, s, dbID, "tenant-hysteresis-1", 0)
	fs2 := completeTestSharedTenant(t, s, dbID, "tenant-hysteresis-2", 0)
	db, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB at soft cap: %v", err)
	}
	if db.TenantCount != 2 || !db.SoftCapReached {
		t.Fatalf("pool at soft cap = count %d latch %v, want 2/true", db.TenantCount, db.SoftCapReached)
	}

	seedPendingTenant(t, s, "tenant-hysteresis-normal-rejected")
	fsRejected, _ := s.EnsureFsID(ctx, "tenant-hysteresis-normal-rejected")
	err = s.CompleteSharedTenantProvision(ctx, "tenant-hysteresis-normal-rejected", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsRejected, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-hysteresis-normal-rejected"))
	if !errors.Is(err, ErrSharedDBCapacityExhausted) {
		t.Fatalf("normal over soft cap error = %v, want ErrSharedDBCapacityExhausted", err)
	}

	fs3 := completeTestSharedTenant(t, s, dbID, "tenant-hysteresis-3", 3)
	db, _ = s.GetSharedDB(ctx, dbID)
	if db.TenantCount != 3 || !db.SoftCapReached {
		t.Fatalf("pool at hard cap = count %d latch %v, want 3/true", db.TenantCount, db.SoftCapReached)
	}

	seedPendingTenant(t, s, "tenant-hysteresis-hard-rejected")
	fsHardRejected, _ := s.EnsureFsID(ctx, "tenant-hysteresis-hard-rejected")
	err = s.CompleteSharedTenantProvisionEmergency(ctx, "tenant-hysteresis-hard-rejected", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsHardRejected, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-hysteresis-hard-rejected"), 3)
	if !errors.Is(err, ErrSharedDBCapacityExhausted) {
		t.Fatalf("over hard cap error = %v, want ErrSharedDBCapacityExhausted", err)
	}

	if err := s.DeleteTenantPlacementAndDecrCount(ctx, fs3, dbID, 0.8); err != nil {
		t.Fatalf("delete overflow placement: %v", err)
	}
	db, _ = s.GetSharedDB(ctx, dbID)
	if db.TenantCount != 2 || !db.SoftCapReached {
		t.Fatalf("pool above reopen threshold = count %d latch %v, want 2/true", db.TenantCount, db.SoftCapReached)
	}
	if err := s.DeleteTenantPlacementAndDecrCount(ctx, fs2, dbID, 0.8); err != nil {
		t.Fatalf("delete to reopen threshold: %v", err)
	}
	db, _ = s.GetSharedDB(ctx, dbID)
	if db.TenantCount != 1 || db.SoftCapReached {
		t.Fatalf("pool at reopen threshold = count %d latch %v, want 1/false", db.TenantCount, db.SoftCapReached)
	}
	if _, err := s.GetTenantPlacement(ctx, fs1); err != nil {
		t.Fatalf("remaining placement: %v", err)
	}
}

func TestFindSharedDBForEmergencyExcludesManuallyRegisteredPools(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	if _, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-emergency", Host: "manual.example.com", Port: 4000,
		User: "root", PasswordCipher: []byte("cipher"), Name: "manual_db", MaxTenants: 100,
	}); err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}

	spendingLimit := MaxTiDBCloudSpendingLimit
	managedID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-emergency", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET status = ?, cluster_id = 'cluster-emergency',
		db_host = 'managed.example.com', db_port = 4000, db_user = 'root', db_password = 'cipher',
		db_name = 'managed_db' WHERE db_id = ?`, SharedDBStatusActive, managedID); err != nil {
		t.Fatalf("activate managed pool: %v", err)
	}

	got, err := s.FindSharedDBForEmergency(ctx, "org-emergency", 1.2)
	if err != nil {
		t.Fatalf("FindSharedDBForEmergency: %v", err)
	}
	if got.ID != managedID {
		t.Fatalf("emergency pool = %d, want managed pool %d", got.ID, managedID)
	}
}

func TestCompleteSharedTenantProvisionDoesNotUpdateOtherProvisioningPools(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	targetID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-target", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 10, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool target: %v", err)
	}
	otherID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-other", ProvisioningKey: bytes.Repeat([]byte{2}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 10, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool other: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool
		SET status = ?, cluster_id = 'target-cluster', db_host = 'target-host', db_port = 4000,
			db_user = 'u', db_password = 'c', db_name = 'target_db' WHERE db_id = ?`, SharedDBStatusActive, targetID); err != nil {
		t.Fatalf("activate target pool: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET cluster_id = 'other-cluster' WHERE db_id = ?`, otherID); err != nil {
		t.Fatalf("seed other provisioning cluster: %v", err)
	}

	completeTestSharedTenant(t, s, targetID, "tenant-direct-create-parentheses", 0)

	target, err := s.GetSharedDB(ctx, targetID)
	if err != nil {
		t.Fatalf("GetSharedDB target: %v", err)
	}
	other, err := s.GetSharedDB(ctx, otherID)
	if err != nil {
		t.Fatalf("GetSharedDB other: %v", err)
	}
	if target.TenantCount != 1 {
		t.Fatalf("target tenant_count = %d, want 1", target.TenantCount)
	}
	if other.TenantCount != 0 || other.SoftCapReached {
		t.Fatalf("other tenant_count/latch = %d/%v, want 0/false", other.TenantCount, other.SoftCapReached)
	}
}

func TestCompleteSharedTenantProvisionCommitsAtomically(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-complete", Host: "h", Port: 4000, User: "u",
		PasswordCipher: []byte("c"), Name: "complete_db", MaxTenants: 5,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	seedPendingTenant(t, s, "tenant-complete-1")
	fsID, err := s.EnsureFsID(ctx, "tenant-complete-1")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := s.CompleteSharedTenantProvision(ctx, "tenant-complete-1", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-complete-1")); err != nil {
		t.Fatalf("CompleteSharedTenantProvision: %v", err)
	}

	tenant, err := s.GetTenant(ctx, "tenant-complete-1")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if tenant.Provider != "tidb_cloud_native_shared" || tenant.Status != TenantActive {
		t.Fatalf("tenant = provider %q status %q, want tidb_cloud_native_shared/active", tenant.Provider, tenant.Status)
	}
	p, err := s.GetTenantPlacement(ctx, fsID)
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	if p.DbID != dbID || p.Placement != PlacementShared {
		t.Fatalf("placement = %+v", p)
	}
	db, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if db.TenantCount != 1 {
		t.Fatalf("TenantCount = %d, want 1", db.TenantCount)
	}
	var keyCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ?`, "tenant-complete-1").Scan(&keyCount); err != nil {
		t.Fatal(err)
	}
	if keyCount != 1 {
		t.Fatalf("owner keys = %d, want 1", keyCount)
	}
}

func TestCompleteSharedTenantProvisionKeepsTenantProvisioningUntilDBPoolIsActive(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	dbID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-provisioning", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET cluster_id = 'cluster-provisioning' WHERE db_id = ?`, dbID); err != nil {
		t.Fatalf("mark provisioning cluster created: %v", err)
	}
	seedPendingTenant(t, s, "tenant-on-provisioning-pool")
	virtualLimit := int64(1000)
	if err := s.SetQuotaConfigPatch(ctx, "tenant-on-provisioning-pool", QuotaConfigPatch{TiDBCloudSpendingLimit: &virtualLimit}); err != nil {
		t.Fatalf("SetQuotaConfigPatch: %v", err)
	}
	fsID, err := s.EnsureFsID(ctx, "tenant-on-provisioning-pool")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := s.CompleteSharedTenantProvision(ctx, "tenant-on-provisioning-pool", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-on-provisioning-pool")); err != nil {
		t.Fatalf("CompleteSharedTenantProvision: %v", err)
	}
	tenant, err := s.GetTenant(ctx, "tenant-on-provisioning-pool")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if tenant.Status != TenantProvisioning || tenant.Provider != "tidb_cloud_native_shared" {
		t.Fatalf("tenant = provider %q status %q, want shared/provisioning", tenant.Provider, tenant.Status)
	}
	placement, err := s.GetTenantPlacement(ctx, fsID)
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	if placement.Status != SharedDBStatusActive {
		t.Fatalf("placement status = %q, want active routing", placement.Status)
	}
}

func TestManagedSharedDBPoolCloudResultSchemaAndActivation(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	dbID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		ProvisioningKey: make([]byte, 32), CloudProvider: "aws", Region: "us-east-1",
		MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	cloudResult := &SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-managed", ClusterID: "cluster-managed",
		Host: "shared.example.com", Port: 4000, User: "root", PasswordCipher: []byte("root-cipher"),
		Name: "tidbcloud_fs", TLSMode: "true",
	}
	if err := s.UpdateManagedSharedDBPoolCloudResult(ctx, cloudResult); err != nil {
		t.Fatalf("UpdateManagedSharedDBPoolCloudResult: %v", err)
	}
	if err := s.UpdateManagedSharedDBPoolCloudResult(ctx, cloudResult); err != nil {
		t.Fatalf("idempotent UpdateManagedSharedDBPoolCloudResult: %v", err)
	}
	got, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB after cloud result: %v", err)
	}
	if got.TiDBCloudOrganizationID != "org-managed" || got.ClusterID != "cluster-managed" || got.Host != "shared.example.com" || got.Port != 4000 {
		t.Fatalf("cloud result not persisted: %+v", got)
	}
	if len(got.ProvisioningKey) != 0 {
		t.Fatalf("provisioning key = %x, want cleared after organization is durable", got.ProvisioningKey)
	}
	if got.Status != SharedDBStatusProvisioning {
		t.Fatalf("status after cloud result = %q, want provisioning", got.Status)
	}

	if err := s.UpdateSharedDBSchemaVersion(ctx, dbID, 17); err != nil {
		t.Fatalf("UpdateSharedDBSchemaVersion: %v", err)
	}
	if err := s.ActivateSharedDBPool(ctx, dbID); err != nil {
		t.Fatalf("ActivateSharedDBPool: %v", err)
	}
	got, err = s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB after activate: %v", err)
	}
	if got.Status != SharedDBStatusActive || got.SchemaVersion != 17 {
		t.Fatalf("activated pool = status %q schema %d, want active/17", got.Status, got.SchemaVersion)
	}
	rows, err := s.ListSharedDBsByStatus(ctx, SharedDBStatusActive, 10)
	if err != nil {
		t.Fatalf("ListSharedDBsByStatus: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != dbID {
		t.Fatalf("active rows = %+v, want db %d", rows, dbID)
	}
}

func TestActivateSharedTenantsBatchRequiresReadyPoolPlacementAndOwnerKey(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	dbID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-activation", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET cluster_id = 'cluster-activation' WHERE db_id = ?`, dbID); err != nil {
		t.Fatalf("mark activation cluster created: %v", err)
	}
	for _, tenantID := range []string{"tenant-activate-1", "tenant-activate-2"} {
		seedPendingTenant(t, s, tenantID)
		limit := int64(1000)
		if err := s.SetQuotaConfigPatch(ctx, tenantID, QuotaConfigPatch{TiDBCloudSpendingLimit: &limit}); err != nil {
			t.Fatalf("SetQuotaConfigPatch %s: %v", tenantID, err)
		}
		fsID, err := s.EnsureFsID(ctx, tenantID)
		if err != nil {
			t.Fatalf("EnsureFsID %s: %v", tenantID, err)
		}
		if err := s.CompleteSharedTenantProvision(ctx, tenantID, "tidb_cloud_native_shared", &TenantPlacement{
			FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
		}, testOwnerKey(tenantID)); err != nil {
			t.Fatalf("CompleteSharedTenantProvision %s: %v", tenantID, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM tenant_api_keys WHERE tenant_id = ?`, "tenant-activate-2"); err != nil {
		t.Fatalf("remove second owner key: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET status = ?, db_host = 'h', db_port = 4000,
		db_user = 'u', db_password = 'c', db_name = 'n', schema_version = 1 WHERE db_id = ?`, SharedDBStatusActive, dbID); err != nil {
		t.Fatalf("mark pool active: %v", err)
	}

	activated, err := s.ActivateSharedTenantsBatch(ctx, dbID, 100)
	if err != nil {
		t.Fatalf("ActivateSharedTenantsBatch: %v", err)
	}
	if activated != 1 {
		t.Fatalf("activated = %d, want 1", activated)
	}
	first, _ := s.GetTenant(ctx, "tenant-activate-1")
	second, _ := s.GetTenant(ctx, "tenant-activate-2")
	if first.Status != TenantActive || second.Status != TenantProvisioning {
		t.Fatalf("statuses = %q/%q, want active/provisioning", first.Status, second.Status)
	}
}

func TestFindSharedDBForAllocationPrefersActiveWithoutVirtualSpendingHeadroom(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingLimit := MaxTiDBCloudSpendingLimit
	provisioningID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-allocate", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool provisioning: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET cluster_id = 'cluster-allocate-provisioning' WHERE db_id = ?`, provisioningID); err != nil {
		t.Fatalf("mark provisioning cluster created: %v", err)
	}
	activeID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-allocate", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool active: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET status = ?, db_host = 'h', db_port = 4000,
		db_user = 'u', db_password = 'c', db_name = CONCAT('n', db_id), schema_version = 1 WHERE db_id = ?`, SharedDBStatusActive, activeID); err != nil {
		t.Fatalf("activate second pool: %v", err)
	}

	got, err := s.FindSharedDBForAllocation(ctx, "org-allocate", nil)
	if err != nil {
		t.Fatalf("FindSharedDBForAllocation active: %v", err)
	}
	if got.ID != activeID {
		t.Fatalf("allocation chose %d, want active pool %d before provisioning %d", got.ID, activeID, provisioningID)
	}

	seedPendingTenant(t, s, "tenant-headroom")
	limit := MaxTiDBCloudSpendingLimit
	if err := s.SetQuotaConfigPatch(ctx, "tenant-headroom", QuotaConfigPatch{TiDBCloudSpendingLimit: &limit}); err != nil {
		t.Fatalf("SetQuotaConfigPatch: %v", err)
	}
	fsID, _ := s.EnsureFsID(ctx, "tenant-headroom")
	if err := s.CompleteSharedTenantProvision(ctx, "tenant-headroom", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsID, DbID: activeID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-headroom")); err != nil {
		t.Fatalf("CompleteSharedTenantProvision: %v", err)
	}
	got, err = s.FindSharedDBForAllocation(ctx, "org-allocate", nil)
	if err != nil {
		t.Fatalf("FindSharedDBForAllocation fallback: %v", err)
	}
	if got.ID != activeID {
		t.Fatalf("allocation after maximum virtual value chose %d, want active pool %d", got.ID, activeID)
	}
}

func TestUpdateSharedTenantQuotaConfigTreatsSpendingLimitAsVirtualValue(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	spendingTarget := MaxTiDBCloudSpendingLimit
	dbID, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-quota", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET cluster_id = 'cluster-quota' WHERE db_id = ?`, dbID); err != nil {
		t.Fatalf("mark quota cluster created: %v", err)
	}
	for _, tenantID := range []string{"tenant-shared-quota-1", "tenant-shared-quota-2"} {
		seedPendingTenant(t, s, tenantID)
		limit := int64(1000)
		if err := s.SetQuotaConfig(ctx, &QuotaConfig{
			TenantID: tenantID, MaxStorageBytes: 100, MaxFileSizeBytes: 10,
			MaxMediaLLMFiles: 1, MaxVideoLLMFiles: 1, TiDBCloudSpendingLimit: &limit,
		}); err != nil {
			t.Fatalf("SetQuotaConfig %s: %v", tenantID, err)
		}
		fsID, _ := s.EnsureFsID(ctx, tenantID)
		if err := s.CompleteSharedTenantProvision(ctx, tenantID, "tidb_cloud_native_shared", &TenantPlacement{
			FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
		}, testOwnerKey(tenantID)); err != nil {
			t.Fatalf("CompleteSharedTenantProvision %s: %v", tenantID, err)
		}
	}

	withinHeadroom := int64(1500)
	storage := int64(200)
	if err := s.UpdateSharedTenantQuotaConfig(ctx, "tenant-shared-quota-1", QuotaConfigPatch{
		MaxStorageBytes: &storage, TiDBCloudSpendingLimit: &withinHeadroom,
	}); err != nil {
		t.Fatalf("UpdateSharedTenantQuotaConfig within headroom: %v", err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-shared-quota-1")
	if err != nil {
		t.Fatalf("GetQuotaConfig: %v", err)
	}
	if cfg.MaxStorageBytes != storage || cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != withinHeadroom {
		t.Fatalf("updated quota = %+v", cfg)
	}

	maximumVirtualValue := MaxTiDBCloudSpendingLimit
	updatedStorage := int64(300)
	err = s.UpdateSharedTenantQuotaConfig(ctx, "tenant-shared-quota-1", QuotaConfigPatch{
		MaxStorageBytes: &updatedStorage, TiDBCloudSpendingLimit: &maximumVirtualValue,
	})
	if err != nil {
		t.Fatalf("UpdateSharedTenantQuotaConfig maximum virtual value: %v", err)
	}
	cfg, err = s.GetQuotaConfig(ctx, "tenant-shared-quota-1")
	if err != nil {
		t.Fatalf("GetQuotaConfig after maximum virtual value: %v", err)
	}
	if cfg.MaxStorageBytes != updatedStorage || cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != maximumVirtualValue {
		t.Fatalf("maximum virtual value update = %+v", cfg)
	}
}

func TestCompleteSharedTenantProvisionCapacityExhaustedLeavesNoPartialState(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-capped", Host: "h", Port: 4000, User: "u",
		PasswordCipher: []byte("c"), Name: "capped_db", MaxTenants: 1,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	seedPendingTenant(t, s, "tenant-cap-1")
	seedPendingTenant(t, s, "tenant-cap-2")
	fs1, _ := s.EnsureFsID(ctx, "tenant-cap-1")
	fs2, _ := s.EnsureFsID(ctx, "tenant-cap-2")
	if err := s.CompleteSharedTenantProvision(ctx, "tenant-cap-1", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fs1, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-cap-1")); err != nil {
		t.Fatalf("first provision: %v", err)
	}
	err = s.CompleteSharedTenantProvision(ctx, "tenant-cap-2", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fs2, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-cap-2"))
	if !errors.Is(err, ErrSharedDBCapacityExhausted) {
		t.Fatalf("second provision error = %v, want ErrSharedDBCapacityExhausted", err)
	}

	// Nothing of the losing provision may persist.
	if _, err := s.GetTenantPlacement(ctx, fs2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("loser placement = %v, want ErrNotFound", err)
	}
	tenant, err := s.GetTenant(ctx, "tenant-cap-2")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if tenant.Provider != "tidb_cloud_native" || tenant.Status != TenantPending {
		t.Fatalf("loser tenant = provider %q status %q, want unchanged tidb_cloud_native/pending", tenant.Provider, tenant.Status)
	}
	var keyCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ?`, "tenant-cap-2").Scan(&keyCount); err != nil {
		t.Fatal(err)
	}
	if keyCount != 0 {
		t.Fatalf("loser owner keys = %d, want 0", keyCount)
	}
	db, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if db.TenantCount != 1 {
		t.Fatalf("TenantCount = %d, want 1 (no leak)", db.TenantCount)
	}
}

func TestCompleteSharedTenantProvisionRollsBackOnMissingTenant(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-rollback", Host: "h", Port: 4000, User: "u",
		PasswordCipher: []byte("c"), Name: "rollback_db",
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	fsID, err := s.EnsureFsID(ctx, "tenant-ghost")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	// No tenant row for tenant-ghost: the tenant update fails and the whole
	// transaction must roll back — no placement, no capacity, no key.
	err = s.CompleteSharedTenantProvision(ctx, "tenant-ghost", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-ghost"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("provision error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetTenantPlacement(ctx, fsID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("placement after rollback = %v, want ErrNotFound", err)
	}
	db, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if db.TenantCount != 0 {
		t.Fatalf("TenantCount = %d, want 0 after rollback", db.TenantCount)
	}
	var keyCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ?`, "tenant-ghost").Scan(&keyCount); err != nil {
		t.Fatal(err)
	}
	if keyCount != 0 {
		t.Fatalf("owner keys after rollback = %d, want 0", keyCount)
	}
}

func TestDeleteTenantPlacementAndDecrCountIsAtomic(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-release", Host: "h", Port: 4000, User: "u",
		PasswordCipher: []byte("c"), Name: "release_db",
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	seedPendingTenant(t, s, "tenant-release-1")
	fsID, err := s.EnsureFsID(ctx, "tenant-release-1")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := s.CompleteSharedTenantProvision(ctx, "tenant-release-1", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-release-1")); err != nil {
		t.Fatalf("provision: %v", err)
	}

	if err := s.DeleteTenantPlacementAndDecrCount(ctx, fsID, dbID, 0.8); err != nil {
		t.Fatalf("DeleteTenantPlacementAndDecrCount: %v", err)
	}
	if _, err := s.GetTenantPlacement(ctx, fsID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("placement after delete = %v, want ErrNotFound", err)
	}
	got, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if got.TenantCount != 0 {
		t.Fatalf("TenantCount = %d, want 0", got.TenantCount)
	}
	if got.SoftCapReached {
		t.Fatal("unlimited pool soft-cap latch = true, want false")
	}

	// Missing placement: ErrNotFound.
	if err := s.DeleteTenantPlacementAndDecrCount(ctx, 299, dbID, 0.8); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing placement error = %v, want ErrNotFound", err)
	}

	// When the pool row is gone, the whole tx must roll back: the placement
	// row survives so the caller can retry instead of orphaning it.
	if err := s.UpsertTenantPlacement(ctx, &TenantPlacement{
		FsID: 301, DbID: dbID + 999, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}); err != nil {
		t.Fatalf("seed orphan-candidate placement: %v", err)
	}
	if err := s.DeleteTenantPlacementAndDecrCount(ctx, 301, dbID+999, 0.8); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete with missing pool error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetTenantPlacement(ctx, 301); err != nil {
		t.Fatalf("placement must survive rolled-back delete: %v", err)
	}
}

func TestDeleteTenantPlacementAndDecrCountUsesReopenRatio(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-release-ratio", Host: "h", Port: 4000, User: "u",
		PasswordCipher: []byte("c"), Name: "release_ratio_db", MaxTenants: 10,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	seedPendingTenant(t, s, "tenant-release-ratio")
	fsID, err := s.EnsureFsID(ctx, "tenant-release-ratio")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := s.UpsertTenantPlacement(ctx, &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE db_pool SET tenant_count = 6, soft_cap_reached = 1 WHERE db_id = ?`, dbID); err != nil {
		t.Fatalf("seed capacity: %v", err)
	}

	if err := s.DeleteTenantPlacementAndDecrCount(ctx, fsID, dbID, 0.65); err != nil {
		t.Fatalf("DeleteTenantPlacementAndDecrCount: %v", err)
	}
	got, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if got.TenantCount != 5 || got.SoftCapReached {
		t.Fatalf("capacity after delete = count %d latch %v, want 5/false", got.TenantCount, got.SoftCapReached)
	}
}

func TestFinalizeSharedTenantDeleteMetadataIsAtomic(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-finalize-delete", Host: "h", Port: 4000, User: "u",
		PasswordCipher: []byte("c"), Name: "finalize_delete_db",
	})
	if err != nil {
		t.Fatal(err)
	}
	seedPendingTenant(t, s, "tenant-finalize-delete")
	fsID, err := s.EnsureFsID(ctx, "tenant-finalize-delete")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteSharedTenantProvision(ctx, "tenant-finalize-delete", tidbCloudNativeSharedProvider, &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-finalize-delete")); err != nil {
		t.Fatal(err)
	}

	err = s.FinalizeSharedTenantDeleteMetadata(ctx, "missing-tenant", fsID, dbID, 0.8, true)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("FinalizeSharedTenantDeleteMetadata error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("placement did not roll back: %v", err)
	}
	pool, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatal(err)
	}
	if pool.TenantCount != 1 {
		t.Fatalf("tenant count after rollback = %d, want 1", pool.TenantCount)
	}

	if err := s.FinalizeSharedTenantDeleteMetadata(ctx, "tenant-finalize-delete", fsID, dbID, 0.8, true); err != nil {
		t.Fatalf("FinalizeSharedTenantDeleteMetadata: %v", err)
	}
	if _, err := s.GetTenantPlacement(ctx, fsID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("placement after finalize = %v, want ErrNotFound", err)
	}
	tenant, err := s.GetTenant(ctx, "tenant-finalize-delete")
	if err != nil {
		t.Fatal(err)
	}
	if tenant.Status != TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", tenant.Status, TenantDeleted)
	}

	seedPendingTenant(t, s, "tenant-finalize-job")
	jobFsID, err := s.EnsureFsID(ctx, "tenant-finalize-job")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteSharedTenantProvision(ctx, "tenant-finalize-job", tidbCloudNativeSharedProvider, &TenantPlacement{
		FsID: jobFsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-finalize-job")); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeSharedTenantDeleteMetadata(ctx, "tenant-finalize-job", jobFsID, dbID, 0.8, false); err != nil {
		t.Fatalf("FinalizeSharedTenantDeleteMetadata with external cleanup: %v", err)
	}
	placement, err := s.GetTenantPlacement(ctx, jobFsID)
	if err != nil {
		t.Fatalf("authorization placement missing: %v", err)
	}
	if placement.Status != PlacementStatusDeleting {
		t.Fatalf("placement status = %q, want %q", placement.Status, PlacementStatusDeleting)
	}
}

// TestCompleteSharedTenantProvisionRollsBackOnKeyFailure covers the named
// failure class from review: the owner key insert fails (duplicate key id)
// inside the provision transaction, so the capacity reservation, the
// placement, AND the active/provider transition must all roll back with it —
// the tenant can never be left active without its placement and key.
func TestCompleteSharedTenantProvisionRollsBackOnKeyFailure(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-keyfail", Host: "h", Port: 4000, User: "u",
		PasswordCipher: []byte("c"), Name: "keyfail_db",
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	seedPendingTenant(t, s, "tenant-keyfail")
	fsID, err := s.EnsureFsID(ctx, "tenant-keyfail")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	// Pre-insert a key with the same id, so the tx's key insert duplicates.
	if err := s.InsertAPIKey(ctx, testOwnerKey("tenant-keyfail")); err != nil {
		t.Fatalf("seed duplicate key: %v", err)
	}
	err = s.CompleteSharedTenantProvision(ctx, "tenant-keyfail", "tidb_cloud_native_shared", &TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}, testOwnerKey("tenant-keyfail"))
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("provision error = %v, want ErrDuplicate", err)
	}
	tenant, err := s.GetTenant(ctx, "tenant-keyfail")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if tenant.Provider != "tidb_cloud_native" || tenant.Status != TenantPending {
		t.Fatalf("tenant = provider %q status %q, want unchanged tidb_cloud_native/pending", tenant.Provider, tenant.Status)
	}
	if _, err := s.GetTenantPlacement(ctx, fsID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("placement after key-failure rollback = %v, want ErrNotFound", err)
	}
	db, err := s.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if db.TenantCount != 0 {
		t.Fatalf("TenantCount = %d, want 0 after key-failure rollback", db.TenantCount)
	}
}
