package meta

import (
	"context"
	"errors"
	"testing"
	"time"
)

func registerTestSharedDB(t *testing.T, s *Store, orgID, host, name string) int64 {
	t.Helper()
	id, err := s.RegisterSharedDB(context.Background(), &SharedDB{
		OrgID:          orgID,
		Host:           host,
		Port:           4000,
		User:           "root",
		PasswordCipher: []byte("cipher-" + name),
		Name:           name,
		TLSMode:        "skip-verify",
		MaxTenants:     100,
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
	if got.DbID != id {
		t.Fatalf("DbID = %d, want %d", got.DbID, id)
	}
	if got.OrgID != "org-a" {
		t.Fatalf("OrgID = %q, want org-a", got.OrgID)
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

func TestRegisterSharedDBUpsertKeepsIDAndTenantCount(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	first := registerTestSharedDB(t, s, "org-a", "shared-a.example.com", "shared_db_a")
	if err := s.IncrSharedDBTenantCount(ctx, first, 3); err != nil {
		t.Fatalf("IncrSharedDBTenantCount: %v", err)
	}

	second, err := s.RegisterSharedDB(ctx, &SharedDB{
		OrgID:          "org-a",
		Host:           "shared-a.example.com",
		Port:           5000,
		User:           "app",
		PasswordCipher: []byte("rotated-cipher"),
		Name:           "shared_db_a",
		TLSMode:        "true",
		MaxTenants:     200,
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
}

func TestRegisterSharedDBValidation(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	valid := func() *SharedDB {
		return &SharedDB{
			OrgID:          "org-a",
			Host:           "h",
			Port:           4000,
			User:           "u",
			PasswordCipher: []byte("c"),
			Name:           "n",
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
	if got.DbID != orgID {
		t.Fatalf("exact match db_id = %d, want %d", got.DbID, orgID)
	}

	got, err = s.FindSharedDBForOrg(ctx, "org-other")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg fallback: %v", err)
	}
	if got.DbID != wildID {
		t.Fatalf("wildcard fallback db_id = %d, want %d", got.DbID, wildID)
	}

	got, err = s.FindSharedDBForOrg(ctx, "")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg empty org: %v", err)
	}
	if got.DbID != wildID {
		t.Fatalf("empty org db_id = %d, want wildcard %d", got.DbID, wildID)
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
		OrgID:          "org-b",
		Host:           "shared-draining.example.com",
		Port:           4000,
		User:           "root",
		PasswordCipher: []byte("cipher"),
		Name:           "shared_db_draining",
		Status:         "draining",
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
	if list[0].DbID != first || list[1].DbID != second {
		t.Fatalf("ListSharedDBs order = [%d %d], want [%d %d]",
			list[0].DbID, list[1].DbID, first, second)
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
		OrgID:          "org-capped",
		Host:           "shared-capped.example.com",
		Port:           4000,
		User:           "root",
		PasswordCipher: []byte("cipher"),
		Name:           "shared_db_capped",
		MaxTenants:     1,
	}); err != nil {
		t.Fatalf("cap pool at 1: %v", err)
	}
	wildID := registerTestSharedDB(t, s, SharedDBOrgWildcard, "shared-wild.example.com", "shared_db_wild")

	got, err := s.FindSharedDBForOrg(ctx, "org-capped")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg before full: %v", err)
	}
	if got.DbID != fullID {
		t.Fatalf("before full db_id = %d, want exact pool %d", got.DbID, fullID)
	}

	if err := s.IncrSharedDBTenantCount(ctx, fullID, 1); err != nil {
		t.Fatalf("fill pool: %v", err)
	}
	got, err = s.FindSharedDBForOrg(ctx, "org-capped")
	if err != nil {
		t.Fatalf("FindSharedDBForOrg after full: %v", err)
	}
	if got.DbID != wildID {
		t.Fatalf("after full db_id = %d, want wildcard %d", got.DbID, wildID)
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

func TestCompleteSharedTenantProvisionCommitsAtomically(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		OrgID: "org-complete", Host: "h", Port: 4000, User: "u",
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

func TestCompleteSharedTenantProvisionCapacityExhaustedLeavesNoPartialState(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	dbID, err := s.RegisterSharedDB(ctx, &SharedDB{
		OrgID: "org-capped", Host: "h", Port: 4000, User: "u",
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
		OrgID: "org-rollback", Host: "h", Port: 4000, User: "u",
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
		OrgID: "org-release", Host: "h", Port: 4000, User: "u",
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

	if err := s.DeleteTenantPlacementAndDecrCount(ctx, fsID, dbID); err != nil {
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

	// Missing placement: ErrNotFound.
	if err := s.DeleteTenantPlacementAndDecrCount(ctx, 299, dbID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing placement error = %v, want ErrNotFound", err)
	}

	// When the pool row is gone, the whole tx must roll back: the placement
	// row survives so the caller can retry instead of orphaning it.
	if err := s.UpsertTenantPlacement(ctx, &TenantPlacement{
		FsID: 301, DbID: dbID + 999, Placement: PlacementShared, SchemaShape: SchemaShapeShared,
	}); err != nil {
		t.Fatalf("seed orphan-candidate placement: %v", err)
	}
	if err := s.DeleteTenantPlacementAndDecrCount(ctx, 301, dbID+999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete with missing pool error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetTenantPlacement(ctx, 301); err != nil {
		t.Fatalf("placement must survive rolled-back delete: %v", err)
	}
}
