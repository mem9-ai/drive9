package meta

import (
	"context"
	"errors"
	"testing"
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
		TLS:            true,
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
	if !got.TLS {
		t.Fatal("TLS = false, want true")
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
		TLS:            false,
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
		got.MaxTenants != 200 || got.TLS {
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
	}
	for name, mutate := range cases {
		p := valid()
		mutate(p)
		if err := s.UpsertTenantPlacement(ctx, p); err == nil {
			t.Fatalf("%s: expected error, got nil", name)
		}
	}
}

func TestGetTenantPlacementNotFound(t *testing.T) {
	s := newControlStore(t)
	if _, err := s.GetTenantPlacement(context.Background(), 123456); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTenantPlacement error = %v, want ErrNotFound", err)
	}
}
