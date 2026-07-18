package meta

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEnsureFsIDAllocatesAndStaysStable(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	id1, err := s.EnsureFsID(ctx, "tenant-fs-a")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if id1 <= 0 {
		t.Fatalf("fs_id = %d, want positive", id1)
	}
	again, err := s.EnsureFsID(ctx, "tenant-fs-a")
	if err != nil {
		t.Fatalf("EnsureFsID again: %v", err)
	}
	if again != id1 {
		t.Fatalf("EnsureFsID not stable: first %d, second %d", id1, again)
	}

	id2, err := s.EnsureFsID(ctx, "tenant-fs-b")
	if err != nil {
		t.Fatalf("EnsureFsID second tenant: %v", err)
	}
	if id2 == id1 {
		t.Fatalf("distinct tenants share fs_id %d", id1)
	}
}

func TestEnsureFsIDRejectsEmptyTenant(t *testing.T) {
	s := newControlStore(t)
	if _, err := s.EnsureFsID(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty tenant id")
	}
}

func TestResolveFsIDNotFound(t *testing.T) {
	s := newControlStore(t)
	_, err := s.ResolveFsID(context.Background(), "tenant-never-registered")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveFsID error = %v, want ErrNotFound", err)
	}
}

func TestResolveTenantIDRoundTrip(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	id, err := s.EnsureFsID(ctx, "tenant-roundtrip")
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	tenantID, err := s.ResolveTenantID(ctx, id)
	if err != nil {
		t.Fatalf("ResolveTenantID: %v", err)
	}
	if tenantID != "tenant-roundtrip" {
		t.Fatalf("ResolveTenantID = %q, want tenant-roundtrip", tenantID)
	}
	if _, err := s.ResolveTenantID(ctx, id+100000); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveTenantID unknown id error = %v, want ErrNotFound", err)
	}
}

func TestInsertTenantAllocatesFsID(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	now := time.Now()
	if err := s.InsertTenant(ctx, &Tenant{
		ID:               "tenant-auto-fs",
		Status:           TenantPending,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "db_tenant_auto_fs",
		DBTLS:            true,
		Provider:         "tidb_zero",
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	if _, err := s.ResolveFsID(ctx, "tenant-auto-fs"); err != nil {
		t.Fatalf("ResolveFsID after InsertTenant: %v", err)
	}
}

func TestBackfillFsRegistryIsIdempotent(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	// Insert tenants with raw SQL to simulate pre-existing rows from before
	// fs_registry existed (InsertTenant itself now allocates fs_ids).
	for _, id := range []string{"tenant-backfill-1", "tenant-backfill-2"} {
		if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenants
			(id, status, db_host, db_port, db_user, db_password, db_name, provider, created_at, updated_at)
			VALUES (?, 'active', '127.0.0.1', 4000, 'root', ?, ?, 'tidb_zero', NOW(3), NOW(3))`,
			id, []byte("cipher"), "db_"+id); err != nil {
			t.Fatalf("insert legacy tenant %s: %v", id, err)
		}
	}

	inserted, err := s.BackfillFsRegistry(ctx)
	if err != nil {
		t.Fatalf("BackfillFsRegistry: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("first backfill inserted %d rows, want 2", inserted)
	}
	again, err := s.BackfillFsRegistry(ctx)
	if err != nil {
		t.Fatalf("BackfillFsRegistry again: %v", err)
	}
	if again != 0 {
		t.Fatalf("second backfill inserted %d rows, want 0", again)
	}

	for _, id := range []string{"tenant-backfill-1", "tenant-backfill-2"} {
		if _, err := s.ResolveFsID(ctx, id); err != nil {
			t.Fatalf("ResolveFsID %s after backfill: %v", id, err)
		}
	}
}
