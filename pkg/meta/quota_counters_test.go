package meta

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// TestIncrQuotaUsageCountersTx covers the combined counter update used by the
// batched "hot row last" mutation apply paths: all four counters move with
// one statement, all-zero deltas are a no-op (even for a tenant without a
// usage row), and a non-zero delta against a missing row errors exactly like
// the per-counter Incr*Tx methods (ensureRowsAffected).
func TestIncrQuotaUsageCountersTx(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	if err := s.EnsureQuotaUsageRow(ctx, "tenant-counters"); err != nil {
		t.Fatal(err)
	}
	// Seed non-zero counters so negative deltas are observable too.
	if err := s.IncrStorageBytes(ctx, "tenant-counters", 100); err != nil {
		t.Fatal(err)
	}
	if err := s.IncrReservedBytes(ctx, "tenant-counters", 40); err != nil {
		t.Fatal(err)
	}

	err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.IncrQuotaUsageCountersTx(tx, "tenant-counters", 50, 2, 1, -40)
	})
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.GetQuotaUsage(ctx, "tenant-counters")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 150 || usage.FileCount != 2 || usage.MediaFileCount != 1 || usage.ReservedBytes != 0 {
		t.Fatalf("usage = %+v, want storage=150 file=2 media=1 reserved=0", usage)
	}

	// All-zero deltas are a no-op: counters unchanged, and no error even for
	// a tenant whose usage row does not exist (no statement is executed).
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		return s.IncrQuotaUsageCountersTx(tx, "tenant-counters", 0, 0, 0, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		return s.IncrQuotaUsageCountersTx(tx, "tenant-without-usage-row", 0, 0, 0, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	usage, err = s.GetQuotaUsage(ctx, "tenant-counters")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 150 || usage.FileCount != 2 || usage.MediaFileCount != 1 || usage.ReservedBytes != 0 {
		t.Fatalf("usage after zero-delta no-op = %+v, want unchanged", usage)
	}

	// A non-zero delta against a missing usage row errors like the
	// per-counter Incr*Tx methods do.
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		return s.IncrQuotaUsageCountersTx(tx, "tenant-without-usage-row", 1, 0, 0, 0)
	})
	if err == nil || !strings.Contains(err.Error(), "tenant_quota_usage row missing") {
		t.Fatalf("err = %v, want tenant_quota_usage row missing", err)
	}
}
