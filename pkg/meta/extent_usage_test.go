package meta

import (
	"context"
	"database/sql"
	"testing"
)

// The extent usage report spans two stores, so its apply must be idempotent on
// its own: the watermark compare-and-set is what makes a retried range (log
// landed, marker commit failed, next pass re-reports under a fresh log id)
// apply its bytes exactly once. This exercises the SQL directly for both a
// growing and a shrinking range, plus the superseded-range replay ordering.
//
// Contract pinned here: ApplyExtentUsageRangeTx moves ONLY the watermark. The
// storage_bytes delta is flushed separately by the apply owner
// (IncrQuotaUsageCountersTx in the same transaction) — the flush below stands
// in for that owner. When the method also moved the counter, every applied
// report was double-counted on the real store (the counter moved inside the
// CAS and again in the owner's flush), which is what this test exists to
// prevent.
func TestApplyExtentUsageRangeIsIdempotent(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	const tenant = "extent-range-tenant"
	if err := s.EnsureQuotaUsageRow(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	// Seed the state a busy tenant is in: extent bytes already reported into
	// the counters (storage_bytes == extent watermark; the classic share rides
	// on top of it and is not this test's subject).
	if _, err := s.db.Exec(
		`UPDATE tenant_quota_usage SET storage_bytes = 100, extent_reported_bytes = 100 WHERE tenant_id = ?`, tenant,
	); err != nil {
		t.Fatal(err)
	}

	apply := func(from, to int64) (moved bool) {
		t.Helper()
		if err := s.InTx(ctx, func(tx *sql.Tx) error {
			var err error
			moved, err = s.ApplyExtentUsageRangeTx(tx, tenant, from, to)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return moved
	}
	// The apply owner's counter flush, in its own transaction like the real
	// dispatcher/replay owners run it.
	flush := func(delta int64) {
		t.Helper()
		if err := s.InTx(ctx, func(tx *sql.Tx) error {
			return s.IncrQuotaUsageCountersTx(tx, tenant, delta, 0, 0, 0)
		}); err != nil {
			t.Fatal(err)
		}
	}
	usage := func() (int64, int64) {
		t.Helper()
		var storage, watermark int64
		if err := s.db.QueryRow(
			`SELECT storage_bytes, extent_reported_bytes FROM tenant_quota_usage WHERE tenant_id = ?`, tenant,
		).Scan(&storage, &watermark); err != nil {
			t.Fatal(err)
		}
		return storage, watermark
	}

	// Growth 100 -> 150, applied twice (the retry window). The CAS alone must
	// not move the counter; only the first application produces a delta for
	// the owner to flush, exactly once.
	wantStorage := []int64{100, 100 + 50}
	for i := 0; i < 2; i++ {
		moved := apply(100, 150)
		switch {
		case i == 0 && !moved:
			t.Fatal("first application of the range did not move the watermark")
		case i == 1 && moved:
			t.Fatal("second application of the same range moved the watermark again (double count)")
		}
		// Before this iteration's (single) flush: the CAS must never have
		// moved the counter, and the previous iteration flushed exactly once.
		if storage, _ := usage(); storage != wantStorage[i] {
			t.Fatalf("storage after apply #%d = %d, want %d (the CAS must not touch the counter)", i, storage, wantStorage[i])
		}
		if i == 0 {
			flush(50)
		}
	}
	if storage, watermark := usage(); storage != 150 || watermark != 150 {
		t.Fatalf("after growth storage=%d watermark=%d, want 150/150 (100 seeded + 50 flushed once)", storage, watermark)
	}

	// Shrink 150 -> 50, applied twice: a retried negative range must not
	// subtract twice and leave the counters below the real usage.
	shrinkWantStorage := []int64{150, 150 - 100}
	for i := 0; i < 2; i++ {
		moved := apply(150, 50)
		switch {
		case i == 0 && !moved:
			t.Fatal("first application of the shrink did not move the watermark")
		case i == 1 && moved:
			t.Fatal("second application of the shrink moved the watermark again (permanent under-count)")
		}
		if storage, _ := usage(); storage != shrinkWantStorage[i] {
			t.Fatalf("storage after shrink apply #%d = %d, want %d (the CAS must not touch the counter)", i, storage, shrinkWantStorage[i])
		}
		if i == 0 {
			flush(-100)
		}
	}
	if storage, watermark := usage(); storage != 50 || watermark != 50 {
		t.Fatalf("after shrink storage=%d watermark=%d, want 50/50 (150 - 100 flushed once)", storage, watermark)
	}

	// A superseded range (replay of an old pending row after a newer report
	// landed) no-ops instead of moving the watermark backwards — and must not
	// produce a counter delta for its owner to flush.
	if moved := apply(100, 150); moved {
		t.Fatal("a superseded range moved the watermark")
	}
	if storage, watermark := usage(); storage != 50 || watermark != 50 {
		t.Fatalf("after superseded replay storage=%d watermark=%d, want 50/50 (unchanged)", storage, watermark)
	}

	// The reader the worker derives its ranges from reports the watermark.
	if reported, err := s.GetExtentReportedBytes(ctx, tenant); err != nil || reported != 50 {
		t.Fatalf("GetExtentReportedBytes = %d, %v; want 50, nil", reported, err)
	}
}
