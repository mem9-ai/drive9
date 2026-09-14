package backend

import (
	"context"
	"testing"
)

// The report path spans two stores: the central mutation log (plus its apply)
// and the tenant-side reported marker. A crash or failed marker commit after
// the log insert makes the next pass re-report the same range under a fresh
// log id, so the mutation itself must be safe to apply twice. This simulates
// that retry window end-to-end through the backend — two Report calls with
// the same range, one apply each — and asserts the central counters moved
// once, for growth and for shrink.
func TestExtentUsageRangeRetryAppliesOnce(t *testing.T) {
	for _, tc := range []struct {
		name       string
		from, to   int64
		wantStored int64
	}{
		{"growth retried", 100, 150, 50},
		{"shrink retried", 150, 50, -100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, fake := newCentralQuotaBackend(t)
			ctx := context.Background()
			// A tenant that already reported up to from: the retry window
			// presupposes an earlier report landed, so the watermark starts
			// there and the fake's CAS matches exactly like the real one.
			fake.extentReported["tenant-a"] = tc.from
			// The marker commit failing after the report landed is the whole
			// window: the next pass peeks the same central watermark and
			// re-reports the same range.
			for i := 0; i < 2; i++ {
				if err := b.ReportExtentUsageRange(ctx, tc.from, tc.to); err != nil {
					t.Fatalf("report %d: %v", i, err)
				}
				drainCentralQuotaMutations(t, b)
			}
			usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if usage.StorageBytes != tc.wantStored {
				t.Fatalf("storage_bytes after a retried report = %d, want %d (applied exactly once)", usage.StorageBytes, tc.wantStored)
			}
			reported, err := b.ExtentUsageReported(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if reported != tc.to {
				t.Fatalf("extent watermark after a retried report = %d, want %d", reported, tc.to)
			}
		})
	}
}

// A zero-width range is not a mutation at all: the worker's marker-repair
// path and a no-op pass must not grow the log.
func TestExtentUsageRangeZeroWidthSkipped(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	ctx := context.Background()
	if err := b.ReportExtentUsageRange(ctx, 100, 100); err != nil {
		t.Fatal(err)
	}
	drainCentralQuotaMutations(t, b)
	if got := len(fake.mutations); got != 0 {
		t.Fatalf("mutation log rows for a zero-width range = %d, want 0", got)
	}
	if reported, err := b.ExtentUsageReported(ctx); err != nil || reported != 0 {
		t.Fatalf("ExtentUsageReported = %d, %v; want 0, nil", reported, err)
	}
}
