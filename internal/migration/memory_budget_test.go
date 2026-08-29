package migration

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryBudgetAccountsPeakReleasesAndCancellation(t *testing.T) {
	budget, err := newMemoryBudget(100)
	if err != nil {
		t.Fatal(err)
	}
	releaseA, err := budget.Acquire(context.Background(), 60)
	if err != nil {
		t.Fatal(err)
	}
	releaseB, err := budget.Acquire(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := budget.Acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	used, peak, limit := budget.Snapshot()
	if used != 100 || peak != 100 || limit != 100 {
		t.Fatalf("snapshot = %d/%d/%d", used, peak, limit)
	}
	releaseA()
	releaseA()
	releaseB()
	used, peak, _ = budget.Snapshot()
	if used != 0 || peak != 100 {
		t.Fatalf("released snapshot = %d/%d", used, peak)
	}
	if _, err := budget.Acquire(context.Background(), 101); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatalf("oversized error = %v", err)
	}
}
