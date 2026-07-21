package meta

import (
	"context"
	"testing"
)

func TestMonthlyLLMCostMillicents(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	// Seed the pre-aggregated counter the way production does.
	if err := s.IncrMonthlyLLMCost(ctx, "t1", 1500); err != nil {
		t.Fatal(err)
	}
	if err := s.IncrMonthlyLLMCost(ctx, "t1", 2500); err != nil {
		t.Fatal(err)
	}
	// Different tenant.
	if err := s.IncrMonthlyLLMCost(ctx, "t2", 9999); err != nil {
		t.Fatal(err)
	}

	total, err := s.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 4000 {
		t.Fatalf("got %d, want 4000", total)
	}

	// t2 should have its own total.
	total2, err := s.MonthlyLLMCostMillicents(ctx, "t2")
	if err != nil {
		t.Fatal(err)
	}
	if total2 != 9999 {
		t.Fatalf("got %d, want 9999", total2)
	}

	// Tenant without a counter row returns 0.
	total3, err := s.MonthlyLLMCostMillicents(ctx, "t-nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if total3 != 0 {
		t.Fatalf("got %d, want 0", total3)
	}
}
