package meta

import (
	"context"
	"testing"
	"time"
)

func TestInsertAndQueryLLMUsage(t *testing.T) {
	s := newControlStore(t)
	// Clean llm_usage table.
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 1500, 100, "tokens"); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertLLMUsage(ctx, "t1", "audio_extract_text", "task-2", 2500, 200, "tokens"); err != nil {
		t.Fatal(err)
	}
	// Different tenant.
	if err := s.InsertLLMUsage(ctx, "t2", "img_extract_text", "task-3", 9999, 50, "tokens"); err != nil {
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

	// Non-existent tenant returns 0.
	total3, err := s.MonthlyLLMCostMillicents(ctx, "t-nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if total3 != 0 {
		t.Fatalf("got %d, want 0", total3)
	}
}

func TestLLMCostCacheStaleHit(t *testing.T) {
	s := newControlStore(t)
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 1000, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	cache := NewLLMCostCache(s, "t1", 30*time.Second)

	// First call: populates cache.
	total, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 1000 {
		t.Fatalf("got %d, want 1000", total)
	}

	// Insert via cache delegates to store.
	if err := cache.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-2", 500, 50, "tokens"); err != nil {
		t.Fatal(err)
	}

	// Fresh query should include the new insert.
	total, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 1500 {
		t.Fatalf("got %d, want 1500", total)
	}
}
