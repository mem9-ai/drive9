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

func TestLLMCostCacheFreshHit(t *testing.T) {
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

func TestLLMCostCacheStaleHit(t *testing.T) {
	s := newControlStore(t)
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 2000, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	cache := NewLLMCostCache(s, "t1", 1*time.Hour) // long TTL so cache won't expire

	// Populate cache.
	total, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 2000 {
		t.Fatalf("got %d, want 2000", total)
	}

	// Close the store's DB to simulate meta store failure.
	_ = s.Close()

	// Cache should return stale value.
	total, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatalf("expected stale cache hit (no error), got err: %v", err)
	}
	if total != 2000 {
		t.Fatalf("stale cache: got %d, want 2000", total)
	}
}

func TestLLMCostCacheExpired_FailOpen(t *testing.T) {
	s := newControlStore(t)
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 3000, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	// Use a very short TTL that will expire immediately.
	cache := NewLLMCostCache(s, "t1", 1*time.Nanosecond)

	// Populate cache.
	total, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 3000 {
		t.Fatalf("got %d, want 3000", total)
	}

	// Wait for cache to expire.
	time.Sleep(10 * time.Millisecond)

	// Close the store's DB to simulate meta store failure.
	_ = s.Close()

	// Cache is expired + meta store down → fail-open (return 0, nil).
	total, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatalf("expected fail-open (no error), got err: %v", err)
	}
	if total != 0 {
		t.Fatalf("fail-open: got %d, want 0", total)
	}
}

func TestLLMCostCacheInsertAdvancesStaleFallback(t *testing.T) {
	s := newControlStore(t)
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 4900, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	cache := NewLLMCostCache(s, "t1", 1*time.Hour)

	// Populate cache with 4900.
	total, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 4900 {
		t.Fatalf("got %d, want 4900", total)
	}

	// Insert 200 via cache — should advance cached total to 5100.
	if err := cache.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-2", 200, 50, "tokens"); err != nil {
		t.Fatal(err)
	}

	// Close the store to simulate meta store failure.
	_ = s.Close()

	// Stale fallback should return 5100 (4900 + 200), not 4900.
	total, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatalf("expected stale cache hit (no error), got err: %v", err)
	}
	if total != 5100 {
		t.Fatalf("stale cache after insert: got %d, want 5100", total)
	}
}

func TestLLMCostCacheColdStart_FailOpen(t *testing.T) {
	s := newControlStore(t)

	// Close immediately — meta store is unreachable from the start.
	_ = s.Close()

	cache := NewLLMCostCache(s, "t1", 30*time.Second)
	ctx := context.Background()

	// Cold start + meta store down → fail-open (return 0, nil).
	total, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatalf("expected fail-open (no error), got err: %v", err)
	}
	if total != 0 {
		t.Fatalf("cold-start fail-open: got %d, want 0", total)
	}
}
