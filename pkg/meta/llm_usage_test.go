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

// TestLLMCostCacheInsertRefreshesFreshness verifies that a successful insert
// resets the cache freshness (fetchedAt) so the stale fallback doesn't expire
// prematurely. Regression test for: insert near TTL expiry should extend the
// cache lifetime, not leave fetchedAt at the original DB read time.
func TestLLMCostCacheInsertRefreshesFreshness(t *testing.T) {
	s := newControlStore(t)
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 4900, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	// Use a short TTL so we can test near-expiry behavior.
	cache := NewLLMCostCache(s, "t1", 100*time.Millisecond)

	// Populate cache at t=0.
	total, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 4900 {
		t.Fatalf("got %d, want 4900", total)
	}

	// Wait until close to TTL expiry.
	time.Sleep(80 * time.Millisecond)

	// Insert near expiry — should refresh fetchedAt to now.
	if err := cache.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-2", 200, 50, "tokens"); err != nil {
		t.Fatal(err)
	}

	// Wait past the original TTL (>100ms from initial read).
	time.Sleep(50 * time.Millisecond)

	// Close DB to force stale fallback.
	_ = s.Close()

	// If fetchedAt was refreshed by the insert, cache is still fresh (only ~50ms
	// since insert, well within 100ms TTL). Stale fallback should return 5100.
	// If fetchedAt was NOT refreshed, the cache expired (>100ms since original
	// read) and we'd get 0 (fail-open).
	total, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatalf("expected stale cache hit, got err: %v", err)
	}
	if total != 5100 {
		t.Fatalf("near-expiry insert freshness: got %d, want 5100", total)
	}
}

// TestLLMCostCacheReadInsertInterleaving is a regression test for the
// read/insert race: a slow DB refresh must NOT overwrite a cache that was
// bumped by a concurrent insert.
//
// Timeline simulated via afterDBRead hook:
//   1. cache = 4900 (populated by initial read)
//   2. goroutine A starts MonthlyLLMCostMillicents, DB returns 4900
//   3. (between DB read and cache update) goroutine B InsertLLMUsage(+200),
//      bumping cache to 5100 and version to 1
//   4. goroutine A tries to overwrite cache with 4900 — version check prevents it
//   5. meta store goes down; stale fallback must return 5100, not 4900
func TestLLMCostCacheReadInsertInterleaving(t *testing.T) {
	s := newControlStore(t)
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 4900, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	cache := NewLLMCostCache(s, "t1", 1*time.Hour)

	// Step 1: populate cache with 4900.
	total, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 4900 {
		t.Fatalf("initial cache: got %d, want 4900", total)
	}

	// Step 2-4: set up the interleaving hook. After the next DB read completes
	// (returning 4900 or 5100 depending on timing), inject a concurrent insert
	// that bumps the cache to 5100 and the version counter.
	hookCalled := false
	cache.afterDBRead = func() {
		hookCalled = true
		// Simulate a concurrent insert happening between DB read and cache update.
		if err := cache.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-2", 200, 50, "tokens"); err != nil {
			t.Errorf("insert in hook: %v", err)
		}
	}

	// This read triggers the hook: DB read completes, then insert bumps cache,
	// then the read tries to overwrite — version check should prevent it.
	_, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !hookCalled {
		t.Fatal("afterDBRead hook was not called")
	}

	// Clear the hook so it doesn't fire again.
	cache.afterDBRead = nil

	// Step 5: close DB to force stale fallback.
	_ = s.Close()

	// The stale fallback must return 5100 (4900 + 200), NOT the DB-read value
	// that the refresh tried to write. This proves the version check prevented
	// the stale overwrite.
	total, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatalf("expected stale cache hit (no error), got err: %v", err)
	}
	if total != 5100 {
		t.Fatalf("stale cache after interleaved insert: got %d, want 5100", total)
	}
}

// TestLLMCostCacheColdCacheInterleaving is a regression test for the cold-cache
// variant of the read/insert interleaving race. When cached is nil, a concurrent
// insert bumps the version but cannot materialize a cache entry (it doesn't know
// the total). The DB read must still install its result so the stale fallback
// has a value to return.
//
// Timeline:
//   1. cache = nil (cold start)
//   2. goroutine A starts MonthlyLLMCostMillicents, snapshots vBefore = 0
//   3. (hook) goroutine B InsertLLMUsage(+200), bumps version to 1, but cached stays nil
//   4. goroutine A's DB read succeeds — must install cache despite version mismatch
//   5. meta store goes down; stale fallback must return the DB result, not 0
func TestLLMCostCacheColdCacheInterleaving(t *testing.T) {
	s := newControlStore(t)
	_, _ = s.DB().Exec("DELETE FROM llm_usage")

	ctx := context.Background()
	// Pre-insert so the DB has a non-zero total to return.
	if err := s.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-1", 3000, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	cache := NewLLMCostCache(s, "t1", 1*time.Hour)
	// cache.cached is nil at this point (cold cache).

	hookCalled := false
	cache.afterDBRead = func() {
		hookCalled = true
		// Simulate a concurrent insert during the DB read.
		// This bumps version but cannot create a cache entry (cached is nil).
		if err := cache.InsertLLMUsage(ctx, "t1", "img_extract_text", "task-2", 200, 50, "tokens"); err != nil {
			t.Errorf("insert in hook: %v", err)
		}
	}

	// DB read succeeds (returns 3000 or 3200 depending on insert timing).
	// Despite version mismatch, the result must be installed because cached is nil.
	_, err := cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !hookCalled {
		t.Fatal("afterDBRead hook was not called")
	}

	// Clear hook and close DB to force stale fallback.
	cache.afterDBRead = nil
	_ = s.Close()

	// Stale fallback must return a non-zero value (the installed DB result),
	// NOT 0 (which would mean the valid DB read was discarded).
	total, err = cache.MonthlyLLMCostMillicents(ctx, "t1")
	if err != nil {
		t.Fatalf("expected stale cache hit (no error), got err: %v", err)
	}
	if total == 0 {
		t.Fatal("cold-cache interleaving: stale fallback returned 0, expected non-zero (DB result was discarded)")
	}
	// The exact value depends on whether the DB read saw the insert (3000 or 3200).
	// Either is acceptable; the key invariant is total != 0.
	t.Logf("cold-cache interleaving: stale fallback returned %d (valid)", total)
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
