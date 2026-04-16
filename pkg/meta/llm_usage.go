package meta

import (
	"context"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/metrics"
)

// InsertLLMUsage records one billable LLM call in the control-plane store.
func (s *Store) InsertLLMUsage(ctx context.Context, tenantID, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_llm_usage", start, &err)
	_, err = s.db.ExecContext(ctx, `INSERT INTO llm_usage (tenant_id, task_type, task_id, cost_millicents, raw_units, raw_unit_type, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		tenantID, taskType, taskID, costMillicents, rawUnits, rawUnitType, time.Now().UTC())
	return err
}

// MonthlyLLMCostMillicents returns the sum of cost_millicents for a tenant in
// the current calendar month (UTC).
func (s *Store) MonthlyLLMCostMillicents(ctx context.Context, tenantID string) (total int64, err error) {
	start := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		metrics.RecordOperation("meta", "monthly_llm_cost", result, time.Since(start))
	}()
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(cost_millicents), 0) FROM llm_usage WHERE tenant_id = ? AND created_at >= ?`,
		tenantID, monthStart).Scan(&total)
	return total, err
}

// llmCostCacheEntry holds a cached monthly cost result with expiry.
type llmCostCacheEntry struct {
	total     int64
	fetchedAt time.Time
}

// LLMCostCache provides a per-process stale cache for monthly LLM cost
// lookups. On meta store failure, it returns the last known value until TTL
// expires, providing fail-open behavior without losing budget enforcement.
// It also delegates InsertLLMUsage to the underlying Store so callers can
// use a single object for both reads and writes.
type LLMCostCache struct {
	store *Store
	ttl   time.Duration

	mu      sync.Mutex
	cached  *llmCostCacheEntry
	version uint64 // bumped on every successful insert; prevents stale DB reads from overwriting insert-advanced cache

	// afterDBRead is a test hook called after the DB read completes but before
	// the cache is updated. It allows tests to inject concurrent inserts to
	// exercise the read/insert interleaving race.
	afterDBRead func()
}

// NewLLMCostCache creates a cache that wraps meta store budget lookups.
func NewLLMCostCache(store *Store, tenantID string, ttl time.Duration) *LLMCostCache {
	// tenantID is accepted for API compatibility but not stored — callers
	// pass tenantID per-call via MonthlyLLMCostMillicents.
	_ = tenantID
	return &LLMCostCache{store: store, ttl: ttl}
}

// InsertLLMUsage delegates to the underlying Store. On success, it advances
// the cached monthly total so that a subsequent stale-cache fallback reflects
// the newly recorded cost.
func (c *LLMCostCache) InsertLLMUsage(ctx context.Context, tenantID, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) error {
	err := c.store.InsertLLMUsage(ctx, tenantID, taskType, taskID, costMillicents, rawUnits, rawUnitType)
	if err == nil && costMillicents > 0 {
		c.mu.Lock()
		if c.cached != nil {
			c.cached = &llmCostCacheEntry{
				total:     c.cached.total + costMillicents,
				fetchedAt: c.cached.fetchedAt,
			}
		}
		c.version++
		c.mu.Unlock()
	}
	return err
}

// MonthlyLLMCostMillicents returns the current monthly cost, using the cache
// on meta store failure. Returns (0, nil) on cold-start + meta store failure
// (fail-open).
func (c *LLMCostCache) MonthlyLLMCostMillicents(ctx context.Context, tenantID string) (int64, error) {
	// Snapshot version before the (potentially slow) DB read so we can detect
	// concurrent inserts that advanced the cache while we were querying.
	c.mu.Lock()
	vBefore := c.version
	c.mu.Unlock()

	total, err := c.store.MonthlyLLMCostMillicents(ctx, tenantID)
	if c.afterDBRead != nil {
		c.afterDBRead()
	}
	if err == nil {
		c.mu.Lock()
		// Only overwrite the cache if no insert bumped the version since the
		// DB read started. This prevents a stale DB result from reverting a
		// more recent insert-advanced total.
		if c.version == vBefore {
			c.cached = &llmCostCacheEntry{total: total, fetchedAt: time.Now()}
		}
		c.mu.Unlock()
		return total, nil
	}
	// Meta store failure: return stale cache if available and not expired.
	c.mu.Lock()
	entry := c.cached
	c.mu.Unlock()
	if entry != nil && time.Since(entry.fetchedAt) < c.ttl {
		metrics.RecordOperation("llm_cost_budget", "cache_stale_hit", "ok", 0)
		return entry.total, nil
	}
	// Cold start or cache expired: fail-open.
	metrics.RecordOperation("llm_cost_budget", "cache_miss_fail_open", "ok", 0)
	return 0, nil
}
