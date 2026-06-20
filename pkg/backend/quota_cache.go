package backend

import (
	"context"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	// quotaCacheRefreshInterval is how often the background goroutine
	// refreshes the cached quota snapshot from the central DB.
	quotaCacheRefreshInterval = 30 * time.Second
)

// quotaSnapshot holds a cached copy of the tenant's quota state.
type quotaSnapshot struct {
	usage  *QuotaUsageView
	config *QuotaConfigView
}

// quotaCache is a per-tenant, write-through cache for quota state read from
// the central (server) DB. It replaces per-fsync synchronous DB round trips
// (~37ms each) with in-memory reads refreshed asynchronously.
//
// Safety: the quota check is already fail-open (see ensureStorageQuotaServer).
// A stale cache only widens the accepted-race window that was already accepted
// in the PR #251 review (thread #pr251:000001d1). Hard quota convergence is
// restored by MutationReplayWorker and the backfill-quota CLI tool.
type quotaCache struct {
	tenantID string
	store    MetaQuotaStore

	mu       sync.RWMutex
	snapshot *quotaSnapshot

	cancel context.CancelFunc
	done   chan struct{}
}

// newQuotaCache creates and starts a background-refreshing quota cache.
// Performs one synchronous initial load so the first quota check has data.
// If the initial load fails, the cache starts empty (fail-open).
func newQuotaCache(tenantID string, store MetaQuotaStore) *quotaCache {
	ctx, cancel := context.WithCancel(context.Background())
	c := &quotaCache{
		tenantID: tenantID,
		store:    store,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	// Best-effort initial load.
	c.refresh(ctx)
	go c.run(ctx)
	return c
}

// get returns the cached quota snapshot. Returns nil, nil if the cache has
// not been populated yet (caller should fail-open).
func (c *quotaCache) get() (*QuotaUsageView, *QuotaConfigView) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snapshot == nil {
		return nil, nil
	}
	return c.snapshot.usage, c.snapshot.config
}

// stop shuts down the background refresh goroutine.
func (c *quotaCache) stop() {
	c.cancel()
	<-c.done
}

func (c *quotaCache) run(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(quotaCacheRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

func (c *quotaCache) refresh(ctx context.Context) {
	start := time.Now()
	usage, err := c.store.GetQuotaUsage(ctx, c.tenantID)
	if err != nil {
		logger.Warn(ctx, "quota_cache_refresh_usage_failed",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordOperation("quota_cache", "refresh", "usage_error", time.Since(start))
		return
	}
	cfg, err := c.store.GetQuotaConfig(ctx, c.tenantID)
	if err != nil {
		logger.Warn(ctx, "quota_cache_refresh_config_failed",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordOperation("quota_cache", "refresh", "config_error", time.Since(start))
		return
	}
	c.mu.Lock()
	c.snapshot = &quotaSnapshot{usage: usage, config: cfg}
	c.mu.Unlock()
	metrics.RecordOperation("quota_cache", "refresh", "ok", time.Since(start))
}
