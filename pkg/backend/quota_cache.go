package backend

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	// quotaConfigCacheRefreshInterval is how often the background goroutine
	// polls the tenant quota config version from the central DB.
	quotaConfigCacheRefreshInterval = 30 * time.Second
)

type quotaConfigSnapshot struct {
	config  *QuotaConfigView
	version string
}

// quotaConfigCache is a per-tenant cache for low-frequency quota config.
//
// It intentionally does not cache usage counters. In multi-server deployments,
// storage_bytes/reserved_bytes/media_file_count are high-churn shared state, so
// quota checks read those counters from the central DB directly. This cache only
// removes repeated config reads and uses version polling so config changes
// converge without a cross-server invalidation channel.
type quotaConfigCache struct {
	tenantID string
	store    MetaQuotaStore

	mu       sync.RWMutex
	snapshot *quotaConfigSnapshot

	cancel context.CancelFunc
	done   chan struct{}
}

// newQuotaConfigCache creates and starts a background-refreshing config cache.
// Performs one best-effort synchronous initial load so the first quota check
// usually has config available. If the initial load fails, callers fail open.
func newQuotaConfigCache(tenantID string, store MetaQuotaStore) *quotaConfigCache {
	ctx, cancel := context.WithCancel(context.Background())
	c := &quotaConfigCache{
		tenantID: tenantID,
		store:    store,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	c.refresh(ctx)
	go c.run(ctx)
	return c
}

// get returns a copy of the cached quota config. Returns nil when the cache has
// not been populated yet, allowing callers to fail open or fall back.
func (c *quotaConfigCache) get() *QuotaConfigView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snapshot == nil || c.snapshot.config == nil {
		return nil
	}
	cfg := *c.snapshot.config
	return &cfg
}

func (c *quotaConfigCache) stop() {
	c.cancel()
	<-c.done
}

func (c *quotaConfigCache) run(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(quotaConfigCacheRefreshInterval)
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

func (c *quotaConfigCache) refresh(ctx context.Context) {
	start := time.Now()
	version, err := c.store.GetQuotaConfigVersion(ctx, c.tenantID)
	if err != nil {
		logger.Warn(ctx, "quota_config_cache_version_failed",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordOperation("quota_config_cache", "refresh", "version_error", time.Since(start))
		return
	}

	c.mu.RLock()
	snapshot := c.snapshot
	if snapshot != nil && snapshot.version == version {
		c.mu.RUnlock()
		metrics.RecordOperation("quota_config_cache", "refresh", "unchanged", time.Since(start))
		return
	}
	c.mu.RUnlock()

	cfg, err := c.store.GetQuotaConfig(ctx, c.tenantID)
	if err != nil {
		logger.Warn(ctx, "quota_config_cache_config_failed",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordOperation("quota_config_cache", "refresh", "config_error", time.Since(start))
		return
	}
	c.mu.Lock()
	c.snapshot = &quotaConfigSnapshot{config: cfg, version: version}
	c.mu.Unlock()
	metrics.RecordOperation("quota_config_cache", "refresh", "ok", time.Since(start))
}
