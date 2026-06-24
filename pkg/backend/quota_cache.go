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
	// quotaUsageCacheTTL bounds how long soft small-write quota checks may
	// reuse central usage counters. Strict upload reservations still read
	// central usage directly.
	quotaUsageCacheTTL = 100 * time.Millisecond
)

type quotaConfigSnapshot struct {
	config  *QuotaConfigView
	version string
}

// quotaConfigCache is a per-tenant cache for low-frequency quota config.
//
// It intentionally does not cache usage counters. In multi-server deployments,
// storage_bytes/reserved_bytes/file_count/media_file_count are high-churn shared
// state, so quota checks read those counters from the central DB directly. This
// cache only removes repeated config reads and uses version polling so config
// changes converge without a cross-server invalidation channel.
type quotaConfigCache struct {
	tenantID string
	store    MetaQuotaStore

	mu       sync.RWMutex
	snapshot *quotaConfigSnapshot
	loadMu   sync.Mutex

	cancel context.CancelFunc
	done   chan struct{}
}

// newQuotaConfigCache creates and starts a background-refreshing config cache.
// Backend construction must stay cheap for read-only operations such as ls, so
// the initial load is lazy: the first quota check loads config on demand and
// the background refresher keeps it current after that.
func newQuotaConfigCache(tenantID string, store MetaQuotaStore) *quotaConfigCache {
	ctx, cancel := context.WithCancel(context.Background())
	c := &quotaConfigCache{
		tenantID: tenantID,
		store:    store,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
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

func (c *quotaConfigCache) load(ctx context.Context) *QuotaConfigView {
	if cfg := c.get(); cfg != nil {
		return cfg
	}
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	if cfg := c.get(); cfg != nil {
		return cfg
	}

	start := time.Now()
	cfg, err := c.store.GetQuotaConfig(ctx, c.tenantID)
	if err != nil {
		logger.Warn(ctx, "quota_config_cache_config_failed",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordOperation("quota_config_cache", "load", "config_error", time.Since(start))
		return nil
	}
	if cfg == nil {
		metrics.RecordOperation("quota_config_cache", "load", "config_empty", time.Since(start))
		return nil
	}
	c.mu.Lock()
	version := ""
	if c.snapshot != nil {
		version = c.snapshot.version
	}
	c.snapshot = &quotaConfigSnapshot{config: cfg, version: version}
	c.mu.Unlock()
	metrics.RecordOperation("quota_config_cache", "load", "ok", time.Since(start))
	return cfg
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

type quotaUsageSnapshot struct {
	usage     *QuotaUsageView
	expiresAt time.Time
}

// quotaUsageCache is used only by soft small-write admission checks. It avoids
// one central DB read per 1KB write while keeping the stale window short.
type quotaUsageCache struct {
	tenantID string
	store    MetaQuotaStore
	ttl      time.Duration

	mu       sync.Mutex
	snapshot *quotaUsageSnapshot
}

func newQuotaUsageCache(tenantID string, store MetaQuotaStore, ttl time.Duration) *quotaUsageCache {
	if ttl <= 0 {
		ttl = quotaUsageCacheTTL
	}
	return &quotaUsageCache{tenantID: tenantID, store: store, ttl: ttl}
}

func (c *quotaUsageCache) get(ctx context.Context) *QuotaUsageView {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshot != nil && c.snapshot.usage != nil && now.Before(c.snapshot.expiresAt) {
		usage := *c.snapshot.usage
		return &usage
	}

	start := time.Now()
	usage, err := c.store.GetQuotaUsage(ctx, c.tenantID)
	if err != nil {
		logger.Warn(ctx, "server_quota_usage_fail_open",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordOperation("server_quota", "usage_cache", "load_error", time.Since(start))
		return nil
	}
	if usage == nil {
		metrics.RecordOperation("server_quota", "usage_cache", "load_empty", time.Since(start))
		return nil
	}
	c.snapshot = &quotaUsageSnapshot{
		usage:     usage,
		expiresAt: now.Add(c.ttl),
	}
	copied := *usage
	metrics.RecordOperation("server_quota", "usage_cache", "load_ok", time.Since(start))
	return &copied
}
