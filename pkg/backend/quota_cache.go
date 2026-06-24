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
	// quotaPendingDeltasCacheTTL bounds tenant-local pending outbox aggregate
	// reuse for soft small-write checks. The cache is adjusted for mutations
	// enqueued/acked by this backend instance and periodically reloads to see
	// other servers.
	quotaPendingDeltasCacheTTL = 100 * time.Millisecond
)

type quotaConfigSnapshot struct {
	config  *QuotaConfigView
	version string
}

func cloneQuotaConfigView(cfg *QuotaConfigView) *QuotaConfigView {
	if cfg == nil {
		return nil
	}
	cp := *cfg
	return &cp
}

// quotaConfigCache is a per-tenant cache for low-frequency quota config. It
// only removes repeated config reads and uses version polling so config changes
// converge without a cross-server invalidation channel.
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
	return cloneQuotaConfigView(c.snapshot.config)
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
	if c.snapshot != nil && c.snapshot.config != nil {
		existing := cloneQuotaConfigView(c.snapshot.config)
		c.mu.Unlock()
		metrics.RecordOperation("quota_config_cache", "load", "raced_refresh", time.Since(start))
		return existing
	}
	c.snapshot = &quotaConfigSnapshot{config: cloneQuotaConfigView(cfg), version: ""}
	c.mu.Unlock()
	metrics.RecordOperation("quota_config_cache", "load", "ok", time.Since(start))
	return cloneQuotaConfigView(cfg)
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
	c.snapshot = &quotaConfigSnapshot{config: cloneQuotaConfigView(cfg), version: version}
	c.mu.Unlock()
	metrics.RecordOperation("quota_config_cache", "refresh", "ok", time.Since(start))
}

type quotaUsageSnapshot struct {
	usage     *QuotaUsageView
	expiresAt time.Time
}

// quotaUsageCache is used only by soft small-write admission checks. It avoids
// one central DB read per tiny write while keeping the stale window short.
// In multi-server deployments, each backend can briefly admit writes against a
// usage snapshot that is stale by at most ttl; strict upload reservations must
// continue to call loadQuotaUsage directly.
type quotaUsageCache struct {
	tenantID string
	store    MetaQuotaStore
	ttl      time.Duration

	mu       sync.RWMutex
	snapshot *quotaUsageSnapshot
	loadMu   sync.Mutex
}

func newQuotaUsageCache(tenantID string, store MetaQuotaStore, ttl time.Duration) *quotaUsageCache {
	if ttl <= 0 {
		ttl = quotaUsageCacheTTL
	}
	return &quotaUsageCache{tenantID: tenantID, store: store, ttl: ttl}
}

func (c *quotaUsageCache) get(ctx context.Context) *QuotaUsageView {
	now := time.Now()
	if usage := c.cached(now); usage != nil {
		return usage
	}

	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	now = time.Now()
	if usage := c.cached(now); usage != nil {
		return usage
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
	copied := *usage
	c.mu.Lock()
	c.snapshot = &quotaUsageSnapshot{
		usage:     &copied,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
	metrics.RecordOperation("server_quota", "usage_cache", "load_ok", time.Since(start))
	return cloneQuotaUsageView(usage)
}

func (c *quotaUsageCache) cached(now time.Time) *QuotaUsageView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snapshot != nil && c.snapshot.usage != nil && now.Before(c.snapshot.expiresAt) {
		return cloneQuotaUsageView(c.snapshot.usage)
	}
	return nil
}

func cloneQuotaUsageView(usage *QuotaUsageView) *QuotaUsageView {
	if usage == nil {
		return nil
	}
	copied := *usage
	return &copied
}

type quotaPendingDeltas struct {
	storageDelta int64
	fileDelta    int64
	mediaDelta   int64
}

type quotaPendingDeltasSnapshot struct {
	deltas    quotaPendingDeltas
	expiresAt time.Time
}

type quotaPendingDeltasLoader func(context.Context) (storageDelta, fileDelta, mediaDelta int64, err error)

// quotaPendingDeltasCache avoids SUM(quota_outbox) on every small write. It is
// only used by soft admission checks; strict upload reservation still reads the
// tenant DB directly.
type quotaPendingDeltasCache struct {
	load quotaPendingDeltasLoader
	ttl  time.Duration

	mu       sync.Mutex
	snapshot *quotaPendingDeltasSnapshot
}

func newQuotaPendingDeltasCache(load quotaPendingDeltasLoader, ttl time.Duration) *quotaPendingDeltasCache {
	if ttl <= 0 {
		ttl = quotaPendingDeltasCacheTTL
	}
	return &quotaPendingDeltasCache{load: load, ttl: ttl}
}

func (c *quotaPendingDeltasCache) get(ctx context.Context) (quotaPendingDeltas, bool) {
	if c == nil || c.load == nil {
		return quotaPendingDeltas{}, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshot != nil && now.Before(c.snapshot.expiresAt) {
		return c.snapshot.deltas, true
	}

	start := time.Now()
	storageDelta, fileDelta, mediaDelta, err := c.load(ctx)
	if err != nil {
		logger.Warn(ctx, "server_quota_pending_outbox_delta_fail_open", zap.Error(err))
		metrics.RecordOperation("server_quota", "pending_delta_cache", "load_error", time.Since(start))
		return quotaPendingDeltas{}, false
	}
	deltas := quotaPendingDeltas{
		storageDelta: storageDelta,
		fileDelta:    fileDelta,
		mediaDelta:   mediaDelta,
	}
	c.snapshot = &quotaPendingDeltasSnapshot{
		deltas:    deltas,
		expiresAt: now.Add(c.ttl),
	}
	metrics.RecordOperation("server_quota", "pending_delta_cache", "load_ok", time.Since(start))
	return deltas, true
}

func (c *quotaPendingDeltasCache) add(storageDelta, fileDelta, mediaDelta int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// If the snapshot is missing or expired, leave it cold. The next soft
	// admission check reloads from quota_outbox and sees this backend's queued
	// row along with rows from other servers.
	if c.snapshot == nil || time.Now().After(c.snapshot.expiresAt) {
		return
	}
	c.snapshot.deltas.storageDelta += storageDelta
	c.snapshot.deltas.fileDelta += fileDelta
	c.snapshot.deltas.mediaDelta += mediaDelta
}
