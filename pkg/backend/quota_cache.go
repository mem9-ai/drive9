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
	// defaultQuotaConfigCacheRefreshInterval is the default interval for the
	// background goroutine that polls the tenant quota config version from
	// the central DB. Override with DRIVE9_QUOTA_CACHE_REFRESH_SECONDS.
	defaultQuotaConfigCacheRefreshInterval = 30 * time.Second
	// quotaUsageCacheTTL bounds how long soft small-write quota checks may
	// reuse central usage counters. Strict upload reservations still read
	// central usage directly.
	defaultQuotaUsageCacheTTL = 250 * time.Millisecond
	// quotaPendingDeltasCacheTTL bounds tenant-local pending outbox aggregate
	// reuse for soft small-write checks. The cache is adjusted for mutations
	// enqueued/acked by this backend instance and periodically reloads to see
	// other servers.
	defaultQuotaPendingDeltasCacheTTL = 250 * time.Millisecond
)

// quotaConfigCacheRefreshInterval is the resolved refresh interval (package-level
// var so it can be set from env at startup).
var (
	quotaConfigCacheRefreshInterval = defaultQuotaConfigCacheRefreshInterval
	quotaUsageCacheTTL              = defaultQuotaUsageCacheTTL
	quotaPendingDeltasCacheTTL      = defaultQuotaPendingDeltasCacheTTL
)

// InitQuotaConfigCacheRefreshInterval overrides the default refresh interval.
// seconds <= 0 keeps the default (30s). Must be called before any backend
// is created (before quota caches are instantiated).
func InitQuotaConfigCacheRefreshInterval(seconds int) {
	if seconds > 0 {
		quotaConfigCacheRefreshInterval = time.Duration(seconds) * time.Second
	}
}

// InitQuotaAdmissionCacheTTLs overrides soft quota admission cache TTLs. The
// caches are used only for small-write admission; strict upload reservations
// continue to read current central and tenant-local quota state directly.
func InitQuotaAdmissionCacheTTLs(usageTTL, pendingDeltasTTL time.Duration) {
	if usageTTL < 0 {
		logger.Warn(context.Background(), "quota_usage_cache_ttl_invalid", zap.Duration("ttl", usageTTL))
	} else if usageTTL > 0 {
		quotaUsageCacheTTL = usageTTL
	}
	if pendingDeltasTTL < 0 {
		logger.Warn(context.Background(), "quota_pending_deltas_cache_ttl_invalid", zap.Duration("ttl", pendingDeltasTTL))
	} else if pendingDeltasTTL > 0 {
		quotaPendingDeltasCacheTTL = pendingDeltasTTL
	}
}

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
		metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "load", "config_error", time.Since(start))
		return nil
	}
	if cfg == nil {
		metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "load", "config_empty", time.Since(start))
		return nil
	}
	c.mu.Lock()
	if c.snapshot != nil && c.snapshot.config != nil {
		existing := cloneQuotaConfigView(c.snapshot.config)
		c.mu.Unlock()
		metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "load", "raced_refresh", time.Since(start))
		return existing
	}
	c.snapshot = &quotaConfigSnapshot{config: cloneQuotaConfigView(cfg), version: ""}
	c.mu.Unlock()
	metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "load", "ok", time.Since(start))
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
		metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "refresh", "version_error", time.Since(start))
		return
	}

	c.mu.RLock()
	snapshot := c.snapshot
	if snapshot != nil && snapshot.version == version {
		c.mu.RUnlock()
		metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "refresh", "unchanged", time.Since(start))
		return
	}
	c.mu.RUnlock()

	cfg, err := c.store.GetQuotaConfig(ctx, c.tenantID)
	if err != nil {
		logger.Warn(ctx, "quota_config_cache_config_failed",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "refresh", "config_error", time.Since(start))
		return
	}
	c.mu.Lock()
	c.snapshot = &quotaConfigSnapshot{config: cloneQuotaConfigView(cfg), version: version}
	c.mu.Unlock()
	metrics.RecordTenantOperation(c.tenantID, "quota_config_cache", "refresh", "ok", time.Since(start))
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
		metrics.RecordTenantOperation(c.tenantID, "server_quota", "usage_cache", "load_error", time.Since(start))
		return nil
	}
	if usage == nil {
		metrics.RecordTenantOperation(c.tenantID, "server_quota", "usage_cache", "load_empty", time.Since(start))
		return nil
	}
	copied := *usage
	c.mu.Lock()
	c.snapshot = &quotaUsageSnapshot{
		usage:     &copied,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
	metrics.RecordTenantOperation(c.tenantID, "server_quota", "usage_cache", "load_ok", time.Since(start))
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

func (c *quotaUsageCache) invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.snapshot = nil
	c.mu.Unlock()
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

type quotaPendingDeltaEntry struct {
	deltas    quotaPendingDeltas
	expiresAt time.Time
}

type quotaPendingDeltasLoader func(context.Context) (storageDelta, fileDelta, mediaDelta int64, err error)

// quotaPendingDeltasCache tracks this process's central quota mutations that
// have been logged but not yet applied. It deliberately has no tenant-DB
// fallback: runtime quota admission must not SUM quota_outbox from the user DB.
type quotaPendingDeltasCache struct {
	tenantID   string
	load       quotaPendingDeltasLoader
	ttl        time.Duration
	pendingTTL time.Duration

	mu                  sync.RWMutex
	snapshot            *quotaPendingDeltasSnapshot
	generation          uint64
	localDeltas         quotaPendingDeltas
	localPositiveDeltas quotaPendingDeltas
	localEntries        []quotaPendingDeltaEntry
	loadMu              sync.Mutex
}

func newQuotaPendingDeltasCache(tenantID string, load quotaPendingDeltasLoader, ttl time.Duration) *quotaPendingDeltasCache {
	if ttl <= 0 {
		ttl = quotaPendingDeltasCacheTTL
	}
	return &quotaPendingDeltasCache{tenantID: tenantID, load: load, ttl: ttl, pendingTTL: localPendingMutationTTL()}
}

func localPendingMutationTTL() time.Duration {
	ttl := replayMinAge() + replayPollInterval() + quotaPendingDeltasCacheTTL
	if ttl <= 0 {
		return defaultReplayMinAge + defaultReplayPollInterval + defaultQuotaPendingDeltasCacheTTL
	}
	return ttl
}

func (c *quotaPendingDeltasCache) get(ctx context.Context) (quotaPendingDeltas, bool) {
	if c == nil {
		return quotaPendingDeltas{}, false
	}
	if c.load == nil {
		// Expiry mutates localDeltas/localEntries, so the hot no-loader path
		// needs the write lock even though callers are logically reading.
		c.mu.Lock()
		defer c.mu.Unlock()
		c.expireLocalDeltasLocked(time.Now())
		return c.localDeltas, true
	}
	now := time.Now()
	c.mu.Lock()
	c.expireLocalDeltasLocked(now)
	c.mu.Unlock()
	if deltas, ok := c.cached(now); ok {
		return deltas, true
	}

	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	now = time.Now()
	if deltas, ok := c.cached(now); ok {
		return deltas, true
	}

	c.mu.RLock()
	generation := c.generation
	localPositiveDeltas := c.localPositiveDeltas
	c.mu.RUnlock()

	start := time.Now()
	storageDelta, fileDelta, mediaDelta, err := c.load(ctx)
	if err != nil {
		logger.Warn(ctx, "server_quota_pending_outbox_delta_fail_open", zap.String("tenant_id", c.tenantID), zap.Error(err))
		metrics.RecordTenantOperation(c.tenantID, "server_quota", "pending_delta_cache", "load_error", time.Since(start))
		return quotaPendingDeltas{}, false
	}
	deltas := quotaPendingDeltas{
		storageDelta: storageDelta,
		fileDelta:    fileDelta,
		mediaDelta:   mediaDelta,
	}
	expiresAt := time.Now().Add(c.ttl)
	c.mu.Lock()
	if c.generation != generation {
		deltas.add(c.localPositiveDeltas.sub(localPositiveDeltas))
		c.snapshot = &quotaPendingDeltasSnapshot{
			deltas:    deltas,
			expiresAt: expiresAt,
		}
		c.mu.Unlock()
		// A local mutation raced this DB load. This legacy loader path is kept
		// only for tests; runtime central quota uses in-memory deltas with no
		// tenant DB read.
		metrics.RecordTenantOperation(c.tenantID, "server_quota", "pending_delta_cache", "raced_local_delta", time.Since(start))
		return deltas, true
	}
	c.snapshot = &quotaPendingDeltasSnapshot{
		deltas:    deltas,
		expiresAt: expiresAt,
	}
	c.mu.Unlock()
	metrics.RecordTenantOperation(c.tenantID, "server_quota", "pending_delta_cache", "load_ok", time.Since(start))
	return deltas, true
}

func (c *quotaPendingDeltasCache) cached(now time.Time) (quotaPendingDeltas, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snapshot != nil && now.Before(c.snapshot.expiresAt) {
		return c.snapshot.deltas, true
	}
	return quotaPendingDeltas{}, false
}

func (c *quotaPendingDeltasCache) addPending(storageDelta, fileDelta, mediaDelta int64) {
	if c == nil {
		return
	}
	deltas := quotaPendingDeltas{
		storageDelta: storageDelta,
		fileDelta:    fileDelta,
		mediaDelta:   mediaDelta,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.expireLocalDeltasLocked(now)
	c.generation++
	c.localDeltas.add(deltas)
	c.localPositiveDeltas.add(quotaPendingDeltas{
		storageDelta: maxInt64(storageDelta, 0),
		fileDelta:    maxInt64(fileDelta, 0),
		mediaDelta:   maxInt64(mediaDelta, 0),
	})
	if !deltas.zero() {
		c.localEntries = append(c.localEntries, quotaPendingDeltaEntry{
			deltas:    deltas,
			expiresAt: now.Add(c.pendingTTL),
		})
	}
	// If the snapshot is missing or expired, leave it cold. The no-loader
	// runtime path reads localDeltas directly; the legacy loader path refreshes
	// snapshots on demand.
	if c.snapshot == nil || now.After(c.snapshot.expiresAt) {
		return
	}
	c.snapshot.deltas.storageDelta += storageDelta
	c.snapshot.deltas.fileDelta += fileDelta
	c.snapshot.deltas.mediaDelta += mediaDelta
}

func (c *quotaPendingDeltasCache) clearPending(storageDelta, fileDelta, mediaDelta int64) {
	if c == nil {
		return
	}
	deltas := quotaPendingDeltas{
		storageDelta: storageDelta,
		fileDelta:    fileDelta,
		mediaDelta:   mediaDelta,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.expireLocalDeltasLocked(now)
	c.generation++
	if !c.removeLocalEntryLocked(deltas) {
		return
	}
	c.localDeltas.add(quotaPendingDeltas{
		storageDelta: -storageDelta,
		fileDelta:    -fileDelta,
		mediaDelta:   -mediaDelta,
	})
	if c.snapshot == nil || now.After(c.snapshot.expiresAt) {
		return
	}
	c.snapshot.deltas.storageDelta -= storageDelta
	c.snapshot.deltas.fileDelta -= fileDelta
	c.snapshot.deltas.mediaDelta -= mediaDelta
}

func (c *quotaPendingDeltasCache) expireLocalDeltasLocked(now time.Time) {
	if len(c.localEntries) == 0 {
		return
	}
	kept := c.localEntries[:0]
	for _, entry := range c.localEntries {
		if now.Before(entry.expiresAt) {
			kept = append(kept, entry)
			continue
		}
		c.localDeltas.add(quotaPendingDeltas{
			storageDelta: -entry.deltas.storageDelta,
			fileDelta:    -entry.deltas.fileDelta,
			mediaDelta:   -entry.deltas.mediaDelta,
		})
		if c.snapshot != nil && now.Before(c.snapshot.expiresAt) {
			c.snapshot.deltas.storageDelta -= entry.deltas.storageDelta
			c.snapshot.deltas.fileDelta -= entry.deltas.fileDelta
			c.snapshot.deltas.mediaDelta -= entry.deltas.mediaDelta
		}
	}
	c.localEntries = kept
}

func (c *quotaPendingDeltasCache) removeLocalEntryLocked(deltas quotaPendingDeltas) bool {
	// Match by aggregate delta rather than log ID: admission only reads the
	// aggregate total, so same-delta pending entries are interchangeable.
	for i, entry := range c.localEntries {
		if entry.deltas == deltas {
			copy(c.localEntries[i:], c.localEntries[i+1:])
			c.localEntries = c.localEntries[:len(c.localEntries)-1]
			return true
		}
	}
	return false
}

func (d *quotaPendingDeltas) add(other quotaPendingDeltas) {
	d.storageDelta += other.storageDelta
	d.fileDelta += other.fileDelta
	d.mediaDelta += other.mediaDelta
}

func (d quotaPendingDeltas) sub(other quotaPendingDeltas) quotaPendingDeltas {
	return quotaPendingDeltas{
		storageDelta: d.storageDelta - other.storageDelta,
		fileDelta:    d.fileDelta - other.fileDelta,
		mediaDelta:   d.mediaDelta - other.mediaDelta,
	}
}

func (d quotaPendingDeltas) zero() bool {
	return d.storageDelta == 0 && d.fileDelta == 0 && d.mediaDelta == 0
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
