package backend

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	// defaultQuotaConfigCacheRefreshInterval is the default TTL for lazily
	// loaded tenant quota config. Override with DRIVE9_QUOTA_CACHE_REFRESH_SECONDS.
	defaultQuotaConfigCacheRefreshInterval = 30 * time.Second
	// quotaConfigCacheLoadTimeout bounds a coalesced refresh independently of
	// the request that happened to claim load ownership.
	quotaConfigCacheLoadTimeout = 5 * time.Second
	// defaultQuotaConfigCacheAsyncRefreshSlots bounds detached warm-cache
	// refreshes so a MetaDB slowdown cannot create one in-flight query per
	// resident tenant. Keep headroom in the shared metadata connection pool for
	// foreground requests and other control-plane work.
	defaultQuotaConfigCacheAsyncRefreshSlots = 50
	// quotaConfigCacheSlotRetryInterval prevents a tenant that could not claim
	// an async refresh slot from retrying on every request while the budget is
	// exhausted.
	quotaConfigCacheSlotRetryInterval = time.Second
	// quotaConfigCacheFailureRetryInterval keeps quota changes responsive after
	// a transient MetaDB failure. Successful refreshes retain the normal TTL.
	quotaConfigCacheFailureRetryInterval = 5 * time.Second
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
	quotaConfigCacheRefreshInterval   = defaultQuotaConfigCacheRefreshInterval
	quotaUsageCacheTTL                = defaultQuotaUsageCacheTTL
	quotaPendingDeltasCacheTTL        = defaultQuotaPendingDeltasCacheTTL
	quotaConfigCacheAsyncRefreshSlots = make(chan struct{}, defaultQuotaConfigCacheAsyncRefreshSlots)
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

func cloneQuotaConfigView(cfg *QuotaConfigView) *QuotaConfigView {
	if cfg == nil {
		return nil
	}
	cp := *cfg
	return &cp
}

// quotaConfigCacheRefreshDelay spreads refreshes over the last 10% of the
// configured TTL. The delay never exceeds the configured value, preserving
// the maximum quota-config convergence window.
func quotaConfigCacheRefreshDelay(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	maxJitter := ttl / 10
	if maxJitter <= 0 {
		return ttl
	}
	return ttl - time.Duration(rand.Int64N(int64(maxJitter)+1))
}

// quotaConfigCacheFailureRetryDelay spreads failed loads symmetrically around
// the five-second retry target. Unlike successful refreshes, failures may be
// retried slightly after the target so independently warmed tenants do not
// converge into a synchronized retry wave.
func quotaConfigCacheFailureRetryDelay(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	jitter := ttl / 10
	if jitter <= 0 {
		return ttl
	}
	return ttl - jitter + time.Duration(rand.Int64N(int64(2*jitter)+1))
}

// quotaConfigCache is a passive per-tenant cache for low-frequency quota
// config. Requests refresh an expired snapshot; idle tenants create no
// goroutines and issue no quota queries.
type quotaConfigCache struct {
	tenantID       string
	tidbCloudOrgID string
	store          MetaQuotaStore

	mu          sync.RWMutex
	snapshot    *QuotaConfigView
	nextRefresh time.Time
	loadDone    chan struct{}
}

// newQuotaConfigCache creates an empty request-driven config cache. Backend
// construction stays cheap: the first quota check loads config on demand.
func newQuotaConfigCache(tenantID, tidbCloudOrgID string, store MetaQuotaStore) *quotaConfigCache {
	return &quotaConfigCache{
		tenantID:       tenantID,
		tidbCloudOrgID: normalizeTenantMetricTiDBCloudOrgID(tidbCloudOrgID),
		store:          store,
	}
}

// cached returns the current snapshot and whether another store read is
// suppressed until nextRefresh. A nil snapshot can still be current during
// the short retry cooldown after an initial load failure.
func (c *quotaConfigCache) cached(now time.Time) (*QuotaConfigView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !now.Before(c.nextRefresh) {
		return nil, false
	}
	if c.snapshot == nil {
		return nil, true
	}
	return cloneQuotaConfigView(c.snapshot), true
}

// get returns a defensive copy of the cached config, refreshing it once when
// its TTL has expired. Warm callers serve stale config while a refresh is in
// flight; cold callers wait only until their own context is done.
func (c *quotaConfigCache) get(ctx context.Context) *QuotaConfigView {
	now := time.Now()
	if cfg, current := c.cached(now); current {
		return cfg
	}

	c.mu.Lock()
	now = time.Now()
	if now.Before(c.nextRefresh) {
		cfg := cloneQuotaConfigView(c.snapshot)
		c.mu.Unlock()
		return cfg
	}
	if c.loadDone != nil {
		done := c.loadDone
		stale := cloneQuotaConfigView(c.snapshot)
		c.mu.Unlock()
		if stale != nil {
			return stale
		}
		select {
		case <-done:
			c.mu.RLock()
			cfg := cloneQuotaConfigView(c.snapshot)
			c.mu.RUnlock()
			return cfg
		case <-ctx.Done():
			return nil
		}
	}
	c.loadDone = make(chan struct{})
	c.mu.Unlock()

	start := now
	if stale := c.snapshotCopy(); stale != nil {
		if !tryAcquireQuotaConfigAsyncRefreshSlot() {
			c.deferAsyncRefresh(start)
			return stale
		}
		go func() {
			defer releaseQuotaConfigAsyncRefreshSlot()
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.Error(context.Background(), "quota_config_cache_async_load_panicked",
						zap.String("tenant_id", c.tenantID), zap.Any("panic", recovered))
				}
			}()
			c.loadConfig(context.WithoutCancel(ctx), start)
		}()
		return stale
	}
	return c.loadConfig(ctx, start)
}

func tryAcquireQuotaConfigAsyncRefreshSlot() bool {
	select {
	case quotaConfigCacheAsyncRefreshSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func releaseQuotaConfigAsyncRefreshSlot() {
	<-quotaConfigCacheAsyncRefreshSlots
}

func (c *quotaConfigCache) deferAsyncRefresh(start time.Time) {
	c.mu.Lock()
	if c.loadDone != nil {
		c.nextRefresh = time.Now().Add(quotaConfigCacheSlotRetryInterval)
		c.finishConfigLoadLocked()
	}
	c.mu.Unlock()
	metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "quota_config_cache", "load", "deferred", time.Since(start))
}

func (c *quotaConfigCache) snapshotCopy() *QuotaConfigView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneQuotaConfigView(c.snapshot)
}

func (c *quotaConfigCache) loadConfig(ctx context.Context, start time.Time) (result *QuotaConfigView) {
	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quotaConfigCacheLoadTimeout)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			c.finishPanickedLoad(start)
			panic(recovered)
		}
	}()

	cfg, err := c.store.GetQuotaConfig(loadCtx, c.tenantID)
	if err != nil {
		logger.Warn(loadCtx, "quota_config_cache_config_failed",
			zap.String("tenant_id", c.tenantID), zap.Error(err))
		return c.finishFailedLoad(start, "config_error")
	}
	if cfg == nil {
		return c.finishFailedLoad(start, "config_empty")
	}
	c.mu.Lock()
	c.snapshot = cloneQuotaConfigView(cfg)
	c.nextRefresh = time.Now().Add(quotaConfigCacheRefreshDelay(quotaConfigCacheRefreshInterval))
	c.finishConfigLoadLocked()
	c.mu.Unlock()
	metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "quota_config_cache", "load", "ok", time.Since(start))
	return cloneQuotaConfigView(cfg)
}

func (c *quotaConfigCache) finishFailedLoad(start time.Time, result string) *QuotaConfigView {
	c.mu.Lock()
	c.nextRefresh = time.Now().Add(quotaConfigCacheFailureRetryDelay(quotaConfigCacheFailureRetryInterval))
	var stale *QuotaConfigView
	if c.snapshot != nil {
		stale = cloneQuotaConfigView(c.snapshot)
	}
	c.finishConfigLoadLocked()
	c.mu.Unlock()
	metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "quota_config_cache", "load", result, time.Since(start))
	return stale
}

func (c *quotaConfigCache) finishPanickedLoad(start time.Time) {
	c.mu.Lock()
	if c.loadDone == nil {
		c.mu.Unlock()
		return
	}
	c.nextRefresh = time.Now().Add(quotaConfigCacheFailureRetryDelay(quotaConfigCacheFailureRetryInterval))
	c.finishConfigLoadLocked()
	c.mu.Unlock()
	metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "quota_config_cache", "load", "panic_error", time.Since(start))
}

// finishConfigLoadLocked publishes the completed load before waking cold
// waiters. c.mu must be held.
func (c *quotaConfigCache) finishConfigLoadLocked() {
	done := c.loadDone
	c.loadDone = nil
	close(done)
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
	tenantID       string
	tidbCloudOrgID string
	store          MetaQuotaStore
	ttl            time.Duration

	mu       sync.RWMutex
	snapshot *quotaUsageSnapshot
	loadMu   sync.Mutex
}

func newQuotaUsageCache(tenantID, tidbCloudOrgID string, store MetaQuotaStore, ttl time.Duration) *quotaUsageCache {
	if ttl <= 0 {
		ttl = quotaUsageCacheTTL
	}
	return &quotaUsageCache{tenantID: tenantID, tidbCloudOrgID: normalizeTenantMetricTiDBCloudOrgID(tidbCloudOrgID), store: store, ttl: ttl}
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
		metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "server_quota", "usage_cache", "load_error", time.Since(start))
		return nil
	}
	if usage == nil {
		metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "server_quota", "usage_cache", "load_empty", time.Since(start))
		return nil
	}
	copied := *usage
	c.mu.Lock()
	c.snapshot = &quotaUsageSnapshot{
		usage:     &copied,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
	metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "server_quota", "usage_cache", "load_ok", time.Since(start))
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
	tenantID       string
	tidbCloudOrgID string
	load           quotaPendingDeltasLoader
	ttl            time.Duration
	pendingTTL     time.Duration

	mu                  sync.RWMutex
	snapshot            *quotaPendingDeltasSnapshot
	generation          uint64
	localDeltas         quotaPendingDeltas
	localPositiveDeltas quotaPendingDeltas
	localEntries        []quotaPendingDeltaEntry
	loadMu              sync.Mutex
}

func newQuotaPendingDeltasCache(tenantID, tidbCloudOrgID string, load quotaPendingDeltasLoader, ttl time.Duration) *quotaPendingDeltasCache {
	if ttl <= 0 {
		ttl = quotaPendingDeltasCacheTTL
	}
	return &quotaPendingDeltasCache{tenantID: tenantID, tidbCloudOrgID: normalizeTenantMetricTiDBCloudOrgID(tidbCloudOrgID), load: load, ttl: ttl, pendingTTL: localPendingMutationTTL()}
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
		metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "server_quota", "pending_delta_cache", "load_error", time.Since(start))
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
		metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "server_quota", "pending_delta_cache", "raced_local_delta", time.Since(start))
		return deltas, true
	}
	c.snapshot = &quotaPendingDeltasSnapshot{
		deltas:    deltas,
		expiresAt: expiresAt,
	}
	c.mu.Unlock()
	metrics.RecordTenantOperationWithOrg(c.tenantID, c.tidbCloudOrgID, "server_quota", "pending_delta_cache", "load_ok", time.Since(start))
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
	c.localPositiveDeltas.add(deltas.positivePart())
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
	c.localPositiveDeltas.add(deltas.positivePart().negate())
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
		c.localPositiveDeltas.add(entry.deltas.positivePart().negate())
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

func (d quotaPendingDeltas) positivePart() quotaPendingDeltas {
	return quotaPendingDeltas{
		storageDelta: maxInt64(d.storageDelta, 0),
		fileDelta:    maxInt64(d.fileDelta, 0),
		mediaDelta:   maxInt64(d.mediaDelta, 0),
	}
}

func (d quotaPendingDeltas) negate() quotaPendingDeltas {
	return quotaPendingDeltas{
		storageDelta: -d.storageDelta,
		fileDelta:    -d.fileDelta,
		mediaDelta:   -d.mediaDelta,
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
