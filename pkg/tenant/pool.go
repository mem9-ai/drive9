package tenant

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/migrate"
	"github.com/mem9-ai/drive9/pkg/mysqlutil"
	"github.com/mem9-ai/drive9/pkg/s3client"
	"github.com/mem9-ai/drive9/pkg/semantic"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
	"go.uber.org/zap"
)

type PoolConfig struct {
	MaxTenants         int
	S3Dir              string
	PublicURL          string
	S3Bucket           string
	S3Region           string
	S3Prefix           string
	S3RoleARN          string
	S3Endpoint         string
	S3ForcePathStyle   bool
	S3AccessKeyID      string
	S3SecretAccessKey  string
	S3SessionToken     string
	S3EncryptionPolicy meta.S3EncryptionPolicy

	BackendOptions backend.Options

	// SkipTiDBSchemaCheck disables the TiDB auto-embedding schema
	// ensure/validate steps during Acquire. Used in tests that run
	// against plain MySQL but need a TiDB-class provider for vault.
	SkipTiDBSchemaCheck bool

	// DisableDatabaseAutoEmbedding makes newly resolved NULL tenant embedding
	// profiles use fts_only mode. Once a tenant profile has an embedding_mode,
	// the tenant-level mode is the source of truth.
	DisableDatabaseAutoEmbedding bool

	// LeaderChecker, when set, gates per-tenant FileGCWorker to run only on
	// the leader pod. When nil, FileGCWorker starts unconditionally (single-pod).
	//
	// FileGC reacts to leadership transitions and races safely with concurrent
	// backend creation: the server calls StartAllFileGC on leadership gain and
	// StopAllFileGC on loss. These set a pool-owned fileGCEnabled flag under p.mu,
	// and Get/Acquire start FileGC on a newly inserted backend according to that
	// flag (read under p.mu), so a backend created concurrently with a transition
	// resolves to the post-transition state instead of a moment-in-time
	// IsLeader() snapshot.
	LeaderChecker LeaderChecker

	// IdleTimeout controls how long a cached backend can stay in the warm
	// cache without any activity before the idle reaper evicts it. 0
	// disables idle eviction (LRU capacity eviction still applies).
	//
	// "Activity" means any access through Get, Acquire, or S3Backend —
	// including foreground user requests (HTTP/FUSE), tenant-specific
	// durable work driven by kicks (semantic/file_gc task processing), and
	// object-GC candidate processing. These are all legitimate uses that
	// should keep a tenant warm.
	//
	// AcquireCached (safety-net scan) does NOT refresh the idle timer on
	// acquire or release, so warm-only periodic observation cannot keep an
	// idle tenant warm forever. Object-GC may cold-open a tenant for a due
	// candidate and intentionally keeps it warm for one idle-TTL window
	// per due attempt.
	IdleTimeout time.Duration

	// IdleReapInterval is how often the idle reaper scans for idle backends.
	// Defaults to defaultTenantPoolIdleReapInterval when IdleTimeout > 0.
	IdleReapInterval time.Duration
}

// LeaderChecker reports whether the current process is the leader. Used by
// the tenant pool to gate per-tenant background workers (e.g. FileGCWorker).
type LeaderChecker interface {
	IsLeader() bool
}

// ActiveTenantStores snapshots the (tenantID, store) pairs currently cached in
// the pool. It is intended for leader-only background maintenance (e.g. the
// fs_events cleanup goroutine) that should operate only on tenants with a
// live backend — fs_events rows only accumulate for tenants that have served
// traffic and thus have a cached backend. Unlike ListTenantsByStatus +
// Acquire, this does NOT open backends for dormant tenants (which would
// exhaust connections and fail for tenants whose credentials are not
// materialized), so it is cheap and safe to call on every cleanup tick.
// The returned stores are pinned only for the duration of the snapshot call;
// callers must not retain them past the cleanup operation (a concurrent
// retirement may close the underlying DB once this returns). In practice the
// leader cleanup runs briefly and the entry's refs prevent close mid-use only
// while Acquire is held — for a read-only COUNT/DELETE that tolerates a stale
// reference error, this snapshot is sufficient and matches the existing
// StartAllFileGC iteration pattern.
func (p *Pool) ActiveTenantStores() []TenantStoreEntry {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	out := make([]TenantStoreEntry, 0, len(p.items))
	for _, e := range p.items {
		if e.store == nil || e.retired {
			continue
		}
		out = append(out, TenantStoreEntry{TenantID: e.tenantID, Store: e.store})
	}
	p.mu.Unlock()
	return out
}

// TenantStoreEntry is a (tenantID, store) pair returned by ActiveTenantStores.
type TenantStoreEntry struct {
	TenantID string
	Store    *datastore.Store
}

type Pool struct {
	mu                 sync.Mutex
	cfg                PoolConfig
	enc                encrypt.Encryptor
	metaStore          *meta.Store // central server DB for quota operations; nil disables central quota
	items              map[string]*entry
	order              *list.List
	maxSize            int
	tenantWorkNotifier atomic.Pointer[func(tenantID, tidbCloudOrgID string, workMask int)]
	// fileGCEnabled is retained for backward compatibility but no longer used:
	// FileGC is now kick-driven through the unified tenant worker, not a
	// per-backend goroutine.
	fileGCEnabled bool
	idleTimeout   time.Duration
	reapInterval  time.Duration
	reapStop      context.CancelFunc
	reapWG        sync.WaitGroup
	// sharedDBs caches one *sql.DB handle per shared-schema database (keyed by
	// db_pool.id). All tenants placed on the same shared DB share its
	// handle; each Acquire still gets its own Store (carrying that tenant's
	// fs_id scope) over the shared handle.
	sharedMu         sync.Mutex
	sharedDBs        map[int64]*sql.DB
	sharedDBRefs     map[int64]int
	sharedDBLastUsed map[int64]time.Time
}

type tenantAutoEmbeddingProfile struct {
	schemaProfile schema.TiDBAutoEmbeddingProfile
	provider      schema.TiDBAutoEmbeddingProviderConfig
	mode          string
	modeWasNull   bool
}

var (
	applyTiDBAutoEmbeddingProviderConfig    = schema.ApplyTiDBAutoEmbeddingProviderConfig
	ensureTiDBSchemaForAutoEmbeddingProfile = schema.EnsureTiDBSchemaForAutoEmbeddingProfile
	ensureTiDBSchemaForFTSOnlyProfile       = schema.EnsureTiDBSchemaForFTSOnlyProfile
	ensureSharedDBSchema                    = schema.EnsureSharedSchema
	defaultTenantPoolDrainTimeout           = 30 * time.Second
	defaultTenantPoolMaxTenants             = 1024
	defaultTenantPoolIdleReapInterval       = 2 * time.Minute
	defaultSharedDBHandleIdleTTL            = 30 * time.Minute
)

func ensureTiDBSchemaForEmbeddingMode(ctx context.Context, db *sql.DB, mode string, profile schema.TiDBAutoEmbeddingProfile) error {
	tidbMode, err := TiDBEmbeddingModeForTenantMode(mode)
	if err != nil {
		return err
	}
	switch tidbMode {
	case schema.TiDBEmbeddingModeAuto:
		return ensureTiDBSchemaForAutoEmbeddingProfile(ctx, db, profile)
	case schema.TiDBEmbeddingModeFTSOnly:
		return ensureTiDBSchemaForFTSOnlyProfile(ctx, db, profile)
	default:
		return schema.EnsureTiDBSchemaForEmbeddingModeProfile(ctx, db, tidbMode, profile)
	}
}

func durationMs(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func poolAcquireTimingFields(tenantID string, coldOpen bool, createBackendDurationMs float64, totalDuration time.Duration) []zap.Field {
	fields := []zap.Field{
		zap.String("tenant_id", tenantID),
		zap.Bool("cold_open", coldOpen),
	}
	if coldOpen {
		fields = append(fields, zap.Float64("create_backend_ms", createBackendDurationMs))
	}
	fields = append(fields, zap.Float64("total_ms", durationMs(totalDuration)))
	return fields
}

func (p *Pool) resolveTenantEmbeddingMode(persisted string) (mode string, wasNull bool, err error) {
	return meta.ResolveTenantEmbeddingMode(persisted, p.cfg.DisableDatabaseAutoEmbedding)
}

type entry struct {
	tenantID           string
	storageNamespaceID string
	s3EncryptionPolicy meta.S3EncryptionPolicy
	backend            *backend.Dat9Backend
	store              *datastore.Store
	elem               *list.Element
	refs               int
	retired            bool
	lastUsed           time.Time // refreshed by Get/Acquire/S3Backend (foreground + durable work) and on Acquire release; never by AcquireCached
	sharedDBID         int64     // db_pool.id for shared-schema tenants; zero for standalone tenants
}

func NewPool(cfg PoolConfig, enc encrypt.Encryptor) *Pool {
	max := cfg.MaxTenants
	if max <= 0 {
		max = defaultTenantPoolMaxTenants
	}
	idleTimeout := cfg.IdleTimeout
	reapInterval := cfg.IdleReapInterval
	if reapInterval <= 0 && idleTimeout > 0 {
		reapInterval = defaultTenantPoolIdleReapInterval
	}
	metrics.RecordGauge("tenant_pool", "cached_backends", 0)
	metrics.RecordGauge("tenant_pool", "max_backends", float64(max))
	return &Pool{
		cfg:              cfg,
		enc:              enc,
		items:            map[string]*entry{},
		order:            list.New(),
		maxSize:          max,
		sharedDBs:        map[int64]*sql.DB{},
		sharedDBRefs:     map[int64]int{},
		sharedDBLastUsed: map[int64]time.Time{},
		// No LeaderChecker means single-pod mode: FileGC runs unconditionally.
		fileGCEnabled: cfg.LeaderChecker == nil,
		idleTimeout:   idleTimeout,
		reapInterval:  reapInterval,
	}
}

// SetMetaStore sets the central server DB store for quota operations.
// Must be called before any backend is created via Get/Acquire.
func (p *Pool) SetMetaStore(ms *meta.Store) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metaStore = ms
}

// SetTenantWorkNotifier registers a callback invoked with the tenant ID, TiDB
// Cloud org ID, and work mask whenever one of this pool's backends commits
// durable work (semantic task, file_gc task, quota outbox row), so the tenant
// worker can claim the new work immediately instead of waiting for a periodic
// scan.
func (p *Pool) SetTenantWorkNotifier(fn func(tenantID, tidbCloudOrgID string, workMask int)) {
	if p == nil {
		return
	}
	p.tenantWorkNotifier.Store(&fn)
}

func (p *Pool) wireTenantWorkNotifier(b *backend.Dat9Backend, tenantID string) {
	// Resolve the notifier at call time, not wire time, so backends created
	// before SetTenantWorkNotifier (e.g. during tenant resume on startup) still
	// notify once the tenant worker registers.
	b.SetWorkEnqueuedNotifier(func(workMask int, tidbCloudOrgID string) {
		if fn := p.tenantWorkNotifier.Load(); fn != nil && *fn != nil {
			(*fn)(tenantID, tidbCloudOrgID, workMask)
		}
	})
}

func (p *Pool) Get(ctx context.Context, t *meta.Tenant) (out *backend.Dat9Backend, err error) {
	start := time.Now()
	defer observePool(ctx, "get", t.ID, &err, start)

	if t.Status != meta.TenantActive {
		logger.Warn(ctx, "tenant_pool_get_skipped_inactive",
			zap.String("tenant_id", t.ID),
			zap.String("status", string(t.Status)))
		p.Invalidate(t.ID)
		return nil, fmt.Errorf("tenant status: %s", t.Status)
	}

	s3EncryptionPolicy := t.S3EncryptionPolicy()
	storageNamespaceID := t.StorageNamespaceID
	var toClose []*entry
	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		if e.s3EncryptionPolicy == s3EncryptionPolicy && (storageNamespaceID == "" || e.storageNamespaceID == storageNamespaceID) {
			p.order.MoveToFront(e.elem)
			e.lastUsed = time.Now()
			b := e.backend
			p.mu.Unlock()
			return b, nil
		}
		if removed := p.removeLocked(e.elem, "replace"); removed != nil {
			toClose = append(toClose, removed)
		}
		p.mu.Unlock()
		for _, retired := range toClose {
			p.closeEntry(retired)
		}
		toClose = nil
	} else {
		p.mu.Unlock()
	}

	b, st, _, err := p.createBackend(ctx, t)
	if err != nil {
		return nil, err
	}
	sharedDBID := p.sharedDBIDForStore(st)

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		if e.s3EncryptionPolicy == s3EncryptionPolicy && (t.StorageNamespaceID == "" || e.storageNamespaceID == t.StorageNamespaceID) {
			b.Close()
			_ = st.Close()
			p.order.MoveToFront(e.elem)
			e.lastUsed = time.Now()
			p.mu.Unlock()
			return e.backend, nil
		}
		if removed := p.removeLocked(e.elem, "replace"); removed != nil {
			toClose = append(toClose, removed)
		}
	}
	e := &entry{tenantID: t.ID, storageNamespaceID: t.StorageNamespaceID, s3EncryptionPolicy: s3EncryptionPolicy, backend: b, store: st, lastUsed: time.Now(), sharedDBID: sharedDBID}
	e.elem = p.order.PushFront(e)
	p.items[t.ID] = e
	p.retainSharedDB(sharedDBID)
	for p.order.Len() > p.maxSize {
		oldest := p.order.Back()
		if oldest != nil {
			if removed := p.removeLocked(oldest, "evict"); removed != nil {
				toClose = append(toClose, removed)
			}
		}
	}
	p.recordCachedBackendCountLocked()
	// FileGC is now kick-driven through the unified tenant worker; no
	// per-backend goroutine is started here.
	p.mu.Unlock()
	for _, retired := range toClose {
		p.closeEntry(retired)
	}
	return b, nil
}

// Acquire returns a backend that is pinned for the caller's active use. The
// returned release callback must be called when the caller is done with the
// backend so a retired entry can be closed.
func (p *Pool) Acquire(ctx context.Context, t *meta.Tenant) (out *backend.Dat9Backend, release func(), err error) {
	start := time.Now()
	defer observePool(ctx, "acquire", t.ID, &err, start)

	if t.Status != meta.TenantActive {
		logger.Warn(ctx, "tenant_pool_get_skipped_inactive",
			zap.String("tenant_id", t.ID),
			zap.String("status", string(t.Status)))
		p.Invalidate(t.ID)
		return nil, nil, fmt.Errorf("tenant status: %s", t.Status)
	}

	s3EncryptionPolicy := t.S3EncryptionPolicy()
	storageNamespaceID := t.StorageNamespaceID
	var toClose []*entry
	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		if e.s3EncryptionPolicy == s3EncryptionPolicy && (storageNamespaceID == "" || e.storageNamespaceID == storageNamespaceID) {
			e.refs++
			p.order.MoveToFront(e.elem)
			e.lastUsed = time.Now()
			p.mu.Unlock()
			metrics.RecordOperation("tenant_pool", "cache_lookup", "hit", 0)
			totalDuration := time.Since(start)
			logger.InfoOpenPoolTiming(ctx, "tenant_pool_acquire_timing", totalDuration,
				poolAcquireTimingFields(t.ID, false, 0, totalDuration)...)
			return e.backend, p.makeRelease(e), nil
		}
		if removed := p.removeLocked(e.elem, "replace"); removed != nil {
			toClose = append(toClose, removed)
		}
		p.mu.Unlock()
		for _, retired := range toClose {
			p.closeEntry(retired)
		}
		toClose = nil
	} else {
		p.mu.Unlock()
	}

	createBackendStart := time.Now()
	b, st, tidbCloudOrgID, err := p.createBackend(ctx, t)
	if err != nil {
		// Cold-open failure: a tenant TiDB open was attempted but failed.
		// Record so alerts can detect a scan path that is churning cold opens.
		metrics.RecordTenantOperationWithOrg(t.ID, tidbCloudOrgID, "user_db_access", "acquire_cold_open", "error", time.Since(createBackendStart))
		return nil, nil, err
	}
	sharedDBID := p.sharedDBIDForStore(st)
	createBackendDurationMs := float64(time.Since(createBackendStart).Microseconds()) / 1000.0
	// Cold-open success: a tenant TiDB was opened from scratch (cache miss).
	// This is the canonical "a serverless TiDB was woken up" signal — the
	// primary billing-storm indicator. PR #660 eliminated periodic scans that
	// caused cold opens at scale; this metric guards against regressions.
	metrics.RecordTenantOperationWithOrg(t.ID, tidbCloudOrgID, "user_db_access", "acquire_cold_open", "ok", time.Since(createBackendStart))

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		if e.s3EncryptionPolicy == s3EncryptionPolicy && (t.StorageNamespaceID == "" || e.storageNamespaceID == t.StorageNamespaceID) {
			e.refs++
			p.order.MoveToFront(e.elem)
			e.lastUsed = time.Now()
			p.mu.Unlock()
			b.Close()
			_ = st.Close()
			metrics.RecordOperation("tenant_pool", "cache_lookup", "hit", 0)
			totalDuration := time.Since(start)
			logger.InfoOpenPoolTiming(ctx, "tenant_pool_acquire_timing", totalDuration,
				poolAcquireTimingFields(t.ID, true, createBackendDurationMs, totalDuration)...)
			return e.backend, p.makeRelease(e), nil
		}
		if removed := p.removeLocked(e.elem, "replace"); removed != nil {
			toClose = append(toClose, removed)
		}
	}
	e := &entry{tenantID: t.ID, storageNamespaceID: t.StorageNamespaceID, s3EncryptionPolicy: s3EncryptionPolicy, backend: b, store: st, refs: 1, lastUsed: time.Now(), sharedDBID: sharedDBID}
	e.elem = p.order.PushFront(e)
	p.items[t.ID] = e
	p.retainSharedDB(sharedDBID)
	for p.order.Len() > p.maxSize {
		oldest := p.order.Back()
		if oldest != nil {
			if removed := p.removeLocked(oldest, "evict"); removed != nil {
				toClose = append(toClose, removed)
			}
		}
	}
	p.recordCachedBackendCountLocked()
	// FileGC is now kick-driven through the unified tenant worker; no
	// per-backend goroutine is started here.
	p.mu.Unlock()
	for _, retired := range toClose {
		p.closeEntry(retired)
	}
	metrics.RecordOperation("tenant_pool", "cache_lookup", "miss", 0)
	totalDuration := time.Since(start)
	logger.InfoOpenPoolTiming(ctx, "tenant_pool_acquire_timing", totalDuration,
		poolAcquireTimingFields(t.ID, true, createBackendDurationMs, totalDuration)...)
	return b, p.makeRelease(e), nil
}

func (p *Pool) Invalidate(tenantID string) {
	var toClose *entry
	p.mu.Lock()
	if e, ok := p.items[tenantID]; ok {
		toClose = p.removeLocked(e.elem, "invalidate")
	}
	p.mu.Unlock()
	p.closeEntry(toClose)
}

func (p *Pool) WaitTenantIdle(ctx context.Context, tenantID string) error {
	ctx, cancel := withTenantPoolDrainTimeout(ctx)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		p.mu.Lock()
		e, ok := p.items[tenantID]
		refs := 0
		if ok {
			refs = e.refs
		}
		idle := !ok || e.refs == 0
		p.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			logger.Warn(ctx, "tenant_pool_wait_idle_timeout",
				zap.String("tenant_id", tenantID),
				zap.Int("refs", refs),
				zap.Error(ctx.Err()))
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// AcquireCached returns the cached backend for the tenant, pinning it for the
// caller's active use, without creating a new backend if the tenant is not
// already cached. This is used by the safety-net scan to recover expired
// leases only for warm tenants (those with a live backend in the cache),
// avoiding waking dormant serverless tenant TiDBs. Returns (nil, nil, false)
// when the tenant is not cached (cold).
// AcquireCached pins a cached backend without creating one on cache miss. It
// is intended for the safety-net scan and other background work that may
// process already-active tenants but must not wake dormant tenant TiDBs.
//
// If the tenant's S3 encryption policy or storage namespace has changed since
// the backend was cached, AcquireCached returns false (the stale backend is
// not usable). In this rare case the safety-net scan skips the tenant — its
// expired lease recovery is deferred until the next write kick triggers a
// fresh Acquire with the updated policy. This is acceptable because policy
// changes are rare and the tenant's tasks remain durable in its TiDB.
func (p *Pool) AcquireCached(t *meta.Tenant) (b *backend.Dat9Backend, release func(), ok bool) {
	if p == nil || t == nil {
		return nil, nil, false
	}
	if t.Status != meta.TenantActive {
		p.Invalidate(t.ID)
		return nil, nil, false
	}
	s3EncryptionPolicy := t.S3EncryptionPolicy()
	p.mu.Lock()
	e, exists := p.items[t.ID]
	if !exists {
		p.mu.Unlock()
		// Cold-skip: the tenant is not cached (TiDB likely scaled to zero).
		// This is the warm-only invariant working as designed — the safety-net
		// must never open a cold tenant TiDB. Record so the invariant-violation
		// alert can confirm AcquireCached is staying warm-only.
		metrics.RecordTenantOperationWithOrg(t.ID, "", "user_db_access", "acquire_cached", "cold_skipped", 0)
		return nil, nil, false
	}
	if e.s3EncryptionPolicy != s3EncryptionPolicy || (t.StorageNamespaceID != "" && e.storageNamespaceID != t.StorageNamespaceID) {
		p.mu.Unlock()
		// Stale-skip: the cached backend's encryption policy / storage namespace
		// no longer matches the tenant. The safety-net defers to the next write
		// kick. Record so this rare edge case is observable.
		metrics.RecordTenantOperationWithOrg(t.ID, e.backend.TiDBCloudOrgID(), "user_db_access", "acquire_cached", "stale_skipped", 0)
		return nil, nil, false
	}
	e.refs++
	p.order.MoveToFront(e.elem)
	p.mu.Unlock()
	metrics.RecordOperation("tenant_pool", "cache_lookup", "hit", 0)
	metrics.RecordTenantOperationWithOrg(t.ID, e.backend.TiDBCloudOrgID(), "user_db_access", "acquire_cached", "hit", 0)
	return e.backend, p.makeReleaseNoRefresh(e), true
}

func (p *Pool) InvalidateAndWait(ctx context.Context, tenantID string) error {
	ctx, cancel := withTenantPoolDrainTimeout(ctx)
	defer cancel()
	var retired *entry
	var toClose *entry
	p.mu.Lock()
	if e, ok := p.items[tenantID]; ok {
		retired = e
		toClose = p.removeLocked(e.elem, "invalidate")
	}
	p.mu.Unlock()
	p.closeEntry(toClose)
	if retired == nil || toClose != nil {
		return nil
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		p.mu.Lock()
		idle := retired.refs == 0
		refs := retired.refs
		p.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			logger.Warn(ctx, "tenant_pool_invalidate_wait_timeout",
				zap.String("tenant_id", tenantID),
				zap.Int("refs", refs),
				zap.Error(ctx.Err()))
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Pool) S3ForStorageNamespace(ctx context.Context, ns *meta.StorageNamespace) (out s3client.S3Client, err error) {
	start := time.Now()
	namespaceID := ""
	if ns != nil {
		namespaceID = ns.ID
	}
	defer observePool(ctx, "s3_for_storage_namespace", namespaceID, &err, start)
	if ns == nil {
		return nil, fmt.Errorf("storage namespace is required")
	}
	switch ns.Backend {
	case "s3":
		if p.cfg.S3Bucket == "" {
			return nil, fmt.Errorf("s3 bucket is not configured")
		}
		if ns.Bucket != "" && ns.Bucket != p.cfg.S3Bucket {
			return nil, fmt.Errorf("storage namespace bucket %q does not match configured bucket", ns.Bucket)
		}
		return s3client.New(ctx, s3client.AWSConfig{
			Region:          p.cfg.S3Region,
			Bucket:          p.cfg.S3Bucket,
			Prefix:          ns.Prefix,
			RoleARN:         p.cfg.S3RoleARN,
			Endpoint:        p.cfg.S3Endpoint,
			ForcePathStyle:  p.cfg.S3ForcePathStyle,
			AccessKeyID:     p.cfg.S3AccessKeyID,
			SecretAccessKey: p.cfg.S3SecretAccessKey,
			SessionToken:    p.cfg.S3SessionToken,
		})
	case "local":
		if p.cfg.S3Dir == "" {
			return nil, fmt.Errorf("local s3 dir is not configured")
		}
		localPrefix, err := cleanStorageNamespaceLocalPrefix(ns.Prefix)
		if err != nil {
			return nil, err
		}
		rootDir, err := filepath.Abs(p.cfg.S3Dir)
		if err != nil {
			return nil, fmt.Errorf("resolve local s3 dir: %w", err)
		}
		s3Dir := filepath.Join(rootDir, localPrefix)
		if rel, err := filepath.Rel(rootDir, s3Dir); err != nil {
			return nil, fmt.Errorf("resolve local storage namespace prefix: %w", err)
		} else if rel == ".." || strings.HasPrefix(rel, "../") {
			return nil, fmt.Errorf("storage namespace prefix escapes local s3 dir")
		}
		baseURL := strings.TrimRight(p.cfg.PublicURL, "/")
		if baseURL != "" {
			baseURL += "/s3/" + localPrefix
		}
		return s3client.NewLocal(s3Dir, baseURL)
	default:
		return nil, fmt.Errorf("unsupported storage backend %q", ns.Backend)
	}
}

func cleanStorageNamespaceLocalPrefix(prefix string) (string, error) {
	if filepath.IsAbs(prefix) {
		return "", fmt.Errorf("storage namespace prefix must be relative")
	}
	localPrefix := strings.Trim(prefix, "/")
	if localPrefix == "" {
		return "", fmt.Errorf("storage namespace prefix is required")
	}
	for _, part := range strings.Split(localPrefix, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid storage namespace prefix %q", prefix)
		}
	}
	return localPrefix, nil
}

func withTenantPoolDrainTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultTenantPoolDrainTimeout)
}

// Start launches the idle reaper goroutine if IdleTimeout is configured.
// Safe to call on a pool with IdleTimeout=0 (no-op). The reaper is stopped
// by Close.
func (p *Pool) Start(ctx context.Context) {
	if p == nil || p.idleTimeout <= 0 {
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	p.reapStop = cancel
	p.reapWG.Add(1)
	go func() {
		defer p.reapWG.Done()
		p.reapLoop(workerCtx)
	}()
}

func (p *Pool) reapLoop(ctx context.Context) {
	if p.reapInterval <= 0 {
		return
	}
	ticker := time.NewTicker(p.reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.reapOnce(ctx)
		}
	}
}

func (p *Pool) reapOnce(ctx context.Context) {
	if p.idleTimeout <= 0 {
		return
	}
	now := time.Now()
	var toClose []*entry
	p.mu.Lock()
	for _, e := range p.items {
		if e.retired || e.refs > 0 {
			continue
		}
		if now.Sub(e.lastUsed) > p.idleTimeout {
			if removed := p.removeLocked(e.elem, "idle"); removed != nil {
				toClose = append(toClose, removed)
			}
		}
	}
	p.recordCachedBackendCountLocked()
	p.mu.Unlock()
	for _, retired := range toClose {
		p.closeEntry(retired)
	}
	p.reapIdleSharedDBs(now)
}

func (p *Pool) Close() {
	// Stop the idle reaper first so it doesn't race with shutdown eviction.
	if p.reapStop != nil {
		p.reapStop()
		p.reapWG.Wait()
		p.reapStop = nil
	}
	toClose := make([]*entry, 0, p.order.Len())
	p.mu.Lock()
	for p.order.Len() > 0 {
		if removed := p.removeLocked(p.order.Back(), "shutdown"); removed != nil {
			toClose = append(toClose, removed)
		}
	}
	p.mu.Unlock()
	for _, retired := range toClose {
		p.closeEntry(retired)
	}
	type sharedDBToClose struct {
		id int64
		db *sql.DB
	}
	p.sharedMu.Lock()
	sharedToClose := make([]sharedDBToClose, 0, len(p.sharedDBs))
	for dbID, db := range p.sharedDBs {
		sharedToClose = append(sharedToClose, sharedDBToClose{id: dbID, db: db})
		delete(p.sharedDBs, dbID)
		delete(p.sharedDBRefs, dbID)
		delete(p.sharedDBLastUsed, dbID)
	}
	p.sharedMu.Unlock()
	for _, item := range sharedToClose {
		if err := mysqlutil.CloseInstrumented(item.db); err != nil {
			logger.Warn(context.Background(), "shared_db_close_failed", zap.Int64("db_id", item.id), zap.Error(err))
		}
	}
}

func (p *Pool) S3Backend(tenantID string) *backend.Dat9Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.items[tenantID]; ok {
		e.lastUsed = time.Now()
		return e.backend
	}
	return nil
}

func (p *Pool) Decrypt(ctx context.Context, cipher []byte) ([]byte, error) {
	return p.enc.Decrypt(ctx, cipher)
}

func (p *Pool) Encrypt(ctx context.Context, plain []byte) ([]byte, error) {
	return p.enc.Encrypt(ctx, plain)
}

// SupportsAsyncImageExtract reports whether tenant backends created by this
// pool carry the async image extraction runtime.
func (p *Pool) SupportsAsyncImageExtract() bool {
	return p != nil && p.cfg.BackendOptions.AsyncImageExtract.Enabled
}

// AutoSemanticTaskTypes returns the auto-backend durable semantic task types
// implied by PoolConfig.BackendOptions (async image and/or audio extract). This
// is a coarse routing hint for tenant list filtering before a backend is
// acquired; it does not include app-managed embed tasks. Nil means the pool
// contributes no auto semantic tasks. The returned slice must be treated as
// read-only.
//
// Image: viability matches backend.Options.configureOptions — when Enabled, a nil
// Image Extractor is replaced with NewBasicImageTextExtractor before workers start.
// Audio: Phase 2 requires Enabled and a non-nil Extractor (no implicit default);
// pool routing must stay aligned with Dat9Backend.SupportsAsyncAudioExtract.
func (p *Pool) AutoSemanticTaskTypes() []semantic.TaskType {
	if p == nil {
		return nil
	}
	var out []semantic.TaskType
	if backend.AsyncImageExtractWillWireRuntime(p.cfg.BackendOptions.AsyncImageExtract) {
		out = append(out, semantic.TaskTypeImgExtractText)
	}
	if backend.AsyncAudioExtractWillWireRuntime(p.cfg.BackendOptions.AsyncAudioExtract) {
		out = append(out, semantic.TaskTypeAudioExtractText)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (p *Pool) LoadS3Backend(ctx context.Context, metaStore *meta.Store, tenantID string) (out *backend.Dat9Backend) {
	start := time.Now()
	var err error
	defer observePool(ctx, "load_s3_backend", tenantID, &err, start)

	b := p.S3Backend(tenantID)
	if b != nil {
		return b
	}
	tenant, err := metaStore.GetTenant(ctx, tenantID)
	if err != nil {
		if !errors.Is(err, meta.ErrNotFound) {
			logger.Error(ctx, "tenant_pool_load_s3_backend_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		}
		return nil
	}
	b, err = p.Get(ctx, tenant)
	if err != nil {
		logger.Error(ctx, "tenant_pool_get_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return nil
	}
	return b
}

// fsIDForTenant resolves the tenant's internal fs_id from the meta DB,
// allocating one on first use. Returns (0, nil) when the pool has no meta
// store (tests, non-multi-tenant mode); the standalone SQL shape never emits
// fs_id, so 0 is inert there.
func (p *Pool) fsIDForTenant(ctx context.Context, t *meta.Tenant) (int64, error) {
	if p.metaStore == nil || t == nil || t.ID == "" {
		return 0, nil
	}
	fsID, err := p.metaStore.EnsureFsID(ctx, t.ID)
	if err != nil {
		return 0, fmt.Errorf("ensure fs_id for tenant %s: %w", t.ID, err)
	}
	return fsID, nil
}

func (p *Pool) tenantMetricTiDBCloudOrgID(ctx context.Context, t *meta.Tenant) string {
	if t == nil || strings.TrimSpace(t.ID) == "" || t.Provider != ProviderTiDBCloudNative {
		return defaultTenantMetricTiDBCloudOrgID
	}
	if orgID := strings.TrimSpace(t.TiDBCloudOrgID); orgID != "" {
		return orgID
	}
	if p == nil || p.metaStore == nil {
		return defaultTenantMetricTiDBCloudOrgID
	}
	binding, err := p.metaStore.GetTenantTiDBCloudOrgBinding(ctx, t.ID)
	if err != nil {
		if !errors.Is(err, meta.ErrNotFound) {
			logger.Warn(ctx, "tenant_pool_metric_org_lookup_failed",
				zap.String("tenant_id", t.ID),
				zap.Error(err))
		}
		return defaultTenantMetricTiDBCloudOrgID
	}
	orgID := strings.TrimSpace(binding.OrganizationID)
	if orgID == "" {
		return defaultTenantMetricTiDBCloudOrgID
	}
	return orgID
}

// placementForTenant returns the tenant's placement row, or (nil, nil) when
// the tenant has none — which is the normal case for every standalone tenant
// today. Placement lookup errors are fatal to the cold open: silently falling
// back to standalone could route a shared tenant with the wrong schema shape.
func (p *Pool) placementForTenant(ctx context.Context, fsID int64) (*meta.TenantPlacement, error) {
	if p.metaStore == nil || fsID <= 0 {
		return nil, nil
	}
	placement, err := p.metaStore.GetTenantPlacement(ctx, fsID)
	if errors.Is(err, meta.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve tenant placement for fs_id %d: %w", fsID, err)
	}
	return placement, nil
}

// sharedDBIDForStore identifies whether a newly created tenant store uses one
// of this pool's physical shared-DB handles. It is called only on cold opens,
// before the tenant backend is inserted into the LRU.
func (p *Pool) sharedDBIDForStore(store *datastore.Store) int64 {
	if store == nil {
		return 0
	}
	db := store.DB()
	p.sharedMu.Lock()
	defer p.sharedMu.Unlock()
	for dbID, shared := range p.sharedDBs {
		if shared == db {
			return dbID
		}
	}
	return 0
}

func (p *Pool) retainSharedDB(dbID int64) {
	if dbID <= 0 {
		return
	}
	p.sharedMu.Lock()
	p.sharedDBRefs[dbID]++
	p.sharedDBLastUsed[dbID] = time.Now()
	p.sharedMu.Unlock()
}

func (p *Pool) releaseSharedDB(dbID int64) {
	if dbID <= 0 {
		return
	}
	p.sharedMu.Lock()
	if refs := p.sharedDBRefs[dbID]; refs > 1 {
		p.sharedDBRefs[dbID] = refs - 1
	} else {
		delete(p.sharedDBRefs, dbID)
	}
	p.sharedDBLastUsed[dbID] = time.Now()
	p.sharedMu.Unlock()
}

// reapIdleSharedDBs closes physical shared-DB handles only after no cached or
// active tenant backend references them and the handle has then stayed unused
// for the longer shared-handle TTL. The existing tenant-pool reaper invokes
// this; no separate worker is needed.
func (p *Pool) reapIdleSharedDBs(now time.Time) {
	type sharedDBToClose struct {
		id int64
		db *sql.DB
	}
	var toClose []sharedDBToClose
	p.sharedMu.Lock()
	for dbID, db := range p.sharedDBs {
		if p.sharedDBRefs[dbID] > 0 {
			continue
		}
		lastUsed := p.sharedDBLastUsed[dbID]
		if lastUsed.IsZero() || now.Sub(lastUsed) <= defaultSharedDBHandleIdleTTL {
			continue
		}
		toClose = append(toClose, sharedDBToClose{id: dbID, db: db})
		delete(p.sharedDBs, dbID)
		delete(p.sharedDBRefs, dbID)
		delete(p.sharedDBLastUsed, dbID)
	}
	p.sharedMu.Unlock()
	for _, item := range toClose {
		if err := mysqlutil.CloseInstrumented(item.db); err != nil {
			logger.Warn(context.Background(), "shared_db_idle_close_failed", zap.Int64("db_id", item.id), zap.Error(err))
		}
	}
}

// sharedDBHandle returns the cached *sql.DB for a shared-schema database
// (db_pool.id), opening it on first use. An unreferenced handle remains warm
// for defaultSharedDBHandleIdleTTL and is then closed by the existing reaper.
func (p *Pool) sharedDBHandle(ctx context.Context, dbID int64) (*sql.DB, error) {
	p.sharedMu.Lock()
	defer p.sharedMu.Unlock()
	if db, ok := p.sharedDBs[dbID]; ok {
		p.sharedDBLastUsed[dbID] = time.Now()
		return db, nil
	}
	if p.metaStore == nil {
		return nil, fmt.Errorf("shared db %d: no meta store", dbID)
	}
	info, err := p.metaStore.GetSharedDB(ctx, dbID)
	if err != nil {
		return nil, fmt.Errorf("shared db %d: %w", dbID, err)
	}
	pass, err := p.enc.Decrypt(ctx, info.PasswordCipher)
	if err != nil {
		return nil, fmt.Errorf("shared db %d: decrypt password: %w", dbID, err)
	}
	query := "parseTime=true"
	if info.TLSMode != "" {
		query += "&tls=" + info.TLSMode
	}
	// An empty TLSMode means a plain connection (local/self-hosted databases);
	// shared DBs on TiDB Cloud are registered with tls=skip-verify or tls=true.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", info.User, string(pass), info.Host, info.Port, info.Name, query)
	db, err := mysqlutil.OpenInstrumentedForTenantWithOrg(ctx, dsn, mysqlutil.RoleShared,
		fmt.Sprintf("shared:%d", dbID), info.TiDBCloudOrganizationID)
	if err != nil {
		return nil, fmt.Errorf("shared db %d: open: %w", dbID, err)
	}
	if info.SchemaVersion != schema.CurrentSharedTiDBSchemaVersion {
		if err := ensureSharedDBSchema(ctx, db); err != nil {
			_ = mysqlutil.CloseInstrumented(db)
			return nil, fmt.Errorf("shared db %d: ensure schema: %w", dbID, err)
		}
		if err := p.metaStore.UpdateSharedDBSchemaVersion(ctx, dbID, schema.CurrentSharedTiDBSchemaVersion); err != nil {
			_ = mysqlutil.CloseInstrumented(db)
			return nil, fmt.Errorf("shared db %d: persist schema version: %w", dbID, err)
		}
	}
	p.sharedDBs[dbID] = db
	p.sharedDBLastUsed[dbID] = time.Now()
	return db, nil
}

// EnsureSharedDBReady opens the shared physical DB through the normal cache
// path, which also compares and applies the checked-in shared schema version.
func (p *Pool) EnsureSharedDBReady(ctx context.Context, dbID int64) error {
	_, err := p.sharedDBHandle(ctx, dbID)
	return err
}

// InvalidateSharedDB removes and closes one cached physical shared-DB handle.
// A later access reopens it from the current db_pool connection metadata.
func (p *Pool) InvalidateSharedDB(dbID int64) error {
	if dbID <= 0 {
		return fmt.Errorf("shared db id must be positive")
	}
	p.sharedMu.Lock()
	db := p.sharedDBs[dbID]
	delete(p.sharedDBs, dbID)
	delete(p.sharedDBRefs, dbID)
	delete(p.sharedDBLastUsed, dbID)
	p.sharedMu.Unlock()
	if db == nil {
		return nil
	}
	return mysqlutil.CloseInstrumented(db)
}

// PurgeSharedTenant deletes all rows belonging to fsID from the shared DB it
// is placed on, in bounded batches. It runs after the tenant's pool entry has
// been drained, so no backend is concurrently writing through the same scope.
func (p *Pool) PurgeSharedTenant(ctx context.Context, fsID, dbID int64) error {
	handle, err := p.sharedDBHandle(ctx, dbID)
	if err != nil {
		return err
	}
	return datastore.NewStoreWithDB(handle, datastore.SharedScope(fsID)).PurgeTenantData(ctx)
}

func (p *Pool) createBackend(ctx context.Context, t *meta.Tenant) (*backend.Dat9Backend, *datastore.Store, string, error) {
	start := time.Now()
	tidbCloudOrgID := p.tenantMetricTiDBCloudOrgID(ctx, t)
	opts := p.cfg.BackendOptions
	resolvedEncryptionPolicy, err := meta.ResolveS3EncryptionPolicy(p.cfg.S3EncryptionPolicy, t.S3EncryptionPolicy())
	if err != nil {
		return nil, nil, tidbCloudOrgID, fmt.Errorf("resolve s3 encryption policy: %w", err)
	}
	opts.TenantID = t.ID
	opts.TiDBCloudOrgID = tidbCloudOrgID
	opts.S3EncryptionPolicy = resolvedEncryptionPolicy

	fsID, err := p.fsIDForTenant(ctx, t)
	if err != nil {
		return nil, nil, tidbCloudOrgID, err
	}
	placement, err := p.placementForTenant(ctx, fsID)
	if err != nil {
		return nil, nil, tidbCloudOrgID, err
	}
	sharedTenant := placement != nil && placement.SchemaShape == meta.SchemaShapeShared
	if IsSharedSchemaProvider(t.Provider) && !sharedTenant {
		// The persisted provider says shared but the placement row is gone.
		// Opening a standalone store against the (connectionless) tenant row
		// would fail with a confusing empty-host DSN error — and if the row
		// still had coordinates it could even serve the wrong schema shape.
		// This happens only mid-delete (placement is removed before final
		// cleanup), so fail fast with a clear message instead.
		return nil, nil, tidbCloudOrgID, fmt.Errorf("tenant %s is shared-schema but has no placement row", t.ID)
	}

	decryptDurationMs := 0.0
	openStoreStart := time.Now()
	var store *datastore.Store
	if sharedTenant {
		// Shared-schema tenant: connect through the shared DB handle and skip
		// the per-tenant DSN/decrypt entirely — the tenant row carries no
		// connection coordinates for shared placements.
		handle, err := p.sharedDBHandle(ctx, placement.DbID)
		if err != nil {
			return nil, nil, tidbCloudOrgID, err
		}
		store = datastore.NewStoreWithDB(handle, datastore.SharedScope(fsID))
	} else {
		decryptStart := time.Now()
		pass, err := p.enc.Decrypt(ctx, t.DBPasswordCipher)
		if err != nil {
			return nil, nil, tidbCloudOrgID, fmt.Errorf("decrypt db password: %w", err)
		}
		decryptDurationMs = float64(time.Since(decryptStart).Microseconds()) / 1000.0
		query := "parseTime=true"
		if t.DBTLS {
			query += "&tls=true"
		} else if t.Provider == ProviderTiDBCloudNative {
			query += "&tls=skip-verify"
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", t.DBUser, string(pass), t.DBHost, t.DBPort, t.DBName, query)
		store, err = datastore.OpenForTenantScoped(ctx, dsn, t.ID, tidbCloudOrgID, datastore.StandaloneScope(fsID))
		if err != nil {
			return nil, nil, tidbCloudOrgID, fmt.Errorf("open datastore: %w", err)
		}
	}
	openStoreDurationMs := float64(time.Since(openStoreStart).Microseconds()) / 1000.0
	ensureSchemaDurationMs := 0.0
	migrateDurationMs := 0.0
	if sharedTenant {
		// Shared schema has no generated columns, so database auto-embedding
		// is unavailable; schema is managed per physical DB, not per tenant,
		// so the Acquire-time ensure below is skipped entirely.
		opts.DatabaseAutoEmbedding = false
		opts.AppSemanticTasksEnabled = false
		opts.AsyncImageExtract = backend.AsyncImageExtractOptions{}
		opts.AsyncAudioExtract = backend.AsyncAudioExtractOptions{}
	} else if UsesTiDBAutoEmbedding(t.Provider) {
		autoEmbeddingProfile, err := p.autoEmbeddingProfileForTenant(ctx, t)
		if err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, fmt.Errorf("resolve tenant auto-embedding profile: %w", err)
		}
		if autoEmbeddingProfile.mode == meta.TenantEmbeddingModeAuto {
			opts.DatabaseAutoEmbedding = true
			if err := applyTiDBAutoEmbeddingProviderConfig(ctx, store.DB(), autoEmbeddingProfile.provider); err != nil {
				_ = store.Close()
				return nil, nil, tidbCloudOrgID, fmt.Errorf("apply tenant auto-embedding provider config: %w", err)
			}
		} else {
			opts.DatabaseAutoEmbedding = false
			opts.AppSemanticTasksEnabled = false
			opts.AsyncImageExtract = backend.AsyncImageExtractOptions{}
			opts.AsyncAudioExtract = backend.AsyncAudioExtractOptions{}
		}
		if !p.cfg.SkipTiDBSchemaCheck {
			targetSchemaVersion, err := TiDBTenantSchemaVersionForEmbeddingMode(autoEmbeddingProfile.mode, autoEmbeddingProfile.schemaProfile)
			if err != nil {
				_ = store.Close()
				return nil, nil, tidbCloudOrgID, fmt.Errorf("resolve tenant embedding schema version: %w", err)
			}
			if t.SchemaVersion != targetSchemaVersion {
				ensureSchemaStart := time.Now()
				schemaCtx := schema.WithTenantID(ctx, t.ID)
				if err := ensureTiDBSchemaForEmbeddingMode(schemaCtx, store.DB(), autoEmbeddingProfile.mode, autoEmbeddingProfile.schemaProfile); err != nil {
					_ = store.Close()
					return nil, nil, tidbCloudOrgID, fmt.Errorf("ensure tidb embedding schema: %w", err)
				}
				ensureSchemaDurationMs += float64(time.Since(ensureSchemaStart).Microseconds()) / 1000.0
				if p.metaStore != nil {
					// Record the tenant-profile-specific version only after the
					// schema has been confirmed to match that tenant's profile.
					// Version-matched opens trust this durable value and skip the
					// physical diff by default; out-of-band schema drift is detected
					// only when the target schema version changes, or through an
					// explicit validation/repair path. SkipTiDBSchemaCheck disables
					// the Acquire-time TiDB schema ensure/check entirely, mainly for
					// tests that use a TiDB-class provider against plain MySQL.
					updateSchemaVersionStart := time.Now()
					if verErr := p.metaStore.UpdateTenantSchemaVersion(ctx, t.ID, targetSchemaVersion); verErr != nil {
						recordTenantSchemaVersionUpdateFailure(ctx, t.ID, targetSchemaVersion, time.Since(updateSchemaVersionStart), verErr)
					}
				}
			}
			if autoEmbeddingProfile.modeWasNull && p.metaStore != nil {
				if _, modeErr := p.metaStore.SetTenantAutoEmbeddingProfileModeIfNull(ctx, t.ID, autoEmbeddingProfile.mode); modeErr != nil {
					_ = store.Close()
					return nil, nil, tidbCloudOrgID, fmt.Errorf("persist tenant embedding mode: %w", modeErr)
				}
			}
		}
	}
	// Run split-tables migration if needed. Only for tenants that have the
	// legacy files table; new tenants skip it entirely.
	migrateStart := time.Now()
	if store.HasLegacyFiles() {
		if err := p.migrateSplitTables(ctx, store.DB(), t.Provider); err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, fmt.Errorf("migrate split tables: %w", err)
		}
	}
	migrateDurationMs = float64(time.Since(migrateStart).Microseconds()) / 1000.0
	if p.cfg.S3Bucket != "" {
		ns, err := p.resolveStorageNamespace(ctx, t, "s3", p.cfg.S3Bucket)
		if err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, err
		}
		opts.StorageNamespaceID = ns.ID
		prefix := ns.Prefix
		s3ClientStart := time.Now()
		s3c, err := s3client.New(ctx, s3client.AWSConfig{
			Region:          p.cfg.S3Region,
			Bucket:          p.cfg.S3Bucket,
			Prefix:          prefix,
			RoleARN:         p.cfg.S3RoleARN,
			Endpoint:        p.cfg.S3Endpoint,
			ForcePathStyle:  p.cfg.S3ForcePathStyle,
			AccessKeyID:     p.cfg.S3AccessKeyID,
			SecretAccessKey: p.cfg.S3SecretAccessKey,
			SessionToken:    p.cfg.S3SessionToken,
		})
		if err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, fmt.Errorf("create aws s3 client: %w", err)
		}
		s3ClientDurationMs := float64(time.Since(s3ClientStart).Microseconds()) / 1000.0
		smallInDB := SmallInDB(t.Provider)
		backendCreateStart := time.Now()
		b, err := backend.NewWithS3ModeAndOptions(store, s3c, smallInDB, opts)
		if err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, fmt.Errorf("create backend with s3 mode: %w", err)
		}
		backendCreateDurationMs := float64(time.Since(backendCreateStart).Microseconds()) / 1000.0
		totalDuration := time.Since(start)
		logger.InfoOpenPoolTiming(ctx, "tenant_pool_create_backend_timing", totalDuration,
			zap.String("tenant_id", t.ID),
			zap.String("provider", t.Provider),
			zap.String("storage_mode", "aws_s3"),
			zap.Float64("decrypt_db_password_ms", decryptDurationMs),
			zap.Float64("open_datastore_ms", openStoreDurationMs),
			zap.Float64("ensure_schema_ms", ensureSchemaDurationMs),
			zap.Float64("migrate_duration_ms", migrateDurationMs),
			zap.Float64("create_s3_client_ms", s3ClientDurationMs),
			zap.Float64("create_backend_ms", backendCreateDurationMs),
			zap.Float64("total_ms", durationMs(totalDuration)))
		p.wireQuotaStore(ctx, b, t.ID)
		p.wireTenantWorkNotifier(b, t.ID)
		return b, store, tidbCloudOrgID, nil
	}
	if p.cfg.S3Dir != "" {
		ns, err := p.resolveStorageNamespace(ctx, t, "local", "")
		if err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, err
		}
		opts.StorageNamespaceID = ns.ID
		localPrefix := strings.Trim(ns.Prefix, "/")
		s3Dir := strings.TrimRight(p.cfg.S3Dir, "/") + "/" + localPrefix
		s3BaseURL := strings.TrimRight(p.cfg.PublicURL, "/") + "/s3/" + localPrefix
		s3ClientStart := time.Now()
		s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
		if err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, fmt.Errorf("create local s3 client: %w", err)
		}
		s3ClientDurationMs := float64(time.Since(s3ClientStart).Microseconds()) / 1000.0
		smallInDB := SmallInDB(t.Provider)
		backendCreateStart := time.Now()
		b, err := backend.NewWithS3ModeAndOptions(store, s3c, smallInDB, opts)
		if err != nil {
			_ = store.Close()
			return nil, nil, tidbCloudOrgID, fmt.Errorf("create backend with local s3 mode: %w", err)
		}
		backendCreateDurationMs := float64(time.Since(backendCreateStart).Microseconds()) / 1000.0
		totalDuration := time.Since(start)
		logger.InfoOpenPoolTiming(ctx, "tenant_pool_create_backend_timing", totalDuration,
			zap.String("tenant_id", t.ID),
			zap.String("provider", t.Provider),
			zap.String("storage_mode", "local_s3"),
			zap.Float64("decrypt_db_password_ms", decryptDurationMs),
			zap.Float64("open_datastore_ms", openStoreDurationMs),
			zap.Float64("ensure_schema_ms", ensureSchemaDurationMs),
			zap.Float64("migrate_duration_ms", migrateDurationMs),
			zap.Float64("create_s3_client_ms", s3ClientDurationMs),
			zap.Float64("create_backend_ms", backendCreateDurationMs),
			zap.Float64("total_ms", durationMs(totalDuration)))
		p.wireQuotaStore(ctx, b, t.ID)
		p.wireTenantWorkNotifier(b, t.ID)
		return b, store, tidbCloudOrgID, nil
	}
	backendCreateStart := time.Now()
	b, err := backend.NewWithOptions(store, opts)
	if err != nil {
		_ = store.Close()
		return nil, nil, tidbCloudOrgID, fmt.Errorf("create backend: %w", err)
	}
	backendCreateDurationMs := float64(time.Since(backendCreateStart).Microseconds()) / 1000.0
	totalDuration := time.Since(start)
	logger.InfoOpenPoolTiming(ctx, "tenant_pool_create_backend_timing", totalDuration,
		zap.String("tenant_id", t.ID),
		zap.String("provider", t.Provider),
		zap.String("storage_mode", "db_only"),
		zap.Float64("decrypt_db_password_ms", decryptDurationMs),
		zap.Float64("open_datastore_ms", openStoreDurationMs),
		zap.Float64("ensure_schema_ms", ensureSchemaDurationMs),
		zap.Float64("migrate_duration_ms", migrateDurationMs),
		zap.Float64("create_s3_client_ms", 0),
		zap.Float64("create_backend_ms", backendCreateDurationMs),
		zap.Float64("total_ms", durationMs(totalDuration)))
	p.wireQuotaStore(ctx, b, t.ID)
	p.wireTenantWorkNotifier(b, t.ID)
	return b, store, tidbCloudOrgID, nil
}

func (p *Pool) migrateSplitTables(ctx context.Context, db *sql.DB, provider string) error {
	// All current providers use MySQL/TiDB as the metadata store,
	// so we default to MySQL dialect.
	dialect := migrate.DialectMySQL
	m := migrate.NewSplitTablesMigratorWithDialect(db, dialect)
	_, err := m.Run(ctx)
	return err
}

func (p *Pool) autoEmbeddingProfileForTenant(ctx context.Context, t *meta.Tenant) (tenantAutoEmbeddingProfile, error) {
	if p.metaStore == nil || t == nil || t.ID == "" {
		out, err := defaultTenantAutoEmbeddingProfile()
		if err != nil {
			return tenantAutoEmbeddingProfile{}, err
		}
		mode, _, err := p.resolveTenantEmbeddingMode("")
		if err != nil {
			return tenantAutoEmbeddingProfile{}, err
		}
		out.mode = mode
		return out, nil
	}
	profile, err := p.metaStore.EnsureTenantAutoEmbeddingProfile(ctx, t.ID)
	if err != nil {
		return tenantAutoEmbeddingProfile{}, err
	}
	mode, modeWasNull, err := p.resolveTenantEmbeddingMode(profile.EmbeddingMode)
	if err != nil {
		return tenantAutoEmbeddingProfile{}, err
	}
	schemaProfile := schema.TiDBAutoEmbeddingProfile{
		Model:       profile.Model,
		Dimensions:  profile.Dimensions,
		OptionsJSON: profile.OptionsJSON,
	}
	apiKey := ""
	if len(profile.APIKeyCipher) > 0 {
		plain, err := p.enc.Decrypt(ctx, profile.APIKeyCipher)
		if err != nil {
			return tenantAutoEmbeddingProfile{}, fmt.Errorf("decrypt tenant auto-embedding api key: %w", err)
		}
		apiKey = string(plain)
	}
	return tenantAutoEmbeddingProfile{
		schemaProfile: schemaProfile,
		mode:          mode,
		modeWasNull:   modeWasNull,
		provider: schema.TiDBAutoEmbeddingProviderConfig{
			Model:   schemaProfile.Model,
			APIKey:  apiKey,
			APIBase: profile.APIBase,
		},
	}, nil
}

func defaultTenantAutoEmbeddingProfile() (tenantAutoEmbeddingProfile, error) {
	schemaProfile, err := schema.TiDBAutoEmbeddingProfileFromConfig(schema.TiDBAutoEmbeddingConfig{
		Model:      schema.DefaultTiDBAutoEmbeddingModel,
		Dimensions: schema.DefaultTiDBAutoEmbeddingDimensions,
	})
	if err != nil {
		return tenantAutoEmbeddingProfile{}, fmt.Errorf("build default tenant auto-embedding profile: %w", err)
	}
	return tenantAutoEmbeddingProfile{
		schemaProfile: schemaProfile,
		mode:          meta.TenantEmbeddingModeAuto,
		provider: schema.TiDBAutoEmbeddingProviderConfig{
			Model: schemaProfile.Model,
		},
	}, nil
}

func recordTenantSchemaVersionUpdateFailure(ctx context.Context, tenantID string, version int, d time.Duration, err error) {
	logger.Warn(ctx, "tenant_pool_update_schema_version_failed",
		zap.String("tenant_id", tenantID),
		zap.Int("version", version),
		zap.Error(err))
	metrics.RecordOperation("tenant_pool", "update_schema_version_failed", "error", d)
}

func (p *Pool) resolveStorageNamespace(ctx context.Context, t *meta.Tenant, backendName, bucket string) (*meta.StorageNamespace, error) {
	if p.metaStore == nil {
		return &meta.StorageNamespace{
			ID:            t.ID,
			OwnerTenantID: t.ID,
			Backend:       backendName,
			Bucket:        bucket,
			Prefix:        p.defaultStorageNamespacePrefix(t.ID, backendName),
			State:         meta.StorageNamespaceActive,
		}, nil
	}
	ns, err := p.metaStore.EnsureTenantStorageNamespace(ctx, t.ID, backendName, bucket, p.defaultStorageNamespacePrefix(t.ID, backendName))
	if err != nil {
		return nil, fmt.Errorf("resolve storage namespace: %w", err)
	}
	// Keep the tenant value in sync with the namespace persisted by
	// EnsureTenantStorageNamespace. Get/Acquire use this resolved ID to key the
	// cache entry created after backend construction.
	t.StorageNamespaceID = ns.ID
	return ns, nil
}

func (p *Pool) defaultStorageNamespacePrefix(tenantID, backendName string) string {
	if backendName == "s3" {
		prefix := strings.Trim(p.cfg.S3Prefix, "/")
		if prefix != "" {
			prefix += "/"
		}
		return prefix + tenantID + "/"
	}
	return tenantID + "/"
}

// wireQuotaStore sets the central quota store on a newly created backend.
// No-op when the pool's metaStore is nil (tests, non-multi-tenant mode).
func (p *Pool) wireQuotaStore(ctx context.Context, b *backend.Dat9Backend, tenantID string) {
	if p.metaStore == nil {
		return
	}
	adapter := NewMetaQuotaAdapter(p.metaStore)
	b.SetMetaQuotaStore(ctx, tenantID, adapter)
}

// removeLocked drops elem from the pool. reason distinguishes capacity eviction
// ("evict") from entry replacement ("replace"), explicit invalidation
// ("invalidate"), and pool shutdown ("shutdown") so the tenant_pool/remove metric
// doesn't conflate them — only result="evict" reflects real cache pressure.
func (p *Pool) removeLocked(elem *list.Element, reason string) *entry {
	e := elem.Value.(*entry)
	p.order.Remove(elem)
	e.elem = nil
	delete(p.items, e.tenantID)
	e.retired = true
	p.recordCachedBackendCountLocked()
	metrics.RecordOperation("tenant_pool", "remove", reason, 0)
	if e.refs == 0 {
		return e
	}
	return nil
}

func (p *Pool) recordCachedBackendCountLocked() {
	metrics.RecordGauge("tenant_pool", "cached_backends", float64(len(p.items)))
}

func (p *Pool) makeRelease(e *entry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.releaseEntry(e, true)
		})
	}
}

// makeReleaseNoRefresh is like makeRelease but does not refresh lastUsed on
// release. Used by AcquireCached so internal scans (safety-net) do not reset
// the idle timer.
func (p *Pool) makeReleaseNoRefresh(e *entry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.releaseEntry(e, false)
		})
	}
}

func (p *Pool) releaseEntry(e *entry, refreshLastUsed bool) {
	if e == nil {
		return
	}
	var toClose *entry
	p.mu.Lock()
	if e.refs > 0 {
		e.refs--
	}
	if e.refs == 0 && !e.retired && refreshLastUsed {
		e.lastUsed = time.Now()
	}
	if e.refs == 0 && e.retired {
		toClose = e
	}
	p.mu.Unlock()
	if toClose != nil {
		p.closeEntry(toClose)
	}
}

func (p *Pool) closeEntry(e *entry) {
	if e == nil {
		return
	}
	if e.backend != nil {
		e.backend.Close()
	}
	if e.store != nil {
		_ = e.store.Close()
	}
	p.releaseSharedDB(e.sharedDBID)
	p.mu.Lock()
	_, active := p.items[e.tenantID]
	p.mu.Unlock()
	if !active {
		metrics.DeleteTenantCounters(e.tenantID)
	}
}

type tenantPoolResult string

const (
	tenantPoolResultOK                   tenantPoolResult = "ok"
	tenantPoolResultNotFound             tenantPoolResult = "not_found"
	tenantPoolResultAuthFailed           tenantPoolResult = "auth_failed"
	tenantPoolResultUsageQuotaExhausted  tenantPoolResult = "usage_quota_exhausted"
	tenantPoolResultError                tenantPoolResult = "error"
	defaultTenantMetricTiDBCloudOrgID    string           = "guest"
	mysqlErrAccessDenied                 uint16           = 1045
	mysqlErrUsageQuotaExhaustedCandidate uint16           = 1105
)

func observePool(ctx context.Context, op, tenantID string, errp *error, start time.Time) {
	result := tenantPoolResultOK
	if errp != nil && *errp != nil {
		result = tenantPoolErrorResult(*errp)
		fields := []zap.Field{zap.String("operation", op), zap.String("tenant_id", tenantID), zap.String("result", string(result)), zap.Error(*errp)}
		if result == tenantPoolResultError {
			logger.Error(ctx, "tenant_pool_op_failed", fields...)
		} else {
			logger.Warn(ctx, "tenant_pool_op_failed", fields...)
		}
	}
	metrics.RecordOperation("tenant_pool", op, string(result), time.Since(start))
}

func tenantPoolErrorResult(err error) tenantPoolResult {
	switch {
	case err == nil:
		return tenantPoolResultOK
	case errors.Is(err, meta.ErrNotFound):
		return tenantPoolResultNotFound
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case mysqlErrAccessDenied:
			return tenantPoolResultAuthFailed
		case mysqlErrUsageQuotaExhaustedCandidate:
			if strings.Contains(strings.ToLower(mysqlErr.Message), "usage quota") {
				return tenantPoolResultUsageQuotaExhausted
			}
		}
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "please check your user name and password"):
		return tenantPoolResultAuthFailed
	case strings.Contains(msg, "due to the usage quota being exhausted"):
		return tenantPoolResultUsageQuotaExhausted
	default:
		return tenantPoolResultError
	}
}
