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

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/migrate"
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

	// DisableDatabaseAutoEmbedding forces DatabaseAutoEmbedding=false for all
	// tenants, even when the provider normally enables it (tidb_zero,
	// tidb_cloud_starter). Use when the TiDB Cloud cluster does not have a
	// supported auto-embedding provider configured.
	DisableDatabaseAutoEmbedding bool
}

type Pool struct {
	mu                        sync.Mutex
	cfg                       PoolConfig
	enc                       encrypt.Encryptor
	metaStore                 *meta.Store // central server DB for quota operations; nil disables central quota
	items                     map[string]*entry
	order                     *list.List
	maxSize                   int
	tidbSchemaValidationOpens atomic.Uint64
	semanticTaskNotifier      atomic.Pointer[func(tenantID string)]
}

type tenantAutoEmbeddingProfile struct {
	schemaProfile schema.TiDBAutoEmbeddingProfile
	provider      schema.TiDBAutoEmbeddingProviderConfig
}

var (
	applyTiDBAutoEmbeddingProviderConfig      = schema.ApplyTiDBAutoEmbeddingProviderConfig
	ensureTiDBSchemaForAutoEmbeddingProfile   = schema.EnsureTiDBSchemaForAutoEmbeddingProfile
	validateTiDBSchemaForAutoEmbeddingProfile = schema.ValidateTiDBSchemaForAutoEmbeddingProfile
	// Validate once on the first version-matched open after process start, then
	// periodically thereafter to catch out-of-band schema drift without putting a
	// full schema diff on every tenant open.
	periodicTiDBSchemaValidationEvery uint64 = 32
	defaultTenantPoolDrainTimeout            = 30 * time.Second
)

type entry struct {
	tenantID           string
	storageNamespaceID string
	s3EncryptionPolicy meta.S3EncryptionPolicy
	backend            *backend.Dat9Backend
	store              *datastore.Store
	elem               *list.Element
	refs               int
	retired            bool
}

func NewPool(cfg PoolConfig, enc encrypt.Encryptor) *Pool {
	max := cfg.MaxTenants
	if max <= 0 {
		max = 128
	}
	return &Pool{cfg: cfg, enc: enc, items: map[string]*entry{}, order: list.New(), maxSize: max}
}

// SetMetaStore sets the central server DB store for quota operations.
// Must be called before any backend is created via Get/Acquire.
func (p *Pool) SetMetaStore(ms *meta.Store) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metaStore = ms
}

// SetSemanticTaskNotifier registers a callback invoked with the tenant ID
// whenever one of this pool's backends commits a durable semantic task, so the
// semantic worker can claim the new work immediately instead of waiting for
// the periodic tenant scan.
func (p *Pool) SetSemanticTaskNotifier(fn func(tenantID string)) {
	if p == nil {
		return
	}
	p.semanticTaskNotifier.Store(&fn)
}

func (p *Pool) wireSemanticTaskNotifier(b *backend.Dat9Backend, tenantID string) {
	// Resolve the notifier at call time, not wire time, so backends created
	// before SetSemanticTaskNotifier (e.g. during tenant resume on startup)
	// still notify once the semantic worker registers.
	b.SetSemanticTaskEnqueuedNotifier(func() {
		if fn := p.semanticTaskNotifier.Load(); fn != nil && *fn != nil {
			(*fn)(tenantID)
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
			b := e.backend
			p.mu.Unlock()
			return b, nil
		}
		if removed := p.removeLocked(e.elem); removed != nil {
			toClose = append(toClose, removed)
		}
		p.mu.Unlock()
		for _, retired := range toClose {
			closeEntry(retired)
		}
		toClose = nil
	} else {
		p.mu.Unlock()
	}

	b, st, err := p.createBackend(ctx, t)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		if e.s3EncryptionPolicy == s3EncryptionPolicy && (t.StorageNamespaceID == "" || e.storageNamespaceID == t.StorageNamespaceID) {
			b.Close()
			_ = st.Close()
			p.order.MoveToFront(e.elem)
			p.mu.Unlock()
			return e.backend, nil
		}
		if removed := p.removeLocked(e.elem); removed != nil {
			toClose = append(toClose, removed)
		}
	}
	e := &entry{tenantID: t.ID, storageNamespaceID: t.StorageNamespaceID, s3EncryptionPolicy: s3EncryptionPolicy, backend: b, store: st}
	e.elem = p.order.PushFront(e)
	p.items[t.ID] = e
	for p.order.Len() > p.maxSize {
		oldest := p.order.Back()
		if oldest != nil {
			if removed := p.removeLocked(oldest); removed != nil {
				toClose = append(toClose, removed)
			}
		}
	}
	p.mu.Unlock()
	for _, retired := range toClose {
		closeEntry(retired)
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
			p.mu.Unlock()
			logger.InfoBenchTiming(ctx, "tenant_pool_acquire_timing",
				zap.String("tenant_id", t.ID),
				zap.Bool("cache_hit", true),
				zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
			return e.backend, p.makeRelease(e), nil
		}
		if removed := p.removeLocked(e.elem); removed != nil {
			toClose = append(toClose, removed)
		}
		p.mu.Unlock()
		for _, retired := range toClose {
			closeEntry(retired)
		}
		toClose = nil
	} else {
		p.mu.Unlock()
	}

	createBackendStart := time.Now()
	b, st, err := p.createBackend(ctx, t)
	if err != nil {
		return nil, nil, err
	}
	createBackendDurationMs := float64(time.Since(createBackendStart).Microseconds()) / 1000.0

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		if e.s3EncryptionPolicy == s3EncryptionPolicy && (t.StorageNamespaceID == "" || e.storageNamespaceID == t.StorageNamespaceID) {
			e.refs++
			p.order.MoveToFront(e.elem)
			p.mu.Unlock()
			b.Close()
			_ = st.Close()
			logger.InfoBenchTiming(ctx, "tenant_pool_acquire_timing",
				zap.String("tenant_id", t.ID),
				zap.Bool("cache_hit", true),
				zap.Float64("create_backend_ms", createBackendDurationMs),
				zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
			return e.backend, p.makeRelease(e), nil
		}
		if removed := p.removeLocked(e.elem); removed != nil {
			toClose = append(toClose, removed)
		}
	}
	e := &entry{tenantID: t.ID, storageNamespaceID: t.StorageNamespaceID, s3EncryptionPolicy: s3EncryptionPolicy, backend: b, store: st, refs: 1}
	e.elem = p.order.PushFront(e)
	p.items[t.ID] = e
	for p.order.Len() > p.maxSize {
		oldest := p.order.Back()
		if oldest != nil {
			if removed := p.removeLocked(oldest); removed != nil {
				toClose = append(toClose, removed)
			}
		}
	}
	p.mu.Unlock()
	for _, retired := range toClose {
		closeEntry(retired)
	}
	logger.InfoBenchTiming(ctx, "tenant_pool_acquire_timing",
		zap.String("tenant_id", t.ID),
		zap.Bool("cache_hit", false),
		zap.Float64("create_backend_ms", createBackendDurationMs),
		zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
	return b, p.makeRelease(e), nil
}

func (p *Pool) Invalidate(tenantID string) {
	var toClose *entry
	p.mu.Lock()
	if e, ok := p.items[tenantID]; ok {
		toClose = p.removeLocked(e.elem)
	}
	p.mu.Unlock()
	closeEntry(toClose)
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

func (p *Pool) InvalidateAndWait(ctx context.Context, tenantID string) error {
	ctx, cancel := withTenantPoolDrainTimeout(ctx)
	defer cancel()
	var retired *entry
	var toClose *entry
	p.mu.Lock()
	if e, ok := p.items[tenantID]; ok {
		retired = e
		toClose = p.removeLocked(e.elem)
	}
	p.mu.Unlock()
	closeEntry(toClose)
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

func (p *Pool) Close() {
	toClose := make([]*entry, 0, p.order.Len())
	p.mu.Lock()
	for p.order.Len() > 0 {
		if removed := p.removeLocked(p.order.Back()); removed != nil {
			toClose = append(toClose, removed)
		}
	}
	p.mu.Unlock()
	for _, retired := range toClose {
		closeEntry(retired)
	}
}

func (p *Pool) S3Backend(tenantID string) *backend.Dat9Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.items[tenantID]; ok {
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
	// When database auto-embedding is disabled, image/audio extract tasks are
	// still valid (they don't depend on EMBED_TEXT). However the semantic worker
	// routing for TiDB auto-embedding is suppressed, which is handled in
	// taskTypesForProvider via IsAutoEmbeddingDisabled().
	if len(out) == 0 {
		return nil
	}
	return out
}

// IsAutoEmbeddingDisabled reports whether database auto-embedding has been
// explicitly disabled for this pool via DisableDatabaseAutoEmbedding.
// This is distinct from AutoSemanticTaskTypes returning nil because no
// image/audio extract is configured.
func (p *Pool) IsAutoEmbeddingDisabled() bool {
	return p != nil && p.cfg.DisableDatabaseAutoEmbedding
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

func (p *Pool) createBackend(ctx context.Context, t *meta.Tenant) (*backend.Dat9Backend, *datastore.Store, error) {
	start := time.Now()
	decryptStart := time.Now()
	pass, err := p.enc.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypt db password: %w", err)
	}
	decryptDurationMs := float64(time.Since(decryptStart).Microseconds()) / 1000.0
	opts := p.cfg.BackendOptions
	resolvedEncryptionPolicy, err := meta.ResolveS3EncryptionPolicy(p.cfg.S3EncryptionPolicy, t.S3EncryptionPolicy())
	if err != nil {
		return nil, nil, fmt.Errorf("resolve s3 encryption policy: %w", err)
	}
	opts.TenantID = t.ID
	opts.S3EncryptionPolicy = resolvedEncryptionPolicy
	if UsesTiDBAutoEmbedding(t.Provider) && !p.cfg.DisableDatabaseAutoEmbedding {
		opts.DatabaseAutoEmbedding = true
	}
	query := "parseTime=true"
	if t.DBTLS {
		query += "&tls=true"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", t.DBUser, string(pass), t.DBHost, t.DBPort, t.DBName, query)
	openStoreStart := time.Now()
	store, err := datastore.Open(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open datastore: %w", err)
	}
	if p.cfg.DisableDatabaseAutoEmbedding && UsesTiDBAutoEmbedding(t.Provider) {
		store.DisableAutoEmbedTextWrites()
	}
	openStoreDurationMs := float64(time.Since(openStoreStart).Microseconds()) / 1000.0
	ensureSchemaDurationMs := 0.0
	migrateDurationMs := 0.0
	if opts.DatabaseAutoEmbedding && (t.Provider == ProviderTiDBZero || t.Provider == ProviderTiDBCloudStarter || t.Provider == ProviderTiDBCloudNative) {
		autoEmbeddingProfile, err := p.autoEmbeddingProfileForTenant(ctx, t)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("resolve tenant auto-embedding profile: %w", err)
		}
		if err := applyTiDBAutoEmbeddingProviderConfig(ctx, store.DB(), autoEmbeddingProfile.provider); err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("apply tenant auto-embedding provider config: %w", err)
		}
		if !p.cfg.SkipTiDBSchemaCheck {
			targetSchemaVersion, err := schema.TiDBTenantSchemaVersionForAutoEmbeddingProfile(autoEmbeddingProfile.schemaProfile)
			if err != nil {
				_ = store.Close()
				return nil, nil, fmt.Errorf("resolve tenant auto-embedding schema version: %w", err)
			}
			if t.SchemaVersion != targetSchemaVersion {
				ensureSchemaStart := time.Now()
				if err := ensureTiDBSchemaForAutoEmbeddingProfile(ctx, store.DB(), autoEmbeddingProfile.schemaProfile); err != nil {
					_ = store.Close()
					return nil, nil, fmt.Errorf("ensure tidb auto-embedding schema: %w", err)
				}
				ensureSchemaDurationMs = float64(time.Since(ensureSchemaStart).Microseconds()) / 1000.0
				if p.metaStore != nil {
					// Record the tenant-profile-specific version only after the
					// schema has been confirmed to match that tenant's profile.
					// Any tenant whose schema diverges between two consecutive
					// opens will be caught on the next open because its stored
					// version will differ from the profile-derived target.
					updateSchemaVersionStart := time.Now()
					if verErr := p.metaStore.UpdateTenantSchemaVersion(ctx, t.ID, targetSchemaVersion); verErr != nil {
						recordTenantSchemaVersionUpdateFailure(ctx, t.ID, targetSchemaVersion, time.Since(updateSchemaVersionStart), verErr)
					}
				}
			} else if p.shouldPeriodicValidateTiDBSchemaOnOpen() {
				validateSchemaStart := time.Now()
				if err := validateTiDBSchemaForAutoEmbeddingProfile(ctx, store.DB(), autoEmbeddingProfile.schemaProfile); err != nil {
					_ = store.Close()
					return nil, nil, fmt.Errorf("validate tidb auto-embedding schema: %w", err)
				}
				ensureSchemaDurationMs = float64(time.Since(validateSchemaStart).Microseconds()) / 1000.0
			}
		}
	}
	// Run split-tables migration if needed. Only for tenants that have the
	// legacy files table; new tenants skip it entirely.
	migrateStart := time.Now()
	if store.HasLegacyFiles() {
		if err := p.migrateSplitTables(ctx, store.DB(), t.Provider); err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("migrate split tables: %w", err)
		}
	}
	migrateDurationMs = float64(time.Since(migrateStart).Microseconds()) / 1000.0
	if p.cfg.S3Bucket != "" {
		ns, err := p.resolveStorageNamespace(ctx, t, "s3", p.cfg.S3Bucket)
		if err != nil {
			_ = store.Close()
			return nil, nil, err
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
			return nil, nil, fmt.Errorf("create aws s3 client: %w", err)
		}
		s3ClientDurationMs := float64(time.Since(s3ClientStart).Microseconds()) / 1000.0
		smallInDB := SmallInDB(t.Provider)
		backendCreateStart := time.Now()
		b, err := backend.NewWithS3ModeAndOptions(store, s3c, smallInDB, opts)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("create backend with s3 mode: %w", err)
		}
		backendCreateDurationMs := float64(time.Since(backendCreateStart).Microseconds()) / 1000.0
		logger.InfoBenchTiming(ctx, "tenant_pool_create_backend_timing",
			zap.String("tenant_id", t.ID),
			zap.String("provider", t.Provider),
			zap.String("storage_mode", "aws_s3"),
			zap.Float64("decrypt_db_password_ms", decryptDurationMs),
			zap.Float64("open_datastore_ms", openStoreDurationMs),
			zap.Float64("ensure_schema_ms", ensureSchemaDurationMs),
			zap.Float64("migrate_duration_ms", migrateDurationMs),
			zap.Float64("create_s3_client_ms", s3ClientDurationMs),
			zap.Float64("create_backend_ms", backendCreateDurationMs),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
		p.wireQuotaStore(b, t.ID)
		p.wireSemanticTaskNotifier(b, t.ID)
		b.StartFileGCWorker(backend.FileGCWorkerOptions{})
		return b, store, nil
	}
	if p.cfg.S3Dir != "" {
		ns, err := p.resolveStorageNamespace(ctx, t, "local", "")
		if err != nil {
			_ = store.Close()
			return nil, nil, err
		}
		opts.StorageNamespaceID = ns.ID
		localPrefix := strings.Trim(ns.Prefix, "/")
		s3Dir := strings.TrimRight(p.cfg.S3Dir, "/") + "/" + localPrefix
		s3BaseURL := strings.TrimRight(p.cfg.PublicURL, "/") + "/s3/" + localPrefix
		s3ClientStart := time.Now()
		s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("create local s3 client: %w", err)
		}
		s3ClientDurationMs := float64(time.Since(s3ClientStart).Microseconds()) / 1000.0
		smallInDB := SmallInDB(t.Provider)
		backendCreateStart := time.Now()
		b, err := backend.NewWithS3ModeAndOptions(store, s3c, smallInDB, opts)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("create backend with local s3 mode: %w", err)
		}
		backendCreateDurationMs := float64(time.Since(backendCreateStart).Microseconds()) / 1000.0
		logger.InfoBenchTiming(ctx, "tenant_pool_create_backend_timing",
			zap.String("tenant_id", t.ID),
			zap.String("provider", t.Provider),
			zap.String("storage_mode", "local_s3"),
			zap.Float64("decrypt_db_password_ms", decryptDurationMs),
			zap.Float64("open_datastore_ms", openStoreDurationMs),
			zap.Float64("ensure_schema_ms", ensureSchemaDurationMs),
			zap.Float64("migrate_duration_ms", migrateDurationMs),
			zap.Float64("create_s3_client_ms", s3ClientDurationMs),
			zap.Float64("create_backend_ms", backendCreateDurationMs),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
		p.wireQuotaStore(b, t.ID)
		p.wireSemanticTaskNotifier(b, t.ID)
		b.StartFileGCWorker(backend.FileGCWorkerOptions{})
		return b, store, nil
	}
	backendCreateStart := time.Now()
	b, err := backend.NewWithOptions(store, opts)
	if err != nil {
		_ = store.Close()
		return nil, nil, fmt.Errorf("create backend: %w", err)
	}
	backendCreateDurationMs := float64(time.Since(backendCreateStart).Microseconds()) / 1000.0
	logger.InfoBenchTiming(ctx, "tenant_pool_create_backend_timing",
		zap.String("tenant_id", t.ID),
		zap.String("provider", t.Provider),
		zap.String("storage_mode", "db_only"),
		zap.Float64("decrypt_db_password_ms", decryptDurationMs),
		zap.Float64("open_datastore_ms", openStoreDurationMs),
		zap.Float64("ensure_schema_ms", ensureSchemaDurationMs),
		zap.Float64("migrate_duration_ms", migrateDurationMs),
		zap.Float64("create_s3_client_ms", 0),
		zap.Float64("create_backend_ms", backendCreateDurationMs),
		zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
	p.wireQuotaStore(b, t.ID)
	p.wireSemanticTaskNotifier(b, t.ID)
	b.StartFileGCWorker(backend.FileGCWorkerOptions{})
	return b, store, nil
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
		return defaultTenantAutoEmbeddingProfile()
	}
	profile, err := p.metaStore.EnsureTenantAutoEmbeddingProfile(ctx, t.ID)
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
		provider: schema.TiDBAutoEmbeddingProviderConfig{
			Model: schemaProfile.Model,
		},
	}, nil
}

func (p *Pool) shouldPeriodicValidateTiDBSchemaOnOpen() bool {
	if p == nil {
		return false
	}
	every := periodicTiDBSchemaValidationEvery
	if every == 0 {
		return false
	}
	count := p.tidbSchemaValidationOpens.Add(1)
	return count == 1 || count%every == 0
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
// Also ensures a tenant_quota_usage row exists so that counter UPDATEs
// do not fail for newly provisioned tenants.
func (p *Pool) wireQuotaStore(b *backend.Dat9Backend, tenantID string) {
	if p.metaStore == nil {
		return
	}
	adapter := NewMetaQuotaAdapter(p.metaStore)
	b.SetMetaQuotaStore(tenantID, adapter)
	// Bootstrap quota usage row (INSERT IGNORE — idempotent, cheap).
	if err := p.metaStore.EnsureQuotaUsageRow(context.Background(), tenantID); err != nil {
		logger.Warn(context.Background(), "wire_quota_store_ensure_usage_row_failed",
			zap.String("tenant_id", tenantID),
			zap.Error(err))
	}
}

func (p *Pool) removeLocked(elem *list.Element) *entry {
	e := elem.Value.(*entry)
	p.order.Remove(elem)
	e.elem = nil
	delete(p.items, e.tenantID)
	e.retired = true
	if e.refs == 0 {
		return e
	}
	return nil
}

func (p *Pool) makeRelease(e *entry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.releaseEntry(e)
		})
	}
}

func (p *Pool) releaseEntry(e *entry) {
	if e == nil {
		return
	}
	var toClose *entry
	p.mu.Lock()
	if e.refs > 0 {
		e.refs--
	}
	if e.refs == 0 && e.retired {
		toClose = e
	}
	p.mu.Unlock()
	closeEntry(toClose)
}

func closeEntry(e *entry) {
	if e == nil {
		return
	}
	if e.backend != nil {
		e.backend.Close()
	}
	if e.store != nil {
		_ = e.store.Close()
	}
}

func observePool(ctx context.Context, op, tenantID string, errp *error, start time.Time) {
	result := "ok"
	if errp != nil && *errp != nil {
		switch {
		case errors.Is(*errp, meta.ErrNotFound):
			result = "not_found"
		default:
			result = "error"
		}
		logger.Error(ctx, "tenant_pool_op_failed", zap.String("operation", op), zap.String("tenant_id", tenantID), zap.String("result", result), zap.Error(*errp))
	}
	metrics.RecordOperation("tenant_pool", op, result, time.Since(start))
}
