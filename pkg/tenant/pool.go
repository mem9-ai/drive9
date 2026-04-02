package tenant

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"go.uber.org/zap"
)

type PoolConfig struct {
	MaxTenants int
	S3Dir      string
	PublicURL  string
	S3Bucket   string
	S3Region   string
	S3Prefix   string
	S3RoleARN  string

	BackendOptions backend.Options
}

type Pool struct {
	mu      sync.Mutex
	cfg     PoolConfig
	enc     encrypt.Encryptor
	items   map[string]*entry
	order   *list.List
	maxSize int
}

type entry struct {
	tenantID string
	backend  *backend.Dat9Backend
	store    *datastore.Store
	elem     *list.Element
	refs     int
	retired  bool
}

func NewPool(cfg PoolConfig, enc encrypt.Encryptor) *Pool {
	max := cfg.MaxTenants
	if max <= 0 {
		max = 128
	}
	return &Pool{cfg: cfg, enc: enc, items: map[string]*entry{}, order: list.New(), maxSize: max}
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

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		p.order.MoveToFront(e.elem)
		b := e.backend
		p.mu.Unlock()
		return b, nil
	}
	p.mu.Unlock()

	b, st, err := p.createBackend(ctx, t)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		b.Close()
		_ = st.Close()
		p.order.MoveToFront(e.elem)
		p.mu.Unlock()
		return e.backend, nil
	}
	e := &entry{tenantID: t.ID, backend: b, store: st}
	e.elem = p.order.PushFront(e)
	p.items[t.ID] = e
	toClose := make([]*entry, 0, 1)
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

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		e.refs++
		p.order.MoveToFront(e.elem)
		p.mu.Unlock()
		return e.backend, p.makeRelease(e), nil
	}
	p.mu.Unlock()

	b, st, err := p.createBackend(ctx, t)
	if err != nil {
		return nil, nil, err
	}

	p.mu.Lock()
	if e, ok := p.items[t.ID]; ok {
		e.refs++
		p.order.MoveToFront(e.elem)
		p.mu.Unlock()
		b.Close()
		_ = st.Close()
		return e.backend, p.makeRelease(e), nil
	}
	e := &entry{tenantID: t.ID, backend: b, store: st, refs: 1}
	e.elem = p.order.PushFront(e)
	p.items[t.ID] = e
	toClose := make([]*entry, 0, 1)
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
	pass, err := p.enc.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypt db password: %w", err)
	}
	opts := p.cfg.BackendOptions
	if UsesTiDBAutoEmbedding(t.Provider) {
		opts.DatabaseAutoEmbedding = true
	}
	query := "parseTime=true"
	if t.DBTLS {
		query += "&tls=true"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", t.DBUser, string(pass), t.DBHost, t.DBPort, t.DBName, query)
	store, err := datastore.Open(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open datastore: %w", err)
	}
	if opts.DatabaseAutoEmbedding && (t.Provider == ProviderTiDBZero || t.Provider == ProviderTiDBCloudStarter) {
		if err := ValidateTiDBSchemaForMode(store.DB(), TiDBEmbeddingModeAuto); err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("validate tidb auto-embedding schema: %w", err)
		}
	}
	if p.cfg.S3Bucket != "" {
		prefix := strings.Trim(p.cfg.S3Prefix, "/")
		if prefix != "" {
			prefix += "/"
		}
		prefix += t.ID + "/"
		s3c, err := s3client.NewAWS(ctx, s3client.AWSConfig{
			Region:  p.cfg.S3Region,
			Bucket:  p.cfg.S3Bucket,
			Prefix:  prefix,
			RoleARN: p.cfg.S3RoleARN,
		})
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("create aws s3 client: %w", err)
		}
		smallInDB := SmallInDB(t.Provider)
		b, err := backend.NewWithS3ModeAndOptions(store, s3c, smallInDB, opts)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("create backend with s3 mode: %w", err)
		}
		return b, store, nil
	}
	if p.cfg.S3Dir != "" {
		s3Dir := p.cfg.S3Dir + "/" + t.ID
		s3BaseURL := p.cfg.PublicURL + "/s3/" + t.ID
		s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("create local s3 client: %w", err)
		}
		smallInDB := SmallInDB(t.Provider)
		b, err := backend.NewWithS3ModeAndOptions(store, s3c, smallInDB, opts)
		if err != nil {
			_ = store.Close()
			return nil, nil, fmt.Errorf("create backend with local s3 mode: %w", err)
		}
		return b, store, nil
	}
	b, err := backend.NewWithOptions(store, opts)
	if err != nil {
		_ = store.Close()
		return nil, nil, fmt.Errorf("create backend: %w", err)
	}
	return b, store, nil
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
