package tenant

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

// PoolConfig configures the tenant backend pool.
type PoolConfig struct {
	MaxTenants int    // max cached backends (LRU eviction beyond this)
	BlobDir    string // base blob directory; per-tenant subdirs are created
	S3Dir      string // base S3 directory for local S3 (empty = no S3)
	PublicURL  string // public base URL for presigned URLs
}

// Pool caches per-tenant Dat9Backend instances with LRU eviction.
// Revoked/suspended tenants are rejected and evicted.
type Pool struct {
	mu      sync.Mutex
	cfg     PoolConfig
	enc     *Encryptor
	items   map[string]*list.Element // tenantID → LRU element
	order   *list.List               // front = most recently used
	maxSize int
}

type poolEntry struct {
	tenantID string
	backend  *backend.Dat9Backend
	store    *meta.Store
	status   Status // cached status for fast rejection
}

// NewPool creates a tenant backend pool.
func NewPool(cfg PoolConfig, enc *Encryptor) *Pool {
	maxSize := cfg.MaxTenants
	if maxSize <= 0 {
		maxSize = 128
	}
	return &Pool{
		cfg:     cfg,
		enc:     enc,
		items:   make(map[string]*list.Element),
		order:   list.New(),
		maxSize: maxSize,
	}
}

// Get returns a cached backend for the tenant, or creates one.
// Returns error if the tenant is not active.
func (p *Pool) Get(t *Tenant) (*backend.Dat9Backend, error) {
	if t.Status != StatusActive {
		// Reject and evict if cached
		p.Evict(t.ID)
		return nil, fmt.Errorf("tenant %s status: %s", t.ID, t.Status)
	}

	p.mu.Lock()
	if elem, ok := p.items[t.ID]; ok {
		entry := elem.Value.(*poolEntry)
		// Check if tenant was revoked since cached
		if entry.status != StatusActive {
			p.removeLocked(elem)
			p.mu.Unlock()
			return nil, fmt.Errorf("tenant %s status: %s", t.ID, entry.status)
		}
		p.order.MoveToFront(elem)
		b := entry.backend
		p.mu.Unlock()
		return b, nil
	}
	p.mu.Unlock()

	// Create new backend outside lock
	b, store, err := p.createBackend(t)
	if err != nil {
		return nil, fmt.Errorf("create backend for tenant %s: %w", t.ID, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check: another goroutine may have created it
	if elem, ok := p.items[t.ID]; ok {
		p.order.MoveToFront(elem)
		// Close the one we just created
		store.Close()
		return elem.Value.(*poolEntry).backend, nil
	}

	entry := &poolEntry{
		tenantID: t.ID,
		backend:  b,
		store:    store,
		status:   t.Status,
	}
	elem := p.order.PushFront(entry)
	p.items[t.ID] = elem

	// Evict LRU if over capacity
	for p.order.Len() > p.maxSize {
		oldest := p.order.Back()
		if oldest != nil {
			p.removeLocked(oldest)
		}
	}

	return b, nil
}

// S3Backend returns the cached backend's S3 client for a tenant, if available.
// Returns nil if the tenant is not cached or has no S3 client.
func (p *Pool) S3Backend(tenantID string) *backend.Dat9Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	if elem, ok := p.items[tenantID]; ok {
		return elem.Value.(*poolEntry).backend
	}
	return nil
}

// Evict removes a tenant's cached backend and closes its connections.
func (p *Pool) Evict(tenantID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if elem, ok := p.items[tenantID]; ok {
		p.removeLocked(elem)
	}
}

// Invalidate marks a tenant as non-active in the cache, causing the next
// Get to reject it. Also evicts the cached backend.
func (p *Pool) Invalidate(tenantID string) {
	p.Evict(tenantID)
}

// Close evicts all cached backends and closes their connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.order.Len() > 0 {
		p.removeLocked(p.order.Back())
	}
}

func (p *Pool) removeLocked(elem *list.Element) {
	entry := elem.Value.(*poolEntry)
	p.order.Remove(elem)
	delete(p.items, entry.tenantID)
	// Gracefully close DB connection
	if entry.store != nil {
		entry.store.Close()
	}
}

func (p *Pool) createBackend(t *Tenant) (*backend.Dat9Backend, *meta.Store, error) {
	// Decrypt password
	password, err := p.enc.Decrypt(t.DBPasswordEnc)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypt password: %w", err)
	}

	dsn := DSN(t.DBHost, t.DBPort, t.DBUser, string(password), t.DBName, t.DBTLS)
	store, err := meta.Open(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open tenant db: %w", err)
	}

	blobDir := p.cfg.BlobDir + "/" + t.ID
	if p.cfg.S3Dir != "" {
		s3Dir := p.cfg.S3Dir + "/" + t.ID
		s3BaseURL := p.cfg.PublicURL + "/s3/" + t.ID
		s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
		if err != nil {
			store.Close()
			return nil, nil, fmt.Errorf("create s3 client: %w", err)
		}
		b, err := backend.NewWithS3(store, blobDir, s3c)
		if err != nil {
			store.Close()
			return nil, nil, err
		}
		return b, store, nil
	}

	b, err := backend.New(store, blobDir)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	return b, store, nil
}
