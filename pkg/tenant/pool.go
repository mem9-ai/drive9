package tenant

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

type PoolConfig struct {
	MaxTenants int
	S3Dir      string
	PublicURL  string
}

type Pool struct {
	mu      sync.Mutex
	cfg     PoolConfig
	enc     encrypt.Encryptor
	items   map[string]*list.Element
	order   *list.List
	maxSize int
}

type entry struct {
	tenantID string
	backend  *backend.Dat9Backend
	store    *datastore.Store
}

func NewPool(cfg PoolConfig, enc encrypt.Encryptor) *Pool {
	max := cfg.MaxTenants
	if max <= 0 {
		max = 128
	}
	return &Pool{cfg: cfg, enc: enc, items: map[string]*list.Element{}, order: list.New(), maxSize: max}
}

func (p *Pool) Get(t *meta.Tenant) (*backend.Dat9Backend, error) {
	if t.Status != meta.TenantActive {
		p.Invalidate(t.ID)
		return nil, fmt.Errorf("tenant status: %s", t.Status)
	}

	p.mu.Lock()
	if elem, ok := p.items[t.ID]; ok {
		p.order.MoveToFront(elem)
		b := elem.Value.(*entry).backend
		p.mu.Unlock()
		return b, nil
	}
	p.mu.Unlock()

	b, st, err := p.createBackend(t)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if elem, ok := p.items[t.ID]; ok {
		_ = st.Close()
		p.order.MoveToFront(elem)
		return elem.Value.(*entry).backend, nil
	}
	elem := p.order.PushFront(&entry{tenantID: t.ID, backend: b, store: st})
	p.items[t.ID] = elem
	for p.order.Len() > p.maxSize {
		oldest := p.order.Back()
		if oldest != nil {
			p.removeLocked(oldest)
		}
	}
	return b, nil
}

func (p *Pool) Invalidate(tenantID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if elem, ok := p.items[tenantID]; ok {
		p.removeLocked(elem)
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.order.Len() > 0 {
		p.removeLocked(p.order.Back())
	}
}

func (p *Pool) S3Backend(tenantID string) *backend.Dat9Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	if elem, ok := p.items[tenantID]; ok {
		return elem.Value.(*entry).backend
	}
	return nil
}

func (p *Pool) Decrypt(cipher []byte) ([]byte, error) {
	return p.enc.Decrypt(context.Background(), cipher)
}

func (p *Pool) Encrypt(plain []byte) ([]byte, error) {
	return p.enc.Encrypt(context.Background(), plain)
}

func (p *Pool) LoadS3Backend(metaStore *meta.Store, tenantID string) *backend.Dat9Backend {
	b := p.S3Backend(tenantID)
	if b != nil {
		return b
	}
	tenant, err := metaStore.GetTenant(tenantID)
	if err != nil {
		return nil
	}
	b, err = p.Get(tenant)
	if err != nil {
		return nil
	}
	return b
}

func (p *Pool) createBackend(t *meta.Tenant) (*backend.Dat9Backend, *datastore.Store, error) {
	pass, err := p.enc.Decrypt(context.Background(), t.DBPasswordCipher)
	if err != nil {
		return nil, nil, err
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", t.DBUser, string(pass), t.DBHost, t.DBPort, t.DBName)
	store, err := datastore.Open(dsn)
	if err != nil {
		return nil, nil, err
	}
	if p.cfg.S3Dir != "" {
		s3Dir := p.cfg.S3Dir + "/" + t.ID
		s3BaseURL := p.cfg.PublicURL + "/s3/" + t.ID
		s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
		if err != nil {
			_ = store.Close()
			return nil, nil, err
		}
		smallInDB := SmallInDB(t.Provider)
		b, err := backend.NewWithS3Mode(store, s3c, smallInDB)
		if err != nil {
			_ = store.Close()
			return nil, nil, err
		}
		return b, store, nil
	}
	b, err := backend.New(store)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	return b, store, nil
}

func (p *Pool) removeLocked(elem *list.Element) {
	e := elem.Value.(*entry)
	p.order.Remove(elem)
	delete(p.items, e.tenantID)
	if e.store != nil {
		_ = e.store.Close()
	}
}
