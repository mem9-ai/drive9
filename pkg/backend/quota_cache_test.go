package backend

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// cacheTestStore wraps fakeMetaQuotaStore with error injection for cache tests.
type cacheTestStore struct {
	*fakeMetaQuotaStore
	configCalls  atomic.Int64
	versionCalls atomic.Int64
	usageCalls   atomic.Int64
	versionErr   error
	configErr    error
}

func newCacheTestStore() *cacheTestStore {
	return &cacheTestStore{fakeMetaQuotaStore: newFakeMetaQuotaStore()}
}

func (m *cacheTestStore) GetQuotaUsage(ctx context.Context, tenantID string) (*QuotaUsageView, error) {
	m.usageCalls.Add(1)
	return m.fakeMetaQuotaStore.GetQuotaUsage(ctx, tenantID)
}

func (m *cacheTestStore) GetQuotaConfig(ctx context.Context, tenantID string) (*QuotaConfigView, error) {
	m.configCalls.Add(1)
	if m.configErr != nil {
		return nil, m.configErr
	}
	return m.fakeMetaQuotaStore.GetQuotaConfig(ctx, tenantID)
}

func (m *cacheTestStore) GetQuotaConfigVersion(ctx context.Context, tenantID string) (string, error) {
	m.versionCalls.Add(1)
	if m.versionErr != nil {
		return "", m.versionErr
	}
	return m.fakeMetaQuotaStore.GetQuotaConfigVersion(ctx, tenantID)
}

func TestQuotaConfigCacheLazyLoad(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	cfg := c.get()
	if cfg != nil {
		t.Fatalf("config = %+v, want nil before lazy load", cfg)
	}
	if got := store.versionCalls.Load(); got != 0 {
		t.Fatalf("versionCalls = %d, want 0", got)
	}
	if got := store.configCalls.Load(); got != 0 {
		t.Fatalf("configCalls = %d, want 0", got)
	}

	cfg = c.load(context.Background())
	if cfg == nil {
		t.Fatal("config is nil after lazy load")
	}
	if cfg.MaxStorageBytes != 1000 {
		t.Fatalf("MaxStorageBytes = %d, want 1000", cfg.MaxStorageBytes)
	}
	if got := store.versionCalls.Load(); got != 0 {
		t.Fatalf("versionCalls = %d, want 0", got)
	}
	if got := store.configCalls.Load(); got != 1 {
		t.Fatalf("configCalls = %d, want 1", got)
	}
	if got := store.usageCalls.Load(); got != 0 {
		t.Fatalf("usageCalls = %d, want 0", got)
	}
}

func TestQuotaConfigCacheRefreshFailOpenOnVersionError(t *testing.T) {
	store := newCacheTestStore()
	store.versionErr = context.DeadlineExceeded
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	c.refresh(context.Background())
	if cfg := c.get(); cfg != nil {
		t.Fatalf("config = %+v, want nil", cfg)
	}
	if got := store.versionCalls.Load(); got != 1 {
		t.Fatalf("versionCalls = %d, want 1", got)
	}
	if got := store.configCalls.Load(); got != 0 {
		t.Fatalf("configCalls = %d, want 0", got)
	}
	if got := store.usageCalls.Load(); got != 0 {
		t.Fatalf("usageCalls = %d, want 0", got)
	}
}

func TestQuotaConfigCacheRefreshOnlyLoadsConfigWhenVersionChanges(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	c.refresh(context.Background())
	if got := store.versionCalls.Load(); got != 1 {
		t.Fatalf("versionCalls = %d, want 1", got)
	}
	if got := store.configCalls.Load(); got != 1 {
		t.Fatalf("configCalls = %d, want 1", got)
	}

	c.refresh(context.Background())
	if got := store.versionCalls.Load(); got != 2 {
		t.Fatalf("versionCalls = %d, want 2", got)
	}
	if got := store.configCalls.Load(); got != 1 {
		t.Fatalf("configCalls = %d, want 1", got)
	}

	store.mu.Lock()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 2000}
	store.mu.Unlock()
	c.refresh(context.Background())

	cfg := c.get()
	if cfg == nil {
		t.Fatal("config is nil")
	}
	if cfg.MaxStorageBytes != 2000 {
		t.Fatalf("MaxStorageBytes = %d, want 2000", cfg.MaxStorageBytes)
	}
	if got := store.versionCalls.Load(); got != 3 {
		t.Fatalf("versionCalls = %d, want 3", got)
	}
	if got := store.configCalls.Load(); got != 2 {
		t.Fatalf("configCalls = %d, want 2", got)
	}
	if got := store.usageCalls.Load(); got != 0 {
		t.Fatalf("usageCalls = %d, want 0", got)
	}
}

func TestQuotaUsageCacheUsesTTL(t *testing.T) {
	store := newCacheTestStore()
	store.usage["t1"] = &QuotaUsageView{TenantID: "t1", StorageBytes: 10}
	c := newQuotaUsageCache("t1", store, time.Hour)

	first := c.get(context.Background())
	if first == nil || first.StorageBytes != 10 {
		t.Fatalf("first usage = %+v, want storage 10", first)
	}
	store.mu.Lock()
	store.usage["t1"].StorageBytes = 20
	store.mu.Unlock()
	second := c.get(context.Background())
	if second == nil || second.StorageBytes != 10 {
		t.Fatalf("second usage = %+v, want cached storage 10", second)
	}
	if got := store.usageCalls.Load(); got != 1 {
		t.Fatalf("usageCalls = %d, want 1", got)
	}
}

func TestQuotaConfigCacheStop(t *testing.T) {
	store := newCacheTestStore()
	c := newQuotaConfigCache("t1", store)
	c.stop()
	// Should not panic or block.
}
