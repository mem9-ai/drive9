package backend

import (
	"context"
	"sync"
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
	configHook   func()
	usageHook    func()
}

func newCacheTestStore() *cacheTestStore {
	return &cacheTestStore{fakeMetaQuotaStore: newFakeMetaQuotaStore()}
}

func (m *cacheTestStore) GetQuotaUsage(ctx context.Context, tenantID string) (*QuotaUsageView, error) {
	m.usageCalls.Add(1)
	if m.usageHook != nil {
		m.usageHook()
	}
	return m.fakeMetaQuotaStore.GetQuotaUsage(ctx, tenantID)
}

func (m *cacheTestStore) GetQuotaConfig(ctx context.Context, tenantID string) (*QuotaConfigView, error) {
	m.configCalls.Add(1)
	if m.configErr != nil {
		return nil, m.configErr
	}
	if m.configHook != nil {
		m.configHook()
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

func TestQuotaConfigCacheLazyLoadDoesNotOverwriteRefreshedSnapshot(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	store.configHook = func() {
		c.mu.Lock()
		c.snapshot = &quotaConfigSnapshot{
			config:  &QuotaConfigView{MaxStorageBytes: 2000},
			version: "new-version",
		}
		c.mu.Unlock()
	}

	cfg := c.load(context.Background())
	if cfg == nil {
		t.Fatal("config is nil")
	}
	if cfg.MaxStorageBytes != 2000 {
		t.Fatalf("lazy load config = %d, want refreshed 2000", cfg.MaxStorageBytes)
	}
	cached := c.get()
	if cached == nil || cached.MaxStorageBytes != 2000 {
		t.Fatalf("cached config = %+v, want refreshed 2000", cached)
	}
}

func TestQuotaConfigCacheLazyLoadReturnsDefensiveCopy(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	cfg := c.load(context.Background())
	if cfg == nil {
		t.Fatal("config is nil")
	}
	cfg.MaxStorageBytes = 2000

	cached := c.get()
	if cached == nil {
		t.Fatal("cached config is nil")
	}
	if cached.MaxStorageBytes != 1000 {
		t.Fatalf("cached MaxStorageBytes = %d, want 1000", cached.MaxStorageBytes)
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

func TestQuotaUsageCacheCoalescesConcurrentMisses(t *testing.T) {
	store := newCacheTestStore()
	store.usage["t1"] = &QuotaUsageView{TenantID: "t1", StorageBytes: 10}
	c := newQuotaUsageCache("t1", store, time.Hour)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.usageHook = func() {
		once.Do(func() { close(started) })
		<-release
	}

	const workers = 5
	results := make(chan *QuotaUsageView, workers)
	for i := 0; i < workers; i++ {
		go func() {
			results <- c.get(context.Background())
		}()
	}

	<-started
	close(release)
	for i := 0; i < workers; i++ {
		usage := <-results
		if usage == nil || usage.StorageBytes != 10 {
			t.Fatalf("usage = %+v, want storage 10", usage)
		}
	}
	if got := store.usageCalls.Load(); got != 1 {
		t.Fatalf("usageCalls = %d, want 1", got)
	}
}

func TestQuotaUsageCacheAddUpdatesFreshSnapshot(t *testing.T) {
	store := newCacheTestStore()
	store.usage["t1"] = &QuotaUsageView{TenantID: "t1", StorageBytes: 10, FileCount: 1}
	c := newQuotaUsageCache("t1", store, time.Hour)

	first := c.get(context.Background())
	if first == nil || first.StorageBytes != 10 || first.FileCount != 1 {
		t.Fatalf("first usage = %+v, want storage 10 file_count 1", first)
	}
	store.mu.Lock()
	store.usage["t1"].StorageBytes = 99
	store.usage["t1"].FileCount = 99
	store.mu.Unlock()
	c.add(5, 2, 1)
	updated := c.get(context.Background())
	if updated == nil || updated.StorageBytes != 15 || updated.FileCount != 3 || updated.MediaFileCount != 1 {
		t.Fatalf("updated usage = %+v, want storage 15 file_count 3 media_count 1", updated)
	}
	if got := store.usageCalls.Load(); got != 1 {
		t.Fatalf("usageCalls = %d, want 1", got)
	}
}

func TestQuotaPendingDeltasCacheUsesTTLAndLocalAdjustments(t *testing.T) {
	var calls atomic.Int64
	storage := int64(10)
	file := int64(1)
	media := int64(0)
	c := newQuotaPendingDeltasCache(func(context.Context) (int64, int64, int64, error) {
		calls.Add(1)
		return storage, file, media, nil
	}, time.Hour)

	first, ok := c.get(context.Background())
	if !ok {
		t.Fatal("first get failed")
	}
	if first.storageDelta != 10 || first.fileDelta != 1 || first.mediaDelta != 0 {
		t.Fatalf("first deltas = %+v, want 10/1/0", first)
	}

	storage = 99
	file = 9
	c.add(5, 2, 1)
	second, ok := c.get(context.Background())
	if !ok {
		t.Fatal("second get failed")
	}
	if second.storageDelta != 15 || second.fileDelta != 3 || second.mediaDelta != 1 {
		t.Fatalf("second deltas = %+v, want 15/3/1", second)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

func TestQuotaPendingDeltasCacheDoesNotPublishSnapshotWhenLocalDeltaRacesLoad(t *testing.T) {
	var calls atomic.Int64
	storage := int64(10)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c := newQuotaPendingDeltasCache(func(context.Context) (int64, int64, int64, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-release
		return storage, 1, 0, nil
	}, time.Hour)

	type result struct {
		deltas quotaPendingDeltas
		ok     bool
	}
	done := make(chan result, 1)
	go func() {
		deltas, ok := c.get(context.Background())
		done <- result{deltas: deltas, ok: ok}
	}()

	<-started
	c.add(5, 1, 0)
	storage = 15
	close(release)

	got := <-done
	if got.ok {
		t.Fatalf("racing get ok=true deltas=%+v, want fallback", got.deltas)
	}
	next, ok := c.get(context.Background())
	if !ok {
		t.Fatal("second get failed")
	}
	if next.storageDelta != 15 || next.fileDelta != 1 || next.mediaDelta != 0 {
		t.Fatalf("second deltas = %+v, want 15/1/0", next)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
}

func TestQuotaConfigCacheStop(t *testing.T) {
	store := newCacheTestStore()
	c := newQuotaConfigCache("t1", store)
	c.stop()
	// Should not panic or block.
}
