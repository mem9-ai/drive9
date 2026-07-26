package backend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// cacheTestStore wraps fakeMetaQuotaStore with error injection for cache tests.
type cacheTestStore struct {
	*fakeMetaQuotaStore
	configCalls   atomic.Int64
	usageCalls    atomic.Int64
	configErr     error
	configHook    func()
	configCtxHook func(context.Context)
	usageHook     func()
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
	if m.configCtxHook != nil {
		m.configCtxHook(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.configErr != nil {
		return nil, m.configErr
	}
	if m.configHook != nil {
		m.configHook()
	}
	return m.fakeMetaQuotaStore.GetQuotaConfig(ctx, tenantID)
}

func TestQuotaConfigCacheCanceledCallerDoesNotPoisonSharedRefresh(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	loadHasDeadline := false
	store.configCtxHook = func(ctx context.Context) {
		_, loadHasDeadline = ctx.Deadline()
	}
	c := newQuotaConfigCache("t1", "", store)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	first := c.get(canceledCtx)
	if first == nil || first.MaxStorageBytes != 1000 {
		t.Errorf("first config = %+v, want storage 1000", first)
	}
	if !loadHasDeadline {
		t.Error("shared load context has no deadline")
	}
	second := c.get(context.Background())
	if second == nil || second.MaxStorageBytes != 1000 {
		t.Errorf("second config = %+v, want storage 1000", second)
	}
	if got := store.configCalls.Load(); got != 1 {
		t.Errorf("configCalls = %d, want 1 shared load", got)
	}
}

func TestQuotaConfigCacheRefreshDelayStaysWithinTTL(t *testing.T) {
	const ttl = 30 * time.Second
	for range 1000 {
		delay := quotaConfigCacheRefreshDelay(ttl)
		if delay < 27*time.Second || delay > ttl {
			t.Errorf("refresh delay = %s, want [27s, 30s]", delay)
		}
	}
}

func TestQuotaConfigCacheFailureCooldownIsFiveSeconds(t *testing.T) {
	store := newCacheTestStore()
	store.configErr = errors.New("temporary metadb failure")
	c := newQuotaConfigCache("t1", "", store)

	before := time.Now()
	if cfg := c.get(context.Background()); cfg != nil {
		t.Errorf("config = %+v, want nil", cfg)
	}
	c.mu.RLock()
	nextRefresh := c.nextRefresh
	c.mu.RUnlock()
	if delay := nextRefresh.Sub(before); delay < 5*time.Second || delay > 6*time.Second {
		t.Errorf("failure cooldown = %s, want approximately 5s", delay)
	}
}

func TestQuotaConfigCacheColdWaiterHonorsOwnDeadline(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	store.configHook = func() {
		startedOnce.Do(func() { close(started) })
		<-release
	}
	c := newQuotaConfigCache("t1", "", store)

	leaderDone := make(chan *QuotaConfigView, 1)
	go func() { leaderDone <- c.get(context.Background()) }()
	<-started

	waiterCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	waiterDone := make(chan *QuotaConfigView, 1)
	go func() { waiterDone <- c.get(waiterCtx) }()

	select {
	case cfg := <-waiterDone:
		if cfg != nil {
			t.Errorf("waiter config = %+v, want nil before cold load completes", cfg)
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("cold waiter remained blocked after its context deadline")
	}

	unblock()
	if cfg := <-leaderDone; cfg == nil || cfg.MaxStorageBytes != 1000 {
		t.Errorf("leader config = %+v, want storage 1000", cfg)
	}
}

func TestQuotaConfigCacheWarmWaiterReturnsStaleWithoutWaiting(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", "", store)
	if cfg := c.get(context.Background()); cfg == nil || cfg.MaxStorageBytes != 1000 {
		t.Fatalf("initial config = %+v, want storage 1000", cfg)
	}

	c.mu.Lock()
	c.nextRefresh = time.Time{}
	c.mu.Unlock()
	store.mu.Lock()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 2000}
	store.mu.Unlock()
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	store.configHook = func() {
		startedOnce.Do(func() { close(started) })
		<-release
	}

	leaderDone := make(chan *QuotaConfigView, 1)
	go func() { leaderDone <- c.get(context.Background()) }()
	<-started
	waiterDone := make(chan *QuotaConfigView, 1)
	go func() { waiterDone <- c.get(context.Background()) }()

	select {
	case cfg := <-waiterDone:
		if cfg == nil || cfg.MaxStorageBytes != 1000 {
			t.Errorf("waiter config = %+v, want stale storage 1000", cfg)
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("warm waiter blocked behind the in-flight refresh")
	}

	unblock()
	if cfg := <-leaderDone; cfg == nil || cfg.MaxStorageBytes != 2000 {
		t.Errorf("leader config = %+v, want refreshed storage 2000", cfg)
	}
}

func TestQuotaConfigCacheIsPassiveUntilFirstAccess(t *testing.T) {
	previousRefreshInterval := quotaConfigCacheRefreshInterval
	quotaConfigCacheRefreshInterval = 5 * time.Millisecond
	t.Cleanup(func() { quotaConfigCacheRefreshInterval = previousRefreshInterval })

	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", "", store)

	// Construction must not start a polling loop or touch the store.
	time.Sleep(20 * time.Millisecond)
	if got := store.configCalls.Load(); got != 0 {
		t.Errorf("configCalls = %d, want 0", got)
	}

	cfg := c.get(context.Background())
	if cfg == nil {
		t.Fatal("config is nil after first access")
	}
	if cfg.MaxStorageBytes != 1000 {
		t.Errorf("MaxStorageBytes = %d, want 1000", cfg.MaxStorageBytes)
	}
	if got := store.configCalls.Load(); got != 1 {
		t.Errorf("configCalls = %d, want 1", got)
	}
	if got := store.usageCalls.Load(); got != 0 {
		t.Errorf("usageCalls = %d, want 0", got)
	}
}

func TestQuotaConfigCacheReturnsDefensiveCopy(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", "", store)

	cfg := c.get(context.Background())
	if cfg == nil {
		t.Fatal("config is nil")
	}
	cfg.MaxStorageBytes = 2000

	cached := c.get(context.Background())
	if cached == nil || cached.MaxStorageBytes != 1000 {
		t.Errorf("cached config = %+v, want storage 1000", cached)
	}
}

func TestQuotaConfigCacheReusesSnapshotUntilTTLExpires(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", "", store)

	if cfg := c.get(context.Background()); cfg == nil || cfg.MaxStorageBytes != 1000 {
		t.Errorf("first config = %+v, want storage 1000", cfg)
	}
	store.mu.Lock()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 2000}
	store.mu.Unlock()
	if cfg := c.get(context.Background()); cfg == nil || cfg.MaxStorageBytes != 1000 {
		t.Errorf("cached config = %+v, want storage 1000", cfg)
	}
	if got := store.configCalls.Load(); got != 1 {
		t.Errorf("configCalls before expiry = %d, want 1", got)
	}

	c.mu.Lock()
	c.nextRefresh = time.Time{}
	c.mu.Unlock()
	if cfg := c.get(context.Background()); cfg == nil || cfg.MaxStorageBytes != 2000 {
		t.Errorf("refreshed config = %+v, want storage 2000", cfg)
	}
	if got := store.configCalls.Load(); got != 2 {
		t.Errorf("configCalls after expiry = %d, want 2", got)
	}
}

func TestQuotaConfigCacheCoalescesConcurrentExpiredAccess(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", "", store)
	if cfg := c.get(context.Background()); cfg == nil {
		t.Fatal("initial config is nil")
	}
	c.mu.Lock()
	c.nextRefresh = time.Time{}
	c.mu.Unlock()

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.configHook = func() {
		once.Do(func() { close(started) })
		<-release
	}
	const callers = 32
	results := make(chan *QuotaConfigView, callers)
	for range callers {
		go func() { results <- c.get(context.Background()) }()
	}
	<-started
	close(release)
	for range callers {
		if cfg := <-results; cfg == nil || cfg.MaxStorageBytes != 1000 {
			t.Errorf("concurrent config = %+v, want storage 1000", cfg)
		}
	}
	if got := store.configCalls.Load(); got != 2 {
		t.Errorf("configCalls = %d, want initial load plus one coalesced refresh", got)
	}
}

func TestQuotaConfigCacheRefreshFailureReturnsStaleAndUsesRetryCooldown(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", "", store)

	first := c.get(context.Background())
	if first == nil {
		t.Fatal("initial config is nil")
	}
	c.mu.Lock()
	c.nextRefresh = time.Time{}
	c.mu.Unlock()
	store.configErr = errors.New("temporary metadb failure")

	stale := c.get(context.Background())
	if stale == nil || stale.MaxStorageBytes != 1000 {
		t.Errorf("stale config = %+v, want storage 1000", stale)
	}
	if got := store.configCalls.Load(); got != 2 {
		t.Errorf("configCalls after failed refresh = %d, want 2", got)
	}
	stale = c.get(context.Background())
	if stale == nil || stale.MaxStorageBytes != 1000 {
		t.Errorf("cooldown config = %+v, want storage 1000", stale)
	}
	if got := store.configCalls.Load(); got != 2 {
		t.Errorf("configCalls during failure cooldown = %d, want 2", got)
	}
}

func TestQuotaConfigCacheInitialFailureUsesRetryCooldown(t *testing.T) {
	store := newCacheTestStore()
	store.configErr = errors.New("temporary metadb failure")
	c := newQuotaConfigCache("t1", "", store)

	if cfg := c.get(context.Background()); cfg != nil {
		t.Errorf("config = %+v, want nil on initial failure", cfg)
	}
	if cfg := c.get(context.Background()); cfg != nil {
		t.Errorf("config during cooldown = %+v, want nil", cfg)
	}
	if got := store.configCalls.Load(); got != 1 {
		t.Errorf("configCalls during failure cooldown = %d, want 1", got)
	}
	if got := store.usageCalls.Load(); got != 0 {
		t.Errorf("usageCalls = %d, want 0", got)
	}
}

func TestQuotaUsageCacheUsesTTL(t *testing.T) {
	store := newCacheTestStore()
	store.usage["t1"] = &QuotaUsageView{TenantID: "t1", StorageBytes: 10}
	c := newQuotaUsageCache("t1", "", store, time.Hour)

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
	c := newQuotaUsageCache("t1", "", store, time.Hour)
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

func TestQuotaUsageCacheInvalidateForcesReload(t *testing.T) {
	store := newCacheTestStore()
	store.usage["t1"] = &QuotaUsageView{TenantID: "t1", StorageBytes: 10}
	c := newQuotaUsageCache("t1", "", store, time.Hour)

	first := c.get(context.Background())
	if first == nil || first.StorageBytes != 10 {
		t.Fatalf("first usage = %+v, want storage 10", first)
	}
	store.mu.Lock()
	store.usage["t1"].StorageBytes = 20
	store.mu.Unlock()
	if cached := c.get(context.Background()); cached == nil || cached.StorageBytes != 10 {
		t.Fatalf("cached usage = %+v, want storage 10", cached)
	}

	c.invalidate()
	reloaded := c.get(context.Background())
	if reloaded == nil || reloaded.StorageBytes != 20 {
		t.Fatalf("reloaded usage = %+v, want storage 20", reloaded)
	}
	if got := store.usageCalls.Load(); got != 2 {
		t.Fatalf("usageCalls = %d, want 2", got)
	}
}

func TestQuotaPendingDeltasCacheUsesTTLAndLocalAdjustments(t *testing.T) {
	var calls atomic.Int64
	storage := int64(10)
	file := int64(1)
	media := int64(0)
	c := newQuotaPendingDeltasCache("test-tenant", "", func(context.Context) (int64, int64, int64, error) {
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
	c.addPending(5, 2, 1)
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

func TestQuotaPendingDeltasCachePublishesConservativeSnapshotWhenLocalDeltaRacesLoad(t *testing.T) {
	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c := newQuotaPendingDeltasCache("test-tenant", "", func(context.Context) (int64, int64, int64, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-release
		return 10, 1, 0, nil
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
	c.addPending(5, 1, 0)
	close(release)

	got := <-done
	if !got.ok {
		t.Fatal("racing get failed")
	}
	if got.deltas.storageDelta != 15 || got.deltas.fileDelta != 2 || got.deltas.mediaDelta != 0 {
		t.Fatalf("racing deltas = %+v, want 15/2/0", got.deltas)
	}
	next, ok := c.get(context.Background())
	if !ok {
		t.Fatal("second get failed")
	}
	if next.storageDelta != 15 || next.fileDelta != 2 || next.mediaDelta != 0 {
		t.Fatalf("second deltas = %+v, want 15/2/0", next)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

func TestQuotaPendingDeltasCacheIgnoresNegativeRaceDeltasWhenPublishing(t *testing.T) {
	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c := newQuotaPendingDeltasCache("test-tenant", "", func(context.Context) (int64, int64, int64, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-release
		return 10, 2, 1, nil
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
	c.addPending(-5, -1, -1)
	close(release)

	got := <-done
	if !got.ok {
		t.Fatal("racing get failed")
	}
	if got.deltas.storageDelta != 10 || got.deltas.fileDelta != 2 || got.deltas.mediaDelta != 1 {
		t.Fatalf("racing deltas = %+v, want 10/2/1", got.deltas)
	}
	next, ok := c.get(context.Background())
	if !ok {
		t.Fatal("second get failed")
	}
	if next.storageDelta != 10 || next.fileDelta != 2 || next.mediaDelta != 1 {
		t.Fatalf("second deltas = %+v, want 10/2/1", next)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

func TestQuotaPendingDeltasCacheExpiresNoLoaderPending(t *testing.T) {
	c := newQuotaPendingDeltasCache("test-tenant", "", nil, time.Hour)
	c.pendingTTL = 10 * time.Millisecond

	c.addPending(8, 1, -1)
	first, ok := c.get(context.Background())
	if !ok {
		t.Fatal("first get failed")
	}
	if first.storageDelta != 8 || first.fileDelta != 1 || first.mediaDelta != -1 {
		t.Fatalf("first deltas = %+v, want 8/1/-1", first)
	}

	time.Sleep(20 * time.Millisecond)
	expired, ok := c.get(context.Background())
	if !ok {
		t.Fatal("expired get failed")
	}
	if expired.storageDelta != 0 || expired.fileDelta != 0 || expired.mediaDelta != 0 {
		t.Fatalf("expired deltas = %+v, want zero", expired)
	}
}

func TestQuotaPendingDeltasCacheClearPreventsExpiryDoubleSubtract(t *testing.T) {
	c := newQuotaPendingDeltasCache("test-tenant", "", nil, time.Hour)
	c.pendingTTL = 10 * time.Millisecond

	c.addPending(8, 1, 0)
	c.clearPending(8, 1, 0)
	cleared, ok := c.get(context.Background())
	if !ok {
		t.Fatal("cleared get failed")
	}
	if cleared.storageDelta != 0 || cleared.fileDelta != 0 || cleared.mediaDelta != 0 {
		t.Fatalf("cleared deltas = %+v, want zero", cleared)
	}

	time.Sleep(20 * time.Millisecond)
	expired, ok := c.get(context.Background())
	if !ok {
		t.Fatal("expired get failed")
	}
	if expired.storageDelta != 0 || expired.fileDelta != 0 || expired.mediaDelta != 0 {
		t.Fatalf("expired deltas = %+v, want zero", expired)
	}
}

func TestQuotaPendingDeltasCacheClearAfterExpiryDoesNotGoNegative(t *testing.T) {
	c := newQuotaPendingDeltasCache("test-tenant", "", nil, time.Hour)
	c.pendingTTL = 10 * time.Millisecond

	c.addPending(8, 1, 0)
	time.Sleep(20 * time.Millisecond)
	expired, ok := c.get(context.Background())
	if !ok {
		t.Fatal("expired get failed")
	}
	if expired.storageDelta != 0 || expired.fileDelta != 0 || expired.mediaDelta != 0 {
		t.Fatalf("expired deltas = %+v, want zero", expired)
	}

	c.clearPending(8, 1, 0)
	cleared, ok := c.get(context.Background())
	if !ok {
		t.Fatal("cleared get failed")
	}
	if cleared.storageDelta != 0 || cleared.fileDelta != 0 || cleared.mediaDelta != 0 {
		t.Fatalf("cleared deltas = %+v, want zero after late clear", cleared)
	}
}

func TestQuotaPendingDeltasCacheRemovesPositiveRaceDeltasOnClearAndExpire(t *testing.T) {
	c := newQuotaPendingDeltasCache("test-tenant", "", nil, time.Hour)
	c.pendingTTL = 10 * time.Millisecond

	c.addPending(8, 1, -1)
	c.clearPending(8, 1, -1)
	if got := c.localPositiveDeltas; got.storageDelta != 0 || got.fileDelta != 0 || got.mediaDelta != 0 {
		t.Fatalf("positive deltas after clear = %+v, want zero", got)
	}

	c.addPending(5, 2, 1)
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.get(context.Background()); !ok {
		t.Fatal("expired get failed")
	}
	if got := c.localPositiveDeltas; got.storageDelta != 0 || got.fileDelta != 0 || got.mediaDelta != 0 {
		t.Fatalf("positive deltas after expire = %+v, want zero", got)
	}
}
