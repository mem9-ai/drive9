package backend

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestQuotaConfigCacheInitialLoad(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	cfg := c.get()
	require.NotNil(t, cfg)
	require.Equal(t, int64(1000), cfg.MaxStorageBytes)
	require.Equal(t, int64(1), store.versionCalls.Load())
	require.Equal(t, int64(1), store.configCalls.Load())
	require.Equal(t, int64(0), store.usageCalls.Load())
}

func TestQuotaConfigCacheFailOpenOnVersionError(t *testing.T) {
	store := newCacheTestStore()
	store.versionErr = context.DeadlineExceeded
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	require.Nil(t, c.get())
	require.Equal(t, int64(1), store.versionCalls.Load())
	require.Equal(t, int64(0), store.configCalls.Load())
	require.Equal(t, int64(0), store.usageCalls.Load())
}

func TestQuotaConfigCacheRefreshOnlyLoadsConfigWhenVersionChanges(t *testing.T) {
	store := newCacheTestStore()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaConfigCache("t1", store)
	defer c.stop()

	c.refresh(context.Background())
	require.Equal(t, int64(2), store.versionCalls.Load())
	require.Equal(t, int64(1), store.configCalls.Load())

	store.mu.Lock()
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 2000}
	store.mu.Unlock()
	c.refresh(context.Background())

	cfg := c.get()
	require.NotNil(t, cfg)
	require.Equal(t, int64(2000), cfg.MaxStorageBytes)
	require.Equal(t, int64(3), store.versionCalls.Load())
	require.Equal(t, int64(2), store.configCalls.Load())
	require.Equal(t, int64(0), store.usageCalls.Load())
}

func TestQuotaConfigCacheStop(t *testing.T) {
	store := newCacheTestStore()
	c := newQuotaConfigCache("t1", store)
	c.stop()
	// Should not panic or block.
}
