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
	callCount atomic.Int64
	usageErr  error
	configErr error
}

func newCacheTestStore() *cacheTestStore {
	return &cacheTestStore{fakeMetaQuotaStore: newFakeMetaQuotaStore()}
}

func (m *cacheTestStore) GetQuotaUsage(ctx context.Context, tenantID string) (*QuotaUsageView, error) {
	m.callCount.Add(1)
	if m.usageErr != nil {
		return nil, m.usageErr
	}
	return m.fakeMetaQuotaStore.GetQuotaUsage(ctx, tenantID)
}

func (m *cacheTestStore) GetQuotaConfig(ctx context.Context, tenantID string) (*QuotaConfigView, error) {
	m.callCount.Add(1)
	if m.configErr != nil {
		return nil, m.configErr
	}
	return m.fakeMetaQuotaStore.GetQuotaConfig(ctx, tenantID)
}

func TestQuotaCache_InitialLoad(t *testing.T) {
	store := newCacheTestStore()
	store.usage["t1"] = &QuotaUsageView{StorageBytes: 100, ReservedBytes: 50}
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaCache("t1", store)
	defer c.stop()

	usage, cfg := c.get()
	require.NotNil(t, usage)
	require.NotNil(t, cfg)
	require.Equal(t, int64(100), usage.StorageBytes)
	require.Equal(t, int64(1000), cfg.MaxStorageBytes)
}

func TestQuotaCache_FailOpen_OnError(t *testing.T) {
	store := newCacheTestStore()
	store.usageErr = context.DeadlineExceeded
	c := newQuotaCache("t1", store)
	defer c.stop()

	usage, cfg := c.get()
	require.Nil(t, usage)
	require.Nil(t, cfg)
}

func TestQuotaCache_Refresh(t *testing.T) {
	store := newCacheTestStore()
	store.usage["t1"] = &QuotaUsageView{StorageBytes: 100}
	store.config["t1"] = &QuotaConfigView{MaxStorageBytes: 1000}
	c := newQuotaCache("t1", store)
	defer c.stop()

	// Update the underlying store and trigger refresh.
	store.mu.Lock()
	store.usage["t1"] = &QuotaUsageView{StorageBytes: 200}
	store.mu.Unlock()
	c.refresh(context.Background())

	usage, _ := c.get()
	require.Equal(t, int64(200), usage.StorageBytes)
}

func TestQuotaCache_Stop(t *testing.T) {
	store := newCacheTestStore()
	c := newQuotaCache("t1", store)
	c.stop()
	// Should not panic or block.
}
