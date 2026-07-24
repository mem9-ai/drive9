package server

import (
	"fmt"
	"testing"
	"time"
)

func TestTiDBCloudNonFreePlanCacheHitExpiryAndRemoval(t *testing.T) {
	now := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	cache := newTiDBCloudNonFreePlanCache(30 * time.Minute)
	cache.now = func() time.Time { return now }

	if cache.isNonFree("org-1") {
		t.Fatal("empty cache returned a hit")
	}
	cache.rememberNonFree(" org-1 ")
	if !cache.isNonFree("org-1") {
		t.Fatal("remembered organization did not hit")
	}
	now = now.Add(30*time.Minute + time.Nanosecond)
	if cache.isNonFree("org-1") {
		t.Fatal("expired organization returned a hit")
	}
	cache.rememberNonFree("org-1")
	cache.remove("org-1")
	if cache.isNonFree("org-1") {
		t.Fatal("removed organization returned a hit")
	}
}

func TestTiDBCloudNonFreePlanCacheDefaultsAndBounds(t *testing.T) {
	cache := newTiDBCloudNonFreePlanCache(0)
	if cache.ttl != defaultTiDBCloudNonFreePlanCacheTTL {
		t.Fatalf("ttl = %v, want %v", cache.ttl, defaultTiDBCloudNonFreePlanCacheTTL)
	}
	for i := 0; i < tidbCloudNonFreePlanCacheMaxEntries+10; i++ {
		cache.rememberNonFree(fmt.Sprintf("org-%d", i))
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if len(cache.entries) != tidbCloudNonFreePlanCacheMaxEntries {
		t.Fatalf("entries = %d, want %d", len(cache.entries), tidbCloudNonFreePlanCacheMaxEntries)
	}
}

func TestTiDBCloudNonFreePlanCacheStoresOnlyOrganizationExpiry(t *testing.T) {
	cache := newTiDBCloudNonFreePlanCache(time.Hour)
	cache.rememberNonFree("org-no-secrets")
	cache.mu.RLock()
	entry, ok := cache.entries["org-no-secrets"]
	cache.mu.RUnlock()
	if !ok || entry.IsZero() {
		t.Fatalf("cache entry = %v, ok=%v", entry, ok)
	}
}
