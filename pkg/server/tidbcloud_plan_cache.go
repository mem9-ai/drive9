package server

import (
	"strings"
	"sync"
	"time"
)

const (
	defaultTiDBCloudNonFreePlanCacheTTL = 30 * time.Minute
	tidbCloudNonFreePlanCacheMaxEntries = 10000
)

type tidbCloudNonFreePlanCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]time.Time
	now     func() time.Time
}

func newTiDBCloudNonFreePlanCache(ttl time.Duration) *tidbCloudNonFreePlanCache {
	if ttl <= 0 {
		ttl = defaultTiDBCloudNonFreePlanCacheTTL
	}
	return &tidbCloudNonFreePlanCache{
		ttl:     ttl,
		entries: make(map[string]time.Time),
		now:     time.Now,
	}
}

func (c *tidbCloudNonFreePlanCache) isNonFree(organizationID string) bool {
	if c == nil {
		return false
	}
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return false
	}
	now := c.now()
	c.mu.RLock()
	expiresAt, ok := c.entries[organizationID]
	c.mu.RUnlock()
	if !ok {
		return false
	}
	if now.After(expiresAt) {
		c.mu.Lock()
		if current, exists := c.entries[organizationID]; exists && now.After(current) {
			delete(c.entries, organizationID)
		}
		c.mu.Unlock()
		return false
	}
	return true
}

func (c *tidbCloudNonFreePlanCache) rememberNonFree(organizationID string) {
	if c == nil {
		return
	}
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	c.entries[organizationID] = now.Add(c.ttl)
	for len(c.entries) > tidbCloudNonFreePlanCacheMaxEntries {
		for key := range c.entries {
			delete(c.entries, key)
			break
		}
	}
}

func (c *tidbCloudNonFreePlanCache) remove(organizationID string) {
	if c == nil {
		return
	}
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return
	}
	c.mu.Lock()
	delete(c.entries, organizationID)
	c.mu.Unlock()
}

func (c *tidbCloudNonFreePlanCache) pruneLocked(now time.Time) {
	for organizationID, expiresAt := range c.entries {
		if now.After(expiresAt) {
			delete(c.entries, organizationID)
		}
	}
}
