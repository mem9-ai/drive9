package server

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

const (
	tidbCloudRBACCacheTTL        = time.Hour
	tidbCloudRBACCacheMaxEntries = 10000
	tidbCloudRBACCacheMaxLists   = 1000
)

type tidbCloudRBACCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]tidbCloudRBACEntry
	lists   map[string]tidbCloudRBACListEntry
}

type tidbCloudRBACEntry struct {
	clusterID      string
	organizationID string
	expiresAt      time.Time
}

type tidbCloudRBACListEntry struct {
	clusters  []tenant.CloudClusterInfo
	expiresAt time.Time
}

func newTiDBCloudRBACCache(ttl time.Duration) *tidbCloudRBACCache {
	if ttl <= 0 {
		ttl = tidbCloudRBACCacheTTL
	}
	return &tidbCloudRBACCache{
		ttl:     ttl,
		entries: make(map[string]tidbCloudRBACEntry),
		lists:   make(map[string]tidbCloudRBACListEntry),
	}
}

func (c *tidbCloudRBACCache) getCluster(cred tenant.CredentialProvisionRequest, clusterID string) (tenant.CloudClusterInfo, bool) {
	if c == nil {
		return tenant.CloudClusterInfo{}, false
	}
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return tenant.CloudClusterInfo{}, false
	}
	key := c.entryKey(cred, clusterID)
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return tenant.CloudClusterInfo{}, false
	}
	if now.After(entry.expiresAt) {
		c.mu.Lock()
		if current, currentOK := c.entries[key]; currentOK && now.After(current.expiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return tenant.CloudClusterInfo{}, false
	}
	return tenant.CloudClusterInfo{ClusterID: entry.clusterID, OrganizationID: entry.organizationID}, true
}

func (c *tidbCloudRBACCache) getClusterList(cred tenant.CredentialProvisionRequest) ([]tenant.CloudClusterInfo, bool) {
	if c == nil {
		return nil, false
	}
	key := c.credentialKey(cred)
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.lists[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if now.After(entry.expiresAt) {
		c.mu.Lock()
		if current, currentOK := c.lists[key]; currentOK && now.After(current.expiresAt) {
			delete(c.lists, key)
		}
		c.mu.Unlock()
		return nil, false
	}
	out := make([]tenant.CloudClusterInfo, len(entry.clusters))
	copy(out, entry.clusters)
	return out, true
}

func (c *tidbCloudRBACCache) rememberCluster(cred tenant.CredentialProvisionRequest, cluster tenant.CloudClusterInfo) {
	if c == nil {
		return
	}
	cluster.ClusterID = strings.TrimSpace(cluster.ClusterID)
	if cluster.ClusterID == "" {
		return
	}
	cluster.OrganizationID = strings.TrimSpace(cluster.OrganizationID)
	key := c.entryKey(cred, cluster.ClusterID)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(time.Now())
	c.entries[key] = tidbCloudRBACEntry{
		clusterID:      cluster.ClusterID,
		organizationID: cluster.OrganizationID,
		expiresAt:      time.Now().Add(c.ttl),
	}
	c.enforceSizeLocked()
}

func (c *tidbCloudRBACCache) rememberClusterList(cred tenant.CredentialProvisionRequest, clusters []tenant.CloudClusterInfo) {
	if c == nil {
		return
	}
	now := time.Now()
	expiresAt := now.Add(c.ttl)
	seen := make(map[string]bool, len(clusters))
	out := make([]tenant.CloudClusterInfo, 0, len(clusters))
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	for _, cluster := range clusters {
		cluster.ClusterID = strings.TrimSpace(cluster.ClusterID)
		if cluster.ClusterID == "" || seen[cluster.ClusterID] {
			continue
		}
		cluster.OrganizationID = strings.TrimSpace(cluster.OrganizationID)
		seen[cluster.ClusterID] = true
		c.entries[c.entryKey(cred, cluster.ClusterID)] = tidbCloudRBACEntry{
			clusterID:      cluster.ClusterID,
			organizationID: cluster.OrganizationID,
			expiresAt:      expiresAt,
		}
		out = append(out, tenant.CloudClusterInfo{ClusterID: cluster.ClusterID, OrganizationID: cluster.OrganizationID})
	}
	key := c.credentialKey(cred)
	if len(out) == 0 {
		delete(c.lists, key)
		return
	}
	c.lists[key] = tidbCloudRBACListEntry{clusters: out, expiresAt: expiresAt}
	c.enforceSizeLocked()
}

func (c *tidbCloudRBACCache) forgetClusterList(cred tenant.CredentialProvisionRequest) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.lists, c.credentialKey(cred))
}

func (c *tidbCloudRBACCache) pruneLocked(now time.Time) {
	for key, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, key)
		}
	}
	for key, entry := range c.lists {
		if now.After(entry.expiresAt) {
			delete(c.lists, key)
		}
	}
}

func (c *tidbCloudRBACCache) enforceSizeLocked() {
	for len(c.entries) > tidbCloudRBACCacheMaxEntries {
		for key := range c.entries {
			delete(c.entries, key)
			break
		}
	}
	for len(c.lists) > tidbCloudRBACCacheMaxLists {
		for key := range c.lists {
			delete(c.lists, key)
			break
		}
	}
}

func (c *tidbCloudRBACCache) credentialKey(cred tenant.CredentialProvisionRequest) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(cred.PublicKey) + "\x00" + strings.TrimSpace(cred.PrivateKey)))
	return hex.EncodeToString(sum[:])
}

func (c *tidbCloudRBACCache) entryKey(cred tenant.CredentialProvisionRequest, clusterID string) string {
	return c.credentialKey(cred) + "\x00" + strings.TrimSpace(clusterID)
}
