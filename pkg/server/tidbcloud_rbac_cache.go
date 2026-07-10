package server

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

const tidbCloudRBACCacheTTL = 6 * time.Hour

type tidbCloudRBACCache struct {
	mu      sync.Mutex
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
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[c.entryKey(cred, clusterID)]
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			delete(c.entries, c.entryKey(cred, clusterID))
		}
		return tenant.CloudClusterInfo{}, false
	}
	return tenant.CloudClusterInfo{ClusterID: entry.clusterID, OrganizationID: entry.organizationID}, true
}

func (c *tidbCloudRBACCache) getClusterList(cred tenant.CredentialProvisionRequest) ([]tenant.CloudClusterInfo, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.credentialKey(cred)
	entry, ok := c.lists[key]
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			delete(c.lists, key)
		}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[c.entryKey(cred, cluster.ClusterID)] = tidbCloudRBACEntry{
		clusterID:      cluster.ClusterID,
		organizationID: cluster.OrganizationID,
		expiresAt:      time.Now().Add(c.ttl),
	}
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
	c.lists[c.credentialKey(cred)] = tidbCloudRBACListEntry{clusters: out, expiresAt: expiresAt}
}

func (c *tidbCloudRBACCache) credentialKey(cred tenant.CredentialProvisionRequest) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(cred.PublicKey) + "\x00" + strings.TrimSpace(cred.PrivateKey)))
	return hex.EncodeToString(sum[:])
}

func (c *tidbCloudRBACCache) entryKey(cred tenant.CredentialProvisionRequest, clusterID string) string {
	return c.credentialKey(cred) + "\x00" + strings.TrimSpace(clusterID)
}
