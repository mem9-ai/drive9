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
)

type tidbCloudRBACCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]tidbCloudRBACIdentityEntry
}

type tidbCloudRBACIdentityEntry struct {
	identity  tenant.TiDBCloudAPIKeyIdentity
	expiresAt time.Time
}

func newTiDBCloudRBACCache(ttl time.Duration) *tidbCloudRBACCache {
	if ttl <= 0 {
		ttl = tidbCloudRBACCacheTTL
	}
	return &tidbCloudRBACCache{ttl: ttl, entries: make(map[string]tidbCloudRBACIdentityEntry)}
}

func (c *tidbCloudRBACCache) getIdentity(cred tenant.CredentialProvisionRequest) (tenant.TiDBCloudAPIKeyIdentity, bool) {
	if c == nil {
		return tenant.TiDBCloudAPIKeyIdentity{}, false
	}
	key := c.credentialKey(cred)
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return tenant.TiDBCloudAPIKeyIdentity{}, false
	}
	if now.After(entry.expiresAt) {
		c.mu.Lock()
		if current, currentOK := c.entries[key]; currentOK && now.After(current.expiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return tenant.TiDBCloudAPIKeyIdentity{}, false
	}
	return entry.identity, true
}

func (c *tidbCloudRBACCache) rememberIdentity(cred tenant.CredentialProvisionRequest, identity tenant.TiDBCloudAPIKeyIdentity) {
	if c == nil {
		return
	}
	identity.OrganizationID = strings.TrimSpace(identity.OrganizationID)
	identity.Role = strings.TrimSpace(identity.Role)
	if identity.OrganizationID == "" || identity.Role == "" {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	c.entries[c.credentialKey(cred)] = tidbCloudRBACIdentityEntry{identity: identity, expiresAt: now.Add(c.ttl)}
	c.enforceSizeLocked()
}

func (c *tidbCloudRBACCache) pruneLocked(now time.Time) {
	for key, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, key)
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
}

func (c *tidbCloudRBACCache) credentialKey(cred tenant.CredentialProvisionRequest) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(cred.PublicKey) + "\x00" + strings.TrimSpace(cred.PrivateKey)))
	return hex.EncodeToString(sum[:])
}
