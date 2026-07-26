package meta

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"

	"golang.org/x/sync/singleflight"
)

// defaultAPIKeyResolveCacheTTL bounds how long a resolved API key entry may be
// served from the in-process cache. Tenant-row and tenant_api_keys mutations
// made through this Store evict the tenant's cached entries via the tenant
// index; the TTL backstops changes made by other processes (each pod caches
// independently) and the residual fill-after-evict race (a resolve reading
// pre-mutation state concurrently with the mutation, then filling after the
// eviction — that window has no version check and closes only at TTL expiry).
const defaultAPIKeyResolveCacheTTL = 10 * time.Second

// envAPIKeyResolveCacheTTLMS overrides defaultAPIKeyResolveCacheTTL (in
// milliseconds). Empty, invalid, or non-positive values fall back to the
// default, mirroring the env parsing idiom in pkg/mysqlutil.
const envAPIKeyResolveCacheTTLMS = "DRIVE9_META_AUTH_CACHE_TTL_MS"

type apiKeyResolveCacheEntry struct {
	rec       TenantWithAPIKey
	expiresAt time.Time
}

// apiKeyResolveCache is a per-Store TTL cache for ResolveByAPIKeyHash results,
// keyed by jwt_hash. It exists because tenantAuthMiddleware resolves the API
// key on every authenticated request and the underlying 6-table join dominated
// metadb load under stress (76M execs/12h).
//
// Only successful resolutions are cached: ErrNotFound is never stored, so a
// newly created key is visible immediately. Misses on the same hash are
// collapsed with singleflight so a cold key costs one DB query per TTL window.
// Mutations made through this Store evict the tenant's cached entries through
// a tenant→hashes secondary index; expired entries are removed lazily on
// access (there is no background sweeper, the number of distinct API keys is
// small). Eviction is exact for entries already cached when the mutation
// commits; a resolve racing the mutation can still fill a pre-mutation record
// just after the evict, which only the TTL reaps (see the TTL comment).
type apiKeyResolveCache struct {
	ttl time.Duration

	mu             sync.Mutex
	byHash         map[string]apiKeyResolveCacheEntry
	hashesByTenant map[string]map[string]struct{}

	flight singleflight.Group
}

func newAPIKeyResolveCache() *apiKeyResolveCache {
	return &apiKeyResolveCache{
		ttl:            apiKeyResolveCacheTTLFromEnv(),
		byHash:         make(map[string]apiKeyResolveCacheEntry),
		hashesByTenant: make(map[string]map[string]struct{}),
	}
}

func apiKeyResolveCacheTTLFromEnv() time.Duration {
	raw := os.Getenv(envAPIKeyResolveCacheTTLMS)
	if raw == "" {
		return defaultAPIKeyResolveCacheTTL
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return defaultAPIKeyResolveCacheTTL
	}
	return time.Duration(ms) * time.Millisecond
}

// get returns a per-caller copy of the cached record so callers (and the
// tenant pool) can never mutate the shared entry.
func (c *apiKeyResolveCache) get(hash string) (*TenantWithAPIKey, bool) {
	rec, ok := c.lookup(hash)
	if !ok {
		metrics.RecordAPIKeyResolveCacheRequest("miss")
		return nil, false
	}
	metrics.RecordAPIKeyResolveCacheRequest("hit")
	return rec, true
}

// lookup is the counter-free internal read path; the singleflight double-check
// uses it so a single cold resolve accounts exactly one miss.
func (c *apiKeyResolveCache) lookup(hash string) (*TenantWithAPIKey, bool) {
	c.mu.Lock()
	entry, ok := c.byHash[hash]
	entriesAfterExpiry := -1
	if ok && !time.Now().Before(entry.expiresAt) {
		c.deleteLocked(hash)
		entriesAfterExpiry = len(c.byHash)
		ok = false
	}
	c.mu.Unlock()
	if entriesAfterExpiry >= 0 {
		metrics.RecordAPIKeyResolveCacheEntries(entriesAfterExpiry)
	}
	if !ok {
		return nil, false
	}
	rec := cloneTenantWithAPIKey(entry.rec)
	return &rec, true
}

// fill stores rec (already cloned by the caller) and indexes the hash under
// its tenant so revocations can evict precisely.
func (c *apiKeyResolveCache) fill(hash string, rec TenantWithAPIKey) {
	c.mu.Lock()
	if old, ok := c.byHash[hash]; ok {
		c.removeTenantIndexLocked(old.rec.Tenant.ID, hash)
	}
	c.byHash[hash] = apiKeyResolveCacheEntry{rec: rec, expiresAt: time.Now().Add(c.ttl)}
	set, ok := c.hashesByTenant[rec.Tenant.ID]
	if !ok {
		set = make(map[string]struct{})
		c.hashesByTenant[rec.Tenant.ID] = set
	}
	set[hash] = struct{}{}
	entries := len(c.byHash)
	c.mu.Unlock()
	metrics.RecordAPIKeyResolveCacheEntries(entries)
}

// evictTenant drops every cached hash known to belong to tenantID. Called
// after successful tenant_api_keys mutations (revocations) and tenants row
// mutations (status/provider/credential/connection/branch/schema updates —
// for tx-wrapped ones, only after the transaction commits); unknown tenants
// are a no-op.
func (c *apiKeyResolveCache) evictTenant(tenantID string) {
	c.mu.Lock()
	for hash := range c.hashesByTenant[tenantID] {
		delete(c.byHash, hash)
	}
	delete(c.hashesByTenant, tenantID)
	entries := len(c.byHash)
	c.mu.Unlock()
	metrics.RecordAPIKeyResolveCacheEntries(entries)
}

func (c *apiKeyResolveCache) deleteLocked(hash string) {
	entry, ok := c.byHash[hash]
	if !ok {
		return
	}
	delete(c.byHash, hash)
	c.removeTenantIndexLocked(entry.rec.Tenant.ID, hash)
}

func (c *apiKeyResolveCache) removeTenantIndexLocked(tenantID, hash string) {
	set, ok := c.hashesByTenant[tenantID]
	if !ok {
		return
	}
	delete(set, hash)
	if len(set) == 0 {
		delete(c.hashesByTenant, tenantID)
	}
}

// cloneTenantWithAPIKey copies the record by value and clones every mutable
// reference field (byte slices and pointer scalars) so the copy shares no
// mutable state with the source.
func cloneTenantWithAPIKey(rec TenantWithAPIKey) TenantWithAPIKey {
	out := rec
	out.Tenant.DBPasswordCipher = cloneBytes(rec.Tenant.DBPasswordCipher)
	out.Tenant.ClaimExpiresAt = cloneTimePtr(rec.Tenant.ClaimExpiresAt)
	out.Tenant.S3BucketKeyEnabled = cloneBoolPtr(rec.Tenant.S3BucketKeyEnabled)
	out.APIKey.JWTCiphertext = cloneBytes(rec.APIKey.JWTCiphertext)
	out.APIKey.IssuedByMetadataJSON = cloneBytes(rec.APIKey.IssuedByMetadataJSON)
	out.APIKey.RevokedAt = cloneTimePtr(rec.APIKey.RevokedAt)
	return out
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func cloneTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	out := *t
	return &out
}

func cloneBoolPtr(b *bool) *bool {
	if b == nil {
		return nil
	}
	out := *b
	return &out
}
