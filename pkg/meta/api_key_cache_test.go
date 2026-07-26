package meta

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

func TestAPIKeyResolveCacheExportsHitMissAndEntries(t *testing.T) {
	c := newAPIKeyResolveCache()
	if _, ok := c.get("metrics-miss"); ok {
		t.Fatal("unexpected cache hit")
	}
	c.fill("metrics-hit", TenantWithAPIKey{Tenant: Tenant{ID: "cache-metrics-tenant"}})
	if _, ok := c.get("metrics-hit"); !ok {
		t.Fatal("expected cache hit")
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text := rec.Body.String()
	for _, want := range []string{
		`drive9_api_key_resolve_cache_requests_total{result="miss"}`,
		`drive9_api_key_resolve_cache_requests_total{result="hit"}`,
		`drive9_api_key_resolve_cache_entries 1.000000`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing API key cache metric %q:\n%s", want, text)
		}
	}
}

func insertCacheTestTenant(t *testing.T, s *Store, tenantID string) {
	t.Helper()
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               tenantID,
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db",
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
}

func insertCacheTestKey(t *testing.T, s *Store, tenantID, keyID, hash string) {
	t.Helper()
	now := time.Now().UTC()
	if err := s.InsertAPIKey(context.Background(), &APIKey{
		ID:            keyID,
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: []byte("jwt-cipher"),
		JWTHash:       hash,
		TokenVersion:  1,
		Status:        APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestResolveByAPIKeyHashCachesWithinTTL(t *testing.T) {
	s := newControlStore(t)
	insertCacheTestTenant(t, s, "cache-t1")
	insertCacheTestKey(t, s, "cache-t1", "cache-k1", "cache-hash1")

	if _, err := s.ResolveByAPIKeyHash(context.Background(), "cache-hash1"); err != nil {
		t.Fatal(err)
	}
	if got := s.apiKeys.hits.Load(); got != 0 {
		t.Fatalf("cache hits after first resolve = %d, want 0", got)
	}
	got, err := s.ResolveByAPIKeyHash(context.Background(), "cache-hash1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Tenant.ID != "cache-t1" || got.APIKey.ID != "cache-k1" {
		t.Fatalf("cached resolve = tenant %s key %s, want cache-t1/cache-k1", got.Tenant.ID, got.APIKey.ID)
	}
	if hits := s.apiKeys.hits.Load(); hits != 1 {
		t.Fatalf("cache hits after second resolve = %d, want 1", hits)
	}
}

func TestResolveByAPIKeyHashDoesNotCacheNotFound(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-missing-hash"); err != ErrNotFound {
		t.Fatalf("resolve missing hash error = %v, want ErrNotFound", err)
	}
	// A newly created key must be visible immediately, even though a resolve
	// for the same hash returned ErrNotFound just before.
	insertCacheTestTenant(t, s, "cache-t2")
	insertCacheTestKey(t, s, "cache-t2", "cache-k2", "cache-missing-hash")
	got, err := s.ResolveByAPIKeyHash(ctx, "cache-missing-hash")
	if err != nil {
		t.Fatalf("resolve after insert error = %v, want nil", err)
	}
	if got.APIKey.ID != "cache-k2" {
		t.Fatalf("resolved key = %s, want cache-k2", got.APIKey.ID)
	}
}

func TestResolveByAPIKeyHashRevokeAPIKeyEvictsCache(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	insertCacheTestTenant(t, s, "cache-t3")
	insertCacheTestKey(t, s, "cache-t3", "cache-k3", "cache-hash3")

	// Populate the cache; the second resolve must be served from it.
	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash3"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash3"); err != nil {
		t.Fatal(err)
	}
	if hits := s.apiKeys.hits.Load(); hits != 1 {
		t.Fatalf("cache hits = %d, want 1", hits)
	}

	if err := s.RevokeAPIKey(ctx, "cache-t3", "cache-k3"); err != nil {
		t.Fatal(err)
	}
	missesBefore := s.apiKeys.misses.Load()
	revoked, err := s.ResolveByAPIKeyHash(ctx, "cache-hash3")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.apiKeys.misses.Load(); got != missesBefore+1 {
		t.Fatalf("cache misses after revoke = %d, want %d (entry evicted)", got, missesBefore+1)
	}
	if revoked.APIKey.Status != APIKeyRevoked {
		t.Fatalf("revoked key status = %s, want %s", revoked.APIKey.Status, APIKeyRevoked)
	}
}

func TestResolveByAPIKeyHashRevokeTenantAPIKeysEvictsCache(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	insertCacheTestTenant(t, s, "cache-t4")
	insertCacheTestKey(t, s, "cache-t4", "cache-k4", "cache-hash4")

	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash4"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash4"); err != nil {
		t.Fatal(err)
	}
	if hits := s.apiKeys.hits.Load(); hits != 1 {
		t.Fatalf("cache hits = %d, want 1", hits)
	}

	if err := s.RevokeTenantAPIKeys(ctx, "cache-t4"); err != nil {
		t.Fatal(err)
	}
	missesBefore := s.apiKeys.misses.Load()
	revoked, err := s.ResolveByAPIKeyHash(ctx, "cache-hash4")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.apiKeys.misses.Load(); got != missesBefore+1 {
		t.Fatalf("cache misses after RevokeTenantAPIKeys = %d, want %d", got, missesBefore+1)
	}
	if revoked.APIKey.Status != APIKeyRevoked {
		t.Fatalf("tenant-revoked key status = %s, want %s", revoked.APIKey.Status, APIKeyRevoked)
	}
}

func TestResolveByAPIKeyHashRevokeByIssuerEvictsCache(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	insertCacheTestTenant(t, s, "cache-t5")
	insertCacheTestKey(t, s, "cache-t5", "cache-k5", "cache-hash5")
	if _, err := s.DB().ExecContext(ctx, `UPDATE tenant_api_keys
		SET issued_by_provider = 'slock', issued_by_subject_key = 'subject-1'
		WHERE id = 'cache-k5'`); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash5"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash5"); err != nil {
		t.Fatal(err)
	}
	if hits := s.apiKeys.hits.Load(); hits != 1 {
		t.Fatalf("cache hits = %d, want 1", hits)
	}

	if err := s.RevokeAPIKeysByIssuer(ctx, "cache-t5", "slock", "subject-1", ""); err != nil {
		t.Fatal(err)
	}
	missesBefore := s.apiKeys.misses.Load()
	revoked, err := s.ResolveByAPIKeyHash(ctx, "cache-hash5")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.apiKeys.misses.Load(); got != missesBefore+1 {
		t.Fatalf("cache misses after issuer revoke = %d, want %d", got, missesBefore+1)
	}
	if revoked.APIKey.Status != APIKeyRevoked {
		t.Fatalf("issuer-revoked key status = %s, want %s", revoked.APIKey.Status, APIKeyRevoked)
	}
}

func TestResolveByAPIKeyHashTenantStatusUpdateEvictsCache(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	insertCacheTestTenant(t, s, "cache-t8")
	insertCacheTestKey(t, s, "cache-t8", "cache-k8", "cache-hash8")

	// Populate the cache; the second resolve must be served from it.
	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash8"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash8"); err != nil {
		t.Fatal(err)
	}
	if hits := s.apiKeys.hits.Load(); hits != 1 {
		t.Fatalf("cache hits = %d, want 1", hits)
	}

	if err := s.UpdateTenantStatus(ctx, "cache-t8", TenantSuspended); err != nil {
		t.Fatal(err)
	}
	missesBefore := s.apiKeys.misses.Load()
	got, err := s.ResolveByAPIKeyHash(ctx, "cache-hash8")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.apiKeys.misses.Load(); got != missesBefore+1 {
		t.Fatalf("cache misses after status update = %d, want %d (entry evicted)", got, missesBefore+1)
	}
	if got.Tenant.Status != TenantSuspended {
		t.Fatalf("tenant status after update = %s, want %s (no TTL wait)", got.Tenant.Status, TenantSuspended)
	}
}

func TestResolveByAPIKeyHashDBCredentialUpdateEvictsCache(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	insertCacheTestTenant(t, s, "cache-t9")
	insertCacheTestKey(t, s, "cache-t9", "cache-k9", "cache-hash9")

	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash9"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveByAPIKeyHash(ctx, "cache-hash9"); err != nil {
		t.Fatal(err)
	}
	if hits := s.apiKeys.hits.Load(); hits != 1 {
		t.Fatalf("cache hits = %d, want 1", hits)
	}

	updated, err := s.UpdateTenantDBCredentialIf(ctx, "cache-t9", "root", "app_user", []byte("new-cipher"))
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("UpdateTenantDBCredentialIf updated = false, want true")
	}
	missesBefore := s.apiKeys.misses.Load()
	got, err := s.ResolveByAPIKeyHash(ctx, "cache-hash9")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.apiKeys.misses.Load(); got != missesBefore+1 {
		t.Fatalf("cache misses after credential update = %d, want %d (entry evicted)", got, missesBefore+1)
	}
	if got.Tenant.DBUser != "app_user" || string(got.Tenant.DBPasswordCipher) != "new-cipher" {
		t.Fatalf("credentials after update = %q/%q, want app_user/new-cipher (no TTL wait)",
			got.Tenant.DBUser, got.Tenant.DBPasswordCipher)
	}
}

func TestResolveByAPIKeyHashTTLExpiryRefetches(t *testing.T) {
	s := newControlStore(t)
	s.apiKeys.ttl = 20 * time.Millisecond
	insertCacheTestTenant(t, s, "cache-t6")
	insertCacheTestKey(t, s, "cache-t6", "cache-k6", "cache-hash6")

	if _, err := s.ResolveByAPIKeyHash(context.Background(), "cache-hash6"); err != nil {
		t.Fatal(err)
	}
	missesBefore := s.apiKeys.misses.Load()
	time.Sleep(40 * time.Millisecond)
	if _, err := s.ResolveByAPIKeyHash(context.Background(), "cache-hash6"); err != nil {
		t.Fatal(err)
	}
	if got := s.apiKeys.misses.Load(); got != missesBefore+1 {
		t.Fatalf("cache misses after TTL expiry = %d, want %d (entry re-fetched)", got, missesBefore+1)
	}
}

func TestResolveByAPIKeyHashCachedEntryIsIsolatedPerCaller(t *testing.T) {
	s := newControlStore(t)
	insertCacheTestTenant(t, s, "cache-t7")
	insertCacheTestKey(t, s, "cache-t7", "cache-k7", "cache-hash7")

	first, err := s.ResolveByAPIKeyHash(context.Background(), "cache-hash7")
	if err != nil {
		t.Fatal(err)
	}
	// Populate the cache, then mutate the first returned copy in every mutable
	// field; the cached entry and later callers must stay intact.
	if _, err := s.ResolveByAPIKeyHash(context.Background(), "cache-hash7"); err != nil {
		t.Fatal(err)
	}
	first.APIKey.JWTCiphertext[0] ^= 0xff
	first.Tenant.DBPasswordCipher[0] ^= 0xff
	first.Tenant.Status = TenantSuspended

	again, err := s.ResolveByAPIKeyHash(context.Background(), "cache-hash7")
	if err != nil {
		t.Fatal(err)
	}
	if hits := s.apiKeys.hits.Load(); hits != 2 {
		t.Fatalf("cache hits = %d, want 2", hits)
	}
	if string(again.APIKey.JWTCiphertext) != "jwt-cipher" {
		t.Fatalf("cached jwt ciphertext mutated: %q", again.APIKey.JWTCiphertext)
	}
	if string(again.Tenant.DBPasswordCipher) != "cipher" {
		t.Fatalf("cached db password cipher mutated: %q", again.Tenant.DBPasswordCipher)
	}
	if again.Tenant.Status != TenantActive {
		t.Fatalf("cached tenant status mutated: %s", again.Tenant.Status)
	}
}
