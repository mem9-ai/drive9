package server

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

// newMultiTenantTestServer creates a server in multi-tenant mode with a single active tenant.
// Returns (server, rawAPIKey, cleanup).
func newMultiTenantTestServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	if testDSN == "" {
		t.Skip("no test database available")
	}

	masterKey := make([]byte, 32)
	rand.Read(masterKey)
	enc, err := tenant.NewEncryptor(masterKey)
	if err != nil {
		t.Fatal(err)
	}

	// Open control plane store (reuses the test DB)
	tenantStore, err := tenant.OpenStore(testDSN, enc)
	if err != nil {
		t.Fatal(err)
	}

	// Clean tenant table
	tenantStore.DB().Exec("DELETE FROM tenants")

	// Create a tenant with its own meta store (reuses same DB for testing)
	raw, prefix, hash, err := tenant.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}

	pwEnc, err := tenantStore.EncryptPassword("unused")
	if err != nil {
		t.Fatal(err)
	}

	// Open tenant's meta store
	tenantMetaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, tenantMetaStore.DB())

	blobDir, _ := os.MkdirTemp("", "dat9-auth-blobs-*")

	tenantBackend, err := backend.New(tenantMetaStore, blobDir)
	if err != nil {
		t.Fatal(err)
	}

	now := mustNow()
	tid := hash[:16]
	tenantStore.Insert(&tenant.Tenant{
		ID:            tid,
		APIKeyPrefix:  prefix,
		APIKeyHash:    hash,
		Status:        tenant.StatusActive,
		DBHost:        "127.0.0.1",
		DBPort:        4000,
		DBUser:        "root",
		DBPasswordEnc: pwEnc,
		DBName:        "test",
		CreatedAt:     now,
		UpdatedAt:     now,
	})

	// Create pool that returns our pre-built backend
	pool := tenant.NewPool(tenant.PoolConfig{
		MaxTenants: 10,
		BlobDir:    blobDir,
	}, enc)

	// We need to inject the backend into the pool. Since pool.Get creates new
	// backends from DSN, and we're testing auth flow, use injectBackendMiddleware
	// approach — but we actually want the real auth middleware. For a proper test,
	// we'll set up so pool.Get works by having a valid DSN in the tenant record.
	// Since we're reusing testDSN, extract its components.

	// Actually, for auth tests, let's just use the server with fallback + tenant auth.
	// The simplest approach: create server with both tenant store and pool.
	srv := New(Config{
		Tenants:  tenantStore,
		Pool:     pool,
		AdminKey: "test-admin-key",
	})

	cleanup := func() {
		pool.Close()
		tenantStore.DB().Exec("DELETE FROM tenants")
		tenantStore.Close()
		tenantMetaStore.Close()
		os.RemoveAll(blobDir)
	}

	// Pre-warm the pool by injecting backend directly is not possible from outside,
	// so we accept that pool.Get will try to connect using the tenant's DSN.
	// In test, the tenant's DB creds won't work (encrypted "unused").
	// For auth-level tests (401/403/503), we don't need the backend to succeed.
	_ = tenantBackend

	return srv, raw, cleanup
}

func mustNow() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

func TestAuthMissingKey(t *testing.T) {
	srv, _, cleanup := newMultiTenantTestServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/fs/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthInvalidKey(t *testing.T) {
	srv, _, cleanup := newMultiTenantTestServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/test.txt", nil)
	req.Header.Set("Authorization", "Bearer invalid-key-here")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}

	// Verify error message doesn't leak tenant existence
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/test.txt", nil)
	req.Header.Set("Authorization", "Bearer invalid-key-here")
	resp, _ = http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var errResp map[string]string
	json.Unmarshal(body, &errResp)
	if errResp["error"] != "invalid API key" {
		t.Errorf("error message should be generic, got %q", errResp["error"])
	}
}

func TestAuthSuspendedTenant(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}

	masterKey := make([]byte, 32)
	rand.Read(masterKey)
	enc, _ := tenant.NewEncryptor(masterKey)
	tenantStore, err := tenant.OpenStore(testDSN, enc)
	if err != nil {
		t.Fatal(err)
	}
	defer tenantStore.Close()
	tenantStore.DB().Exec("DELETE FROM tenants")

	raw, prefix, hash, _ := tenant.GenerateAPIKey()
	pwEnc, _ := tenantStore.EncryptPassword("unused")
	now := mustNow()
	tenantStore.Insert(&tenant.Tenant{
		ID: hash[:16], APIKeyPrefix: prefix, APIKeyHash: hash,
		Status: tenant.StatusSuspended, DBPasswordEnc: pwEnc,
		CreatedAt: now, UpdatedAt: now,
	})

	pool := tenant.NewPool(tenant.PoolConfig{MaxTenants: 10, BlobDir: "/tmp"}, enc)
	defer pool.Close()

	srv := New(Config{Tenants: tenantStore, Pool: pool})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/test.txt", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for suspended tenant, got %d", resp.StatusCode)
	}
}

func TestAuthProvisioningTenant(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}

	masterKey := make([]byte, 32)
	rand.Read(masterKey)
	enc, _ := tenant.NewEncryptor(masterKey)
	tenantStore, err := tenant.OpenStore(testDSN, enc)
	if err != nil {
		t.Fatal(err)
	}
	defer tenantStore.Close()
	tenantStore.DB().Exec("DELETE FROM tenants")

	raw, prefix, hash, _ := tenant.GenerateAPIKey()
	pwEnc, _ := tenantStore.EncryptPassword("unused")
	now := mustNow()
	tenantStore.Insert(&tenant.Tenant{
		ID: hash[:16], APIKeyPrefix: prefix, APIKeyHash: hash,
		Status: tenant.StatusProvisioning, DBPasswordEnc: pwEnc,
		CreatedAt: now, UpdatedAt: now,
	})

	pool := tenant.NewPool(tenant.PoolConfig{MaxTenants: 10, BlobDir: "/tmp"}, enc)
	defer pool.Close()

	srv := New(Config{Tenants: tenantStore, Pool: pool})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/test.txt", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for provisioning tenant, got %d", resp.StatusCode)
	}
}

func TestProvisionEndpointAdminKeyRequired(t *testing.T) {
	srv := New(Config{AdminKey: "secret-admin"})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// No auth
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without admin key, got %d", resp.StatusCode)
	}

	// Wrong key
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong admin key, got %d", resp.StatusCode)
	}
}

func TestLocalDevModeNoAuth(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}

	blobDir, _ := os.MkdirTemp("", "dat9-noauth-*")
	defer os.RemoveAll(blobDir)

	store, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	defer store.Close()

	b, _ := backend.New(store, blobDir)

	// Local dev mode: no tenants configured, backend as fallback
	srv := New(Config{Backend: b})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Should work without any auth
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/noauth.txt", strings.NewReader("hello"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 in local dev mode, got %d", resp.StatusCode)
	}

	// Read back
	resp, err = http.Get(ts.URL + "/v1/fs/noauth.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello" {
		t.Errorf("got %q, want %q", body, "hello")
	}
}
