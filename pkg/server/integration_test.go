package server

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// mockProvisioner creates a new database per tenant to simulate real isolation.
type mockProvisioner struct {
	dsn    string
	dbSeq  int
	parsed *mysql.Config
	dbs    []string // track created DBs for cleanup
}

func newMockProvisioner(dsn string) *mockProvisioner {
	cfg, _ := mysql.ParseDSN(dsn)
	return &mockProvisioner{dsn: dsn, parsed: cfg, dbs: make([]string, 0)}
}

func (m *mockProvisioner) Provision(_ context.Context) (*tenant.ClusterInfo, error) {
	m.dbSeq++
	dbName := fmt.Sprintf("dat9_tenant_%d", m.dbSeq)

	// Create a new database for this tenant
	adminCfg := m.parsed.Clone()
	adminCfg.DBName = ""
	adminDB, err := openDB(adminCfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open admin db: %w", err)
	}
	defer adminDB.Close()
	if _, err := adminDB.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
		return nil, fmt.Errorf("create tenant db: %w", err)
	}
	m.dbs = append(m.dbs, dbName)

	host, port := "127.0.0.1", 3306
	if m.parsed.Addr != "" {
		h, p, _ := strings.Cut(m.parsed.Addr, ":")
		if h != "" {
			host = h
		}
		if p != "" {
			fmt.Sscanf(p, "%d", &port)
		}
	}
	return &tenant.ClusterInfo{
		ClusterID: fmt.Sprintf("test-cluster-%d", m.dbSeq),
		Host:      host,
		Port:      port,
		Username:  m.parsed.User,
		Password:  m.parsed.Passwd,
		DBName:    dbName,
	}, nil
}

func (m *mockProvisioner) InitSchema(_ context.Context, dsn string) error {
	store, err := meta.Open(dsn)
	if err != nil {
		return err
	}
	store.Close()
	return nil
}

func (m *mockProvisioner) ProviderType() string { return "mock" }

func (m *mockProvisioner) cleanup() {
	adminCfg := m.parsed.Clone()
	adminCfg.DBName = ""
	adminDB, err := openDB(adminCfg.FormatDSN())
	if err != nil {
		return
	}
	defer adminDB.Close()
	for _, db := range m.dbs {
		adminDB.Exec("DROP DATABASE IF EXISTS " + db)
	}
}

// newIntegrationServer creates a full multi-tenant server backed by the test database.
// Provision endpoint uses a mock provisioner that targets testDSN.
func newIntegrationServer(t *testing.T) (*httptest.Server, *tenant.Store, func()) {
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

	tenantStore, err := tenant.OpenStore(testDSN, enc)
	if err != nil {
		t.Fatal(err)
	}
	tenantStore.DB().Exec("DELETE FROM tenants")

	blobDir, _ := os.MkdirTemp("", "dat9-integ-blobs-*")

	pool := tenant.NewPool(tenant.PoolConfig{
		MaxTenants: 10,
		BlobDir:    blobDir,
	}, enc)

	prov := newMockProvisioner(testDSN)
	srv := New(Config{
		Tenants:     tenantStore,
		Pool:        pool,
		Provisioner: prov,
		AdminKey:    "test-admin-key",
	})
	ts := httptest.NewServer(srv)

	cleanup := func() {
		ts.Close()
		pool.Close()
		prov.cleanup()
		tenantStore.DB().Exec("DELETE FROM tenants")
		tenantStore.Close()
		os.RemoveAll(blobDir)
	}

	return ts, tenantStore, cleanup
}

// provisionTenant calls POST /v1/provision and returns the raw API key.
func provisionTenant(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("provision: expected 201, got %d: %s", resp.StatusCode, body)
	}
	var result struct {
		APIKey   string `json:"api_key"`
		TenantID string `json:"tenant_id"`
		Status   string `json:"status"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.APIKey == "" {
		t.Fatal("provision returned empty API key")
	}
	if result.Status != "active" {
		t.Fatalf("expected status active, got %q", result.Status)
	}
	return result.APIKey
}

func authedRequest(method, url, apiKey string, body io.Reader) *http.Request {
	req, _ := http.NewRequest(method, url, body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	return req
}

func TestProvisionAndAuth(t *testing.T) {
	ts, _, cleanup := newIntegrationServer(t)
	defer cleanup()

	// Provision a tenant
	apiKey := provisionTenant(t, ts)

	// Use the key to write a file
	req := authedRequest(http.MethodPut, ts.URL+"/v1/fs/hello.txt", apiKey, strings.NewReader("world"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write: expected 200, got %d", resp.StatusCode)
	}

	// Read it back
	req = authedRequest(http.MethodGet, ts.URL+"/v1/fs/hello.txt", apiKey, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read: expected 200, got %d", resp.StatusCode)
	}
	if string(body) != "world" {
		t.Errorf("read: got %q, want %q", body, "world")
	}
}

func TestProvisionAdminKeyRequired(t *testing.T) {
	ts, _, cleanup := newIntegrationServer(t)
	defer cleanup()

	// No key
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}

	// Wrong key
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	req.Header.Set("Authorization", "Bearer wrong-admin-key")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}

	// Wrong method
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/provision", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestRevokedKeyRejected(t *testing.T) {
	ts, tenantStore, cleanup := newIntegrationServer(t)
	defer cleanup()

	apiKey := provisionTenant(t, ts)

	// Verify it works first
	req := authedRequest(http.MethodPut, ts.URL+"/v1/fs/test.txt", apiKey, strings.NewReader("data"))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write before revoke: expected 200, got %d", resp.StatusCode)
	}

	// Revoke: find tenant ID from hash and suspend
	hash := tenant.HashAPIKey(apiKey)
	tnt, err := tenantStore.GetByAPIKeyHash(hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := tenantStore.UpdateStatus(tnt.ID, tenant.StatusSuspended); err != nil {
		t.Fatal(err)
	}

	// Now the key should be rejected with 403
	req = authedRequest(http.MethodGet, ts.URL+"/v1/fs/test.txt", apiKey, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 after revoke, got %d", resp.StatusCode)
	}
}

func TestTenantIsolation(t *testing.T) {
	ts, _, cleanup := newIntegrationServer(t)
	defer cleanup()

	// Provision two tenants
	key1 := provisionTenant(t, ts)
	key2 := provisionTenant(t, ts)

	// Tenant 1 writes a file
	req := authedRequest(http.MethodPut, ts.URL+"/v1/fs/secret.txt", key1, strings.NewReader("tenant1-data"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tenant1 write: expected 200, got %d", resp.StatusCode)
	}

	// Tenant 2 writes a different file
	req = authedRequest(http.MethodPut, ts.URL+"/v1/fs/secret.txt", key2, strings.NewReader("tenant2-data"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tenant2 write: expected 200, got %d", resp.StatusCode)
	}

	// Tenant 1 reads back their file — should see their own data
	req = authedRequest(http.MethodGet, ts.URL+"/v1/fs/secret.txt", key1, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tenant1 read: expected 200, got %d", resp.StatusCode)
	}
	if string(body) != "tenant1-data" {
		t.Errorf("tenant1 read: got %q, want %q", body, "tenant1-data")
	}

	// Tenant 2 reads back their file — should see their own data
	req = authedRequest(http.MethodGet, ts.URL+"/v1/fs/secret.txt", key2, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tenant2 read: expected 200, got %d", resp.StatusCode)
	}
	if string(body) != "tenant2-data" {
		t.Errorf("tenant2 read: got %q, want %q", body, "tenant2-data")
	}
}

func TestWrongKeyRejected(t *testing.T) {
	ts, _, cleanup := newIntegrationServer(t)
	defer cleanup()

	// Provision a tenant
	_ = provisionTenant(t, ts)

	// Try with a completely fabricated key
	req := authedRequest(http.MethodGet, ts.URL+"/v1/fs/test.txt", "dat9_fakefakefakefake", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong key, got %d", resp.StatusCode)
	}
}
