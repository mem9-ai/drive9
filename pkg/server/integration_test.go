package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

// mockProvisioner returns the test DSN as the cluster, simulating provision.
type mockProvisioner struct {
	dsn string
}

func (m *mockProvisioner) Provision(_ context.Context) (*tenant.ClusterInfo, error) {
	// Parse the test DSN to extract host/port/user/password/dbname
	// For testing, just return localhost defaults — InitSchema does the real work.
	return &tenant.ClusterInfo{
		ClusterID: "test-cluster",
		Host:      "127.0.0.1",
		Port:      3306,
		Username:  "root",
		Password:  "",
		DBName:    "test",
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

	srv := New(Config{
		Tenants:     tenantStore,
		Pool:        pool,
		Provisioner: &mockProvisioner{dsn: testDSN},
		AdminKey:    "test-admin-key",
	})
	ts := httptest.NewServer(srv)

	cleanup := func() {
		ts.Close()
		pool.Close()
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
