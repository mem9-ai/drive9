package server

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

func newAuthServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	if testDSN == "" {
		t.Skip("no test database available")
	}

	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host, port := "127.0.0.1", 3306
	if parsed.Addr != "" {
		h, p, _ := strings.Cut(parsed.Addr, ":")
		if h != "" {
			host = h
		}
		if p != "" {
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}
	now := time.Now().UTC()
	tenantID := tenant.NewID()
	tenantDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", parsed.User, parsed.Passwd, host, port, parsed.DBName)
	initServerTenantSchema(t, tenantDSN)
	passCipher, err := pool.Encrypt([]byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := tenant.IssueToken(tokenSecret, tenantID, 1)
	if err != nil {
		t.Fatal(err)
	}
	tokCipher, err := pool.Encrypt([]byte(tok))
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertTenant(&meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertAPIKey(&meta.APIKey{
		ID:            tenant.NewID(),
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: tokCipher,
		JWTHash:       tenant.HashToken(tok),
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}

	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, TokenSecret: tokenSecret})
	cleanup := func() {
		pool.Close()
		_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
		_, _ = metaStore.DB().Exec("DELETE FROM tenants")
		_ = metaStore.Close()
	}
	return srv, tok, cleanup
}

func mustTempDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "dat9-auth-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

func TestAuthRequiresAPIKey(t *testing.T) {
	srv, _, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/fs/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestAuthValidKeyCanWrite(t *testing.T) {
	srv, tok, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/tenant-scope.txt", strings.NewReader("hello"))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestProvisionWithoutProvisionerReturnsNotFound(t *testing.T) {
	srv, _, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderDB9})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestTenantStatusWithValidKey(t *testing.T) {
	srv, tok, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/tenant/status", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out["status"] != string(meta.TenantActive) {
		t.Fatalf("unexpected tenant status response: %+v", out)
	}
}

func TestTenantStatusReturnsProvisioningState(t *testing.T) {
	srv, tok, cleanup := newAuthServer(t)
	defer cleanup()
	if _, err := srv.meta.DB().Exec("UPDATE tenants SET status = ?", string(meta.TenantProvisioning)); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/tenant/status", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("expected provisioning status, got %+v", out)
	}
}
