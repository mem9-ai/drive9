package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

type authTestRuntime struct {
	meta        *meta.Store
	pool        *tenant.Pool
	tokenSecret []byte
	token       string
}

func newAuthRuntime(t *testing.T) (*authTestRuntime, func()) {
	t.Helper()
	if testDSN == "" {
		t.Skip("no test database available")
	}

	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	resetServerTestState(t, testDSN, metaStore.DB())

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

	conn := parseTestTenantConnInfo(t, testDSN)
	now := time.Now().UTC()
	tenantID := tenant.NewID()
	passCipher, err := pool.Encrypt(context.Background(), []byte(conn.Password))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := tenant.IssueToken(tokenSecret, tenantID, 1)
	if err != nil {
		t.Fatal(err)
	}
	tokCipher, err := pool.Encrypt(context.Background(), []byte(tok))
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           conn.Host,
		DBPort:           conn.Port,
		DBUser:           conn.User,
		DBPasswordCipher: passCipher,
		DBName:           conn.DBName,
		DBTLS:            conn.TLS,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertAPIKey(context.Background(), &meta.APIKey{
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
	cleanup := func() {
		pool.Close()
		resetServerTestState(t, testDSN, metaStore.DB())
		_ = metaStore.Close()
	}
	return &authTestRuntime{meta: metaStore, pool: pool, tokenSecret: tokenSecret, token: tok}, cleanup
}

func newAuthServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	rt, cleanup := newAuthRuntime(t)
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	return srv, rt.token, func() {
		srv.waitBackgroundTasks()
		cleanup()
	}
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

func TestAuthKeepsBorrowedBackendValidDuringRequestAfterInvalidate(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	h := tenantAuthMiddleware(rt.meta, rt.pool, rt.tokenSecret, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := ScopeFromContext(r.Context())
		if scope == nil || scope.Backend == nil {
			t.Fatal("expected tenant scope backend")
		}
		rt.pool.Invalidate(scope.TenantID)
		if err := scope.Backend.Store().DB().PingContext(r.Context()); err != nil {
			t.Fatalf("borrowed backend store should remain usable during request: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	ts := httptest.NewServer(h)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/borrow", nil)
	req.Header.Set("Authorization", "Bearer "+rt.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
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

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
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

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
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
