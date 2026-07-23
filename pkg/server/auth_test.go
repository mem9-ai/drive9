package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type authTestRuntime struct {
	meta        *meta.Store
	pool        *tenant.Pool
	tokenSecret []byte
	token       string
	tenantID    string
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
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_key_fs_scopes")
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
	tenantID := token.NewID()
	tenantDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", parsed.User, parsed.Passwd, host, port, parsed.DBName)
	initServerTenantSchema(t, tenantDSN)
	passCipher, err := pool.Encrypt(context.Background(), []byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := token.IssueToken(tokenSecret, tenantID, 1)
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
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertAPIKey(context.Background(), &meta.APIKey{
		ID:            token.NewID(),
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: tokCipher,
		JWTHash:       token.HashToken(tok),
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
		_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_key_fs_scopes")
		_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
		_, _ = metaStore.DB().Exec("DELETE FROM tenants")
		_ = metaStore.Close()
	}
	return &authTestRuntime{meta: metaStore, pool: pool, tokenSecret: tokenSecret, token: tok, tenantID: tenantID}, cleanup
}

func newAuthServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	rt, cleanup := newAuthRuntime(t)
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	return srv, rt.token, cleanup
}

func replaceAuthRuntimeToken(t *testing.T, rt *authTestRuntime, tok string) {
	t.Helper()
	ctx := context.Background()
	tokCipher, err := rt.pool.Encrypt(ctx, []byte(tok))
	if err != nil {
		t.Fatal(err)
	}
	res, err := rt.meta.DB().ExecContext(ctx, `UPDATE tenant_api_keys
		SET jwt_ciphertext = ?, jwt_hash = ?, updated_at = ?
		WHERE tenant_id = ?`, tokCipher, token.HashToken(tok), time.Now().UTC(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("updated %d api keys, want 1", n)
	}
}

func setAuthRuntimeScopeKind(t *testing.T, rt *authTestRuntime, kind meta.APIKeyScopeKind) meta.APIKey {
	t.Helper()
	ctx := context.Background()
	res, err := rt.meta.DB().ExecContext(ctx, `UPDATE tenant_api_keys
		SET scope_kind = ?, updated_at = ?
		WHERE tenant_id = ?`, kind, time.Now().UTC(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("updated %d api keys, want 1", n)
	}
	resolved, err := rt.meta.ResolveByAPIKeyHash(ctx, token.HashToken(rt.token))
	if err != nil {
		t.Fatal(err)
	}
	return resolved.APIKey
}

type recordingFSScopeLoader struct {
	calls    int
	tenantID string
	apiKeyID string
	rows     []meta.APIKeyFSScope
	err      error
}

func (l *recordingFSScopeLoader) ListAPIKeyFSScopes(ctx context.Context, tenantID, apiKeyID string) ([]meta.APIKeyFSScope, error) {
	l.calls++
	l.tenantID = tenantID
	l.apiKeyID = apiKeyID
	return l.rows, l.err
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

func TestAuthLegacyTokenKeepsOwnerJournalPermissions(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()

	h := tenantAuthMiddleware(rt.meta, rt.pool, rt.tokenSecret, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := ScopeFromContext(r.Context())
		for _, permission := range []string{
			JournalPermissionCreate,
			JournalPermissionAppend,
			JournalPermissionRead,
			JournalPermissionFind,
			JournalPermissionVerify,
			JournalPermissionSourceGateway,
			JournalPermissionSourceImport,
		} {
			if !scope.HasJournalPermission(permission) {
				http.Error(w, "missing "+permission, http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/journal-permissions", nil)
	req.Header.Set("Authorization", "Bearer "+rt.token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestAuthOwnerKeyDoesNotLoadFSScopes(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	loader := &recordingFSScopeLoader{}

	h := tenantAuthMiddlewareWithFSScopeLoader(rt.meta, rt.pool, rt.tokenSecret, loader, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := ScopeFromContext(r.Context())
		if scope == nil {
			http.Error(w, "missing scope", http.StatusInternalServerError)
			return
		}
		if scope.IsScoped {
			http.Error(w, "owner key should not be scoped", http.StatusInternalServerError)
			return
		}
		if len(scope.FSScopes) != 0 {
			http.Error(w, "owner key should not carry fs scopes", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/owner", nil)
	req.Header.Set("Authorization", "Bearer "+rt.token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if loader.calls != 0 {
		t.Fatalf("FS scope loader calls = %d, want 0", loader.calls)
	}
}

func TestAuthScopedKeyLoadsFSScopes(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	key := setAuthRuntimeScopeKind(t, rt, meta.APIKeyScopeKindFS)
	loader := &recordingFSScopeLoader{rows: []meta.APIKeyFSScope{{
		TenantID: rt.tenantID,
		APIKeyID: key.ID,
		Prefix:   "/scratch/run-1/",
		Ops:      "read,list",
	}}}

	h := tenantAuthMiddlewareWithFSScopeLoader(rt.meta, rt.pool, rt.tokenSecret, loader, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := ScopeFromContext(r.Context())
		if scope == nil {
			http.Error(w, "missing scope", http.StatusInternalServerError)
			return
		}
		if !scope.IsScoped {
			http.Error(w, "scoped key was not marked scoped", http.StatusInternalServerError)
			return
		}
		if len(scope.FSScopes) != 1 || scope.FSScopes[0].Prefix != "/scratch/run-1" || !scope.FSScopes[0].Ops[FSOpRead] || !scope.FSScopes[0].Ops[FSOpList] {
			http.Error(w, fmt.Sprintf("unexpected fs scopes: %#v", scope.FSScopes), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/scoped", nil)
	req.Header.Set("Authorization", "Bearer "+rt.token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if loader.calls != 1 {
		t.Fatalf("FS scope loader calls = %d, want 1", loader.calls)
	}
	if loader.tenantID != rt.tenantID || loader.apiKeyID != key.ID {
		t.Fatalf("loader args tenant=%q key=%q, want tenant=%q key=%q", loader.tenantID, loader.apiKeyID, rt.tenantID, key.ID)
	}
}

// TestScopedBusinessEndpointGuardDeniesEndpointsOutOfWorkspaceZonesScope
// checks that scoped tokens are still 403'd at the dispatcher for every
// endpoint NOT opened by PR C1 (read-side) / C2a (write-side) / C2b
// (uploads). SQL/fork/events/journals/vault are permanently out of scope.
// chmod stays owner-only forever. A bare POST /v1/fs/* without an action
// selector also denies (ambiguous request).
func TestScopedBusinessEndpointGuardDeniesEndpointsOutOfWorkspaceZonesScope(t *testing.T) {
	type endpoint struct {
		method string
		path   string
	}
	deniedC1 := []endpoint{
		{http.MethodPost, "/v1/sql"},
		{http.MethodPost, "/v1/fs/file.txt"}, // no action selector → ambiguous → deny
		{http.MethodPost, "/v1/fork"},
		{http.MethodPost, "/v1/tokens"},
		{http.MethodGet, "/v1/events"},
		{http.MethodGet, "/v1/journals"},
		{http.MethodGet, "/v1/vault/secrets"},
	}
	for _, ep := range deniedC1 {
		req := httptest.NewRequest(ep.method, ep.path, nil)
		req = req.WithContext(withScope(req.Context(), &TenantScope{IsScoped: true}))
		rr := httptest.NewRecorder()

		(&Server{}).handleBusiness(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Errorf("%s %s status=%d body=%s, want 403 (scoped token must not enter handler)",
				ep.method, ep.path, rr.Code, rr.Body.String())
		}
	}
}

func TestAuthUsesJournalPermissionsFromTokenClaims(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()

	scoped, err := token.IssueTokenWithJournalPermissions(rt.tokenSecret, rt.tenantID, 1, time.Time{}, []string{
		JournalPermissionAppend,
		JournalPermissionFind,
	})
	if err != nil {
		t.Fatal(err)
	}
	replaceAuthRuntimeToken(t, rt, scoped)

	h := tenantAuthMiddleware(rt.meta, rt.pool, rt.tokenSecret, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := ScopeFromContext(r.Context())
		if !scope.HasJournalPermission(JournalPermissionAppend) || !scope.HasJournalPermission(JournalPermissionFind) {
			http.Error(w, "missing scoped permission", http.StatusInternalServerError)
			return
		}
		for _, denied := range []string{JournalPermissionRead, JournalPermissionCreate, JournalPermissionSourceGateway, JournalPermissionAdmin} {
			if scope.HasJournalPermission(denied) {
				http.Error(w, "unexpected "+denied, http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/journal-permissions", nil)
	req.Header.Set("Authorization", "Bearer "+scoped)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestJournalAdminPermissionIsWildcard(t *testing.T) {
	scope := &TenantScope{JournalPermissions: map[string]bool{JournalPermissionAdmin: true}}
	for _, permission := range []string{JournalPermissionRead, JournalPermissionAppend, JournalPermissionSourceGateway} {
		if !scope.HasJournalPermission(permission) {
			t.Fatalf("admin scope missing %s", permission)
		}
	}
}

func TestAuthClientCanceledDoesNotLogBackendUnavailable(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()

	core, recorded := observer.New(zap.InfoLevel)
	restoreLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(restoreLogger) })

	calledNext := false
	h := tenantAuthMiddleware(rt.meta, rt.pool, rt.tokenSecret, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calledNext = true
		w.WriteHeader(http.StatusNoContent)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/fs/canceled.txt", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+rt.token)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if calledNext {
		t.Fatal("next handler should not be called after canceled auth")
	}
	if rec.Code != statusClientClosedRequest {
		t.Fatalf("status=%d, want %d", rec.Code, statusClientClosedRequest)
	}
	if entries := recorded.FilterField(zap.String("event", "auth_backend_unavailable")).AllUntimed(); len(entries) != 0 {
		t.Fatalf("auth_backend_unavailable logs = %d, want 0", len(entries))
	}
	if entries := recorded.FilterField(zap.String("event", "auth_client_canceled")).AllUntimed(); len(entries) != 1 {
		t.Fatalf("auth_client_canceled logs = %d, want 1", len(entries))
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
	var out TenantStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != string(meta.TenantActive) {
		t.Fatalf("unexpected tenant status response: %+v", out)
	}
	if out.MaxUploadBytes != srv.maxUploadBytes {
		t.Fatalf("max_upload_bytes = %d, want %d", out.MaxUploadBytes, srv.maxUploadBytes)
	}
}

func TestTenantStatusReturnsInlineThreshold(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	const customThreshold = int64(256_000)
	srv := NewWithConfig(Config{
		Meta:            rt.meta,
		Pool:            rt.pool,
		TokenSecret:     rt.tokenSecret,
		InlineThreshold: customThreshold,
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	req.Header.Set("Authorization", "Bearer "+rt.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out TenantStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.InlineThreshold != customThreshold {
		t.Fatalf("inline_threshold = %d, want %d", out.InlineThreshold, customThreshold)
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
	var out TenantStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != string(meta.TenantProvisioning) {
		t.Fatalf("expected provisioning status, got %+v", out)
	}
}

func TestTenantStatusForkProvisioningWithoutReadyBranchOmitsMessage(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	if _, err := srv.meta.DB().Exec("UPDATE tenants SET status = ?, kind = ?, parent_tenant_id = ?, branch_id = ? WHERE id = ?",
		string(meta.TenantProvisioning), string(meta.TenantKindFork), "source", "", rt.tenantID); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	req.Header.Set("Authorization", "Bearer "+rt.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out TenantStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != string(meta.TenantProvisioning) {
		t.Fatalf("status = %q, want provisioning", out.Status)
	}
	if out.Message != "" {
		t.Fatalf("message = %q, want empty", out.Message)
	}
}

func TestTenantStatusForkProvisioningBranchShowsMigrationMessage(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	if _, err := srv.meta.DB().Exec(`UPDATE tenants
		SET status = ?, kind = ?, parent_tenant_id = ?, branch_id = ?, db_host = ?, db_port = ?, db_user = ?
		WHERE id = ?`,
		string(meta.TenantProvisioning), string(meta.TenantKindFork), "source", "branch-a", "", 0, "", rt.tenantID); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	req.Header.Set("Authorization", "Bearer "+rt.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out TenantStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != string(meta.TenantProvisioning) {
		t.Fatalf("status = %q, want provisioning", out.Status)
	}
	if !strings.Contains(out.Message, "Migrating fork data") {
		t.Fatalf("message = %q", out.Message)
	}
}

func TestSanitizeClientError_NilError(t *testing.T) {
	if got := sanitizeClientError(nil); got != "backend unavailable" {
		t.Fatalf("nil error: got %q, want %q", got, "backend unavailable")
	}
}

func TestSanitizeClientError_QuotaExhausted(t *testing.T) {
	err := fmt.Errorf("open db: Error 1105 (HY000): Due to the usage quota being exhausted")
	got := sanitizeClientError(err)
	if got != "tenant usage quota exceeded" {
		t.Fatalf("got %q, want %q", got, "tenant usage quota exceeded")
	}
}

func TestSanitizeClientError_QuotaExhaustedLowerCase(t *testing.T) {
	err := fmt.Errorf("open db: error 1105 (hy000): due to the usage quota being exhausted")
	got := sanitizeClientError(err)
	if got != "tenant usage quota exceeded" {
		t.Fatalf("case-insensitive: got %q, want %q", got, "tenant usage quota exceeded")
	}
}

func TestSanitizeClientError_QuotaExhaustedWrapped(t *testing.T) {
	err := fmt.Errorf("open datastore: open db: Error 1105 (HY000): Due to the usage quota being exhausted, access to the cluster has been restricted. Try increasing spending limits to gain full access.")
	got := sanitizeClientError(err)
	if got != "tenant usage quota exceeded" {
		t.Fatalf("wrapped: got %q, want %q", got, "tenant usage quota exceeded")
	}
}

func TestSanitizeClientError_MetadataError(t *testing.T) {
	tests := []string{
		"ensure tidb auto-embedding schema: Error 1060: Duplicate column name 'foo'",
		"validate tidb auto-embedding schema: column mismatch",
		"migrate split tables: Error 1146: Table doesn't exist",
		"detect legacy files table: Error 1045: Access denied",
	}
	for _, tc := range tests {
		err := fmt.Errorf("%s", tc)
		got := sanitizeClientError(err)
		if got != "tenant metadata error" {
			t.Fatalf("input=%q: got %q, want %q", tc, got, "tenant metadata error")
		}
	}
}

func TestSanitizeClientError_Fallback(t *testing.T) {
	tests := []string{
		"dial tcp: connection refused",
		"open db: Error 1045: Access denied for user",
		"some random error",
	}
	for _, tc := range tests {
		err := fmt.Errorf("%s", tc)
		got := sanitizeClientError(err)
		if got != "backend unavailable" {
			t.Fatalf("input=%q: got %q, want %q", tc, got, "backend unavailable")
		}
	}
	// Non-quota 1105 wrapped with "quota" in prefix should not be classified as quota error.
	err := fmt.Errorf("set quota config for tenant: Error 1105 (HY000): connection refused")
	if got := sanitizeClientError(err); got != "backend unavailable" {
		t.Fatalf("non-quota 1105 with quota prefix: got %q, want %q", got, "backend unavailable")
	}
}

func TestBackendErrorStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "client canceled context",
			err:  context.Canceled,
			want: statusClientClosedRequest,
		},
		{
			name: "deadline exceeded",
			err:  context.DeadlineExceeded,
			want: http.StatusGatewayTimeout,
		},
		{
			name: "tidb quota error 1105",
			err:  fmt.Errorf("open db: Error 1105 (HY000): Due to the usage quota being exhausted"),
			want: http.StatusPaymentRequired,
		},
		{
			name: "tidb quota error lower case",
			err:  fmt.Errorf("error 1105 (hy000): quota limit reached"),
			want: http.StatusPaymentRequired,
		},
		{
			name: "non-quota 1105 wrapped with quota prefix",
			err:  fmt.Errorf("set quota config for tenant: Error 1105 (HY000): connection refused"),
			want: http.StatusInternalServerError,
		},
		{
			name: "schema migration error",
			err:  fmt.Errorf("migrate split tables: Error 1146: Table doesn't exist"),
			want: http.StatusBadGateway,
		},
		{
			name: "unknown error",
			err:  fmt.Errorf("some random error"),
			want: http.StatusInternalServerError,
		},
		{
			name: "nil error",
			err:  nil,
			want: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			switch {
			case errors.Is(tt.err, context.Canceled):
				c, cancel := context.WithCancel(context.Background())
				cancel()
				ctx = c
			case errors.Is(tt.err, context.DeadlineExceeded):
				c, cancel := context.WithTimeout(context.Background(), 0)
				cancel()
				ctx = c
			}
			if got := backendErrorStatus(ctx, tt.err); got != tt.want {
				t.Errorf("backendErrorStatus() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAuthForkDeleteSkipsPoolAcquire(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()

	h := tenantAuthMiddleware(rt.meta, rt.pool, rt.tokenSecret, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := ScopeFromContext(r.Context())
		if scope == nil {
			t.Fatal("expected tenant scope")
		}
		if scope.Backend != nil {
			t.Fatal("fork delete should not acquire backend")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	ts := httptest.NewServer(h)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/fork", strings.NewReader(`{}`))
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
