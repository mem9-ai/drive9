package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"github.com/mem9-ai/dat9/pkg/vault"
)

type vaultGrantReadRuntime struct {
	server   *Server
	store    *vault.Store
	tenantID string
	otherID  string
	issuer   string
	cleanup  func()
}

func newVaultGrantReadRuntime(t *testing.T) *vaultGrantReadRuntime {
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
	initServerTenantSchema(t, testDSN)
	passCipher, err := pool.Encrypt(context.Background(), []byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tenantID := token.NewID()
	otherID := token.NewID()
	newTenantMeta := func(id string) *meta.Tenant {
		return &meta.Tenant{
			ID:               id,
			Status:           meta.TenantActive,
			DBHost:           host,
			DBPort:           port,
			DBUser:           parsed.User,
			DBPasswordCipher: passCipher,
			DBName:           parsed.DBName,
			DBTLS:            false,
			Provider:         tenant.ProviderTiDBZero,
			SchemaVersion:    1,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
	}
	tenantMeta := newTenantMeta(tenantID)
	for _, tenantRecord := range []*meta.Tenant{tenantMeta, newTenantMeta(otherID)} {
		if err := metaStore.InsertTenant(context.Background(), tenantRecord); err != nil {
			t.Fatal(err)
		}
	}
	backend, release, err := pool.Acquire(context.Background(), tenantMeta)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if err := vault.InitSchema(backend.Store().DB()); err != nil {
		t.Fatalf("init vault schema: %v", err)
	}
	for _, tbl := range []string{
		"vault_audit_log",
		"vault_grants",
		"vault_tokens",
		"vault_secret_fields",
		"vault_secrets",
		"vault_deks",
		"vault_policies",
	} {
		if _, err := backend.Store().DB().Exec("DELETE FROM " + tbl); err != nil {
			t.Fatalf("clean %s: %v", tbl, err)
		}
	}

	vaultKey := make([]byte, 32)
	if _, err := rand.Read(vaultKey); err != nil {
		t.Fatal(err)
	}
	issuer := "https://vault-test.invalid"
	store := vault.NewStore(backend.Store().DB(), mustVaultMasterKey(t, vaultKey))
	server := NewWithConfig(Config{
		Meta:           metaStore,
		Pool:           pool,
		TokenSecret:    []byte("test-token-secret"),
		VaultMasterKey: vaultKey,
		VaultIssuerURL: issuer,
	})

	return &vaultGrantReadRuntime{
		server:   server,
		store:    store,
		tenantID: tenantID,
		otherID:  otherID,
		issuer:   issuer,
		cleanup: func() {
			pool.Close()
			_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
			_, _ = metaStore.DB().Exec("DELETE FROM tenants")
			_ = metaStore.Close()
		},
	}
}

func mustVaultMasterKey(t *testing.T, raw []byte) *vault.MasterKey {
	t.Helper()
	mk, err := vault.NewMasterKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	return mk
}

func (rt *vaultGrantReadRuntime) issueGrant(t *testing.T, scope []string, perm vault.GrantPerm) (string, *vault.VaultGrant) {
	t.Helper()
	tok, grant, err := rt.store.IssueGrant(
		context.Background(), rt.tenantID, rt.issuer,
		vault.PrincipalDelegated, "agent-test", scope, perm, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}
	return tok, grant
}

func (rt *vaultGrantReadRuntime) createSecret(t *testing.T) {
	t.Helper()
	_, err := rt.store.CreateSecret(context.Background(), rt.tenantID, "test3", "owner", vault.SecretTypeGeneric, map[string][]byte{
		"k1": []byte("value1"),
		"k2": []byte("value2"),
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
}

func doVaultGrantRead(t *testing.T, srv *Server, token, path string) (*http.Response, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp, body
}

func tamperGrantTenantID(t *testing.T, raw, tenantID string) string {
	t.Helper()
	if !strings.HasPrefix(raw, "vt_") {
		t.Fatal("grant token prefix is not vt_")
	}
	body := strings.TrimPrefix(raw, "vt_")
	parts := strings.Split(body, ".")
	if len(parts) != 3 {
		t.Fatalf("grant token parts = %d, want 3", len(parts))
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	payload["tenant_id"] = tenantID
	payloadJSON, err = json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	parts[1] = base64.RawURLEncoding.EncodeToString(payloadJSON)
	return "vt_" + strings.Join(parts, ".")
}

func TestVaultGrantReadTokenSuccessResponseShape(t *testing.T) {
	rt := newVaultGrantReadRuntime(t)
	defer rt.cleanup()
	rt.createSecret(t)
	tok, _ := rt.issueGrant(t, []string{"test3"}, vault.GrantPermRead)

	resp, body := doVaultGrantRead(t, rt.server, tok, "/v1/vault/read/test3?format=json")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("whole secret status=%d body=%s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("whole secret content-type = %q, want application/json", ct)
	}
	var fields map[string]string
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("decode whole secret JSON: %v body=%s", err, body)
	}
	if fields["k1"] != "value1" || fields["k2"] != "value2" {
		t.Fatalf("fields = %#v", fields)
	}

	resp, body = doVaultGrantRead(t, rt.server, tok, "/v1/vault/read/test3/k1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("field status=%d body=%s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("field content-type = %q, want text/plain", ct)
	}
	if string(body) != "value1" {
		t.Fatalf("field body = %q, want value1", body)
	}
}

func TestVaultGrantReadScopeDenied(t *testing.T) {
	rt := newVaultGrantReadRuntime(t)
	defer rt.cleanup()
	rt.createSecret(t)
	tok, _ := rt.issueGrant(t, []string{"test3/k2"}, vault.GrantPermRead)

	resp, body := doVaultGrantRead(t, rt.server, tok, "/v1/vault/read/test3/k1")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d body=%s, want 404", resp.StatusCode, body)
	}
}

func TestVaultGrantReadRevoked(t *testing.T) {
	rt := newVaultGrantReadRuntime(t)
	defer rt.cleanup()
	rt.createSecret(t)
	tok, grant := rt.issueGrant(t, []string{"test3"}, vault.GrantPermRead)
	if err := rt.store.RevokeGrant(context.Background(), rt.tenantID, grant.GrantID, "owner", "test"); err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}

	resp, body := doVaultGrantRead(t, rt.server, tok, "/v1/vault/read/test3/k1")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s, want 401", resp.StatusCode, body)
	}
	if !bytes.Contains(body, []byte("token revoked")) {
		t.Fatalf("body=%s, want token revoked", body)
	}
}

func TestVaultGrantReadTamperedTenantIDFailsNoFallback(t *testing.T) {
	rt := newVaultGrantReadRuntime(t)
	defer rt.cleanup()
	rt.createSecret(t)
	tok, _ := rt.issueGrant(t, []string{"test3"}, vault.GrantPermRead)
	tampered := tamperGrantTenantID(t, tok, rt.otherID)
	peekedTenant, err := vault.PeekGrantTenantID(tampered)
	if err != nil {
		t.Fatalf("PeekGrantTenantID: %v", err)
	}
	if peekedTenant != rt.otherID {
		t.Fatalf("peeked tenant = %q, want forged route %q", peekedTenant, rt.otherID)
	}
	if _, err := rt.store.VerifyAndResolveGrant(context.Background(), rt.otherID, rt.issuer, tampered); err == nil {
		t.Fatal("expected tampered tenant_id to fail HMAC verification under forged tenant")
	}

	resp, body := doVaultGrantRead(t, rt.server, tampered, "/v1/vault/read/test3/k1")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s, want 401", resp.StatusCode, body)
	}
	if bytes.Contains(body, []byte("value1")) {
		t.Fatalf("tampered tenant_id unexpectedly read secret: %s", body)
	}
}

func TestVaultGrantReadRejectsWritePermAfterVerify(t *testing.T) {
	rt := newVaultGrantReadRuntime(t)
	defer rt.cleanup()
	rt.createSecret(t)
	tok, _ := rt.issueGrant(t, []string{"test3"}, vault.GrantPermWrite)
	claims, err := rt.store.VerifyAndResolveGrant(context.Background(), rt.tenantID, rt.issuer, tok)
	if err != nil {
		t.Fatalf("VerifyAndResolveGrant: %v", err)
	}
	if claims.Perm != vault.GrantPermWrite {
		t.Fatalf("verified perm = %q, want write", claims.Perm)
	}

	resp, body := doVaultGrantRead(t, rt.server, tok, "/v1/vault/read/test3/k1")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s, want 401", resp.StatusCode, body)
	}
}
