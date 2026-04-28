package server

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
	"github.com/mem9-ai/dat9/pkg/vault"
)

// newVaultReadTestServer creates a single-tenant server with vault enabled.
// It returns the server, httptest server, vault store, and vault master key.
func newVaultReadTestServer(t *testing.T) (*httptest.Server, *vault.Store, *vault.MasterKey) {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-vault-read-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initServerTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	// Init vault tables on the same DB.
	if err := schema.ExecSchemaStatements(store.DB(), schema.VaultTiDBSchemaStatements()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	s3c, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		t.Fatal(err)
	}

	vaultKey := make([]byte, 32)
	if _, err := rand.Read(vaultKey); err != nil {
		t.Fatal(err)
	}
	mk, err := vault.NewMasterKey(vaultKey)
	if err != nil {
		t.Fatal(err)
	}

	srv := NewWithConfig(Config{
		Backend:        b,
		VaultMasterKey: vaultKey,
		VaultIssuerURL: "https://test.invalid",
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	vs := vault.NewStore(store.DB(), mk)
	return ts, vs, mk
}

func vaultReadReq(t *testing.T, ts *httptest.Server, path, token string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s: %v", path, err)
	}
	return resp
}

// TestVaultReadGrantTokenSuccess proves the full /v1/vault/read/* path works
// with a vt_ grant token: issue grant → store secret → read via HTTP.
func TestVaultReadGrantTokenSuccess(t *testing.T) {
	ts, vs, _ := newVaultReadTestServer(t)
	tenantID := "local" // single-tenant fallback uses "local"

	// Store a secret.
	_, err := vs.CreateSecret(t.Context(), tenantID, "db-prod", "test", vault.SecretTypeGeneric, map[string][]byte{
		"password": []byte("s3cret"),
		"host":     []byte("db.example.com"),
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	// Issue a read grant.
	tok, _, err := vs.IssueGrant(
		t.Context(), tenantID, "https://test.invalid",
		vault.PrincipalDelegated, "agent-1", []string{"db-prod"},
		vault.GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	// Read whole secret.
	resp := vaultReadReq(t, ts, "/v1/vault/read/db-prod", tok)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	var secretResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&secretResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	fields, ok := secretResp["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %T: %v", secretResp["fields"], secretResp)
	}
	if fields["password"] != "s3cret" {
		t.Fatalf("password: got %v", fields["password"])
	}

	// Read single field.
	resp2 := vaultReadReq(t, ts, "/v1/vault/read/db-prod/host", tok)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("field read: expected 200, got %d: %s", resp2.StatusCode, body)
	}
	fieldBody, _ := io.ReadAll(resp2.Body)
	if string(fieldBody) != "db.example.com" {
		t.Fatalf("field value: got %q", fieldBody)
	}
}

// TestVaultReadGrantTokenScopeDenied proves that a grant token cannot read
// secrets outside its scope.
func TestVaultReadGrantTokenScopeDenied(t *testing.T) {
	ts, vs, _ := newVaultReadTestServer(t)
	tenantID := "local"

	_, err := vs.CreateSecret(t.Context(), tenantID, "aws-prod", "test", vault.SecretTypeGeneric, map[string][]byte{
		"key": []byte("AKIA..."),
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	// Grant scoped to "db-prod" only.
	tok, _, err := vs.IssueGrant(
		t.Context(), tenantID, "https://test.invalid",
		vault.PrincipalDelegated, "agent-1", []string{"db-prod"},
		vault.GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	// Try reading "aws-prod" — should be denied.
	resp := vaultReadReq(t, ts, "/v1/vault/read/aws-prod", tok)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403, got %d: %s", resp.StatusCode, body)
	}
}

// TestVaultReadGrantTokenRevoked proves that a revoked grant token is rejected
// on the read path.
func TestVaultReadGrantTokenRevoked(t *testing.T) {
	ts, vs, _ := newVaultReadTestServer(t)
	tenantID := "local"

	_, err := vs.CreateSecret(t.Context(), tenantID, "db-prod", "test", vault.SecretTypeGeneric, map[string][]byte{
		"password": []byte("s3cret"),
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	tok, grant, err := vs.IssueGrant(
		t.Context(), tenantID, "https://test.invalid",
		vault.PrincipalDelegated, "agent-1", []string{"db-prod"},
		vault.GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	// Revoke the grant.
	if err := vs.RevokeGrant(t.Context(), tenantID, grant.GrantID, "admin", "rotated"); err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}

	resp := vaultReadReq(t, ts, "/v1/vault/read/db-prod", tok)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 401, got %d: %s", resp.StatusCode, body)
	}
}

// TestVaultReadGrantTokenPermNotRead proves that a write-perm grant token is
// rejected by the server read path (verifyGrantReadToken enforces perm==read).
func TestVaultReadGrantTokenPermNotRead(t *testing.T) {
	ts, vs, _ := newVaultReadTestServer(t)
	tenantID := "local"

	_, err := vs.CreateSecret(t.Context(), tenantID, "db-prod", "test", vault.SecretTypeGeneric, map[string][]byte{
		"password": []byte("s3cret"),
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	// Issue a WRITE grant — should be rejected on the read path.
	tok, _, err := vs.IssueGrant(
		t.Context(), tenantID, "https://test.invalid",
		vault.PrincipalDelegated, "agent-1", []string{"db-prod"},
		vault.GrantPermWrite, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	resp := vaultReadReq(t, ts, "/v1/vault/read/db-prod", tok)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 401 for write-perm grant on read path, got %d: %s", resp.StatusCode, body)
	}
}

// TestVaultReadGrantTokenTamperedTenantID proves that a vt_ grant token with a
// tampered tenant_id (peeked for routing) fails at HMAC verification because
// the server derives a different CSK for the wrong tenant.
//
// In single-tenant fallback mode, tenant_id is always "local" from the
// injected scope, so a token issued for a different tenant_id will fail because
// VerifyAndResolveGrant derives CSK from "local" but the token was signed with
// CSK derived from a different tenant.
func TestVaultReadGrantTokenTamperedTenantID(t *testing.T) {
	ts, vs, _ := newVaultReadTestServer(t)

	// Issue a grant for tenant "evil-tenant" — this uses a different CSK.
	tok, _, err := vs.IssueGrant(
		t.Context(), "evil-tenant", "https://test.invalid",
		vault.PrincipalDelegated, "agent-1", []string{"db-prod"},
		vault.GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	// Send to the server — scope.TenantID is "local", but the token was signed
	// for "evil-tenant". HMAC verification will fail (different CSK).
	resp := vaultReadReq(t, ts, "/v1/vault/read/db-prod", tok)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 401 for tampered tenant_id, got %d: %s", resp.StatusCode, body)
	}
}

// TestVaultReadFieldContentType proves response-shape parity: single-field
// reads return application/octet-stream (raw bytes), not JSON.
func TestVaultReadFieldContentType(t *testing.T) {
	ts, vs, _ := newVaultReadTestServer(t)
	tenantID := "local"

	_, err := vs.CreateSecret(t.Context(), tenantID, "db-prod", "test", vault.SecretTypeGeneric, map[string][]byte{
		"password": []byte("s3cret"),
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	tok, _, err := vs.IssueGrant(
		t.Context(), tenantID, "https://test.invalid",
		vault.PrincipalDelegated, "agent-1", []string{"db-prod"},
		vault.GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	resp := vaultReadReq(t, ts, "/v1/vault/read/db-prod/password", tok)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/octet-stream" {
		t.Fatalf("Content-Type: got %q, want application/octet-stream", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "s3cret" {
		t.Fatalf("field value: got %q", body)
	}
}
