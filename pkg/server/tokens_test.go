package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

func TestScopedTokenIssueCreatesFSScopeKey(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	body := `{"ttl_seconds":3600,"scopes":[{"prefix":":/scratch/run-123/","ops":["write","read","list"]}]}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+rt.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out scopedTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Token == "" || out.TokenID == "" || out.Subject != "" || out.ScopeKind != string(meta.APIKeyScopeKindFS) || out.ExpiresAt == nil {
		t.Fatalf("unexpected response: %+v", out)
	}
	if len(out.Scopes) != 1 || out.Scopes[0].Prefix != "/scratch/run-123" || strings.Join(out.Scopes[0].Ops, ",") != "read,list,write" {
		t.Fatalf("unexpected response scopes: %+v", out.Scopes)
	}

	resolved, err := rt.meta.ResolveByAPIKeyHash(context.Background(), token.HashToken(out.Token))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.APIKey.ID != out.TokenID || resolved.APIKey.ScopeKind != meta.APIKeyScopeKindFS || resolved.APIKey.KeyName != "" {
		t.Fatalf("resolved key = %+v", resolved.APIKey)
	}
	rows, err := rt.meta.ListAPIKeyFSScopes(context.Background(), rt.tenantID, out.TokenID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Prefix != "/scratch/run-123" || rows[0].Ops != "read,list,write" {
		t.Fatalf("scope rows = %+v", rows)
	}

	writeReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/scratch/run-123/out.txt", strings.NewReader("ok"))
	writeReq.Header.Set("Authorization", "Bearer "+out.Token)
	writeResp, err := http.DefaultClient.Do(writeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = writeResp.Body.Close()
	if writeResp.StatusCode != http.StatusOK {
		t.Fatalf("in-scope write status=%d, want 200", writeResp.StatusCode)
	}

	denyReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/main/out.txt", strings.NewReader("no"))
	denyReq.Header.Set("Authorization", "Bearer "+out.Token)
	denyResp, err := http.DefaultClient.Do(denyReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = denyResp.Body.Close()
	if denyResp.StatusCode != http.StatusForbidden {
		t.Fatalf("out-of-scope write status=%d, want 403", denyResp.StatusCode)
	}
}

func TestScopedTokenIssueSameTTLDoesNotHashCollide(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	for _, subject := range []string{"vm0-a", "vm0-b"} {
		body := `{"subject":"` + subject + `","ttl_seconds":3600,"scopes":[{"prefix":"/scratch","ops":["read"]}]}`
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+rt.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("subject %s status=%d, want 201", subject, resp.StatusCode)
		}
	}
}

func TestScopedTokenIssueAllowsRepeatedSubject(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	for i := 0; i < 2; i++ {
		body := `{"subject":"same-audit-label","ttl_seconds":3600,"scopes":[{"prefix":"/scratch/` + string(rune('a'+i)) + `","ops":["read"]}]}`
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+rt.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("issue %d status=%d, want 201", i, resp.StatusCode)
		}
	}
}

func TestScopedTokenCannotManageTokens(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	key := setAuthRuntimeScopeKind(t, rt, meta.APIKeyScopeKindFS)
	if err := rt.meta.InsertAPIKeyFSScope(context.Background(), &meta.APIKeyFSScope{
		TenantID: rt.tenantID,
		APIKeyID: key.ID,
		Prefix:   "/scratch",
		Ops:      "read,write",
	}); err != nil {
		t.Fatal(err)
	}
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(`{"subject":"x","ttl_seconds":60,"scopes":[{"prefix":"/scratch","ops":["read"]}]}`))
	req.Header.Set("Authorization", "Bearer "+rt.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", resp.StatusCode)
	}

	revokeReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens/revoke", strings.NewReader(`{"api_key":"dat9_target"}`))
	revokeReq.Header.Set("Authorization", "Bearer "+rt.token)
	revokeReq.Header.Set("Content-Type", "application/json")
	revokeResp, err := http.DefaultClient.Do(revokeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = revokeResp.Body.Close()
	if revokeResp.StatusCode != http.StatusForbidden {
		t.Fatalf("revoke status=%d, want 403", revokeResp.StatusCode)
	}
}

func TestScopedTokenIssueRejectsInvalidPolicy(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cases := []struct {
		name string
		body string
	}{
		{name: "missing ttl", body: `{"subject":"vm0","scopes":[{"prefix":"/scratch","ops":["read"]}]}`},
		{name: "missing scopes", body: `{"subject":"vm0","ttl_seconds":60}`},
		{name: "bare colon prefix", body: `{"subject":"vm0-bare-colon","ttl_seconds":60,"scopes":[{"prefix":":","ops":["read"]}]}`},
		{name: "search without read", body: `{"subject":"vm0-search-only","ttl_seconds":60,"scopes":[{"prefix":"/scratch","ops":["search"]}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+rt.token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400", resp.StatusCode)
			}
		})
	}
}

func TestScopedTokenIssuePrevalidatesAllScopesBeforeInsert(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	badBody := `{"subject":"retry-subject","ttl_seconds":3600,"scopes":[{"prefix":"/scratch/ok","ops":["read"]},{"prefix":"/scratch/bad","ops":["search"]}]}`
	badReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(badBody))
	badReq.Header.Set("Authorization", "Bearer "+rt.token)
	badReq.Header.Set("Content-Type", "application/json")
	badResp, err := http.DefaultClient.Do(badReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad issue status=%d, want 400", badResp.StatusCode)
	}

	var count int
	if err := rt.meta.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND key_name = ?`,
		rt.tenantID, "retry-subject").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("api key rows after invalid request = %d, want 0", count)
	}

	goodBody := `{"subject":"retry-subject","ttl_seconds":3600,"scopes":[{"prefix":"/scratch/ok","ops":["read","write"]}]}`
	goodReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(goodBody))
	goodReq.Header.Set("Authorization", "Bearer "+rt.token)
	goodReq.Header.Set("Content-Type", "application/json")
	goodResp, err := http.DefaultClient.Do(goodReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = goodResp.Body.Close() }()
	if goodResp.StatusCode != http.StatusCreated {
		t.Fatalf("good issue status=%d, want 201", goodResp.StatusCode)
	}
	var issued scopedTokenResponse
	if err := json.NewDecoder(goodResp.Body).Decode(&issued); err != nil {
		t.Fatal(err)
	}
	rows, err := rt.meta.ListAPIKeyFSScopes(context.Background(), rt.tenantID, issued.TokenID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Prefix != "/scratch/ok" || rows[0].Ops != "read,write" {
		t.Fatalf("scope rows = %+v, want one intended row", rows)
	}
}

func TestScopedTokenIssueRejectsDuplicateNormalizedPrefixesBeforeInsert(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	body := `{"subject":"dup-prefix","ttl_seconds":3600,"scopes":[{"prefix":"/scratch","ops":["read"]},{"prefix":"/scratch/","ops":["write"]}]}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+rt.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
	var count int
	if err := rt.meta.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND key_name = ?`,
		rt.tenantID, "dup-prefix").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("api key rows after duplicate normalized prefix = %d, want 0", count)
	}
}

func TestScopedTokenRevokeInvalidatesToken(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	issueBody := []byte(`{"subject":"vm0-revoke","ttl_seconds":3600,"scopes":[{"prefix":"/scratch","ops":["read","write"]}]}`)
	issueReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", bytes.NewReader(issueBody))
	issueReq.Header.Set("Authorization", "Bearer "+rt.token)
	issueReq.Header.Set("Content-Type", "application/json")
	issueResp, err := http.DefaultClient.Do(issueReq)
	if err != nil {
		t.Fatal(err)
	}
	var issued scopedTokenResponse
	if err := json.NewDecoder(issueResp.Body).Decode(&issued); err != nil {
		t.Fatal(err)
	}
	_ = issueResp.Body.Close()
	if issueResp.StatusCode != http.StatusCreated {
		t.Fatalf("issue status=%d", issueResp.StatusCode)
	}

	revokeReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/tokens/"+issued.TokenID, nil)
	revokeReq.Header.Set("Authorization", "Bearer "+rt.token)
	revokeResp, err := http.DefaultClient.Do(revokeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = revokeResp.Body.Close()
	if revokeResp.StatusCode != http.StatusOK {
		t.Fatalf("revoke status=%d, want 200", revokeResp.StatusCode)
	}

	readReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/scratch/file.txt", nil)
	readReq.Header.Set("Authorization", "Bearer "+issued.Token)
	readResp, err := http.DefaultClient.Do(readReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = readResp.Body.Close()
	if readResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked token status=%d, want 401", readResp.StatusCode)
	}

	revokeAgainReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/tokens/"+issued.TokenID, nil)
	revokeAgainReq.Header.Set("Authorization", "Bearer "+rt.token)
	revokeAgainResp, err := http.DefaultClient.Do(revokeAgainReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = revokeAgainResp.Body.Close()
	if revokeAgainResp.StatusCode != http.StatusNotFound {
		t.Fatalf("second revoke status=%d, want 404", revokeAgainResp.StatusCode)
	}
}

func TestScopedTokenRevokeByAPIKeyInvalidatesToken(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	issueBody := []byte(`{"ttl_seconds":3600,"scopes":[{"prefix":"/scratch","ops":["read","write"]}]}`)
	issueReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", bytes.NewReader(issueBody))
	issueReq.Header.Set("Authorization", "Bearer "+rt.token)
	issueReq.Header.Set("Content-Type", "application/json")
	issueResp, err := http.DefaultClient.Do(issueReq)
	if err != nil {
		t.Fatal(err)
	}
	var issued scopedTokenResponse
	if err := json.NewDecoder(issueResp.Body).Decode(&issued); err != nil {
		t.Fatal(err)
	}
	_ = issueResp.Body.Close()
	if issueResp.StatusCode != http.StatusCreated {
		t.Fatalf("issue status=%d", issueResp.StatusCode)
	}

	revokeReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens/revoke", strings.NewReader(`{"api_key":"`+issued.Token+`"}`))
	revokeReq.Header.Set("Authorization", "Bearer "+rt.token)
	revokeReq.Header.Set("Content-Type", "application/json")
	revokeResp, err := http.DefaultClient.Do(revokeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = revokeResp.Body.Close()
	if revokeResp.StatusCode != http.StatusOK {
		t.Fatalf("revoke status=%d, want 200", revokeResp.StatusCode)
	}

	readReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/scratch/file.txt", nil)
	readReq.Header.Set("Authorization", "Bearer "+issued.Token)
	readResp, err := http.DefaultClient.Do(readReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = readResp.Body.Close()
	if readResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked token status=%d, want 401", readResp.StatusCode)
	}
}

func TestScopedTokenRevokeByAPIKeyCannotRevokeOwnerKey(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens/revoke", strings.NewReader(`{"api_key":"`+rt.token+`"}`))
	req.Header.Set("Authorization", "Bearer "+rt.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", resp.StatusCode)
	}
}

func TestScopedTokenRevokeByAPIKeyCannotCrossTenant(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	otherToken := insertAuthRuntimeOwnerToken(t, rt, token.NewID())
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	issueBody := []byte(`{"ttl_seconds":3600,"scopes":[{"prefix":"/scratch","ops":["read"]}]}`)
	issueReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens", bytes.NewReader(issueBody))
	issueReq.Header.Set("Authorization", "Bearer "+rt.token)
	issueReq.Header.Set("Content-Type", "application/json")
	issueResp, err := http.DefaultClient.Do(issueReq)
	if err != nil {
		t.Fatal(err)
	}
	var issued scopedTokenResponse
	if err := json.NewDecoder(issueResp.Body).Decode(&issued); err != nil {
		t.Fatal(err)
	}
	_ = issueResp.Body.Close()
	if issueResp.StatusCode != http.StatusCreated {
		t.Fatalf("issue status=%d", issueResp.StatusCode)
	}

	revokeReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tokens/revoke", strings.NewReader(`{"api_key":"`+issued.Token+`"}`))
	revokeReq.Header.Set("Authorization", "Bearer "+otherToken)
	revokeReq.Header.Set("Content-Type", "application/json")
	revokeResp, err := http.DefaultClient.Do(revokeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = revokeResp.Body.Close()
	if revokeResp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant revoke status=%d, want 404", revokeResp.StatusCode)
	}

	readReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/scratch/file.txt", nil)
	readReq.Header.Set("Authorization", "Bearer "+issued.Token)
	readResp, err := http.DefaultClient.Do(readReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = readResp.Body.Close()
	if readResp.StatusCode == http.StatusUnauthorized {
		t.Fatal("target token was revoked by cross-tenant owner")
	}
}

func insertAuthRuntimeOwnerToken(t *testing.T, rt *authTestRuntime, tenantID string) string {
	t.Helper()
	now := time.Now().UTC()
	source, err := rt.meta.ResolveByAPIKeyHash(context.Background(), token.HashToken(rt.token))
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           source.Tenant.DBHost,
		DBPort:           source.Tenant.DBPort,
		DBUser:           source.Tenant.DBUser,
		DBPasswordCipher: source.Tenant.DBPasswordCipher,
		DBName:           source.Tenant.DBName,
		DBTLS:            source.Tenant.DBTLS,
		Provider:         source.Tenant.Provider,
		SchemaVersion:    source.Tenant.SchemaVersion,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	rawToken, err := token.IssueToken(rt.tokenSecret, tenantID, 1)
	if err != nil {
		t.Fatal(err)
	}
	cipherToken, err := rt.pool.Encrypt(context.Background(), []byte(rawToken))
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.InsertAPIKey(context.Background(), &meta.APIKey{
		ID:            token.NewID(),
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: cipherToken,
		JWTHash:       token.HashToken(rawToken),
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	return rawToken
}
