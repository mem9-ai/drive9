package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdminGenerateTokenOwnerOmitsScopesAndBearer(t *testing.T) {
	issuedAt := time.Date(2026, 11, 18, 8, 0, 0, 0, time.UTC)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tokens/generate" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Fatalf("unexpected Authorization header %q", got)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "pub-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "priv-1" {
			t.Fatalf("missing TiDB Cloud headers: %+v", r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["tenant_id"] != "tnt-1" || body["key_name"] != "cutover" {
			t.Fatalf("unexpected body: %+v", body)
		}
		if _, exists := body["scopes"]; exists {
			t.Fatalf("owner key request must omit scopes, got %+v", body)
		}
		if _, exists := body["ttl_seconds"]; exists {
			t.Fatalf("owner key request without ttl must omit ttl_seconds, got %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "d9_owner_secret",
			"token_id":   "key_1",
			"tenant_id":  "tnt-1",
			"key_name":   "cutover",
			"scope_kind": "owner",
			"status":     "active",
			"issued_at":  issuedAt.Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	// Construct with an existing Bearer key to verify AdminGenerateToken never sends it.
	c := New(ts.URL, "d9_existing_bearer")
	resp, err := c.AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID:   "tnt-1",
		KeyName:    "cutover",
		Scopes:     []FSScopeGrant{},
		PublicKey:  "pub-1",
		PrivateKey: "priv-1",
	})
	if err != nil {
		t.Fatalf("AdminGenerateToken: %v", err)
	}
	if resp.Token != "d9_owner_secret" || resp.TokenID != "key_1" || resp.ScopeKind != "owner" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestAdminGenerateTokenScopedAndConflictError(t *testing.T) {
	ttl := int64(3600)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body AdminGenerateTokenRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.TenantID == "tnt-provisioning" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "tenant is not ready",
				"code":  "conflict",
			})
			return
		}
		if body.TTLSeconds == nil || *body.TTLSeconds != 3600 || len(body.Scopes) != 1 {
			t.Fatalf("unexpected scoped request: %+v", body)
		}
		exp := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(AdminGenerateTokenResponse{
			Token:     "d9_scoped_secret",
			TokenID:   "key_scoped",
			TenantID:  body.TenantID,
			KeyName:   "agent",
			ScopeKind: "fs_scoped",
			Status:    "active",
			IssuedAt:  exp.Add(-time.Hour),
			ExpiresAt: &exp,
			Scopes:    body.Scopes,
		})
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	if _, err := c.AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{}); err == nil {
		t.Fatal("expected empty tenant_id error")
	}
	if _, err := c.AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID: "tnt-1",
		Scopes:   []FSScopeGrant{{Prefix: "/data", Ops: []string{"read"}}},
	}); err == nil {
		t.Fatal("expected missing ttl_seconds error for scoped token")
	}

	resp, err := c.AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID:   "tnt-1",
		KeyName:    "agent",
		TTLSeconds: &ttl,
		Scopes:     []FSScopeGrant{{Prefix: "/data", Ops: []string{"read", "list"}}},
		PublicKey:  "pub",
		PrivateKey: "priv",
	})
	if err != nil {
		t.Fatalf("AdminGenerateToken scoped: %v", err)
	}
	if resp.ScopeKind != "fs_scoped" || len(resp.Scopes) != 1 || resp.ExpiresAt == nil {
		t.Fatalf("unexpected scoped response: %+v", resp)
	}

	_, err = c.AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID:   "tnt-provisioning",
		PublicKey:  "pub",
		PrivateKey: "priv",
	})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 StatusError, got %v", err)
	}
}
