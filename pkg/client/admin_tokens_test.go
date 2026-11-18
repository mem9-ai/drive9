package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdminGenerateTokenOwnerOmitsScopesAndBearer(t *testing.T) {
	issuedAt := time.Date(2026, 11, 18, 8, 0, 0, 0, time.UTC)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tokens/generate" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("unexpected Authorization header %q", got)
			http.Error(w, "unexpected authorization", http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "pub-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "priv-1" {
			t.Errorf("missing TiDB Cloud headers: %+v", r.Header)
			http.Error(w, "missing credentials", http.StatusUnauthorized)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if body["tenant_id"] != "tnt-1" || body["key_name"] != "cutover" {
			t.Errorf("unexpected body: %+v", body)
			http.Error(w, "unexpected body", http.StatusBadRequest)
			return
		}
		if _, exists := body["scopes"]; exists {
			t.Errorf("owner key request must omit scopes, got %+v", body)
			http.Error(w, "unexpected scopes", http.StatusBadRequest)
			return
		}
		if _, exists := body["ttl_seconds"]; exists {
			t.Errorf("owner key request without ttl must omit ttl_seconds, got %+v", body)
			http.Error(w, "unexpected ttl_seconds", http.StatusBadRequest)
			return
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

	// Construct with an existing Bearer key to verify AdminGenerateToken never sends it,
	// and leave Scopes nil for an intentional owner request.
	c := New(ts.URL, "d9_existing_bearer")
	resp, err := c.AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID:   "tnt-1",
		KeyName:    "cutover",
		Scopes:     nil,
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
			t.Errorf("decode body: %v", err)
			http.Error(w, "bad json", http.StatusBadRequest)
			return
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
			t.Errorf("unexpected scoped request: %+v", body)
			http.Error(w, "unexpected scoped request", http.StatusBadRequest)
			return
		}
		if body.Scopes[0].Prefix != "/data" || len(body.Scopes[0].Ops) != 2 || body.Scopes[0].Ops[0] != "read" || body.Scopes[0].Ops[1] != "list" {
			t.Errorf("unexpected scope grant: %+v", body.Scopes[0])
			http.Error(w, "unexpected scope grant", http.StatusBadRequest)
			return
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
	// A non-nil empty slice (e.g. after filtering all grants out of a policy) must
	// be rejected rather than silently upgraded to an unrestricted owner key.
	if _, err := c.AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID:   "tnt-1",
		TTLSeconds: &ttl,
		Scopes:     []FSScopeGrant{},
	}); err == nil || !strings.Contains(err.Error(), "scopes must not be empty") {
		t.Fatalf("expected non-nil empty scopes rejection, got %v", err)
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

func TestAdminGenerateTokenRejectsCrossOriginRedirectAndEmptyToken(t *testing.T) {
	var leakedHits atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leakedHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer evil.Close()

	redirectOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evil.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer redirectOrigin.Close()

	_, err := New(redirectOrigin.URL, "").AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID:   "tnt-1",
		PublicKey:  "pub-secret",
		PrivateKey: "priv-secret",
	})
	if err == nil || !strings.Contains(err.Error(), "cross-origin redirect refused") {
		t.Fatalf("expected cross-origin redirect refusal, got %v", err)
	}
	if got := leakedHits.Load(); got != 0 {
		t.Fatalf("cross-origin target received %d request(s), want 0", got)
	}

	emptyTokenOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"token_id": "key_missing_secret"})
	}))
	defer emptyTokenOrigin.Close()

	_, err = New(emptyTokenOrigin.URL, "").AdminGenerateToken(context.Background(), AdminGenerateTokenRequest{
		TenantID:   "tnt-1",
		PublicKey:  "pub-secret",
		PrivateKey: "priv-secret",
	})
	if err == nil || !strings.Contains(err.Error(), "missing token") {
		t.Fatalf("expected missing token error, got %v", err)
	}
}
