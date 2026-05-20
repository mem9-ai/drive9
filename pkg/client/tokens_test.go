package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIssueScopedTokenSendsRequest(t *testing.T) {
	var gotAuth string
	var gotSubject any
	var gotTTL float64
	var gotScopes []any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/tokens" {
			t.Fatalf("path = %q, want /v1/tokens", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotSubject = body["subject"]
		gotTTL, _ = body["ttl_seconds"].(float64)
		gotScopes, _ = body["scopes"].([]any)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"dat9_scoped","token_id":"key_123","subject":"vm0","scope_kind":"fs_scoped","expires_at":"2026-05-21T00:00:00Z","scopes":[{"prefix":"/scratch","ops":["read","write"]}]}`))
	}))
	defer ts.Close()

	c := New(ts.URL, "owner-key")
	resp, err := c.IssueScopedToken(context.Background(), IssueScopedTokenRequest{
		TTLSeconds: 3600,
		Scopes:     []FSScopeGrant{{Prefix: ":/scratch", Ops: []string{"read", "write"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer owner-key" {
		t.Fatalf("Authorization = %q, want owner bearer", gotAuth)
	}
	if gotSubject != nil || gotTTL != 3600 || len(gotScopes) != 1 {
		t.Fatalf("request body subject=%q ttl=%v scopes=%v", gotSubject, gotTTL, gotScopes)
	}
	if resp.Token != "dat9_scoped" || resp.TokenID != "key_123" || resp.ScopeKind != "fs_scoped" {
		t.Fatalf("response = %+v", resp)
	}
	if resp.ExpiresAt == nil {
		t.Fatal("ExpiresAt = nil, want timestamp")
	}
}

func TestRevokeScopedTokenByAPIKeyUsesPOSTBody(t *testing.T) {
	var gotPath, gotMethod string
	var gotBody map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := New(ts.URL, "owner-key")
	if err := c.RevokeScopedTokenByAPIKey(context.Background(), "dat9_scoped"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/v1/tokens/revoke" {
		t.Fatalf("path = %q, want /v1/tokens/revoke", gotPath)
	}
	if gotBody["api_key"] != "dat9_scoped" {
		t.Fatalf("body = %#v", gotBody)
	}
}

func TestRevokeScopedTokenUsesDELETE(t *testing.T) {
	var gotPath, gotMethod string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	c := New(ts.URL, "owner-key")
	if err := c.RevokeScopedToken(context.Background(), "key_abc"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete {
		t.Fatalf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/v1/tokens/key_abc" {
		t.Fatalf("path = %q, want /v1/tokens/key_abc", gotPath)
	}
}
