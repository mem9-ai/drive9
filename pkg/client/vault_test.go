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

func TestIssueVaultTokenUsesManagementAuthAndPayload(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/vault/tokens" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tenant-key" {
			t.Fatalf("Authorization = %q", got)
		}
		var req struct {
			AgentID string   `json:"agent_id"`
			TaskID  string   `json:"task_id"`
			Scope   []string `json:"scope"`
			TTLSecs int      `json:"ttl_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if req.AgentID != "deploy-agent" || req.TaskID != "task-123" {
			t.Fatalf("unexpected request: %+v", req)
		}
		if req.TTLSecs != 3600 {
			t.Fatalf("ttl_seconds = %d, want 3600", req.TTLSecs)
		}
		if len(req.Scope) != 2 || req.Scope[0] != "aws-prod" || req.Scope[1] != "db-prod/password" {
			t.Fatalf("scope = %+v", req.Scope)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "vault_token",
			"token_id":   "cap_123",
			"expires_at": "2026-04-14T00:00:00Z",
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	resp, err := c.IssueVaultToken(context.Background(), "deploy-agent", "task-123", []string{"aws-prod", "db-prod/password"}, time.Hour)
	if err != nil {
		t.Fatalf("IssueVaultToken: %v", err)
	}
	if resp.Token != "vault_token" || resp.TokenID != "cap_123" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestReadVaultSecretFieldUsesCapabilityToken(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read/db-prod/password" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer cap-token" {
			t.Fatalf("Authorization = %q", got)
		}
		_, _ = w.Write([]byte("hunter2"))
	}))
	defer srv.Close()

	c := New(srv.URL, "cap-token")
	data, err := c.ReadVaultSecretField(context.Background(), "db-prod", "password")
	if err != nil {
		t.Fatalf("ReadVaultSecretField: %v", err)
	}
	if data != "hunter2" {
		t.Fatalf("data = %q", data)
	}
}

func TestCreateVaultSecretReturnsStatusError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"secret already exists"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	_, err := c.CreateVaultSecret(context.Background(), "aws-prod", map[string]string{"access_key": "AKIA"})
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusConflict {
		t.Fatalf("statusErr = %#v", statusErr)
	}
}

func TestIssueVaultGrantSendsPayloadAndDecodesResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/vault/grants" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tenant-key" {
			t.Fatalf("Authorization = %q", got)
		}
		// Decode into a map so we can assert on which keys were emitted —
		// principal_type MUST NOT appear on the wire (server mints delegated
		// unconditionally per spec §16).
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := req["principal_type"]; ok {
			t.Fatalf("client MUST NOT send principal_type: got %v", req)
		}
		if req["agent"] != "alice" {
			t.Fatalf("agent = %v", req["agent"])
		}
		if req["ttl_seconds"].(float64) != 3600 {
			t.Fatalf("ttl_seconds = %v", req["ttl_seconds"])
		}
		if req["perm"] != "read" {
			t.Fatalf("perm = %v", req["perm"])
		}
		if req["label_hint"] != "prod-db-readonly" {
			t.Fatalf("label_hint = %v", req["label_hint"])
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "vt_header.payload.sig",
			"grant_id":   "grt_abc",
			"expires_at": "2026-04-19T13:00:00Z",
			"scope":      []string{"prod-db/DB_URL"},
			"perm":       "read",
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	resp, err := c.IssueVaultGrant(context.Background(), VaultGrantIssueRequest{
		Agent:      "alice",
		Scope:      []string{"prod-db/DB_URL"},
		Perm:       "read",
		TTLSeconds: 3600,
		LabelHint:  "prod-db-readonly",
	})
	if err != nil {
		t.Fatalf("IssueVaultGrant: %v", err)
	}
	if resp.Token != "vt_header.payload.sig" || resp.GrantID != "grt_abc" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(resp.Scope) != 1 || resp.Scope[0] != "prod-db/DB_URL" {
		t.Fatalf("scope = %+v", resp.Scope)
	}
	if resp.Perm != "read" {
		t.Fatalf("perm = %q", resp.Perm)
	}
}

func TestIssueVaultGrantPropagatesStatusError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"ttl_seconds is required and must be > 0"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	_, err := c.IssueVaultGrant(context.Background(), VaultGrantIssueRequest{
		Agent: "alice",
		Scope: []string{"prod-db"},
		Perm:  "read",
	})
	if err == nil {
		t.Fatal("expected error on missing ttl_seconds")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("statusErr = %#v", statusErr)
	}
}

func TestRevokeVaultGrantSendsDeleteWithPayload(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/v1/vault/grants/grt_abc" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tenant-key" {
			t.Fatalf("Authorization = %q", got)
		}
		var body struct {
			RevokedBy string `json:"revoked_by"`
			Reason    string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if body.RevokedBy != "admin" || body.Reason != "rotated" {
			t.Fatalf("unexpected body: %+v", body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	if err := c.RevokeVaultGrant(context.Background(), "grt_abc", "admin", "rotated"); err != nil {
		t.Fatalf("RevokeVaultGrant: %v", err)
	}
}

func TestRevokeVaultGrantNotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"grant not found"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	err := c.RevokeVaultGrant(context.Background(), "grt_missing", "admin", "gone")
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("statusErr = %#v", statusErr)
	}
}
