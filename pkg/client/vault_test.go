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
