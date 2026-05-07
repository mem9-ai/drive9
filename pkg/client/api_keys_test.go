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

func TestListTenantAPIKeysDecodesResponse(t *testing.T) {
	t.Parallel()

	issuedAt := time.Date(2026, time.May, 7, 8, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/v1/tenants/keys" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tenant-key" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"key_id":    "k_default",
				"key_name":  "default",
				"status":    "active",
				"issued_at": issuedAt.Format(time.RFC3339),
			}},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	keys, err := c.ListTenantAPIKeys(context.Background())
	if err != nil {
		t.Fatalf("ListTenantAPIKeys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("len(keys) = %d, want 1", len(keys))
	}
	if keys[0].KeyName != "default" || keys[0].KeyID != "k_default" {
		t.Fatalf("unexpected key: %+v", keys[0])
	}
}

func TestCreateTenantAPIKeySendsPayloadAndDecodesResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/tenants/keys" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tenant-key" {
			t.Fatalf("Authorization = %q", got)
		}
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if req["key_name"] != "worker" {
			t.Fatalf("key_name = %q, want worker", req["key_name"])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"api_key":  "sk_worker",
			"key_id":   "k_worker",
			"key_name": "worker",
			"status":   "active",
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	key, err := c.CreateTenantAPIKey(context.Background(), "worker")
	if err != nil {
		t.Fatalf("CreateTenantAPIKey: %v", err)
	}
	if key.APIKey != "sk_worker" || key.KeyName != "worker" || key.KeyID != "k_worker" {
		t.Fatalf("unexpected key: %+v", key)
	}
}

func TestGetTenantAPIKeyDecodesResponse(t *testing.T) {
	t.Parallel()

	issuedAt := time.Date(2026, time.May, 7, 8, 30, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/v1/tenants/keys/worker" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"api_key":   "sk_worker",
			"key_id":    "k_worker",
			"key_name":  "worker",
			"status":    "active",
			"issued_at": issuedAt.Format(time.RFC3339),
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	key, err := c.GetTenantAPIKey(context.Background(), "worker")
	if err != nil {
		t.Fatalf("GetTenantAPIKey: %v", err)
	}
	if key.APIKey != "sk_worker" || !key.IssuedAt.Equal(issuedAt) {
		t.Fatalf("unexpected key: %+v", key)
	}
}

func TestDeleteTenantAPIKeyPropagatesStatusError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/v1/tenants/keys/worker" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"api key not found or already revoked"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "tenant-key")
	err := c.DeleteTenantAPIKey(context.Background(), "worker")
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("statusErr = %#v", statusErr)
	}
}
