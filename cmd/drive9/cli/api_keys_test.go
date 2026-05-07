package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIKeyListJSON(t *testing.T) {
	home := withIsolatedHome(t)
	_ = home
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	issuedAt := time.Date(2026, time.May, 7, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tenants/keys" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-owner" {
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

	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "sk-owner")
	out, err := captureStdoutE(t, func() error { return APIKeyCmd([]string{"ls", "--json"}) })
	if err != nil {
		t.Fatalf("APIKeyCmd ls: %v", err)
	}
	if !strings.Contains(out, `"key_name": "default"`) {
		t.Fatalf("output = %q, want default key json", out)
	}
}

func TestAPIKeyCreatePrintsKeyValueLines(t *testing.T) {
	home := withIsolatedHome(t)
	_ = home
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tenants/keys" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
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

	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "sk-owner")
	out, err := captureStdoutE(t, func() error { return APIKeyCmd([]string{"create", "worker"}) })
	if err != nil {
		t.Fatalf("APIKeyCmd create: %v", err)
	}
	for _, want := range []string{"api_key=sk_worker", "key_id=k_worker", "key_name=worker", "status=active"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output = %q, want %q", out, want)
		}
	}
}

func TestAPIKeyGetJSON(t *testing.T) {
	home := withIsolatedHome(t)
	_ = home
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	issuedAt := time.Date(2026, time.May, 7, 12, 30, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tenants/keys/worker" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
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

	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "sk-owner")
	out, err := captureStdoutE(t, func() error { return APIKeyCmd([]string{"get", "worker", "--json"}) })
	if err != nil {
		t.Fatalf("APIKeyCmd get: %v", err)
	}
	if !strings.Contains(out, `"api_key": "sk_worker"`) || !strings.Contains(out, `"issued_at": "2026-05-07T12:30:00Z"`) {
		t.Fatalf("output = %q, want api key json", out)
	}
}

func TestAPIKeyDeleteCallsEndpoint(t *testing.T) {
	home := withIsolatedHome(t)
	_ = home
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/v1/tenants/keys/worker" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "sk-owner")
	out, err := captureStdoutE(t, func() error { return APIKeyCmd([]string{"rm", "worker"}) })
	if err != nil {
		t.Fatalf("APIKeyCmd rm: %v", err)
	}
	if !called {
		t.Fatal("delete endpoint was not called")
	}
	if out != "" {
		t.Fatalf("stdout = %q, want empty", out)
	}
}
