package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminTenantEmbeddingConfigHelp(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	tenantHelp, err := captureStdoutE(t, func() error { return Admin([]string{"tenant", "--help"}) })
	if err != nil {
		t.Fatalf("tenant help: %v", err)
	}
	if !strings.Contains(tenantHelp, "embedding-config <get|set>") {
		t.Fatalf("tenant help missing embedding-config command:\n%s", tenantHelp)
	}

	stdout, err := captureStdoutE(t, func() error { return Admin([]string{"tenant", "embedding-config", "--help"}) })
	if err != nil {
		t.Fatalf("embedding config help: %v", err)
	}
	for _, want := range []string{"embedding-config <get|set>", "--enabled true|false", "--api-base URL", "--api-key KEY", "--model MODEL", "full replacement"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("help missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantEmbeddingConfigGetPrintsTableAndRedactsKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/tenants/tenant-1/embedding-config" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud headers")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled": true, "api_base": "https://provider.example.com/v1", "api_key": "plain-provider-key",
			"model": "embedding-model", "source": "custom", "generation": 4, "updated_at": "2026-08-28T01:02:03Z",
		})
	}))
	defer srv.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "embedding-config", "get", "--server", srv.URL, "--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1", "--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if strings.Contains(stdout, "plain-provider-key") {
		t.Fatal("table output leaked provider API key")
	}
	for _, want := range []string{"ENABLED", "SOURCE", "API_BASE", "API_KEY", "MODEL", "GENERATION", "UPDATED_AT", "true", "custom", "plai********", "embedding-model", "4"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("output missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantEmbeddingConfigSetEnabledSendsCompleteBody(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/admin/tenants/tenant-1/embedding-config" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"enabled": true, "api_key": "prov********", "source": "custom"})
	}))
	defer srv.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "embedding-config", "set", "--server", srv.URL, "--tenant-id", "tenant-1", "--enabled", "true",
			"--api-base", "https://provider.example.com/v1", "--api-key", "provider-secret", "--model", "embedding-model",
			"--tidbcloud-public-key", "public-1", "--tidbcloud-private-key", "private-1", "--json",
		})
	})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if len(gotBody) != 4 || gotBody["enabled"] != true || gotBody["api_base"] != "https://provider.example.com/v1" || gotBody["api_key"] != "provider-secret" || gotBody["model"] != "embedding-model" {
		t.Fatalf("request body = %#v", gotBody)
	}
	if strings.Contains(stdout, "provider-secret") {
		t.Fatal("JSON output leaked provider API key")
	}
}

func TestAdminTenantEmbeddingConfigSetDisabledSendsEnabledOnly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/admin/tenants/tenant-1/embedding-config" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"enabled": false, "source": "custom"})
	}))
	defer srv.Close()

	_, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "embedding-config", "set", "--server", srv.URL, "--tenant-id", "tenant-1", "--enabled", "false",
			"--tidbcloud-public-key", "public-1", "--tidbcloud-private-key", "private-1", "--json",
		})
	})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if len(gotBody) != 1 || gotBody["enabled"] != false {
		t.Fatalf("request body = %#v, want enabled=false only", gotBody)
	}
}

func TestAdminTenantEmbeddingConfigRejectsInvalidReplacementFlags(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	for _, args := range [][]string{
		{"tenant", "embedding-config", "get", "--server", "https://drive9.example.com"},
		{"tenant", "embedding-config", "set", "--server", "https://drive9.example.com", "--tenant-id", "tenant-1"},
		{"tenant", "embedding-config", "set", "--server", "https://drive9.example.com", "--tenant-id", "tenant-1", "--enabled", "true", "--api-base", "https://provider.example.com", "--api-key", "key"},
		{"tenant", "embedding-config", "set", "--server", "https://drive9.example.com", "--tenant-id", "tenant-1", "--enabled", "false", "--model", "model"},
	} {
		if _, err := captureStdoutE(t, func() error { return Admin(args) }); err == nil {
			t.Fatalf("Admin(%v) error = nil", args)
		}
	}
}
