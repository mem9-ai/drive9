package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminTenantExtractConfigHelp(t *testing.T) {
	tenantHelp, err := captureStdoutE(t, func() error {
		return Admin([]string{"tenant", "--help"})
	})
	if err != nil {
		t.Fatalf("tenant help: %v", err)
	}
	if !strings.Contains(tenantHelp, "extract-config <get|set>") {
		t.Fatalf("tenant help missing extract-config command:\n%s", tenantHelp)
	}

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{"tenant", "extract-config", "--help"})
	})
	if err != nil {
		t.Fatalf("help: %v", err)
	}
	for _, want := range []string{
		"usage: drive9 admin tenant extract-config <get|set>",
		"--tenant-id ID",
		"--media-type TYPE",
		"--api-base URL",
		"--api-key KEY",
		"--model MODEL",
		"--prompt TEXT",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("help missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantExtractConfigGetPrintsTableAndHeaders(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/tenants/tenant-1/extract-config/audio" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud headers")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":    true,
			"api_base":   "https://provider.example.com",
			"api_key":    "prov********",
			"model":      "audio-model",
			"prompt":     "extract audio",
			"source":     "custom",
			"updated_at": "2026-08-21T01:02:03Z",
		})
	}))
	defer srv.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "extract-config", "get",
			"--server", srv.URL,
			"--tenant-id", "tenant-1",
			"--media-type", "audio",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	for _, want := range []string{"MEDIA_TYPE", "ENABLED", "SOURCE", "API_BASE", "API_KEY", "MODEL", "PROMPT", "UPDATED_AT", "audio", "true", "custom", "prov********"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("output missing %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "provider-secret") {
		t.Fatalf("output leaked provider key: %s", stdout)
	}
}

func TestAdminTenantExtractConfigSetPreservesExplicitFields(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/admin/tenants/tenant-1/extract-config/image" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled": false,
			"source":  "custom",
		})
	}))
	defer srv.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "extract-config", "set",
			"--server", srv.URL,
			"--tenant-id", "tenant-1",
			"--media-type", "image",
			"--enabled", "false",
			"--prompt", "",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if gotBody["enabled"] != false || gotBody["prompt"] != "" || len(gotBody) != 2 {
		t.Fatalf("body = %#v, want explicit false and empty prompt", gotBody)
	}
	if strings.Contains(stdout, "provider-secret") {
		t.Fatalf("output leaked provider key: %s", stdout)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, stdout)
	}
	if out["enabled"] != false || out["source"] != "custom" {
		t.Fatalf("JSON output = %#v", out)
	}
}

func TestAdminTenantExtractConfigSetFullProviderBody(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":  true,
			"api_base": "https://provider.example.com",
			"api_key":  "prov********",
			"model":    "vision-model",
			"source":   "custom",
		})
	}))
	defer srv.Close()

	_, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "extract-config", "set",
			"--server", srv.URL,
			"--tenant-id", "tenant-1",
			"--media-type", "image",
			"--enabled", "true",
			"--api-base", "https://provider.example.com",
			"--api-key", "provider-secret",
			"--model", "vision-model",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if gotBody["enabled"] != true || gotBody["api_base"] != "https://provider.example.com" || gotBody["api_key"] != "provider-secret" || gotBody["model"] != "vision-model" || len(gotBody) != 4 {
		t.Fatalf("body = %#v", gotBody)
	}
}

func TestAdminTenantExtractConfigRejectsMissingOrInvalidFlags(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	for _, args := range [][]string{
		{"tenant", "extract-config", "get", "--server", "https://drive9.example.com", "--media-type", "image"},
		{"tenant", "extract-config", "get", "--server", "https://drive9.example.com", "--tenant-id", "tenant-1"},
		{"tenant", "extract-config", "get", "--server", "https://drive9.example.com", "--tenant-id", "tenant-1", "--media-type", "image"},
		{"tenant", "extract-config", "set", "--server", "https://drive9.example.com", "--tenant-id", "tenant-1", "--media-type", "image", "--enabled", "maybe"},
		{"tenant", "extract-config", "set", "--server", "https://drive9.example.com", "--tenant-id", "tenant-1", "--media-type", "image"},
	} {
		if _, err := captureStdoutE(t, func() error { return Admin(args) }); err == nil {
			t.Fatalf("Admin(%v) error = nil", args)
		}
	}
}

func TestAdminTenantExtractConfigSetErrorDoesNotPrintProviderSecret(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"provider validation failed"}`)
	}))
	defer srv.Close()

	errOutput, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "extract-config", "set",
			"--server", srv.URL,
			"--tenant-id", "tenant-1",
			"--media-type", "image",
			"--enabled", "true",
			"--api-base", "https://provider.example.com",
			"--api-key", "provider-secret",
			"--model", "vision-model",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err == nil || strings.Contains(err.Error(), "provider-secret") || strings.Contains(errOutput, "provider-secret") {
		t.Fatalf("error/output leaked provider secret: err=%v output=%q", err, errOutput)
	}
}
