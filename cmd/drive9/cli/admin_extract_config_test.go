package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
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
		"--protocol PROTOCOL",
		"--prompt TEXT",
		"non-empty when enabling",
		"empty clears",
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
			"api_key":    "plain-provider-key",
			"model":      "audio-model",
			"protocol":   "qwen-asr",
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
	if strings.Contains(stdout, "plain-provider-key") {
		t.Fatal("table output leaked the provider API key")
	}
	for _, want := range []string{"MEDIA_TYPE", "ENABLED", "SOURCE", "API_BASE", "API_KEY", "MODEL", "PROTOCOL", "PROMPT", "UPDATED_AT", "audio", "true", "custom", "qwen-asr", "plai********"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("output missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantExtractConfigGetRedactsPlaintextAPIKeyInJSON(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled": true,
			"api_key": "plain-provider-key",
			"source":  "custom",
		})
	}))
	defer srv.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "extract-config", "get",
			"--server", srv.URL,
			"--tenant-id", "tenant-1",
			"--media-type", "future-media",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if strings.Contains(stdout, "plain-provider-key") {
		t.Fatal("JSON output leaked the provider API key")
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("decode JSON output: %v", err)
	}
	if out["api_key"] != "plai********" {
		t.Fatalf("api_key = %q, want redacted value", out["api_key"])
	}
}

func TestRedactAdminTenantExtractConfigCopiesResponseAndKeepsMaskedKey(t *testing.T) {
	plaintext := "plain-provider-key"
	original := &client.AdminTenantExtractConfig{APIKey: &plaintext}
	redacted := redactAdminTenantExtractConfig(original)
	if original.APIKey == nil || *original.APIKey != "plain-provider-key" {
		t.Fatal("redaction modified the SDK response")
	}
	if redacted == original || redacted.APIKey == original.APIKey || redacted.APIKey == nil || *redacted.APIKey != "plai********" {
		t.Fatalf("redacted response = %#v", redacted)
	}

	alreadyMasked := "prov****"
	masked := redactAdminTenantExtractConfig(&client.AdminTenantExtractConfig{APIKey: &alreadyMasked})
	if masked.APIKey == nil || *masked.APIKey != alreadyMasked {
		t.Fatalf("already-masked API key = %q", optionalExtractString(masked.APIKey))
	}

	embeddedAsterisk := "ab*c-remaining-value"
	masked = redactAdminTenantExtractConfig(&client.AdminTenantExtractConfig{APIKey: &embeddedAsterisk})
	if masked.APIKey == nil || *masked.APIKey != "ab*c********" {
		t.Fatal("an embedded asterisk incorrectly bypassed redaction")
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
		t.Fatal("output leaked the provider API key")
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
			"--protocol", "openai",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if gotBody["enabled"] != true || gotBody["api_base"] != "https://provider.example.com" || gotBody["api_key"] != "provider-secret" || gotBody["model"] != "vision-model" || gotBody["protocol"] != "openai" || len(gotBody) != 5 {
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
	if err == nil {
		t.Fatal("set error = nil")
	}
	if strings.Contains(err.Error(), "provider-secret") || strings.Contains(errOutput, "provider-secret") {
		t.Fatal("error or output leaked the provider API key")
	}
}
