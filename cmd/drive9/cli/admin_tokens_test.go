package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminTenantGenerateTokenOwnerAndTokenOnly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tokens/generate" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "pub-target" || r.Header.Get("X-TiDBCloud-Private-Key") != "priv-target" {
			t.Fatalf("missing TiDB Cloud headers: %+v", r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["tenant_id"] != "tnt-migrated" {
			t.Fatalf("unexpected tenant_id: %+v", body)
		}
		if _, ok := body["scopes"]; ok {
			t.Fatalf("owner key must omit scopes: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "d9_new_target_owner_key",
			"token_id":   "key_target_1",
			"tenant_id":  "tnt-migrated",
			"key_name":   "cutover",
			"scope_kind": "owner",
			"status":     "active",
			"issued_at":  time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC).Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	tableOut, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "generate-token",
			"--server", ts.URL,
			"--tenant-id", "tnt-migrated",
			"--key-name", "cutover",
			"--tidbcloud-public-key", "pub-target",
			"--tidbcloud-private-key", "priv-target",
		})
	})
	if err != nil {
		t.Fatalf("generate-token table: %v", err)
	}
	for _, want := range []string{"TENANT_ID", "TOKEN_ID", "tnt-migrated", "key_target_1", "owner", "d9_new_target_owner_key"} {
		if !strings.Contains(tableOut, want) {
			t.Fatalf("table output missing %q:\n%s", want, tableOut)
		}
	}

	rawOut, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "generate-token",
			"--server", ts.URL,
			"--tenant-id", "tnt-migrated",
			"--token-only",
			"--tidbcloud-public-key", "pub-target",
			"--tidbcloud-private-key", "priv-target",
		})
	})
	if err != nil {
		t.Fatalf("generate-token --token-only: %v", err)
	}
	if strings.TrimSpace(rawOut) != "d9_new_target_owner_key" {
		t.Fatalf("got %q, want bare token", rawOut)
	}
}

func TestAdminTenantGenerateTokenScopedAndValidation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["ttl_seconds"] != float64(7200) {
			t.Fatalf("unexpected ttl_seconds: %+v", body)
		}
		scopes, _ := body["scopes"].([]any)
		if len(scopes) != 1 {
			t.Fatalf("unexpected scopes: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "d9_scoped_target_key",
			"token_id":   "key_scoped_1",
			"tenant_id":  "tnt-1",
			"key_name":   "default",
			"scope_kind": "fs_scoped",
			"status":     "active",
			"issued_at":  "2026-11-18T10:00:00Z",
			"expires_at": "2026-11-18T12:00:00Z",
		})
	}))
	defer ts.Close()

	jsonOut, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "generate-token",
			"--server", ts.URL,
			"--tenant-id", "tnt-1",
			"--ttl", "2h",
			"--allow", "/workspace:read,list",
			"--json",
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
		})
	})
	if err != nil {
		t.Fatalf("generate-token scoped --json: %v", err)
	}
	if !strings.Contains(jsonOut, `"scope_kind": "fs_scoped"`) || !strings.Contains(jsonOut, `"d9_scoped_target_key"`) {
		t.Fatalf("unexpected JSON output:\n%s", jsonOut)
	}

	for _, badArgs := range [][]string{
		{"tenant", "generate-token", "--server", ts.URL, "--tidbcloud-public-key", "p", "--tidbcloud-private-key", "s"},
		{"tenant", "generate-token", "--server", ts.URL, "--tenant-id", "tnt-1", "--allow", "/a:read", "--tidbcloud-public-key", "p", "--tidbcloud-private-key", "s"},
		{"tenant", "generate-token", "--server", ts.URL, "--tenant-id", "tnt-1", "--json", "--token-only", "--tidbcloud-public-key", "p", "--tidbcloud-private-key", "s"},
	} {
		if err := Admin(badArgs); err == nil {
			t.Fatalf("expected validation error for args %v", badArgs)
		}
	}
}
