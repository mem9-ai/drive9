package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAdminTenantGenerateTokenOwnerAndTokenOnly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var (
		mu             sync.Mutex
		lastKeyName    string
		lastTTLSeconds float64
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tokens/generate" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "pub-target" || r.Header.Get("X-TiDBCloud-Private-Key") != "priv-target" {
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
		if body["tenant_id"] != "tnt-migrated" {
			t.Errorf("unexpected tenant_id: %+v", body)
			http.Error(w, "unexpected tenant_id", http.StatusBadRequest)
			return
		}
		if _, ok := body["scopes"]; ok {
			t.Errorf("owner key must omit scopes: %+v", body)
			http.Error(w, "unexpected scopes", http.StatusBadRequest)
			return
		}
		keyNameVal, _ := body["key_name"].(string)
		if keyNameVal == "" {
			keyNameVal = "default"
		}
		ttlVal, _ := body["ttl_seconds"].(float64)
		mu.Lock()
		lastKeyName = keyNameVal
		lastTTLSeconds = ttlVal
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "d9_new_target_owner_key",
			"token_id":   "key_target_1",
			"tenant_id":  "tnt-migrated",
			"key_name":   keyNameVal,
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
	mu.Lock()
	cutoverTTL := lastTTLSeconds
	mu.Unlock()
	if cutoverTTL != 0 {
		t.Fatalf("cutover owner request without --ttl sent ttl_seconds=%v, want omitted (0)", cutoverTTL)
	}

	// Ensure a flag value named "help", --print alias, and optional owner --ttl work cleanly.
	rawOut, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "generate-token",
			"--server", ts.URL,
			"--tenant-id", "tnt-migrated",
			"--key-name", "help",
			"--ttl", "24h",
			"--print",
			"--tidbcloud-public-key", "pub-target",
			"--tidbcloud-private-key", "priv-target",
		})
	})
	if err != nil {
		t.Fatalf("generate-token --key-name help --ttl 24h --print: %v", err)
	}
	mu.Lock()
	gotName, gotTTL := lastKeyName, lastTTLSeconds
	mu.Unlock()
	if strings.TrimSpace(rawOut) != "d9_new_target_owner_key" || gotName != "help" || gotTTL != 86400 {
		t.Fatalf("got out=%q key_name=%q ttl=%v, want bare token with key_name=help ttl=86400", rawOut, gotName, gotTTL)
	}

	// Attached --key-name=VALUE accepts labels beginning with "--".
	if _, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "generate-token",
			"--server", ts.URL,
			"--tenant-id", "tnt-migrated",
			"--key-name=--cutover",
			"--token-only",
			"--tidbcloud-public-key", "pub-target",
			"--tidbcloud-private-key", "priv-target",
		})
	}); err != nil {
		t.Fatalf("generate-token --key-name=--cutover: %v", err)
	}
	mu.Lock()
	gotDashName := lastKeyName
	mu.Unlock()
	if gotDashName != "--cutover" {
		t.Fatalf("got key_name=%q, want --cutover", gotDashName)
	}

	if emptyKeyErr := Admin([]string{
		"tenant", "generate-token",
		"--server", ts.URL,
		"--tenant-id", "tnt-migrated",
		"--key-name", "",
		"--tidbcloud-public-key", "pub-target",
		"--tidbcloud-private-key", "priv-target",
	}); emptyKeyErr == nil || strings.Contains(emptyKeyErr.Error(), "credential") {
		t.Fatalf("expected non-credential empty --key-name error, got %v", emptyKeyErr)
	}

	emptyTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer emptyTS.Close()

	emptyOut, emptyErr := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "generate-token",
			"--server", emptyTS.URL,
			"--tenant-id", "tnt-migrated",
			"--token-only",
			"--tidbcloud-public-key", "pub-target",
			"--tidbcloud-private-key", "priv-target",
		})
	})
	if emptyErr == nil || !strings.Contains(emptyErr.Error(), "missing token") || emptyOut != "" {
		t.Fatalf("expected missing-token CLI error and exact empty stdout, got err=%v stdout=%q", emptyErr, emptyOut)
	}

	if overMaxTTLErr := Admin([]string{
		"tenant", "generate-token",
		"--server", ts.URL,
		"--tenant-id", "tnt-migrated",
		"--ttl", "8761h",
		"--tidbcloud-public-key", "pub-target",
		"--tidbcloud-private-key", "priv-target",
	}); overMaxTTLErr == nil || !strings.Contains(overMaxTTLErr.Error(), "8760h") {
		t.Fatalf("expected --ttl > 8760h error, got %v", overMaxTTLErr)
	}

	if mutualErr := Admin([]string{
		"tenant", "generate-token",
		"--server", ts.URL,
		"--tenant-id", "tnt-migrated",
		"--json", "--print",
		"--tidbcloud-public-key", "pub-target",
		"--tidbcloud-private-key", "priv-target",
	}); mutualErr == nil || !strings.Contains(mutualErr.Error(), "--print") {
		t.Fatalf("expected --json and --print mutual exclusion error, got %v", mutualErr)
	}

	helpOut, err := captureStdoutE(t, func() error {
		return Admin([]string{"tenant", "generate-token", "--help"})
	})
	if err != nil || !strings.Contains(helpOut, "usage: drive9 admin tenant generate-token") {
		t.Fatalf("generate-token --help: err=%v out=%q", err, helpOut)
	}
}

func TestAdminTenantGenerateTokenScopedAndValidation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if body["ttl_seconds"] != float64(7200) {
			t.Errorf("unexpected ttl_seconds: %+v", body)
			http.Error(w, "unexpected ttl_seconds", http.StatusBadRequest)
			return
		}
		scopes, _ := body["scopes"].([]any)
		if len(scopes) != 1 {
			t.Errorf("unexpected scopes: %+v", body)
			http.Error(w, "unexpected scopes", http.StatusBadRequest)
			return
		}
		grant, _ := scopes[0].(map[string]any)
		ops, _ := grant["ops"].([]any)
		if grant["prefix"] != "/workspace" || len(ops) != 2 || ops[0] != "read" || ops[1] != "list" {
			t.Errorf("unexpected grant payload: %+v", grant)
			http.Error(w, "unexpected grant payload", http.StatusBadRequest)
			return
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
		{"tenant", "generate-token", "--server", ts.URL, "--tenant-id", "tnt-1", "--key-name", "--token-only", "--tidbcloud-public-key", "p", "--tidbcloud-private-key", "s"},
	} {
		if err := Admin(badArgs); err == nil {
			t.Fatalf("expected validation error for args %v", badArgs)
		}
	}
}
