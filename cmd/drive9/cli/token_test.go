package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestParseTokenAllowUsesLastColonForDrivePaths(t *testing.T) {
	scope, err := parseTokenAllow(":/scratch/run-123/:write,read,list")
	if err != nil {
		t.Fatal(err)
	}
	if scope.Prefix != ":/scratch/run-123/" {
		t.Fatalf("prefix = %q", scope.Prefix)
	}
	if got := strings.Join(scope.Ops, ","); got != "read,list,write" {
		t.Fatalf("ops = %q, want read,list,write", got)
	}
}

func TestParseTokenAllowRejectsSearchWithoutRead(t *testing.T) {
	_, err := parseTokenAllow("/scratch:search")
	if err == nil {
		t.Fatal("parseTokenAllow error = nil, want error")
	}
	if !strings.Contains(err.Error(), "search op requires read") {
		t.Fatalf("error = %q", err)
	}
}

func TestTokenIssueSendsScopedTokenRequest(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	var gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tokens" {
			http.NotFound(w, r)
			return
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"dat9_scoped","token_id":"key_123","subject":"vm0-chat","scope_kind":"fs_scoped","expires_at":"2026-05-21T00:00:00Z","scopes":[{"prefix":"/scratch/run-123","ops":["read","list","write"]}]}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "owner-key")

	out := captureStdout(t, func() {
		if err := TokenIssue([]string{"vm0-chat", "--ttl", "24h", "--allow", ":/scratch/run-123/:write,read,list"}); err != nil {
			t.Fatalf("TokenIssue: %v", err)
		}
	})
	if gotAuth != "Bearer owner-key" {
		t.Fatalf("Authorization = %q, want owner bearer", gotAuth)
	}
	if _, ok := gotBody["subject"]; ok {
		t.Fatalf("request body includes server-visible subject: %#v", gotBody)
	}
	if gotBody["ttl_seconds"] != float64(86400) {
		t.Fatalf("request body = %#v", gotBody)
	}
	scopes, _ := gotBody["scopes"].([]any)
	if len(scopes) != 1 {
		t.Fatalf("scopes = %#v", gotBody["scopes"])
	}
	if !strings.Contains(out, "name=vm0-chat") || !strings.Contains(out, "token=dat9_scoped") || strings.Contains(out, "token_id=") || !strings.Contains(out, "scope=/scratch/run-123:read,list,write") {
		t.Fatalf("output = %q", out)
	}
	cfg := loadConfig()
	ctx := cfg.Contexts["vm0-chat"]
	if ctx == nil || ctx.Type != PrincipalFSScoped || ctx.APIKey != "dat9_scoped" || len(ctx.Scope) != 1 || ctx.Scope[0] != "/scratch/run-123:read,list,write" {
		t.Fatalf("saved context = %+v", ctx)
	}
}

func TestTokenIssueTokenOnly(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"dat9_scoped","token_id":"key_123","subject":"vm0","scope_kind":"fs_scoped","expires_at":"2026-05-21T00:00:00Z","scopes":[{"prefix":"/scratch","ops":["read"]}]}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "owner-key")

	out := captureStdout(t, func() {
		if err := TokenIssue([]string{"--ttl", "1h", "--allow", "/scratch:read", "--print"}); err != nil {
			t.Fatalf("TokenIssue: %v", err)
		}
	})
	if out != "dat9_scoped\n" {
		t.Fatalf("token-only output = %q", out)
	}
}

func TestTokenIssueNamedRollsBackWhenLocalSaveFails(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	var issueCalls, revokeCalls int
	var revokedAPIKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tokens":
			issueCalls++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"dat9_scoped","scope_kind":"fs_scoped","expires_at":"2026-05-21T00:00:00Z","scopes":[{"prefix":"/scratch","ops":["read"]}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tokens/revoke":
			revokeCalls++
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode revoke request: %v", err)
			}
			revokedAPIKey = body["api_key"]
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	home := t.TempDir()
	if err := os.WriteFile(home+"/.drive9", []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "owner-key")

	err := TokenIssue([]string{"smoke", "--ttl", "1h", "--allow", "/scratch:read"})
	if err == nil {
		t.Fatal("TokenIssue error = nil, want local save failure")
	}
	if !strings.Contains(err.Error(), "issued token was revoked") {
		t.Fatalf("error = %q", err)
	}
	if issueCalls != 1 || revokeCalls != 1 {
		t.Fatalf("issue/revoke calls = %d/%d, want 1/1", issueCalls, revokeCalls)
	}
	if revokedAPIKey != "dat9_scoped" {
		t.Fatalf("revoked api_key = %q, want dat9_scoped", revokedAPIKey)
	}
}

func TestTokenIssueNamedReportsRollbackFailure(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	var revokeCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tokens":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"dat9_scoped","scope_kind":"fs_scoped","expires_at":"2026-05-21T00:00:00Z","scopes":[{"prefix":"/scratch","ops":["read"]}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tokens/revoke":
			revokeCalls++
			http.Error(w, `{"error":"rollback failed"}`, http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	home := t.TempDir()
	if err := os.WriteFile(home+"/.drive9", []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "owner-key")

	err := TokenIssue([]string{"smoke", "--ttl", "1h", "--allow", "/scratch:read"})
	if err == nil {
		t.Fatal("TokenIssue error = nil, want rollback failure")
	}
	if !strings.Contains(err.Error(), "issued token but failed to save local context and rollback revoke failed") {
		t.Fatalf("error = %q", err)
	}
	if !strings.Contains(err.Error(), "token=dat9_scoped") {
		t.Fatalf("error missing recovery token handle: %q", err)
	}
	if revokeCalls != 1 {
		t.Fatalf("revoke calls = %d, want 1", revokeCalls)
	}
}

func TestTokenIssueRequiresOwnerAPIKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvVaultToken, "delegated")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	err := TokenIssue([]string{"--subject", "vm0", "--ttl", "1h", "--allow", "/scratch:read"})
	if err == nil {
		t.Fatal("TokenIssue error = nil, want missing owner API key")
	}
	if !strings.Contains(err.Error(), EnvAPIKey) {
		t.Fatalf("error = %q, want %s", err, EnvAPIKey)
	}
}

func TestTokenRevokeUsesScopedTokenEndpoint(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	if err := TokenRevoke([]string{"key_123"}); err != nil {
		t.Fatalf("TokenRevoke: %v", err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/v1/tokens/key_123" {
		t.Fatalf("method/path = %s %s, want DELETE /v1/tokens/key_123", gotMethod, gotPath)
	}
}

func TestTokenRevokeLocalNameUsesAPIKeyAndForgetsContext(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	cfg := &Config{Server: srv.URL, Contexts: map[string]*Context{}}
	if _, err := ctxAdd(cfg, "owner", &Context{Type: PrincipalOwner, Server: srv.URL, APIKey: "owner-key"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctxAdd(cfg, "smoke", &Context{Type: PrincipalFSScoped, Server: srv.URL, APIKey: "dat9_scoped", Scope: []string{"/scratch:read"}}); err != nil {
		t.Fatal(err)
	}
	cfg.CurrentContext = "owner"
	if err := saveConfig(cfg); err != nil {
		t.Fatal(err)
	}
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	if err := TokenRevoke([]string{"smoke"}); err != nil {
		t.Fatalf("TokenRevoke: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/tokens/revoke" {
		t.Fatalf("method/path = %s %s, want POST /v1/tokens/revoke", gotMethod, gotPath)
	}
	if gotBody["api_key"] != "dat9_scoped" {
		t.Fatalf("request body = %#v", gotBody)
	}
	if _, ok := loadConfig().Contexts["smoke"]; ok {
		t.Fatal("local scoped token context still present after revoke")
	}
}

func TestTokenRevokeRejectsAPIKeyInArgv(t *testing.T) {
	err := TokenRevoke([]string{"dat9_secret"})
	if err == nil {
		t.Fatal("TokenRevoke error = nil, want argv secret rejection")
	}
	if !strings.Contains(err.Error(), "pipe it") {
		t.Fatalf("error = %q", err)
	}
}

func TestTokenForgetRemovesOnlyLocalContext(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg := &Config{Contexts: map[string]*Context{}}
	if _, err := ctxAdd(cfg, "smoke", &Context{Type: PrincipalFSScoped, Server: "https://s", APIKey: "dat9_scoped"}); err != nil {
		t.Fatal(err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := TokenForget([]string{"smoke"}); err != nil {
			t.Fatalf("TokenForget: %v", err)
		}
	})
	if !strings.Contains(out, `forgot local token "smoke"`) {
		t.Fatalf("output = %q", out)
	}
	if _, ok := loadConfig().Contexts["smoke"]; ok {
		t.Fatal("local scoped token context still present after forget")
	}
}

func TestTokenRevokeStdinUsesAPIKeyEndpoint(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tokens/revoke" {
			t.Fatalf("method/path = %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, srv.URL)
	t.Setenv(EnvAPIKey, "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = oldStdin })
	_, _ = w.WriteString("dat9_from_stdin\n")
	_ = w.Close()

	if err := TokenRevoke([]string{"-"}); err != nil {
		t.Fatalf("TokenRevoke: %v", err)
	}
	if gotBody["api_key"] != "dat9_from_stdin" {
		t.Fatalf("request body = %#v", gotBody)
	}
}
