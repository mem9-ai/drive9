package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		if err := TokenIssue([]string{"--subject", "vm0-chat", "--ttl", "24h", "--allow", ":/scratch/run-123/:write,read,list"}); err != nil {
			t.Fatalf("TokenIssue: %v", err)
		}
	})
	if gotAuth != "Bearer owner-key" {
		t.Fatalf("Authorization = %q, want owner bearer", gotAuth)
	}
	if gotBody["subject"] != "vm0-chat" || gotBody["ttl_seconds"] != float64(86400) {
		t.Fatalf("request body = %#v", gotBody)
	}
	scopes, _ := gotBody["scopes"].([]any)
	if len(scopes) != 1 {
		t.Fatalf("scopes = %#v", gotBody["scopes"])
	}
	if !strings.Contains(out, "token=dat9_scoped") || !strings.Contains(out, "token_id=key_123") || !strings.Contains(out, "scope=/scratch/run-123:read,list,write") {
		t.Fatalf("output = %q", out)
	}
}

func TestTokenIssueTokenOnly(t *testing.T) {
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
		if err := TokenIssue([]string{"--subject", "vm0", "--ttl", "1h", "--allow", "/scratch:read", "--token-only"}); err != nil {
			t.Fatalf("TokenIssue: %v", err)
		}
	})
	if out != "dat9_scoped\n" {
		t.Fatalf("token-only output = %q", out)
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
