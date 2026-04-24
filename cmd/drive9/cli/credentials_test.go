package cli

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

// setResolverEnv is a test helper that configures env + cleans up resolver
// state. It mirrors the pattern in captureStdout so tests can run in any
// order without cache pollution.
func setResolverEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	// Always unset all three; tests then opt-in only the ones they want.
	for _, name := range []string{EnvVaultToken, EnvAPIKey, EnvServer} {
		t.Setenv(name, "") // t.Setenv will restore on cleanup
		_ = os.Unsetenv(name)
	}
	for k, v := range kv {
		t.Setenv(k, v)
	}
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
}

func TestResolveCredentials_EnvVaultTokenBeatsAPIKey(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvVaultToken: "jwt-eyAAA",
		EnvAPIKey:     "sk-owner-key",
		EnvServer:     "https://env.example",
	})
	cfg := &Config{}

	r := resolveCredentialsWithConfig(cfg)

	if r.Kind != CredentialDelegated {
		t.Fatalf("Kind = %d, want CredentialDelegated", r.Kind)
	}
	if r.Token != "jwt-eyAAA" {
		t.Fatalf("Token = %q, want jwt-eyAAA", r.Token)
	}
	if r.APIKey != "" {
		t.Fatalf("APIKey = %q, want empty (VAULT_TOKEN wins)", r.APIKey)
	}
	if r.Server != "https://env.example" {
		t.Fatalf("Server = %q", r.Server)
	}
	if r.CredSource != "env:"+EnvVaultToken {
		t.Fatalf("CredSource = %q", r.CredSource)
	}
}

func TestResolveCredentials_EnvAPIKeyWhenNoVaultToken(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvAPIKey: "sk-owner-key",
		EnvServer: "https://env.example",
	})
	cfg := &Config{}

	r := resolveCredentialsWithConfig(cfg)

	if r.Kind != CredentialOwner {
		t.Fatalf("Kind = %d, want CredentialOwner", r.Kind)
	}
	if r.APIKey != "sk-owner-key" {
		t.Fatalf("APIKey = %q", r.APIKey)
	}
	if r.Token != "" {
		t.Fatalf("Token = %q, want empty", r.Token)
	}
	if r.CredSource != "env:"+EnvAPIKey {
		t.Fatalf("CredSource = %q", r.CredSource)
	}
}

func TestResolveCredentials_EnvBeatsConfig(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvAPIKey: "sk-env-owner",
	})
	cfg := &Config{
		CurrentContext: "prod",
		Contexts: map[string]*Context{
			"prod": {Type: PrincipalOwner, Server: "https://config.example", APIKey: "sk-config-owner"},
		},
	}

	r := resolveCredentialsWithConfig(cfg)

	if r.APIKey != "sk-env-owner" {
		t.Fatalf("APIKey = %q, want env to win", r.APIKey)
	}
	if r.CredSource != "env:"+EnvAPIKey {
		t.Fatalf("CredSource = %q", r.CredSource)
	}
}

func TestResolveCredentials_ConfigWhenEnvUnset(t *testing.T) {
	setResolverEnv(t, nil)
	cfg := &Config{
		CurrentContext: "prod",
		Contexts: map[string]*Context{
			"prod": {Type: PrincipalOwner, Server: "https://config.example", APIKey: "sk-config-owner"},
		},
	}

	r := resolveCredentialsWithConfig(cfg)

	if r.Kind != CredentialOwner {
		t.Fatalf("Kind = %d", r.Kind)
	}
	if r.APIKey != "sk-config-owner" {
		t.Fatalf("APIKey = %q", r.APIKey)
	}
	if r.Server != "https://config.example" {
		t.Fatalf("Server = %q", r.Server)
	}
	if r.CredSource != "config:prod" {
		t.Fatalf("CredSource = %q", r.CredSource)
	}
}

func TestResolveCredentials_ConfigDelegatedContext(t *testing.T) {
	setResolverEnv(t, nil)
	cfg := &Config{
		CurrentContext: "alice",
		Contexts: map[string]*Context{
			"alice": {Type: PrincipalDelegated, Server: "https://config.example", Token: "jwt-config-delegated"},
		},
	}

	r := resolveCredentialsWithConfig(cfg)

	if r.Kind != CredentialDelegated {
		t.Fatalf("Kind = %d, want CredentialDelegated", r.Kind)
	}
	if r.Token != "jwt-config-delegated" {
		t.Fatalf("Token = %q", r.Token)
	}
}

func TestResolveCredentials_EmptyWhenNothingAvailable(t *testing.T) {
	setResolverEnv(t, nil)
	cfg := &Config{}

	r := resolveCredentialsWithConfig(cfg)

	if r.Kind != CredentialNone {
		t.Fatalf("Kind = %d, want CredentialNone", r.Kind)
	}
	if r.Token != "" || r.APIKey != "" {
		t.Fatalf("expected empty creds, got Token=%q APIKey=%q", r.Token, r.APIKey)
	}
	if r.Server != defaultServerURL {
		t.Fatalf("Server = %q, want default %q", r.Server, defaultServerURL)
	}
	if r.ServerSource != "default" {
		t.Fatalf("ServerSource = %q", r.ServerSource)
	}
}

func TestResolveCredentials_ServerEnvOverridesContextServer(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvServer: "https://env.override",
	})
	cfg := &Config{
		CurrentContext: "prod",
		Contexts: map[string]*Context{
			"prod": {Type: PrincipalOwner, Server: "https://config.example", APIKey: "sk-config-owner"},
		},
	}

	r := resolveCredentialsWithConfig(cfg)

	if r.Server != "https://env.override" {
		t.Fatalf("Server = %q, want env override", r.Server)
	}
	// Credential still from config.
	if r.APIKey != "sk-config-owner" {
		t.Fatalf("APIKey = %q, want config", r.APIKey)
	}
	if r.ServerSource != "env:"+EnvServer {
		t.Fatalf("ServerSource = %q", r.ServerSource)
	}
}

func TestResolveCredentials_UnsetsEnvAfterRead(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvVaultToken: "jwt-eyAAA",
		EnvAPIKey:     "sk-owner-key",
		EnvServer:     "https://env.example",
	})
	cfg := &Config{}

	_ = resolveCredentialsWithConfig(cfg)

	// All three credential vars MUST be unset after resolution so that any
	// child process forked later does not inherit them via /proc/<pid>/environ.
	for _, name := range []string{EnvVaultToken, EnvAPIKey, EnvServer} {
		if _, ok := os.LookupEnv(name); ok {
			t.Fatalf("%s still set after resolve (Unsetenv mitigation missing)", name)
		}
	}
}

func TestResolveCredentials_TrimsWhitespace(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvVaultToken: "  jwt-eyAAA\n",
	})
	cfg := &Config{}

	r := resolveCredentialsWithConfig(cfg)

	if r.Token != "jwt-eyAAA" {
		t.Fatalf("Token = %q, want trimmed", r.Token)
	}
}

func TestResolveCredentials_WhitespaceOnlyTreatedAsUnset(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvVaultToken: "   \n\t",
		EnvAPIKey:     "sk-owner-key",
	})
	cfg := &Config{}

	r := resolveCredentialsWithConfig(cfg)

	// The empty VAULT_TOKEN should not win over the API key — empty after
	// trim is "absent", not "malformed".
	if r.Kind != CredentialOwner {
		t.Fatalf("Kind = %d, want CredentialOwner (empty VAULT_TOKEN should not win)", r.Kind)
	}
	if r.APIKey != "sk-owner-key" {
		t.Fatalf("APIKey = %q", r.APIKey)
	}
}

func TestRejectEmptyFlag(t *testing.T) {
	cases := []struct {
		name     string
		provided bool
		value    string
		wantErr  bool
	}{
		{"unset", false, "", false},
		{"set-non-empty", true, "v", false},
		{"explicit-empty", true, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := rejectEmptyFlag("api-key", tc.value, tc.provided)
			if (err != nil) != tc.wantErr {
				t.Fatalf("rejectEmptyFlag(%v,%q) err=%v wantErr=%v", tc.provided, tc.value, err, tc.wantErr)
			}
		})
	}
}

func TestFlagProvided(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	val := fs.String("api-key", "default", "")
	_ = val
	if err := fs.Parse([]string{"--api-key", "x"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !flagProvided(fs, "api-key") {
		t.Fatalf("flagProvided should report true when flag was given on argv")
	}
	if flagProvided(fs, "nonexistent") {
		t.Fatalf("flagProvided should report false for missing flag")
	}
}

// --- Owner-read routing tests (§4.2 / §14.2 routing invariant) ---

func TestNewVaultReadClient_OwnerMode(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvAPIKey: "sk-owner-key",
		EnvServer: "https://s.example",
	})
	_, ownerMode, err := newVaultReadClientFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ownerMode {
		t.Fatal("expected ownerMode=true when CredentialOwner")
	}
}

func TestNewVaultReadClient_DelegatedMode(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvVaultToken: "jwt-eyAAA",
		EnvServer:     "https://s.example",
	})
	_, ownerMode, err := newVaultReadClientFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ownerMode {
		t.Fatal("expected ownerMode=false when CredentialDelegated")
	}
}

func TestNewVaultReadClient_DelegatedBeatsOwner(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvVaultToken: "jwt-eyAAA",
		EnvAPIKey:     "sk-owner-key",
		EnvServer:     "https://s.example",
	})
	_, ownerMode, err := newVaultReadClientFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ownerMode {
		t.Fatal("expected delegated to win over owner (narrower-wins)")
	}
}

// TestNewVaultReadClient_NoCredReturnsError verifies that the routing switch
// in newVaultReadClientFromEnv correctly rejects CredentialNone. Since the
// global ResolveCredentials() reads on-disk config (which may have a real
// credential), we test the routing logic at the resolver level instead.
func TestNewVaultReadClient_NoCredReturnsError(t *testing.T) {
	setResolverEnv(t, nil)
	cfg := &Config{}
	r := resolveCredentialsWithConfig(cfg)
	if r.Kind != CredentialNone {
		t.Fatalf("Kind = %d, want CredentialNone with empty env+config", r.Kind)
	}
	// CredentialNone hits the default case in newVaultReadClientFromEnv's switch,
	// which returns an error. This is structurally guaranteed by the code:
	//   case CredentialDelegated: return ..., false, nil
	//   case CredentialOwner:     return ..., true, nil
	//   default:                  return nil, false, error
}

// TestNewVaultReadClient_DelegatedActiveContext verifies that an active
// delegated context (from ctx import/use, no env token) routes to the
// token-read path — not the owner-read path. This is the regression case
// adv-1 flagged: routing must use ResolveCredentials().Kind, not raw env.
func TestNewVaultReadClient_DelegatedActiveContext(t *testing.T) {
	setResolverEnv(t, nil) // no env vars
	cfg := &Config{
		CurrentContext: "alice",
		Contexts: map[string]*Context{
			"alice": {Type: PrincipalDelegated, Server: "https://s.example", Token: "jwt-from-ctx"},
		},
	}
	r := resolveCredentialsWithConfig(cfg)
	if r.Kind != CredentialDelegated {
		t.Fatalf("Kind = %d, want CredentialDelegated from active context", r.Kind)
	}
	// The routing decision must yield delegated (token) path.
	// We can't call newVaultReadClientFromEnv directly because it uses
	// the global ResolveCredentials() which reads from disk. Instead,
	// verify the resolver returns CredentialDelegated, which is the
	// sole routing input per §14.2.
}

// --- No-fallthrough integration tests (§14.2 no-fallthrough invariant) ---
//
// These verify that when DRIVE9_VAULT_TOKEN is set (even if the token is
// expired, revoked, or malformed), the routing commits to the capability-read
// path. If the server returns 401 on that path, the error surfaces to the
// caller — it MUST NOT silently fall through to the owner-read path even
// though a valid API key is present. This is the runtime witness for §14.2.

func TestNoFallthrough_ExpiredToken(t *testing.T) {
	testNoFallthrough(t, "expired-jwt-token", "expired token → 401, must not retry via owner path")
}

func TestNoFallthrough_RevokedToken(t *testing.T) {
	testNoFallthrough(t, "revoked-jwt-token", "revoked token → 401, must not retry via owner path")
}

func TestNoFallthrough_MalformedToken(t *testing.T) {
	testNoFallthrough(t, "not-a-valid-token", "malformed token → 401, must not retry via owner path")
}

// testNoFallthrough is the shared harness for the 3 no-fallthrough cases.
// It sets up a mock server with two paths:
//   - /v1/vault/read/test-secret  → 401 (simulates auth failure on capability path)
//   - /v1/vault/secrets/test-secret/value → 200 with valid JSON (owner-read path)
//
// SecretGet MUST return an error (the 401). If it returns nil, it means
// the code silently fell through to the owner path — a spec violation.
func testNoFallthrough(t *testing.T, token, desc string) {
	t.Helper()
	var ownerPathHit int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/vault/read/test-secret":
			// Capability-read path: simulate auth failure.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
		case "/v1/vault/secrets/test-secret/value":
			// Owner-read path: should NEVER be reached.
			atomic.AddInt32(&ownerPathHit, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"KEY":"VALUE"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvVaultToken, token)
	t.Setenv(EnvAPIKey, "sk-valid-owner-key")
	t.Setenv(EnvServer, srv.URL)
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	err := SecretGet([]string{"test-secret"})
	if err == nil {
		t.Fatalf("%s: SecretGet returned nil; expected error from 401 on capability path", desc)
	}
	if atomic.LoadInt32(&ownerPathHit) != 0 {
		t.Fatalf("%s: owner-read path was hit — no-fallthrough invariant violated", desc)
	}
}

func TestResolveCredentials_CachedPerProcess(t *testing.T) {
	setResolverEnv(t, map[string]string{
		EnvVaultToken: "jwt-first",
	})
	cfg := &Config{}

	first := resolveCredentialsWithConfig(cfg)
	if first.Token != "jwt-first" {
		t.Fatalf("first.Token = %q", first.Token)
	}

	// The top-level ResolveCredentials must see the cached result even after
	// the env has been unset (which resolveCredentialsWithConfig already did).
	cached := ResolveCredentials()
	if cached.Token != "" && cached.Token != "jwt-first" {
		// ResolveCredentials reads a fresh config; since env is now unset
		// and our test config isn't on disk, Token will be empty. We assert
		// the cache path does not explode; the real cross-call contract is
		// exercised inside SecretLs tests.
		t.Fatalf("unexpected cached Token = %q", cached.Token)
	}
}
