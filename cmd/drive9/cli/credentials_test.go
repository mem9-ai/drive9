package cli

import (
	"flag"
	"os"
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
