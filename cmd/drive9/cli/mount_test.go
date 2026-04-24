package cli

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fakeLookPath(binMap map[string]bool) func(string) (string, error) {
	return func(name string) (string, error) {
		if binMap[name] {
			return "/usr/bin/" + name, nil
		}
		return "", errors.New("not found")
	}
}

func TestUmountArgvDarwin(t *testing.T) {
	got, err := umountArgv("darwin", fakeLookPath(nil), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"umount", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvPrefersFusermount3(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"fusermount3": true,
		"fusermount":  true,
		"umount":      true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"fusermount3", "-u", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvFallsBackToFusermount(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"fusermount": true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"fusermount", "-u", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvFallsBackToUmount(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"umount": true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"umount", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvNoBinary(t *testing.T) {
	_, err := umountArgv("linux", fakeLookPath(nil), "/mnt/drive9")
	if err == nil {
		t.Fatal("expected error when no unmount binaries are available")
	}
}

// TestResolveMountCredentials_OwnerFromResolver binds a mount to an owner
// API key sourced from the resolver (no --api-key flag). Asserts that
// apiKey routes through MountOptions.APIKey and token stays empty, which
// in pkg/fuse.Mount dispatches to client.New (tenantAuthMiddleware path).
func TestResolveMountCredentials_OwnerFromResolver(t *testing.T) {
	r := ResolvedCredentials{
		Kind:   CredentialOwner,
		Server: "https://owner.example",
		APIKey: "sk-owner",
	}
	server, apiKey, token, err := resolveMountCredentials(r, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server != "https://owner.example" {
		t.Fatalf("server = %q", server)
	}
	if apiKey != "sk-owner" {
		t.Fatalf("apiKey = %q, want sk-owner", apiKey)
	}
	if token != "" {
		t.Fatalf("token = %q, want empty (owner path)", token)
	}
}

// TestResolveMountCredentials_DelegatedFromResolver binds a mount to a
// delegated JWT sourced from the resolver (active context or
// DRIVE9_VAULT_TOKEN). Asserts that token routes through MountOptions.Token
// and apiKey stays empty, which in pkg/fuse.Mount dispatches to
// client.NewWithToken (capabilityAuthMiddleware path).
func TestResolveMountCredentials_DelegatedFromResolver(t *testing.T) {
	r := ResolvedCredentials{
		Kind:   CredentialDelegated,
		Server: "https://delegated.example",
		Token:  "jwt-aaa.bbb.ccc",
	}
	server, apiKey, token, err := resolveMountCredentials(r, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server != "https://delegated.example" {
		t.Fatalf("server = %q", server)
	}
	if apiKey != "" {
		t.Fatalf("apiKey = %q, want empty (delegated path)", apiKey)
	}
	if token != "jwt-aaa.bbb.ccc" {
		t.Fatalf("token = %q, want jwt-aaa.bbb.ccc", token)
	}
}

// TestResolveMountCredentials_Invariant6Snapshot is the CLI-layer half of
// Invariant #6: the credential captured at mount time MUST NOT be
// retroactively overridden by a later resolver snapshot (e.g. `ctx use`
// between call and mount). We simulate by taking two independent
// snapshots and asserting both were captured as-of their respective
// resolver states. There is no shared mutable state the second call can
// mutate into the first.
func TestResolveMountCredentials_Invariant6Snapshot(t *testing.T) {
	first := ResolvedCredentials{Kind: CredentialDelegated, Server: "https://s.example", Token: "jwt-original"}
	_, _, tok1, err := resolveMountCredentials(first, "", "")
	if err != nil {
		t.Fatalf("first: %v", err)
	}

	// Simulate `ctx use other-context` happening between two mount
	// attempts. Since the helper is pure, the second call cannot affect
	// the first's result — the first mount's binding is already frozen
	// into its returned triple.
	second := ResolvedCredentials{Kind: CredentialOwner, Server: "https://s.example", APIKey: "sk-rotated"}
	_, api2, tok2, err := resolveMountCredentials(second, "", "")
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if tok1 != "jwt-original" {
		t.Fatalf("first mount token = %q, want jwt-original (Invariant #6: first mount binding is frozen)", tok1)
	}
	if api2 != "sk-rotated" || tok2 != "" {
		t.Fatalf("second mount (apiKey=%q, token=%q) want (sk-rotated, empty)", api2, tok2)
	}
}

// TestResolveMountCredentials_FlagAPIKeyBeatsResolver documents that an
// explicit --api-key flag forces the owner path even when the resolver
// would otherwise return a delegated token. The flag is owner-only by
// construction; there is no --token flag.
func TestResolveMountCredentials_FlagAPIKeyBeatsResolver(t *testing.T) {
	r := ResolvedCredentials{
		Kind:   CredentialDelegated,
		Server: "https://s.example",
		Token:  "jwt-should-be-ignored",
	}
	_, apiKey, token, err := resolveMountCredentials(r, "", "sk-flag-owner")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if apiKey != "sk-flag-owner" {
		t.Fatalf("apiKey = %q, want sk-flag-owner (flag wins)", apiKey)
	}
	if token != "" {
		t.Fatalf("token = %q, want empty when --api-key given", token)
	}
}

// TestResolveMountCredentials_MissingCredential rejects mounts where
// neither a flag nor resolver produced a credential. Mount must refuse
// rather than silently succeed against a public endpoint.
func TestResolveMountCredentials_MissingCredential(t *testing.T) {
	r := ResolvedCredentials{Server: "https://s.example"} // Kind=CredentialNone
	_, _, _, err := resolveMountCredentials(r, "", "")
	if err == nil {
		t.Fatal("expected error when no credential is available")
	}
}

// TestResolveMountCredentials_MissingServer rejects mounts with no
// server URL (neither flag, env, nor config).
func TestResolveMountCredentials_MissingServer(t *testing.T) {
	r := ResolvedCredentials{Kind: CredentialOwner, APIKey: "sk-owner"}
	_, _, _, err := resolveMountCredentials(r, "", "")
	if err == nil {
		t.Fatal("expected error when no server URL is available")
	}
}

func TestValidateLookupRetryFlags(t *testing.T) {
	if err := validateLookupRetryFlags(2, 250*time.Millisecond); err != nil {
		t.Fatalf("validateLookupRetryFlags() unexpected error: %v", err)
	}

	if err := validateLookupRetryFlags(0, 250*time.Millisecond); err == nil || !strings.Contains(err.Error(), "--lookup-retry-count") {
		t.Fatalf("count=0 error = %v, want count validation error", err)
	}

	if err := validateLookupRetryFlags(2, 0); err == nil || !strings.Contains(err.Error(), "--lookup-retry-timeout") {
		t.Fatalf("timeout=0 error = %v, want timeout validation error", err)
	}
}

// ---------------------------------------------------------------------------
// Row A — only the CURRENT backend keyword ("vault") is special. All other
// first positionals flow into the legacy parser, which rejects extra
// positionals instead of pre-reserving names for future backends.
// ---------------------------------------------------------------------------

func TestMountCmd_BareWordFirstArgFlowsToLegacyArityCheck(t *testing.T) {
	for _, s := range []string{"kv", "s3", "gcs", "nfs", "mnt", "tmp", "vaultdir", "data"} {
		err := MountCmd([]string{s, "/mnt/x"})
		if err == nil {
			t.Fatalf("%q: expected positional-arity error", s)
		}
		if got := err.Error(); !strings.Contains(got, "exactly one mountpoint required") {
			t.Fatalf("%q: error = %q, want positional-arity rejection", s, got)
		}
		if strings.Contains(err.Error(), "unsupported mount backend") {
			t.Fatalf("%q: must not be rejected as reserved backend keyword", s)
		}
	}
}

func TestMountCmd_VaultStillDispatchesSeparately(t *testing.T) {
	err := MountCmd([]string{"vault", "/mnt/a", "/mnt/b"})
	if err == nil {
		t.Fatal("expected vault subcommand arity error")
	}
	if got := err.Error(); !strings.Contains(got, "drive9 mount vault: exactly one mountpoint required") {
		t.Fatalf("error = %q, want vault-specific arity rejection", got)
	}
}
