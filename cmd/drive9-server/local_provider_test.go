package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestApplyLocalProviderEnvDefaults(t *testing.T) {
	keys := []string{
		"DRIVE9_TENANT_PROVIDER",
		"DRIVE9_META_DSN",
		"DRIVE9_LOCAL_DSN",
		"DRIVE9_MASTER_KEY",
		"DRIVE9_TOKEN_SIGNING_KEY",
		"DRIVE9_LEADER_DISABLED",
		"DRIVE9_S3_DIR",
		"DRIVE9_S3_BUCKET",
		"DRIVE9_ENCRYPT_TYPE",
		"DRIVE9_DISABLE_AUTO_EMBEDDING",
		"DRIVE9_PUBLIC_URL",
		"DRIVE9_LISTEN_ADDR",
		"DRIVE9_LOCAL_MYSQL_DSN",
	}
	keys = append(keys, EnvLocalDevKeyFile, envLocalDevKeysInUse)
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	keyFile := filepath.Join(t.TempDir(), "local-dev-keys.json")
	t.Setenv(EnvLocalDevKeyFile, keyFile)

	if err := applyLocalProviderEnvDefaults(""); err != nil {
		t.Fatalf("applyLocalProviderEnvDefaults: %v", err)
	}

	if os.Getenv("DRIVE9_LISTEN_ADDR") != "127.0.0.1:9009" {
		t.Fatalf("LISTEN_ADDR=%q", os.Getenv("DRIVE9_LISTEN_ADDR"))
	}
	if os.Getenv("DRIVE9_META_DSN") != defaultLocalAdminDSN {
		t.Fatalf("META_DSN=%q", os.Getenv("DRIVE9_META_DSN"))
	}
	master, token := os.Getenv("DRIVE9_MASTER_KEY"), os.Getenv("DRIVE9_TOKEN_SIGNING_KEY")
	if err := validateKeyHex("DRIVE9_MASTER_KEY", master); err != nil {
		t.Fatalf("MASTER_KEY: %v", err)
	}
	if err := validateKeyHex("DRIVE9_TOKEN_SIGNING_KEY", token); err != nil {
		t.Fatalf("TOKEN_SIGNING_KEY: %v", err)
	}
	if master == token {
		t.Fatal("generated master and token signing keys must differ")
	}
	if os.Getenv(envLocalDevKeysInUse) != "1" {
		t.Fatal("generated keys were not recorded as dev keys")
	}
	info, err := os.Stat(keyFile)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("key file mode = %04o, want 0600", perm)
	}
	if os.Getenv("DRIVE9_LEADER_DISABLED") != "1" {
		t.Fatalf("LEADER_DISABLED=%q", os.Getenv("DRIVE9_LEADER_DISABLED"))
	}
	if os.Getenv("DRIVE9_TENANT_PROVIDER") != "" && os.Getenv("DRIVE9_TENANT_PROVIDER") != tenant.ProviderLocal {
		t.Fatalf("unexpected TENANT_PROVIDER=%q", os.Getenv("DRIVE9_TENANT_PROVIDER"))
	}
	if os.Getenv("DRIVE9_PUBLIC_URL") != "http://127.0.0.1:9009" {
		t.Fatalf("PUBLIC_URL=%q", os.Getenv("DRIVE9_PUBLIC_URL"))
	}
}

func TestApplyLocalProviderEnvDefaultsPublicURLFollowsListen(t *testing.T) {
	keys := []string{"DRIVE9_LISTEN_ADDR", "DRIVE9_PUBLIC_URL", "DRIVE9_MASTER_KEY", "DRIVE9_TOKEN_SIGNING_KEY", "DRIVE9_ENCRYPT_TYPE", "DRIVE9_LEADER_DISABLED", "DRIVE9_DISABLE_AUTO_EMBEDDING", "DRIVE9_S3_DIR", "DRIVE9_S3_BUCKET", "DRIVE9_LOCAL_DSN", "DRIVE9_LOCAL_MYSQL_DSN", "DRIVE9_META_DSN", EnvLocalDevKeyFile, envLocalDevKeysInUse}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	t.Setenv(EnvLocalDevKeyFile, filepath.Join(t.TempDir(), "local-dev-keys.json"))
	if err := applyLocalProviderEnvDefaults("127.0.0.1:19009"); err != nil {
		t.Fatalf("applyLocalProviderEnvDefaults: %v", err)
	}
	if os.Getenv("DRIVE9_LISTEN_ADDR") != "127.0.0.1:19009" {
		t.Fatalf("LISTEN_ADDR=%q", os.Getenv("DRIVE9_LISTEN_ADDR"))
	}
	if os.Getenv("DRIVE9_PUBLIC_URL") != "http://127.0.0.1:19009" {
		t.Fatalf("PUBLIC_URL=%q, want http://127.0.0.1:19009", os.Getenv("DRIVE9_PUBLIC_URL"))
	}
}

func TestResolveServerListenAddr(t *testing.T) {
	if got := resolveServerListenAddr("0.0.0.0:9009", "127.0.0.1:9009", true); got != "0.0.0.0:9009" {
		t.Fatalf("cli override = %q", got)
	}
	if got := resolveServerListenAddr("", "127.0.0.1:19009", true); got != "127.0.0.1:19009" {
		t.Fatalf("env = %q", got)
	}
	if got := resolveServerListenAddr("", "", true); got != "127.0.0.1:9009" {
		t.Fatalf("local default = %q", got)
	}
	if got := resolveServerListenAddr("", "", false); got != defaultListenAddr {
		t.Fatalf("non-local default = %q", got)
	}
}

func TestRejectLocalProviderNonLoopbackDefaults(t *testing.T) {
	keys := []string{"DRIVE9_LISTEN_ADDR", "DRIVE9_TOKEN_SIGNING_KEY", "DRIVE9_MASTER_KEY"}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })

	t.Setenv(envLocalDevKeysInUse, "1")
	if err := rejectLocalProviderNonLoopbackDefaults("127.0.0.1:9009"); err != nil {
		t.Fatalf("loopback: %v", err)
	}

	if err := rejectLocalProviderNonLoopbackDefaults("0.0.0.0:9009"); err == nil {
		t.Fatal("expected reject for 0.0.0.0 with default keys")
	}
	if err := rejectLocalProviderNonLoopbackDefaults(":9009"); err == nil {
		t.Fatal("expected reject for wildcard :9009 with default keys")
	}

	t.Setenv("DRIVE9_LISTEN_ADDR", "127.0.0.1:9009")
	t.Setenv(EnvLocalDevKeyFile, filepath.Join(t.TempDir(), "local-dev-keys.json"))
	if err := applyLocalProviderEnvDefaults("127.0.0.1:9009"); err != nil {
		t.Fatalf("applyLocalProviderEnvDefaults: %v", err)
	}
	if err := rejectLocalProviderNonLoopbackDefaults(resolveServerListenAddr("0.0.0.0:9009", os.Getenv("DRIVE9_LISTEN_ADDR"), true)); err == nil {
		t.Fatal("expected reject when positional listen overrides loopback env")
	}

	// Rotating to operator-supplied keys clears the dev-key marker, which is what
	// the guard keys off now.
	if err := os.Unsetenv(envLocalDevKeysInUse); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DRIVE9_TOKEN_SIGNING_KEY", strings.Repeat("ab", 32))
	t.Setenv("DRIVE9_MASTER_KEY", strings.Repeat("cd", 32))
	if err := rejectLocalProviderNonLoopbackDefaults("0.0.0.0:9009"); err != nil {
		t.Fatalf("overridden keys: %v", err)
	}
}

// The dev key file is the only source of local-provider keys: it must be
// generated once, reused across restarts, and never emitted again in place.
func TestLocalDevKeysAreGeneratedOnceAndReused(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "nested", "local-dev-keys.json")
	t.Setenv(EnvLocalDevKeyFile, keyFile)

	first, path, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("loadOrCreateLocalDevKeys: %v", err)
	}
	if path != keyFile {
		t.Fatalf("path = %q, want %q", path, keyFile)
	}
	if err := first.validate(); err != nil {
		t.Fatalf("generated keys invalid: %v", err)
	}
	second, _, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if second != first {
		t.Fatal("a restart regenerated the dev keys, which would invalidate tokens and strand vault data")
	}

	// Deleting the file is the rotation path.
	if err := os.Remove(keyFile); err != nil {
		t.Fatal(err)
	}
	rotated, _, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.MasterKeyHex == first.MasterKeyHex || rotated.TokenSigningKeyHex == first.TokenSigningKeyHex {
		t.Fatal("deleting the key file did not rotate the keys")
	}
}

// An unreadable key file must fail closed: silently regenerating would leave
// existing encrypted tenant data behind a key nobody holds.
func TestLocalDevKeysRejectMalformedFile(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "local-dev-keys.json")
	t.Setenv(EnvLocalDevKeyFile, keyFile)
	for _, body := range []string{
		"not json",
		`{"master_key_hex":"zz","token_signing_key_hex":"zz"}`,
		`{"master_key_hex":"","token_signing_key_hex":""}`,
		`{"master_key_hex":"` + strings.Repeat("ab", 32) + `","token_signing_key_hex":"` + strings.Repeat("ab", 32) + `"}`,
	} {
		if err := os.WriteFile(keyFile, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadOrCreateLocalDevKeys(); err == nil {
			t.Fatalf("accepted a malformed key file: %s", body)
		}
	}
}

// Operator-supplied keys win and are not overwritten by the generated pair.
func TestApplyLocalProviderDevKeysKeepsEnvironmentKeys(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "local-dev-keys.json")
	t.Setenv(EnvLocalDevKeyFile, keyFile)
	t.Setenv("DRIVE9_MASTER_KEY", strings.Repeat("11", 32))
	t.Setenv("DRIVE9_TOKEN_SIGNING_KEY", strings.Repeat("22", 32))
	t.Setenv(envLocalDevKeysInUse, "")

	if err := applyLocalProviderDevKeys(); err != nil {
		t.Fatalf("applyLocalProviderDevKeys: %v", err)
	}
	if got := os.Getenv("DRIVE9_MASTER_KEY"); got != strings.Repeat("11", 32) {
		t.Fatalf("master key was overwritten: %q", got)
	}
	if _, err := os.Stat(keyFile); err == nil {
		t.Fatal("a key file was written even though the environment supplied both keys")
	}
	if os.Getenv(envLocalDevKeysInUse) == "1" {
		t.Fatal("environment keys were marked as generated dev keys")
	}
}
