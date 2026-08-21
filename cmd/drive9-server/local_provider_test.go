package main

import (
	"os"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/local"
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
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	applyLocalProviderEnvDefaults("")

	if os.Getenv("DRIVE9_LISTEN_ADDR") != "127.0.0.1:9009" {
		t.Fatalf("LISTEN_ADDR=%q", os.Getenv("DRIVE9_LISTEN_ADDR"))
	}
	if os.Getenv("DRIVE9_META_DSN") != defaultLocalAdminDSN {
		t.Fatalf("META_DSN=%q", os.Getenv("DRIVE9_META_DSN"))
	}
	if os.Getenv("DRIVE9_MASTER_KEY") != local.DefaultMasterKeyHex {
		t.Fatalf("MASTER_KEY not defaulted")
	}
	if os.Getenv("DRIVE9_TOKEN_SIGNING_KEY") != local.DefaultTokenSigningKeyHex {
		t.Fatalf("TOKEN_SIGNING_KEY not defaulted")
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
	keys := []string{"DRIVE9_LISTEN_ADDR", "DRIVE9_PUBLIC_URL", "DRIVE9_MASTER_KEY", "DRIVE9_TOKEN_SIGNING_KEY", "DRIVE9_ENCRYPT_TYPE", "DRIVE9_LEADER_DISABLED", "DRIVE9_DISABLE_AUTO_EMBEDDING", "DRIVE9_S3_DIR", "DRIVE9_S3_BUCKET", "DRIVE9_LOCAL_DSN", "DRIVE9_LOCAL_MYSQL_DSN", "DRIVE9_META_DSN"}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	applyLocalProviderEnvDefaults("127.0.0.1:19009")
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

	t.Setenv("DRIVE9_TOKEN_SIGNING_KEY", local.DefaultTokenSigningKeyHex)
	t.Setenv("DRIVE9_MASTER_KEY", local.DefaultMasterKeyHex)
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
	applyLocalProviderEnvDefaults("127.0.0.1:9009")
	if err := rejectLocalProviderNonLoopbackDefaults(resolveServerListenAddr("0.0.0.0:9009", os.Getenv("DRIVE9_LISTEN_ADDR"), true)); err == nil {
		t.Fatal("expected reject when positional listen overrides loopback env")
	}

	t.Setenv("DRIVE9_TOKEN_SIGNING_KEY", strings.Repeat("ab", 32))
	t.Setenv("DRIVE9_MASTER_KEY", strings.Repeat("cd", 32))
	if err := rejectLocalProviderNonLoopbackDefaults("0.0.0.0:9009"); err != nil {
		t.Fatalf("overridden keys: %v", err)
	}
}
