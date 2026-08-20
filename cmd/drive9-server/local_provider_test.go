package main

import (
	"os"
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

	applyLocalProviderEnvDefaults()

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
}
