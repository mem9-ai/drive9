package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/mem9-ai/drive9/pkg/tenant/local"
)

const defaultLocalAdminDSN = "root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"

func applyLocalProviderEnvDefaults() {
	setEnvIfEmpty("DRIVE9_LISTEN_ADDR", "127.0.0.1:9009")
	setEnvIfEmpty("DRIVE9_ENCRYPT_TYPE", "local_aes")
	setEnvIfEmpty("DRIVE9_MASTER_KEY", local.DefaultMasterKeyHex)
	setEnvIfEmpty("DRIVE9_TOKEN_SIGNING_KEY", local.DefaultTokenSigningKeyHex)
	setEnvIfEmpty("DRIVE9_LEADER_DISABLED", "1")
	setEnvIfEmpty("DRIVE9_PUBLIC_URL", "http://127.0.0.1:9009")
	setEnvIfEmpty("DRIVE9_DISABLE_AUTO_EMBEDDING", "true")
	if strings.TrimSpace(os.Getenv("DRIVE9_S3_BUCKET")) == "" {
		setEnvIfEmpty("DRIVE9_S3_DIR", filepath.Join(os.TempDir(), "drive9-local-s3"))
	}
	if localAdminDSN() == "" {
		setEnvIfEmpty(local.EnvLegacyDSN, defaultLocalAdminDSN)
	}
	dsn := localAdminDSN()
	if dsn != "" {
		setEnvIfEmpty("DRIVE9_META_DSN", dsn)
		setEnvIfEmpty(local.EnvAdminDSN, dsn)
		setEnvIfEmpty(local.EnvLegacyDSN, dsn)
	}
}

func localAdminDSN() string {
	for _, key := range []string{local.EnvAdminDSN, local.EnvLegacyDSN, "DRIVE9_META_DSN"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func setEnvIfEmpty(key, value string) {
	if strings.TrimSpace(os.Getenv(key)) == "" {
		_ = os.Setenv(key, value)
	}
}
