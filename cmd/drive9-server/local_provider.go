package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant/local"
)

const defaultLocalAdminDSN = "root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"

func resolveServerListenAddr(cliAddr, envAddr string, localProvider bool) string {
	if v := strings.TrimSpace(cliAddr); v != "" {
		return v
	}
	if v := strings.TrimSpace(envAddr); v != "" {
		return v
	}
	if localProvider {
		return "127.0.0.1:9009"
	}
	return defaultListenAddr
}

func applyLocalProviderEnvDefaults(listenAddr string) error {
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		listenAddr = "127.0.0.1:9009"
	}
	_ = os.Setenv("DRIVE9_LISTEN_ADDR", listenAddr)
	setEnvIfEmpty("DRIVE9_ENCRYPT_TYPE", "local_aes")
	if err := applyLocalProviderDevKeys(); err != nil {
		return err
	}
	setEnvIfEmpty("DRIVE9_LEADER_DISABLED", "1")
	setEnvIfEmpty("DRIVE9_PUBLIC_URL", "http://"+listenAddr)
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
	return nil
}

// applyLocalProviderDevKeys fills the two key slots the local provider needs.
//
// Environment values win; anything missing comes from a per-installation key
// file that is generated on first start. Nothing is baked into the binary, so a
// copied source tree holds no usable credential, and the file is the single
// place to rotate.
func applyLocalProviderDevKeys() error {
	masterSet := strings.TrimSpace(os.Getenv("DRIVE9_MASTER_KEY")) != ""
	tokenSet := strings.TrimSpace(os.Getenv("DRIVE9_TOKEN_SIGNING_KEY")) != ""
	if masterSet && tokenSet {
		return nil
	}
	keys, path, err := loadOrCreateLocalDevKeys()
	if err != nil {
		return err
	}
	setEnvIfEmpty("DRIVE9_MASTER_KEY", keys.MasterKeyHex)
	setEnvIfEmpty("DRIVE9_TOKEN_SIGNING_KEY", keys.TokenSigningKeyHex)
	_ = os.Setenv(envLocalDevKeysInUse, "1")
	fmt.Fprintf(os.Stderr,
		"drive9: local provider is using generated development keys from %s; delete that file (or set DRIVE9_MASTER_KEY and DRIVE9_TOKEN_SIGNING_KEY) to rotate\n",
		path)
	if age, ageErr := localDevKeysFileAge(path); ageErr == nil && age < time.Minute {
		// First generation. Keys are per installation, so data written by an
		// older release that used its own built-in defaults cannot be read now.
		fmt.Fprint(os.Stderr,
			"drive9: if this installation ran an older drive9-server, its local tenant data was encrypted and signed with that release's built-in dev keys and will not decrypt; re-run with a fresh local tenant, or export the data with the old build first\n")
	}
	return nil
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

func rejectLocalProviderNonLoopbackDefaults(listenAddr string) error {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" {
		addr = strings.TrimSpace(os.Getenv("DRIVE9_LISTEN_ADDR"))
	}
	if localListenIsLoopback(addr) {
		return nil
	}
	if os.Getenv(envLocalDevKeysInUse) == "1" {
		return fmt.Errorf("DRIVE9_TENANT_PROVIDER=local refuses non-loopback listen %q while it is using generated development keys; set DRIVE9_TOKEN_SIGNING_KEY and DRIVE9_MASTER_KEY or bind loopback", addr)
	}
	return nil
}

func localListenIsLoopback(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}
