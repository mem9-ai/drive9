package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

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

func applyLocalProviderEnvDefaults(listenAddr string) {
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		listenAddr = "127.0.0.1:9009"
	}
	_ = os.Setenv("DRIVE9_LISTEN_ADDR", listenAddr)
	setEnvIfEmpty("DRIVE9_ENCRYPT_TYPE", "local_aes")
	setEnvIfEmpty("DRIVE9_MASTER_KEY", local.DefaultMasterKeyHex)
	setEnvIfEmpty("DRIVE9_TOKEN_SIGNING_KEY", local.DefaultTokenSigningKeyHex)
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
	tok := strings.TrimSpace(os.Getenv("DRIVE9_TOKEN_SIGNING_KEY"))
	master := strings.TrimSpace(os.Getenv("DRIVE9_MASTER_KEY"))
	if tok == local.DefaultTokenSigningKeyHex || master == local.DefaultMasterKeyHex {
		return fmt.Errorf("DRIVE9_TENANT_PROVIDER=local refuses non-loopback listen %q with default token/master keys; set DRIVE9_TOKEN_SIGNING_KEY and DRIVE9_MASTER_KEY or bind loopback", addr)
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
