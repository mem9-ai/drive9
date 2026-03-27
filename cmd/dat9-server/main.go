// Command dat9-server starts the dat9 HTTP server.
//
// Usage:
//
//	dat9-server [listen-addr]
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/server"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

const (
	defaultListenAddr = ":9009"
	defaultS3Dir      = "s3"
)

func main() {
	if len(os.Args) > 2 {
		usage()
	}

	addr := envOr("DAT9_LISTEN_ADDR", defaultListenAddr)
	if len(os.Args) == 2 {
		addr = os.Args[1]
	}

	metaDSN := os.Getenv("DAT9_META_DSN")
	if metaDSN == "" {
		die(fmt.Errorf("DAT9_META_DSN is required"))
	}

	s3Dir := envOr("DAT9_S3_DIR", defaultS3Dir)

	store, err := meta.Open(metaDSN)
	if err != nil {
		die(fmt.Errorf("open control-plane store: %w", err))
	}
	defer func() { _ = store.Close() }()

	if err := os.MkdirAll(s3Dir, 0o755); err != nil {
		die(fmt.Errorf("create s3 dir: %w", err))
	}
	log.Printf("using local S3 root directory (dir=%s)", s3Dir)

	encryptType := envOr("DAT9_ENCRYPT_TYPE", "local_aes")
	masterHex := os.Getenv("DAT9_MASTER_KEY")
	kmsKey := os.Getenv("DAT9_ENCRYPT_KEY")
	tokenHex := os.Getenv("DAT9_TOKEN_SIGNING_KEY")
	defaultProvider := envOr("DAT9_TENANT_PROVIDER", tenant.ProviderTiDBZero)
	provisioners := map[string]tenant.Provisioner{}
	if p, err := tenant.NewZeroProvisionerFromEnv(); err == nil {
		provisioners[p.ProviderType()] = p
	}
	if p, err := tenant.NewStarterProvisionerFromEnv(); err == nil {
		provisioners[p.ProviderType()] = p
	}
	if p, err := tenant.NewDB9ProvisionerFromEnv(); err == nil {
		provisioners[p.ProviderType()] = p
	}

	var pool *tenant.Pool
	var tokenSecret []byte
	if tokenHex != "" {
		tokenSecret, err = hex.DecodeString(tokenHex)
		if err != nil {
			die(fmt.Errorf("invalid DAT9_TOKEN_SIGNING_KEY: %w", err))
		}
		eKey := masterHex
		eType := encrypt.Type(encryptType)
		if eType == encrypt.TypeKMS {
			eKey = kmsKey
		}
		enc, err := encrypt.New(context.Background(), encrypt.Config{
			Type:   eType,
			Key:    eKey,
			Region: envOr("DAT9_S3_REGION", "us-east-1"),
		})
		if err != nil {
			die(fmt.Errorf("create encryptor: %w", err))
		}

		if _, err := tenant.NormalizeProvider(defaultProvider); err != nil {
			die(err)
		}

		if err := store.DB().Ping(); err != nil {
			die(fmt.Errorf("control-plane db unavailable: %w", err))
		}

		pool = tenant.NewPool(tenant.PoolConfig{S3Dir: s3Dir, PublicURL: publicBaseURL(addr)}, enc)
		defer pool.Close()
	}

	die(server.NewWithConfig(server.Config{
		Meta:            store,
		Pool:            pool,
		Provisioners:    provisioners,
		TokenSecret:     tokenSecret,
		DefaultProvider: defaultProvider,
		S3Dir:           s3Dir,
	}).ListenAndServe(addr))
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9-server [listen-addr]

environment:
  DAT9_LISTEN_ADDR serve listen address (default: :9009)
  DAT9_PUBLIC_URL  externally reachable base URL for presigned URLs (required for remote clients)
  DAT9_META_DSN    control-plane MySQL DSN (required)
  DAT9_ENCRYPT_TYPE local_aes|kms
  DAT9_MASTER_KEY  32-byte hex key for local_aes encryptor
  DAT9_ENCRYPT_KEY KMS key id or alias (required for kms)
  DAT9_TOKEN_SIGNING_KEY  32-byte hex key for JWT API key signing
  DAT9_TENANT_PROVIDER db9|tidb_zero|tidb_cloud_starter (default for provisioning)

  S3 storage (set DAT9_S3_BUCKET to enable AWS S3, otherwise local mock):
  DAT9_S3_BUCKET   S3 bucket name (enables AWS S3 mode)
  DAT9_S3_REGION   AWS region (default: us-east-1)
  DAT9_S3_PREFIX   S3 key prefix (e.g. "tenants/abc/")
  DAT9_S3_ROLE_ARN IAM role ARN to assume via STS (optional)
  DAT9_S3_DIR      local s3 mock directory (default: ./s3, only used without DAT9_S3_BUCKET)
`)
	os.Exit(2)
}

func die(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "dat9-server: %v\n", err)
	os.Exit(1)
}

func publicBaseURL(listenAddr string) string {
	if v := strings.TrimRight(os.Getenv("DAT9_PUBLIC_URL"), "/"); v != "" {
		return v
	}

	// Without DAT9_PUBLIC_URL, only allow explicit loopback addresses.
	// Wildcard or non-loopback addresses would produce unreachable presigned URLs.
	switch {
	case strings.HasPrefix(listenAddr, "127.0.0.1:"),
		strings.HasPrefix(listenAddr, "localhost:"),
		strings.HasPrefix(listenAddr, "[::1]:"):
		return "http://" + listenAddr
	case strings.HasPrefix(listenAddr, "http://"), strings.HasPrefix(listenAddr, "https://"):
		return strings.TrimRight(listenAddr, "/")
	default:
		log.Fatalf("DAT9_PUBLIC_URL is required when listen address is %q (wildcard or non-loopback). "+
			"Set DAT9_PUBLIC_URL to the externally reachable base URL.", listenAddr)
		return "" // unreachable
	}
}
