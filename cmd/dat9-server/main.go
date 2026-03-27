// Command dat9-server starts the dat9 HTTP server.
//
// Usage:
//
//	dat9-server [listen-addr]
package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/server"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

const (
	defaultListenAddr = ":9009"
	defaultBlobDir    = "blobs"
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

	blobDir := envOr("DAT9_BLOB_DIR", defaultBlobDir)
	s3Dir := envOr("DAT9_S3_DIR", defaultS3Dir)

	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		die(fmt.Errorf("create blob dir: %w", err))
	}
	if err := os.MkdirAll(s3Dir, 0o755); err != nil {
		die(fmt.Errorf("create s3 dir: %w", err))
	}

	pubURL := publicBaseURL(addr)
	s3BaseURL := pubURL + "/s3"

	cfg := server.Config{}

	// Multi-tenant mode: control plane DSN + master key + admin key
	cpDSN := os.Getenv("DAT9_CONTROL_PLANE_DSN")
	masterKeyHex := os.Getenv("DAT9_MASTER_KEY")
	adminKey := os.Getenv("DAT9_ADMIN_KEY")

	if cpDSN != "" && masterKeyHex != "" {
		// Multi-tenant mode
		masterKey, err := hex.DecodeString(masterKeyHex)
		if err != nil {
			die(fmt.Errorf("DAT9_MASTER_KEY must be 64 hex chars (32 bytes): %w", err))
		}
		enc, err := tenant.NewEncryptor(masterKey)
		if err != nil {
			die(fmt.Errorf("create encryptor: %w", err))
		}
		tenants, err := tenant.OpenStore(cpDSN, enc)
		if err != nil {
			die(fmt.Errorf("open control plane: %w", err))
		}
		defer tenants.Close()

		pool := tenant.NewPool(tenant.PoolConfig{
			MaxTenants: 128,
			BlobDir:    blobDir,
			S3Dir:      s3Dir,
			PublicURL:  pubURL,
		}, enc)
		defer pool.Close()

		cfg.Tenants = tenants
		cfg.Pool = pool
		cfg.AdminKey = adminKey

		// Set up provisioner if TiDB Cloud credentials are available
		provisioner, err := tenant.NewTiDBStarterFromEnv()
		if err != nil {
			log.Printf("tidb starter not configured: %v (provisioning disabled)", err)
		} else {
			cfg.Provisioner = provisioner
		}

		log.Printf("multi-tenant mode enabled (admin key: %v, provisioner: %v)",
			adminKey != "", cfg.Provisioner != nil)
	} else {
		// Local dev mode: single backend
		mysqlDSN := os.Getenv("DAT9_MYSQL_DSN")
		if mysqlDSN == "" {
			die(fmt.Errorf("DAT9_MYSQL_DSN is required in local dev mode (or set DAT9_CONTROL_PLANE_DSN + DAT9_MASTER_KEY for multi-tenant)"))
		}
		store, err := meta.Open(mysqlDSN)
		if err != nil {
			die(fmt.Errorf("open meta store: %w", err))
		}
		defer store.Close()

		s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
		if err != nil {
			die(fmt.Errorf("create local s3 client: %w", err))
		}

		b, err := backend.NewWithS3(store, blobDir, s3c)
		if err != nil {
			die(fmt.Errorf("create backend: %w", err))
		}
		cfg.Backend = b
	}

	die(server.New(cfg).ListenAndServe(addr))
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9-server [listen-addr]

environment (local dev mode):
  DAT9_MYSQL_DSN   TiDB/MySQL DSN (required)

environment (multi-tenant mode):
  DAT9_CONTROL_PLANE_DSN  control plane MySQL DSN (required)
  DAT9_MASTER_KEY         32-byte hex master key for password encryption (required)
  DAT9_ADMIN_KEY          admin key for /v1/provision (required for provisioning)
  DAT9_TIDBCLOUD_API_KEY     TiDB Cloud API public key
  DAT9_TIDBCLOUD_API_SECRET  TiDB Cloud API private key
  DAT9_TIDBCLOUD_POOL_ID     TiDB Cloud cluster pool ID

environment (common):
  DAT9_LISTEN_ADDR serve listen address (default: :9009)
  DAT9_PUBLIC_URL  externally reachable base URL for presigned URLs
  DAT9_BLOB_DIR    blob directory (default: ./blobs)
  DAT9_S3_DIR      s3 directory (default: ./s3)
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
