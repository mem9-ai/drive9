// Command dat9-server starts the dat9 HTTP server.
//
// Usage:
//
//	dat9-server [listen-addr]
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/server"
)

const (
	defaultListenAddr = ":9009"
	defaultDBPath     = "dat9.db"
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

	dbPath := envOr("DAT9_DB_PATH", defaultDBPath)
	blobDir := envOr("DAT9_BLOB_DIR", defaultBlobDir)
	s3Dir := envOr("DAT9_S3_DIR", defaultS3Dir)

	if err := ensureParentDir(dbPath); err != nil {
		die(err)
	}
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		die(fmt.Errorf("create blob dir: %w", err))
	}
	if err := os.MkdirAll(s3Dir, 0o755); err != nil {
		die(fmt.Errorf("create s3 dir: %w", err))
	}

	store, err := meta.Open(dbPath)
	if err != nil {
		die(fmt.Errorf("open meta store: %w", err))
	}
	defer store.Close()

	s3BaseURL := publicBaseURL(addr) + "/s3"
	s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
	if err != nil {
		die(fmt.Errorf("create local s3 client: %w", err))
	}

	b, err := backend.NewWithS3(store, blobDir, s3c)
	if err != nil {
		die(fmt.Errorf("create backend: %w", err))
	}

	die(server.New(b).ListenAndServe(addr))
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create db dir: %w", err)
	}
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9-server [listen-addr]

environment:
  DAT9_LISTEN_ADDR serve listen address (default: :9009)
  DAT9_PUBLIC_URL  externally reachable base URL for presigned URLs (required for remote clients)
  DAT9_DB_PATH     sqlite path (default: ./dat9.db)
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
