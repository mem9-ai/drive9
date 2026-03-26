// Command dat9-server starts the dat9 HTTP server.
//
// Usage:
//
//	dat9-server [listen-addr]
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/server"
)

const (
	defaultListenAddr = ":9009"
	defaultDBPath     = "dat9.db"
	defaultBlobDir    = "blobs"
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

	if err := ensureParentDir(dbPath); err != nil {
		die(err)
	}
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		die(fmt.Errorf("create blob dir: %w", err))
	}

	store, err := meta.Open(dbPath)
	if err != nil {
		die(fmt.Errorf("open meta store: %w", err))
	}
	defer store.Close()

	b, err := backend.New(store, blobDir)
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
  DAT9_DB_PATH     sqlite path (default: ./dat9.db)
  DAT9_BLOB_DIR    blob directory (default: ./blobs)
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
