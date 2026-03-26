package cli

import (
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/server"
)

const (
	defaultListenAddr = ":9009"
	defaultBlobDir    = "blobs"
)

// Serve starts the dat9 HTTP server backed by TiDB/MySQL + local blob stand-in
// used in P0.
func Serve(args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("usage: dat9 serve [listen-addr]")
	}

	addr := envOr("DAT9_LISTEN_ADDR", defaultListenAddr)
	if len(args) == 1 {
		addr = args[0]
	}

	mysqlDSN := os.Getenv("DAT9_MYSQL_DSN")
	if mysqlDSN == "" {
		return fmt.Errorf("DAT9_MYSQL_DSN is required")
	}
	blobDir := envOr("DAT9_BLOB_DIR", defaultBlobDir)

	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return fmt.Errorf("create blob dir: %w", err)
	}

	store, err := meta.Open(mysqlDSN)
	if err != nil {
		return fmt.Errorf("open meta store: %w", err)
	}
	defer store.Close()

	b, err := backend.New(store, blobDir)
	if err != nil {
		return fmt.Errorf("create backend: %w", err)
	}

	return server.New(b).ListenAndServe(addr)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
