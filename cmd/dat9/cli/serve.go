package cli

import (
	"fmt"
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

// Serve starts the dat9 HTTP server backed by local SQLite/blob and local-S3
// stand-ins suitable for development and smoke testing.
func Serve(args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("usage: dat9 serve [listen-addr]")
	}

	addr := envOr("DAT9_LISTEN_ADDR", defaultListenAddr)
	if len(args) == 1 {
		addr = args[0]
	}

	dbPath := envOr("DAT9_DB_PATH", defaultDBPath)
	blobDir := envOr("DAT9_BLOB_DIR", defaultBlobDir)
	s3Dir := envOr("DAT9_S3_DIR", defaultS3Dir)

	if err := ensureParentDir(dbPath); err != nil {
		return err
	}
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return fmt.Errorf("create blob dir: %w", err)
	}
	if err := os.MkdirAll(s3Dir, 0o755); err != nil {
		return fmt.Errorf("create s3 dir: %w", err)
	}

	store, err := meta.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open meta store: %w", err)
	}
	defer store.Close()

	s3BaseURL := publicBaseURL(addr) + "/s3"
	s3c, err := s3client.NewLocal(s3Dir, s3BaseURL)
	if err != nil {
		return fmt.Errorf("create local s3 client: %w", err)
	}

	b, err := backend.NewWithS3(store, blobDir, s3c)
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

func publicBaseURL(listenAddr string) string {
	if v := strings.TrimRight(os.Getenv("DAT9_PUBLIC_URL"), "/"); v != "" {
		return v
	}

	switch {
	case strings.HasPrefix(listenAddr, ":"):
		return "http://127.0.0.1" + listenAddr
	case strings.HasPrefix(listenAddr, "0.0.0.0:"):
		return "http://127.0.0.1:" + strings.TrimPrefix(listenAddr, "0.0.0.0:")
	case strings.HasPrefix(listenAddr, "[::]:"):
		return "http://127.0.0.1:" + strings.TrimPrefix(listenAddr, "[::]:")
	case strings.HasPrefix(listenAddr, "http://"), strings.HasPrefix(listenAddr, "https://"):
		return strings.TrimRight(listenAddr, "/")
	default:
		return "http://" + listenAddr
	}
}
