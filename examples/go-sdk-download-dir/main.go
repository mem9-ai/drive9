// Package main is a runnable demo of the Go SDK DownloadDir capability.
//
// It connects to a drive9 server using DRIVE9_SERVER/DRIVE9_BASE +
// DRIVE9_API_KEY env vars (or ~/.drive9/config), builds a small remote
// directory tree, downloads it to a local temp dir via DownloadDirCtx,
// verifies the round-trip, and cleans up both remote and local paths.
//
// Run it with:
//
//	go run ./examples/go-sdk-download-dir
//
// Set DRIVE9_SERVER and DRIVE9_API_KEY, or configure ~/.drive9/config.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func main() {
	creds, err := resolveCredentials()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	root := fmt.Sprintf("/sdk-download-dir-demo-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	c := client.New(creds.server, creds.apiKey)
	if err := run(ctx, c, root); err != nil {
		fmt.Fprintf(os.Stderr, "demo failed: %v\n", err)
		// Best-effort cleanup even on failure.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.RemoveAllCtx(cleanupCtx, root+"/")
		os.Exit(1)
	}
}

func run(ctx context.Context, c *client.Client, root string) error {
	c.Warm(ctx)

	// 1. Build a remote directory tree:
	//   <root>/
	//     top.txt       "top content"
	//     sub/
	//       nested.txt  "nested content"
	//       deep/
	//         leaf.txt  "leaf content"
	//     empty/
	fmt.Println("Building remote tree at", root+"/")
	must(ctx, "mkdir root", func() error { return c.MkdirCtx(ctx, root, 0o755) })
	must(ctx, "mkdir sub", func() error { return c.MkdirCtx(ctx, root+"/sub", 0o755) })
	must(ctx, "mkdir sub/deep", func() error { return c.MkdirCtx(ctx, root+"/sub/deep", 0o755) })
	must(ctx, "mkdir empty", func() error { return c.MkdirCtx(ctx, root+"/empty", 0o755) })
	must(ctx, "write top.txt", func() error { return c.WriteCtx(ctx, root+"/top.txt", []byte("top content")) })
	must(ctx, "write nested.txt", func() error { return c.WriteCtx(ctx, root+"/sub/nested.txt", []byte("nested content")) })
	must(ctx, "write leaf.txt", func() error { return c.WriteCtx(ctx, root+"/sub/deep/leaf.txt", []byte("leaf content")) })

	// 2. Download the entire remote tree to a local temp directory.
	localDir, err := os.MkdirTemp("", "drive9-download-dir-demo-*")
	if err != nil {
		return fmt.Errorf("create local temp dir: %w", err)
	}
	fmt.Println("Downloading", root+"/ →", localDir)
	if err := c.DownloadDirCtx(ctx, root, localDir); err != nil {
		return fmt.Errorf("DownloadDirCtx: %w", err)
	}

	// 3. Verify the round-trip.
	wantFiles := map[string]string{
		filepath.Join(localDir, "top.txt"):                 "top content",
		filepath.Join(localDir, "sub", "nested.txt"):       "nested content",
		filepath.Join(localDir, "sub", "deep", "leaf.txt"): "leaf content",
	}
	for localPath, want := range wantFiles {
		got, err := os.ReadFile(localPath)
		if err != nil {
			return fmt.Errorf("read %s: %w", localPath, err)
		}
		if string(got) != want {
			return fmt.Errorf("%s = %q, want %q", localPath, got, want)
		}
		fmt.Printf("  ✓ %s → %q\n", localPath, got)
	}
	wantDirs := []string{
		localDir,
		filepath.Join(localDir, "sub"),
		filepath.Join(localDir, "sub", "deep"),
		filepath.Join(localDir, "empty"),
	}
	for _, d := range wantDirs {
		info, err := os.Stat(d)
		if err != nil {
			return fmt.Errorf("stat dir %s: %w", d, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("%s is not a directory", d)
		}
	}
	fmt.Println("  ✓ empty dir preserved:", filepath.Join(localDir, "empty"))

	// 4. Clean up remote and local.
	fmt.Println("Cleaning up remote tree", root+"/")
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.RemoveAllCtx(cleanupCtx, root+"/"); err != nil {
		return fmt.Errorf("remove remote %s: %w", root, err)
	}
	fmt.Println("Cleaning up local dir", localDir)
	if err := os.RemoveAll(localDir); err != nil {
		return fmt.Errorf("remove local %s: %w", localDir, err)
	}

	fmt.Println("Done — DownloadDir demo succeeded!")
	return nil
}

func must(ctx context.Context, desc string, fn func() error) {
	if err := fn(); err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ %s: %v\n", desc, err)
		os.Exit(1)
	}
}

// ── credential resolution (mirrors examples/go-sdk-basic) ─────────────

type exampleCredentials struct {
	server string
	apiKey string
}

type drive9Config struct {
	Server         string                   `json:"server"`
	CurrentContext string                   `json:"current_context"`
	Contexts       map[string]drive9Context `json:"contexts"`
}

type drive9Context struct {
	Type   string `json:"type"`
	Server string `json:"server"`
	APIKey string `json:"api_key"`
}

func resolveCredentials() (exampleCredentials, error) {
	server := firstNonEmpty(os.Getenv("DRIVE9_SERVER"), os.Getenv("DRIVE9_BASE"))
	apiKey := os.Getenv("DRIVE9_API_KEY")
	if server != "" && apiKey != "" {
		return exampleCredentials{server: server, apiKey: apiKey}, nil
	}

	configPath, err := defaultConfigPath()
	if err != nil {
		return exampleCredentials{}, err
	}
	cfgCreds, err := loadCredentialsFromConfig(configPath)
	if err != nil {
		return exampleCredentials{}, fmt.Errorf("set DRIVE9_SERVER/DRIVE9_BASE and DRIVE9_API_KEY, or configure %s: %w", configPath, err)
	}
	if server != "" {
		cfgCreds.server = server
	}
	if apiKey != "" {
		cfgCreds.apiKey = apiKey
	}
	if cfgCreds.server == "" || cfgCreds.apiKey == "" {
		return exampleCredentials{}, fmt.Errorf("set DRIVE9_SERVER/DRIVE9_BASE and DRIVE9_API_KEY, or use an owner/fs_scoped context in %s", configPath)
	}
	return cfgCreds, nil
}

func defaultConfigPath() (string, error) {
	if override := strings.TrimSpace(os.Getenv("DRIVE9_CONFIG")); override != "" {
		return override, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".drive9", "config"), nil
}

func loadCredentialsFromConfig(path string) (exampleCredentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return exampleCredentials{}, err
	}
	var cfg drive9Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return exampleCredentials{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Contexts == nil {
		return exampleCredentials{}, fmt.Errorf("no contexts")
	}
	if cfg.CurrentContext != "" {
		if creds, ok := credentialsFromContext(cfg, cfg.CurrentContext); ok {
			return creds, nil
		}
	}
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if creds, ok := credentialsFromContext(cfg, name); ok {
			return creds, nil
		}
	}
	return exampleCredentials{}, fmt.Errorf("no owner/fs_scoped context with an API key")
}

func credentialsFromContext(cfg drive9Config, name string) (exampleCredentials, bool) {
	entry, ok := cfg.Contexts[name]
	if !ok {
		return exampleCredentials{}, false
	}
	if entry.Type != "owner" && entry.Type != "fs_scoped" {
		return exampleCredentials{}, false
	}
	if strings.TrimSpace(entry.APIKey) == "" {
		return exampleCredentials{}, false
	}
	server := strings.TrimSpace(entry.Server)
	if server == "" {
		server = strings.TrimSpace(cfg.Server)
	}
	if server == "" {
		return exampleCredentials{}, false
	}
	return exampleCredentials{server: server, apiKey: strings.TrimSpace(entry.APIKey)}, true
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
