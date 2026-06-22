// Package main is a minimal drive9 Go SDK integration smoke test.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	root, err := normalizeRoot(os.Getenv("DRIVE9_SDK_ROOT"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := client.New(creds.server, creds.apiKey)
	if err := run(ctx, c, root, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c *client.Client, root string, out io.Writer) error {
	c.Warm(ctx)

	if err := c.MkdirCtx(ctx, root, 0o755); err != nil {
		return fmt.Errorf("create scratch directory %s: %w", root, err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.RemoveAllCtx(cleanupCtx, root)
	}()

	body := []byte("drive9 Go SDK smoke payload\n")
	remoteFile := root + "hello.txt"
	summary, err := c.WriteStreamWithSummary(
		ctx,
		remoteFile,
		bytes.NewReader(body),
		int64(len(body)),
		nil,
		client.WithTags(map[string]string{"example": "go-sdk", "kind": "smoke"}),
		client.WithDescription("drive9 Go SDK basic integration smoke test"),
	)
	if err != nil {
		return fmt.Errorf("upload %s: %w", remoteFile, err)
	}

	got, err := c.ReadCtx(ctx, remoteFile)
	if err != nil {
		return fmt.Errorf("read %s: %w", remoteFile, err)
	}
	if !bytes.Equal(got, body) {
		return fmt.Errorf("readback mismatch for %s: got %q", remoteFile, got)
	}

	meta, err := c.StatMetadataCompatCtx(ctx, remoteFile)
	if err != nil {
		return fmt.Errorf("stat %s: %w", remoteFile, err)
	}
	entries, err := c.ListCtx(ctx, root)
	if err != nil {
		return fmt.Errorf("list %s: %w", root, err)
	}
	batch, err := c.BatchStatCtx(ctx, []string{remoteFile})
	if err != nil {
		return fmt.Errorf("batch stat %s: %w", remoteFile, err)
	}
	if len(batch) != 1 {
		return fmt.Errorf("batch stat %s: got %d results, want 1", remoteFile, len(batch))
	}
	results, err := c.Grep("drive9 sdk", root, 10)
	if err != nil {
		return fmt.Errorf("grep %s: %w", root, err)
	}

	if _, err := io.WriteString(out, fmt.Sprintf(
		"root: %s\nfile: %s\nupload_mode: %s\nrevision: %d\nsize: %d\nentries: %d\nbatch_status: %d\nsearch_results: %d\n",
		root,
		remoteFile,
		summary.Mode,
		meta.Revision,
		meta.Size,
		len(entries),
		batch[0].Status,
		len(results),
	)); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	return nil
}

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

func normalizeRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		root = fmt.Sprintf("/sdk-go-basic-%d", time.Now().UnixNano())
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	root = strings.TrimRight(root, "/")
	if root == "" {
		return "", fmt.Errorf("DRIVE9_SDK_ROOT must not be /")
	}
	return root + "/", nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
