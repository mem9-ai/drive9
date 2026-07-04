package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestRunUsesDrive9GoSDK(t *testing.T) {
	const apiKey = "test-api-key"
	const root = "/sdk-test/"
	const remoteFile = root + "hello.txt"

	files := map[string][]byte{}
	var sawWrite bool

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("status method = %s, want GET", r.Method)
		}
		writeJSON(t, w, map[string]any{
			"status":           "ok",
			"inline_threshold": 50_000,
			"max_upload_bytes": 1 << 30,
		})
	})
	mux.HandleFunc("/v1/fs:batch-stat", func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r, apiKey)
		if r.Method != http.MethodPost {
			t.Fatalf("batch-stat method = %s, want POST", r.Method)
		}
		var req struct {
			Paths []string `json:"paths"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode batch stat request: %v", err)
		}
		if len(req.Paths) != 1 || req.Paths[0] != remoteFile {
			t.Fatalf("batch paths = %v, want [%s]", req.Paths, remoteFile)
		}
		writeJSON(t, w, map[string]any{
			"results": []map[string]any{
				{"path": remoteFile, "status": 200, "size": len(files[remoteFile]), "revision": 7},
			},
		})
	})
	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r, apiKey)
		remotePath := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("mkdir"):
			if remotePath != root {
				t.Fatalf("mkdir path = %q, want %q", remotePath, root)
			}
			writeJSON(t, w, map[string]any{"ok": true})
		case r.Method == http.MethodPut:
			if remotePath != remoteFile {
				t.Fatalf("write path = %q, want %q", remotePath, remoteFile)
			}
			if got := r.Header.Get("X-Dat9-Description"); got == "" {
				t.Fatal("missing X-Dat9-Description")
			}
			gotTags := r.Header.Values("X-Dat9-Tag")
			if len(gotTags) != 2 || gotTags[0] != "example=go-sdk" || gotTags[1] != "kind=smoke" {
				t.Fatalf("X-Dat9-Tag = %v, want [example=go-sdk kind=smoke]", gotTags)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read write body: %v", err)
			}
			files[remotePath] = body
			sawWrite = true
			writeJSON(t, w, map[string]any{"revision": 7})
		case r.Method == http.MethodGet && r.URL.Query().Has("stat"):
			data, ok := files[remotePath]
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeJSON(t, w, map[string]any{
				"size":          len(data),
				"isdir":         false,
				"revision":      7,
				"content_type":  "text/plain",
				"semantic_text": "drive9 Go SDK smoke payload",
				"tags":          map[string]string{"example": "go-sdk", "kind": "smoke"},
			})
		case r.Method == http.MethodGet && r.URL.Query().Has("list"):
			// ArchiveDir normalizes the root to drop a trailing slash, so the
			// list handler must accept both "/sdk-test/" and "/sdk-test".
			if remotePath != root && remotePath != strings.TrimSuffix(root, "/") {
				t.Fatalf("list path = %q, want %q", remotePath, root)
			}
			writeJSON(t, w, map[string]any{
				"entries": []map[string]any{
					{"name": "hello.txt", "size": len(files[remoteFile]), "isDir": false},
				},
			})
		case r.Method == http.MethodGet && r.URL.Query().Has("grep"):
			if remotePath != root {
				t.Fatalf("grep path = %q, want %q", remotePath, root)
			}
			if got := r.URL.Query().Get("grep"); got != "drive9 sdk" {
				t.Fatalf("grep query = %q, want drive9 sdk", got)
			}
			writeJSON(t, w, []map[string]any{
				{"path": remoteFile, "name": "hello.txt", "size_bytes": len(files[remoteFile])},
			})
		case r.Method == http.MethodGet:
			data, ok := files[remotePath]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete && r.URL.Query().Has("recursive"):
			if remotePath != root {
				t.Fatalf("delete path = %q, want %q", remotePath, root)
			}
			delete(files, remoteFile)
			writeJSON(t, w, map[string]any{"ok": true})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var out bytes.Buffer
	if err := run(ctx, client.New(ts.URL, apiKey), root, &out); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !sawWrite {
		t.Fatal("SDK smoke did not write a file")
	}
	if got := out.String(); !strings.Contains(got, "upload_mode: direct_put") ||
		!strings.Contains(got, "batch_status: 200") ||
		!strings.Contains(got, "search_results: 1") ||
		!strings.Contains(got, "archive_entries: 2") {
		t.Fatalf("unexpected output:\n%s", got)
	}
}

func TestLoadCredentialsFromConfigUsesCurrentContext(t *testing.T) {
	configPath := writeConfig(t, `{
	  "server": "https://fallback.example",
	  "current_context": "prod",
	  "contexts": {
	    "prod": {
	      "type": "owner",
	      "server": "https://prod.example",
	      "api_key": "prod-key"
	    },
	    "other": {
	      "type": "fs_scoped",
	      "server": "https://other.example",
	      "api_key": "other-key"
	    }
	  }
	}`)

	creds, err := loadCredentialsFromConfig(configPath)
	if err != nil {
		t.Fatalf("loadCredentialsFromConfig error = %v", err)
	}
	if creds.server != "https://prod.example" || creds.apiKey != "prod-key" {
		t.Fatalf("creds = server %q api %q, want prod context", creds.server, creds.apiKey)
	}
}

func TestLoadCredentialsFromConfigFallsBackToFirstUsableContext(t *testing.T) {
	configPath := writeConfig(t, `{
	  "server": "https://fallback.example",
	  "current_context": "delegated",
	  "contexts": {
	    "delegated": {
	      "type": "delegated",
	      "server": "https://delegated.example",
	      "token": "jwt"
	    },
	    "usable": {
	      "type": "fs_scoped",
	      "api_key": "scoped-key"
	    }
	  }
	}`)

	creds, err := loadCredentialsFromConfig(configPath)
	if err != nil {
		t.Fatalf("loadCredentialsFromConfig error = %v", err)
	}
	if creds.server != "https://fallback.example" || creds.apiKey != "scoped-key" {
		t.Fatalf("creds = server %q api %q, want fallback server and scoped key", creds.server, creds.apiKey)
	}
}

func TestResolveCredentialsEnvOverridesConfig(t *testing.T) {
	configPath := writeConfig(t, `{
	  "server": "https://config.example",
	  "current_context": "prod",
	  "contexts": {
	    "prod": {
	      "type": "owner",
	      "api_key": "config-key"
	    }
	  }
	}`)

	t.Setenv("DRIVE9_CONFIG", configPath)
	t.Setenv("DRIVE9_SERVER", "https://env.example")
	t.Setenv("DRIVE9_API_KEY", "env-key")

	creds, err := resolveCredentials()
	if err != nil {
		t.Fatalf("resolveCredentials error = %v", err)
	}
	if creds.server != "https://env.example" || creds.apiKey != "env-key" {
		t.Fatalf("creds = server %q api %q, want env values", creds.server, creds.apiKey)
	}
}

func requireAuth(t *testing.T, r *http.Request, apiKey string) {
	t.Helper()
	if got, want := r.Header.Get("Authorization"), "Bearer "+apiKey; got != want {
		t.Fatalf("Authorization = %q, want %q", got, want)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
