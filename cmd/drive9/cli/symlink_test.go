package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestSymlink(t *testing.T) {
	t.Run("creates_link", func(t *testing.T) {
		var gotMethod string
		var gotPath string
		var gotSymlink string
		var gotTarget string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			gotSymlink = r.URL.Query().Get("symlink")
			var req struct {
				Target string `json:"target"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode request body: %v", err)
			}
			gotTarget = req.Target
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		if err := Symlink(c, []string{"../target.txt", "/link"}); err != nil {
			t.Fatalf("Symlink error = %v", err)
		}
		if gotMethod != http.MethodPost {
			t.Fatalf("method = %q, want POST", gotMethod)
		}
		if gotPath != "/v1/fs/link" {
			t.Fatalf("path = %q, want /v1/fs/link", gotPath)
		}
		if gotSymlink != "1" {
			t.Fatalf("symlink query = %q, want 1", gotSymlink)
		}
		if gotTarget != "../target.txt" {
			t.Fatalf("target = %q, want ../target.txt", gotTarget)
		}
	})

	t.Run("remote_path_prefix", func(t *testing.T) {
		var gotPath string
		var gotSymlink string
		var gotTarget string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotSymlink = r.URL.Query().Get("symlink")
			var req struct {
				Target string `json:"target"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode request body: %v", err)
			}
			gotTarget = req.Target
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		if err := Symlink(c, []string{":/target.txt", ":/workspace/link"}); err != nil {
			t.Fatalf("Symlink(remote) error = %v", err)
		}
		if gotPath != "/v1/fs/workspace/link" {
			t.Fatalf("path = %q, want /v1/fs/workspace/link", gotPath)
		}
		if gotSymlink != "1" {
			t.Fatalf("symlink query = %q, want 1", gotSymlink)
		}
		if gotTarget != "/target.txt" {
			t.Fatalf("target = %q, want /target.txt", gotTarget)
		}
	})
}

func TestSymlinkTargetForCLI(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		linkRP     RemotePath
		linkRemote bool
		want       string
		wantErr    string
	}{
		{
			name:   "relative_literal",
			target: "../target",
			want:   "../target",
		},
		{
			name:       "current_context_remote_shorthand",
			target:     ":/target",
			linkRemote: true,
			linkRP:     RemotePath{Path: "/link"},
			want:       "/target",
		},
		{
			name:       "same_named_context",
			target:     "ctx:/target",
			linkRemote: true,
			linkRP:     RemotePath{Context: "ctx", Path: "/link"},
			want:       "/target",
		},
		{
			name:       "cross_named_context",
			target:     "other:/target",
			linkRemote: true,
			linkRP:     RemotePath{Context: "ctx", Path: "/link"},
			wantErr:    "cross-context symlink not supported",
		},
		{
			name:    "named_target_without_named_link",
			target:  "ctx:/target",
			wantErr: "requires link path to use the same context prefix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := symlinkTargetForCLI(tt.target, tt.linkRP, tt.linkRemote)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("target = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSymlinkUsageErrors(t *testing.T) {
	c := client.New("http://example.invalid", "")

	tests := []struct {
		name string
		args []string
	}{
		{"no_args", nil},
		{"one_arg", []string{"/target"}},
		{"too_many_args", []string{"/target", "/link", "/extra"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Symlink(c, tt.args)
			if err == nil {
				t.Fatal("expected usage error")
			}
			if !strings.Contains(err.Error(), "usage: drive9 fs symlink") {
				t.Fatalf("error = %q, want usage message", err.Error())
			}
		})
	}
}
