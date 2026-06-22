package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestHardlink(t *testing.T) {
	t.Run("creates_link", func(t *testing.T) {
		var gotMethod string
		var gotPath string
		var gotHardlink string
		var gotSource string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			gotHardlink = r.URL.Query().Get("hardlink")
			gotSource = r.Header.Get("X-Dat9-Hardlink-Source")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		if err := Hardlink(c, []string{"/target.txt", "/link"}); err != nil {
			t.Fatalf("Hardlink error = %v", err)
		}
		if gotMethod != http.MethodPost {
			t.Fatalf("method = %q, want POST", gotMethod)
		}
		if gotPath != "/v1/fs/link" {
			t.Fatalf("path = %q, want /v1/fs/link", gotPath)
		}
		if gotHardlink != "1" {
			t.Fatalf("hardlink query = %q, want 1", gotHardlink)
		}
		if gotSource != "/target.txt" {
			t.Fatalf("source = %q, want /target.txt", gotSource)
		}
	})

	t.Run("remote_path_prefix", func(t *testing.T) {
		var gotPath string
		var gotHardlink string
		var gotSource string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotHardlink = r.URL.Query().Get("hardlink")
			gotSource = r.Header.Get("X-Dat9-Hardlink-Source")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		if err := Hardlink(c, []string{":/target.txt", ":/workspace/link"}); err != nil {
			t.Fatalf("Hardlink(remote) error = %v", err)
		}
		if gotPath != "/v1/fs/workspace/link" {
			t.Fatalf("path = %q, want /v1/fs/workspace/link", gotPath)
		}
		if gotHardlink != "1" {
			t.Fatalf("hardlink query = %q, want 1", gotHardlink)
		}
		if gotSource != "/target.txt" {
			t.Fatalf("source = %q, want /target.txt", gotSource)
		}
	})
}

func TestHardlinkUsageErrors(t *testing.T) {
	c := client.New("http://example.invalid", "")

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"no_args", nil, "usage: drive9 fs hardlink"},
		{"one_arg", []string{"/target"}, "usage: drive9 fs hardlink"},
		{"too_many_args", []string{"/target", "/link", "/extra"}, "usage: drive9 fs hardlink"},
		{"cross_context", []string{"ctx:/target", "other:/link"}, "cross-context hardlink not supported"},
		{"named_source_without_named_link", []string{"ctx:/target", "/link"}, "cross-context hardlink not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Hardlink(c, tt.args)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}
