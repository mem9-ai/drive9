package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestRm(t *testing.T) {
	t.Run("non_recursive_delete", func(t *testing.T) {
		var gotMethod string
		var gotPath string
		var gotRecursive bool

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			gotRecursive = r.URL.Query().Has("recursive")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		if err := Rm(c, []string{"/a.txt"}); err != nil {
			t.Fatalf("Rm(non-recursive) error = %v", err)
		}

		if gotMethod != http.MethodDelete {
			t.Fatalf("Rm(non-recursive) method = %q, want %q", gotMethod, http.MethodDelete)
		}
		if gotPath != "/v1/fs/a.txt" {
			t.Fatalf("Rm(non-recursive) path = %q, want %q", gotPath, "/v1/fs/a.txt")
		}
		if gotRecursive {
			t.Fatal("Rm(non-recursive) unexpectedly sent recursive query")
		}
	})

	t.Run("recursive_delete", func(t *testing.T) {
		var gotPath string
		var gotRecursive bool

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotRecursive = r.URL.Query().Has("recursive")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		if err := Rm(c, []string{"-r", "/dir/"}); err != nil {
			t.Fatalf("Rm(recursive) error = %v", err)
		}

		if gotPath != "/v1/fs/dir/" {
			t.Fatalf("Rm(recursive) path = %q, want %q", gotPath, "/v1/fs/dir/")
		}
		if !gotRecursive {
			t.Fatal("Rm(recursive) did not send recursive query")
		}
	})

	t.Run("recursive_remote_path", func(t *testing.T) {
		var gotPath string
		var gotRecursive bool

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotRecursive = r.URL.Query().Has("recursive")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		if err := Rm(c, []string{"--recursive", ":/dir/"}); err != nil {
			t.Fatalf("Rm(recursive remote) error = %v", err)
		}

		if gotPath != "/v1/fs/dir/" {
			t.Fatalf("Rm(recursive remote) path = %q, want %q", gotPath, "/v1/fs/dir/")
		}
		if !gotRecursive {
			t.Fatal("Rm(recursive remote) did not send recursive query")
		}
	})
}

func TestRmInvalidArgs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing_path",
			args:    nil,
			wantErr: "usage: drive9 rm [-r|--recursive] <path>",
		},
		{
			name:    "unknown_flag",
			args:    []string{"-f", "/dir/"},
			wantErr: `unknown flag "-f"`,
		},
		{
			name:    "extra_path",
			args:    []string{"-r", "/dir/", "/extra"},
			wantErr: "usage: drive9 rm [-r|--recursive] <path>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Rm(c, tt.args)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Rm(%v) error = %q, want substring %q", tt.args, err.Error(), tt.wantErr)
			}
		})
	}
}
