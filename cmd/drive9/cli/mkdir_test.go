package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestMkdir(t *testing.T) {
	t.Run("creates_directory", func(t *testing.T) {
		var gotMethod string
		var gotPath string
		var gotMkdir bool

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			gotMkdir = r.URL.Query().Has("mkdir")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		out, err := captureStdoutE(t, func() error { return Mkdir(c, []string{"/data"}) })
		if err != nil {
			t.Fatalf("Mkdir error = %v", err)
		}
		if gotMethod != http.MethodPost {
			t.Fatalf("method = %q, want POST", gotMethod)
		}
		if gotPath != "/v1/fs/data" {
			t.Fatalf("path = %q, want /v1/fs/data", gotPath)
		}
		if !gotMkdir {
			t.Fatal("missing ?mkdir query parameter")
		}
		if !strings.Contains(out, "created /data") {
			t.Fatalf("output = %q, want 'created /data'", out)
		}
	})

	t.Run("remote_path_prefix", func(t *testing.T) {
		var gotPath string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		_, err := captureStdoutE(t, func() error { return Mkdir(c, []string{":/workspace/logs"}) })
		if err != nil {
			t.Fatalf("Mkdir(remote) error = %v", err)
		}
		if gotPath != "/v1/fs/workspace/logs" {
			t.Fatalf("path = %q, want /v1/fs/workspace/logs", gotPath)
		}
	})

	t.Run("bare_path", func(t *testing.T) {
		var gotPath string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		_, err := captureStdoutE(t, func() error { return Mkdir(c, []string{"/a/b/c"}) })
		if err != nil {
			t.Fatalf("Mkdir(bare) error = %v", err)
		}
		if gotPath != "/v1/fs/a/b/c" {
			t.Fatalf("path = %q, want /v1/fs/a/b/c", gotPath)
		}
	})

	t.Run("server_error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal error"}`))
		}))
		defer srv.Close()

		c := client.New(srv.URL, "")
		err := Mkdir(c, []string{"/fail"})
		if err == nil {
			t.Fatal("expected error on 500")
		}
	})
}

func TestMkdirUsageErrors(t *testing.T) {
	c := client.New("http://example.invalid", "")

	tests := []struct {
		name string
		args []string
	}{
		{"no_args", nil},
		{"too_many_args", []string{"/a", "/b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Mkdir(c, tt.args)
			if err == nil {
				t.Fatal("expected usage error")
			}
			if !strings.Contains(err.Error(), "usage: drive9 fs mkdir") {
				t.Fatalf("error = %q, want usage message", err.Error())
			}
		})
	}
}
