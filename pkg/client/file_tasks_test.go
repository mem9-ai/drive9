//go:build !integration

package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFileTasksCtxDecodesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/doc.txt" || !r.URL.Query().Has("tasks") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set(fileTasksMarkerHeader, "1")
		// Charset-suffixed Content-Type must not affect decoding.
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write([]byte(`{"path":"/doc.txt","tasks":[{"task_type":"embed","status":"failed","last_error":"embedding_provider_auth_rejected: provider rejected the credentials (HTTP 401)"}]}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	got, err := c.FileTasksCtx(context.Background(), "/doc.txt")
	if err != nil {
		t.Fatalf("FileTasksCtx: %v", err)
	}
	if got.Path != "/doc.txt" || len(got.Tasks) != 1 {
		t.Fatalf("got %+v", got)
	}
	if got.Tasks[0].TaskType != "embed" || got.Tasks[0].Status != "failed" {
		t.Fatalf("task = %+v", got.Tasks[0])
	}
	if got.Tasks[0].LastError != "embedding_provider_auth_rejected: provider rejected the credentials (HTTP 401)" {
		t.Fatalf("last_error = %q", got.Tasks[0].LastError)
	}
}

func TestFileTasksCtxEmptyTasksIsEmptySlice(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(fileTasksMarkerHeader, "1")
		w.Header().Set("Content-Type", "application/json")
		// The server omits tasks entirely (or could send null); the client must
		// still return a non-nil empty slice so JSON encodes as [].
		_, _ = w.Write([]byte(`{"path":"/doc.txt"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	got, err := c.FileTasksCtx(context.Background(), "/doc.txt")
	if err != nil {
		t.Fatalf("FileTasksCtx: %v", err)
	}
	if got.Tasks == nil {
		t.Fatal("Tasks must be a non-nil empty slice")
	}
	if len(got.Tasks) != 0 {
		t.Fatalf("got %d tasks, want 0", len(got.Tasks))
	}
}

// A server without ?tasks falls through to a plain read. Even when the
// fall-through body is valid JSON (for example an application/json file), the
// missing marker must be reported as unsupported rather than decoded as an
// empty task list.
func TestFileTasksCtxRequiresServerMarker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"path":"","tasks":[]}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.FileTasksCtx(context.Background(), "/config.json")
	if !errors.Is(err, ErrFileTasksUnsupported) {
		t.Fatalf("err = %v, want ErrFileTasksUnsupported", err)
	}
}

// An old server serving an object-backed file answers with a redirect to a
// presigned URL. The client must not follow it (which would download the whole
// object) and must report the server as unsupported.
func TestFileTasksCtxRejectsRedirectWithoutDownloading(t *testing.T) {
	var objectHits int
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		objectHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"path":"/config.json","tasks":[]}`))
	}))
	defer object.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, object.URL+"/config.json", http.StatusFound)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.FileTasksCtx(context.Background(), "/config.json")
	if !errors.Is(err, ErrFileTasksUnsupported) {
		t.Fatalf("err = %v, want ErrFileTasksUnsupported", err)
	}
	if !strings.Contains(err.Error(), "redirect to 127.0.0.1") {
		t.Fatalf("err = %v, want redirect host in message", err)
	}
	if objectHits != 0 {
		t.Fatalf("object served %d times, want 0 (must not follow redirect)", objectHits)
	}
}

// A same-host redirect (for example an ingress http->https upgrade) is not
// evidence that the server predates ?tasks, so it must not be reported as
// unsupported.
func TestFileTasksCtxSameHostRedirectIsNotUnsupported(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/v1/fs/config.json", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL+"/v2/fs/config.json?tasks=1", http.StatusMovedPermanently)
	})

	c := New(srv.URL, "")
	_, err := c.FileTasksCtx(context.Background(), "/config.json")
	if err == nil {
		t.Fatal("expected an error for a same-host redirect")
	}
	if errors.Is(err, ErrFileTasksUnsupported) {
		t.Fatalf("same-host redirect must not be reported as unsupported: %v", err)
	}
	if !IsFileTasksUnexpectedRedirect(err) {
		t.Fatalf("err = %v, want IsFileTasksUnexpectedRedirect", err)
	}
}

// TestSameHostTreatsDefaultPortsAsEqual pins the origin comparison against the
// spellings httptest cannot produce: a default port on only one side must still
// be the same origin (an ingress that upgrades the scheme and spells :443/:80),
// while a genuinely different explicit port stays cross-origin.
func TestSameHostTreatsDefaultPortsAsEqual(t *testing.T) {
	cases := []struct {
		base string
		host string
		want bool
	}{
		{"https://h.example", "h.example:443", true},
		{"http://h.example", "h.example:80", true},
		{"http://h.example:80", "h.example", true},
		{"https://h.example", "h.example", true},
		{"https://h.example:443", "h.example", true},
		{"https://H.EXAMPLE", "h.example:443", true},
		{"https://h.example:443", "H.Example", true},
		{"https://h.example", "h.example:8443", false},
		{"http://h.example:8080", "h.example", false},
		{"https://h.example", "other.example", false},
	}
	for _, tc := range cases {
		if got := sameHost(tc.base, tc.host); got != tc.want {
			t.Errorf("sameHost(%q, %q) = %t, want %t", tc.base, tc.host, got, tc.want)
		}
	}
}

func TestFileTasksCtxSurfacesStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"path is a directory"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.FileTasksCtx(context.Background(), "/dir/")
	if err == nil || !strings.Contains(err.Error(), "path is a directory") {
		t.Fatalf("err = %v, want directory error", err)
	}
	if errors.Is(err, ErrFileTasksUnsupported) {
		t.Fatalf("status error must not be reported as unsupported: %v", err)
	}
}
