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
