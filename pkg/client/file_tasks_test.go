//go:build !integration

package client

import (
	"context"
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
		w.Header().Set("Content-Type", "application/json")
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
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"path":"/doc.txt","tasks":[]}`))
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

func TestFileTasksCtxRejectsNonJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("file contents"))
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.FileTasksCtx(context.Background(), "/doc.txt")
	if err == nil || !strings.Contains(err.Error(), "unexpected Content-Type") {
		t.Fatalf("err = %v, want Content-Type error", err)
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
}
