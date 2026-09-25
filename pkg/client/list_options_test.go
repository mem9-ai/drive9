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

func TestListWithOptionsCtxAcceptsCompleteBoundedResponse(t *testing.T) {
	body := `{"entries":[{"name":"a","size":1,"revision":2}]}`
	server := newListResponseServer(t, body)
	defer server.Close()

	result, err := New(server.URL, "").ListWithOptionsCtx(context.Background(), "/dir", ListOptions{
		MaxResponseBytes: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("ListWithOptionsCtx error = %v", err)
	}
	if result.ResponseBytes != int64(len(body)) {
		t.Fatalf("response bytes = %d, want %d", result.ResponseBytes, len(body))
	}
	if len(result.Entries) != 1 || result.Entries[0].Name != "a" {
		t.Fatalf("entries = %+v, want a", result.Entries)
	}
}

func TestListWithOptionsCtxRejectsOversizedResponse(t *testing.T) {
	body := `{"entries":[{"name":"oversized","size":1}]}`
	server := newListResponseServer(t, body)
	defer server.Close()

	_, err := New(server.URL, "").ListWithOptionsCtx(context.Background(), "/dir", ListOptions{
		MaxResponseBytes: int64(len(body) - 1),
	})
	if !errors.Is(err, ErrListResponseTooLarge) {
		t.Fatalf("ListWithOptionsCtx error = %v, want ErrListResponseTooLarge", err)
	}
}

func TestListWithOptionsCtxRejectsTruncatedResponse(t *testing.T) {
	body := `{"entries":[{"name":"broken"}`
	server := newListResponseServer(t, body)
	defer server.Close()

	_, err := New(server.URL, "").ListWithOptionsCtx(context.Background(), "/dir", ListOptions{
		MaxResponseBytes: 1024,
	})
	if err == nil || errors.Is(err, ErrListResponseTooLarge) {
		t.Fatalf("ListWithOptionsCtx error = %v, want non-size decode error", err)
	}
}

func TestListWithOptionsCtxBoundsErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(strings.Repeat("x", 2048)))
	}))
	defer server.Close()

	_, err := New(server.URL, "").ListWithOptionsCtx(context.Background(), "/dir", ListOptions{MaxResponseBytes: 1024})
	if !errors.Is(err, ErrListResponseTooLarge) {
		t.Fatalf("ListWithOptionsCtx error = %v, want ErrListResponseTooLarge", err)
	}
}

func TestListCtxKeepsUnboundedForegroundBehavior(t *testing.T) {
	body := `{"entries":[{"name":"` + strings.Repeat("x", 2048) + `"}]}`
	server := newListResponseServer(t, body)
	defer server.Close()

	entries, err := New(server.URL, "").ListCtx(context.Background(), "/dir")
	if err != nil {
		t.Fatalf("ListCtx error = %v", err)
	}
	if len(entries) != 1 || len(entries[0].Name) != 2048 {
		t.Fatalf("entries = %+v, want one 2048-byte name", entries)
	}
}

func newListResponseServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/fs/dir" || request.URL.Query().Get("list") != "1" {
			t.Errorf("request = %s %s?%s, want GET /v1/fs/dir?list=1", request.Method, request.URL.Path, request.URL.RawQuery)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
}
