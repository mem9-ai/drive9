package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCachedAppendLogSupported(t *testing.T) {
	var statusCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			t.Fatalf("path = %q, want /v1/status", r.URL.Path)
		}
		statusCalls++
		_, _ = io.WriteString(w, `{"storage_capabilities":{"append_log_v1":true}}`)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	if c.CachedAppendLogSupported() {
		t.Fatal("append-log support true before warm")
	}
	if statusCalls != 0 {
		t.Fatalf("status calls = %d, want 0", statusCalls)
	}
	c.Warm(context.Background())
	if !c.CachedAppendLogSupported() {
		t.Fatal("append-log support false after warm")
	}
	if statusCalls != 1 {
		t.Fatalf("status calls = %d, want 1", statusCalls)
	}
}

func TestCachedAppendLogSupportedWarmFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.Warm(context.Background())
	if c.CachedAppendLogSupported() {
		t.Fatal("append-log support true after failed warm")
	}
}

func TestStatCtxParsesContentLayout(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		want   ContentLayout
	}{
		{name: "append-log", header: "append_log", want: ContentLayoutAppendLog},
		{name: "single", header: "single", want: ContentLayoutSingle},
		{name: "missing", want: ""},
		{name: "unknown", header: "future", want: ContentLayout("future")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "7")
				if tc.header != "" {
					w.Header().Set("X-Dat9-Content-Layout", tc.header)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			stat, err := New(srv.URL, "").StatCtx(context.Background(), "/file")
			if err != nil {
				t.Fatal(err)
			}
			if stat.ContentLayout != tc.want {
				t.Fatalf("content layout = %q, want %q", stat.ContentLayout, tc.want)
			}
		})
	}
}

func TestReadErrorPreservesCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "rebase required",
			"code":  AppendLogCodeRebased,
		})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").AppendLog(context.Background(), "/file", bytes.NewReader(nil), 0, 0, 0)
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error type = %T, want *StatusError", err)
	}
	if statusErr.Code != AppendLogCodeRebased {
		t.Fatalf("code = %q, want %q", statusErr.Code, AppendLogCodeRebased)
	}
}

func TestAppendLogRequestAndSuccess(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/v1/fs/db-wal" || !r.URL.Query().Has("append-log") {
			t.Fatalf("url = %s", r.URL.String())
		}
		if r.ContentLength != 4 {
			t.Fatalf("content length = %d, want 4", r.ContentLength)
		}
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "7" {
			t.Fatalf("expected revision = %q", got)
		}
		if got := r.Header.Get("X-Dat9-Expected-Size"); got != "10" {
			t.Fatalf("expected size = %q", got)
		}
		gotBody, _ = io.ReadAll(r.Body)
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 8, "size_bytes": 14})
	}))
	defer srv.Close()

	result, err := New(srv.URL, "").AppendLog(context.Background(), "/db-wal", bytes.NewReader([]byte("tail")), 4, 7, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Revision != 8 || result.Size != 14 {
		t.Fatalf("result = %+v", result)
	}
	if got := string(gotBody); got != "tail" {
		t.Fatalf("body = %q, want tail", got)
	}
}

func TestAppendLogRejectsInvalidSuccess(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "missing-revision", body: `{"size_bytes":4}`},
		{name: "wrong-size", body: `{"revision":1,"size_bytes":5}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			_, err := New(srv.URL, "").AppendLog(context.Background(), "/file", bytes.NewReader([]byte("data")), 4, 0, 0)
			if err == nil {
				t.Fatal("AppendLog error = nil")
			}
		})
	}
}

func TestWriteServerStreamConditionalUsesOnePUT(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPut || r.URL.Path != "/v1/fs/file" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.ContentLength != 5 {
			t.Fatalf("content length = %d, want 5", r.ContentLength)
		}
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "9" {
			t.Fatalf("expected revision = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(body); got != "whole" {
			t.Fatalf("body = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 10})
	}))
	defer srv.Close()

	revision, err := New(srv.URL, "").WriteServerStreamConditional(context.Background(), "/file", bytes.NewReader([]byte("whole")), 5, 9)
	if err != nil {
		t.Fatal(err)
	}
	if revision != 10 || calls != 1 {
		t.Fatalf("revision=%d calls=%d", revision, calls)
	}
}
