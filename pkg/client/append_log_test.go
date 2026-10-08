package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestCachedExtentXattrSupportedFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		status int
		want   bool
	}{
		{name: "supported", body: `{"storage_capabilities":{"extent_xattr_v1":true}}`, status: http.StatusOK, want: true},
		{name: "false", body: `{"storage_capabilities":{"extent_xattr_v1":false}}`, status: http.StatusOK},
		{name: "old-server", body: `{"storage_capabilities":{}}`, status: http.StatusOK},
		{name: "missing", body: `{}`, status: http.StatusOK},
		{name: "malformed", body: `{"storage_capabilities":{"extent_xattr_v1":"v1"}}`, status: http.StatusOK},
		{name: "unavailable", body: `{"storage_capabilities":{"extent_xattr_v1":true}}`, status: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(srv.Close)
			c := New(srv.URL, "")
			if c.CachedExtentXattrSupported() {
				t.Fatal("extent xattr support true before warm")
			}
			c.Warm(context.Background())
			if got := c.CachedExtentXattrSupported(); got != tc.want {
				t.Fatalf("extent xattr support=%t, want %t", got, tc.want)
			}
		})
	}
}

func TestRequireExtentXattrV1PreservesFailureCause(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		status      int
		wantError   string
		unsupported bool
	}{
		{name: "supported", body: `{"storage_capabilities":{"extent_xattr_v1":true}}`, status: http.StatusOK},
		{name: "missing", body: `{"storage_capabilities":{}}`, status: http.StatusOK, wantError: "did not advertise drive9.extent_xattr.v1", unsupported: true},
		{name: "malformed", body: `{"storage_capabilities":{"extent_xattr_v1":"v1"}}`, status: http.StatusOK, wantError: "decode tenant status"},
		{name: "bad-request", body: `invalid request`, status: http.StatusBadRequest, wantError: "HTTP 400: invalid request"},
		{name: "status", body: `upstream unavailable`, status: http.StatusServiceUnavailable, wantError: "HTTP 503: upstream unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/v1/status" || r.Header.Get("Authorization") != "Bearer sk-test" {
					t.Fatalf("unexpected status request: %s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(srv.Close)
			err := New(srv.URL, "sk-test").RequireExtentXattrV1(t.Context())
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				if calls != 1 {
					t.Fatalf("status calls = %d, want 1", calls)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error = %v, want %q", err, tc.wantError)
			}
			if got := errors.Is(err, ErrExtentXattrUnsupported); got != tc.unsupported {
				t.Fatalf("unsupported = %t, want %t (err=%v)", got, tc.unsupported, err)
			}
			if calls != 1 {
				t.Fatalf("status calls = %d, want 1", calls)
			}
		})
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

func TestAppendLogZeroBodyUsesExplicitLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			t.Fatalf("zero append request = length %d transfer=%v, want 0/non-chunked", r.ContentLength, r.TransferEncoding)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1, "size_bytes": 0})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").AppendLog(context.Background(), "/file", io.NopCloser(bytes.NewReader(nil)), 0, 0, 0)
	if err != nil {
		t.Fatal(err)
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

func TestWriteServerStreamConditionalZeroBodyUsesExplicitLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %q, want PUT", r.Method)
		}
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			t.Fatalf("zero conditional PUT = length %d transfer=%v, want 0/non-chunked", r.ContentLength, r.TransferEncoding)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").WriteServerStreamConditional(context.Background(), "/file", io.NopCloser(bytes.NewReader(nil)), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
}
