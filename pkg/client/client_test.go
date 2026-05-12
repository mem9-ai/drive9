package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/server"
)

func newTestClient(t *testing.T) (*Client, func()) {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-client-s3-*")
	if err != nil {
		t.Fatal(err)
	}

	initClientTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	baseURL := "http://" + ln.Addr().String()
	s3c, err := s3client.NewLocal(s3Dir, baseURL+"/s3")
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}

	srv := server.New(b)
	ts := httptest.NewUnstartedServer(srv)
	ts.Listener = ln
	ts.Start()

	cleanup := func() {
		ts.Close()
		_ = store.Close()
		_ = os.RemoveAll(s3Dir)
	}

	return New(ts.URL, ""), cleanup
}
func TestWriteAndRead(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/hello.txt", []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	data, err := c.Read("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q", data)
	}
}

func TestListDir(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/data/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/data/b.txt", []byte("bb")); err != nil {
		t.Fatal(err)
	}

	entries, err := c.List("/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
}

func TestBatchStatCtxPreservesPerPathErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/fs:batch-stat" {
			t.Fatalf("path = %q, want /v1/fs:batch-stat", r.URL.Path)
		}
		var req struct {
			Paths []string `json:"paths"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if got, want := strings.Join(req.Paths, ","), "/ok.txt,/missing.txt,/ok.txt"; got != want {
			t.Fatalf("paths = %q, want %q", got, want)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"path": "/ok.txt", "status": 200, "size": 7, "isDir": false, "revision": 3, "mtime": 11},
				{"path": "/missing.txt", "status": 404, "error": "not found"},
				{"path": "/ok.txt", "status": 200, "size": 7, "isDir": false, "revision": 3, "mtime": 11},
			},
		})
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	results, err := c.BatchStatCtx(context.Background(), []string{"/ok.txt", "/missing.txt", "/ok.txt"})
	if err != nil {
		t.Fatalf("BatchStatCtx error = %v, want nil", err)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d, want 3", len(results))
	}
	if !results[0].OK() || results[0].Revision != 3 || results[0].Size != 7 || results[0].Mtime != 11 {
		t.Fatalf("first result = %+v, want ok rev=3 size=7 mtime=11", results[0])
	}
	if results[1].OK() || results[1].Status != http.StatusNotFound {
		t.Fatalf("second result = %+v, want per-path 404", results[1])
	}
	if !results[2].OK() || results[2].Path != "/ok.txt" {
		t.Fatalf("third result = %+v, want duplicate ok path", results[2])
	}
}

func TestBatchStatCtxRejectsTooManyPathsBeforeRequest(t *testing.T) {
	var called bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer ts.Close()

	paths := make([]string, MaxBatchStatPaths+1)
	c := New(ts.URL, "")
	_, err := c.BatchStatCtx(context.Background(), paths)
	if err == nil {
		t.Fatal("BatchStatCtx error = nil, want too-large error")
	}
	if called {
		t.Fatal("server was called despite client-side batch limit")
	}
}

func TestBatchReadSmallCtxPreservesPerPathErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/fs:batch-read-small" {
			t.Fatalf("path = %q, want /v1/fs:batch-read-small", r.URL.Path)
		}
		var req struct {
			Paths    []string `json:"paths"`
			MaxBytes int64    `json:"max_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if got, want := strings.Join(req.Paths, ","), "/ok.txt,/missing.txt,/ok.txt"; got != want {
			t.Fatalf("paths = %q, want %q", got, want)
		}
		if req.MaxBytes != 16 {
			t.Fatalf("max_bytes = %d, want 16", req.MaxBytes)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"path": "/ok.txt", "status": 200, "data": []byte("hello"), "size": 5, "revision": 3, "mtime": 11},
				{"path": "/missing.txt", "status": 404, "error": "not found"},
				{"path": "/ok.txt", "status": 200, "data": []byte("hello"), "size": 5, "revision": 3, "mtime": 11},
			},
		})
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	results, err := c.BatchReadSmallCtx(context.Background(), []string{"/ok.txt", "/missing.txt", "/ok.txt"}, 16)
	if err != nil {
		t.Fatalf("BatchReadSmallCtx error = %v, want nil", err)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d, want 3", len(results))
	}
	if !results[0].OK() || string(results[0].Data) != "hello" || results[0].Revision != 3 || results[0].Size != 5 || results[0].Mtime != 11 {
		t.Fatalf("first result = %+v, want ok data/rev/size/mtime", results[0])
	}
	if results[1].OK() || results[1].Status != http.StatusNotFound {
		t.Fatalf("second result = %+v, want per-path 404", results[1])
	}
	if !results[2].OK() || results[2].Path != "/ok.txt" || string(results[2].Data) != "hello" {
		t.Fatalf("third result = %+v, want duplicate ok path", results[2])
	}
}

func TestBatchReadSmallCtxRejectsTooManyPathsBeforeRequest(t *testing.T) {
	var called bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer ts.Close()

	paths := make([]string, MaxBatchReadSmallPaths+1)
	c := New(ts.URL, "")
	_, err := c.BatchReadSmallCtx(context.Background(), paths, 1024)
	if err == nil {
		t.Fatal("BatchReadSmallCtx error = nil, want too-large error")
	}
	if called {
		t.Fatal("server was called despite client-side batch limit")
	}
}

func TestBatchReadSmallCtxRejectsPathMismatch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"path": "/b.txt", "status": 200, "data": []byte("wrong")},
			},
		})
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	_, err := c.BatchReadSmallCtx(context.Background(), []string{"/a.txt"}, 16)
	if err == nil {
		t.Fatal("BatchReadSmallCtx error = nil, want path mismatch error")
	}
	if !strings.Contains(err.Error(), `result[0] path = "/b.txt", want "/a.txt"`) {
		t.Fatalf("BatchReadSmallCtx error = %v, want path mismatch", err)
	}
}

func TestStat(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/test.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}
	info, err := c.Stat("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 4 || info.IsDir {
		t.Errorf("unexpected: %+v", info)
	}
}

func TestStatMetadataIncludesTagsAndSupportsTagReplace(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	err := c.WriteCtxConditionalWithTags(context.Background(), "/meta.txt", []byte("hello"), -1, map[string]string{
		"owner": "alice",
		"topic": "note",
	})
	if err != nil {
		t.Fatalf("WriteCtxConditionalWithTags(create): %v", err)
	}

	meta, err := c.StatMetadata("/meta.txt")
	if err != nil {
		t.Fatalf("StatMetadata(create): %v", err)
	}
	if meta.Size != 5 || meta.IsDir || meta.Revision != 1 {
		t.Fatalf("unexpected stat metadata: %+v", meta)
	}
	if meta.ResourceID == "" {
		t.Fatalf("resource_id should not be empty: %+v", meta)
	}
	if meta.Mtime == nil || *meta.Mtime <= 0 {
		t.Fatalf("mtime = %v, want > 0", meta.Mtime)
	}
	if meta.ContentType == "" {
		t.Fatal("content_type should not be empty")
	}
	if meta.SemanticText == "" {
		t.Fatal("semantic_text should not be empty")
	}
	if meta.Tags["owner"] != "alice" || meta.Tags["topic"] != "note" || len(meta.Tags) != 2 {
		t.Fatalf("tags = %+v, want owner/topic", meta.Tags)
	}

	err = c.WriteCtxConditionalWithTags(context.Background(), "/meta.txt", []byte("hello v2"), -1, map[string]string{
		"owner": "bob",
	})
	if err != nil {
		t.Fatalf("WriteCtxConditionalWithTags(overwrite): %v", err)
	}

	meta, err = c.StatMetadata("/meta.txt")
	if err != nil {
		t.Fatalf("StatMetadata(overwrite): %v", err)
	}
	if meta.Revision != 2 {
		t.Fatalf("revision = %d, want 2", meta.Revision)
	}
	if meta.Tags["owner"] != "bob" || len(meta.Tags) != 1 {
		t.Fatalf("tags after replace = %+v, want only owner=bob", meta.Tags)
	}
}

func TestWriteStreamWithSummaryPreservesExistingTagsWhenTagsNil(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	c.smallFileThreshold = 1

	_, err := c.WriteStreamWithSummaryAndTags(
		context.Background(),
		"/preserve-tags.bin",
		bytes.NewReader([]byte("abcdefgh")),
		8,
		nil,
		map[string]string{"owner": "alice", "topic": "note"},
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummaryAndTags(create): %v", err)
	}

	_, err = c.WriteStreamWithSummary(
		context.Background(),
		"/preserve-tags.bin",
		bytes.NewReader([]byte("ijklmnop")),
		8,
		nil,
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummary(overwrite): %v", err)
	}

	meta, err := c.StatMetadata("/preserve-tags.bin")
	if err != nil {
		t.Fatalf("StatMetadata: %v", err)
	}
	if meta.Revision != 2 {
		t.Fatalf("revision = %d, want 2", meta.Revision)
	}
	if meta.Tags["owner"] != "alice" || meta.Tags["topic"] != "note" || len(meta.Tags) != 2 {
		t.Fatalf("tags after nil-tag overwrite = %+v, want owner/topic preserved", meta.Tags)
	}
}

func TestStatMetadataCompatFallsBackOnUnexpectedContentType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/legacy.txt" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("legacy response"))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/legacy.txt":
			w.Header().Set("Content-Length", "14")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.Header().Set("X-Dat9-Mtime", "1700000000")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	got, err := c.StatMetadataCompat("/legacy.txt")
	if err != nil {
		t.Fatalf("StatMetadataCompat: %v", err)
	}
	if got.Size != 14 || got.IsDir || got.Revision != 7 {
		t.Fatalf("unexpected fallback metadata: %+v", got)
	}
	if got.Mtime == nil || *got.Mtime != 1700000000 {
		t.Fatalf("fallback metadata mtime = %v, want 1700000000", got.Mtime)
	}
	if got.ContentType != "" || got.SemanticText != "" {
		t.Fatalf("fallback metadata should keep enriched fields empty: %+v", got)
	}
	if len(got.Tags) != 0 {
		t.Fatalf("fallback metadata should return empty tags, got: %+v", got.Tags)
	}
	if !got.Degraded {
		t.Fatalf("fallback metadata degraded = false, want true: %+v", got)
	}
}

func TestStatMetadataCompatFallsBackOnNonJSONContentType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/legacy.json" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte(`{"size":0,"revision":0}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/legacy.json":
			w.Header().Set("Content-Length", "22")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "9")
			w.Header().Set("X-Dat9-Mtime", "1700000123")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	got, err := c.StatMetadataCompat("/legacy.json")
	if err != nil {
		t.Fatalf("StatMetadataCompat: %v", err)
	}
	if got.Size != 22 || got.IsDir || got.Revision != 9 {
		t.Fatalf("unexpected fallback metadata: %+v", got)
	}
	if got.Mtime == nil || *got.Mtime != 1700000123 {
		t.Fatalf("fallback metadata mtime = %v, want 1700000123", got.Mtime)
	}
	if got.ContentType != "" || got.SemanticText != "" {
		t.Fatalf("fallback metadata should keep enriched fields empty: %+v", got)
	}
	if len(got.Tags) != 0 {
		t.Fatalf("fallback metadata should return empty tags, got: %+v", got.Tags)
	}
	if !got.Degraded {
		t.Fatalf("fallback metadata degraded = false, want true: %+v", got)
	}
}

func TestStatMetadataCompatDoesNotFallbackOnMalformedJSON(t *testing.T) {
	headCalled := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/broken.txt" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{"))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/broken.txt":
			headCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	_, err := c.StatMetadataCompat("/broken.txt")
	if err == nil {
		t.Fatal("StatMetadataCompat error = nil, want decode error")
	}
	if !strings.Contains(err.Error(), "decode stat metadata:") {
		t.Fatalf("error = %v, want decode stat metadata error", err)
	}
	if headCalled {
		t.Fatal("HEAD fallback should not run on malformed JSON metadata response")
	}
}

func TestStatMetadataCompatDoesNotFallbackOnUnauthorized(t *testing.T) {
	headCalled := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/secure.txt" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/secure.txt":
			headCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	_, err := c.StatMetadataCompat("/secure.txt")
	if err == nil {
		t.Fatal("StatMetadataCompat error = nil, want unauthorized error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("StatMetadataCompat error type = %T, want *StatusError", err)
	}
	if statusErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d", statusErr.StatusCode, http.StatusUnauthorized)
	}
	if headCalled {
		t.Fatal("HEAD fallback should not run on unauthorized metadata request")
	}
}

func TestStatMetadataCompatDoesNotFallbackOnNotFound(t *testing.T) {
	headCalled := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/missing.txt" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"missing metadata"}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/missing.txt":
			headCalled = true
			w.WriteHeader(http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	_, err := c.StatMetadataCompat("/missing.txt")
	if err == nil {
		t.Fatal("StatMetadataCompat error = nil, want not found error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("StatMetadataCompat error type = %T, want *StatusError", err)
	}
	if statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d", statusErr.StatusCode, http.StatusNotFound)
	}
	if headCalled {
		t.Fatal("HEAD fallback should not run on not found metadata request")
	}
}

func TestStatCtxPreservesStatusErrorOnNotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/missing.txt":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"missing stat"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	_, err := c.StatCtx(context.Background(), "/missing.txt")
	if err == nil {
		t.Fatal("StatCtx error = nil, want not found error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("StatCtx error type = %T, want *StatusError", err)
	}
	if statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d", statusErr.StatusCode, http.StatusNotFound)
	}
}

func TestCreateFileCtxPostsCreateActionAndReturnsRevision(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/fs/empty.txt" {
			t.Errorf("path = %s, want /v1/fs/empty.txt", r.URL.Path)
		}
		if !r.URL.Query().Has("create") {
			t.Errorf("query = %q, want create action", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": int64(1)})
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	rev, err := c.CreateFileCtx(context.Background(), "/empty.txt")
	if err != nil {
		t.Fatalf("CreateFileCtx error = %v", err)
	}
	if rev != 1 {
		t.Fatalf("revision = %d, want 1", rev)
	}
}

func TestCreateFileCtxReturnsMalformedJSONError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{"))
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	_, err := c.CreateFileCtx(context.Background(), "/empty.txt")
	if err == nil {
		t.Fatal("CreateFileCtx error = nil, want decode error")
	}
	if !strings.Contains(err.Error(), "decode create file response:") {
		t.Fatalf("error = %v, want decode create file response error", err)
	}
}

func TestWriteCtxConditionalWithTagsRejectsInvalidHeaderTags(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	tests := []struct {
		name    string
		tags    map[string]string
		wantErr string
	}{
		{
			name:    "key contains equals",
			tags:    map[string]string{"a=b": "v"},
			wantErr: "contains '='",
		},
		{
			name:    "key contains control characters",
			tags:    map[string]string{"owner\n": "alice"},
			wantErr: "contains control characters",
		},
		{
			name:    "value contains control characters",
			tags:    map[string]string{"owner": "alice\r\nbob"},
			wantErr: "contains control characters",
		},
		{
			name:    "key contains invalid utf8",
			tags:    map[string]string{string([]byte{0xff}): "alice"},
			wantErr: "invalid UTF-8",
		},
		{
			name:    "value contains invalid utf8",
			tags:    map[string]string{"owner": string([]byte{0xff})},
			wantErr: "invalid UTF-8",
		},
		{
			name:    "key too long",
			tags:    map[string]string{strings.Repeat("k", 256): "alice"},
			wantErr: "key exceeds 255 characters",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requests = 0
			c := New(ts.URL, "")
			err := c.WriteCtxConditionalWithTags(context.Background(), "/bad-tags.txt", []byte("x"), -1, tc.tags)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
			if requests != 0 {
				t.Fatalf("request count = %d, want 0", requests)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/del.txt", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("/del.txt"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Read("/del.txt")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestDeleteKindHints(t *testing.T) {
	var got []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.RawQuery)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	if err := c.DeleteFileCtx(context.Background(), "/file.txt"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteDirCtx(context.Background(), "/dir/"); err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("requests = %d, want 2", len(got))
	}
	if got[0] != "kind=file" {
		t.Fatalf("file delete query = %q, want kind=file", got[0])
	}
	if got[1] != "kind=dir" {
		t.Fatalf("dir delete query = %q, want kind=dir", got[1])
	}
}

func TestRemoveAll(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/data"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/data/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir("/data/nested"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/data/nested/b.txt", []byte("b")); err != nil {
		t.Fatal(err)
	}

	if err := c.RemoveAll("/data/"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat("/data/"); err == nil {
		t.Fatal("expected error after recursive delete")
	}
	if _, err := c.Read("/data/a.txt"); err == nil {
		t.Fatal("expected sibling file read to fail after recursive delete")
	}
	if _, err := c.Read("/data/nested/b.txt"); err == nil {
		t.Fatal("expected nested file read to fail after recursive delete")
	}
	entries, err := c.List("/")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name == "data" || entry.Name == "data/" {
			t.Fatalf("expected removed directory to be absent from root listing, got entries %+v", entries)
		}
	}
}

func TestCopy(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/src.txt", []byte("shared")); err != nil {
		t.Fatal(err)
	}
	if err := c.Copy("/src.txt", "/dst.txt"); err != nil {
		t.Fatal(err)
	}
	data, _ := c.Read("/dst.txt")
	if string(data) != "shared" {
		t.Errorf("got %q", data)
	}
}

func TestRename(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/old.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := c.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatal(err)
	}
	data, err := c.Read("/new.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data" {
		t.Errorf("got %q", data)
	}
	_, err = c.Read("/old.txt")
	if err == nil {
		t.Error("expected error for old path")
	}
}

func TestMkdir(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/mydir"); err != nil {
		t.Fatal(err)
	}
	info, err := c.Stat("/mydir/")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir {
		t.Error("expected directory")
	}
}

// statusFakeServer returns an httptest server that responds to GET /v1/status
// with the given JSON body and counts the number of times it was called.
type statusFakeServer struct {
	srv  *httptest.Server
	hits *int
}

func newStatusFakeServer(t *testing.T, body []byte) *statusFakeServer {
	t.Helper()
	hits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &statusFakeServer{srv: srv, hits: &hits}
}

func TestSmallFileThresholdCachesFromServer(t *testing.T) {
	body := []byte(`{"status":"active","max_upload_bytes":1048576,"inline_threshold":262144}`)
	fake := newStatusFakeServer(t, body)
	c := New(fake.srv.URL, "")

	if got := c.SmallFileThreshold(context.Background()); got != 262144 {
		t.Fatalf("SmallFileThreshold = %d, want 262144", got)
	}
	if got := c.MaxUploadBytes(context.Background()); got != 1048576 {
		t.Fatalf("MaxUploadBytes = %d, want 1048576", got)
	}
	// Subsequent calls must hit the in-memory cache, not the server.
	if got := c.SmallFileThreshold(context.Background()); got != 262144 {
		t.Fatalf("second SmallFileThreshold = %d", got)
	}
	if *fake.hits != 1 {
		t.Fatalf("expected 1 status fetch, got %d", *fake.hits)
	}
}

func TestSmallFileThresholdFallsBackWhenServerOmits(t *testing.T) {
	// Older servers don't include inline_threshold. Client should leave
	// statusInline = 0 so uploadThreshold returns 0 ("force multipart") —
	// this is the safe choice when the operator may have configured the
	// threshold below the historical 50KB. MaxUploadBytes still echoes
	// the field that was present.
	body := []byte(`{"status":"active","max_upload_bytes":1048576}`)
	fake := newStatusFakeServer(t, body)
	c := New(fake.srv.URL, "")

	if got := c.SmallFileThreshold(context.Background()); got != 0 {
		t.Fatalf("SmallFileThreshold without server field = %d, want 0", got)
	}
	if got := c.uploadThreshold(context.Background()); got != 0 {
		t.Fatalf("uploadThreshold without server field = %d, want 0 (force multipart)", got)
	}
	if got := c.MaxUploadBytes(context.Background()); got != 1048576 {
		t.Fatalf("MaxUploadBytes = %d", got)
	}
}

func TestSmallFileThresholdLookupFailureCachesZero(t *testing.T) {
	// Transient warm failure must NOT consume the once-style guard: a later
	// successful Warm/SmallFileThreshold call should refetch and pick up
	// the server-advertised value. This guards against the failure mode
	// where FUSE Init's bounded warm misses (5s timeout, 5xx, network)
	// and the client is then permanently stuck on the local fallback.
	var failing atomic.Bool
	failing.Store(true)
	hits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		hits++
		if failing.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"inline_threshold":262144}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := New(srv.URL, "")

	if got := c.SmallFileThreshold(context.Background()); got != 0 {
		t.Fatalf("SmallFileThreshold on 500 = %d, want 0", got)
	}
	if got := c.uploadThreshold(context.Background()); got != 0 {
		t.Fatalf("uploadThreshold during failure = %d, want 0 (force multipart)", got)
	}

	// Server recovers; a subsequent call must retry, not stay cached at 0.
	failing.Store(false)
	if got := c.SmallFileThreshold(context.Background()); got != 262144 {
		t.Fatalf("SmallFileThreshold after recovery = %d, want 262144 (failures must not consume the once-guard)", got)
	}
	if hits < 2 {
		t.Fatalf("expected refetch after transient failure; total hits=%d", hits)
	}

	// Once we have a successful response, we must not fetch again.
	prev := hits
	_ = c.SmallFileThreshold(context.Background())
	_ = c.SmallFileThreshold(context.Background())
	if hits != prev {
		t.Fatalf("post-success calls fetched again: hits %d -> %d", prev, hits)
	}
}

func TestCachedSmallFileThresholdDoesNotFetch(t *testing.T) {
	mux := http.NewServeMux()
	hits := 0
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"inline_threshold":131072}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := New(srv.URL, "")

	// CachedSmallFileThreshold must never trigger /v1/status.
	if got := c.CachedSmallFileThreshold(); got != 0 {
		t.Fatalf("cached pre-warm = %d, want 0", got)
	}
	if hits != 0 {
		t.Fatalf("CachedSmallFileThreshold triggered %d fetches; must be 0", hits)
	}
	// After warm-up via SmallFileThreshold, cache returns server value.
	_ = c.SmallFileThreshold(context.Background())
	if got := c.CachedSmallFileThreshold(); got != 131072 {
		t.Fatalf("cached post-warm = %d, want 131072", got)
	}
}

func TestSmallFileThresholdConcurrentReadersAreRaceFree(t *testing.T) {
	body := []byte(`{"status":"active","max_upload_bytes":1048576,"inline_threshold":262144}`)
	fake := newStatusFakeServer(t, body)
	c := New(fake.srv.URL, "")

	// Mix one warmup goroutine (calls SmallFileThreshold which writes the
	// atomic) with many CachedSmallFileThreshold readers. Run with -race;
	// pre-fix this race tripped the detector.
	const readers = 32
	done := make(chan struct{})
	for i := 0; i < readers; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					_ = c.CachedSmallFileThreshold()
					_ = c.statusInline.Load()
				}
			}
		}()
	}
	// Warmup goroutine.
	go c.SmallFileThreshold(context.Background())

	// Run for a brief period and stop.
	for i := 0; i < 10000; i++ {
		_ = c.CachedSmallFileThreshold()
	}
	close(done)
}

func TestUploadThresholdReturnsZeroBeforeWarm(t *testing.T) {
	// uploadThreshold must NOT fall back to DefaultSmallFileThreshold
	// before /v1/status warmup. If the operator configured the server
	// below 50KB, falling back to 50KB would direct-PUT files the server
	// then rejects with `missing X-Dat9-Part-Checksums`. Returning 0 is
	// the documented "force multipart" signal that callers honor.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"inline_threshold":30000}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := New(srv.URL, "")

	if got := c.uploadThreshold(context.Background()); got != 0 {
		t.Fatalf("pre-warm uploadThreshold = %d, want 0 (force multipart)", got)
	}
	// After warmup, returns the negotiated value.
	c.Warm(context.Background())
	if got := c.uploadThreshold(context.Background()); got != 30000 {
		t.Fatalf("post-warm uploadThreshold = %d, want 30000", got)
	}
}

func TestUploadThresholdHonoursLoweredServerThreshold(t *testing.T) {
	// End-to-end: when the server is configured at 30KB and the client
	// has warmed, a 40KB upload must NOT go direct PUT — the server's
	// IsLargeFile gate would reject it. The fake server enforces this
	// by failing any direct PUT that lands here.
	const serverThreshold = int64(30000)
	const fileSize = 40000
	directPUTs := 0
	multipartInits := 0

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"inline_threshold":30000}`))
	})
	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// Server-side: any PUT >= threshold without X-Dat9-Part-Checksums
		// is rejected. Mirrors the production gate at server.go:776.
		cl := r.ContentLength
		if cl >= serverThreshold && r.Header.Get("X-Dat9-Part-Checksums") == "" {
			directPUTs++
			http.Error(w, "missing X-Dat9-Part-Checksums header", http.StatusBadRequest)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"revision":1}`))
	})
	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		multipartInits++
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"upload_id":"u1","key":"k1","part_size":8388608,"total_parts":1}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(srv.URL, "")
	c.Warm(context.Background())

	// Stream through writeStreamConditionalWithSummary; size > threshold
	// must take the multipart path. We use the WriteStream entry point
	// since direct PUT for small files is the path under test.
	body := bytes.NewReader(make([]byte, fileSize))
	// The fake server only wires initiate and direct PUT — presign /
	// parts / complete intentionally aren't implemented because we only
	// need to observe routing. The call is therefore expected to fail
	// past initiate; what matters is which endpoint the client hit
	// before failing. The unused-error discard is deliberate.
	_, _ = c.writeStreamConditionalWithSummary(context.Background(), "/test.bin", body, fileSize, nil, -1, nil, "")
	if directPUTs > 0 {
		t.Fatalf("client issued %d direct PUTs for %dB above %dB threshold; multipart was required", directPUTs, fileSize, serverThreshold)
	}
	if multipartInits == 0 {
		t.Fatalf("client never issued multipart initiate; routing missed the lowered threshold")
	}
}

func TestUploadThresholdWarmFailureForcesMultipart(t *testing.T) {
	// /v1/status fails (5xx); the client must fall back to "force
	// multipart" rather than the historical 50KB. Otherwise an operator
	// who lowered the server threshold below 50KB would see direct PUTs
	// rejected after every transient warm failure.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := New(srv.URL, "")
	c.Warm(context.Background())

	if got := c.uploadThreshold(context.Background()); got != 0 {
		t.Fatalf("uploadThreshold after warm failure = %d, want 0 (force multipart)", got)
	}
}

func TestSmallFileThresholdOverrideShortCircuits(t *testing.T) {
	mux := http.NewServeMux()
	hits := 0
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"inline_threshold":999999}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := New(srv.URL, "")
	c.smallFileThreshold = 7 // explicit per-Client override

	if got := c.SmallFileThreshold(context.Background()); got != 7 {
		t.Fatalf("override = %d, want 7", got)
	}
	if got := c.CachedSmallFileThreshold(); got != 7 {
		t.Fatalf("override cached = %d, want 7", got)
	}
	if hits != 0 {
		t.Fatalf("override should short-circuit network; saw %d hits", hits)
	}
}
