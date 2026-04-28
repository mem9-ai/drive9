package client

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
