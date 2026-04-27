package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-srv-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initServerTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	s3c, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		t.Fatal(err)
	}
	return New(b)
}

func newLocalTenantShimServer(t *testing.T, apiKey string) *Server {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-srv-s3-local-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initServerTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	s3c, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		t.Fatal(err)
	}
	return NewWithConfig(Config{Backend: b, LocalTenantAPIKey: apiKey})
}

func TestWriteAndRead(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Write
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/data/hello.txt", strings.NewReader("hello world"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("write: %d", resp.StatusCode)
	}

	// Read
	resp, err = http.Get(ts.URL + "/v1/fs/data/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("read: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello world" {
		t.Errorf("got %q", body)
	}
}

func TestReadInlineNoRedirect(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Write a small file (db-inline, no S3).
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/inline.txt", strings.NewReader("tiny"))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// GET should return 200 with inline body, not a 302 redirect.
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse // don't follow redirects
	}}
	resp, err := client.Get(ts.URL + "/v1/fs/inline.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 for db-inline GET, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "tiny" {
		t.Errorf("body=%q, want 'tiny'", body)
	}
}

func TestReadDirectoryReturns404(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Create a directory by writing a file inside it.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/mydir/file.txt", strings.NewReader("x"))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// GET on the directory path should return 404, not 500.
	resp, err := http.Get(ts.URL + "/v1/fs/mydir/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("GET directory: expected 404, got %d", resp.StatusCode)
	}

	// Also test without trailing slash.
	resp, err = http.Get(ts.URL + "/v1/fs/mydir")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("GET directory (no slash): expected 404, got %d", resp.StatusCode)
	}
}

func TestListDir(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Write two files
	for _, name := range []string{"a.txt", "b.txt"} {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/data/"+name, strings.NewReader(name))
		resp, _ := http.DefaultClient.Do(req)
		_ = resp.Body.Close()
	}

	// List
	resp, err := http.Get(ts.URL + "/v1/fs/data/?list=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Entries []struct {
			Name  string `json:"name"`
			IsDir bool   `json:"isDir"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result.Entries))
	}
}

func TestStat(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Write a file
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/test.txt", strings.NewReader("data"))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// Stat
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/test.txt", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("stat: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "4" {
		t.Errorf("expected Content-Length 4, got %s", resp.Header.Get("Content-Length"))
	}
	if resp.Header.Get("X-Dat9-IsDir") != "false" {
		t.Errorf("expected X-Dat9-IsDir false, got %s", resp.Header.Get("X-Dat9-IsDir"))
	}
}

func TestStatMetadataIncludesTagsAndSemanticText(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	writeReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/meta.txt", strings.NewReader("hello metadata"))
	writeReq.Header.Add("X-Dat9-Tag", "owner=alice")
	writeReq.Header.Add("X-Dat9-Tag", "topic=note")
	writeResp, err := http.DefaultClient.Do(writeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = writeResp.Body.Close()
	if writeResp.StatusCode != http.StatusOK {
		t.Fatalf("write: %d", writeResp.StatusCode)
	}

	resp, err := http.Get(ts.URL + "/v1/fs/meta.txt?stat=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat metadata: %d", resp.StatusCode)
	}

	var out struct {
		Size         int64             `json:"size"`
		IsDir        bool              `json:"isdir"`
		Revision     int64             `json:"revision"`
		Mtime        *int64            `json:"mtime"`
		ContentType  string            `json:"content_type"`
		SemanticText string            `json:"semantic_text"`
		Tags         map[string]string `json:"tags"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if out.Size != int64(len("hello metadata")) || out.IsDir || out.Revision != 1 {
		t.Fatalf("unexpected metadata shape: %+v", out)
	}
	if out.ContentType == "" || out.SemanticText == "" {
		t.Fatalf("expected content_type and semantic_text, got %+v", out)
	}
	if out.Mtime == nil || *out.Mtime <= 0 {
		t.Fatalf("expected mtime > 0, got %+v", out)
	}
	if got := resp.Header.Get("X-Dat9-Mtime"); got != strconv.FormatInt(*out.Mtime, 10) {
		t.Fatalf("X-Dat9-Mtime = %q, want %d", got, *out.Mtime)
	}
	if out.Tags["owner"] != "alice" || out.Tags["topic"] != "note" || len(out.Tags) != 2 {
		t.Fatalf("unexpected tags: %+v", out.Tags)
	}
}

func TestWriteRejectsDuplicateTagHeaders(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/dup-tags.txt", strings.NewReader("x"))
	req.Header.Add("X-Dat9-Tag", "owner=alice")
	req.Header.Add("X-Dat9-Tag", "owner=bob")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "duplicate X-Dat9-Tag key") {
		t.Fatalf("response = %q, want duplicate tag error", body)
	}
}

func TestWriteRejectsOverlongTagHeaders(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	tests := []struct {
		name   string
		header string
		want   string
	}{
		{
			name:   "key too long",
			header: strings.Repeat("k", 256) + "=v",
			want:   "key exceeds 255 characters",
		},
		{
			name:   "value too long",
			header: "owner=" + strings.Repeat("v", 256),
			want:   "value exceeds 255 characters",
		},
		{
			name:   "key has leading or trailing whitespace",
			header: " owner =alice",
			want:   "leading or trailing whitespace",
		},
		{
			name:   "value has leading or trailing whitespace",
			header: "owner= alice",
			want:   "leading or trailing whitespace",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/long-tags.txt", strings.NewReader("x"))
			req.Header.Add("X-Dat9-Tag", tc.header)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			body, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(body), tc.want) {
				t.Fatalf("response = %q, want %q", body, tc.want)
			}
		})
	}
}

func TestWriteRejectsTagsOnLegacyLargeFileInitiate(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/large-tags.bin", strings.NewReader(""))
	req.Header.Set("X-Dat9-Content-Length", "50000")
	req.Header.Set("X-Dat9-Part-Checksums", "AAAAAA==")
	req.Header.Add("X-Dat9-Tag", "owner=alice")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "X-Dat9-Tag is not supported on large-file PUT initiate") {
		t.Fatalf("response = %q, want unsupported large PUT tag error", body)
	}
}

func TestStatDirectoryWithoutTrailingSlash(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/dir?mkdir", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/dir", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat dir without trailing slash: %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Dat9-IsDir") != "true" {
		t.Errorf("expected X-Dat9-IsDir true, got %s", resp.Header.Get("X-Dat9-IsDir"))
	}
	if resp.Header.Get("Content-Length") != "0" {
		t.Errorf("expected Content-Length 0, got %s", resp.Header.Get("Content-Length"))
	}
}

func TestStatDirectoryWithoutTrailingSlashDoesNotLogDatastoreError(t *testing.T) {
	core, recorded := observer.New(zap.ErrorLevel)
	restoreLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(restoreLogger) })

	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/dir?mkdir", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/dir", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat dir without trailing slash: %d", resp.StatusCode)
	}

	if entries := recorded.FilterMessage("datastore_op_failed").AllUntimed(); len(entries) != 0 {
		t.Fatalf("expected no datastore_op_failed logs, got %d", len(entries))
	}
}

func TestWriteWithExpectedRevisionConflict(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/cas.txt", strings.NewReader("v1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial write: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/cas.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rev, _ := strconv.ParseInt(resp.Header.Get("X-Dat9-Revision"), 10, 64)
	_ = resp.Body.Close()
	if rev != 1 {
		t.Fatalf("initial revision = %d, want 1", rev)
	}

	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/cas.txt", strings.NewReader("stale"))
	req.Header.Set("X-Dat9-Expected-Revision", "0")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("create-if-absent conflict status = %d, want 409", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/cas.txt", strings.NewReader("v2"))
	req.Header.Set("X-Dat9-Expected-Revision", "1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("conditional overwrite status = %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/cas.txt", strings.NewReader("late"))
	req.Header.Set("X-Dat9-Expected-Revision", "1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale revision conflict status = %d, want 409", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/v1/fs/cas.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "v2" {
		t.Fatalf("final body = %q, want v2", body)
	}
}

func TestDelete(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Write
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/del.txt", strings.NewReader("data"))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// Delete
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/v1/fs/del.txt", nil)
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}

	// Verify gone
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/del.txt", nil)
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestLocalTenantShimProvisionAndStatus(t *testing.T) {
	s := newLocalTenantShimServer(t, "local-dev-key")
	ts := httptest.NewServer(s)
	defer ts.Close()

	provisionReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	provisionResp, err := http.DefaultClient.Do(provisionReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provisionResp.Body.Close() }()
	if provisionResp.StatusCode != http.StatusAccepted {
		t.Fatalf("provision: %d", provisionResp.StatusCode)
	}
	var provisionBody map[string]string
	if err := json.NewDecoder(provisionResp.Body).Decode(&provisionBody); err != nil {
		t.Fatal(err)
	}
	if got := provisionBody["api_key"]; got != "local-dev-key" {
		t.Fatalf("api_key = %q, want local-dev-key", got)
	}
	if got := provisionBody["status"]; got != "provisioning" {
		t.Fatalf("status = %q, want provisioning", got)
	}
	if len(provisionBody) != 2 {
		t.Fatalf("provision body keys = %v, want only api_key/status", provisionBody)
	}

	statusReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	statusReq.Header.Set("Authorization", "Bearer local-dev-key")
	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", statusResp.StatusCode)
	}
	var statusBody TenantStatusResponse
	if err := json.NewDecoder(statusResp.Body).Decode(&statusBody); err != nil {
		t.Fatal(err)
	}
	if statusBody.Status != "active" {
		t.Fatalf("status = %q, want active", statusBody.Status)
	}
}

func TestLocalTenantShimStatusRejectsWrongToken(t *testing.T) {
	s := newLocalTenantShimServer(t, "local-dev-key")
	ts := httptest.NewServer(s)
	defer ts.Close()

	statusReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	statusReq.Header.Set("Authorization", "Bearer wrong-key")
	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", statusResp.StatusCode)
	}
}

func TestCopy(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Write source
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/src.txt", strings.NewReader("shared"))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// Copy (zero-copy)
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/dst.txt?copy", nil)
	req.Header.Set("X-Dat9-Copy-Source", "/src.txt")
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("copy: %d", resp.StatusCode)
	}

	// Read copy
	resp, _ = http.Get(ts.URL + "/v1/fs/dst.txt")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "shared" {
		t.Errorf("got %q", body)
	}
}

func TestRename(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Write
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/old.txt", strings.NewReader("data"))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// Rename
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/new.txt?rename", nil)
	req.Header.Set("X-Dat9-Rename-Source", "/old.txt")
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("rename: %d", resp.StatusCode)
	}

	// Old gone
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/old.txt", nil)
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}

	// New exists
	resp, _ = http.Get(ts.URL + "/v1/fs/new.txt")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "data" {
		t.Errorf("got %q", body)
	}
}

func TestPatchMissingPathReturnsNotFound(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPatch, ts.URL+"/v1/fs/missing.bin", strings.NewReader(`{"new_size":16,"dirty_parts":[1]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("patch missing path: got %d, want 404", resp.StatusCode)
	}
}

func TestPatchDBBackedFileReturnsBadRequest(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	writeReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/small.txt", strings.NewReader("hello"))
	writeResp, err := http.DefaultClient.Do(writeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = writeResp.Body.Close()
	if writeResp.StatusCode != http.StatusOK {
		t.Fatalf("write small file: got %d, want 200", writeResp.StatusCode)
	}

	patchReq, _ := http.NewRequest(http.MethodPatch, ts.URL+"/v1/fs/small.txt", strings.NewReader(`{"new_size":16,"dirty_parts":[1]}`))
	patchReq.Header.Set("Content-Type", "application/json")
	patchResp, err := http.DefaultClient.Do(patchReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = patchResp.Body.Close()
	if patchResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("patch small file: got %d, want 400", patchResp.StatusCode)
	}
}

func TestAppendMissingPathReturnsNotFound(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	appendReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/missing.bin?append", strings.NewReader(`{"append_size":16}`))
	appendReq.Header.Set("Content-Type", "application/json")
	appendResp, err := http.DefaultClient.Do(appendReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = appendResp.Body.Close()
	if appendResp.StatusCode != http.StatusNotFound {
		t.Fatalf("append missing path: got %d, want 404", appendResp.StatusCode)
	}
}

func TestAppendDBBackedFileReturnsBadRequest(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	writeReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/small-append.txt", strings.NewReader("hello"))
	writeResp, err := http.DefaultClient.Do(writeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = writeResp.Body.Close()
	if writeResp.StatusCode != http.StatusOK {
		t.Fatalf("write small file: got %d, want 200", writeResp.StatusCode)
	}

	appendReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/small-append.txt?append", strings.NewReader(`{"append_size":16}`))
	appendReq.Header.Set("Content-Type", "application/json")
	appendResp, err := http.DefaultClient.Do(appendReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = appendResp.Body.Close()
	if appendResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("append small file: got %d, want 400", appendResp.StatusCode)
	}
}

func TestAppendRejectsOversizedRequestBody(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	body := `{"append_size":16,"padding":"` + strings.Repeat("a", 1<<20) + `"}`
	appendReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/missing.bin?append", strings.NewReader(body))
	appendReq.Header.Set("Content-Type", "application/json")

	appendResp, err := http.DefaultClient.Do(appendReq)
	if err != nil {
		t.Fatal(err)
	}
	respBody, err := io.ReadAll(appendResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	_ = appendResp.Body.Close()

	if appendResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("append oversized body: got %d, want 400", appendResp.StatusCode)
	}
	if !strings.Contains(string(respBody), "invalid request body") {
		t.Fatalf("append oversized body response = %q, want invalid request body", respBody)
	}
}

func TestNotFound(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/fs/nonexistent.txt")
	if err != nil {
		t.Fatalf("GET nonexistent: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestTraceIDHeaderPropagation(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/nonexistent.txt", nil)
	req.Header.Set("X-Trace-ID", "trace-e2e-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got := resp.Header.Get("X-Trace-ID"); got != "trace-e2e-123" {
		t.Fatalf("expected trace header echo, got %q", got)
	}

	resp2, err := http.Get(ts.URL + "/v1/fs/nonexistent.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if got := resp2.Header.Get("X-Trace-ID"); got == "" {
		t.Fatal("expected generated X-Trace-ID header")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/m.txt", strings.NewReader("abc"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	sqlReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sql", strings.NewReader(`{"query":"SELECT 1"}`))
	sqlReq.Header.Set("Content-Type", "application/json")
	sqlResp, err := http.DefaultClient.Do(sqlReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = sqlResp.Body.Close()

	readResp, err := http.Get(ts.URL + "/v1/fs/m.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = readResp.Body.Close()

	statReq, _ := http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/m.txt", nil)
	statResp, err := http.DefaultClient.Do(statReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = statResp.Body.Close()

	badFSReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/m.txt?unknown=1", nil)
	badFSResp, err := http.DefaultClient.Do(badFSReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = badFSResp.Body.Close()

	methodFSReq, _ := http.NewRequest(http.MethodPatch, ts.URL+"/v1/fs/m.txt", nil)
	methodFSResp, err := http.DefaultClient.Do(methodFSReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = methodFSResp.Body.Close()

	statusMethodReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/status", nil)
	statusMethodResp, err := http.DefaultClient.Do(statusMethodReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = statusMethodResp.Body.Close()

	statusReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = statusResp.Body.Close()

	provisionReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	provisionResp, err := http.DefaultClient.Do(provisionReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = provisionResp.Body.Close()

	badSQLReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sql", strings.NewReader(`{"query":""}`))
	badSQLReq.Header.Set("Content-Type", "application/json")
	badSQLResp, err := http.DefaultClient.Do(badSQLReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = badSQLResp.Body.Close()

	grepBadReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/m.txt?grep=abc&limit=NaN", nil)
	grepBadResp, err := http.DefaultClient.Do(grepBadReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = grepBadResp.Body.Close()

	findBadReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/m.txt?find=1&newer=bad-date", nil)
	findBadResp, err := http.DefaultClient.Do(findBadReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = findBadResp.Body.Close()

	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	if !strings.Contains(text, "dat9_http_requests_total") {
		t.Fatalf("expected requests metric in response: %s", text)
	}
	if !strings.Contains(text, `route="/v1/fs/*"`) {
		t.Fatalf("expected fs route metric label in response: %s", text)
	}
	if !strings.Contains(text, "dat9_http_inflight_requests") {
		t.Fatalf("expected inflight metric in response: %s", text)
	}
	if !strings.Contains(text, `dat9_service_operations_total{component="backend",operation="exec_sql",result="ok"}`) {
		t.Fatalf("expected backend service metric in response: %s", text)
	}
	if !strings.Contains(text, `dat9_http_request_duration_seconds_bucket{method="GET",route="/v1/fs/*",le="0.1"}`) {
		t.Fatalf("expected http duration histogram bucket in response: %s", text)
	}
	if !strings.Contains(text, `dat9_service_operation_duration_seconds_bucket{component="backend",operation="exec_sql",result="ok",le="0.01"}`) {
		t.Fatalf("expected service operation histogram bucket in response: %s", text)
	}
	if !strings.Contains(text, `dat9_tenant_events_total{event="fs_write",result="ok"}`) {
		t.Fatalf("expected fs_write tenant event metric in response: %s", text)
	}
	if !strings.Contains(text, `dat9_tenant_events_total{event="fs_read",result="ok"}`) {
		t.Fatalf("expected fs_read tenant event metric in response: %s", text)
	}
	if !strings.Contains(text, `dat9_tenant_events_total{event="tenant_provision",result="error"}`) {
		t.Fatalf("expected tenant_provision error metric in response: %s", text)
	}
}

func TestUploadActionMetrics(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	completeReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/nonexistent/complete", nil)
	completeResp, err := http.DefaultClient.Do(completeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for upload complete, got %d", completeResp.StatusCode)
	}

	resumeReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/nonexistent/resume", nil)
	resumeResp, err := http.DefaultClient.Do(resumeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = resumeResp.Body.Close()
	if resumeResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for upload resume, got %d", resumeResp.StatusCode)
	}

	abortReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/uploads/nonexistent", nil)
	abortResp, err := http.DefaultClient.Do(abortReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = abortResp.Body.Close()
	if abortResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for upload abort, got %d", abortResp.StatusCode)
	}

	uploadsReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/uploads", nil)
	uploadsResp, err := http.DefaultClient.Do(uploadsReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = uploadsResp.Body.Close()
	if uploadsResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for uploads list without path, got %d", uploadsResp.StatusCode)
	}

	uploadActionReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/nonexistent/unknown", nil)
	uploadActionResp, err := http.DefaultClient.Do(uploadActionReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = uploadActionResp.Body.Close()
	if uploadActionResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown upload action, got %d", uploadActionResp.StatusCode)
	}

	metricsResp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metricsResp.Body.Close() }()
	body, _ := io.ReadAll(metricsResp.Body)
	text := string(body)

	if !strings.Contains(text, `dat9_tenant_events_total{event="upload_complete",result="error"}`) {
		t.Fatalf("expected upload_complete metric, got: %s", text)
	}
	if !strings.Contains(text, `dat9_tenant_events_total{event="upload_resume",result="error"}`) {
		t.Fatalf("expected upload_resume metric, got: %s", text)
	}
	if !strings.Contains(text, `dat9_tenant_events_total{event="upload_abort",result="error"}`) {
		t.Fatalf("expected upload_abort metric, got: %s", text)
	}
}
