package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/s3client"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newTestServer(t *testing.T) *Server {
	return newTestServerWithLogger(t, nil)
}

func newTestServerWithLogger(t *testing.T, log *zap.Logger) *Server {
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
	return NewWithConfig(Config{Backend: b, Logger: log})
}

func insertTestS3File(t *testing.T, s *Server, p string, size int64) {
	t.Helper()
	now := time.Now().UTC()
	fileID := "test-s3-" + strings.Trim(pathutil.BaseName(p), ".")
	store := s.fallback.Store()
	if err := store.InsertFile(context.Background(), &datastore.File{
		FileID:                fileID,
		StorageType:           datastore.StorageS3,
		StorageRef:            "blobs/" + fileID,
		StorageEncryptionMode: datastore.StorageEncryptionLegacy,
		SizeBytes:             size,
		Revision:              1,
		Status:                datastore.StatusConfirmed,
		CreatedAt:             now,
		ConfirmedAt:           &now,
	}); err != nil {
		t.Fatalf("insert s3 file: %v", err)
	}
	if err := store.EnsureParentDirs(context.Background(), p, func() string { return "node-parent-" + fileID }); err != nil {
		t.Fatalf("ensure parent dirs: %v", err)
	}
	if err := store.InsertNode(context.Background(), &datastore.FileNode{
		NodeID:     "node-" + fileID,
		Path:       p,
		ParentPath: pathutil.ParentPath(p),
		Name:       pathutil.BaseName(p),
		FileID:     fileID,
		CreatedAt:  now,
	}); err != nil {
		t.Fatalf("insert s3 node: %v", err)
	}
}

func insertHistoricalRootDentry(t *testing.T, s *Server, inodeID string, mode uint32) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	store := s.fallback.Store()
	if err := store.InsertInode(ctx, &datastore.Inode{
		InodeID:   inodeID,
		SizeBytes: 0,
		Revision:  1,
		Mode:      mode,
		Status:    datastore.StatusConfirmed,
		CreatedAt: now,
		Mtime:     now,
	}); err != nil {
		t.Fatalf("insert root inode: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, inode_id, created_at)
		VALUES (?, ?, ?, ?, 1, ?, ?)`,
		"node-"+inodeID, "/", "/", "root-alias", inodeID, now); err != nil {
		t.Fatalf("insert historical root dentry: %v", err)
	}
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

func TestWriteReturnsCommittedRevision(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Create a new file — revision should be 1.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/data/rev.txt", strings.NewReader("v1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create: status %d, body %s", resp.StatusCode, body)
	}
	var result struct {
		Revision int64 `json:"revision"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal create response: %v (body: %s)", err, body)
	}
	if result.Revision != 1 {
		t.Fatalf("create revision = %d, want 1", result.Revision)
	}

	// Overwrite the file — revision should increment to 2.
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/data/rev.txt", strings.NewReader("v2"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("overwrite: status %d, body %s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal overwrite response: %v (body: %s)", err, body)
	}
	if result.Revision != 2 {
		t.Fatalf("overwrite revision = %d, want 2", result.Revision)
	}
}

func TestWriteEmitsBenchPhaseTiming(t *testing.T) {
	logger.ResetBenchTimingLogEnabledForTest()
	t.Cleanup(logger.ResetBenchTimingLogEnabledForTest)
	t.Setenv("DRIVE9_BENCH_TIMING_LOG_ENABLED", "true")

	core, recorded := observer.New(zap.InfoLevel)
	s := newTestServerWithLogger(t, zap.New(core))
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/timing.txt", strings.NewReader("timing body"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status = %d, body %s", resp.StatusCode, body)
	}

	assertObservedTimingFields(t, recorded, "server_write_timing",
		"path", "result", "bytes", "body_read_ms", "backend_write_ms", "response_ms", "total_ms")
	assertObservedTimingFields(t, recorded, "backend_write_timing",
		"path", "canonical_path", "operation", "stat_ms", "implementation_ms", "total_ms")
	assertObservedTimingFields(t, recorded, "backend_write_create_timing",
		"path", "result", "prepare_ms", "tenant_tx_ms", "central_quota_ms", "total_ms")
	assertObservedTimingFields(t, recorded, "central_quota_mutation_timing",
		"mutation_type", "result", "insert_log_ms", "apply_tx_ms", "total_ms")
}

func assertObservedTimingFields(t *testing.T, recorded *observer.ObservedLogs, message string, fields ...string) {
	t.Helper()
	entries := recorded.FilterMessage(message).AllUntimed()
	if len(entries) == 0 {
		t.Fatalf("missing log message %q", message)
	}
	ctx := entries[0].ContextMap()
	for _, field := range fields {
		if _, ok := ctx[field]; !ok {
			t.Fatalf("%s missing field %q; got fields %#v", message, field, ctx)
		}
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

func TestGitObjectPackAPIRoundTrip(t *testing.T) {
	s := newTestServer(t)
	initServerGitObjectPackTestSchema(t, s)
	ts := httptest.NewServer(s)
	defer ts.Close()

	c := client.New(ts.URL, "")
	ctx := context.Background()
	workspaceID := "ws1"

	content := []byte("small inline pack")
	sum := sha256.Sum256(content)
	wantPackID := hex.EncodeToString(sum[:])
	pack, err := c.PutGitObjectPack(ctx, workspaceID, client.GitObjectPackRequest{Content: content})
	if err != nil {
		t.Fatalf("PutGitObjectPack: %v", err)
	}
	if pack.PackID != wantPackID || pack.ChecksumSHA256 != wantPackID {
		t.Fatalf("pack ids = %q/%q, want %q", pack.PackID, pack.ChecksumSHA256, wantPackID)
	}
	if pack.SizeBytes != int64(len(content)) {
		t.Fatalf("pack size = %d, want %d", pack.SizeBytes, len(content))
	}
	if len(pack.Content) != 0 {
		t.Fatalf("upsert response included content, want metadata only")
	}

	packs, err := c.ListGitObjectPacks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ListGitObjectPacks: %v", err)
	}
	if len(packs) != 1 || packs[0].PackID != wantPackID {
		t.Fatalf("packs = %+v, want one pack %s", packs, wantPackID)
	}
	if len(packs[0].Content) != 0 {
		t.Fatalf("list response included content, want metadata only")
	}

	downloaded, err := c.GetGitObjectPack(ctx, workspaceID, wantPackID)
	if err != nil {
		t.Fatalf("GetGitObjectPack: %v", err)
	}
	if string(downloaded.Content) != string(content) {
		t.Fatalf("downloaded content = %q, want %q", downloaded.Content, content)
	}
}

func initServerGitObjectPackTestSchema(t *testing.T, s *Server) {
	t.Helper()
	db := s.fallback.Store().DB()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS git_workspace_object_packs (
			workspace_id    VARCHAR(64) NOT NULL,
			pack_id         VARCHAR(64) NOT NULL,
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			content_blob    LONGBLOB,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (workspace_id, pack_id)
		)`,
		`CREATE INDEX idx_git_object_packs_created ON git_workspace_object_packs(workspace_id, created_at)`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			if strings.Contains(err.Error(), "Duplicate key name") {
				continue
			}
			t.Fatal(err)
		}
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
			Name       string `json:"name"`
			IsDir      bool   `json:"isDir"`
			ResourceID string `json:"resource_id"`
			Nlink      uint32 `json:"nlink"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result.Entries))
	}
	if result.Entries[0].ResourceID == "" {
		t.Fatal("list entry resource_id is empty")
	}
	if result.Entries[0].Nlink != 1 {
		t.Fatalf("list entry nlink = %d, want 1", result.Entries[0].Nlink)
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

func TestBatchStatReturnsPerPathResults(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/data/a.txt", strings.NewReader("data"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/data/dir?mkdir", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir: %d", resp.StatusCode)
	}

	body := `{"paths":["/data/a.txt","/data/dir","/missing.txt","/data/a.txt","/bad\\path"]}`
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-stat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		response, _ := io.ReadAll(resp.Body)
		t.Fatalf("batch stat status = %d, body %s", resp.StatusCode, response)
	}
	var out struct {
		Results []struct {
			Path     string `json:"path"`
			Status   int    `json:"status"`
			Error    string `json:"error"`
			Size     int64  `json:"size"`
			IsDir    bool   `json:"isDir"`
			Revision int64  `json:"revision"`
			Mtime    int64  `json:"mtime"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 5 {
		t.Fatalf("results len = %d, want 5", len(out.Results))
	}
	if out.Results[0].Status != http.StatusOK || out.Results[0].Size != 4 || out.Results[0].IsDir || out.Results[0].Revision != 1 || out.Results[0].Mtime == 0 {
		t.Fatalf("file result = %+v, want ok file metadata", out.Results[0])
	}
	if out.Results[1].Status != http.StatusOK || !out.Results[1].IsDir {
		t.Fatalf("dir result = %+v, want ok directory metadata", out.Results[1])
	}
	if out.Results[2].Status != http.StatusNotFound || out.Results[2].Error == "" {
		t.Fatalf("missing result = %+v, want per-path 404", out.Results[2])
	}
	if out.Results[3].Status != http.StatusOK || out.Results[3].Path != "/data/a.txt" {
		t.Fatalf("duplicate result = %+v, want duplicate preserved", out.Results[3])
	}
	if out.Results[4].Status != http.StatusBadRequest || out.Results[4].Error == "" {
		t.Fatalf("invalid path result = %+v, want per-path 400", out.Results[4])
	}
}

func TestBatchStatEmptyAndTooLargeInputs(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-stat", strings.NewReader(`{"paths":[]}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var empty struct {
		Results []struct{} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&empty); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("empty status = %d, want 200", resp.StatusCode)
	}
	if len(empty.Results) != 0 {
		t.Fatalf("empty results len = %d, want 0", len(empty.Results))
	}

	paths := make([]string, maxBatchStatPaths+1)
	for i := range paths {
		paths[i] = "/x.txt"
	}
	payload, err := json.Marshal(map[string][]string{"paths": paths})
	if err != nil {
		t.Fatal(err)
	}
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-stat", strings.NewReader(string(payload)))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("too-large status = %d, want 400", resp.StatusCode)
	}
}

func TestBatchReadSmallReturnsPerPathResults(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/data/a.txt", strings.NewReader("hello"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/data/dir?mkdir", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir: %d", resp.StatusCode)
	}

	body := `{"paths":["/data/a.txt","/missing.txt","/bad\\path","/data/dir","/data/a.txt"],"max_bytes":16}`
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-read-small", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		response, _ := io.ReadAll(resp.Body)
		t.Fatalf("batch read-small status = %d, body %s", resp.StatusCode, response)
	}
	var out struct {
		Results []struct {
			Path     string `json:"path"`
			Status   int    `json:"status"`
			Error    string `json:"error"`
			Data     []byte `json:"data"`
			Size     int64  `json:"size"`
			Revision int64  `json:"revision"`
			Mtime    int64  `json:"mtime"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 5 {
		t.Fatalf("results len = %d, want 5", len(out.Results))
	}
	if out.Results[0].Status != http.StatusOK || string(out.Results[0].Data) != "hello" || out.Results[0].Size != 5 || out.Results[0].Revision != 1 || out.Results[0].Mtime == 0 {
		t.Fatalf("file result = %+v, want ok file data", out.Results[0])
	}
	if out.Results[1].Status != http.StatusNotFound || out.Results[1].Error == "" {
		t.Fatalf("missing result = %+v, want per-path 404", out.Results[1])
	}
	if out.Results[2].Status != http.StatusBadRequest || out.Results[2].Error == "" {
		t.Fatalf("invalid path result = %+v, want per-path 400", out.Results[2])
	}
	if out.Results[3].Status != http.StatusNotFound || out.Results[3].Error == "" {
		t.Fatalf("directory result = %+v, want per-path 404 matching GET semantics", out.Results[3])
	}
	if out.Results[4].Status != http.StatusOK || out.Results[4].Path != "/data/a.txt" || string(out.Results[4].Data) != "hello" {
		t.Fatalf("duplicate result = %+v, want duplicate preserved", out.Results[4])
	}
}

func TestBatchReadSmallEmptyTooLargeAndFileTooLarge(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-read-small", strings.NewReader(`{"paths":[]}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var empty struct {
		Results []struct{} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&empty); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("empty status = %d, want 200", resp.StatusCode)
	}
	if len(empty.Results) != 0 {
		t.Fatalf("empty results len = %d, want 0", len(empty.Results))
	}

	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/data/large.txt", strings.NewReader("hello"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write large candidate: %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-read-small", strings.NewReader(`{"paths":["/data/large.txt"],"max_bytes":4}`))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var tooLargeFile struct {
		Results []struct {
			Status int    `json:"status"`
			Error  string `json:"error"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tooLargeFile); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("file-too-large batch status = %d, want 200", resp.StatusCode)
	}
	if len(tooLargeFile.Results) != 1 || tooLargeFile.Results[0].Status != http.StatusRequestEntityTooLarge || tooLargeFile.Results[0].Error == "" {
		t.Fatalf("file-too-large result = %+v, want per-path 413", tooLargeFile.Results)
	}

	insertTestS3File(t, s, "/data/s3.txt", 1)
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-read-small", strings.NewReader(`{"paths":["/data/s3.txt"],"max_bytes":50000}`))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var nonInlineFile struct {
		Results []struct {
			Status int    `json:"status"`
			Error  string `json:"error"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nonInlineFile); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("non-inline batch status = %d, want 200", resp.StatusCode)
	}
	if len(nonInlineFile.Results) != 1 ||
		nonInlineFile.Results[0].Status != http.StatusRequestEntityTooLarge ||
		nonInlineFile.Results[0].Error != "file is not available for inline read" {
		t.Fatalf("non-inline result = %+v, want per-path 413 inline-storage error", nonInlineFile.Results)
	}

	paths := make([]string, maxBatchReadSmallPaths+1)
	for i := range paths {
		paths[i] = "/x.txt"
	}
	payload, err := json.Marshal(map[string][]string{"paths": paths})
	if err != nil {
		t.Fatal(err)
	}
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs:batch-read-small", strings.NewReader(string(payload)))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("too-large status = %d, want 400", resp.StatusCode)
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
		ResourceID   string            `json:"resource_id"`
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
	if out.ResourceID == "" {
		t.Fatalf("expected resource_id, got %+v", out)
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

	mkdirReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/dir?mkdir", nil)
	mkdirResp, err := http.DefaultClient.Do(mkdirReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = mkdirResp.Body.Close()
	if mkdirResp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir: %d", mkdirResp.StatusCode)
	}

	dirResp, err := http.Get(ts.URL + "/v1/fs/dir?stat=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dirResp.Body.Close() }()
	if dirResp.StatusCode != http.StatusOK {
		t.Fatalf("dir stat metadata: %d", dirResp.StatusCode)
	}
	var dirOut struct {
		IsDir      bool   `json:"isdir"`
		ResourceID string `json:"resource_id"`
	}
	if err := json.NewDecoder(dirResp.Body).Decode(&dirOut); err != nil {
		t.Fatalf("decode dir metadata: %v", err)
	}
	if !dirOut.IsDir || dirOut.ResourceID == "" {
		t.Fatalf("expected directory resource_id, got %+v", dirOut)
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

func TestStatRoot(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("stat root: expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Dat9-IsDir") != "true" {
		t.Errorf("expected X-Dat9-IsDir true, got %s", resp.Header.Get("X-Dat9-IsDir"))
	}
	if resp.Header.Get("Content-Length") != "0" {
		t.Errorf("expected Content-Length 0, got %s", resp.Header.Get("Content-Length"))
	}
}

func TestStatMetadataRoot(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/fs/?stat=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("stat metadata root: expected 200, got %d", resp.StatusCode)
	}
	var body struct {
		IsDir bool              `json:"isdir"`
		Mtime *int64            `json:"mtime"`
		Tags  map[string]string `json:"tags"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.IsDir {
		t.Error("expected isdir true")
	}
	if body.Mtime == nil || *body.Mtime != 0 {
		t.Errorf("expected mtime 0, got %v", body.Mtime)
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

func TestStatDirectoryReturnsMode(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Create a directory with a specific mode (0o700 = 448 decimal)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/modeddir?mkdir&mode=448", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir: %d", resp.StatusCode)
	}

	// Stat the directory
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/modeddir/", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat dir: %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Dat9-IsDir") != "true" {
		t.Errorf("expected X-Dat9-IsDir true, got %s", resp.Header.Get("X-Dat9-IsDir"))
	}
	modeHdr := resp.Header.Get("X-Dat9-Mode")
	if modeHdr == "" {
		t.Error("expected X-Dat9-Mode header for directory, got empty")
	}
	if modeHdr != "448" { // 0o700 = 448 decimal
		t.Errorf("expected X-Dat9-Mode 448, got %s", modeHdr)
	}
}

func TestSymlinkRoundTrip(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	reqBody := strings.NewReader(`{"target":"../target.txt"}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/link?symlink=1", reqBody)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("symlink: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/link", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat link: %d", resp.StatusCode)
	}
	wantMode := strconv.FormatUint(uint64(uint32(syscall.S_IFLNK)|0o777), 10)
	if got := resp.Header.Get("X-Dat9-Mode"); got != wantMode {
		t.Fatalf("X-Dat9-Mode = %s, want %s", got, wantMode)
	}
	if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len("../target.txt")) {
		t.Fatalf("Content-Length = %s, want %d", got, len("../target.txt"))
	}

	resp, err = http.Get(ts.URL + "/v1/fs/link")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read link payload: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "../target.txt" {
		t.Fatalf("read link payload = %q, want ../target.txt", body)
	}
}

func TestChmodRootHistoricalDentryReturnsBadRequest(t *testing.T) {
	s := newTestServer(t)
	const inodeID = "root-inode"
	const originalMode = 0o755
	insertHistoricalRootDentry(t, s, inodeID, originalMode)

	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/?chmod=1", strings.NewReader(`{"mode":448}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	var mode uint32
	if err := s.fallback.Store().DB().QueryRowContext(context.Background(), `SELECT mode FROM inodes WHERE inode_id = ?`, inodeID).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != originalMode {
		t.Fatalf("root inode mode = %o, want unchanged %o", mode, originalMode)
	}
}

func TestPatchAndAppendRootReturnBadRequest(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	cases := []struct {
		name   string
		method string
		url    string
		body   string
	}{
		{
			name:   "patch",
			method: http.MethodPatch,
			url:    ts.URL + "/v1/fs/",
			body:   `{"new_size":1,"dirty_parts":[1]}`,
		},
		{
			name:   "append",
			method: http.MethodPost,
			url:    ts.URL + "/v1/fs/?append=1",
			body:   `{"append_size":1}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestSymlinkRejectsOversizeTarget(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	target := strings.Repeat("x", backend.MaxSymlinkTargetBytes+1)
	reqBody := strings.NewReader(`{"target":"` + target + `"}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/too-long-link?symlink=1", reqBody)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("symlink oversize target status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/too-long-link", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("oversize symlink should not be inserted; stat status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestSymlinkRejectsOversizeBody(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	reqBody := strings.NewReader(`{"target":"` + strings.Repeat("x", maxSymlinkBodyBytes) + `"}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/body-too-large-link?symlink=1", reqBody)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("symlink oversize body status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func TestSymlinkAllowsMaxTargetWithWorstCaseJSONEscaping(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	target := strings.Repeat("\x01", backend.MaxSymlinkTargetBytes)
	body, err := json.Marshal(map[string]string{"target": target})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > maxSymlinkBodyBytes {
		t.Fatalf("test body length = %d exceeds symlink body cap %d", len(body), maxSymlinkBodyBytes)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/escaped-max-link?symlink=1", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("symlink escaped max target status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}
}

func TestSymlinkReturns507WhenTenantStorageQuotaExceeded(t *testing.T) {
	s, _ := newTestServerWithS3Config(t, backend.Options{MaxTenantStorageBytes: 10}, SemanticWorkerOptions{})
	ts := httptest.NewServer(s)
	defer ts.Close()

	reqBody := strings.NewReader(`{"target":"12345678901"}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/quota-link?symlink=1", reqBody)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInsufficientStorage {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("symlink quota status = %d, want %d: %s", resp.StatusCode, http.StatusInsufficientStorage, body)
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

func TestDeleteKindHint(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/typed.txt", strings.NewReader("data"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status = %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/typed-dir?mkdir", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir status = %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/v1/fs/typed.txt?kind=file", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("file delete status = %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/v1/fs/typed-dir?kind=dir", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dir delete status = %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/v1/fs/missing?kind=bogus", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid kind status = %d, want 400", resp.StatusCode)
	}
}

func TestCreateFileActionCreatesEmptyFileAndConflicts(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/empty.txt?create", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("create Content-Type = %q, want application/json", got)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/empty.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Content-Length") != "0" {
		t.Fatalf("Content-Length = %q, want 0", resp.Header.Get("Content-Length"))
	}
	if resp.Header.Get("X-Dat9-Revision") != "1" {
		t.Fatalf("X-Dat9-Revision = %q, want 1", resp.Header.Get("X-Dat9-Revision"))
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat status = %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/empty.txt?create", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate create status = %d, want 409", resp.StatusCode)
	}

	var inodeRows int
	if err := s.fallback.Store().DB().QueryRow(`SELECT COUNT(*) FROM inodes`).Scan(&inodeRows); err != nil {
		t.Fatal(err)
	}
	if inodeRows != 1 {
		t.Fatalf("inode rows after duplicate create = %d, want 1", inodeRows)
	}
	var orphanRows int
	if err := s.fallback.Store().DB().QueryRow(`SELECT COUNT(*)
		FROM inodes i
		LEFT JOIN file_nodes fn ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id
		WHERE fn.node_id IS NULL`).Scan(&orphanRows); err != nil {
		t.Fatal(err)
	}
	if orphanRows != 0 {
		t.Fatalf("orphan inodes after duplicate create = %d, want 0", orphanRows)
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
	if got := provisionBody["tenant_id"]; got != "local" {
		t.Fatalf("tenant_id = %q, want local", got)
	}
	if got := provisionBody["status"]; got != "provisioning" {
		t.Fatalf("status = %q, want provisioning", got)
	}
	if len(provisionBody) != 3 {
		t.Fatalf("provision body keys = %v, want only tenant_id/api_key/status", provisionBody)
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

func TestHardlinkRoundTrip(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/src.txt", strings.NewReader("shared"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write src: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/dst.txt?hardlink=1", nil)
	req.Header.Set("X-Dat9-Hardlink-Source", "/src.txt")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("hardlink: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/src.txt", nil)
	srcStat, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = srcStat.Body.Close()
	if srcStat.StatusCode != http.StatusOK {
		t.Fatalf("stat src: %d", srcStat.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/dst.txt", nil)
	dstStat, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = dstStat.Body.Close()
	if dstStat.StatusCode != http.StatusOK {
		t.Fatalf("stat dst: %d", dstStat.StatusCode)
	}
	if srcStat.Header.Get("X-Dat9-Resource-ID") == "" {
		t.Fatal("src resource id is empty")
	}
	if srcStat.Header.Get("X-Dat9-Resource-ID") != dstStat.Header.Get("X-Dat9-Resource-ID") {
		t.Fatalf("resource ids differ: src=%q dst=%q",
			srcStat.Header.Get("X-Dat9-Resource-ID"), dstStat.Header.Get("X-Dat9-Resource-ID"))
	}
	if got := srcStat.Header.Get("X-Dat9-Nlink"); got != "2" {
		t.Fatalf("src nlink = %q, want 2", got)
	}
	if got := dstStat.Header.Get("X-Dat9-Nlink"); got != "2" {
		t.Fatalf("dst nlink = %q, want 2", got)
	}

	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/dst.txt", strings.NewReader("updated"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write dst: %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/src.txt", nil)
	srcStatAfter, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = srcStatAfter.Body.Close()
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/dst.txt", nil)
	dstStatAfter, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = dstStatAfter.Body.Close()
	if srcStatAfter.Header.Get("X-Dat9-Resource-ID") != dstStatAfter.Header.Get("X-Dat9-Resource-ID") {
		t.Fatalf("resource ids differ after overwrite: src=%q dst=%q",
			srcStatAfter.Header.Get("X-Dat9-Resource-ID"), dstStatAfter.Header.Get("X-Dat9-Resource-ID"))
	}
	if got := srcStatAfter.Header.Get("X-Dat9-Nlink"); got != "2" {
		t.Fatalf("src nlink after overwrite = %q, want 2", got)
	}
	if got := dstStatAfter.Header.Get("X-Dat9-Nlink"); got != "2" {
		t.Fatalf("dst nlink after overwrite = %q, want 2", got)
	}
	resp, err = http.Get(ts.URL + "/v1/fs/src.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "updated" {
		t.Fatalf("src body = %q, want updated", body)
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

func TestRenameReplacesExistingFile(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/config", strings.NewReader("old"))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write config: %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/config.lock", strings.NewReader("new config"))
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write config.lock: %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/config?rename", nil)
	req.Header.Set("X-Dat9-Rename-Source", "/config.lock")
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rename replace: %d", resp.StatusCode)
	}

	resp, _ = http.Get(ts.URL + "/v1/fs/config")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "new config" {
		t.Fatalf("config = %q, want new config", body)
	}
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/v1/fs/config.lock", nil)
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("config.lock status = %d, want 404", resp.StatusCode)
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
	if !strings.Contains(text, "drive9_http_requests_total") {
		t.Fatalf("expected requests metric in response: %s", text)
	}
	if !strings.Contains(text, `route="/v1/fs/*"`) {
		t.Fatalf("expected fs route metric label in response: %s", text)
	}
	if !strings.Contains(text, "drive9_http_inflight_requests") {
		t.Fatalf("expected inflight metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_http_inflight_requests{route="/v1/fs/*"} 0.000000`) {
		t.Fatalf("expected route-scoped inflight metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_service_operations_total{component="backend",operation="exec_sql",result="ok"}`) {
		t.Fatalf("expected backend service metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_http_request_duration_seconds_bucket{method="GET",route="/v1/fs/*",le="0.1"}`) {
		t.Fatalf("expected http duration histogram bucket in response: %s", text)
	}
	if !strings.Contains(text, `drive9_service_operation_duration_seconds_bucket{component="backend",operation="exec_sql",result="ok",le="0.01"}`) {
		t.Fatalf("expected service operation histogram bucket in response: %s", text)
	}
	if !strings.Contains(text, `drive9_db_operations_total{operation="`) || !strings.Contains(text, `role="user"`) {
		t.Fatalf("expected user db operation metric in response: %s", text)
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "drive9_db_") && strings.Contains(line, `tenant_id="`) {
			t.Fatalf("expected db operation metrics to avoid tenant_id labels: %s", line)
		}
	}
	if !strings.Contains(text, `drive9_db_pool_registered{role="user"}`) {
		t.Fatalf("expected user db pool metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_tenant_requests_total{action="write",result="ok",status="200",status_class="2xx",surface="fs",tenant_id="local"}`) {
		t.Fatalf("expected tenant request usage metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_tenant_request_duration_seconds_bucket{action="read",result="ok",status="200",status_class="2xx",surface="fs",tenant_id="local",le="0.1"}`) {
		t.Fatalf("expected tenant request duration usage metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_tenant_inflight_requests{action="read",surface="fs",tenant_id="local"} 0.000000`) {
		t.Fatalf("expected tenant in-flight usage metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_tenant_http_bytes_total{action="write",direction="request",surface="fs",tenant_id="local"}`) {
		t.Fatalf("expected tenant HTTP byte metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_tenant_file_bytes_total{action="write",direction="write",surface="fs",tenant_id="local"}`) {
		t.Fatalf("expected tenant file write byte metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_tenant_file_bytes_total{action="read",direction="read",surface="fs",tenant_id="local"}`) {
		t.Fatalf("expected tenant file read byte metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_business_events_total{event="fs_write",result="ok"}`) {
		t.Fatalf("expected fs_write tenant event metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_business_events_total{event="fs_read",result="ok"}`) {
		t.Fatalf("expected fs_read tenant event metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_business_events_total{event="tenant_provision",result="error"}`) {
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

	if !strings.Contains(text, `drive9_business_events_total{event="upload_complete",result="error"}`) {
		t.Fatalf("expected upload_complete metric, got: %s", text)
	}
	if !strings.Contains(text, `drive9_business_events_total{event="upload_resume",result="error"}`) {
		t.Fatalf("expected upload_resume metric, got: %s", text)
	}
	if !strings.Contains(text, `drive9_business_events_total{event="upload_abort",result="error"}`) {
		t.Fatalf("expected upload_abort metric, got: %s", text)
	}
}
