package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
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
