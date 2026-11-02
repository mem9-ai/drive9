package mockserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func newTestServer(t *testing.T) (*Server, *httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		ts.Close()
		_ = s.Close()
	})
	return s, ts, dir
}

type client struct {
	t   *testing.T
	base string
	key string
}

func (c client) do(method, path string, body []byte, hdr map[string]string) (*http.Response, []byte) {
	c.t.Helper()
	var rd *bytes.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		c.t.Fatal(err)
	}
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

func provision(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	req, _ := http.NewRequest("POST", ts.URL+"/v1/provision", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("provision status = %d, want 202", resp.StatusCode)
	}
	var out struct {
		TenantID string `json:"tenant_id"`
		APIKey   string `json:"api_key"`
		Status   string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "active" || out.APIKey == "" || out.TenantID == "" {
		t.Fatalf("provision response incomplete: %+v", out)
	}
	return out.APIKey
}

func TestLifecycleAndFSDomain(t *testing.T) {
	_, ts, _ := newTestServer(t)

	// healthz needs no auth
	resp, _ := http.Get(ts.URL + "/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	resp.Body.Close()

	key := provision(t, ts)
	c := client{t: t, base: ts.URL, key: key}

	// unauthenticated is rejected
	bad := client{t: t, base: ts.URL}
	if r, _ := bad.do("GET", "/v1/fs/a.txt", nil, nil); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET = %d, want 401", r.StatusCode)
	}

	// status
	r, body := c.do("GET", "/v1/status", nil, nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", r.StatusCode)
	}
	var st struct {
		Status          string `json:"status"`
		InlineThreshold int64  `json:"inline_threshold"`
	}
	if err := json.Unmarshal(body, &st); err != nil || st.Status != "active" || st.InlineThreshold <= 0 {
		t.Fatalf("status body = %s", body)
	}

	// mkdir → PUT → stat → list → read → range
	if r, _ = c.do("POST", "/v1/fs/dir/?mkdir=1", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("mkdir = %d", r.StatusCode)
	}
	payload := []byte("hello mock backend")
	r, _ = c.do("PUT", "/v1/fs/dir/a.txt", payload, map[string]string{"Content-Type": "application/octet-stream"})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d", r.StatusCode)
	}
	r, body = c.do("GET", "/v1/fs/dir/a.txt?stat=1", nil, nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("stat = %d", r.StatusCode)
	}
	var stat map[string]any
	if err := json.Unmarshal(body, &stat); err != nil || stat["size"].(float64) != float64(len(payload)) || stat["isdir"] != false {
		t.Fatalf("stat body = %s", body)
	}
	if r.Header.Get("X-Dat9-Mtime") == "" {
		t.Fatal("stat missing X-Dat9-Mtime")
	}
	r, body = c.do("GET", "/v1/fs/dir/?list=1", nil, nil)
	var list struct {
		Entries []map[string]any `json:"entries"`
	}
	if err := json.Unmarshal(body, &list); err != nil || len(list.Entries) != 1 || list.Entries[0]["name"] != "a.txt" {
		t.Fatalf("list body = %s", body)
	}
	r, body = c.do("GET", "/v1/fs/dir/a.txt", nil, nil)
	if r.StatusCode != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("read = %d body=%q", r.StatusCode, body)
	}
	if r.Header.Get("X-Dat9-Revision") == "" || r.Header.Get("Accept-Ranges") != "bytes" {
		t.Fatal("read missing X-Dat9-* headers")
	}
	r, body = c.do("GET", "/v1/fs/dir/a.txt", nil, map[string]string{"Range": "bytes=6-9"})
	if r.StatusCode != http.StatusPartialContent || string(body) != "mock" {
		t.Fatalf("range = %d body=%q", r.StatusCode, body)
	}
	if r.Header.Get("Content-Range") != fmt.Sprintf("bytes 6-9/%d", len(payload)) {
		t.Fatalf("content-range = %q", r.Header.Get("Content-Range"))
	}
	if r, _ = c.do("GET", "/v1/fs/dir/a.txt", nil, map[string]string{"Range": "bytes=999-"}); r.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("bad range = %d, want 416", r.StatusCode)
	}

	// HEAD
	r, _ = c.do("HEAD", "/v1/fs/dir/a.txt", nil, nil)
	if r.StatusCode != http.StatusOK || r.Header.Get("X-Dat9-IsDir") != "false" || r.Header.Get("X-Dat9-Size") != fmt.Sprint(len(payload)) {
		t.Fatalf("HEAD headers: %+v", r.Header)
	}

	// rename + copy + 409 on existing target
	if r, _ = c.do("POST", "/v1/fs/dir/b.txt?rename=1", nil, map[string]string{"X-Dat9-Rename-Source": "/dir/a.txt"}); r.StatusCode != http.StatusOK {
		t.Fatalf("rename = %d", r.StatusCode)
	}
	if r, _ = c.do("POST", "/v1/fs/dir/c.txt?copy=1", nil, map[string]string{"X-Dat9-Copy-Source": "/dir/b.txt"}); r.StatusCode != http.StatusOK {
		t.Fatalf("copy = %d", r.StatusCode)
	}
	if r, _ = c.do("POST", "/v1/fs/dir/c.txt?rename=1", nil, map[string]string{"X-Dat9-Rename-Source": "/dir/b.txt"}); r.StatusCode != http.StatusConflict {
		t.Fatalf("rename onto existing = %d, want 409", r.StatusCode)
	}

	// delete: non-empty dir → 409, file → ok, missing → 404
	if r, _ = c.do("DELETE", "/v1/fs/dir/", nil, nil); r.StatusCode != http.StatusConflict {
		t.Fatalf("delete non-empty dir = %d, want 409", r.StatusCode)
	}
	if r, _ = c.do("DELETE", "/v1/fs/dir/b.txt", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("delete file = %d", r.StatusCode)
	}
	if r, _ = c.do("GET", "/v1/fs/dir/b.txt", nil, nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("read deleted = %d, want 404", r.StatusCode)
	}
	if r, _ = c.do("DELETE", "/v1/fs/dir/?recursive=1", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("recursive delete = %d", r.StatusCode)
	}
	if r, _ = c.do("GET", "/v1/fs/dir/?list=1", nil, nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("list deleted dir = %d, want 404", r.StatusCode)
	}
}

func TestUploadsV1(t *testing.T) {
	_, ts, _ := newTestServer(t)
	c := client{t: t, base: ts.URL, key: provision(t, ts)}
	if r, _ := c.do("POST", "/v1/fs/up/?mkdir=1", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatal("mkdir failed")
	}
	body, _ := json.Marshal(map[string]any{
		"path":          "/up/big.bin",
		"total_size":    10,
		"part_checksums": []string{"AAAA"},
	})
	r, out := c.do("POST", "/v1/uploads", body, map[string]string{"Content-Type": "application/json"})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("initiate = %d body=%s", r.StatusCode, out)
	}
	var plan struct {
		UploadID string `json:"upload_id"`
		Parts    []struct {
			Number int    `json:"number"`
			URL    string `json:"url"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(out, &plan); err != nil || len(plan.Parts) != 1 {
		t.Fatalf("plan = %s", out)
	}
	// missing part_checksums → 400
	bad, _ := json.Marshal(map[string]any{"path": "/up/x", "total_size": 5})
	if r, _ = c.do("POST", "/v1/uploads", bad, nil); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("no checksums = %d, want 400", r.StatusCode)
	}
	// PUT the part (absolute URL)
	req, _ := http.NewRequest("PUT", plan.Parts[0].URL, bytes.NewReader([]byte("0123456789")))
	req.Header.Set("Authorization", "Bearer "+c.key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("put part err=%v status=%v", err, resp)
	}
	resp.Body.Close()
	// complete
	if r, out = c.do("POST", "/v1/uploads/"+plan.UploadID+"/complete", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("complete = %d body=%s", r.StatusCode, out)
	}
	// assembled file is readable
	if r, out = c.do("GET", "/v1/fs/up/big.bin", nil, nil); r.StatusCode != http.StatusOK || string(out) != "0123456789" {
		t.Fatalf("assembled = %d %q", r.StatusCode, out)
	}
	// second complete → 410
	if r, _ = c.do("POST", "/v1/uploads/"+plan.UploadID+"/complete", nil, nil); r.StatusCode != http.StatusGone {
		t.Fatalf("recomplete = %d, want 410", r.StatusCode)
	}
}

func TestExtentMetaAndSQLAndCredential(t *testing.T) {
	_, ts, _ := newTestServer(t)
	c := client{t: t, base: ts.URL, key: provision(t, ts)}

	op := func(name string, in map[string]any) (map[string]any, *http.Response) {
		b, _ := json.Marshal(in)
		r, out := c.do("POST", "/v1/extent/meta", b, map[string]string{"X-Drive9-Extent-Op": name})
		var m map[string]any
		_ = json.Unmarshal(out, &m)
		return m, r
	}

	// mknod a file under the root (inode 1)
	m, r := op("mknod", map[string]any{"parent": 1, "name": "f.txt", "type": 2, "mode": 420})
	if r.StatusCode != http.StatusOK || m["errno"].(float64) != 0 {
		t.Fatalf("mknod = %d %v", r.StatusCode, m)
	}
	ino := uint64(m["inode"].(float64))
	// mknod again → EEXIST (17)
	if m, _ = op("mknod", map[string]any{"parent": 1, "name": "f.txt", "type": 2, "mode": 420}); m["errno"].(float64) != 17 {
		t.Fatalf("mknod dup errno = %v", m["errno"])
	}
	// lookup / getattr
	if m, _ = op("lookup", map[string]any{"parent": 1, "name": "f.txt"}); m["inode"].(float64) != float64(ino) {
		t.Fatalf("lookup = %v", m)
	}
	if m, _ = op("getattr", map[string]any{"inode": ino}); m["attr"].(map[string]any)["Typ"].(float64) != 2 {
		t.Fatalf("getattr = %v", m)
	}
	// unknown op → ENOSYS (38)
	if m, _ = op("flock", map[string]any{"inode": ino}); m["errno"].(float64) != 38 {
		t.Fatalf("flock errno = %v, want 38", m["errno"])
	}
	// write parts + read back + readdir
	parts := []map[string]any{{"off": 0, "slice": map[string]any{"id": 1, "size": 4}}}
	if m, _ = op("write", map[string]any{"inode": ino, "indx": 0, "parts": parts}); m["errno"].(float64) != 0 {
		t.Fatalf("write = %v", m)
	}
	if m, _ = op("read", map[string]any{"inode": ino, "indx": 0}); m["errno"].(float64) != 0 {
		t.Fatalf("read = %v", m)
	}
	if m, _ = op("readdir", map[string]any{"inode": 1}); len(m["entries"].([]any)) != 1 {
		t.Fatalf("readdir = %v", m)
	}

	// /v1/sql: black-box read of the mock's own tables
	sqlBody, _ := json.Marshal(map[string]string{"query": "SELECT COUNT(*) AS n FROM jfs_node WHERE inode = " + fmt.Sprint(ino)})
	r, out := c.do("POST", "/v1/sql", sqlBody, nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("sql = %d %s", r.StatusCode, out)
	}
	var rows []map[string]any
	if err := json.Unmarshal(out, &rows); err != nil || rows[0]["n"].(float64) != 1 {
		t.Fatalf("sql rows = %s", out)
	}
	// write via sql is rejected (read-only handle)
	sqlBody, _ = json.Marshal(map[string]string{"query": "DELETE FROM jfs_node"})
	if r, _ = c.do("POST", "/v1/sql", sqlBody, nil); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("sql write = %d, want 400", r.StatusCode)
	}

	// data-credential: file scheme by default
	r, out = c.do("POST", "/v1/data-credential", nil, nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("data-credential = %d", r.StatusCode)
	}
	var cred map[string]any
	if err := json.Unmarshal(out, &cred); err != nil || cred["scheme"] != "file" || cred["prefix"].(string) == "" {
		t.Fatalf("cred = %s", out)
	}
	if !bytes.Contains(out, []byte(`"prefix":"t/`)) {
		t.Fatalf("cred prefix not tenant-scoped: %s", out)
	}
}

func TestDataCredentialS3Scheme(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, S3Endpoint: "http://127.0.0.1:19000", S3Bucket: "b", S3AK: "ak", S3SK: "sk"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	c := client{t: t, base: ts.URL, key: provision(t, ts)}
	r, out := c.do("POST", "/v1/data-credential", nil, nil)
	var cred map[string]any
	if err := json.Unmarshal(out, &cred); err != nil || cred["scheme"] != "s3" || cred["endpoint"] != "http://127.0.0.1:19000" || cred["force_path_style"] != true {
		t.Fatalf("cred = %s", out)
	}
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", r.StatusCode)
	}
}

func TestEmbeddedS3Endpoint(t *testing.T) {
	_, ts, dir := newTestServer(t)
	c := client{t: t, base: ts.URL, key: provision(t, ts)}
	_ = c
	// bucket create + object roundtrip + list
	if r, _ := c.do("PUT", "/s3/drive9-mock", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatal("bucket create failed")
	}
	if r, _ := c.do("PUT", "/s3/drive9-mock/t%2Fx%2Fblock0", []byte("BLOCKDATA"), nil); r.StatusCode != http.StatusOK {
		t.Fatal("object put failed")
	}
	if r, out := c.do("GET", "/s3/drive9-mock/t%2Fx%2Fblock0", nil, nil); r.StatusCode != http.StatusOK || string(out) != "BLOCKDATA" {
		t.Fatalf("object get = %d %q", r.StatusCode, out)
	}
	if r, out := c.do("GET", "/s3/drive9-mock?list-type=2&prefix=t/x/", nil, nil); r.StatusCode != http.StatusOK || !bytes.Contains(out, []byte("t/x/block0")) {
		t.Fatalf("list = %d %s", r.StatusCode, out)
	}
	if r, _ := c.do("DELETE", "/s3/drive9-mock/t%2Fx%2Fblock0", nil, nil); r.StatusCode != http.StatusNoContent {
		t.Fatal("object delete failed")
	}
	// objects land under Dir/objects (the file-scheme data plane reads them)
	if _, err := os.Stat(filepath.Join(dir, "objects", "drive9-mock")); err != nil {
		t.Fatalf("objects dir missing: %v", err)
	}
}

func TestTenantDeleteInvalidatesKey(t *testing.T) {
	_, ts, _ := newTestServer(t)
	key := provision(t, ts)
	c := client{t: t, base: ts.URL, key: key}
	if r, _ := c.do("DELETE", "/v1/tenant", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("tenant delete = %d", r.StatusCode)
	}
	if r, _ := c.do("GET", "/v1/status", nil, nil); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("deleted key status = %d, want 401", r.StatusCode)
	}
}
