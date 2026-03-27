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
	"github.com/mem9-ai/dat9/pkg/meta"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()

	blobDir, err := os.MkdirTemp("", "dat9-srv-blobs-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(blobDir) })

	store, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	b, err := backend.New(store, blobDir)
	if err != nil {
		t.Fatal(err)
	}
	return New(Config{Backend: b})
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
	resp, err := http.Get(ts.URL + "/v1/fs/data/?list")
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
