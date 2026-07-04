package client

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
)

// archiveMockServer is a minimal httptest harness that serves a remote tree
// (listings + inline file bodies) for ArchiveDir tests.
type archiveMockServer struct {
	mu          sync.Mutex
	listEntries map[string][]FileInfo
	fileBodies  map[string][]byte
}

func newArchiveMockServer(files map[string]string) *archiveMockServer {
	m := &archiveMockServer{
		listEntries: map[string][]FileInfo{},
		fileBodies:  map[string][]byte{},
	}
	dirSeen := map[string]map[string]bool{}
	addChild := func(dir, name string, info FileInfo) {
		if dirSeen[dir] == nil {
			dirSeen[dir] = map[string]bool{}
		}
		if dirSeen[dir][name] {
			return
		}
		dirSeen[dir][name] = true
		m.listEntries[dir] = append(m.listEntries[dir], info)
	}
	for p, body := range files {
		m.fileBodies[p] = []byte(body)
		dir := "/"
		rel := strings.TrimPrefix(p, "/")
		segs := strings.Split(rel, "/")
		for i, seg := range segs {
			isLast := i == len(segs)-1
			if isLast {
				addChild(dir, seg, FileInfo{Name: seg, Size: int64(len(body)), IsDir: false, HasMode: true, Mode: 0o644})
				break
			}
			var child string
			if dir == "/" {
				child = "/" + seg
			} else {
				child = dir + "/" + seg
			}
			addChild(dir, seg, FileInfo{Name: seg, IsDir: true, HasMode: true, Mode: 0o755})
			if m.listEntries[child] == nil {
				m.listEntries[child] = []FileInfo{}
			}
			dir = child
		}
	}
	return m
}

func (m *archiveMockServer) server(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/fs/") && r.URL.Query().Has("list"):
			p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			entries := m.listEntries[p]
			out := make([]map[string]any, len(entries))
			for i, e := range entries {
				out[i] = map[string]any{
					"name": e.Name, "size": e.Size, "isDir": e.IsDir,
					"mode": e.Mode, "hasMode": e.HasMode,
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			if body, ok := m.fileBodies[p]; ok {
				_, _ = w.Write(body)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestArchiveDirTarGzBasicTree(t *testing.T) {
	mock := newArchiveMockServer(map[string]string{
		"/proj/README.md":        "hello world\n",
		"/proj/src/main.go":      "package main\n",
		"/proj/src/util/util.go": "package util\n",
	})
	srv := mock.server(t)
	defer srv.Close()
	c := New(srv.URL, "")
	c.smallFileThreshold = DefaultSmallFileThreshold

	var buf bytes.Buffer
	if err := c.ArchiveDir(context.Background(), "/proj", &buf, ArchiveOptions{}); err != nil {
		t.Fatalf("ArchiveDir: %v", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		name := hdr.Name
		if hdr.Typeflag == tar.TypeDir {
			name = strings.TrimSuffix(name, "/") + "/"
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, want := range []string{"proj/", "proj/README.md", "proj/src/", "proj/src/main.go", "proj/src/util/", "proj/src/util/util.go"} {
		if !containsStr(names, want) {
			t.Fatalf("archive missing %q; got %v", want, names)
		}
	}
}

func TestArchiveDirExcludeSkipsNodeModules(t *testing.T) {
	mock := newArchiveMockServer(map[string]string{
		"/proj/src/app.go":            "package src\n",
		"/proj/node_modules/react/x.js": "module.exports\n",
	})
	srv := mock.server(t)
	defer srv.Close()
	c := New(srv.URL, "")
	c.smallFileThreshold = DefaultSmallFileThreshold

	var buf bytes.Buffer
	if err := c.ArchiveDir(context.Background(), "/proj", &buf, ArchiveOptions{
		Exclude: []string{"**/node_modules/**"},
	}); err != nil {
		t.Fatalf("ArchiveDir: %v", err)
	}
	gz, _ := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		if strings.Contains(hdr.Name, "node_modules") {
			t.Fatalf("excluded node_modules leaked into archive: %q", hdr.Name)
		}
		if hdr.Name == "proj/src/app.go" {
			return
		}
	}
	t.Fatal("app.go missing from archive")
}

func TestArchiveDirIncludeWhitelist(t *testing.T) {
	mock := newArchiveMockServer(map[string]string{
		"/proj/src/app.go":  "package src\n",
		"/proj/README.md":   "# readme\n",
		"/proj/docs/guide.md": "# guide\n",
	})
	srv := mock.server(t)
	defer srv.Close()
	c := New(srv.URL, "")
	c.smallFileThreshold = DefaultSmallFileThreshold

	var buf bytes.Buffer
	if err := c.ArchiveDir(context.Background(), "/proj", &buf, ArchiveOptions{
		Include: []string{"docs/**", "README.md"},
	}); err != nil {
		t.Fatalf("ArchiveDir: %v", err)
	}
	gz, _ := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var foundGuide, foundReadme bool
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		if strings.HasSuffix(hdr.Name, ".go") {
			t.Fatalf("include whitelist should drop .go files, got %q", hdr.Name)
		}
		if hdr.Name == "proj/docs/guide.md" {
			foundGuide = true
		}
		if hdr.Name == "proj/README.md" {
			foundReadme = true
		}
	}
	if !foundGuide {
		t.Fatal("guide.md missing")
	}
	if !foundReadme {
		t.Fatal("README.md missing")
	}
}

func TestArchiveDirZipFormat(t *testing.T) {
	mock := newArchiveMockServer(map[string]string{
		"/proj/a.txt":   "AAA",
		"/proj/b/c.txt": "CCC",
	})
	srv := mock.server(t)
	defer srv.Close()
	c := New(srv.URL, "")
	c.smallFileThreshold = DefaultSmallFileThreshold

	var buf bytes.Buffer
	if err := c.ArchiveDir(context.Background(), "/proj", &buf, ArchiveOptions{Format: ArchiveFormatZip}); err != nil {
		t.Fatalf("ArchiveDir: %v", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("zip reader: %v", err)
	}
	var names []string
	for _, f := range zr.File {
		names = append(names, f.Name)
	}
	if !containsStr(names, "proj/a.txt") || !containsStr(names, "proj/b/c.txt") {
		t.Fatalf("zip entries = %v; want a.txt and b/c.txt", names)
	}
}

func TestArchiveDirFlatStripsHierarchy(t *testing.T) {
	mock := newArchiveMockServer(map[string]string{
		"/proj/src/deep/nested/a.go": "package nested\n",
		"/proj/b.go":                 "package main\n",
	})
	srv := mock.server(t)
	defer srv.Close()
	c := New(srv.URL, "")
	c.smallFileThreshold = DefaultSmallFileThreshold

	var buf bytes.Buffer
	if err := c.ArchiveDir(context.Background(), "/proj", &buf, ArchiveOptions{Flat: true}); err != nil {
		t.Fatalf("ArchiveDir: %v", err)
	}
	gz, _ := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		names = append(names, hdr.Name)
	}
	if !containsStr(names, "a.go") || !containsStr(names, "b.go") {
		t.Fatalf("flat entries = %v; want a.go and b.go", names)
	}
	for _, n := range names {
		if strings.Contains(n, "/") {
			t.Fatalf("flat mode should emit basenames only, got %q", n)
		}
	}
}

func TestArchiveDirInvalidFormat(t *testing.T) {
	c := New("http://127.0.0.1:1", "")
	var buf bytes.Buffer
	err := c.ArchiveDir(context.Background(), "/proj", &buf, ArchiveOptions{Format: ArchiveFormat("rar")})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "tar.gz or zip") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestArchiveDirInvalidPattern(t *testing.T) {
	c := New("http://127.0.0.1:1", "")
	var buf bytes.Buffer
	err := c.ArchiveDir(context.Background(), "/proj", &buf, ArchiveOptions{
		Exclude: []string{"**/../bad/**"},
	})
	if err == nil {
		t.Fatal("expected error for invalid pattern")
	}
}

func containsStr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}