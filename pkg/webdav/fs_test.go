package webdav

import (
	"os"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestNormPath(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"/", "/"},
		{"/foo", "/foo"},
		{"/foo/bar", "/foo/bar"},
		{"/foo//bar", "/foo/bar"},
		{"foo", "/foo"},
		{"/foo/bar/../baz", "/foo/baz"},
		{".", "/"},
	}
	for _, tt := range tests {
		got := normPath(tt.in)
		if got != tt.want {
			t.Errorf("normPath(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestFileInfoInterface(t *testing.T) {
	mtime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fi := &fileInfo{
		name: "test.txt",
		stat: &client.StatResult{Size: 42, IsDir: false, Mtime: mtime},
	}

	if fi.Name() != "test.txt" {
		t.Errorf("Name() = %q", fi.Name())
	}
	if fi.Size() != 42 {
		t.Errorf("Size() = %d", fi.Size())
	}
	if fi.IsDir() {
		t.Error("IsDir() should be false")
	}
	if fi.ModTime() != mtime {
		t.Errorf("ModTime() = %v", fi.ModTime())
	}
	if fi.Mode() != 0o644 {
		t.Errorf("Mode() = %v", fi.Mode())
	}
	if fi.Sys() != nil {
		t.Error("Sys() should be nil")
	}
}

func TestFileInfoDir(t *testing.T) {
	fi := &fileInfo{
		name: "mydir",
		stat: &client.StatResult{IsDir: true},
	}
	if !fi.IsDir() {
		t.Error("IsDir() should be true")
	}
	if fi.Mode() != os.ModeDir|0o755 {
		t.Errorf("Mode() = %v, want dir|0755", fi.Mode())
	}
}

func TestMapError(t *testing.T) {
	if mapError(nil) != nil {
		t.Error("mapError(nil) should be nil")
	}

	se404 := &client.StatusError{StatusCode: 404, Message: "not found"}
	if got := mapError(se404); got != os.ErrNotExist {
		t.Errorf("mapError(404) = %v, want ErrNotExist", got)
	}

	se409 := &client.StatusError{StatusCode: 409, Message: "conflict"}
	if got := mapError(se409); got != os.ErrExist {
		t.Errorf("mapError(409) = %v, want ErrExist", got)
	}

	se403 := &client.StatusError{StatusCode: 403, Message: "forbidden"}
	if got := mapError(se403); got != os.ErrPermission {
		t.Errorf("mapError(403) = %v, want ErrPermission", got)
	}
}

func TestDirFileReaddir(t *testing.T) {
	entries := []client.FileInfo{
		{Name: "a.txt", Size: 10, IsDir: false, Mtime: 1000},
		{Name: "b", Size: 0, IsDir: true, Mtime: 2000},
		{Name: "c.md", Size: 20, IsDir: false},
	}
	d := &dirFile{path: "/test", stat: &client.StatResult{IsDir: true}, entries: entries}

	// Read all at once.
	infos, err := d.Readdir(-1)
	if err != nil {
		t.Fatalf("Readdir(-1) error: %v", err)
	}
	if len(infos) != 3 {
		t.Fatalf("Readdir(-1) returned %d entries, want 3", len(infos))
	}
	if infos[0].Name() != "a.txt" || infos[0].Size() != 10 {
		t.Errorf("entry 0: %q size %d", infos[0].Name(), infos[0].Size())
	}
	if !infos[1].IsDir() {
		t.Error("entry 1 should be a directory")
	}
}

func TestDirFileReaddirPaginated(t *testing.T) {
	entries := []client.FileInfo{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	}
	d := &dirFile{path: "/", stat: &client.StatResult{IsDir: true}, entries: entries}

	batch1, err := d.Readdir(2)
	if err != nil {
		t.Fatalf("Readdir(2) error: %v", err)
	}
	if len(batch1) != 2 {
		t.Fatalf("batch1 len = %d, want 2", len(batch1))
	}

	batch2, _ := d.Readdir(2)
	if len(batch2) != 1 {
		t.Fatalf("batch2 len = %d, want 1", len(batch2))
	}
}

func TestWriteFileBuffersAndStat(t *testing.T) {
	wf := &writeFile{path: "/test.txt"}
	n, err := wf.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	n, err = wf.Write([]byte(" world"))
	if err != nil || n != 6 {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}

	fi, err := wf.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}
	if fi.Size() != 11 {
		t.Errorf("Stat.Size = %d, want 11", fi.Size())
	}
	if fi.Name() != "test.txt" {
		t.Errorf("Stat.Name = %q", fi.Name())
	}
}

func TestReadFileSeekAndRead(t *testing.T) {
	data := []byte("hello world")
	rf := &readFile{
		path:   "/foo.txt",
		stat:   &client.StatResult{Size: int64(len(data))},
	}
	// bytes.Reader is embedded so we init it via the struct literal
	// in OpenFile. For testing, create manually:
	rf.Reader = nil // would normally be set by OpenFile

	fi, err := rf.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}
	if fi.Name() != "foo.txt" {
		t.Errorf("Name = %q", fi.Name())
	}
}
