package backend

import (
	"io"
	"os"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/meta"
)

func newTestBackend(t *testing.T) *Dat9Backend {
	t.Helper()
	dbFile, err := os.CreateTemp("", "dat9-*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbFile.Close()
	t.Cleanup(func() { os.Remove(dbFile.Name()) })

	blobDir, err := os.MkdirTemp("", "dat9-blobs-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(blobDir) })

	store, err := meta.Open(dbFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	b, err := New(store, blobDir)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var _ filesystem.FileSystem = (*Dat9Backend)(nil)

func TestCreateAndStat(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Create("/hello.txt"); err != nil {
		t.Fatal(err)
	}
	info, err := b.Stat("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "hello.txt" || info.IsDir || info.Size != 0 {
		t.Errorf("unexpected: %+v", info)
	}
}

func TestWriteAndRead(t *testing.T) {
	b := newTestBackend(t)
	n, err := b.Write("/data/file.txt", []byte("hello world"), 0, filesystem.WriteFlagCreate)
	if err != nil {
		t.Fatal(err)
	}
	if n != 11 {
		t.Errorf("expected 11 bytes, got %d", n)
	}
	data, err := b.Read("/data/file.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q", data)
	}
}

func TestWriteOverwrite(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/f.txt", []byte("old"), 0, filesystem.WriteFlagCreate)
	b.Write("/f.txt", []byte("new"), 0, filesystem.WriteFlagTruncate)
	data, _ := b.Read("/f.txt", 0, -1)
	if string(data) != "new" {
		t.Errorf("got %q", data)
	}
}

func TestWriteAppend(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/f.txt", []byte("hello"), 0, filesystem.WriteFlagCreate)
	b.Write("/f.txt", []byte(" world"), 0, filesystem.WriteFlagAppend)
	data, _ := b.Read("/f.txt", 0, -1)
	if string(data) != "hello world" {
		t.Errorf("got %q", data)
	}
}

func TestReadWithOffset(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/f.txt", []byte("hello world"), 0, filesystem.WriteFlagCreate)
	data, _ := b.Read("/f.txt", 6, 5)
	if string(data) != "world" {
		t.Errorf("got %q", data)
	}
}

func TestMkdirAndReadDir(t *testing.T) {
	b := newTestBackend(t)
	b.Mkdir("/data", 0o755)
	b.Write("/data/a.txt", []byte("a"), 0, filesystem.WriteFlagCreate)
	b.Write("/data/b.txt", []byte("bb"), 0, filesystem.WriteFlagCreate)

	entries, err := b.ReadDir("/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
	if entries[0].Name != "a.txt" || entries[1].Name != "b.txt" {
		t.Errorf("unexpected: %+v", entries)
	}
}

func TestRemove(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/f.txt", []byte("data"), 0, filesystem.WriteFlagCreate)
	if err := b.Remove("/f.txt"); err != nil {
		t.Fatal(err)
	}
	_, err := b.Stat("/f.txt")
	if err != meta.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRemoveAll(t *testing.T) {
	b := newTestBackend(t)
	b.Mkdir("/data", 0o755)
	b.Write("/data/a.txt", []byte("a"), 0, filesystem.WriteFlagCreate)
	b.Write("/data/b.txt", []byte("b"), 0, filesystem.WriteFlagCreate)
	if err := b.RemoveAll("/data/"); err != nil {
		t.Fatal(err)
	}
	_, err := b.Stat("/data/")
	if err != meta.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRename(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/old.txt", []byte("data"), 0, filesystem.WriteFlagCreate)
	if err := b.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat("/old.txt"); err != meta.ErrNotFound {
		t.Error("old path should be gone")
	}
	data, _ := b.Read("/new.txt", 0, -1)
	if string(data) != "data" {
		t.Errorf("got %q", data)
	}
}

func TestZeroCopyCp(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/a.txt", []byte("shared"), 0, filesystem.WriteFlagCreate)
	if err := b.CopyFile("/a.txt", "/b.txt"); err != nil {
		t.Fatal(err)
	}
	dataA, _ := b.Read("/a.txt", 0, -1)
	dataB, _ := b.Read("/b.txt", 0, -1)
	if string(dataA) != string(dataB) {
		t.Error("content mismatch")
	}
	// Delete one, other survives
	b.Remove("/a.txt")
	dataB, err := b.Read("/b.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(dataB) != "shared" {
		t.Errorf("got %q", dataB)
	}
}

func TestAutoCreateParentDirs(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/a/b/c/file.txt", []byte("deep"), 0, filesystem.WriteFlagCreate)
	for _, p := range []string{"/a/", "/a/b/", "/a/b/c/"} {
		info, err := b.Stat(p)
		if err != nil {
			t.Errorf("expected dir %s: %v", p, err)
			continue
		}
		if !info.IsDir {
			t.Errorf("%s should be dir", p)
		}
	}
}

func TestEnsureParentDirsNoRootSelfInsert(t *testing.T) {
	b := newTestBackend(t)
	// Creating a file at root level should not insert "/" as a child of itself
	b.Write("/top.txt", []byte("x"), 0, filesystem.WriteFlagCreate)
	entries, err := b.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == "/" || e.Name == "" {
			t.Errorf("root dir should not appear as its own child: %+v", e)
		}
	}
}

func TestOffsetWritePreservesTail(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/f.txt", []byte("ABCDEFGH"), 0, filesystem.WriteFlagCreate)
	// Overwrite bytes 2-4 with "XY", should preserve tail "EFGH"
	b.Write("/f.txt", []byte("XY"), 2, 0)
	data, err := b.Read("/f.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "ABXYEFGH" {
		t.Errorf("expected ABXYEFGH, got %q", string(data))
	}
}

func TestRenameDirUpdatesName(t *testing.T) {
	b := newTestBackend(t)
	b.Mkdir("/alpha", 0o755)
	b.Write("/alpha/file.txt", []byte("data"), 0, filesystem.WriteFlagCreate)
	if err := b.Rename("/alpha/", "/beta/"); err != nil {
		t.Fatal(err)
	}
	info, err := b.Stat("/beta/")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "beta" {
		t.Errorf("expected name 'beta', got %q", info.Name)
	}
}

func TestRenameDirEnsuresParentDirs(t *testing.T) {
	b := newTestBackend(t)
	b.Mkdir("/src", 0o755)
	b.Write("/src/file.txt", []byte("data"), 0, filesystem.WriteFlagCreate)
	// Rename to a deeply nested path whose parents don't exist
	if err := b.Rename("/src/", "/x/y/dst/"); err != nil {
		t.Fatal(err)
	}
	// Parent dirs /x/ and /x/y/ should have been auto-created
	for _, p := range []string{"/x/", "/x/y/"} {
		info, err := b.Stat(p)
		if err != nil {
			t.Errorf("expected parent dir %s to exist: %v", p, err)
			continue
		}
		if !info.IsDir {
			t.Errorf("expected %s to be a directory", p)
		}
	}
	// The renamed dir and its contents should be accessible
	data, err := b.Read("/x/y/dst/file.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data" {
		t.Errorf("expected 'data', got %q", string(data))
	}
}

func TestOpenAndOpenWrite(t *testing.T) {
	b := newTestBackend(t)
	b.Write("/f.txt", []byte("content"), 0, filesystem.WriteFlagCreate)

	rc, _ := b.Open("/f.txt")
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "content" {
		t.Errorf("got %q", data)
	}

	wc, _ := b.OpenWrite("/f.txt")
	wc.Write([]byte("new content"))
	wc.Close()

	readData, _ := b.Read("/f.txt", 0, -1)
	if string(readData) != "new content" {
		t.Errorf("got %q", readData)
	}
}
