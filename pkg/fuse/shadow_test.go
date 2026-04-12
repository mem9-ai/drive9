package fuse

import (
	"bytes"
	"testing"
)

func TestShadowStoreWriteExtentsReadAt(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Create a write buffer with some dirty data.
	wb := NewWriteBuffer("/test/file.txt", 0, 0)
	wb.Write(0, []byte("hello world"))

	// Write extents to shadow.
	if err := ss.WriteExtents("/test/file.txt", wb, 42); err != nil {
		t.Fatal(err)
	}

	// Verify size.
	if sz := ss.Size("/test/file.txt"); sz != 11 {
		t.Errorf("size = %d, want 11", sz)
	}

	// Read back.
	buf := make([]byte, 11)
	n, err := ss.ReadAt("/test/file.txt", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 11 {
		t.Errorf("read n = %d, want 11", n)
	}
	if !bytes.Equal(buf, []byte("hello world")) {
		t.Errorf("data = %q, want %q", buf, "hello world")
	}

	// Verify base rev.
	if rev := ss.BaseRev("/test/file.txt"); rev != 42 {
		t.Errorf("baseRev = %d, want 42", rev)
	}
}

func TestShadowStorePartialWrite(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write buffer with data in two separate parts.
	wb := NewWriteBuffer("/partial.txt", 0, 1024) // 1KB part size
	wb.Write(0, bytes.Repeat([]byte("A"), 1024))   // Part 0 — full
	wb.Write(1024, bytes.Repeat([]byte("B"), 500))  // Part 1 — partial

	if err := ss.WriteExtents("/partial.txt", wb, 1); err != nil {
		t.Fatal(err)
	}

	// Read part 0.
	buf0 := make([]byte, 1024)
	n, _ := ss.ReadAt("/partial.txt", 0, buf0)
	if n != 1024 {
		t.Errorf("read n = %d, want 1024", n)
	}
	if !bytes.Equal(buf0, bytes.Repeat([]byte("A"), 1024)) {
		t.Error("part 0 data mismatch")
	}

	// Read part 1.
	buf1 := make([]byte, 500)
	n, _ = ss.ReadAt("/partial.txt", 1024, buf1)
	if n != 500 {
		t.Errorf("read n = %d, want 500", n)
	}
	if !bytes.Equal(buf1, bytes.Repeat([]byte("B"), 500)) {
		t.Error("part 1 data mismatch")
	}
}

func TestShadowStoreRemove(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/remove.txt", 0, 0)
	wb.Write(0, []byte("data"))
	ss.WriteExtents("/remove.txt", wb, 1)

	if !ss.Has("/remove.txt") {
		t.Error("expected shadow file to exist")
	}

	ss.Remove("/remove.txt")

	if ss.Has("/remove.txt") {
		t.Error("expected shadow file to be removed")
	}
}

func TestShadowStoreRename(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/old.txt", 0, 0)
	wb.Write(0, []byte("renamed"))
	ss.WriteExtents("/old.txt", wb, 1)

	ok := ss.Rename("/old.txt", "/new.txt")
	if !ok {
		t.Fatal("expected rename to succeed")
	}

	if ss.Has("/old.txt") {
		t.Error("expected old path to be gone")
	}

	buf := make([]byte, 7)
	n, err := ss.ReadAt("/new.txt", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 || !bytes.Equal(buf, []byte("renamed")) {
		t.Errorf("read after rename = %q, want %q", buf[:n], "renamed")
	}
}

func TestShadowStoreReadAll(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/readall.txt", 0, 0)
	wb.Write(0, []byte("full content"))
	ss.WriteExtents("/readall.txt", wb, 1)

	data, err := ss.ReadAll("/readall.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte("full content")) {
		t.Errorf("ReadAll = %q, want %q", data, "full content")
	}
}

func TestShadowStoreCheckDiskSpace(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Just verify the function doesn't panic.
	_ = ss.CheckDiskSpace()
}
