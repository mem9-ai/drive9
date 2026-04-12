package fuse

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPendingIndexPutGetRemove(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Put
	gen, err := idx.Put("/test/file.txt", 1024, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if gen == 0 {
		t.Error("expected non-zero generation")
	}

	// GetMeta
	meta, ok := idx.GetMeta("/test/file.txt")
	if !ok {
		t.Fatal("expected pending entry")
	}
	if meta.Path != "/test/file.txt" {
		t.Errorf("path = %s, want /test/file.txt", meta.Path)
	}
	if meta.Size != 1024 {
		t.Errorf("size = %d, want 1024", meta.Size)
	}
	if meta.Kind != PendingNew {
		t.Errorf("kind = %d, want PendingNew", meta.Kind)
	}

	// HasPending
	if !idx.HasPending("/test/file.txt") {
		t.Error("expected HasPending to be true")
	}
	if idx.HasPending("/nonexistent") {
		t.Error("expected HasPending to be false for nonexistent path")
	}

	// Remove
	idx.Remove("/test/file.txt")
	_, ok = idx.GetMeta("/test/file.txt")
	if ok {
		t.Error("expected entry to be removed")
	}

	// .meta file should be removed from disk
	metaPath := filepath.Join(dir, hashPath("/test/file.txt")+".meta")
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("expected .meta file to be removed from disk")
	}
}

func TestPendingIndexRenamePending(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.PutWithBaseRev("/old/file.txt", 512, PendingOverwrite, 11)
	if err != nil {
		t.Fatal(err)
	}

	ok := idx.RenamePending("/old/file.txt", "/new/file.txt")
	if !ok {
		t.Fatal("expected rename to succeed")
	}

	// Old path should be gone
	if idx.HasPending("/old/file.txt") {
		t.Error("expected old path to be removed")
	}

	// New path should exist with same size
	meta, ok := idx.GetMeta("/new/file.txt")
	if !ok {
		t.Fatal("expected new path to exist")
	}
	if meta.Size != 512 {
		t.Errorf("size = %d, want 512", meta.Size)
	}
	if meta.Kind != PendingOverwrite {
		t.Errorf("kind = %d, want PendingOverwrite", meta.Kind)
	}
	if meta.BaseRev != 11 {
		t.Errorf("baseRev = %d, want 11", meta.BaseRev)
	}
}

func TestPendingIndexRecoverFromDisk(t *testing.T) {
	dir := t.TempDir()

	// Create an index and put some entries
	idx1, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx1.Put("/recover/a.txt", 100, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx1.Put("/recover/b.txt", 200, PendingOverwrite)
	if err != nil {
		t.Fatal(err)
	}

	// Create a new index and recover from disk
	idx2, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}

	meta, ok := idx2.GetMeta("/recover/a.txt")
	if !ok {
		t.Fatal("expected /recover/a.txt after recovery")
	}
	if meta.Size != 100 {
		t.Errorf("a.txt size = %d, want 100", meta.Size)
	}

	meta, ok = idx2.GetMeta("/recover/b.txt")
	if !ok {
		t.Fatal("expected /recover/b.txt after recovery")
	}
	if meta.Size != 200 {
		t.Errorf("b.txt size = %d, want 200", meta.Size)
	}
}

func TestPendingIndexListPendingPaths(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Empty
	if paths := idx.ListPendingPaths(); paths != nil {
		t.Error("expected nil for empty index")
	}

	_, _ = idx.Put("/a.txt", 10, PendingNew)
	_, _ = idx.Put("/b.txt", 20, PendingNew)

	paths := idx.ListPendingPaths()
	if len(paths) != 2 {
		t.Errorf("expected 2 paths, got %d", len(paths))
	}
	if _, ok := paths["/a.txt"]; !ok {
		t.Error("expected /a.txt in paths")
	}
	if _, ok := paths["/b.txt"]; !ok {
		t.Error("expected /b.txt in paths")
	}
}

func TestPendingIndexListByPrefix(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = idx.Put("/dir/a.txt", 10, PendingNew)
	_, _ = idx.Put("/dir/b.txt", 20, PendingNew)
	_, _ = idx.Put("/other/c.txt", 30, PendingNew)

	results := idx.ListByPrefix("/dir/")
	if len(results) != 2 {
		t.Errorf("expected 2 results for /dir/ prefix, got %d", len(results))
	}
}

func TestPendingIndexCount(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	if idx.Count() != 0 {
		t.Errorf("expected count 0, got %d", idx.Count())
	}

	_, _ = idx.Put("/a.txt", 10, PendingNew)
	if idx.Count() != 1 {
		t.Errorf("expected count 1, got %d", idx.Count())
	}

	_, _ = idx.Put("/b.txt", 20, PendingNew)
	if idx.Count() != 2 {
		t.Errorf("expected count 2, got %d", idx.Count())
	}

	idx.Remove("/a.txt")
	if idx.Count() != 1 {
		t.Errorf("expected count 1 after remove, got %d", idx.Count())
	}
}

func TestPendingIndexRenameNonexistent(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	ok := idx.RenamePending("/nonexistent", "/new")
	if ok {
		t.Error("expected rename of nonexistent path to return false")
	}
}
