package fuse

import (
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestOpenHandleIndexTracksByInodeAndPath(t *testing.T) {
	idx := NewOpenHandleIndex()
	fh1 := &FileHandle{Ino: 10, Path: "/a.txt"}
	fh2 := &FileHandle{Ino: 11, Path: "/a.txt"}

	idx.Add(fh1)
	idx.Add(fh2)

	if !idx.Has(10, "") {
		t.Fatal("index missing inode 10")
	}
	if !idx.Has(0, "/a.txt") {
		t.Fatal("index missing /a.txt")
	}
	if got := len(idx.SnapshotPath("/a.txt")); got != 2 {
		t.Fatalf("SnapshotPath count = %d, want 2", got)
	}

	idx.Remove(fh1)
	if idx.Has(10, "") {
		t.Fatal("inode 10 remained after remove")
	}
	if !idx.Has(0, "/a.txt") {
		t.Fatal("path removed while another handle is still open")
	}

	idx.Remove(fh2)
	if idx.Has(11, "/a.txt") {
		t.Fatal("handle remained after final remove")
	}
}

func TestOpenHandleIndexRenamePathPrefix(t *testing.T) {
	idx := NewOpenHandleIndex()
	file := &FileHandle{Ino: 10, Path: "/dir/a.txt"}
	child := &FileHandle{Ino: 11, Path: "/dir/sub/b.txt"}
	other := &FileHandle{Ino: 12, Path: "/dir-other/c.txt"}
	idx.Add(file)
	idx.Add(child)
	idx.Add(other)

	retargeted := idx.RenamePathPrefix("/dir", "/renamed")

	if idx.Has(0, "/dir/a.txt") || idx.Has(0, "/dir/sub/b.txt") {
		t.Fatal("old paths remained indexed after rename")
	}
	if !idx.Has(0, "/renamed/a.txt") {
		t.Fatal("renamed file path missing from index")
	}
	if !idx.Has(0, "/renamed/sub/b.txt") {
		t.Fatal("renamed child path missing from index")
	}
	if retargeted[file] != "/renamed/a.txt" {
		t.Fatalf("retargeted file path = %q, want /renamed/a.txt", retargeted[file])
	}
	if p, ok := idx.Path(child); !ok || p != "/renamed/sub/b.txt" {
		t.Fatalf("Path(child) = %q, %t; want /renamed/sub/b.txt, true", p, ok)
	}
	if !idx.Has(0, "/dir-other/c.txt") {
		t.Fatal("unrelated prefix path was changed")
	}
}

func TestDat9FSFileHandleIndexLifecycle(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	fh := &FileHandle{Ino: 20, Path: "/b.txt"}
	fhID := fs.allocateFileHandle(fh)
	if !fs.hasOpenHandle(20, "/b.txt") {
		t.Fatal("allocated file handle was not indexed")
	}

	fs.deleteFileHandle(fhID, fh)
	if fs.hasOpenHandle(20, "/b.txt") {
		t.Fatal("deleted file handle remained indexed")
	}
	if _, ok := fs.fileHandles.Get(fhID); ok {
		t.Fatal("deleted file handle remained in handle table")
	}
}

func TestDat9FSFinishLocalRenameUpdatesOpenHandleIndex(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	ino := fs.inodes.Lookup("/old.txt", false, 4, time.Now())
	fh := &FileHandle{Ino: ino, Path: "/old.txt", Dirty: NewWriteBuffer("/old.txt", 0, 0)}
	fh.Streamer = NewStreamUploader(fs.client, "/old.txt", -1, fs.remoteRoot())
	fh.Prefetch = NewPrefetcher(fs.client, fs.remotePath("/old.txt"), 4)
	fhID := fs.allocateFileHandle(fh)
	defer fs.deleteFileHandle(fhID, fh)

	fs.finishLocalRename(&gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "/old.txt", "/new.txt")

	if fs.hasOpenHandle(0, "/old.txt") {
		t.Fatal("old path remained indexed after finishLocalRename")
	}
	if !fs.hasOpenHandle(0, "/new.txt") {
		t.Fatal("new path was not indexed after finishLocalRename")
	}
	if fh.Path != "/new.txt" {
		t.Fatalf("file handle path = %q, want /new.txt", fh.Path)
	}
	if fh.Dirty.path != "/new.txt" {
		t.Fatalf("dirty buffer path = %q, want /new.txt", fh.Dirty.path)
	}
	if fh.Streamer.path != "/new.txt" {
		t.Fatalf("streamer path = %q, want /new.txt", fh.Streamer.path)
	}
	if fh.Streamer.remotePath != "/new.txt" {
		t.Fatalf("streamer remote path = %q, want /new.txt", fh.Streamer.remotePath)
	}
	if got := fh.Prefetch.pathString(); got != "/new.txt" {
		t.Fatalf("prefetch path = %q, want /new.txt", got)
	}
}
