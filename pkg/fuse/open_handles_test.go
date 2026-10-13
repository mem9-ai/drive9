package fuse

// Ports of v1 pkg/fuse/open_handles_test.go rows for the facade's
// OpenHandleIndex. bypass_special_test.go already pins the
// create/register/release lifecycle; these add the by-inode+by-path tracks
// with shared paths, the conservative OR semantics of Has, the full
// rename-prefix rebinding map, and the unlink-anonymization/relink round
// trip (v1 unlink-survivor bookkeeping).
//
// Not ported: TestDat9FSFinishLocalRenameUpdatesOpenHandleIndex and the two
// TestDat9FSClearReadTargetsForPath* rows — their subjects (FileHandle
// streamer/prefetch/read-target rewiring) moved into the writeback
// extension with the data plane; the facade's rename hook is the
// openIdx.RenamePathPrefix call covered below, and read-target clearing is
// event-driven inside the cache extension.

import (
	"testing"
)

func TestOpenHandleIndexTracksByInodeAndPath(t *testing.T) {
	idx := NewOpenHandleIndex()
	fh1 := &fileHandle{ino: 10, path: "/a.txt"}
	fh2 := &fileHandle{ino: 11, path: "/a.txt"}

	idx.Add(fh1)
	idx.Add(fh2)

	if !idx.Has(10, "") {
		t.Fatal("index missing inode 10")
	}
	if !idx.HasOpenInode(10) {
		t.Fatal("HasOpenInode missing inode 10")
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
	if idx.HasOpenPath("/a.txt") {
		t.Fatal("HasOpenPath remained true after final remove")
	}
}

func TestOpenHandleIndexHasUsesConservativeOR(t *testing.T) {
	idx := NewOpenHandleIndex()
	idx.Add(&fileHandle{ino: 10, path: "/a.txt"})
	idx.Add(&fileHandle{ino: 11, path: "/b.txt"})

	if !idx.Has(10, "/b.txt") {
		t.Fatal("Has should match an open inode or an open path")
	}
	if !idx.Has(11, "/a.txt") {
		t.Fatal("Has should preserve OR semantics when both keys are supplied")
	}
	if idx.Has(99, "/missing.txt") {
		t.Fatal("Has matched unrelated inode and path")
	}
}

func TestOpenHandleIndexRenamePathPrefix(t *testing.T) {
	idx := NewOpenHandleIndex()
	file := &fileHandle{ino: 10, path: "/dir/a.txt"}
	child := &fileHandle{ino: 11, path: "/dir/sub/b.txt"}
	other := &fileHandle{ino: 12, path: "/dir-other/c.txt"}
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

// v1 unlink-survivor rows: UnlinkPath anonymizes every handle of the path
// (Has by path and by inode stop counting it; Path reports none) and
// RelinkPath rebinds the survivors onto a fresh incarnation of the name.
func TestOpenHandleIndexUnlinkAnonymizesAndRelinkRestores(t *testing.T) {
	idx := NewOpenHandleIndex()
	fh := &fileHandle{ino: 10, path: "/a.txt"}
	idx.Add(fh)

	idx.UnlinkPath("/a.txt")

	if idx.HasOpenPath("/a.txt") {
		t.Fatal("unlinked path still indexed by path")
	}
	if idx.Has(10, "") || idx.Has(10, "/a.txt") {
		t.Fatal("anonymous handle still counted as open")
	}
	if p, ok := idx.Path(fh); ok || p != "" {
		t.Fatalf("anonymous Path = %q %v, want empty/false", p, ok)
	}
	// The inode track itself is intact for a later rebind.
	if got := idx.SnapshotInode(10); len(got) != 1 || got[0] != fh {
		t.Fatalf("SnapshotInode after unlink = %v, want the survivor", got)
	}

	idx.RelinkPath("/a.txt", []*fileHandle{fh})

	if !idx.Has(10, "/a.txt") {
		t.Fatal("relinked handle not indexed again")
	}
	if p, ok := idx.Path(fh); !ok || p != "/a.txt" {
		t.Fatalf("relinked Path = %q %v, want /a.txt true", p, ok)
	}
}
