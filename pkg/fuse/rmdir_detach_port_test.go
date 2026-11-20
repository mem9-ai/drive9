package fuse

import (
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// A removed directory must leave the inode table the way an unlinked file
// does: its name is detached, so a later mkdir of the same name allocates a
// FRESH nodeid instead of handing the removed directory's nodeid to a
// different object. An open descriptor on the removed directory keeps the
// tombstone alive for fstat, but every enumeration through it answers ENOENT
// (Linux readdir-after-rmdir) — it must never silently list whatever occupies
// the recycled name.
func TestRmdirDetachesDirectoryInode(t *testing.T) {
	core := newMemCore()
	f := newTestFS(t, core)

	mkdir := func() gofuse.EntryOut {
		t.Helper()
		var out gofuse.EntryOut
		if st := f.Mkdir(nil, &gofuse.MkdirIn{InHeader: *rootHeader(), Mode: 0o755}, "d", &out); st != gofuse.OK {
			t.Fatalf("mkdir /d = %v", st)
		}
		return out
	}
	readdir := func(nodeid, fh uint64) gofuse.Status {
		t.Helper()
		out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
		return f.ReadDir(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: nodeid}, Fh: fh, Size: 4096}, out)
	}

	removed := mkdir()
	var opened gofuse.OpenOut
	if st := f.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: removed.NodeId}}, &opened); st != gofuse.OK {
		t.Fatalf("opendir removed.NodeId = %v", st)
	}
	if st := readdir(removed.NodeId, opened.Fh); st != gofuse.OK {
		t.Fatalf("readdir before rmdir = %v", st)
	}
	if st := f.Rmdir(nil, rootHeader(), "d"); st != gofuse.OK {
		t.Fatalf("rmdir /d = %v", st)
	}
	if _, ok := f.inodes.GetInode("/d"); ok {
		t.Fatal("rmdir left the removed directory's name bound in the inode table")
	}
	// Between the rmdir and a recreate the descriptor already answers ENOENT:
	// the name is gone remotely, and the handle must not fall back to a
	// pathname lookup.
	if st := readdir(removed.NodeId, opened.Fh); st != gofuse.ENOENT {
		t.Fatalf("readdir on the removed directory = %v, want ENOENT", st)
	}

	recreated := mkdir()
	if recreated.NodeId == removed.NodeId {
		t.Fatalf("mkdir after rmdir reused nodeid %d of the removed directory", removed.NodeId)
	}
	if ino, ok := f.inodes.GetInode("/d"); !ok || ino != recreated.NodeId {
		t.Fatalf("recreated /d maps to inode %d (ok=%v), want %d", ino, ok, recreated.NodeId)
	}
	// The recreated directory is only reachable through a descriptor opened
	// on ITS nodeid; the old descriptor keeps answering ENOENT for the
	// directory it lost, never the replacement's contents.
	if st := readdir(removed.NodeId, opened.Fh); st != gofuse.ENOENT {
		t.Fatalf("readdir on the removed directory after recreate = %v, want ENOENT", st)
	}
	var reopened gofuse.OpenOut
	if st := f.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: recreated.NodeId}}, &reopened); st != gofuse.OK {
		t.Fatalf("opendir recreated.NodeId = %v", st)
	}
	if st := readdir(recreated.NodeId, reopened.Fh); st != gofuse.OK {
		t.Fatalf("readdir on the recreated directory = %v", st)
	}
}

// The descriptor resolves its directory by inode, not by its open-time path:
// a directory that only MOVED still exists, and Linux keeps its getdents
// working under the new name.
func TestDirHandleFollowsRenamedDirectory(t *testing.T) {
	core := newMemCore()
	f := newTestFS(t, core)

	var dir gofuse.EntryOut
	if st := f.Mkdir(nil, &gofuse.MkdirIn{InHeader: *rootHeader(), Mode: 0o755}, "d", &dir); st != gofuse.OK {
		t.Fatalf("mkdir /d = %v", st)
	}
	var opened gofuse.OpenOut
	if st := f.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: dir.NodeId}}, &opened); st != gofuse.OK {
		t.Fatalf("opendir = %v", st)
	}
	if st := f.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "d", "e"); st != gofuse.OK {
		t.Fatalf("rename /d -> /e = %v", st)
	}
	out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
	if st := f.ReadDir(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: dir.NodeId}, Fh: opened.Fh, Size: 4096}, out); st != gofuse.OK {
		t.Fatalf("readdir after the directory moved = %v", st)
	}
}

// The rmdir detach keeps the inode as a tombstone (an open descriptor may
// still fstat it) instead of dropping it; the name is what disappears.
func TestRemoveDirLinkKeepsTombstoneForOpenDescriptor(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/d", true, 0, time.Now())
	entry, ok := m.GetEntry(ino)
	if !ok {
		t.Fatal("missing entry")
	}
	m.RemoveDirLink("/d")
	if _, ok := m.GetInode("/d"); ok {
		t.Fatal("RemoveDirLink left the name bound")
	}
	kept, ok := m.GetEntry(ino)
	if !ok {
		t.Fatal("RemoveDirLink dropped the inode a kernel reference still holds")
	}
	if !kept.Detached {
		t.Fatal("removed directory is not marked detached")
	}
	if kept.Nlink != 0 {
		t.Fatalf("removed directory nlink = %d, want 0", kept.Nlink)
	}
	if p, ok := m.GetPath(ino); ok {
		t.Fatalf("detached tombstone still resolves path %q", p)
	}
	if entry.Nlookup <= 0 {
		t.Fatal("test precondition: the entry must carry a kernel lookup")
	}
}
