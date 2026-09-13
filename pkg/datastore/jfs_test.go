package datastore

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestExtentMetaWriteUpdatesLength(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "blob", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/blob",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 1, Size: 4, Off: 0, Len: 4},
	})
	body, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil)
	if err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v body=%s", errno, err, body)
	}
	getattr, _ := json.Marshal(map[string]any{"inode": 2})
	body, errno, err = s.RunExtentMetaOp(ctx, "getattr", getattr, nil)
	if err != nil || errno != 0 {
		t.Fatalf("getattr errno=%d err=%v", errno, err)
	}
	var out struct {
		Attr ExtentAttr `json:"attr"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Attr.Length != 4 {
		t.Fatalf("length=%d, want 4", out.Attr.Length)
	}
}
func TestExtentMetaMknodFifoCreatesProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "pipe", "type": 4, "mode": 0644,
		"inode": 5, "proj_path": "/pipe",
		"attr": ExtentAttr{Typ: 4, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod fifo errno=%d err=%v", errno, err)
	}
	proj, err := s.GetExtentProjection(ctx, "/pipe")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 5 {
		t.Fatalf("fifo projection ino=%d, want 5", proj.ExtentIno)
	}
	getattr, _ := json.Marshal(map[string]any{"inode": 5})
	body, errno, err := s.RunExtentMetaOp(ctx, "getattr", getattr, nil)
	if err != nil || errno != 0 {
		t.Fatalf("getattr fifo errno=%d err=%v", errno, err)
	}
	var out struct {
		Attr ExtentAttr `json:"attr"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Attr.Typ != 4 {
		t.Fatalf("fifo type=%d, want 4", out.Attr.Typ)
	}
}
func TestExtentMetaSymlinkReadlink(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "link", "type": 3, "mode": 0777,
		"inode": 6, "proj_path": "/link", "path": "target",
		"attr": ExtentAttr{Typ: 3, Mode: 0777, Nlink: 1, Length: 6, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod symlink errno=%d err=%v", errno, err)
	}
	readlink, _ := json.Marshal(map[string]any{"inode": 6})
	body, errno, err := s.RunExtentMetaOp(ctx, "readlink", readlink, nil)
	if err != nil || errno != 0 {
		t.Fatalf("readlink errno=%d err=%v body=%s", errno, err, body)
	}
	var out struct {
		Target []byte `json:"target"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Target) != "target" {
		t.Fatalf("readlink %q, want target", out.Target)
	}
}

// A directory listing must label extent children with their JuiceFS inode,
// because that is the only layout signal readdirplus gets. Without it a client
// cannot route an extent file to the extent data plane from the listing alone
// and has to spend a HEAD probe per file, which is what forced the removed
// name-based overlay to guess.
func TestListDirCarriesExtentInoOnlyForExtentChildren(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "blob.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/blob.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	now := time.Now()
	if err := s.InsertInode(ctx, &Inode{InodeID: "plain-inode", Revision: 1, Mode: 0o644, Status: StatusConfirmed, CreatedAt: now, Mtime: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "plain-node", Path: "/plain.txt", ParentPath: "/", Name: "plain.txt", InodeID: "plain-inode", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 1, Size: 7, Off: 0, Len: 7},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil); err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v", errno, err)
	}

	entries, err := s.ListDir(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*NodeWithFile, len(entries))
	for _, e := range entries {
		byName[e.Node.Name] = e
	}
	blob, ok := byName["blob.db"]
	if !ok {
		t.Fatalf("blob.db missing from listing %v", byName)
	}
	if blob.ContentLayout != ContentLayoutExtent || blob.ExtentIno != 2 {
		t.Fatalf("extent child layout=%q ino=%d, want extent/2", blob.ContentLayout, blob.ExtentIno)
	}
	if blob.File == nil || blob.File.SizeBytes != 7 {
		t.Fatalf("extent child size = %+v, want 7 from jfs_node.length", blob.File)
	}
	plain, ok := byName["plain.txt"]
	if !ok {
		t.Fatalf("plain.txt missing from listing %v", byName)
	}
	if plain.ExtentIno != 0 || plain.ContentLayout == ContentLayoutExtent {
		t.Fatalf("single-layout child layout=%q ino=%d, want empty/0", plain.ContentLayout, plain.ExtentIno)
	}
}
