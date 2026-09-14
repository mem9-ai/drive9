package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExtentMetaRenameReplacesDestination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mknod := func(name, path string, inode uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"parent": 1, "name": name, "type": 1, "mode": 0644,
			"inode": inode, "proj_path": path,
			"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", body, nil); err != nil || errno != 0 {
			t.Fatalf("mknod %s errno=%d err=%v", name, errno, err)
		}
	}
	mknod("src.db", "/src.db", 2)
	mknod("dst.db", "/dst.db", 3)
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "src.db",
		"dst_parent": 1, "dst_name": "dst.db",
		"src_path": "/src.db", "dst_path": "/dst.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil || errno != 0 {
		t.Fatalf("rename-replace errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/src.db"); err == nil {
		t.Fatal("source projection still present")
	}
	proj, err := s.GetExtentProjection(ctx, "/dst.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("replaced dest ino=%d, want 2", proj.ExtentIno)
	}
	lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "dst.db"})
	body, errno, err := s.RunExtentMetaOp(ctx, "lookup", lookup, nil)
	if err != nil || errno != 0 {
		t.Fatalf("lookup dest errno=%d err=%v", errno, err)
	}
	var out struct {
		Inode uint64 `json:"inode"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Inode != 2 {
		t.Fatalf("lookup dest ino=%d, want 2", out.Inode)
	}
}
func TestExtentMetaRenameReplaceWhenDestHasProjectionButNoEdge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mknod := func(name, path string, inode uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"parent": 1, "name": name, "type": 1, "mode": 0644,
			"inode": inode, "proj_path": path,
			"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", body, nil); err != nil || errno != 0 {
			t.Fatalf("mknod %s errno=%d err=%v", name, errno, err)
		}
	}
	mknod("src.db", "/src.db", 2)
	mknod("dst.db", "/dst.db", 3)
	if _, err := s.DB().Exec(`DELETE FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("dst.db")); err != nil {
		t.Fatal(err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "src.db",
		"dst_parent": 1, "dst_name": "dst.db",
		"src_path": "/src.db", "dst_path": "/dst.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil || errno != 0 {
		t.Fatalf("rename-replace leftover dest projection errno=%d err=%v, want 0 (pjdfstest rename/09 dest without juicefs edge)", errno, err)
	}
	proj, err := s.GetExtentProjection(ctx, "/dst.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("replaced dest ino=%d, want 2", proj.ExtentIno)
	}
	// The replaced row was inode 3's last name and its jfs edge was already
	// gone, so nothing but a jfs_delfile record can lead the drain back to its
	// chunks. Without one, the node, its slices and their blocks leak for ever.
	var queued int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = 3`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 1 {
		t.Fatalf("jfs_delfile rows for the orphaned inode = %d, want 1", queued)
	}
	var nodes int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 3`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 {
		t.Fatalf("jfs_node rows for the orphaned inode = %d, want 0", nodes)
	}
}
func TestExtentMetaRenameMovesProjectionByExtentIno(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "old.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/old.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "old.db",
		"dst_parent": 1, "dst_name": "new.db",
		"src_path": "/not-the-real-src.db", "dst_path": "/new.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil || errno != 0 {
		t.Fatalf("rename with mismatched src_path errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/old.db"); err == nil {
		t.Fatal("old projection still present after extent_ino rename")
	}
	proj, err := s.GetExtentProjection(ctx, "/new.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("renamed ino=%d, want 2", proj.ExtentIno)
	}
}
func TestExtentMetaRenameUpdatesProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "old.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/old.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "old.db",
		"dst_parent": 1, "dst_name": "new.db",
		"src_path": "/old.db", "dst_path": "/new.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil || errno != 0 {
		t.Fatalf("rename errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/old.db"); err == nil {
		t.Fatal("old projection still present")
	}
	proj, err := s.GetExtentProjection(ctx, "/new.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("renamed ino=%d, want 2", proj.ExtentIno)
	}
	lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "new.db"})
	body, errno, err := s.RunExtentMetaOp(ctx, "lookup", lookup, nil)
	if err != nil || errno != 0 {
		t.Fatalf("lookup errno=%d err=%v", errno, err)
	}
	var out struct {
		Inode uint64 `json:"inode"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Inode != 2 {
		t.Fatalf("lookup ino=%d", out.Inode)
	}
}
func TestExtentMetaRenameFifoOntoRegular(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	reg, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "file", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/file",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", reg, nil); err != nil || errno != 0 {
		t.Fatalf("mknod file errno=%d err=%v", errno, err)
	}
	fifo, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "pipe", "type": 4, "mode": 0644,
		"inode": 3, "proj_path": "/pipe",
		"attr": ExtentAttr{Typ: 4, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", fifo, nil); err != nil || errno != 0 {
		t.Fatalf("mknod fifo errno=%d err=%v", errno, err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "file",
		"dst_parent": 1, "dst_name": "pipe",
		"src_path": "/file", "dst_path": "/pipe",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil || errno != 0 {
		t.Fatalf("rename file onto fifo errno=%d err=%v", errno, err)
	}
	proj, err := s.GetExtentProjection(ctx, "/pipe")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("replaced dest ino=%d, want 2", proj.ExtentIno)
	}
}

// RenameFileNoReplace only rewrites file_nodes, so it has to move the jfs edge
// itself. Without that the extent file keeps its old dentry in the jfs tree:
// the old name answers EEXIST forever and the new name has no edge to unlink,
// which is exactly how the two trees fork.
func TestRenameFileNoReplaceMovesExtentEdge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "src.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/src.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.RenameFileNoReplace(ctx, "/src.db", "/dst.db", "/", "dst.db"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	proj, err := s.GetExtentProjection(ctx, "/dst.db")
	if err != nil {
		t.Fatalf("projection at new path: %v", err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("moved projection ino=%d, want 2", proj.ExtentIno)
	}
	if _, err := s.GetExtentProjection(ctx, "/src.db"); err == nil {
		t.Fatal("old path still resolves an extent projection after rename")
	}
	// The jfs edge has to have moved too: looking the child up under the
	// destination parent must find inode 2, and under the source name nothing.
	dstParent, err := s.GetExtentProjection(ctx, "/dst.db")
	if err != nil {
		t.Fatal(err)
	}
	var moved sql.NullInt64
	if err := s.DB().QueryRowContext(ctx, `SELECT inode FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("dst.db")).Scan(&moved); err != nil {
		t.Fatalf("jfs edge at dst.db: %v (ino=%d)", err, dstParent.ExtentIno)
	}
	if !moved.Valid || uint64(moved.Int64) != 2 {
		t.Fatalf("jfs edge ino=%v, want 2", moved)
	}
	var stale int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("src.db")).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Fatalf("stale jfs edge rows at src.db = %d, want 0", stale)
	}
}

// Replacing an extent file with a classic one has to take the extent
// destination out through the extent unlink. Otherwise the destination's
// jfs_node/jfs_chunk rows and S3 blocks are unreclaimable (only jfs_delfile can
// reach them, and reclaimReplacedTargetTx deliberately skips an extent target)
// and its edge survives, so the next create at that path gets EEXIST forever.
func TestRenameFileReplacingExtentTargetReclaimsIt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "dst.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/dst.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 7, Size: 9, Off: 0, Len: 9},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil); err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v", errno, err)
	}
	// A classic file to rename onto it.
	now := time.Now()
	if err := s.InsertInode(ctx, &Inode{InodeID: "src-inode", Revision: 1, Mode: 0o644, Status: StatusConfirmed, CreatedAt: now, Mtime: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "src-node", Path: "/src.txt", ParentPath: "/", Name: "src.txt", InodeID: "src-inode", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.RenameFileReplacingTarget(ctx, "/src.txt", "/dst.db", "/", "dst.db"); err != nil {
		t.Fatalf("rename onto extent target: %v", err)
	}

	// The destination path now names the classic file, not the extent file that
	// used to be there.
	proj, err := s.GetExtentProjection(ctx, "/dst.db")
	if err != nil {
		t.Fatalf("projection at dst.db after rename: %v", err)
	}
	if proj.ContentLayout == ContentLayoutExtent || proj.ExtentIno != 0 {
		t.Fatalf("dst.db still resolves the extent file: layout=%q ino=%d", proj.ContentLayout, proj.ExtentIno)
	}
	var edges int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("dst.db")).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if edges != 0 {
		t.Fatalf("stale jfs edge rows at dst.db = %d, want 0", edges)
	}
	// The replaced extent file's blocks must be queued for reclamation.
	var delfile int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM jfs_delfile WHERE inode = 2`).Scan(&delfile); err != nil {
		t.Fatal(err)
	}
	if delfile != 1 {
		t.Fatalf("jfs_delfile rows for the replaced extent inode = %d, want 1", delfile)
	}
	// The path is reusable as an extent file once the classic file occupying it
	// is gone. Without the destination unlink above this mknod fails EEXIST on
	// the orphaned edge.
	if _, err := s.DeleteFileWithRefCheck(ctx, "/dst.db"); err != nil {
		t.Fatalf("delete the classic file at dst.db: %v", err)
	}
	recreate, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "dst.db", "type": 1, "mode": 0644,
		"inode": 3, "proj_path": "/dst.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", recreate, nil); err != nil || errno != 0 {
		t.Fatalf("recreate after rename-over errno=%d err=%v", errno, err)
	}
}

// A classic cross-directory rename must move the jfs edge AND keep
// jfs_node.parent in step. The extent unlink resolves the edge through the
// node's parent, so a stale parent made the moved file undeletable through
// every non-FUSE path: the projection row was deleted, the edge lookup missed,
// and the transaction rolled back with ErrNotFound.
func TestRenameFileMovesJfsParentAcrossDirectories(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Two ordinary projection-only directories, the shape a CLI mkdir leaves:
	// jfsEnsureParentsTx mirrors them into the jfs tree when the rename runs.
	now := time.Now()
	for _, d := range []struct{ id, path, name string }{
		{"dir-a", "/a/", "a"},
		{"dir-b", "/b/", "b"},
	} {
		if err := s.InsertNode(ctx, &FileNode{NodeID: d.id, Path: d.path, ParentPath: "/", Name: d.name, IsDirectory: true, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "x.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/a/x.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}

	if err := s.RenameFileNoReplace(ctx, "/a/x.db", "/b/x.db", "/b/", "x.db"); err != nil {
		t.Fatalf("cross-directory rename: %v", err)
	}

	// The node's parent must be the jfs inode of /b/, not of /a/.
	var aIno, bIno, nodeParent uint64
	if err := s.DB().QueryRow(`SELECT e.inode FROM jfs_edge e JOIN jfs_node n ON n.inode = e.inode
		WHERE e.name = ? AND n.type = 2 AND e.parent = 1`, []byte("a")).Scan(&aIno); err != nil {
		t.Fatalf("jfs dir a: %v", err)
	}
	if err := s.DB().QueryRow(`SELECT inode FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("b")).Scan(&bIno); err != nil {
		t.Fatalf("jfs dir b: %v", err)
	}
	if err := s.DB().QueryRow(`SELECT parent FROM jfs_node WHERE inode = 2`).Scan(&nodeParent); err != nil {
		t.Fatal(err)
	}
	if nodeParent != bIno {
		t.Fatalf("jfs_node.parent = %d, want %d (the new directory); a=%d", nodeParent, bIno, aIno)
	}
	var movedEdge int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = ? AND name = ?`,
		bIno, []byte("x.db")).Scan(&movedEdge); err != nil {
		t.Fatal(err)
	}
	if movedEdge != 1 {
		t.Fatalf("edges at b/x.db = %d, want 1", movedEdge)
	}

	// And the file is deletable through the classic path.
	if _, err := s.DeleteFileWithRefCheck(ctx, "/b/x.db"); err != nil {
		t.Fatalf("delete the renamed extent file: %v", err)
	}
}

// A classic rename of one hardlink alias must move only that alias's edge. An
// inode-keyed edge move put every alias on the same dentry, which violates
// uk_jfs_edge(parent, name) and fails the whole rename.
func TestRenameFileHardlinkAliasMovesOnlyThatEdge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "src.db", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/src.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.LinkFileNodeTx(ctx, tx, "/src.db", "/alias.db", "/", "alias.db", "link-alias", time.Now())
	}); err != nil {
		t.Fatalf("hardlink alias: %v", err)
	}

	if err := s.RenameFileNoReplace(ctx, "/alias.db", "/moved.db", "/", "moved.db"); err != nil {
		t.Fatalf("rename alias: %v (the other alias's edge must survive)", err)
	}

	var srcEdge, movedEdge int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("src.db")).Scan(&srcEdge); err != nil {
		t.Fatal(err)
	}
	if srcEdge != 1 {
		t.Fatalf("edges at src.db = %d, want 1 (the untouched alias)", srcEdge)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("moved.db")).Scan(&movedEdge); err != nil {
		t.Fatal(err)
	}
	if movedEdge != 1 {
		t.Fatalf("edges at moved.db = %d, want 1", movedEdge)
	}
	// Both projections still point at the one extent inode.
	for _, p := range []string{"/src.db", "/moved.db"} {
		proj, err := s.GetExtentProjection(ctx, p)
		if err != nil {
			t.Fatalf("projection %s: %v", p, err)
		}
		if proj.ExtentIno != 9 {
			t.Fatalf("projection %s ino=%d, want 9", p, proj.ExtentIno)
		}
	}
}

// Renaming onto a path under a file must be refused even when nothing is sitting
// on the destination name: the edge insert would otherwise create a child of a
// file. The same rule applies to a path component adopted by the parent mirror.
func TestRenameAndParentMirrorRejectFileParent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "file.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/file.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// A source dentry to rename, under the root.
	src, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "src.db", "type": 1, "mode": 0644,
		"inode": 3, "proj_path": "/src.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", src, nil); err != nil || errno != 0 {
		t.Fatalf("mknod src errno=%d err=%v", errno, err)
	}

	// inode 2 is a file; use it as the destination parent (empty name slot).
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "src.db",
		"dst_parent": 2, "dst_name": "moved.db",
		"src_path": "/src.db", "dst_path": "/file.db/moved.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil {
		t.Fatalf("rename under a file: %v", err)
	} else if errno != int(syscall.ENOTDIR) {
		t.Fatalf("rename under a file reported errno=%d, want ENOTDIR", errno)
	}
	var edges int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = 2`).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if edges != 0 {
		t.Fatalf("edges under a file = %d, want 0", edges)
	}

	// The parent mirror must not adopt the file as a directory either.
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		_, err := s.jfsEnsureParentsTx(tx, "/file.db/deep/leaf.txt", 0, 0)
		return err
	}); err == nil {
		t.Fatal("jfsEnsureParentsTx adopted a file as a path component")
	}
}

// A rename that replaces an OPEN extent file must keep the replaced file's node
// and blocks until the last handle closes, exactly as an unlink of an open file
// does: POSIX lets the holder keep reading what it opened. Without the
// dst_opened/sid plumbing the rename reclaimed the file at rename time and the
// open handle's next read or write failed.
func TestExtentRenameOverOpenDestinationDefersReclaim(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mknod := func(name, path string, inode uint64, sliceID uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"parent": 1, "name": name, "type": 1, "mode": 0644,
			"inode": inode, "proj_path": path, "indx": 0,
			"parts": []map[string]any{
				{"off": 0, "slice": map[string]any{"Id": sliceID, "Size": 10, "Off": 0, "Len": 10}},
			},
			"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", body, nil); err != nil || errno != 0 {
			t.Fatalf("mknod %s errno=%d err=%v", name, errno, err)
		}
	}
	mknod("src.db", "/src.db", 2, 90)
	mknod("dst.db", "/dst.db", 3, 91)
	const sid = 7
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "src.db",
		"dst_parent": 1, "dst_name": "dst.db",
		"src_path": "/src.db", "dst_path": "/dst.db",
		"dst_opened": true, "sid": sid,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil || errno != 0 {
		t.Fatalf("rename over an open destination errno=%d err=%v", errno, err)
	}
	// The name now resolves to the source, and the replaced inode is sustained
	// for the session that still holds it open.
	var ino uint64
	if err := s.DB().QueryRow(`SELECT inode FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("dst.db")).Scan(&ino); err != nil {
		t.Fatal(err)
	}
	if ino != 2 {
		t.Fatalf("dst.db -> inode %d, want the source inode 2", ino)
	}
	var nlink uint32
	var length uint64
	if err := s.DB().QueryRow(`SELECT nlink, length FROM jfs_node WHERE inode = 3`).Scan(&nlink, &length); err != nil {
		t.Fatalf("the replaced inode was reclaimed at rename time: %v", err)
	}
	if nlink != 0 || length != 10 {
		t.Fatalf("replaced inode nlink=%d length=%d, want nlink=0 length=10", nlink, length)
	}
	var sustained int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_sustained WHERE sid = ? AND inode = 3`, sid).Scan(&sustained); err != nil {
		t.Fatal(err)
	}
	if sustained != 1 {
		t.Fatalf("jfs_sustained rows for the open replaced inode = %d, want 1", sustained)
	}
	var queued int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = 3`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("the replaced file's blocks were queued for GC at rename time (%d delfile rows)", queued)
	}
	// The close op reclaims it when a client sends one; production drains the
	// row at session end (see jfsUnlinkTx's opened branch), so this asserts the
	// server contract the FUSE session relies on.
	closeReq, _ := json.Marshal(map[string]any{"sid": sid, "inode": 3})
	if _, errno, err := s.RunExtentMetaOp(ctx, "delete_sustained", closeReq, nil); err != nil || errno != 0 {
		t.Fatalf("delete_sustained errno=%d err=%v", errno, err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 3`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("jfs_node rows for the closed inode = %d, want 0", queued)
	}
	var chunks int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_chunk WHERE inode = 3`).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if chunks != 0 {
		t.Fatalf("jfs_chunk rows for the closed inode = %d, want 0 (the close must reclaim the slices)", chunks)
	}
	var blocks int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM block_gc_tasks WHERE block_key LIKE ?`, "%91%").Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if blocks == 0 {
		t.Fatal("closing the last handle did not queue the replaced file's block for GC")
	}
}

// A rename of a dentry that a concurrent unlink already removed must not
// "succeed" into a dangling edge. The source edge read is locked, so the rename
// waits for the unlink and then finds no row; without the lock it deleted 0
// rows, inserted its destination edge anyway, and left an edge pointing at a
// deleted inode — a name that answers EEXIST to a create and ENOENT to a
// rename, for ever.
func TestRenameRacingUnlinkOfSameDentryLeavesNoDanglingEdge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "src.db", "type": 1, "mode": 0644,
		"inode": 5, "proj_path": "/src.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}

	// The unlink holds the edge and node locks until it commits.
	tx1, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback() }()
	if _, eno, err := s.jfsUnlinkTx(ctx, tx1, 1, "src.db", "/src.db", 0, false); err != nil || eno != 0 {
		t.Fatalf("unlink eno=%d err=%v", eno, err)
	}

	type result struct {
		eno int
		err error
	}
	done := make(chan result, 1)
	go func() {
		tx2, err := s.DB().BeginTx(ctx, nil)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = tx2.Rollback() }()
		_, _, _, _, eno, err := s.jfsRenameTx(ctx, tx2, 1, "src.db", 1, "moved.db", "/src.db", "/moved.db", 0, false, 0)
		if err == nil && eno == 0 {
			err = tx2.Commit()
		}
		done <- result{eno: eno, err: err}
	}()
	// Let the rename reach the edge it must wait on before the unlink commits;
	// committing first would let the rename start on the post-unlink state and
	// the test would pass without exercising the race.
	select {
	case r := <-done:
		t.Fatalf("the rename finished before the unlink committed (eno=%d err=%v): it did not wait for the edge lock", r.eno, r.err)
	case <-time.After(400 * time.Millisecond):
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil {
		t.Fatalf("rename error: %v", r.err)
	}
	if r.eno != int(syscall.ENOENT) {
		t.Fatalf("rename of a dentry the unlink removed reported eno=%d, want ENOENT", r.eno)
	}

	// No edge may point at the deleted inode.
	var dangling int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge e
		LEFT JOIN jfs_node n ON n.inode = e.inode WHERE n.inode IS NULL`).Scan(&dangling); err != nil {
		t.Fatal(err)
	}
	if dangling != 0 {
		t.Fatalf("dangling edges = %d, want 0", dangling)
	}
	var moved int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE name = ?`, []byte("moved.db")).Scan(&moved); err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("the rename created %d destination edges for a deleted source", moved)
	}
}

// Two clients renaming different files onto the same destination must not lose
// one of them. TiDB has no gap locks, so the rename's locked destination read
// does not reserve the name: the rename used to DELETE whatever edge appeared
// there and then INSERT its own, which discarded the other client's committed
// create (both callers told "success") and left its node with nlink=1, no edge
// and no delfile row — a permanent leak. The rename now lets the unique key
// catch the race and retries, this time replacing the destination through
// jfsUnlinkTx with full cleanup.
//
// Driven through RunExtentMetaOp, which owns that retry loop, so both renames
// report success and the replaced file is reclaimed rather than orphaned.
func TestConcurrentRenamesOntoSameNameLoseNothing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for round := 0; round < 12; round++ {
		paths := make([]string, 2)
		for i := 0; i < 2; i++ {
			paths[i] = fmt.Sprintf("/src-%d-%d.db", round, i)
			body, _ := json.Marshal(map[string]any{
				"parent": 1, "name": filepath.Base(paths[i]), "type": 1, "mode": 0644,
				"inode": uint64(100 + round*2 + i), "proj_path": paths[i],
				"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
			})
			if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", body, nil); err != nil || errno != 0 {
				t.Fatalf("mknod %s errno=%d err=%v", paths[i], errno, err)
			}
		}
		dstPath := fmt.Sprintf("/dst-%d.db", round)
		dstName := fmt.Sprintf("dst-%d.db", round)

		start := make(chan struct{})
		errs := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func(i int) {
				<-start
				body, _ := json.Marshal(map[string]any{
					"src_parent": 1, "src_name": filepath.Base(paths[i]),
					"dst_parent": 1, "dst_name": dstName,
					"src_path": paths[i], "dst_path": dstPath,
				})
				_, errno, err := s.RunExtentMetaOp(ctx, "rename", body, nil)
				if err != nil {
					errs <- err
					return
				}
				if errno != 0 {
					errs <- syscall.Errno(errno)
					return
				}
				errs <- nil
			}(i)
		}
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: rename failed: %v", round, err)
			}
		}
		var dstEdges int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE name = ?`, []byte(dstName)).Scan(&dstEdges); err != nil {
			t.Fatal(err)
		}
		if dstEdges != 1 {
			t.Fatalf("round %d: edges at %s = %d, want 1", round, dstName, dstEdges)
		}
	}

	// No node may be left unreachable: an inode with edges nowhere, still
	// counted as linked, and with no delfile row recording it for reclamation.
	var leaked int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node n
		WHERE n.inode <> 1
		  AND n.nlink > 0
		  AND NOT EXISTS (SELECT 1 FROM jfs_edge e WHERE e.inode = n.inode)
		  AND NOT EXISTS (SELECT 1 FROM jfs_delfile d WHERE d.inode = n.inode)`).Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Fatalf("unreachable inodes = %d, want 0 (a rename deleted another client's edge)", leaked)
	}
	var dangling int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge e
		LEFT JOIN jfs_node n ON n.inode = e.inode WHERE n.inode IS NULL`).Scan(&dangling); err != nil {
		t.Fatal(err)
	}
	if dangling != 0 {
		t.Fatalf("dangling edges = %d, want 0", dangling)
	}
}

// A rename onto a non-empty directory must fail ENOTEMPTY and leave both trees
// exactly as they were. The emptiness check happens under the destination
// directory's node lock (jfsRmdirTx), so a create that lands between the
// rename's destination read and the removal is seen by a current count instead
// of being deleted by it.
func TestExtentRenameOntoNonEmptyDirectoryKeepsBothTrees(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, d := range []string{"/a/", "/b/", "/b/keep/"} {
		parent, name := "/", strings.TrimSuffix(strings.TrimPrefix(d, "/"), "/")
		if i := strings.LastIndex(name, "/"); i >= 0 {
			parent, name = "/"+name[:i+1], name[i+1:]
		}
		if err := s.InsertNode(ctx, &FileNode{
			NodeID: "dir-" + name, Path: d, ParentPath: parent, Name: name,
			IsDirectory: true, CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	mkdir := func(parent, inode uint64, name, projPath string) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"parent": parent, "name": name, "type": 2, "mode": 0755,
			"inode": inode, "proj_path": projPath,
			"attr": ExtentAttr{Typ: 2, Mode: 0755, Nlink: 2, Parent: parent, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", body, nil); err != nil || errno != 0 {
			t.Fatalf("mkdir %s errno=%d err=%v", projPath, errno, err)
		}
	}
	mkdir(jfsRootIno, 2, "a", "/a/")
	mkdir(jfsRootIno, 3, "b", "/b/")
	mkdir(3, 4, "keep", "/b/keep/")

	rename, _ := json.Marshal(map[string]any{
		"src_parent": jfsRootIno, "src_name": "a",
		"dst_parent": jfsRootIno, "dst_name": "b",
		"src_path": "/a/", "dst_path": "/b/",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.ENOTEMPTY) {
		t.Fatalf("rename onto a non-empty directory reported errno=%d, want ENOTEMPTY", errno)
	}
	for _, want := range []struct {
		parent uint64
		name   string
		inode  uint64
	}{{jfsRootIno, "a", 2}, {jfsRootIno, "b", 3}, {3, "keep", 4}} {
		var ino uint64
		if err := s.DB().QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`,
			want.parent, []byte(want.name)).Scan(&ino); err != nil {
			t.Fatalf("edge %q under %d: %v", want.name, want.parent, err)
		}
		if ino != want.inode {
			t.Fatalf("edge %q under %d -> inode %d, want %d", want.name, want.parent, ino, want.inode)
		}
	}
	// The drive9 rows keep their routing: a rejected rename must not clear the
	// directory's extent_ino on the way to failing.
	for _, d := range []struct {
		path  string
		inode uint64
	}{{"/a/", 2}, {"/b/", 3}, {"/b/keep/", 4}} {
		var ino sql.NullInt64
		if err := s.DB().QueryRow(`SELECT extent_ino FROM file_nodes WHERE `+
			s.scope.And(`path_hash = ? AND path = ?`),
			s.scope.Args(fileNodePathHash(d.path), d.path)...).Scan(&ino); err != nil {
			t.Fatalf("projection %s: %v", d.path, err)
		}
		if !ino.Valid || uint64(ino.Int64) != d.inode {
			t.Fatalf("projection %s extent_ino=%v, want %d", d.path, ino, d.inode)
		}
	}
}
