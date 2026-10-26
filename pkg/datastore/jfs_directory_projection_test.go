package datastore

import (
	"context"
	"encoding/json"
	"syscall"
	"testing"
	"time"
)

// Both HTTP views must observe one namespace transaction, including empty dirs.
func TestExtentDirectoryProjectionLifecycle(t *testing.T) {
	for _, source := range []string{"/empty/", "/empty", ""} {
		t.Run("source="+source, func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			meta := func(op string, args map[string]any) {
				t.Helper()
				body, err := json.Marshal(args)
				if err != nil {
					t.Fatal(err)
				}
				if result, errno, err := s.RunExtentMetaOp(ctx, op, body, nil); err != nil || errno != 0 {
					t.Fatalf("%s: errno=%d err=%v body=%s", op, errno, err, result)
				}
			}
			mkdir := func(parent, ino uint64, name, path string) {
				t.Helper()
				meta("mknod", map[string]any{"parent": parent, "inode": ino, "name": name, "type": 2, "mode": 0700, "create_projection": true, "proj_path": path, "attr": ExtentAttr{Typ: 2, Mode: 0700, Nlink: 2, Parent: parent, Full: true}})
			}
			mkdir(1, 200, "empty", "/empty/")
			node, err := s.GetNode(ctx, "/empty/")
			if err != nil {
				t.Fatal(err)
			}
			var mode uint32
			var extentIno uint64
			if err := s.db.QueryRowContext(ctx, `SELECT mode FROM inodes WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(node.InodeID)...).Scan(&mode); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRowContext(ctx, `SELECT extent_ino FROM file_nodes WHERE `+s.scope.And(`path = ?`), s.scope.Args("/empty/")...).Scan(&extentIno); err != nil {
				t.Fatal(err)
			}
			if !node.IsDirectory || mode != 0700 || extentIno != 200 {
				t.Fatalf("empty directory=%+v mode=%o extent=%d", node, mode, extentIno)
			}
			mkdir(200, 201, "nested", "/empty/nested/")
			meta("mknod", map[string]any{"parent": 201, "inode": 202, "name": "file", "type": 1, "mode": 0600, "create_projection": true, "proj_path": "/empty/nested/file", "attr": ExtentAttr{Typ: 1, Mode: 0600, Nlink: 1, Parent: 201, Full: true}})
			meta("rename", map[string]any{"src_parent": 1, "src_name": "empty", "dst_parent": 1, "dst_name": "moved", "src_path": source, "dst_path": "/moved"})
			for _, path := range []string{"/empty/", "/empty/nested/", "/empty/nested/file"} {
				if _, err := s.GetNode(ctx, path); err == nil {
					t.Fatalf("old projection remains: %s", path)
				}
			}
			for _, path := range []string{"/moved/", "/moved/nested/", "/moved/nested/file"} {
				node, err := s.GetNode(ctx, path)
				if err != nil {
					t.Fatalf("new projection %s: %v", path, err)
				}
				if node.ParentPath != parentPath(path) {
					t.Fatalf("stale parent: %+v", node)
				}
			}
			meta("lookup", map[string]any{"parent": 1, "name": "moved"})
			meta("lookup", map[string]any{"parent": 201, "name": "file"})
			meta("unlink", map[string]any{"parent": 201, "name": "file", "proj_path": "/moved/nested/file"})
			meta("rmdir", map[string]any{"parent": 200, "name": "nested", "proj_path": "/moved/nested/"})
			meta("rmdir", map[string]any{"parent": 1, "name": "moved", "proj_path": map[string]string{"/empty/": "/moved/", "/empty": "/moved", "": ""}[source]})
			if _, err := s.GetNode(ctx, "/moved/"); err == nil {
				t.Fatal("rmdir left projection")
			}

		})
	}
}

func TestExtentDirectoryProjectionRejectsForeignHintAndClassicChildren(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	meta := func(op string, args map[string]any) int {
		t.Helper()
		body, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		_, errno, err := s.RunExtentMetaOp(ctx, op, body, nil)
		if err != nil {
			t.Fatal(err)
		}
		return errno
	}
	for i, name := range []string{"a", "b"} {
		if eno := meta("mknod", map[string]any{"parent": 1, "inode": 300 + i, "name": name, "type": 2, "mode": 0700, "create_projection": true, "proj_path": "/" + name + "/", "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: 1, Nlink: 2, Full: true}}); eno != 0 {
			t.Fatal(eno)
		}
	}
	if eno := meta("rename", map[string]any{"src_parent": 1, "src_name": "a", "dst_parent": 1, "dst_name": "moved", "src_path": "/b/", "dst_path": "/moved/"}); eno != int(syscall.EINVAL) {
		t.Fatalf("foreign rename hint errno=%d", eno)
	}
	if eno := meta("rename", map[string]any{"src_parent": 1, "src_name": "a", "dst_parent": 1, "dst_name": "moved", "src_path": "/a/", "dst_path": "/b/"}); eno != int(syscall.EINVAL) {
		t.Fatalf("foreign destination hint errno=%d", eno)
	}
	for _, path := range []string{"/a/", "/b/"} {
		if _, err := s.GetNode(ctx, path); err != nil {
			t.Fatal(err)
		}
	}
	if eno := meta("rmdir", map[string]any{"parent": 1, "name": "a", "proj_path": "/b/"}); eno != int(syscall.EINVAL) {
		t.Fatalf("foreign rmdir hint errno=%d", eno)
	}
	if eno := meta("mknod", map[string]any{"parent": 300, "inode": 302, "name": "nested", "type": 2, "mode": 0700, "create_projection": true, "proj_path": "/a/nested/", "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: 300, Nlink: 2, Full: true}}); eno != 0 {
		t.Fatal(eno)
	}
	if eno := meta("rename", map[string]any{"src_parent": 1, "src_name": "a", "dst_parent": 302, "dst_name": "inside"}); eno != int(syscall.EINVAL) {
		t.Fatalf("directory cycle errno=%d", eno)
	}
	if eno := meta("rmdir", map[string]any{"parent": 300, "name": "nested"}); eno != 0 {
		t.Fatal(eno)
	}
	// A classic-only child has no jfs_edge and must still make the directory nonempty.
	now := time.Now().UTC()
	if err := s.InsertNode(ctx, &FileNode{NodeID: "classic-child", Path: "/a/classic", ParentPath: "/a/", Name: "classic", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if eno := meta("rmdir", map[string]any{"parent": 1, "name": "a"}); eno != int(syscall.ENOTEMPTY) {
		t.Fatalf("classic child removal errno=%d", eno)
	}
	if _, err := s.GetNode(ctx, "/a/"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteNode(ctx, "/a/classic"); err != nil {
		t.Fatal(err)
	}
	if eno := meta("rmdir", map[string]any{"parent": 1, "name": "a"}); eno != 0 {
		t.Fatal(eno)
	}
	if _, err := s.GetNode(ctx, "/a/"); err == nil {
		t.Fatal("omitted hint left directory projection")
	}
}

// A tiny analyzed table may otherwise choose a locking table scan. A directory
// operation must not hold unrelated projection rows while waiting on its inode.
func TestExtentDirectoryProjectionLockIsLocal(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i, name := range []string{"a", "b"} {
		raw, _ := json.Marshal(map[string]any{"parent": 1, "inode": 400 + i, "name": name, "type": 2, "mode": 0700, "create_projection": true, "proj_path": "/" + name + "/", "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: 1, Nlink: 2, Full: true}})
		if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", raw, nil); err != nil || eno != 0 {
			t.Fatalf("mkdir errno=%d err=%v", eno, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, "ANALYZE TABLE file_nodes"); err != nil {
		t.Fatal(err)
	}
	ino, other := uint64(400), uint64(401)
	if deterministicNodeID("/a/") < deterministicNodeID("/b/") {
		ino, other = other, ino
	}
	first, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback() }()
	if _, eno, err := s.jfsDirectoryProjectionTx(ctx, first, ino, ""); err != nil || eno != 0 {
		t.Fatalf("first projection errno=%d err=%v", eno, err)
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	second, err := s.db.BeginTx(deadline, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Rollback() }()
	if _, eno, err := s.jfsDirectoryProjectionTx(deadline, second, other, ""); err != nil || eno != 0 {
		t.Fatalf("unrelated projection blocked errno=%d err=%v", eno, err)
	}
}

func TestExtentDirectoryProjectionRejectsMetadataOnlyCycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	meta := func(op string, args map[string]any) int {
		t.Helper()
		raw, _ := json.Marshal(args)
		_, eno, err := s.RunExtentMetaOp(ctx, op, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		return eno
	}
	for _, entry := range []struct {
		parent, ino uint64
		name        string
	}{{1, 500, "parent"}, {500, 501, "child"}} {
		if eno := meta("mknod", map[string]any{"parent": entry.parent, "inode": entry.ino, "name": entry.name, "type": 2, "mode": 0700, "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: entry.parent, Nlink: 2, Full: true}}); eno != 0 {
			t.Fatal(eno)
		}
	}
	if eno := meta("rename", map[string]any{"src_parent": 1, "src_name": "parent", "dst_parent": 501, "dst_name": "inside"}); eno != int(syscall.EINVAL) {
		t.Fatalf("metadata-only cycle errno=%d", eno)
	}
	if eno := meta("lookup", map[string]any{"parent": 1, "name": "parent"}); eno != 0 {
		t.Fatalf("cycle rejection changed root edge errno=%d", eno)
	}
}

func TestExtentDirectoryProjectionConcurrentCycleRejection(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i, name := range []string{"a", "b"} {
		raw, _ := json.Marshal(map[string]any{"parent": 1, "inode": 600 + i, "name": name, "type": 2, "mode": 0700, "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: 1, Nlink: 2, Full: true}})
		if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", raw, nil); err != nil || eno != 0 {
			t.Fatalf("mkdir errno=%d err=%v", eno, err)
		}
	}
	type result struct {
		errno int
		err   error
	}
	done := make(chan result, 2)
	start := make(chan struct{})
	for i, name := range []string{"a", "b"} {
		go func(i int, name string) {
			<-start
			raw, _ := json.Marshal(map[string]any{"src_parent": 1, "src_name": name, "dst_parent": 601 - i, "dst_name": name})
			_, eno, err := s.RunExtentMetaOp(ctx, "rename", raw, nil)
			done <- result{eno, err}
		}(i, name)
	}
	close(start)
	successes, rejected := 0, 0
	for i := 0; i < 2; i++ {
		r := <-done
		if r.err != nil {
			t.Fatal(r.err)
		}
		switch r.errno {
		case 0:
			successes++
		case int(syscall.EINVAL):
			rejected++
		default:
			t.Fatalf("rename errno=%d", r.errno)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("opposite moves successes=%d rejected=%d", successes, rejected)
	}
	var a, b uint64
	if err := s.db.QueryRowContext(ctx, `SELECT parent FROM jfs_node WHERE `+s.scope.And(`inode = ?`), s.scope.Args(600)...).Scan(&a); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT parent FROM jfs_node WHERE `+s.scope.And(`inode = ?`), s.scope.Args(601)...).Scan(&b); err != nil {
		t.Fatal(err)
	}
	if a == 601 && b == 600 {
		t.Fatal("opposite moves committed a directory cycle")
	}
}

// Existing Go/FUSE clients intentionally follow metadata mkdir with classic
// mkdir. The default must not pre-create the row consumed by that second call.
func TestExtentDirectoryProjectionLegacyTwoPhaseMkdir(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{"parent": 1, "inode": 700, "name": "legacy", "type": 2, "mode": 0700, "proj_path": "/legacy/", "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: 1, Nlink: 2, Full: true}})
	if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || eno != 0 {
		t.Fatalf("mknod errno=%d err=%v", eno, err)
	}
	if _, err := s.GetNode(ctx, "/legacy/"); err == nil {
		t.Fatal("legacy mknod pre-created the classic directory")
	}
	now := time.Now().UTC()
	if err := s.InsertInode(ctx, &Inode{InodeID: "legacy-inode", Mode: 0700, Revision: 1, Status: StatusConfirmed, CreatedAt: now, Mtime: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "legacy-node", InodeID: "legacy-inode", Path: "/legacy/", ParentPath: "/", Name: "legacy", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatalf("classic second phase: %v", err)
	}
	// Opting in must preserve ordinary mkdir's exclusive-create semantics,
	// roll back the attempted metadata node, and retain the original mode/ID.
	conflict, _ := json.Marshal(map[string]any{"parent": 1, "inode": 701, "name": "legacy", "type": 2, "mode": 0755, "create_projection": true, "proj_path": "/legacy/", "attr": ExtentAttr{Typ: 2, Mode: 0755, Parent: 1, Nlink: 2, Full: true}})
	if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", conflict, nil); err != nil || eno != int(syscall.EEXIST) {
		t.Fatalf("exclusive create errno=%d err=%v", eno, err)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jfs_node WHERE `+s.scope.And(`inode = ?`), s.scope.Args(701)...).Scan(&count); err != nil || count != 0 {
		t.Fatalf("conflict left metadata node count=%d err=%v", count, err)
	}
	if node, err := s.GetNode(ctx, "/legacy/"); err != nil || node.InodeID != "legacy-inode" {
		t.Fatalf("conflict replaced classic identity node=%+v err=%v", node, err)
	}

	rename, _ := json.Marshal(map[string]any{"src_parent": 1, "src_name": "legacy", "dst_parent": 1, "dst_name": "moved", "src_path": "/legacy/", "dst_path": "/moved/"})
	if _, eno, err := s.RunExtentMetaOp(ctx, "rename", rename, nil); err != nil || eno != 0 {
		t.Fatalf("legacy rename errno=%d err=%v", eno, err)
	}
	remove, _ := json.Marshal(map[string]any{"parent": 1, "name": "moved", "proj_path": "/moved/"})
	if _, eno, err := s.RunExtentMetaOp(ctx, "rmdir", remove, nil); err != nil || eno != 0 {
		t.Fatalf("legacy rmdir errno=%d err=%v", eno, err)
	}
	if _, err := s.GetNode(ctx, "/moved/"); err == nil {
		t.Fatal("legacy rmdir left projection")
	}
}

func TestExtentDirectoryProjectionOptInRejectsInvalidPaths(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, path := range []string{"", "/different/", "/missing-parent/child/"} {
		raw, _ := json.Marshal(map[string]any{"parent": 1, "inode": 800, "name": "child", "type": 2, "mode": 0700, "create_projection": true, "proj_path": path, "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: 1, Nlink: 2, Full: true}})
		if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", raw, nil); err != nil || eno != int(syscall.EINVAL) {
			t.Fatalf("path %q errno=%d err=%v", path, eno, err)
		}
	}
	lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "child"})
	if _, eno, err := s.RunExtentMetaOp(ctx, "lookup", lookup, nil); err != nil || eno != int(syscall.ENOENT) {
		t.Fatalf("invalid projection mutated namespace errno=%d err=%v", eno, err)
	}
}

func TestExtentDirectoryProjectionRejectsClassicOnlyConflict(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.InsertInode(ctx, &Inode{InodeID: "classic-only-inode", Mode: 0700, Revision: 1, Status: StatusConfirmed, CreatedAt: now, Mtime: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "classic-only-node", InodeID: "classic-only-inode", Path: "/classic-only/", ParentPath: "/", Name: "classic-only", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "occupied-file-node", InodeID: "classic-only-inode", Path: "/occupied", ParentPath: "/", Name: "occupied", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	occupied, _ := json.Marshal(map[string]any{"parent": 1, "inode": 901, "name": "occupied", "type": 2, "mode": 0700, "create_projection": true, "proj_path": "/occupied/", "attr": ExtentAttr{Typ: 2, Mode: 0700, Parent: 1, Nlink: 2, Full: true}})
	if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", occupied, nil); err != nil || eno != int(syscall.EEXIST) {
		t.Fatalf("classic file conflict errno=%d err=%v", eno, err)
	}

	raw, _ := json.Marshal(map[string]any{"parent": 1, "inode": 900, "name": "classic-only", "type": 2, "mode": 0755, "create_projection": true, "proj_path": "/classic-only/", "attr": ExtentAttr{Typ: 2, Mode: 0755, Parent: 1, Nlink: 2, Full: true}})
	for i := 0; i < 2; i++ {
		if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", raw, nil); err != nil || eno != int(syscall.EEXIST) {
			t.Fatalf("classic conflict errno=%d err=%v", eno, err)
		}
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jfs_node WHERE `+s.scope.And(`inode = ?`), s.scope.Args(900)...).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected create left node count=%d err=%v", count, err)
	}
	if node, err := s.GetNode(ctx, "/classic-only/"); err != nil || node.InodeID != "classic-only-inode" {
		t.Fatalf("rejected create replaced classic identity node=%+v err=%v", node, err)
	}
}

func TestExtentRenameRejectsClassicOnlyDestinationConflicts(t *testing.T) {
	for _, test := range []struct {
		name                        string
		sourceDir, targetDir, child bool
		errno                       int
	}{
		{"file_over_directory", false, true, false, int(syscall.EISDIR)},
		{"directory_over_file", true, false, false, int(syscall.ENOTDIR)},
		{"directory_over_nonempty_directory", true, true, true, int(syscall.ENOTEMPTY)},
		{"directory_over_empty_directory", true, true, false, 0},
		{"file_over_file", false, false, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			typ, source, target := 1, "/source", "/target"
			if test.sourceDir {
				typ = 2
				source += "/"
			}
			if test.targetDir {
				target += "/"
			}
			raw, _ := json.Marshal(map[string]any{"parent": 1, "inode": 950, "name": "source", "type": typ, "mode": 0700, "create_projection": true, "proj_path": source, "attr": ExtentAttr{Typ: uint8(typ), Mode: 0700, Parent: 1, Nlink: 1, Full: true}})
			if _, eno, err := s.RunExtentMetaOp(ctx, "mknod", raw, nil); err != nil || eno != 0 {
				t.Fatalf("source errno=%d err=%v", eno, err)
			}
			now := time.Now().UTC()
			if err := s.InsertInode(ctx, &Inode{InodeID: "classic-target", Mode: 0700, Revision: 1, Status: StatusConfirmed, CreatedAt: now, Mtime: now}); err != nil {
				t.Fatal(err)
			}
			if err := s.InsertNode(ctx, &FileNode{NodeID: "classic-target-node", InodeID: "classic-target", Path: target, ParentPath: "/", Name: "target", IsDirectory: test.targetDir, CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if test.child {
				if err := s.InsertNode(ctx, &FileNode{NodeID: "classic-child-node", InodeID: "classic-target", Path: "/target/child", ParentPath: "/target/", Name: "child", CreatedAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			// Raw metadata clients cannot depend on a C++/Go preflight or mirroring.
			raw, _ = json.Marshal(map[string]any{"src_parent": 1, "src_name": "source", "dst_parent": 1, "dst_name": "target", "src_path": source, "dst_path": map[bool]string{true: "/target/", false: "/target"}[test.sourceDir]})
			if _, eno, err := s.RunExtentMetaOp(ctx, "rename", raw, nil); err != nil || eno != test.errno {
				t.Fatalf("rename errno=%d want=%d err=%v", eno, test.errno, err)
			}
			if test.errno == 0 {
				if _, err := s.GetNode(ctx, source); err == nil {
					t.Fatal("successful rename retained source")
				}
				if _, err := s.GetNode(ctx, target); err != nil {
					t.Fatal(err)
				}
				return
			}
			for _, path := range []string{source, target} {
				if _, err := s.GetNode(ctx, path); err != nil {
					t.Fatalf("rejected rename changed %s: %v", path, err)
				}
			}
			lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "source"})
			if _, eno, err := s.RunExtentMetaOp(ctx, "lookup", lookup, nil); err != nil || eno != 0 {
				t.Fatalf("source edge changed errno=%d err=%v", eno, err)
			}
		})
	}
}
