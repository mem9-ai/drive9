package datastore

import (
	"context"
	"encoding/json"
	"testing"
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
				meta("mknod", map[string]any{"parent": parent, "inode": ino, "name": name, "type": 2, "mode": 0700, "proj_path": path, "attr": ExtentAttr{Typ: 2, Mode: 0700, Nlink: 2, Parent: parent, Full: true}})
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
			meta("mknod", map[string]any{"parent": 201, "inode": 202, "name": "file", "type": 1, "mode": 0600, "proj_path": "/empty/nested/file", "attr": ExtentAttr{Typ: 1, Mode: 0600, Nlink: 1, Parent: 201, Full: true}})
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
			meta("rmdir", map[string]any{"parent": 1, "name": "moved", "proj_path": "/moved/"})
			if _, err := s.GetNode(ctx, "/moved/"); err == nil {
				t.Fatal("rmdir left projection")
			}

		})
	}
}
