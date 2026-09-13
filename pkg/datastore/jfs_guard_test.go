package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"syscall"
	"testing"
)

func TestExtentMetaConcurrentParentCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const n = 8
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("f%d.db", i)
			body, _ := json.Marshal(map[string]any{
				"parent": 999, "name": name, "type": 1, "mode": 0644,
				"proj_path": "/run/" + name,
				"attr":      ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
			})
			_, errno, err := s.RunExtentMetaOp(ctx, "mknod", body, nil)
			if err != nil {
				errCh <- err
				return
			}
			if errno != 0 {
				errCh <- fmt.Errorf("mknod %s errno=%d", name, errno)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("/run/f%d.db", i)
		if _, err := s.GetExtentProjection(ctx, path); err != nil {
			t.Fatalf("projection %s: %v", path, err)
		}
	}
}
func TestExtentMetaRollbackBetweenTrees(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	jfsTestFailBeforeProjection = func() error {
		return syscall.EIO
	}
	t.Cleanup(func() { jfsTestFailBeforeProjection = nil })
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "roll.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/roll.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	_, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil)
	if err == nil && errno == 0 {
		t.Fatal("expected rollback error")
	}
	if _, err := s.GetExtentProjection(ctx, "/roll.db"); err == nil {
		t.Fatal("projection committed after injected failure")
	}
	lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "roll.db"})
	body, errno, err := s.RunExtentMetaOp(ctx, "lookup", lookup, nil)
	if err != nil {
		t.Fatal(err)
	}
	if errno != int(syscall.ENOENT) {
		t.Fatalf("lookup after rollback errno=%d body=%s, want ENOENT", errno, body)
	}
}

// An edge insert must refuse a parent that is not a directory. Upstream
// doMknod returns ENOTDIR; without the check this RPC (reachable directly at
// POST /v1/extent/meta) accepts a file's inode and hangs children off it, which
// makes readdir of that file return entries and leaves them undeletable through
// the classic paths.
func TestMknodRejectsNonDirectoryParent(t *testing.T) {
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
	// inode 2 is the file created above.
	child, _ := json.Marshal(map[string]any{
		"parent": 2, "name": "child.db", "type": 1, "mode": 0644,
		"inode": 3, "proj_path": "/file.db/child.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 2, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", child, nil); err != nil {
		t.Fatalf("mknod under a file: %v", err)
	} else if errno != int(syscall.ENOTDIR) {
		t.Fatalf("mknod under a file reported errno=%d, want ENOTDIR", errno)
	}
	var edges int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = 2`).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if edges != 0 {
		t.Fatalf("edges under a file = %d, want 0", edges)
	}
}

// A chmod that arrives through the classic path must take effect on an extent
// file. Its mode lives in jfs_node (the projection's inodes.mode is not
// maintained on the extent write path), and every reader overlays jfs_node, so
// writing only inodes.mode used to report success without changing anything.
func TestChmodMirrorsModeOntoExtentNode(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "perm.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/perm.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.Chmod(ctx, "/perm.db", 0o600); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	var jfsMode int
	if err := s.DB().QueryRow(`SELECT mode FROM jfs_node WHERE inode = 2`).Scan(&jfsMode); err != nil {
		t.Fatal(err)
	}
	if jfsMode&0o7777 != 0o600 {
		t.Fatalf("jfs_node.mode = %o, want 600", jfsMode&0o7777)
	}
	// And the stat overlay must report it, which is what a remote reader sees.
	nf, err := s.StatForRead(ctx, "/perm.db")
	if err != nil {
		t.Fatal(err)
	}
	if nf.Mode&0o7777 != 0o600 {
		t.Fatalf("stat mode = %o, want 600", nf.Mode&0o7777)
	}
	// A mounted chmod (the JuiceFS SetAttr op) touches jfs_node only. The API
	// chmod that follows must adopt the new permission bits, and must not report
	// ErrNotFound just because inodes.mode already held the value it writes.
	if _, errno, err := s.RunExtentMetaOp(ctx, "setattr", mustJSON(map[string]any{
		"inode": 2, "valid": 0x8, "mode": 0o640,
	}), nil); err != nil || errno != 0 {
		t.Fatalf("mounted setattr errno=%d err=%v", errno, err)
	}
	if err := s.Chmod(ctx, "/perm.db", 0o640); err != nil {
		t.Fatalf("repeat chmod of an existing file: %v", err)
	}
	if err := s.DB().QueryRow(`SELECT mode FROM jfs_node WHERE inode = 2`).Scan(&jfsMode); err != nil {
		t.Fatal(err)
	}
	if jfsMode&0o7777 != 0o640 {
		t.Fatalf("jfs_node.mode = %o after the repeat chmod, want 640", jfsMode&0o7777)
	}
	var apiMode int
	if err := s.DB().QueryRow(`SELECT mode FROM inodes WHERE inode_id = (
		SELECT COALESCE(inode_id, file_id) FROM file_nodes WHERE path = ?)`, "/perm.db").Scan(&apiMode); err != nil {
		t.Fatal(err)
	}
	if apiMode&0o7777 != 0o640 {
		t.Fatalf("inodes.mode = %o, want 640", apiMode&0o7777)
	}
	// The identical chmod again is the case that matters: TiDB reports 0 changed
	// rows for an UPDATE that writes the value already there, so a row-count
	// based existence check would answer ErrNotFound here.
	if err := s.Chmod(ctx, "/perm.db", 0o640); err != nil {
		t.Fatalf("no-op chmod of an existing file: %v", err)
	}
}
