package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestExtentMetaCreateUnlinkRecreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, err := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "type": 1, "mode": 0644, "cumask": 0,
		"inode": 2, "proj_path": "/foo.db", "uid": 0, "gid": 0,
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	body, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil)
	if err != nil {
		t.Fatal(err)
	}
	if errno != 0 {
		t.Fatalf("mknod errno=%d body=%s", errno, body)
	}
	proj, err := s.GetExtentProjection(ctx, "/foo.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ContentLayout != ContentLayoutExtent || proj.ExtentIno != 2 {
		t.Fatalf("projection = %+v", proj)
	}

	unlink, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "proj_path": "/foo.db", "opened": false,
	})
	if _, errno, err = s.RunExtentMetaOp(ctx, "unlink", unlink, nil); err != nil || errno != 0 {
		t.Fatalf("unlink errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/foo.db"); err == nil {
		t.Fatal("projection still present after unlink")
	}

	create2, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "type": 1, "mode": 0644, "cumask": 0,
		"inode": 3, "proj_path": "/foo.db", "uid": 0, "gid": 0,
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err = s.RunExtentMetaOp(ctx, "mknod", create2, nil); err != nil || errno != 0 {
		t.Fatalf("recreate errno=%d err=%v", errno, err)
	}
	proj, err = s.GetExtentProjection(ctx, "/foo.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 3 {
		t.Fatalf("recreate ino = %d, want 3", proj.ExtentIno)
	}

	if _, errno, err = s.RunExtentMetaOp(ctx, "mknod", create2, nil); err != nil {
		t.Fatal(err)
	}
	if errno != int(syscall.EEXIST) {
		t.Fatalf("exclusive recreate errno=%d, want EEXIST", errno)
	}
}
func TestExtentUnlinkPathAndHTTPDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "gone.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/gone.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if _, err := s.DeleteFileWithRefCheck(ctx, "/gone.db"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetExtentProjection(ctx, "/gone.db"); err == nil {
		t.Fatal("projection survived HTTP delete")
	}
}
func TestExtentUnlinkMissingEdgeDeletesProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "orphan.db", "type": 1, "mode": 0644,
		"inode": 4, "proj_path": "/orphan.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if _, err := s.DB().Exec(`DELETE FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("orphan.db")); err != nil {
		t.Fatal(err)
	}
	unlink, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "orphan.db", "proj_path": "/orphan.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "unlink", unlink, nil); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.ENOENT) {
		t.Fatalf("unlink missing edge errno=%d, want ENOENT", errno)
	}
	if _, err := s.GetExtentProjection(ctx, "/orphan.db"); err == nil {
		t.Fatal("projection survived unlink of missing juicefs edge")
	}
	// The deleted row was inode 4's last name: the jfs edge is already gone, so
	// a jfs_delfile record is the only way the drain can still find the chunks.
	var queued int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = 4`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 1 {
		t.Fatalf("jfs_delfile rows for the orphaned inode = %d, want 1", queued)
	}
	var nodes int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 4`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 {
		t.Fatalf("jfs_node rows for the orphaned inode = %d, want 0", nodes)
	}
}

// The mirror image: an orphaned projection row whose inode is still reachable
// through a hardlink alias must NOT be handed to the block drain. Reclaiming it
// would free the chunks of a live file — the alias would then read zeros or
// fail — so the cleanup has to see the surviving edge and leave the inode alone.
func TestExtentUnlinkMissingEdgeKeepsLiveAliasChunks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertNode(ctx, &FileNode{NodeID: "dir-alias", Path: "/orph/", ParentPath: "/", Name: "orph", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "src.db", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/src.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.LinkFileNode(ctx, "/src.db", "/orph/b.db", "/orph/", "b.db", "link-alias", now); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	// Only the /src.db dentry goes: the inode keeps its alias edge, but its
	// /src.db projection row is left behind for the cleanup to find.
	if _, err := s.DB().Exec(`DELETE FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("src.db")); err != nil {
		t.Fatal(err)
	}
	unlink, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "src.db", "proj_path": "/src.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "unlink", unlink, nil); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.ENOENT) {
		t.Fatalf("unlink missing edge errno=%d, want ENOENT", errno)
	}
	var queued int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = 9`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("jfs_delfile rows for an inode with a live alias = %d, want 0", queued)
	}
	var nodes int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 9`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 {
		t.Fatalf("jfs_node rows for the aliased inode = %d, want 1", nodes)
	}
	var edges int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE inode = 9`).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if edges != 1 {
		t.Fatalf("jfs_edge rows for the aliased inode = %d, want 1", edges)
	}
}
func TestLinkFileNodeCopiesExtentProjection(t *testing.T) {
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
	if err := s.LinkFileNode(ctx, "/src.db", "/alias.db", "/", "alias.db", "link-1", time.Now()); err != nil {
		t.Fatalf("LinkFileNode: %v", err)
	}
	proj, err := s.GetExtentProjection(ctx, "/alias.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ContentLayout != ContentLayoutExtent || proj.ExtentIno != 9 {
		t.Fatalf("hardlink projection = %+v", proj)
	}

	// The hardlink must also extend the JuiceFS inode: a projection row alone
	// leaves nlink=1 and a stale ctime on the extent inode (pjdfstest link/00
	// "successful link updates ctime").
	var before int64
	if err := s.DB().QueryRow(`SELECT ctime FROM jfs_node WHERE inode = 9`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.LinkFileNodeTx(ctx, tx, "/src.db", "/alias2.db", "/", "alias2.db", "link-2", time.Now())
	}); err != nil {
		t.Fatalf("LinkFileNodeTx alias2: %v", err)
	}
	var nlink int
	var ctime int64
	if err := s.DB().QueryRow(`SELECT nlink, ctime FROM jfs_node WHERE inode = 9`).Scan(&nlink, &ctime); err != nil {
		t.Fatal(err)
	}
	if nlink != 3 {
		t.Fatalf("extent inode nlink = %d, want 3", nlink)
	}
	if ctime <= before {
		t.Fatalf("extent inode ctime = %d, want > %d after hardlink", ctime, before)
	}
	var edgeIno uint64
	if err := s.DB().QueryRow(`SELECT inode FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("alias2.db")).Scan(&edgeIno); err != nil {
		t.Fatalf("jfs edge for alias2: %v", err)
	}
	if edgeIno != 9 {
		t.Fatalf("jfs edge inode = %d, want 9", edgeIno)
	}
}

// Deleting an extent file has to mark the drive9 inode deleted. jfsTruncateTx
// keeps inodes.size_bytes exact for extent files, and ConfirmedStorageBytesTx
// (SUM over CONFIRMED inodes) is the number both quota planes read, so leaving
// the row CONFIRMED means truncate-then-delete cycles never release usage and
// the tenant wedges at EDQUOT with no live data.
func TestExtentUnlinkReleasesConfirmedBytes(t *testing.T) {
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
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 5, Size: 11, Off: 0, Len: 11},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil); err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v", errno, err)
	}
	// A truncate is what makes inodes.size_bytes non-zero for an extent file.
	truncate, _ := json.Marshal(map[string]any{"inode": 2, "length": 4096})
	if _, errno, err := s.RunExtentMetaOp(ctx, "truncate", truncate, nil); err != nil || errno != 0 {
		t.Fatalf("truncate errno=%d err=%v", errno, err)
	}
	if got, err := s.ConfirmedStorageBytesTx(s.DB()); err != nil {
		t.Fatal(err)
	} else if got != 4096 {
		t.Fatalf("confirmed bytes after truncate = %d, want 4096", got)
	}

	if _, err := s.DeleteFileWithRefCheck(ctx, "/blob.db"); err != nil {
		t.Fatalf("delete extent file: %v", err)
	}

	got, err := s.ConfirmedStorageBytesTx(s.DB())
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("confirmed bytes after delete = %d, want 0 (the inode row was left CONFIRMED)", got)
	}
}

// Deleting through a hardlink alias must work. The extent unlink resolves the
// edge from the path's directory; resolving it from jfs_node.parent instead
// finds only the inode's other alias's dentry, so the delete returns ErrNotFound
// and the file stays listed and readable but is unremovable through
// CLI/HTTP/SDK. FUSE was unaffected because it passes the dentry directly.
func TestExtentUnlinkResolvesHardlinkAliasPath(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertNode(ctx, &FileNode{NodeID: "dir-b3", Path: "/b3/", ParentPath: "/", Name: "b3", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "f.db", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/f.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// The alias lives in another directory, so its edge's parent differs from
	// the inode's jfs_node.parent (which keeps naming the original directory).
	if err := s.LinkFileNode(ctx, "/f.db", "/b3/g.db", "/b3/", "g.db", "link-alias3", now); err != nil {
		t.Fatalf("hardlink into another directory: %v", err)
	}

	if _, err := s.DeleteFileWithRefCheck(ctx, "/b3/g.db"); err != nil {
		t.Fatalf("delete through the alias path: %v", err)
	}
	if _, err := s.GetExtentProjection(ctx, "/b3/g.db"); err == nil {
		t.Fatal("alias projection survived the delete")
	}
	if _, err := s.GetExtentProjection(ctx, "/f.db"); err != nil {
		t.Fatalf("the original dentry must be untouched: %v", err)
	}
	// The last reference still deletes normally.
	if _, err := s.DeleteFileWithRefCheck(ctx, "/f.db"); err != nil {
		t.Fatalf("delete the last dentry: %v", err)
	}
}

// Two concurrent unlinks of two hardlink aliases must not lose an nlink
// decrement. With an unlocked read both transactions see nlink=2, both write 1,
// and the inode is left with no edges, nlink=1 and no jfs_delfile row: the
// node, its chunks and its blocks leak permanently. This drives the interleave
// deterministically: tx1 unlinks one alias and stays open while tx2 tries the
// other, which can only proceed after tx1 commits once the read takes the row
// lock.
func TestConcurrentAliasUnlinksDoNotLoseNlink(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertNode(ctx, &FileNode{NodeID: "dir-c1", Path: "/c1/", ParentPath: "/", Name: "c1", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "a.db", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/a.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.LinkFileNode(ctx, "/a.db", "/c1/b.db", "/c1/", "b.db", "link-c1", now); err != nil {
		t.Fatalf("hardlink: %v", err)
	}

	tx1, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback() }()
	if err := s.unlinkExtentPathTx(ctx, tx1, "/a.db", false); err != nil {
		t.Fatalf("first unlink: %v", err)
	}

	// While tx1 holds the node row, the second unlink must wait for it. Run it
	// in its own transaction so the wait happens on a real connection.
	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		tx2, err := s.DB().BeginTx(ctx, nil)
		if err != nil {
			done <- result{err}
			return
		}
		defer func() { _ = tx2.Rollback() }()
		if err := s.unlinkExtentPathTx(ctx, tx2, "/c1/b.db", false); err != nil {
			done <- result{err}
			return
		}
		done <- result{tx2.Commit()}
	}()

	select {
	case r := <-done:
		t.Fatalf("second unlink finished before the first committed (%v): it did not wait for the row lock", r.err)
	case <-time.After(400 * time.Millisecond):
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if r := <-done; r.err != nil {
		t.Fatalf("second unlink after the first committed: %v", r.err)
	}

	// The last unlink removes the node, so the leak this guards against shows up
	// as a surviving node with nlink=1 and no delfile row.
	var nodes, edges int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 9`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE inode = 9`).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if edges != 0 || nodes != 0 {
		t.Fatalf("after both unlinks edges=%d nodes=%d, want 0/0 (a lost nlink decrement leaves the node behind)", edges, nodes)
	}
	var delfile int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = 9`).Scan(&delfile); err != nil {
		t.Fatal(err)
	}
	if delfile != 1 {
		t.Fatalf("jfs_delfile rows = %d, want 1 (blocks must be queued for reclamation)", delfile)
	}
}

// Two concurrent unlinks of the SAME dentry must not both decrement nlink. The
// node read is locked, so without a locking read of the edge both transactions
// pass the edge lookup and serialize on the node: the loser then re-reads the
// post-commit nlink, decrements it again, and — with a hardlink alias present —
// takes the last-reference branch, deleting the node and queueing the blocks of
// a file whose other name is still live.
//
// This drives jfsUnlinkTx directly rather than through unlinkExtentPathTx: the
// latter reads the projection row first, and that read is what a same-path race
// usually loses on, so going one level down is what makes the edge lock the
// property under test.
func TestConcurrentUnlinksOfSameDentryKeepAliasIntact(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertNode(ctx, &FileNode{NodeID: "dir-d1", Path: "/d1/", ParentPath: "/", Name: "d1", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "a.db", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/a.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.LinkFileNode(ctx, "/a.db", "/d1/b.db", "/d1/", "b.db", "link-d1", now); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	var d1Ino uint64
	if err := s.DB().QueryRow(`SELECT extent_ino FROM file_nodes WHERE path = ?`, "/d1/").Scan(&d1Ino); err != nil {
		t.Fatalf("d1 jfs inode: %v", err)
	}

	// The winner unlinks /a.db and holds its transaction open.
	tx1, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback() }()
	if _, eno, err := s.jfsUnlinkTx(ctx, tx1, 1, "a.db", "/a.db", 0, false); err != nil || eno != 0 {
		t.Fatalf("first unlink eno=%d err=%v", eno, err)
	}

	// The loser unlinks the same dentry.
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
		_, eno, err := s.jfsUnlinkTx(ctx, tx2, 1, "a.db", "/a.db", 0, false)
		if err == nil && eno == 0 {
			err = tx2.Commit()
		}
		done <- result{eno: eno, err: err}
	}()
	select {
	case r := <-done:
		t.Fatalf("the second unlink finished before the first committed (eno=%d err=%v): it did not wait for the edge lock", r.eno, r.err)
	case <-time.After(400 * time.Millisecond):
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil {
		t.Fatalf("second unlink error: %v", r.err)
	}
	if r.eno != int(syscall.ENOENT) {
		t.Fatalf("second unlink of the same dentry reported eno=%d, want ENOENT", r.eno)
	}

	// The alias is untouched: node present, nlink 1, no delfile row.
	var nodes, edges, nlink, delfile int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 9`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE inode = 9`).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COALESCE(MAX(nlink), -1) FROM jfs_node WHERE inode = 9`).Scan(&nlink); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = 9`).Scan(&delfile); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 || edges != 1 || nlink != 1 || delfile != 0 {
		t.Fatalf("after the double unlink nodes=%d edges=%d nlink=%d delfile=%d, want 1/1/1/0 (the live alias must keep its node and blocks)",
			nodes, edges, nlink, delfile)
	}
	if _, err := s.GetExtentProjection(ctx, "/d1/b.db"); err != nil {
		t.Fatalf("the alias projection must survive: %v", err)
	}
	_ = d1Ino
}

// Unlinking an extent file while a handle is open must still release the drive9
// entity. Only the bytes may wait for the last close (jfs_sustained), but
// jfsTruncateTx keeps inodes.size_bytes exact for extent files and
// ConfirmedStorageBytesTx sums it, so an entity left CONFIRMED holds quota for
// ever with nothing referencing it.
func TestOpenUnlinkStillReleasesConfirmedBytes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "open.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/open.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 5, Size: 11, Off: 0, Len: 11},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil); err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v", errno, err)
	}
	truncate, _ := json.Marshal(map[string]any{"inode": 2, "length": 4096})
	if _, errno, err := s.RunExtentMetaOp(ctx, "truncate", truncate, nil); err != nil || errno != 0 {
		t.Fatalf("truncate errno=%d err=%v", errno, err)
	}
	session, _ := json.Marshal(map[string]any{"sid": 7, "expire": 0})
	if _, errno, err := s.RunExtentMetaOp(ctx, "new_session", session, nil); err != nil || errno != 0 {
		t.Fatalf("new_session errno=%d err=%v", errno, err)
	}
	// opened: true is what FUSE sends when a handle is open.
	unlink, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "open.db", "proj_path": "/open.db", "opened": true, "sid": 7,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "unlink", unlink, nil); err != nil || errno != 0 {
		t.Fatalf("open unlink errno=%d err=%v", errno, err)
	}
	if got, err := s.ConfirmedStorageBytesTx(s.DB()); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("confirmed bytes right after the open unlink = %d, want 0", got)
	}
	// The last close reclaims the jfs side; drive9 must stay released.
	closeSustained, _ := json.Marshal(map[string]any{"sid": 7, "inode": 2})
	if _, errno, err := s.RunExtentMetaOp(ctx, "delete_sustained", closeSustained, nil); err != nil || errno != 0 {
		t.Fatalf("delete_sustained errno=%d err=%v", errno, err)
	}
	if got, err := s.ConfirmedStorageBytesTx(s.DB()); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("confirmed bytes after the last close = %d, want 0", got)
	}
}

// The orphaned-projection cleanup must decide "last dentry" from a CURRENT read.
// TiDB's REPEATABLE-READ pessimistic mode keeps this transaction's start_ts
// snapshot for plain reads even after the row lock is granted, so a plain COUNT
// still sees a sibling row another transaction just deleted and refuses to
// release the entity: the inode stays CONFIRMED and its size_bytes keeps counting
// against the quota with no dentry left. This drives two overlapping cleanups of
// two orphaned rows of one inode (two hardlink aliases whose jfs edges are gone).
func TestOrphanCleanupReleasesInodeAfterConcurrentSiblingDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertNode(ctx, &FileNode{NodeID: "dir-orph", Path: "/orph/", ParentPath: "/", Name: "orph", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "a.db", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/a.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.LinkFileNode(ctx, "/a.db", "/orph/b.db", "/orph/", "b.db", "link-orph", now); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	// Give the entity a non-zero size_bytes so the quota effect is observable.
	truncate, _ := json.Marshal(map[string]any{"inode": 9, "length": 4096})
	if _, errno, err := s.RunExtentMetaOp(ctx, "truncate", truncate, nil); err != nil || errno != 0 {
		t.Fatalf("truncate errno=%d err=%v", errno, err)
	}
	// The drive9 entity behind both dentries, captured here because the cleanup
	// deletes the last projection row: looking it up through a path afterwards
	// would answer ErrNoRows for the wrong reason (the missing dentry), not
	// because the release happened.
	var entityID string
	if err := s.DB().QueryRow(`SELECT COALESCE(inode_id, file_id) FROM file_nodes WHERE path = ?`, "/a.db").Scan(&entityID); err != nil {
		t.Fatal(err)
	}
	// Orphan both projections: the jfs node stays, the edges go.
	if _, err := s.DB().Exec(`DELETE FROM jfs_edge WHERE inode = 9`); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ConfirmedStorageBytesTx(s.DB()); err != nil {
		t.Fatal(err)
	} else if got != 4096 {
		t.Fatalf("confirmed bytes before the cleanup = %d, want 4096", got)
	}

	tx1, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback() }()
	if err := s.unlinkExtentPathTx(ctx, tx1, "/a.db", false); err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("first cleanup: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		tx2, err := s.DB().BeginTx(ctx, nil)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = tx2.Rollback() }()
		if err := s.unlinkExtentPathTx(ctx, tx2, "/orph/b.db", false); err != nil && !errors.Is(err, ErrNotFound) {
			done <- err
			return
		}
		done <- tx2.Commit()
	}()
	// Let the second cleanup reach the entity lock before the first commits.
	select {
	case err := <-done:
		t.Fatalf("the second cleanup finished before the first committed (%v): it did not wait for the entity lock", err)
	case <-time.After(400 * time.Millisecond):
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("second cleanup: %v", err)
	}

	if got, err := s.ConfirmedStorageBytesTx(s.DB()); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("confirmed bytes after both cleanups = %d, want 0 (a snapshot count sees the sibling row and never releases the entity)", got)
	}
	var status string
	if err := s.DB().QueryRow(`SELECT status FROM inodes WHERE inode_id = ?`, entityID).Scan(&status); err != nil {
		t.Fatalf("the released inode %s has no row at all: %v", entityID, err)
	}
	if status != string(StatusDeleted) {
		t.Fatalf("the released inode %s is %q, want %q (the last dentry must release the entity)", entityID, status, StatusDeleted)
	}
}
