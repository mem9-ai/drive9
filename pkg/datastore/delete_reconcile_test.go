package datastore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
)

// newSplitOnlyTestStore returns a store whose database has no legacy `files`
// table, so the stranded-inode scan (split schema only) runs.
func newSplitOnlyTestStore(t *testing.T) *Store {
	t.Helper()
	initDatastoreSchema(t, testDSN)
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS files`); err != nil {
		_ = db.Close()
		t.Fatalf("drop files: %v", err)
	}
	testmysql.ResetDBWithoutFiles(t, db)
	_ = db.Close()

	s, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	if s.HasLegacyFiles() {
		_ = s.Close()
		t.Fatal("expected split-only store, got legacy files")
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func reconcilePlantFile(t *testing.T, s *Store, fileID string) {
	t.Helper()
	now := time.Now().UTC()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      fileID,
		StorageType: StorageDB9,
		StorageRef:  "/blobs/" + fileID,
		SizeBytes:   4,
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
}

func reconcilePlantDir(t *testing.T, s *Store, nodeID, path, parentPath, inodeID string) {
	t.Helper()
	now := time.Now().UTC()
	if err := s.InsertInode(context.Background(), &Inode{
		InodeID:   inodeID,
		Revision:  1,
		Mode:      0o755,
		Status:    StatusConfirmed,
		CreatedAt: now,
		Mtime:     now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{
		NodeID:      nodeID,
		Path:        path,
		ParentPath:  parentPath,
		Name:        baseName(path),
		IsDirectory: true,
		InodeID:     inodeID,
		CreatedAt:   now,
	}); err != nil {
		t.Fatal(err)
	}
}

func reconcilePlantFileNode(t *testing.T, s *Store, nodeID, path, parentPath, fileID string) {
	t.Helper()
	if err := s.InsertNode(context.Background(), &FileNode{
		NodeID:     nodeID,
		Path:       path,
		ParentPath: parentPath,
		Name:       baseName(path),
		FileID:     fileID,
		CreatedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
}

func reconcileNodeExists(t *testing.T, s *Store, path string) bool {
	t.Helper()
	exists, err := s.NodeExists(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}

func reconcileInodeStatus(t *testing.T, s *Store, inodeID string) string {
	t.Helper()
	var status string
	if err := s.DB().QueryRowContext(context.Background(),
		`SELECT status FROM inodes WHERE inode_id = ?`, inodeID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func reconcileGCTaskCount(t *testing.T, s *Store, fileID string) int {
	t.Helper()
	var n int
	if err := s.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM file_gc_tasks WHERE file_id = ?`, fileID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Regression red line: top-level rows (parent_path == "/") must never be
// classified as broken — the implicit root has no file_nodes row.
func TestReconcileDeleteOrphansRepairsOnlyBrokenDentry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Normal top-level file and directory.
	reconcilePlantFile(t, s, "f-top")
	reconcilePlantFileNode(t, s, "n-top", "/top.txt", "/", "f-top")
	reconcilePlantDir(t, s, "n-topdir", "/dir/", "/", "d-top")

	// A genuinely broken dentry: parent /ghost/ does not exist.
	reconcilePlantFile(t, s, "f-orphan")
	reconcilePlantFileNode(t, s, "n-orphan", "/ghost/a.txt", "/ghost/", "f-orphan")

	report, err := s.ReconcileDeleteOrphans(ctx, false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if report.BrokenDentries != 1 || report.Repaired != 1 {
		t.Fatalf("report = %+v, want BrokenDentries=1 Repaired=1", report)
	}

	// Top-level rows untouched (zero false positives).
	if !reconcileNodeExists(t, s, "/top.txt") {
		t.Fatal("top-level file /top.txt was wrongly removed")
	}
	if !reconcileNodeExists(t, s, "/dir/") {
		t.Fatal("top-level dir /dir/ was wrongly removed")
	}
	if got := reconcileInodeStatus(t, s, "f-top"); got != string(StatusConfirmed) {
		t.Fatalf("f-top status = %s, want CONFIRMED", got)
	}
	if n := reconcileGCTaskCount(t, s, "f-top"); n != 0 {
		t.Fatalf("f-top gc tasks = %d, want 0", n)
	}

	// Broken dentry repaired: node gone, file DELETED, GC task enqueued.
	if reconcileNodeExists(t, s, "/ghost/a.txt") {
		t.Fatal("broken dentry /ghost/a.txt still present")
	}
	if got := reconcileInodeStatus(t, s, "f-orphan"); got != string(StatusDeleted) {
		t.Fatalf("f-orphan status = %s, want DELETED", got)
	}
	if n := reconcileGCTaskCount(t, s, "f-orphan"); n != 1 {
		t.Fatalf("f-orphan gc tasks = %d, want 1", n)
	}
}

// A broken-chain directory with an intact subtree converges over multiple
// rounds: round 1 repairs only the directory's own dentry, later rounds pick
// up the children stranded one level down.
func TestReconcileDeleteOrphansBrokenDirMultiRound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	reconcilePlantDir(t, s, "n-sub", "/ghost/sub/", "/ghost/", "d-sub")
	reconcilePlantFile(t, s, "f-1")
	reconcilePlantFileNode(t, s, "n-f1", "/ghost/sub/f.txt", "/ghost/sub/", "f-1")
	reconcilePlantDir(t, s, "n-inner", "/ghost/sub/inner/", "/ghost/sub/", "d-inner")
	reconcilePlantFile(t, s, "f-2")
	reconcilePlantFileNode(t, s, "n-f2", "/ghost/sub/inner/g.txt", "/ghost/sub/inner/", "f-2")

	// Round 1: only the top broken dentry is repaired; the children's chains
	// were intact at scan time.
	report, err := s.ReconcileDeleteOrphans(ctx, false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if report.Repaired != 1 {
		t.Fatalf("round 1 repaired = %d, want 1", report.Repaired)
	}
	if reconcileNodeExists(t, s, "/ghost/sub/") {
		t.Fatal("round 1: /ghost/sub/ still present")
	}
	if !reconcileNodeExists(t, s, "/ghost/sub/f.txt") || !reconcileNodeExists(t, s, "/ghost/sub/inner/") {
		t.Fatal("round 1 repaired children whose chains were intact at scan time")
	}

	// Later rounds converge: every descendant is repaired, files get GC tasks.
	converged := false
	for round := 2; round <= 6; round++ {
		report, err := s.ReconcileDeleteOrphans(ctx, false, 100)
		if err != nil {
			t.Fatal(err)
		}
		if report.BrokenDentries == 0 && report.StrandedInodes == 0 {
			converged = true
			break
		}
	}
	if !converged {
		t.Fatal("reconciliation did not converge within 5 follow-up rounds")
	}
	for _, p := range []string{"/ghost/sub/f.txt", "/ghost/sub/inner/", "/ghost/sub/inner/g.txt"} {
		if reconcileNodeExists(t, s, p) {
			t.Fatalf("node %s still present after convergence", p)
		}
	}
	for _, id := range []string{"f-1", "f-2"} {
		if got := reconcileInodeStatus(t, s, id); got != string(StatusDeleted) {
			t.Fatalf("%s status = %s, want DELETED", id, got)
		}
		if n := reconcileGCTaskCount(t, s, id); n != 1 {
			t.Fatalf("%s gc tasks = %d, want 1", id, n)
		}
	}
}

func TestReconcileDeleteOrphansStrandedInode(t *testing.T) {
	s := newSplitOnlyTestStore(t)
	ctx := context.Background()

	// CONFIRMED inode + contents, no file_nodes reference.
	reconcilePlantFile(t, s, "f-stray")

	report, err := s.ReconcileDeleteOrphans(ctx, false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if report.StrandedInodes != 1 || report.Repaired != 1 {
		t.Fatalf("report = %+v, want StrandedInodes=1 Repaired=1", report)
	}
	if got := reconcileInodeStatus(t, s, "f-stray"); got != string(StatusDeleted) {
		t.Fatalf("f-stray status = %s, want DELETED", got)
	}
	if n := reconcileGCTaskCount(t, s, "f-stray"); n != 1 {
		t.Fatalf("f-stray gc tasks = %d, want 1", n)
	}
}

// Directory inodes (inodes row, no contents row) are a deliberate Non-goal:
// the sweep must leave them alone.
func TestReconcileDeleteOrphansIgnoresDirectoryInode(t *testing.T) {
	s := newSplitOnlyTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	if err := s.InsertInode(ctx, &Inode{
		InodeID:   "d-stray",
		Revision:  1,
		Mode:      0o755,
		Status:    StatusConfirmed,
		CreatedAt: now,
		Mtime:     now,
	}); err != nil {
		t.Fatal(err)
	}

	report, err := s.ReconcileDeleteOrphans(ctx, false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if report.StrandedInodes != 0 || report.Repaired != 0 {
		t.Fatalf("report = %+v, want zero hits for directory inode", report)
	}
	if got := reconcileInodeStatus(t, s, "d-stray"); got != string(StatusConfirmed) {
		t.Fatalf("d-stray status = %s, want CONFIRMED (untouched)", got)
	}
	if n := reconcileGCTaskCount(t, s, "d-stray"); n != 0 {
		t.Fatalf("d-stray gc tasks = %d, want 0", n)
	}
}

func TestReconcileDeleteOrphansDryRun(t *testing.T) {
	s := newSplitOnlyTestStore(t)
	ctx := context.Background()

	reconcilePlantFile(t, s, "f-orphan")
	reconcilePlantFileNode(t, s, "n-orphan", "/ghost/a.txt", "/ghost/", "f-orphan")
	reconcilePlantFile(t, s, "f-stray")

	report, err := s.ReconcileDeleteOrphans(ctx, true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if report.BrokenDentries != 1 || report.StrandedInodes != 1 || report.Repaired != 0 {
		t.Fatalf("report = %+v, want BrokenDentries=1 StrandedInodes=1 Repaired=0", report)
	}

	// Dry-run must not modify anything.
	if !reconcileNodeExists(t, s, "/ghost/a.txt") {
		t.Fatal("dry-run removed the broken dentry")
	}
	if got := reconcileInodeStatus(t, s, "f-orphan"); got != string(StatusConfirmed) {
		t.Fatalf("f-orphan status = %s, want CONFIRMED", got)
	}
	if got := reconcileInodeStatus(t, s, "f-stray"); got != string(StatusConfirmed) {
		t.Fatalf("f-stray status = %s, want CONFIRMED", got)
	}
	if n := reconcileGCTaskCount(t, s, "f-orphan"); n != 0 {
		t.Fatalf("f-orphan gc tasks = %d, want 0", n)
	}
	if n := reconcileGCTaskCount(t, s, "f-stray"); n != 0 {
		t.Fatalf("f-stray gc tasks = %d, want 0", n)
	}
}

func TestDeleteReconcileRepairEnabled(t *testing.T) {
	if DeleteReconcileRepairEnabled() {
		t.Fatal("repair mode enabled without env var")
	}
	t.Setenv("DRIVE9_DELETE_RECONCILE_REPAIR", "1")
	if !DeleteReconcileRepairEnabled() {
		t.Fatal("repair mode disabled with DRIVE9_DELETE_RECONCILE_REPAIR=1")
	}
}

// TestReconcileStrandedInodeSharedScopeIsolation is the regression test for
// the ungrouped-OR bug in the repair re-checks: under shared schema,
// `fs_id = ? AND file_id = ? OR inode_id = ?` lets the inode_id branch escape
// the fs_id conjunct, so a *different tenant's* file_nodes/file_gc_tasks row
// with the same ID would make the repair wrongly conclude "still referenced"
// and skip the stranded inode forever. Both re-checks must group the OR.
func TestReconcileStrandedInodeSharedScopeIsolation(t *testing.T) {
	installSharedCoreFSNoLegacy(t)
	ctx := context.Background()
	const fsA, fsB int64 = 4600001, 4600002
	storeA := newSharedStore(t, fsA)
	storeB := newSharedStore(t, fsB)
	now := time.Now()

	// Two stranded CONFIRMED file inodes for tenant A: no file_nodes row, no
	// GC task.
	for _, id := range []string{"shared-stray-1", "shared-stray-2"} {
		if err := storeA.InsertFile(ctx, &File{
			FileID: id, StorageType: StorageDB9, StorageRef: "inline:" + id,
			SizeBytes: 1, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
		}); err != nil {
			t.Fatalf("InsertFile %s: %v", id, err)
		}
	}

	// Tenant B references the same IDs — a file_nodes row for one, a GC task
	// for the other. Neither must affect A's reconciliation.
	if _, err := storeB.DB().Exec(`INSERT INTO file_nodes
		(fs_id, node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory, file_id, created_at)
		VALUES (?, ?, ?, ?, '/', ?, 'b.txt', 0, 'shared-stray-1', ?)`,
		fsB, "shared-b-node-1", "/b.txt", fileNodePathHash("/b.txt"), fileNodePathHash("/"), now); err != nil {
		t.Fatalf("insert B file_nodes row: %v", err)
	}
	if _, err := storeB.EnqueueFileGCTaskTx(storeB.DB(), &FileGCTask{
		FileID: "shared-stray-2", StorageType: StorageDB9, StorageRef: "inline:b",
	}); err != nil {
		t.Fatalf("insert B gc task: %v", err)
	}

	report, err := storeA.ReconcileDeleteOrphans(ctx, false, 0)
	if err != nil {
		t.Fatalf("ReconcileDeleteOrphans: %v", err)
	}
	if report.StrandedInodes != 2 || report.Repaired != 2 {
		t.Fatalf("report = %+v, want 2 stranded inodes and 2 repairs", report)
	}
	for _, id := range []string{"shared-stray-1", "shared-stray-2"} {
		var status string
		if err := storeA.DB().QueryRow(`SELECT status FROM inodes WHERE fs_id = ? AND inode_id = ?`,
			fsA, id).Scan(&status); err != nil {
			t.Fatalf("read inode %s: %v", id, err)
		}
		if status != string(StatusDeleted) {
			t.Fatalf("inode %s status = %s, want DELETED", id, status)
		}
		if _, err := storeA.GetFileGCTaskByFileID(ctx, id); err != nil {
			t.Fatalf("gc task for %s: %v", id, err)
		}
	}

	// Tenant B's rows are untouched.
	var bNodes int
	if err := storeB.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE fs_id = ? AND file_id = ?`,
		fsB, "shared-stray-1").Scan(&bNodes); err != nil {
		t.Fatal(err)
	}
	if bNodes != 1 {
		t.Fatalf("tenant B file_nodes row count = %d, want 1", bNodes)
	}
	if _, err := storeB.GetFileGCTaskByFileID(ctx, "shared-stray-2"); err != nil {
		t.Fatalf("tenant B gc task should survive: %v", err)
	}
}
