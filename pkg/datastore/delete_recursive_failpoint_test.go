//go:build failpoint

package datastore

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
)

// Deterministic race tests for the batched recursive directory delete
// (docs/design/recursive-delete-batched-design.md §6). The injection points
// live in delete_recursive.go and are failpoint markers: with the tree not
// rewritten by failpoint-ctl they are empty no-op calls, so failpoint-off
// behavior is identical to the non-instrumented code. Every callback is
// scoped narrowly (path/op match + fire-once) and paired with a
// t.Cleanup Disable. Synchronization uses channels, never sleeps.
//
// Engine note: tests run against MySQL 8 (REPEATABLE READ) via
// testcontainers. Unlike TiDB, InnoDB takes gap locks on the drain batch's
// locking reads, so a concurrent create into a directory whose drain
// transaction is open is serialized behind that transaction's commit. Where
// that changes the observable interleaving versus TiDB, the test says so and
// asserts the MySQL-deterministic outcome.

const (
	fpDrainAfterSelect      = "github.com/mem9-ai/drive9/pkg/datastore/drainChildrenBatchAfterSelect"
	fpDrainBeforeSelect     = "github.com/mem9-ai/drive9/pkg/datastore/drainBatchBeforeSelect"
	fpEmptyDirBeforeRecheck = "github.com/mem9-ai/drive9/pkg/datastore/drainEmptyDirBeforeRecheck"
	fpLiftBeforePathLookup  = "github.com/mem9-ai/drive9/pkg/datastore/liftBeforePathLookup"
	fpBeforeRootDelete      = "github.com/mem9-ai/drive9/pkg/datastore/deleteDirBeforeRootDelete"
	fpBatchError            = "github.com/mem9-ai/drive9/pkg/datastore/deleteDirBatchError"
)

// enableCallFailpoint registers fn for the InjectCall failpoint and disarms
// it on test cleanup.
func enableCallFailpoint(t *testing.T, fp string, fn any) {
	t.Helper()
	if err := failpoint.EnableCall(fp, fn); err != nil {
		t.Fatalf("enable failpoint %s: %v", fp, err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(fp)
	})
}

// insertConfirmedFileE is insertConfirmedFile with an error return, for use
// inside failpoint callbacks and helper goroutines where t.Fatal is illegal.
func insertConfirmedFileE(s *Store, fileID string) error {
	now := time.Now().UTC()
	return s.InsertFile(context.Background(), &File{
		FileID: fileID, StorageType: StorageDB9, StorageRef: "/blobs/" + fileID,
		SizeBytes: 1, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
	})
}

// createFileViaRealPath creates one file dentry through the real create path
// (EnsureParentDirsTx + InsertNodeTx) in a single transaction, mirroring how
// backend creates land nodes.
func createFileViaRealPath(s *Store, path, fileID string) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.EnsureParentDirsTx(tx, path, func() string { return deterministicNodeID(path) }); err != nil {
		return fmt.Errorf("ensure parents %s: %w", path, err)
	}
	if err := s.InsertNodeTx(tx, fileTestNode(path, fileID, time.Now())); err != nil {
		return fmt.Errorf("insert node %s: %w", path, err)
	}
	return tx.Commit()
}

// ensureParentsOnly runs only the EnsureParentDirsTx half of the create path,
// committing whatever parent dentries it (re)creates.
func ensureParentsOnly(s *Store, path string) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.EnsureParentDirsTx(tx, path, func() string { return deterministicNodeID(path) }); err != nil {
		return err
	}
	return tx.Commit()
}

// insertNodeOnly runs only the InsertNodeTx half of the create path (a bare
// INSERT, no parent-existence check — see store.go InsertNodeTx).
func insertNodeOnly(s *Store, path, fileID string) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.InsertNodeTx(tx, fileTestNode(path, fileID, time.Now())); err != nil {
		return err
	}
	return tx.Commit()
}

func inodeStatus(t *testing.T, s *Store, id string) string {
	t.Helper()
	var status string
	if err := s.db.QueryRow(`SELECT status FROM inodes WHERE inode_id = ?`, id).Scan(&status); err != nil {
		t.Fatalf("inode status for %q: %v", id, err)
	}
	return status
}

func countAllGCTasks(t *testing.T, s *Store) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM file_gc_tasks`).Scan(&n); err != nil {
		t.Fatalf("count gc tasks: %v", err)
	}
	return n
}

// bulkFixtureTree inserts count files (f%03d naming) with confirmed inodes
// directly under dir and returns their file IDs.
func bulkFixtureTree(t *testing.T, s *Store, dir string, count int) []string {
	t.Helper()
	now := time.Now()
	nodes := []*FileNode{dirTestNode(dir, now)}
	fileIDs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		fid := fmt.Sprintf("%sf%03d", dir[1:len(dir)-1], i)
		fileIDs = append(fileIDs, fid)
		insertConfirmedFile(t, s, fid, now)
		nodes = append(nodes, fileTestNode(fmt.Sprintf("%sf%03d.txt", dir, i), fid, now))
	}
	bulkInsertNodes(t, s, nodes...)
	return fileIDs
}

// TestDeleteDirBatchedTOCTOUCreateConverges injects a create through the real
// create path while a drain batch of the target directory holds its
// SELECT ... FOR UPDATE locks. The racing file sorts after every fixture
// file, so on MySQL its INSERT does not touch the paused batch's locked gap
// and commits mid-sweep; a later batch of the same drain loop must pick it up
// (the loop always re-selects the first N current names). The red line: no
// blob leak (inode DELETED + GC task) and no invisible orphan.
func TestDeleteDirBatchedTOCTOUCreateConverges(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)

	bulkFixtureTree(t, s, "/r/", 65)

	hit := make(chan struct{})
	release := make(chan struct{})
	var fired atomic.Bool
	enableCallFailpoint(t, fpDrainAfterSelect, func(parent string) {
		if parent != "/r/" || !fired.CompareAndSwap(false, true) {
			return
		}
		close(hit)
		<-release
	})

	deleteDone := make(chan error, 1)
	go func() {
		_, err := s.DeleteDirRecursive(context.Background(), "/r/")
		deleteDone <- err
	}()

	select {
	case <-hit:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("drain batch failpoint never fired")
	}

	// Create while the first drain batch of /r/ is paused with locks held.
	insertConfirmedFile(t, s, "f-race", time.Now())
	createDone := make(chan error, 1)
	go func() { createDone <- createFileViaRealPath(s, "/r/zzz-race.txt", "f-race") }()
	select {
	case err := <-createDone:
		if err != nil {
			close(release)
			t.Fatalf("racing create: %v", err)
		}
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("racing create blocked behind the drain batch (gap-lock analysis broke)")
	}
	close(release)

	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteDirRecursive: %v", err)
	}

	// The racing file was swept by a later drain batch: dentry gone, inode
	// marked DELETED, exactly one GC task — no blob leak, no orphan.
	assertNodeGone(t, s, "/r/zzz-race.txt")
	assertNodeGone(t, s, "/r/")
	if n := countNodesLike(t, s, "/r/%"); n != 0 {
		t.Fatalf("%d nodes remain under /r/, want 0", n)
	}
	if n := countGCTasks(t, s, "f-race"); n != 1 {
		t.Fatalf("gc tasks for f-race = %d, want 1", n)
	}
	if st := inodeStatus(t, s, "f-race"); st != string(StatusDeleted) {
		t.Fatalf("inode f-race status = %q, want DELETED (blob leak otherwise)", st)
	}
	if n := countAllGCTasks(t, s); n != 66 {
		t.Fatalf("total gc tasks = %d, want 66 (65 fixture + 1 racing)", n)
	}
}

// TestDeleteDirBatchedEmptyDirDeleteRace injects a create between an empty
// directory's dentry DELETE and the post-DELETE re-check inside the same
// drain batch. On MySQL the racing INSERT blocks on the drain transaction's
// gap lock and commits right after the batch — the design's acknowledged
// residual window (§4.2 "Residual window"; on TiDB the insert lands before
// the re-check and rolls the batch back instead). The test pins that window
// deterministically and asserts the companion reconciliation sweep (§4.2.1)
// detects and repairs the orphan: no lasting invisible orphan, no blob leak.
func TestDeleteDirBatchedEmptyDirDeleteRace(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	t.Setenv("DRIVE9_DELETE_RECONCILE_REPAIR", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)

	// 25 loose files force the sweep; /e/sub/ (empty) sorts last, so the
	// second drain batch of /e/ deletes its dentry. Both directory dentries
	// carry an inode_id like real directories: without it the racing
	// create's EnsureParentDirsTx would backfill inode_id with an UPDATE
	// that blocks on the paused batch's locks, deadlocking the test.
	now := time.Now()
	rootDir := dirTestNode("/e/", now)
	rootDir.InodeID = "inode-e"
	subDir := dirTestNode("/e/sub/", now)
	subDir.InodeID = "inode-e-sub"
	nodes := []*FileNode{rootDir, subDir}
	for i := 0; i < 25; i++ {
		fid := fmt.Sprintf("ef%03d", i)
		insertConfirmedFile(t, s, fid, now)
		nodes = append(nodes, fileTestNode(fmt.Sprintf("/e/f%03d.txt", i), fid, now))
	}
	bulkInsertNodes(t, s, nodes...)

	hit := make(chan struct{})
	release := make(chan struct{})
	var fired atomic.Bool
	enableCallFailpoint(t, fpEmptyDirBeforeRecheck, func(dirPath string) {
		if dirPath != "/e/sub/" || !fired.CompareAndSwap(false, true) {
			return
		}
		close(hit)
		<-release
	})

	deleteDone := make(chan error, 1)
	go func() {
		_, err := s.DeleteDirRecursive(context.Background(), "/e/")
		deleteDone <- err
	}()

	select {
	case <-hit:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("empty-dir recheck failpoint never fired")
	}

	// The dentry DELETE is uncommitted, so the real create path still sees
	// /e/sub/ alive and does not recreate it. The bare INSERT then lands
	// after the batch commits (gap lock), leaving a broken-chain row.
	insertConfirmedFile(t, s, "f-race2", now)
	if err := ensureParentsOnly(s, "/e/sub/zzz-new.txt"); err != nil {
		close(release)
		t.Fatalf("ensure parents: %v", err)
	}
	insertDone := make(chan error, 1)
	go func() { insertDone <- insertNodeOnly(s, "/e/sub/zzz-new.txt", "f-race2") }()
	close(release)

	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteDirRecursive: %v", err)
	}
	select {
	case err := <-insertDone:
		if err != nil {
			t.Fatalf("racing insert: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("racing insert still blocked after the delete returned")
	}

	// Residual window state: the row exists but its parent chain is broken,
	// and no GC task covers it yet.
	assertNodeAlive(t, s, "/e/sub/zzz-new.txt")
	assertNodeGone(t, s, "/e/sub/")
	assertNodeGone(t, s, "/e/")
	if n := countGCTasks(t, s, "f-race2"); n != 0 {
		t.Fatalf("gc tasks for f-race2 = %d, want 0 before reconciliation", n)
	}

	// The reconciliation sweep (repair mode) must remove the broken-chain
	// dentry and run the orphan pipeline for its file.
	report, err := s.ReconcileDeleteOrphans(context.Background(), false, 100, nil)
	if err != nil {
		t.Fatalf("ReconcileDeleteOrphans: %v", err)
	}
	if report.BrokenDentries != 1 || report.Repaired != 1 {
		t.Fatalf("reconcile report = %+v, want exactly 1 broken dentry repaired", report)
	}
	assertNodeGone(t, s, "/e/sub/zzz-new.txt")
	if n := countNodesLike(t, s, "/e/%"); n != 0 {
		t.Fatalf("%d nodes remain under /e/ after reconcile, want 0", n)
	}
	if n := countGCTasks(t, s, "f-race2"); n != 1 {
		t.Fatalf("gc tasks for f-race2 = %d, want 1 after reconcile", n)
	}
	if st := inodeStatus(t, s, "f-race2"); st != string(StatusDeleted) {
		t.Fatalf("inode f-race2 status = %q, want DELETED", st)
	}
	if n := countAllGCTasks(t, s); n != 26 {
		t.Fatalf("total gc tasks = %d, want 26 (25 fixture + 1 racing)", n)
	}
}

// TestDeleteDirBatchedLiftIdempotent races a plain rmdir (DeleteEmptyDir)
// against the post-order lift of an already-drained subdirectory: the lift's
// path lookup must treat the missing dentry as idempotent success and the
// overall delete must complete.
func TestDeleteDirBatchedLiftIdempotent(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)
	now := time.Now()

	// /l/sub/ holds 25 files: the fast path overflows, the sweep drains
	// /l/sub/ in two batches, then lifts /l/sub/.
	fileIDs := bulkFixtureTree(t, s, "/l/sub/", 25)
	bulkInsertNodes(t, s, dirTestNode("/l/", now))

	hit := make(chan struct{})
	release := make(chan struct{})
	var fired atomic.Bool
	enableCallFailpoint(t, fpLiftBeforePathLookup, func(dirPath string) {
		if dirPath != "/l/sub/" || !fired.CompareAndSwap(false, true) {
			return
		}
		close(hit)
		<-release
	})

	deleteDone := make(chan error, 1)
	var summary *DeleteDirRecursiveSummary
	go func() {
		var err error
		summary, err = s.DeleteDirRecursive(context.Background(), "/l/")
		deleteDone <- err
	}()

	select {
	case <-hit:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("lift failpoint never fired")
	}

	// The lift transaction has begun but holds no locks yet (the failpoint
	// fires before its path lookup), so this rmdir commits immediately.
	if err := s.DeleteEmptyDir(context.Background(), "/l/sub/"); err != nil {
		close(release)
		t.Fatalf("concurrent rmdir: %v", err)
	}
	close(release)

	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteDirRecursive: %v", err)
	}
	if summary.NodesDeleted != 25 {
		t.Fatalf("summary.NodesDeleted = %d, want 25 (lift counted no already-gone dentry)", summary.NodesDeleted)
	}
	if n := countNodesLike(t, s, "/l/%"); n != 0 {
		t.Fatalf("%d nodes remain under /l/, want 0", n)
	}
	assertNodeGone(t, s, "/l/")
	for _, fid := range fileIDs {
		if n := countGCTasks(t, s, fid); n != 1 {
			t.Fatalf("gc tasks for %s = %d, want 1", fid, n)
		}
	}
}

// TestDeleteDirBatchedErrorOnPartial arms a writer that keeps the root
// non-empty before every root-delete attempt: with root attempts bounded to
// 2 the delete must return ErrDirectoryNotEmpty (never a partial success),
// and a later call after the writer stops must complete the delete.
func TestDeleteDirBatchedErrorOnPartial(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	oldAttempts := deleteDirRootAttempts
	deleteDirRootAttempts = 2
	t.Cleanup(func() { deleteDirRootAttempts = oldAttempts })
	s := newTestStore(t)

	bulkFixtureTree(t, s, "/w/", 25)

	var writerErr error
	var created []string
	enableCallFailpoint(t, fpBeforeRootDelete, func(dirPath string, attempt int) {
		if dirPath != "/w/" {
			return
		}
		// Runs outside any sweep transaction (no locks held), so a
		// synchronous create through the real path is safe here.
		fid := fmt.Sprintf("f-writer-%d", attempt)
		p := fmt.Sprintf("/w/zz-writer-%d.txt", attempt)
		if err := insertConfirmedFileE(s, fid); err != nil {
			writerErr = err
			return
		}
		if err := createFileViaRealPath(s, p, fid); err != nil {
			writerErr = err
			return
		}
		created = append(created, p)
	})

	_, err := s.DeleteDirRecursive(context.Background(), "/w/")
	if !errors.Is(err, ErrDirectoryNotEmpty) {
		t.Fatalf("DeleteDirRecursive err = %v, want ErrDirectoryNotEmpty", err)
	}
	if writerErr != nil {
		t.Fatalf("writer: %v", writerErr)
	}
	if len(created) != 2 {
		t.Fatalf("writer created %d files, want 2 (one per root attempt)", len(created))
	}

	// Stop the writer: a fresh call resumes and completes the delete.
	if err := failpoint.Disable(fpBeforeRootDelete); err != nil {
		t.Fatalf("disable failpoint: %v", err)
	}
	if _, err := s.DeleteDirRecursive(context.Background(), "/w/"); err != nil {
		t.Fatalf("second DeleteDirRecursive: %v", err)
	}
	if n := countNodesLike(t, s, "/w/%"); n != 0 {
		t.Fatalf("%d nodes remain under /w/, want 0", n)
	}
	assertNodeGone(t, s, "/w/")
	if n := countAllGCTasks(t, s); n != 27 {
		t.Fatalf("total gc tasks = %d, want 27 (25 fixture + 2 writer)", n)
	}
}

// TestDeleteDirBatchedRenameOutMidSweep renames a subdirectory out of the
// target tree while the delete is in flight (sweep entered, root dentry
// pinned, before the first drain batch). The moved subtree must survive
// intact — the sweep deletes only rows it currently selects by node_id, so it
// can never follow stale paths — and must not enqueue GC for moved files.
//
// The rename runs synchronously inside the failpoint callback: no sweep
// transaction is open at this point, so RenameDir's unindexed
// `path LIKE ... FOR UPDATE` scan (design §2.2) cannot deadlock against the
// sweep. Pausing later — inside a drain batch — would deadlock the test,
// because that scan would block on the paused batch's locks.
func TestDeleteDirBatchedRenameOutMidSweep(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)
	now := time.Now()

	looseIDs := bulkFixtureTree(t, s, "/r/", 25)
	var subIDs []string
	subNodes := []*FileNode{dirTestNode("/r/sub/", now)}
	for i := 0; i < 10; i++ {
		fid := fmt.Sprintf("sub-f%02d", i)
		subIDs = append(subIDs, fid)
		insertConfirmedFile(t, s, fid, now)
		subNodes = append(subNodes, fileTestNode(fmt.Sprintf("/r/sub/s%02d.txt", i), fid, now))
	}
	bulkInsertNodes(t, s, subNodes...)

	var renameErr error
	var renamed int64
	var fired atomic.Bool
	enableCallFailpoint(t, fpDrainBeforeSelect, func(parent string) {
		if parent != "/r/" || !fired.CompareAndSwap(false, true) {
			return
		}
		renamed, renameErr = s.RenameDir(context.Background(), "/r/sub/", "/outside/")
	})

	if _, err := s.DeleteDirRecursive(context.Background(), "/r/"); err != nil {
		t.Fatalf("DeleteDirRecursive: %v", err)
	}
	if !fired.Load() {
		t.Fatal("drain-before-select failpoint never fired")
	}
	if renameErr != nil {
		t.Fatalf("RenameDir: %v", renameErr)
	}
	if renamed != 11 {
		t.Fatalf("RenameDir moved %d rows, want 11 (dentry + 10 files)", renamed)
	}

	// The in-tree portion is fully gone.
	if n := countNodesLike(t, s, "/r/%"); n != 0 {
		t.Fatalf("%d nodes remain under /r/, want 0", n)
	}
	assertNodeGone(t, s, "/r/")

	// The renamed-away copy is intact, with no stray GC for its files.
	assertNodeAlive(t, s, "/outside/")
	for i, fid := range subIDs {
		assertNodeAlive(t, s, fmt.Sprintf("/outside/s%02d.txt", i))
		if n := countGCTasks(t, s, fid); n != 0 {
			t.Fatalf("gc tasks for moved file %s = %d, want 0", fid, n)
		}
		if st := inodeStatus(t, s, fid); st != string(StatusConfirmed) {
			t.Fatalf("inode %s status = %q, want CONFIRMED", fid, st)
		}
	}
	for _, fid := range looseIDs {
		if n := countGCTasks(t, s, fid); n != 1 {
			t.Fatalf("gc tasks for %s = %d, want 1", fid, n)
		}
	}
}

// TestDeleteDirBatchedPerBatchRetry injects synthetic deadlock errors into
// the first two drain-batch attempts and asserts the bounded per-batch retry
// absorbs them: the delete completes and only committed batches count toward
// the transaction budget.
func TestDeleteDirBatchedPerBatchRetry(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)

	fileIDs := bulkFixtureTree(t, s, "/b/", 25)

	// Two one-shot injected deadlocks, scoped to drain batches (not lifts).
	if err := failpoint.Enable(fpBatchError, `2*return("delete_dir_recursive_batch")`); err != nil {
		t.Fatalf("enable failpoint %s: %v", fpBatchError, err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(fpBatchError)
	})

	summary, err := s.DeleteDirRecursive(context.Background(), "/b/")
	if err != nil {
		t.Fatalf("DeleteDirRecursive: %v (retry must absorb the injected deadlocks)", err)
	}
	// 25 files / batch 20 = 2 committed drain batches; retried (rolled-back)
	// attempts must not count.
	if summary.TxCount != 2 {
		t.Fatalf("summary.TxCount = %d, want 2 committed batches (retries not counted)", summary.TxCount)
	}
	if summary.NodesDeleted != 25 {
		t.Fatalf("summary.NodesDeleted = %d, want 25", summary.NodesDeleted)
	}
	if n := countNodesLike(t, s, "/b/%"); n != 0 {
		t.Fatalf("%d nodes remain under /b/, want 0", n)
	}
	assertNodeGone(t, s, "/b/")
	for _, fid := range fileIDs {
		if n := countGCTasks(t, s, fid); n != 1 {
			t.Fatalf("gc tasks for %s = %d, want 1", fid, n)
		}
	}
}
