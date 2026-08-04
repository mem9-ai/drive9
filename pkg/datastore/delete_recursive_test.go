package datastore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Tests for the batched recursive directory delete
// (docs/design/recursive-delete-batched-design.md §4.2/§6). All tests run with
// DRIVE9_RECURSIVE_DELETE_BATCHED=1 except the legacy smoke test.

// setDeleteDirTunables overrides the batched-delete tunables for one test and
// restores them on cleanup.
func setDeleteDirTunables(t *testing.T, batchSize int, maxDuration time.Duration) {
	t.Helper()
	oldBatch, oldDuration := deleteDirSweepBatchSize, deleteDirMaxDuration
	deleteDirSweepBatchSize = batchSize
	deleteDirMaxDuration = maxDuration
	t.Cleanup(func() {
		deleteDirSweepBatchSize = oldBatch
		deleteDirMaxDuration = oldDuration
	})
}

// dirTestNode builds a directory dentry for path (must end in '/').
func dirTestNode(path string, now time.Time) *FileNode {
	return &FileNode{
		NodeID:      "n" + fileNodePathHash(path)[:20],
		Path:        path,
		ParentPath:  parentPath(path),
		Name:        baseName(path),
		IsDirectory: true,
		CreatedAt:   now,
	}
}

// fileTestNode builds a file dentry for path (no trailing '/') backed by fileID.
func fileTestNode(path, fileID string, now time.Time) *FileNode {
	return &FileNode{
		NodeID:     "n" + fileNodePathHash(path)[:20],
		Path:       path,
		ParentPath: parentPath(path),
		Name:       baseName(path),
		FileID:     fileID,
		CreatedAt:  now,
	}
}

// bulkInsertNodes inserts dentries directly, bypassing per-row InsertNode so
// large/deep fixture trees stay fast.
func bulkInsertNodes(t *testing.T, s *Store, nodes ...*FileNode) {
	t.Helper()
	const cols = `node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory, file_id, inode_id, created_at`
	for start := 0; start < len(nodes); start += 200 {
		end := min(start+200, len(nodes))
		var sb strings.Builder
		sb.WriteString(`INSERT INTO file_nodes (` + s.scope.InsCols(cols) + `) VALUES `)
		var args []any
		for i, n := range nodes[start:end] {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(" + s.scope.InsVals("?, ?, ?, ?, ?, ?, ?, ?, ?, ?") + ")")
			args = append(args, s.scope.Args(n.NodeID, n.Path, fileNodePathHash(n.Path), n.ParentPath,
				fileNodePathHash(n.ParentPath), n.Name, n.IsDirectory, nullStr(n.FileID), nullStr(n.InodeID),
				n.CreatedAt.UTC())...)
		}
		if _, err := s.db.Exec(sb.String(), args...); err != nil {
			t.Fatalf("bulk insert nodes: %v", err)
		}
	}
}

// insertConfirmedFile inserts one CONFIRMED db9-backed file identity.
func insertConfirmedFile(t *testing.T, s *Store, fileID string, now time.Time) {
	t.Helper()
	if err := s.InsertFile(context.Background(), &File{
		FileID: fileID, StorageType: StorageDB9, StorageRef: "/blobs/" + fileID,
		SizeBytes: 1, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		t.Fatalf("InsertFile %s: %v", fileID, err)
	}
}

func assertNodeGone(t *testing.T, s *Store, path string) {
	t.Helper()
	if _, err := s.GetNode(context.Background(), path); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetNode(%q) err = %v, want ErrNotFound", path, err)
	}
}

func assertNodeAlive(t *testing.T, s *Store, path string) {
	t.Helper()
	if _, err := s.GetNode(context.Background(), path); err != nil {
		t.Errorf("GetNode(%q) err = %v, want alive", path, err)
	}
}

// countNodesLike counts file_nodes rows whose path matches a SQL LIKE pattern
// (test fixture paths are chosen free of LIKE metacharacters where a plain
// pattern is used).
func countNodesLike(t *testing.T, s *Store, pattern string) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE ?`, pattern).Scan(&n); err != nil {
		t.Fatalf("count nodes like %q: %v", pattern, err)
	}
	return n
}

func countDirNodesLike(t *testing.T, s *Store, pattern string) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE is_directory = 1 AND path LIKE ?`, pattern).Scan(&n); err != nil {
		t.Fatalf("count dir nodes like %q: %v", pattern, err)
	}
	return n
}

func countGCTasks(t *testing.T, s *Store, fileID string) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM file_gc_tasks WHERE file_id = ?`, fileID).Scan(&n); err != nil {
		t.Fatalf("count gc tasks for %q: %v", fileID, err)
	}
	return n
}

func TestDeleteDirBatchedSmallTreeFastPath(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	s := newTestStore(t)
	now := time.Now()

	insertConfirmedFile(t, s, "f1", now)
	insertConfirmedFile(t, s, "f2", now)
	bulkInsertNodes(t, s,
		dirTestNode("/data/", now),
		dirTestNode("/data/sub/", now),
		fileTestNode("/data/a.txt", "f1", now),
		fileTestNode("/data/sub/b.txt", "f2", now),
	)

	summary, err := s.DeleteDirRecursive(context.Background(), "/data/")
	if err != nil {
		t.Fatalf("DeleteDirRecursive: %v", err)
	}
	if summary.TxCount != 1 {
		t.Fatalf("summary.TxCount = %d, want 1 (fast path single transaction)", summary.TxCount)
	}
	if summary.NodesDeleted != 4 || summary.OrphansEnqueued != 2 {
		t.Fatalf("summary = %+v, want 4 nodes deleted and 2 orphans", summary)
	}
	for _, p := range []string{"/data/", "/data/sub/", "/data/a.txt", "/data/sub/b.txt"} {
		assertNodeGone(t, s, p)
	}
	for _, fid := range []string{"f1", "f2"} {
		task, err := s.GetFileGCTaskByFileID(context.Background(), fid)
		if err != nil {
			t.Fatalf("get gc task %s: %v", fid, err)
		}
		if task.Status != FileGCTaskQueued || task.StorageRef != "/blobs/"+fid {
			t.Fatalf("unexpected gc task for %s: %+v", fid, task)
		}
	}
}

func TestDeleteDirBatchedDeepTreePostOrder(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)
	now := time.Now()

	// Depth-3 tree whose intermediate directories are non-empty at first
	// visit; 26 subtree nodes overflows batch=20 so the sweep (not the fast
	// path) handles it. The post-order lift (I4) must remove every directory
	// dentry.
	nodes := []*FileNode{dirTestNode("/d/", now), dirTestNode("/d/a/", now), dirTestNode("/d/a/b/", now)}
	var fileIDs []string
	for i, dir := range []string{"/d/", "/d/a/", "/d/a/b/"} {
		for j := 0; j < 8; j++ {
			fid := fmt.Sprintf("f-%d-%d", i, j)
			fileIDs = append(fileIDs, fid)
			insertConfirmedFile(t, s, fid, now)
			nodes = append(nodes, fileTestNode(fmt.Sprintf("%sfile-%d.txt", dir, j), fid, now))
		}
	}
	bulkInsertNodes(t, s, nodes...)

	if _, err := s.DeleteDirRecursive(context.Background(), "/d/"); err != nil {
		t.Fatalf("DeleteDirRecursive: %v", err)
	}
	// Regression red line: zero directory dentries left under /d/.
	if n := countDirNodesLike(t, s, "/d/%"); n != 0 {
		t.Fatalf("%d directory dentries remain under /d/, want 0 (post-order lift)", n)
	}
	if n := countNodesLike(t, s, "/d/%"); n != 0 {
		t.Fatalf("%d nodes remain under /d/, want 0", n)
	}
	assertNodeGone(t, s, "/d/")
	for _, fid := range fileIDs {
		if n := countGCTasks(t, s, fid); n != 1 {
			t.Fatalf("gc tasks for %s = %d, want 1", fid, n)
		}
	}
}

func TestDeleteDirBatchedDrainLivelock(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)
	now := time.Now()

	// 45 non-empty subdirectories (> 2x batch size): a drain batch of /w/
	// deletes zero rows, so a "batch full means progress" loop would spin
	// forever (I5 regression).
	nodes := []*FileNode{dirTestNode("/w/", now)}
	for i := 0; i < 45; i++ {
		dir := fmt.Sprintf("/w/w%02d/", i)
		fid := fmt.Sprintf("f-%02d", i)
		insertConfirmedFile(t, s, fid, now)
		nodes = append(nodes, dirTestNode(dir, now), fileTestNode(dir+"x.txt", fid, now))
	}
	bulkInsertNodes(t, s, nodes...)

	if _, err := s.DeleteDirRecursive(context.Background(), "/w/"); err != nil {
		t.Fatalf("DeleteDirRecursive: %v (must not return ErrDeleteIncomplete)", err)
	}
	if n := countNodesLike(t, s, "/w/%"); n != 0 {
		t.Fatalf("%d nodes remain under /w/, want 0", n)
	}
	assertNodeGone(t, s, "/w/")
}

func TestDeleteDirBatchedDeepChain(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)
	now := time.Now()

	// 100-level single-child chain: ~2 transactions per directory, so a
	// budget derived from file counts alone would abort with
	// ErrDeleteIncomplete (regression for enumeration-derived budget).
	insertConfirmedFile(t, s, "f-leaf", now)
	nodes := []*FileNode{dirTestNode("/c0/", now)}
	prefix := "/c0/"
	for i := 1; i < 100; i++ {
		p := fmt.Sprintf("%sc%d/", prefix, i)
		nodes = append(nodes, dirTestNode(p, now))
		prefix = p
	}
	nodes = append(nodes, fileTestNode(prefix+"file.txt", "f-leaf", now))
	bulkInsertNodes(t, s, nodes...)

	if _, err := s.DeleteDirRecursive(context.Background(), "/c0/"); err != nil {
		t.Fatalf("DeleteDirRecursive: %v (must not return ErrDeleteIncomplete)", err)
	}
	if n := countNodesLike(t, s, "/c0/%"); n != 0 {
		t.Fatalf("%d nodes remain under /c0/, want 0", n)
	}
	assertNodeGone(t, s, "/c0/")
	if n := countGCTasks(t, s, "f-leaf"); n != 1 {
		t.Fatalf("gc tasks for f-leaf = %d, want 1", n)
	}
}

func TestDeleteDirBatchedResume(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	// 1ns max duration makes the sweep abort at its first budget check.
	setDeleteDirTunables(t, 20, time.Nanosecond)
	s := newTestStore(t)
	now := time.Now()

	nodes := []*FileNode{dirTestNode("/r/", now)}
	var fileIDs []string
	for i := 0; i < 30; i++ {
		fid := fmt.Sprintf("f-%02d", i)
		fileIDs = append(fileIDs, fid)
		insertConfirmedFile(t, s, fid, now)
		nodes = append(nodes, fileTestNode(fmt.Sprintf("/r/f%02d.txt", i), fid, now))
	}
	bulkInsertNodes(t, s, nodes...)

	_, err := s.DeleteDirRecursive(context.Background(), "/r/")
	if !errors.Is(err, ErrDeleteIncomplete) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("first DeleteDirRecursive err = %v, want ErrDeleteIncomplete or context error", err)
	}

	// Resume: restore a generous duration and re-call the same entry point.
	// (deleteDirMaxDuration still holds the 1ns override here, so set the
	// default explicitly.)
	setDeleteDirTunables(t, 20, 5*time.Minute)
	if _, err := s.DeleteDirRecursive(context.Background(), "/r/"); err != nil {
		t.Fatalf("resumed DeleteDirRecursive: %v", err)
	}
	if n := countNodesLike(t, s, "/r/%"); n != 0 {
		t.Fatalf("%d nodes remain under /r/ after resume, want 0", n)
	}
	assertNodeGone(t, s, "/r/")
	for _, fid := range fileIDs {
		if n := countGCTasks(t, s, fid); n != 1 {
			t.Fatalf("gc tasks for %s = %d, want exactly 1 (idempotent re-run)", fid, n)
		}
	}
}

func TestDeleteDirBatchedWideDirectory(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	setDeleteDirTunables(t, 20, deleteDirMaxDuration)
	s := newTestStore(t)
	now := time.Now()

	nodes := []*FileNode{dirTestNode("/wide/", now)}
	for i := 0; i < 100; i++ {
		fid := fmt.Sprintf("f-%03d", i)
		insertConfirmedFile(t, s, fid, now)
		nodes = append(nodes, fileTestNode(fmt.Sprintf("/wide/f%03d.txt", i), fid, now))
	}
	bulkInsertNodes(t, s, nodes...)

	if _, err := s.DeleteDirRecursive(context.Background(), "/wide/"); err != nil {
		t.Fatalf("DeleteDirRecursive: %v", err)
	}
	if n := countNodesLike(t, s, "/wide/%"); n != 0 {
		t.Fatalf("%d nodes remain under /wide/, want 0", n)
	}
	assertNodeGone(t, s, "/wide/")
	var gcTotal int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM file_gc_tasks`).Scan(&gcTotal); err != nil {
		t.Fatalf("count gc tasks: %v", err)
	}
	if gcTotal != 100 {
		t.Fatalf("gc tasks = %d, want 100", gcTotal)
	}
}

func TestDeleteDirBatchedHardlink(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	s := newTestStore(t)
	now := time.Now()

	insertConfirmedFile(t, s, "f1", now)
	bulkInsertNodes(t, s,
		dirTestNode("/del/", now),
		dirTestNode("/keep/", now),
		fileTestNode("/del/link.txt", "f1", now),
		fileTestNode("/keep/link.txt", "f1", now),
	)

	summary, err := s.DeleteDirRecursive(context.Background(), "/del/")
	if err != nil {
		t.Fatalf("DeleteDirRecursive /del/: %v", err)
	}
	if summary.OrphansEnqueued != 0 {
		t.Fatalf("orphans enqueued = %d, want 0 (f1 still referenced by /keep/)", summary.OrphansEnqueued)
	}
	if _, err := s.GetFileGCTaskByFileID(context.Background(), "f1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("gc task for f1 after first delete: err = %v, want ErrNotFound", err)
	}
	assertNodeAlive(t, s, "/keep/")
	assertNodeAlive(t, s, "/keep/link.txt")

	summary, err = s.DeleteDirRecursive(context.Background(), "/keep/")
	if err != nil {
		t.Fatalf("DeleteDirRecursive /keep/: %v", err)
	}
	if summary.OrphansEnqueued != 1 {
		t.Fatalf("orphans enqueued = %d, want 1 (last reference removed)", summary.OrphansEnqueued)
	}
	if n := countGCTasks(t, s, "f1"); n != 1 {
		t.Fatalf("gc tasks for f1 = %d, want 1 after last link deleted", n)
	}
}

func TestDeleteDirBatchedLikeMetachars(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	s := newTestStore(t)
	now := time.Now()

	insertConfirmedFile(t, s, "f1", now)
	insertConfirmedFile(t, s, "f2", now)
	bulkInsertNodes(t, s,
		dirTestNode("/a_b/", now),
		dirTestNode("/axb/", now),
		fileTestNode("/a_b/a.txt", "f1", now),
		fileTestNode("/axb/b.txt", "f2", now),
	)

	if _, err := s.DeleteDirRecursive(context.Background(), "/a_b/"); err != nil {
		t.Fatalf("DeleteDirRecursive /a_b/: %v", err)
	}
	assertNodeGone(t, s, "/a_b/")
	assertNodeGone(t, s, "/a_b/a.txt")
	// '_' is a LIKE single-character wildcard: an unescaped prefix match
	// would also delete /axb/ (regression for design §2.2).
	assertNodeAlive(t, s, "/axb/")
	assertNodeAlive(t, s, "/axb/b.txt")
	if n := countGCTasks(t, s, "f1"); n != 1 {
		t.Fatalf("gc tasks for f1 = %d, want 1", n)
	}
	if _, err := s.GetFileGCTaskByFileID(context.Background(), "f2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("gc task for f2: err = %v, want ErrNotFound", err)
	}
}

func TestDeleteDirBatchedRootNotFound(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "1")
	s := newTestStore(t)

	_, err := s.DeleteDirRecursive(context.Background(), "/missing/")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteDirRecursive err = %v, want ErrNotFound", err)
	}
}

func TestDeleteDirBatchedLegacyFlagOff(t *testing.T) {
	t.Setenv("DRIVE9_RECURSIVE_DELETE_BATCHED", "0")
	s := newTestStore(t)
	now := time.Now()

	insertConfirmedFile(t, s, "f1", now)
	insertConfirmedFile(t, s, "f2", now)
	bulkInsertNodes(t, s,
		dirTestNode("/data/", now),
		dirTestNode("/data/sub/", now),
		fileTestNode("/data/a.txt", "f1", now),
		fileTestNode("/data/sub/b.txt", "f2", now),
	)

	summary, err := s.DeleteDirRecursive(context.Background(), "/data/")
	if err != nil {
		t.Fatalf("DeleteDirRecursive (legacy): %v", err)
	}
	if summary.NodesDeleted != 4 || summary.OrphansEnqueued != 2 {
		t.Fatalf("summary = %+v, want 4 nodes deleted and 2 orphans", summary)
	}
	for _, p := range []string{"/data/", "/data/sub/", "/data/a.txt", "/data/sub/b.txt"} {
		assertNodeGone(t, s, p)
	}
	for _, fid := range []string{"f1", "f2"} {
		if n := countGCTasks(t, s, fid); n != 1 {
			t.Fatalf("gc tasks for %s = %d, want 1", fid, n)
		}
	}
}
