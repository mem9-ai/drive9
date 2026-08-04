package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

// Batched recursive directory delete, per
// docs/design/recursive-delete-batched-design.md. Enabled with
// DRIVE9_RECURSIVE_DELETE_BATCHED; when unset the legacy single-transaction
// implementation runs unchanged.
//
// Invariants (see the design doc §4.2):
//
//	I1  a directory dentry is deleted only in a transaction that verified it
//	    childless, with a post-DELETE re-check in the same transaction;
//	I2  the root dentry is deleted last — while it exists it pins the path
//	    against recreation (idx_path uniqueness);
//	I3  orphan/GC decisions derive only from rows deleted in the same
//	    transaction;
//	I4  a directory that was non-empty at first visit is re-evaluated for
//	    unlinking after it drains (post-order lift);
//	I5  a drain batch that deleted zero rows breaks out — batch fullness is
//	    never a progress signal.

// errDirDeleteRaced marks a batch/lift whose post-DELETE re-check found new
// children: the transaction must roll back and the directory be revisited.
var errDirDeleteRaced = errors.New("directory delete raced with concurrent create")

// DeleteDirRecursiveSummary reports the outcome of a recursive delete.
type DeleteDirRecursiveSummary struct {
	NodesDeleted    int64
	OrphansEnqueued int64
	TxCount         int64
}

// recursiveDeleteBatchedEnabled reports whether the batched implementation is
// enabled. Read per call so tests and operators can toggle it without a
// restart.
func recursiveDeleteBatchedEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DRIVE9_RECURSIVE_DELETE_BATCHED"))) {
	case "1", "true", "on":
		return true
	default:
		return false
	}
}

// DeleteDirRecursive deletes dirPath and its whole subtree. With the batched
// flag enabled, small trees are deleted in one atomic transaction (fast path)
// and large trees in bounded per-batch transactions (post-order sweep);
// success means the root dentry is gone. Without the flag, the legacy
// single-transaction behavior applies.
func (s *Store) DeleteDirRecursive(ctx context.Context, dirPath string) (out *DeleteDirRecursiveSummary, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "delete_dir_recursive", start, &err)

	if recursiveDeleteBatchedEnabled() {
		return s.deleteDirRecursiveBatched(ctx, dirPath)
	}
	return s.deleteDirRecursiveLegacy(ctx, dirPath)
}

const (
	// deleteDirEstimateMaxDirs bounds the counting walk used to derive the
	// transaction budget.
	deleteDirEstimateMaxDirs = 100_000
)

// Tunables are package-level variables so tests can shrink them.
var (
	// deleteDirSweepBatchSize bounds the rows touched by one sweep
	// transaction.
	deleteDirSweepBatchSize = deleteFileIDBatchSize
	// deleteDirMaxDuration bounds one recursive delete call; the sweep is
	// resumable, so a bounded caller may simply retry.
	deleteDirMaxDuration = 5 * time.Minute
	// deleteDirRootAttempts bounds root-dentry retries against racing
	// concurrent writers before ErrDirectoryNotEmpty is returned.
	deleteDirRootAttempts = 5
	// deleteDirBatchRetries bounds deadlock/lock-wait retries per batch
	// transaction.
	deleteDirBatchRetries = 3
	// deleteDirMaxBatchesCap caps the derived transaction budget.
	deleteDirMaxBatchesCap = int64(1_000_000)
)

func isLockWaitTimeout(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Lock wait timeout exceeded")
}

func isRetryableTxnError(err error) bool {
	return isDeadlock(err) || isLockWaitTimeout(err)
}

// deleteChildRow is one file_nodes row selected during a drain batch.
type deleteChildRow struct {
	nodeID string
	path   string
	fileID string
	isDir  bool
}

// dirDeleteQueue is a LIFO stack of directory paths with set dedup, giving a
// naturally post-order-ish traversal (deep directories are processed before
// their ancestors).
type dirDeleteQueue struct {
	stack []string
	in    map[string]bool
}

func newDirDeleteQueue() *dirDeleteQueue { return &dirDeleteQueue{in: make(map[string]bool)} }

func (q *dirDeleteQueue) push(p string) {
	if p == "" || q.in[p] {
		return
	}
	q.in[p] = true
	q.stack = append(q.stack, p)
}

func (q *dirDeleteQueue) pop() (string, bool) {
	if len(q.stack) == 0 {
		return "", false
	}
	p := q.stack[len(q.stack)-1]
	q.stack = q.stack[:len(q.stack)-1]
	delete(q.in, p)
	return p, true
}

func (q *dirDeleteQueue) empty() bool { return len(q.stack) == 0 }

// AbortUploadsByTargetPrefix best-effort aborts active uploads whose target
// path lies under dirPath, so their finalize cannot materialize nodes inside
// a tree being deleted. uploads has no target_path prefix index, so this is a
// bounded scan of the tenant's active uploads.
func (s *Store) AbortUploadsByTargetPrefix(ctx context.Context, dirPath string) (n int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "abort_uploads_by_target_prefix", start, &err)

	where, args := pathPrefixPredicate("target_path", dirPath)
	res, err := s.db.ExecContext(ctx, `UPDATE uploads SET status = 'ABORTED', updated_at = ?
		WHERE `+s.scope.And(`status IN ('INITIATED', 'UPLOADING') AND `+where),
		append([]any{time.Now().UTC()}, s.scope.Args(args...)...)...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// deleteDirRecursiveBatched implements the flag-on path: fast path for small
// trees, post-order batched sweep otherwise.
func (s *Store) deleteDirRecursiveBatched(ctx context.Context, dirPath string) (*DeleteDirRecursiveSummary, error) {
	if dirPath == "/" {
		return nil, ErrInvalidRootDentry
	}

	// Step 0: best-effort abort of in-flight uploads into the subtree.
	if n, err := s.AbortUploadsByTargetPrefix(ctx, dirPath); err != nil {
		logger.Warn(ctx, "delete_dir_recursive_abort_uploads_failed", zap.String("path", dirPath), zap.Error(err))
	} else if n > 0 {
		logger.Info(ctx, "delete_dir_recursive_abort_uploads", zap.String("path", dirPath), zap.Int64("aborted", n))
	}

	// The root dentry must exist (I2 pins the path for the whole operation).
	if _, err := s.lookupDirNodeID(ctx, dirPath); err != nil {
		return nil, err
	}

	// Step 1: fast path — the whole subtree in one transaction.
	summary, done, err := s.deleteDirFastPath(ctx, dirPath)
	if err != nil {
		return nil, err
	}
	if done {
		logger.Info(ctx, "delete_dir_recursive_done",
			zap.String("path", dirPath), zap.String("mode", "fast_path"),
			zap.Int64("nodes_deleted", summary.NodesDeleted),
			zap.Int64("orphans_enqueued", summary.OrphansEnqueued),
			zap.Int64("tx_count", summary.TxCount))
		return summary, nil
	}
	summary = &DeleteDirRecursiveSummary{}

	// Budget derived from the (walkable) subtree estimate: deep chains cost
	// ~2 transactions per directory, wide dirs ~1 per batch of files.
	files, dirs := s.estimateSubtreeSize(ctx, dirPath)
	maxBatches := int64(32) + 8*(files/int64(deleteDirSweepBatchSize)+dirs+1)
	if maxBatches > deleteDirMaxBatchesCap {
		maxBatches = deleteDirMaxBatchesCap
	}

	sw := &dirSweeper{
		s:          s,
		root:       dirPath,
		queue:      newDirDeleteQueue(),
		maxBatches: maxBatches,
		deadline:   time.Now().Add(deleteDirMaxDuration),
		summary:    summary,
	}

	// Steps 2+3: sweep, then delete the root last; a non-empty root means
	// concurrent writers raced us — re-seed and go again, bounded.
	for attempt := 0; ; attempt++ {
		sw.queue.push(dirPath)
		if err := sw.sweep(ctx); err != nil {
			return sw.summary, err
		}
		failpoint.InjectCall("deleteDirBeforeRootDelete", dirPath, attempt)
		err := s.DeleteEmptyDir(ctx, dirPath)
		if err == nil || errors.Is(err, ErrNotFound) {
			// ErrNotFound: a concurrent delete removed the root — the goal
			// state is reached either way.
			logger.Info(ctx, "delete_dir_recursive_done",
				zap.String("path", dirPath), zap.String("mode", "sweep"),
				zap.Int64("nodes_deleted", sw.summary.NodesDeleted),
				zap.Int64("orphans_enqueued", sw.summary.OrphansEnqueued),
				zap.Int64("tx_count", sw.summary.TxCount))
			return sw.summary, nil
		}
		if !errors.Is(err, ErrDirectoryNotEmpty) {
			return sw.summary, err
		}
		if attempt+1 >= deleteDirRootAttempts {
			return sw.summary, fmt.Errorf("%w: %s", ErrDirectoryNotEmpty, dirPath)
		}
	}
}

// lookupDirNodeID returns the root directory's node_id, or ErrNotFound.
func (s *Store) lookupDirNodeID(ctx context.Context, dirPath string) (string, error) {
	var nodeID string
	err := s.db.QueryRowContext(ctx, `SELECT node_id FROM file_nodes WHERE `+
		s.scope.And(`path_hash = ? AND path = ? AND is_directory = 1`),
		s.scope.Args(fileNodePathHash(dirPath), dirPath)...).Scan(&nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return nodeID, nil
}

// estimateSubtreeSize counts files and directories under dirPath with an
// indexed per-directory COUNT walk. The walk is bounded by
// deleteDirEstimateMaxDirs; a partial estimate still yields a safe budget.
func (s *Store) estimateSubtreeSize(ctx context.Context, dirPath string) (files, dirs int64) {
	queue := []string{dirPath}
	for len(queue) > 0 && dirs < deleteDirEstimateMaxDirs {
		parent := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		var total, dirCount int64
		err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(is_directory), 0) FROM file_nodes
			WHERE `+s.scope.And(`parent_path_hash = ? AND parent_path = ?`),
			s.scope.Args(fileNodePathHash(parent), parent)...).Scan(&total, &dirCount)
		if err != nil {
			return files, dirs
		}
		files += total - dirCount
		dirs += dirCount
		// Enqueue child directories by name page to avoid unbounded memory.
		offset := ""
		for {
			rows, err := s.db.QueryContext(ctx, `SELECT path FROM file_nodes
				WHERE `+s.scope.And(`parent_path_hash = ? AND parent_path = ? AND is_directory = 1 AND name > ?`)+`
				ORDER BY name LIMIT 1000`,
				s.scope.Args(fileNodePathHash(parent), parent, offset)...)
			if err != nil {
				return files, dirs
			}
			n := 0
			for rows.Next() {
				var p string
				if err := rows.Scan(&p); err != nil {
					_ = rows.Close()
					return files, dirs
				}
				queue = append(queue, p)
				offset = baseName(p)
				n++
			}
			_ = rows.Close()
			if n < 1000 {
				break
			}
		}
	}
	return files, dirs
}

// --- fast path ---

// deleteDirFastPath attempts the whole subtree in a single transaction when
// it enumerates to at most deleteDirSweepBatchSize nodes. done=false means the
// caller should continue with the batched sweep.
func (s *Store) deleteDirFastPath(ctx context.Context, dirPath string) (summary *DeleteDirRecursiveSummary, done bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Bounded BFS enumeration, children locked FOR UPDATE as we go.
	var nodes []deleteChildRow
	queue := []string{dirPath}
	for len(queue) > 0 {
		parent := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		remaining := deleteDirSweepBatchSize - len(nodes) + 1
		rows, err := s.selectChildrenForUpdateTx(ctx, tx, parent, remaining)
		if err != nil {
			return nil, false, err
		}
		for _, r := range rows {
			if r.isDir {
				queue = append(queue, r.path)
			}
		}
		nodes = append(nodes, rows...)
		if len(nodes) > deleteDirSweepBatchSize {
			return nil, false, nil // overflow → sweep
		}
	}

	// Children before parents: directory paths end in '/', so a deeper path
	// always sorts after its ancestors.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].path > nodes[j].path })

	var fileIDs []string
	for start := 0; start < len(nodes); start += deleteFileIDBatchSize {
		end := start + deleteFileIDBatchSize
		if end > len(nodes) {
			end = len(nodes)
		}
		batch := nodes[start:end]
		ids := make([]any, 0, len(batch))
		for _, n := range batch {
			ids = append(ids, n.nodeID)
			if !n.isDir && n.fileID != "" {
				fileIDs = append(fileIDs, n.fileID)
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM file_nodes WHERE `+
			s.scope.And(`node_id IN (`+questionPlaceholders(len(batch))+`)`),
			s.scope.Args(ids...)...); err != nil {
			return nil, false, err
		}
	}

	orphans, err := s.processOrphansTx(ctx, tx, fileIDs)
	if err != nil {
		return nil, false, err
	}

	// Root last (I2), with a post-DELETE re-check (I1).
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_nodes WHERE `+
		s.scope.And(`path_hash = ? AND path = ? AND is_directory = 1`),
		s.scope.Args(fileNodePathHash(dirPath), dirPath)...); err != nil {
		return nil, false, err
	}
	hasChildren, err := s.dirHasChildrenTx(ctx, tx, dirPath)
	if err != nil {
		return nil, false, err
	}
	if hasChildren {
		return nil, false, nil // raced with a concurrent create → sweep
	}

	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return &DeleteDirRecursiveSummary{
		NodesDeleted:    int64(len(nodes)) + 1,
		OrphansEnqueued: orphans,
		TxCount:         1,
	}, true, nil
}

// --- batched sweep ---

type dirSweeper struct {
	s          *Store
	root       string
	queue      *dirDeleteQueue
	maxBatches int64
	deadline   time.Time
	summary    *DeleteDirRecursiveSummary
}

func (sw *dirSweeper) checkBudget(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sw.summary.TxCount >= sw.maxBatches || time.Now().After(sw.deadline) {
		return fmt.Errorf("%w: %s (tx=%d)", ErrDeleteIncomplete, sw.root, sw.summary.TxCount)
	}
	return nil
}

// sweep drains the work queue in bounded per-batch transactions.
func (sw *dirSweeper) sweep(ctx context.Context) error {
	for !sw.queue.empty() {
		parent, _ := sw.queue.pop()

		// Drain this directory in batches.
		raced := false
		for {
			if err := sw.checkBudget(ctx); err != nil {
				return err
			}
			failpoint.InjectCall("drainBatchBeforeSelect", parent)
			out, err := sw.runDrainBatch(ctx, parent)
			if errors.Is(err, errDirDeleteRaced) {
				// Post-DELETE re-check failed: revisit the raced subdir and
				// this parent promptly (I1); skip the lift this round.
				sw.queue.push(parent)
				sw.queue.push(out.racedDir)
				raced = true
				break
			}
			if err != nil {
				return err
			}
			sw.summary.NodesDeleted += out.deleted
			sw.summary.OrphansEnqueued += out.orphans
			for _, sub := range out.subdirs {
				sw.queue.push(sub)
			}
			if out.deleted == 0 {
				break // I5: nothing deletable at this level — descend first
			}
			if out.rows < int64(deleteDirSweepBatchSize) {
				break // level drained
			}
		}

		// Post-order lift (I4): delete the drained directory's own dentry.
		// The root is owned by the caller's DeleteEmptyDir step.
		if raced || parent == sw.root {
			continue
		}
		if err := sw.checkBudget(ctx); err != nil {
			return err
		}
		lifted, err := sw.runLift(ctx, parent)
		if err != nil {
			return err
		}
		if lifted {
			sw.summary.NodesDeleted++
			sw.queue.push(parentPath(parent))
		}
	}
	return nil
}

type drainOutcome struct {
	rows     int64
	deleted  int64
	orphans  int64
	fileIDs  []string
	subdirs  []string
	racedDir string
}

// runDrainBatch executes one drain batch transaction with bounded retries on
// deadlock/lock-wait.
func (sw *dirSweeper) runDrainBatch(ctx context.Context, parent string) (*drainOutcome, error) {
	var out *drainOutcome
	err := sw.withBatchRetry(ctx, "delete_dir_recursive_batch", func(tx *sql.Tx) error {
		var err error
		out, err = sw.s.drainChildrenBatchTx(ctx, tx, parent)
		return err
	})
	if err != nil {
		if out == nil {
			out = &drainOutcome{}
		}
		return out, err
	}
	sw.summary.TxCount++
	return out, nil
}

// runLift executes one post-order lift transaction. lifted=true means the
// dentry was deleted and the parent should be revisited. A non-empty or
// already-gone directory is a committed no-op (idempotent).
func (sw *dirSweeper) runLift(ctx context.Context, dirPath string) (lifted bool, err error) {
	err = sw.withBatchRetry(ctx, "delete_dir_recursive_lift", func(tx *sql.Tx) error {
		lifted = false
		failpoint.InjectCall("liftBeforePathLookup", dirPath)
		var nodeID string
		err := tx.QueryRowContext(ctx, `SELECT node_id FROM file_nodes WHERE `+
			sw.s.scope.And(`path_hash = ? AND path = ? AND is_directory = 1`)+` FOR UPDATE`,
			sw.s.scope.Args(fileNodePathHash(dirPath), dirPath)...).Scan(&nodeID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // already gone (concurrent rmdir): idempotent success
		}
		if err != nil {
			return err
		}
		has, err := sw.s.dirHasChildrenTx(ctx, tx, dirPath)
		if err != nil {
			return err
		}
		if has {
			return nil // legit children queued below, or racing creates
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM file_nodes WHERE `+
			sw.s.scope.And(`node_id = ?`), sw.s.scope.Args(nodeID)...); err != nil {
			return err
		}
		still, err := sw.s.dirHasChildrenTx(ctx, tx, dirPath)
		if err != nil {
			return err
		}
		if still {
			return errDirDeleteRaced // rollback; step-3 re-seed rediscovers it
		}
		lifted = true
		return nil
	})
	if errors.Is(err, errDirDeleteRaced) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	sw.summary.TxCount++
	return lifted, nil
}

// withBatchRetry runs fn in a transaction, retrying deadlock/lock-wait
// failures with backoff. errDirDeleteRaced and other errors propagate without
// retry.
func (sw *dirSweeper) withBatchRetry(ctx context.Context, op string, fn func(tx *sql.Tx) error) (err error) {
	start := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
			if errors.Is(err, errDirDeleteRaced) {
				result = "raced"
			}
		}
		metrics.RecordOperation("datastore", op, result, time.Since(start))
	}()

	for attempt := 0; ; attempt++ {
		tx, err := sw.s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		fnErr := fn(tx)
		if fnErr == nil {
			fnErr = deleteDirBatchFailpointError(op)
		}
		if fnErr != nil {
			_ = tx.Rollback()
			if isRetryableTxnError(fnErr) && attempt < deleteDirBatchRetries {
				metrics.RecordOperation("datastore", op+"_retry", "retryable", 0)
				if !sleepWithContext(ctx, time.Duration(50*(1<<attempt))*time.Millisecond) {
					return ctx.Err()
				}
				continue
			}
			return fnErr
		}
		if err := tx.Commit(); err != nil {
			if isRetryableTxnError(err) && attempt < deleteDirBatchRetries {
				metrics.RecordOperation("datastore", op+"_retry", "retryable", 0)
				if !sleepWithContext(ctx, time.Duration(50*(1<<attempt))*time.Millisecond) {
					return ctx.Err()
				}
				continue
			}
			return err
		}
		return nil
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// errDeleteDirBatchInjected is the synthetic deadlock returned by
// deleteDirBatchFailpointError when its failpoint is armed; the message keeps
// isRetryableTxnError true so the batch retry path is exercised.
var errDeleteDirBatchInjected = errors.New("injected failpoint: Deadlock found when trying to get lock; try restarting transaction")

// deleteDirBatchFailpointError lets failpoint tests inject one synthetic
// retryable error per batch operation (op). It is a no-op unless the
// failpoint is armed: the failpoint.Inject marker compiles to an empty call
// until failpoint-ctl rewrites it, so normal runs see exactly the
// non-instrumented behavior.
func deleteDirBatchFailpointError(op string) error {
	var injected error
	failpoint.Inject("deleteDirBatchError", func(val failpoint.Value) {
		if name, ok := val.(string); ok && name == op {
			injected = errDeleteDirBatchInjected
		}
	})
	return injected
}

// selectChildrenForUpdateTx selects up to limit children of parent, locking
// them FOR UPDATE. The (parent_path_hash, name) index makes this a bounded
// range scan.
func (s *Store) selectChildrenForUpdateTx(ctx context.Context, tx *sql.Tx, parent string, limit int) ([]deleteChildRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT node_id, path, COALESCE(file_id, ''), is_directory FROM file_nodes
		WHERE `+s.scope.And(`parent_path_hash = ? AND parent_path = ?`)+`
		ORDER BY name LIMIT ? FOR UPDATE`,
		s.scope.Args(fileNodePathHash(parent), parent, limit)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []deleteChildRow
	for rows.Next() {
		var r deleteChildRow
		var isDir int
		if err := rows.Scan(&r.nodeID, &r.path, &r.fileID, &isDir); err != nil {
			return nil, err
		}
		r.isDir = isDir != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// drainChildrenBatchTx processes one batch of parent's children: files are
// deleted and fed to the orphan pipeline (I3), empty directories are deleted
// with a post-DELETE re-check (I1), non-empty directories are queued for
// descent.
func (s *Store) drainChildrenBatchTx(ctx context.Context, tx *sql.Tx, parent string) (*drainOutcome, error) {
	rows, err := s.selectChildrenForUpdateTx(ctx, tx, parent, deleteDirSweepBatchSize)
	if err != nil {
		return nil, err
	}
	failpoint.InjectCall("drainChildrenBatchAfterSelect", parent)
	out := &drainOutcome{rows: int64(len(rows))}
	for _, r := range rows {
		if !r.isDir {
			if _, err := tx.ExecContext(ctx, `DELETE FROM file_nodes WHERE `+
				s.scope.And(`node_id = ?`), s.scope.Args(r.nodeID)...); err != nil {
				return nil, err
			}
			out.deleted++
			if r.fileID != "" {
				out.fileIDs = append(out.fileIDs, r.fileID)
			}
			continue
		}
		has, err := s.dirHasChildrenTx(ctx, tx, r.path)
		if err != nil {
			return nil, err
		}
		if has {
			out.subdirs = append(out.subdirs, r.path)
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM file_nodes WHERE `+
			s.scope.And(`node_id = ?`), s.scope.Args(r.nodeID)...); err != nil {
			return nil, err
		}
		out.deleted++
		failpoint.InjectCall("drainEmptyDirBeforeRecheck", r.path)
		still, err := s.dirHasChildrenTx(ctx, tx, r.path)
		if err != nil {
			return nil, err
		}
		if still {
			out.racedDir = r.path
			return out, errDirDeleteRaced
		}
	}
	orphans, err := s.processOrphansTx(ctx, tx, out.fileIDs)
	if err != nil {
		return nil, err
	}
	out.orphans = orphans
	return out, nil
}

// processOrphansTx runs the orphan pipeline for file IDs whose dentries were
// deleted in this transaction: lock the inodes, keep only IDs with no
// remaining file_nodes reference, mark them DELETED, drop their tags, and
// enqueue batched GC tasks.
func (s *Store) processOrphansTx(ctx context.Context, tx *sql.Tx, fileIDs []string) (int64, error) {
	if len(fileIDs) == 0 {
		return 0, nil
	}
	sort.Strings(fileIDs)
	deduped := fileIDs[:0]
	for i, id := range fileIDs {
		if i == 0 || id != fileIDs[i-1] {
			deduped = append(deduped, id)
		}
	}
	if err := s.lockFileIDsForDeleteTx(ctx, tx, deduped); err != nil {
		return 0, err
	}
	orphaned, err := s.scanOrphanedFilesByIDTx(ctx, tx, deduped)
	if err != nil {
		return 0, err
	}
	if len(orphaned) == 0 {
		return 0, nil
	}
	orphanIDs := make([]string, 0, len(orphaned))
	for _, f := range orphaned {
		orphanIDs = append(orphanIDs, f.FileID)
	}
	if err := s.markFilesDeletedTx(ctx, tx, orphanIDs); err != nil {
		return 0, err
	}
	if err := s.deleteFileTagsByIDsTx(ctx, tx, orphanIDs); err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	tasks := make([]*FileGCTask, 0, len(orphaned))
	for _, f := range orphaned {
		task, err := NewFileGCTaskFromFile(f, now)
		if err != nil {
			return 0, err
		}
		tasks = append(tasks, task)
	}
	return s.enqueueFileGCTasksTx(tx, tasks)
}
