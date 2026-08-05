package datastore

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

// Orphan reconciliation sweep, per
// docs/design/recursive-delete-batched-design.md §4.2.1. The batched
// recursive delete (delete_recursive.go) has a single-statement residual race
// window at the commit boundary that can orphan a row; this sweep is the
// backstop that detects and repairs those orphans. It runs in dry-run
// (audit-only) mode by default and repairs only when
// DRIVE9_DELETE_RECONCILE_REPAIR is set.
//
// Safety rules that must not be weakened:
//
//   - A file_nodes row with parent_path == "/" is ALWAYS valid and is skipped
//     without any lookup: the root is implicit, there is no file_nodes row
//     for "/", so a parent point-check for a top-level row always misses.
//   - A non-top-level parent must exist AND be a directory
//     (is_directory = 1); existence alone is not sufficient.
//   - Repair locks the candidate row (SELECT ... FOR UPDATE) and re-runs the
//     parent/reference checks as current reads in the same transaction before
//     deleting anything; the scan itself is a snapshot read.
//   - The stranded-inode scan covers file inodes only: the contents JOIN
//     excludes directory inodes, whose leak is a deliberate Non-goal.

// DefaultDeleteReconcileBatchSize bounds the rows scanned per page when the
// caller does not specify a batch size.
const DefaultDeleteReconcileBatchSize = 500

// deleteReconcileMaxScanRows bounds the rows one reconciliation round scans
// per table, so a hot tenant cannot turn a 10-minute piggyback round into an
// hours-long scan. Repair progress persists across rounds (repaired rows
// disappear), so capping a round is safe; dry-run simply re-audits. A
// package-level variable so tests can shrink it.
var deleteReconcileMaxScanRows = 200_000

// DeleteReconcileState carries the scan cursors between reconciliation
// rounds. The per-tenant worker holds one instance per tenant (alongside its
// other throttle state), so a round that hits deleteReconcileMaxScanRows
// resumes where it stopped instead of re-auditing the same prefix forever.
// It is in-worker state, not durable: a pod restart re-covers earlier rows,
// which is acceptable for an audit/repair backstop whose repairs themselves
// persist. A nil state is equivalent to a fresh one (tests, one-shot runs).
//
// Cursors advance in node_id / inode_id sort order: rows created behind the
// cursor are seen by the current pass, rows created ahead of it wait for the
// next full pass. This is eventual coverage, not real-time auditing. If a
// repair fails mid-page, the cursor rolls back to the page start so the next
// round retries that page.
type DeleteReconcileState struct {
	DentryCursor string
	InodeCursor  string
}

// DeleteReconcileReport summarizes one reconciliation round.
type DeleteReconcileReport struct {
	// BrokenDentries counts scan-time broken-chain dentry candidates.
	BrokenDentries int64
	// StrandedInodes counts scan-time stranded file inode candidates
	// (split schema only; always 0 on legacy-files stores).
	StrandedInodes int64
	// Repaired counts rows actually repaired; always 0 in dry-run mode.
	Repaired int64
}

// DeleteReconcileRepairEnabled reports whether reconciliation runs in repair
// mode. Read per call so tests and operators can toggle it without a restart.
func DeleteReconcileRepairEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DRIVE9_DELETE_RECONCILE_REPAIR"))) {
	case "1", "true", "on":
		return true
	default:
		return false
	}
}

// ReconcileDeleteOrphans runs one reconciliation round over this tenant's
// store: a schema-agnostic broken-chain dentry scan, plus a stranded file
// inode scan on split-schema stores (skipped when HasLegacyFiles is true).
// In dry-run mode candidates are only counted and logged. state carries the
// scan cursors between rounds (nil = fresh); a round that completes a full
// pass resets its cursor so the next round starts over.
func (s *Store) ReconcileDeleteOrphans(ctx context.Context, dryRun bool, batchSize int, state *DeleteReconcileState) (report *DeleteReconcileReport, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "delete_reconcile", start, &err)

	if batchSize <= 0 {
		batchSize = DefaultDeleteReconcileBatchSize
	}
	if state == nil {
		state = &DeleteReconcileState{}
	}
	report = &DeleteReconcileReport{}
	if err := s.reconcileBrokenDentries(ctx, dryRun, batchSize, state, report); err != nil {
		return report, err
	}
	if !s.useLegacyFiles {
		if err := s.reconcileStrandedInodes(ctx, dryRun, batchSize, state, report); err != nil {
			return report, err
		}
	}
	logger.Info(ctx, "delete_reconcile_round",
		zap.Bool("dry_run", dryRun),
		zap.Int64("broken_dentries", report.BrokenDentries),
		zap.Int64("stranded_inodes", report.StrandedInodes),
		zap.Int64("repaired", report.Repaired))
	return report, nil
}

// --- broken-chain dentries ---

// reconcileDentryCandidate is a file_nodes row whose parent chain was broken
// at scan time.
type reconcileDentryCandidate struct {
	nodeID     string
	path       string
	parentPath string
	fileID     string
	isDir      bool
}

func (s *Store) reconcileBrokenDentries(ctx context.Context, dryRun bool, batchSize int, state *DeleteReconcileState, report *DeleteReconcileReport) error {
	scannedTotal := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageStart := state.DentryCursor
		rows, err := s.db.QueryContext(ctx, `SELECT node_id, path, parent_path, COALESCE(file_id, ''), is_directory
			FROM file_nodes WHERE `+s.scope.And(`node_id > ?`)+` ORDER BY node_id LIMIT ?`,
			s.scope.Args(state.DentryCursor, batchSize)...)
		if err != nil {
			return err
		}
		var page []reconcileDentryCandidate
		scanned := 0
		for rows.Next() {
			var c reconcileDentryCandidate
			var isDir int
			if err := rows.Scan(&c.nodeID, &c.path, &c.parentPath, &c.fileID, &isDir); err != nil {
				_ = rows.Close()
				return err
			}
			c.isDir = isDir != 0
			scanned++
			state.DentryCursor = c.nodeID
			// Top-level rows are always valid: the root is implicit and has
			// no file_nodes row, so a parent lookup would always miss.
			if c.parentPath == "/" {
				continue
			}
			page = append(page, c)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		scannedTotal += scanned

		// Classify the whole page (one batched snapshot check) before
		// repairing, so a repair cannot reclassify an unscanned sibling in
		// this round.
		parentPaths := make([]string, 0, len(page))
		for _, c := range page {
			parentPaths = append(parentPaths, c.parentPath)
		}
		existing, err := s.reconcileExistingParentDirs(ctx, parentPaths)
		if err != nil {
			return err
		}
		var broken []reconcileDentryCandidate
		for _, c := range page {
			if !existing[c.parentPath] {
				broken = append(broken, c)
			}
		}
		report.BrokenDentries += int64(len(broken))
		if !dryRun {
			for _, c := range broken {
				repaired, err := s.repairBrokenDentry(ctx, c)
				if err != nil {
					// Roll the cursor back to the page start so the next
					// round retries this page's remaining repairs instead
					// of skipping them until the next full pass.
					state.DentryCursor = pageStart
					return err
				}
				if repaired {
					report.Repaired++
				}
			}
		}
		if scanned < batchSize {
			// Full pass completed: restart from the beginning next round.
			state.DentryCursor = ""
			return nil
		}
		if scannedTotal >= deleteReconcileMaxScanRows {
			// Capped round: keep the cursor in state so the next round
			// resumes here instead of re-auditing the same prefix.
			logger.Info(ctx, "delete_reconcile_scan_capped",
				zap.Int("scanned_rows", scannedTotal))
			return nil
		}
	}
}

// reconcileExistingParentDirs returns the set of parent paths that exist as
// directory dentries. It queries path_hash IN (...) (an index-friendly form
// on both MySQL and TiDB) and filters the exact path match in memory, instead
// of a point check per candidate row.
func (s *Store) reconcileExistingParentDirs(ctx context.Context, parentPaths []string) (map[string]bool, error) {
	existing := make(map[string]bool, len(parentPaths))
	hashToPaths := make(map[string][]string, len(parentPaths))
	for _, p := range parentPaths {
		h := fileNodePathHash(p)
		if len(hashToPaths[h]) == 0 {
			hashToPaths[h] = []string{p}
			continue
		}
		dup := false
		for _, q := range hashToPaths[h] {
			if q == p {
				dup = true
				break
			}
		}
		if !dup {
			hashToPaths[h] = append(hashToPaths[h], p)
		}
	}
	hashes := make([]string, 0, len(hashToPaths))
	for h := range hashToPaths {
		hashes = append(hashes, h)
	}
	for start := 0; start < len(hashes); start += DefaultDeleteReconcileBatchSize {
		end := start + DefaultDeleteReconcileBatchSize
		if end > len(hashes) {
			end = len(hashes)
		}
		chunk := hashes[start:end]
		rows, err := s.db.QueryContext(ctx, `SELECT path_hash, path FROM file_nodes WHERE `+
			s.scope.And(`path_hash IN (`+questionPlaceholders(len(chunk))+`) AND is_directory = 1`),
			s.scope.Args(stringsToAny(chunk)...)...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var h, p string
			if err := rows.Scan(&h, &p); err != nil {
				_ = rows.Close()
				return nil, err
			}
			for _, q := range hashToPaths[h] {
				if q == p {
					existing[p] = true
				}
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

// reconcileParentDirExistsTx is the same check as a current read inside a
// repair transaction.
func (s *Store) reconcileParentDirExistsTx(ctx context.Context, tx *sql.Tx, parentPath string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM file_nodes WHERE `+
		s.scope.And(`path_hash = ? AND path = ? AND is_directory = 1`),
		s.scope.Args(fileNodePathHash(parentPath), parentPath)...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// repairBrokenDentry repairs one broken-chain candidate in its own small
// transaction: lock the row by node_id, re-run the parent check as a current
// read, and only then delete the dentry and run the orphan pipeline for its
// file_id. repaired=false means the row was already gone or its parent
// reappeared (the chain was mid-EnsureParentDirsTx when scanned).
func (s *Store) repairBrokenDentry(ctx context.Context, c reconcileDentryCandidate) (repaired bool, err error) {
	err = s.reconcileWithRetry(ctx, func(tx *sql.Tx) error {
		repaired = false
		var parentPath, fileID string
		var isDir int
		err := tx.QueryRowContext(ctx, `SELECT parent_path, COALESCE(file_id, ''), is_directory
			FROM file_nodes WHERE `+s.scope.And(`node_id = ?`)+` FOR UPDATE`,
			s.scope.Args(c.nodeID)...).Scan(&parentPath, &fileID, &isDir)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // already gone (concurrent delete): idempotent no-op
		}
		if err != nil {
			return err
		}
		if parentPath == "/" {
			return nil // top-level row: never broken
		}
		ok, err := s.reconcileParentDirExistsTx(ctx, tx, parentPath)
		if err != nil {
			return err
		}
		if ok {
			return nil // parent reappeared since the snapshot scan
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM file_nodes WHERE `+
			s.scope.And(`node_id = ?`), s.scope.Args(c.nodeID)...); err != nil {
			return err
		}
		repaired = true
		logger.Info(ctx, "delete_reconcile_dentry_repaired",
			zap.String("node_id", c.nodeID),
			zap.String("path", c.path),
			zap.String("parent_path", parentPath),
			zap.Bool("is_directory", isDir != 0))
		if isDir == 0 && fileID != "" {
			if _, err := s.processOrphansTx(ctx, tx, []string{fileID}); err != nil {
				return err
			}
		}
		return nil
	})
	return repaired, err
}

// --- stranded file inodes (split schema only) ---

func (s *Store) reconcileStrandedInodes(ctx context.Context, dryRun bool, batchSize int, state *DeleteReconcileState, report *DeleteReconcileReport) error {
	scannedTotal := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageStart := state.InodeCursor
		// Candidate set: CONFIRMED inodes joined to contents (a directory
		// inode has no contents row, so the join excludes them), with no
		// file_nodes reference via either column (file dentries reference
		// through file_id, directory dentries through inode_id) and no
		// file_gc_tasks row.
		args := s.scope.Args()                 // i
		args = append(args, s.scope.Args()...) // c
		args = append(args, state.InodeCursor) // i.inode_id > ?
		args = append(args, s.scope.Args()...) // fn
		args = append(args, s.scope.Args()...) // g
		args = append(args, batchSize)         // LIMIT
		rows, err := s.db.QueryContext(ctx, `SELECT i.inode_id FROM inodes i
			JOIN contents c ON c.inode_id = i.inode_id
			WHERE `+s.scope.AndAs("i", s.scope.AndAs("c", `i.status = 'CONFIRMED' AND i.inode_id > ?`))+`
			  AND NOT EXISTS (SELECT 1 FROM file_nodes fn WHERE `+
			s.scope.AndAs("fn", `(fn.file_id = i.inode_id OR fn.inode_id = i.inode_id)`)+`)
			  AND NOT EXISTS (SELECT 1 FROM file_gc_tasks g WHERE `+
			s.scope.AndAs("g", `(g.file_id = i.inode_id OR g.inode_id = i.inode_id)`)+`)
			ORDER BY i.inode_id LIMIT ?`, args...)
		if err != nil {
			return err
		}
		var page []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return err
			}
			page = append(page, id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}

		report.StrandedInodes += int64(len(page))
		if !dryRun {
			for _, id := range page {
				repaired, err := s.repairStrandedInode(ctx, id)
				if err != nil {
					// Roll the cursor back to the page start so the next
					// round retries this page's remaining repairs instead
					// of skipping them until the next full pass.
					state.InodeCursor = pageStart
					return err
				}
				if repaired {
					report.Repaired++
				}
			}
		}
		if len(page) < batchSize {
			// Full pass completed: restart from the beginning next round.
			state.InodeCursor = ""
			return nil
		}
		scannedTotal += len(page)
		state.InodeCursor = page[len(page)-1]
		if scannedTotal >= deleteReconcileMaxScanRows {
			// Capped round: keep the cursor in state so the next round
			// resumes here instead of re-auditing the same prefix.
			logger.Info(ctx, "delete_reconcile_scan_capped",
				zap.Int("scanned_rows", scannedTotal))
			return nil
		}
	}
}

// repairStrandedInode repairs one stranded file inode in its own small
// transaction: lock the inode, re-run the reference checks as current reads,
// then mark it DELETED, drop its tags, and enqueue the GC task.
func (s *Store) repairStrandedInode(ctx context.Context, inodeID string) (repaired bool, err error) {
	err = s.reconcileWithRetry(ctx, func(tx *sql.Tx) error {
		repaired = false
		var status string
		err := tx.QueryRowContext(ctx, `SELECT status FROM inodes WHERE `+
			s.scope.And(`inode_id = ?`)+` FOR UPDATE`,
			s.scope.Args(inodeID)...).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if FileStatus(status) != StatusConfirmed {
			return nil
		}
		// Fetch the contents row (also the directory-inode exclusion: a
		// directory inode has no contents row) to build the GC task.
		var sizeBytes int64
		var storageType, storageRef, contentType sql.NullString
		err = tx.QueryRowContext(ctx, `SELECT i.size_bytes, c.storage_type, c.storage_ref, c.content_type
			FROM inodes i JOIN contents c ON c.inode_id = i.inode_id
			WHERE `+s.scope.AndAs("i", s.scope.AndAs("c", `i.inode_id = ?`)),
			scopeWhereArgs(s.scope, 2, inodeID)...).Scan(
			&sizeBytes, &storageType, &storageRef, &contentType)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // no contents row: directory inode — Non-goal, leave it
		}
		if err != nil {
			return err
		}
		f := &File{
			FileID:      inodeID,
			StorageType: StorageType(storageType.String),
			StorageRef:  storageRef.String,
			ContentType: contentType.String,
			SizeBytes:   sizeBytes,
			Status:      StatusDeleted,
		}
		// Current-read re-checks under the inode lock.
		referenced, err := s.reconcileInodeReferencedTx(ctx, tx, inodeID)
		if err != nil {
			return err
		}
		if referenced {
			return nil
		}
		hasTask, err := s.reconcileInodeHasGCTaskTx(ctx, tx, inodeID)
		if err != nil {
			return err
		}
		if hasTask {
			return nil // the GC worker already owns it
		}
		if _, err := tx.ExecContext(ctx, `UPDATE inodes SET status = 'DELETED' WHERE `+
			s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
			return err
		}
		if err := s.deleteFileTagsByIDsTx(ctx, tx, []string{inodeID}); err != nil {
			return err
		}
		task, err := NewFileGCTaskFromFile(f, time.Now().UTC())
		if err != nil {
			return err
		}
		if _, err := s.enqueueFileGCTasksTx(tx, []*FileGCTask{task}); err != nil {
			return err
		}
		repaired = true
		logger.Info(ctx, "delete_reconcile_inode_repaired", zap.String("inode_id", inodeID))
		return nil
	})
	return repaired, err
}

// reconcileInodeReferencedTx reports whether any file_nodes row references
// the inode via either column (current read).
func (s *Store) reconcileInodeReferencedTx(ctx context.Context, tx *sql.Tx, inodeID string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM file_nodes WHERE `+
		s.scope.And(`(file_id = ? OR inode_id = ?)`)+` LIMIT 1`,
		s.scope.Args(inodeID, inodeID)...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// reconcileInodeHasGCTaskTx reports whether a file_gc_tasks row already
// exists for the inode (current read).
func (s *Store) reconcileInodeHasGCTaskTx(ctx context.Context, tx *sql.Tx, inodeID string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM file_gc_tasks WHERE `+
		s.scope.And(`(file_id = ? OR inode_id = ?)`)+` LIMIT 1`,
		s.scope.Args(inodeID, inodeID)...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// reconcileWithRetry runs fn in a transaction, retrying deadlock/lock-wait
// failures with backoff, mirroring the sweep's withBatchRetry.
func (s *Store) reconcileWithRetry(ctx context.Context, fn func(tx *sql.Tx) error) error {
	for attempt := 0; ; attempt++ {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		fnErr := fn(tx)
		if fnErr != nil {
			_ = tx.Rollback()
			if isRetryableTxnError(fnErr) && attempt < deleteDirBatchRetries {
				if !sleepWithContext(ctx, time.Duration(50*(1<<attempt))*time.Millisecond) {
					return ctx.Err()
				}
				continue
			}
			return fnErr
		}
		if err := tx.Commit(); err != nil {
			if isRetryableTxnError(err) && attempt < deleteDirBatchRetries {
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
