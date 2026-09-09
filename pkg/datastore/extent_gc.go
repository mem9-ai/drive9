package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const defaultBlockGCMaxAttempts = 8

// BlockGCTask is a row in block_gc_tasks.
type BlockGCTask struct {
	TaskID       string
	BlockKey     string
	InodeID      string
	SizeBytes    int64
	Status       string
	AttemptCount int
	MaxAttempts  int
	Receipt      string
	LeasedAt     *time.Time
	LeaseUntil   *time.Time
	AvailableAt  time.Time
	LastError    string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  *time.Time
}

// SliceCompactTask is a row in slice_compact_tasks.
type SliceCompactTask struct {
	TaskID       string
	InodeID      string
	Chunk        int64
	Status       string
	AttemptCount int
	MaxAttempts  int
	Receipt      string
	LeasedAt     *time.Time
	LeaseUntil   *time.Time
	AvailableAt  time.Time
	LastError    string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  *time.Time
}

func (s *Store) enqueueBlockGCTx(ctx context.Context, tx *sql.Tx, blockKey, inodeID string, sizeBytes int64, availableAt time.Time) error {
	now := time.Now().UTC()
	if availableAt.IsZero() {
		availableAt = now.Add(ExtentBlockGCGrace)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO block_gc_tasks
		(`+s.scope.InsCols(`task_id, block_key, inode_id, size_bytes, status, attempt_count, max_attempts, available_at, created_at, updated_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, 0, ?, ?, ?, ?`)+`)`,
		s.scope.Args(blockKey, blockKey, nullStr(inodeID), sizeBytes, BlockGCTaskQueued, defaultBlockGCMaxAttempts, availableAt.UTC(), now, now)...)
	if err != nil && !isUniqueViolation(err) {
		return fmt.Errorf("enqueue block gc: %w", err)
	}
	return nil
}

func (s *Store) ClaimBlockGCTask(ctx context.Context, now time.Time, lease time.Duration) (*BlockGCTask, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `SELECT `+s.scope.SelCols(`task_id, block_key, inode_id, size_bytes, status, attempt_count, max_attempts, receipt, leased_at, lease_until, available_at, last_error, created_at, updated_at, completed_at`)+`
		FROM block_gc_tasks
		WHERE `+s.scope.And(`status = ? AND available_at <= ?`)+`
		ORDER BY available_at, created_at
		LIMIT 1 FOR UPDATE SKIP LOCKED`,
		s.scope.Args(BlockGCTaskQueued, now.UTC())...)
	task, err := scanBlockGCTask(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	leaseUntil := now.Add(lease)
	receipt := task.TaskID + ":" + now.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `UPDATE block_gc_tasks SET status = ?, receipt = ?, leased_at = ?, lease_until = ?, updated_at = ?
		WHERE `+s.scope.And(`task_id = ?`),
		append([]any{BlockGCTaskLeased, receipt, now.UTC(), leaseUntil.UTC(), now.UTC()}, s.scope.Args(task.TaskID)...)...); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	task.Status = BlockGCTaskLeased
	task.Receipt = receipt
	task.LeasedAt = &now
	task.LeaseUntil = &leaseUntil
	return task, true, nil
}

func (s *Store) AckBlockGCTask(ctx context.Context, taskID, receipt string) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE block_gc_tasks SET status = ?, receipt = NULL, completed_at = ?, updated_at = ?
		WHERE `+s.scope.And(`task_id = ? AND receipt = ?`),
		append([]any{BlockGCTaskCompleted, now, now}, s.scope.Args(taskID, receipt)...)...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileGCTaskLeaseMismatch
	}
	return nil
}

func (s *Store) RetryBlockGCTask(ctx context.Context, taskID, receipt string, retryAt time.Time, lastError string) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE block_gc_tasks SET status = ?, receipt = NULL, leased_at = NULL, lease_until = NULL,
		available_at = ?, last_error = ?, attempt_count = attempt_count + 1, updated_at = ?
		WHERE `+s.scope.And(`task_id = ? AND receipt = ?`),
		append([]any{BlockGCTaskQueued, retryAt.UTC(), lastError, now}, s.scope.Args(taskID, receipt)...)...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileGCTaskLeaseMismatch
	}
	return nil
}

func (s *Store) RecoverExpiredBlockGCTasks(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := s.db.ExecContext(ctx, `UPDATE block_gc_tasks SET status = ?, receipt = NULL, leased_at = NULL, lease_until = NULL, updated_at = ?
		WHERE `+s.scope.And(`status = ? AND lease_until IS NOT NULL AND lease_until < ?`)+` LIMIT ?`,
		append([]any{BlockGCTaskQueued, now.UTC()}, s.scope.Args(BlockGCTaskLeased, now.UTC(), limit)...)...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) ListExpiredPendingBlocks(ctx context.Context, cutoff time.Time, limit int) ([]PendingBlock, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT block_key, inode_id, size_bytes, checksum_sha256, reserved_bytes, source, created_at
		FROM pending_blocks WHERE `+s.scope.And(`created_at <= ?`)+` ORDER BY created_at LIMIT ?`,
		append(s.scope.Args(cutoff.UTC()), limit)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []PendingBlock
	for rows.Next() {
		var b PendingBlock
		if err := rows.Scan(&b.BlockKey, &b.InodeID, &b.SizeBytes, &b.ChecksumSHA256, &b.ReservedBytes, &b.Source, &b.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) DeletePendingBlock(ctx context.Context, blockKey string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pending_blocks WHERE `+s.scope.And(`block_key = ?`), s.scope.Args(blockKey)...)
	return err
}

func (s *Store) ClaimSliceCompactTask(ctx context.Context, now time.Time, lease time.Duration) (*SliceCompactTask, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `SELECT `+s.scope.SelCols(`task_id, inode_id, chunk, status, attempt_count, max_attempts, receipt, leased_at, lease_until, available_at, last_error, created_at, updated_at, completed_at`)+`
		FROM slice_compact_tasks
		WHERE `+s.scope.And(`status = ? AND available_at <= ? AND attempt_count < max_attempts`)+`
		ORDER BY available_at, created_at
		LIMIT 1 FOR UPDATE SKIP LOCKED`,
		s.scope.Args(SliceCompactQueued, now.UTC())...)
	task, err := scanSliceCompactTask(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	leaseUntil := now.Add(lease)
	receipt := task.TaskID + ":" + now.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `UPDATE slice_compact_tasks SET status = ?, receipt = ?, leased_at = ?, lease_until = ?, updated_at = ?
		WHERE `+s.scope.And(`task_id = ?`),
		append([]any{SliceCompactLeased, receipt, now.UTC(), leaseUntil.UTC(), now.UTC()}, s.scope.Args(task.TaskID)...)...); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	task.Status = SliceCompactLeased
	task.Receipt = receipt
	task.LeasedAt = &now
	task.LeaseUntil = &leaseUntil
	return task, true, nil
}

func (s *Store) DeleteSliceCompactTask(ctx context.Context, taskID, receipt string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM slice_compact_tasks WHERE `+s.scope.And(`task_id = ? AND receipt = ?`),
		s.scope.Args(taskID, receipt)...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileGCTaskLeaseMismatch
	}
	return nil
}

func (s *Store) RecoverExpiredSliceCompactTasks(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := s.db.ExecContext(ctx, `UPDATE slice_compact_tasks SET status = ?, receipt = NULL, leased_at = NULL, lease_until = NULL, updated_at = ?
		WHERE `+s.scope.And(`status = ? AND lease_until IS NOT NULL AND lease_until < ?`)+` LIMIT ?`,
		append([]any{SliceCompactQueued, now.UTC()}, s.scope.Args(SliceCompactLeased, now.UTC(), limit)...)...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) DelaySliceCompactTask(ctx context.Context, taskID, receipt string, retryAt time.Time, lastError string) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE slice_compact_tasks SET status = ?, receipt = NULL, leased_at = NULL, lease_until = NULL,
		available_at = ?, last_error = ?, updated_at = ?
		WHERE `+s.scope.And(`task_id = ? AND receipt = ?`),
		append([]any{SliceCompactQueued, retryAt.UTC(), lastError, now}, s.scope.Args(taskID, receipt)...)...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileGCTaskLeaseMismatch
	}
	return nil
}

func (s *Store) RetrySliceCompactTask(ctx context.Context, taskID, receipt string, retryAt time.Time, lastError string) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE slice_compact_tasks SET status = ?, receipt = NULL, leased_at = NULL, lease_until = NULL,
		available_at = ?, last_error = ?, attempt_count = attempt_count + 1, updated_at = ?
		WHERE `+s.scope.And(`task_id = ? AND receipt = ?`),
		append([]any{SliceCompactQueued, retryAt.UTC(), lastError, now}, s.scope.Args(taskID, receipt)...)...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileGCTaskLeaseMismatch
	}
	return nil
}

func (s *Store) DeleteOldSliceCommitOps(ctx context.Context, inodeID string, keep int, olderThan time.Time) error {
	if keep < 1 {
		keep = ExtentCommitOpsRetain
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM slice_commit_ops
		WHERE `+s.scope.And(`inode_id = ? AND created_at < ? AND op_id NOT IN (
			SELECT op_id FROM (
				SELECT op_id FROM slice_commit_ops WHERE inode_id = ? ORDER BY created_at DESC LIMIT ?
			) keep_rows
		)`), s.scope.Args(inodeID, olderThan.UTC(), inodeID, keep)...)
	return err
}

func scanBlockGCTask(row *sql.Row) (*BlockGCTask, error) {
	var t BlockGCTask
	var inodeID, receipt, lastError sql.NullString
	var leasedAt, leaseUntil, completedAt sql.NullTime
	err := row.Scan(&t.TaskID, &t.BlockKey, &inodeID, &t.SizeBytes, &t.Status, &t.AttemptCount, &t.MaxAttempts,
		&receipt, &leasedAt, &leaseUntil, &t.AvailableAt, &lastError, &t.CreatedAt, &t.UpdatedAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.InodeID = inodeID.String
	t.Receipt = receipt.String
	t.LastError = lastError.String
	if leasedAt.Valid {
		t.LeasedAt = &leasedAt.Time
	}
	if leaseUntil.Valid {
		t.LeaseUntil = &leaseUntil.Time
	}
	if completedAt.Valid {
		t.CompletedAt = &completedAt.Time
	}
	return &t, nil
}

func scanSliceCompactTask(row *sql.Row) (*SliceCompactTask, error) {
	var t SliceCompactTask
	var receipt, lastError sql.NullString
	var leasedAt, leaseUntil, completedAt sql.NullTime
	err := row.Scan(&t.TaskID, &t.InodeID, &t.Chunk, &t.Status, &t.AttemptCount, &t.MaxAttempts,
		&receipt, &leasedAt, &leaseUntil, &t.AvailableAt, &lastError, &t.CreatedAt, &t.UpdatedAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.Receipt = receipt.String
	t.LastError = lastError.String
	if leasedAt.Valid {
		t.LeasedAt = &leasedAt.Time
	}
	if leaseUntil.Valid {
		t.LeaseUntil = &leaseUntil.Time
	}
	if completedAt.Valid {
		t.CompletedAt = &completedAt.Time
	}
	return &t, nil
}
