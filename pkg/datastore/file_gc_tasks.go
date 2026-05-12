package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// File GC is a correctness cleanup path, not best-effort enrichment work.
// A zero max_attempts means retry indefinitely until the blob/quota cleanup is
// done or an operator explicitly updates the task policy.
const defaultFileGCMaxAttempts = 0

type FileGCTaskStatus string

const (
	FileGCTaskQueued       FileGCTaskStatus = "queued"
	FileGCTaskProcessing   FileGCTaskStatus = "processing"
	FileGCTaskSucceeded    FileGCTaskStatus = "succeeded"
	FileGCTaskDeadLettered FileGCTaskStatus = "dead_lettered"
)

// FileGCTask records durable cleanup for an orphaned file identity. The task is
// intentionally keyed by file_id/storage_ref rather than path so retries cannot
// delete another actor's later recreate at the same path. It does not track old
// blob refs produced by overwriting a still-live file_id; those need a separate
// blob-ref GC path.
type FileGCTask struct {
	TaskID       string
	FileID       string
	StorageType  StorageType
	StorageRef   string
	SizeBytes    int64
	ContentType  string
	Status       FileGCTaskStatus
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

// EnqueueFileGCTaskTx inserts a queued file cleanup task inside an existing
// metadata transaction. Duplicate tasks for the same file_id are treated as a
// successful no-op because file IDs are never reused.
func (s *Store) EnqueueFileGCTaskTx(db execer, task *FileGCTask) (bool, error) {
	return s.enqueueFileGCTask(db, task)
}

// GetFileGCTaskByFileID returns the durable cleanup task for a file.
func (s *Store) GetFileGCTaskByFileID(ctx context.Context, fileID string) (out *FileGCTask, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "get_file_gc_task_by_file_id", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT task_id, file_id, storage_type, storage_ref,
		size_bytes, content_type, status, attempt_count, max_attempts, receipt,
		leased_at, lease_until, available_at, last_error, created_at, updated_at, completed_at
		FROM file_gc_tasks WHERE file_id = ?`, fileID)
	return scanFileGCTask(row)
}

func (s *Store) ListFileGCTaskS3Refs(ctx context.Context, cursor string, limit int) ([]ConfirmedS3Ref, string, error) {
	if limit <= 0 {
		limit = 500
	}
	var rows *sql.Rows
	var err error
	if cursor == "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT DISTINCT storage_ref
			 FROM file_gc_tasks
			 WHERE storage_type = 's3' AND storage_ref <> ''
			 ORDER BY storage_ref ASC LIMIT ?`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT DISTINCT storage_ref
			 FROM file_gc_tasks
			 WHERE storage_type = 's3' AND storage_ref <> '' AND storage_ref > ?
			 ORDER BY storage_ref ASC LIMIT ?`, cursor, limit)
	}
	if err != nil {
		return nil, "", fmt.Errorf("query file gc s3 refs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ConfirmedS3Ref, 0)
	for rows.Next() {
		var ref ConfirmedS3Ref
		if err := rows.Scan(&ref.StorageRef); err != nil {
			return nil, "", fmt.Errorf("scan file gc s3 ref: %w", err)
		}
		ref.StorageRefHash = StorageRefHash(ref.StorageRef)
		out = append(out, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(out) == limit {
		nextCursor = out[len(out)-1].StorageRef
	}
	return out, nextCursor, nil
}

// ClaimFileGCTask claims one queued file GC task and leases it to the caller.
func (s *Store) ClaimFileGCTask(ctx context.Context, now time.Time, leaseDuration time.Duration) (out *FileGCTask, found bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "claim_file_gc_task", start, &err)

	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `SELECT task_id, file_id, storage_type, storage_ref,
		size_bytes, content_type, status, attempt_count, max_attempts, receipt,
		leased_at, lease_until, available_at, last_error, created_at, updated_at, completed_at
		FROM file_gc_tasks
		WHERE status = ? AND available_at <= ?
		ORDER BY available_at, created_at, task_id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`, FileGCTaskQueued, now)
	task, err := scanFileGCTask(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	receipt := uuid.NewString()
	leasedAt := now
	leaseUntil := now.Add(leaseDuration)
	res, err := tx.ExecContext(ctx, `UPDATE file_gc_tasks SET status = ?,
		attempt_count = attempt_count + 1, receipt = ?, leased_at = ?,
		lease_until = ?, updated_at = ?
		WHERE task_id = ? AND status = ? AND available_at <= ?`,
		FileGCTaskProcessing, receipt, leasedAt, leaseUntil, now,
		task.TaskID, FileGCTaskQueued, now)
	if err != nil {
		return nil, false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if rowsAffected == 0 {
		return nil, false, nil
	}

	task.Status = FileGCTaskProcessing
	task.AttemptCount++
	task.Receipt = receipt
	task.LeasedAt = &leasedAt
	task.LeaseUntil = &leaseUntil
	task.UpdatedAt = now

	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return task, true, nil
}

// AckFileGCTask marks a leased file GC task as succeeded.
func (s *Store) AckFileGCTask(ctx context.Context, taskID, receipt string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ack_file_gc_task", start, &err)

	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE file_gc_tasks SET status = ?, receipt = NULL,
		leased_at = NULL, lease_until = NULL, completed_at = ?, updated_at = ?
		WHERE task_id = ? AND status = ? AND receipt = ? AND lease_until IS NOT NULL AND lease_until > ?`,
		FileGCTaskSucceeded, now, now, taskID, FileGCTaskProcessing, receipt, now)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected > 0 {
		return nil
	}
	return s.fileGCTaskLeaseError(ctx, taskID)
}

// RetryFileGCTask requeues or dead-letters a leased file GC task.
func (s *Store) RetryFileGCTask(ctx context.Context, taskID, receipt string, retryAt time.Time, lastErr string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "retry_file_gc_task", start, &err)

	now := time.Now().UTC()
	if retryAt.IsZero() {
		retryAt = now
	} else {
		retryAt = retryAt.UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `SELECT task_id, file_id, storage_type, storage_ref,
		size_bytes, content_type, status, attempt_count, max_attempts, receipt,
		leased_at, lease_until, available_at, last_error, created_at, updated_at, completed_at
		FROM file_gc_tasks WHERE task_id = ? FOR UPDATE`, taskID)
	task, err := scanFileGCTask(row)
	if err != nil {
		return err
	}
	if task.Status != FileGCTaskProcessing || task.Receipt != receipt || task.LeaseUntil == nil || !task.LeaseUntil.After(now) {
		return ErrFileGCTaskLeaseMismatch
	}

	if task.MaxAttempts > 0 && task.AttemptCount >= task.MaxAttempts {
		_, err = tx.ExecContext(ctx, `UPDATE file_gc_tasks SET status = ?, receipt = NULL,
			leased_at = NULL, lease_until = NULL, last_error = ?, completed_at = ?, updated_at = ?
			WHERE task_id = ?`, FileGCTaskDeadLettered, nullStr(lastErr), now, now, taskID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE file_gc_tasks SET status = ?, receipt = NULL,
			leased_at = NULL, lease_until = NULL, available_at = ?, last_error = ?,
			completed_at = NULL, updated_at = ?
			WHERE task_id = ?`, FileGCTaskQueued, retryAt, nullStr(lastErr), now, taskID)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// RecoverExpiredFileGCTasks requeues processing tasks whose lease expired.
func (s *Store) RecoverExpiredFileGCTasks(ctx context.Context, now time.Time, limit int) (recovered int, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "recover_expired_file_gc_tasks", start, &err)

	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	query := `SELECT task_id FROM file_gc_tasks
		WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?
		ORDER BY lease_until, created_at, task_id`
	args := []any{FileGCTaskProcessing, now}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	query += ` FOR UPDATE SKIP LOCKED`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var taskIDs []string
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			return 0, err
		}
		taskIDs = append(taskIDs, taskID)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, taskID := range taskIDs {
		res, err := tx.ExecContext(ctx, `UPDATE file_gc_tasks SET status = ?, receipt = NULL,
			leased_at = NULL, lease_until = NULL, available_at = ?, updated_at = ?
			WHERE task_id = ? AND status = ? AND lease_until IS NOT NULL AND lease_until < ?`,
			FileGCTaskQueued, now, now, taskID, FileGCTaskProcessing, now)
		if err != nil {
			return 0, err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		recovered += int(rowsAffected)
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return recovered, nil
}

func NewFileGCTaskFromFile(f *File, now time.Time) (*FileGCTask, error) {
	if f == nil {
		return nil, fmt.Errorf("file is required")
	}
	if strings.TrimSpace(f.FileID) == "" {
		return nil, fmt.Errorf("file_id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	return &FileGCTask{
		TaskID:      f.FileID,
		FileID:      f.FileID,
		StorageType: f.StorageType,
		StorageRef:  f.StorageRef,
		SizeBytes:   f.SizeBytes,
		ContentType: f.ContentType,
		Status:      FileGCTaskQueued,
		MaxAttempts: defaultFileGCMaxAttempts,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func (s *Store) enqueueFileGCTask(db execer, task *FileGCTask) (bool, error) {
	if task == nil {
		return false, fmt.Errorf("file gc task is required")
	}
	if strings.TrimSpace(task.TaskID) == "" {
		task.TaskID = task.FileID
	}
	if strings.TrimSpace(task.FileID) == "" {
		return false, fmt.Errorf("file_id is required")
	}
	now := time.Now().UTC()
	status := task.Status
	if status == "" {
		status = FileGCTaskQueued
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultFileGCMaxAttempts
	}
	availableAt := task.AvailableAt
	if availableAt.IsZero() {
		availableAt = now
	}
	createdAt := task.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := task.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	_, err := db.Exec(`INSERT INTO file_gc_tasks
		(task_id, file_id, storage_type, storage_ref, size_bytes, content_type,
		 status, attempt_count, max_attempts, receipt, leased_at, lease_until,
		 available_at, last_error, created_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.TaskID, task.FileID, task.StorageType, task.StorageRef, task.SizeBytes,
		nullStr(task.ContentType), status, task.AttemptCount, maxAttempts,
		nullStr(task.Receipt), nilTime(task.LeasedAt), nilTime(task.LeaseUntil),
		availableAt.UTC(), nullStr(task.LastError), createdAt.UTC(),
		updatedAt.UTC(), nilTime(task.CompletedAt))
	if err == nil {
		return true, nil
	}
	if isUniqueViolation(err) {
		return false, nil
	}
	return false, err
}

func (s *Store) fileGCTaskLeaseError(ctx context.Context, taskID string) error {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM file_gc_tasks WHERE task_id = ?`, taskID).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	return ErrFileGCTaskLeaseMismatch
}

func scanFileGCTask(s scanner) (*FileGCTask, error) {
	var task FileGCTask
	var contentType, receipt, lastError sql.NullString
	var leasedAt, leaseUntil, completedAt sql.NullTime
	err := s.Scan(&task.TaskID, &task.FileID, &task.StorageType, &task.StorageRef,
		&task.SizeBytes, &contentType, &task.Status, &task.AttemptCount,
		&task.MaxAttempts, &receipt, &leasedAt, &leaseUntil, &task.AvailableAt,
		&lastError, &task.CreatedAt, &task.UpdatedAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	task.ContentType = contentType.String
	task.Receipt = receipt.String
	task.LastError = lastError.String
	task.AvailableAt = task.AvailableAt.UTC()
	task.CreatedAt = task.CreatedAt.UTC()
	task.UpdatedAt = task.UpdatedAt.UTC()
	if leasedAt.Valid {
		t := leasedAt.Time.UTC()
		task.LeasedAt = &t
	}
	if leaseUntil.Valid {
		t := leaseUntil.Time.UTC()
		task.LeaseUntil = &t
	}
	if completedAt.Valid {
		t := completedAt.Time.UTC()
		task.CompletedAt = &t
	}
	return &task, nil
}
