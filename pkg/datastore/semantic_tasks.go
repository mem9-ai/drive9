package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

const defaultSemanticMaxAttempts = 5

// SemanticTaskObservation summarizes queue state for one tenant-local
// semantic_tasks table at a point in time. It is read-only state used for
// observability and must not be treated as a source of delivery truth.
type SemanticTaskObservation struct {
	Queued                     int
	Processing                 int
	DeadLettered               int
	OldestClaimableAvailableAt *time.Time
}

// EnqueueSemanticTask inserts a queued semantic task unless the same
// task_type/resource_id/resource_version tuple already exists.
func (s *Store) EnqueueSemanticTask(ctx context.Context, task *semantic.Task) (created bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "enqueue_semantic_task", start, &err)
	return s.enqueueSemanticTask(s.db, task)
}

// EnsureSemanticTaskQueued makes sure the semantic task exists and is queued.
// Existing terminal tasks for the same resource/version are re-queued in place.
func (s *Store) EnsureSemanticTaskQueued(ctx context.Context, task *semantic.Task) (queued bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ensure_semantic_task_queued", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	queued, err = s.ensureSemanticTaskQueuedTx(tx, task)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return queued, nil
}

// EnqueueSemanticTaskTx inserts a queued semantic task inside an existing transaction.
func (s *Store) EnqueueSemanticTaskTx(db execer, task *semantic.Task) (bool, error) {
	return s.enqueueSemanticTask(db, task)
}

// EnsureSemanticTaskQueuedTx makes sure the semantic task exists and is queued
// inside an existing transaction.
func (s *Store) EnsureSemanticTaskQueuedTx(tx *sql.Tx, task *semantic.Task) (bool, error) {
	return s.ensureSemanticTaskQueuedTx(tx, task)
}

// ObserveSemanticTasks returns a best-effort read-only queue snapshot for the
// current tenant store without mutating any semantic task state.
func (s *Store) ObserveSemanticTasks(ctx context.Context, now time.Time) (out *SemanticTaskObservation, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "observe_semantic_tasks", start, &err)

	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	obs := &SemanticTaskObservation{}

	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM semantic_tasks
		WHERE status IN (?, ?, ?)
		GROUP BY status`,
		semantic.TaskQueued, semantic.TaskProcessing, semantic.TaskDeadLettered)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			status string
			count  int
		)
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		switch semantic.TaskStatus(status) {
		case semantic.TaskQueued:
			obs.Queued = count
		case semantic.TaskProcessing:
			obs.Processing = count
		case semantic.TaskDeadLettered:
			obs.DeadLettered = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var availableAt sql.NullTime
	err = s.db.QueryRowContext(ctx, `SELECT available_at FROM semantic_tasks
		WHERE status = ? AND available_at <= ?
		ORDER BY available_at, created_at, task_id
		LIMIT 1`,
		semantic.TaskQueued, now).Scan(&availableAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if availableAt.Valid {
		t := availableAt.Time.UTC()
		obs.OldestClaimableAvailableAt = &t
	}
	return obs, nil
}

func (s *Store) enqueueSemanticTask(db execer, task *semantic.Task) (bool, error) {
	if task == nil {
		return false, fmt.Errorf("semantic task is required")
	}
	now := time.Now().UTC()
	status := task.Status
	if status == "" {
		status = semantic.TaskQueued
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultSemanticMaxAttempts
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

	_, err := db.Exec(`INSERT INTO semantic_tasks
		(task_id, task_type, resource_id, resource_version, status, attempt_count, max_attempts,
		 receipt, leased_at, lease_until, available_at, payload_json, last_error,
		 created_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.TaskID, task.TaskType, task.ResourceID, task.ResourceVersion, status, task.AttemptCount,
		maxAttempts, nullStr(task.Receipt), nilTime(task.LeasedAt), nilTime(task.LeaseUntil),
		availableAt.UTC(), nilBytes(task.PayloadJSON), nullStr(task.LastError),
		createdAt.UTC(), updatedAt.UTC(), nilTime(task.CompletedAt))
	if err == nil {
		return true, nil
	}
	if !isUniqueViolation(err) {
		return false, err
	}
	duplicate, dupErr := semanticTaskExistsByResource(db, task.TaskType, task.ResourceID, task.ResourceVersion)
	if dupErr != nil {
		return false, dupErr
	}
	if duplicate {
		return false, nil
	}
	return false, err
}

func (s *Store) ensureSemanticTaskQueuedTx(tx *sql.Tx, task *semantic.Task) (bool, error) {
	if task == nil {
		return false, fmt.Errorf("semantic task is required")
	}
	created, err := s.enqueueSemanticTask(tx, task)
	if err != nil {
		return false, err
	}
	if created {
		return true, nil
	}

	now := time.Now().UTC()
	row := tx.QueryRow(`SELECT task_id, task_type, resource_id, resource_version, status,
		attempt_count, max_attempts, receipt, leased_at, lease_until, available_at,
		payload_json, last_error, created_at, updated_at, completed_at
		FROM semantic_tasks
		WHERE task_type = ? AND resource_id = ? AND resource_version = ?
		FOR UPDATE`, task.TaskType, task.ResourceID, task.ResourceVersion)
	existing, err := scanSemanticTask(row)
	if err != nil {
		return false, err
	}
	if existing.Status == semantic.TaskProcessing && existing.LeaseUntil != nil && existing.LeaseUntil.After(now) {
		return false, nil
	}

	availableAt := task.AvailableAt
	if availableAt.IsZero() {
		availableAt = now
	}
	maxAttempts := existing.MaxAttempts
	if task.MaxAttempts > 0 {
		maxAttempts = task.MaxAttempts
	}
	payload := existing.PayloadJSON
	if len(task.PayloadJSON) > 0 {
		payload = task.PayloadJSON
	}
	_, err = tx.Exec(`UPDATE semantic_tasks SET status = ?, receipt = NULL, leased_at = NULL,
		lease_until = NULL, available_at = ?, payload_json = ?, last_error = NULL,
		max_attempts = ?, completed_at = NULL, updated_at = ?
		WHERE task_id = ?`,
		semantic.TaskQueued, availableAt.UTC(), nilBytes(payload), maxAttempts, now, existing.TaskID)
	if err != nil {
		return false, err
	}
	return true, nil
}

// ClaimSemanticTask claims one queued semantic task and leases it to the
// caller. When taskTypes is empty, any queued task type is eligible.
// Otherwise, only tasks whose task_type is included in taskTypes are eligible.
func (s *Store) ClaimSemanticTask(ctx context.Context, now time.Time, leaseDuration time.Duration, taskTypes ...semantic.TaskType) (out *semantic.Task, found bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "claim_semantic_task", start, &err)
	return s.claimSemanticTask(ctx, now, leaseDuration, taskTypes)
}

func (s *Store) claimSemanticTask(ctx context.Context, now time.Time, leaseDuration time.Duration, taskTypes []semantic.TaskType) (out *semantic.Task, found bool, err error) {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	normalizedTypes, err := normalizeClaimTaskTypes(taskTypes)
	if err != nil {
		return nil, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	query, args := claimSemanticTaskQuery(now, normalizedTypes)
	row := tx.QueryRowContext(ctx, query, args...)
	task, scanErr := scanSemanticTask(row)
	if scanErr != nil {
		if errors.Is(scanErr, semantic.ErrTaskNotFound) {
			return nil, false, nil
		}
		return nil, false, scanErr
	}

	receipt := uuid.NewString()
	leasedAt := now
	leaseUntil := now.Add(leaseDuration)
	res, err := tx.ExecContext(ctx, `UPDATE semantic_tasks SET status = ?, attempt_count = attempt_count + 1,
		receipt = ?, leased_at = ?, lease_until = ?, updated_at = ?
		WHERE task_id = ? AND status = ? AND available_at <= ?`,
		semantic.TaskProcessing, receipt, leasedAt, leaseUntil, now,
		task.TaskID, semantic.TaskQueued, now)
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

	task.Status = semantic.TaskProcessing
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

func normalizeClaimTaskTypes(taskTypes []semantic.TaskType) ([]semantic.TaskType, error) {
	if len(taskTypes) == 0 {
		return nil, nil
	}
	seen := make(map[semantic.TaskType]struct{}, len(taskTypes))
	normalized := make([]semantic.TaskType, 0, len(taskTypes))
	for _, taskType := range taskTypes {
		if strings.TrimSpace(string(taskType)) == "" {
			return nil, fmt.Errorf("claim task type is required")
		}
		if _, ok := seen[taskType]; ok {
			continue
		}
		seen[taskType] = struct{}{}
		normalized = append(normalized, taskType)
	}
	return normalized, nil
}

func claimSemanticTaskQuery(now time.Time, taskTypes []semantic.TaskType) (string, []any) {
	query := `SELECT task_id, task_type, resource_id, resource_version, status,
		attempt_count, max_attempts, receipt, leased_at, lease_until, available_at,
		payload_json, last_error, created_at, updated_at, completed_at
		FROM semantic_tasks
		WHERE status = ?`
	args := []any{semantic.TaskQueued}
	if len(taskTypes) == 1 {
		query += ` AND task_type = ?`
		args = append(args, taskTypes[0])
	} else if len(taskTypes) > 1 {
		query += ` AND task_type IN (` + questionPlaceholders(len(taskTypes)) + `)`
		for _, taskType := range taskTypes {
			args = append(args, taskType)
		}
	}
	query += ` AND available_at <= ?
		ORDER BY available_at, created_at, task_id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`
	args = append(args, now)
	return query, args
}

func questionPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

// AckSemanticTask marks a leased semantic task as succeeded.
func (s *Store) AckSemanticTask(ctx context.Context, taskID, receipt string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ack_semantic_task", start, &err)

	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE semantic_tasks SET status = ?, receipt = NULL,
		leased_at = NULL, lease_until = NULL, completed_at = ?, updated_at = ?
		WHERE task_id = ? AND status = ? AND receipt = ? AND lease_until IS NOT NULL AND lease_until > ?`,
		semantic.TaskSucceeded, now, now, taskID, semantic.TaskProcessing, receipt, now)
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
	return s.semanticTaskLeaseError(ctx, taskID)
}

// RenewSemanticTask extends the lease for a currently owned semantic task.
func (s *Store) RenewSemanticTask(ctx context.Context, taskID, receipt string, leaseDuration time.Duration) (leaseUntil time.Time, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "renew_semantic_task", start, &err)

	now := time.Now().UTC()
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	leaseUntil = now.Add(leaseDuration)
	res, err := s.db.ExecContext(ctx, `UPDATE semantic_tasks SET lease_until = ?, updated_at = ?
		WHERE task_id = ? AND status = ? AND receipt = ? AND lease_until IS NOT NULL AND lease_until > ?`,
		leaseUntil, now, taskID, semantic.TaskProcessing, receipt, now)
	if err != nil {
		return time.Time{}, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return time.Time{}, err
	}
	if rowsAffected > 0 {
		return leaseUntil, nil
	}
	if err := s.semanticTaskLeaseError(ctx, taskID); err != nil {
		return time.Time{}, err
	}
	return time.Time{}, nil
}

// RetrySemanticTask requeues or dead-letters a leased semantic task.
func (s *Store) RetrySemanticTask(ctx context.Context, taskID, receipt string, retryAt time.Time, lastErr string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "retry_semantic_task", start, &err)

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

	row := tx.QueryRowContext(ctx, `SELECT task_id, task_type, resource_id, resource_version, status,
		attempt_count, max_attempts, receipt, leased_at, lease_until, available_at,
		payload_json, last_error, created_at, updated_at, completed_at
		FROM semantic_tasks WHERE task_id = ? FOR UPDATE`, taskID)
	task, err := scanSemanticTask(row)
	if err != nil {
		if errors.Is(err, semantic.ErrTaskNotFound) {
			return semantic.ErrTaskNotFound
		}
		return err
	}
	if task.Status != semantic.TaskProcessing || task.Receipt != receipt || task.LeaseUntil == nil || !task.LeaseUntil.After(now) {
		return semantic.ErrTaskLeaseMismatch
	}

	if task.AttemptCount >= task.MaxAttempts {
		_, err = tx.ExecContext(ctx, `UPDATE semantic_tasks SET status = ?, receipt = NULL, leased_at = NULL,
			lease_until = NULL, last_error = ?, completed_at = ?, updated_at = ?
			WHERE task_id = ?`, semantic.TaskDeadLettered, nullStr(lastErr), now, now, taskID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE semantic_tasks SET status = ?, receipt = NULL, leased_at = NULL,
			lease_until = NULL, available_at = ?, last_error = ?, completed_at = NULL, updated_at = ?
			WHERE task_id = ?`, semantic.TaskQueued, retryAt, nullStr(lastErr), now, taskID)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// RecoverExpiredSemanticTasks requeues expired processing tasks.
func (s *Store) RecoverExpiredSemanticTasks(ctx context.Context, now time.Time, limit int) (recovered int, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "recover_expired_semantic_tasks", start, &err)

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

	query := `SELECT task_id FROM semantic_tasks
		WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?
		ORDER BY lease_until, created_at, task_id`
	args := []any{semantic.TaskProcessing, now}
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
		res, err := tx.ExecContext(ctx, `UPDATE semantic_tasks SET status = ?, receipt = NULL,
			leased_at = NULL, lease_until = NULL, available_at = ?, updated_at = ?
			WHERE task_id = ? AND status = ? AND lease_until IS NOT NULL AND lease_until < ?`,
			semantic.TaskQueued, now, now, taskID, semantic.TaskProcessing, now)
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

func semanticTaskExistsByResource(db execer, taskType semantic.TaskType, resourceID string, resourceVersion int64) (bool, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM semantic_tasks
		WHERE task_type = ? AND resource_id = ? AND resource_version = ?`,
		taskType, resourceID, resourceVersion).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Store) semanticTaskLeaseError(ctx context.Context, taskID string) error {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM semantic_tasks WHERE task_id = ?`, taskID).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return semantic.ErrTaskNotFound
	}
	return semantic.ErrTaskLeaseMismatch
}

func scanSemanticTask(s scanner) (*semantic.Task, error) {
	var task semantic.Task
	var receipt, lastError sql.NullString
	var leasedAt, leaseUntil, completedAt sql.NullTime
	var payload []byte
	err := s.Scan(&task.TaskID, &task.TaskType, &task.ResourceID, &task.ResourceVersion,
		&task.Status, &task.AttemptCount, &task.MaxAttempts, &receipt, &leasedAt,
		&leaseUntil, &task.AvailableAt, &payload, &lastError, &task.CreatedAt,
		&task.UpdatedAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, semantic.ErrTaskNotFound
		}
		return nil, err
	}
	task.Receipt = receipt.String
	task.PayloadJSON = append([]byte(nil), payload...)
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
