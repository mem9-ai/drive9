package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const defaultQuotaOutboxMaxAttempts = 0
const quotaAdmissionLockName = "default"

type QuotaOutboxStatus string

const (
	QuotaOutboxQueued       QuotaOutboxStatus = "queued"
	QuotaOutboxProcessing   QuotaOutboxStatus = "processing"
	QuotaOutboxSucceeded    QuotaOutboxStatus = "succeeded"
	QuotaOutboxDeadLettered QuotaOutboxStatus = "dead_lettered"
)

// QuotaOutboxEntry records a tenant-local quota mutation that must be applied
// to the central quota tables. It is written in the same tenant DB transaction
// as the file mutation, so a successful file write cannot lose its quota event.
type QuotaOutboxEntry struct {
	ID           int64
	FileID       string
	MutationType string
	MutationData json.RawMessage
	StorageDelta int64
	MediaDelta   int64
	Status       QuotaOutboxStatus
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

// ErrQuotaOutboxLeaseMismatch means a worker tried to ack/retry an outbox row
// it no longer owns.
var ErrQuotaOutboxLeaseMismatch = errors.New("quota outbox lease mismatch")

// EnqueueQuotaOutboxTx inserts a queued quota mutation inside an existing
// tenant metadata transaction.
func (s *Store) EnqueueQuotaOutboxTx(db execer, entry *QuotaOutboxEntry) (int64, error) {
	return s.enqueueQuotaOutbox(db, entry)
}

// LockQuotaAdmissionTx serializes quota admission checks and quota_outbox
// enqueue in a tenant-local transaction. This protects multi-server deployments
// from concurrent writers reading the same pending-delta snapshot.
func (s *Store) LockQuotaAdmissionTx(db execer) error {
	now := time.Now().UTC()
	if _, err := db.Exec(`INSERT INTO quota_admission_locks (name, updated_at)
		VALUES (?, ?) ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)`,
		quotaAdmissionLockName, now); err != nil {
		return err
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM quota_admission_locks WHERE name = ? FOR UPDATE`,
		quotaAdmissionLockName).Scan(&name); err != nil {
		return err
	}
	return nil
}

// ClaimQuotaOutbox claims the oldest queued quota mutation for this tenant
// store. Only one row is allowed to be processing at a time, preserving
// per-tenant apply order even when multiple server instances poll the same
// tenant DB.
func (s *Store) ClaimQuotaOutbox(ctx context.Context, now time.Time, leaseDuration time.Duration) (out *QuotaOutboxEntry, found bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "claim_quota_outbox", start, &err)

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

	row := tx.QueryRowContext(ctx, quotaOutboxSelectSQL+`
		FROM quota_outbox q
		WHERE q.status = ? AND q.available_at <= ?
		  AND NOT EXISTS (
		    SELECT 1 FROM quota_outbox older
		     WHERE older.id < q.id AND older.status IN (?, ?)
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM quota_outbox active
		     WHERE active.status = ? AND active.lease_until IS NOT NULL AND active.lease_until > ?
		  )
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`, QuotaOutboxQueued, now,
		QuotaOutboxQueued, QuotaOutboxProcessing, QuotaOutboxProcessing, now)
	entry, err := scanQuotaOutbox(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	receipt := uuid.NewString()
	leasedAt := now
	leaseUntil := now.Add(leaseDuration)
	res, err := tx.ExecContext(ctx, `UPDATE quota_outbox SET status = ?,
		attempt_count = attempt_count + 1, receipt = ?, leased_at = ?,
		lease_until = ?, updated_at = ?
		WHERE id = ? AND status = ? AND available_at <= ?`,
		QuotaOutboxProcessing, receipt, leasedAt, leaseUntil, now,
		entry.ID, QuotaOutboxQueued, now)
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

	entry.Status = QuotaOutboxProcessing
	entry.AttemptCount++
	entry.Receipt = receipt
	entry.LeasedAt = &leasedAt
	entry.LeaseUntil = &leaseUntil
	entry.UpdatedAt = now

	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return entry, true, nil
}

// AckQuotaOutbox marks a leased quota outbox row as successfully applied.
func (s *Store) AckQuotaOutbox(ctx context.Context, id int64, receipt string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ack_quota_outbox", start, &err)

	return s.ackQuotaOutbox(ctx, s.db, id, receipt)
}

// AckQuotaOutboxTx marks a leased quota outbox row as successfully applied
// inside a caller-owned transaction.
func (s *Store) AckQuotaOutboxTx(ctx context.Context, tx *sql.Tx, id int64, receipt string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ack_quota_outbox_tx", start, &err)

	if tx == nil {
		return fmt.Errorf("quota outbox ack transaction is required")
	}
	return s.ackQuotaOutbox(ctx, tx, id, receipt)
}

func (s *Store) ackQuotaOutbox(ctx context.Context, db execer, id int64, receipt string) error {
	now := time.Now().UTC()
	res, err := db.ExecContext(ctx, `UPDATE quota_outbox SET status = ?, receipt = NULL,
		leased_at = NULL, lease_until = NULL, completed_at = ?, updated_at = ?
		WHERE id = ? AND status = ? AND receipt = ? AND lease_until IS NOT NULL AND lease_until > ?`,
		QuotaOutboxSucceeded, now, now, id, QuotaOutboxProcessing, receipt, now)
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
	return s.quotaOutboxLeaseError(ctx, db, id)
}

// RetryQuotaOutbox requeues or dead-letters a leased quota outbox row.
func (s *Store) RetryQuotaOutbox(ctx context.Context, id int64, receipt string, retryAt time.Time, lastErr string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "retry_quota_outbox", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.retryQuotaOutboxTx(ctx, tx, id, receipt, retryAt, lastErr); err != nil {
		return err
	}
	return tx.Commit()
}

// RetryQuotaOutboxTx requeues or dead-letters a leased quota outbox row inside
// a caller-owned transaction.
func (s *Store) RetryQuotaOutboxTx(ctx context.Context, tx *sql.Tx, id int64, receipt string, retryAt time.Time, lastErr string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "retry_quota_outbox_tx", start, &err)

	if tx == nil {
		return fmt.Errorf("quota outbox retry transaction is required")
	}
	return s.retryQuotaOutboxTx(ctx, tx, id, receipt, retryAt, lastErr)
}

func (s *Store) retryQuotaOutboxTx(ctx context.Context, tx *sql.Tx, id int64, receipt string, retryAt time.Time, lastErr string) error {
	now := time.Now().UTC()
	if retryAt.IsZero() {
		retryAt = now
	} else {
		retryAt = retryAt.UTC()
	}

	row := tx.QueryRowContext(ctx, quotaOutboxSelectSQL+` FROM quota_outbox WHERE id = ? FOR UPDATE`, id)
	entry, err := scanQuotaOutbox(row)
	if err != nil {
		return err
	}
	if entry.Status != QuotaOutboxProcessing || entry.Receipt != receipt || entry.LeaseUntil == nil || !entry.LeaseUntil.After(now) {
		return ErrQuotaOutboxLeaseMismatch
	}

	if entry.MaxAttempts > 0 && entry.AttemptCount >= entry.MaxAttempts {
		_, err = tx.ExecContext(ctx, `UPDATE quota_outbox SET status = ?, receipt = NULL,
			leased_at = NULL, lease_until = NULL, last_error = ?, completed_at = ?, updated_at = ?
			WHERE id = ?`, QuotaOutboxDeadLettered, nullStr(lastErr), now, now, id)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE quota_outbox SET status = ?, receipt = NULL,
			leased_at = NULL, lease_until = NULL, available_at = ?, last_error = ?,
			completed_at = NULL, updated_at = ?
			WHERE id = ?`, QuotaOutboxQueued, retryAt, nullStr(lastErr), now, id)
	}
	if err != nil {
		return err
	}
	return nil
}

// RecoverExpiredQuotaOutbox requeues processing outbox rows whose lease expired.
func (s *Store) RecoverExpiredQuotaOutbox(ctx context.Context, now time.Time, limit int) (recovered int, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "recover_expired_quota_outbox", start, &err)

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

	query := `SELECT id FROM quota_outbox
		WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?
		ORDER BY lease_until, id`
	args := []any{QuotaOutboxProcessing, now}
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

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, id := range ids {
		res, err := tx.ExecContext(ctx, `UPDATE quota_outbox SET status = ?, receipt = NULL,
			leased_at = NULL, lease_until = NULL, available_at = ?, updated_at = ?
			WHERE id = ? AND status = ? AND lease_until IS NOT NULL AND lease_until < ?`,
			QuotaOutboxQueued, now, now, id, QuotaOutboxProcessing, now)
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

// PendingQuotaOutboxDeltasTx returns the queued/processing quota deltas that
// have not necessarily converged to the central quota tables yet.
func (s *Store) PendingQuotaOutboxDeltasTx(tx *sql.Tx) (storageDelta, mediaDelta int64, err error) {
	return scanQuotaOutboxPendingDeltas(tx.QueryRow(`SELECT
		COALESCE(SUM(storage_delta), 0),
		COALESCE(SUM(media_delta), 0)
		FROM quota_outbox WHERE status IN (?, ?)`,
		QuotaOutboxQueued, QuotaOutboxProcessing))
}

// PendingQuotaOutboxDeltas returns queued/processing quota deltas outside a
// caller-owned transaction.
func (s *Store) PendingQuotaOutboxDeltas(ctx context.Context) (storageDelta, mediaDelta int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "pending_quota_outbox_deltas", start, &err)
	return scanQuotaOutboxPendingDeltas(s.db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(storage_delta), 0),
		COALESCE(SUM(media_delta), 0)
		FROM quota_outbox WHERE status IN (?, ?)`,
		QuotaOutboxQueued, QuotaOutboxProcessing))
}

// HasPendingQuotaOutboxFileMutation reports whether a file still has a queued
// or processing tenant-local quota mutation.
func (s *Store) HasPendingQuotaOutboxFileMutation(ctx context.Context, fileID string) (pending bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "has_pending_quota_outbox_file_mutation", start, &err)

	var count int
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox
		WHERE file_id = ?
		  AND mutation_type IN ('file_create', 'file_overwrite')
		  AND status IN (?, ?)`,
		fileID, QuotaOutboxQueued, QuotaOutboxProcessing).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func scanQuotaOutboxPendingDeltas(row scanner) (storageDelta, mediaDelta int64, err error) {
	if err := row.Scan(&storageDelta, &mediaDelta); err != nil {
		return 0, 0, err
	}
	return storageDelta, mediaDelta, nil
}

func (s *Store) enqueueQuotaOutbox(db execer, entry *QuotaOutboxEntry) (int64, error) {
	if entry == nil {
		return 0, fmt.Errorf("quota outbox entry is required")
	}
	if strings.TrimSpace(entry.MutationType) == "" {
		return 0, fmt.Errorf("mutation_type is required")
	}
	if len(entry.MutationData) == 0 {
		return 0, fmt.Errorf("mutation_data is required")
	}
	now := time.Now().UTC()
	status := entry.Status
	if status == "" {
		status = QuotaOutboxQueued
	}
	maxAttempts := entry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultQuotaOutboxMaxAttempts
	}
	availableAt := entry.AvailableAt
	if availableAt.IsZero() {
		availableAt = now
	}
	createdAt := entry.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := entry.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	res, err := db.Exec(`INSERT INTO quota_outbox
		(file_id, mutation_type, mutation_data, storage_delta, media_delta,
		 status, attempt_count, max_attempts, receipt, leased_at, lease_until,
		 available_at, last_error, created_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullStr(entry.FileID), entry.MutationType, []byte(entry.MutationData),
		entry.StorageDelta, entry.MediaDelta, status, entry.AttemptCount, maxAttempts,
		nullStr(entry.Receipt), nilTime(entry.LeasedAt), nilTime(entry.LeaseUntil),
		availableAt.UTC(), nullStr(entry.LastError), createdAt.UTC(),
		updatedAt.UTC(), nilTime(entry.CompletedAt))
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (s *Store) quotaOutboxLeaseError(ctx context.Context, db execer, id int64) error {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox WHERE id = ?`, id).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	return ErrQuotaOutboxLeaseMismatch
}

const quotaOutboxSelectSQL = `SELECT id, file_id, mutation_type, mutation_data,
	storage_delta, media_delta, status, attempt_count, max_attempts, receipt,
	leased_at, lease_until, available_at, last_error, created_at, updated_at,
	completed_at`

func scanQuotaOutbox(s scanner) (*QuotaOutboxEntry, error) {
	var entry QuotaOutboxEntry
	var fileID, receipt, lastError sql.NullString
	var mutationData []byte
	var leasedAt, leaseUntil, completedAt sql.NullTime
	err := s.Scan(&entry.ID, &fileID, &entry.MutationType, &mutationData,
		&entry.StorageDelta, &entry.MediaDelta, &entry.Status, &entry.AttemptCount,
		&entry.MaxAttempts, &receipt, &leasedAt, &leaseUntil, &entry.AvailableAt,
		&lastError, &entry.CreatedAt, &entry.UpdatedAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	entry.FileID = fileID.String
	entry.MutationData = append(entry.MutationData[:0], mutationData...)
	entry.Receipt = receipt.String
	entry.LastError = lastError.String
	entry.AvailableAt = entry.AvailableAt.UTC()
	entry.CreatedAt = entry.CreatedAt.UTC()
	entry.UpdatedAt = entry.UpdatedAt.UTC()
	if leasedAt.Valid {
		t := leasedAt.Time.UTC()
		entry.LeasedAt = &t
	}
	if leaseUntil.Valid {
		t := leaseUntil.Time.UTC()
		entry.LeaseUntil = &t
	}
	if completedAt.Valid {
		t := completedAt.Time.UTC()
		entry.CompletedAt = &t
	}
	return &entry, nil
}
