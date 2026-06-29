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

// defaultQuotaOutboxMaxAttempts bounds how long a poisoned quota mutation can
// block later tenant-local mutations behind the ordering barrier. With the
// current capped exponential backoff this is roughly tens of minutes, while
// still allowing normal transient central-store failures to recover.
const defaultQuotaOutboxMaxAttempts = 100
const quotaOutboxClaimMaxAttempts = 3
const quotaAdmissionLockName = "default"
const quotaOutboxClaimScanMultiplier = 64

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
	FileDelta    int64
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

// QuotaOutboxBatchClaimResult reports the outcome of a batch claim. An
// exhausted claim conflict means concurrent workers repeatedly won the same
// claim race; callers can treat it like an empty poll while recording
// tenant-scoped observability.
type QuotaOutboxBatchClaimResult struct {
	Entries           []QuotaOutboxEntry
	ConflictExhausted bool
}

// ErrQuotaOutboxLeaseMismatch means a worker tried to ack/retry an outbox row
// it no longer owns.
var ErrQuotaOutboxLeaseMismatch = errors.New("quota outbox lease mismatch")

var errQuotaOutboxClaimConflict = errors.New("quota outbox claim conflict")

// EnqueueQuotaOutboxTx inserts a queued quota mutation inside an existing
// tenant metadata transaction.
func (s *Store) EnqueueQuotaOutboxTx(db execer, entry *QuotaOutboxEntry) (int64, error) {
	return s.enqueueQuotaOutbox(db, entry)
}

// LockQuotaAdmissionTx serializes strict quota admission with quota_outbox
// apply/ack in a tenant-local transaction. Small writes intentionally do not
// take this lock; they use soft quota admission and durable outbox convergence.
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

// ClaimQuotaOutbox claims one queued quota mutation for this tenant store.
func (s *Store) ClaimQuotaOutbox(ctx context.Context, now time.Time, leaseDuration time.Duration) (out *QuotaOutboxEntry, found bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "claim_quota_outbox", start, &err)

	entries, err := s.claimQuotaOutboxBatch(ctx, now, leaseDuration, 1)
	if err != nil {
		return nil, false, err
	}
	if len(entries) == 0 {
		return nil, false, nil
	}
	return &entries[0], true, nil
}

// ClaimQuotaOutboxBatch claims up to limit queued quota mutations. It preserves
// ordering per file_id while allowing unrelated files to be processed in the
// same batch.
func (s *Store) ClaimQuotaOutboxBatch(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) (entries []QuotaOutboxEntry, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "claim_quota_outbox_batch", start, &err)

	result, err := s.claimQuotaOutboxBatchResult(ctx, now, leaseDuration, limit)
	if err != nil {
		return nil, err
	}
	return result.Entries, nil
}

// ClaimQuotaOutboxBatchResult claims queued quota mutations and returns whether
// repeated benign claim conflicts exhausted the bounded retry loop.
func (s *Store) ClaimQuotaOutboxBatchResult(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) (result QuotaOutboxBatchClaimResult, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "claim_quota_outbox_batch", start, &err)

	return s.claimQuotaOutboxBatchResult(ctx, now, leaseDuration, limit)
}

func (s *Store) claimQuotaOutboxBatch(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) ([]QuotaOutboxEntry, error) {
	result, err := s.claimQuotaOutboxBatchResult(ctx, now, leaseDuration, limit)
	if err != nil {
		return nil, err
	}
	return result.Entries, nil
}

func (s *Store) claimQuotaOutboxBatchResult(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) (QuotaOutboxBatchClaimResult, error) {
	for attempt := 0; attempt < quotaOutboxClaimMaxAttempts; attempt++ {
		entries, err := s.claimQuotaOutboxBatchOnce(ctx, now, leaseDuration, limit)
		if errors.Is(err, errQuotaOutboxClaimConflict) {
			continue
		}
		return QuotaOutboxBatchClaimResult{Entries: entries}, err
	}
	return QuotaOutboxBatchClaimResult{ConflictExhausted: true}, nil
}

func (s *Store) claimQuotaOutboxBatchOnce(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) ([]QuotaOutboxEntry, error) {
	if limit <= 0 {
		limit = 1
	}
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
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Keep the first step as a simple claim-index scan. The original single
	// NOT EXISTS query had to encode both per-file ordering and NULL-file global
	// barriers in SQL; TiDB can plan that as a large cartesian anti join on hot
	// outbox tables. Lock a bounded candidate window first, then use indexed
	// minimum-id checks below to preserve the same ordering semantics.
	scanLimit := quotaOutboxClaimScanLimit(limit)
	var entries []QuotaOutboxEntry
	var lastCandidateID int64
	for {
		rows, err := tx.QueryContext(ctx, quotaOutboxSelectSQL+`
			FROM quota_outbox q FORCE INDEX (idx_quota_outbox_claim)
			WHERE q.status = ? AND q.available_at <= ? AND q.id > ?
			ORDER BY q.id
			LIMIT ?
			FOR UPDATE SKIP LOCKED`, QuotaOutboxQueued, now, lastCandidateID, scanLimit)
		if err != nil {
			return nil, err
		}

		candidates := make([]QuotaOutboxEntry, 0, scanLimit)
		for rows.Next() {
			entry, err := scanQuotaOutbox(rows)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			candidates = append(candidates, *entry)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}

		if len(candidates) == 0 {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		lastCandidateID = candidates[len(candidates)-1].ID

		entries, err = s.claimableQuotaOutboxEntries(ctx, tx, candidates, limit)
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			break
		}
		if len(candidates) < scanLimit {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return nil, nil
		}
	}

	receipt := uuid.NewString()
	leasedAt := now
	leaseUntil := now.Add(leaseDuration)
	ids := make([]any, 0, len(entries))
	for i := range entries {
		ids = append(ids, entries[i].ID)
	}
	args := append([]any{
		QuotaOutboxProcessing, receipt, leasedAt, leaseUntil, now,
		QuotaOutboxQueued, now,
	}, ids...)
	res, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE quota_outbox SET status = ?,
		attempt_count = attempt_count + 1, receipt = ?, leased_at = ?,
		lease_until = ?, updated_at = ?
		WHERE status = ? AND available_at <= ? AND id IN (%s)`, sqlPlaceholders(len(entries))), args...)
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rowsAffected != int64(len(entries)) {
		// Under concurrent SKIP LOCKED claims, TiDB can report that fewer rows
		// were claimed than the preceding read selected. Treat that as a benign
		// claim race: rollback this tx and let this worker retry or let another
		// worker process the rows. Ack/retry paths still use
		// ErrQuotaOutboxLeaseMismatch for real ownership failures.
		return nil, errQuotaOutboxClaimConflict
	}

	for i := range entries {
		entries[i].Status = QuotaOutboxProcessing
		entries[i].AttemptCount++
		entries[i].Receipt = receipt
		entries[i].LeasedAt = &leasedAt
		entries[i].LeaseUntil = &leaseUntil
		entries[i].UpdatedAt = now
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return entries, nil
}

func quotaOutboxClaimScanLimit(limit int) int {
	if limit <= 0 {
		limit = 1
	}
	scanLimit := limit * quotaOutboxClaimScanMultiplier
	if scanLimit < limit {
		return limit
	}
	return scanLimit
}

func (s *Store) claimableQuotaOutboxEntries(ctx context.Context, tx *sql.Tx, candidates []QuotaOutboxEntry, limit int) ([]QuotaOutboxEntry, error) {
	if len(candidates) == 0 || limit <= 0 {
		return nil, nil
	}

	fileIDs := make([]string, 0, len(candidates))
	fileIDSeen := make(map[string]struct{}, len(candidates))
	for i := range candidates {
		if candidates[i].FileID == "" {
			continue
		}
		if _, ok := fileIDSeen[candidates[i].FileID]; ok {
			continue
		}
		fileIDSeen[candidates[i].FileID] = struct{}{}
		fileIDs = append(fileIDs, candidates[i].FileID)
	}

	minPendingByFile, err := s.minPendingQuotaOutboxIDByFile(ctx, tx, fileIDs)
	if err != nil {
		return nil, err
	}
	minNullPendingID, err := s.minNullFilePendingQuotaOutboxID(ctx, tx)
	if err != nil {
		return nil, err
	}

	out := make([]QuotaOutboxEntry, 0, minInt(limit, len(candidates)))
	for i := range candidates {
		candidate := candidates[i]
		if candidate.FileID == "" {
			blocked, err := s.hasPendingQuotaOutboxBeforeID(ctx, tx, candidate.ID)
			if err != nil {
				return nil, err
			}
			if !blocked {
				out = append(out, candidate)
			}
			break
		} else {
			if minID, ok := minPendingByFile[candidate.FileID]; !ok || minID != candidate.ID {
				continue
			}
			if minNullPendingID > 0 && minNullPendingID < candidate.ID {
				continue
			}
		}
		out = append(out, candidate)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) minPendingQuotaOutboxIDByFile(ctx context.Context, tx *sql.Tx, fileIDs []string) (map[string]int64, error) {
	out := make(map[string]int64, len(fileIDs))
	if len(fileIDs) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(fileIDs)+2)
	for _, fileID := range fileIDs {
		args = append(args, fileID)
	}
	args = append(args, QuotaOutboxQueued, QuotaOutboxProcessing)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT file_id, MIN(id)
		FROM quota_outbox FORCE INDEX (idx_quota_outbox_file_order)
		WHERE file_id IN (%s) AND status IN (?, ?)
		GROUP BY file_id`, sqlPlaceholders(len(fileIDs))), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var fileID string
		var id int64
		if err := rows.Scan(&fileID, &id); err != nil {
			return nil, err
		}
		out[fileID] = id
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) minNullFilePendingQuotaOutboxID(ctx context.Context, tx *sql.Tx) (int64, error) {
	var id sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT MIN(id)
		FROM quota_outbox FORCE INDEX (idx_quota_outbox_file_order)
		WHERE file_id IS NULL AND status IN (?, ?)`,
		QuotaOutboxQueued, QuotaOutboxProcessing).Scan(&id)
	if err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func (s *Store) hasPendingQuotaOutboxBeforeID(ctx context.Context, tx *sql.Tx, id int64) (bool, error) {
	var olderID int64
	err := tx.QueryRowContext(ctx, `SELECT id
		FROM quota_outbox FORCE INDEX (PRIMARY)
		WHERE id < ? AND status IN (?, ?)
		ORDER BY id
		LIMIT 1`,
		id, QuotaOutboxQueued, QuotaOutboxProcessing).Scan(&olderID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// AckQuotaOutboxBatchTx marks leased quota outbox rows as successfully applied
// inside a caller-owned transaction.
func (s *Store) AckQuotaOutboxBatchTx(ctx context.Context, tx *sql.Tx, entries []QuotaOutboxEntry) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ack_quota_outbox_batch_tx", start, &err)

	if tx == nil {
		return fmt.Errorf("quota outbox ack transaction is required")
	}
	if len(entries) == 0 {
		return nil
	}
	now := time.Now().UTC()
	ids := make([]any, 0, len(entries))
	receipt := entries[0].Receipt
	for i := range entries {
		if entries[i].Receipt != receipt {
			return ErrQuotaOutboxLeaseMismatch
		}
		ids = append(ids, entries[i].ID)
	}
	// Receipt is the worker ownership token. Recovery clears receipt before
	// requeueing, so stale owners cannot match after another worker reclaims.
	args := append([]any{QuotaOutboxSucceeded, now, now, QuotaOutboxProcessing, receipt}, ids...)
	res, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE quota_outbox SET status = ?, receipt = NULL,
		leased_at = NULL, lease_until = NULL, completed_at = ?, updated_at = ?
		WHERE status = ? AND receipt = ?
		  AND id IN (%s)`, sqlPlaceholders(len(entries))), args...)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != int64(len(entries)) {
		return ErrQuotaOutboxLeaseMismatch
	}
	return nil
}

func (s *Store) ackQuotaOutbox(ctx context.Context, db execer, id int64, receipt string) error {
	now := time.Now().UTC()
	res, err := db.ExecContext(ctx, `UPDATE quota_outbox SET status = ?, receipt = NULL,
		leased_at = NULL, lease_until = NULL, completed_at = ?, updated_at = ?
		WHERE id = ? AND status = ? AND receipt = ?`,
		QuotaOutboxSucceeded, now, now, id, QuotaOutboxProcessing, receipt)
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

func sqlPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	if n == 1 {
		return "?"
	}
	return "?" + strings.Repeat(",?", n-1)
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
	// Receipt is the worker ownership token. Recovery clears the receipt before
	// requeueing, so stale owners fail here even when their old lease expired.
	if entry.Status != QuotaOutboxProcessing || entry.Receipt != receipt {
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
func (s *Store) PendingQuotaOutboxDeltasTx(tx *sql.Tx) (storageDelta, fileDelta, mediaDelta int64, err error) {
	return scanQuotaOutboxPendingDeltas(tx.QueryRow(`SELECT
		COALESCE(SUM(storage_delta), 0),
		COALESCE(SUM(file_delta), 0),
		COALESCE(SUM(media_delta), 0)
		FROM quota_outbox WHERE status IN (?, ?)`,
		QuotaOutboxQueued, QuotaOutboxProcessing))
}

// PendingQuotaOutboxDeltas returns queued/processing quota deltas outside a
// caller-owned transaction.
func (s *Store) PendingQuotaOutboxDeltas(ctx context.Context) (storageDelta, fileDelta, mediaDelta int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "pending_quota_outbox_deltas", start, &err)
	return scanQuotaOutboxPendingDeltas(s.db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(storage_delta), 0),
		COALESCE(SUM(file_delta), 0),
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
		  AND mutation_type IN ('file_create', 'file_overwrite', 'upload_complete')
		  AND status IN (?, ?)`,
		fileID, QuotaOutboxQueued, QuotaOutboxProcessing).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func scanQuotaOutboxPendingDeltas(row scanner) (storageDelta, fileDelta, mediaDelta int64, err error) {
	if err := row.Scan(&storageDelta, &fileDelta, &mediaDelta); err != nil {
		return 0, 0, 0, err
	}
	return storageDelta, fileDelta, mediaDelta, nil
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
		(file_id, mutation_type, mutation_data, storage_delta, file_delta, media_delta,
		 status, attempt_count, max_attempts, receipt, leased_at, lease_until,
		 available_at, last_error, created_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullStr(entry.FileID), entry.MutationType, []byte(entry.MutationData),
		entry.StorageDelta, entry.FileDelta, entry.MediaDelta, status, entry.AttemptCount, maxAttempts,
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
	storage_delta, file_delta, media_delta, status, attempt_count, max_attempts, receipt,
	leased_at, lease_until, available_at, last_error, created_at, updated_at,
	completed_at`

func scanQuotaOutbox(s scanner) (*QuotaOutboxEntry, error) {
	var entry QuotaOutboxEntry
	var fileID, receipt, lastError sql.NullString
	var mutationData []byte
	var leasedAt, leaseUntil, completedAt sql.NullTime
	err := s.Scan(&entry.ID, &fileID, &entry.MutationType, &mutationData,
		&entry.StorageDelta, &entry.FileDelta, &entry.MediaDelta, &entry.Status, &entry.AttemptCount,
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
