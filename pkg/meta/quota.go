package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// QuotaConfig holds per-tenant quota limits stored in the central server DB.
type QuotaConfig struct {
	TenantID         string
	MaxStorageBytes  int64
	MaxMediaLLMFiles int64
	MaxMonthlyCostMC int64 // millicents; 0 = disabled
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// QuotaUsage holds pre-aggregated quota counters for a tenant.
type QuotaUsage struct {
	TenantID       string
	StorageBytes   int64
	ReservedBytes  int64
	MediaFileCount int64
	UpdatedAt      time.Time
}

// FileMeta tracks per-file quota-relevant metadata in the server DB.
// This is the authoritative source for file size and media classification
// used to compute quota deltas on overwrite/delete.
type FileMeta struct {
	TenantID  string
	FileID    string
	SizeBytes int64
	IsMedia   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// UploadReservation tracks a single in-flight upload's storage reservation.
type UploadReservation struct {
	TenantID      string
	UploadID      string
	ReservedBytes int64
	TargetPath    string
	Status        string // "active", "completed", "aborted"
	ExpiresAt     time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// LLMUsageRecord is a single billable LLM call in the central ledger.
type LLMUsageRecord struct {
	ID             int64
	TenantID       string
	TaskType       string
	TaskID         string
	CostMillicents int64
	RawUnits       int64
	RawUnitType    string
	CreatedAt      time.Time
}

// MutationLogEntry represents a durable outbox entry for quota state mutations.
type MutationLogEntry struct {
	ID           int64
	TenantID     string
	MutationType string // file_create, file_overwrite, file_delete, upload_initiate, upload_complete, upload_abort, llm_cost_record
	MutationData json.RawMessage
	Status       string // "pending", "applied", "failed"
	RetryCount   int
	CreatedAt    time.Time
	AppliedAt    *time.Time
}

// --- QuotaConfig operations ---

// GetQuotaConfig returns the per-tenant quota configuration.
// Returns default values if no row exists for the tenant.
func (s *Store) GetQuotaConfig(ctx context.Context, tenantID string) (*QuotaConfig, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "get_quota_config", start, &err)

	cfg := &QuotaConfig{TenantID: tenantID}
	err = s.db.QueryRowContext(ctx,
		`SELECT max_storage_bytes, max_media_llm_files, max_monthly_cost_mc, created_at, updated_at
		 FROM tenant_quota_config WHERE tenant_id = ?`, tenantID,
	).Scan(&cfg.MaxStorageBytes, &cfg.MaxMediaLLMFiles, &cfg.MaxMonthlyCostMC, &cfg.CreatedAt, &cfg.UpdatedAt)
	if err == sql.ErrNoRows {
		// Return defaults when no per-tenant config exists.
		cfg.MaxStorageBytes = 53687091200 // 50 GiB
		cfg.MaxMediaLLMFiles = 500
		cfg.MaxMonthlyCostMC = 0
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// SetQuotaConfig upserts per-tenant quota configuration.
func (s *Store) SetQuotaConfig(ctx context.Context, cfg *QuotaConfig) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "set_quota_config", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_quota_config (tenant_id, max_storage_bytes, max_media_llm_files, max_monthly_cost_mc)
		 VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   max_storage_bytes = VALUES(max_storage_bytes),
		   max_media_llm_files = VALUES(max_media_llm_files),
		   max_monthly_cost_mc = VALUES(max_monthly_cost_mc)`,
		cfg.TenantID, cfg.MaxStorageBytes, cfg.MaxMediaLLMFiles, cfg.MaxMonthlyCostMC)
	return err
}

// --- QuotaUsage operations ---

// GetQuotaUsage returns the pre-aggregated quota counters for a tenant.
func (s *Store) GetQuotaUsage(ctx context.Context, tenantID string) (*QuotaUsage, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "get_quota_usage", start, &err)

	u := &QuotaUsage{TenantID: tenantID}
	err = s.db.QueryRowContext(ctx,
		`SELECT storage_bytes, reserved_bytes, media_file_count, updated_at
		 FROM tenant_quota_usage WHERE tenant_id = ?`, tenantID,
	).Scan(&u.StorageBytes, &u.ReservedBytes, &u.MediaFileCount, &u.UpdatedAt)
	if err == sql.ErrNoRows {
		return u, nil // zero counters
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

// EnsureQuotaUsageRow creates a zero-valued quota usage row if none exists.
func (s *Store) EnsureQuotaUsageRow(ctx context.Context, tenantID string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT IGNORE INTO tenant_quota_usage (tenant_id) VALUES (?)`, tenantID)
	return err
}

// ensureRowsAffected checks that an UPDATE touched at least one row.
// Returns an error if the tenant_quota_usage row is missing.
func ensureRowsAffected(res sql.Result, tenantID string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("tenant_quota_usage row missing for tenant %s", tenantID)
	}
	return nil
}

// IncrStorageBytes atomically adjusts the storage_bytes counter.
func (s *Store) IncrStorageBytes(ctx context.Context, tenantID string, delta int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "incr_storage_bytes", start, &err)

	res, err := s.db.ExecContext(ctx,
		`UPDATE tenant_quota_usage SET storage_bytes = storage_bytes + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// IncrStorageBytesTx atomically adjusts the storage_bytes counter inside a transaction.
func (s *Store) IncrStorageBytesTx(tx *sql.Tx, tenantID string, delta int64) error {
	res, err := tx.Exec(
		`UPDATE tenant_quota_usage SET storage_bytes = storage_bytes + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// IncrReservedBytes atomically adjusts the reserved_bytes counter.
func (s *Store) IncrReservedBytes(ctx context.Context, tenantID string, delta int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "incr_reserved_bytes", start, &err)

	res, err := s.db.ExecContext(ctx,
		`UPDATE tenant_quota_usage SET reserved_bytes = reserved_bytes + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// IncrReservedBytesTx atomically adjusts the reserved_bytes counter inside a transaction.
func (s *Store) IncrReservedBytesTx(tx *sql.Tx, tenantID string, delta int64) error {
	res, err := tx.Exec(
		`UPDATE tenant_quota_usage SET reserved_bytes = reserved_bytes + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// IncrMediaFileCount atomically adjusts the media_file_count counter.
func (s *Store) IncrMediaFileCount(ctx context.Context, tenantID string, delta int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "incr_media_file_count", start, &err)

	res, err := s.db.ExecContext(ctx,
		`UPDATE tenant_quota_usage SET media_file_count = media_file_count + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// IncrMediaFileCountTx atomically adjusts the media_file_count counter inside a transaction.
func (s *Store) IncrMediaFileCountTx(tx *sql.Tx, tenantID string, delta int64) error {
	res, err := tx.Exec(
		`UPDATE tenant_quota_usage SET media_file_count = media_file_count + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// TransferReservedToConfirmed atomically moves bytes from reserved to confirmed storage.
func (s *Store) TransferReservedToConfirmed(ctx context.Context, tenantID string, reservedDelta, storageDelta int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "transfer_reserved_to_confirmed", start, &err)

	res, err := s.db.ExecContext(ctx,
		`UPDATE tenant_quota_usage
		 SET reserved_bytes = reserved_bytes + ?, storage_bytes = storage_bytes + ?
		 WHERE tenant_id = ?`,
		reservedDelta, storageDelta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// TransferReservedToConfirmedTx atomically moves bytes from reserved to confirmed storage in a transaction.
func (s *Store) TransferReservedToConfirmedTx(tx *sql.Tx, tenantID string, reservedDelta, storageDelta int64) error {
	res, err := tx.Exec(
		`UPDATE tenant_quota_usage
		 SET reserved_bytes = reserved_bytes + ?, storage_bytes = storage_bytes + ?
		 WHERE tenant_id = ?`,
		reservedDelta, storageDelta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// defaultMaxStorageBytes is the fallback limit when no per-tenant config row exists.
const defaultMaxStorageBytes = int64(50 * (1 << 30)) // 50 GiB

// AtomicReserveUpload performs an atomic check-and-claim for upload reservation.
// Returns ErrStorageQuotaExceeded if the projected total exceeds the quota.
// This is the server-reserve-first protocol for upload initiate.
// When no tenant_quota_config row exists, falls back to the default 50 GiB limit.
func (s *Store) AtomicReserveUpload(ctx context.Context, tenantID string, reserveBytes int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "atomic_reserve_upload", start, &err)

	res, err := s.db.ExecContext(ctx,
		`UPDATE tenant_quota_usage
		 SET reserved_bytes = reserved_bytes + ?
		 WHERE tenant_id = ?
		   AND storage_bytes + reserved_bytes + ? <=
		       COALESCE((SELECT max_storage_bytes FROM tenant_quota_config WHERE tenant_id = ?), ?)`,
		reserveBytes, tenantID, reserveBytes, tenantID, defaultMaxStorageBytes)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		err = ErrStorageQuotaExceeded
		return err
	}
	return nil
}

// --- FileMeta operations ---

// UpsertFileMeta creates or updates a file's quota-relevant metadata.
func (s *Store) UpsertFileMeta(ctx context.Context, fm *FileMeta) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "upsert_file_meta", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_file_meta (tenant_id, file_id, size_bytes, is_media)
		 VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   size_bytes = VALUES(size_bytes),
		   is_media = VALUES(is_media)`,
		fm.TenantID, fm.FileID, fm.SizeBytes, boolToInt(fm.IsMedia))
	return err
}

// UpsertFileMetaTx creates or updates a file's quota metadata inside a transaction.
func (s *Store) UpsertFileMetaTx(tx *sql.Tx, fm *FileMeta) error {
	_, err := tx.Exec(
		`INSERT INTO tenant_file_meta (tenant_id, file_id, size_bytes, is_media)
		 VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   size_bytes = VALUES(size_bytes),
		   is_media = VALUES(is_media)`,
		fm.TenantID, fm.FileID, fm.SizeBytes, boolToInt(fm.IsMedia))
	return err
}

// GetFileMeta returns a file's quota-relevant metadata from the server DB.
func (s *Store) GetFileMeta(ctx context.Context, tenantID, fileID string) (*FileMeta, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "get_file_meta", start, &err)

	fm := &FileMeta{TenantID: tenantID, FileID: fileID}
	var isMedia int
	err = s.db.QueryRowContext(ctx,
		`SELECT size_bytes, is_media, created_at, updated_at
		 FROM tenant_file_meta WHERE tenant_id = ? AND file_id = ?`,
		tenantID, fileID,
	).Scan(&fm.SizeBytes, &isMedia, &fm.CreatedAt, &fm.UpdatedAt)
	if err == sql.ErrNoRows {
		err = ErrNotFound
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	fm.IsMedia = isMedia == 1
	return fm, nil
}

// DeleteFileMeta removes a file's metadata from the server DB.
func (s *Store) DeleteFileMeta(ctx context.Context, tenantID, fileID string) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "delete_file_meta", start, &err)

	_, err = s.db.ExecContext(ctx,
		`DELETE FROM tenant_file_meta WHERE tenant_id = ? AND file_id = ?`,
		tenantID, fileID)
	return err
}

// DeleteFileMetaTx removes a file's metadata inside a transaction.
func (s *Store) DeleteFileMetaTx(tx *sql.Tx, tenantID, fileID string) error {
	_, err := tx.Exec(
		`DELETE FROM tenant_file_meta WHERE tenant_id = ? AND file_id = ?`,
		tenantID, fileID)
	return err
}

// --- Upload reservation operations ---

// InsertUploadReservation creates a new active upload reservation.
func (s *Store) InsertUploadReservation(ctx context.Context, r *UploadReservation) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "insert_upload_reservation", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_upload_reservations (tenant_id, upload_id, reserved_bytes, target_path, status, expires_at)
		 VALUES (?, ?, ?, ?, 'active', ?)`,
		r.TenantID, r.UploadID, r.ReservedBytes, r.TargetPath, r.ExpiresAt)
	return err
}

// UpdateUploadReservationStatus updates a reservation's status.
func (s *Store) UpdateUploadReservationStatus(ctx context.Context, tenantID, uploadID, status string) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "update_upload_reservation_status", start, &err)

	_, err = s.db.ExecContext(ctx,
		`UPDATE tenant_upload_reservations SET status = ? WHERE tenant_id = ? AND upload_id = ? AND status = 'active'`,
		status, tenantID, uploadID)
	return err
}

// UpdateUploadReservationStatusTx updates a reservation's status inside a transaction.
func (s *Store) UpdateUploadReservationStatusTx(tx *sql.Tx, tenantID, uploadID, status string) error {
	_, err := tx.Exec(
		`UPDATE tenant_upload_reservations SET status = ? WHERE tenant_id = ? AND upload_id = ? AND status = 'active'`,
		status, tenantID, uploadID)
	return err
}

// GetUploadReservation returns a single reservation.
func (s *Store) GetUploadReservation(ctx context.Context, tenantID, uploadID string) (*UploadReservation, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "get_upload_reservation", start, &err)

	r := &UploadReservation{TenantID: tenantID, UploadID: uploadID}
	err = s.db.QueryRowContext(ctx,
		`SELECT reserved_bytes, target_path, status, expires_at, created_at, updated_at
		 FROM tenant_upload_reservations WHERE tenant_id = ? AND upload_id = ?`,
		tenantID, uploadID,
	).Scan(&r.ReservedBytes, &r.TargetPath, &r.Status, &r.ExpiresAt, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		err = ErrNotFound
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ExpireActiveReservations marks expired active reservations as aborted and
// returns the total bytes released. This is called by the expiry sweep worker.
// All changes are applied in a single transaction to prevent double-release
// if the process crashes mid-sweep.
func (s *Store) ExpireActiveReservations(ctx context.Context) (int64, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "expire_active_reservations", start, &err)

	now := time.Now().UTC()
	var totalReleased int64

	err = s.InTx(ctx, func(tx *sql.Tx) error {
		// Collect tenant-level totals so we can adjust quota counters.
		rows, err := tx.QueryContext(ctx,
			`SELECT tenant_id, SUM(reserved_bytes)
			 FROM tenant_upload_reservations
			 WHERE status = 'active' AND expires_at < ?
			 GROUP BY tenant_id`, now)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		type tenantRelease struct {
			tenantID string
			released int64
		}
		var releases []tenantRelease
		for rows.Next() {
			var tr tenantRelease
			if err := rows.Scan(&tr.tenantID, &tr.released); err != nil {
				return err
			}
			releases = append(releases, tr)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, tr := range releases {
			res, err := tx.Exec(
				`UPDATE tenant_quota_usage SET reserved_bytes = reserved_bytes + ? WHERE tenant_id = ?`,
				-tr.released, tr.tenantID)
			if err != nil {
				return fmt.Errorf("release reserved bytes for %s: %w", tr.tenantID, err)
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return fmt.Errorf("tenant_quota_usage row missing for tenant %s", tr.tenantID)
			}
			totalReleased += tr.released
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE tenant_upload_reservations SET status = 'aborted'
			 WHERE status = 'active' AND expires_at < ?`, now)
		return err
	})
	return totalReleased, err
}

// --- LLM usage operations ---

// InsertCentralLLMUsage records a billable LLM call in the central ledger.
func (s *Store) InsertCentralLLMUsage(ctx context.Context, r *LLMUsageRecord) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "insert_central_llm_usage", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_llm_usage (tenant_id, task_type, task_id, cost_millicents, raw_units, raw_unit_type)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		r.TenantID, r.TaskType, r.TaskID, r.CostMillicents, r.RawUnits, r.RawUnitType)
	return err
}

// InsertCentralLLMUsageTx records a billable LLM call in the central ledger inside a transaction.
func (s *Store) InsertCentralLLMUsageTx(tx *sql.Tx, r *LLMUsageRecord) error {
	_, err := tx.Exec(
		`INSERT INTO tenant_llm_usage (tenant_id, task_type, task_id, cost_millicents, raw_units, raw_unit_type)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		r.TenantID, r.TaskType, r.TaskID, r.CostMillicents, r.RawUnits, r.RawUnitType)
	return err
}

// IncrMonthlyLLMCost atomically increments the pre-aggregated monthly cost counter.
func (s *Store) IncrMonthlyLLMCost(ctx context.Context, tenantID string, costMC int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "incr_monthly_llm_cost", start, &err)

	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_monthly_llm_cost (tenant_id, month_start, total_mc)
		 VALUES (?, ?, ?)
		 ON DUPLICATE KEY UPDATE total_mc = total_mc + VALUES(total_mc)`,
		tenantID, monthStart, costMC)
	return err
}

// IncrMonthlyLLMCostTx atomically increments the pre-aggregated monthly cost counter inside a transaction.
func (s *Store) IncrMonthlyLLMCostTx(tx *sql.Tx, tenantID string, costMC int64) error {
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	_, err := tx.Exec(
		`INSERT INTO tenant_monthly_llm_cost (tenant_id, month_start, total_mc)
		 VALUES (?, ?, ?)
		 ON DUPLICATE KEY UPDATE total_mc = total_mc + VALUES(total_mc)`,
		tenantID, monthStart, costMC)
	return err
}

// --- Mutation log operations ---

// InsertMutationLog writes a pending mutation to the outbox.
func (s *Store) InsertMutationLog(ctx context.Context, entry *MutationLogEntry) (int64, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "insert_mutation_log", start, &err)

	res, err := s.db.ExecContext(ctx,
		`INSERT INTO quota_mutation_log (tenant_id, mutation_type, mutation_data, status)
		 VALUES (?, ?, ?, 'pending')`,
		entry.TenantID, entry.MutationType, entry.MutationData)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ListPendingMutations returns pending mutations older than the given age,
// ordered by tenant_id and id for sequential per-tenant processing.
func (s *Store) ListPendingMutations(ctx context.Context, minAge time.Duration, limit int) ([]MutationLogEntry, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "list_pending_mutations", start, &err)

	if limit <= 0 {
		limit = 100
	}
	cutoff := time.Now().UTC().Add(-minAge)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, mutation_type, mutation_data, status, retry_count, created_at, applied_at
		 FROM quota_mutation_log
		 WHERE status = 'pending' AND created_at < ?
		 ORDER BY tenant_id, id ASC
		 LIMIT ?`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var entries []MutationLogEntry
	for rows.Next() {
		var e MutationLogEntry
		if err := rows.Scan(&e.ID, &e.TenantID, &e.MutationType, &e.MutationData, &e.Status, &e.RetryCount, &e.CreatedAt, &e.AppliedAt); err != nil {
			return entries, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// MarkMutationApplied marks a mutation log entry as applied within a transaction.
// The WHERE clause includes status = 'pending' so that concurrent replay workers
// cannot both apply the same mutation: the second worker's UPDATE will affect 0 rows,
// causing the transaction to roll back and preventing double-apply of counter mutations.
func (s *Store) MarkMutationAppliedTx(tx *sql.Tx, id int64) error {
	res, err := tx.Exec(
		`UPDATE quota_mutation_log SET status = 'applied', applied_at = ? WHERE id = ? AND status = 'pending'`,
		time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("mutation %d already applied or not pending", id)
	}
	return nil
}

// IncrMutationRetry increments the retry count. If max retries exceeded, marks as failed.
func (s *Store) IncrMutationRetry(ctx context.Context, id int64, maxRetries int) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "incr_mutation_retry", start, &err)

	_, err = s.db.ExecContext(ctx,
		`UPDATE quota_mutation_log
		 SET retry_count = retry_count + 1,
		     status = CASE WHEN retry_count + 1 >= ? THEN 'failed' ELSE 'pending' END
		 WHERE id = ?`, maxRetries, id)
	return err
}

// SetQuotaCounters atomically sets the absolute quota counter values for a
// tenant. Used by the backfill CLI to bootstrap counters from tenant DBs.
func (s *Store) SetQuotaCounters(ctx context.Context, tenantID string, storageBytes, mediaFileCount int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "set_quota_counters", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_quota_usage (tenant_id, storage_bytes, reserved_bytes, media_file_count)
		 VALUES (?, ?, 0, ?)
		 ON DUPLICATE KEY UPDATE
		   storage_bytes = VALUES(storage_bytes),
		   media_file_count = VALUES(media_file_count)`,
		tenantID, storageBytes, mediaFileCount)
	return err
}

// InTx runs a function within a server DB transaction.
func (s *Store) InTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
