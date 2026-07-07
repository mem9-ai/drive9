package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
)

// QuotaConfig holds per-tenant quota limits stored in the central server DB.
type QuotaConfig struct {
	TenantID         string
	MaxStorageBytes  int64
	MaxFileSizeBytes int64
	MaxFileCount     int64 // 0 = unlimited
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
	FileCount      int64
	MediaFileCount int64
	UpdatedAt      time.Time
}

// QuotaConfigPatch carries externally settable quota limits. Nil fields leave
// existing values unchanged; inserted rows inherit defaults for nil fields.
type QuotaConfigPatch struct {
	MaxStorageBytes  *int64
	MaxFileSizeBytes *int64
	MaxFileCount     *int64
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
	TenantID       string
	UploadID       string
	ReservedBytes  int64
	FileCountDelta int64
	TargetPath     string
	Status         string // "active", "completed", "aborted"
	ExpiresAt      time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
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

// MutationBacklogObservation is a tenant-level view of pending quota mutation
// replay work for metrics and alerts.
type MutationBacklogObservation struct {
	TenantID                string
	PendingCount            int64
	OldestPendingAgeSeconds float64
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
		`SELECT max_storage_bytes, max_file_size_bytes, max_file_count,
		        max_media_llm_files, max_monthly_cost_mc, created_at, updated_at
		 FROM tenant_quota_config WHERE tenant_id = ?`, tenantID,
	).Scan(&cfg.MaxStorageBytes, &cfg.MaxFileSizeBytes, &cfg.MaxFileCount,
		&cfg.MaxMediaLLMFiles, &cfg.MaxMonthlyCostMC, &cfg.CreatedAt, &cfg.UpdatedAt)
	if err == sql.ErrNoRows {
		// Return defaults when no per-tenant config exists.
		cfg.MaxStorageBytes = DefaultMaxStorageBytes()
		cfg.MaxFileSizeBytes = DefaultMaxFileSizeBytes()
		cfg.MaxFileCount = 0
		cfg.MaxMediaLLMFiles = 500
		cfg.MaxMonthlyCostMC = 0
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	if cfg.MaxFileSizeBytes <= 0 {
		cfg.MaxFileSizeBytes = DefaultMaxFileSizeBytes()
	}
	return cfg, nil
}

// GetQuotaConfigVersion returns a lightweight content token for a tenant's
// explicit quota config. An empty token means no row exists and callers should
// use GetQuotaConfig's default config. The token is derived from the effective
// config values instead of updated_at so updates inside the same timestamp tick
// cannot hide a real config change from cache invalidation.
func (s *Store) GetQuotaConfigVersion(ctx context.Context, tenantID string) (string, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "get_quota_config_version", start, &err)

	var maxStorageBytes, maxFileSizeBytes, maxFileCount, maxMediaLLMFiles, maxMonthlyCostMC int64
	err = s.db.QueryRowContext(ctx,
		`SELECT max_storage_bytes, max_file_size_bytes, max_file_count,
		        max_media_llm_files, max_monthly_cost_mc
		 FROM tenant_quota_config WHERE tenant_id = ?`, tenantID,
	).Scan(&maxStorageBytes, &maxFileSizeBytes, &maxFileCount, &maxMediaLLMFiles, &maxMonthlyCostMC)
	if err == sql.ErrNoRows {
		logger.Info(ctx, "quota_config_not_found_using_defaults",
			zap.String("tenant_id", tenantID))
		err = nil
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get quota config version for tenant %q: %w", tenantID, err)
	}
	return fmt.Sprintf("v2:%d:%d:%d:%d:%d", maxStorageBytes, maxFileSizeBytes, maxFileCount, maxMediaLLMFiles, maxMonthlyCostMC), nil
}

// SetQuotaConfig upserts per-tenant quota configuration.
func (s *Store) SetQuotaConfig(ctx context.Context, cfg *QuotaConfig) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "set_quota_config", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_quota_config (tenant_id, max_storage_bytes, max_file_size_bytes, max_file_count,
		                                  max_media_llm_files, max_monthly_cost_mc)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   max_storage_bytes = VALUES(max_storage_bytes),
		   max_file_size_bytes = VALUES(max_file_size_bytes),
		   max_file_count = VALUES(max_file_count),
		   max_media_llm_files = VALUES(max_media_llm_files),
		   max_monthly_cost_mc = VALUES(max_monthly_cost_mc)`,
		cfg.TenantID, cfg.MaxStorageBytes, cfg.MaxFileSizeBytes, cfg.MaxFileCount,
		cfg.MaxMediaLLMFiles, cfg.MaxMonthlyCostMC)
	return err
}

// SetQuotaStorageBytes atomically updates the storage quota, preserving internal
// media/monthly limits when a config row already exists. New rows inherit the
// database defaults for fields that are not externally writable.
func (s *Store) SetQuotaStorageBytes(ctx context.Context, tenantID string, maxStorageBytes int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "set_quota_storage_bytes", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_quota_config (tenant_id, max_storage_bytes)
		 VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE
		   max_storage_bytes = VALUES(max_storage_bytes)`,
		tenantID, maxStorageBytes)
	if err != nil {
		return fmt.Errorf("set quota storage bytes for tenant %q: %w", tenantID, err)
	}
	return nil
}

// SetQuotaConfigPatch updates externally writable quota limits while preserving
// internal media/monthly limits and any omitted external limits.
func (s *Store) SetQuotaConfigPatch(ctx context.Context, tenantID string, patch QuotaConfigPatch) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "set_quota_config_patch", start, &err)

	insertStorage := DefaultMaxStorageBytes()
	if patch.MaxStorageBytes != nil {
		insertStorage = *patch.MaxStorageBytes
	}
	insertFileSize := int64(0)
	if patch.MaxFileSizeBytes != nil {
		insertFileSize = *patch.MaxFileSizeBytes
	}
	insertFileCount := int64(0)
	if patch.MaxFileCount != nil {
		insertFileCount = *patch.MaxFileCount
	}
	updateStorage := sql.NullInt64{}
	if patch.MaxStorageBytes != nil {
		updateStorage = sql.NullInt64{Int64: *patch.MaxStorageBytes, Valid: true}
	}
	updateFileSize := sql.NullInt64{}
	if patch.MaxFileSizeBytes != nil {
		updateFileSize = sql.NullInt64{Int64: *patch.MaxFileSizeBytes, Valid: true}
	}
	updateFileCount := sql.NullInt64{}
	if patch.MaxFileCount != nil {
		updateFileCount = sql.NullInt64{Int64: *patch.MaxFileCount, Valid: true}
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_quota_config (tenant_id, max_storage_bytes, max_file_size_bytes, max_file_count)
		 VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   max_storage_bytes = COALESCE(?, max_storage_bytes),
		   max_file_size_bytes = COALESCE(?, max_file_size_bytes),
		   max_file_count = COALESCE(?, max_file_count)`,
		tenantID, insertStorage, insertFileSize, insertFileCount,
		updateStorage, updateFileSize, updateFileCount)
	if err != nil {
		return fmt.Errorf("set quota config patch for tenant %q: %w", tenantID, err)
	}
	return nil
}

func (s *Store) CopyQuotaConfig(ctx context.Context, sourceTenantID, destTenantID string) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "copy_quota_config", start, &err)

	cfg, err := s.GetQuotaConfig(ctx, sourceTenantID)
	if err != nil {
		return err
	}
	cfg.TenantID = destTenantID
	return s.SetQuotaConfig(ctx, cfg)
}

// --- QuotaUsage operations ---

// GetQuotaUsage returns the pre-aggregated quota counters for a tenant.
func (s *Store) GetQuotaUsage(ctx context.Context, tenantID string) (*QuotaUsage, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "get_quota_usage", start, &err)

	u := &QuotaUsage{TenantID: tenantID}
	err = s.db.QueryRowContext(ctx,
		`SELECT storage_bytes, reserved_bytes, file_count, media_file_count, updated_at
		 FROM tenant_quota_usage WHERE tenant_id = ?`, tenantID,
	).Scan(&u.StorageBytes, &u.ReservedBytes, &u.FileCount, &u.MediaFileCount, &u.UpdatedAt)
	if err == sql.ErrNoRows {
		return u, nil // zero counters
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

// IncrFileCount atomically adjusts the file_count counter.
func (s *Store) IncrFileCount(ctx context.Context, tenantID string, delta int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "incr_file_count", start, &err)

	res, err := s.db.ExecContext(ctx,
		`UPDATE tenant_quota_usage SET file_count = file_count + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
}

// IncrFileCountTx atomically adjusts the file_count counter inside a transaction.
func (s *Store) IncrFileCountTx(tx *sql.Tx, tenantID string, delta int64) error {
	res, err := tx.Exec(
		`UPDATE tenant_quota_usage SET file_count = file_count + ? WHERE tenant_id = ?`,
		delta, tenantID)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res, tenantID)
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
var defaultMaxStorageBytes atomic.Int64
var defaultMaxFileSizeBytes atomic.Int64

func init() {
	defaultMaxStorageBytes.Store(int64(50 * (1 << 30)))  // 50 GiB
	defaultMaxFileSizeBytes.Store(int64(10 * (1 << 30))) // 10 GiB
}

// SetDefaultMaxStorageBytes overrides the per-tenant fallback storage quota.
func SetDefaultMaxStorageBytes(bytes int64) {
	if bytes > 0 {
		defaultMaxStorageBytes.Store(bytes)
	}
}

// DefaultMaxStorageBytes returns the configured per-tenant fallback storage quota.
func DefaultMaxStorageBytes() int64 { return defaultMaxStorageBytes.Load() }

// SetDefaultMaxFileSizeBytes overrides the per-tenant fallback file size quota.
func SetDefaultMaxFileSizeBytes(bytes int64) {
	if bytes > 0 {
		defaultMaxFileSizeBytes.Store(bytes)
	}
}

// DefaultMaxFileSizeBytes returns the configured per-tenant fallback file size quota.
func DefaultMaxFileSizeBytes() int64 { return defaultMaxFileSizeBytes.Load() }

// AtomicReserveAndInsertUpload claims reserved_bytes and inserts the reservation
// tracking row inside a single server DB transaction. This is the correct API
// for the upload-initiate path: either both rows are written, or neither is.
//
// Returns:
//   - ErrStorageQuotaExceeded when the projected storage total exceeds the
//     per-tenant limit (or the default when no config row exists). Transaction
//     rolls back so reserved_bytes and the reservation table are both untouched.
//   - ErrFileCountQuotaExceeded when the projected file count exceeds the
//     per-tenant limit. Transaction rolls back.
//   - ErrReservationAlreadyExists when a row with the same (tenant_id, upload_id)
//     already exists (primary-key collision). Idempotent-retry callers should
//     detect this sentinel and NOT bump reserved_bytes a second time.
//   - Any other DB error causes the transaction to roll back.
func (s *Store) AtomicReserveAndInsertUpload(ctx context.Context, r *UploadReservation) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "atomic_reserve_and_insert_upload", start, &err)

	err = s.InTx(ctx, func(tx *sql.Tx) error {
		if _, execErr := tx.ExecContext(ctx,
			`INSERT IGNORE INTO tenant_quota_usage (tenant_id) VALUES (?)`,
			r.TenantID); execErr != nil {
			return execErr
		}
		res, execErr := tx.ExecContext(ctx,
			`UPDATE tenant_quota_usage
			 SET reserved_bytes = reserved_bytes + ?,
			     file_count = file_count + ?
			 WHERE tenant_id = ?
			   AND storage_bytes + reserved_bytes + ? <=
			       COALESCE((SELECT max_storage_bytes FROM tenant_quota_config WHERE tenant_id = ?), ?)
			   AND (? <= 0 OR
			        COALESCE((SELECT max_file_count FROM tenant_quota_config WHERE tenant_id = ?), 0) <= 0 OR
			        file_count + ? <= COALESCE((SELECT max_file_count FROM tenant_quota_config WHERE tenant_id = ?), 0))`,
			r.ReservedBytes, r.FileCountDelta, r.TenantID, r.ReservedBytes, r.TenantID, DefaultMaxStorageBytes(),
			r.FileCountDelta, r.TenantID, r.FileCountDelta, r.TenantID)
		if execErr != nil {
			return execErr
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			if quotaErr := s.uploadReservationQuotaErrorTx(ctx, tx, r); quotaErr != nil {
				return quotaErr
			}
			return ErrStorageQuotaExceeded
		}
		if _, execErr := tx.ExecContext(ctx,
			`INSERT INTO tenant_upload_reservations
			   (tenant_id, upload_id, reserved_bytes, file_count_delta, target_path, status, expires_at)
			 VALUES (?, ?, ?, ?, ?, 'active', ?)`,
			r.TenantID, r.UploadID, r.ReservedBytes, r.FileCountDelta, r.TargetPath, r.ExpiresAt); execErr != nil {
			if isDuplicateEntry(execErr) {
				return ErrReservationAlreadyExists
			}
			return execErr
		}
		return nil
	})
	return err
}

func (s *Store) uploadReservationQuotaErrorTx(ctx context.Context, tx *sql.Tx, r *UploadReservation) error {
	var storageBytes, reservedBytes, fileCount int64
	if err := tx.QueryRowContext(ctx,
		`SELECT storage_bytes, reserved_bytes, file_count
		 FROM tenant_quota_usage WHERE tenant_id = ?`, r.TenantID,
	).Scan(&storageBytes, &reservedBytes, &fileCount); err != nil {
		return nil
	}
	var maxStorageBytes, maxFileCount sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT max_storage_bytes, max_file_count
		 FROM tenant_quota_config WHERE tenant_id = ?`, r.TenantID,
	).Scan(&maxStorageBytes, &maxFileCount); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	storageLimit := DefaultMaxStorageBytes()
	if maxStorageBytes.Valid {
		storageLimit = maxStorageBytes.Int64
	}
	if storageLimit > 0 && storageBytes+reservedBytes+r.ReservedBytes > storageLimit {
		return ErrStorageQuotaExceeded
	}
	if r.FileCountDelta > 0 && maxFileCount.Valid && maxFileCount.Int64 > 0 && fileCount+r.FileCountDelta > maxFileCount.Int64 {
		return ErrFileCountQuotaExceeded
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

	return scanFileMeta(s.db.QueryRowContext(ctx,
		`SELECT size_bytes, is_media, created_at, updated_at
		 FROM tenant_file_meta WHERE tenant_id = ? AND file_id = ?`,
		tenantID, fileID,
	), tenantID, fileID)
}

// GetFileMetaForUpdateTx returns quota-relevant file metadata and locks the row
// for the caller's central quota transaction.
func (s *Store) GetFileMetaForUpdateTx(tx *sql.Tx, tenantID, fileID string) (*FileMeta, error) {
	return scanFileMeta(tx.QueryRow(
		`SELECT size_bytes, is_media, created_at, updated_at
		 FROM tenant_file_meta WHERE tenant_id = ? AND file_id = ? FOR UPDATE`,
		tenantID, fileID,
	), tenantID, fileID)
}

func scanFileMeta(row interface {
	Scan(dest ...any) error
}, tenantID, fileID string) (*FileMeta, error) {
	fm := &FileMeta{TenantID: tenantID, FileID: fileID}
	var isMedia int
	err := row.Scan(&fm.SizeBytes, &isMedia, &fm.CreatedAt, &fm.UpdatedAt)
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

// DeleteFileMetaIfExistsTx removes a file's metadata inside a transaction and
// reports whether a row existed. Callers use the boolean to make delete-side
// quota counter updates idempotent across task retries.
func (s *Store) DeleteFileMetaIfExistsTx(tx *sql.Tx, tenantID, fileID string) (bool, error) {
	res, err := tx.Exec(
		`DELETE FROM tenant_file_meta WHERE tenant_id = ? AND file_id = ?`,
		tenantID, fileID)
	if err != nil {
		return false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected > 0, nil
}

// --- Upload reservation operations ---

// InsertUploadReservation creates a new active upload reservation.
func (s *Store) InsertUploadReservation(ctx context.Context, r *UploadReservation) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "insert_upload_reservation", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_upload_reservations (tenant_id, upload_id, reserved_bytes, file_count_delta, target_path, status, expires_at)
		 VALUES (?, ?, ?, ?, ?, 'active', ?)`,
		r.TenantID, r.UploadID, r.ReservedBytes, r.FileCountDelta, r.TargetPath, r.ExpiresAt)
	return err
}

// UpdateUploadReservationStatus updates a reservation's status.
func (s *Store) UpdateUploadReservationStatus(ctx context.Context, tenantID, uploadID, status string) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "update_upload_reservation_status", start, &err)

	_, err = s.db.ExecContext(ctx,
		`UPDATE tenant_upload_reservations SET status = ? WHERE tenant_id = ? AND upload_id = ? AND status IN ('active', 'completing')`,
		status, tenantID, uploadID)
	return err
}

// UpdateUploadReservationStatusTx updates a reservation's status inside a transaction.
func (s *Store) UpdateUploadReservationStatusTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID, status string) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE tenant_upload_reservations SET status = ? WHERE tenant_id = ? AND upload_id = ? AND status IN ('active', 'completing')`,
		status, tenantID, uploadID)
	return err
}

// AbortActiveReservationTx atomically claims an active/completing reservation
// for abort and reports the counters that must be released by the caller in the
// same transaction.
func (s *Store) AbortActiveReservationTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID string) (aborted bool, reservedBytes, fileCountDelta int64, err error) {
	var bytes, files sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT reserved_bytes, file_count_delta FROM tenant_upload_reservations
		 WHERE tenant_id = ? AND upload_id = ? AND status IN ('active', 'completing') FOR UPDATE`,
		tenantID, uploadID).Scan(&bytes, &files); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, 0, 0, nil
		}
		return false, 0, 0, err
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE tenant_upload_reservations SET status = 'aborted'
		 WHERE tenant_id = ? AND upload_id = ? AND status IN ('active', 'completing')`,
		tenantID, uploadID)
	if err != nil {
		return false, 0, 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, 0, 0, nil
	}
	return true, bytes.Int64, files.Int64, nil
}

// SettleActiveReservationTx transitions a reservation from 'active' to the
// given terminal status inside a transaction, reporting whether a row was
// actually updated. Callers use this to decide — inside the same tx — whether
// to apply reserved_bytes-related counter changes.
//
// settled=true means an 'active' row was atomically flipped to status; the
// caller should do the reserved→storage transfer. settled=false signals "do
// not touch reserved_bytes" and covers three indistinguishable sub-cases:
//   - fail-open initiate: no reservation row was ever written because the
//     server DB was down at initiate time.
//   - already-settled terminal row: a concurrent replay or abort flipped
//     the row before this tx ran.
//   - apply-time expiry sweep race: the expiry sweep released reserved_bytes
//     and marked the row aborted between initiate and apply.
//
// This is the atomic replacement for a separate GetUploadReservation lookup
// outside the transaction, which has two failure modes we care about:
//   - Transient error: we cannot tell whether a reservation exists, so we
//     would otherwise have to bail out and silently drop the paired mutation.
//   - TOCTOU: between lookup and apply another worker could settle the row.
func (s *Store) SettleActiveReservationTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID, status string) (settled bool, fileCountDelta int64, err error) {
	var delta sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT file_count_delta FROM tenant_upload_reservations
		 WHERE tenant_id = ? AND upload_id = ? AND status IN ('active', 'completing') FOR UPDATE`,
		tenantID, uploadID).Scan(&delta); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, 0, nil
		}
		return false, 0, err
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE tenant_upload_reservations SET status = ? WHERE tenant_id = ? AND upload_id = ? AND status IN ('active', 'completing')`,
		status, tenantID, uploadID)
	if err != nil {
		return false, 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, 0, nil
	}
	return true, delta.Int64, nil
}

// GetUploadReservation returns a single reservation.
func (s *Store) GetUploadReservation(ctx context.Context, tenantID, uploadID string) (*UploadReservation, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "get_upload_reservation", start, &err)

	r := &UploadReservation{TenantID: tenantID, UploadID: uploadID}
	err = s.db.QueryRowContext(ctx,
		`SELECT reserved_bytes, file_count_delta, target_path, status, expires_at, created_at, updated_at
		 FROM tenant_upload_reservations WHERE tenant_id = ? AND upload_id = ?`,
		tenantID, uploadID,
	).Scan(&r.ReservedBytes, &r.FileCountDelta, &r.TargetPath, &r.Status, &r.ExpiresAt, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		err = ErrNotFound
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *Store) HasActiveUploadReservations(ctx context.Context, tenantID string) (bool, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "has_active_upload_reservations", start, &err)

	var one int
	err = s.db.QueryRowContext(ctx,
		`SELECT 1
		 FROM tenant_upload_reservations
		 WHERE tenant_id = ? AND status IN ('active', 'completing') AND expires_at > ?
		 LIMIT 1`, tenantID, time.Now().UTC()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) AbortActiveUploadReservations(ctx context.Context, tenantID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "abort_active_upload_reservations", start, &err)

	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var totalBytes, totalFiles sql.NullInt64
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(SUM(reserved_bytes), 0), COALESCE(SUM(file_count_delta), 0)
			 FROM tenant_upload_reservations
			 WHERE tenant_id = ? AND status IN ('active', 'completing')`, tenantID).Scan(&totalBytes, &totalFiles); err != nil {
			return err
		}
		if totalBytes.Int64 != 0 || totalFiles.Int64 != 0 {
			res, err := tx.ExecContext(ctx,
				`UPDATE tenant_quota_usage
				 SET reserved_bytes = GREATEST(reserved_bytes - ?, 0),
				     file_count = GREATEST(file_count - ?, 0)
				 WHERE tenant_id = ?`, totalBytes.Int64, totalFiles.Int64, tenantID)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return fmt.Errorf("tenant_quota_usage row missing for tenant %s", tenantID)
			}
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE tenant_upload_reservations SET status = 'aborted'
			 WHERE tenant_id = ? AND status IN ('active', 'completing')`, tenantID)
		return err
	})
	return err
}

// ExpireActiveReservations marks expired active/completing reservations as
// aborted and returns the total bytes released. This is called by the expiry
// sweep worker. All changes are applied in a single transaction to prevent
// double-release if the process crashes mid-sweep. Completing reservations are
// included so a crash after mark-completing but before finalize settle does not
// leak reserved_bytes forever.
func (s *Store) ExpireActiveReservations(ctx context.Context) (int64, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "expire_active_reservations", start, &err)

	now := time.Now().UTC()
	var totalReleased int64

	err = s.InTx(ctx, func(tx *sql.Tx) error {
		// Collect tenant-level totals so we can adjust quota counters.
		rows, err := tx.QueryContext(ctx,
			`SELECT tenant_id, SUM(reserved_bytes), SUM(file_count_delta)
			 FROM tenant_upload_reservations
			 WHERE status IN ('active', 'completing') AND expires_at < ?
			 GROUP BY tenant_id`, now)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		type tenantRelease struct {
			tenantID      string
			releasedBytes int64
			releasedFiles int64
		}
		var releases []tenantRelease
		for rows.Next() {
			var tr tenantRelease
			if err := rows.Scan(&tr.tenantID, &tr.releasedBytes, &tr.releasedFiles); err != nil {
				return err
			}
			releases = append(releases, tr)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, tr := range releases {
			res, err := tx.Exec(
				`UPDATE tenant_quota_usage
				 SET reserved_bytes = reserved_bytes + ?,
				     file_count = GREATEST(file_count - ?, 0)
				 WHERE tenant_id = ?`,
				-tr.releasedBytes, tr.releasedFiles, tr.tenantID)
			if err != nil {
				return fmt.Errorf("release reserved bytes for %s: %w", tr.tenantID, err)
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return fmt.Errorf("tenant_quota_usage row missing for tenant %s", tr.tenantID)
			}
			totalReleased += tr.releasedBytes
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE tenant_upload_reservations SET status = 'aborted'
			 WHERE status IN ('active', 'completing') AND expires_at < ?`, now)
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

// ObservePendingMutations returns per-tenant pending mutation backlog and age.
func (s *Store) ObservePendingMutations(ctx context.Context) ([]MutationBacklogObservation, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "observe_pending_mutations", start, &err)

	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id, COUNT(*), MIN(created_at)
		 FROM quota_mutation_log FORCE INDEX (idx_pending_tenant_age)
		 WHERE status = 'pending'
		 GROUP BY tenant_id
		 ORDER BY tenant_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	now := time.Now().UTC()
	var out []MutationBacklogObservation
	for rows.Next() {
		var obs MutationBacklogObservation
		var oldest time.Time
		if err = rows.Scan(&obs.TenantID, &obs.PendingCount, &oldest); err != nil {
			return out, err
		}
		age := now.Sub(oldest.UTC()).Seconds()
		if age < 0 {
			age = 0
		}
		obs.OldestPendingAgeSeconds = age
		out = append(out, obs)
	}
	err = rows.Err()
	return out, err
}

// HasPendingFileMutation reports whether an unapplied create/overwrite
// mutation still exists for fileID. GC uses this to avoid acking central cleanup
// before fail-open quota mutations have converged.
func (s *Store) HasPendingFileMutation(ctx context.Context, tenantID, fileID string) (bool, error) {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "has_pending_file_mutation", start, &err)

	var one int
	err = s.db.QueryRowContext(ctx,
		`SELECT 1
		 FROM quota_mutation_log
		 WHERE tenant_id = ?
		   AND status = 'pending'
		   AND mutation_type IN ('file_create', 'file_overwrite')
		   AND JSON_UNQUOTE(JSON_EXTRACT(mutation_data, '$.file_id')) = ?
		 LIMIT 1`, tenantID, fileID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// IncrMutationRetry increments the retry count for a pending mutation.
// The WHERE status='pending' guard mirrors MarkMutationAppliedTx and prevents
// any caller (test fakes, backfill CLI, future refactors) from silently flipping
// applied/failed terminal rows back to pending or bumping retry_count on a row
// that has already been settled. If max retries is reached, transitions to
// the terminal 'failed' status. Calls against rows not in 'pending' state
// are no-ops.
func (s *Store) IncrMutationRetry(ctx context.Context, id int64, maxRetries int) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "incr_mutation_retry", start, &err)

	_, err = s.db.ExecContext(ctx,
		`UPDATE quota_mutation_log
		 SET retry_count = retry_count + 1,
		     status = CASE WHEN retry_count + 1 >= ? THEN 'failed' ELSE 'pending' END
		 WHERE id = ? AND status = 'pending'`, maxRetries, id)
	return err
}

// SetQuotaCounters atomically sets the absolute quota counter values for a
// tenant. Used by the backfill CLI to bootstrap counters from tenant DBs.
func (s *Store) SetQuotaCounters(ctx context.Context, tenantID string, storageBytes, mediaFileCount, fileCount int64) error {
	start := time.Now()
	var err error
	defer observeMeta(ctx, "set_quota_counters", start, &err)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_quota_usage (tenant_id, storage_bytes, reserved_bytes, file_count, media_file_count)
		 VALUES (?, ?, 0, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   storage_bytes = VALUES(storage_bytes),
		   file_count = VALUES(file_count),
		   media_file_count = VALUES(media_file_count)`,
		tenantID, storageBytes, fileCount, mediaFileCount)
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
