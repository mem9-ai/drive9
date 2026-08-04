package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

func (s *Store) InsertTiDBCloudFreeTenantReservation(ctx context.Context, t *Tenant, quota QuotaConfigPatch) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_tidbcloud_free_tenant_reservation", start, &err)
	if t == nil {
		return fmt.Errorf("tenant is required")
	}
	if quota.MaxStorageBytes == nil || *quota.MaxStorageBytes <= 0 ||
		quota.MaxFileSizeBytes == nil || *quota.MaxFileSizeBytes <= 0 ||
		quota.MaxFileCount == nil || *quota.MaxFileCount <= 0 ||
		quota.TiDBCloudSpendingLimit == nil || *quota.TiDBCloudSpendingLimit != 0 {
		return fmt.Errorf("explicit free tenant quota reservation is incomplete")
	}
	return withMetaLockConflictRetry(ctx, "insert_tidbcloud_free_tenant_reservation", func() error {
		return s.InTx(ctx, func(tx *sql.Tx) error {
			if err := insertTenantTx(ctx, tx, t); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO tenant_quota_config
				(tenant_id, max_storage_bytes, max_file_size_bytes, max_file_count, tidbcloud_spending_limit)
				VALUES (?, ?, ?, ?, ?)`, t.ID, *quota.MaxStorageBytes, *quota.MaxFileSizeBytes,
				*quota.MaxFileCount, *quota.TiDBCloudSpendingLimit)
			return err
		})
	})
}

func (s *Store) CountTiDBCloudFreeTenants(ctx context.Context, organizationID string) (count int, err error) {
	start := time.Now()
	defer observeMeta(ctx, "count_tidbcloud_free_tenants", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return 0, fmt.Errorf("tidbcloud organization id is required")
	}
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(free_count), 0)
		FROM (
			SELECT COUNT(*) AS free_count
			FROM tenant_tidbcloud_org_bindings b
			STRAIGHT_JOIN tenants t ON t.id = b.tenant_id
			STRAIGHT_JOIN tenant_quota_config q ON q.tenant_id = t.id
			WHERE b.organization_id = ?
				AND b.pool_status <> ?
				AND t.provider = ?
				AND t.status <> ?
				AND q.tidbcloud_spending_limit = 0
			UNION ALL
			SELECT COUNT(*) AS free_count
			FROM db_pool d
			STRAIGHT_JOIN tenant_placements p ON p.db_id = d.db_id
			STRAIGHT_JOIN fs_registry f ON f.fs_id = p.fs_id
			STRAIGHT_JOIN tenants t ON t.id = f.tenant_id
			STRAIGHT_JOIN tenant_quota_config q ON q.tenant_id = t.id
			LEFT JOIN tenant_pool_memberships m ON m.tenant_id = t.id
			WHERE d.org_id = ?
				AND t.provider = ?
				AND t.status <> ?
				AND q.tidbcloud_spending_limit = 0
				AND (m.tenant_id IS NULL OR m.pool_status <> ?)
		) free_tenants`,
		organizationID, TenantPoolBindingFree, tidbCloudNativeProvider, TenantDeleted,
		organizationID, tidbCloudNativeSharedProvider, TenantDeleted, TenantPoolBindingFree).Scan(&count)
	return count, err
}

// DeleteTiDBCloudFreeReservation releases a pending or failed free reservation
// only when it has no provisioned resource or pool ownership metadata.
func (s *Store) DeleteTiDBCloudFreeReservation(ctx context.Context, tenantID string) (deleted bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_tidbcloud_free_reservation", start, &err)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return false, fmt.Errorf("tenant id is required")
	}
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE tenants t
		SET t.status = ?, t.updated_at = ?
		WHERE t.id = ? AND t.status IN (?, ?)
			AND COALESCE(t.storage_namespace_id, '') = ''
			AND COALESCE(t.cluster_id, '') = ''
			AND EXISTS (
				SELECT 1 FROM tenant_quota_config q
				WHERE q.tenant_id = t.id AND q.tidbcloud_spending_limit = 0)
			AND NOT EXISTS (
				SELECT 1 FROM tenant_api_keys k WHERE k.tenant_id = t.id)
			AND NOT EXISTS (
				SELECT 1 FROM tenant_tidbcloud_org_bindings b WHERE b.tenant_id = t.id)
			AND NOT EXISTS (
				SELECT 1 FROM tenant_pool_memberships m WHERE m.tenant_id = t.id)
			AND NOT EXISTS (
				SELECT 1 FROM fs_registry f
				JOIN tenant_placements p ON p.fs_id = f.fs_id
				WHERE f.tenant_id = t.id)`,
		TenantDeleted, now, tenantID, TenantPending, TenantFailed)
	if err != nil {
		return false, fmt.Errorf("delete tidbcloud free reservation %s: %w", tenantID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete tidbcloud free reservation rows affected %s: %w", tenantID, err)
	}
	return affected == 1, nil
}

// DeleteStaleTiDBCloudFreeReservation atomically deletes a stale pending row
// only when it still contains reservation metadata and no provisioned resource
// or pool-ownership metadata.
func (s *Store) DeleteStaleTiDBCloudFreeReservation(ctx context.Context, tenantID string, updatedBefore time.Time) (deleted bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_stale_tidbcloud_free_reservation", start, &err)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return false, fmt.Errorf("tenant id is required")
	}
	if updatedBefore.IsZero() {
		return false, fmt.Errorf("reservation stale cutoff is required")
	}
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE tenants t
		SET t.status = ?, t.updated_at = ?
		WHERE t.id = ? AND t.status = ? AND t.updated_at <= ?
			AND COALESCE(t.storage_namespace_id, '') = ''
			AND COALESCE(t.cluster_id, '') = ''
			AND EXISTS (
				SELECT 1 FROM tenant_quota_config q
				WHERE q.tenant_id = t.id AND q.tidbcloud_spending_limit = 0)
			AND NOT EXISTS (
				SELECT 1 FROM tenant_api_keys k WHERE k.tenant_id = t.id)
			AND NOT EXISTS (
				SELECT 1 FROM tenant_tidbcloud_org_bindings b WHERE b.tenant_id = t.id)
			AND NOT EXISTS (
				SELECT 1 FROM tenant_pool_memberships m WHERE m.tenant_id = t.id)
			AND NOT EXISTS (
				SELECT 1 FROM fs_registry f
				JOIN tenant_placements p ON p.fs_id = f.fs_id
				WHERE f.tenant_id = t.id)`,
		TenantDeleted, now, tenantID, TenantPending, updatedBefore.UTC())
	if err != nil {
		return false, fmt.Errorf("delete stale tidbcloud free reservation %s: %w", tenantID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete stale tidbcloud free reservation rows affected %s: %w", tenantID, err)
	}
	return affected == 1, nil
}

// HasTenantPoolOwnership reports whether a tenant is explicitly associated
// with a native pool binding, logical pool membership, or shared DB placement.
func (s *Store) HasTenantPoolOwnership(ctx context.Context, tenantID string) (owned bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "has_tenant_pool_ownership", start, &err)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return false, fmt.Errorf("tenant id is required")
	}
	err = s.db.QueryRowContext(ctx, `SELECT
		EXISTS (
			SELECT 1 FROM tenant_tidbcloud_org_bindings b
			WHERE b.tenant_id = ? AND TRIM(b.pool_id) <> '')
		OR EXISTS (
			SELECT 1 FROM tenant_pool_memberships m WHERE m.tenant_id = ?)
		OR EXISTS (
			SELECT 1 FROM fs_registry f
			JOIN tenant_placements p ON p.fs_id = f.fs_id
			WHERE f.tenant_id = ?)`, tenantID, tenantID, tenantID).Scan(&owned)
	if err != nil {
		return false, fmt.Errorf("check tenant pool ownership %s: %w", tenantID, err)
	}
	return owned, nil
}

func upsertTenantQuotaPatchTx(ctx context.Context, tx *sql.Tx, tenantID string, patch QuotaConfigPatch) error {
	insertStorage := DefaultMaxStorageBytes()
	if patch.MaxStorageBytes != nil {
		insertStorage = *patch.MaxStorageBytes
	}
	insertFileSize := DefaultMaxFileSizeBytes()
	if patch.MaxFileSizeBytes != nil {
		insertFileSize = *patch.MaxFileSizeBytes
	}
	insertFileCount := DefaultMaxFileCount()
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
	_, err := tx.ExecContext(ctx, `INSERT INTO tenant_quota_config
		(tenant_id, max_storage_bytes, max_file_size_bytes, max_file_count,
		 tidbcloud_spending_limit, tidbcloud_spending_limit_checked_at,
		 max_media_llm_files, max_video_llm_files)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 max_storage_bytes = COALESCE(?, max_storage_bytes),
		 max_file_size_bytes = COALESCE(?, max_file_size_bytes),
		 max_file_count = COALESCE(?, max_file_count),
		 tidbcloud_spending_limit = COALESCE(?, tidbcloud_spending_limit),
		 tidbcloud_spending_limit_checked_at = COALESCE(?, tidbcloud_spending_limit_checked_at)`,
		tenantID, insertStorage, insertFileSize, insertFileCount,
		nullInt64FromPtr(patch.TiDBCloudSpendingLimit), nullTimeFromPtr(patch.TiDBCloudSpendingLimitCheckedAt),
		DefaultMaxMediaLLMFiles(), DefaultMaxVideoLLMFiles(),
		updateStorage, updateFileSize, updateFileCount,
		nullInt64FromPtr(patch.TiDBCloudSpendingLimit), nullTimeFromPtr(patch.TiDBCloudSpendingLimitCheckedAt))
	return err
}

func (s *Store) AcquireTiDBCloudFreeQuotaLock(ctx context.Context, organizationID string) (release func() error, err error) {
	start := time.Now()
	defer observeMeta(ctx, "acquire_tidbcloud_free_quota_lock", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return nil, fmt.Errorf("tidbcloud organization id is required")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close()
		}
	}()
	var databaseName sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&databaseName); err != nil {
		return nil, err
	}
	lockName := tenantPoolDatabaseLockName(tidbCloudFreeQuotaLockName(organizationID), databaseName.String)
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, tidbCloudFreeQuotaLockTimeoutSeconds).Scan(&got); err != nil {
		return nil, err
	}
	if !got.Valid {
		return nil, fmt.Errorf("tidbcloud free quota named lock returned NULL")
	}
	if got.Int64 != 1 {
		return nil, ErrTiDBCloudFreeQuotaBusy
	}
	closeOnError = false
	var once sync.Once
	var releaseErr error
	return func() error {
		once.Do(func() {
			defer func() {
				if closeErr := conn.Close(); closeErr != nil {
					releaseErr = errors.Join(releaseErr, closeErr)
				}
			}()
			releaseCtx, cancel := context.WithTimeout(context.Background(), tidbCloudFreeQuotaReleaseLockTimeout)
			defer cancel()
			var released sql.NullInt64
			if err := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released); err != nil {
				releaseErr = err
				return
			}
			if !released.Valid {
				releaseErr = fmt.Errorf("tidbcloud free quota named lock release returned NULL")
				return
			}
			if released.Int64 != 1 {
				releaseErr = fmt.Errorf("tidbcloud free quota named lock was not held by current connection")
			}
		})
		return releaseErr
	}, nil
}

func (s *Store) WithTiDBCloudFreeQuotaLock(ctx context.Context, organizationID string, fn func(context.Context) error) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "tidbcloud_free_quota_lock", start, &err)
	if fn == nil {
		return fmt.Errorf("tidbcloud free quota lock callback is required")
	}
	release, err := s.AcquireTiDBCloudFreeQuotaLock(ctx, organizationID)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, release())
	}()
	return fn(ctx)
}

func tidbCloudFreeQuotaLockName(organizationID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(organizationID)))
	return "d9_free_quota:" + hex.EncodeToString(sum[:16])
}
