package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

type TenantBillingOrgBinding struct {
	TenantID                string
	TiDBCloudOrganizationID string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

func (s *Store) InsertTiDBCloudTenantReservation(ctx context.Context, t *Tenant, organizationID string, quota *QuotaConfigPatch) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_tidbcloud_tenant_reservation", start, &err)
	if t == nil {
		return fmt.Errorf("tenant is required")
	}
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return fmt.Errorf("tidbcloud organization id is required")
	}
	if quota != nil {
		if quota.MaxStorageBytes == nil || quota.MaxFileSizeBytes == nil || quota.MaxFileCount == nil || quota.TiDBCloudSpendingLimit == nil {
			return fmt.Errorf("explicit tenant quota reservation is incomplete")
		}
	}
	return withMetaLockConflictRetry(ctx, "insert_tidbcloud_tenant_reservation", func() error {
		return s.InTx(ctx, func(tx *sql.Tx) error {
			if err := insertTenantTx(ctx, tx, t); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_billing_org_bindings
				(tenant_id, tidbcloud_organization_id) VALUES (?, ?)`, t.ID, organizationID); err != nil {
				return err
			}
			if quota == nil {
				return nil
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO tenant_quota_config
				(tenant_id, max_storage_bytes, max_file_size_bytes, max_file_count, tidbcloud_spending_limit)
				VALUES (?, ?, ?, ?, ?)`, t.ID, *quota.MaxStorageBytes, *quota.MaxFileSizeBytes,
				*quota.MaxFileCount, *quota.TiDBCloudSpendingLimit)
			return err
		})
	})
}

func (s *Store) ReserveTiDBCloudFreeTenant(ctx context.Context, t *Tenant, organizationID string, maxTenants int, quota QuotaConfigPatch) error {
	if maxTenants <= 0 {
		return fmt.Errorf("free TiDB Cloud tenant limit must be positive")
	}
	if quota.MaxStorageBytes == nil || *quota.MaxStorageBytes <= 0 ||
		quota.MaxFileSizeBytes == nil || *quota.MaxFileSizeBytes <= 0 ||
		quota.MaxFileCount == nil || *quota.MaxFileCount <= 0 ||
		quota.TiDBCloudSpendingLimit == nil || *quota.TiDBCloudSpendingLimit != 0 {
		return fmt.Errorf("free TiDB Cloud tenant reservation requires explicit positive quotas and zero spending limit")
	}
	return s.WithTiDBCloudFreeQuotaLock(ctx, organizationID, func(ctx context.Context) error {
		count, err := s.CountTiDBCloudFreeTenants(ctx, organizationID)
		if err != nil {
			return err
		}
		if count >= maxTenants {
			return ErrTiDBCloudFreeTenantLimitReached
		}
		return s.InsertTiDBCloudTenantReservation(ctx, t, organizationID, &quota)
	})
}

func (s *Store) UpsertTenantBillingOrgBinding(ctx context.Context, tenantID, organizationID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_tenant_billing_org_binding", start, &err)
	tenantID = strings.TrimSpace(tenantID)
	organizationID = strings.TrimSpace(organizationID)
	if tenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if organizationID == "" {
		return fmt.Errorf("tidbcloud organization id is required")
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_billing_org_bindings
		(tenant_id, tidbcloud_organization_id) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE
			tidbcloud_organization_id = VALUES(tidbcloud_organization_id),
			updated_at = CURRENT_TIMESTAMP(3)`, tenantID, organizationID)
	return err
}

func (s *Store) GetTenantBillingOrgBinding(ctx context.Context, tenantID string) (binding *TenantBillingOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant_billing_org_binding", start, &err)
	binding = &TenantBillingOrgBinding{}
	err = s.db.QueryRowContext(ctx, `SELECT tenant_id, tidbcloud_organization_id, created_at, updated_at
		FROM tenant_billing_org_bindings WHERE tenant_id = ?`, strings.TrimSpace(tenantID)).
		Scan(&binding.TenantID, &binding.TiDBCloudOrganizationID, &binding.CreatedAt, &binding.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return binding, nil
}

func (s *Store) CountTiDBCloudFreeTenants(ctx context.Context, organizationID string) (count int, err error) {
	start := time.Now()
	defer observeMeta(ctx, "count_tidbcloud_free_tenants", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return 0, fmt.Errorf("tidbcloud organization id is required")
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM tenant_billing_org_bindings b
		JOIN tenants t ON t.id = b.tenant_id
		JOIN tenant_quota_config q ON q.tenant_id = t.id
		WHERE b.tidbcloud_organization_id = ?
			AND t.status <> ?
			AND q.tidbcloud_spending_limit = 0`, organizationID, TenantDeleted).Scan(&count)
	return count, err
}

func (s *Store) IsTiDBCloudFreeTenantCounted(ctx context.Context, tenantID, organizationID string) (counted bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "is_tidbcloud_free_tenant_counted", start, &err)
	err = s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM tenant_billing_org_bindings b
		JOIN tenants t ON t.id = b.tenant_id
		JOIN tenant_quota_config q ON q.tenant_id = t.id
		WHERE b.tenant_id = ? AND b.tidbcloud_organization_id = ?
			AND t.status <> ? AND q.tidbcloud_spending_limit = 0)`,
		strings.TrimSpace(tenantID), strings.TrimSpace(organizationID), TenantDeleted).Scan(&counted)
	return counted, err
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
				SELECT 1 FROM tenant_billing_org_bindings b WHERE b.tenant_id = t.id)
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

func ensureTenantBillingOrgBindingTx(ctx context.Context, tx *sql.Tx, tenantID, organizationID string) error {
	tenantID = strings.TrimSpace(tenantID)
	organizationID = strings.TrimSpace(organizationID)
	if tenantID == "" || organizationID == "" {
		return fmt.Errorf("tenant id and tidbcloud organization id are required")
	}
	var existing string
	err := tx.QueryRowContext(ctx, `SELECT tidbcloud_organization_id
		FROM tenant_billing_org_bindings WHERE tenant_id = ? FOR UPDATE`, tenantID).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO tenant_billing_org_bindings
			(tenant_id, tidbcloud_organization_id) VALUES (?, ?)`, tenantID, organizationID)
		return err
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(existing) != organizationID {
		return fmt.Errorf("tenant billing organization is immutable: have %q, got %q", existing, organizationID)
	}
	return nil
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

func (s *Store) WithTiDBCloudFreeQuotaLock(ctx context.Context, organizationID string, fn func(context.Context) error) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "tidbcloud_free_quota_lock", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return fmt.Errorf("tidbcloud organization id is required")
	}
	if fn == nil {
		return fmt.Errorf("tidbcloud free quota lock callback is required")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var databaseName sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&databaseName); err != nil {
		return err
	}
	lockName := tenantPoolDatabaseLockName(tidbCloudFreeQuotaLockName(organizationID), databaseName.String)
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, tidbCloudFreeQuotaLockTimeoutSeconds).Scan(&got); err != nil {
		return err
	}
	if !got.Valid {
		return fmt.Errorf("tidbcloud free quota named lock returned NULL")
	}
	if got.Int64 != 1 {
		return ErrTiDBCloudFreeQuotaBusy
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), tidbCloudFreeQuotaReleaseLockTimeout)
		defer cancel()
		var released sql.NullInt64
		releaseErr := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
		if releaseErr != nil {
			err = errors.Join(err, releaseErr)
			return
		}
		if !released.Valid {
			err = errors.Join(err, fmt.Errorf("tidbcloud free quota named lock release returned NULL"))
			return
		}
		if released.Int64 != 1 {
			err = errors.Join(err, fmt.Errorf("tidbcloud free quota named lock was not held by current connection"))
		}
	}()
	return fn(ctx)
}

func tidbCloudFreeQuotaLockName(organizationID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(organizationID)))
	return "d9_free_quota:" + hex.EncodeToString(sum[:16])
}

func backfillTenantBillingOrgBindings(ctx context.Context, db *sql.DB) error {
	const sources = `
		SELECT tenant_id, organization_id AS organization_id
		FROM tenant_tidbcloud_org_bindings
		WHERE TRIM(organization_id) <> ''
		UNION ALL
		SELECT f.tenant_id, d.org_id AS organization_id
		FROM fs_registry f
		JOIN tenant_placements p ON p.fs_id = f.fs_id
		JOIN db_pool d ON d.db_id = p.db_id
		WHERE d.org_id IS NOT NULL AND TRIM(d.org_id) <> ''
		UNION ALL
		SELECT tenant_id, tidbcloud_organization_id AS organization_id
		FROM tenant_pool_memberships
		WHERE tidbcloud_organization_id IS NOT NULL AND TRIM(tidbcloud_organization_id) <> ''`

	var conflictingTenantID, conflictingOrganizations string
	err := db.QueryRowContext(ctx, `SELECT tenant_id,
		GROUP_CONCAT(DISTINCT organization_id ORDER BY organization_id SEPARATOR ',')
		FROM (`+sources+`
		UNION ALL
		SELECT tenant_id, tidbcloud_organization_id AS organization_id
		FROM tenant_billing_org_bindings) reliable_sources
		GROUP BY tenant_id
		HAVING COUNT(DISTINCT organization_id) > 1
		ORDER BY tenant_id
		LIMIT 1`).Scan(&conflictingTenantID, &conflictingOrganizations)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check tenant billing organization backfill conflicts: %w", err)
	}
	if err == nil {
		return fmt.Errorf("tenant billing organization backfill conflict for tenant %q: %s", conflictingTenantID, conflictingOrganizations)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO tenant_billing_org_bindings
		(tenant_id, tidbcloud_organization_id)
		SELECT tenant_id, MIN(organization_id)
		FROM (`+sources+`) reliable_sources
		GROUP BY tenant_id
		ON DUPLICATE KEY UPDATE
			tidbcloud_organization_id = VALUES(tidbcloud_organization_id),
			updated_at = CURRENT_TIMESTAMP(3)`)
	if err != nil {
		return fmt.Errorf("backfill tenant billing organization bindings: %w", err)
	}
	return nil
}
