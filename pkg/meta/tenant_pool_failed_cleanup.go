package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ListFailedNativeTenantCleanupCandidates lists failed native tenants owned by
// an organization, excluding bindings already claimed from a pool.
func (s *Store) ListFailedNativeTenantCleanupCandidates(ctx context.Context, organizationID string, updatedBefore time.Time, limit int) (out []TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_failed_native_tenant_cleanup_candidates", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return nil, fmt.Errorf("organization_id is required")
	}
	if limit <= 0 {
		limit = 1
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
			t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
			t.db_host, t.db_port, t.db_user, t.db_password, t.db_name,
			t.db_tls, t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
			t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at,
			b.tenant_id, b.organization_id, b.cluster_id, b.branch_id, b.pool_id, b.pool_status, b.used_at, b.created_at, b.updated_at
		FROM tenant_tidbcloud_org_bindings b
		JOIN tenants t ON t.id = b.tenant_id
		WHERE b.organization_id = ?
			AND (b.pool_status = ? OR b.pool_id = '')
			AND t.provider = ? AND t.status = ? AND t.updated_at <= ?
		ORDER BY t.updated_at ASC, t.id ASC
		LIMIT ?`, organizationID, TenantPoolBindingFree, tidbCloudNativeProvider, TenantFailed,
		updatedBefore.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list failed native tenant cleanup candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanTenantBindingRows(rows)
}

// MarkFailedNativeTenantDeleting claims an eligible native cleanup row. The
// locked read and update both repeat the organization and pool-claim boundary.
func (s *Store) MarkFailedNativeTenantDeleting(ctx context.Context, tenantID, organizationID string, updatedBefore time.Time) (updated bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_failed_native_tenant_deleting", start, &err)
	tenantID = strings.TrimSpace(tenantID)
	organizationID = strings.TrimSpace(organizationID)
	if tenantID == "" {
		return false, fmt.Errorf("tenant_id is required")
	}
	if organizationID == "" {
		return false, fmt.Errorf("organization_id is required")
	}
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var lockedTenantID string
		if err := tx.QueryRowContext(ctx, `SELECT t.id
			FROM tenants t
			JOIN tenant_tidbcloud_org_bindings b ON b.tenant_id = t.id
			WHERE t.id = ? AND t.provider = ? AND t.status = ? AND t.updated_at <= ?
				AND b.organization_id = ?
				AND (b.pool_status = ? OR b.pool_id = '')
			LIMIT 1 FOR UPDATE`, tenantID, tidbCloudNativeProvider, TenantFailed,
			updatedBefore.UTC(), organizationID, TenantPoolBindingFree).Scan(&lockedTenantID); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE tenants t
			SET t.status = ?, t.updated_at = ?
			WHERE t.id = ? AND t.provider = ? AND t.status = ? AND t.updated_at <= ?
				AND EXISTS (
					SELECT 1 FROM tenant_tidbcloud_org_bindings b
					WHERE b.tenant_id = t.id AND b.organization_id = ?
						AND (b.pool_status = ? OR b.pool_id = '')
				)`, TenantDeleting, time.Now().UTC(), lockedTenantID, tidbCloudNativeProvider,
			TenantFailed, updatedBefore.UTC(), organizationID, TenantPoolBindingFree)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		updated = affected == 1
		if updated {
			return insertTenantNotifyTx(ctx, tx, lockedTenantID, TenantNotifyWorkMetricsCleanup)
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("mark failed native tenant %s deleting: %w", tenantID, err)
	}
	if updated {
		s.apiKeys.evictTenant(tenantID)
	}
	return updated, nil
}
