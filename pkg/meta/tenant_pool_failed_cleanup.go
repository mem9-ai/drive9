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

// ListFailedSharedTenantCleanupCandidates lists failed shared tenants owned by
// an organization. A tenant belongs through a free pool membership, or through
// its physical placement when it has no logical-pool membership.
func (s *Store) ListFailedSharedTenantCleanupCandidates(ctx context.Context, organizationID string, updatedBefore time.Time, limit int) (out []Tenant, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_failed_shared_tenant_cleanup_candidates", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return nil, fmt.Errorf("organization_id is required")
	}
	if limit <= 0 {
		limit = 10
	}
	// Direct placement attribution intentionally requires an exact organization-owned pool.
	// Wildcard pools cannot prove a tenant's resolved organization until that identity is persisted per tenant.
	rows, err := s.db.QueryContext(ctx, `SELECT
			c.id, c.status, c.kind, c.parent_tenant_id, c.storage_namespace_id,
			c.db_host, c.db_port, c.db_user, c.db_password, c.db_name,
			c.db_tls, c.provider, c.cluster_id, c.branch_id, c.claim_url, c.claim_expires_at, c.schema_version,
			c.s3_encryption_mode, c.s3_kms_key_id, c.s3_bucket_key_enabled, c.created_at, c.updated_at
		FROM (
			SELECT
				t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
				t.db_host, t.db_port, t.db_user, t.db_password, t.db_name,
				t.db_tls, t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
				t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at
			FROM tenant_pool_memberships m
			JOIN tenants t ON t.id = m.tenant_id
			WHERE m.tidbcloud_organization_id = ? AND m.pool_status = ?
				AND t.provider = ? AND t.status = ? AND t.updated_at <= ?

			UNION ALL

			SELECT
				t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
				t.db_host, t.db_port, t.db_user, t.db_password, t.db_name,
				t.db_tls, t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
				t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at
			FROM db_pool d
			JOIN tenant_placements p ON p.db_id = d.db_id
			JOIN fs_registry f ON f.fs_id = p.fs_id
			JOIN tenants t ON t.id = f.tenant_id
			LEFT JOIN tenant_pool_memberships m ON m.tenant_id = t.id
			WHERE d.org_id = ? AND m.tenant_id IS NULL
				AND t.provider = ? AND t.status = ? AND t.updated_at <= ?
		) c
		ORDER BY c.updated_at ASC, c.id ASC
		LIMIT ?`, organizationID, TenantPoolBindingFree, tidbCloudNativeSharedProvider,
		TenantFailed, updatedBefore.UTC(), organizationID, tidbCloudNativeSharedProvider,
		TenantFailed, updatedBefore.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list failed shared tenant cleanup candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanTenantRows(rows)
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
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("mark failed native tenant %s deleting: %w", tenantID, err)
	}
	return updated, nil
}

// MarkFailedSharedTenantDeleting claims an eligible shared cleanup row. The
// locked read and update both repeat the organization and membership boundary.
func (s *Store) MarkFailedSharedTenantDeleting(ctx context.Context, tenantID, organizationID string, updatedBefore time.Time) (updated bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_failed_shared_tenant_deleting", start, &err)
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
			WHERE t.id = ? AND t.provider = ? AND t.status = ? AND t.updated_at <= ?
				AND (
					EXISTS (
						SELECT 1 FROM tenant_pool_memberships m
						WHERE m.tenant_id = t.id AND m.pool_status = ?
							AND m.tidbcloud_organization_id = ?
					)
					OR (
						NOT EXISTS (
							SELECT 1 FROM tenant_pool_memberships m
							WHERE m.tenant_id = t.id
						)
						AND EXISTS (
							SELECT 1
							FROM fs_registry f
							JOIN tenant_placements p ON p.fs_id = f.fs_id
							JOIN db_pool d ON d.db_id = p.db_id
							WHERE f.tenant_id = t.id AND d.org_id = ?
						)
					)
				)
			LIMIT 1 FOR UPDATE`, tenantID, tidbCloudNativeSharedProvider, TenantFailed,
			updatedBefore.UTC(), TenantPoolBindingFree, organizationID, organizationID).Scan(&lockedTenantID); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE tenants t
			SET t.status = ?, t.updated_at = ?
			WHERE t.id = ? AND t.provider = ? AND t.status = ? AND t.updated_at <= ?
				AND (
					EXISTS (
						SELECT 1 FROM tenant_pool_memberships m
						WHERE m.tenant_id = t.id AND m.pool_status = ?
							AND m.tidbcloud_organization_id = ?
					)
					OR (
						NOT EXISTS (
							SELECT 1 FROM tenant_pool_memberships m
							WHERE m.tenant_id = t.id
						)
						AND EXISTS (
							SELECT 1
							FROM fs_registry f
							JOIN tenant_placements p ON p.fs_id = f.fs_id
							JOIN db_pool d ON d.db_id = p.db_id
							WHERE f.tenant_id = t.id AND d.org_id = ?
						)
					)
				)`, TenantDeleting, time.Now().UTC(), lockedTenantID,
			tidbCloudNativeSharedProvider, TenantFailed, updatedBefore.UTC(), TenantPoolBindingFree,
			organizationID, organizationID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		updated = affected == 1
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("mark failed shared tenant %s deleting: %w", tenantID, err)
	}
	return updated, nil
}
