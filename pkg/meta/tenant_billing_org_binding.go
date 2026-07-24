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
