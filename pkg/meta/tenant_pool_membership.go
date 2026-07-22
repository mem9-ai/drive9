package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TenantPoolMembership is the shared-provider membership record for the
// provider-agnostic logical tenant pool. Native membership remains in
// tenant_tidbcloud_org_bindings.
type TenantPoolMembership struct {
	TenantID                string
	TiDBCloudOrganizationID string
	PoolID                  string
	PoolStatus              TenantPoolBindingStatus
	UsedAt                  *time.Time
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type TenantWithPoolMembership struct {
	Tenant     Tenant
	Membership TenantPoolMembership
}

func (s *Store) UpsertTenantPoolMembership(ctx context.Context, membership *TenantPoolMembership) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_tenant_pool_membership", start, &err)
	if membership == nil {
		return fmt.Errorf("tenant pool membership is required")
	}
	if strings.TrimSpace(membership.TenantID) == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if strings.TrimSpace(membership.PoolID) == "" {
		return fmt.Errorf("pool_id is required")
	}
	status := membership.PoolStatus
	if status == "" {
		status = TenantPoolBindingFree
	}
	if status != TenantPoolBindingFree && status != TenantPoolBindingUsed {
		return fmt.Errorf("unsupported tenant pool membership status %q", status)
	}
	createdAt := membership.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	updatedAt := membership.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	var organizationID any
	if strings.TrimSpace(membership.TiDBCloudOrganizationID) != "" {
		organizationID = strings.TrimSpace(membership.TiDBCloudOrganizationID)
	}
	return s.InTx(ctx, func(tx *sql.Tx) error {
		var provider string
		if err := tx.QueryRowContext(ctx, `SELECT provider FROM tenants WHERE id = ? FOR UPDATE`,
			membership.TenantID).Scan(&provider); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("lock tenant for pool membership %s: %w", membership.TenantID, err)
		}
		if provider != tidbCloudNativeSharedProvider {
			return fmt.Errorf("tenant %q with provider %q cannot use shared pool membership",
				membership.TenantID, provider)
		}
		var dedicatedBindingCount int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_tidbcloud_org_bindings
			WHERE tenant_id = ?`, membership.TenantID).Scan(&dedicatedBindingCount); err != nil {
			return err
		}
		if dedicatedBindingCount != 0 {
			return fmt.Errorf("tenant %q already has a dedicated tidbcloud org binding", membership.TenantID)
		}
		var existingOrganizationID sql.NullString
		lookupErr := tx.QueryRowContext(ctx, `SELECT tidbcloud_organization_id FROM tenant_pool_memberships
			WHERE tenant_id = ? FOR UPDATE`, membership.TenantID).Scan(&existingOrganizationID)
		if errors.Is(lookupErr, sql.ErrNoRows) {
			_, insertErr := tx.ExecContext(ctx, `INSERT INTO tenant_pool_memberships
				(tenant_id, tidbcloud_organization_id, pool_id, pool_status, used_at, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?)`, membership.TenantID, organizationID, membership.PoolID,
				status, membership.UsedAt, createdAt, updatedAt)
			if insertErr != nil {
				return fmt.Errorf("insert tenant pool membership for %s: %w", membership.TenantID, insertErr)
			}
			return nil
		}
		if lookupErr != nil {
			return fmt.Errorf("lock tenant pool membership for %s: %w", membership.TenantID, lookupErr)
		}
		incomingOrganizationID := strings.TrimSpace(membership.TiDBCloudOrganizationID)
		if existingOrganizationID.Valid && existingOrganizationID.String != "" && incomingOrganizationID != "" &&
			existingOrganizationID.String != incomingOrganizationID {
			return fmt.Errorf("tenant pool membership organization is immutable: have %q, got %q",
				existingOrganizationID.String, incomingOrganizationID)
		}
		if existingOrganizationID.Valid && existingOrganizationID.String != "" {
			organizationID = existingOrganizationID.String
		}
		_, updateErr := tx.ExecContext(ctx, `UPDATE tenant_pool_memberships
			SET tidbcloud_organization_id = ?, pool_id = ?, pool_status = ?, used_at = ?, updated_at = ?
			WHERE tenant_id = ?`, organizationID, membership.PoolID, status, membership.UsedAt,
			updatedAt, membership.TenantID)
		if updateErr != nil {
			return fmt.Errorf("update tenant pool membership for %s: %w", membership.TenantID, updateErr)
		}
		return nil
	})
}

func (s *Store) GetTenantPoolMembership(ctx context.Context, tenantID string) (membership *TenantPoolMembership, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant_pool_membership", start, &err)
	membership = &TenantPoolMembership{}
	var organizationID sql.NullString
	var usedAt sql.NullTime
	err = s.db.QueryRowContext(ctx, `SELECT tenant_id, tidbcloud_organization_id, pool_id, pool_status,
		used_at, created_at, updated_at FROM tenant_pool_memberships WHERE tenant_id = ?`, tenantID).
		Scan(&membership.TenantID, &organizationID, &membership.PoolID, &membership.PoolStatus,
			&usedAt, &membership.CreatedAt, &membership.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get tenant pool membership for %s: %w", tenantID, err)
	}
	membership.TiDBCloudOrganizationID = organizationID.String
	if usedAt.Valid {
		t := usedAt.Time
		membership.UsedAt = &t
	}
	return membership, nil
}

func (s *Store) ClaimTenantPoolMembership(ctx context.Context, tenantID, poolID string, usedAt time.Time) (claimed bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "claim_tenant_pool_membership", start, &err)
	if usedAt.IsZero() {
		usedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_pool_memberships
		SET pool_status = ?, used_at = ?, updated_at = ?
		WHERE tenant_id = ? AND pool_id = ? AND pool_status = ?`,
		TenantPoolBindingUsed, usedAt, usedAt, tenantID, poolID, TenantPoolBindingFree)
	if err != nil {
		return false, fmt.Errorf("claim tenant pool membership for %s: %w", tenantID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim tenant pool membership rows affected for %s: %w", tenantID, err)
	}
	return affected == 1, nil
}

func (s *Store) GetOldestFreeTenantPoolMembership(ctx context.Context, poolID string) (*TenantWithPoolMembership, error) {
	return s.getFreeTenantPoolMembershipCandidate(ctx, poolID, false)
}

func (s *Store) GetNewestFreeTenantPoolMembership(ctx context.Context, poolID string) (*TenantWithPoolMembership, error) {
	return s.getFreeTenantPoolMembershipCandidate(ctx, poolID, true)
}

func (s *Store) GetNewestFreeTenantPoolMembershipForDelete(ctx context.Context, poolID string) (*TenantWithPoolMembership, error) {
	var tenantID string
	err := s.db.QueryRowContext(ctx, `SELECT m.tenant_id FROM tenant_pool_memberships m
		JOIN tenants t ON t.id = m.tenant_id
		WHERE m.pool_id = ? AND m.pool_status = ? AND t.status IN (?, ?, ?, ?)
		ORDER BY m.created_at DESC, m.tenant_id DESC LIMIT 1`, poolID, TenantPoolBindingFree,
		TenantPending, TenantProvisioning, TenantActive, TenantFailed).Scan(&tenantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t, err := s.GetTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	m, err := s.GetTenantPoolMembership(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return &TenantWithPoolMembership{Tenant: *t, Membership: *m}, nil
}

func (s *Store) getFreeTenantPoolMembershipCandidate(ctx context.Context, poolID string, newest bool) (*TenantWithPoolMembership, error) {
	direction := "ASC"
	if newest {
		direction = "DESC"
	}
	var tenantID string
	err := s.db.QueryRowContext(ctx, `SELECT m.tenant_id
		FROM tenant_pool_memberships m
		JOIN tenants t ON t.id = m.tenant_id
		WHERE m.pool_id = ? AND m.pool_status = ? AND t.status = ?
		ORDER BY m.created_at `+direction+`, m.tenant_id `+direction+` LIMIT 1`,
		poolID, TenantPoolBindingFree, TenantActive).Scan(&tenantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get free tenant pool membership candidate: %w", err)
	}
	tenant, err := s.GetTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	membership, err := s.GetTenantPoolMembership(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return &TenantWithPoolMembership{Tenant: *tenant, Membership: *membership}, nil
}

func (s *Store) DeleteTenantPoolMembership(ctx context.Context, tenantID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM tenant_pool_memberships WHERE tenant_id = ?`, tenantID)
	if err != nil {
		return fmt.Errorf("delete tenant pool membership for %s: %w", tenantID, err)
	}
	return nil
}

func (s *Store) MarkFreeSharedTenantPoolTenantDeleting(ctx context.Context, tenantID string, from TenantStatus) (bool, error) {
	var updated bool
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		var id string
		if err := tx.QueryRowContext(ctx, `SELECT t.id FROM tenants t
			JOIN tenant_pool_memberships m ON m.tenant_id = t.id
			WHERE t.id = ? AND t.status = ? AND m.pool_status = ? FOR UPDATE`, tenantID, from,
			TenantPoolBindingFree).Scan(&id); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ?
			WHERE id = ? AND status = ?`, TenantDeleting, time.Now().UTC(), tenantID, from)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		updated = n == 1
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return updated, err
}

func (s *Store) CountFreeTenantPoolMemberships(ctx context.Context, organizationID string, statuses []TenantStatus) (int, error) {
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" || len(statuses) == 0 {
		return 0, fmt.Errorf("organization id and tenant statuses are required")
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(statuses)), ",")
	args := []any{organizationID, TenantPoolBindingFree, tidbCloudNativeSharedProvider}
	for _, status := range statuses {
		args = append(args, status)
	}
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_pool_memberships m
		JOIN tenants t ON t.id = m.tenant_id
		WHERE m.tidbcloud_organization_id = ? AND m.pool_status = ? AND t.provider = ?
			AND t.status IN (`+placeholders+`)`, args...).Scan(&count)
	return count, err
}

// UpdateTenantPoolMembershipOrganization fills the query/metrics dimension
// after a first Cloud cluster resolves the organization identity.
func (s *Store) UpdateTenantPoolMembershipOrganization(ctx context.Context, poolID, organizationID string) error {
	poolID = strings.TrimSpace(poolID)
	organizationID = strings.TrimSpace(organizationID)
	if poolID == "" || organizationID == "" {
		return fmt.Errorf("pool id and tidbcloud organization id are required")
	}
	return s.InTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT tidbcloud_organization_id
			FROM tenant_pool_memberships WHERE pool_id = ? FOR UPDATE`, poolID)
		if err != nil {
			return fmt.Errorf("lock tenant pool memberships for %s: %w", poolID, err)
		}
		for rows.Next() {
			var existing sql.NullString
			if err := rows.Scan(&existing); err != nil {
				_ = rows.Close()
				return err
			}
			if existing.Valid && existing.String != "" && existing.String != organizationID {
				_ = rows.Close()
				return fmt.Errorf("tenant pool membership organization is immutable: have %q, got %q",
					existing.String, organizationID)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE tenant_pool_memberships
			SET tidbcloud_organization_id = ?, updated_at = ?
			WHERE pool_id = ? AND (tidbcloud_organization_id IS NULL OR tidbcloud_organization_id = '')`,
			organizationID, time.Now().UTC(), poolID)
		return err
	})
}

// DetachUsedTenantPoolMemberships removes logical-pool ownership from claimed
// shared tenants while leaving their tenant and physical DB placement intact.
func (s *Store) DetachUsedTenantPoolMemberships(ctx context.Context, poolID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM tenant_pool_memberships
		WHERE pool_id = ? AND pool_status = ?`, strings.TrimSpace(poolID), TenantPoolBindingUsed)
	return err
}

// ClaimSharedTenantPoolMembership atomically finalizes a free shared member:
// virtual quota patch, membership CAS, and owner key.
func (s *Store) ClaimSharedTenantPoolMembership(ctx context.Context, tenantID, poolID string, patch QuotaConfigPatch, k *APIKey) (err error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(poolID) == "" || k == nil {
		return fmt.Errorf("tenant id, pool id, and api key are required")
	}
	scopeKind, err := apiKeyScopeKindForInsert(k)
	if err != nil {
		return err
	}
	return s.InTx(ctx, func(tx *sql.Tx) error {
		var dbID int64
		if err := tx.QueryRowContext(ctx, `SELECT p.db_id
			FROM tenant_pool_memberships m
			JOIN tenants t ON t.id = m.tenant_id
			JOIN fs_registry f ON f.tenant_id = t.id
			JOIN tenant_placements p ON p.fs_id = f.fs_id
			WHERE m.tenant_id = ? AND m.pool_id = ? AND m.pool_status = ?
				AND t.provider = ? AND t.status = ? FOR UPDATE`, tenantID, poolID,
			TenantPoolBindingFree, tidbCloudNativeSharedProvider, TenantActive).Scan(&dbID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		var storage, fileSize, fileCount int64
		var spending sql.NullInt64
		var checkedAt sql.NullTime
		if err := tx.QueryRowContext(ctx, `SELECT max_storage_bytes, max_file_size_bytes,
			max_file_count, tidbcloud_spending_limit, tidbcloud_spending_limit_checked_at
			FROM tenant_quota_config WHERE tenant_id = ? FOR UPDATE`, tenantID).
			Scan(&storage, &fileSize, &fileCount, &spending, &checkedAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrSharedDBQuotaNotMaterialized
			}
			return err
		}
		if !spending.Valid {
			return ErrSharedDBQuotaNotMaterialized
		}
		nextSpending := spending.Int64
		if patch.TiDBCloudSpendingLimit != nil {
			nextSpending = *patch.TiDBCloudSpendingLimit
		}
		if patch.MaxStorageBytes != nil {
			storage = *patch.MaxStorageBytes
		}
		if patch.MaxFileSizeBytes != nil {
			fileSize = *patch.MaxFileSizeBytes
		}
		if patch.MaxFileCount != nil {
			fileCount = *patch.MaxFileCount
		}
		if patch.TiDBCloudSpendingLimitCheckedAt != nil {
			checkedAt = sql.NullTime{Time: patch.TiDBCloudSpendingLimitCheckedAt.UTC(), Valid: true}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tenant_quota_config SET max_storage_bytes = ?,
			max_file_size_bytes = ?, max_file_count = ?, tidbcloud_spending_limit = ?,
			tidbcloud_spending_limit_checked_at = ? WHERE tenant_id = ?`, storage, fileSize,
			fileCount, nextSpending, checkedAt, tenantID); err != nil {
			return err
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(ctx, `UPDATE tenant_pool_memberships SET pool_status = ?, used_at = ?, updated_at = ?
			WHERE tenant_id = ? AND pool_id = ? AND pool_status = ?`, TenantPoolBindingUsed, now, now,
			tenantID, poolID, TenantPoolBindingFree)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrNotFound
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_api_keys
			(id, tenant_id, key_name, jwt_ciphertext, jwt_hash, token_version, status, scope_kind,
			 issued_by_provider, issued_by_subject_key, issued_by_metadata_json,
			 issued_at, revoked_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, k.ID, k.TenantID, k.KeyName,
			k.JWTCiphertext, k.JWTHash, k.TokenVersion, k.Status, scopeKind, k.IssuedByProvider,
			k.IssuedBySubjectKey, nullableBytes(k.IssuedByMetadataJSON), k.IssuedAt.UTC(), k.RevokedAt,
			k.CreatedAt.UTC(), k.UpdatedAt.UTC()); err != nil {
			if isDuplicateEntry(err) {
				return ErrDuplicate
			}
			return err
		}
		return nil
	})
}
