package meta

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ManagedSharedDBPoolWaveMember is one logical tenant reservation staged with
// its physical managed shared DB pool.
type ManagedSharedDBPoolWaveMember struct {
	Tenant               *Tenant
	Membership           *TenantPoolMembership
	Quota                *QuotaConfig
	AutoEmbeddingProfile *TenantAutoEmbeddingProfile
}

// ManagedSharedDBPoolWavePlan describes one physical pool and every logical
// tenant slot reserved from it before the pool becomes visible to allocators.
type ManagedSharedDBPoolWavePlan struct {
	DB      *SharedDB
	Members []ManagedSharedDBPoolWaveMember
}

// ManagedSharedDBPoolWaveResult contains durable identifiers created for one
// physical partition of a refill wave.
type ManagedSharedDBPoolWaveResult struct {
	DBID      int64
	TenantIDs []string
}

type preparedManagedSharedDBPoolWavePlan struct {
	values  managedSharedDBInsertValues
	members []ManagedSharedDBPoolWaveMember
}

func multiRowPlaceholders(rows, columns int) string {
	row := "(" + strings.TrimRight(strings.Repeat("?,", columns), ",") + ")"
	return strings.TrimRight(strings.Repeat(row+",", rows), ",")
}

// CreateManagedSharedDBPoolTenantWave atomically creates each physical pool
// together with all logical tenant reservations assigned to it. A direct
// allocator therefore cannot observe refill-staged physical capacity before
// tenant_count, placements, and pool memberships account for the wave.
func (s *Store) CreateManagedSharedDBPoolTenantWave(ctx context.Context, plans []ManagedSharedDBPoolWavePlan) (out []ManagedSharedDBPoolWaveResult, err error) {
	start := time.Now()
	defer observeMeta(ctx, "create_managed_shared_db_pool_tenant_wave", start, &err)
	if len(plans) == 0 {
		return nil, nil
	}
	prepared := make([]preparedManagedSharedDBPoolWavePlan, len(plans))
	seenTenants := make(map[string]struct{})
	for i := range plans {
		values, prepErr := managedSharedDBInsertValuesFor(plans[i].DB)
		if prepErr != nil {
			return nil, fmt.Errorf("prepare managed shared DB plan %d: %w", i, prepErr)
		}
		if len(plans[i].Members) == 0 || len(plans[i].Members) > values.maxTenants {
			return nil, fmt.Errorf("managed shared DB plan %d has %d members outside capacity 1..%d", i, len(plans[i].Members), values.maxTenants)
		}
		for j := range plans[i].Members {
			member := plans[i].Members[j]
			if member.Tenant == nil || strings.TrimSpace(member.Tenant.ID) == "" {
				return nil, fmt.Errorf("managed shared DB plan %d member %d has no tenant", i, j)
			}
			tenantID := member.Tenant.ID
			if _, exists := seenTenants[tenantID]; exists {
				return nil, fmt.Errorf("duplicate tenant %q in managed shared DB wave", tenantID)
			}
			seenTenants[tenantID] = struct{}{}
			if member.Tenant.Provider != tidbCloudNativeSharedProvider || member.Tenant.Status != TenantPending {
				return nil, fmt.Errorf("managed shared DB wave tenant %q must be pending shared provider", tenantID)
			}
			if member.Membership == nil || member.Membership.TenantID != tenantID || strings.TrimSpace(member.Membership.PoolID) == "" || member.Membership.PoolStatus != TenantPoolBindingFree {
				return nil, fmt.Errorf("managed shared DB wave tenant %q has invalid free membership", tenantID)
			}
			if strings.TrimSpace(member.Membership.TiDBCloudOrganizationID) != values.organizationID {
				return nil, fmt.Errorf("managed shared DB wave tenant %q organization does not match physical pool", tenantID)
			}
			if member.Quota == nil || member.Quota.TenantID != tenantID {
				return nil, fmt.Errorf("managed shared DB wave tenant %q has invalid quota", tenantID)
			}
			if member.AutoEmbeddingProfile != nil && member.AutoEmbeddingProfile.TenantID != tenantID {
				return nil, fmt.Errorf("managed shared DB wave tenant %q has invalid auto-embedding profile", tenantID)
			}
		}
		prepared[i] = preparedManagedSharedDBPoolWavePlan{values: values, members: plans[i].Members}
	}

	err = s.InTx(ctx, func(tx *sql.Tx) error {
		out = make([]ManagedSharedDBPoolWaveResult, len(prepared))
		for i := range prepared {
			dbID, insertErr := insertManagedSharedDBPool(ctx, tx, prepared[i].values)
			if insertErr != nil {
				return insertErr
			}
			result, insertErr := createManagedSharedDBPoolWaveMembers(ctx, tx, dbID, prepared[i])
			if insertErr != nil {
				return fmt.Errorf("stage managed shared DB pool %d: %w", dbID, insertErr)
			}
			out[i] = result
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func createManagedSharedDBPoolWaveMembers(ctx context.Context, tx *sql.Tx, dbID int64, plan preparedManagedSharedDBPoolWavePlan) (ManagedSharedDBPoolWaveResult, error) {
	now := time.Now().UTC()
	tenantArgs := make([]any, 0, len(plan.members)*22)
	tenantIDs := make([]string, 0, len(plan.members))
	for _, member := range plan.members {
		t := member.Tenant
		tenantIDs = append(tenantIDs, t.ID)
		tenantArgs = append(tenantArgs,
			t.ID, t.Status, tenantKindForInsert(t), t.ParentTenantID, t.StorageNamespaceID,
			t.DBHost, t.DBPort, t.DBUser, t.DBPasswordCipher, t.DBName, boolToInt(t.DBTLS),
			t.Provider, nullStr(t.ClusterID), t.BranchID, nullStr(t.ClaimURL), t.ClaimExpiresAt, t.SchemaVersion,
			tenantS3EncryptionModeForInsert(t), t.S3KMSKeyID, boolToInt(tenantS3BucketKeyEnabledForInsert(t)),
			t.CreatedAt.UTC(), t.UpdatedAt.UTC())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenants
		(id, status, kind, parent_tenant_id, storage_namespace_id, db_host, db_port, db_user, db_password, db_name, db_tls,
		 provider, cluster_id, branch_id, claim_url, claim_expires_at, schema_version,
		 s3_encryption_mode, s3_kms_key_id, s3_bucket_key_enabled, created_at, updated_at) VALUES `+
		multiRowPlaceholders(len(plan.members), 22), tenantArgs...); err != nil {
		if isDuplicateEntry(err) {
			return ManagedSharedDBPoolWaveResult{}, ErrDuplicate
		}
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("insert wave tenants: %w", err)
	}

	fsArgs := make([]any, len(tenantIDs))
	for i := range tenantIDs {
		fsArgs[i] = tenantIDs[i]
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fs_registry (tenant_id) VALUES `+multiRowPlaceholders(len(tenantIDs), 1), fsArgs...); err != nil {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("insert wave fs registry: %w", err)
	}
	selectArgs := make([]any, len(tenantIDs))
	copy(selectArgs, fsArgs)
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id, fs_id FROM fs_registry WHERE tenant_id IN (`+
		strings.TrimRight(strings.Repeat("?,", len(tenantIDs)), ",")+`)`, selectArgs...)
	if err != nil {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("load wave fs registry: %w", err)
	}
	fsIDs := make(map[string]int64, len(tenantIDs))
	for rows.Next() {
		var tenantID string
		var fsID int64
		if err := rows.Scan(&tenantID, &fsID); err != nil {
			_ = rows.Close()
			return ManagedSharedDBPoolWaveResult{}, err
		}
		fsIDs[tenantID] = fsID
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return ManagedSharedDBPoolWaveResult{}, err
	}
	if err := rows.Close(); err != nil {
		return ManagedSharedDBPoolWaveResult{}, err
	}
	if len(fsIDs) != len(tenantIDs) {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("resolved %d fs IDs for %d wave tenants", len(fsIDs), len(tenantIDs))
	}

	quotaArgs := make([]any, 0, len(plan.members)*9)
	placementArgs := make([]any, 0, len(plan.members)*6)
	membershipArgs := make([]any, 0, len(plan.members)*6)
	profileArgs := make([]any, 0, len(plan.members)*9)
	profileCount := 0
	for _, member := range plan.members {
		tenantID := member.Tenant.ID
		q := member.Quota
		quotaArgs = append(quotaArgs, q.TenantID, q.MaxStorageBytes, q.MaxFileSizeBytes, q.MaxFileCount,
			q.MaxMediaLLMFiles, q.MaxVideoLLMFiles, q.MaxMonthlyCostMC,
			nullInt64FromPtr(q.TiDBCloudSpendingLimit), nullTimeFromPtr(q.TiDBCloudSpendingLimitCheckedAt))
		placementArgs = append(placementArgs, fsIDs[tenantID], dbID, PlacementShared, SchemaShapeShared, SharedDBStatusActive, nil)
		m := member.Membership
		createdAt := m.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		updatedAt := m.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = createdAt
		}
		membershipArgs = append(membershipArgs, tenantID, strings.TrimSpace(m.TiDBCloudOrganizationID), m.PoolID,
			TenantPoolBindingFree, createdAt.UTC(), updatedAt.UTC())
		if p := member.AutoEmbeddingProfile; p != nil {
			profileCount++
			profileArgs = append(profileArgs, p.TenantID, nullStr(p.EmbeddingMode), p.Model, p.Dimensions, p.OptionsJSON,
				nullStr(p.APIBase), nullableBytes(p.APIKeyCipher), p.CreatedAt.UTC(), p.UpdatedAt.UTC())
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_quota_config
		(tenant_id, max_storage_bytes, max_file_size_bytes, max_file_count, max_media_llm_files, max_video_llm_files,
		 max_monthly_cost_mc, tidbcloud_spending_limit, tidbcloud_spending_limit_checked_at) VALUES `+
		multiRowPlaceholders(len(plan.members), 9), quotaArgs...); err != nil {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("insert wave quota configs: %w", err)
	}
	if profileCount > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_auto_embedding_profiles
			(tenant_id, embedding_mode, model, dimensions, options_json, api_base, api_key_cipher, created_at, updated_at) VALUES `+
			multiRowPlaceholders(profileCount, 9), profileArgs...); err != nil {
			return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("insert wave auto-embedding profiles: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_placements
		(fs_id, db_id, placement, schema_shape, status, target_db_id) VALUES `+
		multiRowPlaceholders(len(plan.members), 6), placementArgs...); err != nil {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("insert wave placements: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_pool_memberships
		(tenant_id, tidbcloud_organization_id, pool_id, pool_status, created_at, updated_at) VALUES `+
		multiRowPlaceholders(len(plan.members), 6), membershipArgs...); err != nil {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("insert wave memberships: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE db_pool SET tenant_count = ?, soft_cap_reached = CASE WHEN ? >= max_tenants THEN 1 ELSE 0 END
		WHERE db_id = ? AND status = ?`, len(plan.members), len(plan.members), dbID, SharedDBStatusPending)
	if err != nil {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("reserve wave physical capacity: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("reserve wave physical capacity rows affected: %w", err)
	}
	if affected != 1 {
		return ManagedSharedDBPoolWaveResult{}, fmt.Errorf("reserve wave physical capacity changed %d rows for db pool %d", affected, dbID)
	}
	return ManagedSharedDBPoolWaveResult{DBID: dbID, TenantIDs: tenantIDs}, nil
}
