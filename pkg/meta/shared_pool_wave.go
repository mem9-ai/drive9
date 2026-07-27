package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
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

// ManagedSharedDBPoolWaveResult contains one durable physical assignment for a
// refill wave. Created reports whether the wave inserted the physical pool or
// reused capacity from an existing row.
type ManagedSharedDBPoolWaveResult struct {
	DBID      int64
	TenantIDs []string
	Created   bool
}

// ManagedSharedDBPoolWaveResize atomically grows the logical tenant pool with
// a shared physical-pool reservation wave. ExpectedSize fences stale admin
// decisions; TargetSize is the capacity limit published with the wave.
type ManagedSharedDBPoolWaveResize struct {
	ExpectedSize int
	TargetSize   int
}

type preparedManagedSharedDBPoolWavePlan struct {
	values       managedSharedDBInsertValues
	members      []ManagedSharedDBPoolWaveMember
	tenantStatus TenantStatus
}

type stagedManagedSharedDBPoolWavePlan struct {
	dbID           int64
	dbStatus       string
	tenantIDs      []string
	placementArgs  []any
	membershipArgs []any
}

type managedSharedDBPoolWaveCandidate struct {
	dbID        int64
	status      string
	maxTenants  int
	tenantCount int
}

func multiRowPlaceholders(rows, columns int) string {
	row := "(" + strings.TrimRight(strings.Repeat("?,", columns), ",") + ")"
	return strings.TrimRight(strings.Repeat(row+",", rows), ",")
}

// CreateManagedSharedDBPoolTenantWave atomically fills existing physical pool
// capacity before creating physical pools for any remaining logical tenant
// reservations. A direct allocator therefore cannot observe refill-staged
// capacity before tenant_count, placements, and memberships account for it.
func (s *Store) CreateManagedSharedDBPoolTenantWave(ctx context.Context, plans []ManagedSharedDBPoolWavePlan) ([]ManagedSharedDBPoolWaveResult, error) {
	return s.CreateManagedSharedDBPoolTenantWaveWithResize(ctx, plans, nil)
}

// CreateManagedSharedDBPoolTenantWaveWithResize stages a wave and optionally
// grows its logical pool in the same transaction.
func (s *Store) CreateManagedSharedDBPoolTenantWaveWithResize(ctx context.Context, plans []ManagedSharedDBPoolWavePlan, resize *ManagedSharedDBPoolWaveResize) (out []ManagedSharedDBPoolWaveResult, err error) {
	start := time.Now()
	defer observeMeta(ctx, "create_managed_shared_db_pool_tenant_wave", start, &err)
	if len(plans) == 0 {
		return nil, nil
	}
	prepared := make([]preparedManagedSharedDBPoolWavePlan, len(plans))
	waveMembers := make([]ManagedSharedDBPoolWaveMember, 0)
	seenTenants := make(map[string]struct{})
	wavePoolID := ""
	waveOrganizationID := ""
	waveMemberCount := 0
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
			memberPoolID := ""
			if member.Membership != nil {
				memberPoolID = strings.TrimSpace(member.Membership.PoolID)
			}
			if member.Membership == nil || member.Membership.TenantID != tenantID || memberPoolID == "" || member.Membership.PoolStatus != TenantPoolBindingFree {
				return nil, fmt.Errorf("managed shared DB wave tenant %q has invalid free membership", tenantID)
			}
			if strings.TrimSpace(member.Membership.TiDBCloudOrganizationID) != values.organizationID {
				return nil, fmt.Errorf("managed shared DB wave tenant %q organization does not match physical pool", tenantID)
			}
			if wavePoolID == "" {
				wavePoolID = memberPoolID
				waveOrganizationID = values.organizationID
			} else if memberPoolID != wavePoolID || values.organizationID != waveOrganizationID {
				return nil, fmt.Errorf("managed shared DB wave must belong to one logical pool and organization")
			}
			if member.Quota == nil || member.Quota.TenantID != tenantID {
				return nil, fmt.Errorf("managed shared DB wave tenant %q has invalid quota", tenantID)
			}
			if member.AutoEmbeddingProfile != nil && member.AutoEmbeddingProfile.TenantID != tenantID {
				return nil, fmt.Errorf("managed shared DB wave tenant %q has invalid auto-embedding profile", tenantID)
			}
		}
		prepared[i] = preparedManagedSharedDBPoolWavePlan{values: values}
		waveMembers = append(waveMembers, plans[i].Members...)
		waveMemberCount += len(plans[i].Members)
	}

	err = withMetaLockConflictRetry(ctx, "create_managed_shared_db_pool_tenant_wave", func() error {
		attemptOut := make([]ManagedSharedDBPoolWaveResult, 0, len(prepared))
		staged := make([]stagedManagedSharedDBPoolWavePlan, 0, len(prepared))
		txErr := s.InTx(ctx, func(tx *sql.Tx) error {
			memberOffset := 0
			candidates, candidateErr := lockManagedSharedDBPoolWaveCandidates(ctx, tx, waveOrganizationID)
			if candidateErr != nil {
				return candidateErr
			}
			for _, candidate := range candidates {
				if memberOffset >= len(waveMembers) {
					break
				}
				assigned := min(candidate.maxTenants-candidate.tenantCount, len(waveMembers)-memberOffset)
				if assigned <= 0 {
					continue
				}
				tenantStatus, statusErr := tenantStatusForSharedDBPoolWave(candidate.status)
				if statusErr != nil {
					return statusErr
				}
				partition := preparedManagedSharedDBPoolWavePlan{
					members:      waveMembers[memberOffset : memberOffset+assigned],
					tenantStatus: tenantStatus,
				}
				stagedPartition, stageErr := stageManagedSharedDBPoolWaveMembers(ctx, tx, candidate.dbID, candidate.status, partition)
				if stageErr != nil {
					return fmt.Errorf("stage existing managed shared DB pool %d: %w", candidate.dbID, stageErr)
				}
				staged = append(staged, stagedPartition)
				attemptOut = append(attemptOut, ManagedSharedDBPoolWaveResult{DBID: candidate.dbID, TenantIDs: stagedPartition.tenantIDs})
				memberOffset += assigned
			}
			for i := range prepared {
				if memberOffset >= len(waveMembers) {
					break
				}
				assigned := min(prepared[i].values.maxTenants, len(waveMembers)-memberOffset)
				partition := preparedManagedSharedDBPoolWavePlan{
					values:       prepared[i].values,
					members:      waveMembers[memberOffset : memberOffset+assigned],
					tenantStatus: TenantPending,
				}
				dbID, insertErr := insertManagedSharedDBPool(ctx, tx, partition.values)
				if insertErr != nil {
					return insertErr
				}
				stagedPartition, insertErr := stageManagedSharedDBPoolWaveMembers(ctx, tx, dbID, SharedDBStatusPending, partition)
				if insertErr != nil {
					return fmt.Errorf("stage managed shared DB pool %d: %w", dbID, insertErr)
				}
				staged = append(staged, stagedPartition)
				attemptOut = append(attemptOut, ManagedSharedDBPoolWaveResult{DBID: dbID, TenantIDs: stagedPartition.tenantIDs, Created: true})
				memberOffset += assigned
			}
			if memberOffset != len(waveMembers) {
				return fmt.Errorf("managed shared DB wave assigned %d of %d members", memberOffset, len(waveMembers))
			}
			// Take logical-pool ownership only after the expensive independent
			// tenant/quota/profile staging. If delete or shrink committed first,
			// the complete uncommitted wave rolls back before any membership is
			// published or Cloud request can start.
			if validateErr := validateManagedSharedDBPoolWaveOwnership(ctx, tx, wavePoolID, waveOrganizationID, waveMemberCount, resize); validateErr != nil {
				return validateErr
			}
			for i := range staged {
				if finalizeErr := finalizeManagedSharedDBPoolWaveMembers(ctx, tx, staged[i]); finalizeErr != nil {
					return fmt.Errorf("finalize managed shared DB pool %d: %w", staged[i].dbID, finalizeErr)
				}
			}
			return nil
		})
		if txErr == nil {
			out = attemptOut
		}
		return txErr
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

const lockManagedSharedDBPoolWaveCandidatesSQL = `SELECT db_id, status, max_tenants, tenant_count
		FROM db_pool
		WHERE org_id = ? AND role = ? AND status IN (?, ?, ?)
			AND max_tenants > 0 AND soft_cap_reached = 0 AND tenant_count < max_tenants
		ORDER BY db_id
		FOR UPDATE`

func lockManagedSharedDBPoolWaveCandidates(ctx context.Context, tx *sql.Tx, organizationID string) ([]managedSharedDBPoolWaveCandidate, error) {
	// Acquire every reusable row for the organization in immutable primary-key
	// order. Status can advance concurrently, so using it in the locking order
	// can invert acquisition between overlapping refill waves. The result is
	// sorted by readiness only after the complete stable lock set is held.
	rows, err := tx.QueryContext(ctx, lockManagedSharedDBPoolWaveCandidatesSQL, organizationID, SharedDBRoleShared,
		SharedDBStatusActive, SharedDBStatusProvisioning, SharedDBStatusPending)
	if err != nil {
		return nil, fmt.Errorf("lock reusable managed shared DB pools: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []managedSharedDBPoolWaveCandidate
	for rows.Next() {
		var candidate managedSharedDBPoolWaveCandidate
		if err := rows.Scan(&candidate.dbID, &candidate.status, &candidate.maxTenants, &candidate.tenantCount); err != nil {
			return nil, err
		}
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sortManagedSharedDBPoolWaveCandidates(out)
	return out, nil
}

func sortManagedSharedDBPoolWaveCandidates(candidates []managedSharedDBPoolWaveCandidate) {
	statusRank := func(status string) int {
		switch status {
		case SharedDBStatusActive:
			return 0
		case SharedDBStatusProvisioning:
			return 1
		case SharedDBStatusPending:
			return 2
		default:
			return 3
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := statusRank(candidates[i].status), statusRank(candidates[j].status)
		if left != right {
			return left < right
		}
		return candidates[i].dbID < candidates[j].dbID
	})
}

func tenantStatusForSharedDBPoolWave(status string) (TenantStatus, error) {
	switch status {
	case SharedDBStatusActive:
		return TenantActive, nil
	case SharedDBStatusProvisioning:
		return TenantProvisioning, nil
	case SharedDBStatusPending:
		return TenantPending, nil
	default:
		return "", fmt.Errorf("shared DB pool status %q is not reusable", status)
	}
}

func stageManagedSharedDBPoolWaveMembers(ctx context.Context, tx *sql.Tx, dbID int64, dbStatus string, plan preparedManagedSharedDBPoolWavePlan) (stagedManagedSharedDBPoolWavePlan, error) {
	now := time.Now().UTC()
	tenantArgs := make([]any, 0, len(plan.members)*22)
	tenantIDs := make([]string, 0, len(plan.members))
	for _, member := range plan.members {
		t := member.Tenant
		tenantIDs = append(tenantIDs, t.ID)
		tenantArgs = append(tenantArgs,
			t.ID, plan.tenantStatus, tenantKindForInsert(t), t.ParentTenantID, t.StorageNamespaceID,
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
			return stagedManagedSharedDBPoolWavePlan{}, ErrDuplicate
		}
		return stagedManagedSharedDBPoolWavePlan{}, fmt.Errorf("insert wave tenants: %w", err)
	}

	fsArgs := make([]any, len(tenantIDs))
	for i := range tenantIDs {
		fsArgs[i] = tenantIDs[i]
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fs_registry (tenant_id) VALUES `+multiRowPlaceholders(len(tenantIDs), 1), fsArgs...); err != nil {
		return stagedManagedSharedDBPoolWavePlan{}, fmt.Errorf("insert wave fs registry: %w", err)
	}
	selectArgs := make([]any, len(tenantIDs))
	copy(selectArgs, fsArgs)
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id, fs_id FROM fs_registry WHERE tenant_id IN (`+
		strings.TrimRight(strings.Repeat("?,", len(tenantIDs)), ",")+`)`, selectArgs...)
	if err != nil {
		return stagedManagedSharedDBPoolWavePlan{}, fmt.Errorf("load wave fs registry: %w", err)
	}
	fsIDs := make(map[string]int64, len(tenantIDs))
	for rows.Next() {
		var tenantID string
		var fsID int64
		if err := rows.Scan(&tenantID, &fsID); err != nil {
			_ = rows.Close()
			return stagedManagedSharedDBPoolWavePlan{}, err
		}
		fsIDs[tenantID] = fsID
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return stagedManagedSharedDBPoolWavePlan{}, err
	}
	if err := rows.Close(); err != nil {
		return stagedManagedSharedDBPoolWavePlan{}, err
	}
	if len(fsIDs) != len(tenantIDs) {
		return stagedManagedSharedDBPoolWavePlan{}, fmt.Errorf("resolved %d fs IDs for %d wave tenants", len(fsIDs), len(tenantIDs))
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
		return stagedManagedSharedDBPoolWavePlan{}, fmt.Errorf("insert wave quota configs: %w", err)
	}
	if profileCount > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_auto_embedding_profiles
			(tenant_id, embedding_mode, model, dimensions, options_json, api_base, api_key_cipher, created_at, updated_at) VALUES `+
			multiRowPlaceholders(profileCount, 9), profileArgs...); err != nil {
			return stagedManagedSharedDBPoolWavePlan{}, fmt.Errorf("insert wave auto-embedding profiles: %w", err)
		}
	}
	return stagedManagedSharedDBPoolWavePlan{dbID: dbID, dbStatus: dbStatus, tenantIDs: tenantIDs,
		placementArgs: placementArgs, membershipArgs: membershipArgs}, nil
}

func validateManagedSharedDBPoolWaveOwnership(ctx context.Context, tx *sql.Tx, poolID, organizationID string, incoming int, resize *ManagedSharedDBPoolWaveResize) error {
	var currentOrganizationID sql.NullString
	var size int
	var status TenantPoolStatus
	err := tx.QueryRowContext(ctx, `SELECT organization_id, size, status
		FROM tenant_tidbcloud_pools WHERE pool_id = ? FOR UPDATE`, poolID).Scan(&currentOrganizationID, &size, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock logical tenant pool %q: %w", poolID, err)
	}
	if status != TenantPoolActive {
		return fmt.Errorf("logical tenant pool %q is %s, want active", poolID, status)
	}
	if !currentOrganizationID.Valid || strings.TrimSpace(currentOrganizationID.String) != organizationID {
		return fmt.Errorf("logical tenant pool %q organization does not match refill wave", poolID)
	}
	capacityLimit := size
	if resize != nil {
		if resize.ExpectedSize <= 0 || resize.TargetSize <= resize.ExpectedSize {
			return fmt.Errorf("invalid logical tenant pool grow contract")
		}
		if size != resize.ExpectedSize {
			return fmt.Errorf("logical tenant pool %q size changed: have=%d expected=%d", poolID, size, resize.ExpectedSize)
		}
		capacityLimit = resize.TargetSize
	}

	var nativeSlots, sharedActiveSlots, sharedPlannedSlots int
	// Match leader refill accounting: native work is request-credential-owned,
	// so only active native inventory offsets a leader-owned shared wave.
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM tenant_tidbcloud_org_bindings b
		STRAIGHT_JOIN tenants t ON t.id = b.tenant_id
		WHERE b.pool_id = ? AND b.organization_id = ? AND b.pool_status = ? AND t.provider = ?
			AND t.status = ? FOR UPDATE`,
		poolID, organizationID, TenantPoolBindingFree, tidbCloudNativeProvider,
		TenantActive).Scan(&nativeSlots)
	if err != nil {
		return fmt.Errorf("count logical tenant pool %q native slots: %w", poolID, err)
	}
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM tenant_pool_memberships m
		STRAIGHT_JOIN tenants t ON t.id = m.tenant_id
		WHERE m.pool_id = ? AND m.tidbcloud_organization_id = ? AND m.pool_status = ? AND t.provider = ?
			AND t.status = ? FOR UPDATE`,
		poolID, organizationID, TenantPoolBindingFree, tidbCloudNativeSharedProvider,
		TenantActive).Scan(&sharedActiveSlots)
	if err != nil {
		return fmt.Errorf("count logical tenant pool %q active shared slots: %w", poolID, err)
	}
	// A pending/provisioning membership is planned capacity only while its fs,
	// placement, and non-terminal physical DB attempt still exist. This matches
	// CountTenantPoolPlannedSlots and prevents orphan memberships from fencing
	// otherwise valid replacement waves.
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM tenant_pool_memberships m
		STRAIGHT_JOIN tenants t ON t.id = m.tenant_id
		STRAIGHT_JOIN fs_registry f ON f.tenant_id = t.id
		STRAIGHT_JOIN tenant_placements p ON p.fs_id = f.fs_id
		STRAIGHT_JOIN db_pool d ON d.db_id = p.db_id
		WHERE m.pool_id = ? AND m.tidbcloud_organization_id = ? AND m.pool_status = ? AND t.provider = ?
			AND t.status IN (?, ?) AND d.status IN (?, ?, ?) FOR UPDATE`,
		poolID, organizationID, TenantPoolBindingFree, tidbCloudNativeSharedProvider,
		TenantPending, TenantProvisioning, SharedDBStatusPending, SharedDBStatusProvisioning, SharedDBStatusActive).
		Scan(&sharedPlannedSlots)
	if err != nil {
		return fmt.Errorf("count logical tenant pool %q planned shared slots: %w", poolID, err)
	}
	current := nativeSlots + sharedActiveSlots + sharedPlannedSlots
	if current+incoming > capacityLimit {
		return fmt.Errorf("logical tenant pool %q capacity changed: durable=%d incoming=%d size=%d", poolID, current, incoming, capacityLimit)
	}
	if resize != nil {
		res, updateErr := tx.ExecContext(ctx, `UPDATE tenant_tidbcloud_pools
			SET size = ?, updated_at = ? WHERE pool_id = ? AND size = ? AND status = ?`,
			resize.TargetSize, time.Now().UTC(), poolID, resize.ExpectedSize, TenantPoolActive)
		if updateErr != nil {
			return fmt.Errorf("grow logical tenant pool %q: %w", poolID, updateErr)
		}
		updated, rowsErr := res.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("grow logical tenant pool %q rows affected: %w", poolID, rowsErr)
		}
		if updated != 1 {
			return fmt.Errorf("grow logical tenant pool %q affected %d rows, want 1", poolID, updated)
		}
	}
	return nil
}

func finalizeManagedSharedDBPoolWaveMembers(ctx context.Context, tx *sql.Tx, staged stagedManagedSharedDBPoolWavePlan) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_placements
		(fs_id, db_id, placement, schema_shape, status, target_db_id) VALUES `+
		multiRowPlaceholders(len(staged.tenantIDs), 6), staged.placementArgs...); err != nil {
		return fmt.Errorf("insert wave placements: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_pool_memberships
		(tenant_id, tidbcloud_organization_id, pool_id, pool_status, created_at, updated_at) VALUES `+
		multiRowPlaceholders(len(staged.tenantIDs), 6), staged.membershipArgs...); err != nil {
		return fmt.Errorf("insert wave memberships: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE db_pool
		SET soft_cap_reached = CASE WHEN tenant_count + ? >= max_tenants THEN 1 ELSE 0 END,
			tenant_count = tenant_count + ?
		WHERE db_id = ? AND status = ? AND max_tenants > 0 AND soft_cap_reached = 0
			AND tenant_count + ? <= max_tenants`,
		len(staged.tenantIDs), len(staged.tenantIDs), staged.dbID, staged.dbStatus, len(staged.tenantIDs))
	if err != nil {
		return fmt.Errorf("reserve wave physical capacity: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("reserve wave physical capacity rows affected: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("reserve wave physical capacity changed %d rows for db pool %d", affected, staged.dbID)
	}
	return nil
}
