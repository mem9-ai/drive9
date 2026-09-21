package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

// PromotionRetireAllocationRequest serializes an undispatched allocation with
// first CreateImport. A valid allocation proof is required because no owner or
// recovery token exists before CreateImport wins.
type PromotionRetireAllocationRequest struct {
	TenantID        string
	MigrationID     string
	AllocationProof string
	WriterLease     string
}

// PromotionRetireAllocationResult reports the sole durable winner. Import is
// populated when CreateImport won; Retired is true when the allocation can no
// longer be accepted.
type PromotionRetireAllocationResult struct {
	Retired bool
	Import  *PromotionImport
}

// PromotionAcknowledgeResultRequest acknowledges a terminal result only after
// the coordinator has durably recorded its corresponding local phase.
type PromotionAcknowledgeResultRequest struct {
	TenantID             string
	MigrationID          string
	RecoveryToken        string
	TerminalState        string
	TerminalResultDigest string
	WriterLease          string
}

// PromotionRetentionSweepRequest bounds one normal-generation compaction and
// retirement pass. Allocation claims use the same offline-recovery window as
// terminal results before they become eligible for irreversible retirement.
type PromotionRetentionSweepRequest struct {
	TenantID    string
	WriterLease string
	Limit       int
}

// PromotionRetentionSweepResult separates representation compaction from
// irreversible identity retirement for observability and tests.
type PromotionRetentionSweepResult struct {
	Compacted int
	Retired   int
}

type promotionTerminalRetentionRow struct {
	target                       string
	allocationProofDigest        string
	requestDigest                string
	ownerTokenHash               string
	recoveryTokenHash            string
	terminalState                string
	terminalResultBlob           string
	terminalResultDigest         string
	containmentLineageID         sql.NullString
	containmentPosition          sql.NullString
	containmentCheckpointDigest  sql.NullString
	sourceReleaseState           string
	sourceReleaseCertificate     sql.NullString
	sourceReleaseFloorCheckpoint sql.NullString
	terminalAt                   time.Time
	resultAcknowledgedAt         sql.NullTime
	fullRowCompactNotBefore      time.Time
	retireAfter                  time.Time
}

// RetireImportAllocation makes an undispatched allocation irreversibly
// retired or returns the accepted import that won the same claim-row lock.
func (s *PromotionStore) RetireImportAllocation(ctx context.Context, req PromotionRetireAllocationRequest) (*PromotionRetireAllocationResult, error) {
	claims, identity, err := s.verifyPromotionAllocationProof(req.TenantID, req.MigrationID, req.AllocationProof)
	if err != nil || req.WriterLease == "" {
		return nil, ErrNotFound
	}
	if err := s.cfg.Authorizer.AuthorizePromotionWrite(ctx, req.TenantID, claims.Target); err != nil {
		return nil, ErrNotFound
	}

	var out PromotionRetireAllocationResult
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, _, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		var target, proofDigest, claimState string
		err = tx.QueryRowContext(ctx, `SELECT target_path, allocation_proof_digest, claim_state
			FROM promotion_import_id_claims
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence).
			Scan(&target, &proofDigest, &claimState)
		if errors.Is(err, sql.ErrNoRows) {
			retired, checkErr := promotionIdentityRetiredTx(ctx, tx, req.TenantID, identity)
			if checkErr != nil {
				return checkErr
			}
			if !retired {
				return ErrPromotionRecoveryRequired
			}
			out.Retired = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock promotion allocation retirement: %w", err)
		}
		if target != claims.Target || !promotionHashEqual(proofDigest, promotionHashString(req.AllocationProof)) {
			return ErrNotFound
		}
		switch claimState {
		case "ALLOCATED":
			result, err := tx.ExecContext(ctx, `UPDATE promotion_import_id_claims
				SET claim_state = 'RETIRED', retired_at = CURRENT_TIMESTAMP(3)
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
				  AND claim_state = 'ALLOCATED'`, req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
			if err != nil {
				return fmt.Errorf("retire promotion allocation claim: %w", err)
			}
			if affected, err := result.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionConflict
			}
			if tenant.LiveClaimCount == 0 {
				return ErrPromotionRecoveryRequired
			}
			updated, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_tenants
				SET live_claim_count = live_claim_count - 1 WHERE tenant_id = ? AND live_claim_count > 0`, req.TenantID)
			if err != nil {
				return fmt.Errorf("release promotion live claim: %w", err)
			}
			if affected, err := updated.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionRecoveryRequired
			}
			if err := retirePromotionIdentityTx(ctx, tx, req.TenantID, identity, tenant); err != nil {
				return err
			}
			deleted, err := tx.ExecContext(ctx, `DELETE FROM promotion_import_id_claims
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? AND claim_state = 'RETIRED'`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
			if err != nil {
				return fmt.Errorf("delete retired promotion allocation claim: %w", err)
			}
			if affected, err := deleted.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionRecoveryRequired
			}
			out.Retired = true
			return nil
		case "RETIRED":
			out.Retired = true
			return nil
		case "ACCEPTED":
			accepted, err := selectPromotionStatusTx(ctx, tx, req.TenantID, req.MigrationID, identity)
			if err != nil {
				return err
			}
			out.Import = accepted
			return nil
		default:
			return ErrPromotionRecoveryRequired
		}
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// AcknowledgeImportResult durably records the exact terminal result digest.
// Full-row compaction may use this acknowledgement only after its stored
// not-before time; tombstone retries return the same immutable result.
func (s *PromotionStore) AcknowledgeImportResult(ctx context.Context, req PromotionAcknowledgeResultRequest) (*PromotionImport, error) {
	if req.TenantID == "" || req.RecoveryToken == "" || len(req.RecoveryToken) > 512 ||
		(req.TerminalState != "COMMITTED" && req.TerminalState != "ABORTED") ||
		!validPromotionSHA256(req.TerminalResultDigest) || req.WriterLease == "" {
		return nil, ErrPromotionConflict
	}
	identity, err := promotion.DecodeMigrationID(req.MigrationID)
	if err != nil {
		return nil, ErrNotFound
	}
	target, err := s.readPromotionContinuationTarget(ctx, req.TenantID, identity)
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Authorizer.AuthorizePromotionWrite(ctx, req.TenantID, target); err != nil {
		return nil, ErrNotFound
	}

	var out *PromotionImport
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		_, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if errors.Is(err, ErrNotFound) {
			var tombstoneTarget, state, blob, digest, recoveryHash string
			if err := tx.QueryRowContext(ctx, `SELECT target_path, terminal_state,
				terminal_result_blob, terminal_result_digest, recovery_token_hash
				FROM promotion_import_tombstones
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence).
				Scan(&tombstoneTarget, &state, &blob, &digest, &recoveryHash); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("lock promotion result tombstone acknowledgement: %w", err)
			}
			if !promotionHashEqual(recoveryHash, promotionHashString(req.RecoveryToken)) {
				return ErrNotFound
			}
			if tombstoneTarget != target || state != req.TerminalState ||
				!promotionHashEqual(digest, req.TerminalResultDigest) {
				return ErrPromotionConflict
			}
			if err := validatePromotionTerminalResult(state,
				sql.NullString{String: blob, Valid: true},
				sql.NullString{String: digest, Valid: true}); err != nil {
				return err
			}
			out = &PromotionImport{
				MigrationID: req.MigrationID, AllocationEpoch: identity.AllocationEpoch,
				AllocationSequence: identity.AllocationSequence, Target: tombstoneTarget,
				State: state, TerminalResultBlob: blob, TerminalResultDigest: digest,
				RestoreGeneration:   namespace.restoreGeneration,
				DatabaseIncarnation: namespace.databaseIncarnation,
				WriterGeneration:    namespace.writerGeneration,
			}
			return nil
		}
		if err != nil {
			return err
		}
		if row.target != target || !promotionHashEqual(row.recoveryTokenHash, promotionHashString(req.RecoveryToken)) {
			return ErrNotFound
		}
		if row.state != req.TerminalState || !row.terminalResultBlob.Valid || !row.terminalResultDigest.Valid ||
			!promotionHashEqual(row.terminalResultDigest.String, req.TerminalResultDigest) {
			return ErrPromotionConflict
		}
		if err := validatePromotionTerminalResult(row.state, row.terminalResultBlob, row.terminalResultDigest); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE promotion_imports
			SET result_acknowledged_at = COALESCE(result_acknowledged_at, CURRENT_TIMESTAMP(3))
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND state = ? AND terminal_result_digest = ?`, req.TenantID, identity.AllocationEpoch,
			identity.AllocationSequence, req.TerminalState, req.TerminalResultDigest)
		if err != nil {
			return fmt.Errorf("acknowledge promotion terminal result: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected > 1 {
			return ErrPromotionRecoveryRequired
		}
		out = row.response(req.MigrationID)
		out.TerminalResultBlob = row.terminalResultBlob.String
		out.TerminalResultDigest = row.terminalResultDigest.String
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SweepPromotionRetention compacts terminal full rows, retires eligible
// tombstones, and retires abandoned allocations. Every destructive mutation
// holds the normal writer guard and the exact epoch counter lock.
func (s *PromotionStore) SweepPromotionRetention(ctx context.Context, req PromotionRetentionSweepRequest) (PromotionRetentionSweepResult, error) {
	if req.TenantID == "" || req.WriterLease == "" || req.Limit <= 0 || req.Limit > 1000 {
		return PromotionRetentionSweepResult{}, ErrPromotionConflict
	}
	var out PromotionRetentionSweepResult
	identities, err := s.scanPromotionTerminalCompactionCandidates(ctx, req)
	if err != nil {
		return out, err
	}
	for _, identity := range identities {
		compacted, err := s.compactPromotionTerminal(ctx, req, identity)
		if err != nil {
			return out, err
		}
		if compacted {
			out.Compacted++
		}
	}

	remaining := req.Limit
	if out.Compacted < remaining {
		remaining -= out.Compacted
	} else {
		return out, nil
	}
	retirementCandidates, err := s.scanPromotionRetirementCandidates(ctx, req.TenantID, remaining)
	if err != nil {
		return out, err
	}
	for _, candidate := range retirementCandidates {
		retired, err := s.retirePromotionCandidate(ctx, req, candidate.identity, candidate.kind)
		if err != nil {
			return out, err
		}
		if retired {
			out.Retired++
		}
	}
	return out, nil
}

func (s *PromotionStore) verifyPromotionAllocationProof(tenantID, migrationID, proof string) (promotion.AllocationProofClaims, promotion.MigrationIdentity, error) {
	if tenantID == "" || proof == "" || len(proof) > 8192 {
		return promotion.AllocationProofClaims{}, promotion.MigrationIdentity{}, ErrNotFound
	}
	identity, err := promotion.DecodeMigrationID(migrationID)
	if err != nil {
		return promotion.AllocationProofClaims{}, promotion.MigrationIdentity{}, ErrNotFound
	}
	claims, err := s.cfg.ProofSigner.Verify(proof)
	if err != nil || claims.TenantID != tenantID || claims.MigrationID != migrationID ||
		claims.AllocationEpoch != identity.AllocationEpoch || claims.AllocationSequence != identity.AllocationSequence ||
		claims.Target == "" {
		return promotion.AllocationProofClaims{}, promotion.MigrationIdentity{}, ErrNotFound
	}
	return claims, identity, nil
}

// classifyMissingPromotionProof distinguishes a retired identity from an
// allocated request that has not yet won CreateImport. The signed proof and
// target authorization have already been checked by the caller. A proof for a
// missing epoch or an impossible gap is storage corruption/outcome-unknown,
// never permission to recreate the identity.
func (s *PromotionStore) classifyMissingPromotionProof(ctx context.Context, tenantID string, identity promotion.MigrationIdentity) error {
	var claimState string
	err := s.store.db.QueryRowContext(ctx, `SELECT claim_state FROM promotion_import_id_claims
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&claimState)
	if err == nil {
		switch claimState {
		case "ALLOCATED":
			return ErrNotFound
		case "RETIRED":
			return ErrPromotionIDRetired
		case "ACCEPTED":
			return ErrPromotionRecoveryRequired
		default:
			return ErrPromotionRecoveryRequired
		}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read promotion missing-proof claim: %w", err)
	}

	var lastIssued, retiredThrough uint64
	err = s.store.db.QueryRowContext(ctx, `SELECT last_issued_sequence, retired_through
		FROM promotion_import_identity_epochs WHERE tenant_id = ? AND allocation_epoch = ?`,
		tenantID, identity.AllocationEpoch).Scan(&lastIssued, &retiredThrough)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrPromotionRecoveryRequired
	}
	if err != nil {
		return fmt.Errorf("read promotion missing-proof epoch: %w", err)
	}
	if identity.AllocationSequence <= retiredThrough {
		return ErrPromotionIDRetired
	}
	var one int
	err = s.store.db.QueryRowContext(ctx, `SELECT 1 FROM promotion_retired_import_sequences
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&one)
	if err == nil {
		return ErrPromotionIDRetired
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read promotion missing-proof sparse retirement: %w", err)
	}
	// A server-authenticated proof cannot legitimately name an unissued
	// sequence or a materialized sequence whose claim/import/tombstone vanished.
	if lastIssued < retiredThrough || identity.AllocationSequence == 0 {
		return ErrPromotionRecoveryRequired
	}
	return ErrPromotionRecoveryRequired
}

func selectPromotionStatusTx(ctx context.Context, tx *sql.Tx, tenantID, migrationID string, identity promotion.MigrationIdentity) (*PromotionImport, error) {
	var out PromotionImport
	var blob, digest sql.NullString
	fromTombstone := false
	err := tx.QueryRowContext(ctx, `SELECT target_path, manifest_hash, quota_reservation_id, state,
		state_version, owner_epoch, activity_deadline, lease_expires_at,
		accepted_restore_generation, accepted_database_incarnation, accepted_writer_generation,
		terminal_result_blob, terminal_result_digest
		FROM promotion_imports WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&out.Target, &out.ManifestHash, &out.QuotaReservationID, &out.State, &out.StateVersion,
			&out.OwnerEpoch, &out.ActivityDeadline, &out.LeaseExpiresAt, &out.RestoreGeneration,
			&out.DatabaseIncarnation, &out.WriterGeneration, &blob, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		fromTombstone = true
		err = tx.QueryRowContext(ctx, `SELECT target_path, terminal_state, terminal_result_blob, terminal_result_digest
			FROM promotion_import_tombstones
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
			tenantID, identity.AllocationEpoch, identity.AllocationSequence).
			Scan(&out.Target, &out.State, &blob, &digest)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPromotionRecoveryRequired
	}
	if err != nil {
		return nil, fmt.Errorf("read promotion accepted status: %w", err)
	}
	if err := validatePromotionTerminalResult(out.State, blob, digest); err != nil {
		return nil, err
	}
	if fromTombstone {
		if err := tx.QueryRowContext(ctx, `SELECT restore_generation, database_incarnation, writer_generation
			FROM promotion_namespace_capabilities WHERE tenant_id = ? FOR UPDATE`, tenantID).
			Scan(&out.RestoreGeneration, &out.DatabaseIncarnation, &out.WriterGeneration); err != nil {
			return nil, ErrPromotionRecoveryRequired
		}
	}
	out.MigrationID = migrationID
	out.AllocationEpoch = identity.AllocationEpoch
	out.AllocationSequence = identity.AllocationSequence
	if blob.Valid {
		out.TerminalResultBlob = blob.String
		out.TerminalResultDigest = digest.String
	}
	return &out, nil
}

func promotionIdentityRetiredTx(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity) (bool, error) {
	var retiredThrough uint64
	err := tx.QueryRowContext(ctx, `SELECT retired_through FROM promotion_import_identity_epochs
		WHERE tenant_id = ? AND allocation_epoch = ? FOR UPDATE`, tenantID, identity.AllocationEpoch).Scan(&retiredThrough)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrPromotionRecoveryRequired
	}
	if err != nil {
		return false, fmt.Errorf("read promotion retired watermark: %w", err)
	}
	if identity.AllocationSequence <= retiredThrough {
		return true, nil
	}
	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM promotion_retired_import_sequences
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&one)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read promotion sparse retirement: %w", err)
	}
	return false, nil
}

func retirePromotionIdentityTx(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity, tenant promotionIdentityTenant) error {
	var globalCount uint64
	if err := tx.QueryRowContext(ctx, `SELECT materialized_identity_count FROM promotion_import_identity_global
		WHERE capacity_key = ? FOR UPDATE`, promotionGlobalCapacityKey).Scan(&globalCount); err != nil {
		return ErrPromotionRecoveryRequired
	}
	var lastIssued, retiredThrough uint64
	var codecVersion, epochState string
	if err := tx.QueryRowContext(ctx, `SELECT last_issued_sequence, retired_through, id_codec_version, epoch_state
		FROM promotion_import_identity_epochs
		WHERE tenant_id = ? AND allocation_epoch = ? FOR UPDATE`, tenantID, identity.AllocationEpoch).
		Scan(&lastIssued, &retiredThrough, &codecVersion, &epochState); err != nil {
		return ErrPromotionRecoveryRequired
	}
	if codecVersion != "p1" || (epochState != "ACTIVE" && epochState != "SEALED") ||
		lastIssued < retiredThrough || identity.AllocationSequence > lastIssued ||
		tenant.MaterializedCount == 0 || globalCount == 0 {
		return ErrPromotionRecoveryRequired
	}
	if identity.AllocationSequence <= retiredThrough {
		return ErrPromotionRecoveryRequired
	}
	var marker int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM promotion_retired_import_sequences
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&marker)
	if err == nil {
		return ErrPromotionRecoveryRequired
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read promotion retirement marker: %w", err)
	}
	if identity.AllocationSequence > retiredThrough+1 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO promotion_retired_import_sequences
			(tenant_id, allocation_epoch, allocation_sequence) VALUES (?, ?, ?)`,
			tenantID, identity.AllocationEpoch, identity.AllocationSequence); err != nil {
			return fmt.Errorf("insert promotion sparse retirement: %w", err)
		}
		return nil
	}

	newWatermark := identity.AllocationSequence
	rows, err := tx.QueryContext(ctx, `SELECT allocation_sequence FROM promotion_retired_import_sequences
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence > ?
		ORDER BY allocation_sequence FOR UPDATE`, tenantID, identity.AllocationEpoch, newWatermark)
	if err != nil {
		return fmt.Errorf("lock promotion sparse retirements: %w", err)
	}
	var consumed []uint64
	for rows.Next() {
		var sequence uint64
		if err := rows.Scan(&sequence); err != nil {
			_ = rows.Close()
			return err
		}
		if sequence != newWatermark+1 {
			break
		}
		newWatermark = sequence
		consumed = append(consumed, sequence)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	decrement := uint64(1 + len(consumed))
	if tenant.MaterializedCount < decrement || globalCount < decrement {
		return ErrPromotionRecoveryRequired
	}
	for _, sequence := range consumed {
		deleted, err := tx.ExecContext(ctx, `DELETE FROM promotion_retired_import_sequences
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
			tenantID, identity.AllocationEpoch, sequence)
		if err != nil {
			return fmt.Errorf("consume promotion sparse retirement: %w", err)
		}
		if affected, err := deleted.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionRecoveryRequired
		}
	}
	updatedEpoch, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_epochs SET retired_through = ?
		WHERE tenant_id = ? AND allocation_epoch = ? AND retired_through = ?`,
		newWatermark, tenantID, identity.AllocationEpoch, retiredThrough)
	if err != nil {
		return fmt.Errorf("advance promotion retirement watermark: %w", err)
	}
	if affected, err := updatedEpoch.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionRecoveryRequired
	}
	updatedTenant, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_tenants
		SET materialized_identity_count = materialized_identity_count - ?
		WHERE tenant_id = ? AND materialized_identity_count >= ?`, decrement, tenantID, decrement)
	if err != nil {
		return fmt.Errorf("release promotion tenant identity capacity: %w", err)
	}
	if affected, err := updatedTenant.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionRecoveryRequired
	}
	updatedGlobal, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_global
		SET materialized_identity_count = materialized_identity_count - ?
		WHERE capacity_key = ? AND materialized_identity_count >= ?`, decrement, promotionGlobalCapacityKey, decrement)
	if err != nil {
		return fmt.Errorf("release promotion global identity capacity: %w", err)
	}
	if affected, err := updatedGlobal.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionRecoveryRequired
	}
	return nil
}

func (s *PromotionStore) scanPromotionTerminalCompactionCandidates(ctx context.Context, req PromotionRetentionSweepRequest) ([]promotion.MigrationIdentity, error) {
	rows, err := s.store.db.QueryContext(ctx, `SELECT allocation_epoch, allocation_sequence FROM promotion_imports
		WHERE tenant_id = ? AND state IN ('COMMITTED', 'ABORTED') AND (
			(result_acknowledged_at IS NOT NULL AND full_row_compact_not_before <= CURRENT_TIMESTAMP(3)) OR
			terminal_at <= DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL ? MICROSECOND)
		) ORDER BY terminal_at, allocation_epoch, allocation_sequence LIMIT ?`,
		req.TenantID, s.cfg.FullRowMaxRetention.Microseconds(), req.Limit)
	if err != nil {
		return nil, fmt.Errorf("scan promotion terminal compaction: %w", err)
	}
	defer rows.Close()
	var identities []promotion.MigrationIdentity
	for rows.Next() {
		var identity promotion.MigrationIdentity
		if err := rows.Scan(&identity.AllocationEpoch, &identity.AllocationSequence); err != nil {
			return nil, err
		}
		identities = append(identities, identity)
	}
	return identities, rows.Err()
}

func (s *PromotionStore) compactPromotionTerminal(ctx context.Context, req PromotionRetentionSweepRequest, identity promotion.MigrationIdentity) (bool, error) {
	compacted := false
	err := s.store.InTx(ctx, func(tx *sql.Tx) error {
		_, _, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := lockPromotionTerminalRetentionRow(ctx, tx, req.TenantID, identity)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := validatePromotionTerminalResult(row.terminalState,
			sql.NullString{String: row.terminalResultBlob, Valid: true},
			sql.NullString{String: row.terminalResultDigest, Valid: true}); err != nil {
			return err
		}
		now, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		eligible := row.resultAcknowledgedAt.Valid && !now.Before(row.fullRowCompactNotBefore)
		if !eligible && !now.Before(row.terminalAt.Add(s.cfg.FullRowMaxRetention)) {
			eligible = true
		}
		if !eligible {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO promotion_import_tombstones
			(tenant_id, allocation_epoch, allocation_sequence, allocation_proof_digest,
			 target_path, target_path_hash, request_digest, owner_token_hash, recovery_token_hash,
			 terminal_state, terminal_result_blob, terminal_result_digest,
			 commit_containment_lineage_id, commit_containment_position,
			 commit_containment_checkpoint_digest, source_release_state,
			 source_release_certificate_digest, source_release_floor_checkpoint, retire_after)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence, row.allocationProofDigest,
			row.target, fileNodePathHash(row.target), row.requestDigest, row.ownerTokenHash, row.recoveryTokenHash,
			row.terminalState, row.terminalResultBlob, row.terminalResultDigest,
			row.containmentLineageID, row.containmentPosition, row.containmentCheckpointDigest,
			row.sourceReleaseState, row.sourceReleaseCertificate, row.sourceReleaseFloorCheckpoint, row.retireAfter); err != nil {
			return fmt.Errorf("insert promotion terminal tombstone: %w", err)
		}
		for _, statement := range []string{
			`DELETE FROM promotion_import_entries WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
			`DELETE FROM promotion_import_contents WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		} {
			if _, err := tx.ExecContext(ctx, statement, req.TenantID, identity.AllocationEpoch, identity.AllocationSequence); err != nil {
				return fmt.Errorf("compact promotion terminal staging: %w", err)
			}
		}
		deletedReservation, err := tx.ExecContext(ctx, `DELETE FROM promotion_quota_reservations
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
		if err != nil {
			return fmt.Errorf("compact promotion terminal reservation: %w", err)
		}
		if affected, err := deletedReservation.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionRecoveryRequired
		}
		deletedImport, err := tx.ExecContext(ctx, `DELETE FROM promotion_imports
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? AND state IN ('COMMITTED', 'ABORTED')`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
		if err != nil {
			return fmt.Errorf("compact promotion terminal import: %w", err)
		}
		if affected, err := deletedImport.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionRecoveryRequired
		}
		compacted = true
		return nil
	})
	return compacted, err
}

type promotionRetirementCandidate struct {
	identity promotion.MigrationIdentity
	kind     string
}

func (s *PromotionStore) scanPromotionRetirementCandidates(ctx context.Context, tenantID string, limit int) ([]promotionRetirementCandidate, error) {
	rows, err := s.store.db.QueryContext(ctx, `(SELECT allocation_epoch, allocation_sequence, 'tombstone' AS kind, retire_after AS due_at
		FROM promotion_import_tombstones WHERE tenant_id = ? AND retire_after <= CURRENT_TIMESTAMP(3)
		  AND (terminal_state = 'ABORTED' OR source_release_state = 'ACKNOWLEDGED'))
		UNION ALL
		(SELECT allocation_epoch, allocation_sequence, 'claim' AS kind,
		 DATE_ADD(create_before, INTERVAL ? MICROSECOND) AS due_at
		 FROM promotion_import_id_claims WHERE tenant_id = ? AND claim_state = 'ALLOCATED'
		  AND create_before <= DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL ? MICROSECOND))
		ORDER BY due_at, allocation_epoch, allocation_sequence LIMIT ?`,
		tenantID, s.cfg.TerminalRetention.Microseconds(), tenantID,
		s.cfg.TerminalRetention.Microseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("scan promotion retirement candidates: %w", err)
	}
	defer rows.Close()
	var out []promotionRetirementCandidate
	for rows.Next() {
		var candidate promotionRetirementCandidate
		var dueAt time.Time
		if err := rows.Scan(&candidate.identity.AllocationEpoch, &candidate.identity.AllocationSequence, &candidate.kind, &dueAt); err != nil {
			return nil, err
		}
		out = append(out, candidate)
	}
	return out, rows.Err()
}

func (s *PromotionStore) retirePromotionCandidate(ctx context.Context, req PromotionRetentionSweepRequest, identity promotion.MigrationIdentity, kind string) (bool, error) {
	retired := false
	err := s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, _, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		now, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		switch kind {
		case "tombstone":
			var state, sourceRelease string
			var retireAfter time.Time
			err := tx.QueryRowContext(ctx, `SELECT terminal_state, source_release_state, retire_after
				FROM promotion_import_tombstones WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence).
				Scan(&state, &sourceRelease, &retireAfter)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			if now.Before(retireAfter) || (state != "ABORTED" && !(state == "COMMITTED" && sourceRelease == "ACKNOWLEDGED")) {
				return nil
			}
			deletedTombstone, err := tx.ExecContext(ctx, `DELETE FROM promotion_import_tombstones
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
			if err != nil {
				return err
			}
			if affected, err := deletedTombstone.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionRecoveryRequired
			}
			deletedClaim, err := tx.ExecContext(ctx, `DELETE FROM promotion_import_id_claims
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? AND claim_state = 'ACCEPTED'`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
			if err != nil {
				return err
			}
			if affected, err := deletedClaim.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionRecoveryRequired
			}
		case "claim":
			var createBefore time.Time
			var claimState string
			err := tx.QueryRowContext(ctx, `SELECT create_before, claim_state FROM promotion_import_id_claims
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&createBefore, &claimState)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			if claimState != "ALLOCATED" || now.Before(createBefore.Add(s.cfg.TerminalRetention)) {
				return nil
			}
			if tenant.LiveClaimCount == 0 {
				return ErrPromotionRecoveryRequired
			}
			deletedClaim, err := tx.ExecContext(ctx, `DELETE FROM promotion_import_id_claims
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? AND claim_state = 'ALLOCATED'`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
			if err != nil {
				return err
			}
			if affected, err := deletedClaim.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionRecoveryRequired
			}
			updated, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_tenants
				SET live_claim_count = live_claim_count - 1 WHERE tenant_id = ? AND live_claim_count > 0`, req.TenantID)
			if err != nil {
				return err
			}
			if affected, err := updated.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionRecoveryRequired
			}
		default:
			return ErrPromotionRecoveryRequired
		}
		if err := retirePromotionIdentityTx(ctx, tx, req.TenantID, identity, tenant); err != nil {
			return err
		}
		retired = true
		return nil
	})
	return retired, err
}

func lockPromotionTerminalRetentionRow(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity) (*promotionTerminalRetentionRow, error) {
	row := &promotionTerminalRetentionRow{}
	err := tx.QueryRowContext(ctx, `SELECT target_path, allocation_proof_digest, create_request_digest,
		owner_token_hash, recovery_token_hash, state, terminal_result_blob, terminal_result_digest,
		commit_containment_lineage_id, commit_containment_position, commit_containment_checkpoint_digest,
		source_release_state, source_release_certificate_digest, source_release_floor_checkpoint,
		terminal_at, result_acknowledged_at, full_row_compact_not_before, retire_after
		FROM promotion_imports WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&row.target, &row.allocationProofDigest, &row.requestDigest, &row.ownerTokenHash,
			&row.recoveryTokenHash, &row.terminalState, &row.terminalResultBlob, &row.terminalResultDigest,
			&row.containmentLineageID, &row.containmentPosition, &row.containmentCheckpointDigest,
			&row.sourceReleaseState, &row.sourceReleaseCertificate, &row.sourceReleaseFloorCheckpoint,
			&row.terminalAt, &row.resultAcknowledgedAt, &row.fullRowCompactNotBefore, &row.retireAfter)
	return row, err
}

func (s *PromotionStore) readPromotionContinuationTarget(ctx context.Context, tenantID string, identity promotion.MigrationIdentity) (string, error) {
	var target string
	err := s.store.db.QueryRowContext(ctx, `SELECT target_path FROM promotion_imports
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&target)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.store.db.QueryRowContext(ctx, `SELECT target_path FROM promotion_import_tombstones
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
			tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&target)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("resolve promotion continuation target: %w", err)
	}
	return target, nil
}
