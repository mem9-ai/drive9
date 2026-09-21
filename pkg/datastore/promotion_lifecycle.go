package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

// PromotionGetRequest authenticates status recovery with exactly one durable
// recovery credential. A migration ID alone never reveals existence, target,
// state, or result.
type PromotionGetRequest struct {
	TenantID        string
	MigrationID     string
	RecoveryToken   string
	AllocationProof string
}

// PromotionTakeOverRequest transfers one expired client-owned import to a new
// owner epoch. Retrying the exact expected epoch and new owner token recovers
// the first durable result instead of advancing ownership twice.
type PromotionTakeOverRequest struct {
	TenantID           string
	MigrationID        string
	ExpectedOwnerEpoch uint64
	RecoveryToken      string
	NewOwnerToken      string
	WriterLease        string
}

// PromotionRenewRequest renews the current client owner without changing its
// epoch. Renewal is bounded by the immutable activity deadline.
type PromotionRenewRequest struct {
	TenantID    string
	MigrationID string
	OwnerEpoch  uint64
	OwnerToken  string
	WriterLease string
}

// PromotionSweepRequest bounds one normal-generation deadline/expired-owner
// cleanup pass. It is an internal worker contract, not a client credential.
type PromotionSweepRequest struct {
	TenantID    string
	WriterLease string
	Limit       int
}

type promotionImportStatusRow struct {
	allocationProofDigest string
	recoveryTokenHash     string
	target                string
	response              PromotionImport
	terminalResultBlob    sql.NullString
	terminalResultDigest  sql.NullString
}

// GetImport returns active or terminal state only after authenticating the
// current target read scope and an operation-bound recovery credential.
func (s *PromotionStore) GetImport(ctx context.Context, req PromotionGetRequest) (*PromotionImport, error) {
	if req.TenantID == "" || (req.RecoveryToken == "") == (req.AllocationProof == "") ||
		len(req.RecoveryToken) > 512 || len(req.AllocationProof) > 8192 {
		return nil, ErrNotFound
	}
	identity, err := promotion.DecodeMigrationID(req.MigrationID)
	if err != nil {
		return nil, ErrNotFound
	}

	var proofTarget, proofDigest string
	if req.AllocationProof != "" {
		claims, err := s.cfg.ProofSigner.Verify(req.AllocationProof)
		if err != nil || claims.TenantID != req.TenantID || claims.MigrationID != req.MigrationID ||
			claims.AllocationEpoch != identity.AllocationEpoch || claims.AllocationSequence != identity.AllocationSequence {
			return nil, ErrNotFound
		}
		proofTarget = claims.Target
		proofDigest = promotionHashString(req.AllocationProof)
		if err := s.cfg.Authorizer.AuthorizePromotionRead(ctx, req.TenantID, proofTarget); err != nil {
			return nil, ErrNotFound
		}
	}

	row, err := s.readPromotionImportStatus(ctx, req.TenantID, req.MigrationID, identity)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if proofTarget == "" {
		if err := s.cfg.Authorizer.AuthorizePromotionRead(ctx, req.TenantID, row.target); err != nil {
			return nil, ErrNotFound
		}
	}
	proofOK := proofTarget != "" && row.target == proofTarget &&
		promotionHashEqual(row.allocationProofDigest, proofDigest)
	recoveryOK := req.RecoveryToken != "" &&
		promotionHashEqual(row.recoveryTokenHash, promotionHashString(req.RecoveryToken))
	if !proofOK && !recoveryOK {
		return nil, ErrNotFound
	}
	if err := validatePromotionTerminalResult(row.response.State, row.terminalResultBlob, row.terminalResultDigest); err != nil {
		return nil, err
	}
	if row.terminalResultBlob.Valid {
		row.response.TerminalResultBlob = row.terminalResultBlob.String
		row.response.TerminalResultDigest = row.terminalResultDigest.String
	}
	return &row.response, nil
}

// TakeOverImport installs a new owner only after the previous lease expires.
// The restore/writer guard, import tuple, deadline and ownership CAS are all
// checked in the same transaction.
func (s *PromotionStore) TakeOverImport(ctx context.Context, req PromotionTakeOverRequest) (*PromotionImport, error) {
	if req.TenantID == "" || req.ExpectedOwnerEpoch == 0 || req.RecoveryToken == "" ||
		len(req.RecoveryToken) > 512 || req.NewOwnerToken == "" || len(req.NewOwnerToken) > 512 || req.WriterLease == "" {
		return nil, ErrPromotionConflict
	}
	identity, err := promotion.DecodeMigrationID(req.MigrationID)
	if err != nil {
		return nil, ErrNotFound
	}
	target, err := s.authorizePromotionMutationTarget(ctx, req.TenantID, identity)
	if err != nil {
		return nil, err
	}

	var out *PromotionImport
	deadlineWon := false
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if row.target != target || !promotionHashEqual(row.recoveryTokenHash, promotionHashString(req.RecoveryToken)) {
			return ErrPromotionConflict
		}
		if err := validatePromotionImportIncarnation(row, tenant, namespace); err != nil {
			return err
		}
		if row.state != "CREATED" && row.state != "STAGING" && row.state != "VERIFIED" {
			return ErrPromotionConflict
		}

		lockedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, lockedAt); err != nil {
			return err
		}

		newOwnerHash := promotionHashString(req.NewOwnerToken)
		if row.ownerEpoch == req.ExpectedOwnerEpoch+1 && promotionHashEqual(row.ownerTokenHash, newOwnerHash) {
			out = row.response(req.MigrationID)
			return nil
		}
		if !lockedAt.Before(row.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, row, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if row.ownerEpoch != req.ExpectedOwnerEpoch || lockedAt.Before(row.leaseExpiresAt) {
			return ErrPromotionConflict
		}

		leaseExpiresAt := lockedAt.Add(s.cfg.LeaseTTL)
		if leaseExpiresAt.After(row.activityDeadline) {
			leaseExpiresAt = row.activityDeadline
		}
		result, err := tx.ExecContext(ctx, `UPDATE promotion_imports
			SET owner_epoch = owner_epoch + 1, owner_token_hash = ?,
			    lease_expires_at = ?, state_version = state_version + 1
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND owner_epoch = ? AND state = ? AND state_version = ?`,
			newOwnerHash, leaseExpiresAt, req.TenantID, identity.AllocationEpoch,
			identity.AllocationSequence, row.ownerEpoch, row.state, row.stateVersion)
		if err != nil {
			return fmt.Errorf("take over promotion import: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		row.ownerEpoch++
		row.ownerTokenHash = newOwnerHash
		row.leaseExpiresAt = leaseExpiresAt
		row.stateVersion++
		out = row.response(req.MigrationID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if deadlineWon {
		return nil, ErrPromotionDeadlineExceeded
	}
	return out, nil
}

// RenewImportLease extends a live owner lease only near its renewal boundary.
// An immediate lost-response retry therefore returns the already-durable
// expiry and version rather than extending the lease twice.
func (s *PromotionStore) RenewImportLease(ctx context.Context, req PromotionRenewRequest) (*PromotionImport, error) {
	if req.TenantID == "" || req.OwnerEpoch == 0 || req.OwnerToken == "" ||
		len(req.OwnerToken) > 512 || req.WriterLease == "" {
		return nil, ErrPromotionConflict
	}
	identity, err := promotion.DecodeMigrationID(req.MigrationID)
	if err != nil {
		return nil, ErrNotFound
	}
	target, err := s.authorizePromotionMutationTarget(ctx, req.TenantID, identity)
	if err != nil {
		return nil, err
	}

	var out *PromotionImport
	deadlineWon := false
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if row.target != target {
			return ErrPromotionRecoveryRequired
		}
		if err := validatePromotionOwnerMutation(row, tenant, namespace, req.OwnerEpoch, req.OwnerToken); err != nil {
			return err
		}
		if row.state != "CREATED" && row.state != "STAGING" && row.state != "VERIFIED" {
			return ErrPromotionConflict
		}
		lockedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, lockedAt); err != nil {
			return err
		}
		if !lockedAt.Before(row.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, row, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if !lockedAt.Before(row.leaseExpiresAt) {
			return ErrPromotionConflict
		}

		// Renewal is accepted only in the final half of the current TTL. This
		// makes the ordinary response-loss retry recover the same stored result.
		if row.leaseExpiresAt.Sub(lockedAt) > s.cfg.LeaseTTL/2 {
			out = row.response(req.MigrationID)
			return nil
		}
		leaseExpiresAt := lockedAt.Add(s.cfg.LeaseTTL)
		if leaseExpiresAt.After(row.activityDeadline) {
			leaseExpiresAt = row.activityDeadline
		}
		if !leaseExpiresAt.After(row.leaseExpiresAt) {
			out = row.response(req.MigrationID)
			return nil
		}
		result, err := tx.ExecContext(ctx, `UPDATE promotion_imports
			SET lease_expires_at = ?, state_version = state_version + 1
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND owner_epoch = ? AND state = ? AND state_version = ?`,
			leaseExpiresAt, req.TenantID, identity.AllocationEpoch, identity.AllocationSequence,
			row.ownerEpoch, row.state, row.stateVersion)
		if err != nil {
			return fmt.Errorf("renew promotion import lease: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		row.leaseExpiresAt = leaseExpiresAt
		row.stateVersion++
		out = row.response(req.MigrationID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if deadlineWon {
		return nil, ErrPromotionDeadlineExceeded
	}
	return out, nil
}

// SweepPromotionImports claims due deadline/expired-owner cleanup under the
// current normal writer generation and resumes already-owned cleanup attempts.
// Every claim and its unique cleanup tuple are one transaction; final cleanup
// remains exactly-once through the reservation and import state CASes.
func (s *PromotionStore) SweepPromotionImports(ctx context.Context, req PromotionSweepRequest) (int, error) {
	if req.TenantID == "" || req.WriterLease == "" || req.Limit <= 0 || req.Limit > 1000 {
		return 0, ErrPromotionConflict
	}
	rows, err := s.store.db.QueryContext(ctx, `SELECT allocation_epoch, allocation_sequence
		FROM promotion_imports
		WHERE tenant_id = ? AND (
			state = 'ABORTING' OR
			(state IN ('CREATED', 'STAGING', 'VERIFIED') AND
			 (activity_deadline <= CURRENT_TIMESTAMP(3) OR lease_expires_at <= CURRENT_TIMESTAMP(3)))
		)
		ORDER BY activity_deadline, allocation_epoch, allocation_sequence
		LIMIT ?`, req.TenantID, req.Limit)
	if err != nil {
		return 0, fmt.Errorf("scan promotion cleanup candidates: %w", err)
	}
	var identities []promotion.MigrationIdentity
	for rows.Next() {
		var identity promotion.MigrationIdentity
		if err := rows.Scan(&identity.AllocationEpoch, &identity.AllocationSequence); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan promotion cleanup candidate: %w", err)
		}
		identities = append(identities, identity)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close promotion cleanup scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate promotion cleanup candidates: %w", err)
	}

	completed := 0
	for _, identity := range identities {
		migrationID, err := promotion.EncodeMigrationID(identity)
		if err != nil {
			return completed, ErrPromotionRecoveryRequired
		}
		target, claimed, err := s.claimPromotionCleanupCandidate(ctx, req, identity)
		if err != nil {
			return completed, err
		}
		if !claimed {
			continue
		}
		workerReq := PromotionAbortRequest{
			TenantID: req.TenantID, MigrationID: migrationID, WriterLease: req.WriterLease,
		}
		if err := s.finishPromotionAbort(context.WithoutCancel(ctx), workerReq, identity, target); err != nil {
			return completed, err
		}
		completed++
	}
	return completed, nil
}

func (s *PromotionStore) claimPromotionCleanupCandidate(
	ctx context.Context,
	req PromotionSweepRequest,
	identity promotion.MigrationIdentity,
) (string, bool, error) {
	var target string
	claimed := false
	err := s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := validatePromotionImportIncarnation(row, tenant, namespace); err != nil {
			return err
		}
		target = row.target
		if row.state == "ABORTING" {
			if err := validatePromotionCleanupAttempt(row); err != nil {
				return err
			}
			if uint64(row.cleanupWriterGeneration.Int64) != namespace.writerGeneration {
				return ErrPromotionRestoreFenced
			}
			claimed = true
			return nil
		}
		if row.state != "CREATED" && row.state != "STAGING" && row.state != "VERIFIED" {
			return nil
		}
		lockedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, lockedAt); err != nil {
			return err
		}
		switch {
		case !lockedAt.Before(row.activityDeadline):
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, row, namespace.writerGeneration); err != nil {
				return err
			}
		case !lockedAt.Before(row.leaseExpiresAt):
			if err := s.transitionPromotionAbortTx(ctx, tx, req.TenantID, row, namespace.writerGeneration, "owner_lease_expired"); err != nil {
				return err
			}
		default:
			return nil
		}
		claimed = true
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return target, claimed, nil
}

func validatePromotionImportIncarnation(row *promotionOwnerImport, tenant promotionIdentityTenant, namespace promotionNamespaceCapability) error {
	if row.acceptedRestoreGeneration != tenant.RestoreGeneration ||
		row.acceptedRestoreGeneration != namespace.restoreGeneration ||
		row.acceptedDatabaseIncarnation != tenant.DatabaseIncarnation ||
		row.acceptedDatabaseIncarnation != namespace.databaseIncarnation ||
		row.acceptedWriterGeneration != tenant.WriterGeneration ||
		row.acceptedWriterGeneration != namespace.writerGeneration {
		return ErrPromotionRestoreFenced
	}
	return nil
}

func (s *PromotionStore) readPromotionImportStatus(
	ctx context.Context,
	tenantID, migrationID string,
	identity promotion.MigrationIdentity,
) (*promotionImportStatusRow, error) {
	row := &promotionImportStatusRow{}
	err := s.store.db.QueryRowContext(ctx, `SELECT allocation_proof_digest, recovery_token_hash,
		target_path, manifest_hash, quota_reservation_id, state, state_version,
		owner_epoch, activity_deadline, lease_expires_at, accepted_restore_generation,
		accepted_database_incarnation, accepted_writer_generation,
		terminal_result_blob, terminal_result_digest
		FROM promotion_imports
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&row.allocationProofDigest, &row.recoveryTokenHash,
			&row.target, &row.response.ManifestHash, &row.response.QuotaReservationID,
			&row.response.State, &row.response.StateVersion, &row.response.OwnerEpoch,
			&row.response.ActivityDeadline, &row.response.LeaseExpiresAt,
			&row.response.RestoreGeneration, &row.response.DatabaseIncarnation,
			&row.response.WriterGeneration, &row.terminalResultBlob, &row.terminalResultDigest)
	if err == nil {
		row.response.MigrationID = migrationID
		row.response.AllocationEpoch = identity.AllocationEpoch
		row.response.AllocationSequence = identity.AllocationSequence
		row.response.Target = row.target
		return row, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("read promotion import status: %w", err)
	}

	err = s.store.db.QueryRowContext(ctx, `SELECT allocation_proof_digest, recovery_token_hash,
		target_path, terminal_state, terminal_result_blob, terminal_result_digest
		FROM promotion_import_tombstones
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&row.allocationProofDigest, &row.recoveryTokenHash, &row.target,
			&row.response.State, &row.terminalResultBlob, &row.terminalResultDigest)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("read promotion import tombstone status: %w", err)
	}
	namespace, err := s.readPromotionNamespaceCapability(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	row.response.MigrationID = migrationID
	row.response.AllocationEpoch = identity.AllocationEpoch
	row.response.AllocationSequence = identity.AllocationSequence
	row.response.Target = row.target
	row.response.RestoreGeneration = namespace.restoreGeneration
	row.response.DatabaseIncarnation = namespace.databaseIncarnation
	row.response.WriterGeneration = namespace.writerGeneration
	return row, nil
}
