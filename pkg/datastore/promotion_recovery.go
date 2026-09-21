package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

// PromotionAbortRequest aborts one client-owned import or resumes the exact
// durable cleanup attempt that already owns it.
type PromotionAbortRequest struct {
	TenantID    string
	MigrationID string
	OwnerEpoch  uint64
	OwnerToken  string
	WriterLease string
}

type promotionAbortResult struct {
	Version        string `json:"version"`
	MigrationID    string `json:"migration_id"`
	Target         string `json:"target"`
	State          string `json:"state"`
	TerminalReason string `json:"terminal_reason"`
}

var promotionTestAfterAbortAccepted func()
var promotionTestFailBeforeAbortFinalize func() error
var promotionTestBeforeAbortFinalLeaseCheck func()

// AbortImport atomically gives cleanup a durable owner before dispatching the
// server-owned worker. Once accepted, caller cancellation no longer owns the
// work. Retries resume that same attempt or return its immutable result.
func (s *PromotionStore) AbortImport(ctx context.Context, req PromotionAbortRequest) (*PromotionImport, error) {
	if req.TenantID == "" || req.OwnerEpoch == 0 || req.OwnerToken == "" || len(req.OwnerToken) > 512 || req.WriterLease == "" {
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
	if recovered, found, err := s.readPromotionAbortTerminal(ctx, req, identity, target); err != nil {
		return nil, err
	} else if found {
		return recovered, nil
	}

	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if row.target != target || row.ownerEpoch != req.OwnerEpoch ||
			!promotionHashEqual(row.ownerTokenHash, promotionHashString(req.OwnerToken)) {
			return ErrPromotionConflict
		}
		if row.state == "COMMITTED" || row.state == "ABORTED" {
			return nil
		}
		if row.state == "ABORTING" {
			return validatePromotionCleanupAttempt(row)
		}
		if err := validatePromotionOwnerMutation(row, tenant, namespace, req.OwnerEpoch, req.OwnerToken); err != nil {
			return err
		}
		if row.state == "COMMITTING" {
			return ErrPromotionConflict
		}
		if row.state != "CREATED" && row.state != "STAGING" && row.state != "VERIFIED" {
			return ErrPromotionRecoveryRequired
		}
		lockedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if !lockedAt.Before(row.activityDeadline) {
			return s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, row, namespace.writerGeneration)
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, lockedAt); err != nil {
			return err
		}
		if !lockedAt.Before(row.leaseExpiresAt) {
			return ErrPromotionConflict
		}
		return s.transitionPromotionAbortTx(ctx, tx, req.TenantID, row, namespace.writerGeneration, "client_aborted")
	})
	if err != nil {
		return nil, err
	}
	if promotionTestAfterAbortAccepted != nil {
		promotionTestAfterAbortAccepted()
	}

	workerCtx := context.WithoutCancel(ctx)
	if err := s.finishPromotionAbort(workerCtx, req, identity, target); err != nil {
		if recovered, found, recoveryErr := s.readPromotionAbortTerminal(workerCtx, req, identity, target); recoveryErr != nil {
			return nil, recoveryErr
		} else if found {
			return recovered, nil
		}
		return nil, err
	}
	recovered, found, err := s.readPromotionAbortTerminal(workerCtx, req, identity, target)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrPromotionRecoveryRequired
	}
	return recovered, nil
}

func (s *PromotionStore) transitionPromotionAbortTx(ctx context.Context, tx *sql.Tx, tenantID string, row *promotionOwnerImport, writerGeneration uint64, reason string) error {
	attemptID, err := newPromotionID("pca")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE promotion_imports
		SET state = 'ABORTING', state_version = state_version + 1,
		    cleanup_attempt_id = ?, cleanup_writer_generation = ?, terminal_reason = ?
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
		  AND state = ? AND state_version = ?`,
		attemptID, writerGeneration, reason, tenantID, row.identity.AllocationEpoch,
		row.identity.AllocationSequence, row.state, row.stateVersion)
	if err != nil {
		return fmt.Errorf("claim promotion abort cleanup: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionConflict
	}
	return nil
}

func validatePromotionCleanupAttempt(row *promotionOwnerImport) error {
	if row.state != "ABORTING" || !row.cleanupAttemptID.Valid || row.cleanupAttemptID.String == "" ||
		!row.cleanupWriterGeneration.Valid || row.cleanupWriterGeneration.Int64 < 0 ||
		!row.terminalReason.Valid || row.terminalReason.String == "" {
		return ErrPromotionRecoveryRequired
	}
	return nil
}

func (s *PromotionStore) finishPromotionAbort(ctx context.Context, req PromotionAbortRequest, identity promotion.MigrationIdentity, target string) error {
	return s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if row.state == "ABORTED" {
			return nil
		}
		if row.target != target || row.acceptedRestoreGeneration != tenant.RestoreGeneration ||
			row.acceptedRestoreGeneration != namespace.restoreGeneration ||
			row.acceptedDatabaseIncarnation != tenant.DatabaseIncarnation ||
			row.acceptedDatabaseIncarnation != namespace.databaseIncarnation ||
			row.acceptedWriterGeneration != tenant.WriterGeneration ||
			row.acceptedWriterGeneration != namespace.writerGeneration {
			return ErrPromotionRestoreFenced
		}
		if err := validatePromotionCleanupAttempt(row); err != nil {
			return err
		}
		if uint64(row.cleanupWriterGeneration.Int64) != namespace.writerGeneration {
			return ErrPromotionRestoreFenced
		}

		var reservedBytes, reservedFiles uint64
		if err := tx.QueryRowContext(ctx, `SELECT reserved_bytes, reserved_files FROM promotion_quota_accounts
			WHERE tenant_id = ? FOR UPDATE`, req.TenantID).
			Scan(&reservedBytes, &reservedFiles); err != nil {
			return ErrPromotionRecoveryRequired
		}
		var reservationBytes, reservationFiles, reservationVersion uint64
		var reservationState string
		if err := tx.QueryRowContext(ctx, `SELECT reserved_bytes, reserved_files, state, state_version
			FROM promotion_quota_reservations WHERE tenant_id = ? AND reservation_id = ?
			  AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
			req.TenantID, row.quotaReservationID, identity.AllocationEpoch, identity.AllocationSequence).
			Scan(&reservationBytes, &reservationFiles, &reservationState, &reservationVersion); err != nil {
			return ErrPromotionRecoveryRequired
		}
		if reservationState != "RESERVED" || reservationBytes != row.byteTotal ||
			reservedBytes < reservationBytes || reservedFiles < reservationFiles {
			return ErrPromotionRecoveryRequired
		}
		entries, err := selectPromotionEntriesTx(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		contents, err := selectPromotionContentsTx(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		storedManifest, err := validatePromotionStoredManifest(row, entries)
		if err != nil || reservationFiles != storedManifest.fileCount {
			return ErrPromotionRecoveryRequired
		}
		if err := validatePromotionStagedContents(storedManifest, contents); err != nil {
			return err
		}
		if promotionTestBeforeAbortFinalLeaseCheck != nil {
			promotionTestBeforeAbortFinalLeaseCheck()
		}
		finishedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, finishedAt); err != nil {
			return err
		}
		if promotionTestFailBeforeAbortFinalize != nil {
			if err := promotionTestFailBeforeAbortFinalize(); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM promotion_import_entries
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence); err != nil {
			return fmt.Errorf("delete promotion staged entries: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM promotion_import_contents
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence); err != nil {
			return fmt.Errorf("delete promotion staged contents: %w", err)
		}
		if reservationBytes != 0 || reservationFiles != 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE promotion_quota_accounts
				SET reserved_bytes = reserved_bytes - ?, reserved_files = reserved_files - ?
				WHERE tenant_id = ?`, reservationBytes, reservationFiles, req.TenantID); err != nil {
				return fmt.Errorf("release promotion quota account: %w", err)
			}
		}
		released, err := tx.ExecContext(ctx, `UPDATE promotion_quota_reservations
			SET state = 'RELEASED', state_version = state_version + 1
			WHERE tenant_id = ? AND reservation_id = ? AND state = 'RESERVED' AND state_version = ?`,
			req.TenantID, row.quotaReservationID, reservationVersion)
		if err != nil {
			return fmt.Errorf("release promotion quota reservation: %w", err)
		}
		if affected, err := released.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionRecoveryRequired
		}
		resultRaw, err := json.Marshal(promotionAbortResult{
			Version: "p1", MigrationID: req.MigrationID, Target: target,
			State: "ABORTED", TerminalReason: row.terminalReason.String,
		})
		if err != nil {
			return err
		}
		resultBlob := string(resultRaw)
		resultDigest := promotionHashString(resultBlob)
		aborted, err := tx.ExecContext(ctx, `UPDATE promotion_imports
			SET state = 'ABORTED', state_version = state_version + 1,
			    terminal_result_blob = ?, terminal_result_digest = ?, terminal_at = ?,
			    full_row_compact_not_before = ?, retire_after = ?
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND state = 'ABORTING' AND state_version = ? AND cleanup_attempt_id = ?`,
			resultBlob, resultDigest, finishedAt,
			finishedAt.Add(s.cfg.FullRowCompactDelay), finishedAt.Add(s.cfg.TerminalRetention),
			req.TenantID, identity.AllocationEpoch,
			identity.AllocationSequence, row.stateVersion, row.cleanupAttemptID.String)
		if err != nil {
			return fmt.Errorf("finish promotion abort: %w", err)
		}
		if affected, err := aborted.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		return nil
	})
}

func (s *PromotionStore) readPromotionAbortTerminal(ctx context.Context, req PromotionAbortRequest, identity promotion.MigrationIdentity, target string) (*PromotionImport, bool, error) {
	var out PromotionImport
	var ownerHash string
	var blob, digest sql.NullString
	err := s.store.db.QueryRowContext(ctx, `SELECT target_path, manifest_hash, quota_reservation_id, state,
		state_version, owner_epoch, activity_deadline, lease_expires_at,
		accepted_restore_generation, accepted_database_incarnation, accepted_writer_generation,
		owner_token_hash, terminal_result_blob, terminal_result_digest
		FROM promotion_imports WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		req.TenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&out.Target, &out.ManifestHash, &out.QuotaReservationID, &out.State,
			&out.StateVersion, &out.OwnerEpoch, &out.ActivityDeadline, &out.LeaseExpiresAt,
			&out.RestoreGeneration, &out.DatabaseIncarnation, &out.WriterGeneration,
			&ownerHash, &blob, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, ErrNotFound
	}
	if err != nil {
		return nil, false, fmt.Errorf("read promotion abort result: %w", err)
	}
	if out.Target != target || out.OwnerEpoch != req.OwnerEpoch ||
		!promotionHashEqual(ownerHash, promotionHashString(req.OwnerToken)) {
		return nil, false, ErrPromotionConflict
	}
	if out.State != "COMMITTED" && out.State != "ABORTED" {
		return nil, false, nil
	}
	if err := validatePromotionTerminalResult(out.State, blob, digest); err != nil {
		return nil, false, err
	}
	out.MigrationID = req.MigrationID
	out.AllocationEpoch = identity.AllocationEpoch
	out.AllocationSequence = identity.AllocationSequence
	out.TerminalResultBlob = blob.String
	out.TerminalResultDigest = digest.String
	return &out, true, nil
}
