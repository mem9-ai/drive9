package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
)

// --- Server-first saga: upload initiate reservation ---

// reserveUploadOnServer performs the server-reserve-first protocol for an
// upload via a single server-DB transaction: AtomicReserveAndInsertUpload
// claims reserved_bytes and inserts the reservation row atomically. Either
// both rows are written or neither is; there is no compensating path that
// can leak reserved_bytes.
//
// Return semantics:
//   - (true, nil):  reservation successfully claimed on the server DB.
//   - (false, ErrStorageQuotaExceeded): quota check failed; tx rolled back
//     so reserved_bytes and the reservation table are untouched.
//   - (false, nil): metaStore is not wired (central quota disabled) OR the
//     server DB is unreachable (fail-open). Callers treat this as "no
//     server-side reservation", so the matching abort/complete paths skip
//     counter adjustments.
//
// Idempotency: a duplicate (tenant_id, upload_id) is treated as
// (true, nil) — the original initiate already claimed the reservation;
// retrying MUST NOT bump reserved_bytes a second time.
func (b *Dat9Backend) reserveUploadOnServer(ctx context.Context, uploadID, targetPath string, totalSize int64) (bool, error) {
	if b.metaStore == nil {
		return false, nil
	}
	start := time.Now()

	err := b.metaStore.AtomicReserveAndInsertUpload(ctx, &UploadReservationView{
		TenantID:      b.tenantID,
		UploadID:      uploadID,
		ReservedBytes: totalSize,
		TargetPath:    targetPath,
		Status:        "active",
		ExpiresAt:     time.Now().Add(24 * time.Hour),
	})
	switch {
	case err == nil:
		metrics.RecordOperation("central_quota", "reserve_upload", "ok", time.Since(start))
		return true, nil
	case errors.Is(err, ErrStorageQuotaExceeded):
		metrics.RecordOperation("central_quota", "reserve_upload", "quota_exceeded", time.Since(start))
		return false, err
	case errors.Is(err, ErrReservationAlreadyExists):
		// Idempotent retry: reservation already exists from an earlier initiate.
		// Do NOT bump reserved_bytes again; treat as "server has the reservation".
		logger.Info(ctx, "central_quota_reserve_upload_duplicate",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID))
		metrics.RecordOperation("central_quota", "reserve_upload", "duplicate", time.Since(start))
		return true, nil
	default:
		logger.Warn(ctx, "central_quota_reserve_upload_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Int64("size", totalSize),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "reserve_upload", "fail_open", time.Since(start))
		// Fail-open: allow the upload to proceed when the server DB is down.
		return false, nil
	}
}

// --- Upload abort: release reservation ---

// abortUploadReservation is called after a tenant-DB upload abort.
// It releases the reserved_bytes back to the pool. Only adjusts counters
// if a server-side reservation row exists and is active, preventing
// counter corruption when the initiate-time reserve was a fail-open no-op.
func (b *Dat9Backend) abortUploadReservation(ctx context.Context, uploadID string, totalSize int64) {
	if b.metaStore == nil {
		return
	}
	start := time.Now()

	// Check if a reservation row actually exists and is active.
	r, err := b.metaStore.GetUploadReservation(ctx, b.tenantID, uploadID)
	if errors.Is(err, ErrReservationNotFound) {
		// No reservation row — initiate was a fail-open no-op; nothing to release.
		metrics.RecordOperation("central_quota", "abort_reservation", "skip_no_reservation", time.Since(start))
		return
	}
	if err != nil {
		// Transient DB error — don't touch counters to avoid corruption.
		logger.Warn(ctx, "central_quota_abort_lookup_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "abort_reservation", "lookup_error", time.Since(start))
		return
	}
	if r.Status != "active" {
		// Already completed or aborted — nothing to release.
		metrics.RecordOperation("central_quota", "abort_reservation", "skip_no_reservation", time.Since(start))
		return
	}

	// Atomically release reserved bytes and mark reservation aborted.
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.metaStore.IncrReservedBytesTx(tx, b.tenantID, -r.ReservedBytes); err != nil {
			return err
		}
		return b.metaStore.UpdateUploadReservationStatusTx(tx, b.tenantID, uploadID, "aborted")
	}); err != nil {
		logger.Warn(ctx, "central_quota_abort_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "abort_reservation", "error", time.Since(start))
		return
	}

	metrics.RecordOperation("central_quota", "abort_reservation", "ok", time.Since(start))
}

// --- Mutation log helpers ---

type uploadCompleteMutationData struct {
	UploadID      string `json:"upload_id"`
	FileID        string `json:"file_id"`
	ReservedBytes int64  `json:"reserved_bytes"`
	OldSizeBytes  int64  `json:"old_size_bytes"`
	OldIsMedia    bool   `json:"old_is_media"`
	NewSizeBytes  int64  `json:"new_size_bytes"`
	NewIsMedia    bool   `json:"new_is_media"`
}

// completeUploadReservation applies the upload-complete saga in mutation-first
// style. The reservation-state decision ("is this reservation still active?
// should reserved_bytes be transferred?") is made INSIDE the apply/replay
// transaction via SettleActiveReservationTx, not by a pre-check outside the
// tx. This guarantees the durable-outbox contract: the mutation log entry is
// always written (unless InsertMutationLog itself fails), so storage_bytes,
// file_meta, and media_count deltas cannot be silently dropped by a transient
// lookup error. Any transient failure during the initial apply attempt leaves
// the mutation in 'pending' status for the replay worker.
//
// reservedBytes is the bytes that were claimed at initiate time; the apply tx
// will transfer them reserved→storage only when an active reservation row is
// actually found (settled=true). When no active row exists (fail-open
// initiate or already-completed), the transfer is skipped so counters stay
// consistent.
func (b *Dat9Backend) completeUploadReservation(ctx context.Context, uploadID string, reservedBytes int64, fileID string, oldSizeBytes int64, oldIsMedia bool, newSizeBytes int64, newIsMedia bool) {
	if b.metaStore == nil {
		return
	}
	start := time.Now()

	data, err := json.Marshal(uploadCompleteMutationData{
		UploadID:      uploadID,
		FileID:        fileID,
		ReservedBytes: reservedBytes,
		OldSizeBytes:  oldSizeBytes,
		OldIsMedia:    oldIsMedia,
		NewSizeBytes:  newSizeBytes,
		NewIsMedia:    newIsMedia,
	})
	if err != nil {
		logger.Warn(ctx, "central_quota_upload_complete_marshal_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
		return
	}
	logID, err := b.metaStore.InsertMutationLog(ctx, &MutationLogView{
		TenantID:     b.tenantID,
		MutationType: "upload_complete",
		MutationData: data,
	})
	if err != nil {
		logger.Warn(ctx, "central_quota_mutation_log_insert_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "upload_complete", "fail_open", time.Since(start))
		return
	}
	mediaDelta := int64(0)
	switch {
	case !oldIsMedia && newIsMedia:
		mediaDelta = 1
	case oldIsMedia && !newIsMedia:
		mediaDelta = -1
	}
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		return b.applyUploadCompleteTx(tx, uploadID, fileID, reservedBytes, oldSizeBytes, newSizeBytes, newIsMedia, mediaDelta, logID)
	}); err != nil {
		logger.Warn(ctx, "central_quota_upload_complete_apply_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Int64("log_id", logID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "upload_complete", "pending", time.Since(start))
		// Leave entry in 'pending' — replay worker will retry inside
		// applyUploadCompleteTx with the same reservation-state decision.
		return
	}
	metrics.RecordOperation("central_quota", "upload_complete", "ok", time.Since(start))
}

// applyUploadCompleteTx is the single tx-scoped apply body shared by the
// inline fast path and the replay worker. The reservation-state branch lives
// here (inside the tx) so that transient lookup failures outside never cause
// silent data loss of the paired shadow-state mutation.
//
// The settled flag from SettleActiveReservationTx partitions the
// reservation-state space into exactly two branches:
//
//   - settled=true: an 'active' reservation row existed and was atomically
//     transitioned to the terminal status in this tx. The initiate-time
//     claim on reserved_bytes is real, so we transfer reserved → storage.
//
//   - settled=false: no active reservation row found. This covers THREE
//     sub-cases that are indistinguishable from the apply tx's POV and must
//     all land on the same code path:
//       (1) fail-open initiate — reserveUploadOnServer returned (false, nil)
//           because the server DB was unreachable; no reserved_bytes were
//           ever claimed and no reservation row was written.
//       (2) already-settled terminal row — a concurrent actor (another
//           replay attempt, CLI backfill) completed or aborted the row
//           before we got here.
//       (3) apply-time expiry sweep race — the row existed at initiate time
//           but ExpireActiveReservations released it (and backed out the
//           reserved_bytes) between initiate and this apply tx.
//
//     In all three sub-cases reserved_bytes is either untouched (case 1) or
//     already balanced (cases 2, 3), so we skip the reserved→storage
//     transfer and charge storage_bytes directly with newSizeBytes.
func (b *Dat9Backend) applyUploadCompleteTx(tx *sql.Tx, uploadID, fileID string, reservedBytes, oldSizeBytes, newSizeBytes int64, newIsMedia bool, mediaDelta int64, logID int64) error {
	settled, err := b.metaStore.SettleActiveReservationTx(tx, b.tenantID, uploadID, "completed")
	if err != nil {
		return err
	}
	if settled {
		if err := b.metaStore.TransferReservedToConfirmedTx(tx, b.tenantID, -reservedBytes, reservedBytes); err != nil {
			return err
		}
	} else {
		// settled=false: fail-open initiate / already-settled / expiry sweep
		// race. reserved_bytes is already balanced; charge storage directly.
		if newSizeBytes != 0 {
			if err := b.metaStore.IncrStorageBytesTx(tx, b.tenantID, newSizeBytes); err != nil {
				return err
			}
		}
	}
	if oldSizeBytes != 0 {
		if err := b.metaStore.IncrStorageBytesTx(tx, b.tenantID, -oldSizeBytes); err != nil {
			return err
		}
	}
	if err := b.metaStore.UpsertFileMetaTx(tx, &FileMetaView{
		TenantID:  b.tenantID,
		FileID:    fileID,
		SizeBytes: newSizeBytes,
		IsMedia:   newIsMedia,
	}); err != nil {
		return err
	}
	if mediaDelta != 0 {
		if err := b.metaStore.IncrMediaFileCountTx(tx, b.tenantID, mediaDelta); err != nil {
			return err
		}
	}
	return b.metaStore.MarkMutationAppliedTx(tx, logID)
}
