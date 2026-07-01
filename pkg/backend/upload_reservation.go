package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

// --- Meta-first saga: upload initiate reservation ---

// reserveUploadOnServer performs the central reserve-first protocol for an
// upload using only the meta DB. AtomicReserveAndInsertUpload claims
// reserved_bytes and inserts the reservation row atomically in the meta DB.
// Either both rows are written or neither is; there is no compensating path that
// can leak reserved_bytes. This intentionally does not take the legacy tenant DB
// quota_admission_locks row.
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
func (b *Dat9Backend) reserveUploadOnServer(ctx context.Context, uploadID, targetPath string, totalSize, fileCountDelta int64) (bool, error) {
	if b.metaStore == nil {
		return false, nil
	}
	start := time.Now()

	if _, err := b.metaStore.GetUploadReservation(ctx, b.tenantID, uploadID); err == nil {
		logger.Info(ctx, "central_quota_reserve_upload_duplicate",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID))
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "reserve_upload", "duplicate", time.Since(start))
		return true, nil
	} else if err != nil && !errors.Is(err, ErrReservationNotFound) {
		logger.Warn(ctx, "central_quota_reserve_upload_duplicate_lookup_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
	}

	reservation := &UploadReservationView{
		TenantID:       b.tenantID,
		UploadID:       uploadID,
		ReservedBytes:  totalSize,
		FileCountDelta: fileCountDelta,
		TargetPath:     targetPath,
		Status:         "active",
		ExpiresAt:      time.Now().Add(24 * time.Hour),
	}
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	if err := b.ensureUploadReserveFitsPendingQuota(ctx, totalSize, fileCountDelta); err != nil {
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "reserve_upload", "quota_exceeded", time.Since(start))
		return false, err
	}
	err := b.metaStore.AtomicReserveAndInsertUpload(ctx, reservation)
	switch {
	case err == nil:
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "reserve_upload", "ok", time.Since(start))
		return true, nil
	case errors.Is(err, ErrStorageQuotaExceeded):
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "reserve_upload", "quota_exceeded", time.Since(start))
		return false, err
	case errors.Is(err, ErrFileCountQuotaExceeded):
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "reserve_upload", "quota_exceeded", time.Since(start))
		return false, err
	case errors.Is(err, ErrReservationAlreadyExists):
		// Idempotent retry: reservation already exists from an earlier initiate.
		// Do NOT bump reserved_bytes again; treat as "server has the reservation".
		logger.Info(ctx, "central_quota_reserve_upload_duplicate",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID))
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "reserve_upload", "duplicate", time.Since(start))
		return true, nil
	default:
		logger.Warn(ctx, "central_quota_reserve_upload_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Int64("size", totalSize),
			zap.Error(err))
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "reserve_upload", "fail_open", time.Since(start))
		// Fail-open: allow the upload to proceed when the server DB is down.
		return false, nil
	}
}

func (b *Dat9Backend) ensureUploadReserveFitsPendingQuota(ctx context.Context, totalSize, fileCountDelta int64) error {
	if !b.UseServerQuota() || (totalSize <= 0 && fileCountDelta <= 0) {
		return nil
	}
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "upload_reserve_pending_check", "fail_open", 0)
		return nil
	}
	if cfg.MaxStorageBytes <= 0 && (fileCountDelta <= 0 || cfg.MaxFileCount <= 0) {
		return nil
	}
	usage := b.loadQuotaUsage(ctx)
	if usage == nil {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "upload_reserve_pending_check", "fail_open", 0)
		return nil
	}
	pendingStorageDelta, pendingFileDelta, _ := b.pendingCentralMutationDeltas(ctx)
	projected := usage.StorageBytes + usage.ReservedBytes + pendingStorageDelta + totalSize
	if cfg.MaxStorageBytes > 0 && projected > cfg.MaxStorageBytes {
		return fmt.Errorf("%w: server limit=%d used=%d reserved=%d pending=%d delta=%d",
			ErrStorageQuotaExceeded, cfg.MaxStorageBytes, usage.StorageBytes, usage.ReservedBytes, pendingStorageDelta, totalSize)
	}
	projectedFiles := usage.FileCount + pendingFileDelta + fileCountDelta
	if fileCountDelta > 0 && cfg.MaxFileCount > 0 && projectedFiles > cfg.MaxFileCount {
		return fmt.Errorf("%w: server limit=%d used=%d pending=%d delta=%d",
			ErrFileCountQuotaExceeded, cfg.MaxFileCount, usage.FileCount, pendingFileDelta, fileCountDelta)
	}
	return nil
}

// --- Upload abort: release reservation ---

// abortUploadReservation is called after a tenant-DB upload abort.
// It releases the reserved_bytes back to the pool. Only adjusts counters if a
// server-side reservation row exists and is active/completing, preventing
// counter corruption when the initiate-time reserve was a fail-open no-op.
func (b *Dat9Backend) abortUploadReservation(ctx context.Context, uploadID string, totalSize int64) {
	if b.metaStore == nil {
		return
	}
	start := time.Now()

	aborted := false
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		ok, reservedBytes, fileCountDelta, err := b.metaStore.AbortActiveReservationTx(ctx, tx, b.tenantID, uploadID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		aborted = true
		if err := b.metaStore.IncrReservedBytesTx(tx, b.tenantID, -reservedBytes); err != nil {
			return err
		}
		if fileCountDelta > 0 {
			if err := b.metaStore.IncrFileCountTx(tx, b.tenantID, -fileCountDelta); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		logger.Warn(ctx, "central_quota_abort_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "abort_reservation", "error", time.Since(start))
		return
	}
	if !aborted {
		metrics.RecordTenantOperation(b.tenantID, "central_quota", "abort_reservation", "skip_no_reservation", time.Since(start))
		return
	}

	metrics.RecordTenantOperation(b.tenantID, "central_quota", "abort_reservation", "ok", time.Since(start))
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

// completeUploadReservation logs and enqueues the upload-complete saga through
// the same ordered mutation queue as create/overwrite. The reservation-state
// decision ("is this reservation still active? should reserved_bytes be
// transferred?") is made INSIDE the apply/replay transaction via
// SettleActiveReservationTx, not by a pre-check outside the tx. This guarantees
// the durable-outbox contract: the mutation log entry is always written (unless
// InsertMutationLog itself fails), so storage_bytes, file_meta, and media_count
// deltas cannot be silently dropped by a transient lookup error. Any transient
// failure during async apply leaves the mutation in 'pending' status for the
// replay worker.
//
// reservedBytes is the bytes that were claimed at initiate time; the apply tx
// will transfer them reserved→storage only when an active reservation row is
// actually found (settled=true). When no active row exists (fail-open
// initiate or already-completed), the transfer is skipped so counters stay
// consistent.
func (b *Dat9Backend) completeUploadReservation(ctx context.Context, uploadID string, reservedBytes int64, fileID string, oldSizeBytes int64, oldIsMedia bool, newSizeBytes int64, newIsMedia bool) error {
	if b.metaStore == nil {
		return nil
	}
	data := uploadCompleteMutationData{
		UploadID:      uploadID,
		FileID:        fileID,
		ReservedBytes: reservedBytes,
		OldSizeBytes:  oldSizeBytes,
		OldIsMedia:    oldIsMedia,
		NewSizeBytes:  newSizeBytes,
		NewIsMedia:    newIsMedia,
	}
	return b.logAndEnqueueMutation(ctx, "upload_complete", data, quotaPendingDeltas{}, func(applyCtx context.Context, tx *sql.Tx) error {
		return applyUploadCompleteTx(applyCtx, b.metaStore, tx, b.tenantID, data)
	})
}

// applyUploadCompleteTx is the single tx-scoped apply body shared by both the
// backend mutation worker and the replay worker (mutation_replay.go
// upload_complete case). The reservation-state branch lives here (inside the
// tx) so that transient lookup failures outside never cause silent data loss of
// the paired shadow-state mutation.
//
// The settled flag from SettleActiveReservationTx partitions the
// reservation-state space into exactly two branches:
//
//   - settled=true: an 'active' reservation row existed and was atomically
//     transitioned to the terminal status in this tx. The initiate-time
//     claim on reserved_bytes is real, so we transfer reserved → storage.
//
//   - settled=false: no active reservation row found. This covers THREE
//     sub-cases:
//     (1) fail-open initiate — reserveUploadOnServer returned (false, nil)
//     because the server DB was unreachable; no reserved_bytes were
//     ever claimed and no reservation row was written.
//     (2) already-settled terminal row — a concurrent actor (another
//     replay attempt, CLI backfill) completed or aborted the row
//     before we got here.
//     (3) apply-time expiry sweep race — the row existed at initiate time
//     but ExpireActiveReservations released it (and backed out the
//     reserved_bytes) between initiate and this apply tx.
//
//     If file_meta already equals the post-upload state, the row is a
//     duplicate retry after a previous central apply committed but the
//     tenant-local ack did not. In that case this helper is a no-op. Otherwise
//     reserved_bytes is either untouched (case 1) or already balanced
//     (cases 2, 3), so we skip the reserved→storage transfer and charge
//     storage_bytes directly with newSizeBytes.
//
// This function is package-level (not a *Dat9Backend method) so that the
// replay worker — which only holds a MetaQuotaStore, no backend — can call
// it directly without duplicating logic. Caller owns MarkMutationAppliedTx:
// the inline path calls it inside completeUploadReservation's InTx, and the
// replay path calls it inside replayOne. Keeping the mark-applied call out
// of this helper prevents a double-mark rollback trap if any future caller
// also marks applied in the outer tx.
func applyUploadCompleteTx(ctx context.Context, store MetaQuotaStore, tx *sql.Tx, tenantID string, data uploadCompleteMutationData) error {
	mediaDelta := int64(0)
	switch {
	case !data.OldIsMedia && data.NewIsMedia:
		mediaDelta = 1
	case data.OldIsMedia && !data.NewIsMedia:
		mediaDelta = -1
	}
	oldExists := false
	var oldMeta *FileMetaView
	if old, err := store.GetFileMetaForUpdateTx(tx, tenantID, data.FileID); err != nil {
		if !errors.Is(err, meta.ErrNotFound) {
			return err
		}
	} else if old != nil {
		oldExists = true
		oldMeta = old
	}
	settled, reservedFileCountDelta, err := store.SettleActiveReservationTx(ctx, tx, tenantID, data.UploadID, "completed")
	if err != nil {
		return err
	}
	if !settled && oldMeta != nil && oldMeta.SizeBytes == data.NewSizeBytes && oldMeta.IsMedia == data.NewIsMedia {
		return nil
	}
	if settled {
		if err := store.TransferReservedToConfirmedTx(tx, tenantID, -data.ReservedBytes, data.ReservedBytes); err != nil {
			return err
		}
	} else {
		// settled=false: fail-open initiate / already-settled / expiry sweep
		// race. reserved_bytes is already balanced; charge storage directly.
		if data.NewSizeBytes != 0 {
			if err := store.IncrStorageBytesTx(tx, tenantID, data.NewSizeBytes); err != nil {
				return err
			}
		}
	}
	if data.OldSizeBytes != 0 {
		if err := store.IncrStorageBytesTx(tx, tenantID, -data.OldSizeBytes); err != nil {
			return err
		}
	}
	if err := store.UpsertFileMetaTx(tx, &FileMetaView{
		TenantID:  tenantID,
		FileID:    data.FileID,
		SizeBytes: data.NewSizeBytes,
		IsMedia:   data.NewIsMedia,
	}); err != nil {
		return err
	}
	if !oldExists && (!settled || reservedFileCountDelta <= 0) {
		if err := store.IncrFileCountTx(tx, tenantID, 1); err != nil {
			return err
		}
	}
	if mediaDelta != 0 {
		if err := store.IncrMediaFileCountTx(tx, tenantID, mediaDelta); err != nil {
			return err
		}
	}
	return nil
}
