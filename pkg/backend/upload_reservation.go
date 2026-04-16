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

// reserveUploadOnServer performs the server-reserve-first protocol for an upload:
// 1. AtomicReserveUpload (check + claim reserved_bytes in one UPDATE)
// 2. InsertUploadReservation (tracking row)
//
// Returns (reserved, nil) on success where reserved indicates whether bytes
// were actually claimed on the server. On quota exceeded returns (false, err).
// When metaStore is nil (central quota not wired), this is a no-op returning (false, nil).
func (b *Dat9Backend) reserveUploadOnServer(ctx context.Context, uploadID, targetPath string, totalSize int64) (bool, error) {
	if b.metaStore == nil {
		return false, nil
	}
	start := time.Now()

	// Step 1: atomic reserve — check+claim in a single UPDATE.
	if err := b.metaStore.AtomicReserveUpload(ctx, b.tenantID, totalSize); err != nil {
		if errors.Is(err, ErrStorageQuotaExceeded) {
			metrics.RecordOperation("central_quota", "reserve_upload", "quota_exceeded", time.Since(start))
			return false, err
		}
		logger.Warn(ctx, "central_quota_reserve_upload_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Int64("size", totalSize),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "reserve_upload", "fail_open", time.Since(start))
		// Fail-open: allow the upload to proceed even if server DB is down.
		return false, nil
	}

	// Step 2: record the reservation row.
	if err := b.metaStore.InsertUploadReservation(ctx, &UploadReservationView{
		TenantID:      b.tenantID,
		UploadID:      uploadID,
		ReservedBytes: totalSize,
		TargetPath:    targetPath,
		Status:        "active",
		ExpiresAt:     time.Now().Add(24 * time.Hour),
	}); err != nil {
		// Compensate: release the reserved bytes.
		if rErr := b.metaStore.IncrReservedBytes(ctx, b.tenantID, -totalSize); rErr != nil {
			logger.Error(ctx, "central_quota_reserve_compensate_failed",
				zap.String("tenant_id", b.tenantID),
				zap.String("upload_id", uploadID),
				zap.Error(rErr))
		}
		logger.Warn(ctx, "central_quota_insert_reservation_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "reserve_upload", "fail_open", time.Since(start))
		// Fail-open.
		return false, nil
	}

	metrics.RecordOperation("central_quota", "reserve_upload", "ok", time.Since(start))
	return true, nil
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
	if err != nil || r.Status != "active" {
		// No active reservation — initiate was a fail-open no-op; nothing to release.
		metrics.RecordOperation("central_quota", "abort_reservation", "skip_no_reservation", time.Since(start))
		return
	}

	// Release reserved bytes.
	if err := b.metaStore.IncrReservedBytes(ctx, b.tenantID, -r.ReservedBytes); err != nil {
		logger.Warn(ctx, "central_quota_abort_release_reserved_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "abort_reservation", "error", time.Since(start))
		return
	}

	// Mark reservation aborted.
	if err := b.metaStore.UpdateUploadReservationStatus(ctx, b.tenantID, uploadID, "aborted"); err != nil {
		logger.Warn(ctx, "central_quota_update_reservation_status_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Error(err))
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

func (b *Dat9Backend) completeUploadReservation(ctx context.Context, uploadID string, reservedBytes int64, fileID string, oldSizeBytes int64, oldIsMedia bool, newSizeBytes int64, newIsMedia bool) {
	if b.metaStore == nil {
		return
	}
	start := time.Now()

	// Check if a reservation row actually exists and is active before
	// touching counters. If the initiate-time reserve was a fail-open no-op,
	// there is no reservation to complete.
	r, err := b.metaStore.GetUploadReservation(ctx, b.tenantID, uploadID)
	if err != nil || r.Status != "active" {
		// No active reservation — log the file mutation via the outbox anyway
		// so the file meta shadow state stays in sync, but use 0 reserved bytes
		// to avoid touching the reserved_bytes counter.
		reservedBytes = 0
	}

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
		if err := b.metaStore.TransferReservedToConfirmedTx(tx, b.tenantID, -reservedBytes, reservedBytes); err != nil {
			return err
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
		if err := b.metaStore.UpdateUploadReservationStatusTx(tx, b.tenantID, uploadID, "completed"); err != nil {
			return err
		}
		return b.metaStore.MarkMutationAppliedTx(tx, logID)
	}); err != nil {
		logger.Warn(ctx, "central_quota_upload_complete_apply_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("upload_id", uploadID),
			zap.Int64("log_id", logID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", "upload_complete", "pending", time.Since(start))
		return
	}
	metrics.RecordOperation("central_quota", "upload_complete", "ok", time.Since(start))
}
