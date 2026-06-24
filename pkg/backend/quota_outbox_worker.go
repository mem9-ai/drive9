package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	quotaOutboxNotifySize               = 1
	quotaOutboxNotifyDelay              = 50 * time.Millisecond
	quotaOutboxNotifyMaxDelay           = 200 * time.Millisecond
	quotaOutboxPollInterval             = 1 * time.Second
	quotaOutboxLeaseDuration            = 5 * time.Minute
	quotaOutboxBatchSize                = 100
	quotaOutboxRecoverLimit             = 100
	quotaOutboxUploadDrainLimit         = 1000
	quotaOutboxUploadDrainMaxWait       = 5 * time.Second
	quotaOutboxUploadDrainWait          = 25 * time.Millisecond
	quotaOutboxUploadDrainWarnThreshold = 10
	quotaOutboxRetryBaseDelay           = 200 * time.Millisecond
	quotaOutboxRetryMaxDelay            = 30 * time.Second
	quotaMutationTypeFileCreate         = "file_create"
	quotaMutationTypeOverwrite          = "file_overwrite"
)

func (b *Dat9Backend) startQuotaOutboxWorker() {
	if b.quotaOutboxNotify != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.quotaOutboxStop = cancel
	b.quotaOutboxNotify = make(chan struct{}, quotaOutboxNotifySize)
	b.quotaOutboxWG.Add(1)
	go b.runQuotaOutboxWorker(ctx)
}

func (b *Dat9Backend) stopQuotaOutboxWorker() {
	if b.quotaOutboxStop != nil {
		b.quotaOutboxStop()
		b.quotaOutboxWG.Wait()
		b.quotaOutboxStop = nil
		b.quotaOutboxNotify = nil
	}
}

func (b *Dat9Backend) notifyQuotaOutbox(enqueued bool) {
	if !enqueued || b.quotaOutboxNotify == nil {
		return
	}
	select {
	case b.quotaOutboxNotify <- struct{}{}:
	default:
	}
}

func (b *Dat9Backend) runQuotaOutboxWorker(ctx context.Context) {
	defer b.quotaOutboxWG.Done()

	ticker := time.NewTicker(quotaOutboxPollInterval)
	defer ticker.Stop()

	b.processQuotaOutboxAvailable(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.quotaOutboxNotify:
			if !b.waitQuotaOutboxNotifyQuiet(ctx) {
				return
			}
			b.processQuotaOutboxAvailable(ctx)
		case <-ticker.C:
			b.processQuotaOutboxAvailable(ctx)
		}
	}
}

func (b *Dat9Backend) waitQuotaOutboxNotifyQuiet(ctx context.Context) bool {
	if quotaOutboxNotifyDelay <= 0 {
		return true
	}
	// Coalesce bursts until the channel is quiet for quotaOutboxNotifyDelay.
	// quotaOutboxNotifyMaxDelay bounds the wait under a steady write stream.
	timer := time.NewTimer(quotaOutboxNotifyDelay)
	defer timer.Stop()
	maxTimer := time.NewTimer(quotaOutboxNotifyMaxDelay)
	defer maxTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-maxTimer.C:
			return true
		case <-b.quotaOutboxNotify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(quotaOutboxNotifyDelay)
		}
	}
}

func (b *Dat9Backend) processQuotaOutboxAvailable(ctx context.Context) {
	if ctx.Err() != nil || b.store == nil || b.metaStore == nil || b.tenantID == "" {
		return
	}
	now := time.Now().UTC()
	if _, err := b.store.RecoverExpiredQuotaOutbox(ctx, now, quotaOutboxRecoverLimit); err != nil {
		if isContextDone(err) {
			return
		}
		logger.Warn(ctx, "quota_outbox_recover_expired_failed",
			zap.String("tenant_id", b.tenantID),
			zap.Error(err))
		metrics.RecordOperation("quota_outbox", "recover_expired", "error", 0)
	}
	for i := 0; i < quotaOutboxBatchSize; i++ {
		if ctx.Err() != nil {
			return
		}
		processed, err := b.ProcessOneQuotaOutbox(ctx)
		if err != nil {
			if isContextDone(err) {
				return
			}
			logger.Warn(ctx, "quota_outbox_process_failed",
				zap.String("tenant_id", b.tenantID),
				zap.Error(err))
		}
		if !processed {
			return
		}
	}
}

func (b *Dat9Backend) drainQuotaOutboxForUploadTarget(ctx context.Context, target *datastore.NodeWithFile, targetExists bool) error {
	if !targetExists || !b.UseServerQuota() || b.store == nil {
		return nil
	}
	if target == nil || target.File == nil || target.File.FileID == "" {
		return nil
	}
	return b.drainQuotaOutboxForFile(ctx, target.File.FileID, quotaOutboxUploadDrainLimit)
}

func (b *Dat9Backend) drainQuotaOutboxForFile(ctx context.Context, fileID string, limit int) error {
	if fileID == "" || b.store == nil {
		return nil
	}
	if limit <= 0 {
		limit = quotaOutboxUploadDrainLimit
	}
	start := time.Now()
	processedCount := 0
	for i := 0; i < limit; i++ {
		if quotaOutboxUploadDrainMaxWait > 0 && time.Since(start) > quotaOutboxUploadDrainMaxWait {
			return fmt.Errorf("quota outbox for file %s still pending after %s", fileID, quotaOutboxUploadDrainMaxWait)
		}
		pending, err := b.store.HasPendingQuotaOutboxFileMutation(ctx, fileID)
		if err != nil {
			return fmt.Errorf("check pending quota outbox for file: %w", err)
		}
		if !pending {
			if processedCount > quotaOutboxUploadDrainWarnThreshold {
				logger.Warn(ctx, "quota_outbox_upload_target_deep_drain",
					zap.String("tenant_id", b.tenantID),
					zap.String("file_id", fileID),
					zap.Int("processed", processedCount))
			}
			return nil
		}
		processed, err := b.ProcessOneQuotaOutbox(ctx)
		if err != nil {
			return fmt.Errorf("drain quota outbox for file: %w", err)
		}
		if !processed {
			timer := time.NewTimer(quotaOutboxUploadDrainWait)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return fmt.Errorf("quota outbox for file %s still pending but not claimable: %w", fileID, ctx.Err())
			case <-timer.C:
			}
			continue
		}
		processedCount++
	}
	return fmt.Errorf("quota outbox for file %s still pending after %d entries", fileID, limit)
}

// ProcessOneQuotaOutbox applies at most one tenant-local quota outbox row. It is
// exported for deterministic tests and admin drain hooks.
func (b *Dat9Backend) ProcessOneQuotaOutbox(ctx context.Context) (processed bool, err error) {
	start := time.Now()
	if b.store == nil || b.metaStore == nil || b.tenantID == "" {
		return false, nil
	}
	entry, found, err := b.store.ClaimQuotaOutbox(ctx, time.Now().UTC(), quotaOutboxLeaseDuration)
	if err != nil {
		metrics.RecordOperation("quota_outbox", "claim", "error", time.Since(start))
		return false, err
	}
	if !found {
		metrics.RecordOperation("quota_outbox", "claim", "empty", time.Since(start))
		return false, nil
	}

	var applyErr error
	var ackErr error
	var retryErr error
	err = b.withQuotaAdmissionLock(ctx, func(tx *sql.Tx) error {
		applyErr = b.applyQuotaOutboxEntry(ctx, entry)
		if applyErr == nil {
			if tx != nil {
				ackErr = b.store.AckQuotaOutboxTx(ctx, tx, entry.ID, entry.Receipt)
			} else {
				ackErr = b.store.AckQuotaOutbox(ctx, entry.ID, entry.Receipt)
			}
			return ackErr
		}
		retryAt := time.Now().UTC().Add(quotaOutboxRetryDelay(entry.AttemptCount, quotaOutboxRetryBaseDelay, quotaOutboxRetryMaxDelay))
		if tx != nil {
			retryErr = b.store.RetryQuotaOutboxTx(ctx, tx, entry.ID, entry.Receipt, retryAt, applyErr.Error())
		} else {
			retryErr = b.store.RetryQuotaOutbox(ctx, entry.ID, entry.Receipt, retryAt, applyErr.Error())
		}
		if retryErr != nil {
			return fmt.Errorf("process quota outbox %d: %w; update retry: %v", entry.ID, applyErr, retryErr)
		}
		return nil
	})
	if err == nil {
		if applyErr != nil {
			metrics.RecordOperation("quota_outbox", entry.MutationType, "error", time.Since(start))
			return true, applyErr
		}
		if b.quotaUsageCache != nil {
			b.quotaUsageCache.invalidate()
		}
		b.addLocalQuotaPendingDeltas(-entry.StorageDelta, -entry.FileDelta, -entry.MediaDelta)
		metrics.RecordOperation("quota_outbox", entry.MutationType, "ok", time.Since(start))
		return true, nil
	}
	if ackErr != nil {
		metrics.RecordOperation("quota_outbox", "ack", "error", time.Since(start))
		return true, ackErr
	}
	if retryErr != nil {
		metrics.RecordOperation("quota_outbox", "retry", "error", time.Since(start))
		return true, err
	}
	metrics.RecordOperation("quota_outbox", entry.MutationType, "error", time.Since(start))
	return true, err
}

func (b *Dat9Backend) withQuotaAdmissionLock(ctx context.Context, fn func(tx *sql.Tx) error) error {
	if !b.UseServerQuota() || b.store == nil {
		return fn(nil)
	}
	return b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.lockQuotaAdmissionTx(ctx, tx); err != nil {
			return err
		}
		return fn(tx)
	})
}

func (b *Dat9Backend) applyQuotaOutboxEntry(ctx context.Context, entry *datastore.QuotaOutboxEntry) error {
	if entry == nil {
		return fmt.Errorf("quota outbox entry is required")
	}
	return b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		return applyCentralQuotaMutationTx(b.metaStore, tx, b.tenantID, entry.MutationType, entry.MutationData, entry.ID)
	})
}

func quotaOutboxRetryDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = quotaOutboxRetryBaseDelay
	}
	if max <= 0 {
		max = quotaOutboxRetryMaxDelay
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}

func (b *Dat9Backend) enqueueQuotaFileCreateOutboxTx(tx *sql.Tx, fileID string, sizeBytes int64, contentType string) (bool, error) {
	if !b.UseServerQuota() {
		return false, nil
	}
	isMedia := isQuotaMediaContentType(contentType)
	data := fileCreateMutationData{
		FileID:    fileID,
		SizeBytes: sizeBytes,
		IsMedia:   isMedia,
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return false, err
	}
	mediaDelta := int64(0)
	if isMedia {
		mediaDelta = 1
	}
	_, err = b.store.EnqueueQuotaOutboxTx(tx, &datastore.QuotaOutboxEntry{
		FileID:       fileID,
		MutationType: quotaMutationTypeFileCreate,
		MutationData: raw,
		StorageDelta: sizeBytes,
		FileDelta:    1,
		MediaDelta:   mediaDelta,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *Dat9Backend) enqueueQuotaFileOverwriteOutboxTx(tx *sql.Tx, fileID string, oldSize int64, oldContentType string, newSize int64, newContentType string) (bool, error) {
	if !b.UseServerQuota() {
		return false, nil
	}
	oldIsMedia := isQuotaMediaContentType(oldContentType)
	newIsMedia := isQuotaMediaContentType(newContentType)
	data := fileOverwriteMutationData{
		FileID:       fileID,
		OldSizeBytes: oldSize,
		OldIsMedia:   oldIsMedia,
		NewSizeBytes: newSize,
		NewIsMedia:   newIsMedia,
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return false, err
	}
	mediaDelta := quotaMediaDelta(oldIsMedia, newIsMedia)
	_, err = b.store.EnqueueQuotaOutboxTx(tx, &datastore.QuotaOutboxEntry{
		FileID:       fileID,
		MutationType: quotaMutationTypeOverwrite,
		MutationData: raw,
		StorageDelta: newSize - oldSize,
		FileDelta:    0,
		MediaDelta:   mediaDelta,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}
