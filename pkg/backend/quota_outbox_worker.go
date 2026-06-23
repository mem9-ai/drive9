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
	quotaOutboxNotifySize       = 1
	quotaOutboxPollInterval     = 1 * time.Second
	quotaOutboxLeaseDuration    = 5 * time.Minute
	quotaOutboxBatchSize        = 100
	quotaOutboxRecoverLimit     = 100
	quotaOutboxRetryBaseDelay   = 200 * time.Millisecond
	quotaOutboxRetryMaxDelay    = 30 * time.Second
	quotaMutationTypeFileCreate = "file_create"
	quotaMutationTypeOverwrite  = "file_overwrite"
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
			b.processQuotaOutboxAvailable(ctx)
		case <-ticker.C:
			b.processQuotaOutboxAvailable(ctx)
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

	err = b.applyQuotaOutboxEntry(ctx, entry)
	if err == nil {
		if ackErr := b.store.AckQuotaOutbox(ctx, entry.ID, entry.Receipt); ackErr != nil {
			metrics.RecordOperation("quota_outbox", "ack", "error", time.Since(start))
			return true, ackErr
		}
		metrics.RecordOperation("quota_outbox", entry.MutationType, "ok", time.Since(start))
		return true, nil
	}

	retryAt := time.Now().UTC().Add(quotaOutboxRetryDelay(entry.AttemptCount, quotaOutboxRetryBaseDelay, quotaOutboxRetryMaxDelay))
	if retryErr := b.store.RetryQuotaOutbox(ctx, entry.ID, entry.Receipt, retryAt, err.Error()); retryErr != nil {
		metrics.RecordOperation("quota_outbox", "retry", "error", time.Since(start))
		return true, fmt.Errorf("process quota outbox %d: %w; update retry: %v", entry.ID, err, retryErr)
	}
	metrics.RecordOperation("quota_outbox", entry.MutationType, "error", time.Since(start))
	return true, err
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
	mediaDelta := int64(0)
	switch {
	case !oldIsMedia && newIsMedia:
		mediaDelta = 1
	case oldIsMedia && !newIsMedia:
		mediaDelta = -1
	}
	_, err = b.store.EnqueueQuotaOutboxTx(tx, &datastore.QuotaOutboxEntry{
		FileID:       fileID,
		MutationType: quotaMutationTypeOverwrite,
		MutationData: raw,
		StorageDelta: newSize - oldSize,
		MediaDelta:   mediaDelta,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}
