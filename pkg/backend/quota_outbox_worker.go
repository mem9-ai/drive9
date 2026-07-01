package backend

import (
	"context"
	"database/sql"
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
	quotaMutationTypeUploadComplete     = "upload_complete"
)

type quotaOutboxBatchClaimer func(context.Context, time.Time, time.Duration, int) (datastore.QuotaOutboxBatchClaimResult, error)

func (b *Dat9Backend) stopQuotaOutboxWorker() {
	// Runtime quota accounting no longer starts the tenant quota_outbox worker.
	// Keep this cleanup hook for legacy tests that explicitly disable it.
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
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "recover_expired", metrics.ResultForError(err), 0)
	}
	processedTotal := 0
	for processedTotal < quotaOutboxBatchSize {
		processed, err := b.ProcessQuotaOutboxBatch(ctx, quotaOutboxBatchSize-processedTotal)
		if err != nil {
			if isContextDone(err) {
				return
			}
			logger.Warn(ctx, "quota_outbox_process_failed",
				zap.String("tenant_id", b.tenantID),
				zap.Error(err))
			if processed == 0 {
				return
			}
		}
		if processed == 0 {
			return
		}
		processedTotal += processed
	}
	b.notifyQuotaOutbox(true)
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
	for processedCount < limit {
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
		batchLimit := quotaOutboxBatchSize
		if remaining := limit - processedCount; remaining < batchLimit {
			batchLimit = remaining
		}
		processed, err := b.ProcessQuotaOutboxBatch(ctx, batchLimit)
		if err != nil {
			pendingAfterErr, pendingErr := b.store.HasPendingQuotaOutboxFileMutation(ctx, fileID)
			if pendingErr == nil && !pendingAfterErr {
				return nil
			}
			if processed == 0 {
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
		}
		if processed == 0 {
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
		processedCount += processed
	}
	return fmt.Errorf("quota outbox for file %s still pending after %d entries", fileID, limit)
}

// ProcessOneQuotaOutbox applies at most one tenant-local quota outbox row. It is
// exported for deterministic tests and admin drain hooks.
func (b *Dat9Backend) ProcessOneQuotaOutbox(ctx context.Context) (processed bool, err error) {
	n, err := b.ProcessQuotaOutboxBatch(ctx, 1)
	return n > 0, err
}

// ProcessQuotaOutboxBatch applies up to limit tenant-local quota outbox rows.
// Rows are claimed with per-file ordering, so unrelated file mutations can
// converge in one tenant admission-lock window.
func (b *Dat9Backend) ProcessQuotaOutboxBatch(ctx context.Context, limit int) (processed int, err error) {
	start := time.Now()
	if b.metaStore == nil || b.tenantID == "" {
		return 0, nil
	}
	if limit <= 0 {
		limit = 1
	}
	claimQuotaOutbox := b.claimQuotaOutbox
	if claimQuotaOutbox == nil {
		if b.store == nil {
			return 0, nil
		}
		claimQuotaOutbox = b.store.ClaimQuotaOutboxBatchResult
	}
	claim, err := claimQuotaOutbox(ctx, time.Now().UTC(), quotaOutboxLeaseDuration, limit)
	if err != nil {
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "claim", metrics.ResultForError(err), time.Since(start))
		return 0, err
	}
	if claim.ConflictExhausted {
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "claim", "conflict", time.Since(start))
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "claim_conflict_exhausted", "conflict", time.Since(start))
		return 0, nil
	}
	entries := claim.Entries
	if len(entries) == 0 {
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "claim", "empty", time.Since(start))
		return 0, nil
	}
	metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "claim", "ok", time.Since(start))

	appliedEntries := make([]datastore.QuotaOutboxEntry, 0, len(entries))
	var batchApplyErr error
	err = b.withQuotaAdmissionLock(ctx, func(tx *sql.Tx) error {
		if tx != nil && len(entries) > 1 {
			if applyErr := b.applyQuotaOutboxEntries(ctx, entries); applyErr == nil {
				// The central apply commits before this tenant-local ack. If the
				// lease expires or the ack tx rolls back, recovery may re-apply
				// the same rows later; file-state mutations are idempotent and
				// pending deltas remain conservative until those rows are acked.
				if ackErr := b.store.AckQuotaOutboxBatchTx(ctx, tx, entries); ackErr != nil {
					return ackErr
				}
				processed = len(entries)
				appliedEntries = append(appliedEntries, entries...)
				return nil
			} else {
				batchApplyErr = applyErr
				return nil
			}
		}
		return nil
	})
	if err != nil {
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "process", metrics.ResultForError(err), time.Since(start))
		return processed, err
	}
	if batchApplyErr == nil && processed > 0 {
		b.recordAppliedQuotaOutboxEntries(appliedEntries)
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "process", "ok", time.Since(start))
		return processed, nil
	}
	if len(entries) > 1 && batchApplyErr != nil {
		logger.Warn(ctx, "quota_outbox_batch_apply_failed_falling_back",
			zap.String("tenant_id", b.tenantID),
			zap.Int("entries", len(entries)),
			zap.Error(batchApplyErr))
	}

	var fallbackErr error
	for i := range entries {
		applied, entryProcessed, entryErr := b.processQuotaOutboxEntry(ctx, &entries[i])
		if entryProcessed {
			processed++
		}
		if applied {
			appliedEntries = append(appliedEntries, entries[i])
		}
		if entryErr != nil {
			metrics.RecordTenantOperationCount(b.tenantID, "quota_outbox", entries[i].MutationType, metrics.ResultForError(entryErr))
			if !entryProcessed {
				b.recordAppliedQuotaOutboxEntries(appliedEntries)
				metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "process", metrics.ResultForError(entryErr), time.Since(start))
				return processed, entryErr
			}
			if fallbackErr == nil {
				fallbackErr = entryErr
			}
		}
	}
	b.recordAppliedQuotaOutboxEntries(appliedEntries)
	if fallbackErr != nil {
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "process", metrics.ResultForError(fallbackErr), time.Since(start))
		return processed, fallbackErr
	}
	if processed > 0 {
		metrics.RecordTenantOperation(b.tenantID, "quota_outbox", "process", "ok", time.Since(start))
	}
	return processed, nil
}

func (b *Dat9Backend) processQuotaOutboxEntry(ctx context.Context, entry *datastore.QuotaOutboxEntry) (applied, processed bool, err error) {
	var applyErr error
	err = b.withQuotaAdmissionLock(ctx, func(tx *sql.Tx) error {
		applyErr = b.applyQuotaOutboxEntry(ctx, entry)
		if applyErr == nil {
			if tx != nil {
				return b.store.AckQuotaOutboxTx(ctx, tx, entry.ID, entry.Receipt)
			}
			return b.store.AckQuotaOutbox(ctx, entry.ID, entry.Receipt)
		}

		retryAt := time.Now().UTC().Add(quotaOutboxRetryDelay(entry.AttemptCount, quotaOutboxRetryBaseDelay, quotaOutboxRetryMaxDelay))
		var retryErr error
		if tx != nil {
			retryErr = b.store.RetryQuotaOutboxTx(ctx, tx, entry.ID, entry.Receipt, retryAt, applyErr.Error())
		} else {
			retryErr = b.store.RetryQuotaOutbox(ctx, entry.ID, entry.Receipt, retryAt, applyErr.Error())
		}
		if retryErr != nil {
			return fmt.Errorf("process quota outbox %d: %w; update retry: %v", entry.ID, applyErr, retryErr)
		}
		processed = true
		return nil
	})
	if err != nil {
		return false, processed, err
	}
	if applyErr != nil {
		return false, processed, applyErr
	}
	return true, true, nil
}

func (b *Dat9Backend) recordAppliedQuotaOutboxEntries(entries []datastore.QuotaOutboxEntry) {
	if len(entries) == 0 {
		return
	}
	if b.quotaUsageCache != nil {
		b.quotaUsageCache.invalidate()
	}
	for _, entry := range entries {
		b.addPendingCentralMutationDeltas(-entry.StorageDelta, -entry.FileDelta, -entry.MediaDelta)
		metrics.RecordTenantOperationCount(b.tenantID, "quota_outbox", entry.MutationType, "ok")
	}
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
		return b.applyQuotaOutboxEntryTx(tx, entry)
	})
}

func (b *Dat9Backend) applyQuotaOutboxEntries(ctx context.Context, entries []datastore.QuotaOutboxEntry) error {
	return b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		for i := range entries {
			if err := b.applyQuotaOutboxEntryTx(tx, &entries[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Dat9Backend) applyQuotaOutboxEntryTx(tx *sql.Tx, entry *datastore.QuotaOutboxEntry) error {
	if entry == nil {
		return fmt.Errorf("quota outbox entry is required")
	}
	// Reuse the central mutation dispatcher; upload_complete is handled there
	// by applyUploadCompleteTx, the same body used by mutation replay.
	return applyCentralQuotaMutationTx(b.metaStore, tx, b.tenantID, entry.MutationType, entry.MutationData, entry.ID)
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
