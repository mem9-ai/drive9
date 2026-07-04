package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	defaultFileGCLeaseDuration  = 5 * time.Minute
	defaultFileGCBatchSize      = 100
	defaultFileGCRecoverLimit   = 100
	defaultFileGCRetryBaseDelay = 2 * time.Second
	defaultFileGCRetryMaxDelay  = 2 * time.Minute
)

// FileGCWorkerOptions controls file GC task processing parameters. These
// options are used by ProcessOneFileGCTask (called by the unified tenant
// worker); the per-backend goroutine has been removed.
type FileGCWorkerOptions struct {
	LeaseDuration time.Duration
	BatchSize     int
	RecoverLimit  int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

// ProcessOneFileGCTask processes at most one queued cleanup task. It is exposed
// for deterministic tests, admin-triggered drain paths, and the unified tenant
// worker's kick-driven drain.
func (b *Dat9Backend) ProcessOneFileGCTask(ctx context.Context) (bool, error) {
	opts := FileGCWorkerOptions{}
	opts.normalize()
	return b.processOneFileGCTask(ctx, opts)
}

func (b *Dat9Backend) processOneFileGCTask(ctx context.Context, opts FileGCWorkerOptions) (processed bool, err error) {
	start := time.Now()
	tenantID := b.tenantID
	task, found, err := b.store.ClaimFileGCTask(ctx, time.Now().UTC(), opts.LeaseDuration)
	if err != nil {
		metrics.RecordTenantOperation(tenantID, "file_gc", "claim", metrics.ResultForError(err), time.Since(start))
		return false, err
	}
	if !found {
		metrics.RecordTenantOperation(tenantID, "file_gc", "claim", "empty", time.Since(start))
		return false, nil
	}

	err = b.processFileGCTask(ctx, task)
	if err == nil {
		if ackErr := b.store.AckFileGCTask(ctx, task.TaskID, task.Receipt); ackErr != nil {
			metrics.RecordTenantOperation(tenantID, "file_gc", "ack", metrics.ResultForError(ackErr), time.Since(start))
			return true, ackErr
		}
		metrics.RecordTenantOperation(tenantID, "file_gc", "process", "ok", time.Since(start))
		return true, nil
	}

	retryAt := time.Now().UTC().Add(fileGCRetryDelay(task.AttemptCount, opts.RetryBase, opts.RetryMax))
	if retryErr := b.store.RetryFileGCTask(ctx, task.TaskID, task.Receipt, retryAt, err.Error()); retryErr != nil {
		metrics.RecordTenantOperation(tenantID, "file_gc", "retry", metrics.ResultForError(retryErr), time.Since(start))
		return true, fmt.Errorf("process file gc task %s: %w; update retry: %v", task.TaskID, err, retryErr)
	}
	metrics.RecordTenantOperation(tenantID, "file_gc", "process", metrics.ResultForError(err), time.Since(start))
	return true, err
}

func (b *Dat9Backend) processFileGCTask(ctx context.Context, task *datastore.FileGCTask) error {
	if task == nil {
		return fmt.Errorf("file gc task is required")
	}
	if err := b.deleteCentralFileMetaForGCTask(ctx, task); err != nil {
		return err
	}
	if task.StorageType == datastore.StorageS3 && task.StorageRef != "" {
		handled, err := b.enqueueObjectGCCandidateCtx(ctx, task.StorageRef, meta.ObjectGCReasonFileDelete, task.FileID)
		if handled && err == nil {
			return nil
		}
		if err != nil {
			logger.Warn(ctx, "file_gc_object_gc_candidate_failed",
				zap.String("tenant_id", b.tenantID),
				zap.String("file_id", task.FileID),
				zap.String("storage_ref", task.StorageRef),
				zap.Error(err))
			return err
		}
		return fmt.Errorf("object gc candidate enqueue is not configured for storage ref %s", task.StorageRef)
	}
	return nil
}

func (b *Dat9Backend) deleteCentralFileMetaForGCTask(ctx context.Context, task *datastore.FileGCTask) error {
	if b.metaStore == nil || b.tenantID == "" {
		return nil
	}
	// A fail-open create/overwrite can still be pending in the central outbox.
	// Deleting/acking GC before that mutation replays would let replay recreate
	// quota state for an already-deleted tenant file.
	if err := b.checkNoPendingCentralFileMutation(ctx, task.FileID); err != nil {
		return err
	}
	fm, err := b.metaStore.GetFileMeta(ctx, b.tenantID, task.FileID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) || errors.Is(err, datastore.ErrNotFound) {
			return nil
		}
		return err
	}
	return b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		deleted, err := b.metaStore.DeleteFileMetaIfExistsTx(tx, b.tenantID, task.FileID)
		if err != nil {
			return err
		}
		if !deleted {
			return nil
		}
		return b.applyCentralFileDeleteCountersTx(tx, fm.SizeBytes, fm.IsMedia)
	})
}

func (b *Dat9Backend) checkNoPendingCentralFileMutation(ctx context.Context, fileID string) error {
	pending, err := b.metaStore.HasPendingFileMutation(ctx, b.tenantID, fileID)
	if err != nil {
		return err
	}
	if pending {
		return fmt.Errorf("central file mutation pending for file %s", fileID)
	}
	return nil
}

func (b *Dat9Backend) applyCentralFileDeleteCountersTx(tx *sql.Tx, sizeBytes int64, isMedia bool) error {
	if sizeBytes != 0 {
		if err := b.metaStore.IncrStorageBytesTx(tx, b.tenantID, -sizeBytes); err != nil {
			return err
		}
	}
	if err := b.metaStore.IncrFileCountTx(tx, b.tenantID, -1); err != nil {
		return err
	}
	if isMedia {
		if err := b.metaStore.IncrMediaFileCountTx(tx, b.tenantID, -1); err != nil {
			return err
		}
	}
	return nil
}

func fileGCRetryDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = defaultFileGCRetryBaseDelay
	}
	if max <= 0 {
		max = defaultFileGCRetryMaxDelay
	}
	if attempt <= 1 {
		return base
	}
	delay := base
	for i := 1; i < attempt && delay < max; i++ {
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func (o *FileGCWorkerOptions) normalize() {
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = defaultFileGCLeaseDuration
	}
	if o.BatchSize <= 0 {
		o.BatchSize = defaultFileGCBatchSize
	}
	if o.RecoverLimit <= 0 {
		o.RecoverLimit = defaultFileGCRecoverLimit
	}
	if o.RetryBase <= 0 {
		o.RetryBase = defaultFileGCRetryBaseDelay
	}
	if o.RetryMax <= 0 {
		o.RetryMax = defaultFileGCRetryMaxDelay
	}
}
