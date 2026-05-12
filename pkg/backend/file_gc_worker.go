package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	defaultFileGCPollInterval   = 10 * time.Second
	defaultFileGCLeaseDuration  = 5 * time.Minute
	defaultFileGCBatchSize      = 100
	defaultFileGCRecoverLimit   = 100
	defaultFileGCRetryBaseDelay = 2 * time.Second
	defaultFileGCRetryMaxDelay  = 2 * time.Minute
)

// FileGCWorkerOptions controls the durable file cleanup worker.
type FileGCWorkerOptions struct {
	PollInterval  time.Duration
	LeaseDuration time.Duration
	BatchSize     int
	RecoverLimit  int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

// FileGCWorker processes file_gc_tasks for one tenant backend.
type FileGCWorker struct {
	backend *Dat9Backend
	opts    FileGCWorkerOptions
	cancel  context.CancelFunc
	done    chan struct{}
}

// StartFileGCWorker starts durable cleanup for orphaned files owned by this backend.
func (b *Dat9Backend) StartFileGCWorker(opts FileGCWorkerOptions) *FileGCWorker {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fileGCWorker != nil {
		return b.fileGCWorker
	}
	opts.normalize()
	ctx, cancel := context.WithCancel(backgroundWithTrace())
	w := &FileGCWorker{
		backend: b,
		opts:    opts,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	b.fileGCWorker = w
	go w.run(ctx)
	logger.Info(ctx, "file_gc_worker_started",
		zap.Duration("poll_interval", opts.PollInterval),
		zap.Duration("lease_duration", opts.LeaseDuration),
		zap.Int("batch_size", opts.BatchSize))
	return w
}

func (b *Dat9Backend) stopFileGCWorker() {
	b.mu.Lock()
	w := b.fileGCWorker
	b.fileGCWorker = nil
	b.mu.Unlock()
	if w != nil {
		w.Stop()
	}
}

// Stop stops the worker and waits for its goroutine to exit.
func (w *FileGCWorker) Stop() {
	if w == nil || w.cancel == nil {
		return
	}
	w.cancel()
	<-w.done
	w.cancel = nil
}

func (w *FileGCWorker) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()

	w.processAvailable(ctx)
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "file_gc_worker_stopped")
			return
		case <-ticker.C:
			w.processAvailable(ctx)
		}
	}
}

func (w *FileGCWorker) processAvailable(ctx context.Context) {
	now := time.Now().UTC()
	if _, err := w.backend.store.RecoverExpiredFileGCTasks(ctx, now, w.opts.RecoverLimit); err != nil {
		logger.Warn(ctx, "file_gc_recover_expired_failed", zap.Error(err))
		metrics.RecordOperation("file_gc", "recover_expired", "error", 0)
	}
	for i := 0; i < w.opts.BatchSize; i++ {
		processed, err := w.backend.processOneFileGCTask(ctx, w.opts)
		if err != nil {
			logger.Warn(ctx, "file_gc_task_process_failed", zap.Error(err))
		}
		if !processed {
			return
		}
	}
}

// ProcessOneFileGCTask processes at most one queued cleanup task. It is exposed
// for deterministic tests and admin-triggered drain paths.
func (b *Dat9Backend) ProcessOneFileGCTask(ctx context.Context) (bool, error) {
	opts := FileGCWorkerOptions{}
	opts.normalize()
	return b.processOneFileGCTask(ctx, opts)
}

func (b *Dat9Backend) processOneFileGCTask(ctx context.Context, opts FileGCWorkerOptions) (processed bool, err error) {
	start := time.Now()
	task, found, err := b.store.ClaimFileGCTask(ctx, time.Now().UTC(), opts.LeaseDuration)
	if err != nil {
		metrics.RecordOperation("file_gc", "claim", "error", time.Since(start))
		return false, err
	}
	if !found {
		metrics.RecordOperation("file_gc", "claim", "empty", time.Since(start))
		return false, nil
	}

	err = b.processFileGCTask(ctx, task)
	if err == nil {
		if ackErr := b.store.AckFileGCTask(ctx, task.TaskID, task.Receipt); ackErr != nil {
			metrics.RecordOperation("file_gc", "ack", "error", time.Since(start))
			return true, ackErr
		}
		metrics.RecordOperation("file_gc", "process", "ok", time.Since(start))
		return true, nil
	}

	retryAt := time.Now().UTC().Add(fileGCRetryDelay(task.AttemptCount, opts.RetryBase, opts.RetryMax))
	if retryErr := b.store.RetryFileGCTask(ctx, task.TaskID, task.Receipt, retryAt, err.Error()); retryErr != nil {
		metrics.RecordOperation("file_gc", "retry", "error", time.Since(start))
		return true, fmt.Errorf("process file gc task %s: %w; update retry: %v", task.TaskID, err, retryErr)
	}
	metrics.RecordOperation("file_gc", "process", "error", time.Since(start))
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
			logger.Warn(ctx, "file_gc_object_gc_candidate_failed_fallback_delete",
				zap.String("file_id", task.FileID),
				zap.String("storage_ref", task.StorageRef),
				zap.Error(err))
			// Fallback: GC enqueue failed, delete the blob directly to avoid leaking storage.
			b.deleteBlobCtx(ctx, task.StorageRef)
			return nil
		}
		// GC enqueue not configured — fall back to direct delete.
		b.deleteBlobCtx(ctx, task.StorageRef)
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
	if o.PollInterval <= 0 {
		o.PollInterval = defaultFileGCPollInterval
	}
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
