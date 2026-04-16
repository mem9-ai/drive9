package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	replayPollInterval = 30 * time.Second
	replayMinAge       = 5 * time.Second // only replay mutations older than 5s (avoid racing with inline apply)
	replayBatchLimit   = 100
	replayMaxRetries   = 5
)

// MutationReplayWorker reads pending mutations from the quota_mutation_log
// and applies them idempotently. It runs as a background goroutine.
type MutationReplayWorker struct {
	store  MetaQuotaStore
	cancel context.CancelFunc
	done   chan struct{}
}

// StartMutationReplayWorker starts the background replay loop.
// Returns nil if store is nil (central quota not wired).
func StartMutationReplayWorker(store MetaQuotaStore) *MutationReplayWorker {
	if store == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &MutationReplayWorker{
		store:  store,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go w.run(ctx)
	return w
}

// Stop gracefully shuts down the replay worker.
func (w *MutationReplayWorker) Stop() {
	if w == nil {
		return
	}
	w.cancel()
	<-w.done
}

func (w *MutationReplayWorker) run(ctx context.Context) {
	defer close(w.done)

	logger.Info(ctx, "mutation_replay_worker_started")
	ticker := time.NewTicker(replayPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "mutation_replay_worker_stopped")
			return
		case <-ticker.C:
			w.replayBatch(ctx)
		}
	}
}

func (w *MutationReplayWorker) replayBatch(ctx context.Context) {
	start := time.Now()
	entries, err := w.store.ListPendingMutations(ctx, replayMinAge, replayBatchLimit)
	if err != nil {
		logger.Warn(ctx, "mutation_replay_list_failed", zap.Error(err))
		metrics.RecordOperation("mutation_replay", "list", "error", time.Since(start))
		return
	}
	if len(entries) == 0 {
		return
	}

	applied := 0
	failed := 0
	for _, entry := range entries {
		if err := w.replayOne(ctx, entry); err != nil {
			logger.Warn(ctx, "mutation_replay_entry_failed",
				zap.Int64("log_id", entry.ID),
				zap.String("tenant_id", entry.TenantID),
				zap.String("mutation_type", entry.MutationType),
				zap.Error(err))
			if rErr := w.store.IncrMutationRetry(ctx, entry.ID, replayMaxRetries); rErr != nil {
				logger.Error(ctx, "mutation_replay_incr_retry_failed",
					zap.Int64("log_id", entry.ID),
					zap.Error(rErr))
			}
			failed++
		} else {
			applied++
		}
	}

	metrics.RecordGauge("mutation_replay", "batch_applied", float64(applied))
	metrics.RecordGauge("mutation_replay", "batch_failed", float64(failed))
	logger.Info(ctx, "mutation_replay_batch_complete",
		zap.Int("total", len(entries)),
		zap.Int("applied", applied),
		zap.Int("failed", failed),
		zap.Float64("duration_ms", float64(time.Since(start).Milliseconds())))
}

func (w *MutationReplayWorker) replayOne(ctx context.Context, entry MutationLogView) error {
	return w.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := w.applyMutation(tx, entry); err != nil {
			return err
		}
		return w.store.MarkMutationAppliedTx(tx, entry.ID)
	})
}

func (w *MutationReplayWorker) applyMutation(tx *sql.Tx, entry MutationLogView) error {
	switch entry.MutationType {
	case "file_create":
		var data fileCreateMutationData
		if err := json.Unmarshal(entry.MutationData, &data); err != nil {
			return err
		}
		if err := w.store.UpsertFileMetaTx(tx, &FileMetaView{
			TenantID:  entry.TenantID,
			FileID:    data.FileID,
			SizeBytes: data.SizeBytes,
			IsMedia:   data.IsMedia,
		}); err != nil {
			return err
		}
		if data.SizeBytes != 0 {
			if err := w.store.IncrStorageBytesTx(tx, entry.TenantID, data.SizeBytes); err != nil {
				return err
			}
		}
		if data.IsMedia {
			if err := w.store.IncrMediaFileCountTx(tx, entry.TenantID, 1); err != nil {
				return err
			}
		}
		return nil

	case "file_overwrite":
		var data fileOverwriteMutationData
		if err := json.Unmarshal(entry.MutationData, &data); err != nil {
			return err
		}
		if err := w.store.UpsertFileMetaTx(tx, &FileMetaView{
			TenantID:  entry.TenantID,
			FileID:    data.FileID,
			SizeBytes: data.NewSizeBytes,
			IsMedia:   data.NewIsMedia,
		}); err != nil {
			return err
		}
		storageDelta := data.NewSizeBytes - data.OldSizeBytes
		if storageDelta != 0 {
			if err := w.store.IncrStorageBytesTx(tx, entry.TenantID, storageDelta); err != nil {
				return err
			}
		}
		mediaDelta := int64(0)
		switch {
		case !data.OldIsMedia && data.NewIsMedia:
			mediaDelta = 1
		case data.OldIsMedia && !data.NewIsMedia:
			mediaDelta = -1
		}
		if mediaDelta != 0 {
			if err := w.store.IncrMediaFileCountTx(tx, entry.TenantID, mediaDelta); err != nil {
				return err
			}
		}
		return nil

	case "file_delete":
		var data fileDeleteMutationData
		if err := json.Unmarshal(entry.MutationData, &data); err != nil {
			return err
		}
		if err := w.store.DeleteFileMetaTx(tx, entry.TenantID, data.FileID); err != nil {
			return err
		}
		if data.SizeBytes != 0 {
			if err := w.store.IncrStorageBytesTx(tx, entry.TenantID, -data.SizeBytes); err != nil {
				return err
			}
		}
		if data.IsMedia {
			if err := w.store.IncrMediaFileCountTx(tx, entry.TenantID, -1); err != nil {
				return err
			}
		}
		return nil

	case "upload_complete":
		var data uploadCompleteMutationData
		if err := json.Unmarshal(entry.MutationData, &data); err != nil {
			return err
		}
		if err := w.store.TransferReservedToConfirmedTx(tx, entry.TenantID, -data.ReservedBytes, data.ReservedBytes); err != nil {
			return err
		}
		if data.OldSizeBytes != 0 {
			if err := w.store.IncrStorageBytesTx(tx, entry.TenantID, -data.OldSizeBytes); err != nil {
				return err
			}
		}
		if err := w.store.UpsertFileMetaTx(tx, &FileMetaView{
			TenantID:  entry.TenantID,
			FileID:    data.FileID,
			SizeBytes: data.NewSizeBytes,
			IsMedia:   data.NewIsMedia,
		}); err != nil {
			return err
		}
		mediaDelta := int64(0)
		switch {
		case !data.OldIsMedia && data.NewIsMedia:
			mediaDelta = 1
		case data.OldIsMedia && !data.NewIsMedia:
			mediaDelta = -1
		}
		if mediaDelta != 0 {
			if err := w.store.IncrMediaFileCountTx(tx, entry.TenantID, mediaDelta); err != nil {
				return err
			}
		}
		if err := w.store.UpdateUploadReservationStatusTx(tx, entry.TenantID, data.UploadID, "completed"); err != nil {
			return err
		}
		return nil

	case "llm_cost_record":
		var data llmCostMutationData
		if err := json.Unmarshal(entry.MutationData, &data); err != nil {
			return err
		}
		if err := w.store.InsertCentralLLMUsageTx(tx, &LLMUsageView{
			TenantID:       entry.TenantID,
			TaskType:       data.TaskType,
			TaskID:         data.TaskID,
			CostMillicents: data.CostMillicents,
			RawUnits:       data.RawUnits,
			RawUnitType:    data.RawUnitType,
		}); err != nil {
			return err
		}
		return w.store.IncrMonthlyLLMCostTx(tx, entry.TenantID, data.CostMillicents)

	default:
		logger.Warn(context.Background(), "mutation_replay_unknown_type",
			zap.String("mutation_type", entry.MutationType),
			zap.Int64("log_id", entry.ID))
		return nil // skip unknown types gracefully
	}
}
