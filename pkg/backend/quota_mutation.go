package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

type fileCreateMutationData struct {
	FileID    string `json:"file_id"`
	SizeBytes int64  `json:"size_bytes"`
	IsMedia   bool   `json:"is_media"`
}

type fileOverwriteMutationData struct {
	FileID       string `json:"file_id"`
	OldSizeBytes int64  `json:"old_size_bytes"`
	OldIsMedia   bool   `json:"old_is_media"`
	NewSizeBytes int64  `json:"new_size_bytes"`
	NewIsMedia   bool   `json:"new_is_media"`
}

// fileDeleteMutationData is retained for replaying historical file_delete
// mutation log entries. New file deletes are cleaned up through file_gc_tasks.
type fileDeleteMutationData struct {
	FileID    string `json:"file_id"`
	SizeBytes int64  `json:"size_bytes"`
	IsMedia   bool   `json:"is_media"`
}

type llmCostMutationData struct {
	TaskType       string `json:"task_type"`
	TaskID         string `json:"task_id"`
	CostMillicents int64  `json:"cost_millicents"`
	RawUnits       int64  `json:"raw_units"`
	RawUnitType    string `json:"raw_unit_type"`
}

func isQuotaMediaContentType(contentType string) bool {
	return strings.HasPrefix(contentType, "image/") || strings.HasPrefix(contentType, "audio/")
}

// logQuotaMutation durably records a mutation in the central quota_mutation_log.
// Returns the log ID and true on success, or (0, false) if the mutation could
// not be logged (caller should treat as fail-open). This is the synchronous
// half of the split mutation path — it MUST complete before the write/fsync
// returns success so that MutationReplayWorker can recover the mutation after
// a crash.
func (b *Dat9Backend) logQuotaMutation(ctx context.Context, mutationType string, payload any) (int64, bool) {
	if b.metaStore == nil || b.tenantID == "" {
		return 0, false
	}
	data, err := json.Marshal(payload)
	if err != nil {
		logger.Warn(ctx, "central_quota_mutation_marshal_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Error(err))
		return 0, false
	}
	logID, err := b.metaStore.InsertMutationLog(ctx, &MutationLogView{
		TenantID:     b.tenantID,
		MutationType: mutationType,
		MutationData: data,
	})
	if err != nil {
		logger.Warn(ctx, "central_quota_mutation_log_insert_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Error(err))
		metrics.RecordOperation("central_quota", mutationType, "fail_open", time.Duration(0))
		return 0, false
	}
	return logID, true
}

// applyQuotaMutation applies a previously-logged mutation and marks it as
// applied. This is the async-safe half of the split mutation path — if it
// fails or never runs, MutationReplayWorker picks up the pending log entry.
func (b *Dat9Backend) applyQuotaMutation(ctx context.Context, mutationType string, logID int64, apply func(tx *sql.Tx) error) {
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		if err := apply(tx); err != nil {
			return err
		}
		return b.metaStore.MarkMutationAppliedTx(tx, logID)
	}); err != nil {
		logger.Warn(ctx, "central_quota_mutation_apply_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Int64("log_id", logID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", mutationType, "pending", time.Duration(0))
		return
	}
	metrics.RecordOperation("central_quota", mutationType, "ok", time.Duration(0))
}

// logAndEnqueueMutation atomically logs a mutation and enqueues its apply
// function under mutationMu. This ensures that within a single backend
// instance, durable log_id order and channel enqueue order are identical,
// preventing reordering between concurrent same-tenant writes on this
// process. The mutex scope is kept minimal: just the log insert (~1ms) +
// channel send (non-blocking into 256-slot buffer).
//
// Cross-instance ordering: in a multi-pod deployment, each pod has its own
// mutationMu and worker queue. Two pods can apply mutations for the same
// tenant in different log_id order. This is a pre-existing condition — the
// the old synchronous log+apply path also had no cross-pod ordering.
// UpsertFileMetaTx is last-writer-wins; MutationReplayWorker replays
// pending (unapplied) entries in (tenant_id, id) order, which handles
// crash recovery. For cross-pod last-writer divergence on file_meta,
// the backfill-quota tool can be run manually to reconcile.
func (b *Dat9Backend) logAndEnqueueMutation(ctx context.Context, mutationType string, payload any, apply func(tx *sql.Tx) error) {
	start := time.Now()

	b.mutationMu.Lock()
	logID, ok := b.logQuotaMutation(ctx, mutationType, payload)
	if !ok {
		b.mutationMu.Unlock()
		return
	}
	b.enqueueMutation(func() {
		b.applyQuotaMutation(context.Background(), mutationType, logID, apply)
	})
	b.mutationMu.Unlock()

	logger.InfoBenchTiming(ctx, "central_quota_mutation_sync_timing",
		zap.String("tenant_id", b.tenantID),
		zap.String("mutation_type", mutationType),
		zap.Int64("log_id", logID),
		zap.Float64("total_ms", backendDurationMs(time.Since(start))))
}

func (b *Dat9Backend) syncCentralFileCreate(ctx context.Context, fileID string, sizeBytes int64, contentType string) {
	isMedia := isQuotaMediaContentType(contentType)
	b.logAndEnqueueMutation(ctx, "file_create", fileCreateMutationData{
		FileID:    fileID,
		SizeBytes: sizeBytes,
		IsMedia:   isMedia,
	}, func(tx *sql.Tx) error {
		if err := b.metaStore.UpsertFileMetaTx(tx, &FileMetaView{
			TenantID:  b.tenantID,
			FileID:    fileID,
			SizeBytes: sizeBytes,
			IsMedia:   isMedia,
		}); err != nil {
			return err
		}
		if sizeBytes != 0 {
			if err := b.metaStore.IncrStorageBytesTx(tx, b.tenantID, sizeBytes); err != nil {
				return err
			}
		}
		if isMedia {
			if err := b.metaStore.IncrMediaFileCountTx(tx, b.tenantID, 1); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Dat9Backend) syncCentralFileOverwrite(ctx context.Context, fileID string, oldSize int64, oldContentType string, newSize int64, newContentType string) {
	oldIsMedia := isQuotaMediaContentType(oldContentType)
	newIsMedia := isQuotaMediaContentType(newContentType)
	storageDelta := newSize - oldSize
	mediaDelta := int64(0)
	switch {
	case !oldIsMedia && newIsMedia:
		mediaDelta = 1
	case oldIsMedia && !newIsMedia:
		mediaDelta = -1
	}
	b.logAndEnqueueMutation(ctx, "file_overwrite", fileOverwriteMutationData{
		FileID:       fileID,
		OldSizeBytes: oldSize,
		OldIsMedia:   oldIsMedia,
		NewSizeBytes: newSize,
		NewIsMedia:   newIsMedia,
	}, func(tx *sql.Tx) error {
		if err := b.metaStore.UpsertFileMetaTx(tx, &FileMetaView{
			TenantID:  b.tenantID,
			FileID:    fileID,
			SizeBytes: newSize,
			IsMedia:   newIsMedia,
		}); err != nil {
			return err
		}
		if storageDelta != 0 {
			if err := b.metaStore.IncrStorageBytesTx(tx, b.tenantID, storageDelta); err != nil {
				return err
			}
		}
		if mediaDelta != 0 {
			if err := b.metaStore.IncrMediaFileCountTx(tx, b.tenantID, mediaDelta); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Dat9Backend) syncCentralLLMCostRecord(ctx context.Context, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) {
	b.logAndEnqueueMutation(ctx, "llm_cost_record", llmCostMutationData{
		TaskType:       taskType,
		TaskID:         taskID,
		CostMillicents: costMillicents,
		RawUnits:       rawUnits,
		RawUnitType:    rawUnitType,
	}, func(tx *sql.Tx) error {
		if err := b.metaStore.InsertCentralLLMUsageTx(tx, &LLMUsageView{
			TenantID:       b.tenantID,
			TaskType:       taskType,
			TaskID:         taskID,
			CostMillicents: costMillicents,
			RawUnits:       rawUnits,
			RawUnitType:    rawUnitType,
		}); err != nil {
			return err
		}
		return b.metaStore.IncrMonthlyLLMCostTx(tx, b.tenantID, costMillicents)
	})
}
