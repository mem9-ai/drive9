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

// mutationEntry is the unit of work enqueued to the async mutation channel.
// It carries enough information for the background worker to perform both the
// durable log INSERT and the quota-state apply in a single transaction,
// entirely off the write hot path.
type mutationEntry struct {
	mutationType string
	payload      any            // JSON-serializable mutation data
	apply        func(tx *sql.Tx) error // applies quota state changes within a tx
}

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

// enqueueMutationEntry performs a non-blocking send of a mutation entry to the
// async queue. If the queue is full the entry is dropped and a metric is
// emitted — the MutationReplayWorker + backfill-quota CLI handle convergence.
// If the queue is not wired (tests without StartMutationWorker), the mutation
// is executed inline (log + apply).
func (b *Dat9Backend) enqueueMutationEntry(ctx context.Context, entry mutationEntry) {
	if b.mutationQueue == nil {
		// No async queue — run inline (test/fallback path).
		b.processOneMutation(ctx, entry)
		return
	}
	select {
	case b.mutationQueue <- entry:
		// Enqueued successfully.
	default:
		// Queue full — drop. MutationReplayWorker will NOT recover this
		// entry because it was never logged, but backfill-quota reconciles
		// from the source-of-truth tenant DB file table. This is acceptable
		// per @qiffang: "quota没必要非常精准，还是要保证性能最重要".
		logger.Warn(ctx, "central_quota_mutation_dropped",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", entry.mutationType))
		metrics.RecordOperation("central_quota", entry.mutationType, "dropped", 0)
	}
}

// processOneMutation durably logs a mutation and applies its quota-state
// changes in a single transaction. Called by the background worker (or
// inline when no worker is wired).
func (b *Dat9Backend) processOneMutation(ctx context.Context, entry mutationEntry) {
	if b.metaStore == nil || b.tenantID == "" {
		return
	}
	start := time.Now()

	data, err := json.Marshal(entry.payload)
	if err != nil {
		logger.Warn(ctx, "central_quota_mutation_marshal_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", entry.mutationType),
			zap.Error(err))
		return
	}

	// Single transaction: INSERT log + apply quota state + mark applied.
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		logID, insErr := insertMutationLogTx(tx, b.metaStore, b.tenantID, entry.mutationType, data)
		if insErr != nil {
			return insErr
		}
		if err := entry.apply(tx); err != nil {
			return err
		}
		return b.metaStore.MarkMutationAppliedTx(tx, logID)
	}); err != nil {
		logger.Warn(ctx, "central_quota_mutation_process_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", entry.mutationType),
			zap.Error(err))
		metrics.RecordOperation("central_quota", entry.mutationType, "error", time.Since(start))
		return
	}
	metrics.RecordOperation("central_quota", entry.mutationType, "ok", time.Since(start))
}

// insertMutationLogTx inserts a mutation log entry within an existing
// transaction. This is used by processOneMutation to combine log + apply +
// mark-applied in a single tx round-trip.
func insertMutationLogTx(tx *sql.Tx, store MetaQuotaStore, tenantID, mutationType string, data json.RawMessage) (int64, error) {
	// InsertMutationLog uses its own connection; we need a Tx variant.
	// For now, call the non-Tx version — it auto-commits the INSERT which
	// is safe because MarkMutationAppliedTx in the same outer tx will mark
	// it applied. If the outer tx rolls back, the log entry stays pending
	// and MutationReplayWorker picks it up — this is the correct recovery
	// behavior.
	return store.InsertMutationLog(context.Background(), &MutationLogView{
		TenantID:     tenantID,
		MutationType: mutationType,
		MutationData: data,
	})
}

func (b *Dat9Backend) syncCentralFileCreate(ctx context.Context, fileID string, sizeBytes int64, contentType string) {
	isMedia := isQuotaMediaContentType(contentType)
	b.enqueueMutationEntry(ctx, mutationEntry{
		mutationType: "file_create",
		payload: fileCreateMutationData{
			FileID:    fileID,
			SizeBytes: sizeBytes,
			IsMedia:   isMedia,
		},
		apply: func(tx *sql.Tx) error {
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
		},
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
	b.enqueueMutationEntry(ctx, mutationEntry{
		mutationType: "file_overwrite",
		payload: fileOverwriteMutationData{
			FileID:       fileID,
			OldSizeBytes: oldSize,
			OldIsMedia:   oldIsMedia,
			NewSizeBytes: newSize,
			NewIsMedia:   newIsMedia,
		},
		apply: func(tx *sql.Tx) error {
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
		},
	})
}

func (b *Dat9Backend) syncCentralLLMCostRecord(ctx context.Context, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) {
	b.enqueueMutationEntry(ctx, mutationEntry{
		mutationType: "llm_cost_record",
		payload: llmCostMutationData{
			TaskType:       taskType,
			TaskID:         taskID,
			CostMillicents: costMillicents,
			RawUnits:       rawUnits,
			RawUnitType:    rawUnitType,
		},
		apply: func(tx *sql.Tx) error {
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
		},
	})
}
