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
// Used for storage quota mutations (file create/overwrite) that can tolerate
// eventual consistency. LLM cost records use processOneMutation directly
// (synchronous) because they are billable and must not be dropped.
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
		// Queue full — drop. The entry was never logged, so neither
		// MutationReplayWorker nor automatic reconciliation will recover it.
		// Quota counters will remain inaccurate until a manual run of the
		// backfill-quota CLI, which reconciles from the source-of-truth
		// tenant DB file table. Acceptable for storage quota per product
		// decision (performance > precision).
		logger.Warn(ctx, "central_quota_mutation_dropped",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", entry.mutationType))
		metrics.RecordOperation("central_quota", entry.mutationType, "dropped", 0)
	}
}

// processOneMutation durably logs a mutation and applies its quota-state
// changes. Called by the background worker (or inline when no worker is
// wired).
//
// Two-phase approach:
//  1. INSERT into quota_mutation_log (auto-commit, separate connection).
//  2. InTx: apply quota state + MarkMutationAppliedTx.
//
// If phase 2 fails or crashes, the log entry stays pending and
// MutationReplayWorker picks it up on its next poll (30s interval).
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

	// Phase 1: durable log INSERT (auto-commit, own connection).
	logID, err := b.metaStore.InsertMutationLog(ctx, &MutationLogView{
		TenantID:     b.tenantID,
		MutationType: entry.mutationType,
		MutationData: data,
	})
	if err != nil {
		logger.Warn(ctx, "central_quota_mutation_log_insert_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", entry.mutationType),
			zap.Error(err))
		metrics.RecordOperation("central_quota", entry.mutationType, "log_failed", time.Since(start))
		return
	}

	// Phase 2: apply quota state + mark applied in one transaction.
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		if err := entry.apply(tx); err != nil {
			return err
		}
		return b.metaStore.MarkMutationAppliedTx(tx, logID)
	}); err != nil {
		logger.Warn(ctx, "central_quota_mutation_apply_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", entry.mutationType),
			zap.Int64("log_id", logID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", entry.mutationType, "pending", time.Since(start))
		return
	}
	metrics.RecordOperation("central_quota", entry.mutationType, "ok", time.Since(start))
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

// syncCentralLLMCostRecord logs and applies an LLM cost mutation
// synchronously. Unlike file create/overwrite, LLM cost records are
// billable and must not be silently dropped. This is not in the file
// write hot path (called from image/audio extract workers), so the
// synchronous overhead is acceptable.
func (b *Dat9Backend) syncCentralLLMCostRecord(ctx context.Context, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) {
	b.processOneMutation(ctx, mutationEntry{
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
