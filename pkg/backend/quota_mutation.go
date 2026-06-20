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

func (b *Dat9Backend) applyLoggedQuotaMutation(ctx context.Context, mutationType string, payload any, apply func(tx *sql.Tx) error) {
	timingEnabled := logger.BenchTimingLogEnabled()
	start := time.Time{}
	if timingEnabled {
		start = time.Now()
	}
	var marshalDuration time.Duration
	var insertLogDuration time.Duration
	var applyTxDuration time.Duration
	logTiming := func(result string, logID int64, err error) {
		if !timingEnabled {
			return
		}
		fields := []zap.Field{
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.String("result", result),
			zap.Int64("log_id", logID),
			zap.Float64("marshal_ms", backendDurationMs(marshalDuration)),
			zap.Float64("insert_log_ms", backendDurationMs(insertLogDuration)),
			zap.Float64("apply_tx_ms", backendDurationMs(applyTxDuration)),
			zap.Float64("total_ms", backendDurationMs(time.Since(start))),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		logger.InfoBenchTiming(ctx, "central_quota_mutation_timing", fields...)
	}
	if b.metaStore == nil || b.tenantID == "" {
		logTiming("skipped", 0, nil)
		return
	}
	marshalStart := time.Time{}
	if timingEnabled {
		marshalStart = time.Now()
	}
	data, err := json.Marshal(payload)
	if timingEnabled {
		marshalDuration = time.Since(marshalStart)
	}
	if err != nil {
		logTiming("marshal_error", 0, err)
		logger.Warn(ctx, "central_quota_mutation_marshal_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Error(err))
		return
	}
	insertLogStart := time.Time{}
	if timingEnabled {
		insertLogStart = time.Now()
	}
	logID, err := b.metaStore.InsertMutationLog(ctx, &MutationLogView{
		TenantID:     b.tenantID,
		MutationType: mutationType,
		MutationData: data,
	})
	if timingEnabled {
		insertLogDuration = time.Since(insertLogStart)
	}
	if err != nil {
		logTiming("insert_log_error", 0, err)
		logger.Warn(ctx, "central_quota_mutation_log_insert_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Error(err))
		metrics.RecordOperation("central_quota", mutationType, "fail_open", time.Duration(0))
		return
	}
	applyTxStart := time.Time{}
	if timingEnabled {
		applyTxStart = time.Now()
	}
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		if err := apply(tx); err != nil {
			return err
		}
		return b.metaStore.MarkMutationAppliedTx(tx, logID)
	}); err != nil {
		if timingEnabled {
			applyTxDuration = time.Since(applyTxStart)
		}
		logTiming("apply_error", logID, err)
		logger.Warn(ctx, "central_quota_mutation_apply_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Int64("log_id", logID),
			zap.Error(err))
		metrics.RecordOperation("central_quota", mutationType, "pending", time.Duration(0))
		return
	}
	if timingEnabled {
		applyTxDuration = time.Since(applyTxStart)
	}
	logTiming("ok", logID, nil)
	metrics.RecordOperation("central_quota", mutationType, "ok", time.Duration(0))
}

func (b *Dat9Backend) syncCentralFileCreate(ctx context.Context, fileID string, sizeBytes int64, contentType string) {
	isMedia := isQuotaMediaContentType(contentType)
	b.applyLoggedQuotaMutation(ctx, "file_create", fileCreateMutationData{
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
	b.applyLoggedQuotaMutation(ctx, "file_overwrite", fileOverwriteMutationData{
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
	b.applyLoggedQuotaMutation(ctx, "llm_cost_record", llmCostMutationData{
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
