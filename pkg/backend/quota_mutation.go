package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
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

const postCommitQuotaMutationTimeout = 30 * time.Second

// PostCommitQuotaMutationError reports that the user-visible file mutation has
// already committed, but the central quota handoff failed afterward.
type PostCommitQuotaMutationError struct {
	Op  string
	Err error
}

func (e *PostCommitQuotaMutationError) Error() string {
	return fmt.Sprintf("%s after commit: %v", e.Op, e.Err)
}

func (e *PostCommitQuotaMutationError) Unwrap() error { return e.Err }

func postCommitQuotaMutationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(backgroundWithTrace(), postCommitQuotaMutationTimeout)
}

func postCommitQuotaMutationError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &PostCommitQuotaMutationError{Op: op, Err: err}
}

func isQuotaMediaContentType(contentType string) bool {
	return strings.HasPrefix(contentType, "image/") || strings.HasPrefix(contentType, "audio/")
}

// logQuotaMutation durably records a mutation in the central quota_mutation_log.
// This is the only runtime quota accounting handoff after tenant quota_outbox
// removal. Callers must surface an error instead of silently dropping the
// mutation. The current write paths still call this after the tenant DB commit,
// so a process crash between tenant commit and this insert remains a known
// reconciliation/backfill window.
func (b *Dat9Backend) logQuotaMutation(ctx context.Context, mutationType string, payload any) (int64, error) {
	if b.metaStore == nil || b.tenantID == "" {
		return 0, nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		logger.Warn(ctx, "central_quota_mutation_marshal_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Error(err))
		return 0, err
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
		metrics.RecordTenantOperation(b.tenantID, "central_quota", mutationType, "log_error", time.Duration(0))
		return 0, err
	}
	return logID, nil
}

// applyQuotaMutation applies a previously-logged mutation and marks it as
// applied. This is the async-safe half of the split mutation path — if it
// fails or never runs, MutationReplayWorker picks up the pending log entry.
func (b *Dat9Backend) applyQuotaMutation(ctx context.Context, mutationType string, logID int64, pending quotaPendingDeltas, apply func(context.Context, *sql.Tx) error) {
	defer func() {
		if b.quotaUsageCache != nil {
			b.quotaUsageCache.invalidate()
		}
		b.addPendingCentralMutationDeltas(-pending.storageDelta, -pending.fileDelta, -pending.mediaDelta)
	}()
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		if err := apply(ctx, tx); err != nil {
			return err
		}
		return b.metaStore.MarkMutationAppliedTx(tx, logID)
	}); err != nil {
		logger.Warn(ctx, "central_quota_mutation_apply_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Int64("log_id", logID),
			zap.Error(err))
		metrics.RecordTenantOperation(b.tenantID, "central_quota", mutationType, "pending", time.Duration(0))
		return
	}
	metrics.RecordTenantOperation(b.tenantID, "central_quota", mutationType, "ok", time.Duration(0))
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
func (b *Dat9Backend) logAndEnqueueMutation(ctx context.Context, mutationType string, payload any, pending quotaPendingDeltas, apply func(context.Context, *sql.Tx) error) error {
	if b.metaStore == nil || b.tenantID == "" {
		return nil
	}
	start := time.Now()

	b.mutationMu.Lock()
	logID, err := b.logQuotaMutation(ctx, mutationType, payload)
	if err != nil {
		b.mutationMu.Unlock()
		return err
	}
	b.addPendingCentralMutationDeltas(pending.storageDelta, pending.fileDelta, pending.mediaDelta)
	b.enqueueMutation(func(applyCtx context.Context) {
		b.applyQuotaMutation(applyCtx, mutationType, logID, pending, apply)
	})
	b.mutationMu.Unlock()

	logger.InfoBenchTiming(ctx, "central_quota_mutation_sync_timing",
		zap.String("tenant_id", b.tenantID),
		zap.String("mutation_type", mutationType),
		zap.Int64("log_id", logID),
		zap.Float64("total_ms", backendDurationMs(time.Since(start))))
	return nil
}

func applyCentralFileStateTx(store MetaQuotaStore, tx *sql.Tx, tenantID, fileID string, sizeBytes int64, isMedia bool) error {
	oldSize := int64(0)
	oldIsMedia := false
	oldExists := false
	old, err := store.GetFileMetaForUpdateTx(tx, tenantID, fileID)
	if err != nil {
		if !errors.Is(err, meta.ErrNotFound) {
			return err
		}
	} else if old != nil {
		oldExists = true
		oldSize = old.SizeBytes
		oldIsMedia = old.IsMedia
	}
	if err := store.UpsertFileMetaTx(tx, &FileMetaView{
		TenantID:  tenantID,
		FileID:    fileID,
		SizeBytes: sizeBytes,
		IsMedia:   isMedia,
	}); err != nil {
		return err
	}
	storageDelta := sizeBytes - oldSize
	if storageDelta != 0 {
		if err := store.IncrStorageBytesTx(tx, tenantID, storageDelta); err != nil {
			return err
		}
	}
	if !oldExists {
		if err := store.IncrFileCountTx(tx, tenantID, 1); err != nil {
			return err
		}
	}
	mediaDelta := quotaMediaDelta(oldIsMedia, isMedia)
	if mediaDelta != 0 {
		if err := store.IncrMediaFileCountTx(tx, tenantID, mediaDelta); err != nil {
			return err
		}
	}
	return nil
}

func applyCentralFileCreateTx(store MetaQuotaStore, tx *sql.Tx, tenantID string, data fileCreateMutationData) error {
	return applyCentralFileStateTx(store, tx, tenantID, data.FileID, data.SizeBytes, data.IsMedia)
}

func applyCentralFileOverwriteTx(store MetaQuotaStore, tx *sql.Tx, tenantID string, data fileOverwriteMutationData) error {
	return applyCentralFileStateTx(store, tx, tenantID, data.FileID, data.NewSizeBytes, data.NewIsMedia)
}

func (b *Dat9Backend) recordCentralFileCreateMutation(ctx context.Context, fileID string, sizeBytes int64, contentType string) error {
	isMedia := isQuotaMediaContentType(contentType)
	data := fileCreateMutationData{
		FileID:    fileID,
		SizeBytes: sizeBytes,
		IsMedia:   isMedia,
	}
	mediaDelta := int64(0)
	if isMedia {
		mediaDelta = 1
	}
	return b.logAndEnqueueMutation(ctx, "file_create", data, quotaPendingDeltas{
		storageDelta: sizeBytes,
		fileDelta:    1,
		mediaDelta:   mediaDelta,
	}, func(applyCtx context.Context, tx *sql.Tx) error {
		return applyCentralFileCreateTx(b.metaStore, tx, b.tenantID, data)
	})
}

func (b *Dat9Backend) recordCentralFileOverwriteMutation(ctx context.Context, fileID string, oldSize int64, oldContentType string, newSize int64, newContentType string) error {
	oldIsMedia := isQuotaMediaContentType(oldContentType)
	newIsMedia := isQuotaMediaContentType(newContentType)
	data := fileOverwriteMutationData{
		FileID:       fileID,
		OldSizeBytes: oldSize,
		OldIsMedia:   oldIsMedia,
		NewSizeBytes: newSize,
		NewIsMedia:   newIsMedia,
	}
	return b.logAndEnqueueMutation(ctx, "file_overwrite", data, quotaPendingDeltas{
		storageDelta: newSize - oldSize,
		mediaDelta:   quotaMediaDelta(oldIsMedia, newIsMedia),
	}, func(applyCtx context.Context, tx *sql.Tx) error {
		return applyCentralFileOverwriteTx(b.metaStore, tx, b.tenantID, data)
	})
}

func (b *Dat9Backend) syncCentralLLMCostRecord(ctx context.Context, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) error {
	return b.logAndEnqueueMutation(ctx, "llm_cost_record", llmCostMutationData{
		TaskType:       taskType,
		TaskID:         taskID,
		CostMillicents: costMillicents,
		RawUnits:       rawUnits,
		RawUnitType:    rawUnitType,
	}, quotaPendingDeltas{}, func(applyCtx context.Context, tx *sql.Tx) error {
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
