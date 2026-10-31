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

// quotaCounterDeltas accumulates the tenant_quota_usage counter adjustments
// produced by one mutation apply. Applies RETURN their deltas instead of
// executing the per-counter Incr*Tx statements inline; the transaction owner
// (dispatcher batch runner, per-item applyQuotaMutation, or replayOne)
// aggregates them per tenant and flushes them at the END of the transaction
// via IncrQuotaUsageCountersTx — "hot row last" — so the single per-tenant
// tenant_quota_usage row is locked only just before commit instead of from
// the first mutation apply to commit, and N mutations for one tenant
// collapse into one UPDATE.
type quotaCounterDeltas struct {
	storageBytes   int64
	fileCount      int64
	mediaFileCount int64
	reservedBytes  int64
}

func (d *quotaCounterDeltas) add(o quotaCounterDeltas) {
	d.storageBytes += o.storageBytes
	d.fileCount += o.fileCount
	d.mediaFileCount += o.mediaFileCount
	d.reservedBytes += o.reservedBytes
}

// applyQuotaCounterDeltasTx flushes one tenant's aggregated counter deltas at
// the end of a mutation transaction. IncrQuotaUsageCountersTx is a no-op on
// all-zero deltas, so callers may invoke this unconditionally.
func applyQuotaCounterDeltasTx(store MetaQuotaStore, tx *sql.Tx, tenantID string, deltas quotaCounterDeltas) error {
	return store.IncrQuotaUsageCountersTx(tx, tenantID,
		deltas.storageBytes, deltas.fileCount, deltas.mediaFileCount, deltas.reservedBytes)
}

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
// reconciliation window.
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
		b.recordTenantOperation("central_quota", mutationType, "log_error", time.Duration(0))
		return 0, err
	}
	return logID, nil
}

// applyQuotaMutation applies a previously-logged mutation and marks it as
// applied. This is the async-safe half of the split mutation path — if it
// fails or never runs, MutationReplayWorker picks up the pending log entry.
// The apply returns its tenant_quota_usage counter deltas instead of
// executing them inline; they are flushed in one statement right before the
// mark ("hot row last"), keeping the semantics identical from the caller's
// perspective while shrinking the hot-row lock window to just before commit.
func (b *Dat9Backend) applyQuotaMutation(ctx context.Context, mutationType string, logID int64, pending quotaPendingDeltas, apply func(context.Context, *sql.Tx) (quotaCounterDeltas, error)) {
	if err := b.metaStore.InTx(ctx, func(tx *sql.Tx) error {
		deltas, err := apply(ctx, tx)
		if err != nil {
			return err
		}
		if err := applyQuotaCounterDeltasTx(b.metaStore, tx, b.tenantID, deltas); err != nil {
			return err
		}
		return b.metaStore.MarkMutationAppliedTx(tx, logID)
	}); err != nil {
		logger.Warn(ctx, "central_quota_mutation_apply_failed",
			zap.String("tenant_id", b.tenantID),
			zap.String("mutation_type", mutationType),
			zap.Int64("log_id", logID),
			zap.Error(err))
		b.recordTenantOperation("central_quota", mutationType, "pending", time.Duration(0))
		return
	}
	if b.quotaUsageCache != nil {
		b.quotaUsageCache.invalidate()
	}
	b.clearPendingCentralMutationDeltas(pending.storageDelta, pending.fileDelta, pending.mediaDelta)
	b.recordTenantOperation("central_quota", mutationType, "ok", time.Duration(0))
}

// logAndEnqueueMutation atomically logs a mutation and enqueues its apply
// function under mutationMu. This ensures that within a single backend
// instance, durable log_id order and dispatcher enqueue order are identical,
// preventing reordering between concurrent same-tenant writes on this
// process. The mutex scope is kept minimal: just the log insert (~1ms) +
// shard channel send (non-blocking while the 4096-slot shard buffer has
// capacity; a full buffer applies backpressure).
//
// Cross-instance ordering: in a multi-pod deployment, each pod has its own
// mutationMu and dispatcher. Two pods can apply mutations for the same
// tenant in different log_id order. This is a pre-existing condition — the
// the old synchronous log+apply path also had no cross-pod ordering.
// UpsertFileMetaTx is last-writer-wins; MutationReplayWorker replays
// pending (unapplied) entries in (tenant_id, id) order, which handles
// crash recovery. Cross-pod last-writer divergence on file_meta requires
// operational reconciliation from tenant file metadata.
func (b *Dat9Backend) logAndEnqueueMutation(ctx context.Context, mutationType string, payload any, pending quotaPendingDeltas, apply func(context.Context, *sql.Tx) (quotaCounterDeltas, error)) error {
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
	b.enqueueMutationItem(ctx, quotaMutationItem{
		backend:        b,
		store:          b.metaStore,
		tenantID:       b.tenantID,
		tidbCloudOrgID: b.tidbCloudOrgID,
		mutationType:   mutationType,
		logID:          logID,
		pending:        pending,
		apply:          apply,
	})
	b.mutationMu.Unlock()

	logger.InfoBenchTiming(ctx, "central_quota_mutation_sync_timing",
		zap.String("tenant_id", b.tenantID),
		zap.String("mutation_type", mutationType),
		zap.Int64("log_id", logID),
		zap.Float64("total_ms", backendDurationMs(time.Since(start))))
	return nil
}

// applyCentralFileStateTx applies a create/overwrite shadow-state mutation:
// read the current tenant_file_meta row (FOR UPDATE), upsert it, and RETURN
// the resulting tenant_quota_usage counter deltas for the caller to flush at
// the end of the transaction ("hot row last" — see quotaCounterDeltas).
func applyCentralFileStateTx(store MetaQuotaStore, tx *sql.Tx, tenantID, fileID string, sizeBytes int64, isMedia bool) (quotaCounterDeltas, error) {
	oldSize := int64(0)
	oldIsMedia := false
	oldExists := false
	old, err := store.GetFileMetaForUpdateTx(tx, tenantID, fileID)
	if err != nil {
		if !errors.Is(err, meta.ErrNotFound) {
			return quotaCounterDeltas{}, err
		}
	} else if old != nil {
		oldExists = true
		oldSize = old.SizeBytes
		oldIsMedia = old.IsMedia
	}
	// No-op fast path: the shadow row already matches the new state, so the
	// upsert would write identical values. Skipping it is safe because
	// UpsertFileMetaTx only maintains size_bytes/is_media (no updated_at
	// upkeep) and nothing consumes tenant_file_meta.updated_at for
	// correctness — file GC settlement keys off HasPendingFileMutation plus
	// size/isMedia. This avoids a useless write for the overwhelming
	// majority of mutations (same-size overwrites). The mutation itself is
	// still marked applied by the caller.
	if oldExists && oldSize == sizeBytes && oldIsMedia == isMedia {
		return quotaCounterDeltas{}, nil
	}
	if err := store.UpsertFileMetaTx(tx, &FileMetaView{
		TenantID:  tenantID,
		FileID:    fileID,
		SizeBytes: sizeBytes,
		IsMedia:   isMedia,
	}); err != nil {
		return quotaCounterDeltas{}, err
	}
	deltas := quotaCounterDeltas{
		storageBytes:   sizeBytes - oldSize,
		mediaFileCount: quotaMediaDelta(oldIsMedia, isMedia),
	}
	if !oldExists {
		deltas.fileCount = 1
	}
	return deltas, nil
}

func applyCentralFileCreateTx(store MetaQuotaStore, tx *sql.Tx, tenantID string, data fileCreateMutationData) (quotaCounterDeltas, error) {
	return applyCentralFileStateTx(store, tx, tenantID, data.FileID, data.SizeBytes, data.IsMedia)
}

func applyCentralFileOverwriteTx(store MetaQuotaStore, tx *sql.Tx, tenantID string, data fileOverwriteMutationData) (quotaCounterDeltas, error) {
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
	}, func(applyCtx context.Context, tx *sql.Tx) (quotaCounterDeltas, error) {
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
	}, func(applyCtx context.Context, tx *sql.Tx) (quotaCounterDeltas, error) {
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
	}, quotaPendingDeltas{}, func(applyCtx context.Context, tx *sql.Tx) (quotaCounterDeltas, error) {
		// LLM cost writes go to tenant_llm_usage and tenant_monthly_llm_cost
		// — different tables than the tenant_quota_usage hot row, and rare —
		// so they stay inline and this apply returns zero counter deltas.
		if err := b.metaStore.InsertCentralLLMUsageTx(tx, &LLMUsageView{
			TenantID:       b.tenantID,
			TaskType:       taskType,
			TaskID:         taskID,
			CostMillicents: costMillicents,
			RawUnits:       rawUnits,
			RawUnitType:    rawUnitType,
		}); err != nil {
			return quotaCounterDeltas{}, err
		}
		if err := b.metaStore.IncrMonthlyLLMCostTx(tx, b.tenantID, costMillicents); err != nil {
			return quotaCounterDeltas{}, err
		}
		return quotaCounterDeltas{}, nil
	})
}
