package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	defaultReplayPollInterval = time.Second
	defaultReplayMinAge       = time.Second // only replay older mutations to avoid racing inline apply
	defaultReplayObserveEvery = 10 * time.Second
	replayBatchLimit          = 100
	replayMaxRetries          = 5
)

func replayPollInterval() time.Duration {
	d := envDurationMS("DRIVE9_QUOTA_REPLAY_POLL_MS", defaultReplayPollInterval)
	if d <= 0 {
		return defaultReplayPollInterval
	}
	return d
}

func replayMinAge() time.Duration {
	d := envDurationMS("DRIVE9_QUOTA_REPLAY_MIN_AGE_MS", defaultReplayMinAge)
	if d <= 0 {
		return defaultReplayMinAge
	}
	return d
}

func envDurationMS(name string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

// MutationReplayWorker reads pending mutations from the quota_mutation_log
// and applies them idempotently. It runs as a background goroutine.
type MutationReplayWorker struct {
	store                  MetaQuotaStore
	cancel                 context.CancelFunc
	done                   chan struct{}
	observedBacklogTenants map[string]struct{}
	lastBacklogObservation time.Time
}

// StartMutationReplayWorker starts the background replay loop.
// Returns nil if store is nil (central quota not wired).
func StartMutationReplayWorker(store MetaQuotaStore) *MutationReplayWorker {
	if store == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &MutationReplayWorker{
		store:                  store,
		cancel:                 cancel,
		done:                   make(chan struct{}),
		observedBacklogTenants: make(map[string]struct{}),
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
	defer func() {
		w.clearPendingBacklogGauges()
		close(w.done)
	}()

	logger.Info(ctx, "mutation_replay_worker_started")
	ticker := time.NewTicker(replayPollInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "mutation_replay_worker_stopped")
			return
		case <-ticker.C:
			if w.replayBatch(ctx) {
				// Fatal error (e.g. database closed) — stop the loop.
				logger.Info(ctx, "mutation_replay_worker_stopped_fatal")
				return
			}
		}
	}
}

// replayBatch processes one batch of pending mutations. Returns true if a
// fatal error occurred (e.g. database closed) and the loop should stop.
func (w *MutationReplayWorker) replayBatch(ctx context.Context) (fatal bool) {
	start := time.Now()
	entries, err := w.store.ListPendingMutations(ctx, replayMinAge(), replayBatchLimit)
	if err != nil {
		if strings.Contains(err.Error(), "database is closed") || strings.Contains(err.Error(), "connection refused") {
			logger.Info(ctx, "mutation_replay_worker_db_closed")
			return true
		}
		logger.Warn(ctx, "mutation_replay_list_failed", zap.Error(err))
		metrics.RecordOperation("mutation_replay", "list", metrics.ResultForError(err), time.Since(start))
		return false
	}
	if len(entries) == 0 {
		w.recordPendingBacklogIfDue(ctx)
		return
	}

	// Per-tenant failure barrier invariant: once any entry for tenant T fails in
	// this batch, all subsequent entries for T are skipped until the next batch.
	// ListPendingMutations orders by (tenant_id, id) so a "barrier" set is
	// sufficient — we do not need to re-sort. This invariant matters for
	// non-counter mutations where ordering affects convergence, e.g.
	// UpsertFileMetaTx followed by DeleteFileMetaTx on the same fileID: if the
	// upsert fails transiently and we then run the delete, we have effectively
	// reordered create+delete and the shadow state will not converge.
	applied := 0
	failed := 0
	skipped := 0
	blockedTenants := make(map[string]struct{})
	for _, entry := range entries {
		if _, blocked := blockedTenants[entry.TenantID]; blocked {
			skipped++
			continue
		}
		if err := w.replayOne(ctx, entry); err != nil {
			logger.Warn(ctx, "mutation_replay_entry_failed",
				zap.Int64("log_id", entry.ID),
				zap.String("tenant_id", entry.TenantID),
				zap.String("mutation_type", entry.MutationType),
				zap.Error(err))
			if rErr := w.store.IncrMutationRetry(ctx, entry.ID, replayMaxRetries); rErr != nil {
				logger.Error(ctx, "mutation_replay_incr_retry_failed",
					zap.String("tenant_id", entry.TenantID),
					zap.Int64("log_id", entry.ID),
					zap.Error(rErr))
			}
			metrics.RecordTenantOperationCount(entry.TenantID, "mutation_replay", entry.MutationType, metrics.ResultForError(err))
			blockedTenants[entry.TenantID] = struct{}{}
			failed++
		} else {
			metrics.RecordTenantOperationCount(entry.TenantID, "mutation_replay", entry.MutationType, "ok")
			applied++
		}
	}

	metrics.RecordGauge("mutation_replay", "batch_applied", float64(applied))
	metrics.RecordGauge("mutation_replay", "batch_failed", float64(failed))
	metrics.RecordGauge("mutation_replay", "batch_skipped", float64(skipped))
	w.recordPendingBacklogIfDue(ctx)
	logger.Info(ctx, "mutation_replay_batch_complete",
		zap.Int("total", len(entries)),
		zap.Int("applied", applied),
		zap.Int("failed", failed),
		zap.Int("skipped_after_barrier", skipped),
		zap.Float64("duration_ms", float64(time.Since(start).Milliseconds())))
	return false
}

func (w *MutationReplayWorker) recordPendingBacklogIfDue(ctx context.Context) {
	if !w.lastBacklogObservation.IsZero() && time.Since(w.lastBacklogObservation) < defaultReplayObserveEvery {
		return
	}
	w.recordPendingBacklog(ctx)
}

func (w *MutationReplayWorker) recordPendingBacklog(ctx context.Context) {
	if w.observedBacklogTenants == nil {
		w.observedBacklogTenants = make(map[string]struct{})
	}
	w.lastBacklogObservation = time.Now()
	start := time.Now()
	observations, err := w.store.ObservePendingMutations(ctx)
	if err != nil {
		logger.Warn(ctx, "mutation_replay_observe_pending_failed", zap.Error(err))
		metrics.RecordOperation("mutation_replay", "observe_pending", metrics.ResultForError(err), time.Since(start))
		return
	}
	current := make(map[string]struct{}, len(observations))
	for _, obs := range observations {
		current[obs.TenantID] = struct{}{}
		w.observedBacklogTenants[obs.TenantID] = struct{}{}
		metrics.RecordTenantGauge(obs.TenantID, "mutation_replay", "pending_mutations", float64(obs.PendingCount))
		metrics.RecordTenantGauge(obs.TenantID, "mutation_replay", "oldest_pending_age_seconds", obs.OldestPendingAgeSeconds)
	}
	for tenantID := range w.observedBacklogTenants {
		if _, ok := current[tenantID]; ok {
			continue
		}
		metrics.RecordTenantGauge(tenantID, "mutation_replay", "pending_mutations", 0)
		metrics.RecordTenantGauge(tenantID, "mutation_replay", "oldest_pending_age_seconds", 0)
		delete(w.observedBacklogTenants, tenantID)
	}
	metrics.RecordOperation("mutation_replay", "observe_pending", "ok", time.Since(start))
}

func (w *MutationReplayWorker) clearPendingBacklogGauges() {
	for tenantID := range w.observedBacklogTenants {
		metrics.RecordTenantGauge(tenantID, "mutation_replay", "pending_mutations", 0)
		metrics.RecordTenantGauge(tenantID, "mutation_replay", "oldest_pending_age_seconds", 0)
		delete(w.observedBacklogTenants, tenantID)
	}
}

func (w *MutationReplayWorker) replayOne(ctx context.Context, entry MutationLogView) error {
	return w.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := w.applyMutation(ctx, tx, entry); err != nil {
			return err
		}
		return w.store.MarkMutationAppliedTx(tx, entry.ID)
	})
}

func (w *MutationReplayWorker) applyMutation(ctx context.Context, tx *sql.Tx, entry MutationLogView) error {
	return applyCentralQuotaMutationTx(ctx, w.store, tx, entry.TenantID, entry.MutationType, entry.MutationData, entry.ID)
}

func applyCentralQuotaMutationTx(ctx context.Context, store MetaQuotaStore, tx *sql.Tx, tenantID, mutationType string, mutationData json.RawMessage, logID int64) error {
	switch mutationType {
	case "file_create":
		var data fileCreateMutationData
		if err := json.Unmarshal(mutationData, &data); err != nil {
			return err
		}
		return applyCentralFileCreateTx(store, tx, tenantID, data)

	case "file_overwrite":
		var data fileOverwriteMutationData
		if err := json.Unmarshal(mutationData, &data); err != nil {
			return err
		}
		return applyCentralFileOverwriteTx(store, tx, tenantID, data)

	case "file_delete":
		var data fileDeleteMutationData
		if err := json.Unmarshal(mutationData, &data); err != nil {
			return err
		}
		deleted, err := store.DeleteFileMetaIfExistsTx(tx, tenantID, data.FileID)
		if err != nil {
			return err
		}
		if !deleted {
			return nil
		}
		if data.SizeBytes != 0 {
			if err := store.IncrStorageBytesTx(tx, tenantID, -data.SizeBytes); err != nil {
				return err
			}
		}
		if err := store.IncrFileCountTx(tx, tenantID, -1); err != nil {
			return err
		}
		if data.IsMedia {
			if err := store.IncrMediaFileCountTx(tx, tenantID, -1); err != nil {
				return err
			}
		}
		return nil

	case "upload_complete":
		var data uploadCompleteMutationData
		if err := json.Unmarshal(mutationData, &data); err != nil {
			return err
		}
		// Real shared helper — same body as the inline fast path in
		// completeUploadReservation. MarkMutationAppliedTx stays with the
		// caller (replayOne wraps applyMutation + MarkMutationAppliedTx in
		// the same InTx), so this delegation is safe w.r.t. the Fix 2
		// status='pending' guard.
		return applyUploadCompleteTx(ctx, store, tx, tenantID, data)

	case "llm_cost_record":
		var data llmCostMutationData
		if err := json.Unmarshal(mutationData, &data); err != nil {
			return err
		}
		if err := store.InsertCentralLLMUsageTx(tx, &LLMUsageView{
			TenantID:       tenantID,
			TaskType:       data.TaskType,
			TaskID:         data.TaskID,
			CostMillicents: data.CostMillicents,
			RawUnits:       data.RawUnits,
			RawUnitType:    data.RawUnitType,
		}); err != nil {
			return err
		}
		return store.IncrMonthlyLLMCostTx(tx, tenantID, data.CostMillicents)

	default:
		logger.Warn(context.Background(), "mutation_replay_unknown_type",
			zap.String("tenant_id", tenantID),
			zap.String("mutation_type", mutationType),
			zap.Int64("log_id", logID))
		return nil // skip unknown types gracefully
	}
}
