package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/semantic"
)

// semanticTenantRef identifies a schedulable tenant for the worker.
type semanticTenantRef struct {
	id     string
	tenant *meta.Tenant
}

// semanticTaskAction classifies the terminal action for a processed task.
type semanticTaskAction string

const (
	semanticTaskActionAck        semanticTaskAction = "ack"
	semanticTaskActionRetry      semanticTaskAction = "retry"
	semanticTaskActionDeadLetter semanticTaskAction = "dead_letter"
)

// semanticTaskOutcome is the result of dispatching one task.
type semanticTaskOutcome struct {
	action  semanticTaskAction
	result  string
	message string
}

// semanticTaskLeaseExecution manages lease renewal for a single in-flight task.
type semanticTaskLeaseExecution struct {
	ctx    context.Context
	cancel context.CancelFunc

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once

	mu             sync.Mutex
	lost           bool
	renewAttempted bool
	lastLeaseUntil *time.Time
}

// semanticTaskLeaseOutcome is the result of stopping a lease execution.
type semanticTaskLeaseOutcome struct {
	lost           bool
	renewAttempted bool
	lastLeaseUntil *time.Time
}

// processTask claims, processes, and finalizes one semantic task. This is the
// core task processing pipeline moved from semantic_worker.go, preserving all
// lease execution, dispatch, ack/retry/dead-letter, and failpoint hooks.
func (m *tenantWorkerManager) processTask(ctx context.Context, target *tenantTarget, task *semantic.Task) {
	if task == nil || target == nil {
		return
	}
	start := time.Now()
	result := "ok"
	tenantID := target.tenantID
	tidbCloudOrgID := target.metricOrgID()
	defer func() {
		metrics.RecordTenantOperationWithOrg(tenantID, tidbCloudOrgID, "semantic_worker", string(task.TaskType), result, time.Since(start))
	}()
	leaseExec := m.startTaskLeaseExecution(ctx, target, task)
	var (
		leaseStopOnce sync.Once
		leaseOutcome  semanticTaskLeaseOutcome
	)
	stopLease := func() semanticTaskLeaseOutcome {
		leaseStopOnce.Do(func() {
			leaseOutcome = leaseExec.stop()
		})
		return leaseOutcome
	}
	defer stopLease()
	outcome := m.dispatchTask(leaseExec.leaseCtx(), target, task)
	// Stop renewal before ack/retry so only one lease owner can finalize outcome.
	leaseOutcome = stopLease()
	if leaseOutcome.lost {
		result = "lease_lost"
		return
	}
	result = outcome.result
	m.injectBeforeSemanticTaskFinalize(target.tenantID, target.store, task, outcome)
	// Re-check ownership after the handler returns so tests can deterministically
	// force a lease-loss window before any terminal state mutation happens.
	stillOwned, err := m.semanticTaskStillOwned(ctx, target, task, outcome.result)
	if err != nil {
		logger.Warn(ctx, "tenant_worker_finalize_ownership_check_failed",
			append([]zap.Field{
				zap.String("tenant_id", target.tenantID),
				zap.String("result", outcome.result),
			}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
	} else if !stillOwned {
		return
	}
	switch outcome.action {
	case semanticTaskActionAck:
		m.ackTask(ctx, target.tenantID, target.store, task, outcome.message)
	case semanticTaskActionRetry:
		m.retryTask(ctx, target, task, outcome.message)
	case semanticTaskActionDeadLetter:
		m.deadLetterTask(ctx, target, task, outcome.message)
	}
}

func (m *tenantWorkerManager) ackTask(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task, reason string) {
	if err := store.AckSemanticTask(ctx, task.TaskID, task.Receipt); err != nil {
		logger.Warn(ctx, "tenant_worker_ack_failed",
			append([]zap.Field{
				zap.String("tenant_id", tenantID),
				zap.String("reason", reason),
				zap.String("result", "error"),
			}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
		return
	}
	logger.Info(ctx, "tenant_worker_ack_ok",
		append([]zap.Field{
			zap.String("tenant_id", tenantID),
			zap.String("reason", reason),
			zap.String("result", reason),
		}, semanticTaskLogFields(task)...)...)
}

func (m *tenantWorkerManager) retryTask(ctx context.Context, target *tenantTarget, task *semantic.Task, message string) {
	start := time.Now()
	tenantID := target.tenantID
	store := target.store
	retryAt := time.Now().UTC().Add(m.retryDelay(task.AttemptCount))
	willDeadLetter := task.AttemptCount >= task.MaxAttempts
	if err := store.RetrySemanticTask(ctx, task.TaskID, task.Receipt, retryAt, message); err != nil {
		metrics.RecordTenantOperationWithOrg(tenantID, target.metricOrgID(), "semantic_worker", "retry", "error", time.Since(start))
		logger.Warn(ctx, "tenant_worker_retry_failed",
			append([]zap.Field{
				zap.String("tenant_id", tenantID),
				zap.String("result", "error"),
			}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
		return
	}
	result := "scheduled"
	logMessage := "tenant_worker_retry_scheduled"
	if willDeadLetter {
		result = "dead_lettered"
		logMessage = "tenant_worker_dead_lettered"
	}
	metrics.RecordTenantOperationWithOrg(tenantID, target.metricOrgID(), "semantic_worker", "retry", result, time.Since(start))
	fields := append([]zap.Field{
		zap.String("tenant_id", tenantID),
		zap.String("result", result),
		zap.String("message", message),
	}, semanticTaskLogFields(task)...)
	if !willDeadLetter {
		fields = append(fields, zap.Time("retry_at", retryAt))
	}
	logger.Warn(ctx, logMessage, fields...)
}

func (m *tenantWorkerManager) deadLetterTask(ctx context.Context, target *tenantTarget, task *semantic.Task, message string) {
	start := time.Now()
	tenantID := target.tenantID
	store := target.store
	if err := store.DeadLetterSemanticTask(ctx, task.TaskID, task.Receipt, message); err != nil {
		metrics.RecordTenantOperationWithOrg(tenantID, target.metricOrgID(), "semantic_worker", "dead_letter", "error", time.Since(start))
		logger.Warn(ctx, "tenant_worker_dead_letter_failed",
			append([]zap.Field{
				zap.String("tenant_id", tenantID),
				zap.String("result", "error"),
			}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
		return
	}
	metrics.RecordTenantOperationWithOrg(tenantID, target.metricOrgID(), "semantic_worker", "dead_letter", "dead_lettered", time.Since(start))
	logger.Warn(ctx, "tenant_worker_dead_lettered",
		append([]zap.Field{
			zap.String("tenant_id", tenantID),
			zap.String("result", "dead_lettered"),
			zap.String("message", message),
		}, semanticTaskLogFields(task)...)...)
}

func (m *tenantWorkerManager) retryDelay(attemptCount int) time.Duration {
	if attemptCount < 1 {
		attemptCount = 1
	}
	delay := m.opts.RetryBaseDelay
	for i := 1; i < attemptCount; i++ {
		delay *= 2
		if delay >= m.opts.RetryMaxDelay {
			return m.opts.RetryMaxDelay
		}
	}
	if delay > m.opts.RetryMaxDelay {
		return m.opts.RetryMaxDelay
	}
	return delay
}

func (m *tenantWorkerManager) dispatchTask(ctx context.Context, target *tenantTarget, task *semantic.Task) semanticTaskOutcome {
	switch task.TaskType {
	case semantic.TaskTypeEmbed:
		return m.processEmbedTask(ctx, target.store, task)
	case semantic.TaskTypeImgExtractText:
		return m.processImgExtractTask(ctx, target.backend, task)
	case semantic.TaskTypeAudioExtractText:
		return m.processAudioExtractTask(ctx, target.backend, task)
	default:
		message := fmt.Sprintf("unsupported task type %q", task.TaskType)
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: "unsupported", message: message}
	}
}

func (m *tenantWorkerManager) startTaskLeaseExecution(ctx context.Context, target *tenantTarget, task *semantic.Task) *semanticTaskLeaseExecution {
	leaseCtx, cancel := context.WithCancel(ctx)
	exec := &semanticTaskLeaseExecution{
		ctx:    leaseCtx,
		cancel: cancel,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	if task != nil && task.LeaseUntil != nil {
		leaseUntil := task.LeaseUntil.UTC()
		exec.lastLeaseUntil = &leaseUntil
	}
	go exec.run(m, target, task)
	return exec
}

func (m *tenantWorkerManager) leaseRenewInterval() time.Duration {
	interval := m.opts.LeaseDuration / 2
	if interval > 0 {
		return interval
	}
	if m.opts.LeaseDuration > 0 {
		return m.opts.LeaseDuration
	}
	return time.Second
}

func (e *semanticTaskLeaseExecution) leaseCtx() context.Context {
	if e == nil || e.ctx == nil {
		return context.Background()
	}
	return e.ctx
}

func (e *semanticTaskLeaseExecution) stop() semanticTaskLeaseOutcome {
	if e == nil {
		return semanticTaskLeaseOutcome{}
	}
	e.stopOnce.Do(func() {
		close(e.stopCh)
		e.cancel()
		<-e.doneCh
	})

	e.mu.Lock()
	defer e.mu.Unlock()
	outcome := semanticTaskLeaseOutcome{
		lost:           e.lost,
		renewAttempted: e.renewAttempted,
	}
	if e.lastLeaseUntil != nil {
		leaseUntil := e.lastLeaseUntil.UTC()
		outcome.lastLeaseUntil = &leaseUntil
	}
	return outcome
}

func (e *semanticTaskLeaseExecution) run(m *tenantWorkerManager, target *tenantTarget, task *semantic.Task) {
	defer close(e.doneCh)
	if m == nil || target == nil || target.store == nil || task == nil {
		return
	}

	ticker := time.NewTicker(m.leaseRenewInterval())
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			e.logStopped(m, target, task)
			return
		case <-e.ctx.Done():
			select {
			case <-e.stopCh:
				e.logStopped(m, target, task)
			default:
			}
			return
		case <-ticker.C:
			e.markRenewAttempted()
			failpoint.InjectCall("semanticWorkerBeforeRenew", target.tenantID, target.store, task)
			renewStart := time.Now()
			leaseUntil, err := target.store.RenewSemanticTask(e.ctx, task.TaskID, task.Receipt, m.opts.LeaseDuration)
			if err != nil {
				if errors.Is(err, semantic.ErrTaskLeaseMismatch) || errors.Is(err, semantic.ErrTaskNotFound) {
					metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "renew", "error", time.Since(renewStart))
					e.markLeaseLost()
					failpoint.InjectCall("semanticWorkerOnLeaseLost", target.tenantID, target.store, task, err)
					metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "lease_lost", "ok", time.Since(renewStart))
					logger.Warn(e.ctx, "tenant_worker_lease_lost",
						append([]zap.Field{
							zap.String("tenant_id", target.tenantID),
							zap.String("result", "lease_lost"),
						}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
					e.cancel()
					e.logStopped(m, target, task)
					return
				}
				if e.shouldStop() {
					e.logStopped(m, target, task)
					return
				}
				metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "renew", "error", time.Since(renewStart))
				logger.Warn(e.ctx, "tenant_worker_lease_renew_failed",
					append([]zap.Field{
						zap.String("tenant_id", target.tenantID),
						zap.String("result", "error"),
					}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
				continue
			}
			e.recordRenewedLease(leaseUntil)
			failpoint.InjectCall("semanticWorkerAfterRenew", target.tenantID, target.store, task, leaseUntil)
			metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "renew", "ok", time.Since(renewStart))
		}
	}
}

func (e *semanticTaskLeaseExecution) shouldStop() bool {
	if e == nil {
		return true
	}
	select {
	case <-e.stopCh:
		return true
	default:
		return false
	}
}

func (e *semanticTaskLeaseExecution) recordRenewedLease(leaseUntil time.Time) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.renewAttempted = true
	t := leaseUntil.UTC()
	e.lastLeaseUntil = &t
}

func (e *semanticTaskLeaseExecution) markRenewAttempted() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.renewAttempted = true
}

func (e *semanticTaskLeaseExecution) markLeaseLost() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lost = true
	e.renewAttempted = true
}

func (e *semanticTaskLeaseExecution) logStopped(m *tenantWorkerManager, target *tenantTarget, task *semantic.Task) {
	if e == nil || m == nil || target == nil || task == nil {
		return
	}
	e.mu.Lock()
	renewAttempted := e.renewAttempted
	lost := e.lost
	var leaseUntil *time.Time
	if e.lastLeaseUntil != nil {
		t := e.lastLeaseUntil.UTC()
		leaseUntil = &t
	}
	e.mu.Unlock()
	if !renewAttempted {
		return
	}

	result := "owned"
	if lost {
		result = "lease_lost"
	}
	fields := append([]zap.Field{
		zap.String("tenant_id", target.tenantID),
		zap.String("result", result),
	}, semanticTaskLogFields(task)...)
	if leaseUntil != nil {
		fields = append(fields, zap.Time("latest_lease_until", leaseUntil.UTC()))
	}
	logger.Info(e.ctx, "tenant_worker_lease_renew_stopped", fields...)
}

func (m *tenantWorkerManager) processEmbedTask(ctx context.Context, store *datastore.Store, task *semantic.Task) semanticTaskOutcome {
	if m.embedder == nil {
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: "handler_missing", message: "embed handler not configured"}
	}
	file, err := store.GetFile(ctx, task.ResourceID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return semanticTaskOutcome{action: semanticTaskActionAck, result: "obsolete", message: "file_not_found"}
		}
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: "get_file_error", message: fmt.Sprintf("get file: %v", err)}
	}
	if file.Status != datastore.StatusConfirmed || file.Revision != task.ResourceVersion {
		return semanticTaskOutcome{action: semanticTaskActionAck, result: "obsolete", message: "stale"}
	}
	if strings.TrimSpace(file.ContentText) == "" && strings.TrimSpace(file.Description) == "" {
		return semanticTaskOutcome{action: semanticTaskActionAck, result: "obsolete", message: "empty_content_and_description"}
	}

	var contentUpdated, descUpdated bool
	if strings.TrimSpace(file.ContentText) != "" {
		vec, err := m.embedder.EmbedText(ctx, file.ContentText)
		if err != nil {
			return semanticTaskOutcome{action: semanticTaskActionRetry, result: "embed_error", message: fmt.Sprintf("embed text: %v", err)}
		}
		if len(vec) == 0 {
			return semanticTaskOutcome{action: semanticTaskActionRetry, result: "embed_empty", message: "embed text returned empty vector"}
		}
		contentUpdated, err = store.UpdateFileEmbedding(ctx, task.ResourceID, task.ResourceVersion, vec)
		if err != nil {
			return semanticTaskOutcome{action: semanticTaskActionRetry, result: "writeback_error", message: fmt.Sprintf("write embedding: %v", err)}
		}
	}

	if strings.TrimSpace(file.Description) != "" {
		vecDesc, err := m.embedder.EmbedText(ctx, file.Description)
		if err != nil {
			return semanticTaskOutcome{action: semanticTaskActionRetry, result: "embed_desc_error", message: fmt.Sprintf("embed description: %v", err)}
		}
		if len(vecDesc) == 0 {
			return semanticTaskOutcome{action: semanticTaskActionRetry, result: "embed_desc_empty", message: "embed description returned empty vector"}
		}
		descUpdated, err = store.UpdateFileDescriptionEmbedding(ctx, task.ResourceID, task.ResourceVersion, vecDesc)
		if err != nil {
			return semanticTaskOutcome{action: semanticTaskActionRetry, result: "writeback_desc_error", message: fmt.Sprintf("write description embedding: %v", err)}
		}
	}

	if !contentUpdated && !descUpdated {
		return semanticTaskOutcome{action: semanticTaskActionAck, result: "obsolete", message: "conditional_write_miss"}
	}
	return semanticTaskOutcome{action: semanticTaskActionAck, result: "ok", message: "written"}
}

func (m *tenantWorkerManager) processImgExtractTask(ctx context.Context, b *backend.Dat9Backend, task *semantic.Task) semanticTaskOutcome {
	result, err := b.ProcessImageExtractTask(ctx, imageExtractTaskSpecFromSemanticTask(task))
	if err != nil {
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: string(result), message: err.Error()}
	}
	if result == backend.ImageExtractResultBudgetExhausted {
		return semanticTaskOutcome{action: semanticTaskActionAck, result: string(result), message: "monthly_llm_cost_budget_exhausted"}
	}
	return semanticTaskOutcome{action: semanticTaskActionAck, result: string(result), message: string(result)}
}

func (m *tenantWorkerManager) processAudioExtractTask(ctx context.Context, b *backend.Dat9Backend, task *semantic.Task) semanticTaskOutcome {
	result, err := b.ProcessAudioExtractTask(ctx, audioExtractTaskSpecFromSemanticTask(task))
	if err != nil {
		if backend.IsNonRetryableAudioExtractError(err) {
			return semanticTaskOutcome{action: semanticTaskActionDeadLetter, result: string(result), message: err.Error()}
		}
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: string(result), message: err.Error()}
	}
	if result == backend.AudioExtractResultBudgetExhausted {
		return semanticTaskOutcome{action: semanticTaskActionAck, result: string(result), message: "monthly_llm_cost_budget_exhausted"}
	}
	return semanticTaskOutcome{action: semanticTaskActionAck, result: string(result), message: string(result)}
}

func (m *tenantWorkerManager) semanticTaskStillOwned(ctx context.Context, target *tenantTarget, task *semantic.Task, result string) (bool, error) {
	tenantID := target.tenantID
	store := target.store
	if store == nil || task == nil {
		return false, fmt.Errorf("check semantic task ownership: nil store or task")
	}
	now := semanticWorkerLeaseNow()
	var count int
	err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM semantic_tasks
		WHERE task_id = ? AND status = ? AND receipt = ? AND lease_until IS NOT NULL AND lease_until > ?`,
		task.TaskID, semantic.TaskProcessing, task.Receipt, now).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check semantic task ownership: %w", err)
	}
	if count > 0 {
		return true, nil
	}
	metrics.RecordTenantOperationWithOrg(tenantID, target.metricOrgID(), "semantic_worker", "lease_lost", "ok", 0)
	logger.Warn(ctx, "tenant_worker_finalize_skipped_lease_lost",
		append([]zap.Field{
			zap.String("tenant_id", tenantID),
			zap.String("result", result),
		}, semanticTaskLogFields(task)...)...)
	return false, nil
}

func semanticWorkerLeaseNow() time.Time {
	now := time.Now().UTC()
	failpoint.Inject("semanticWorkerLeaseNow", func(val failpoint.Value) {
		switch injected := val.(type) {
		case string:
			parsed, err := time.Parse(time.RFC3339Nano, injected)
			if err == nil {
				now = parsed.UTC()
			}
		}
	})
	return now
}

func semanticTaskLogFields(task *semantic.Task) []zap.Field {
	if task == nil {
		return nil
	}
	fields := []zap.Field{
		zap.String("task_id", task.TaskID),
		zap.String("task_type", string(task.TaskType)),
		zap.String("resource_id", task.ResourceID),
		zap.Int64("resource_version", task.ResourceVersion),
		zap.Bool("has_receipt", task.Receipt != ""),
		zap.Int("attempt_count", task.AttemptCount),
	}
	if task.LeaseUntil != nil {
		fields = append(fields, zap.Time("lease_until", task.LeaseUntil.UTC()))
	}
	return fields
}

func imageExtractTaskSpecFromSemanticTask(task *semantic.Task) backend.ImageExtractTaskSpec {
	if task == nil {
		return backend.ImageExtractTaskSpec{}
	}
	spec := backend.ImageExtractTaskSpec{FileID: task.ResourceID, Revision: task.ResourceVersion}
	if len(task.PayloadJSON) == 0 {
		return spec
	}
	var payload semantic.ImgExtractTaskPayload
	if err := json.Unmarshal(task.PayloadJSON, &payload); err == nil {
		spec.Path = payload.Path
		spec.ContentType = payload.ContentType
	}
	return spec
}

func audioExtractTaskSpecFromSemanticTask(task *semantic.Task) backend.AudioExtractTaskSpec {
	if task == nil {
		return backend.AudioExtractTaskSpec{}
	}
	spec := backend.AudioExtractTaskSpec{FileID: task.ResourceID, Revision: task.ResourceVersion}
	if len(task.PayloadJSON) == 0 {
		return spec
	}
	var payload semantic.AudioExtractTaskPayload
	if err := json.Unmarshal(task.PayloadJSON, &payload); err == nil {
		spec.Path = payload.Path
		spec.ContentType = payload.ContentType
	}
	return spec
}

func chainReleases(first, second func()) func() {
	return func() {
		if first != nil {
			first()
		}
		if second != nil {
			second()
		}
	}
}

func hasAnyTaskTypes(types []semantic.TaskType) bool {
	return len(types) > 0
}

// unionTaskTypes merges two task-type slices, deduplicating by value.
func unionTaskTypes(a, b []semantic.TaskType) []semantic.TaskType {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	seen := make(map[semantic.TaskType]struct{}, len(a)+len(b))
	out := make([]semantic.TaskType, 0, len(a)+len(b))
	for _, t := range a {
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	for _, t := range b {
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// tenantWorkerLogTaskTypesFromTypes stringifies task-type slices for zap.
func tenantWorkerLogTaskTypesFromTypes(types []semantic.TaskType) []string {
	if len(types) == 0 {
		return nil
	}
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return out
}
