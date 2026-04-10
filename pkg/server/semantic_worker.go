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

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/embedding"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/semantic"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

const (
	defaultSemanticWorkers              = 1
	defaultSemanticPollInterval         = 200 * time.Millisecond
	defaultSemanticLeaseDuration        = 30 * time.Second
	defaultSemanticRecoverInterval      = 5 * time.Second
	defaultSemanticRetryBaseDelay       = 200 * time.Millisecond
	defaultSemanticRetryMaxDelay        = 30 * time.Second
	defaultSemanticTenantScanLimit      = 128
	defaultSemanticPerTenantConcurrency = 1
	semanticLocalTenantID               = "local"
)

var semanticWorkerUsesTiDBAutoEmbedding = tenant.UsesTiDBAutoEmbedding

var semanticWorkerAllowedEmbedTaskTypes = []semantic.TaskType{semantic.TaskTypeEmbed}

func appManagedSemanticTaskTypes(embedder embedding.Client) []semantic.TaskType {
	if embedder == nil {
		return nil
	}
	return semanticWorkerAllowedEmbedTaskTypes
}

// SemanticWorkerOptions controls background semantic task processing.
type SemanticWorkerOptions struct {
	// Workers is the number of polling worker goroutines.
	Workers int
	// PollInterval is the idle wait between claim attempts.
	PollInterval time.Duration
	// LeaseDuration is the base task lease window used by claim and renew.
	LeaseDuration time.Duration
	// RecoverInterval controls how often expired leases are swept back to queue.
	RecoverInterval time.Duration
	// RetryBaseDelay is the initial backoff for retry scheduling.
	RetryBaseDelay time.Duration
	// RetryMaxDelay is the cap for exponential retry backoff.
	RetryMaxDelay time.Duration
	// TenantScanLimit bounds active tenants checked per scheduling pass.
	TenantScanLimit int
	// PerTenantConcurrency limits concurrent tasks per tenant.
	PerTenantConcurrency int
}

func (o *SemanticWorkerOptions) normalize() {
	if o.Workers <= 0 {
		o.Workers = defaultSemanticWorkers
	}
	if o.PollInterval <= 0 {
		o.PollInterval = defaultSemanticPollInterval
	}
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = defaultSemanticLeaseDuration
	}
	if o.RecoverInterval <= 0 {
		o.RecoverInterval = defaultSemanticRecoverInterval
	}
	if o.RetryBaseDelay <= 0 {
		o.RetryBaseDelay = defaultSemanticRetryBaseDelay
	}
	if o.RetryMaxDelay <= 0 {
		o.RetryMaxDelay = defaultSemanticRetryMaxDelay
	}
	if o.RetryMaxDelay < o.RetryBaseDelay {
		o.RetryMaxDelay = o.RetryBaseDelay
	}
	if o.TenantScanLimit <= 0 {
		o.TenantScanLimit = defaultSemanticTenantScanLimit
	}
	if o.PerTenantConcurrency <= 0 {
		o.PerTenantConcurrency = defaultSemanticPerTenantConcurrency
	}
}

type semanticWorkerManager struct {
	fallback *backend.Dat9Backend
	meta     *meta.Store
	pool     *tenant.Pool
	embedder embedding.Client
	opts     SemanticWorkerOptions

	mu         sync.Mutex
	inflight   map[string]int
	processing int
	rr         int

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type semanticTenantRef struct {
	id     string
	tenant *meta.Tenant
}

type semanticTarget struct {
	tenantID         string
	backend          *backend.Dat9Backend
	store            *datastore.Store
	allowedTaskTypes []semantic.TaskType
	release          func()
}

type semanticObservationSnapshot struct {
	queued          int
	processing      int
	deadLettered    int
	queueLagSeconds float64
	inflight        int
}

type semanticTaskAction string

const (
	semanticTaskActionAck   semanticTaskAction = "ack"
	semanticTaskActionRetry semanticTaskAction = "retry"
)

type semanticTaskOutcome struct {
	action  semanticTaskAction
	result  string
	message string
}

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

type semanticTaskLeaseOutcome struct {
	lost           bool
	renewAttempted bool
	lastLeaseUntil *time.Time
}

func newSemanticWorkerManager(fallback *backend.Dat9Backend, metaStore *meta.Store, pool *tenant.Pool, embedder embedding.Client, opts SemanticWorkerOptions) *semanticWorkerManager {
	hasMultiTenant := metaStore != nil && pool != nil
	if fallback == nil && !hasMultiTenant {
		return nil
	}
	m := &semanticWorkerManager{
		fallback: fallback,
		meta:     metaStore,
		pool:     pool,
		embedder: embedder,
	}
	app := m.appManagedTaskTypes()
	var poolAuto []semantic.TaskType
	if pool != nil {
		poolAuto = pool.AutoSemanticTaskTypes()
	}
	var fbAuto []semantic.TaskType
	if fallback != nil {
		fbAuto = fallback.AutoSemanticTaskTypes()
	}
	// Multi-tenant scheduling never includes the local fallback ref (listTenantRefs
	// only enumerates meta tenants), so fallback auto task types must not make the
	// manager viable by themselves.
	viable := hasAnyTaskTypes(app) || hasAnyTaskTypes(poolAuto)
	if !hasMultiTenant {
		viable = viable || hasAnyTaskTypes(fbAuto)
	}
	if !viable {
		return nil
	}
	// Single-tenant mode: require a viable fallback — either auto image path is
	// fully configured, or app-managed embed is available.
	if fallback != nil && !hasMultiTenant {
		if fallback.UsesDatabaseAutoEmbedding() {
			if !hasAnyTaskTypes(fallback.AutoSemanticTaskTypes()) {
				return nil
			}
		} else if !hasAnyTaskTypes(app) {
			return nil
		}
	}
	opts.normalize()
	m.opts = opts
	m.inflight = make(map[string]int)
	return m
}

func (m *semanticWorkerManager) Start(ctx context.Context) {
	if m == nil || m.cancel != nil {
		return
	}
	workerCtx, cancel := context.WithCancel(backgroundWithTrace(ctx))
	m.cancel = cancel
	metrics.RecordGauge("semantic_worker", "workers", float64(m.opts.Workers))
	metrics.RecordGauge("semantic_worker", "inflight", 0)
	metrics.RecordGauge("semantic_worker", "queued", 0)
	metrics.RecordGauge("semantic_worker", "processing", 0)
	metrics.RecordGauge("semantic_worker", "dead_lettered", 0)
	metrics.RecordGauge("semantic_worker", "queue_lag_seconds", 0)
	for i := 0; i < m.opts.Workers; i++ {
		m.wg.Add(1)
		go m.workerLoop(workerCtx, i+1)
	}
	m.wg.Add(1)
	go m.recoverLoop(workerCtx)
	logger.Info(workerCtx, "semantic_worker_manager_started",
		zap.Int("workers", m.opts.Workers),
		zap.Duration("poll_interval", m.opts.PollInterval),
		zap.Duration("lease_duration", m.opts.LeaseDuration),
		zap.Duration("recover_interval", m.opts.RecoverInterval))
}

func (m *semanticWorkerManager) Stop() {
	if m == nil || m.cancel == nil {
		return
	}
	m.cancel()
	m.wg.Wait()
	m.cancel = nil
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processing = 0
	metrics.RecordGauge("semantic_worker", "workers", 0)
	metrics.RecordGauge("semantic_worker", "inflight", 0)
}

func (m *semanticWorkerManager) workerLoop(ctx context.Context, workerID int) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.opts.PollInterval)
	defer ticker.Stop()
	for {
		processed := m.processNext(ctx)
		if processed {
			continue
		}
		select {
		case <-ctx.Done():
			logger.Info(ctx, "semantic_worker_stopped", zap.Int("worker_id", workerID))
			return
		case <-ticker.C:
		}
	}
}

func (m *semanticWorkerManager) recoverLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.opts.RecoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.recoverExpired(ctx)
			m.observeOnce(ctx, time.Now().UTC())
		}
	}
}

func (m *semanticWorkerManager) processNext(ctx context.Context) bool {
	target, err := m.nextTarget(ctx)
	if err != nil {
		logger.Warn(ctx, "semantic_worker_pick_tenant_failed", zap.Error(err))
		return false
	}
	if target == nil {
		return false
	}
	defer target.release()

	claimStart := time.Now()
	task, found, err := target.store.ClaimSemanticTask(ctx, time.Now().UTC(), m.opts.LeaseDuration, target.allowedTaskTypes...)
	if err != nil {
		metrics.RecordOperation("semantic_worker", "claim", "error", time.Since(claimStart))
		logger.Warn(ctx, "semantic_worker_claim_failed",
			append([]zap.Field{
				zap.String("tenant_id", target.tenantID),
				zap.String("result", "error"),
			}, zap.Error(err))...)
		m.invalidateTenantBackend(target.tenantID)
		return false
	}
	if !found {
		return false
	}
	metrics.RecordOperation("semantic_worker", "claim", "ok", time.Since(claimStart))
	logger.Info(ctx, "semantic_worker_claim_ok",
		append([]zap.Field{
			zap.String("tenant_id", target.tenantID),
			zap.String("result", "ok"),
		}, semanticTaskLogFields(task)...)...)
	m.markProcessingStart()
	defer m.markProcessingDone()
	m.processTask(ctx, target, task)
	return true
}

func (m *semanticWorkerManager) processTask(ctx context.Context, target *semanticTarget, task *semantic.Task) {
	if task == nil || target == nil {
		return
	}
	start := time.Now()
	result := "ok"
	defer func() {
		metrics.RecordOperation("semantic_worker", string(task.TaskType), result, time.Since(start))
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
	stillOwned, err := m.semanticTaskStillOwned(ctx, target.tenantID, target.store, task, outcome.result)
	if err != nil {
		// Fall back to the datastore lease checks in ack/retry when the ownership
		// pre-check cannot read current state, instead of stranding the task in
		// processing on a transient read failure.
		logger.Warn(ctx, "semantic_worker_finalize_ownership_check_failed",
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
		m.retryTask(ctx, target.tenantID, target.store, task, outcome.message)
	}
}

func (m *semanticWorkerManager) ackTask(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task, reason string) {
	if err := store.AckSemanticTask(ctx, task.TaskID, task.Receipt); err != nil {
		logger.Warn(ctx, "semantic_worker_ack_failed",
			append([]zap.Field{
				zap.String("tenant_id", tenantID),
				zap.String("reason", reason),
				zap.String("result", "error"),
			}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
		return
	}
	logger.Info(ctx, "semantic_worker_ack_ok",
		append([]zap.Field{
			zap.String("tenant_id", tenantID),
			zap.String("reason", reason),
			zap.String("result", reason),
		}, semanticTaskLogFields(task)...)...)
}

func (m *semanticWorkerManager) retryTask(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task, message string) {
	start := time.Now()
	retryAt := time.Now().UTC().Add(m.retryDelay(task.AttemptCount))
	willDeadLetter := task.AttemptCount >= task.MaxAttempts
	if err := store.RetrySemanticTask(ctx, task.TaskID, task.Receipt, retryAt, message); err != nil {
		metrics.RecordOperation("semantic_worker", "retry", "error", time.Since(start))
		logger.Warn(ctx, "semantic_worker_retry_failed",
			append([]zap.Field{
				zap.String("tenant_id", tenantID),
				zap.String("result", "error"),
			}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
		return
	}
	result := "scheduled"
	logMessage := "semantic_worker_retry_scheduled"
	if willDeadLetter {
		result = "dead_lettered"
		logMessage = "semantic_worker_dead_lettered"
	}
	metrics.RecordOperation("semantic_worker", "retry", result, time.Since(start))
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

func (m *semanticWorkerManager) retryDelay(attemptCount int) time.Duration {
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

func (m *semanticWorkerManager) recoverExpired(ctx context.Context) {
	refs, err := m.listTenantRefs(ctx)
	if err != nil {
		logger.Warn(ctx, "semantic_worker_list_tenants_for_recovery_failed", zap.Error(err))
		return
	}
	for _, ref := range refs {
		target, err := m.targetForRef(ctx, ref)
		if err != nil {
			logger.Warn(ctx, "semantic_worker_open_store_for_recovery_failed",
				zap.String("tenant_id", ref.id),
				zap.Error(err))
			continue
		}
		func() {
			defer target.release()
			start := time.Now()
			recovered, err := target.store.RecoverExpiredSemanticTasks(ctx, time.Now().UTC(), 64)
			if err != nil {
				metrics.RecordOperation("semantic_worker", "recover", "error", time.Since(start))
				logger.Warn(ctx, "semantic_worker_recover_failed",
					zap.String("tenant_id", ref.id),
					zap.Error(err))
				m.invalidateTenantBackend(ref.id)
				return
			}
			if recovered > 0 {
				metrics.RecordOperation("semantic_worker", "recover", "ok", time.Since(start))
				logger.Info(ctx, "semantic_worker_recover_ok",
					zap.String("tenant_id", ref.id),
					zap.String("result", "ok"),
					zap.Int("recovered", recovered))
			}
		}()
	}
}

func (m *semanticWorkerManager) nextTarget(ctx context.Context) (*semanticTarget, error) {
	refs, err := m.listTenantRefs(ctx)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}

	for i := 0; i < len(refs); i++ {
		ref, ok := m.claimTenantSlot(refs)
		if !ok {
			return nil, nil
		}
		target, err := m.targetForRef(ctx, ref)
		if err != nil {
			logger.Warn(ctx, "semantic_worker_open_store_failed",
				zap.String("tenant_id", ref.id),
				zap.Error(err))
			m.releaseTenantSlot(ref.id)
			continue
		}
		target.release = chainReleases(target.release, func() { m.releaseTenantSlot(ref.id) })
		if len(target.allowedTaskTypes) == 0 {
			target.release()
			continue
		}
		return target, nil
	}
	return nil, nil
}

func (m *semanticWorkerManager) claimTenantSlot(refs []semanticTenantRef) (semanticTenantRef, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(refs) == 0 {
		return semanticTenantRef{}, false
	}
	start := m.rr % len(refs)
	for i := 0; i < len(refs); i++ {
		idx := (start + i) % len(refs)
		ref := refs[idx]
		if m.inflight[ref.id] >= m.opts.PerTenantConcurrency {
			continue
		}
		m.inflight[ref.id]++
		m.rr = (idx + 1) % len(refs)
		return ref, true
	}
	return semanticTenantRef{}, false
}

func (m *semanticWorkerManager) releaseTenantSlot(tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inflight[tenantID] <= 1 {
		delete(m.inflight, tenantID)
		return
	}
	m.inflight[tenantID]--
}

func (m *semanticWorkerManager) listTenantRefs(ctx context.Context) ([]semanticTenantRef, error) {
	if m.meta != nil && m.pool != nil {
		tenants, err := m.meta.ListTenantsByStatus(ctx, meta.TenantActive, m.opts.TenantScanLimit)
		if err != nil {
			return nil, err
		}
		refs := make([]semanticTenantRef, 0, len(tenants))
		for i := range tenants {
			t := tenants[i]
			if !hasAnyTaskTypes(m.taskTypesForProvider(t.Provider)) {
				continue
			}
			refs = append(refs, semanticTenantRef{id: t.ID, tenant: &t})
		}
		return refs, nil
	}
	if m.shouldIncludeFallback() {
		return []semanticTenantRef{{id: semanticLocalTenantID}}, nil
	}
	return nil, nil
}

func (m *semanticWorkerManager) targetForRef(ctx context.Context, ref semanticTenantRef) (*semanticTarget, error) {
	if ref.id == semanticLocalTenantID {
		if m.fallback == nil {
			return nil, fmt.Errorf("backend missing for %s", ref.id)
		}
		return &semanticTarget{
			tenantID:         ref.id,
			backend:          m.fallback,
			store:            m.fallback.Store(),
			allowedTaskTypes: m.taskTypesForTarget(m.fallback),
			release:          func() {},
		}, nil
	}
	if ref.tenant == nil {
		return nil, fmt.Errorf("tenant metadata missing for %s", ref.id)
	}
	b, release, err := m.pool.Acquire(ctx, ref.tenant)
	if err != nil {
		return nil, fmt.Errorf("acquire tenant backend: %w", err)
	}
	if b == nil {
		release()
		return nil, fmt.Errorf("backend missing for %s", ref.id)
	}
	return &semanticTarget{
		tenantID:         ref.id,
		backend:          b,
		store:            b.Store(),
		allowedTaskTypes: m.taskTypesForTarget(b),
		release:          release,
	}, nil
}

func (m *semanticWorkerManager) invalidateTenantBackend(tenantID string) {
	if tenantID == semanticLocalTenantID {
		return
	}
	if m.pool == nil {
		return
	}
	m.pool.Invalidate(tenantID)
}

func (m *semanticWorkerManager) dispatchTask(ctx context.Context, target *semanticTarget, task *semantic.Task) semanticTaskOutcome {
	switch task.TaskType {
	case semantic.TaskTypeEmbed:
		return m.processEmbedTask(ctx, target.store, task)
	case semantic.TaskTypeImgExtractText:
		return m.processImgExtractTask(ctx, target.backend, task)
	default:
		message := fmt.Sprintf("unsupported task type %q", task.TaskType)
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: "unsupported", message: message}
	}
}

func (m *semanticWorkerManager) startTaskLeaseExecution(ctx context.Context, target *semanticTarget, task *semantic.Task) *semanticTaskLeaseExecution {
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

func (m *semanticWorkerManager) leaseRenewInterval() time.Duration {
	// Renew halfway through the lease window to keep enough slack for transient
	// latency while avoiding unnecessary renew write amplification.
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
	// close(stopCh) + cancel() ensures the renew loop exits whether it is waiting
	// on ticker or blocked inside a datastore call using e.ctx.
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

func (e *semanticTaskLeaseExecution) run(m *semanticWorkerManager, target *semanticTarget, task *semantic.Task) {
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
					metrics.RecordOperation("semantic_worker", "renew", "error", time.Since(renewStart))
					// A renew already in flight can still discover lease loss after
					// processTask begins shutdown, so lease mismatch wins over stop.
					e.markLeaseLost()
					failpoint.InjectCall("semanticWorkerOnLeaseLost", target.tenantID, target.store, task, err)
					metrics.RecordOperation("semantic_worker", "lease_lost", "ok", time.Since(renewStart))
					logger.Warn(e.ctx, "semantic_worker_lease_lost",
						append([]zap.Field{
							zap.String("tenant_id", target.tenantID),
							zap.String("result", "lease_lost"),
						}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
					// Cancel handler context immediately once lease ownership is lost.
					e.cancel()
					e.logStopped(m, target, task)
					return
				}
				if e.shouldStop() {
					e.logStopped(m, target, task)
					return
				}
				metrics.RecordOperation("semantic_worker", "renew", "error", time.Since(renewStart))
				logger.Warn(e.ctx, "semantic_worker_lease_renew_failed",
					append([]zap.Field{
						zap.String("tenant_id", target.tenantID),
						zap.String("result", "error"),
					}, append(semanticTaskLogFields(task), zap.Error(err))...)...)
				continue
			}
			e.recordRenewedLease(leaseUntil)
			failpoint.InjectCall("semanticWorkerAfterRenew", target.tenantID, target.store, task, leaseUntil)
			metrics.RecordOperation("semantic_worker", "renew", "ok", time.Since(renewStart))
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

func (e *semanticTaskLeaseExecution) logStopped(m *semanticWorkerManager, target *semanticTarget, task *semantic.Task) {
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
	logger.Info(e.ctx, "semantic_worker_lease_renew_stopped", fields...)
}

func (m *semanticWorkerManager) processEmbedTask(ctx context.Context, store *datastore.Store, task *semantic.Task) semanticTaskOutcome {
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
	if file.Status != datastore.StatusConfirmed || file.Revision != task.ResourceVersion || strings.TrimSpace(file.ContentText) == "" {
		return semanticTaskOutcome{action: semanticTaskActionAck, result: "obsolete", message: "stale_or_empty"}
	}

	vec, err := m.embedder.EmbedText(ctx, file.ContentText)
	if err != nil {
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: "embed_error", message: fmt.Sprintf("embed text: %v", err)}
	}
	if len(vec) == 0 {
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: "embed_empty", message: "embed text returned empty vector"}
	}

	updated, err := store.UpdateFileEmbedding(ctx, task.ResourceID, task.ResourceVersion, vec)
	if err != nil {
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: "writeback_error", message: fmt.Sprintf("write embedding: %v", err)}
	}
	if !updated {
		return semanticTaskOutcome{action: semanticTaskActionAck, result: "obsolete", message: "conditional_write_miss"}
	}
	return semanticTaskOutcome{action: semanticTaskActionAck, result: "ok", message: "written"}
}

func (m *semanticWorkerManager) processImgExtractTask(ctx context.Context, b *backend.Dat9Backend, task *semantic.Task) semanticTaskOutcome {
	result, err := b.ProcessImageExtractTask(ctx, imageExtractTaskSpecFromSemanticTask(task))
	if err != nil {
		return semanticTaskOutcome{action: semanticTaskActionRetry, result: string(result), message: err.Error()}
	}
	return semanticTaskOutcome{action: semanticTaskActionAck, result: string(result), message: string(result)}
}

func (m *semanticWorkerManager) injectBeforeSemanticTaskFinalize(tenantID string, store *datastore.Store, task *semantic.Task, outcome semanticTaskOutcome) {
	failpoint.InjectCall("semanticWorkerBeforeFinalize", tenantID, store, task, string(outcome.action), outcome.message, outcome.result)
}

func (m *semanticWorkerManager) semanticTaskStillOwned(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task, result string) (bool, error) {
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
	// Treat ownership loss as a finalized runtime event: the worker must not ack
	// or retry once another actor could have legitimately reclaimed the task.
	metrics.RecordOperation("semantic_worker", "lease_lost", "ok", 0)
	logger.Warn(ctx, "semantic_worker_finalize_skipped_lease_lost",
		append([]zap.Field{
			zap.String("tenant_id", tenantID),
			zap.String("result", result),
		}, semanticTaskLogFields(task)...)...)
	return false, nil
}

func semanticWorkerLeaseNow() time.Time {
	now := time.Now().UTC()
	// Keep finalize ownership checks deterministic by letting failpoint tests
	// override the clock used by the worker-side lease re-check.
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
		zap.String("receipt", task.Receipt),
		zap.Int("attempt_count", task.AttemptCount),
	}
	if task.LeaseUntil != nil {
		fields = append(fields, zap.Time("lease_until", task.LeaseUntil.UTC()))
	}
	return fields
}

func (m *semanticWorkerManager) markProcessingStart() {
	m.mu.Lock()
	m.processing++
	inflight := m.processing
	m.mu.Unlock()
	metrics.RecordGauge("semantic_worker", "inflight", float64(inflight))
}

func (m *semanticWorkerManager) markProcessingDone() {
	m.mu.Lock()
	if m.processing > 0 {
		m.processing--
	}
	inflight := m.processing
	m.mu.Unlock()
	metrics.RecordGauge("semantic_worker", "inflight", float64(inflight))
}

func (m *semanticWorkerManager) snapshotProcessing() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processing
}

func (m *semanticWorkerManager) observeOnce(ctx context.Context, now time.Time) {
	snapshot := m.collectObservation(ctx, now)
	metrics.RecordGauge("semantic_worker", "inflight", float64(snapshot.inflight))
	metrics.RecordGauge("semantic_worker", "queued", float64(snapshot.queued))
	metrics.RecordGauge("semantic_worker", "processing", float64(snapshot.processing))
	metrics.RecordGauge("semantic_worker", "dead_lettered", float64(snapshot.deadLettered))
	metrics.RecordGauge("semantic_worker", "queue_lag_seconds", snapshot.queueLagSeconds)
}

func (m *semanticWorkerManager) collectObservation(ctx context.Context, now time.Time) semanticObservationSnapshot {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	snapshot := semanticObservationSnapshot{inflight: m.snapshotProcessing()}
	refs, err := m.listTenantRefs(ctx)
	if err != nil {
		logger.Warn(ctx, "semantic_worker_list_tenants_for_observation_failed", zap.Error(err))
		return snapshot
	}

	var oldest *time.Time
	for _, ref := range refs {
		target, err := m.targetForRef(ctx, ref)
		if err != nil {
			logger.Warn(ctx, "semantic_worker_open_store_for_observation_failed",
				zap.String("tenant_id", ref.id),
				zap.Error(err))
			continue
		}
		func() {
			defer target.release()
			obs, err := target.store.ObserveSemanticTasks(ctx, now)
			if err != nil {
				logger.Warn(ctx, "semantic_worker_observe_failed",
					zap.String("tenant_id", ref.id),
					zap.Error(err))
				m.invalidateTenantBackend(ref.id)
				return
			}
			snapshot.queued += obs.Queued
			snapshot.processing += obs.Processing
			snapshot.deadLettered += obs.DeadLettered
			if obs.OldestClaimableAvailableAt != nil && (oldest == nil || obs.OldestClaimableAvailableAt.Before(*oldest)) {
				t := obs.OldestClaimableAvailableAt.UTC()
				oldest = &t
			}
		}()
	}
	if oldest != nil {
		lag := now.Sub(*oldest).Seconds()
		if lag > 0 {
			snapshot.queueLagSeconds = lag
		}
	}
	return snapshot
}

func hasAnyTaskTypes(types []semantic.TaskType) bool {
	return len(types) > 0
}

// appManagedTaskTypes returns task types driven by the worker embedder (app-managed path).
func (m *semanticWorkerManager) appManagedTaskTypes() []semantic.TaskType {
	if m == nil {
		return nil
	}
	return appManagedSemanticTaskTypes(m.embedder)
}

// taskTypesForProvider is the routing filter for meta tenant list scanning: TiDB-auto
// tenants use pool auto types; others require app-managed embed.
func (m *semanticWorkerManager) taskTypesForProvider(provider string) []semantic.TaskType {
	if m == nil {
		return nil
	}
	if semanticWorkerUsesTiDBAutoEmbedding(provider) {
		if m.pool == nil {
			return nil
		}
		return m.pool.AutoSemanticTaskTypes()
	}
	return m.appManagedTaskTypes()
}

func (m *semanticWorkerManager) shouldIncludeFallback() bool {
	if m == nil || m.fallback == nil {
		return false
	}
	if m.fallback.UsesDatabaseAutoEmbedding() {
		return hasAnyTaskTypes(m.fallback.AutoSemanticTaskTypes())
	}
	return hasAnyTaskTypes(m.appManagedTaskTypes())
}

// taskTypesForTarget is the effective ClaimSemanticTask filter for a concrete backend.
func (m *semanticWorkerManager) taskTypesForTarget(b *backend.Dat9Backend) []semantic.TaskType {
	if m == nil || b == nil {
		return nil
	}
	if b.UsesDatabaseAutoEmbedding() {
		return b.AutoSemanticTaskTypes()
	}
	return m.appManagedTaskTypes()
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

// semanticWorkerLogTaskTypesFromTypes stringifies AutoSemanticTaskTypes-style slices for zap.
func semanticWorkerLogTaskTypesFromTypes(types []semantic.TaskType) []string {
	if len(types) == 0 {
		return nil
	}
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return out
}
