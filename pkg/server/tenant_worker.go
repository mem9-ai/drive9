package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/embedding"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/semantic"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

const (
	defaultTenantWorkers              = 1
	defaultTenantPollInterval         = 200 * time.Millisecond
	defaultTenantLeaseDuration        = 30 * time.Second
	defaultTenantRetryBaseDelay        = 200 * time.Millisecond
	defaultTenantRetryMaxDelay         = 30 * time.Second
	defaultTenantPerTenantConcurrency = 1
	tenantLocalID                     = "local"
	// tenantKickQueueCapacity bounds buffered kicks; overflow kicks are left
	// pending in kickPending and re-enqueued by flushDelayedKicks on the next
	// workerLoop ticker tick. The safety-net scan remains the durable fallback
	// for claiming queued tasks.
	tenantKickQueueCapacity = 256
	// tenantKickDrainLimit caps tasks drained per kick so one busy tenant
	// cannot monopolize a worker; the tenant is re-kicked to continue after
	// other pending kicks get their turn.
	tenantKickDrainLimit = 8
	// fileGCRecoverLimit bounds expired file_gc lease recovery per kick.
	fileGCRecoverLimit = 100
	// fileGCDrainBatchSize caps file_gc tasks drained per kick.
	fileGCDrainBatchSize = 100
	// semanticRecoverLimit bounds expired semantic task lease recovery per kick.
	semanticRecoverLimit = 64
	// defaultTenantMaintenanceInterval is the throttle interval for piggybacked
	// maintenance (fs_events cleanup + observation metrics) per tenant.
	defaultTenantMaintenanceInterval = 5 * time.Minute
)

var tenantWorkerUsesTiDBAutoEmbedding = tenant.UsesTiDBAutoEmbedding

var tenantWorkerAllowedEmbedTaskTypes = []semantic.TaskType{semantic.TaskTypeEmbed}

func appManagedTenantTaskTypes(embedder embedding.Client) []semantic.TaskType {
	if embedder == nil {
		return nil
	}
	return tenantWorkerAllowedEmbedTaskTypes
}

// TenantWorkerOptions controls unified background task processing.
type TenantWorkerOptions struct {
	// Workers is the number of worker goroutines consuming kicks.
	Workers int
	// PollInterval is the idle wait for the single-tenant fallback ticker.
	PollInterval time.Duration
	// LeaseDuration is the base semantic task lease window used by claim/renew.
	LeaseDuration time.Duration
	// RetryBaseDelay is the initial backoff for semantic retry scheduling.
	RetryBaseDelay time.Duration
	// RetryMaxDelay is the cap for exponential retry backoff.
	RetryMaxDelay time.Duration
	// PerTenantConcurrency limits concurrent tasks per tenant.
	PerTenantConcurrency int
	// MaintenanceInterval throttles piggybacked maintenance per tenant.
	MaintenanceInterval time.Duration
}

func (o *TenantWorkerOptions) normalize() {
	if o.Workers <= 0 {
		o.Workers = defaultTenantWorkers
	}
	if o.PollInterval <= 0 {
		o.PollInterval = defaultTenantPollInterval
	}
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = defaultTenantLeaseDuration
	}
	if o.RetryBaseDelay <= 0 {
		o.RetryBaseDelay = defaultTenantRetryBaseDelay
	}
	if o.RetryMaxDelay <= 0 {
		o.RetryMaxDelay = defaultTenantRetryMaxDelay
	}
	if o.RetryMaxDelay < o.RetryBaseDelay {
		o.RetryMaxDelay = o.RetryBaseDelay
	}
	if o.PerTenantConcurrency <= 0 {
		o.PerTenantConcurrency = defaultTenantPerTenantConcurrency
	}
	if o.MaintenanceInterval <= 0 {
		o.MaintenanceInterval = defaultTenantMaintenanceInterval
	}
}

// TenantWorkerWillRun reports whether NewWithConfig would construct a non-nil
// tenant worker manager for cfg.
func TenantWorkerWillRun(cfg Config) bool {
	return newTenantWorkerManager(cfg.Backend, cfg.Meta, cfg.Pool, cfg.SemanticEmbedder, cfg.TenantWorkers, cfg.TenantMaintenanceInterval) != nil
}

// ValidateDurableAsyncExtractRequiresTenantWorker returns an error when async
// image or audio extraction runtimes are enabled on the backend template but
// the tenant worker would not start for cfg.
func ValidateDurableAsyncExtractRequiresTenantWorker(cfg Config, template backend.Options, localTiDBAutoOnly bool) error {
	willWire := backend.AsyncImageExtractWillWireRuntime(template.AsyncImageExtract) ||
		backend.AsyncAudioExtractWillWireRuntime(template.AsyncAudioExtract)
	if !willWire {
		return nil
	}
	if localTiDBAutoOnly && !template.DatabaseAutoEmbedding {
		return nil
	}
	if TenantWorkerWillRun(cfg) {
		return nil
	}
	return fmt.Errorf("tenant worker would not start but durable async image/audio extract is enabled; configure DRIVE9_EMBED_* for app-managed embedding or fix worker/task-type routing so img_extract_text and audio_extract_text can be claimed")
}

// kickMsg carries a tenant ID and accumulated work mask to a worker goroutine.
type kickMsg struct {
	tenantID string
	workMask int
}

// tenantWorkerManager is the unified worker that processes kicks from the
// tenant_notify_outbox poller. Each kick carries a work_mask selecting which
// work types to drain: semantic tasks, file_gc tasks, quota outbox rows. The
// manager also recovers expired leases and runs piggyback maintenance
// (fs_events cleanup + observation metrics) with a per-tenant throttle.
//
// In single-tenant (fallback) mode, a workerLoop ticker polls the fallback
// backend for all work types on PollInterval.
type tenantWorkerManager struct {
	fallback *backend.Dat9Backend
	meta     *meta.Store
	pool     *tenant.Pool
	embedder embedding.Client
	opts     TenantWorkerOptions

	mu          sync.Mutex
	inflight    map[string]int
	processing  int
	kickPending map[string]int // tenantID → accumulated work_mask

	kicks chan kickMsg

	lastMaintenance map[string]time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type tenantTarget struct {
	tenantID         string
	backend          *backend.Dat9Backend
	store            *datastore.Store
	allowedTaskTypes []semantic.TaskType
	release          func()
}

func newTenantWorkerManager(fallback *backend.Dat9Backend, metaStore *meta.Store, pool *tenant.Pool, embedder embedding.Client, opts TenantWorkerOptions, maintenanceInterval time.Duration) *tenantWorkerManager {
	hasMultiTenant := metaStore != nil && pool != nil
	if fallback == nil && !hasMultiTenant {
		return nil
	}
	m := &tenantWorkerManager{
		fallback: fallback,
		meta:     metaStore,
		pool:     pool,
		embedder: embedder,
	}
	if maintenanceInterval > 0 {
		opts.MaintenanceInterval = maintenanceInterval
	}
	// The unified tenant worker handles semantic, file_gc, and quota work.
	// It is viable whenever there is a multi-tenant pool or a fallback backend.
	viable := hasMultiTenant || fallback != nil
	if !viable {
		return nil
	}
	// In single-tenant (fallback-only) mode, semantic work requires an
	// embedder or async-extract support; file_gc and quota remain viable
	// regardless, so we do not return nil for file_gc/quota-only deployments.
	opts.normalize()
	m.opts = opts
	m.inflight = make(map[string]int)
	m.kickPending = make(map[string]int)
	m.lastMaintenance = make(map[string]time.Time)
	m.kicks = make(chan kickMsg, tenantKickQueueCapacity)
	return m
}

// Kick prompts a worker to process work for tenantID. The workMask is
// OR-accumulated with any pending kick for the same tenant so a burst of
// different work types coalesces into one kick. Best-effort: duplicates
// collapse while one is pending, and the kick is left pending (delayed) when
// the buffer is full — flushDelayedKicks re-enqueues it on the next ticker
// tick, and the safety-net scan remains the durable fallback path.
func (m *tenantWorkerManager) Kick(tenantID string, workMask int) {
	if m == nil || tenantID == "" || workMask == 0 {
		return
	}
	m.mu.Lock()
	if pending, ok := m.kickPending[tenantID]; ok {
		m.kickPending[tenantID] = pending | workMask
		m.mu.Unlock()
		metrics.RecordTenantOperation(tenantID, "tenant_worker", "kick", "coalesced", 0)
		return
	}
	m.kickPending[tenantID] = workMask
	m.mu.Unlock()
	select {
	case m.kicks <- kickMsg{tenantID: tenantID, workMask: workMask}:
		metrics.RecordTenantOperation(tenantID, "tenant_worker", "kick", "queued", 0)
	default:
		// Channel full: leave kickPending so flushDelayedKicks (ticked from
		// workerLoop) re-enqueues when the channel has space. The work mask is
		// already recorded in kickPending above; do NOT clear it.
		metrics.RecordTenantOperation(tenantID, "tenant_worker", "kick", "delayed", 0)
	}
}

func (m *tenantWorkerManager) clearKickPending(tenantID string) {
	m.mu.Lock()
	delete(m.kickPending, tenantID)
	m.mu.Unlock()
}

// pendingWorkMask returns the accumulated work mask for a tenant (0 if none
// pending), and clears the pending entry. Called by the worker before
// dispatching so a kick arriving during processing triggers a fresh kick.
func (m *tenantWorkerManager) takePendingWorkMask(tenantID string) int {
	m.mu.Lock()
	mask := m.kickPending[tenantID]
	delete(m.kickPending, tenantID)
	m.mu.Unlock()
	return mask
}

// flushDelayedKicks re-enqueues kicks that were left pending when the kicks
// channel was full. Called on each workerLoop ticker tick so delayed work is
// delivered once the channel drains. Entries still pending after a successful
// send are consumed by the receiving worker via takePendingWorkMask.
func (m *tenantWorkerManager) flushDelayedKicks() {
	// Snapshot pending entries under the lock, then attempt non-blocking sends
	// outside the lock to avoid holding it during channel ops.
	m.mu.Lock()
	if len(m.kickPending) == 0 {
		m.mu.Unlock()
		return
	}
	pending := make(map[string]int, len(m.kickPending))
	for tenantID, mask := range m.kickPending {
		pending[tenantID] = mask
	}
	m.mu.Unlock()
	for tenantID, mask := range pending {
		select {
		case m.kicks <- kickMsg{tenantID: tenantID, workMask: mask}:
			// Successfully (re-)queued. Leave the kickPending entry in place:
			// the worker merges it via takePendingWorkMask after claiming the
			// slot, so any further coalesced kicks are not lost.
			metrics.RecordTenantOperation(tenantID, "tenant_worker", "kick", "flushed", 0)
		default:
			// Still full; leave pending for the next tick.
		}
	}
}

func (m *tenantWorkerManager) Start(ctx context.Context) {
	if m == nil || m.cancel != nil {
		return
	}
	workerCtx, cancel := context.WithCancel(backgroundWithTrace(ctx))
	m.cancel = cancel
	metrics.SetModuleAvailability("semantic_worker", true)
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
	logger.Info(workerCtx, "tenant_worker_manager_started",
		zap.Int("workers", m.opts.Workers),
		zap.Duration("poll_interval", m.opts.PollInterval),
		zap.Duration("lease_duration", m.opts.LeaseDuration),
		zap.Duration("maintenance_interval", m.opts.MaintenanceInterval))
}

func (m *tenantWorkerManager) Stop() {
	if m == nil || m.cancel == nil {
		return
	}
	m.cancel()
	m.wg.Wait()
	m.cancel = nil
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processing = 0
	metrics.SetModuleAvailability("semantic_worker", false)
	metrics.RecordGauge("semantic_worker", "workers", 0)
	metrics.RecordGauge("semantic_worker", "inflight", 0)
}

func (m *tenantWorkerManager) workerLoop(ctx context.Context, workerID int) {
	defer m.wg.Done()
	// Single-tenant fallback: poll the fallback backend on PollInterval when
	// no kicks arrive (no meta store / no pool). This keeps the local backend
	// drained without depending on the outbox poller.
	ticker := time.NewTicker(m.opts.PollInterval)
	defer ticker.Stop()
	for {
		// In single-tenant mode with a fallback backend, drain it proactively.
		if m.shouldPollFallback() {
			processed := m.pollFallbackOnce(ctx)
			if processed {
				continue
			}
		}
		select {
		case <-ctx.Done():
			logger.Info(ctx, "tenant_worker_stopped", zap.Int("worker_id", workerID))
			return
		case msg := <-m.kicks:
			m.processKicked(ctx, msg.tenantID, msg.workMask)
		case <-ticker.C:
			// Re-enqueue delayed kicks now that the channel may have space.
			m.flushDelayedKicks()
		}
	}
}

// pollFallbackOnce drains the local fallback backend for one round of semantic,
// file_gc, and quota work. Returns true if any task was processed (caller should
// loop immediately), false if all queues are empty.
func (m *tenantWorkerManager) pollFallbackOnce(ctx context.Context) bool {
	if m.fallback == nil {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	target := &tenantTarget{
		tenantID:         tenantLocalID,
		backend:          m.fallback,
		store:            m.fallback.Store(),
		allowedTaskTypes: m.taskTypesForTarget(m.fallback),
		release:          func() {},
	}
	processed := false
	if len(target.allowedTaskTypes) > 0 {
		if m.claimAndProcessOne(ctx, target) {
			processed = true
		}
	}
	// Drain one file_gc task. ProcessOneFileGCTask returns (false, nil) when
	// the queue is empty — only set processed=true when it actually processed.
	if did, err := m.fallback.ProcessOneFileGCTask(ctx); err == nil && did {
		processed = true
	}
	return processed
}

func (m *tenantWorkerManager) shouldPollFallback() bool {
	if m.fallback == nil {
		return false
	}
	// Only poll fallback proactively in single-tenant mode (no multi-tenant
	// pool). In multi-tenant mode the fallback is not scheduled by kicks.
	return m.meta == nil || m.pool == nil
}

// processKicked resolves a kicked tenant, acquires its backend, and drains all
// selected work types. workMask selects which drains run. After draining, it
// recovers expired leases and runs piggyback maintenance (throttled). The
// tenant is re-kicked if more work remains.
func (m *tenantWorkerManager) processKicked(ctx context.Context, tenantID string, workMask int) {
	// Do NOT clear kickPending here — a kick coalesced while this message was
	// in the channel must be consumed after acquiring the tenant slot.
	ref, ok := m.kickRef(ctx, tenantID)
	if !ok {
		m.clearKickPending(tenantID)
		return
	}
	if !m.tryClaimTenantSlot(ref.id) {
		// Another worker holds the slot; leave kickPending for that worker to consume.
		return
	}
	// Merge any work mask that coalesced while this kick was queued.
	workMask |= m.takePendingWorkMask(tenantID)
	target, err := m.targetForRef(ctx, ref)
	if err != nil {
		logger.Warn(ctx, "tenant_worker_kick_open_store_failed",
			zap.String("tenant_id", ref.id),
			zap.Error(err))
		m.releaseTenantSlot(ref.id)
		return
	}
	target.release = chainReleases(target.release, func() { m.releaseTenantSlot(ref.id) })
	defer target.release()

	// Drain semantic tasks (if selected and allowed).
	var reKickMask int
	semanticMask := workMask & WorkSemantic
	if semanticMask != 0 && len(target.allowedTaskTypes) > 0 {
		semanticDrained := 0
		for range tenantKickDrainLimit {
			if ctx.Err() != nil {
				return
			}
			if !m.claimAndProcessOne(ctx, target) {
				break
			}
			semanticDrained++
		}
		if semanticDrained >= tenantKickDrainLimit {
			reKickMask |= WorkSemantic
		}
	}

	// Drain file_gc tasks (if selected).
	if workMask&WorkFileGC != 0 {
		if m.drainFileGC(ctx, target) {
			reKickMask |= WorkFileGC
		}
	}

	// Recover expired leases for all work types (cheap, runs on every kick).
	m.recoverExpired(ctx, target)

	// Piggyback maintenance: fs_events cleanup + observation metrics, throttled
	// per tenant by MaintenanceInterval.
	m.piggybackMaintenance(ctx, target)

	// Re-kick if there's pending work accumulated during processing, or if a
	// drain hit its cap (more work likely remains for that type).
	pending := m.takePendingWorkMask(tenantID)
	reKick := pending | reKickMask
	if reKick != 0 {
		m.Kick(tenantID, reKick)
	}
}

// drainFileGC recovers expired file_gc leases and drains available tasks.
// Returns true if the drain hit its batch cap (more work likely remains).
func (m *tenantWorkerManager) drainFileGC(ctx context.Context, target *tenantTarget) (hitCap bool) {
	if ctx.Err() != nil {
		return false
	}
	b := target.backend
	if b == nil {
		return false
	}
	now := time.Now().UTC()
	if _, err := target.store.RecoverExpiredFileGCTasks(ctx, now, fileGCRecoverLimit); err != nil {
		if !isContextDoneErr(err) {
			logger.Warn(ctx, "tenant_worker_file_gc_recover_failed",
				zap.String("tenant_id", target.tenantID), zap.Error(err))
		}
	}
	for i := 0; i < fileGCDrainBatchSize; i++ {
		if ctx.Err() != nil {
			return false
		}
		processed, err := b.ProcessOneFileGCTask(ctx)
		if err != nil {
			if !isContextDoneErr(err) {
				logger.Warn(ctx, "tenant_worker_file_gc_process_failed",
					zap.String("tenant_id", target.tenantID), zap.Error(err))
			}
		}
		if !processed {
			return false
		}
	}
	return true // drained the full batch — likely more remains
}

// recoverExpired recovers expired semantic task leases for the tenant.
func (m *tenantWorkerManager) recoverExpired(ctx context.Context, target *tenantTarget) {
	if ctx.Err() != nil {
		return
	}
	start := time.Now()
	recovered, err := target.store.RecoverExpiredSemanticTasks(ctx, time.Now().UTC(), semanticRecoverLimit)
	if err != nil {
		if !isContextDoneErr(err) {
			metrics.RecordTenantOperation(target.tenantID, "semantic_worker", "recover", "error", time.Since(start))
			logger.Warn(ctx, "tenant_worker_recover_failed",
				zap.String("tenant_id", target.tenantID), zap.Error(err))
		}
		return
	}
	if recovered > 0 {
		metrics.RecordTenantOperation(target.tenantID, "semantic_worker", "recover", "ok", time.Since(start))
		logger.Info(ctx, "tenant_worker_recover_ok",
			zap.String("tenant_id", target.tenantID),
			zap.Int("recovered", recovered))
	}
}

// piggybackMaintenance runs fs_events cleanup and observation metrics for the
// tenant, throttled to once per MaintenanceInterval. This replaces the old
// independent cleanupFSEvents leader goroutine and collectObservation scan.
func (m *tenantWorkerManager) piggybackMaintenance(ctx context.Context, target *tenantTarget) {
	if ctx.Err() != nil {
		return
	}
	now := time.Now()
	m.mu.Lock()
	last := m.lastMaintenance[target.tenantID]
	if now.Sub(last) < m.opts.MaintenanceInterval {
		m.mu.Unlock()
		return
	}
	m.lastMaintenance[target.tenantID] = now
	m.mu.Unlock()

	// fs_events cleanup: prune rows older than fsEventsRetention.
	if count, err := target.store.CountFSEvents(ctx); err == nil {
		metrics.RecordFSEventsRows(target.tenantID, count)
	}
	if n, err := target.store.DeleteFSEventsBefore(ctx, now.Add(-fsEventsRetention)); err != nil {
		if ctx.Err() == nil {
			logger.Warn(ctx, "tenant_worker_fs_events_cleanup_failed",
				zap.String("tenant_id", target.tenantID), zap.Error(err))
		}
	} else {
		metrics.RecordFSEventsPruned(target.tenantID, n)
	}

	// Observation metrics: sample queue depth + dead-letter count.
	m.observeTenant(ctx, target, now)
}

func (m *tenantWorkerManager) observeTenant(ctx context.Context, target *tenantTarget, now time.Time) {
	obs, err := target.store.ObserveSemanticTasks(ctx, now.UTC())
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn(ctx, "tenant_worker_observe_failed",
				zap.String("tenant_id", target.tenantID), zap.Error(err))
		}
		return
	}
	metrics.RecordTenantGauge(target.tenantID, "semantic_worker", "dead_lettered", float64(obs.DeadLettered))
	tenantLag := float64(0)
	if obs.OldestClaimableAvailableAt != nil {
		tenantLag = now.UTC().Sub(obs.OldestClaimableAvailableAt.UTC()).Seconds()
		if tenantLag < 0 {
			tenantLag = 0
		}
	}
	metrics.RecordTenantGauge(target.tenantID, "semantic_worker", "queue_lag_seconds", tenantLag)
}

// kickRef resolves a kicked tenant ID to a schedulable ref, applying the same
// status and provider/task-type filters as the scan path.
func (m *tenantWorkerManager) kickRef(ctx context.Context, tenantID string) (semanticTenantRef, bool) {
	if tenantID == tenantLocalID {
		if m.meta == nil || m.pool == nil {
			if m.shouldIncludeFallback() {
				return semanticTenantRef{id: tenantLocalID}, true
			}
		}
		return semanticTenantRef{}, false
	}
	if m.meta == nil || m.pool == nil {
		return semanticTenantRef{}, false
	}
	t, err := m.meta.GetTenant(ctx, tenantID)
	if err != nil {
		logger.Warn(ctx, "tenant_worker_kick_tenant_lookup_failed",
			zap.String("tenant_id", tenantID),
			zap.Error(err))
		return semanticTenantRef{}, false
	}
	if t.Status != meta.TenantActive {
		return semanticTenantRef{}, false
	}
	if !hasAnyTaskTypes(m.taskTypesForProvider(t.Provider)) && !m.hasShardedWorkForTenant(t.Provider) {
		return semanticTenantRef{}, false
	}
	return semanticTenantRef{id: t.ID, tenant: t}, true
}

// hasShardedWorkForTenant reports whether the tenant provider supports any
// sharded work type (file_gc or quota), independent of semantic task types.
// File_gc and quota outbox processing don't depend on semantic task routing.
func (m *tenantWorkerManager) hasShardedWorkForTenant(provider string) bool {
	// File GC and quota outbox run for all active tenants with a backend.
	// The backend must exist (Acquire succeeds) — we don't need task-type
	// routing to enable them. So any active tenant is eligible.
	return true
}

func (m *tenantWorkerManager) targetForRef(ctx context.Context, ref semanticTenantRef) (*tenantTarget, error) {
	if ref.id == tenantLocalID {
		if m.fallback == nil {
			return nil, fmt.Errorf("backend missing for %s", ref.id)
		}
		return &tenantTarget{
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
	return &tenantTarget{
		tenantID:         ref.id,
		backend:          b,
		store:            b.Store(),
		allowedTaskTypes: m.taskTypesForTarget(b),
		release:          release,
	}, nil
}

func (m *tenantWorkerManager) invalidateTenantBackend(tenantID string) {
	if tenantID == tenantLocalID {
		return
	}
	if m.pool == nil {
		return
	}
	m.pool.Invalidate(tenantID)
}

// claimAndProcessOne claims one semantic task and processes it. Returns true
// if a task was claimed (and the caller should try again), false if the queue
// is empty.
func (m *tenantWorkerManager) claimAndProcessOne(ctx context.Context, target *tenantTarget) bool {
	claimStart := time.Now()
	task, found, err := target.store.ClaimSemanticTask(ctx, time.Now().UTC(), m.opts.LeaseDuration, target.allowedTaskTypes...)
	if err != nil {
		metrics.RecordTenantOperation(target.tenantID, "semantic_worker", "claim", "error", time.Since(claimStart))
		logger.Warn(ctx, "tenant_worker_claim_failed",
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
	metrics.RecordTenantOperation(target.tenantID, "semantic_worker", "claim", "ok", time.Since(claimStart))
	logger.Info(ctx, "tenant_worker_claim_ok",
		append([]zap.Field{
			zap.String("tenant_id", target.tenantID),
			zap.String("result", "ok"),
		}, semanticTaskLogFields(task)...)...)
	m.markProcessingStart()
	defer m.markProcessingDone()
	m.processTask(ctx, target, task)
	return true
}

// tryClaimTenantSlot attempts to acquire a per-tenant concurrency slot.
func (m *tenantWorkerManager) tryClaimTenantSlot(tenantID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inflight[tenantID] >= m.opts.PerTenantConcurrency {
		return false
	}
	m.inflight[tenantID]++
	return true
}

func (m *tenantWorkerManager) releaseTenantSlot(tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inflight[tenantID] <= 1 {
		delete(m.inflight, tenantID)
		return
	}
	m.inflight[tenantID]--
}

func (m *tenantWorkerManager) markProcessingStart() {
	m.mu.Lock()
	m.processing++
	inflight := m.processing
	m.mu.Unlock()
	metrics.RecordGauge("semantic_worker", "inflight", float64(inflight))
}

func (m *tenantWorkerManager) markProcessingDone() {
	m.mu.Lock()
	if m.processing > 0 {
		m.processing--
	}
	inflight := m.processing
	m.mu.Unlock()
	metrics.RecordGauge("semantic_worker", "inflight", float64(inflight))
}

func (m *tenantWorkerManager) appManagedTaskTypes() []semantic.TaskType {
	if m == nil {
		return nil
	}
	return appManagedTenantTaskTypes(m.embedder)
}

func (m *tenantWorkerManager) poolExtractTaskTypes() []semantic.TaskType {
	if m == nil || m.pool == nil {
		return nil
	}
	return m.pool.AutoSemanticTaskTypes()
}

func (m *tenantWorkerManager) fallbackExtractTaskTypes() []semantic.TaskType {
	if m == nil || m.fallback == nil {
		return nil
	}
	var out []semantic.TaskType
	if m.fallback.SupportsAsyncImageExtract() {
		out = append(out, semantic.TaskTypeImgExtractText)
	}
	if m.fallback.SupportsAsyncAudioExtract() {
		out = append(out, semantic.TaskTypeAudioExtractText)
	}
	return out
}

func (m *tenantWorkerManager) taskTypesForProvider(provider string) []semantic.TaskType {
	if m == nil {
		return nil
	}
	if tenantWorkerUsesTiDBAutoEmbedding(provider) {
		if m.pool == nil {
			return nil
		}
		types := m.pool.AutoSemanticTaskTypes()
		if m.pool.IsAutoEmbeddingDisabled() {
			return unionTaskTypes(m.appManagedTaskTypes(), types)
		}
		if types != nil {
			return types
		}
		return nil
	}
	return unionTaskTypes(m.appManagedTaskTypes(), m.poolExtractTaskTypes())
}

func (m *tenantWorkerManager) shouldIncludeFallback() bool {
	if m == nil || m.fallback == nil {
		return false
	}
	if m.fallback.UsesDatabaseAutoEmbedding() {
		return hasAnyTaskTypes(m.fallback.AutoSemanticTaskTypes())
	}
	return hasAnyTaskTypes(m.appManagedTaskTypes()) || hasAnyTaskTypes(m.fallbackExtractTaskTypes())
}

func (m *tenantWorkerManager) taskTypesForTarget(b *backend.Dat9Backend) []semantic.TaskType {
	if m == nil || b == nil {
		return nil
	}
	if b.UsesDatabaseAutoEmbedding() {
		return b.AutoSemanticTaskTypes()
	}
	var out []semantic.TaskType
	if b.SupportsAsyncImageExtract() {
		out = append(out, semantic.TaskTypeImgExtractText)
	}
	if b.SupportsAsyncAudioExtract() {
		out = append(out, semantic.TaskTypeAudioExtractText)
	}
	out = append(out, m.appManagedTaskTypes()...)
	if len(out) == 0 {
		return nil
	}
	return out
}

func isContextDoneErr(err error) bool {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	// "database is closed" arises from go-sql-driver/mysql when the underlying
	// connection is torn down during shutdown; treat it as a context-done signal
	// so it is not logged as an unexpected error.
	if strings.Contains(err.Error(), "database is closed") {
		return true
	}
	return false
}

// failpoint injection hook used by semantic task processing (preserved from
// semantic_worker.go for failpoint tests).
func (m *tenantWorkerManager) injectBeforeSemanticTaskFinalize(tenantID string, store *datastore.Store, task *semantic.Task, outcome semanticTaskOutcome) {
	failpoint.InjectCall("semanticWorkerBeforeFinalize", tenantID, store, task, string(outcome.action), outcome.message, outcome.result)
}