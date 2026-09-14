package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/embedding"
	"github.com/mem9-ai/drive9/pkg/extent"
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
	defaultTenantRetryBaseDelay       = 200 * time.Millisecond
	defaultTenantRetryMaxDelay        = 30 * time.Second
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
	// FSEventsRetention is how long fs_events rows are kept before the
	// piggybacked maintenance sweep prunes them (backstop for the lazy
	// write-path sweep). Defaults to defaultFSEventsRetention.
	FSEventsRetention time.Duration
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
	if o.FSEventsRetention <= 0 {
		o.FSEventsRetention = defaultFSEventsRetention
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
	tenantID       string
	tidbCloudOrgID string
	workMask       int
}

type pendingKick struct {
	tidbCloudOrgID string
	workMask       int
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
	kickPending map[string]pendingKick // tenantID → accumulated work_mask + best-known org id

	kicks chan kickMsg

	lastMaintenance   map[string]time.Time
	semanticMetricOrg map[string]string // tenantID -> org used by the last semantic gauge observation

	// extentRuntimes caches one JuiceFS data-plane Runtime per tenant for the
	// extent jobs (block GC / session sweep / compact fallback); see
	// extentRuntimePool.
	extentRuntimes *extentRuntimePool

	// ownsTenant gates work that must not be duplicated across pods. It is set
	// from the shard resolver once the pod ring exists (SetOwnsTenant) and reads
	// as "owns everything" until then, which is the single-pod answer.
	ownsTenant atomic.Pointer[func(string) bool]

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type tenantTarget struct {
	tenantID         string
	tidbCloudOrgID   string
	backend          *backend.Dat9Backend
	store            *datastore.Store
	allowedTaskTypes []semantic.TaskType
	release          func()
}

func (t *tenantTarget) metricOrgID() string {
	if t == nil {
		return defaultTenantMetricTiDBCloudOrgID
	}
	return normalizeTenantMetricTiDBCloudOrgID(t.tidbCloudOrgID)
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
	m.kickPending = make(map[string]pendingKick)
	m.lastMaintenance = make(map[string]time.Time)
	m.semanticMetricOrg = make(map[string]string)
	m.extentRuntimes = newExtentRuntimePool()
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
	m.KickWithOrg(tenantID, "", workMask)
}

// KickWithOrg is Kick with an already-resolved TiDB Cloud org label. Callers
// without a resolved org should pass an empty string and let metrics normalize
// it to the shared guest label.
func (m *tenantWorkerManager) KickWithOrg(tenantID, tidbCloudOrgID string, workMask int) {
	if m == nil || tenantID == "" || workMask == 0 {
		return
	}
	m.mu.Lock()
	if pending, ok := m.kickPending[tenantID]; ok {
		pending.workMask |= workMask
		pending.tidbCloudOrgID = mergeKickOrgID(pending.tidbCloudOrgID, tidbCloudOrgID)
		m.kickPending[tenantID] = pending
		m.mu.Unlock()
		metrics.RecordTenantOperationWithOrg(tenantID, pending.tidbCloudOrgID, "tenant_worker", "kick", "coalesced", 0)
		return
	}
	tidbCloudOrgID = strings.TrimSpace(tidbCloudOrgID)
	m.kickPending[tenantID] = pendingKick{tidbCloudOrgID: tidbCloudOrgID, workMask: workMask}
	m.mu.Unlock()
	select {
	case m.kicks <- kickMsg{tenantID: tenantID, tidbCloudOrgID: tidbCloudOrgID, workMask: workMask}:
		metrics.RecordTenantOperationWithOrg(tenantID, tidbCloudOrgID, "tenant_worker", "kick", "queued", 0)
	default:
		// Channel full: leave kickPending so flushDelayedKicks (ticked from
		// workerLoop) re-enqueues when the channel has space. The work mask is
		// already recorded in kickPending above; do NOT clear it.
		metrics.RecordTenantOperationWithOrg(tenantID, tidbCloudOrgID, "tenant_worker", "kick", "delayed", 0)
	}
}

// SetOwnsTenant installs the shard-ownership predicate used to gate work that
// must not run on two pods at once (the extent quota report). It is the single
// install path — the options struct deliberately has no field for it, because
// the resolver that owns the ring is built after the manager — and it is safe to
// call once the workers are running. A nil predicate means this pod owns
// everything, which is the single-pod answer.
func (m *tenantWorkerManager) SetOwnsTenant(fn func(string) bool) {
	if m == nil {
		return
	}
	if fn == nil {
		fn = func(string) bool { return true }
	}
	m.ownsTenant.Store(&fn)
}

// ownsTenantWork reports whether this pod owns tenantID's sharded work.
func (m *tenantWorkerManager) ownsTenantWork(tenantID string) bool {
	if m == nil {
		return false
	}
	if fn := m.ownsTenant.Load(); fn != nil {
		return (*fn)(tenantID)
	}
	return true
}

// ForgetTenant drops process-local scheduler bookkeeping after the tenant
// lifecycle has durably entered deletion. A queued kick may still be present
// in the channel, but processKicked will reject it against the persisted
// non-active status and clear it again.
func (m *tenantWorkerManager) ForgetTenant(tenantID string) {
	if m == nil || tenantID == "" {
		return
	}
	m.mu.Lock()
	delete(m.kickPending, tenantID)
	delete(m.lastMaintenance, tenantID)
	delete(m.semanticMetricOrg, tenantID)
	m.mu.Unlock()
	// A deleted tenant needs no background work: drop its cached extent
	// runtime so its JuiceFS session does not outlive the tenant.
	m.extentRuntimes.forget(tenantID)
}

func mergeKickOrgID(current, next string) string {
	next = strings.TrimSpace(next)
	if next != "" {
		return next
	}
	return current
}

func (m *tenantWorkerManager) clearKickPending(tenantID string) {
	m.mu.Lock()
	delete(m.kickPending, tenantID)
	m.mu.Unlock()
}

// takePendingKick returns the accumulated work mask and best-known org id for
// a tenant, and clears the pending entry. Called by the worker before
// dispatching so a kick arriving during processing triggers a fresh kick.
func (m *tenantWorkerManager) takePendingKick(tenantID string) pendingKick {
	m.mu.Lock()
	pending := m.kickPending[tenantID]
	delete(m.kickPending, tenantID)
	m.mu.Unlock()
	return pending
}

func (m *tenantWorkerManager) takePendingWorkMask(tenantID string) int {
	pending := m.takePendingKick(tenantID)
	return pending.workMask
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
	pending := make(map[string]pendingKick, len(m.kickPending))
	for tenantID, entry := range m.kickPending {
		pending[tenantID] = entry
	}
	m.mu.Unlock()
	for tenantID, entry := range pending {
		select {
		case m.kicks <- kickMsg{tenantID: tenantID, tidbCloudOrgID: entry.tidbCloudOrgID, workMask: entry.workMask}:
			// Successfully (re-)queued. Leave the kickPending entry in place:
			// the worker merges it via takePendingWorkMask after claiming the
			// slot, so any further coalesced kicks are not lost.
			metrics.RecordTenantOperationWithOrg(tenantID, entry.tidbCloudOrgID, "tenant_worker", "kick", "flushed", 0)
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
	// No worker can be using a cached extent runtime now; close them so their
	// JuiceFS sessions end with the worker.
	m.extentRuntimes.closeAll()
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
			m.processKicked(ctx, msg.tenantID, msg.tidbCloudOrgID, msg.workMask)
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
func (m *tenantWorkerManager) processKicked(ctx context.Context, tenantID, tidbCloudOrgID string, workMask int) {
	// Do NOT clear kickPending here — a kick coalesced while this message was
	// in the channel must be consumed after acquiring the tenant slot.
	// Snapshot the extent runtime's retirement generation BEFORE proving the
	// tenant alive: a deletion that lands anywhere after this read must not let
	// this kick rebuild a Runtime for a tenant nothing will drain (see
	// drainExtentMaintenance). Reading it after kickRef could adopt a generation
	// bumped by a forget in between, and reading it later still — after the
	// semantic drain below, which can take seconds — would miss even more. A
	// stale value can only cause a refusal, never a build, so the earliest read
	// strictly dominates.
	retireGen := m.extentRuntimes.retirementGeneration(tenantID)
	ref, ok := m.kickRef(ctx, tenantID)
	if !ok {
		m.clearKickPending(tenantID)
		return
	}
	if !m.tryClaimTenantSlot(ref.id) {
		// Another worker holds the slot; leave kickPending for that worker to consume.
		return
	}
	// Merge any work that coalesced while this kick was queued.
	pending := m.takePendingKick(tenantID)
	workMask |= pending.workMask
	tidbCloudOrgID = mergeKickOrgID(tidbCloudOrgID, pending.tidbCloudOrgID)
	target, err := m.targetForRef(ctx, ref, tidbCloudOrgID)
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

	// Extent data-plane maintenance runs on its own work bit, on every tenant
	// kick, BEFORE the file_gc drain: folding it into drainFileGC's
	// "file_gc queue is empty" branch starved it whenever file_gc had a
	// backlog. The bit is a re-kick token: it never re-enters the semantic or
	// file_gc drains.
	//
	// retireGen was snapshotted before this kick proved the tenant alive; the
	// extent pool refuses to build a Runtime for a tenant retired since.
	if m.drainExtentMaintenance(ctx, target, retireGen) {
		reKickMask |= WorkExtent
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
	pending = m.takePendingKick(tenantID)
	reKick := pending.workMask | reKickMask
	if reKick != 0 {
		m.KickWithOrg(tenantID, mergeKickOrgID(target.metricOrgID(), pending.tidbCloudOrgID), reKick)
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

// drainExtentMaintenance runs the extent data-plane jobs for one tenant: the
// jfs_delfile safety net, block GC (object deletes for superseded slices), the
// stale-session sweep, and one compact-fallback task. It is the extent work
// type's drain, with the same shape as drainFileGC, and it runs on every
// tenant kick so a busy file_gc queue cannot starve it.
//
// Returns true when a job completed a full batch, i.e. more work is likely
// waiting. Jobs that make no progress (a failing object store, a no-op
// compaction, a compact task that is still leased) report false, so a stuck
// task can never make the worker spin on its own re-kick.
func (m *tenantWorkerManager) drainExtentMaintenance(ctx context.Context, target *tenantTarget, retirementGen uint64) (more bool) {
	if ctx.Err() != nil || m == nil || m.extentRuntimes == nil || target == nil || target.store == nil || target.backend == nil {
		return false
	}
	s3 := target.backend.S3()
	if s3 == nil {
		return false
	}
	drained := runExtentFileGC(ctx, target.store)
	swept := runExtentSessionSweep(ctx, target.store)
	deleted := runExtentBlockGC(ctx, target.store, s3, target.tenantID)
	// Report the extent data plane's bytes to the central quota counters. The
	// extent write path cannot do it per write (that is one more control-plane
	// INSERT on the hot path), and the classic file mutations never see these
	// files because their size lives in jfs_node; this pass is the one place
	// that already has the tenant store and the backend together.
	//
	// Only the shard owner reports: the report is a read-modify-write of the
	// tenant's marker plus an additive central mutation, and every pod runs this
	// maintenance pass for tenants it serves (the worker is not leader-gated),
	// so two reporters would each add the same delta. The other jobs in this pass
	// need no owner either: the compact fallback below is claimed through a lease
	// in the database, and the drains above are idempotent by delete/mark, so a
	// second pod repeats work but cannot corrupt it.
	if m.ownsTenantWork(target.tenantID) {
		reportExtentQuotaUsage(ctx, target)
	}
	if ctx.Err() != nil {
		return false
	}
	// The compactor reuses one runtime per tenant; building a runtime is not
	// itself completed work, so it does not feed the re-kick decision.
	if err := m.extentRuntimes.withRuntime(target.tenantID, retirementGen, target.store, s3, func(rt *extent.Runtime) error {
		runExtentCompactFallback(ctx, target.store, target.tenantID, rt)
		return nil
	}); err != nil {
		logger.Warn(ctx, "tenant_worker_extent_runtime_failed",
			zap.String("tenant_id", target.tenantID), zap.Error(err))
	}
	return drained >= extentFileGCBatchSize ||
		swept >= extentSessionSweepBatch ||
		deleted >= extentBlockGCBatchSize
}

// extentQuotaLeaseReleaseTimeout bounds the post-report lease release, which runs
// on a context detached from the pass so a cancelled kick still releases it.
const extentQuotaLeaseReleaseTimeout = 5 * time.Second

// reportExtentQuotaUsage pushes the extent data plane's byte delta into the
// tenant's central quota counters. The tenant-side marker advances only after
// the report has landed, so a failed report is retried with the same delta on
// the next pass instead of being lost.
//
// The reverse window — a report that lands and a marker write that fails —
// re-reports one delta, and since the counters are relative increments nothing
// recomputes, that over-count persists. It is still the safer half of the trade
// (the alternative silently under-counts for ever). Concurrent reporters are
// excluded by the tenant-DB lease around this function, which expires so a dead
// reporter only delays the next pass.
func reportExtentQuotaUsage(ctx context.Context, target *tenantTarget) {
	if target == nil || target.store == nil || target.backend == nil {
		return
	}
	// Cheap filter first: most kicks have no growth to report, and a claim plus a
	// release is two write transactions on a row this pass would not touch. Only
	// a non-zero delta is worth serializing.
	if _, delta, perr := target.store.PeekExtentUsageDelta(ctx); perr != nil {
		logger.Warn(ctx, "tenant_worker_extent_quota_delta_failed",
			zap.String("tenant_id", target.tenantID), zap.Error(perr))
		return
	} else if delta == 0 {
		return
	}
	// One reporter per tenant, cluster-wide. The shard gate above already means
	// only the owner reports, but a ring transition can hand the tenant over
	// while a report is in flight and both pods would then push the same delta;
	// the lease closes that window and expires, so a crashed reporter cannot
	// block reporting for ever.
	token, ok, err := target.store.ClaimExtentUsageReport(ctx)
	if err != nil {
		logger.Warn(ctx, "tenant_worker_extent_quota_lease_failed",
			zap.String("tenant_id", target.tenantID), zap.Error(err))
		return
	}
	if !ok {
		return
	}
	defer func() {
		// Detached: a cancelled pass must still hand the lease back, or the next
		// report waits for the TTL. A failure here is only a delay, so it is
		// logged rather than propagated.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), extentQuotaLeaseReleaseTimeout)
		defer cancel()
		if rerr := target.store.ReleaseExtentUsageReport(releaseCtx, token); rerr != nil {
			logger.Warn(releaseCtx, "tenant_worker_extent_quota_lease_release_failed",
				zap.String("tenant_id", target.tenantID), zap.Error(rerr))
		}
	}()
	// The delta the report uses is the one read under the lease: another reporter
	// may have covered the growth this pass saw before the lease was granted.
	total, delta, err := target.store.PeekExtentUsageDelta(ctx)
	if err != nil {
		logger.Warn(ctx, "tenant_worker_extent_quota_delta_failed",
			zap.String("tenant_id", target.tenantID), zap.Error(err))
		return
	}
	if delta == 0 {
		return
	}
	if err := target.backend.ReportExtentUsageDelta(ctx, delta); err != nil {
		logger.Warn(ctx, "tenant_worker_extent_quota_report_failed",
			zap.String("tenant_id", target.tenantID), zap.Int64("delta", delta), zap.Error(err))
		return
	}
	if err := target.store.CommitExtentUsageDelta(ctx, total, delta); err != nil {
		logger.Warn(ctx, "tenant_worker_extent_quota_commit_failed",
			zap.String("tenant_id", target.tenantID), zap.Int64("total", total), zap.Error(err))
	}
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
			metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "recover", "error", time.Since(start))
			logger.Warn(ctx, "tenant_worker_recover_failed",
				zap.String("tenant_id", target.tenantID), zap.Error(err))
		}
		return
	}
	if recovered > 0 {
		metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "recover", "ok", time.Since(start))
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

	// fs_events cleanup: prune rows older than the configured retention.
	// Deletes are batched (fsEventsSweepBatchSize rows per statement, capped at
	// fsEventsSweepMaxBatches per sweep) so a hot tenant's first sweep after a
	// long over-retention period cannot hit TiDB transaction size limits.
	// hasMore means the cap was hit with leftover rows; they drain on the next
	// maintenance cycle (or via the lazy write-path sweep).
	if count, err := target.store.CountFSEvents(ctx); err != nil {
		metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "event_bus", "fs_events_count", metrics.ResultForError(err), 0)
	} else if count > 0 {
		metrics.RecordFSEventsRowsWithOrg(target.tenantID, target.metricOrgID(), count)
	} else {
		metrics.DeleteFSEventsRowsWithOrg(target.tenantID, target.metricOrgID())
	}
	if n, hasMore, err := target.store.DeleteFSEventsBefore(ctx, now.Add(-m.opts.FSEventsRetention), fsEventsSweepBatchSize, fsEventsSweepMaxBatches); err != nil {
		metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "event_bus", "retention_sweep", metrics.ResultForError(err), 0)
		if ctx.Err() == nil {
			logger.Warn(ctx, "tenant_worker_fs_events_cleanup_failed",
				zap.String("tenant_id", target.tenantID), zap.Error(err))
		}
	} else {
		metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "event_bus", "retention_sweep", "ok", 0)
		metrics.RecordFSEventsPruned(n)
		if hasMore {
			logger.Info(ctx, "tenant_worker_fs_events_cleanup_has_more",
				zap.String("tenant_id", target.tenantID),
				zap.Int64("deleted", n))
		}
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
	m.recordSemanticWorkerObservation(target.tenantID, target.metricOrgID(), obs, now)
}

func (m *tenantWorkerManager) recordSemanticWorkerObservation(tenantID, tidbCloudOrgID string, obs *datastore.SemanticTaskObservation, now time.Time) {
	tidbCloudOrgID = normalizeTenantMetricTiDBCloudOrgID(tidbCloudOrgID)
	m.mu.Lock()
	previousOrgID := m.semanticMetricOrg[tenantID]
	if m.semanticMetricOrg == nil {
		m.semanticMetricOrg = make(map[string]string)
	}
	m.semanticMetricOrg[tenantID] = tidbCloudOrgID
	m.mu.Unlock()
	if previousOrgID != "" && previousOrgID != tidbCloudOrgID {
		metrics.DeleteTenantGaugeWithOrg(tenantID, previousOrgID, "semantic_worker", "dead_lettered")
		metrics.DeleteTenantGaugeWithOrg(tenantID, previousOrgID, "semantic_worker", "queue_lag_seconds")
	}
	recordSemanticWorkerObservation(tenantID, tidbCloudOrgID, obs, now)
}

func recordSemanticWorkerObservation(tenantID, tidbCloudOrgID string, obs *datastore.SemanticTaskObservation, now time.Time) {
	if obs == nil {
		return
	}
	if obs.DeadLettered > 0 {
		metrics.RecordTenantGaugeWithOrg(tenantID, tidbCloudOrgID, "semantic_worker", "dead_lettered", float64(obs.DeadLettered))
	} else {
		metrics.DeleteTenantGaugeWithOrg(tenantID, tidbCloudOrgID, "semantic_worker", "dead_lettered")
	}
	tenantLag := float64(0)
	if obs.OldestClaimableAvailableAt != nil {
		tenantLag = now.UTC().Sub(obs.OldestClaimableAvailableAt.UTC()).Seconds()
		if tenantLag < 0 {
			tenantLag = 0
		}
	}
	if tenantLag > 0 {
		metrics.RecordTenantGaugeWithOrg(tenantID, tidbCloudOrgID, "semantic_worker", "queue_lag_seconds", tenantLag)
	} else {
		metrics.DeleteTenantGaugeWithOrg(tenantID, tidbCloudOrgID, "semantic_worker", "queue_lag_seconds")
	}
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

func (m *tenantWorkerManager) targetForRef(ctx context.Context, ref semanticTenantRef, tidbCloudOrgID string) (*tenantTarget, error) {
	if ref.id == tenantLocalID {
		if m.fallback == nil {
			return nil, fmt.Errorf("backend missing for %s", ref.id)
		}
		return &tenantTarget{
			tenantID:         ref.id,
			tidbCloudOrgID:   m.fallback.TiDBCloudOrgID(),
			backend:          m.fallback,
			store:            m.fallback.Store(),
			allowedTaskTypes: m.taskTypesForTarget(m.fallback),
			release:          func() {},
		}, nil
	}
	if ref.tenant == nil {
		return nil, fmt.Errorf("tenant metadata missing for %s", ref.id)
	}
	acquireStart := time.Now()
	b, release, err := m.pool.Acquire(ctx, ref.tenant)
	if err != nil {
		// Kick-driven acquire failure: a kick arrived but the tenant TiDB could
		// not be opened. Record so the major alert can detect sustained worker
		// acquire errors (kicks not reaching the tenant DB).
		metrics.RecordTenantOperationWithOrg(ref.tenant.ID, tidbCloudOrgID, "user_db_access", "tenant_worker_acquire", metrics.ResultForError(err), time.Since(acquireStart))
		return nil, fmt.Errorf("acquire tenant backend: %w", err)
	}
	if b == nil {
		release()
		metrics.RecordTenantOperationWithOrg(ref.tenant.ID, tidbCloudOrgID, "user_db_access", "tenant_worker_acquire", "error", time.Since(acquireStart))
		return nil, fmt.Errorf("backend missing for %s", ref.id)
	}
	// Kick-driven acquire success: the tenant TiDB is now open for this kick.
	// The rate follows write traffic (kicks are produced by writes). A spike
	// uncorrelated with writes would suggest a scan path regressing.
	metrics.RecordTenantOperationWithOrg(ref.tenant.ID, b.TiDBCloudOrgID(), "user_db_access", "tenant_worker_acquire", "ok", time.Since(acquireStart))
	return &tenantTarget{
		tenantID:         ref.id,
		tidbCloudOrgID:   b.TiDBCloudOrgID(),
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
		metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "claim", "error", time.Since(claimStart))
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
	metrics.RecordTenantOperationWithOrg(target.tenantID, target.metricOrgID(), "semantic_worker", "claim", "ok", time.Since(claimStart))
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
	if m.fallback.SupportsAsyncVideoExtract() {
		out = append(out, semantic.TaskTypeVideoExtractVisual)
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
	if b.SupportsAsyncVideoExtract() {
		out = append(out, semantic.TaskTypeVideoExtractVisual)
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
