package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/embedding"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/semantic"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"go.uber.org/zap"
)

const (
	defaultSemanticWorkers              = 1
	defaultSemanticPollInterval         = 200 * time.Millisecond
	defaultSemanticLeaseDuration        = 30 * time.Second
	defaultSemanticRecoverInterval      = 5 * time.Second
	defaultSemanticRetryBaseDelay       = 200 * time.Millisecond
	defaultSemanticTenantScanLimit      = 128
	defaultSemanticPerTenantConcurrency = 1
	maxSemanticRetryDelay               = 30 * time.Second
	semanticLocalTenantID               = "local"
)

// SemanticWorkerOptions controls background embed task processing.
type SemanticWorkerOptions struct {
	Workers              int
	PollInterval         time.Duration
	LeaseDuration        time.Duration
	RecoverInterval      time.Duration
	RetryBaseDelay       time.Duration
	TenantScanLimit      int
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

	mu       sync.Mutex
	inflight map[string]int
	rr       int
	stores   map[string]*datastore.Store

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type semanticTenantRef struct {
	id     string
	tenant *meta.Tenant
}

type semanticTarget struct {
	tenantID string
	store    *datastore.Store
	release  func()
}

func newSemanticWorkerManager(fallback *backend.Dat9Backend, metaStore *meta.Store, pool *tenant.Pool, embedder embedding.Client, opts SemanticWorkerOptions) *semanticWorkerManager {
	if embedder == nil {
		return nil
	}
	if fallback == nil && (metaStore == nil || pool == nil) {
		return nil
	}
	opts.normalize()
	return &semanticWorkerManager{
		fallback: fallback,
		meta:     metaStore,
		pool:     pool,
		embedder: embedder,
		opts:     opts,
		inflight: make(map[string]int),
		stores:   make(map[string]*datastore.Store),
	}
}

func (m *semanticWorkerManager) Start(ctx context.Context) {
	if m == nil || m.cancel != nil {
		return
	}
	workerCtx, cancel := context.WithCancel(backgroundWithTrace(ctx))
	m.cancel = cancel
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
	for tenantID, store := range m.stores {
		_ = store.Close()
		delete(m.stores, tenantID)
	}
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

	task, found, err := target.store.ClaimSemanticTask(ctx, time.Now().UTC(), m.opts.LeaseDuration)
	if err != nil {
		logger.Warn(ctx, "semantic_worker_claim_failed",
			zap.String("tenant_id", target.tenantID),
			zap.Error(err))
		m.invalidateTenantStore(target.tenantID)
		return false
	}
	if !found {
		return false
	}
	m.processTask(ctx, target.tenantID, target.store, task)
	return true
}

func (m *semanticWorkerManager) processTask(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task) {
	if task == nil {
		return
	}
	start := time.Now()
	result := "ok"
	defer func() {
		metrics.RecordOperation("semantic_worker", string(task.TaskType), result, time.Since(start))
	}()

	if task.TaskType != semantic.TaskTypeEmbed {
		result = "unsupported"
		m.retryTask(ctx, tenantID, store, task, fmt.Sprintf("unsupported task type %q", task.TaskType))
		return
	}

	file, err := store.GetFile(ctx, task.ResourceID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			result = "obsolete"
			m.ackTask(ctx, tenantID, store, task, "file_not_found")
			return
		}
		result = "get_file_error"
		m.retryTask(ctx, tenantID, store, task, fmt.Sprintf("get file: %v", err))
		return
	}
	if file.Status != datastore.StatusConfirmed || file.Revision != task.ResourceVersion || strings.TrimSpace(file.ContentText) == "" {
		result = "obsolete"
		m.ackTask(ctx, tenantID, store, task, "stale_or_empty")
		return
	}

	vec, err := m.embedder.EmbedText(ctx, file.ContentText)
	if err != nil {
		result = "embed_error"
		m.retryTask(ctx, tenantID, store, task, fmt.Sprintf("embed text: %v", err))
		return
	}
	if len(vec) == 0 {
		result = "embed_empty"
		m.retryTask(ctx, tenantID, store, task, "embed text returned empty vector")
		return
	}

	updated, err := store.UpdateFileEmbedding(ctx, task.ResourceID, task.ResourceVersion, vec)
	if err != nil {
		result = "writeback_error"
		m.retryTask(ctx, tenantID, store, task, fmt.Sprintf("write embedding: %v", err))
		return
	}
	if !updated {
		result = "obsolete"
		m.ackTask(ctx, tenantID, store, task, "conditional_write_miss")
		return
	}
	m.ackTask(ctx, tenantID, store, task, "written")
}

func (m *semanticWorkerManager) ackTask(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task, reason string) {
	if err := store.AckSemanticTask(ctx, task.TaskID, task.Receipt); err != nil {
		logger.Warn(ctx, "semantic_worker_ack_failed",
			zap.String("tenant_id", tenantID),
			zap.String("task_id", task.TaskID),
			zap.String("reason", reason),
			zap.Error(err))
		return
	}
	logger.Info(ctx, "semantic_worker_ack_ok",
		zap.String("tenant_id", tenantID),
		zap.String("task_id", task.TaskID),
		zap.String("resource_id", task.ResourceID),
		zap.Int64("resource_version", task.ResourceVersion),
		zap.String("reason", reason))
}

func (m *semanticWorkerManager) retryTask(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task, message string) {
	retryAt := time.Now().UTC().Add(m.retryDelay(task.AttemptCount))
	if err := store.RetrySemanticTask(ctx, task.TaskID, task.Receipt, retryAt, message); err != nil {
		logger.Warn(ctx, "semantic_worker_retry_failed",
			zap.String("tenant_id", tenantID),
			zap.String("task_id", task.TaskID),
			zap.String("resource_id", task.ResourceID),
			zap.Int64("resource_version", task.ResourceVersion),
			zap.Error(err))
		return
	}
	logger.Warn(ctx, "semantic_worker_retry_scheduled",
		zap.String("tenant_id", tenantID),
		zap.String("task_id", task.TaskID),
		zap.String("resource_id", task.ResourceID),
		zap.Int64("resource_version", task.ResourceVersion),
		zap.Int("attempt_count", task.AttemptCount),
		zap.Time("retry_at", retryAt),
		zap.String("message", message))
}

func (m *semanticWorkerManager) retryDelay(attemptCount int) time.Duration {
	if attemptCount < 1 {
		attemptCount = 1
	}
	delay := m.opts.RetryBaseDelay
	for i := 1; i < attemptCount; i++ {
		delay *= 2
		if delay >= maxSemanticRetryDelay {
			return maxSemanticRetryDelay
		}
	}
	if delay > maxSemanticRetryDelay {
		return maxSemanticRetryDelay
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
		store, err := m.storeForRef(ctx, ref)
		if err != nil {
			logger.Warn(ctx, "semantic_worker_open_store_for_recovery_failed",
				zap.String("tenant_id", ref.id),
				zap.Error(err))
			continue
		}
		recovered, err := store.RecoverExpiredSemanticTasks(ctx, time.Now().UTC(), 64)
		if err != nil {
			logger.Warn(ctx, "semantic_worker_recover_failed",
				zap.String("tenant_id", ref.id),
				zap.Error(err))
			m.invalidateTenantStore(ref.id)
			continue
		}
		if recovered > 0 {
			logger.Info(ctx, "semantic_worker_recover_ok",
				zap.String("tenant_id", ref.id),
				zap.Int("recovered", recovered))
		}
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
		store, err := m.storeForRef(ctx, ref)
		if err != nil {
			m.releaseTenantSlot(ref.id)
			continue
		}
		return &semanticTarget{
			tenantID: ref.id,
			store:    store,
			release: func() {
				m.releaseTenantSlot(ref.id)
			},
		}, nil
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
	if m.fallback != nil {
		return []semanticTenantRef{{id: semanticLocalTenantID}}, nil
	}
	tenants, err := m.meta.ListTenantsByStatus(ctx, meta.TenantActive, m.opts.TenantScanLimit)
	if err != nil {
		return nil, err
	}
	refs := make([]semanticTenantRef, 0, len(tenants))
	for i := range tenants {
		t := tenants[i]
		refs = append(refs, semanticTenantRef{id: t.ID, tenant: &t})
	}
	return refs, nil
}

func (m *semanticWorkerManager) storeForRef(ctx context.Context, ref semanticTenantRef) (*datastore.Store, error) {
	if ref.id == semanticLocalTenantID {
		return m.fallback.Store(), nil
	}
	m.mu.Lock()
	if store := m.stores[ref.id]; store != nil {
		m.mu.Unlock()
		return store, nil
	}
	m.mu.Unlock()

	if ref.tenant == nil {
		return nil, fmt.Errorf("tenant metadata missing for %s", ref.id)
	}
	pass, err := m.pool.Decrypt(ctx, ref.tenant.DBPasswordCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt tenant password: %w", err)
	}
	store, err := datastore.Open(tenantDSN(ref.tenant.DBUser, string(pass), ref.tenant.DBHost, ref.tenant.DBPort, ref.tenant.DBName, ref.tenant.DBTLS))
	if err != nil {
		return nil, fmt.Errorf("open tenant store: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.stores[ref.id]; existing != nil {
		_ = store.Close()
		return existing, nil
	}
	m.stores[ref.id] = store
	return store, nil
}

func (m *semanticWorkerManager) invalidateTenantStore(tenantID string) {
	if tenantID == semanticLocalTenantID {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	store := m.stores[tenantID]
	if store == nil {
		return
	}
	_ = store.Close()
	delete(m.stores, tenantID)
}
