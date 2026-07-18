package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"go.uber.org/zap"
)

const (
	defaultObjectGCBatchSize        = 100
	defaultObjectGCPollInterval     = time.Minute
	defaultObjectGCRetryDelay       = time.Hour
	defaultObjectGCReachableDelay   = 24 * time.Hour
	defaultObjectGCActiveForkDelay  = time.Hour
	defaultObjectGCInactiveOwnerTTL = time.Hour
)

type objectGCWorker struct {
	meta         *meta.Store
	pool         *tenant.Pool
	batchSize    int
	pollInterval time.Duration
	mu           sync.Mutex
	wg           sync.WaitGroup
	stop         context.CancelFunc
}

func newObjectGCWorker(ms *meta.Store, pool *tenant.Pool) *objectGCWorker {
	if ms == nil || pool == nil {
		return nil
	}
	return &objectGCWorker{
		meta:         ms,
		pool:         pool,
		batchSize:    defaultObjectGCBatchSize,
		pollInterval: defaultObjectGCPollInterval,
	}
}

func (w *objectGCWorker) Start(ctx context.Context) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.stop != nil {
		w.mu.Unlock()
		return
	}
	workerCtx, cancel := context.WithCancel(backgroundWithTrace(ctx))
	w.stop = cancel
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		w.loop(workerCtx)
	}()
}

func (w *objectGCWorker) Stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	cancel := w.stop
	w.stop = nil
	w.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	w.wg.Wait()
}

func (w *objectGCWorker) loop(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		if w.processOnce(ctx) {
			// Fatal error (e.g. database closed) — stop the loop to avoid
			// an infinite error-retry cycle that blocks test completion.
			logger.Info(ctx, "object_gc_worker_stopped_fatal")
			return
		}
		select {
		case <-ctx.Done():
			logger.Info(ctx, "object_gc_worker_stopped")
			return
		case <-ticker.C:
		}
	}
}

// processOnce runs one GC poll cycle. Returns true if a fatal error occurred
// (e.g. database closed) and the loop should stop; false to continue polling.
func (w *objectGCWorker) processOnce(ctx context.Context) (fatal bool) {
	candidates, err := w.meta.ListDueObjectGCCandidates(ctx, time.Now().UTC(), w.batchSize)
	if err != nil {
		if strings.Contains(err.Error(), "database is closed") || strings.Contains(err.Error(), "connection refused") {
			logger.Info(ctx, "object_gc_worker_db_closed")
			return true
		}
		logger.Warn(ctx, "object_gc_list_due_failed", zap.Error(err))
		// List failure: the meta-DB read that drives object GC failed. Record
		// so the warning alert can detect sustained listing trouble.
		metrics.RecordOperation("user_db_access", "object_gc_list", "error", 0)
		return false
	}
	for _, candidate := range candidates {
		if err := w.processCandidate(ctx, candidate); err != nil {
			logger.Warn(ctx, "object_gc_candidate_failed",
				zap.String("namespace_id", candidate.NamespaceID),
				zap.String("storage_ref", candidate.StorageRef),
				zap.Error(err))
			_ = w.meta.RetryObjectGCCandidate(ctx, candidate, time.Now().UTC().Add(defaultObjectGCRetryDelay), err.Error())
		}
	}
	return false
}

func (w *objectGCWorker) processCandidate(ctx context.Context, candidate meta.ObjectGCCandidate) error {
	if strings.HasPrefix(candidate.StorageRef, "blobs/") {
		hasFork, err := w.meta.NamespaceHasNonDeletedFork(ctx, candidate.NamespaceID)
		if err != nil {
			return err
		}
		if hasFork {
			return w.meta.PostponeObjectGCCandidate(ctx, candidate, time.Now().UTC().Add(defaultObjectGCActiveForkDelay), "namespace has active fork")
		}
	}

	ns, err := w.meta.GetStorageNamespace(ctx, candidate.NamespaceID)
	if err != nil {
		return err
	}
	owner, err := w.meta.GetTenant(ctx, ns.OwnerTenantID)
	if err != nil {
		return err
	}
	if owner.Status != meta.TenantActive {
		return w.meta.PostponeObjectGCCandidate(ctx, candidate, time.Now().UTC().Add(defaultObjectGCInactiveOwnerTTL), "namespace owner is not active")
	}

	acquireStart := time.Now()
	b, release, err := w.pool.Acquire(ctx, owner)
	if err != nil {
		// Acquire failure: could not open the owner tenant TiDB for this GC
		// candidate. Record so the warning alert can detect a GC path that is
		// churning cold opens or hitting bad connections.
		metrics.RecordTenantOperationWithOrg(owner.ID, tenantMetricTiDBCloudOrgIDFromMeta(ctx, w.meta, owner), "user_db_access", "object_gc_acquire", metrics.ResultForError(err), time.Since(acquireStart))
		return err
	}
	defer release()
	// Acquire success: the owner tenant TiDB is now open for this GC check.
	// Object-GC is leader-gated and opens cold tenants — a spike here is a
	// potential billing-storm contributor, hence the warning alert.
	metrics.RecordTenantOperationWithOrg(owner.ID, b.TiDBCloudOrgID(), "user_db_access", "object_gc_acquire", "ok", time.Since(acquireStart))

	reachable, err := b.HasConfirmedS3StorageRef(ctx, candidate.StorageRefHash, candidate.StorageRef)
	if err != nil {
		return err
	}
	if reachable {
		return w.meta.PostponeObjectGCCandidate(ctx, candidate, time.Now().UTC().Add(defaultObjectGCReachableDelay), "storage ref still reachable")
	}
	if err := b.DeleteS3ObjectForGC(ctx, candidate.StorageRef); err != nil {
		return err
	}
	return w.meta.MarkObjectGCCandidateDeleted(ctx, candidate)
}
