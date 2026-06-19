package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
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
		w.processOnce(ctx)
		select {
		case <-ctx.Done():
			logger.Info(ctx, "object_gc_worker_stopped")
			return
		case <-ticker.C:
		}
	}
}

func (w *objectGCWorker) processOnce(ctx context.Context) {
	candidates, err := w.meta.ListDueObjectGCCandidates(ctx, time.Now().UTC(), w.batchSize)
	if err != nil {
		logger.Warn(ctx, "object_gc_list_due_failed", zap.Error(err))
		return
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

	b, release, err := w.pool.Acquire(ctx, owner)
	if err != nil {
		return err
	}
	defer release()

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
