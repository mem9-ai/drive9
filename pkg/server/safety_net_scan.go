package server

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	// safetyNetScanBatchSize is the max active tenants read per keyset page.
	safetyNetScanBatchSize = 128
	// safetyNetRecoverLimit bounds expired lease recovery per tenant per scan.
	safetyNetRecoverLimit = 64
)

// safetyNetScan is a periodic (5min) scan that recovers expired leases and
// discovers unclaimed queued tasks for tenants this pod owns. It runs on every
// pod (not leader-gated) and filters by shard ownership.
//
// **Design constraint — warm-only, never opens cold tenant TiDBs:**
// The scan exclusively uses AcquireCached (warm-only). A cold tenant whose
// TiDB has scaled to zero is never opened by the safety-net. This preserves
// the issue #658 invariant: "no periodic goroutine ever touches idle tenant
// TiDBs." Opening cold tenants would reintroduce the exact periodic-scan
// cost this PR eliminates.
//
// **Lost-kick recovery for cold tenants:**
// If an outbox kick is lost (cursor advanced past the row, pod crash before
// processing) AND the tenant subsequently goes cold (TiDB scales to zero),
// unclaimed queued tasks are not discovered by the safety-net. They remain
// in the tenant TiDB until the next write triggers a fresh outbox row →
// kick → worker opens the TiDB → drains queued tasks. This is a design
// constraint, not a bug:
//  1. Queued tasks are produced by writes. A write always triggers an
//     in-process kick (~0ms) on the same pod. The kick is lost only if the
//     pod crashes between commit and worker processing — an extreme case.
//  2. A cold tenant has no traffic (no writes, no reads). Its queued tasks
//     have no consumer — no one is searching or reading the content that
//     the task would index. The delay is invisible to users.
//  3. The next write (whenever it comes) produces a fresh kick that opens
//     the TiDB and drains all queued tasks. The task rows are durable in
//     the tenant TiDB — they are never lost, only delayed.
//
// This is the safety net, not the primary path: the unified outbox poller
// is the primary delivery mechanism. The scan catches the rare case where
// a kick was lost for a warm tenant (pod crash before cursor flush, outbox
// row pruned before read, etc). For warm tenants it:
//   - recovers expired semantic and file_gc leases
//   - checks for unclaimed queued tasks (ObserveSemanticTasks, CountQueuedFileGCTasks)
//   - kicks the worker if any work is found
func (s *Server) safetyNetScan(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if s.meta == nil || s.pool == nil {
		return
	}
	shardFn := func(string) bool { return true }
	if s.shardResolver != nil {
		shardFn = s.shardResolver.ownsTenantFn()
	}
	now := time.Now().UTC()
	var afterCreatedAt time.Time
	var afterID string
	for {
		if ctx.Err() != nil {
			return
		}
		tenants, err := s.meta.ListTenantsByStatusAfter(ctx, meta.TenantActive, afterCreatedAt, afterID, safetyNetScanBatchSize)
		if err != nil {
			logger.Warn(ctx, "safety_net_scan_list_failed", zap.Error(err))
			return
		}
		if len(tenants) == 0 {
			return
		}
		for _, t := range tenants {
			if ctx.Err() != nil {
				return
			}
			// Shard filter: only process tenants this pod owns.
			if !shardFn(t.ID) {
				continue
			}
			// Warm-only: AcquireCached returns the backend only if it's
			// already in the pool cache. A cold tenant (TiDB scaled to zero)
			// is skipped — the safety-net never opens a cold TiDB. See the
			// design constraint comment above for the lost-kick recovery
			// semantics of cold tenants.
			b, release, ok := s.pool.AcquireCached(&t)
			if !ok {
				continue
			}
			func() {
				defer release()
				store := b.Store()
				if store == nil {
					return
				}
				needKick := false
				// Recover expired semantic task leases.
				if recovered, err := store.RecoverExpiredSemanticTasks(ctx, now, safetyNetRecoverLimit); err != nil {
					if ctx.Err() == nil {
						logger.Warn(ctx, "safety_net_scan_semantic_recover_failed",
							zap.String("tenant_id", t.ID), zap.Error(err))
					}
				} else if recovered > 0 {
					metrics.RecordTenantOperation(t.ID, "semantic_worker", "safety_net_recover", "ok", 0)
					needKick = true
				}
				// Recover expired file_gc leases.
				if recovered, err := store.RecoverExpiredFileGCTasks(ctx, now, safetyNetRecoverLimit); err != nil {
					if ctx.Err() == nil {
						logger.Warn(ctx, "safety_net_scan_file_gc_recover_failed",
							zap.String("tenant_id", t.ID), zap.Error(err))
					}
				} else if recovered > 0 {
					needKick = true
				}
				// Check for unclaimed queued semantic tasks. If the outbox
				// kick was lost (cursor advanced past the row, pod crashed
				// before processing), queued tasks may never be claimed.
				// ObserveSemanticTasks is a read-only query. If queued > 0,
				// kick the worker so it claims and processes them.
				if obs, err := store.ObserveSemanticTasks(ctx, now); err == nil && obs.Queued > 0 {
					needKick = true
				}
				// Check for unclaimed queued file_gc tasks via a read-only
				// count. Do NOT use ClaimFileGCTask here — it would mutate
				// the task to processing and make it invisible to the
				// worker's drainFileGC.
				if n, err := store.CountQueuedFileGCTasks(ctx); err == nil && n > 0 {
					needKick = true
				}
				if needKick && s.tenantWorker != nil {
					s.tenantWorker.Kick(t.ID, WorkSemantic|WorkFileGC)
				}
			}()
		}
		// Advance keyset cursor.
		last := tenants[len(tenants)-1]
		afterCreatedAt = last.CreatedAt.UTC()
		afterID = last.ID
		if len(tenants) < safetyNetScanBatchSize {
			return
		}
	}
}
