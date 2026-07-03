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
	// safetyNetColdOpenLimit bounds the number of cold (not cached) tenants
	// that the safety-net will open per scan cycle. This prevents waking too
	// many serverless TiDBs in one sweep while still catching stranded queued
	// tasks for idle tenants whose outbox kick was lost.
	safetyNetColdOpenLimit = 10
)

// safetyNetScan is a periodic (5min) scan that recovers expired leases and
// discovers unclaimed queued tasks for tenants this pod owns. It runs on every
// pod (not leader-gated) and filters by shard ownership.
//
// For warm tenants (already in the pool cache via AcquireCached), it:
//   - recovers expired semantic and file_gc leases
//   - checks for unclaimed queued tasks (ObserveSemanticTasks, CountQueuedFileGCTasks)
//   - kicks the worker if any work is found
//
// For cold tenants (not cached), it opens up to safetyNetColdOpenLimit per
// cycle via pool.Acquire to check for queued tasks. This is the durable
// fallback for the lost-kick scenario: if the outbox kick was lost and the
// tenant has no subsequent writes, the safety-net discovers the stranded
// queued tasks within 5 minutes. The cold-open limit prevents waking too many
// serverless TiDBs in a single sweep.
// This is the safety net, not the primary path: the unified outbox poller is
// the primary delivery mechanism. The scan catches the rare case where a kick
// was lost (pod crash before cursor flush, outbox row pruned before read, etc).
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
	coldOpens := 0
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
			// Try warm-only first (AcquireCached — no cold open).
			b, release, ok := s.pool.AcquireCached(&t)
			if !ok {
				// Cold tenant: not in pool cache. Open it via pool.Acquire
				// to check for stranded queued tasks, but limit the number
				// of cold opens per scan cycle to avoid waking too many
				// serverless TiDBs at once.
				if coldOpens >= safetyNetColdOpenLimit {
					continue
				}
				b, release, err = s.pool.Acquire(ctx, &t)
				if err != nil {
					if ctx.Err() == nil {
						logger.Warn(ctx, "safety_net_scan_cold_open_failed",
							zap.String("tenant_id", t.ID), zap.Error(err))
					}
					continue
				}
				coldOpens++
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
				if _, err := store.RecoverExpiredFileGCTasks(ctx, now, safetyNetRecoverLimit); err != nil {
					if ctx.Err() == nil {
						logger.Warn(ctx, "safety_net_scan_file_gc_recover_failed",
							zap.String("tenant_id", t.ID), zap.Error(err))
					}
				} else {
					needKick = true
				}
				// Check for unclaimed queued semantic tasks. If the outbox kick
				// was lost (cursor advanced past the row, pod crashed before
				// processing), queued tasks may never be claimed.
				// ObserveSemanticTasks is a read-only query. If queued > 0, kick.
				if obs, err := store.ObserveSemanticTasks(ctx, now); err == nil && obs.Queued > 0 {
					needKick = true
				}
				// Check for unclaimed queued file_gc tasks via a read-only count.
				// Do NOT use ClaimFileGCTask here — it would mutate the task to
				// processing and make it invisible to the worker's drainFileGC.
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
