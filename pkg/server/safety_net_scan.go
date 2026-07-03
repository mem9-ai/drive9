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

// safetyNetScan is a leader-gated, periodic (5min) scan that recovers expired
// leases for warm tenants whose kick may have been lost. It only acquires
// backends that are already cached (AcquireCached — warm only), so it never
// wakes a dormant serverless tenant TiDB. For each warm tenant it recovers
// expired semantic, file_gc, and quota outbox leases so the next kick (or the
// tenant's own worker poll) can re-claim them.
//
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
			// Shard filter: only recover tenants this pod owns. This avoids
			// redundant work across pods (each pod only scans its own shard).
			if !shardFn(t.ID) {
				continue
			}
			// Warm-only: AcquireCached returns the backend only if it's already
			// in the pool cache. A cold tenant (dormant, TiDB scaled to zero) is
			// skipped — we never wake it just for the safety net.
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
				if _, err := store.RecoverExpiredFileGCTasks(ctx, now, safetyNetRecoverLimit); err != nil {
					if ctx.Err() == nil {
						logger.Warn(ctx, "safety_net_scan_file_gc_recover_failed",
							zap.String("tenant_id", t.ID), zap.Error(err))
					}
				} else {
					needKick = true
				}
				// Check for unclaimed queued semantic tasks. If the outbox kick was
				// lost (cursor advanced past the row, pod crashed before processing),
				// queued tasks may never be claimed. ObserveSemanticTasks is a cheap
				// read that returns the queued count. If >0, kick the worker.
				if obs, err := store.ObserveSemanticTasks(ctx, now); err == nil && obs.Queued > 0 {
					needKick = true
				}
				// Check for unclaimed queued file_gc tasks by attempting a probe
				// claim. If found, release it back to queued and kick the worker.
				if _, found, err := store.ClaimFileGCTask(ctx, now, 1*time.Second); err == nil && found {
					// The claim set the task to processing with a 1s lease. It will
					// be recovered by the next safety-net cycle if the worker doesn't
					// process it in time. Kick the worker so it drains it immediately.
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