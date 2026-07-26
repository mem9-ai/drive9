package server

import (
	"context"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"go.uber.org/zap"
)

const tenantPoolReconcilePageSize = 100

type tenantPoolReconcileJob struct {
	run func(context.Context)
}

func (s *Server) reconcileStuckManagedSharedDBPoolsWithCtx(ctx context.Context) {
	if s.meta == nil {
		return
	}
	timeout := s.managedSharedDBStuckTimeout
	if timeout <= 0 {
		timeout = DefaultManagedSharedDBStuckTimeout
	}
	cutoff := time.Now().UTC().Add(-timeout)
	for _, status := range []string{meta.SharedDBStatusPending, meta.SharedDBStatusProvisioning} {
		var afterID int64
		for {
			rows, err := s.meta.ListSharedDBsByStatusAfter(ctx, status, afterID, tenantPoolReconcilePageSize)
			if err != nil {
				logger.Warn(ctx, "managed_shared_db_pool_stuck_list_failed", zap.String("status", status), zap.Error(err))
				break
			}
			for _, row := range rows {
				if row.UpdatedAt.After(cutoff) {
					continue
				}
				claimErr := s.meta.WithSharedDBPoolWorkClaims(ctx, []int64{row.ID}, func(claimCtx context.Context, ownedIDs []int64) error {
					if len(ownedIDs) == 0 {
						return nil
					}
					failed, changed, failErr := s.meta.MarkStuckSharedDBPoolFailed(claimCtx, row.ID, status, cutoff)
					if failErr != nil {
						return failErr
					}
					if changed {
						logger.Warn(claimCtx, "managed_shared_db_pool_stuck_failed", zap.Int64("db_pool_id", row.ID),
							zap.String("db_pool_uuid", row.UUID), zap.String("status", status),
							zap.Int("tenant_count", len(failed.TenantIDs)), zap.Duration("stuck_timeout", timeout))
					}
					return nil
				})
				if claimErr != nil {
					logger.Warn(ctx, "managed_shared_db_pool_stuck_fail_failed", zap.Int64("db_pool_id", row.ID), zap.String("status", status), zap.Error(claimErr))
				}
			}
			if len(rows) < tenantPoolReconcilePageSize {
				break
			}
			afterID = rows[len(rows)-1].ID
		}
	}
}

// reconcileSharedTenantPoolsWithCtx scans durable logical pools and only
// enqueues their existing asynchronous refill path. Capacity counts and Cloud
// operations remain outside this leader scanner and outside claim requests.
func (s *Server) reconcileSharedTenantPoolsWithCtx(ctx context.Context) {
	if s.meta == nil || s.defaultTenantProvider != tenant.ProviderTiDBCloudNativeShared {
		return
	}
	afterPoolID := ""
	for {
		pools, err := s.meta.ListTenantPoolsByStatusAfter(ctx, meta.TenantPoolActive, afterPoolID, tenantPoolReconcilePageSize)
		if err != nil {
			logger.Warn(ctx, "tenant_pool_reconcile_list_failed", zap.Error(err))
			return
		}
		for _, pool := range pools {
			if !s.enqueueTenantPoolLeaderReplenishment(ctx, pool, tenant.CredentialProvisionRequest{}) {
				return
			}
		}
		if len(pools) < tenantPoolReconcilePageSize {
			return
		}
		afterPoolID = pools[len(pools)-1].PoolID
	}
}

func (s *Server) enqueueTenantPoolReconcileWork(ctx context.Context, fn func(context.Context)) bool {
	if s.tenantPoolReconcileQueue == nil || fn == nil {
		return false
	}
	select {
	case s.tenantPoolReconcileQueue <- tenantPoolReconcileJob{run: fn}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Server) runTenantPoolReconcileWorker(ctx context.Context) {
	for {
		var job tenantPoolReconcileJob
		select {
		case <-ctx.Done():
			return
		case job = <-s.tenantPoolReconcileQueue:
		}
		if job.run != nil {
			job.run(ctx)
		}
		rest := s.tenantPoolReconcileWorkerRest
		if rest <= 0 {
			rest = DefaultTenantPoolReconcileWorkerRest
		}
		timer := time.NewTimer(rest)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		timer.Stop()
	}
}
