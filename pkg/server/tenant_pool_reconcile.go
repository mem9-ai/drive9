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
				failed, changed, failErr := s.meta.MarkStuckSharedDBPoolFailed(ctx, row.ID, status, cutoff)
				if failErr != nil {
					logger.Warn(ctx, "managed_shared_db_pool_stuck_fail_failed", zap.Int64("db_pool_id", row.ID), zap.String("status", status), zap.Error(failErr))
					continue
				}
				if changed {
					logger.Warn(ctx, "managed_shared_db_pool_stuck_failed", zap.Int64("db_pool_id", row.ID),
						zap.String("db_pool_uuid", row.UUID), zap.String("status", status),
						zap.Int("tenant_count", len(failed.TenantIDs)), zap.Duration("stuck_timeout", timeout))
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
			s.replenishTenantPoolLeaderAsync(ctx, pool, tenant.CredentialProvisionRequest{})
		}
		if len(pools) < tenantPoolReconcilePageSize {
			return
		}
		afterPoolID = pools[len(pools)-1].PoolID
	}
}

func (s *Server) startTenantPoolLeaderReconcileWorker(ctx context.Context, fn func(context.Context)) bool {
	if s.tenantPoolReconcileSlots == nil {
		return false
	}
	select {
	case s.tenantPoolReconcileSlots <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	started := s.startManagedSharedDBWorker(ctx, func(workerCtx context.Context) {
		defer func() { <-s.tenantPoolReconcileSlots }()
		fn(workerCtx)
	})
	if !started {
		<-s.tenantPoolReconcileSlots
	}
	return started
}
