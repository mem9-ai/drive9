package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"go.uber.org/zap"
)

type managedSharedDBPoolLoader interface {
	LoadSharedDBPoolWithCredentials(context.Context, int64, string, string, tenant.CredentialProvisionRequest) (*tenant.SharedDBPoolInfo, error)
}

// cleanupFailedManagedSharedDBPoolsWithCtx makes one bounded leader-only pass.
// It reuses the existing failed shared-tenant cleanup state machine and does
// not consume metadata, provisioning, or Cloud-create worker slots.
func (s *Server) cleanupFailedManagedSharedDBPoolsWithCtx(ctx context.Context) {
	if s.meta == nil || s.pool == nil || s.provisioner == nil {
		return
	}
	batchSize := s.managedSharedDBFailedCleanupBatchSize
	if batchSize <= 0 {
		batchSize = DefaultManagedSharedDBFailedCleanupBatchSize
	}
	rows, err := s.meta.ListSharedDBsByStatusAfter(ctx, meta.SharedDBStatusFailed, s.managedSharedDBFailedCleanupCursor, batchSize)
	if err != nil {
		logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_list_failed", zap.Error(err))
		return
	}
	if len(rows) == 0 && s.managedSharedDBFailedCleanupCursor > 0 {
		s.managedSharedDBFailedCleanupCursor = 0
		rows, err = s.meta.ListSharedDBsByStatusAfter(ctx, meta.SharedDBStatusFailed, 0, batchSize)
		if err != nil {
			logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_list_failed", zap.Error(err))
			return
		}
	}
	if len(rows) == 0 {
		return
	}
	s.managedSharedDBFailedCleanupCursor = rows[len(rows)-1].ID
	cutoff := time.Now().UTC().Add(-tenantFailedCleanupRetryDelay)
	for _, row := range rows {
		candidates, listErr := s.meta.ListFailedSharedTenantCleanupCandidatesByDB(
			ctx, row.ID, cutoff, tenantFailedCleanupSharedBatchSize)
		if listErr != nil {
			logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_tenant_list_failed",
				zap.Int64("db_pool_id", row.ID), zap.Error(listErr))
			continue
		}
		for i := range candidates {
			if _, cleanupErr := s.cleanupFailedSharedTenant(ctx, row.TiDBCloudOrganizationID, cutoff, &candidates[i]); cleanupErr != nil {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_tenant_failed",
					zap.Int64("db_pool_id", row.ID), zap.String("tenant_id", candidates[i].ID), zap.Error(cleanupErr))
			}
		}
		current, loadErr := s.meta.GetSharedDB(ctx, row.ID)
		if loadErr != nil {
			if !errors.Is(loadErr, meta.ErrNotFound) {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_reload_failed", zap.Int64("db_pool_id", row.ID), zap.Error(loadErr))
			}
			continue
		}
		if current.Status != meta.SharedDBStatusFailed || current.TenantCount != 0 {
			continue
		}
		clusterID := strings.TrimSpace(current.ClusterID)
		clusterIDPersisted := clusterID != ""
		var cred tenant.CredentialProvisionRequest
		if !clusterIDPersisted {
			loader, ok := s.provisioner.(managedSharedDBPoolLoader)
			if !ok {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_discovery_unsupported", zap.Int64("db_pool_id", row.ID))
				continue
			}
			var credErr error
			cred, credErr = s.sharedDBCloudCredentials()
			if credErr != nil {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_credentials_failed", zap.Int64("db_pool_id", row.ID), zap.Error(credErr))
				continue
			}
			discovered, discoverErr := loader.LoadSharedDBPoolWithCredentials(ctx, row.ID, row.UUID, "", cred)
			if discoverErr != nil {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_discovery_failed", zap.Int64("db_pool_id", row.ID), zap.Error(discoverErr))
				continue
			}
			if discovered != nil {
				clusterID = strings.TrimSpace(discovered.ClusterID)
				if clusterID == "" {
					logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_discovery_incomplete", zap.Int64("db_pool_id", row.ID))
					continue
				}
			}
		}
		if clusterID != "" {
			deprovisioner, ok := s.provisioner.(tenant.CredentialDeprovisioner)
			if !ok {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_deprovision_unsupported", zap.Int64("db_pool_id", row.ID))
				continue
			}
			if clusterIDPersisted {
				var credErr error
				cred, credErr = s.sharedDBCloudCredentials()
				if credErr != nil {
					logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_credentials_failed", zap.Int64("db_pool_id", row.ID), zap.Error(credErr))
					continue
				}
			}
			if deprovisionErr := deprovisioner.DeprovisionWithCredentials(ctx, &tenant.ClusterInfo{ClusterID: clusterID}, cred); deprovisionErr != nil {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_deprovision_failed", zap.Int64("db_pool_id", row.ID), zap.Error(deprovisionErr))
				continue
			}
			if clusterIDPersisted {
				cleared, clearErr := s.meta.ClearFailedSharedDBPoolClusterID(ctx, row.ID, clusterID)
				if clearErr != nil || !cleared {
					logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_clear_cluster_failed",
						zap.Int64("db_pool_id", row.ID), zap.Bool("cleared", cleared), zap.Error(clearErr))
					continue
				}
			}
		}
		deleted, deleteErr := s.meta.DeleteFailedSharedDBPoolIfEmpty(ctx, row.ID)
		if deleteErr != nil || !deleted {
			logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_delete_failed",
				zap.Int64("db_pool_id", row.ID), zap.Bool("deleted", deleted), zap.Error(deleteErr))
			continue
		}
		logger.Info(ctx, "managed_shared_db_pool_failed_cleanup_done", zap.Int64("db_pool_id", row.ID), zap.String("db_pool_uuid", row.UUID))
	}
}
