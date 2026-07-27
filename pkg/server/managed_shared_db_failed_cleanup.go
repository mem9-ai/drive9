package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
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
		metrics.RecordSharedDBPoolCleanup("list_pools", metrics.ResultForError(err))
		logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_list_failed", zap.Error(err))
		return
	}
	if len(rows) == 0 && s.managedSharedDBFailedCleanupCursor > 0 {
		s.managedSharedDBFailedCleanupCursor = 0
		rows, err = s.meta.ListSharedDBsByStatusAfter(ctx, meta.SharedDBStatusFailed, 0, batchSize)
		if err != nil {
			metrics.RecordSharedDBPoolCleanup("list_pools", metrics.ResultForError(err))
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
			metrics.RecordSharedDBPoolCleanup("list_tenants", metrics.ResultForError(listErr))
			logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_tenant_list_failed",
				zap.Int64("db_pool_id", row.ID), zap.Error(listErr))
			continue
		}
		for i := range candidates {
			if _, cleanupErr := s.cleanupFailedSharedTenant(ctx, row.TiDBCloudOrganizationID, cutoff, &candidates[i]); cleanupErr != nil {
				metrics.RecordSharedDBPoolCleanup("tenant", metrics.ResultForError(cleanupErr))
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_tenant_failed",
					zap.Int64("db_pool_id", row.ID), zap.String("tenant_id", candidates[i].ID), zap.Error(cleanupErr))
			} else {
				metrics.RecordSharedDBPoolCleanup("tenant", "ok")
			}
		}
		current, loadErr := s.meta.GetSharedDB(ctx, row.ID)
		if loadErr != nil {
			metrics.RecordSharedDBPoolCleanup("reload", metrics.ResultForError(loadErr))
			if !errors.Is(loadErr, meta.ErrNotFound) {
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_reload_failed", zap.Int64("db_pool_id", row.ID), zap.Error(loadErr))
			}
			continue
		}
		if current.Status != meta.SharedDBStatusFailed {
			continue
		}
		if current.TenantCount != 0 {
			repaired, repairErr := s.meta.RepairFailedSharedDBPoolTenantCountIfEmpty(ctx, row.ID)
			if repairErr != nil {
				metrics.RecordSharedDBPoolCleanup("repair_count", metrics.ResultForError(repairErr))
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_count_repair_failed",
					zap.Int64("db_pool_id", row.ID), zap.Int("tenant_count", current.TenantCount), zap.Error(repairErr))
				continue
			}
			if !repaired {
				continue
			}
			metrics.RecordSharedDBPoolCleanup("repair_count", "ok")
			logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_count_repaired",
				zap.Int64("db_pool_id", row.ID), zap.Int("previous_tenant_count", current.TenantCount))
			current, loadErr = s.meta.GetSharedDB(ctx, row.ID)
			if loadErr != nil || current.Status != meta.SharedDBStatusFailed || current.TenantCount != 0 {
				if loadErr != nil && !errors.Is(loadErr, meta.ErrNotFound) {
					logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_reload_failed", zap.Int64("db_pool_id", row.ID), zap.Error(loadErr))
				}
				continue
			}
		}
		persistedClusterID := strings.TrimSpace(current.ClusterID)
		cred, credErr := s.sharedDBCloudCredentials()
		if credErr != nil {
			metrics.RecordSharedDBPoolCleanup("credentials", metrics.ResultForError(credErr))
			logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_credentials_failed", zap.Int64("db_pool_id", row.ID), zap.Error(credErr))
			continue
		}
		clusterIDs := make([]string, 0, 2)
		if lister, ok := s.provisioner.(tenant.SharedDBPoolLister); ok {
			discovered, discoverErr := lister.ListSharedDBPoolsWithCredentials(ctx, row.ID, row.UUID, cred)
			if discoverErr != nil {
				metrics.RecordSharedDBPoolCleanup("discovery", metrics.ResultForError(discoverErr))
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_discovery_failed", zap.Int64("db_pool_id", row.ID), zap.Error(discoverErr))
				continue
			}
			for _, info := range discovered {
				if info != nil && strings.TrimSpace(info.ClusterID) != "" {
					clusterIDs = append(clusterIDs, strings.TrimSpace(info.ClusterID))
				}
			}
		} else if persistedClusterID == "" {
			loader, ok := s.provisioner.(managedSharedDBPoolLoader)
			if !ok {
				metrics.RecordSharedDBPoolCleanup("discovery", "unsupported")
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_discovery_unsupported", zap.Int64("db_pool_id", row.ID))
				continue
			}
			discovered, discoverErr := loader.LoadSharedDBPoolWithCredentials(ctx, row.ID, row.UUID, "", cred)
			if discoverErr != nil {
				metrics.RecordSharedDBPoolCleanup("discovery", metrics.ResultForError(discoverErr))
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_discovery_failed", zap.Int64("db_pool_id", row.ID), zap.Error(discoverErr))
				continue
			}
			if discovered != nil && strings.TrimSpace(discovered.ClusterID) != "" {
				clusterIDs = append(clusterIDs, strings.TrimSpace(discovered.ClusterID))
			}
		}
		if persistedClusterID != "" {
			clusterIDs = append(clusterIDs, persistedClusterID)
		}
		seenClusterIDs := make(map[string]struct{}, len(clusterIDs))
		uniqueClusterIDs := clusterIDs[:0]
		for _, clusterID := range clusterIDs {
			if _, exists := seenClusterIDs[clusterID]; exists {
				continue
			}
			seenClusterIDs[clusterID] = struct{}{}
			uniqueClusterIDs = append(uniqueClusterIDs, clusterID)
		}
		if len(uniqueClusterIDs) > 0 {
			deprovisioner, ok := s.provisioner.(tenant.CredentialDeprovisioner)
			if !ok {
				metrics.RecordSharedDBPoolCleanup("deprovision", "unsupported")
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_deprovision_unsupported", zap.Int64("db_pool_id", row.ID))
				continue
			}
			var deprovisionErr error
			for _, clusterID := range uniqueClusterIDs {
				if err := deprovisioner.DeprovisionWithCredentials(ctx, &tenant.ClusterInfo{ClusterID: clusterID}, cred); err != nil {
					deprovisionErr = errors.Join(deprovisionErr, fmt.Errorf("cluster %s: %w", clusterID, err))
				}
			}
			if deprovisionErr != nil {
				metrics.RecordSharedDBPoolCleanup("deprovision", metrics.ResultForError(deprovisionErr))
				logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_deprovision_failed", zap.Int64("db_pool_id", row.ID), zap.Error(deprovisionErr))
				continue
			}
			metrics.RecordSharedDBPoolCleanup("deprovision", "ok")
			if persistedClusterID != "" {
				cleared, clearErr := s.meta.ClearFailedSharedDBPoolClusterID(ctx, row.ID, persistedClusterID)
				if clearErr != nil || !cleared {
					result := metrics.ResultForError(clearErr)
					if clearErr == nil {
						result = "not_cleared"
					}
					metrics.RecordSharedDBPoolCleanup("clear_cluster", result)
					logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_clear_cluster_failed",
						zap.Int64("db_pool_id", row.ID), zap.Bool("cleared", cleared), zap.Error(clearErr))
					continue
				}
				metrics.RecordSharedDBPoolCleanup("clear_cluster", "ok")
			}
		}
		deleted, deleteErr := s.meta.DeleteFailedSharedDBPoolIfEmpty(ctx, row.ID)
		if deleteErr != nil || !deleted {
			result := metrics.ResultForError(deleteErr)
			if deleteErr == nil {
				result = "not_deleted"
			}
			metrics.RecordSharedDBPoolCleanup("delete_pool", result)
			logger.Warn(ctx, "managed_shared_db_pool_failed_cleanup_delete_failed",
				zap.Int64("db_pool_id", row.ID), zap.Bool("deleted", deleted), zap.Error(deleteErr))
			continue
		}
		metrics.RecordSharedDBPoolCleanup("delete_pool", "ok")
		logger.Info(ctx, "managed_shared_db_pool_failed_cleanup_done", zap.Int64("db_pool_id", row.ID), zap.String("db_pool_uuid", row.UUID))
	}
}
