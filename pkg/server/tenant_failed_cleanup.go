package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

var (
	tenantFailedCleanupRetryDelay      = 15 * time.Minute
	tenantFailedCleanupRestoreTimeout  = 5 * time.Second
	tenantFailedCleanupMinInterval     = time.Minute
	tenantFailedCleanupNativeBatchSize = 1
	tenantFailedCleanupSharedBatchSize = 10
)

const (
	tenantFailedCleanupNativeOperation = "cleanup_failed_tidb_cloud_native"
	tenantFailedCleanupSharedOperation = "cleanup_failed_tidb_cloud_native_shared"
)

type failedNativeTenantCleanupLoader func(context.Context, string, time.Time, int) ([]meta.TenantWithTiDBCloudOrgBinding, error)
type failedSharedTenantCleanupLoader func(context.Context, string, time.Time, int) ([]meta.Tenant, error)
type failedTenantAPIKeyRevoker func(context.Context, string) error
type failedTenantStatusUpdater func(context.Context, string, meta.TenantStatus, meta.TenantStatus) (bool, error)
type tenantFailedCleanupRunner func(context.Context, string, tenant.CredentialProvisionRequest)

type tenantFailedCleanupJobState struct {
	mu          sync.Mutex
	active      bool
	lastStarted time.Time
}

func (s *Server) startTenantFailedCleanupAsync(
	ctx context.Context,
	organizationID string,
	cred tenant.CredentialProvisionRequest,
) {
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return
	}
	value, _ := s.tenantFailedCleanupJobs.LoadOrStore(organizationID, &tenantFailedCleanupJobState{})
	state := value.(*tenantFailedCleanupJobState)
	now := time.Now().UTC()

	state.mu.Lock()
	if state.active {
		state.mu.Unlock()
		logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_per_pod_skipped",
			"organization_id", organizationID,
			"reason", "active")...)
		return
	}
	if tenantFailedCleanupMinInterval > 0 && !state.lastStarted.IsZero() &&
		now.Sub(state.lastStarted) < tenantFailedCleanupMinInterval {
		lastStarted := state.lastStarted
		state.mu.Unlock()
		logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_per_pod_skipped",
			"organization_id", organizationID,
			"reason", "cooldown",
			"last_started", lastStarted,
			"min_interval", tenantFailedCleanupMinInterval)...)
		return
	}
	previousStart := state.lastStarted
	state.active = true
	state.lastStarted = now
	state.mu.Unlock()

	started := s.startServerWorker(ctx, func(workerCtx context.Context) {
		defer func() {
			state.mu.Lock()
			state.active = false
			state.mu.Unlock()
		}()
		runner := s.tenantFailedCleanupRunner
		if runner == nil {
			runner = s.cleanupFailedOrganizationTenants
		}
		runner(workerCtx, organizationID, cred)
	})
	if started {
		return
	}
	state.mu.Lock()
	state.active = false
	state.lastStarted = previousStart
	state.mu.Unlock()
}

// cleanupFailedOrganizationTenants synchronously makes one bounded cleanup
// pass for an organization. Native and shared candidates are independent: a
// list or candidate failure in either provider never prevents the other pass.
func (s *Server) cleanupFailedOrganizationTenants(ctx context.Context, organizationID string, cred tenant.CredentialProvisionRequest) {
	if s.meta == nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_failed",
			"organization_id", strings.TrimSpace(organizationID),
			"stage", "meta_unavailable")...)
		return
	}
	s.cleanupFailedOrganizationTenantsWithLoaders(
		ctx, organizationID, cred,
		s.meta.ListFailedNativeTenantCleanupCandidates,
		s.meta.ListFailedSharedTenantCleanupCandidates,
	)
}

func (s *Server) cleanupFailedOrganizationTenantsWithLoaders(
	ctx context.Context,
	organizationID string,
	cred tenant.CredentialProvisionRequest,
	nativeLoader failedNativeTenantCleanupLoader,
	sharedLoader failedSharedTenantCleanupLoader,
) {
	organizationID = strings.TrimSpace(organizationID)
	cutoff := time.Now().UTC().Add(-tenantFailedCleanupRetryDelay)
	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_started",
		"organization_id", organizationID,
		"cutoff", cutoff,
		"native_batch_size", tenantFailedCleanupNativeBatchSize,
		"shared_batch_size", tenantFailedCleanupSharedBatchSize)...)
	nativeCandidates, err := nativeLoader(
		ctx, organizationID, cutoff, tenantFailedCleanupNativeBatchSize)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_list_failed",
			"organization_id", organizationID,
			"provider", tenant.ProviderTiDBCloudNative,
			"stage", "list",
			"error", err)...)
	} else {
		for i := range nativeCandidates {
			candidate := &nativeCandidates[i]
			started := time.Now()
			owned, cleanupErr := s.cleanupFailedNativeTenant(ctx, organizationID, cutoff, cred, candidate)
			if owned {
				metrics.RecordOperation(adminTenantPoolMetricsComponent, tenantFailedCleanupNativeOperation,
					metrics.ResultForError(cleanupErr), time.Since(started))
			}
			if cleanupErr != nil {
				logger.Error(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_candidate_failed",
					"organization_id", organizationID,
					"tenant_id", candidate.Tenant.ID,
					"provider", tenant.ProviderTiDBCloudNative,
					"stage", tenantFailedCleanupStage(cleanupErr),
					"owned", owned,
					"error", cleanupErr)...)
			}
		}
	}

	sharedCandidates, err := sharedLoader(
		ctx, organizationID, cutoff, tenantFailedCleanupSharedBatchSize)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_list_failed",
			"organization_id", organizationID,
			"provider", tenant.ProviderTiDBCloudNativeShared,
			"stage", "list",
			"error", err)...)
	} else {
		for i := range sharedCandidates {
			candidate := &sharedCandidates[i]
			started := time.Now()
			owned, cleanupErr := s.cleanupFailedSharedTenant(ctx, organizationID, cutoff, candidate)
			if owned {
				metrics.RecordOperation(adminTenantPoolMetricsComponent, tenantFailedCleanupSharedOperation,
					metrics.ResultForError(cleanupErr), time.Since(started))
			}
			if cleanupErr != nil {
				logger.Error(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_candidate_failed",
					"organization_id", organizationID,
					"tenant_id", candidate.ID,
					"provider", tenant.ProviderTiDBCloudNativeShared,
					"stage", tenantFailedCleanupStage(cleanupErr),
					"owned", owned,
					"error", cleanupErr)...)
			}
		}
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_done",
		"organization_id", organizationID,
		"cutoff", cutoff,
		"native_candidates", len(nativeCandidates),
		"shared_candidates", len(sharedCandidates))...)
}

func tenantFailedCleanupStage(err error) string {
	if err == nil {
		return "done"
	}
	stage, _, ok := strings.Cut(err.Error(), ":")
	if !ok || strings.TrimSpace(stage) == "" {
		return "candidate"
	}
	return strings.TrimSpace(stage)
}

func (s *Server) cleanupFailedNativeTenant(
	ctx context.Context,
	organizationID string,
	cutoff time.Time,
	cred tenant.CredentialProvisionRequest,
	candidate *meta.TenantWithTiDBCloudOrgBinding,
) (owned bool, err error) {
	return s.cleanupFailedNativeTenantWithDependencies(
		ctx, organizationID, cutoff, cred, candidate,
		s.meta.RevokeTenantAPIKeys,
		s.meta.UpdateTenantStatusIf,
	)
}

func (s *Server) cleanupFailedNativeTenantWithDependencies(
	ctx context.Context,
	organizationID string,
	cutoff time.Time,
	cred tenant.CredentialProvisionRequest,
	candidate *meta.TenantWithTiDBCloudOrgBinding,
	revokeTenantAPIKeys failedTenantAPIKeyRevoker,
	restoreStatus failedTenantStatusUpdater,
) (owned bool, err error) {
	if candidate == nil {
		return false, fmt.Errorf("claim: native cleanup candidate is required")
	}
	tenantID := candidate.Tenant.ID
	owned, err = s.meta.MarkFailedNativeTenantDeleting(ctx, tenantID, organizationID, cutoff)
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}
	if !owned {
		return false, nil
	}
	defer func() {
		if err == nil {
			return
		}
		s.restoreFailedTenantAfterCleanupWithUpdater(
			ctx, tenantID, tenant.ProviderTiDBCloudNative, organizationID, err, restoreStatus)
	}()

	t := candidate.Tenant
	if strings.TrimSpace(t.ClusterID) == "" {
		t.ClusterID = strings.TrimSpace(candidate.Binding.ClusterID)
	}
	if strings.TrimSpace(t.ClusterID) != "" {
		if err := s.deprovisionTenantCluster(ctx, &t, cred); err != nil {
			return true, fmt.Errorf("deprovision: %w", err)
		}
	} else {
		logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_native_cloud_skipped",
			"organization_id", organizationID,
			"tenant_id", tenantID,
			"provider", tenant.ProviderTiDBCloudNative,
			"stage", "deprovision",
			"reason", "cluster_id_empty")...)
	}
	if err := revokeTenantAPIKeys(ctx, tenantID); err != nil {
		logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_revoke_keys_failed",
			"organization_id", organizationID,
			"tenant_id", tenantID,
			"provider", tenant.ProviderTiDBCloudNative,
			"stage", "revoke_keys",
			"error", err)...)
	}
	if err := s.meta.MarkTenantDeleted(ctx, tenantID); err != nil {
		return true, fmt.Errorf("finalize_metadata: %w", err)
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_candidate_done",
		"organization_id", organizationID,
		"tenant_id", tenantID,
		"provider", tenant.ProviderTiDBCloudNative,
		"stage", "done")...)
	return true, nil
}

func (s *Server) cleanupFailedSharedTenant(
	ctx context.Context,
	organizationID string,
	cutoff time.Time,
	candidate *meta.Tenant,
) (owned bool, err error) {
	if candidate == nil {
		return false, fmt.Errorf("claim: shared cleanup candidate is required")
	}
	tenantID := candidate.ID
	owned, err = s.meta.MarkFailedSharedTenantDeleting(ctx, tenantID, organizationID, cutoff)
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}
	if !owned {
		return false, nil
	}
	defer func() {
		if err == nil {
			return
		}
		s.restoreFailedTenantAfterCleanup(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared, organizationID, err)
	}()

	if strings.TrimSpace(candidate.StorageNamespaceID) != "" {
		return true, fmt.Errorf("validate_namespace: failed provisioning tenant has storage namespace %q",
			candidate.StorageNamespaceID)
	}
	if s.pool == nil {
		return true, fmt.Errorf("invalidate_backend: tenant pool is unavailable")
	}
	if err := s.pool.InvalidateAndWait(ctx, tenantID); err != nil {
		return true, fmt.Errorf("invalidate_backend: %w", err)
	}
	fsID, err := s.meta.ResolveFsID(ctx, tenantID)
	if err != nil {
		return true, fmt.Errorf("resolve_fs_id: %w", err)
	}
	placement, err := s.meta.GetTenantPlacement(ctx, fsID)
	if err != nil {
		return true, fmt.Errorf("resolve_placement: %w", err)
	}
	dbPool, err := s.meta.GetSharedDB(ctx, placement.DbID)
	if err != nil {
		return true, fmt.Errorf("resolve_shared_db: %w", err)
	}
	connectionReady := dbPool.Status == meta.SharedDBStatusActive &&
		strings.TrimSpace(dbPool.Host) != "" && dbPool.Port > 0 &&
		strings.TrimSpace(dbPool.User) != "" && len(dbPool.PasswordCipher) > 0 &&
		strings.TrimSpace(dbPool.Name) != ""
	if connectionReady {
		if err := s.pool.PurgeSharedTenant(ctx, fsID, placement.DbID); err != nil {
			return true, fmt.Errorf("purge_shared_tenant: %w", err)
		}
	} else {
		logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_shared_purge_skipped",
			"organization_id", organizationID,
			"tenant_id", tenantID,
			"provider", tenant.ProviderTiDBCloudNativeShared,
			"stage", "purge",
			"db_id", placement.DbID,
			"db_status", dbPool.Status,
			"reason", "shared_db_unready")...)
	}
	if err := s.meta.AbortActiveUploadReservations(ctx, tenantID); err != nil {
		logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_abort_reservations_failed",
			"organization_id", organizationID,
			"tenant_id", tenantID,
			"provider", tenant.ProviderTiDBCloudNativeShared,
			"stage", "abort_reservations",
			"error", err)...)
	}
	if err := s.meta.FinalizeSharedTenantDeleteMetadata(
		ctx, tenantID, fsID, placement.DbID, s.sharedDBReopenRatio, true); err != nil {
		return true, fmt.Errorf("finalize_metadata: %w", err)
	}
	if err := s.meta.RevokeTenantAPIKeys(ctx, tenantID); err != nil {
		logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_revoke_keys_failed",
			"organization_id", organizationID,
			"tenant_id", tenantID,
			"provider", tenant.ProviderTiDBCloudNativeShared,
			"stage", "revoke_keys",
			"error", err)...)
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_candidate_done",
		"organization_id", organizationID,
		"tenant_id", tenantID,
		"provider", tenant.ProviderTiDBCloudNativeShared,
		"stage", "done")...)
	return true, nil
}

func (s *Server) restoreFailedTenantAfterCleanup(
	ctx context.Context,
	tenantID, provider, organizationID string,
	cleanupErr error,
) {
	s.restoreFailedTenantAfterCleanupWithUpdater(
		ctx, tenantID, provider, organizationID, cleanupErr, s.meta.UpdateTenantStatusIf)
}

func (s *Server) restoreFailedTenantAfterCleanupWithUpdater(
	ctx context.Context,
	tenantID, provider, organizationID string,
	cleanupErr error,
	updateStatus failedTenantStatusUpdater,
) {
	restoreCtx, cancel := context.WithTimeout(backgroundWithTrace(ctx), tenantFailedCleanupRestoreTimeout)
	defer cancel()
	restored, restoreErr := updateStatus(
		restoreCtx, tenantID, meta.TenantDeleting, meta.TenantFailed)
	if restoreErr != nil {
		logger.Error(restoreCtx, "server_event", eventFields(restoreCtx, "tenant_failed_cleanup_restore_failed",
			"organization_id", organizationID,
			"tenant_id", tenantID,
			"provider", provider,
			"stage", "restore_failed",
			"cleanup_error", cleanupErr,
			"error", restoreErr)...)
		return
	}
	logger.Info(restoreCtx, "server_event", eventFields(restoreCtx, "tenant_failed_cleanup_restored",
		"organization_id", organizationID,
		"tenant_id", tenantID,
		"provider", provider,
		"stage", "restore_failed",
		"restored", restored,
		"cleanup_error", cleanupErr)...)
}
