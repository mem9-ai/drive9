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
)

const (
	tenantFailedCleanupNativeOperation = "cleanup_failed_tidb_cloud_native"
)

type failedNativeTenantCleanupLoader func(context.Context, string, time.Time, int) ([]meta.TenantWithTiDBCloudOrgBinding, error)
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
// pass for an organization.
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
	)
}

func (s *Server) cleanupFailedOrganizationTenantsWithLoaders(
	ctx context.Context,
	organizationID string,
	cred tenant.CredentialProvisionRequest,
	nativeLoader failedNativeTenantCleanupLoader,
) {
	organizationID = strings.TrimSpace(organizationID)
	cutoff := time.Now().UTC().Add(-tenantFailedCleanupRetryDelay)
	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_started",
		"organization_id", organizationID,
		"cutoff", cutoff,
		"native_batch_size", tenantFailedCleanupNativeBatchSize)...)
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

	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_done",
		"organization_id", organizationID,
		"cutoff", cutoff,
		"native_candidates", len(nativeCandidates))...)
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
	s.clearLocalTenantMetrics(tenantID)
	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_failed_cleanup_candidate_done",
		"organization_id", organizationID,
		"tenant_id", tenantID,
		"provider", tenant.ProviderTiDBCloudNative,
		"stage", "done")...)
	return true, nil
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
