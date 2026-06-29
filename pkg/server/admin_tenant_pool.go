package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type adminTenantPoolRequest struct {
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
	PoolSize   *int   `json:"pool_size,omitempty"`
}

type adminTenantPoolResponse struct {
	PoolID         string `json:"pool_id"`
	OrganizationID string `json:"organization_id,omitempty"`
	PoolSize       int    `json:"pool_size"`
	FreeSize       int    `json:"free_size"`
	Status         string `json:"status"`
}

type adminTenantPoolHTTPError struct {
	status  int
	message string
}

func (e *adminTenantPoolHTTPError) Error() string {
	return e.message
}

func adminTenantPoolError(status int, message string) error {
	return &adminTenantPoolHTTPError{status: status, message: message}
}

func writeAdminTenantPoolError(w http.ResponseWriter, err error) {
	var httpErr *adminTenantPoolHTTPError
	if errors.As(err, &httpErr) {
		errJSON(w, httpErr.status, httpErr.message)
		return
	}
	errJSON(w, http.StatusInternalServerError, err.Error())
}

func (s *Server) adminTenantPoolHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.adminTenantAPIEnabled() {
			errJSON(w, http.StatusNotFound, "admin tenant API not enabled")
			return
		}
		switch r.Method {
		case http.MethodPost:
			s.handleAdminTenantPoolCreate(w, r)
		case http.MethodGet:
			s.handleAdminTenantPoolGet(w, r)
		case http.MethodPatch:
			s.handleAdminTenantPoolUpdate(w, r)
		case http.MethodDelete:
			s.handleAdminTenantPoolDelete(w, r)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *Server) handleAdminTenantPoolCreate(w http.ResponseWriter, r *http.Request) {
	var req adminTenantPoolRequest
	if err := decodeJSONBody(w, r, &req, true); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.PoolSize == nil || *req.PoolSize <= 0 {
		errJSON(w, http.StatusBadRequest, "pool_size must be positive")
		return
	}
	if err := s.validateTenantPoolSize(*req.PoolSize); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := adminCredentials(req.PublicKey, req.PrivateKey, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	createLock := s.tenantPoolCreateLock(cred)
	createLock.Lock()
	defer createLock.Unlock()
	orgID, err := s.firstManagedOrganization(r.Context(), cred)
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "create tenant pool")
		return
	}
	if orgID != "" {
		if _, err := s.meta.GetTenantPoolByOrganization(r.Context(), orgID); err == nil {
			errJSON(w, http.StatusConflict, "tenant pool already exists for organization")
			return
		} else if !errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusInternalServerError, "tenant pool lookup failed")
			return
		}
	}
	poolID := token.NewID()
	now := time.Now().UTC()
	if err := s.meta.CreateTenantPool(r.Context(), &meta.TenantPool{
		PoolID:         poolID,
		OrganizationID: orgID,
		Size:           *req.PoolSize,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		if errors.Is(err, meta.ErrDuplicate) {
			errJSON(w, http.StatusConflict, "tenant pool already exists for organization")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to persist tenant pool")
		return
	}
	results, err := s.createFreePoolTenants(r.Context(), poolID, *req.PoolSize, cred, nil)
	if err != nil {
		s.deleteTenantPoolMetadata(r.Context(), poolID, "create_free_tenants_error")
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("create tenant pool failed: %v", err))
		return
	}
	if orgID == "" {
		orgID = firstResultOrganizationID(results)
		if orgID != "" {
			if err := s.meta.UpdateTenantPoolOrganization(r.Context(), poolID, orgID); err != nil {
				s.cleanupCreatedPoolTenants(r.Context(), results, cred, "update_pool_org_error")
				s.deleteTenantPoolMetadata(r.Context(), poolID, "update_pool_org_error")
				if errors.Is(err, meta.ErrDuplicate) {
					errJSON(w, http.StatusConflict, "tenant pool already exists for organization")
					return
				}
				errJSON(w, http.StatusInternalServerError, "failed to update tenant pool organization")
				return
			}
		}
	}
	for _, res := range results {
		s.startProvisionedTenantSchemaInit(r.Context(), res)
	}
	freeSize := len(results)
	if orgID != "" {
		if n, err := s.meta.CountFreeTenantPoolBindings(r.Context(), orgID); err == nil {
			freeSize = n
		}
	}
	writeJSON(w, http.StatusAccepted, adminTenantPoolResponse{
		PoolID:         poolID,
		OrganizationID: orgID,
		PoolSize:       *req.PoolSize,
		FreeSize:       freeSize,
		Status:         string(meta.TenantPoolActive),
	})
}

func (s *Server) handleAdminTenantPoolGet(w http.ResponseWriter, r *http.Request) {
	cred, err := adminCredentialsFromHeaders(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	pool, freeSize, ok := s.authorizedTenantPool(w, r, cred)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, adminTenantPoolResponse{
		PoolID:         pool.PoolID,
		OrganizationID: pool.OrganizationID,
		PoolSize:       pool.Size,
		FreeSize:       freeSize,
		Status:         string(pool.Status),
	})
}

func (s *Server) handleAdminTenantPoolUpdate(w http.ResponseWriter, r *http.Request) {
	var req adminTenantPoolRequest
	if err := decodeJSONBody(w, r, &req, true); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.PoolSize != nil && *req.PoolSize <= 0 {
		errJSON(w, http.StatusBadRequest, "pool_size must be positive")
		return
	}
	if req.PoolSize == nil {
		errJSON(w, http.StatusBadRequest, "pool_size is required")
		return
	}
	if err := s.validateTenantPoolSize(*req.PoolSize); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := adminCredentials(req.PublicKey, req.PrivateKey, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	pool, _, ok := s.authorizedTenantPool(w, r, cred)
	if !ok {
		return
	}
	lock := s.tenantPoolLock(pool.PoolID)
	lock.Lock()
	defer lock.Unlock()

	var out adminTenantPoolResponse
	if err := s.meta.WithTenantPoolLock(r.Context(), pool.PoolID, func(ctx context.Context) error {
		if pool, err = s.meta.GetTenantPoolByID(ctx, pool.PoolID); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "tenant pool lookup failed")
		}
		if pool.Status != meta.TenantPoolActive {
			return adminTenantPoolError(http.StatusConflict, "tenant pool is not active")
		}
		freeSize, err := s.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID)
		if err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "tenant pool free size lookup failed")
		}
		targetSize := *req.PoolSize
		if targetSize > freeSize {
			results, err := s.createFreePoolTenants(ctx, pool.PoolID, targetSize-freeSize, cred, nil)
			if err != nil {
				return adminTenantPoolError(http.StatusBadGateway, fmt.Sprintf("grow tenant pool failed: %v", err))
			}
			for _, res := range results {
				s.startProvisionedTenantSchemaInit(ctx, res)
			}
			freeSize += len(results)
		} else if targetSize < freeSize {
			if err := s.deleteNewestFreePoolTenants(ctx, pool.OrganizationID, freeSize-targetSize, cred, false); err != nil {
				if actualFreeSize, countErr := s.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID); countErr == nil {
					if updateErr := s.meta.UpdateTenantPoolSize(ctx, pool.PoolID, actualFreeSize); updateErr != nil {
						logger.Warn(ctx, "admin_tenant_pool_shrink_partial_size_update_failed", zap.String("pool_id", pool.PoolID), zap.Int("free_size", actualFreeSize), zap.Error(updateErr))
					}
				} else {
					logger.Warn(ctx, "admin_tenant_pool_shrink_partial_count_failed", zap.String("pool_id", pool.PoolID), zap.Error(countErr))
				}
				return adminTenantPoolError(http.StatusBadGateway, fmt.Sprintf("shrink tenant pool failed: %v", err))
			}
			freeSize = targetSize
		}
		if err := s.meta.UpdateTenantPoolSize(ctx, pool.PoolID, targetSize); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "failed to update tenant pool")
		}
		out = adminTenantPoolResponse{
			PoolID:         pool.PoolID,
			OrganizationID: pool.OrganizationID,
			PoolSize:       targetSize,
			FreeSize:       freeSize,
			Status:         string(pool.Status),
		}
		return nil
	}); err != nil {
		writeAdminTenantPoolError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, out)
}

func (s *Server) validateTenantPoolSize(size int) error {
	if s.tenantPoolMaxSize > 0 && size > s.tenantPoolMaxSize {
		return fmt.Errorf("pool_size must be less than or equal to %d", s.tenantPoolMaxSize)
	}
	return nil
}

func (s *Server) handleAdminTenantPoolDelete(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		PublicKey  string `json:"public_key"`
		PrivateKey string `json:"private_key"`
	}
	if err := decodeJSONBody(w, r, &raw, false); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := adminCredentials(raw.PublicKey, raw.PrivateKey, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	pool, _, ok := s.authorizedTenantPool(w, r, cred)
	if !ok {
		return
	}
	lock := s.tenantPoolLock(pool.PoolID)
	lock.Lock()
	defer lock.Unlock()

	var out adminTenantPoolResponse
	if err := s.meta.WithTenantPoolLock(r.Context(), pool.PoolID, func(ctx context.Context) error {
		if err := s.meta.UpdateTenantPoolStatus(ctx, pool.PoolID, meta.TenantPoolDeleting); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "failed to mark tenant pool deleting")
		}
		if err := s.deleteNewestFreePoolTenants(ctx, pool.OrganizationID, 0, cred, true); err != nil {
			return adminTenantPoolError(http.StatusBadGateway, fmt.Sprintf("delete tenant pool failed: %v", err))
		}
		if err := s.meta.DeleteTenantPool(ctx, pool.PoolID); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "failed to delete tenant pool")
		}
		out = adminTenantPoolResponse{
			PoolID:         pool.PoolID,
			OrganizationID: pool.OrganizationID,
			PoolSize:       pool.Size,
			FreeSize:       0,
			Status:         string(meta.TenantPoolDeleting),
		}
		return nil
	}); err != nil {
		writeAdminTenantPoolError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, out)
}

func (s *Server) authorizedTenantPool(w http.ResponseWriter, r *http.Request, cred tenant.CredentialProvisionRequest) (*meta.TenantPool, int, bool) {
	orgID, err := s.firstManagedOrganization(r.Context(), cred)
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "authorize tenant pool")
		return nil, 0, false
	}
	if orgID == "" {
		errJSON(w, http.StatusNotFound, "tenant pool not found")
		return nil, 0, false
	}
	pool, err := s.meta.GetTenantPoolByOrganization(r.Context(), orgID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "tenant pool not found")
			return nil, 0, false
		}
		errJSON(w, http.StatusInternalServerError, "tenant pool lookup failed")
		return nil, 0, false
	}
	freeSize, err := s.meta.CountFreeTenantPoolBindings(r.Context(), orgID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "tenant pool free size lookup failed")
		return nil, 0, false
	}
	return pool, freeSize, true
}

func (s *Server) firstManagedOrganization(ctx context.Context, cred tenant.CredentialProvisionRequest) (string, error) {
	lister, ok := s.provisioner.(tenant.ManagedClusterLister)
	if !ok {
		return "", fmt.Errorf("managed cluster list not enabled")
	}
	page, err := lister.ListManagedClusters(ctx, cred, tenant.ManagedClusterListOptions{PageSize: 1})
	if err != nil || page == nil || len(page.Clusters) == 0 {
		return "", err
	}
	return strings.TrimSpace(page.Clusters[0].OrganizationID), nil
}

func (s *Server) createFreePoolTenants(ctx context.Context, poolID string, count int, cred tenant.CredentialProvisionRequest, quotaOpt *quotaRequest) ([]*provisionTenantResult, error) {
	manager, ok := s.provisioner.(tenant.TenantPoolClusterManager)
	if !ok {
		return nil, fmt.Errorf("tenant pool provisioning not enabled")
	}
	if count <= 0 {
		return []*provisionTenantResult{}, nil
	}
	provider := tenant.ProviderTiDBCloudNative
	tenantIDs := make([]string, 0, count)
	now := time.Now().UTC()
	for i := 0; i < count; i++ {
		tenantID := token.NewID()
		if err := s.insertPendingPoolTenant(ctx, tenantID, provider, now); err != nil {
			s.cleanupPoolProvisionedClusters(ctx, nil, cred, tenantIDs, "insert_pending_tenant_error")
			return nil, err
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	var opts tenant.QuotaUpdateOptions
	if quotaOpt != nil {
		opts.TiDBCloudSpendingLimitMonthly = quotaOpt.TiDBCloudSpendingLimit
	}
	opts.TenantPoolID = poolID
	clusters, _, err := manager.BatchProvisionFreeClustersWithCredentialsAndQuota(ctx, tenantIDs, cred, opts)
	if err != nil && len(clusters) == 0 {
		s.cleanupPoolProvisionedClusters(ctx, clusters, cred, tenantIDs, "batch_provision_error")
		for _, tenantID := range tenantIDs {
			_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
		}
		return nil, err
	}
	if err != nil {
		logger.Warn(ctx, "admin_tenant_pool_batch_metadata_incomplete",
			zap.String("pool_id", poolID),
			zap.Int("cluster_count", len(clusters)),
			zap.Error(err))
	}
	cleanupOnError := err == nil
	defer func() {
		if cleanupOnError {
			s.cleanupPoolProvisionedClusters(ctx, clusters, cred, tenantIDs, "metadata_error")
		}
	}()
	results := make([]*provisionTenantResult, 0, len(clusters))
	persistedTenants := make(map[string]struct{}, len(clusters))
	cloudProvider, region := provisioningCloudRegion(s.provisioner)
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		if strings.TrimSpace(cluster.TenantID) == "" {
			return nil, fmt.Errorf("tidbcloud tenant id label is missing")
		}
		if strings.TrimSpace(cluster.OrganizationID) == "" {
			return nil, fmt.Errorf("tidbcloud organization label is missing")
		}
		persistedTenants[strings.TrimSpace(cluster.TenantID)] = struct{}{}
		if err := s.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
			TenantID:       cluster.TenantID,
			OrganizationID: cluster.OrganizationID,
			ClusterID:      cluster.ClusterID,
			PoolID:         poolID,
			PoolStatus:     meta.TenantPoolBindingFree,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			return nil, err
		}
		if err := s.persistPoolTenantConnection(ctx, cluster, provider); err != nil {
			return nil, err
		}
		if err := s.meta.UpdateTenantStatus(ctx, cluster.TenantID, meta.TenantProvisioning); err != nil {
			return nil, err
		}
		res := &provisionTenantResult{
			TenantID:       cluster.TenantID,
			Status:         meta.TenantProvisioning,
			Provider:       provider,
			CloudProvider:  cloudProvider,
			Region:         region,
			OrganizationID: strings.TrimSpace(cluster.OrganizationID),
		}
		if poolClusterConnectionReady(cluster) {
			res.TenantDSN = tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, cluster.DBName, true, provider)
		} else {
			s.startPoolClusterMetadataResume(ctx, cluster, cred)
		}
		results = append(results, res)
	}
	for _, tenantID := range tenantIDs {
		if _, ok := persistedTenants[tenantID]; ok {
			continue
		}
		_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
	}
	cleanupOnError = false
	return results, nil
}

func (s *Server) persistPoolTenantConnection(ctx context.Context, cluster *tenant.ClusterInfo, provider string) error {
	if cluster == nil {
		return fmt.Errorf("cluster info is required")
	}
	cipherPass, err := s.pool.Encrypt(ctx, []byte(cluster.Password))
	if err != nil {
		return err
	}
	return s.meta.UpdateTenantConnection(ctx, cluster.TenantID, &meta.Tenant{
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           cluster.Username,
		DBPasswordCipher: cipherPass,
		DBName:           cluster.DBName,
		DBTLS:            true,
		Provider:         provider,
		ClusterID:        cluster.ClusterID,
	})
}

func poolClusterConnectionReady(cluster *tenant.ClusterInfo) bool {
	return cluster != nil &&
		strings.TrimSpace(cluster.TenantID) != "" &&
		strings.TrimSpace(cluster.ClusterID) != "" &&
		strings.TrimSpace(cluster.OrganizationID) != "" &&
		strings.TrimSpace(cluster.Host) != "" &&
		cluster.Port > 0 &&
		strings.TrimSpace(cluster.Username) != "" &&
		strings.TrimSpace(cluster.Password) != "" &&
		strings.TrimSpace(cluster.DBName) != ""
}

func (s *Server) startPoolClusterMetadataResume(ctx context.Context, cluster *tenant.ClusterInfo, cred tenant.CredentialProvisionRequest) {
	waiter, ok := s.provisioner.(tenant.TenantPoolClusterMetadataWaiter)
	if !ok {
		logger.Warn(ctx, "admin_tenant_pool_metadata_resume_skipped",
			zap.String("tenant_id", cluster.TenantID),
			zap.String("cluster_id", cluster.ClusterID),
			zap.String("reason", "metadata_waiter_unavailable"))
		return
	}
	clusterCopy := *cluster
	s.startServerWorker(ctx, func(workerCtx context.Context) {
		started := time.Now()
		updated, err := waiter.WaitForPoolClusterMetadata(workerCtx, &clusterCopy, cred)
		if err != nil {
			logger.Warn(workerCtx, "admin_tenant_pool_metadata_resume_failed",
				zap.String("tenant_id", clusterCopy.TenantID),
				zap.String("cluster_id", clusterCopy.ClusterID),
				zap.Error(err))
			return
		}
		if !poolClusterConnectionReady(updated) {
			logger.Warn(workerCtx, "admin_tenant_pool_metadata_resume_incomplete",
				zap.String("tenant_id", clusterCopy.TenantID),
				zap.String("cluster_id", clusterCopy.ClusterID))
			return
		}
		if err := s.persistPoolTenantConnection(workerCtx, updated, tenant.ProviderTiDBCloudNative); err != nil {
			logger.Warn(workerCtx, "admin_tenant_pool_metadata_resume_persist_failed",
				zap.String("tenant_id", updated.TenantID),
				zap.String("cluster_id", updated.ClusterID),
				zap.Error(err))
			return
		}
		cloudProvider, region := provisioningCloudRegion(s.provisioner)
		logProvisionStage(workerCtx, "admin_tenant_pool_metadata_resume_done", updated.TenantID, tenant.ProviderTiDBCloudNative, started, "cluster_id", updated.ClusterID, "organization_id", updated.OrganizationID)
		s.startProvisionedTenantSchemaInit(workerCtx, &provisionTenantResult{
			TenantID:       updated.TenantID,
			Status:         meta.TenantProvisioning,
			Provider:       tenant.ProviderTiDBCloudNative,
			TenantDSN:      tenantDSN(updated.Username, updated.Password, updated.Host, updated.Port, updated.DBName, true, tenant.ProviderTiDBCloudNative),
			CloudProvider:  cloudProvider,
			Region:         region,
			OrganizationID: strings.TrimSpace(updated.OrganizationID),
		})
	})
}

func (s *Server) cleanupPoolProvisionedClusters(ctx context.Context, clusters []*tenant.ClusterInfo, cred tenant.CredentialProvisionRequest, tenantIDs []string, reason string) {
	cleanupCtx := backgroundWithTrace(ctx)
	seenTenants := make(map[string]struct{}, len(tenantIDs)+len(clusters))
	deprovisionFailed := make(map[string]struct{}, len(clusters))
	for _, tenantID := range tenantIDs {
		tenantID = strings.TrimSpace(tenantID)
		if tenantID == "" {
			continue
		}
		seenTenants[tenantID] = struct{}{}
	}
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		tenantID := strings.TrimSpace(cluster.TenantID)
		if tenantID != "" {
			if _, ok := seenTenants[tenantID]; !ok {
				if err := s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed); err != nil {
					logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_mark_failed", zap.String("tenant_id", tenantID), zap.String("reason", reason), zap.Error(err))
				}
			}
		}
		if strings.TrimSpace(cluster.ClusterID) == "" {
			continue
		}
		t := &meta.Tenant{
			ID:        tenantID,
			Provider:  tenant.ProviderTiDBCloudNative,
			ClusterID: strings.TrimSpace(cluster.ClusterID),
			DBHost:    cluster.Host,
			DBPort:    cluster.Port,
			DBUser:    cluster.Username,
			DBName:    cluster.DBName,
		}
		if err := s.deprovisionTenantCluster(cleanupCtx, t, cred); err != nil {
			if tenantID != "" {
				deprovisionFailed[tenantID] = struct{}{}
			}
			logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_cluster_failed", zap.String("tenant_id", tenantID), zap.String("cluster_id", cluster.ClusterID), zap.String("reason", reason), zap.Error(err))
		}
	}
	for tenantID := range seenTenants {
		if _, failed := deprovisionFailed[tenantID]; failed {
			if err := s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed); err != nil {
				logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_mark_failed", zap.String("tenant_id", tenantID), zap.String("reason", reason), zap.Error(err))
			}
			continue
		}
		_ = s.meta.RevokeTenantAPIKeys(cleanupCtx, tenantID)
		if err := s.meta.MarkTenantDeleted(cleanupCtx, tenantID); err != nil {
			logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_mark_deleted_failed", zap.String("tenant_id", tenantID), zap.String("reason", reason), zap.Error(err))
			_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
		}
	}
}

func (s *Server) insertPendingPoolTenant(ctx context.Context, tenantID, provider string, now time.Time) error {
	autoProfile, err := s.defaultAutoEmbeddingProfileForTenant(ctx, tenantID, provider, now)
	if err != nil {
		return fmt.Errorf("build tenant auto-embedding profile: %w", err)
	}
	if err := s.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         provider,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		return fmt.Errorf("persist tenant: %w", err)
	}
	if autoProfile != nil {
		if err := s.meta.UpsertTenantAutoEmbeddingProfile(ctx, autoProfile); err != nil {
			_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
			return fmt.Errorf("persist tenant auto-embedding profile: %w", err)
		}
	}
	return nil
}

func (s *Server) deleteNewestFreePoolTenants(ctx context.Context, organizationID string, count int, cred tenant.CredentialProvisionRequest, deleteAll bool) error {
	if count <= 0 && !deleteAll {
		return nil
	}
	remaining := count
	for deleteAll || remaining > 0 {
		limit := remaining
		if deleteAll {
			limit = 100
		}
		var rows []meta.TenantWithTiDBCloudOrgBinding
		var err error
		if deleteAll {
			rows, err = s.meta.ListFreeTenantPoolBindingsForDelete(ctx, organizationID, true, limit)
		} else {
			rows, err = s.meta.ListFreeTenantPoolBindings(ctx, organizationID, true, limit)
		}
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			if deleteAll {
				return nil
			}
			return fmt.Errorf("not enough free tenants to delete")
		}
		progressed := false
		for _, row := range rows {
			t := row.Tenant
			updated, err := s.meta.MarkFreeTenantPoolTenantDeleting(ctx, t.ID, t.Status)
			if err != nil {
				return err
			}
			if !updated {
				continue
			}
			progressed = true
			if err := s.deprovisionTenantCluster(ctx, &t, cred); err != nil {
				_, _ = s.meta.UpdateTenantStatusIf(context.Background(), t.ID, meta.TenantDeleting, t.Status)
				return err
			}
			_ = s.meta.RevokeTenantAPIKeys(ctx, t.ID)
			if err := s.meta.MarkTenantDeleted(ctx, t.ID); err != nil {
				_ = s.meta.UpdateTenantStatus(context.Background(), t.ID, meta.TenantFailed)
				return err
			}
			if !deleteAll {
				remaining--
			}
			if !deleteAll && remaining == 0 {
				break
			}
		}
		if !progressed {
			return fmt.Errorf("not enough free tenants to delete")
		}
	}
	return nil
}

func (s *Server) cleanupCreatedPoolTenants(ctx context.Context, results []*provisionTenantResult, cred tenant.CredentialProvisionRequest, reason string) {
	cleanupCtx := backgroundWithTrace(ctx)
	for _, res := range results {
		if res == nil || strings.TrimSpace(res.TenantID) == "" {
			continue
		}
		tenantID := strings.TrimSpace(res.TenantID)
		t, err := s.meta.GetTenant(cleanupCtx, tenantID)
		if err != nil {
			logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_get_tenant_failed", zap.String("tenant_id", tenantID), zap.String("reason", reason), zap.Error(err))
			continue
		}
		if err := s.deprovisionTenantCluster(cleanupCtx, t, cred); err != nil {
			logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_created_cluster_failed", zap.String("tenant_id", tenantID), zap.String("cluster_id", t.ClusterID), zap.String("reason", reason), zap.Error(err))
			_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
			continue
		}
		_ = s.meta.RevokeTenantAPIKeys(cleanupCtx, tenantID)
		if err := s.meta.MarkTenantDeleted(cleanupCtx, tenantID); err != nil {
			logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_mark_deleted_failed", zap.String("tenant_id", tenantID), zap.String("reason", reason), zap.Error(err))
		}
	}
}

func (s *Server) deleteTenantPoolMetadata(ctx context.Context, poolID, reason string) {
	if strings.TrimSpace(poolID) == "" {
		return
	}
	cleanupCtx := backgroundWithTrace(ctx)
	if err := s.meta.DeleteTenantPool(cleanupCtx, poolID); err != nil && !errors.Is(err, meta.ErrNotFound) {
		logger.Warn(cleanupCtx, "admin_tenant_pool_delete_metadata_failed", zap.String("pool_id", poolID), zap.String("reason", reason), zap.Error(err))
	}
}

func firstResultOrganizationID(results []*provisionTenantResult) string {
	for _, res := range results {
		if res != nil && strings.TrimSpace(res.OrganizationID) != "" {
			return strings.TrimSpace(res.OrganizationID)
		}
	}
	return ""
}

func (s *Server) replenishTenantPoolAsync(ctx context.Context, pool *meta.TenantPool, cred tenant.CredentialProvisionRequest) {
	if pool == nil || pool.OrganizationID == "" || pool.Size <= 0 {
		return
	}
	workerCtx := backgroundWithTrace(ctx)
	s.startServerWorker(workerCtx, func(ctx context.Context) {
		lock := s.tenantPoolLock(pool.PoolID)
		lock.Lock()
		defer lock.Unlock()
		if err := s.meta.WithTenantPoolLock(ctx, pool.PoolID, func(ctx context.Context) error {
			current, err := s.meta.GetTenantPoolByID(ctx, pool.PoolID)
			if err != nil {
				if !errors.Is(err, meta.ErrNotFound) {
					logger.Warn(ctx, "admin_tenant_pool_replenish_get_pool_failed", zap.String("pool_id", pool.PoolID), zap.Error(err))
				}
				return nil
			}
			if current.Status != meta.TenantPoolActive || current.OrganizationID == "" || current.Size <= 0 {
				return nil
			}
			freeSize, err := s.meta.CountFreeTenantPoolBindings(ctx, current.OrganizationID)
			if err != nil {
				logger.Warn(ctx, "admin_tenant_pool_replenish_count_failed", zap.String("pool_id", current.PoolID), zap.Error(err))
				return nil
			}
			missing := current.Size - freeSize
			if missing <= 0 {
				return nil
			}
			results, err := s.createFreePoolTenants(ctx, current.PoolID, missing, cred, nil)
			if err != nil {
				logger.Warn(ctx, "admin_tenant_pool_replenish_failed", zap.String("pool_id", current.PoolID), zap.Error(err))
				return nil
			}
			for _, res := range results {
				s.startProvisionedTenantSchemaInit(ctx, res)
			}
			return nil
		}); err != nil {
			logger.Warn(ctx, "admin_tenant_pool_replenish_lock_failed", zap.String("pool_id", pool.PoolID), zap.Error(err))
		}
	})
}

func (s *Server) tenantPoolLock(poolID string) *sync.Mutex {
	if strings.TrimSpace(poolID) == "" {
		return &sync.Mutex{}
	}
	v, _ := s.tenantPoolLocks.LoadOrStore(poolID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Server) tenantPoolCreateLock(cred tenant.CredentialProvisionRequest) *sync.Mutex {
	key := strings.TrimSpace(cred.PublicKey)
	if key == "" {
		return &sync.Mutex{}
	}
	// A TiDB Cloud public key belongs to a single org, so this serializes the
	// first-create path before the org id is discoverable from managed clusters.
	v, _ := s.tenantPoolCreateLocks.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Server) claimAdminTenantFromPool(ctx context.Context, cred tenant.CredentialProvisionRequest, quotaOpt *quotaRequest) (*provisionTenantResult, *meta.TenantPool, bool, error) {
	claimStarted := time.Now()
	manager, ok := s.provisioner.(tenant.TenantPoolClusterManager)
	if !ok {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_skipped", "provider", tenant.ProviderTiDBCloudNative, "reason", "pool_manager_unavailable", "duration_ms", durationMillis(claimStarted))...)
		return nil, nil, false, nil
	}
	if _, ok := s.provisioner.(tenant.ManagedClusterLister); !ok {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_skipped", "provider", tenant.ProviderTiDBCloudNative, "reason", "managed_cluster_lister_unavailable", "duration_ms", durationMillis(claimStarted))...)
		return nil, nil, false, nil
	}
	stageStarted := time.Now()
	orgID, err := s.firstManagedOrganization(ctx, cred)
	if err != nil || orgID == "" {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_org_lookup_done", "provider", tenant.ProviderTiDBCloudNative, "organization_id", orgID, "duration_ms", durationMillis(stageStarted), "has_error", err != nil)...)
		return nil, nil, false, err
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_org_lookup_done", "provider", tenant.ProviderTiDBCloudNative, "organization_id", orgID, "duration_ms", durationMillis(stageStarted))...)
	stageStarted = time.Now()
	pool, err := s.meta.GetTenantPoolByOrganization(ctx, orgID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_pool_lookup_missed", "provider", tenant.ProviderTiDBCloudNative, "organization_id", orgID, "duration_ms", durationMillis(stageStarted))...)
			return nil, nil, false, nil
		}
		return nil, nil, false, err
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_pool_lookup_done", "provider", tenant.ProviderTiDBCloudNative, "pool_id", pool.PoolID, "organization_id", orgID, "pool_size", pool.Size, "pool_status", pool.Status, "duration_ms", durationMillis(stageStarted))...)
	if pool.Status != meta.TenantPoolActive {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_skipped", "provider", tenant.ProviderTiDBCloudNative, "pool_id", pool.PoolID, "organization_id", orgID, "reason", "pool_inactive", "pool_status", pool.Status, "duration_ms", durationMillis(claimStarted))...)
		return nil, nil, false, nil
	}
	stageStarted = time.Now()
	row, err := s.meta.ClaimOldestFreeTenantPoolBinding(ctx, orgID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_free_tenant_missed", "provider", tenant.ProviderTiDBCloudNative, "pool_id", pool.PoolID, "organization_id", orgID, "duration_ms", durationMillis(stageStarted))...)
			return nil, pool, false, nil
		}
		return nil, nil, false, err
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_free_tenant_claimed", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID, "cluster_id", row.Binding.ClusterID, "status", row.Tenant.Status)
	usedAt := time.Now().UTC()
	var opts tenant.QuotaUpdateOptions
	if quotaOpt != nil {
		opts.TiDBCloudSpendingLimitMonthly = quotaOpt.TiDBCloudSpendingLimit
	}
	cluster := clusterInfoFromTenant(&row.Tenant)
	stageStarted = time.Now()
	if _, err := manager.MarkClusterPoolUsed(ctx, cluster, cred, usedAt, opts); err != nil {
		s.releaseClaimedPoolTenant(ctx, manager, cluster, cred, row.Tenant.ID, "mark_used_error")
		return nil, nil, false, err
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_cluster_marked_used", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID, "cluster_id", cluster.ClusterID, "quota_requested", quotaOpt != nil)
	success := false
	defer func() {
		if !success {
			s.releaseClaimedPoolTenant(ctx, manager, cluster, cred, row.Tenant.ID, "claim_error")
		}
	}()
	if quotaOpt != nil {
		stageStarted = time.Now()
		quotaReq := *quotaOpt
		quotaReq.TenantID = row.Tenant.ID
		if err := s.applyQuotaLocalConfig(ctx, row.Tenant.ID, quotaReq); err != nil {
			return nil, nil, false, err
		}
		logProvisionStage(ctx, "admin_tenant_pool_claim_quota_local_config_applied", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID)
	}
	stageStarted = time.Now()
	plainPass, err := s.pool.Decrypt(ctx, row.Tenant.DBPasswordCipher)
	if err != nil {
		return nil, nil, false, err
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_db_password_decrypted", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID)
	stageStarted = time.Now()
	apiToken, apiKeyID, err := s.issueOwnerAPIKey(ctx, row.Tenant.ID, "default", 1, apiKeyIssueSource{})
	if err != nil {
		return nil, nil, false, err
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_api_key_issued", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID, "api_key_id", apiKeyID)
	cloudProvider, region := provisioningCloudRegion(s.provisioner)
	success = true
	logProvisionStage(ctx, "admin_tenant_pool_claim_done", row.Tenant.ID, row.Tenant.Provider, claimStarted, "pool_id", pool.PoolID, "organization_id", orgID, "cluster_id", cluster.ClusterID, "api_key_id", apiKeyID, "status", row.Tenant.Status)
	return &provisionTenantResult{
		TenantID:       row.Tenant.ID,
		APIKey:         apiToken,
		APIKeyID:       apiKeyID,
		Status:         row.Tenant.Status,
		Provider:       tenant.ProviderTiDBCloudNative,
		TenantDSN:      tenantDSN(row.Tenant.DBUser, string(plainPass), row.Tenant.DBHost, row.Tenant.DBPort, row.Tenant.DBName, row.Tenant.DBTLS, row.Tenant.Provider),
		CloudProvider:  cloudProvider,
		Region:         region,
		OrganizationID: row.Binding.OrganizationID,
	}, pool, true, nil
}

func (s *Server) releaseClaimedPoolTenant(ctx context.Context, manager tenant.TenantPoolClusterManager, cluster *tenant.ClusterInfo, cred tenant.CredentialProvisionRequest, tenantID, reason string) {
	releaseCtx := backgroundWithTrace(ctx)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" && cluster != nil {
		tenantID = strings.TrimSpace(cluster.TenantID)
	}
	if tenantID != "" {
		if err := s.meta.UpdateTenantPoolBindingStatus(releaseCtx, tenantID, meta.TenantPoolBindingFree, nil); err != nil {
			logger.Warn(releaseCtx, "admin_tenant_pool_release_binding_failed", zap.String("tenant_id", tenantID), zap.String("reason", reason), zap.Error(err))
		}
	}
	if err := manager.MarkClusterPoolFree(releaseCtx, cluster, cred); err != nil {
		clusterID := ""
		if cluster != nil {
			clusterID = cluster.ClusterID
		}
		logger.Warn(releaseCtx, "admin_tenant_pool_release_cluster_failed", zap.String("tenant_id", tenantID), zap.String("cluster_id", clusterID), zap.String("reason", reason), zap.Error(err))
	}
}
