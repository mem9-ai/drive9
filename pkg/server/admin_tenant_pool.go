package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type adminTenantPoolRequest struct {
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
	PoolSize   *int   `json:"pool_size,omitempty"`
}

type adminTenantPoolResponse struct {
	PoolID         string                `json:"pool_id"`
	OrganizationID string                `json:"organization_id,omitempty"`
	PoolSize       int                   `json:"pool_size"`
	FreeSize       int                   `json:"free_size"`
	Status         adminTenantPoolStatus `json:"status"`
}

type adminTenantPoolHTTPError struct {
	status  int
	message string
}

type tenantPoolResumeJob struct {
	rerun atomic.Bool
}

type adminTenantPoolStatus string

const adminTenantPoolStatusCreating adminTenantPoolStatus = "creating"

const adminTenantPoolMetricsComponent = "admin_tenant_pool"

const tenantPoolClaimCASRetryLimit = 8

func retryTenantPoolClaimCAS[T any](attempt func() (T, error)) (T, error) {
	var zero T
	var lastErr error
	for range tenantPoolClaimCASRetryLimit {
		result, err := attempt()
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, meta.ErrNotFound) {
			return zero, err
		}
		lastErr = err
	}
	return zero, fmt.Errorf("tenant pool claim remained busy after %d attempts: %w",
		tenantPoolClaimCASRetryLimit, lastErr)
}

type tenantPoolClaimSelection struct {
	sharedResult *provisionTenantResult
	native       *meta.TenantWithTiDBCloudOrgBinding
}

func (e *adminTenantPoolHTTPError) Error() string {
	return e.message
}

func adminTenantPoolError(status int, message string) error {
	return &adminTenantPoolHTTPError{status: status, message: message}
}

func writeAdminTenantPoolError(w http.ResponseWriter, r *http.Request, err error) {
	var httpErr *adminTenantPoolHTTPError
	if errors.As(err, &httpErr) {
		errJSON(w, httpErr.status, httpErr.message)
		return
	}
	writeBackendError(w, r, err)
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
	createStarted := time.Now()
	metricResult := "ok"
	defer func() {
		metrics.RecordOperation(adminTenantPoolMetricsComponent, "create", metricResult, time.Since(createStarted))
	}()
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_create_requested",
		"provider", tenant.ProviderTiDBCloudNative,
		"pool_size", *req.PoolSize)...)
	createLock := s.tenantPoolCreateLock(cred)
	createLock.Lock()
	defer createLock.Unlock()
	if err := s.meta.WithTenantPoolLock(r.Context(), tenantPoolCreateDatabaseLockKey(cred), func(ctx context.Context) error {
		stageStarted := time.Now()
		orgID, err := s.firstManagedOrganization(ctx, cred)
		if err != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_org_lookup_failed",
				"provider", tenant.ProviderTiDBCloudNative,
				"duration_ms", durationMillis(stageStarted),
				"error", err)...)
			metricResult = "cluster_error"
			writeAdminTiDBCloudError(w, ctx, err, "create tenant pool")
			return nil
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_org_lookup_done",
			"provider", tenant.ProviderTiDBCloudNative,
			"organization_id", orgID,
			"duration_ms", durationMillis(stageStarted))...)
		if orgID != "" {
			if _, err := s.meta.GetTenantPoolByOrganization(ctx, orgID); err == nil {
				metricResult = "conflict"
				errJSON(w, http.StatusConflict, "tenant pool already exists for organization")
				return nil
			} else if !errors.Is(err, meta.ErrNotFound) {
				metricResult = "error"
				errJSON(w, backendErrorStatus(r.Context(), err), "tenant pool lookup failed")
				return nil
			}
		}
		poolID := token.NewID()
		now := time.Now().UTC()
		stageStarted = time.Now()
		if err := s.meta.CreateTenantPool(ctx, &meta.TenantPool{
			PoolID:         poolID,
			OrganizationID: orgID,
			Size:           *req.PoolSize,
			Status:         meta.TenantPoolActive,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			if errors.Is(err, meta.ErrDuplicate) {
				metricResult = "conflict"
				errJSON(w, http.StatusConflict, "tenant pool already exists for organization")
				return nil
			}
			metricResult = "error"
			errJSON(w, backendErrorStatus(r.Context(), err), "failed to persist tenant pool")
			return nil
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_pool_persisted",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", poolID,
			"organization_id", orgID,
			"pool_size", *req.PoolSize,
			"status", meta.TenantPoolActive,
			"duration_ms", durationMillis(stageStarted))...)
		stageStarted = time.Now()
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_free_tenants_started",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", poolID,
			"organization_id", orgID,
			"requested_count", *req.PoolSize)...)
		results, err := s.createFreePoolTenants(ctx, poolID, *req.PoolSize, cred, nil)
		if err != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_free_tenants_failed",
				"provider", tenant.ProviderTiDBCloudNative,
				"pool_id", poolID,
				"organization_id", orgID,
				"requested_count", *req.PoolSize,
				"duration_ms", durationMillis(stageStarted),
				"error", err)...)
			s.cleanupCreatedPoolTenants(ctx, results, cred, "create_free_tenants_error")
			s.deleteTenantPoolMetadata(ctx, poolID, "create_free_tenants_error")
			metricResult = "cluster_error"
			status, msg := clientFacingErrorResponse(http.StatusBadGateway, "create tenant pool failed", err)
			errJSON(w, status, msg)
			return nil
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_free_tenants_done",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", poolID,
			"organization_id", orgID,
			"requested_count", *req.PoolSize,
			"created_count", len(results),
			"duration_ms", durationMillis(stageStarted))...)
		if orgID == "" {
			orgID = firstResultOrganizationID(results)
			if orgID != "" {
				stageStarted = time.Now()
				if err := s.meta.UpdateTenantPoolOrganization(ctx, poolID, orgID); err != nil {
					s.cleanupCreatedPoolTenants(ctx, results, cred, "update_pool_org_error")
					s.deleteTenantPoolMetadata(ctx, poolID, "update_pool_org_error")
					if errors.Is(err, meta.ErrDuplicate) {
						metricResult = "conflict"
						errJSON(w, http.StatusConflict, "tenant pool already exists for organization")
						return nil
					}
					metricResult = "error"
					errJSON(w, backendErrorStatus(r.Context(), err), "failed to update tenant pool organization")
					return nil
				}
				logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_org_persisted",
					"provider", tenant.ProviderTiDBCloudNative,
					"pool_id", poolID,
					"organization_id", orgID,
					"duration_ms", durationMillis(stageStarted))...)
			}
		}
		for _, res := range results {
			s.startProvisionedTenantSchemaInit(ctx, res)
		}
		freeSize := 0
		slotSize := len(results)
		if orgID != "" {
			if n, err := s.meta.CountFreeTenantPoolBindings(ctx, orgID); err == nil {
				freeSize = n
			} else {
				logger.Warn(ctx, "admin_tenant_pool_create_free_size_lookup_failed",
					zap.String("pool_id", poolID),
					zap.String("organization_id", orgID),
					zap.Error(err))
			}
		}
		s.recordTenantPoolCapacity(poolID, orgID, *req.PoolSize, freeSize)
		writeJSON(w, http.StatusAccepted, adminTenantPoolResponse{
			PoolID:         poolID,
			OrganizationID: orgID,
			PoolSize:       *req.PoolSize,
			FreeSize:       freeSize,
			Status:         adminTenantPoolDisplayStatus(meta.TenantPoolActive, freeSize, slotSize),
		})
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_create_done",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", poolID,
			"organization_id", orgID,
			"pool_size", *req.PoolSize,
			"free_size", freeSize,
			"slot_size", slotSize,
			"status", adminTenantPoolDisplayStatus(meta.TenantPoolActive, freeSize, slotSize),
			"duration_ms", durationMillis(createStarted))...)
		return nil
	}); err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_create_failed",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_size", *req.PoolSize,
			"duration_ms", durationMillis(createStarted),
			"error", err)...)
		metricResult = adminTenantPoolMetricResult(err)
		writeAdminTenantPoolError(w, r, err)
		return
	}
}

func (s *Server) handleAdminTenantPoolGet(w http.ResponseWriter, r *http.Request) {
	cred, err := adminCredentialsFromHeaders(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	pool, ok := s.authorizedTenantPool(w, r, cred)
	if !ok {
		return
	}
	freeSize, slotSize, err := s.tenantPoolSizes(r.Context(), pool.OrganizationID)
	if err != nil {
		writeBackendError(w, r, err)
		return
	}
	s.recordTenantPoolCapacity(pool.PoolID, pool.OrganizationID, pool.Size, freeSize)
	writeJSON(w, http.StatusOK, adminTenantPoolResponse{
		PoolID:         pool.PoolID,
		OrganizationID: pool.OrganizationID,
		PoolSize:       pool.Size,
		FreeSize:       freeSize,
		Status:         adminTenantPoolDisplayStatus(pool.Status, freeSize, slotSize),
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
	pool, ok := s.authorizedTenantPool(w, r, cred)
	if !ok {
		return
	}
	updateStarted := time.Now()
	metricResult := "ok"
	defer func() {
		metrics.RecordOperation(adminTenantPoolMetricsComponent, "update", metricResult, time.Since(updateStarted))
	}()
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_update_requested",
		"provider", tenant.ProviderTiDBCloudNative,
		"pool_id", pool.PoolID,
		"organization_id", pool.OrganizationID,
		"current_pool_size", pool.Size,
		"target_pool_size", *req.PoolSize,
		"pool_status", pool.Status)...)
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
		slotSize, err := s.meta.CountTenantPoolFreeSlots(ctx, pool.OrganizationID)
		if err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "tenant pool slot size lookup failed")
		}
		targetSize := *req.PoolSize
		if targetSize > slotSize {
			stageStarted := time.Now()
			growCount := targetSize - slotSize
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_grow_started",
				"provider", tenant.ProviderTiDBCloudNative,
				"pool_id", pool.PoolID,
				"organization_id", pool.OrganizationID,
				"current_pool_size", pool.Size,
				"target_pool_size", targetSize,
				"free_size", freeSize,
				"slot_size", slotSize,
				"grow_count", growCount)...)
			results, err := s.createFreePoolTenants(ctx, pool.PoolID, growCount, cred, nil)
			if err != nil {
				logger.Error(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_grow_failed",
					"provider", tenant.ProviderTiDBCloudNative,
					"pool_id", pool.PoolID,
					"organization_id", pool.OrganizationID,
					"target_pool_size", targetSize,
					"grow_count", growCount,
					"duration_ms", durationMillis(stageStarted),
					"error", err)...)
				status, msg := clientFacingErrorResponse(http.StatusBadGateway, "grow tenant pool failed", err)
				return adminTenantPoolError(status, msg)
			}
			for _, res := range results {
				s.startProvisionedTenantSchemaInit(ctx, res)
			}
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_grow_done",
				"provider", tenant.ProviderTiDBCloudNative,
				"pool_id", pool.PoolID,
				"organization_id", pool.OrganizationID,
				"target_pool_size", targetSize,
				"grow_count", growCount,
				"created_count", len(results),
				"duration_ms", durationMillis(stageStarted))...)
		} else if targetSize < slotSize {
			stageStarted := time.Now()
			shrinkCount := slotSize - targetSize
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_shrink_started",
				"provider", tenant.ProviderTiDBCloudNative,
				"pool_id", pool.PoolID,
				"organization_id", pool.OrganizationID,
				"current_pool_size", pool.Size,
				"target_pool_size", targetSize,
				"free_size", freeSize,
				"slot_size", slotSize,
				"shrink_count", shrinkCount)...)
			deleted, err := s.deleteNewestFreePoolTenants(ctx, pool.PoolID, pool.OrganizationID, shrinkCount, cred, false)
			if err != nil {
				if actualSlotSize, countErr := s.meta.CountTenantPoolFreeSlots(ctx, pool.OrganizationID); countErr == nil {
					if updateErr := s.meta.UpdateTenantPoolSize(ctx, pool.PoolID, actualSlotSize); updateErr != nil {
						logger.Warn(ctx, "admin_tenant_pool_shrink_partial_size_update_failed", zap.String("pool_id", pool.PoolID), zap.Int("slot_size", actualSlotSize), zap.Error(updateErr))
					}
				} else {
					logger.Warn(ctx, "admin_tenant_pool_shrink_partial_count_failed", zap.String("pool_id", pool.PoolID), zap.Error(countErr))
				}
				logger.Error(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_shrink_failed",
					"provider", tenant.ProviderTiDBCloudNative,
					"pool_id", pool.PoolID,
					"organization_id", pool.OrganizationID,
					"target_pool_size", targetSize,
					"shrink_count", shrinkCount,
					"deleted_count", deleted,
					"duration_ms", durationMillis(stageStarted),
					"error", err)...)
				status, msg := clientFacingErrorResponse(http.StatusBadGateway, "shrink tenant pool failed", err)
				return adminTenantPoolError(status, msg)
			}
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_shrink_done",
				"provider", tenant.ProviderTiDBCloudNative,
				"pool_id", pool.PoolID,
				"organization_id", pool.OrganizationID,
				"target_pool_size", targetSize,
				"shrink_count", shrinkCount,
				"deleted_count", deleted,
				"duration_ms", durationMillis(stageStarted))...)
		} else {
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_noop",
				"provider", tenant.ProviderTiDBCloudNative,
				"pool_id", pool.PoolID,
				"organization_id", pool.OrganizationID,
				"current_pool_size", pool.Size,
				"target_pool_size", targetSize,
				"free_size", freeSize,
				"slot_size", slotSize)...)
		}
		stageStarted := time.Now()
		if err := s.meta.UpdateTenantPoolSize(ctx, pool.PoolID, targetSize); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "failed to update tenant pool")
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_update_size_persisted",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", pool.PoolID,
			"organization_id", pool.OrganizationID,
			"target_pool_size", targetSize,
			"duration_ms", durationMillis(stageStarted))...)
		if freeSize, err = s.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "tenant pool free size lookup failed")
		}
		if slotSize, err = s.meta.CountTenantPoolFreeSlots(ctx, pool.OrganizationID); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "tenant pool slot size lookup failed")
		}
		out = adminTenantPoolResponse{
			PoolID:         pool.PoolID,
			OrganizationID: pool.OrganizationID,
			PoolSize:       targetSize,
			FreeSize:       freeSize,
			Status:         adminTenantPoolDisplayStatus(pool.Status, freeSize, slotSize),
		}
		s.recordTenantPoolCapacity(pool.PoolID, pool.OrganizationID, targetSize, freeSize)
		return nil
	}); err != nil {
		metricResult = adminTenantPoolMetricResult(err)
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_update_failed",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", pool.PoolID,
			"organization_id", pool.OrganizationID,
			"current_pool_size", pool.Size,
			"target_pool_size", *req.PoolSize,
			"duration_ms", durationMillis(updateStarted),
			"error", err)...)
		writeAdminTenantPoolError(w, r, err)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_update_done",
		"provider", tenant.ProviderTiDBCloudNative,
		"pool_id", out.PoolID,
		"organization_id", out.OrganizationID,
		"target_pool_size", out.PoolSize,
		"free_size", out.FreeSize,
		"status", out.Status,
		"duration_ms", durationMillis(updateStarted))...)
	writeJSON(w, http.StatusAccepted, out)
}

func (s *Server) validateTenantPoolSize(size int) error {
	maxSize := s.tenantPoolMaxSize
	if maxSize <= 0 {
		maxSize = DefaultTenantPoolMaxSize
	}
	if size > maxSize {
		return fmt.Errorf("pool_size %d exceeds maximum %d", size, maxSize)
	}
	return nil
}

func adminTenantPoolDisplayStatus(status meta.TenantPoolStatus, freeSize, slotSize int) adminTenantPoolStatus {
	if status == meta.TenantPoolActive && freeSize == 0 && slotSize > 0 {
		return adminTenantPoolStatusCreating
	}
	return adminTenantPoolStatus(status)
}

func adminTenantPoolMetricResult(err error) string {
	if err == nil {
		return "ok"
	}
	var httpErr *adminTenantPoolHTTPError
	if errors.As(err, &httpErr) && httpErr.status == http.StatusBadGateway {
		return "cluster_error"
	}
	return metrics.ResultForError(err)
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
	pool, ok := s.authorizedTenantPool(w, r, cred)
	if !ok {
		return
	}
	deleteStarted := time.Now()
	metricResult := "ok"
	defer func() {
		metrics.RecordOperation(adminTenantPoolMetricsComponent, "delete", metricResult, time.Since(deleteStarted))
	}()
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_delete_requested",
		"provider", tenant.ProviderTiDBCloudNative,
		"pool_id", pool.PoolID,
		"organization_id", pool.OrganizationID,
		"pool_size", pool.Size,
		"pool_status", pool.Status)...)
	lock := s.tenantPoolLock(pool.PoolID)
	lock.Lock()
	defer lock.Unlock()

	var out adminTenantPoolResponse
	if err := s.meta.WithTenantPoolLock(r.Context(), pool.PoolID, func(ctx context.Context) error {
		stageStarted := time.Now()
		if err := s.meta.UpdateTenantPoolStatus(ctx, pool.PoolID, meta.TenantPoolDeleting); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "failed to mark tenant pool deleting")
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_delete_pool_marked_deleting",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", pool.PoolID,
			"organization_id", pool.OrganizationID,
			"status", meta.TenantPoolDeleting,
			"duration_ms", durationMillis(stageStarted))...)
		stageStarted = time.Now()
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_delete_free_tenants_started",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", pool.PoolID,
			"organization_id", pool.OrganizationID)...)
		deleted, err := s.deleteNewestFreePoolTenants(ctx, pool.PoolID, pool.OrganizationID, 0, cred, true)
		if err != nil {
			status, msg := clientFacingErrorResponse(http.StatusBadGateway, "delete tenant pool failed", err)
			return adminTenantPoolError(status, msg)
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_delete_free_tenants_done",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", pool.PoolID,
			"organization_id", pool.OrganizationID,
			"deleted_free_tenants", deleted,
			"duration_ms", durationMillis(stageStarted))...)
		stageStarted = time.Now()
		if err := s.meta.DeleteTenantPoolAndDetachUsedMembers(ctx, pool.PoolID); err != nil {
			return adminTenantPoolError(http.StatusInternalServerError, "failed to delete tenant pool")
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_delete_metadata_deleted",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", pool.PoolID,
			"organization_id", pool.OrganizationID,
			"duration_ms", durationMillis(stageStarted))...)
		out = adminTenantPoolResponse{
			PoolID:         pool.PoolID,
			OrganizationID: pool.OrganizationID,
			PoolSize:       pool.Size,
			FreeSize:       0,
			Status:         adminTenantPoolStatus(meta.TenantPoolDeleting),
		}
		return nil
	}); err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_delete_failed",
			"provider", tenant.ProviderTiDBCloudNative,
			"pool_id", pool.PoolID,
			"organization_id", pool.OrganizationID,
			"duration_ms", durationMillis(deleteStarted),
			"error", err)...)
		metricResult = adminTenantPoolMetricResult(err)
		writeAdminTenantPoolError(w, r, err)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "admin_tenant_pool_delete_done",
		"provider", tenant.ProviderTiDBCloudNative,
		"pool_id", pool.PoolID,
		"organization_id", pool.OrganizationID,
		"pool_size", pool.Size,
		"free_size", 0,
		"status", meta.TenantPoolDeleting,
		"duration_ms", durationMillis(deleteStarted))...)
	s.recordTenantPoolCapacity(pool.PoolID, pool.OrganizationID, 0, 0)
	writeJSON(w, http.StatusAccepted, out)
}

func (s *Server) authorizedTenantPool(w http.ResponseWriter, r *http.Request, cred tenant.CredentialProvisionRequest) (*meta.TenantPool, bool) {
	orgID, err := s.firstManagedOrganization(r.Context(), cred)
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "authorize tenant pool")
		return nil, false
	}
	if orgID == "" {
		errJSON(w, http.StatusNotFound, "tenant pool not found")
		return nil, false
	}
	pool, err := s.meta.GetTenantPoolByOrganization(r.Context(), orgID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "tenant pool not found")
			return nil, false
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "tenant pool lookup failed")
		return nil, false
	}
	return pool, true
}

func (s *Server) tenantPoolSizes(ctx context.Context, organizationID string) (int, int, error) {
	freeSize, err := s.meta.CountFreeTenantPoolBindings(ctx, organizationID)
	if err != nil {
		return 0, 0, fmt.Errorf("tenant pool free size lookup failed")
	}
	slotSize, err := s.meta.CountTenantPoolFreeSlots(ctx, organizationID)
	if err != nil {
		return 0, 0, fmt.Errorf("tenant pool slot size lookup failed")
	}
	return freeSize, slotSize, nil
}

func (s *Server) recordTenantPoolCapacity(poolID, organizationID string, size, freeSize int) {
	if poolID == "" || organizationID == "" {
		return
	}
	if size < 0 {
		size = 0
	}
	if freeSize < 0 {
		freeSize = 0
	}
	metrics.RecordTenantPoolCapacity(poolID, organizationID, "size", float64(size))
	metrics.RecordTenantPoolCapacity(poolID, organizationID, "free", float64(freeSize))
}

func (s *Server) refreshTenantPoolCapacity(ctx context.Context, pool *meta.TenantPool) {
	if s == nil || s.meta == nil || pool == nil || pool.OrganizationID == "" {
		return
	}
	freeSize, err := s.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID)
	if err != nil {
		logger.Warn(ctx, "admin_tenant_pool_capacity_refresh_failed",
			zap.String("pool_id", pool.PoolID),
			zap.String("organization_id", pool.OrganizationID),
			zap.Error(err))
		return
	}
	s.recordTenantPoolCapacity(pool.PoolID, pool.OrganizationID, pool.Size, freeSize)
}

func (s *Server) firstManagedOrganization(ctx context.Context, cred tenant.CredentialProvisionRequest) (string, error) {
	identity, err := s.resolveTiDBCloudIdentity(ctx, cred, "provision_org_resolve")
	if err != nil {
		return "", err
	}
	return identity.OrganizationID, nil
}

func (s *Server) createFreePoolTenants(ctx context.Context, poolID string, count int, cred tenant.CredentialProvisionRequest, quotaOpt *quotaRequest) ([]*provisionTenantResult, error) {
	if s.defaultTenantProvider == tenant.ProviderTiDBCloudNativeShared {
		return s.createFreeSharedPoolTenants(ctx, poolID, count, cred, quotaOpt)
	}
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
	stageStarted := time.Now()
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_free_tenants_insert_started",
		"provider", provider,
		"pool_id", poolID,
		"count", count)...)
	for i := 0; i < count; i++ {
		tenantID := token.NewID()
		if err := s.insertPendingPoolTenant(ctx, tenantID, provider, now); err != nil {
			s.cleanupPoolProvisionedClusters(ctx, nil, cred, tenantIDs, "insert_pending_tenant_error")
			return nil, err
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_free_tenants_insert_done",
		"provider", provider,
		"pool_id", poolID,
		"count", len(tenantIDs),
		"duration_ms", durationMillis(stageStarted))...)
	var opts tenant.QuotaUpdateOptions
	if quotaOpt != nil {
		opts.TiDBCloudSpendingLimitMonthly = quotaOpt.TiDBCloudSpendingLimit
	}
	opts.TenantPoolID = poolID
	stageStarted = time.Now()
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_batch_create_started",
		"provider", provider,
		"pool_id", poolID,
		"count", len(tenantIDs),
		"quota_requested", quotaOpt != nil)...)
	clusters, batchCloudCfg, err := manager.BatchProvisionFreeClustersWithCredentialsAndQuota(ctx, tenantIDs, cred, opts)
	if err != nil && len(clusters) == 0 {
		logger.Error(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_batch_create_failed",
			"provider", provider,
			"pool_id", poolID,
			"count", len(tenantIDs),
			"duration_ms", durationMillis(stageStarted),
			"error", err)...)
		s.cleanupPoolProvisionedClusters(ctx, clusters, cred, tenantIDs, "batch_provision_error")
		for _, tenantID := range tenantIDs {
			s.markTenantPoolTenantFailed(ctx, tenantID, "batch_provision_error")
		}
		return nil, err
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_batch_create_done",
		"provider", provider,
		"pool_id", poolID,
		"requested_count", len(tenantIDs),
		"cluster_count", len(clusters),
		"duration_ms", durationMillis(stageStarted),
		"has_error", err != nil)...)
	if err != nil {
		logger.Warn(ctx, "admin_tenant_pool_batch_metadata_incomplete",
			zap.String("pool_id", poolID),
			zap.Int("cluster_count", len(clusters)),
			zap.Error(err))
	}
	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			s.cleanupPoolProvisionedClusters(ctx, clusters, cred, tenantIDs, "metadata_error")
		}
	}()
	results := make([]*provisionTenantResult, 0, len(clusters))
	pendingResume := make([]*tenant.ClusterInfo, 0, len(clusters))
	persistedTenants := make(map[string]struct{}, len(clusters))
	discardedTenants := make(map[string]struct{}, len(clusters))
	cloudProvider, region := provisioningCloudRegion(s.provisioner)
	poolOrgID := ""
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		tenantID := strings.TrimSpace(cluster.TenantID)
		if tenantID == "" {
			logger.Warn(ctx, "admin_tenant_pool_cluster_tenant_missing",
				zap.String("pool_id", poolID),
				zap.String("cluster_id", cluster.ClusterID))
			s.cleanupPoolProvisionedClusters(ctx, []*tenant.ClusterInfo{cluster}, cred, nil, "cluster_tenant_missing")
			continue
		}
		orgID := strings.TrimSpace(cluster.OrganizationID)
		if orgID == "" {
			if poolOrgID == "" {
				pool, lookupErr := s.meta.GetTenantPoolByID(ctx, poolID)
				if lookupErr != nil && !errors.Is(lookupErr, meta.ErrNotFound) {
					cleanupOnError = true
					return nil, lookupErr
				}
				if pool != nil {
					poolOrgID = strings.TrimSpace(pool.OrganizationID)
				}
			}
			orgID = poolOrgID
		}
		if orgID == "" {
			logger.Warn(ctx, "admin_tenant_pool_cluster_org_missing",
				zap.String("pool_id", poolID),
				zap.String("tenant_id", tenantID),
				zap.String("cluster_id", cluster.ClusterID))
			s.cleanupPoolProvisionedClusters(ctx, []*tenant.ClusterInfo{cluster}, cred, []string{tenantID}, "cluster_org_missing")
			discardedTenants[tenantID] = struct{}{}
			continue
		}
		cluster.OrganizationID = orgID
		persistedTenants[tenantID] = struct{}{}
		stageStarted = time.Now()
		if err := s.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
			TenantID:       tenantID,
			OrganizationID: orgID,
			ClusterID:      cluster.ClusterID,
			PoolID:         poolID,
			PoolStatus:     meta.TenantPoolBindingFree,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			cleanupOnError = true
			return nil, err
		}
		logProvisionStage(ctx, "admin_tenant_pool_free_tenant_binding_persisted", tenantID, provider, stageStarted,
			"pool_id", poolID,
			"organization_id", orgID,
			"cluster_id", cluster.ClusterID)
		res := &provisionTenantResult{
			TenantID:       tenantID,
			Status:         meta.TenantPending,
			Provider:       provider,
			CloudProvider:  cloudProvider,
			Region:         region,
			OrganizationID: orgID,
		}
		if poolClusterConnectionReady(cluster) {
			stageStarted = time.Now()
			if err := s.persistPoolTenantConnection(ctx, cluster, provider); err != nil {
				cleanupOnError = true
				return nil, err
			}
			if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantProvisioning); err != nil {
				cleanupOnError = true
				return nil, err
			}
			res.Status = meta.TenantProvisioning
			dbtls := dbTLSForProvisionedTenant(provider)
			res.TenantDSN = tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, cluster.DBName, dbtls, provider)
			logProvisionStage(ctx, "admin_tenant_pool_free_tenant_connection_persisted", tenantID, provider, stageStarted,
				"pool_id", poolID,
				"organization_id", orgID,
				"cluster_id", cluster.ClusterID,
				"db_tls", dbtls,
				"status", meta.TenantProvisioning)
		} else {
			stageStarted = time.Now()
			if err := s.persistPoolTenantProvisionSeed(ctx, cluster, provider); err != nil {
				cleanupOnError = true
				return nil, err
			}
			pendingResume = append(pendingResume, cluster)
			logProvisionStage(ctx, "admin_tenant_pool_free_tenant_seed_persisted", tenantID, provider, stageStarted,
				"pool_id", poolID,
				"organization_id", orgID,
				"cluster_id", cluster.ClusterID,
				"status", meta.TenantPending)
		}
		if quotaOpt != nil {
			stageStarted = time.Now()
			quotaReq := *quotaOpt
			quotaReq.TenantID = tenantID
			if err := s.applyQuotaLocalConfig(ctx, "pool_create", tenantID, quotaReq); err != nil {
				cleanupOnError = true
				return nil, err
			}
			logProvisionStage(ctx, "admin_tenant_pool_free_tenant_quota_local_config_applied", tenantID, provider, stageStarted,
				"pool_id", poolID,
				"organization_id", orgID,
				"cluster_id", cluster.ClusterID)
		}
		if batchCloudCfg != nil && batchCloudCfg.TiDBCloudSpendingLimitMonthly != nil {
			if err := s.syncTiDBCloudSpendingLimit(ctx, "pool_create", tenantID, batchCloudCfg, time.Time{}); err != nil {
				logger.Warn(ctx, "admin_tenant_pool_spending_limit_sync_failed",
					zap.String("tenant_id", tenantID),
					zap.String("pool_id", poolID),
					zap.String("organization_id", orgID),
					zap.String("cluster_id", cluster.ClusterID),
					zap.Error(err))
			}
		}
		results = append(results, res)
	}
	// A partial TiDB Cloud response can omit some requested tenant IDs. Those
	// pending local tenants have no recoverable cluster metadata, so fail them
	// while preserving tenants that were persisted above.
	for _, tenantID := range tenantIDs {
		if _, ok := persistedTenants[tenantID]; ok {
			continue
		}
		if _, ok := discardedTenants[tenantID]; ok {
			continue
		}
		s.markTenantPoolTenantFailed(ctx, tenantID, "missing_cluster_response")
	}
	if len(clusters) > 0 && len(results) == 0 {
		cleanupOnError = false
		return nil, fmt.Errorf("no tenant pool clusters could be persisted")
	}
	cleanupOnError = false
	s.startPoolClustersMetadataResume(ctx, poolID, pendingResume, cred)
	return results, nil
}

func (s *Server) createFreeSharedPoolTenants(ctx context.Context, poolID string, count int, cred tenant.CredentialProvisionRequest, quotaOpt *quotaRequest) ([]*provisionTenantResult, error) {
	if count <= 0 {
		return []*provisionTenantResult{}, nil
	}
	if _, ok := s.provisioner.(tenant.SharedDBPoolProvisioner); !ok {
		return nil, fmt.Errorf("shared tenant pool provisioning not enabled")
	}
	pool, err := s.meta.GetTenantPoolByID(ctx, poolID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	createdPoolIDs := make(map[int64]struct{})
	provisioningPoolIDs := make(map[int64]struct{})
	results := make([]*provisionTenantResult, 0, count)
	for i := 0; i < count; i++ {
		tenantID := token.NewID()
		if err := s.insertPendingPoolTenant(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared, now); err != nil {
			return results, err
		}
		fsID, err := s.meta.EnsureFsID(ctx, tenantID)
		if err != nil {
			_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
			return results, err
		}
		opts := provisionTenantOptions{Quota: quotaOpt}
		if err := s.materializeSharedTenantQuota(ctx, tenantID, opts); err != nil {
			_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
			return results, err
		}
		var selected *meta.SharedDB
		selected, created, err := s.allocateManagedSharedDB(ctx, cred, func(db *meta.SharedDB) error {
			return s.meta.CompleteSharedTenantPoolMember(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared,
				&meta.TenantPlacement{FsID: fsID, DbID: db.ID, Placement: meta.PlacementShared,
					SchemaShape: meta.SchemaShapeShared, Status: meta.SharedDBStatusActive},
				&meta.TenantPoolMembership{TenantID: tenantID, TiDBCloudOrganizationID: pool.OrganizationID,
					PoolID: poolID, PoolStatus: meta.TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now})
		})
		if err != nil {
			_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
			return results, err
		}
		if created {
			createdPoolIDs[selected.ID] = struct{}{}
		}
		if selected.Status == meta.SharedDBStatusProvisioning {
			provisioningPoolIDs[selected.ID] = struct{}{}
		}
		resultStatus := meta.TenantProvisioning
		if selected.Status == meta.SharedDBStatusActive {
			resultStatus = meta.TenantActive
		}
		results = append(results, &provisionTenantResult{TenantID: tenantID,
			Status: resultStatus, Provider: tenant.ProviderTiDBCloudNativeShared,
			CloudProvider: selected.CloudProvider, Region: selected.Region,
			OrganizationID: selected.TiDBCloudOrganizationID})
	}
	createdIDs := make([]int64, 0, len(createdPoolIDs))
	for id := range createdPoolIDs {
		createdIDs = append(createdIDs, id)
	}
	sort.Slice(createdIDs, func(i, j int) bool { return createdIDs[i] < createdIDs[j] })
	resolvedOrg, err := s.provisionManagedSharedDBPoolsBatch(ctx, createdIDs)
	if err != nil {
		return results, err
	}
	if pool.OrganizationID == "" && resolvedOrg != "" {
		if err := s.meta.UpdateTenantPoolOrganization(ctx, poolID, resolvedOrg); err != nil {
			return results, err
		}
		if err := s.meta.UpdateTenantPoolMembershipOrganization(ctx, poolID, resolvedOrg); err != nil {
			return results, err
		}
		for _, result := range results {
			result.OrganizationID = resolvedOrg
		}
	}
	provisioningIDs := make([]int64, 0, len(provisioningPoolIDs))
	for dbID := range provisioningPoolIDs {
		provisioningIDs = append(provisioningIDs, dbID)
	}
	s.scheduleManagedSharedDBContinuations(ctx, provisioningIDs)
	return results, nil
}

func (s *Server) provisionManagedSharedDBPoolsBatch(ctx context.Context, dbIDs []int64) (string, error) {
	if len(dbIDs) == 0 {
		return "", nil
	}
	provisioner := s.provisioner.(tenant.SharedDBPoolProvisioner)
	resolvedOrg := ""
	for start := 0; start < len(dbIDs); start += 10 {
		end := start + 10
		if end > len(dbIDs) {
			end = len(dbIDs)
		}
		chunkOrg, err := s.provisionManagedSharedDBPoolsBatchChunk(ctx, provisioner, dbIDs[start:end])
		if err != nil {
			return resolvedOrg, err
		}
		if resolvedOrg == "" {
			resolvedOrg = chunkOrg
		}
	}
	return resolvedOrg, nil
}

func (s *Server) provisionManagedSharedDBPoolsBatchChunk(ctx context.Context, provisioner tenant.SharedDBPoolProvisioner, dbIDs []int64) (string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		first, err := s.meta.GetSharedDB(ctx, dbIDs[0])
		if err != nil {
			return "", err
		}
		cred, err := s.sharedDBCloudCredentials(ctx)
		if err != nil {
			return "", err
		}
		identity := sharedDBAllocationIdentity(first.TiDBCloudOrganizationID, first.ProvisioningKey)
		restartWithOrganization := false
		resolvedOrg := ""
		err = s.meta.WithSharedDBAllocationLock(ctx, identity, func(lockCtx context.Context) error {
			current, err := s.meta.GetSharedDB(lockCtx, dbIDs[0])
			if err != nil {
				return err
			}
			if strings.TrimSpace(first.TiDBCloudOrganizationID) != strings.TrimSpace(current.TiDBCloudOrganizationID) {
				restartWithOrganization = true
				return nil
			}
			var createErr error
			resolvedOrg, createErr = s.provisionManagedSharedDBPoolsBatchLocked(lockCtx, provisioner, dbIDs, cred)
			return createErr
		})
		if err != nil {
			return resolvedOrg, err
		}
		if !restartWithOrganization {
			return resolvedOrg, nil
		}
	}
	return "", fmt.Errorf("shared DB pool batch allocation identity kept changing")
}

func (s *Server) provisionManagedSharedDBPoolsBatchLocked(ctx context.Context, provisioner tenant.SharedDBPoolProvisioner, dbIDs []int64, cred tenant.CredentialProvisionRequest) (string, error) {
	requests := make([]tenant.SharedDBPoolCreateRequest, 0, len(dbIDs))
	rows := make(map[int64]*meta.SharedDB, len(dbIDs))
	resolvedOrg := ""
	for _, dbID := range dbIDs {
		row, err := s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			return resolvedOrg, err
		}
		if resolvedOrg == "" {
			resolvedOrg = row.TiDBCloudOrganizationID
		}
		// A concurrent direct continuation may have completed physical creation
		// before this batch acquired the organization lock. Its cluster must be
		// adopted/refreshed by the normal continuation, never created again.
		if row.ClusterID != "" {
			continue
		}
		plain, err := s.pool.Decrypt(ctx, row.PasswordCipher)
		if err != nil {
			return resolvedOrg, err
		}
		if row.SpendingLimit == nil {
			return resolvedOrg, fmt.Errorf("managed db pool %d has no spending target", dbID)
		}
		rows[dbID] = row
		requests = append(requests, tenant.SharedDBPoolCreateRequest{DBPoolID: dbID, DBPoolUUID: row.UUID,
			DatabaseName: row.Name, RootPassword: string(plain), SpendingLimitMonthly: *row.SpendingLimit})
	}
	if len(requests) == 0 {
		return resolvedOrg, nil
	}
	created, createErr := provisioner.BatchProvisionSharedDBPoolsWithCredentials(ctx, requests, cred)
	if createErr != nil && len(created) == 0 {
		logger.Warn(ctx, "managed_shared_db_batch_create_failed",
			zap.Int("db_pool_count", len(requests)), zap.Error(createErr))
		return resolvedOrg, createErr
	}
	for _, info := range created {
		if info == nil || rows[info.DBPoolID] == nil || rows[info.DBPoolID].UUID != info.DBPoolUUID {
			return resolvedOrg, fmt.Errorf("shared db batch returned an unknown db pool")
		}
		row := rows[info.DBPoolID]
		logicalOrganizationID := strings.TrimSpace(row.TiDBCloudOrganizationID)
		if logicalOrganizationID == "" {
			return resolvedOrg, fmt.Errorf("managed shared db pool customer organization is required")
		}
		if info.DBName == "" {
			info.DBName = row.Name
		}
		if err := s.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{ID: info.DBPoolID,
			TiDBCloudOrganizationID: logicalOrganizationID, ClusterID: info.ClusterID, Host: info.Host,
			Port: info.Port, User: info.Username, PasswordCipher: row.PasswordCipher, Name: info.DBName,
			TLSMode: map[bool]string{true: "true", false: "skip-verify"}[dbTLSForProvisionedTenant(tenant.ProviderTiDBCloudNativeShared)]}); err != nil {
			return resolvedOrg, err
		}
		if resolvedOrg == "" {
			resolvedOrg = logicalOrganizationID
		}
	}
	if createErr != nil {
		logger.Warn(ctx, "managed_shared_db_batch_create_partial",
			zap.Int("requested", len(requests)), zap.Int("persisted", len(created)), zap.Error(createErr))
	}
	return resolvedOrg, nil
}

func (s *Server) markTenantPoolTenantFailed(ctx context.Context, tenantID, reason string) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return
	}
	if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed); err != nil {
		logger.Warn(ctx, "admin_tenant_pool_mark_failed_failed",
			zap.String("tenant_id", tenantID),
			zap.String("reason", reason),
			zap.Error(err))
	}
}

func (s *Server) persistPoolTenantProvisionSeed(ctx context.Context, cluster *tenant.ClusterInfo, provider string) error {
	if cluster == nil {
		return fmt.Errorf("cluster info is required")
	}
	if strings.TrimSpace(cluster.TenantID) == "" {
		return fmt.Errorf("cluster tenant id is required")
	}
	if strings.TrimSpace(cluster.ClusterID) == "" {
		return fmt.Errorf("cluster id is required")
	}
	if strings.TrimSpace(cluster.Password) == "" {
		return fmt.Errorf("cluster password is required")
	}
	cipherPass, err := s.pool.Encrypt(ctx, []byte(cluster.Password))
	if err != nil {
		return err
	}
	return s.meta.UpdateTenantConnection(ctx, cluster.TenantID, &meta.Tenant{
		DBPasswordCipher: cipherPass,
		DBName:           cluster.DBName,
		DBTLS:            dbTLSForProvisionedTenant(provider),
		Provider:         provider,
		ClusterID:        cluster.ClusterID,
	})
}

func (s *Server) persistPoolTenantConnection(ctx context.Context, cluster *tenant.ClusterInfo, provider string) error {
	if cluster == nil {
		return fmt.Errorf("cluster info is required")
	}
	if !poolClusterConnectionReady(cluster) {
		return fmt.Errorf("cluster connection metadata is incomplete")
	}
	if strings.TrimSpace(cluster.Password) == "" {
		return fmt.Errorf("cluster password is required")
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
		DBTLS:            dbTLSForProvisionedTenant(provider),
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

func (s *Server) startPoolClustersMetadataResume(ctx context.Context, poolID string, clusters []*tenant.ClusterInfo, cred tenant.CredentialProvisionRequest) {
	clusters = compactPoolResumeClusters(clusters)
	if len(clusters) == 0 {
		return
	}
	job := &tenantPoolResumeJob{}
	if poolID != "" {
		actual, loaded := s.tenantPoolResumeJobs.LoadOrStore(poolID, job)
		if loaded {
			if existing, ok := actual.(*tenantPoolResumeJob); ok {
				existing.rerun.Store(true)
			}
			logger.Info(ctx, "admin_tenant_pool_metadata_resume_skipped",
				zap.String("pool_id", poolID),
				zap.String("reason", "metadata_resume_already_running_rerun_requested"),
				zap.Int("cluster_count", len(clusters)))
			return
		}
	}
	clusterCopies := make([]*tenant.ClusterInfo, 0, len(clusters))
	for _, cluster := range clusters {
		copy := *cluster
		clusterCopies = append(clusterCopies, &copy)
	}
	s.startServerWorker(ctx, func(workerCtx context.Context) {
		if poolID != "" {
			defer s.tenantPoolResumeJobs.Delete(poolID)
		}
		workerCtx, cancel := context.WithTimeout(workerCtx, tenantPoolMetadataResumeWaitTimeout)
		defer cancel()

		for {
			started := time.Now()
			s.resumePoolClustersMetadataGroups(workerCtx, started, poolID, clusterCopies, cred)
			if poolID == "" || !job.rerun.Swap(false) {
				return
			}
			next, err := s.pendingTenantPoolResumeClusters(workerCtx, poolID, len(clusterCopies))
			if err != nil {
				logger.Warn(workerCtx, "admin_tenant_pool_metadata_resume_rerun_list_failed",
					zap.String("pool_id", poolID),
					zap.Error(err))
				return
			}
			clusterCopies = next
			if len(clusterCopies) == 0 {
				return
			}
		}
	})
}

func (s *Server) resumePoolClustersMetadataGroups(ctx context.Context, started time.Time, poolID string, clusters []*tenant.ClusterInfo, cred tenant.CredentialProvisionRequest) {
	waitStarted := time.Now()
	groups := poolMetadataResumeGroups(clusters, tenantPoolMetadataResumeGroupSize)
	if len(groups) == 0 {
		return
	}
	overallResult := "ok"
	var overallMu sync.Mutex
	recordGroupResult := func(result string) {
		overallMu.Lock()
		defer overallMu.Unlock()
		if adminTenantPoolMetadataResumeResultRank(result) > adminTenantPoolMetadataResumeResultRank(overallResult) {
			overallResult = result
		}
	}
	var wg sync.WaitGroup
	for i, group := range groups {
		groupIndex := i
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			groupStarted := time.Now()
			updated, err := s.waitForPoolClustersMetadata(ctx, group, cred)
			groupResult := metrics.ResultForError(err)
			groupDuration := time.Since(groupStarted)
			metrics.RecordOperation(adminTenantPoolMetricsComponent, "metadata_resume_group_wait", groupResult, groupDuration)
			metrics.RecordTenantPoolMetadataResumeWait(poolID, poolResumeOrganizationID(group), "group", groupResult, groupDuration)
			recordGroupResult(groupResult)
			if err != nil {
				logger.Warn(ctx, "admin_tenant_pool_metadata_resume_batch_failed",
					zap.String("pool_id", poolID),
					zap.Int("group_index", groupIndex),
					zap.Int("group_count", len(groups)),
					zap.Int("cluster_count", len(group)),
					zap.Strings("tenant_ids", poolResumeTenantIDs(group)),
					zap.Strings("cluster_ids", poolResumeClusterIDs(group)),
					zap.Error(err))
			}
			for _, cluster := range updated {
				s.completePoolClusterMetadataResume(ctx, started, cluster)
			}
		}()
	}
	wg.Wait()
	waitDuration := time.Since(waitStarted)
	metrics.RecordOperation(adminTenantPoolMetricsComponent, "metadata_resume_wait", overallResult, waitDuration)
	metrics.RecordTenantPoolMetadataResumeWait(poolID, poolResumeOrganizationID(clusters), "batch", overallResult, waitDuration)
}

func adminTenantPoolMetadataResumeResultRank(result string) int {
	switch result {
	case "ok":
		return 0
	case "canceled":
		return 1
	case "deadline_exceeded":
		return 2
	case "bad_conn":
		return 3
	case "error":
		return 4
	default:
		return 4
	}
}

func poolMetadataResumeGroups(clusters []*tenant.ClusterInfo, groupSize int) [][]*tenant.ClusterInfo {
	if groupSize <= 0 {
		groupSize = 10
	}
	groups := make([][]*tenant.ClusterInfo, 0, (len(clusters)+groupSize-1)/groupSize)
	for start := 0; start < len(clusters); start += groupSize {
		end := start + groupSize
		if end > len(clusters) {
			end = len(clusters)
		}
		groups = append(groups, clusters[start:end])
	}
	return groups
}

func poolResumeTenantIDs(clusters []*tenant.ClusterInfo) []string {
	out := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		out = append(out, strings.TrimSpace(cluster.TenantID))
	}
	return out
}

func poolResumeClusterIDs(clusters []*tenant.ClusterInfo) []string {
	out := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		out = append(out, strings.TrimSpace(cluster.ClusterID))
	}
	return out
}

func poolResumeOrganizationID(clusters []*tenant.ClusterInfo) string {
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		if orgID := strings.TrimSpace(cluster.OrganizationID); orgID != "" {
			return orgID
		}
	}
	return ""
}

func compactPoolResumeClusters(clusters []*tenant.ClusterInfo) []*tenant.ClusterInfo {
	out := clusters[:0]
	for _, cluster := range clusters {
		if cluster == nil || strings.TrimSpace(cluster.TenantID) == "" || strings.TrimSpace(cluster.ClusterID) == "" {
			continue
		}
		out = append(out, cluster)
	}
	return out
}

func (s *Server) waitForPoolClustersMetadata(ctx context.Context, clusters []*tenant.ClusterInfo, cred tenant.CredentialProvisionRequest) ([]*tenant.ClusterInfo, error) {
	if batchWaiter, ok := s.provisioner.(tenant.TenantPoolClusterMetadataBatchWaiter); ok {
		return batchWaiter.WaitForPoolClustersMetadata(ctx, clusters, cred)
	}
	waiter, ok := s.provisioner.(tenant.TenantPoolClusterMetadataWaiter)
	if !ok {
		return nil, fmt.Errorf("metadata waiter unavailable")
	}
	updated := make([]*tenant.ClusterInfo, 0, len(clusters))
	var errs []error
	for _, cluster := range clusters {
		got, err := waiter.WaitForPoolClusterMetadata(ctx, cluster, cred)
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %s cluster %s: %w", cluster.TenantID, cluster.ClusterID, err))
			continue
		}
		updated = append(updated, got)
	}
	return updated, errors.Join(errs...)
}

func (s *Server) completePoolClusterMetadataResume(ctx context.Context, started time.Time, updated *tenant.ClusterInfo) {
	ctx, cancel := s.tenantPoolMetadataResumePersistContext(ctx)
	defer cancel()

	if !poolClusterConnectionReady(updated) {
		if updated != nil {
			logger.Warn(ctx, "admin_tenant_pool_metadata_resume_incomplete",
				zap.String("tenant_id", updated.TenantID),
				zap.String("cluster_id", updated.ClusterID))
		}
		return
	}
	if err := s.persistPoolTenantConnection(ctx, updated, tenant.ProviderTiDBCloudNative); err != nil {
		logger.Warn(ctx, "admin_tenant_pool_metadata_resume_persist_failed",
			zap.String("tenant_id", updated.TenantID),
			zap.String("cluster_id", updated.ClusterID),
			zap.Error(err))
		return
	}
	statusUpdated, err := s.meta.UpdateTenantStatusIf(ctx, updated.TenantID, meta.TenantPending, meta.TenantProvisioning)
	if err != nil {
		logger.Warn(ctx, "admin_tenant_pool_metadata_resume_status_failed",
			zap.String("tenant_id", updated.TenantID),
			zap.String("cluster_id", updated.ClusterID),
			zap.Error(err))
		return
	}
	if !statusUpdated {
		logger.Info(ctx, "admin_tenant_pool_metadata_resume_status_skipped",
			zap.String("tenant_id", updated.TenantID),
			zap.String("cluster_id", updated.ClusterID),
			zap.String("reason", "status_changed"))
		return
	}
	cloudProvider, region := provisioningCloudRegion(s.provisioner)
	dbtls := dbTLSForProvisionedTenant(tenant.ProviderTiDBCloudNative)
	logProvisionStage(ctx, "admin_tenant_pool_metadata_resume_done", updated.TenantID, tenant.ProviderTiDBCloudNative, started, "cluster_id", updated.ClusterID, "organization_id", updated.OrganizationID)
	s.startProvisionedTenantSchemaInit(ctx, &provisionTenantResult{
		TenantID:       updated.TenantID,
		Status:         meta.TenantProvisioning,
		Provider:       tenant.ProviderTiDBCloudNative,
		TenantDSN:      tenantDSN(updated.Username, updated.Password, updated.Host, updated.Port, updated.DBName, dbtls, tenant.ProviderTiDBCloudNative),
		CloudProvider:  cloudProvider,
		Region:         region,
		OrganizationID: strings.TrimSpace(updated.OrganizationID),
	})
}

func (s *Server) tenantPoolMetadataResumePersistContext(ctx context.Context) (context.Context, context.CancelFunc) {
	parent := backgroundWithTrace(ctx)
	if s.forkWorkerCtx != nil {
		parent = contextWithTrace(s.forkWorkerCtx, ctx)
	}
	return context.WithTimeout(parent, tenantPoolMetadataResumePersistTimeout)
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

func (s *Server) deleteNewestFreePoolTenants(ctx context.Context, poolID, organizationID string, count int, cred tenant.CredentialProvisionRequest, deleteAll bool) (int, error) {
	if count <= 0 && !deleteAll {
		return 0, nil
	}
	remaining := count
	deleted := 0
	for deleteAll || remaining > 0 {
		var native *meta.TenantWithTiDBCloudOrgBinding
		var nativeRows []meta.TenantWithTiDBCloudOrgBinding
		var err error
		if deleteAll {
			nativeRows, err = s.meta.ListFreeTenantPoolBindingsForDelete(ctx, organizationID, true, 1)
		} else {
			nativeRows, err = s.meta.ListTenantPoolFreeSlotsForDelete(ctx, organizationID, true, 1)
		}
		if err != nil {
			return deleted, err
		}
		if len(nativeRows) > 0 {
			native = &nativeRows[0]
		}
		shared, sharedErr := s.meta.GetNewestFreeTenantPoolMembershipForDelete(ctx, poolID)
		if sharedErr != nil && !errors.Is(sharedErr, meta.ErrNotFound) {
			return deleted, sharedErr
		}
		if native == nil && shared == nil {
			if deleteAll {
				return deleted, nil
			}
			return deleted, fmt.Errorf("not enough free tenants to delete")
		}
		useShared := shared != nil && (native == nil || shared.Membership.CreatedAt.After(native.Binding.CreatedAt) ||
			(shared.Membership.CreatedAt.Equal(native.Binding.CreatedAt) && shared.Tenant.ID > native.Tenant.ID))
		if useShared {
			updated, err := s.meta.MarkFreeSharedTenantPoolTenantDeleting(ctx, shared.Tenant.ID, shared.Tenant.Status)
			if err != nil {
				return deleted, err
			}
			if !updated {
				continue
			}
			if err := s.deleteFreeSharedPoolTenant(ctx, &shared.Tenant); err != nil {
				_, _ = s.meta.UpdateTenantStatusIf(context.Background(), shared.Tenant.ID, meta.TenantDeleting, shared.Tenant.Status)
				return deleted, err
			}
		} else {
			t := native.Tenant
			updated, err := s.meta.MarkFreeTenantPoolTenantDeleting(ctx, t.ID, t.Status)
			if err != nil {
				return deleted, err
			}
			if !updated {
				continue
			}
			if err := s.deprovisionTenantCluster(ctx, &t, cred); err != nil {
				_, _ = s.meta.UpdateTenantStatusIf(context.Background(), t.ID, meta.TenantDeleting, t.Status)
				return deleted, err
			}
			_ = s.meta.RevokeTenantAPIKeys(ctx, t.ID)
			if err := s.meta.MarkTenantDeleted(ctx, t.ID); err != nil {
				_ = s.meta.UpdateTenantStatus(context.Background(), t.ID, meta.TenantFailed)
				return deleted, err
			}
		}
		deleted++
		if !deleteAll {
			remaining--
		}
	}
	return deleted, nil
}

func (s *Server) deleteFreeSharedPoolTenant(ctx context.Context, t *meta.Tenant) error {
	if t == nil {
		return meta.ErrNotFound
	}
	if err := s.pool.InvalidateAndWait(ctx, t.ID); err != nil {
		return err
	}
	fsID, err := s.meta.ResolveFsID(ctx, t.ID)
	if err != nil {
		return err
	}
	placement, err := s.meta.GetTenantPlacement(ctx, fsID)
	if err != nil && !errors.Is(err, meta.ErrNotFound) {
		return err
	}
	if placement != nil {
		dbPool, err := s.meta.GetSharedDB(ctx, placement.DbID)
		if err != nil {
			return err
		}
		if sharedDBConnectionReady(dbPool) {
			if err := s.pool.PurgeSharedTenant(ctx, fsID, placement.DbID); err != nil {
				return err
			}
		}
		if err := s.meta.DeleteTenantPlacementAndDecrCount(ctx, fsID, placement.DbID, s.sharedDBReopenRatio); err != nil {
			return err
		}
	}
	if err := s.meta.DeleteTenantPoolMembership(ctx, t.ID); err != nil {
		return err
	}
	_ = s.meta.RevokeTenantAPIKeys(ctx, t.ID)
	_, err = s.enqueueTenantDeleteJob(ctx, t)
	return err
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
		if t.Provider == tenant.ProviderTiDBCloudNativeShared {
			if err := s.deleteFreeSharedPoolTenant(cleanupCtx, t); err != nil {
				logger.Warn(cleanupCtx, "admin_tenant_pool_cleanup_shared_member_failed",
					zap.String("tenant_id", tenantID), zap.String("reason", reason), zap.Error(err))
				_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
			}
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
		replenishStarted := time.Now()
		metricResult := "ok"
		defer func() {
			metrics.RecordOperation(adminTenantPoolMetricsComponent, "replenish", metricResult, time.Since(replenishStarted))
		}()
		lock := s.tenantPoolLock(pool.PoolID)
		lock.Lock()
		defer lock.Unlock()
		if err := s.meta.WithTenantPoolLock(ctx, pool.PoolID, func(ctx context.Context) error {
			current, err := s.meta.GetTenantPoolByID(ctx, pool.PoolID)
			if err != nil {
				if !errors.Is(err, meta.ErrNotFound) {
					logger.Warn(ctx, "admin_tenant_pool_replenish_get_pool_failed", zap.String("pool_id", pool.PoolID), zap.Error(err))
					metricResult = "error"
				} else {
					metricResult = "not_found"
				}
				return nil
			}
			if current.Status != meta.TenantPoolActive || current.OrganizationID == "" || current.Size <= 0 {
				metricResult = "skipped"
				return nil
			}
			defer s.refreshTenantPoolCapacity(ctx, current)
			freeSize, err := s.meta.CountFreeTenantPoolBindings(ctx, current.OrganizationID)
			if err != nil {
				logger.Warn(ctx, "admin_tenant_pool_replenish_free_count_failed", zap.String("pool_id", current.PoolID), zap.Error(err))
				metricResult = "error"
				return nil
			}
			if !s.tenantPoolBelowRefillWatermark(freeSize, current.Size) {
				logger.Info(ctx, "admin_tenant_pool_replenish_skipped",
					zap.String("pool_id", current.PoolID),
					zap.String("organization_id", current.OrganizationID),
					zap.Int("pool_size", current.Size),
					zap.Int("free_size", freeSize),
					zap.Float64("refill_free_ratio", s.effectiveTenantPoolRefillFreeRatio()))
				metricResult = "noop"
				return nil
			}
			// Trigger on active free tenants, but size refill against all free
			// slots, including in-flight pending/provisioning tenants, so
			// concurrent replenishment does not double-provision.
			slotSize, err := s.meta.CountTenantPoolFreeSlots(ctx, current.OrganizationID)
			if err != nil {
				logger.Warn(ctx, "admin_tenant_pool_replenish_count_failed", zap.String("pool_id", current.PoolID), zap.Error(err))
				metricResult = "error"
				return nil
			}
			missing := current.Size - slotSize
			if missing <= 0 {
				metricResult = "noop"
				return nil
			}
			results, err := s.createFreePoolTenants(ctx, current.PoolID, missing, cred, nil)
			if err != nil {
				logger.Warn(ctx, "admin_tenant_pool_replenish_failed", zap.String("pool_id", current.PoolID), zap.Error(err))
				metricResult = "cluster_error"
				return nil
			}
			for _, res := range results {
				s.startProvisionedTenantSchemaInit(ctx, res)
			}
			return nil
		}); err != nil {
			logger.Warn(ctx, "admin_tenant_pool_replenish_lock_failed", zap.String("pool_id", pool.PoolID), zap.Error(err))
			metricResult = adminTenantPoolMetricResult(err)
		}
	})
}

func (s *Server) tenantPoolBelowRefillWatermark(freeSize, poolSize int) bool {
	if poolSize <= 0 {
		return false
	}
	if freeSize < 0 {
		freeSize = 0
	}
	return float64(freeSize)/float64(poolSize) < s.effectiveTenantPoolRefillFreeRatio()
}

func (s *Server) effectiveTenantPoolRefillFreeRatio() float64 {
	if s == nil {
		return DefaultTenantPoolRefillFreeRatio
	}
	return normalizeTenantPoolRefillFreeRatio(s.tenantPoolRefillFreeRatio)
}

func (s *Server) resumePendingTenantPoolAsync(ctx context.Context, pool *meta.TenantPool, cred tenant.CredentialProvisionRequest) {
	if pool == nil || pool.OrganizationID == "" || pool.PoolID == "" {
		return
	}
	workerCtx := backgroundWithTrace(ctx)
	s.startServerWorker(workerCtx, func(ctx context.Context) {
		clusters, err := s.pendingTenantPoolResumeClusters(ctx, pool.PoolID, pool.Size)
		if err != nil {
			logger.Warn(ctx, "admin_tenant_pool_pending_resume_list_failed", zap.String("pool_id", pool.PoolID), zap.Error(err))
			return
		}
		s.startPoolClustersMetadataResume(ctx, pool.PoolID, clusters, cred)
	})
}

func (s *Server) pendingTenantPoolResumeClusters(ctx context.Context, poolID string, limit int) ([]*tenant.ClusterInfo, error) {
	pool, err := s.meta.GetTenantPoolByID(ctx, poolID)
	if err != nil {
		return nil, err
	}
	if pool.OrganizationID == "" || pool.Status != meta.TenantPoolActive {
		return []*tenant.ClusterInfo{}, nil
	}
	if limit <= 0 || limit < pool.Size {
		limit = pool.Size
	}
	rows, err := s.meta.ListPendingTenantPoolBindingsForResume(ctx, pool.OrganizationID, limit)
	if err != nil {
		return nil, err
	}
	clusters := make([]*tenant.ClusterInfo, 0, len(rows))
	for _, row := range rows {
		plainPass, err := s.pool.Decrypt(ctx, row.Tenant.DBPasswordCipher)
		if err != nil || strings.TrimSpace(string(plainPass)) == "" {
			logger.Warn(ctx, "admin_tenant_pool_pending_resume_password_failed",
				zap.String("tenant_id", row.Tenant.ID),
				zap.String("pool_id", pool.PoolID),
				zap.Error(err))
			continue
		}
		cluster := clusterInfoFromTenant(&row.Tenant)
		cluster.OrganizationID = row.Binding.OrganizationID
		cluster.Password = string(plainPass)
		clusters = append(clusters, cluster)
	}
	return clusters, nil
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

func tenantPoolCreateDatabaseLockKey(cred tenant.CredentialProvisionRequest) string {
	key := strings.TrimSpace(cred.PublicKey)
	if key == "" {
		return ""
	}
	return "create:" + key
}

// claimAdminTenantFromPool tries to hand out a pre-warmed tenant from the
// caller org's tenant pool. sharedPoolMatched reports that the org has a
// registered shared-schema pool instead, so callers can route provisioning
// through the shared provider.
func (s *Server) claimAdminTenantFromPool(ctx context.Context, cred tenant.CredentialProvisionRequest, quotaOpt *quotaRequest) (*provisionTenantResult, *meta.TenantPool, bool, bool, error) {
	claimStarted := time.Now()
	manager, ok := s.provisioner.(tenant.TenantPoolClusterManager)
	if !ok {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_skipped", "provider", tenant.ProviderTiDBCloudNative, "reason", "pool_manager_unavailable", "duration_ms", durationMillis(claimStarted))...)
		return nil, nil, false, false, nil
	}
	if _, ok := s.provisioner.(tenant.ManagedClusterLister); !ok {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_skipped", "provider", tenant.ProviderTiDBCloudNative, "reason", "managed_cluster_lister_unavailable", "duration_ms", durationMillis(claimStarted))...)
		return nil, nil, false, false, nil
	}
	stageStarted := time.Now()
	orgID, err := s.firstManagedOrganization(ctx, cred)
	if err != nil || orgID == "" {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_org_lookup_done", "provider", tenant.ProviderTiDBCloudNative, "organization_id", orgID, "duration_ms", durationMillis(stageStarted), "has_error", err != nil)...)
		return nil, nil, false, false, err
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_org_lookup_done", "provider", tenant.ProviderTiDBCloudNative, "organization_id", orgID, "duration_ms", durationMillis(stageStarted))...)
	cleanupCred := cred
	defer s.startTenantFailedCleanupAsync(ctx, orgID, cleanupCred)
	stageStarted = time.Now()
	pool, err := s.meta.GetTenantPoolByOrganization(ctx, orgID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			// A registered external/manual shared DB remains a provision target,
			// but it must not bypass an existing logical pool's mixed inventory.
			// This check therefore runs only after the logical pool lookup misses.
			if sharedDB, sharedErr := s.meta.FindSharedDBForOrg(ctx, orgID); sharedErr == nil && sharedDB != nil {
				logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_skipped",
					"provider", tenant.ProviderTiDBCloudNative, "organization_id", orgID,
					"reason", "external_shared_pool_registered", "duration_ms", durationMillis(claimStarted))...)
				return nil, nil, false, true, nil
			} else if sharedErr != nil && !errors.Is(sharedErr, meta.ErrNotFound) {
				return nil, nil, false, false, sharedErr
			}
			logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_pool_lookup_missed", "provider", tenant.ProviderTiDBCloudNative, "organization_id", orgID, "duration_ms", durationMillis(stageStarted))...)
			return nil, nil, false, false, nil
		}
		return nil, nil, false, false, err
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_pool_lookup_done", "provider", tenant.ProviderTiDBCloudNative, "pool_id", pool.PoolID, "organization_id", orgID, "pool_size", pool.Size, "pool_status", pool.Status, "duration_ms", durationMillis(stageStarted))...)
	if pool.Status != meta.TenantPoolActive {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_skipped", "provider", tenant.ProviderTiDBCloudNative, "pool_id", pool.PoolID, "organization_id", orgID, "reason", "pool_inactive", "pool_status", pool.Status, "duration_ms", durationMillis(claimStarted))...)
		return nil, nil, false, false, nil
	}
	defer s.refreshTenantPoolCapacity(ctx, pool)
	s.resumePendingTenantPoolAsync(ctx, pool, cred)
	stageStarted = time.Now()
	selection, err := retryTenantPoolClaimCAS(func() (tenantPoolClaimSelection, error) {
		var nativeCandidate *meta.TenantWithTiDBCloudOrgBinding
		if rows, listErr := s.meta.ListFreeTenantPoolBindings(ctx, orgID, false, 1); listErr != nil {
			return tenantPoolClaimSelection{}, listErr
		} else if len(rows) > 0 {
			nativeCandidate = &rows[0]
		}
		sharedCandidate, sharedErr := s.meta.GetOldestFreeTenantPoolMembership(ctx, pool.PoolID)
		if sharedErr != nil && !errors.Is(sharedErr, meta.ErrNotFound) {
			return tenantPoolClaimSelection{}, sharedErr
		}
		if sharedCandidate != nil && (nativeCandidate == nil || sharedCandidate.Membership.CreatedAt.Before(nativeCandidate.Binding.CreatedAt) ||
			(sharedCandidate.Membership.CreatedAt.Equal(nativeCandidate.Binding.CreatedAt) && sharedCandidate.Tenant.ID < nativeCandidate.Tenant.ID)) {
			result, claimErr := s.claimSharedTenantPoolMember(ctx, pool, sharedCandidate, quotaOpt)
			if claimErr != nil {
				return tenantPoolClaimSelection{}, claimErr
			}
			return tenantPoolClaimSelection{sharedResult: result}, nil
		}
		if nativeCandidate == nil {
			return tenantPoolClaimSelection{}, nil
		}
		row, claimErr := s.meta.ClaimOldestFreeTenantPoolBinding(ctx, orgID)
		if claimErr != nil {
			return tenantPoolClaimSelection{}, claimErr
		}
		return tenantPoolClaimSelection{native: row}, nil
	})
	if err != nil {
		return nil, nil, false, false, err
	}
	if selection.sharedResult != nil {
		return selection.sharedResult, pool, true, false, nil
	}
	row := selection.native
	if row == nil {
		logger.Info(ctx, "server_event", eventFields(ctx, "admin_tenant_pool_claim_free_tenant_missed", "provider", tenant.ProviderTiDBCloudNative, "pool_id", pool.PoolID, "organization_id", orgID, "duration_ms", durationMillis(stageStarted))...)
		return nil, pool, false, false, nil
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_free_tenant_claimed", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID, "cluster_id", row.Binding.ClusterID, "status", row.Tenant.Status)
	usedAt := time.Now().UTC()
	var opts tenant.QuotaUpdateOptions
	if quotaOpt != nil {
		opts.TiDBCloudSpendingLimitMonthly = quotaOpt.TiDBCloudSpendingLimit
	}
	cluster := clusterInfoFromTenant(&row.Tenant)
	stageStarted = time.Now()
	cloudCfg, err := manager.MarkClusterPoolUsed(ctx, cluster, cred, usedAt, opts)
	if err != nil {
		s.releaseClaimedPoolTenant(ctx, manager, cluster, cred, row.Tenant.ID, "mark_used_error")
		return nil, nil, false, false, err
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_cluster_marked_used", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID, "cluster_id", cluster.ClusterID, "quota_requested", quotaOpt != nil)
	success := false
	defer func() {
		if !success {
			s.releaseClaimedPoolTenant(ctx, manager, cluster, cred, row.Tenant.ID, "claim_error")
		}
	}()
	stageStarted = time.Now()
	quotaSeed := meta.QuotaConfigPatch{
		TiDBCloudSpendingLimit: tidbCloudSpendingLimitFromCloud(cloudCfg),
	}
	if quotaOpt != nil {
		qp, err := quotaConfigPatchFromRequest(*quotaOpt)
		if err != nil {
			return nil, nil, false, false, err
		}
		if qp.MaxStorageBytes != nil {
			quotaSeed.MaxStorageBytes = qp.MaxStorageBytes
		}
		if qp.MaxFileSizeBytes != nil {
			quotaSeed.MaxFileSizeBytes = qp.MaxFileSizeBytes
		}
		if qp.MaxFileCount != nil {
			quotaSeed.MaxFileCount = qp.MaxFileCount
		}
		if qp.TiDBCloudSpendingLimit != nil {
			quotaSeed.TiDBCloudSpendingLimit = qp.TiDBCloudSpendingLimit
		}
	}
	if cloudCfg != nil {
		checkedAt := time.Now().UTC()
		quotaSeed.TiDBCloudSpendingLimitCheckedAt = &checkedAt
	}
	if err := s.meta.SetQuotaConfigPatch(ctx, row.Tenant.ID, quotaSeed); err != nil {
		return nil, nil, false, false, err
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_quota_seeded", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID, "create_time_spending_limit", cloudCfg != nil && cloudCfg.TiDBCloudSpendingLimitMonthly != nil)
	stageStarted = time.Now()
	plainPass, err := s.pool.Decrypt(ctx, row.Tenant.DBPasswordCipher)
	if err != nil {
		return nil, nil, false, false, err
	}
	logProvisionStage(ctx, "admin_tenant_pool_claim_db_password_decrypted", row.Tenant.ID, row.Tenant.Provider, stageStarted, "pool_id", pool.PoolID, "organization_id", orgID)
	stageStarted = time.Now()
	apiToken, apiKeyID, err := s.issueOwnerAPIKey(ctx, row.Tenant.ID, "default", 1, apiKeyIssueSource{})
	if err != nil {
		return nil, nil, false, false, err
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
	}, pool, true, false, nil
}

func (s *Server) claimSharedTenantPoolMember(ctx context.Context, pool *meta.TenantPool, row *meta.TenantWithPoolMembership, quotaOpt *quotaRequest) (*provisionTenantResult, error) {
	if pool == nil || row == nil {
		return nil, meta.ErrNotFound
	}
	// Resolve immutable free-member routing metadata before the claim commits.
	// A transient read failure must not strand a used member whose owner token
	// was committed but never returned to the caller.
	fsID, err := s.meta.ResolveFsID(ctx, row.Tenant.ID)
	if err != nil {
		return nil, err
	}
	placement, err := s.meta.GetTenantPlacement(ctx, fsID)
	if err != nil {
		return nil, err
	}
	dbPool, err := s.meta.GetSharedDB(ctx, placement.DbID)
	if err != nil {
		return nil, err
	}
	var patch meta.QuotaConfigPatch
	if quotaOpt != nil {
		var err error
		patch, err = quotaConfigPatchFromRequest(*quotaOpt)
		if err != nil {
			return nil, err
		}
	}
	rawToken, apiKeyID, key, err := s.buildOwnerAPIKey(ctx, row.Tenant.ID, "default", 1, apiKeyIssueSource{})
	if err != nil {
		return nil, err
	}
	if err := s.meta.ClaimSharedTenantPoolMembership(ctx, row.Tenant.ID, pool.PoolID, patch, key); err != nil {
		return nil, err
	}
	return &provisionTenantResult{TenantID: row.Tenant.ID, APIKey: rawToken, APIKeyID: apiKeyID,
		Status: meta.TenantActive, Provider: tenant.ProviderTiDBCloudNativeShared,
		CloudProvider: dbPool.CloudProvider, Region: dbPool.Region,
		OrganizationID: row.Membership.TiDBCloudOrganizationID}, nil
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
