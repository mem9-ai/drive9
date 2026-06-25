package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

const (
	defaultAdminTenantPageSize = 10
	maxAdminTenantPageSize     = 100
)

type adminTenantCreateRequest struct {
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
	quotaFields
}

type adminTenantQuotaRequest struct {
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
	quotaFields
}

type adminTenantResponse struct {
	TenantID string                   `json:"tenant_id"`
	Status   string                   `json:"status"`
	Kind     string                   `json:"kind"`
	Quota    *adminTenantQuotaSummary `json:"quota,omitempty"`
}

type adminTenantQuotaSummary struct {
	Config quotaConfigResponse `json:"config"`
	Usage  quotaUsageResponse  `json:"usage"`
}

type adminTenantCreateResponse struct {
	TenantID      string `json:"tenant_id"`
	APIKey        string `json:"api_key"`
	Status        string `json:"status"`
	CloudProvider string `json:"cloud_provider,omitempty"`
	Region        string `json:"region,omitempty"`
}

type adminTenantListResponse struct {
	Tenants  []adminTenantResponse `json:"tenants"`
	Page     int                   `json:"page"`
	PageSize int                   `json:"page_size"`
	NextPage int                   `json:"next_page,omitempty"`
}

type adminQuotaResponse struct {
	TenantID string              `json:"tenant_id"`
	Status   string              `json:"status"`
	Config   quotaConfigResponse `json:"config"`
	Usage    quotaUsageResponse  `json:"usage"`
}

type adminTenantDeleteResponse struct {
	TenantID string `json:"tenant_id"`
	Status   string `json:"status"`
}

func (s *Server) adminTenantsRootHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.adminTenantAPIEnabled() {
			errJSON(w, http.StatusNotFound, "admin tenant API not enabled")
			return
		}
		switch r.Method {
		case http.MethodPost:
			s.handleAdminTenantCreate(w, r)
		case http.MethodGet:
			s.handleAdminTenantList(w, r)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *Server) adminTenantsItemHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.adminTenantAPIEnabled() {
			errJSON(w, http.StatusNotFound, "admin tenant API not enabled")
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, "/v1/admin/tenants/")
		rest = strings.Trim(rest, "/")
		if rest == "" {
			errJSON(w, http.StatusNotFound, "tenant not found")
			return
		}
		parts := strings.Split(rest, "/")
		tenantID := strings.TrimSpace(parts[0])
		if tenantID == "" {
			errJSON(w, http.StatusNotFound, "tenant not found")
			return
		}
		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				s.handleAdminTenantGet(w, r, tenantID)
			case http.MethodDelete:
				s.handleAdminTenantDelete(w, r, tenantID)
			default:
				errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}
		if len(parts) == 2 && parts[1] == "quota" {
			switch r.Method {
			case http.MethodPost:
				s.handleAdminTenantQuotaSet(w, r, tenantID)
			default:
				errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}
		errJSON(w, http.StatusNotFound, "not found")
	})
}

func (s *Server) adminTenantAPIEnabled() bool {
	if s.meta == nil || s.pool == nil || s.provisioner == nil || len(s.tokenSecret) == 0 {
		return false
	}
	provider, err := tenant.NormalizeProvider(s.provisioner.ProviderType())
	if err != nil || provider != tenant.ProviderTiDBCloudNative {
		return false
	}
	_, ok := s.provisioner.(tenant.ManagedClusterLister)
	return ok
}

func (s *Server) handleAdminTenantCreate(w http.ResponseWriter, r *http.Request) {
	var req adminTenantCreateRequest
	if err := decodeJSONBody(w, r, &req, true); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := adminCredentials(req.PublicKey, req.PrivateKey, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	quotaReq := quotaRequest{
		TenantID:    "pending",
		quotaFields: req.quotaFields,
	}
	var quotaOpt *quotaRequest
	if quotaReq.anySet() {
		if err := s.validateQuotaSetRequest(quotaReq); err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		quotaOpt = &quotaReq
	}
	res, err := s.provisionTenant(r.Context(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
		Quota:                 quotaOpt,
	})
	if err != nil {
		var pe *provisionTenantError
		if errors.As(err, &pe) {
			errJSON(w, pe.status, pe.message)
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to provision tenant")
		return
	}
	setRequestMetricTenant(r.Context(), res.TenantID, res.APIKeyID, res.Provider, classifyTenantRequest(r))
	s.startProvisionedTenantSchemaInit(r.Context(), res)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(adminTenantCreateResponse{
		TenantID:      res.TenantID,
		APIKey:        res.APIKey,
		Status:        string(res.Status),
		CloudProvider: res.CloudProvider,
		Region:        res.Region,
	})
}

func (s *Server) handleAdminTenantList(w http.ResponseWriter, r *http.Request) {
	cred, err := adminCredentialsFromHeaders(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	clusters, err := s.listAllManagedClusters(r.Context(), cred, "")
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "list tenants")
		return
	}
	authorizedBindings := authorizedTiDBCloudOrgClusterBindings(clusters)
	if len(authorizedBindings) == 0 {
		pageSize, page, _, err := adminPagination(r)
		if err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, adminTenantListResponse{Tenants: []adminTenantResponse{}, Page: page, PageSize: pageSize})
		return
	}
	pageSize, page, offset, err := adminPagination(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	includeQuota := parseBoolQuery(r, "include_quota")
	rows, err := s.meta.ListTenantsByTiDBCloudOrgClusterBindings(r.Context(), authorizedBindings, offset, pageSize+1)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "tenant list failed")
		return
	}
	nextPage := 0
	if len(rows) > pageSize {
		rows = rows[:pageSize]
		nextPage = page + 1
	}
	clusterMap := cloudClusterMap(clusters)
	out := make([]adminTenantResponse, 0, len(rows))
	for _, row := range rows {
		var quota *adminTenantQuotaSummary
		if includeQuota {
			cloudCfg := &tenant.QuotaCloudConfig{}
			if cloud, ok := clusterMap[row.Binding.ClusterID]; ok {
				cloudCfg.TiDBCloudSpendingLimitMonthly = cloud.TiDBCloudSpendingLimitMonthly
			}
			quota = s.adminTenantQuotaSummary(r.Context(), &row.Tenant, cloudCfg)
		}
		out = append(out, s.adminTenantResponse(&row.Tenant, &row.Binding, quota))
	}
	writeJSON(w, http.StatusOK, adminTenantListResponse{Tenants: out, Page: page, PageSize: pageSize, NextPage: nextPage})
}

func (s *Server) handleAdminTenantGet(w http.ResponseWriter, r *http.Request, tenantID string) {
	cred, err := adminCredentialsFromHeaders(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	t, binding, cloudCfg, ok := s.authorizedAdminTenant(w, r, tenantID, cred, true, false)
	if !ok {
		return
	}
	quota, err := s.loadAdminTenantQuotaSummary(r.Context(), t, cloudCfg)
	if err != nil {
		logger.Warn(r.Context(), "admin_tenant_quota_lookup_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		errJSON(w, http.StatusInternalServerError, "quota lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, s.adminTenantResponse(t, binding, quota))
}

func (s *Server) handleAdminTenantQuotaSet(w http.ResponseWriter, r *http.Request, tenantID string) {
	var req adminTenantQuotaRequest
	if err := decodeJSONBody(w, r, &req, true); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := adminCredentials(req.PublicKey, req.PrivateKey, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	quotaReq := quotaRequest{
		TenantID:    tenantID,
		quotaFields: req.quotaFields,
	}
	if err := s.validateQuotaSetRequest(quotaReq); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	t, _, _, ok := s.authorizedAdminTenant(w, r, tenantID, cred, false, false)
	if !ok {
		return
	}
	cloudCfg, err := s.applyQuotaSet(r.Context(), t, cred, quotaReq)
	if err != nil {
		writeQuotaSetError(w, r.Context(), err, "update")
		return
	}
	s.writeAdminQuotaResponse(w, r, t, cloudCfg)
}

func (s *Server) handleAdminTenantDelete(w http.ResponseWriter, r *http.Request, tenantID string) {
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
	t, _, _, ok := s.authorizedAdminTenant(w, r, tenantID, cred, false, true)
	if !ok {
		return
	}
	if t.Kind != meta.TenantKindLive {
		errJSON(w, http.StatusConflict, "only live tenants can be deleted")
		return
	}
	if t.Status == meta.TenantDeleted {
		errJSON(w, http.StatusNotFound, "tenant not found")
		return
	}
	if t.StorageNamespaceID != "" {
		hasFork, err := s.meta.NamespaceHasNonDeletedFork(r.Context(), t.StorageNamespaceID)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to check tenant forks")
			return
		}
		if hasFork {
			errJSON(w, http.StatusConflict, "tenant has non-deleted forks")
			return
		}
	}
	if t.Status == meta.TenantDeleting {
		hasJob, err := s.meta.TenantDeleteJobExists(r.Context(), t.ID)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to check tenant delete cleanup")
			return
		}
		if hasJob {
			_ = s.meta.RevokeTenantAPIKeys(r.Context(), t.ID)
			writeJSON(w, http.StatusAccepted, adminTenantDeleteResponse{TenantID: t.ID, Status: string(meta.TenantDeleting)})
			return
		}
	}
	if updater, ok := s.provisioner.(tenant.QuotaUpdater); ok {
		if _, err := updater.MarkQuotaUpdateStarted(r.Context(), clusterInfoFromTenant(t), cred); err != nil {
			writeQuotaCredentialError(w, r.Context(), err, "delete")
			return
		}
	} else {
		errJSON(w, http.StatusNotFound, "tenant delete not enabled")
		return
	}
	if t.Status != meta.TenantDeleting {
		updated, err := s.meta.UpdateTenantStatusIf(r.Context(), t.ID, t.Status, meta.TenantDeleting)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to mark tenant deleting")
			return
		}
		if !updated {
			writeJSON(w, http.StatusAccepted, adminTenantDeleteResponse{TenantID: t.ID, Status: string(meta.TenantDeleting)})
			return
		}
	}
	if err := s.pool.InvalidateAndWait(r.Context(), t.ID); err != nil {
		if t.Status != meta.TenantDeleting {
			_, _ = s.meta.UpdateTenantStatusIf(r.Context(), t.ID, meta.TenantDeleting, t.Status)
		}
		errJSON(w, http.StatusInternalServerError, "failed to drain tenant backend")
		return
	}
	if err := s.deprovisionTenantCluster(r.Context(), t, cred); err != nil {
		if t.Status != meta.TenantDeleting {
			_, _ = s.meta.UpdateTenantStatusIf(r.Context(), t.ID, meta.TenantDeleting, t.Status)
		}
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("delete tenant cluster failed: %v", err))
		return
	}
	_ = s.meta.AbortActiveUploadReservations(r.Context(), t.ID)
	status, err := s.enqueueTenantDeleteJob(r.Context(), t)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to enqueue tenant delete cleanup")
		return
	}
	_ = s.meta.RevokeTenantAPIKeys(r.Context(), t.ID)
	writeJSON(w, http.StatusAccepted, adminTenantDeleteResponse{TenantID: t.ID, Status: string(status)})
}

func (s *Server) authorizedAdminTenant(w http.ResponseWriter, r *http.Request, tenantID string, cred tenant.CredentialProvisionRequest, loadQuota bool, allowDeletingMissingCluster bool) (*meta.Tenant, *meta.TenantTiDBCloudOrgBinding, *tenant.QuotaCloudConfig, bool) {
	t, err := s.meta.GetTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "tenant not found")
			return nil, nil, nil, false
		}
		errJSON(w, http.StatusInternalServerError, "tenant lookup failed")
		return nil, nil, nil, false
	}
	if t.Provider != tenant.ProviderTiDBCloudNative {
		errJSON(w, http.StatusConflict, "admin tenant API is only supported for tidb_cloud_native tenants")
		return nil, nil, nil, false
	}
	if t.Status == meta.TenantDeleted {
		errJSON(w, http.StatusNotFound, "tenant not found")
		return nil, nil, nil, false
	}
	binding, err := s.meta.GetTenantTiDBCloudOrgBinding(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "tenant tidbcloud organization binding not found")
			return nil, nil, nil, false
		}
		errJSON(w, http.StatusInternalServerError, "tenant organization binding lookup failed")
		return nil, nil, nil, false
	}
	clusters, err := s.listAllManagedClusters(r.Context(), cred, binding.ClusterID)
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "authorize tenant")
		return nil, nil, nil, false
	}
	if len(clusters) == 0 && allowDeletingMissingCluster && t.Status == meta.TenantDeleting {
		clusters, err = s.listAllManagedClusters(r.Context(), cred, "")
		if err != nil {
			writeAdminTiDBCloudError(w, r.Context(), err, "authorize tenant")
			return nil, nil, nil, false
		}
		for _, cluster := range clusters {
			if cluster.OrganizationID == binding.OrganizationID {
				return t, binding, nil, true
			}
		}
	}
	if len(clusters) == 0 {
		errJSON(w, http.StatusForbidden, "no permission to access tenant with TiDB Cloud API key")
		return nil, nil, nil, false
	}
	var cloudCfg *tenant.QuotaCloudConfig
	authorized := false
	for _, cluster := range clusters {
		if cluster.ClusterID == binding.ClusterID && cluster.OrganizationID == binding.OrganizationID {
			authorized = true
			if loadQuota {
				cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: cluster.TiDBCloudSpendingLimitMonthly}
			}
			break
		}
	}
	if !authorized {
		errJSON(w, http.StatusForbidden, "no permission to access tenant with TiDB Cloud API key")
		return nil, nil, nil, false
	}
	return t, binding, cloudCfg, true
}

func (s *Server) listAllManagedClusters(ctx context.Context, cred tenant.CredentialProvisionRequest, clusterID string) ([]tenant.CloudClusterInfo, error) {
	lister, ok := s.provisioner.(tenant.ManagedClusterLister)
	if !ok {
		return nil, fmt.Errorf("managed cluster list not enabled")
	}
	var out []tenant.CloudClusterInfo
	pageToken := ""
	for {
		page, err := lister.ListManagedClusters(ctx, cred, tenant.ManagedClusterListOptions{
			PageSize:  100,
			PageToken: pageToken,
			ClusterID: clusterID,
		})
		if err != nil {
			return nil, err
		}
		if page == nil {
			return out, nil
		}
		out = append(out, page.Clusters...)
		pageToken = strings.TrimSpace(page.NextPageToken)
		if pageToken == "" || clusterID != "" {
			return out, nil
		}
	}
}

func (s *Server) adminTenantResponse(t *meta.Tenant, _ *meta.TenantTiDBCloudOrgBinding, quota *adminTenantQuotaSummary) adminTenantResponse {
	return adminTenantResponse{
		TenantID: t.ID,
		Status:   string(t.Status),
		Kind:     string(t.Kind),
		Quota:    quota,
	}
}

func (s *Server) adminTenantQuotaSummary(ctx context.Context, t *meta.Tenant, cloudCfg *tenant.QuotaCloudConfig) *adminTenantQuotaSummary {
	out, err := s.loadAdminTenantQuotaSummary(ctx, t, cloudCfg)
	if err != nil {
		logger.Warn(ctx, "admin_tenant_quota_lookup_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		return nil
	}
	return out
}

func (s *Server) loadAdminTenantQuotaSummary(ctx context.Context, t *meta.Tenant, cloudCfg *tenant.QuotaCloudConfig) (*adminTenantQuotaSummary, error) {
	cfg, err := s.meta.GetQuotaConfig(ctx, t.ID)
	if err != nil {
		return nil, fmt.Errorf("quota config lookup failed: %w", err)
	}
	usage, err := s.meta.GetQuotaUsage(ctx, t.ID)
	if err != nil {
		return nil, fmt.Errorf("quota usage lookup failed: %w", err)
	}
	var spendingLimit *int64
	if cloudCfg != nil {
		spendingLimit = cloudCfg.TiDBCloudSpendingLimitMonthly
	}
	return &adminTenantQuotaSummary{
		Config: quotaConfigResponse{
			MaxStorageSize:         quotaStorageBytesToSize(cfg.MaxStorageBytes),
			MaxFileSize:            quotaStorageBytesToSize(cfg.MaxFileSizeBytes),
			MaxFileCount:           cfg.MaxFileCount,
			TiDBCloudSpendingLimit: spendingLimit,
		},
		Usage: quotaUsageResponse{
			StorageBytes:  usage.StorageBytes,
			ReservedBytes: usage.ReservedBytes,
			FileCount:     usage.FileCount,
		},
	}, nil
}

func (s *Server) writeAdminQuotaResponse(w http.ResponseWriter, r *http.Request, t *meta.Tenant, cloudCfg *tenant.QuotaCloudConfig) {
	quota, err := s.loadAdminTenantQuotaSummary(r.Context(), t, cloudCfg)
	if err != nil {
		logger.Warn(r.Context(), "admin_tenant_quota_lookup_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		errJSON(w, http.StatusInternalServerError, "quota lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, adminQuotaResponse{
		TenantID: t.ID,
		Status:   string(t.Status),
		Config:   quota.Config,
		Usage:    quota.Usage,
	})
}

func authorizedTiDBCloudOrgClusterBindings(clusters []tenant.CloudClusterInfo) []meta.TenantTiDBCloudOrgBinding {
	out := make([]meta.TenantTiDBCloudOrgBinding, 0, len(clusters))
	seen := make(map[string]bool, len(clusters))
	for _, cluster := range clusters {
		orgID := strings.TrimSpace(cluster.OrganizationID)
		clusterID := strings.TrimSpace(cluster.ClusterID)
		if orgID == "" || clusterID == "" {
			continue
		}
		key := orgID + "\x00" + clusterID
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, meta.TenantTiDBCloudOrgBinding{OrganizationID: orgID, ClusterID: clusterID})
	}
	return out
}

func cloudClusterMap(clusters []tenant.CloudClusterInfo) map[string]tenant.CloudClusterInfo {
	out := make(map[string]tenant.CloudClusterInfo, len(clusters))
	for _, cluster := range clusters {
		if cluster.ClusterID != "" {
			out[cluster.ClusterID] = cluster
		}
	}
	return out
}

func adminCredentialsFromHeaders(r *http.Request) (tenant.CredentialProvisionRequest, error) {
	return adminCredentials("", "", r)
}

func adminCredentials(publicKey, privateKey string, r *http.Request) (tenant.CredentialProvisionRequest, error) {
	if strings.TrimSpace(publicKey) == "" {
		publicKey = r.Header.Get(quotaPublicKeyHeader)
	}
	if strings.TrimSpace(privateKey) == "" {
		privateKey = r.Header.Get(quotaPrivateKeyHeader)
	}
	req := quotaRequest{
		TenantID:   "admin",
		PublicKey:  strings.TrimSpace(publicKey),
		PrivateKey: strings.TrimSpace(privateKey),
	}
	return quotaCredentials(req)
}

func adminPagination(r *http.Request) (pageSize, page, offset int, err error) {
	pageSize = defaultAdminTenantPageSize
	page = 1
	if raw := strings.TrimSpace(r.URL.Query().Get("page_size")); raw != "" {
		v, parseErr := strconv.Atoi(raw)
		if parseErr != nil || v <= 0 {
			return 0, 0, 0, fmt.Errorf("page_size must be a positive integer")
		}
		if v > maxAdminTenantPageSize {
			v = maxAdminTenantPageSize
		}
		pageSize = v
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("page")); raw != "" {
		v, parseErr := strconv.Atoi(raw)
		if parseErr != nil || v <= 0 {
			return 0, 0, 0, fmt.Errorf("page must be a positive integer")
		}
		page = v
	}
	maxOffsetPage := 1 + int(^uint(0)>>1)/pageSize
	if page > maxOffsetPage {
		return 0, 0, 0, fmt.Errorf("page is too large")
	}
	offset = (page - 1) * pageSize
	return pageSize, page, offset, nil
}

func parseBoolQuery(r *http.Request, name string) bool {
	raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get(name)))
	return raw == "1" || raw == "true" || raw == "yes"
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, out any, requireBody bool) error {
	if r.Body == nil {
		if requireBody {
			return fmt.Errorf("request body is required")
		}
		return nil
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCredentialProvisionBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		if errors.Is(err, io.EOF) && !requireBody {
			return nil
		}
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid JSON body: trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeAdminTiDBCloudError(w http.ResponseWriter, ctx context.Context, err error, action string) {
	switch {
	case errors.Is(err, tenant.ErrQuotaPermissionDenied):
		errJSON(w, http.StatusForbidden, "no permission to "+action+" with TiDB Cloud API key")
	case errors.Is(err, tenant.ErrQuotaBackendNotFound):
		errJSON(w, http.StatusNotFound, quotaBackendNotFoundMessage)
	default:
		logger.Warn(ctx, "tidbcloud_admin_failed", zap.String("action", action), zap.Error(err))
		errJSON(w, http.StatusBadGateway, "tidbcloud admin "+action+" failed")
	}
}
