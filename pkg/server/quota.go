package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

type quotaRequest struct {
	TenantID         string `json:"tenant_id"`
	PublicKey        string `json:"public_key"`
	PrivateKey       string `json:"private_key"`
	MaxStorageBytes  *int64 `json:"max_storage_bytes,omitempty"`
	MaxMediaLLMFiles *int64 `json:"max_media_llm_files,omitempty"`
	MaxMonthlyCostMC *int64 `json:"max_monthly_cost_mc,omitempty"`
}

type quotaResponse struct {
	TenantID       string              `json:"tenant_id"`
	Provider       string              `json:"provider"`
	Status         string              `json:"status"`
	SupportsUpdate bool                `json:"supports_update"`
	Config         quotaConfigResponse `json:"config"`
	Usage          quotaUsageResponse  `json:"usage"`
}

type quotaConfigResponse struct {
	MaxStorageBytes  int64 `json:"max_storage_bytes"`
	MaxMediaLLMFiles int64 `json:"max_media_llm_files"`
	MaxMonthlyCostMC int64 `json:"max_monthly_cost_mc"`
}

type quotaUsageResponse struct {
	StorageBytes   int64 `json:"storage_bytes"`
	ReservedBytes  int64 `json:"reserved_bytes"`
	MediaFileCount int64 `json:"media_file_count"`
	MonthlyCostMC  int64 `json:"monthly_cost_mc"`
}

func (s *Server) quotaRootHandler(cfg Config) http.Handler {
	var ownerGet http.Handler = http.HandlerFunc(s.handleQuotaOwnerGet)
	if cfg.Meta != nil && cfg.Pool != nil && len(cfg.TokenSecret) > 0 {
		ownerGet = tenantAuthMiddleware(cfg.Meta, cfg.Pool, cfg.TokenSecret, ownerGet)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			ownerGet.ServeHTTP(w, r)
		case http.MethodPost:
			s.handleQuotaSet(w, r)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *Server) handleQuotaOwnerGet(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil {
		errJSON(w, http.StatusNotFound, "quota query not enabled")
		return
	}
	scope, ok := ownerScopeFromRequest(w, r, "query quota")
	if !ok {
		return
	}
	t, ok := s.quotaTenant(w, r.Context(), scope.TenantID)
	if !ok {
		return
	}
	setRequestMetricTenant(r.Context(), t.ID, scope.APIKeyID, t.Provider, classifyTenantRequest(r))
	s.writeQuotaResponse(w, r, t)
}

func (s *Server) handleQuotaCredentialQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.meta == nil || s.provisioner == nil {
		errJSON(w, http.StatusNotFound, "quota query not enabled")
		return
	}
	req, err := decodeQuotaRequest(w, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := quotaCredentials(req)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	t, ok := s.quotaTenant(w, r.Context(), req.TenantID)
	if !ok {
		return
	}
	if t.Provider != tenant.ProviderTiDBCloudNative {
		errJSON(w, http.StatusConflict, "tidbcloud credential quota query is only supported for tidb_cloud_native tenants")
		return
	}
	authorizer, ok := s.provisioner.(tenant.CredentialQuotaAuthorizer)
	if !ok {
		errJSON(w, http.StatusNotFound, "quota query not enabled")
		return
	}
	if err := authorizer.AuthorizeQuotaWithCredentials(r.Context(), clusterInfoFromTenant(t), cred); err != nil {
		writeQuotaCredentialError(w, err, "query")
		return
	}
	setRequestMetricTenant(r.Context(), t.ID, "", t.Provider, classifyTenantRequest(r))
	s.writeQuotaResponse(w, r, t)
}

func (s *Server) handleQuotaSet(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.provisioner == nil {
		errJSON(w, http.StatusNotFound, "quota setting not enabled")
		return
	}
	req, err := decodeQuotaRequest(w, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := quotaCredentials(req)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateQuotaSetRequest(req); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	t, ok := s.quotaTenant(w, r.Context(), req.TenantID)
	if !ok {
		return
	}
	if t.Provider != tenant.ProviderTiDBCloudNative {
		errJSON(w, http.StatusConflict, "quota setting is only supported for tidb_cloud_native tenants")
		return
	}
	if strings.TrimSpace(t.ClusterID) == "" {
		errJSON(w, http.StatusNotFound, quotaBackendNotFoundMessage)
		return
	}
	current, err := s.meta.GetQuotaConfig(r.Context(), t.ID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "quota config lookup failed")
		return
	}
	next := &meta.QuotaConfig{
		TenantID:         t.ID,
		MaxStorageBytes:  current.MaxStorageBytes,
		MaxMediaLLMFiles: current.MaxMediaLLMFiles,
		MaxMonthlyCostMC: current.MaxMonthlyCostMC,
	}
	if req.MaxStorageBytes != nil {
		next.MaxStorageBytes = *req.MaxStorageBytes
	}
	if req.MaxMediaLLMFiles != nil {
		next.MaxMediaLLMFiles = *req.MaxMediaLLMFiles
	}
	if req.MaxMonthlyCostMC != nil {
		next.MaxMonthlyCostMC = *req.MaxMonthlyCostMC
	}
	updater, ok := s.provisioner.(tenant.CredentialQuotaUpdater)
	if !ok {
		errJSON(w, http.StatusNotFound, "quota setting not enabled")
		return
	}
	if err := updater.UpdateQuotaWithCredentials(r.Context(), clusterInfoFromTenant(t), cred); err != nil {
		writeQuotaCredentialError(w, err, "update")
		return
	}
	if err := s.meta.SetQuotaConfig(r.Context(), next); err != nil {
		errJSON(w, http.StatusInternalServerError, "quota config update failed")
		return
	}
	if err := s.meta.EnsureQuotaUsageRow(r.Context(), t.ID); err != nil {
		errJSON(w, http.StatusInternalServerError, "quota usage initialization failed")
		return
	}
	setRequestMetricTenant(r.Context(), t.ID, "", t.Provider, classifyTenantRequest(r))
	s.writeQuotaResponse(w, r, t)
}

func decodeQuotaRequest(w http.ResponseWriter, r *http.Request) (quotaRequest, error) {
	var req quotaRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCredentialProvisionBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return quotaRequest{}, fmt.Errorf("invalid JSON body: %w", err)
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return quotaRequest{}, fmt.Errorf("invalid JSON body: trailing data")
	}
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.PublicKey = strings.TrimSpace(req.PublicKey)
	req.PrivateKey = strings.TrimSpace(req.PrivateKey)
	return req, nil
}

func quotaCredentials(req quotaRequest) (tenant.CredentialProvisionRequest, error) {
	if req.TenantID == "" {
		return tenant.CredentialProvisionRequest{}, fmt.Errorf("tenant_id is required")
	}
	if req.PublicKey == "" && req.PrivateKey == "" {
		return tenant.CredentialProvisionRequest{}, tenant.ErrCredentialsRequired
	}
	if req.PublicKey == "" || req.PrivateKey == "" {
		return tenant.CredentialProvisionRequest{}, tenant.ErrPartialCredentials
	}
	return tenant.CredentialProvisionRequest{PublicKey: req.PublicKey, PrivateKey: req.PrivateKey}, nil
}

func validateQuotaSetRequest(req quotaRequest) error {
	if req.MaxStorageBytes == nil && req.MaxMediaLLMFiles == nil && req.MaxMonthlyCostMC == nil {
		return fmt.Errorf("at least one quota field is required")
	}
	if req.MaxStorageBytes != nil && *req.MaxStorageBytes <= 0 {
		return fmt.Errorf("max_storage_bytes must be positive")
	}
	if req.MaxMediaLLMFiles != nil && *req.MaxMediaLLMFiles < 0 {
		return fmt.Errorf("max_media_llm_files must be non-negative")
	}
	if req.MaxMonthlyCostMC != nil && *req.MaxMonthlyCostMC < 0 {
		return fmt.Errorf("max_monthly_cost_mc must be non-negative")
	}
	return nil
}

func (s *Server) quotaTenant(w http.ResponseWriter, ctx context.Context, tenantID string) (*meta.Tenant, bool) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		errJSON(w, http.StatusBadRequest, "tenant_id is required")
		return nil, false
	}
	t, err := s.meta.GetTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "tenant not found")
			return nil, false
		}
		errJSON(w, http.StatusInternalServerError, "tenant lookup failed")
		return nil, false
	}
	if t.Status == meta.TenantDeleted {
		errJSON(w, http.StatusNotFound, "tenant not found")
		return nil, false
	}
	return t, true
}

func (s *Server) writeQuotaResponse(w http.ResponseWriter, r *http.Request, t *meta.Tenant) {
	cfg, err := s.meta.GetQuotaConfig(r.Context(), t.ID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "quota config lookup failed")
		return
	}
	usage, err := s.meta.GetQuotaUsage(r.Context(), t.ID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "quota usage lookup failed")
		return
	}
	monthlyCost, err := s.meta.MonthlyLLMCostMillicents(r.Context(), t.ID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "quota usage lookup failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(quotaResponse{
		TenantID:       t.ID,
		Provider:       t.Provider,
		Status:         string(t.Status),
		SupportsUpdate: t.Provider == tenant.ProviderTiDBCloudNative,
		Config: quotaConfigResponse{
			MaxStorageBytes:  cfg.MaxStorageBytes,
			MaxMediaLLMFiles: cfg.MaxMediaLLMFiles,
			MaxMonthlyCostMC: cfg.MaxMonthlyCostMC,
		},
		Usage: quotaUsageResponse{
			StorageBytes:   usage.StorageBytes,
			ReservedBytes:  usage.ReservedBytes,
			MediaFileCount: usage.MediaFileCount,
			MonthlyCostMC:  monthlyCost,
		},
	})
}

const quotaBackendNotFoundMessage = "backend service exception; please check TiDB Cloud starter/native cluster status"

func writeQuotaCredentialError(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, tenant.ErrQuotaPermissionDenied):
		errJSON(w, http.StatusForbidden, "no permission to "+action+" quota with TiDB Cloud API key")
	case errors.Is(err, tenant.ErrQuotaBackendNotFound):
		errJSON(w, http.StatusNotFound, quotaBackendNotFoundMessage)
	default:
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("tidbcloud quota %s failed: %v", action, err))
	}
}
