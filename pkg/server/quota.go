package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

type quotaRequest struct {
	TenantID               string `json:"tenant_id"`
	PublicKey              string `json:"public_key"`
	PrivateKey             string `json:"private_key"`
	MaxStorageSize         *int64 `json:"max_storage_size,omitempty"`
	MaxFileSize            *int64 `json:"max_file_size,omitempty"`
	MaxFileCount           *int64 `json:"max_file_count,omitempty"`
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit,omitempty"`
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
	MaxStorageSize         int64  `json:"max_storage_size"`
	MaxFileSize            int64  `json:"max_file_size"`
	MaxFileCount           int64  `json:"max_file_count"`
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit"`
}

type quotaUsageResponse struct {
	StorageBytes  int64 `json:"storage_bytes"`
	ReservedBytes int64 `json:"reserved_bytes"`
	FileCount     int64 `json:"file_count"`
}

const (
	quotaTenantIDQueryParam = "tenant_id"
	quotaTenantIDHeader     = "X-Drive9-Tenant-ID"
	quotaPublicKeyHeader    = "X-TiDBCloud-Public-Key"
	quotaPrivateKeyHeader   = "X-TiDBCloud-Private-Key"
)

func (s *Server) quotaRootHandler(_ Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleQuotaGet(w, r)
		case http.MethodPost:
			s.handleQuotaSet(w, r)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *Server) handleQuotaGet(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.provisioner == nil {
		errJSON(w, http.StatusNotFound, "quota query not enabled")
		return
	}
	req := decodeQuotaGetRequest(r)
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
	if strings.TrimSpace(t.ClusterID) == "" {
		errJSON(w, http.StatusNotFound, quotaBackendNotFoundMessage)
		return
	}
	getter, ok := s.provisioner.(tenant.QuotaGetter)
	if !ok {
		errJSON(w, http.StatusNotFound, "quota query not enabled")
		return
	}
	cloudCfg, err := getter.GetQuota(r.Context(), clusterInfoFromTenant(t), cred)
	if err != nil {
		writeQuotaCredentialError(w, r.Context(), err, "query")
		return
	}
	setRequestMetricTenant(r.Context(), t.ID, "", t.Provider, classifyTenantRequest(r))
	s.writeQuotaResponse(w, r, t, cloudCfg)
}

func decodeQuotaGetRequest(r *http.Request) quotaRequest {
	q := r.URL.Query()
	return quotaRequest{
		TenantID:   strings.TrimSpace(firstNonEmpty(q.Get(quotaTenantIDQueryParam), r.Header.Get(quotaTenantIDHeader))),
		PublicKey:  strings.TrimSpace(r.Header.Get(quotaPublicKeyHeader)),
		PrivateKey: strings.TrimSpace(r.Header.Get(quotaPrivateKeyHeader)),
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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
	if err := s.validateQuotaSetRequest(req); err != nil {
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
	cloudCfg, err := s.applyQuotaSet(r.Context(), t, cred, req)
	if err != nil {
		writeQuotaSetError(w, r.Context(), err, "update")
		return
	}
	setRequestMetricTenant(r.Context(), t.ID, "", t.Provider, classifyTenantRequest(r))
	s.writeQuotaResponse(w, r, t, cloudCfg)
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

func (s *Server) validateQuotaSetRequest(req quotaRequest) error {
	if req.MaxStorageSize == nil && req.MaxFileSize == nil && req.MaxFileCount == nil && req.TiDBCloudSpendingLimit == nil {
		return fmt.Errorf("at least one of max_storage_size, max_file_size, max_file_count, or tidbcloud_spending_limit is required")
	}
	if req.MaxStorageSize != nil && *req.MaxStorageSize <= 0 {
		return fmt.Errorf("max_storage_size must be positive")
	}
	if req.MaxStorageSize != nil {
		if _, err := quotaStorageSizeToBytes(*req.MaxStorageSize); err != nil {
			return err
		}
	}
	if req.MaxFileSize != nil && *req.MaxFileSize <= 0 {
		return fmt.Errorf("max_file_size must be positive")
	}
	if req.MaxFileSize != nil {
		maxFileSizeBytes, err := quotaFileSizeToBytes(*req.MaxFileSize)
		if err != nil {
			return err
		}
		if s.maxUploadBytes > 0 && maxFileSizeBytes > s.maxUploadBytes {
			return fmt.Errorf("max_file_size must be less than or equal to server max upload size (%dMi)", s.maxUploadBytes/quotaStorageSizeBytes)
		}
	}
	if req.MaxFileCount != nil && *req.MaxFileCount < 0 {
		return fmt.Errorf("max_file_count must be non-negative")
	}
	if req.TiDBCloudSpendingLimit != nil && *req.TiDBCloudSpendingLimit < 0 {
		return fmt.Errorf("tidbcloud_spending_limit must be non-negative")
	}
	if req.TiDBCloudSpendingLimit != nil && *req.TiDBCloudSpendingLimit > maxInt32 {
		return fmt.Errorf("tidbcloud_spending_limit is too large")
	}
	return nil
}

const maxInt32 = int64(1<<31 - 1)

var (
	errQuotaSettingNotEnabled = errors.New("quota setting not enabled")
	errQuotaLocalUpdateFailed = errors.New("quota local update failed")
)

func (s *Server) applyQuotaSet(ctx context.Context, t *meta.Tenant, cred tenant.CredentialProvisionRequest, req quotaRequest) (*tenant.QuotaCloudConfig, error) {
	if t == nil {
		return nil, fmt.Errorf("tenant is required")
	}
	if strings.TrimSpace(t.ClusterID) == "" {
		return nil, tenant.ErrQuotaBackendNotFound
	}
	updater, ok := s.provisioner.(tenant.QuotaUpdater)
	if !ok {
		return nil, errQuotaSettingNotEnabled
	}
	cloudCfg, err := updater.MarkQuotaUpdateStarted(ctx, clusterInfoFromTenant(t), cred)
	if err != nil {
		return nil, err
	}
	if req.TiDBCloudSpendingLimit != nil {
		updatedCloudCfg, err := updater.UpdateQuota(ctx, clusterInfoFromTenant(t), cred, tenant.QuotaUpdateOptions{
			TiDBCloudSpendingLimitMonthly: req.TiDBCloudSpendingLimit,
		})
		if err != nil {
			return nil, err
		}
		if cloudCfg == nil {
			cloudCfg = updatedCloudCfg
		} else if updatedCloudCfg != nil && updatedCloudCfg.TiDBCloudSpendingLimitMonthly != nil {
			cloudCfg.TiDBCloudSpendingLimitMonthly = updatedCloudCfg.TiDBCloudSpendingLimitMonthly
		}
	}
	quotaPatch, err := quotaConfigPatchFromRequest(req)
	if err != nil {
		return nil, err
	}
	if quotaPatch.MaxStorageBytes != nil || quotaPatch.MaxFileSizeBytes != nil || quotaPatch.MaxFileCount != nil {
		if err := s.meta.SetQuotaConfigPatch(ctx, t.ID, quotaPatch); err != nil {
			return nil, fmt.Errorf("%w: quota config update failed: %w", errQuotaLocalUpdateFailed, err)
		}
		if err := s.meta.EnsureQuotaUsageRow(ctx, t.ID); err != nil {
			return nil, fmt.Errorf("%w: quota usage initialization failed: %w", errQuotaLocalUpdateFailed, err)
		}
	}
	return cloudCfg, nil
}

func quotaConfigPatchFromRequest(req quotaRequest) (meta.QuotaConfigPatch, error) {
	var patch meta.QuotaConfigPatch
	if req.MaxStorageSize != nil {
		maxStorageBytes, err := quotaStorageSizeToBytes(*req.MaxStorageSize)
		if err != nil {
			return patch, err
		}
		patch.MaxStorageBytes = &maxStorageBytes
	}
	if req.MaxFileSize != nil {
		maxFileSizeBytes, err := quotaFileSizeToBytes(*req.MaxFileSize)
		if err != nil {
			return patch, err
		}
		patch.MaxFileSizeBytes = &maxFileSizeBytes
	}
	if req.MaxFileCount != nil {
		patch.MaxFileCount = req.MaxFileCount
	}
	return patch, nil
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

func (s *Server) writeQuotaResponse(w http.ResponseWriter, r *http.Request, t *meta.Tenant, cloudCfg *tenant.QuotaCloudConfig) {
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
	var spendingLimit *int64
	if cloudCfg != nil {
		spendingLimit = cloudCfg.TiDBCloudSpendingLimitMonthly
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(quotaResponse{
		TenantID:       t.ID,
		Provider:       t.Provider,
		Status:         string(t.Status),
		SupportsUpdate: t.Provider == tenant.ProviderTiDBCloudNative,
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
	})
}

const quotaStorageSizeBytes int64 = 1024 * 1024

func quotaStorageSizeToBytes(sizeMi int64) (int64, error) {
	return quotaSizeToBytes("max_storage_size", sizeMi)
}

func quotaFileSizeToBytes(sizeMi int64) (int64, error) {
	return quotaSizeToBytes("max_file_size", sizeMi)
}

func quotaSizeToBytes(field string, sizeMi int64) (int64, error) {
	const maxInt64 = int64(1<<63 - 1)
	if sizeMi > maxInt64/quotaStorageSizeBytes {
		return 0, fmt.Errorf("%s is too large", field)
	}
	return sizeMi * quotaStorageSizeBytes, nil
}

func quotaStorageBytesToSize(sizeBytes int64) int64 {
	if sizeBytes <= 0 {
		return 0
	}
	return (sizeBytes + quotaStorageSizeBytes - 1) / quotaStorageSizeBytes
}

const quotaBackendNotFoundMessage = "backend service exception; please check TiDB Cloud starter/native cluster status"

func writeQuotaCredentialError(w http.ResponseWriter, ctx context.Context, err error, action string) {
	switch {
	case errors.Is(err, tenant.ErrQuotaPermissionDenied):
		errJSON(w, http.StatusForbidden, "no permission to "+action+" quota with TiDB Cloud API key")
	case errors.Is(err, tenant.ErrQuotaBackendNotFound):
		errJSON(w, http.StatusNotFound, quotaBackendNotFoundMessage)
	default:
		logger.Warn(ctx, "tidbcloud_quota_failed", zap.String("action", action), zap.Error(err))
		errJSON(w, http.StatusBadGateway, "tidbcloud quota "+action+" failed")
	}
}

func writeQuotaSetError(w http.ResponseWriter, ctx context.Context, err error, action string) {
	if status, msg, ok := quotaSetErrorStatusMessage(err, action); ok {
		if errors.Is(err, errQuotaLocalUpdateFailed) {
			logger.Warn(ctx, "drive9_quota_update_failed", zap.String("action", action), zap.Error(err))
		}
		errJSON(w, status, msg)
		return
	}
	writeQuotaCredentialError(w, ctx, err, action)
}

func quotaSetErrorStatusMessage(err error, action string) (int, string, bool) {
	switch {
	case errors.Is(err, errQuotaSettingNotEnabled):
		return http.StatusNotFound, "quota setting not enabled", true
	case errors.Is(err, errQuotaLocalUpdateFailed):
		return http.StatusInternalServerError, "quota " + action + " failed", true
	default:
		return 0, "", false
	}
}
