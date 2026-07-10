package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type quotaRequest struct {
	TenantID   string `json:"tenant_id"`
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
	quotaFields
}

type quotaFields struct {
	MaxStorageSize         *int64 `json:"max_storage_size,omitempty"`
	MaxFileSize            *int64 `json:"max_file_size,omitempty"`
	MaxFileCount           *int64 `json:"max_file_count,omitempty"`
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit,omitempty"`
}

func (f quotaFields) anySet() bool {
	return f.MaxStorageSize != nil ||
		f.MaxFileSize != nil ||
		f.MaxFileCount != nil ||
		f.TiDBCloudSpendingLimit != nil
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
	hasTiDBCloudCreds := req.PublicKey != "" || req.PrivateKey != ""
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
	cfg, err := s.meta.GetQuotaConfig(r.Context(), t.ID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "quota config lookup failed")
		return
	}
	if cfg.TiDBCloudSpendingLimit != nil && bearerToken(r) != "" {
		apiKeyTenant, apiKeyErr := s.resolveQuotaAPIKey(r.Context(), r)
		if apiKeyErr == nil && apiKeyTenant != nil && apiKeyTenant.Tenant.ID == t.ID {
			setRequestMetricTenant(r.Context(), t.ID, apiKeyTenant.APIKey.ID, t.Provider, classifyTenantRequest(r))
			s.writeQuotaResponse(w, r, t)
			return
		}
		if !hasTiDBCloudCreds {
			if apiKeyErr != nil {
				writeQuotaAPIKeyError(w, r.Context(), apiKeyErr)
				return
			}
			errJSON(w, http.StatusForbidden, "API key tenant does not match requested tenant")
			return
		}
	}
	cred, err := quotaCredentials(req)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	needRefresh := false
	if cfg.TiDBCloudSpendingLimit == nil {
		metrics.RecordTiDBCloudSpendingLimitMissing("quota_get")
		metrics.RecordTiDBCloudRBACCacheRequest("quota_get", "cluster", "bypass")
		needRefresh = true
	} else if !s.tidbCloudRBACAllowed(cred, t.ClusterID, "quota_get") {
		needRefresh = true
	}
	if needRefresh {
		observedAt := time.Now().UTC()
		cloudCfg, err := getter.GetQuota(r.Context(), clusterInfoFromTenant(t), cred)
		if err != nil {
			metrics.RecordTiDBCloudOpenAPIRequest("quota_get", "get_quota", "error")
			writeQuotaCredentialError(w, r.Context(), err, "query")
			return
		}
		metrics.RecordTiDBCloudOpenAPIRequest("quota_get", "get_quota", "ok")
		s.rememberTiDBCloudRBAC(cred, tenant.CloudClusterInfo{ClusterID: t.ClusterID})
		if err := s.syncTiDBCloudSpendingLimit(r.Context(), "quota_get", t.ID, cloudCfg, observedAt); err != nil {
			logger.Warn(r.Context(), "tidbcloud_spending_limit_sync_failed", zap.String("tenant_id", t.ID), zap.Error(err))
			errJSON(w, http.StatusInternalServerError, "quota config update failed")
			return
		}
	}
	setRequestMetricTenant(r.Context(), t.ID, "", t.Provider, classifyTenantRequest(r))
	s.writeQuotaResponse(w, r, t)
}

var (
	errQuotaAPIKeyAuthNotEnabled = errors.New("API key quota auth not enabled")
	errQuotaAPIKeyMissing        = errors.New("missing or malformed Authorization header")
	errQuotaAPIKeyInvalid        = errors.New("invalid API key")
)

func (s *Server) resolveQuotaAPIKey(ctx context.Context, r *http.Request) (*meta.TenantWithAPIKey, error) {
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		return nil, errQuotaAPIKeyAuthNotEnabled
	}
	tok := bearerToken(r)
	if tok == "" {
		return nil, errQuotaAPIKeyMissing
	}
	hash := token.HashToken(tok)
	resolved, err := s.meta.ResolveByAPIKeyHash(ctx, hash)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, errQuotaAPIKeyInvalid
		}
		return nil, fmt.Errorf("resolve API key: %w", err)
	}
	if subtle.ConstantTimeCompare([]byte(hash), []byte(resolved.APIKey.JWTHash)) != 1 {
		return nil, errQuotaAPIKeyInvalid
	}
	if resolved.APIKey.Status != meta.APIKeyActive {
		return nil, errQuotaAPIKeyInvalid
	}
	plain, err := poolDecryptToken(ctx, s.pool, resolved.APIKey.JWTCiphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt API key: %w", err)
	}
	if subtle.ConstantTimeCompare([]byte(tok), plain) != 1 {
		return nil, errQuotaAPIKeyInvalid
	}
	claims, err := token.ParseAndVerifyToken(s.tokenSecret, tok)
	if err != nil {
		return nil, errQuotaAPIKeyInvalid
	}
	if claims.TenantID != resolved.Tenant.ID || claims.TokenVersion != resolved.APIKey.TokenVersion {
		return nil, errQuotaAPIKeyInvalid
	}
	return resolved, nil
}

func writeQuotaAPIKeyError(w http.ResponseWriter, ctx context.Context, err error) {
	switch {
	case errors.Is(err, errQuotaAPIKeyMissing), errors.Is(err, errQuotaAPIKeyInvalid):
		errJSON(w, http.StatusUnauthorized, errQuotaAPIKeyInvalid.Error())
	case errors.Is(err, errQuotaAPIKeyAuthNotEnabled):
		errJSON(w, http.StatusNotFound, "quota query not enabled")
	default:
		logger.Warn(ctx, "quota_api_key_auth_failed", zap.Error(err))
		errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
	}
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
	if err := s.rejectQuotaSetForTenantStatus(t); err != nil {
		errJSON(w, http.StatusConflict, err.Error())
		return
	}
	if err := s.applyQuotaSet(r.Context(), "quota_set", t, cred, req); err != nil {
		writeQuotaSetError(w, r.Context(), err, "update")
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
	if req.TiDBCloudSpendingLimit != nil && *req.TiDBCloudSpendingLimit > 0 && *req.TiDBCloudSpendingLimit < 10 {
		return fmt.Errorf("tidbcloud_spending_limit must be 0 or at least 10 RMB")
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

func (s *Server) rejectQuotaSetForTenantStatus(t *meta.Tenant) error {
	if t == nil {
		return nil
	}
	switch t.Status {
	case meta.TenantProvisioning:
		return fmt.Errorf("tenant is still provisioning")
	case meta.TenantDeleting:
		return fmt.Errorf("tenant is deleting")
	default:
		return nil
	}
}

func (s *Server) applyQuotaSet(ctx context.Context, metricPath string, t *meta.Tenant, cred tenant.CredentialProvisionRequest, req quotaRequest) error {
	if t == nil {
		return fmt.Errorf("tenant is required")
	}
	if strings.TrimSpace(t.ClusterID) == "" {
		return tenant.ErrQuotaBackendNotFound
	}
	updater, ok := s.provisioner.(tenant.QuotaUpdater)
	if !ok {
		return errQuotaSettingNotEnabled
	}
	cloudCfg, err := updater.MarkQuotaUpdateStarted(ctx, clusterInfoFromTenant(t), cred)
	if err != nil {
		metrics.RecordTiDBCloudOpenAPIRequest(metricPath, "mark_quota_update_started", "error")
		return err
	}
	metrics.RecordTiDBCloudOpenAPIRequest(metricPath, "mark_quota_update_started", "ok")
	s.rememberTiDBCloudRBAC(cred, tenant.CloudClusterInfo{ClusterID: t.ClusterID})
	if req.TiDBCloudSpendingLimit != nil {
		updatedCloudCfg, err := updater.UpdateQuota(ctx, clusterInfoFromTenant(t), cred, tenant.QuotaUpdateOptions{
			TiDBCloudSpendingLimitMonthly: req.TiDBCloudSpendingLimit,
		})
		if err != nil {
			metrics.RecordTiDBCloudOpenAPIRequest(metricPath, "update_quota", "error")
			return err
		}
		metrics.RecordTiDBCloudOpenAPIRequest(metricPath, "update_quota", "ok")
		if cloudCfg == nil {
			cloudCfg = updatedCloudCfg
		} else if updatedCloudCfg != nil && updatedCloudCfg.TiDBCloudSpendingLimitMonthly != nil {
			cloudCfg.TiDBCloudSpendingLimitMonthly = updatedCloudCfg.TiDBCloudSpendingLimitMonthly
		}
	}
	quotaPatch, err := quotaConfigPatchFromRequest(req)
	if err != nil {
		return err
	}
	if cloudLimit := tidbCloudSpendingLimitFromCloud(cloudCfg); cloudLimit != nil {
		quotaPatch.TiDBCloudSpendingLimit = cloudLimit
	}
	if quotaPatch.AnySet() {
		hasSpendingLimitPatch := quotaPatch.TiDBCloudSpendingLimit != nil
		spendingSyncResult := ""
		if hasSpendingLimitPatch {
			if result, resultErr := s.tidbCloudSpendingLimitSyncResult(ctx, t.ID, quotaPatch.TiDBCloudSpendingLimit); resultErr == nil {
				spendingSyncResult = result
			}
		}
		if err := s.meta.SetQuotaConfigPatch(ctx, t.ID, quotaPatch); err != nil {
			if hasSpendingLimitPatch {
				metrics.RecordTiDBCloudSpendingLimitSync(metricPath, "error")
			}
			return fmt.Errorf("%w: quota config update failed: %w", errQuotaLocalUpdateFailed, err)
		}
		if spendingSyncResult != "" {
			metrics.RecordTiDBCloudSpendingLimitSync(metricPath, spendingSyncResult)
		}
		if err := s.meta.EnsureQuotaUsageRow(ctx, t.ID); err != nil {
			return fmt.Errorf("%w: quota usage initialization failed: %w", errQuotaLocalUpdateFailed, err)
		}
	}
	return nil
}

func (s *Server) applyQuotaLocalConfig(ctx context.Context, source, tenantID string, req quotaRequest) error {
	quotaPatch, err := quotaConfigPatchFromRequest(req)
	if err != nil {
		return err
	}
	if !quotaPatch.AnySet() {
		return nil
	}
	hasSpendingLimitPatch := quotaPatch.TiDBCloudSpendingLimit != nil
	spendingSyncResult := ""
	if hasSpendingLimitPatch {
		if result, resultErr := s.tidbCloudSpendingLimitSyncResult(ctx, tenantID, quotaPatch.TiDBCloudSpendingLimit); resultErr == nil {
			spendingSyncResult = result
		}
	}
	if err := s.meta.SetQuotaConfigPatch(ctx, tenantID, quotaPatch); err != nil {
		if hasSpendingLimitPatch {
			metrics.RecordTiDBCloudSpendingLimitSync(source, "error")
		}
		return fmt.Errorf("%w: quota config update failed: %w", errQuotaLocalUpdateFailed, err)
	}
	if spendingSyncResult != "" {
		metrics.RecordTiDBCloudSpendingLimitSync(source, spendingSyncResult)
	}
	if err := s.meta.EnsureQuotaUsageRow(ctx, tenantID); err != nil {
		return fmt.Errorf("%w: quota usage initialization failed: %w", errQuotaLocalUpdateFailed, err)
	}
	return nil
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
	if req.TiDBCloudSpendingLimit != nil {
		patch.TiDBCloudSpendingLimit = req.TiDBCloudSpendingLimit
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
			TiDBCloudSpendingLimit: cfg.TiDBCloudSpendingLimit,
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
	if errors.Is(err, tenant.ErrQuotaBackendNotFound) {
		errJSON(w, http.StatusNotFound, quotaBackendNotFoundMessage)
		return
	}
	logger.Warn(ctx, "tidbcloud_quota_failed", zap.String("action", action), zap.Error(err))
	status, msg := clientFacingErrorResponse(http.StatusBadGateway, "tidbcloud quota "+action+" failed", err)
	errJSON(w, status, msg)
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

func (s *Server) tidbCloudRBACAllowed(cred tenant.CredentialProvisionRequest, clusterID, metricPath string) bool {
	_, ok := s.tidbCloudRBACCache.getCluster(cred, clusterID)
	result := "miss"
	if ok {
		result = "hit"
	}
	metrics.RecordTiDBCloudRBACCacheRequest(metricPath, "cluster", result)
	return ok
}

func (s *Server) rememberTiDBCloudRBAC(cred tenant.CredentialProvisionRequest, cluster tenant.CloudClusterInfo) {
	s.tidbCloudRBACCache.rememberCluster(cred, cluster)
}

func (s *Server) forgetTiDBCloudRBACList(cred tenant.CredentialProvisionRequest) {
	s.tidbCloudRBACCache.forgetClusterList(cred)
}

func tidbCloudSpendingLimitFromCloud(cloudCfg *tenant.QuotaCloudConfig) *int64 {
	if cloudCfg == nil || cloudCfg.TiDBCloudSpendingLimitMonthly == nil {
		return nil
	}
	limit := *cloudCfg.TiDBCloudSpendingLimitMonthly
	return &limit
}

func (s *Server) syncTiDBCloudSpendingLimit(ctx context.Context, source, tenantID string, cloudCfg *tenant.QuotaCloudConfig, observedAt time.Time) error {
	checkedAt := time.Now().UTC()
	limit := tidbCloudSpendingLimitFromCloud(cloudCfg)
	if limit == nil {
		if err := s.meta.SetQuotaConfigPatch(ctx, tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimitCheckedAt: &checkedAt}); err != nil {
			metrics.RecordTiDBCloudSpendingLimitSync(source, "error")
			return err
		}
		metrics.RecordTiDBCloudSpendingLimitSync(source, "missing_cloud_value")
		return nil
	}
	result, err := s.tidbCloudSpendingLimitSyncResult(ctx, tenantID, limit)
	if err != nil {
		metrics.RecordTiDBCloudSpendingLimitSync(source, "error")
		return err
	}
	if result == "unchanged" {
		if err := s.meta.SetQuotaConfigPatch(ctx, tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimitCheckedAt: &checkedAt}); err != nil {
			metrics.RecordTiDBCloudSpendingLimitSync(source, "error")
			return err
		}
		metrics.RecordTiDBCloudSpendingLimitSync(source, result)
		return nil
	}
	if !observedAt.IsZero() {
		cfg, err := s.meta.GetQuotaConfig(ctx, tenantID)
		if err != nil {
			metrics.RecordTiDBCloudSpendingLimitSync(source, "error")
			return err
		}
		if cfg.TiDBCloudSpendingLimit != nil && cfg.UpdatedAt.After(observedAt) {
			metrics.RecordTiDBCloudSpendingLimitSync(source, "skipped_newer_local")
			return nil
		}
	}
	if err := s.meta.SetQuotaConfigPatch(ctx, tenantID, meta.QuotaConfigPatch{
		TiDBCloudSpendingLimit:          limit,
		TiDBCloudSpendingLimitCheckedAt: &checkedAt,
	}); err != nil {
		metrics.RecordTiDBCloudSpendingLimitSync(source, "error")
		return err
	}
	metrics.RecordTiDBCloudSpendingLimitSync(source, result)
	return nil
}

func (s *Server) tidbCloudSpendingLimitSyncResult(ctx context.Context, tenantID string, limit *int64) (string, error) {
	if limit == nil {
		return "", nil
	}
	cfg, err := s.meta.GetQuotaConfig(ctx, tenantID)
	if err != nil {
		return "error", err
	}
	if cfg.TiDBCloudSpendingLimit == nil {
		return "inserted", nil
	}
	if *cfg.TiDBCloudSpendingLimit == *limit {
		return "unchanged", nil
	}
	return "updated", nil
}
