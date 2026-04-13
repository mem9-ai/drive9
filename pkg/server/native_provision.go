package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/tidbcloudnative"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
)

// nativeProvisionRequest holds the root database credentials sent by the client.
type nativeProvisionRequest struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// handleNativeProvision handles the tidbcloud-native provision flow.
// Two paths:
//   - Zero instance: verify instance exists via Global Server, then provision by clusterID
//   - Cluster: authorize via account service, then provision by clusterID
//
// Both paths require the caller to supply root database credentials in the
// JSON request body ({"user":"...","password":"..."}). These are used to
// create the fs_admin service user directly via the public load balancer.
//
// In both cases tenantID = clusterID (always populated by ParseHeaders).
func (s *Server) handleNativeProvision(w http.ResponseWriter, r *http.Request, target *tidbcloud.ResolvedTarget) {
	provider := tenant.ProviderTiDBCloudNative
	ctx := r.Context()

	np, ok := authorizeNativeTarget(ctx, w, r, s.provisioner, target)
	if !ok {
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		return
	}

	tenantID := target.ClusterID

	switch target.Type {
	case tidbcloud.TargetZeroInstance:
		logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_requested",
			"tenant_id", tenantID, "provider", provider, "target_type", "instance",
			"instance_id", target.InstanceID)...)
	case tidbcloud.TargetCluster:
		logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_requested",
			"tenant_id", tenantID, "provider", provider, "target_type", "cluster")...)
	}

	// Parse root database credentials from request body.
	var req nativeProvisionRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		logger.Warn(ctx, "server_event", eventFields(ctx, "native_provision_bad_body", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "bad_request")
		errJSON(w, http.StatusBadRequest, "invalid request body: user and password are required")
		return
	}
	if req.User == "" || req.Password == "" {
		logger.Warn(ctx, "server_event", eventFields(ctx, "native_provision_missing_creds", "tenant_id", tenantID)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "bad_request")
		errJSON(w, http.StatusBadRequest, "user and password are required")
		return
	}

	// Both paths converge: resolve cluster connection info (host/port),
	// using the caller-supplied root credentials.
	cluster, err := np.ProvisionWithRootCreds(ctx, tenantID, req.User, req.Password)

	if err != nil {
		if tidbcloud.IsNotFound(err) {
			logger.Warn(ctx, "server_event", eventFields(ctx, "native_provision_not_found", "tenant_id", tenantID, "error", err)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "not_found")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "provision_error")
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("provision failed: %v", err))
		return
	}

	apiToken, err := token.IssueToken(s.tokenSecret, tenantID, 1)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_issue_token_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to issue token")
		return
	}
	hash := token.HashToken(apiToken)
	now := time.Now().UTC()

	cipherToken, err := s.pool.Encrypt(ctx, []byte(apiToken))
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_encrypt_api_key_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to encrypt api key")
		return
	}

	// Encrypt root password — root is used directly for all DB operations.
	// TiDB Cloud Serverless root lacks GRANT OPTION, so we cannot create
	// a dedicated service user. The root credentials are persisted as-is.
	cipherPass, err := s.pool.Encrypt(ctx, []byte(cluster.Password))
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_encrypt_db_password_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to encrypt db password")
		return
	}

	if err := s.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           cluster.Username,
		DBPasswordCipher: cipherPass,
		DBName:           cluster.DBName,
		DBTLS:            true,
		Provider:         provider,
		ClusterID:        cluster.ClusterID,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		if errors.Is(err, meta.ErrDuplicate) {
			logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_already_exists", "tenant_id", tenantID, "provider", provider)...)
			errJSON(w, http.StatusConflict, "tenant already provisioned")
			return
		}
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_insert_tenant_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to persist tenant")
		return
	}

	apiKeyID := token.NewID()
	if err := s.meta.InsertAPIKey(ctx, &meta.APIKey{
		ID:            apiKeyID,
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: cipherToken,
		JWTHash:       hash,
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_insert_api_key_failed", "tenant_id", tenantID, "api_key_id", apiKeyID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		_ = s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantDeleted)
		errJSON(w, http.StatusInternalServerError, "failed to persist api key")
		return
	}

	// Service user creation and schema init run in a background goroutine.
	// The tenant is already persisted with status=provisioning and root
	// credentials, so /v1/status returns "provisioning" and server restarts
	// can resume via resumeProvisioningTenants.
	go s.nativeProvisionAsync(backgroundWithTrace(ctx), np, tenantID, cluster, provider)

	logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_accepted", "tenant_id", tenantID, "provider", provider)...)
	metricEvent(ctx, "tenant_provision", "provider", provider, "result", "accepted")

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"api_key": apiToken,
		"status":  string(meta.TenantProvisioning),
	})
}

// nativeProvisionAsync runs in a background goroutine after the tenant record
// has been persisted. It runs schema init using the root credentials directly.
// TiDB Cloud Serverless root lacks GRANT OPTION, so no dedicated service user
// (fs_admin) is created — root is used for all DB operations.
func (s *Server) nativeProvisionAsync(ctx context.Context, np *tidbcloudnative.Provisioner, tenantID string, cluster *tenant.ClusterInfo, provider string) {
	dbName := cluster.DBName
	if dbName == "" {
		dbName = "mysql"
	}

	dsn := tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, dbName, true)
	s.initTenantSchemaAsync(ctx, tenantID, dsn, provider, np.InitSchema)
}

// handleNativeTenantStatus checks for tidbcloud-native headers on /v1/status.
// Returns (status, true) if handled; ("", false) if no native headers present.
func (s *Server) handleNativeTenantStatus(w http.ResponseWriter, r *http.Request) (string, bool) {
	target, err := tidbcloud.ParseHeaders(r)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_bad_tidbcloud_header", "error", err)...)
		errJSON(w, http.StatusBadRequest, err.Error())
		return "", true
	}
	if target == nil {
		return "", false
	}

	ctx := r.Context()
	if _, ok := authorizeNativeTarget(ctx, w, r, s.provisioner, target); !ok {
		return "", true
	}

	tenantID := target.ClusterID
	t, err := s.meta.GetTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_status_native_not_found", "tenant_id", tenantID)...)
			errJSON(w, http.StatusNotFound, "tenant not found")
			return "", true
		}
		logger.Error(ctx, "server_event", eventFields(ctx, "tenant_status_native_meta_error", "tenant_id", tenantID, "error", err)...)
		errJSON(w, http.StatusInternalServerError, "meta backend unavailable")
		return "", true
	}

	logger.Info(ctx, "server_event", eventFields(ctx, "tenant_status_native_ok", "tenant_id", tenantID, "status", t.Status)...)
	return string(t.Status), true
}
