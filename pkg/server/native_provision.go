package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
	"github.com/mem9-ai/dat9/pkg/tenant/tidbcloudnative"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
)

// handleNativeProvision handles the tidbcloud-native provision flow.
// Two paths:
//   - Zero instance: verify instance exists via Global Server, then provision by clusterID
//   - Cluster: authorize via account service, then provision by clusterID
//
// In both cases tenantID = clusterID (always populated by ParseHeaders).
func (s *Server) handleNativeProvision(w http.ResponseWriter, r *http.Request, target *tidbcloud.ResolvedTarget) {
	provider := tenant.ProviderTiDBCloudNative
	ctx := r.Context()

	np, ok := s.provisioner.(*tidbcloudnative.Provisioner)
	if !ok {
		logger.Warn(ctx, "server_event", eventFields(ctx, "native_provisioner_not_configured")...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("unsupported %s header", tidbcloud.HeaderForTarget(target.Type)))
		return
	}

	tenantID := target.ClusterID

	switch target.Type {
	case tidbcloud.TargetZeroInstance:
		// Verify the instance ID actually exists in TiDB Cloud to prevent forgery.
		if err := np.VerifyZeroInstance(ctx, target.InstanceID); err != nil {
			if tidbcloud.IsNotFound(err) {
				logger.Warn(ctx, "server_event", eventFields(ctx, "native_provision_instance_not_found",
					"instance_id", target.InstanceID, "error", err)...)
				metricEvent(ctx, "tenant_provision", "provider", provider, "result", "not_found")
				errJSON(w, http.StatusNotFound, err.Error())
				return
			}
			logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_verify_instance_failed",
				"instance_id", target.InstanceID, "error", err)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
			errJSON(w, http.StatusBadGateway, fmt.Sprintf("verify instance failed: %v", err))
			return
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_requested",
			"tenant_id", tenantID, "provider", provider, "target_type", "instance",
			"instance_id", target.InstanceID)...)

	case tidbcloud.TargetCluster:
		// Authorize via account service.
		if authErr := np.Authorize(ctx, r, target.ClusterID); authErr != nil {
			if status, ok := tidbcloud.IsAuthError(authErr); ok {
				logger.Warn(ctx, "server_event", eventFields(ctx, "native_provision_auth_failed", "cluster_id", target.ClusterID, "error", authErr)...)
				metricEvent(ctx, "tenant_provision", "provider", provider, "result", "auth_error")
				errJSON(w, status, authErr.Error())
				return
			}
			logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_auth_failed", "cluster_id", target.ClusterID, "error", authErr)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "auth_error")
			errJSON(w, http.StatusForbidden, authErr.Error())
			return
		}
		logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_requested",
			"tenant_id", tenantID, "provider", provider, "target_type", "cluster")...)
	}

	// Both paths converge: provision by clusterID.
	cluster, err := np.Provision(ctx, tenantID)

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

	// Fetch cluster proxy info (proxy endpoint, version, cloud_admin credentials).
	// When the LB blocks cloud_admin, we use the proxy to:
	// 1) execute schema init DDL
	// 2) create a dedicated service user for runtime SQL operations
	proxyInfo, err := np.GetClusterProxyInfo(ctx, tenantID)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_get_proxy_info_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("get proxy info failed: %v", err))
		return
	}

	// Determine the DB user and password for runtime operations.
	// When proxy is available, create a dedicated service user (fs_admin)
	// that the LB allows. Otherwise fall back to cloud_admin.
	dbUser := cluster.Username
	dbPassword := cluster.Password
	schemaInit := np.InitSchema

	if proxyInfo != nil {
		proxy := tidbcloud.NewClusterProxyClient(
			proxyInfo.ProxyEndpoint, proxyInfo.ClusterID,
			proxyInfo.Username, proxyInfo.Password, proxyInfo.Version)

		// Extract the user prefix from the cloud_admin username (e.g. "2wCQ.cloud_admin" → "2wCQ").
		userPrefix := extractUserPrefix(proxyInfo.Username)

		// Create a service user via proxy for runtime SQL.
		svcUser, svcErr := proxy.CreateServiceUser(ctx, userPrefix, cluster.DBName)
		if svcErr != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_create_svc_user_failed",
				"tenant_id", tenantID, "error", svcErr)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
			errJSON(w, http.StatusBadGateway, fmt.Sprintf("create service user failed: %v", svcErr))
			return
		}
		dbUser = svcUser.Username
		dbPassword = svcUser.Password

		logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_svc_user_created",
			"tenant_id", tenantID, "db_user", dbUser)...)

		// Schema init uses proxy (cloud_admin) since DDL needs higher privileges.
		schemaInit = func(initCtx context.Context, _ string) error {
			stmts := schema.TiDBAutoEmbeddingSchemaStatements()
			return proxy.ExecSchemaStatements(initCtx, stmts)
		}
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

	cipherPass, err := s.pool.Encrypt(ctx, []byte(dbPassword))
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_encrypt_db_password_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to encrypt db password")
		return
	}
	cipherToken, err := s.pool.Encrypt(ctx, []byte(apiToken))
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_encrypt_api_key_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to encrypt api key")
		return
	}

	if err := s.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           dbUser,
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

	// Schema init uses proxy (cloud_admin privileges); runtime DSN uses service user.
	dsn := tenantDSN(dbUser, dbPassword, cluster.Host, cluster.Port, cluster.DBName, true)
	go s.initTenantSchemaAsync(backgroundWithTrace(ctx), tenantID, dsn, provider, schemaInit)

	logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_accepted", "tenant_id", tenantID, "provider", provider)...)
	metricEvent(ctx, "tenant_provision", "provider", provider, "result", "accepted")

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"api_key": apiToken,
		"status":  string(meta.TenantProvisioning),
	})
}

// extractUserPrefix extracts the user prefix from a serverless username.
// e.g. "2wCQKHWXMegHiR8.cloud_admin" → "2wCQKHWXMegHiR8"
// Returns empty string if no prefix is present.
func extractUserPrefix(username string) string {
	if i := strings.LastIndex(username, "."); i >= 0 {
		return username[:i]
	}
	return ""
}
