package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"go.uber.org/zap"

	_ "github.com/go-sql-driver/mysql" // register MySQL driver for sql.Open

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
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

	// Encrypt cloud_admin password as the initial credential.
	// The async goroutine will overwrite these with a dedicated service user
	// (fs_admin) once created. If the server restarts before that happens,
	// resumeProvisioningTenants re-runs the full nativeProvisionAsync flow
	// which creates fs_admin and updates the persisted credentials.
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
	// The tenant is already persisted with status=provisioning and cloud_admin
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
// has been persisted. It creates a dedicated service user (fs_admin) via the
// cluster proxy, updates the tenant DB credentials, then runs schema init.
// GRANT role_admin is best-effort inside CreateServiceUser; fs_admin may have
// limited privileges but we still attempt schema init with it.
func (s *Server) nativeProvisionAsync(ctx context.Context, np *tidbcloudnative.Provisioner, tenantID string, cluster *tenant.ClusterInfo, provider string) {
	// Step 1: create a dedicated service user via cluster proxy.
	proxyInfo, err := np.GetClusterProxyInfo(ctx, tenantID)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_get_proxy_info_failed",
			"tenant_id", tenantID, "error", err)...)
		_ = s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		return
	}
	if proxyInfo == nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_no_proxy_endpoint",
			"tenant_id", tenantID)...)
		_ = s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		return
	}

	svcUser, svcErr := np.CreateServiceUser(ctx, tenantID, proxyInfo)
	if svcErr != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_create_svc_user_failed",
			"tenant_id", tenantID, "error", svcErr)...)
		_ = s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		return
	}

	dbUser := svcUser.Username
	dbPassword := svcUser.Password
	dbName := svcUser.DBName
	logger.Info(ctx, "server_event", eventFields(ctx, "native_provision_svc_user_created",
		"tenant_id", tenantID, "db_user", dbUser, "db_name", dbName)...)

	// Step 2: persist the service user credentials and database name.
	cipherPass, encErr := s.pool.Encrypt(ctx, []byte(dbPassword))
	if encErr != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_encrypt_db_password_failed",
			"tenant_id", tenantID, "error", encErr)...)
		_ = s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		return
	}
	if updErr := s.meta.UpdateTenantDBCredentials(ctx, tenantID, dbUser, cipherPass, dbName); updErr != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_update_db_creds_failed",
			"tenant_id", tenantID, "error", updErr)...)
		_ = s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		return
	}
	s.pool.Invalidate(tenantID)

	// Step 3: if using a dedicated database, fs_admin creates it first.
	if dbName != "mysql" {
		bootDSN := tenantDSN(dbUser, dbPassword, cluster.Host, cluster.Port, "", true)
		if err := createDatabaseIfNotExists(ctx, bootDSN, dbName); err != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "native_provision_create_db_failed",
				"tenant_id", tenantID, "db_name", dbName, "error", err)...)
			_ = s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
			return
		}
	}

	// Step 4: schema init with the service user's database.
	dsn := tenantDSN(dbUser, dbPassword, cluster.Host, cluster.Port, dbName, true)
	s.initTenantSchemaAsync(ctx, tenantID, dsn, provider, np.InitSchema)
}

// validDBName matches safe database identifiers: ASCII letters, digits, underscores.
var validDBName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// createDatabaseIfNotExists connects to the cluster without a database and
// executes CREATE DATABASE IF NOT EXISTS. Used when fs_admin cannot access the
// default "mysql" database and needs a dedicated database.
func createDatabaseIfNotExists(ctx context.Context, dsn, dbName string) error {
	if !validDBName.MatchString(dbName) {
		return fmt.Errorf("invalid database name: %q", dbName)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open bootstrap connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping bootstrap connection: %w", err)
	}

	stmt := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create database %s: %w", dbName, err)
	}

	logger.Info(ctx, "native_provision_database_created", zap.String("db_name", dbName))
	return nil
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
