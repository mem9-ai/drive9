// Package tidbcloudnative implements the tidbcloud-native tenant provisioner.
//
// A TiDB Cloud cluster IS a drive9 tenant. The cluster ID is used as the
// tenant ID. For zero-instance requests, the cluster ID is parsed from the
// instance ID locally (no RPC needed) by the server layer before calling
// Provision.
//
// Provision() calls Global Server to fetch cluster endpoint + encrypted
// cloud_admin password, then decrypts via KMS.
package tidbcloudnative

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
	"go.uber.org/zap"
)

// Provisioner implements tenant.Provisioner for the tidbcloud-native provider.
type Provisioner struct {
	global  tidbcloud.GlobalClient
	account tidbcloud.AccountClient
	enc     encrypt.Encryptor
}

// NewProvisioner creates a Provisioner with the given dependencies.
func NewProvisioner(global tidbcloud.GlobalClient, account tidbcloud.AccountClient, enc encrypt.Encryptor) *Provisioner {
	return &Provisioner{global: global, account: account, enc: enc}
}

func (p *Provisioner) ProviderType() string { return tenant.ProviderTiDBCloudNative }

// InitSchema validates the existing TiDB schema matches the auto-embedding
// contract used by TiDB Cloud tenants. Database name is always "mysql".
func (p *Provisioner) InitSchema(_ context.Context, dsn string) error {
	return schema.InitTiDBTenantSchema(dsn)
}

// Provision resolves a cluster ID to its connection info via Global Server,
// fetches the encrypted cloud_admin password and decrypts it via KMS.
// tenantID is the cluster ID (string-encoded uint64).
func (p *Provisioner) Provision(ctx context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	clusterID := tenantID

	info, err := p.global.GetClusterInfo(ctx, clusterID)
	if err != nil {
		return nil, fmt.Errorf("get cluster info %s: %w", clusterID, err)
	}

	encryptedPwd, err := p.global.GetEncryptedCloudAdminPwd(ctx, clusterID)
	if err != nil {
		return nil, fmt.Errorf("get encrypted password for cluster %s: %w", clusterID, err)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(encryptedPwd)
	if err != nil {
		return nil, fmt.Errorf("decode encrypted password for cluster %s: %w", clusterID, err)
	}

	plaintext, err := p.enc.Decrypt(ctx, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt password for cluster %s: %w", clusterID, err)
	}

	logger.Info(ctx, "tidbcloud_cluster_provisioned",
		zap.String("cluster_id", clusterID),
		zap.String("host", info.Host),
		zap.Int("port", info.Port))

	return &tenant.ClusterInfo{
		TenantID:  clusterID,
		ClusterID: clusterID,
		Host:      info.Host,
		Port:      info.Port,
		Username:  info.Username,
		Password:  string(plaintext),
		DBName:    "mysql",
		Provider:  tenant.ProviderTiDBCloudNative,
	}, nil
}

// VerifyZeroInstance calls the zero-instance service to confirm the instance ID
// exists. This prevents forged instance IDs from reaching the provision path.
func (p *Provisioner) VerifyZeroInstance(ctx context.Context, instanceID string) error {
	_, err := p.global.GetZeroInstance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("verify zero instance %s: %w", instanceID, err)
	}
	return nil
}

// Authorize delegates authentication to the account service, then verifies that
// the cluster belongs to the same organization as the authenticated user.
func (p *Provisioner) Authorize(ctx context.Context, r *http.Request, clusterID string) error {
	orgID, err := p.account.Authorize(ctx, r, clusterID)
	if err != nil {
		return err
	}

	info, err := p.global.GetClusterInfo(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("verify cluster org: %w", err)
	}
	if info.OrgID != orgID {
		return fmt.Errorf("%w: cluster %s does not belong to org %d", tidbcloud.ErrAuthForbidden, clusterID, orgID)
	}
	return nil
}

// ClusterProxyInfo holds the cluster proxy endpoint and credentials needed to
// execute SQL via the proxy HTTP API.
type ClusterProxyInfo struct {
	ClusterID     uint64
	ProxyEndpoint string
	Username      string // cloud_admin username (with prefix)
	Password      string // cloud_admin password (plaintext)
}

// GetClusterProxyInfo fetches the proxy endpoint and cloud_admin credentials
// for the given cluster. Returns nil if no proxy endpoint is available.
func (p *Provisioner) GetClusterProxyInfo(ctx context.Context, clusterID string) (*ClusterProxyInfo, error) {
	info, err := p.global.GetClusterInfo(ctx, clusterID)
	if err != nil {
		return nil, fmt.Errorf("get cluster info %s: %w", clusterID, err)
	}
	if info.ProxyEndpoint == "" {
		return nil, nil // no proxy available
	}

	encryptedPwd, err := p.global.GetEncryptedCloudAdminPwd(ctx, clusterID)
	if err != nil {
		return nil, fmt.Errorf("get encrypted password for cluster %s: %w", clusterID, err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedPwd)
	if err != nil {
		return nil, fmt.Errorf("decode encrypted password for cluster %s: %w", clusterID, err)
	}
	plaintext, err := p.enc.Decrypt(ctx, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt password for cluster %s: %w", clusterID, err)
	}

	cid, err := tidbcloud.ParseClusterIDUint64(clusterID)
	if err != nil {
		return nil, err
	}

	return &ClusterProxyInfo{
		ClusterID:     cid,
		ProxyEndpoint: info.ProxyEndpoint,
		Username:      info.Username,
		Password:      string(plaintext),
	}, nil
}

// CreateServiceUser creates a dedicated fs_admin SQL user on the given cluster
// via the cluster proxy HTTP API. The user is assigned the role_admin role via
// GRANT ROLE (which does not require GRANT OPTION, unlike explicit privilege
// GRANTs that fail with error 8121). Returns the ServiceUser credentials.
func (p *Provisioner) CreateServiceUser(ctx context.Context, clusterID string, proxyInfo *ClusterProxyInfo) (*tidbcloud.ServiceUser, error) {
	proxy := tidbcloud.NewClusterProxyClient(
		proxyInfo.ProxyEndpoint, proxyInfo.ClusterID,
		proxyInfo.Username, proxyInfo.Password)

	userPrefix := extractUserPrefix(proxyInfo.Username)

	svcUser, err := proxy.CreateServiceUser(ctx, userPrefix)
	if err != nil {
		return nil, fmt.Errorf("create service user on cluster %s: %w", clusterID, err)
	}

	logger.Info(ctx, "service_user_created_via_proxy",
		zap.String("cluster_id", clusterID),
		zap.String("user", svcUser.Username))

	return svcUser, nil
}

// extractUserPrefix extracts the user prefix from a serverless username.
// e.g. "2wCQKHWXMegHiR8.cloud_admin" → "2wCQKHWXMegHiR8"
func extractUserPrefix(username string) string {
	if i := strings.LastIndex(username, "."); i >= 0 {
		return username[:i]
	}
	return ""
}
