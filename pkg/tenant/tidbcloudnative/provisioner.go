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

// ProvisionWithRootCreds resolves cluster connection info via Global Server
// and returns a ClusterInfo populated with the caller-provided root credentials.
// Unlike Provision, it does not fetch or decrypt cloud_admin passwords.
func (p *Provisioner) ProvisionWithRootCreds(ctx context.Context, tenantID, rootUser, rootPassword string) (*tenant.ClusterInfo, error) {
	clusterID := tenantID

	info, err := p.global.GetClusterInfo(ctx, clusterID)
	if err != nil {
		return nil, fmt.Errorf("get cluster info %s: %w", clusterID, err)
	}

	logger.Info(ctx, "tidbcloud_cluster_provisioned_with_root_creds",
		zap.String("cluster_id", clusterID),
		zap.String("host", info.Host),
		zap.Int("port", info.Port))

	return &tenant.ClusterInfo{
		TenantID:      clusterID,
		ClusterID:     clusterID,
		Host:          info.Host,
		Port:          info.Port,
		Username:      rootUser,
		Password:      rootPassword,
		DBName:        "mysql",
		Provider:      tenant.ProviderTiDBCloudNative,
		ProxyEndpoint: info.ProxyEndpoint,
		UserPrefix:    info.UserPrefix,
	}, nil
}

// CreateServiceUser creates an fs_admin SQL user on the cluster via the internal
// cluster proxy, using the cloud_admin credentials as the operator.
func (p *Provisioner) CreateServiceUser(ctx context.Context, clusterID, proxyEndpoint, userPrefix, newUser, newPass string) error {
	encryptedPwd, err := p.global.GetEncryptedCloudAdminPwd(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("get cloud_admin password for cluster %s: %w", clusterID, err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedPwd)
	if err != nil {
		return fmt.Errorf("decode cloud_admin password for cluster %s: %w", clusterID, err)
	}
	plaintext, err := p.enc.Decrypt(ctx, ciphertext)
	if err != nil {
		return fmt.Errorf("decrypt cloud_admin password for cluster %s: %w", clusterID, err)
	}

	operatorUser := "cloud_admin"
	if userPrefix != "" {
		operatorUser = userPrefix + ".cloud_admin"
	}

	clusterIDUint, err := tidbcloud.ParseClusterIDUint64(clusterID)
	if err != nil {
		return err
	}

	logger.Info(ctx, "creating_service_user_via_proxy",
		zap.String("cluster_id", clusterID),
		zap.String("proxy_endpoint", proxyEndpoint),
		zap.String("operator", operatorUser),
		zap.String("new_user", newUser))

	return tidbcloud.CreateServiceUserViaProxy(ctx, proxyEndpoint, clusterIDUint, operatorUser, string(plaintext), newUser, newPass)
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
