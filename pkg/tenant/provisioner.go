package tenant

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrCredentialsRequired = errors.New("public_key and private_key are required")
var ErrPartialCredentials = errors.New("both public_key and private_key must be provided together")
var ErrQuotaPermissionDenied = errors.New("tidbcloud quota update permission denied")
var ErrQuotaBackendNotFound = errors.New("tidbcloud quota backend not found")

type ClusterInfo struct {
	TenantID       string
	ClusterID      string
	OrganizationID string
	BranchID       string
	Host           string
	Port           int
	Username       string
	Password       string
	DBName         string
	Provider       string
	ClaimURL       string
	ClaimExpiresAt *time.Time
}

type Provisioner interface {
	Provision(ctx context.Context, tenantID string) (*ClusterInfo, error)
	InitSchema(ctx context.Context, dsn string) error
	ProviderType() string
}

type Deprovisioner interface {
	Provisioner
	Deprovision(ctx context.Context, cluster *ClusterInfo) error
}

type CredentialProvisionRequest struct {
	PublicKey  string
	PrivateKey string
}

type QuotaCloudConfig struct {
	TiDBCloudSpendingLimitMonthly *int64
	Labels                        map[string]string
}

type CloudClusterInfo struct {
	ClusterID                     string
	OrganizationID                string
	Labels                        map[string]string
	TiDBCloudSpendingLimitMonthly *int64
}

type CredentialProvisioner interface {
	Provisioner
	ProvisionWithCredentials(ctx context.Context, tenantID string, req CredentialProvisionRequest) (*ClusterInfo, error)
}

type CredentialQuotaProvisioner interface {
	Provisioner
	ProvisionWithCredentialsAndQuota(ctx context.Context, tenantID string, req CredentialProvisionRequest, opts QuotaUpdateOptions) (*ClusterInfo, *QuotaCloudConfig, error)
}

type TenantPoolClusterManager interface {
	Provisioner
	BatchProvisionFreeClustersWithCredentialsAndQuota(ctx context.Context, tenantIDs []string, req CredentialProvisionRequest, opts QuotaUpdateOptions) ([]*ClusterInfo, *QuotaCloudConfig, error)
	MarkClusterPoolUsed(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest, usedAt time.Time, opts QuotaUpdateOptions) (*QuotaCloudConfig, error)
	MarkClusterPoolFree(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) error
}

type SharedDBPoolCreateRequest struct {
	DBPoolID             int64
	DBPoolUUID           string
	DatabaseName         string
	RootPassword         string
	SpendingLimitMonthly int64
}

type SharedDBPoolInfo struct {
	DBPoolID       int64
	DBPoolUUID     string
	ClusterID      string
	OrganizationID string
	Host           string
	Port           int
	Username       string
	Password       string
	DBName         string
}

type SharedDBPoolProvisioner interface {
	Provisioner
	// BatchProvisionSharedDBPoolsWithCredentials may return the successfully
	// decoded subset together with a non-nil error. Callers must persist/process
	// every returned result before handling the error.
	BatchProvisionSharedDBPoolsWithCredentials(ctx context.Context, requests []SharedDBPoolCreateRequest, req CredentialProvisionRequest) ([]*SharedDBPoolInfo, error)
	LoadSharedDBPoolWithCredentials(ctx context.Context, dbPoolID int64, dbPoolUUID, clusterID string, req CredentialProvisionRequest) (*SharedDBPoolInfo, error)
}

type SharedDBPoolMetadataWaiter interface {
	WaitForSharedDBPoolMetadataWithCredentials(ctx context.Context, dbPoolID int64, dbPoolUUID, clusterID string, req CredentialProvisionRequest) (*SharedDBPoolInfo, error)
}

var ErrSharedDBPoolAmbiguous = errors.New("multiple shared db pool cloud resources found")

type TenantPoolClusterMetadataWaiter interface {
	Provisioner
	WaitForPoolClusterMetadata(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) (*ClusterInfo, error)
}

type TenantPoolClusterMetadataBatchWaiter interface {
	Provisioner
	WaitForPoolClustersMetadata(ctx context.Context, clusters []*ClusterInfo, req CredentialProvisionRequest) ([]*ClusterInfo, error)
}

type CredentialDeprovisioner interface {
	Provisioner
	DeprovisionWithCredentials(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) error
}

type QuotaUpdater interface {
	Provisioner
	UpdateQuota(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest, opts QuotaUpdateOptions) (*QuotaCloudConfig, error)
	MarkQuotaUpdateStarted(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) (*QuotaCloudConfig, error)
}

type QuotaGetter interface {
	Provisioner
	GetQuota(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) (*QuotaCloudConfig, error)
}

type QuotaUpdateOptions struct {
	TiDBCloudSpendingLimitMonthly *int64
	TenantPoolID                  string
}

type ManagedClusterListOptions struct {
	PageSize  int
	PageToken string
	ClusterID string
}

type ManagedClusterListResult struct {
	Clusters      []CloudClusterInfo
	NextPageToken string
}

type ManagedClusterLister interface {
	Provisioner
	ListManagedClusters(ctx context.Context, req CredentialProvisionRequest, opts ManagedClusterListOptions) (*ManagedClusterListResult, error)
}

type BranchProvisioner interface {
	Provisioner
	ProvisionBranch(ctx context.Context, forkTenantID string, source *ClusterInfo) (*ClusterInfo, error)
	DeleteBranch(ctx context.Context, clusterID, branchID string) error
}

type AsyncBranchProvisioner interface {
	BranchProvisioner
	CreateBranch(ctx context.Context, forkTenantID string, source *ClusterInfo) (*ClusterInfo, error)
	WaitForBranchActive(ctx context.Context, branch *ClusterInfo) (*ClusterInfo, error)
}

type CredentialBranchProvisioner interface {
	Provisioner
	CreateBranchWithCredentials(ctx context.Context, forkTenantID string, source *ClusterInfo, req CredentialProvisionRequest) (*ClusterInfo, error)
	WaitForBranchActiveWithCredentials(ctx context.Context, branch *ClusterInfo, req CredentialProvisionRequest) (*ClusterInfo, error)
	WaitForBranchUserWithCredentials(ctx context.Context, clusterID, branchID string, req CredentialProvisionRequest) (string, error)
	DeleteBranchWithCredentials(ctx context.Context, clusterID, branchID string, req CredentialProvisionRequest) error
}

func RequireProvisioner(provider string, provisioners map[string]Provisioner) (Provisioner, error) {
	p, ok := provisioners[provider]
	if !ok || p == nil {
		return nil, fmt.Errorf("provisioner not configured for provider: %s", provider)
	}
	return p, nil
}
