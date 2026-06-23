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

type CredentialProvisioner interface {
	Provisioner
	ProvisionWithCredentials(ctx context.Context, tenantID string, req CredentialProvisionRequest) (*ClusterInfo, error)
}

type CredentialDeprovisioner interface {
	Provisioner
	DeprovisionWithCredentials(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) error
}

type CredentialQuotaUpdater interface {
	Provisioner
	UpdateQuotaWithCredentials(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) error
}

type CredentialQuotaAuthorizer interface {
	Provisioner
	AuthorizeQuotaWithCredentials(ctx context.Context, cluster *ClusterInfo, req CredentialProvisionRequest) error
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
	DeleteBranchWithCredentials(ctx context.Context, clusterID, branchID string, req CredentialProvisionRequest) error
}

func RequireProvisioner(provider string, provisioners map[string]Provisioner) (Provisioner, error) {
	p, ok := provisioners[provider]
	if !ok || p == nil {
		return nil, fmt.Errorf("provisioner not configured for provider: %s", provider)
	}
	return p, nil
}
