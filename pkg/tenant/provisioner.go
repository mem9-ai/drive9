package tenant

import (
	"context"
	"fmt"
	"time"
)

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

func RequireProvisioner(provider string, provisioners map[string]Provisioner) (Provisioner, error) {
	p, ok := provisioners[provider]
	if !ok || p == nil {
		return nil, fmt.Errorf("provisioner not configured for provider: %s", provider)
	}
	return p, nil
}
