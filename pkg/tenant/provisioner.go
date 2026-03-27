package tenant

import (
	"context"
	"fmt"
	"time"
)

type ClusterInfo struct {
	TenantID       string
	ClusterID      string
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

func RequireProvisioner(provider string, provisioners map[string]Provisioner) (Provisioner, error) {
	p, ok := provisioners[provider]
	if !ok || p == nil {
		return nil, fmt.Errorf("provisioner not configured for provider: %s", provider)
	}
	return p, nil
}
