package tenant

import (
	"context"
	"errors"

	"github.com/mem9-ai/drive9/pkg/meta"
)

var (
	ErrTiDBCloudBillingUnavailable         = errors.New("tidbcloud billing plan lookup unavailable")
	ErrTiDBCloudBillingResponseInvalid     = errors.New("tidbcloud billing plan response invalid")
	ErrTiDBCloudFreeAdminForbidden         = errors.New("admin API is not available for free TiDB Cloud organizations")
	ErrTiDBCloudFreeQuotaMutationForbidden = errors.New("quota updates are not available for free TiDB Cloud organizations")
	ErrTiDBCloudFreeSpendingLimitForbidden = errors.New("positive spending limit is not available for free TiDB Cloud organizations")
	ErrTiDBCloudFreeQuotaExceeded          = errors.New("requested quota exceeds the free TiDB Cloud organization limit")
	ErrTiDBCloudFreeTenantLimitReached     = meta.ErrTiDBCloudFreeTenantLimitReached
	ErrTiDBCloudFreeQuotaBusy              = meta.ErrTiDBCloudFreeQuotaBusy
)

type TiDBCloudOrganizationPlan struct {
	OrganizationID string
	EffectivePlan  string
	IsFree         bool
}

type TiDBCloudOrganizationPlanResolver interface {
	ResolveOrganizationPlan(ctx context.Context, organizationID string, req CredentialProvisionRequest) (*TiDBCloudOrganizationPlan, error)
}
