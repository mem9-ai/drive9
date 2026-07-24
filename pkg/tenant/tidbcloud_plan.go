package tenant

import (
	"context"
	"errors"
)

var (
	ErrTiDBCloudBillingUnavailable         = errors.New("tidbcloud billing plan lookup unavailable")
	ErrTiDBCloudBillingResponseInvalid     = errors.New("tidbcloud billing plan response invalid")
	ErrTiDBCloudFreeAdminForbidden         = errors.New("admin API is not available for free TiDB Cloud organizations")
	ErrTiDBCloudFreeQuotaMutationForbidden = errors.New("quota updates are not available for free TiDB Cloud organizations")
	ErrTiDBCloudFreeSpendingLimitForbidden = errors.New("positive spending limit is not available for free TiDB Cloud organizations")
	ErrTiDBCloudFreeQuotaExceeded          = errors.New("requested quota exceeds the free TiDB Cloud organization limit")
	ErrTiDBCloudFreeTenantLimitReached     = errors.New("free TiDB Cloud tenant limit reached")
	ErrTiDBCloudFreeQuotaBusy              = errors.New("free tenant quota check is busy; retry later")
)

type TiDBCloudOrganizationPlan struct {
	OrganizationID string
	EffectivePlan  string
	IsFree         bool
}

type TiDBCloudOrganizationPlanResolver interface {
	ResolveOrganizationPlan(ctx context.Context, organizationID string, req CredentialProvisionRequest) (*TiDBCloudOrganizationPlan, error)
}
