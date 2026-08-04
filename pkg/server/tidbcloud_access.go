package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

type tiDBCloudAccessProfile struct {
	OrganizationID string
	IsFree         bool
}

func (s *Server) authorizeTiDBCloudAdminAccess(ctx context.Context, cred tenant.CredentialProvisionRequest, metricPath string) (*tiDBCloudAccessProfile, error) {
	profile, err := s.resolveTiDBCloudAccessProfile(ctx, cred, metricPath)
	if err != nil {
		return nil, err
	}
	if profile.IsFree {
		return nil, tenant.ErrTiDBCloudFreeAdminForbidden
	}
	return profile, nil
}

func (s *Server) authorizeTiDBCloudQuotaMutation(ctx context.Context, cred tenant.CredentialProvisionRequest, metricPath string) (*tiDBCloudAccessProfile, error) {
	profile, err := s.resolveTiDBCloudAccessProfile(ctx, cred, metricPath)
	if err != nil {
		return nil, err
	}
	if profile.IsFree {
		return nil, tenant.ErrTiDBCloudFreeQuotaMutationForbidden
	}
	return profile, nil
}

func (s *Server) resolveTiDBCloudAccessProfile(ctx context.Context, cred tenant.CredentialProvisionRequest, metricPath string) (*tiDBCloudAccessProfile, error) {
	identity, err := s.resolveTiDBCloudIdentity(ctx, cred, metricPath)
	if err != nil {
		return nil, err
	}
	organizationID := strings.TrimSpace(identity.OrganizationID)
	if s.tidbCloudPlanCache.isNonFree(organizationID) {
		metrics.RecordTiDBCloudBillingCacheRequest(metricPath, "hit")
		return &tiDBCloudAccessProfile{
			OrganizationID: organizationID,
			IsFree:         false,
		}, nil
	}
	metrics.RecordTiDBCloudBillingCacheRequest(metricPath, "miss")
	resolver, ok := s.provisioner.(tenant.TiDBCloudOrganizationPlanResolver)
	if !ok {
		return nil, fmt.Errorf("TiDB Cloud Billing plan lookup is not enabled")
	}
	plan, err := resolver.ResolveOrganizationPlan(ctx, organizationID, cred)
	if err != nil {
		return nil, err
	}
	if plan == nil || strings.TrimSpace(plan.OrganizationID) != organizationID {
		return nil, fmt.Errorf("%w: organization mismatch", tenant.ErrTiDBCloudBillingResponseInvalid)
	}
	if plan.IsFree {
		s.tidbCloudPlanCache.remove(organizationID)
	} else {
		s.tidbCloudPlanCache.rememberNonFree(organizationID)
	}
	return &tiDBCloudAccessProfile{
		OrganizationID: organizationID,
		IsFree:         plan.IsFree,
	}, nil
}

func tiDBCloudBillingErrorResponse(err error) (int, string) {
	if apiErr, ok := tiDBCloudAPIError(err); ok {
		switch apiErr.StatusCode {
		case http.StatusUnauthorized:
			return http.StatusUnauthorized, "TiDB Cloud billing API authentication failed"
		case http.StatusForbidden:
			return http.StatusForbidden, "TiDB Cloud billing plan access denied"
		}
	}
	return http.StatusBadGateway, "TiDB Cloud billing plan lookup failed"
}

func isTiDBCloudBillingLookupError(err error) bool {
	if errors.Is(err, tenant.ErrTiDBCloudBillingUnavailable) || errors.Is(err, tenant.ErrTiDBCloudBillingResponseInvalid) {
		return true
	}
	apiErr, ok := tiDBCloudAPIError(err)
	return ok && apiErr.Service == tenant.TiDBCloudAPIServiceBilling
}
