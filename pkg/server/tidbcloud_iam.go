package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func (s *Server) resolveTiDBCloudIdentity(ctx context.Context, cred tenant.CredentialProvisionRequest, metricPath string) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	if identity, ok := s.tidbCloudRBACCache.getIdentity(cred); ok {
		metrics.RecordTiDBCloudRBACCacheRequest(metricPath, "role", "hit")
		return &identity, nil
	}
	metrics.RecordTiDBCloudRBACCacheRequest(metricPath, "role", "miss")
	resolver, ok := s.provisioner.(tenant.TiDBCloudAPIKeyIdentityResolver)
	if !ok {
		return nil, fmt.Errorf("TiDB Cloud IAM identity lookup is not enabled")
	}
	identity, err := resolver.ResolveAPIKeyIdentity(ctx, cred)
	if err != nil {
		return nil, err
	}
	if identity == nil || strings.TrimSpace(identity.OrganizationID) == "" {
		return nil, fmt.Errorf("TiDB Cloud IAM identity is missing organization")
	}
	if identity.Role != tenant.TiDBCloudRoleOrgOwner && identity.Role != tenant.TiDBCloudRoleProjectOwner {
		return nil, fmt.Errorf("%w: role %q; org:owner or project:owner is required", tenant.ErrTiDBCloudRoleInsufficient, identity.Role)
	}
	s.tidbCloudRBACCache.rememberIdentity(cred, *identity)
	return identity, nil
}

func tiDBCloudOrganizationMatches(identityOrganizationID, resourceOrganizationID string) bool {
	identityOrganizationID = strings.TrimSpace(identityOrganizationID)
	resourceOrganizationID = strings.TrimSpace(resourceOrganizationID)
	return identityOrganizationID != "" && (resourceOrganizationID == identityOrganizationID || resourceOrganizationID == "*")
}

func (s *Server) authorizeTiDBCloudOrganization(ctx context.Context, cred tenant.CredentialProvisionRequest, resourceOrganizationID, metricPath string) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	identity, err := s.resolveTiDBCloudIdentity(ctx, cred, metricPath)
	if err != nil {
		return nil, err
	}
	if !tiDBCloudOrganizationMatches(identity.OrganizationID, resourceOrganizationID) {
		return nil, tenant.ErrQuotaPermissionDenied
	}
	return identity, nil
}

func (s *Server) authorizeNativeTenantCredentials(ctx context.Context, t *meta.Tenant, cred tenant.CredentialProvisionRequest, metricPath string) (*meta.TenantTiDBCloudOrgBinding, error) {
	if t == nil {
		return nil, tenant.ErrQuotaBackendNotFound
	}
	binding, err := s.meta.GetTenantTiDBCloudOrgBinding(ctx, t.ID)
	if err != nil {
		return nil, tenant.ErrQuotaBackendNotFound
	}
	if _, err := s.authorizeTiDBCloudOrganization(ctx, cred, binding.OrganizationID, metricPath); err != nil {
		return nil, err
	}
	return binding, nil
}

func (s *Server) sharedDBCloudCredentials(ctx context.Context, organizationID string) (tenant.CredentialProvisionRequest, error) {
	provider, ok := s.provisioner.(tenant.SharedCredentialProvider)
	if !ok {
		return tenant.CredentialProvisionRequest{}, fmt.Errorf("shared TiDB Cloud credentials are not configured")
	}
	cred, ok := provider.DefaultSharedCredentials()
	if !ok {
		return tenant.CredentialProvisionRequest{}, fmt.Errorf("shared TiDB Cloud credentials are not configured")
	}
	identity, err := s.resolveTiDBCloudIdentity(ctx, cred, "shared_pool_role")
	if err != nil {
		return tenant.CredentialProvisionRequest{}, err
	}
	if strings.TrimSpace(organizationID) != "" && strings.TrimSpace(organizationID) != meta.SharedDBOrgWildcard &&
		identity.OrganizationID != strings.TrimSpace(organizationID) {
		return tenant.CredentialProvisionRequest{}, fmt.Errorf("shared TiDB Cloud credential organization %q does not match pool organization %q", identity.OrganizationID, organizationID)
	}
	return cred, nil
}

func isTiDBCloudRoleInsufficient(err error) bool {
	return errors.Is(err, tenant.ErrTiDBCloudRoleInsufficient)
}
