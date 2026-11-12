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

// resolveTiDBCloudIdentity authenticates a TiDB Cloud API key pair and enforces
// the role the calling surface needs.
//
// requiredRole is the role the surface cannot work without: the organization-wide
// admin surfaces (tenant create/delete, quota, tenant pool, object backends) pass
// org:owner, because a key that is only a project:owner must not be able to
// operate on every tenant in the organization. An empty requiredRole accepts any
// owner role, which is what the tenant-bound paths want: they already name the
// target tenant and compare its organization to the caller's.
func (s *Server) resolveTiDBCloudIdentity(ctx context.Context, cred tenant.CredentialProvisionRequest, metricPath string, requiredRole string) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	if identity, ok := s.tidbCloudRBACCache.getIdentity(cred); ok {
		metrics.RecordTiDBCloudRBACCacheRequest(metricPath, "role", "hit")
		if err := requireTiDBCloudRole(&identity, requiredRole); err != nil {
			return nil, err
		}
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
	if err := requireTiDBCloudRole(identity, requiredRole); err != nil {
		return nil, err
	}
	s.tidbCloudRBACCache.rememberIdentity(cred, *identity)
	return identity, nil
}

// requireTiDBCloudRole enforces the minimum role for one surface.
//
// project:owner is a project-scoped role. Drive9 has no project dimension to
// match it against (tenant_tidbcloud_org_bindings carries no project id), so
// accepting it on an organization-wide surface would silently promote it to
// organization admin. Those surfaces therefore require org:owner; the
// tenant-bound paths still accept either role because they name their target and
// the caller has to belong to the tenant's organization anyway.
func requireTiDBCloudRole(identity *tenant.TiDBCloudAPIKeyIdentity, requiredRole string) error {
	if identity == nil {
		return fmt.Errorf("%w: identity is missing", tenant.ErrTiDBCloudRoleInsufficient)
	}
	if requiredRole != "" {
		if identity.Role != requiredRole {
			return fmt.Errorf("%w: role %q; %s is required for this operation", tenant.ErrTiDBCloudRoleInsufficient, identity.Role, requiredRole)
		}
		return nil
	}
	if identity.Role != tenant.TiDBCloudRoleOrgOwner && identity.Role != tenant.TiDBCloudRoleProjectOwner {
		return fmt.Errorf("%w: role %q; org:owner or project:owner is required", tenant.ErrTiDBCloudRoleInsufficient, identity.Role)
	}
	return nil
}

func tiDBCloudOrganizationMatches(identityOrganizationID, resourceOrganizationID string) bool {
	identityOrganizationID = strings.TrimSpace(identityOrganizationID)
	resourceOrganizationID = strings.TrimSpace(resourceOrganizationID)
	return identityOrganizationID != "" && resourceOrganizationID == identityOrganizationID
}

func (s *Server) authorizeNativeTenantCredentials(ctx context.Context, t *meta.Tenant, cred tenant.CredentialProvisionRequest, metricPath string) (*meta.TenantTiDBCloudOrgBinding, error) {
	if t == nil {
		return nil, tenant.ErrQuotaBackendNotFound
	}
	binding, err := s.meta.GetTenantTiDBCloudOrgBinding(ctx, t.ID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, tenant.ErrQuotaBackendNotFound
		}
		return nil, fmt.Errorf("get tenant TiDB Cloud organization binding: %w", err)
	}
	identity, err := s.resolveTiDBCloudIdentity(ctx, cred, metricPath, "")
	if err != nil {
		return nil, err
	}
	if !tiDBCloudOrganizationMatches(identity.OrganizationID, binding.OrganizationID) {
		return nil, tenant.ErrQuotaPermissionDenied
	}
	return binding, nil
}

func (s *Server) authorizeNativeTenantOrganization(ctx context.Context, t *meta.Tenant, organizationID string) (*meta.TenantTiDBCloudOrgBinding, error) {
	if t == nil {
		return nil, tenant.ErrQuotaBackendNotFound
	}
	binding, err := s.meta.GetTenantTiDBCloudOrgBinding(ctx, t.ID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, tenant.ErrQuotaBackendNotFound
		}
		return nil, fmt.Errorf("get tenant TiDB Cloud organization binding: %w", err)
	}
	if !tiDBCloudOrganizationMatches(organizationID, binding.OrganizationID) {
		return nil, tenant.ErrQuotaPermissionDenied
	}
	return binding, nil
}

func isTiDBCloudRoleInsufficient(err error) bool {
	return errors.Is(err, tenant.ErrTiDBCloudRoleInsufficient)
}
