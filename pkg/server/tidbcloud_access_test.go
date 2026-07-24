package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

type accessTestProvisioner struct {
	mu           sync.Mutex
	iamCalls     int
	billingCalls int
	identities   map[string]tenant.TiDBCloudAPIKeyIdentity
	plan         tenant.TiDBCloudOrganizationPlan
	planErr      error
}

func (p *accessTestProvisioner) ProviderType() string                     { return tenant.ProviderTiDBCloudNative }
func (p *accessTestProvisioner) InitSchema(context.Context, string) error { return nil }
func (p *accessTestProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, errors.New("not implemented")
}
func (p *accessTestProvisioner) ResolveAPIKeyIdentity(_ context.Context, req tenant.CredentialProvisionRequest) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.iamCalls++
	identity, ok := p.identities[req.PublicKey]
	if !ok {
		identity = tenant.TiDBCloudAPIKeyIdentity{OrganizationID: "org-1", Role: tenant.TiDBCloudRoleOrgOwner}
	}
	return &identity, nil
}
func (p *accessTestProvisioner) ResolveOrganizationPlan(_ context.Context, organizationID string, _ tenant.CredentialProvisionRequest) (*tenant.TiDBCloudOrganizationPlan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.billingCalls++
	if p.planErr != nil {
		return nil, p.planErr
	}
	plan := p.plan
	if plan.OrganizationID == "" {
		plan.OrganizationID = organizationID
	}
	return &plan, nil
}

func TestResolveTiDBCloudAccessProfileCachesOnlyNonFreeOrganizations(t *testing.T) {
	cred := tenant.CredentialProvisionRequest{PublicKey: "key-1", PrivateKey: "private-1"}
	provisioner := &accessTestProvisioner{
		identities: map[string]tenant.TiDBCloudAPIKeyIdentity{
			"key-1": {OrganizationID: "org-1", Role: tenant.TiDBCloudRoleOrgOwner},
		},
		plan: tenant.TiDBCloudOrganizationPlan{EffectivePlan: "on_demand", IsFree: false},
	}
	s := &Server{
		provisioner:        provisioner,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour),
		tidbCloudPlanCache: newTiDBCloudNonFreePlanCache(time.Hour),
	}

	for i := 0; i < 2; i++ {
		profile, err := s.resolveTiDBCloudAccessProfile(context.Background(), cred, "access_profile_non_free")
		if err != nil || profile.IsFree || profile.OrganizationID != "org-1" {
			t.Fatalf("profile %d = %+v/%v", i, profile, err)
		}
	}
	if provisioner.iamCalls != 1 || provisioner.billingCalls != 1 {
		t.Fatalf("IAM/Billing calls = %d/%d, want 1/1", provisioner.iamCalls, provisioner.billingCalls)
	}

	provisioner.plan = tenant.TiDBCloudOrganizationPlan{EffectivePlan: "free_trial", IsFree: true}
	s.tidbCloudPlanCache.remove("org-1")
	for i := 0; i < 2; i++ {
		profile, err := s.resolveTiDBCloudAccessProfile(context.Background(), cred, "access_profile_free")
		if err != nil || !profile.IsFree {
			t.Fatalf("free profile %d = %+v/%v", i, profile, err)
		}
	}
	if provisioner.billingCalls != 3 {
		t.Fatalf("Billing calls after two free requests = %d, want 3 total", provisioner.billingCalls)
	}
}

func TestResolveTiDBCloudAccessProfileSharesPositivePlanAcrossCredentials(t *testing.T) {
	provisioner := &accessTestProvisioner{
		identities: map[string]tenant.TiDBCloudAPIKeyIdentity{
			"key-a": {OrganizationID: "org-shared", Role: tenant.TiDBCloudRoleOrgOwner},
			"key-b": {OrganizationID: "org-shared", Role: tenant.TiDBCloudRoleProjectOwner},
		},
		plan: tenant.TiDBCloudOrganizationPlan{EffectivePlan: "poc", IsFree: false},
	}
	s := &Server{
		provisioner:        provisioner,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour),
		tidbCloudPlanCache: newTiDBCloudNonFreePlanCache(time.Hour),
	}
	for _, publicKey := range []string{"key-a", "key-b"} {
		if _, err := s.resolveTiDBCloudAccessProfile(context.Background(), tenant.CredentialProvisionRequest{
			PublicKey: publicKey, PrivateKey: "private-" + publicKey,
		}, "access_profile_shared_org"); err != nil {
			t.Fatal(err)
		}
	}
	if provisioner.iamCalls != 2 || provisioner.billingCalls != 1 {
		t.Fatalf("IAM/Billing calls = %d/%d, want 2/1", provisioner.iamCalls, provisioner.billingCalls)
	}
}

func TestResolveTiDBCloudAccessProfileDoesNotCacheBillingErrors(t *testing.T) {
	provisioner := &accessTestProvisioner{
		identities: map[string]tenant.TiDBCloudAPIKeyIdentity{},
		planErr:    tenant.ErrTiDBCloudBillingUnavailable,
	}
	s := &Server{
		provisioner:        provisioner,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour),
		tidbCloudPlanCache: newTiDBCloudNonFreePlanCache(time.Hour),
	}
	cred := tenant.CredentialProvisionRequest{PublicKey: "key-error", PrivateKey: "private-error"}
	for i := 0; i < 2; i++ {
		if _, err := s.resolveTiDBCloudAccessProfile(context.Background(), cred, "access_profile_error"); !errors.Is(err, tenant.ErrTiDBCloudBillingUnavailable) {
			t.Fatalf("error %d = %v", i, err)
		}
	}
	if provisioner.billingCalls != 2 {
		t.Fatalf("Billing calls = %d, want 2", provisioner.billingCalls)
	}
}

func TestTiDBCloudBillingErrorResponse(t *testing.T) {
	tests := []struct {
		err        error
		wantStatus int
		want       string
	}{
		{err: &tenant.TiDBCloudAPIError{Operation: "Billing plan lookup", StatusCode: http.StatusUnauthorized}, wantStatus: http.StatusUnauthorized, want: "TiDB Cloud billing API authentication failed"},
		{err: &tenant.TiDBCloudAPIError{Operation: "Billing plan lookup", StatusCode: http.StatusForbidden}, wantStatus: http.StatusForbidden, want: "TiDB Cloud billing plan access denied"},
		{err: &tenant.TiDBCloudAPIError{Operation: "Billing plan lookup", StatusCode: http.StatusBadRequest}, wantStatus: http.StatusBadGateway, want: "TiDB Cloud billing plan lookup failed"},
		{err: tenant.ErrTiDBCloudBillingResponseInvalid, wantStatus: http.StatusBadGateway, want: "TiDB Cloud billing plan lookup failed"},
		{err: tenant.ErrTiDBCloudBillingUnavailable, wantStatus: http.StatusBadGateway, want: "TiDB Cloud billing plan lookup failed"},
	}
	for _, tt := range tests {
		status, message := tiDBCloudBillingErrorResponse(tt.err)
		if status != tt.wantStatus || message != tt.want {
			t.Fatalf("response for %v = %d %q, want %d %q", tt.err, status, message, tt.wantStatus, tt.want)
		}
	}
}

func TestTiDBCloudAccessGateWritersPreserveBillingErrorContract(t *testing.T) {
	err := &tenant.TiDBCloudAPIError{
		Operation:  "Billing plan lookup",
		StatusCode: http.StatusUnauthorized,
	}
	tests := []struct {
		name  string
		write func(http.ResponseWriter)
	}{
		{
			name: "admin",
			write: func(w http.ResponseWriter) {
				writeAdminTiDBCloudError(w, context.Background(), err, "list tenants")
			},
		},
		{
			name: "quota mutation",
			write: func(w http.ResponseWriter) {
				writeQuotaSetError(w, context.Background(), err, "authorize")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			tt.write(recorder)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", recorder.Code)
			}
			if got, want := strings.TrimSpace(recorder.Body.String()), `{"error":"TiDB Cloud billing API authentication failed"}`; got != want {
				t.Fatalf("body = %s, want %s", got, want)
			}
		})
	}
}
