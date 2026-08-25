package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	tenantschema "github.com/mem9-ai/drive9/pkg/tenant/schema"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type fakeProvisioner struct {
	provider               string
	cloudProvider          string
	region                 string
	cluster                *tenant.ClusterInfo
	initErr                error
	provisionErr           error
	systemUserErr          error
	systemUsername         string
	systemPassword         string
	deprovisionErr         error
	quotaMarkErr           error
	quotaUpdateErr         error
	provisionCalls         atomic.Int32
	credentialCalls        atomic.Int32
	credentialQuotaCalls   atomic.Int32
	systemUserCalls        atomic.Int32
	deprovisionCalls       atomic.Int32
	quotaMarkCalls         atomic.Int32
	quotaUpdateCalls       atomic.Int32
	lastCredentialReq      tenant.CredentialProvisionRequest
	lastDeprovision        *tenant.ClusterInfo
	lastQuotaCluster       *tenant.ClusterInfo
	lastQuotaOptions       tenant.QuotaUpdateOptions
	lastCreateQuotaOptions tenant.QuotaUpdateOptions
	defaultPublicKey       string
	defaultPrivateKey      string
	iamCalls               atomic.Int32
	billingCalls           atomic.Int32
	billingErr             error
	billingFree            bool
	iamMu                  sync.Mutex
	iamCredentials         []tenant.CredentialProvisionRequest
	identityOrg            string
	identityRole           string
	managedClusters        []tenant.CloudClusterInfo
}

type earlyBindingProvisioner struct {
	*fakeProvisioner
	createStarted chan struct{}
	createRelease <-chan struct{}
	created       *tenant.ClusterInfo
	ready         *tenant.ClusterInfo
	waitStarted   chan struct{}
	waitRelease   <-chan struct{}
	waitErr       error
}

func (p *earlyBindingProvisioner) CreateClusterWithCredentialsAndQuota(_ context.Context, tenantID string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	p.lastCredentialReq = req
	p.lastCreateQuotaOptions = opts
	if p.createStarted != nil {
		close(p.createStarted)
	}
	if p.createRelease != nil {
		<-p.createRelease
	}
	out := *p.created
	out.TenantID = tenantID
	out.Provider = tenant.ProviderTiDBCloudNative
	return &out, nil, nil
}

func (p *earlyBindingProvisioner) WaitForClusterMetadataWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	p.lastCredentialReq = req
	close(p.waitStarted)
	if p.waitRelease != nil {
		<-p.waitRelease
	}
	if p.waitErr != nil {
		return cluster, p.waitErr
	}
	out := *p.ready
	out.TenantID = cluster.TenantID
	out.ClusterID = cluster.ClusterID
	out.Password = cluster.Password
	out.DBName = cluster.DBName
	out.Provider = tenant.ProviderTiDBCloudNative
	return &out, nil
}

type blockingDatabaseEnsurer struct {
	fakeProvisioner
	started     chan struct{}
	startedOnce sync.Once
	release     <-chan struct{}
}

func (f *blockingDatabaseEnsurer) EnsureDatabase(ctx context.Context, _ string) error {
	f.startedOnce.Do(func() { close(f.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.release:
		return errors.New("stop after provisioning heartbeat test")
	}
}

func TestBlockingDatabaseEnsurerSignalsStartedOnceAcrossRetries(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	close(release)
	ensurer := &blockingDatabaseEnsurer{started: started, release: release}
	for attempt := 0; attempt < 2; attempt++ {
		if err := ensurer.EnsureDatabase(context.Background(), ""); err == nil {
			t.Fatalf("EnsureDatabase attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	select {
	case <-started:
	default:
		t.Fatal("EnsureDatabase did not signal started")
	}
}

type failingEncryptor struct {
	err error
}

func (e failingEncryptor) Encrypt(context.Context, []byte) ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return nil, fmt.Errorf("encrypt failed")
}

func (e failingEncryptor) Decrypt(context.Context, []byte) ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return nil, fmt.Errorf("decrypt failed")
}

func (f *fakeProvisioner) DefaultCredentials() (tenant.CredentialProvisionRequest, bool) {
	if f.defaultPublicKey == "" || f.defaultPrivateKey == "" {
		return tenant.CredentialProvisionRequest{}, false
	}
	return tenant.CredentialProvisionRequest{
		PublicKey:  f.defaultPublicKey,
		PrivateKey: f.defaultPrivateKey,
	}, true
}

func (f *fakeProvisioner) ResolveAPIKeyIdentity(_ context.Context, req tenant.CredentialProvisionRequest) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	f.iamCalls.Add(1)
	f.iamMu.Lock()
	f.iamCredentials = append(f.iamCredentials, req)
	f.iamMu.Unlock()
	orgID := f.identityOrg
	if orgID == "" && f.cluster != nil {
		orgID = f.cluster.OrganizationID
	}
	if orgID == "" && len(f.managedClusters) > 0 {
		orgID = f.managedClusters[0].OrganizationID
	}
	if orgID == "" {
		orgID = "org-default"
	}
	role := f.identityRole
	if role == "" {
		role = tenant.TiDBCloudRoleOrgOwner
	}
	return &tenant.TiDBCloudAPIKeyIdentity{OrganizationID: orgID, Role: role}, nil
}

func (f *fakeProvisioner) ResolveOrganizationPlan(_ context.Context, organizationID string, _ tenant.CredentialProvisionRequest) (*tenant.TiDBCloudOrganizationPlan, error) {
	f.billingCalls.Add(1)
	if f.billingErr != nil {
		return nil, f.billingErr
	}
	return &tenant.TiDBCloudOrganizationPlan{
		OrganizationID: organizationID,
		IsFree:         f.billingFree,
	}, nil
}

func (f *fakeProvisioner) ProviderType() string { return f.provider }

func (f *fakeProvisioner) ProvisioningCloudProvider() string { return f.cloudProvider }

func (f *fakeProvisioner) ProvisioningRegion() string { return f.region }

func (f *fakeProvisioner) DefaultTiDBCloudSpendingLimit() int64 { return 1000 }

func (f *fakeProvisioner) ListManagedClusters(_ context.Context, _ tenant.CredentialProvisionRequest, _ tenant.ManagedClusterListOptions) (*tenant.ManagedClusterListResult, error) {
	return &tenant.ManagedClusterListResult{Clusters: append([]tenant.CloudClusterInfo(nil), f.managedClusters...)}, nil
}

func (f *fakeProvisioner) InitSchema(_ context.Context, dsn string) error {
	if f.initErr != nil {
		return f.initErr
	}
	return nil
}

func (f *fakeProvisioner) EnsureSystemUser(_ context.Context, _ string, _ string) (string, string, error) {
	f.systemUserCalls.Add(1)
	if f.systemUserErr != nil {
		return "", "", f.systemUserErr
	}
	username := f.systemUsername
	if username == "" {
		username = "u1.tdc_fs_sys"
	}
	password := f.systemPassword
	if password == "" {
		password = "system-pass"
	}
	return username, password, nil
}

func (f *fakeProvisioner) Provision(_ context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	f.provisionCalls.Add(1)
	if f.provisionErr != nil {
		if f.cluster == nil {
			return nil, f.provisionErr
		}
		out := *f.cluster
		out.TenantID = tenantID
		out.Provider = f.provider
		return &out, f.provisionErr
	}
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

func (f *fakeProvisioner) CreateClusterWithCredentialsAndQuota(_ context.Context, tenantID string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		f.credentialQuotaCalls.Add(1)
	} else {
		f.credentialCalls.Add(1)
	}
	f.lastCredentialReq = req
	f.lastCreateQuotaOptions = opts
	if f.provisionErr != nil {
		if f.cluster == nil {
			return nil, nil, f.provisionErr
		}
		out := *f.cluster
		out.TenantID = tenantID
		out.Provider = f.provider
		return &out, nil, f.provisionErr
	}
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	var cloudCfg *tenant.QuotaCloudConfig
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: opts.TiDBCloudSpendingLimitMonthly}
	}
	return &out, cloudCfg, nil
}

func (f *fakeProvisioner) WaitForClusterMetadataWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	f.lastCredentialReq = req
	if cluster == nil {
		return nil, fmt.Errorf("cluster is required")
	}
	out := *cluster
	return &out, nil
}

func (f *fakeProvisioner) Deprovision(_ context.Context, cluster *tenant.ClusterInfo) error {
	if cluster != nil {
		out := *cluster
		f.lastDeprovision = &out
	}
	f.deprovisionCalls.Add(1)
	return f.deprovisionErr
}

func (f *fakeProvisioner) DeprovisionWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	f.lastCredentialReq = req
	if cluster != nil {
		out := *cluster
		f.lastDeprovision = &out
	}
	f.deprovisionCalls.Add(1)
	return f.deprovisionErr
}

func (f *fakeProvisioner) MarkQuotaUpdateStarted(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.QuotaCloudConfig, error) {
	f.quotaMarkCalls.Add(1)
	f.lastCredentialReq = req
	if cluster != nil {
		out := *cluster
		f.lastQuotaCluster = &out
	}
	if f.quotaMarkErr != nil {
		return nil, f.quotaMarkErr
	}
	return nil, nil
}

func (f *fakeProvisioner) UpdateQuota(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	f.quotaUpdateCalls.Add(1)
	f.lastCredentialReq = req
	f.lastQuotaOptions = opts
	if cluster != nil {
		out := *cluster
		f.lastQuotaCluster = &out
	}
	if f.quotaUpdateErr != nil {
		return nil, f.quotaUpdateErr
	}
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		return &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: opts.TiDBCloudSpendingLimitMonthly}, nil
	}
	return nil, nil
}

func (f *fakeProvisioner) ProvisionCallCount() int {
	return int(f.provisionCalls.Load())
}

func TestClientFacingErrorResponseMapsTiDBCloudClientErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{
			name: "invalid request",
			err: fmt.Errorf("update cluster spending limit: %w", &tenant.TiDBCloudAPIError{
				Operation:    "cluster spending limit update",
				StatusCode:   http.StatusBadRequest,
				UpstreamBody: `{"code":400,"message":"Scalable cluster can not set spending limit to 0.","details":[{"requestId":"202607090625337c3caba58b2eb378ca"}]}`,
			}),
			wantStatus: http.StatusBadRequest,
			wantBody:   "Scalable cluster can not set spending limit to 0",
		},
		{
			name: "invalid api key",
			err: fmt.Errorf("list managed clusters: %w", &tenant.TiDBCloudAPIError{
				Operation:  "cluster list",
				StatusCode: http.StatusUnauthorized,
			}),
			wantStatus: http.StatusUnauthorized,
			wantBody:   "invalid TiDB Cloud API key",
		},
		{
			name: "forbidden",
			err: fmt.Errorf("update quota: %w", &tenant.TiDBCloudAPIError{
				Operation:  "cluster spending limit update",
				StatusCode: http.StatusForbidden,
			}),
			wantStatus: http.StatusForbidden,
			wantBody:   "access denied",
		},
		{
			name:       "status-shaped generic error is not parsed",
			err:        errors.New("tidbcloud native cluster list status 401: attacker-controlled"),
			wantStatus: http.StatusBadGateway,
			wantBody:   "claim tenant pool tenant failed",
		},
		{
			name:       "insufficient IAM role hides resolver detail",
			err:        fmt.Errorf("%w: org:viewer SENSITIVE_RESOLVER_DETAIL", tenant.ErrTiDBCloudRoleInsufficient),
			wantStatus: http.StatusForbidden,
			wantBody:   tenant.ErrTiDBCloudRoleInsufficient.Error(),
		},
		{
			name:       "free tenant limit",
			err:        tenant.ErrTiDBCloudFreeTenantLimitReached,
			wantStatus: http.StatusForbidden,
			wantBody:   tenant.ErrTiDBCloudFreeTenantLimitReached.Error(),
		},
		{
			name:       "free quota lock busy",
			err:        tenant.ErrTiDBCloudFreeQuotaBusy,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   tenant.ErrTiDBCloudFreeQuotaBusy.Error(),
		},
		{
			name:       "generic error hides detail",
			err:        errors.New("internal upstream detail"),
			wantStatus: http.StatusBadGateway,
			wantBody:   "claim tenant pool tenant failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotStatus, gotMsg := clientFacingErrorResponse(http.StatusBadGateway, "claim tenant pool tenant failed", tc.err)
			if gotStatus != tc.wantStatus {
				t.Fatalf("status = %d, want %d; msg=%s", gotStatus, tc.wantStatus, gotMsg)
			}
			if !strings.Contains(gotMsg, tc.wantBody) {
				t.Fatalf("msg = %q, want containing %q", gotMsg, tc.wantBody)
			}
			if strings.Contains(gotMsg, "requestId") || strings.Contains(gotMsg, "details") {
				t.Fatalf("msg = %q, should not expose raw TiDB Cloud details", gotMsg)
			}
			if strings.Contains(gotMsg, "internal upstream detail") {
				t.Fatalf("msg = %q, should not expose generic upstream details", gotMsg)
			}
			if strings.Contains(gotMsg, "SENSITIVE_RESOLVER_DETAIL") {
				t.Fatalf("msg = %q, should not expose IAM resolver details", gotMsg)
			}
		})
	}
}

func waitForDeprovisionCalls(t *testing.T, prov *fakeProvisioner, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := prov.deprovisionCalls.Load(); got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deprovision calls = %d, want %d", prov.deprovisionCalls.Load(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTiDBCloudOrgBindingNotFound(t *testing.T, metaStore *meta.Store, tenantID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID); errors.Is(err, meta.ErrNotFound) {
			return
		}
		if time.Now().After(deadline) {
			binding, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID)
			t.Fatalf("tidbcloud org binding = %#v, err = %v, want ErrNotFound", binding, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTenantClusterReference(t *testing.T, metaStore *meta.Store, tenantID, wantClusterID string) (status, provider, clusterID, host, user, dbName string, port int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := metaStore.DB().QueryRow(`
			SELECT status, provider, COALESCE(cluster_id, ''), db_host, db_port, db_user, db_name
			FROM tenants WHERE id = ?`,
			tenantID,
		).Scan(&status, &provider, &clusterID, &host, &port, &user, &dbName)
		if err != nil {
			t.Fatal(err)
		}
		if clusterID == wantClusterID {
			return status, provider, clusterID, host, user, dbName, port
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant cluster_id = %s, want %s", clusterID, wantClusterID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type nonEarlyBindingProvisioner struct {
	provider string
	cluster  *tenant.ClusterInfo
}

func (f *nonEarlyBindingProvisioner) ProviderType() string { return f.provider }

func (f *nonEarlyBindingProvisioner) ResolveAPIKeyIdentity(context.Context, tenant.CredentialProvisionRequest) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	return &tenant.TiDBCloudAPIKeyIdentity{
		OrganizationID: f.cluster.OrganizationID,
		Role:           tenant.TiDBCloudRoleOrgOwner,
	}, nil
}

func (f *nonEarlyBindingProvisioner) ResolveOrganizationPlan(_ context.Context, organizationID string, _ tenant.CredentialProvisionRequest) (*tenant.TiDBCloudOrganizationPlan, error) {
	return &tenant.TiDBCloudOrganizationPlan{OrganizationID: organizationID}, nil
}

func (f *nonEarlyBindingProvisioner) InitSchema(_ context.Context, _ string) error { return nil }

func (f *nonEarlyBindingProvisioner) Provision(_ context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

type profileAwareFakeProvisioner struct {
	fakeProvisioner
	mu                  sync.Mutex
	profileInitCalls    atomic.Int32
	ensureDBCalls       atomic.Int32
	lastProfile         tenantschema.TiDBAutoEmbeddingProfile
	lastProfileTenantID string
	ensureDBErr         error
	lastEnsureDSN       string
	callOrder           []string
}

func (f *profileAwareFakeProvisioner) EnsureDatabase(_ context.Context, dsn string) error {
	f.ensureDBCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastEnsureDSN = dsn
	f.callOrder = append(f.callOrder, "ensure")
	return f.ensureDBErr
}

func (f *profileAwareFakeProvisioner) InitSchemaForAutoEmbeddingProfile(ctx context.Context, _ string, profile tenantschema.TiDBAutoEmbeddingProfile) error {
	f.profileInitCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastProfile = profile
	f.lastProfileTenantID = tenantschema.TenantIDFromContext(ctx)
	f.callOrder = append(f.callOrder, "profile-init")
	return nil
}

func (f *profileAwareFakeProvisioner) callOrderString() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.callOrder, ",")
}

func TestSchemaInitForTenantEnsuresDatabaseBeforeAutoEmbeddingConfig(t *testing.T) {
	ensureErr := errors.New("database is not ready")
	prov := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		ensureDBErr:     ensureErr,
	}
	srv := NewWithConfig(Config{
		Provisioner: prov,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	init := srv.schemaInitForTenant("tenant-native", tenant.ProviderTiDBCloudNative, func(context.Context, string) error {
		t.Fatal("fallback InitSchema was called")
		return nil
	})
	err := init(context.Background(), "u1.root:db-pass@tcp(db.example:4000)/tidbcloud_fs?parseTime=true&tls=true")
	if !errors.Is(err, ensureErr) {
		t.Fatalf("schema init error = %v, want ensure error", err)
	}
	if prov.ensureDBCalls.Load() != 1 {
		t.Fatalf("ensure DB calls = %d, want 1", prov.ensureDBCalls.Load())
	}
	if prov.profileInitCalls.Load() != 0 {
		t.Fatalf("profile init calls = %d, want 0", prov.profileInitCalls.Load())
	}
	if prov.lastEnsureDSN == "" {
		t.Fatal("ensure DB DSN was empty")
	}
}

func TestSchemaInitForTenantEnsuresDatabaseBeforeProfileInit(t *testing.T) {
	prov := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
	}
	srv := NewWithConfig(Config{
		Provisioner: prov,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	init := srv.schemaInitForTenant("tenant-native", tenant.ProviderTiDBCloudNative, func(context.Context, string) error {
		t.Fatal("fallback InitSchema was called")
		return nil
	})
	err := init(context.Background(), "u1.root:db-pass@tcp(db.example:4000)/tidbcloud_fs?parseTime=true&tls=true")
	if err != nil {
		t.Fatalf("schema init: %v", err)
	}
	if prov.ensureDBCalls.Load() != 1 {
		t.Fatalf("ensure DB calls = %d, want 1", prov.ensureDBCalls.Load())
	}
	if prov.profileInitCalls.Load() != 1 {
		t.Fatalf("profile init calls = %d, want 1", prov.profileInitCalls.Load())
	}
	if got, want := prov.callOrderString(), "ensure,profile-init"; got != want {
		t.Fatalf("call order = %s, want %s", got, want)
	}
	prov.mu.Lock()
	lastProfileTenantID := prov.lastProfileTenantID
	prov.mu.Unlock()
	if lastProfileTenantID != "tenant-native" {
		t.Fatalf("profile init tenant id = %q, want tenant-native", lastProfileTenantID)
	}
}

func TestProvisionMarksTenantFailedWhenInitKeepsFailing(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, initErr: fmt.Errorf("boom"), cluster: &tenant.ClusterInfo{
		ClusterID: "bad-cluster",
		Host:      "127.0.0.1",
		Port:      3306,
		Username:  "root",
		Password:  "bad",
		DBName:    "bad",
	}}

	origWindow, origInitBackoff, origMaxBackoff := schemaInitRetryWindow, schemaInitInitialBackoff, schemaInitMaxBackoff
	schemaInitRetryWindow = 120 * time.Millisecond
	schemaInitInitialBackoff = 10 * time.Millisecond
	schemaInitMaxBackoff = 20 * time.Millisecond
	defer func() {
		schemaInitRetryWindow = origWindow
		schemaInitInitialBackoff = origInitBackoff
		schemaInitMaxBackoff = origMaxBackoff
	}()

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" {
		t.Fatalf("unexpected provision response: %+v", out)
	}
	apiKey := out["api_key"]
	if apiKey == "" {
		t.Fatal("empty api_key")
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(apiKey))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := resolved.Tenant.ID

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantFailed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become failed in time, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestProvisionUsesConfiguredProvisioner(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host := "127.0.0.1"
	port := 3306
	if parsed.Addr != "" {
		h, p, ok := strings.Cut(parsed.Addr, ":")
		if ok {
			host = h
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		Host:      host,
		Port:      port,
		Username:  parsed.User,
		Password:  parsed.Passwd,
		DBName:    parsed.DBName,
	}}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero, "db_tls": false})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" {
		t.Fatalf("unexpected provision response: %+v", out)
	}
	claims, err := token.ParseAndVerifyToken(tokenSecret, out["api_key"])
	if err != nil {
		t.Fatalf("ParseAndVerifyToken provision api key: %v", err)
	}
	hasAdmin := false
	for _, permission := range claims.JournalPermissions {
		if permission == JournalPermissionAdmin {
			hasAdmin = true
			break
		}
	}
	if !hasAdmin {
		t.Fatalf("provision api key journal_permissions = %#v, want %s", claims.JournalPermissions, JournalPermissionAdmin)
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(out["api_key"]))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := resolved.Tenant.ID
	if out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("expected provisioning response status, got %q", out["status"])
	}

	deadline := time.Now().Add(3 * time.Second)
	var status, provider, clusterID string
	for {
		row := metaStore.DB().QueryRow("SELECT status, provider, cluster_id FROM tenants WHERE id = ?", tenantID)
		if err := row.Scan(&status, &provider, &clusterID); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active in time: status=%s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if provider != tenant.ProviderTiDBZero || clusterID != "cluster-1" {
		t.Fatalf("unexpected tenant row: status=%s provider=%s cluster_id=%s", status, provider, clusterID)
	}
}

func TestProvisionTiDBCloudNativeUsesRequestCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1", cluster: &tenant.ClusterInfo{
		ClusterID:      "native-cluster-1",
		OrganizationID: "org-1",
		Host:           "db.example",
		Port:           4000,
		Username:       "u1.root",
		Password:       "db-pass",
		DBName:         "customer_db",
	}}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	spendingLimit := int64(10000)
	maxStorageSize := int64(1000)
	body, _ := json.Marshal(map[string]any{
		"public_key":               "public-1",
		"private_key":              "private-1",
		"tidbcloud_spending_limit": spendingLimit,
		"max_storage_size":         maxStorageSize,
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := prov.ProvisionCallCount(); got != 0 {
		t.Fatalf("plain provision calls = %d, want 0", got)
	}
	if got := prov.credentialCalls.Load(); got != 0 {
		t.Fatalf("credential provision calls = %d, want 0 when create-time quota is set", got)
	}
	if got := prov.credentialQuotaCalls.Load(); got != 1 {
		t.Fatalf("credential quota provision calls = %d, want 1", got)
	}
	if got := prov.quotaMarkCalls.Load(); got != 0 {
		t.Fatalf("quota mark calls = %d, want 0 for create-time quota", got)
	}
	if got := prov.quotaUpdateCalls.Load(); got != 0 {
		t.Fatalf("quota update calls = %d, want 0 for create-time quota", got)
	}
	if prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly == nil || *prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly != spendingLimit {
		t.Fatalf("create quota spending limit = %#v, want %d", prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly, spendingLimit)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("credential request = %+v", prov.lastCredentialReq)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" || out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("unexpected response: %+v", out)
	}
	if out["cloud_provider"] != "aws" || out["region"] != "us-east-1" {
		t.Fatalf("native cloud/region response = %+v", out)
	}
	if _, ok := out["mode"]; ok {
		t.Fatalf("native provision response unexpectedly included mode: %+v", out)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		var status string
		if err := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", out["tenant_id"]).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active in time: status=%s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got := prov.systemUserCalls.Load(); got == 0 {
		t.Fatal("native system user setup was not called")
	}
	binding, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get tidbcloud org binding: %v", err)
	}
	if binding.OrganizationID != "org-1" || binding.ClusterID != "native-cluster-1" {
		t.Fatalf("binding = %#v", binding)
	}
	quotaCfg, err := metaStore.GetQuotaConfig(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get quota config: %v", err)
	}
	if quotaCfg.MaxStorageBytes != maxStorageSize*quotaStorageSizeBytes {
		t.Fatalf("quota max storage = %d, want %d", quotaCfg.MaxStorageBytes, maxStorageSize*quotaStorageSizeBytes)
	}

	var provider, dbName, dbUser string
	var passCipher []byte
	if err := metaStore.DB().QueryRow("SELECT provider, db_name, db_user, db_password FROM tenants WHERE id = ?", out["tenant_id"]).Scan(&provider, &dbName, &dbUser, &passCipher); err != nil {
		t.Fatal(err)
	}
	if provider != tenant.ProviderTiDBCloudNative || dbName != "customer_db" {
		t.Fatalf("tenant provider/db = %s/%s", provider, dbName)
	}
	if dbUser != "u1.tdc_fs_sys" {
		t.Fatalf("tenant db_user = %q, want system user", dbUser)
	}
	plain, err := pool.Decrypt(context.Background(), passCipher)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != "system-pass" {
		t.Fatalf("tenant db password = %q, want system password", plain)
	}
}

func TestProvisionTiDBCloudNativePersistsEarlyClusterBindingBeforeMetadataWait(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	release := make(chan struct{})
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-early-server", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-early-server", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
		waitRelease: release,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	type provisionOutcome struct {
		result *provisionTenantResult
		err    error
	}
	outcomes := make(chan provisionOutcome, 1)
	go func() {
		result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
			KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
			TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-early-server"},
		})
		outcomes <- provisionOutcome{result: result, err: err}
	}()

	select {
	case <-prov.waitStarted:
	case outcome := <-outcomes:
		t.Fatalf("provision returned before metadata wait: result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(2 * time.Second):
		t.Fatal("metadata wait did not start")
	}
	var tenantID, status, dbUser string
	if err := rt.meta.DB().QueryRow(`SELECT id, status, db_user FROM tenants WHERE cluster_id = ?`, "cluster-early-server").Scan(&tenantID, &status, &dbUser); err != nil {
		t.Fatalf("query early-bound tenant: %v", err)
	}
	if status != string(meta.TenantPending) || dbUser != "" {
		t.Fatalf("early-bound tenant status/user = %s/%q, want pending/empty", status, dbUser)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID)
	if err != nil || binding.OrganizationID != "org-early-server" || binding.ClusterID != "cluster-early-server" {
		t.Fatalf("early org binding = %+v, err=%v", binding, err)
	}

	close(release)
	outcome := <-outcomes
	if outcome.err != nil || outcome.result == nil || outcome.result.Status != meta.TenantProvisioning {
		t.Fatalf("provision outcome = %+v err=%v", outcome.result, outcome.err)
	}
	row, err := rt.meta.GetTenant(context.Background(), tenantID)
	if err != nil || row.Status != meta.TenantProvisioning || row.DBUser != "u1.root" || row.DBHost != "db.example" {
		t.Fatalf("final tenant = %+v, err=%v", row, err)
	}
}

func TestProvisionFreeTiDBCloudNativeHoldsQuotaLockUntilEarlyBindingAndReleasesBeforeMetadataWait(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	createRelease := make(chan struct{})
	waitRelease := make(chan struct{})
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		createStarted:   make(chan struct{}),
		createRelease:   createRelease,
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-free-lock", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-free-lock", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
		waitRelease: waitRelease,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	type provisionOutcome struct {
		result *provisionTenantResult
		err    error
	}
	outcomes := make(chan provisionOutcome, 1)
	go func() {
		result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
			KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
			TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-free-lock", IsFree: true},
		})
		outcomes <- provisionOutcome{result: result, err: err}
	}()

	select {
	case <-prov.createStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("cluster create did not start")
	}
	type quotaLockOutcome struct {
		release func() error
		err     error
	}
	lockOutcomes := make(chan quotaLockOutcome, 1)
	go func() {
		release, err := rt.meta.AcquireTiDBCloudFreeQuotaLock(context.Background(), "org-free-lock")
		lockOutcomes <- quotaLockOutcome{release: release, err: err}
	}()
	select {
	case outcome := <-lockOutcomes:
		if outcome.release != nil {
			_ = outcome.release()
		}
		t.Fatalf("quota lock was acquirable during cluster create: %v", outcome.err)
	case <-time.After(100 * time.Millisecond):
	}

	close(createRelease)
	select {
	case <-prov.waitStarted:
	case outcome := <-outcomes:
		t.Fatalf("provision returned before metadata wait: result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(2 * time.Second):
		t.Fatal("metadata wait did not start")
	}
	lockResult := <-lockOutcomes
	if lockResult.err != nil {
		t.Fatalf("quota lock remained held during metadata wait: %v", lockResult.err)
	}
	if err := lockResult.release(); err != nil {
		t.Fatalf("release verification lock: %v", err)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free-lock")
	if err != nil || count != 1 {
		t.Fatalf("free count during metadata wait = %d, err=%v, want 1", count, err)
	}

	close(waitRelease)
	outcome := <-outcomes
	if outcome.err != nil || outcome.result == nil {
		t.Fatalf("provision outcome = %+v err=%v", outcome.result, outcome.err)
	}
}

func TestProvisionTiDBCloudNativeRejectsMetadataOrganizationMismatchAndCleansCluster(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-org-mismatch", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-actual", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
		TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-expected"},
	})
	if result != nil || err == nil {
		t.Fatalf("provision result=%+v err=%v, want organization mismatch", result, err)
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusBadGateway {
		t.Fatalf("provision error = %#v, want 502", err)
	}
	waitForDeprovisionCalls(t, prov.fakeProvisioner, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "cluster-org-mismatch" {
		t.Fatalf("deprovision cluster = %+v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq != cred {
		t.Fatalf("cleanup credentials = %+v, want %+v", prov.lastCredentialReq, cred)
	}
	var tenantID, status string
	if err := rt.meta.DB().QueryRow(`SELECT id, status FROM tenants WHERE id <> ? ORDER BY created_at DESC LIMIT 1`, rt.tenantID).Scan(&tenantID, &status); err != nil {
		t.Fatalf("query failed tenant: %v", err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want failed", status)
	}
}

func TestProvisionTiDBCloudNativeCleansClusterWhenEarlyBindingPersistenceFails(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-quota-1", Password: "db-pass", DBName: "customer_db",
		},
		ready:       &tenant.ClusterInfo{OrganizationID: "org-1", Host: "db.example", Port: 4000, Username: "u1.root"},
		waitStarted: make(chan struct{}),
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
		TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-1"},
	})
	if result != nil || err == nil {
		t.Fatalf("provision result=%+v err=%v, want early binding failure", result, err)
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500", err)
	}
	waitForDeprovisionCalls(t, prov.fakeProvisioner, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "cluster-quota-1" {
		t.Fatalf("deprovision cluster = %+v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq != cred {
		t.Fatalf("cleanup credentials = %+v, want %+v", prov.lastCredentialReq, cred)
	}
	var status string
	if err := rt.meta.DB().QueryRow(`SELECT status FROM tenants WHERE id <> ? ORDER BY created_at DESC LIMIT 1`, rt.tenantID).Scan(&status); err != nil {
		t.Fatalf("query failed tenant: %v", err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want failed", status)
	}
}

func TestProvisionTiDBCloudNativeFinalizationDoesNotOverwriteChangedStatus(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	release := make(chan struct{})
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-finalize-cas", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-finalize-cas", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
		waitRelease: release,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	type provisionOutcome struct {
		result *provisionTenantResult
		err    error
	}
	outcomes := make(chan provisionOutcome, 1)
	go func() {
		result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
			KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
			TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-finalize-cas"},
		})
		outcomes <- provisionOutcome{result: result, err: err}
	}()
	select {
	case <-prov.waitStarted:
	case outcome := <-outcomes:
		t.Fatalf("provision returned before metadata wait: result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(2 * time.Second):
		t.Fatal("metadata wait did not start")
	}
	var tenantID string
	if err := rt.meta.DB().QueryRow(`SELECT id FROM tenants WHERE cluster_id = ?`, "cluster-finalize-cas").Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed); err != nil {
		t.Fatal(err)
	}
	close(release)
	outcome := <-outcomes
	if outcome.result != nil || outcome.err == nil {
		t.Fatalf("provision outcome result=%+v err=%v, want lost-CAS failure", outcome.result, outcome.err)
	}
	row, err := rt.meta.GetTenant(context.Background(), tenantID)
	if err != nil || row.Status != meta.TenantFailed {
		t.Fatalf("tenant after lost CAS = %+v, err=%v", row, err)
	}
}

func TestProvisionFreeTiDBCloudTenantPersistsNativeBindingAndExplicitQuota(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 3)
	resp := rt.postProvision(t, map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 202: %s", resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	tenantID := out["tenant_id"]
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID)
	if err != nil || binding.OrganizationID != "org-free" {
		t.Fatalf("native binding = %+v, err=%v", binding, err)
	}
	cfg, err := rt.meta.GetQuotaConfig(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != DefaultTiDBCloudFreeMaxStorageBytes ||
		cfg.MaxFileSizeBytes != DefaultTiDBCloudFreeMaxFileSizeBytes ||
		cfg.MaxFileCount != DefaultTiDBCloudFreeMaxFileCount ||
		cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != 0 {
		t.Fatalf("free quota = %+v", cfg)
	}
	if rt.provisioner.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly == nil || *rt.provisioner.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly != 0 {
		t.Fatalf("create spending limit = %+v, want explicit zero", rt.provisioner.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 1 {
		t.Fatalf("free tenant count = %d, err=%v, want 1", count, err)
	}
}

func TestProvisionFreeTiDBCloudTenantRejectsDisallowedQuotaBeforeMutation(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "positive spending",
			body: map[string]any{"tidbcloud_spending_limit": int64(10)},
			want: tenant.ErrTiDBCloudFreeSpendingLimitForbidden.Error(),
		},
		{
			name: "storage over cap",
			body: map[string]any{"max_storage_size": int64(5121)},
			want: tenant.ErrTiDBCloudFreeQuotaExceeded.Error(),
		},
		{
			name: "unlimited file count",
			body: map[string]any{"max_file_count": int64(0)},
			want: tenant.ErrTiDBCloudFreeQuotaExceeded.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newTiDBCloudFreeProvisionRuntime(t, 3)
			tt.body["public_key"] = "public-1"
			tt.body["private_key"] = "private-1"
			resp := rt.postProvision(t, tt.body)
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusForbidden || strings.TrimSpace(string(body)) != fmt.Sprintf(`{"error":%q}`, tt.want) {
				t.Fatalf("response = %d %s, want 403 %q", resp.StatusCode, body, tt.want)
			}
			var tenantRows int
			if err := rt.meta.DB().QueryRow(`SELECT COUNT(*) FROM tenants`).Scan(&tenantRows); err != nil {
				t.Fatal(err)
			}
			if tenantRows != 0 || rt.provisioner.credentialCalls.Load() != 0 || rt.provisioner.credentialQuotaCalls.Load() != 0 {
				t.Fatalf("mutation after rejection: tenants=%d credential=%d quota=%d", tenantRows, rt.provisioner.credentialCalls.Load(), rt.provisioner.credentialQuotaCalls.Load())
			}
		})
	}
}

func TestProvisionFreeTiDBCloudTenantEnforcesConfiguredTenantLimit(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 1)
	first := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(first.Body)
		t.Fatalf("first status = %d, want 202: %s", first.StatusCode, body)
	}
	second := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = second.Body.Close() }()
	body, err := io.ReadAll(second.Body)
	if err != nil {
		t.Fatal(err)
	}
	if second.StatusCode != http.StatusForbidden || strings.TrimSpace(string(body)) != fmt.Sprintf(`{"error":%q}`, tenant.ErrTiDBCloudFreeTenantLimitReached.Error()) {
		t.Fatalf("second response = %d %s", second.StatusCode, body)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 1 {
		t.Fatalf("free count = %d, err=%v, want 1", count, err)
	}
}

func TestProvisionFreeTiDBCloudTenantRejectsEveryNonZeroSpendingLimitWith403(t *testing.T) {
	for _, spendingLimit := range []int64{-1, 1, meta.MaxTiDBCloudSpendingLimit + 1} {
		t.Run(fmt.Sprintf("spending_%d", spendingLimit), func(t *testing.T) {
			rt := newTiDBCloudFreeProvisionRuntime(t, 3)
			resp := rt.postProvision(t, map[string]any{
				"public_key": "public-1", "private_key": "private-1",
				"tidbcloud_spending_limit": spendingLimit,
			})
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusForbidden {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 403: %s", resp.StatusCode, body)
			}
		})
	}
}

func TestProvisionFreeTiDBCloudTenantReleasesReservationWhenClusterCreateFails(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 1)
	rt.provisioner.cluster = nil
	rt.provisioner.provisionErr = errors.New("cluster create unavailable")
	resp := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502: %s", resp.StatusCode, body)
	}
	var status meta.TenantStatus
	if err := rt.meta.DB().QueryRowContext(context.Background(), "SELECT status FROM tenants").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleted)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 0 {
		t.Fatalf("free tenant count = %d, err=%v, want 0", count, err)
	}
}

func TestProvisionFreeTiDBCloudTenantReleasesReservationAfterClusterCleanup(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 1)
	waitErr := errors.New("metadata unavailable")
	prov := &earlyBindingProvisioner{
		fakeProvisioner: rt.provisioner,
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-cleanup", OrganizationID: "org-free", Password: "db-pass", DBName: "customer_db",
		},
		waitStarted: make(chan struct{}),
		waitErr:     waitErr,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &cred,
		TiDBCloudAccess:       &tiDBCloudAccessProfile{OrganizationID: "org-free", IsFree: true},
	})
	if !errors.Is(err, waitErr) {
		t.Fatalf("provision error = %v, want %v", err, waitErr)
	}
	waitForDeprovisionCalls(t, rt.provisioner, 1)
	deadline := time.Now().Add(2 * time.Second)
	for {
		var status meta.TenantStatus
		if err := rt.meta.DB().QueryRowContext(context.Background(), "SELECT status FROM tenants").Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == meta.TenantDeleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant status = %s, want %s after cleanup", status, meta.TenantDeleted)
		}
		time.Sleep(10 * time.Millisecond)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 0 {
		t.Fatalf("free tenant count = %d, err=%v, want 0", count, err)
	}
}

func TestProvisionNonFreeTiDBCloudTenantDoesNotConsumeFreeQuota(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 3)
	rt.provisioner.billingFree = false
	resp := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 202: %s", resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(context.Background(), out["tenant_id"])
	if err != nil || binding.OrganizationID != "org-free" {
		t.Fatalf("native binding = %+v, err=%v", binding, err)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 0 {
		t.Fatalf("free tenant count = %d, err=%v, want 0", count, err)
	}
}

func TestProvisionTiDBCloudBillingFailurePrecedesTenantMutation(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 3)
	rt.provisioner.billingErr = tenant.ErrTiDBCloudBillingUnavailable
	resp := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502: %s", resp.StatusCode, body)
	}
	var tenantRows int
	if err := rt.meta.DB().QueryRow(`SELECT COUNT(*) FROM tenants`).Scan(&tenantRows); err != nil {
		t.Fatal(err)
	}
	if tenantRows != 0 || rt.provisioner.credentialCalls.Load() != 0 || rt.provisioner.credentialQuotaCalls.Load() != 0 {
		t.Fatalf("mutation after Billing failure: tenants=%d credential=%d quota=%d", tenantRows, rt.provisioner.credentialCalls.Load(), rt.provisioner.credentialQuotaCalls.Load())
	}
}

func TestWriteProvisionTenantErrorMapsFreeQuotaBusyToRetryable503(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeProvisionTenantError(recorder, newProvisionTenantError(
		http.StatusServiceUnavailable,
		tenant.ErrTiDBCloudFreeQuotaBusy.Error(),
		tenant.ErrTiDBCloudFreeQuotaBusy,
	))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if recorder.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", recorder.Header().Get("Retry-After"))
	}
	if got, want := strings.TrimSpace(recorder.Body.String()), fmt.Sprintf(`{"error":%q}`, tenant.ErrTiDBCloudFreeQuotaBusy.Error()); got != want {
		t.Fatalf("body = %s, want %s", got, want)
	}
}

type tiDBCloudFreeProvisionRuntime struct {
	meta        *meta.Store
	provisioner *fakeProvisioner
	server      *Server
	httpServer  *httptest.Server
}

func newTiDBCloudFreeProvisionRuntime(t *testing.T, tenantLimit int) *tiDBCloudFreeProvisionRuntime {
	t.Helper()
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	provisioner := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		identityOrg:   "org-free",
		billingFree:   true,
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-free-cluster",
			OrganizationID: "org-free",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  provisioner,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
		TiDBCloudFreePlanLimits: TiDBCloudFreePlanLimits{
			TenantCount:      tenantLimit,
			MaxStorageBytes:  DefaultTiDBCloudFreeMaxStorageBytes,
			MaxFileSizeBytes: DefaultTiDBCloudFreeMaxFileSizeBytes,
			MaxFileCount:     DefaultTiDBCloudFreeMaxFileCount,
		},
	})
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return &tiDBCloudFreeProvisionRuntime{meta: metaStore, provisioner: provisioner, server: srv, httpServer: ts}
}

func (rt *tiDBCloudFreeProvisionRuntime) postProvision(t *testing.T, body map[string]any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, rt.httpServer.URL+"/v1/provision", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestProvisionTiDBCloudNativeCreateQuotaSkipsQuotaPatch(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-create-quota",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}

	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	maxStorageSize := int64(1000)
	spendingLimit := int64(10000)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
		Quota: &quotaRequest{quotaFields: quotaFields{
			MaxStorageSize:         &maxStorageSize,
			TiDBCloudSpendingLimit: &spendingLimit,
		}},
	})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	if got := prov.credentialQuotaCalls.Load(); got != 1 {
		t.Fatalf("credential quota provision calls = %d, want 1", got)
	}
	if got := prov.quotaMarkCalls.Load(); got != 0 {
		t.Fatalf("quota mark calls = %d, want 0 for create-time quota", got)
	}
	if got := prov.quotaUpdateCalls.Load(); got != 0 {
		t.Fatalf("quota update calls = %d, want 0 for create-time quota", got)
	}
	if got := prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	if prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly == nil || *prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly != spendingLimit {
		t.Fatalf("create quota spending limit = %#v, want %d", prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly, spendingLimit)
	}

	cfg, err := metaStore.GetQuotaConfig(context.Background(), res.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != maxStorageSize*quotaStorageSizeBytes {
		t.Fatalf("quota max storage = %d, want %d", cfg.MaxStorageBytes, maxStorageSize*quotaStorageSizeBytes)
	}
}

func TestProvisionSeedsQuotaConfigWithoutExplicitQuota(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-seed-quota",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}

	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{
		"public_key":  "public-1",
		"private_key": "private-1",
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" || out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("unexpected response: %+v", out)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		var status string
		if err := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", out["tenant_id"]).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active in time: status=%s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	cfg, err := metaStore.GetQuotaConfig(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get quota config: %v", err)
	}
	if cfg.TiDBCloudSpendingLimit != nil {
		t.Fatalf("spending limit = %d, want nil", *cfg.TiDBCloudSpendingLimit)
	}
	if cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("checked_at = %v, want nil", cfg.TiDBCloudSpendingLimitCheckedAt)
	}
}

func TestProvisionTiDBCloudNativeRequiresEarlyBindingProvisioner(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &nonEarlyBindingProvisioner{
		provider: tenant.ProviderTiDBCloudNative,
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-no-quota-provisioner",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want unsupported early-binding provisioner error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500 provisionTenantError", err)
	}
	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
	var clusterID string
	if err := metaStore.DB().QueryRow("SELECT COALESCE(cluster_id, '') FROM tenants WHERE id = ?", tenantID).Scan(&clusterID); err != nil {
		t.Fatal(err)
	}
	if clusterID != "" {
		t.Fatalf("tenant cluster_id = %s, want empty after cleanup", clusterID)
	}
}

func TestProvisionTiDBCloudNativeCreateTimeQuotaLocalPersistenceErrorIsInternal(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	if _, err := metaStore.DB().Exec("RENAME TABLE tenant_quota_config TO tenant_quota_config_unavailable"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = metaStore.DB().Exec("RENAME TABLE tenant_quota_config_unavailable TO tenant_quota_config")
	})

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-quota-local-error",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	maxStorageSize := int64(1000)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
		Quota: &quotaRequest{quotaFields: quotaFields{
			MaxStorageSize: &maxStorageSize,
		}},
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want quota persistence error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
}

func TestProvisionTiDBCloudNativeCleansClusterWhenOrgBindingMissing(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID: "native-cluster-missing-org",
			Host:      "db.example",
			Port:      4000,
			Username:  "u1.root",
			Password:  "db-pass",
			DBName:    "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want org binding error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusBadGateway {
		t.Fatalf("provision error = %#v, want 502 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "native-cluster-missing-org" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	var bindingCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenant_tidbcloud_org_bindings WHERE tenant_id = ?", tenantID).Scan(&bindingCount); err != nil {
		t.Fatal(err)
	}
	if bindingCount != 0 {
		t.Fatalf("binding count = %d, want 0", bindingCount)
	}
}

func TestProvisionTiDBCloudNativeCleansClusterWhenProvisionReturnsClusterAndError(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		provisionErr:  fmt.Errorf("wait metadata failed"),
		cluster: &tenant.ClusterInfo{
			ClusterID: "native-cluster-provision-error",
			Password:  "db-pass",
			DBName:    "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want provision error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusBadGateway {
		t.Fatalf("provision error = %#v, want 502 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "native-cluster-provision-error" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
}

func TestProvisionTiDBCloudNativePersistsClusterReferenceWhenCleanupFails(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:       tenant.ProviderTiDBCloudNative,
		cloudProvider:  "aws",
		region:         "us-east-1",
		provisionErr:   fmt.Errorf("wait metadata failed"),
		deprovisionErr: fmt.Errorf("delete unavailable"),
		cluster: &tenant.ClusterInfo{
			ClusterID: "native-cluster-cleanup-error",
			Host:      "db.example",
			Port:      4000,
			Username:  "u1.root",
			Password:  "db-pass",
			DBName:    "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want provision error")
	}
	waitForDeprovisionCalls(t, prov, 1)

	var tenantID string
	if err := metaStore.DB().QueryRow("SELECT id FROM tenants LIMIT 1").Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	status, provider, clusterID, host, user, dbName, port := waitForTenantClusterReference(t, metaStore, tenantID, "native-cluster-cleanup-error")
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	if provider != tenant.ProviderTiDBCloudNative || clusterID != "native-cluster-cleanup-error" {
		t.Fatalf("tenant provider/cluster = %s/%s, want %s/native-cluster-cleanup-error", provider, clusterID, tenant.ProviderTiDBCloudNative)
	}
	if host != "db.example" || port != 4000 || user != "u1.root" || dbName != "customer_db" {
		t.Fatalf("tenant reference = %s:%d %s/%s, want db.example:4000 u1.root/customer_db", host, port, user, dbName)
	}
}

func TestProvisionTiDBCloudNativeCleansClusterWhenPasswordEncryptFails(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, failingEncryptor{err: fmt.Errorf("kms unavailable")})
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-encrypt-error",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want encrypt error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "native-cluster-encrypt-error" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
	var clusterID string
	if err := metaStore.DB().QueryRow("SELECT COALESCE(cluster_id, '') FROM tenants WHERE id = ?", tenantID).Scan(&clusterID); err != nil {
		t.Fatal(err)
	}
	if clusterID != "" {
		t.Fatalf("tenant cluster_id = %s, want empty after cleanup", clusterID)
	}
}

func TestProvisionTiDBCloudNativeRequiresRequestCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", strings.NewReader(`{"public_key":"public-1"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if got := prov.credentialCalls.Load(); got != 0 {
		t.Fatalf("credential provision calls = %d, want 0", got)
	}
	var tenantCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if tenantCount != 0 {
		t.Fatalf("tenant count = %d, want 0", tenantCount)
	}
}

func TestDecodeCredentialProvisionRequestRejectsTrailingJSON(t *testing.T) {
	body := strings.NewReader(`{"public_key":"public-1","private_key":"private-1"} {}`)
	req, _ := http.NewRequest(http.MethodPost, "/v1/provision", body)
	_, err := decodeCredentialProvisionRequest(httptest.NewRecorder(), req)
	if err == nil {
		t.Fatal("expected trailing JSON error")
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeCredentialProvisionRequestRejectsOversizedBody(t *testing.T) {
	body := strings.NewReader(`{"public_key":"` + strings.Repeat("x", int(maxCredentialProvisionBodyBytes)+1) + `","private_key":"private-1"}`)
	req, _ := http.NewRequest(http.MethodPost, "/v1/provision", body)
	_, err := decodeCredentialProvisionRequest(httptest.NewRecorder(), req)
	if err == nil {
		t.Fatal("expected oversized body error")
	}
	var maxErr *http.MaxBytesError
	if !errors.As(err, &maxErr) {
		t.Fatalf("error = %v, want MaxBytesError", err)
	}
}

func TestProvisionTiDBCloudNativeRejectsQuotaWithoutCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:          tenant.ProviderTiDBCloudNative,
		cluster:           &tenant.ClusterInfo{},
		defaultPublicKey:  "default-public",
		defaultPrivateKey: "default-private",
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	spendingLimit := int64(10000)
	body, _ := json.Marshal(map[string]any{
		"tidbcloud_spending_limit": spendingLimit,
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if got := prov.credentialCalls.Load(); got != 0 {
		t.Fatalf("credential provision calls = %d, want 0", got)
	}
	if got := prov.quotaUpdateCalls.Load(); got != 0 {
		t.Fatalf("quota update calls = %d, want 0", got)
	}
	msg, _ := raw["error"].(string)
	if !strings.Contains(strings.ToLower(msg), "requires public_key and private_key when quota settings are provided") {
		t.Fatalf("error = %#v", raw)
	}
}

func TestProvisionTenantRejectsMissingNativeCredentialsBeforeInsert(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})
	defer srv.Close()

	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{KeyName: "default"})
	if err == nil {
		t.Fatal("expected missing native credentials error")
	}
	if got := prov.ProvisionCallCount(); got != 0 {
		t.Fatalf("plain provision calls = %d, want 0", got)
	}
	var tenantCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if tenantCount != 0 {
		t.Fatalf("tenant count = %d, want 0", tenantCount)
	}
}

func TestProvisionRejectsCredentialsForNonNativeProvider(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	for _, provider := range []string{tenant.ProviderTiDBZero, tenant.ProviderDB9} {
		prov := &fakeProvisioner{provider: provider}
		srv := NewWithConfig(Config{
			Meta:        metaStore,
			Pool:        pool,
			Provisioner: prov,
			TokenSecret: tokenSecret,
		})

		ts := httptest.NewServer(srv)
		body, _ := json.Marshal(map[string]string{"public_key": "test", "private_key": "test"})
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			ts.Close()
			t.Fatalf("%s: request failed: %v", provider, err)
		}
		_ = resp.Body.Close()
		ts.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status=%d, want 400", provider, resp.StatusCode)
		}
	}
}

func TestProvisionPersistsEncryptedAutoEmbeddingProfile(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-profile",
		Host:      "127.0.0.1",
		Port:      4000,
		Username:  "root",
		Password:  "db-pass",
		DBName:    "tenant_db",
	}}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
		TiDBAutoEmbeddingAPIKey: "sk-profile-test",
	})
	defer srv.Close()

	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{KeyName: "default"})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	profile, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if profile.Model != "openai/text-embedding-3-small" {
		t.Fatalf("profile model = %q", profile.Model)
	}
	if profile.Dimensions != 1536 {
		t.Fatalf("profile dimensions = %d", profile.Dimensions)
	}
	if profile.OptionsJSON != `{"dimensions":1536}` {
		t.Fatalf("profile options_json = %q", profile.OptionsJSON)
	}
	if profile.APIBase != "" {
		t.Fatalf("profile api_base = %q", profile.APIBase)
	}
	if string(profile.APIKeyCipher) == "sk-profile-test" {
		t.Fatal("profile API key was stored in plaintext")
	}
	plain, err := pool.Decrypt(context.Background(), profile.APIKeyCipher)
	if err != nil {
		t.Fatalf("decrypt profile API key: %v", err)
	}
	if string(plain) != "sk-profile-test" {
		t.Fatalf("decrypted API key = %q", string(plain))
	}
}

func TestProvisionPersistsAutoEmbeddingProfileWhenDatabaseAutoEmbeddingDisabled(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{
		S3Dir:                        mustTempDir(t),
		PublicURL:                    "http://localhost",
		DisableDatabaseAutoEmbedding: true,
	}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-disabled-profile",
		Host:      "127.0.0.1",
		Port:      4000,
		Username:  "root",
		Password:  "db-pass",
		DBName:    "tenant_db",
	}}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{KeyName: "default"})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	profile, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if profile.Model != "openai/text-embedding-3-small" || profile.Dimensions != 1536 {
		t.Fatalf("profile = %+v", profile)
	}
	if profile.EmbeddingMode != meta.TenantEmbeddingModeFTSOnly {
		t.Fatalf("embedding_mode=%q, want %q", profile.EmbeddingMode, meta.TenantEmbeddingModeFTSOnly)
	}
	if len(profile.APIKeyCipher) != 0 {
		t.Fatal("disabled auto-embedding profile should not store an empty API key cipher")
	}
}

func TestSchemaInitForTenantUsesFTSOnlyProfileWhenDatabaseAutoEmbeddingDisabled(t *testing.T) {
	prov := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBZero},
	}
	origInitFTSOnly := initTiDBTenantSchemaForFTSOnlyProfileFunc
	var ftsOnlyInitCalls int
	var ftsOnlyProfile tenantschema.TiDBAutoEmbeddingProfile
	initTiDBTenantSchemaForFTSOnlyProfileFunc = func(_ context.Context, _ string, profile tenantschema.TiDBAutoEmbeddingProfile) error {
		ftsOnlyInitCalls++
		ftsOnlyProfile = profile
		return nil
	}
	t.Cleanup(func() {
		initTiDBTenantSchemaForFTSOnlyProfileFunc = origInitFTSOnly
	})
	srv := NewWithConfig(Config{
		Provisioner:                  prov,
		DisableDatabaseAutoEmbedding: true,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	fallbackCalled := false
	init := srv.schemaInitForTenant("tenant-disabled", tenant.ProviderTiDBZero, func(context.Context, string) error {
		fallbackCalled = true
		return nil
	})

	if err := init(context.Background(), "dsn"); err != nil {
		t.Fatalf("schema init: %v", err)
	}
	if fallbackCalled {
		t.Fatal("fallback InitSchema was called")
	}
	if prov.profileInitCalls.Load() != 0 {
		t.Fatalf("profile init calls = %d, want 0", prov.profileInitCalls.Load())
	}
	if ftsOnlyInitCalls != 1 {
		t.Fatalf("fts-only init calls = %d, want 1", ftsOnlyInitCalls)
	}
	if ftsOnlyProfile.Model != "openai/text-embedding-3-small" || ftsOnlyProfile.Dimensions != 1536 {
		t.Fatalf("fts-only init profile = %+v", ftsOnlyProfile)
	}
}

func TestInitTenantSchemaAsyncPersistsTargetSchemaVersion(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBPasswordCipher: []byte{},
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	profile := tenantschema.TiDBAutoEmbeddingProfile{
		Model:       "openai/text-embedding-v4",
		Dimensions:  1024,
		OptionsJSON: `{"dimensions":1024}`,
	}
	targetVersion, err := tenantschema.TiDBTenantSchemaVersionForEmbeddingModeProfile(tenantschema.TiDBEmbeddingModeFTSOnly, profile)
	if err != nil {
		t.Fatalf("target schema version: %v", err)
	}
	if err := metaStore.UpsertTenantAutoEmbeddingProfile(context.Background(), &meta.TenantAutoEmbeddingProfile{
		TenantID:      tenantID,
		EmbeddingMode: meta.TenantEmbeddingModeFTSOnly,
		Model:         profile.Model,
		Dimensions:    profile.Dimensions,
		OptionsJSON:   profile.OptionsJSON,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}

	srv := NewWithConfig(Config{Meta: metaStore})
	defer srv.Close()
	schemaInitCalls := 0
	// Direct invocation is blocking; production launches this function in a worker.
	srv.initTenantSchemaAsync(context.Background(), tenantID, "unused-dsn", tenant.ProviderTiDBZero, func(context.Context, string) error {
		schemaInitCalls++
		return nil
	})

	if schemaInitCalls != 1 {
		t.Fatalf("schema init calls = %d, want 1", schemaInitCalls)
	}
	updated, err := metaStore.GetTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if updated.Status != meta.TenantActive {
		t.Fatalf("status = %q, want %q", updated.Status, meta.TenantActive)
	}
	if updated.SchemaVersion != targetVersion {
		t.Fatalf("schema version = %d, want %d", updated.SchemaVersion, targetVersion)
	}
}

func TestInitTenantSchemaAsyncRecordsOrgScopedEvent(t *testing.T) {
	db := newTenantDeleteDBInfo(t)
	metaStore := db.Meta
	testtidb.ResetMetaDB(t, metaStore.DB())

	const (
		tenantID = "tenant-schema-init-event"
		orgID    = "org-schema-init-event"
	)
	now := time.Now().UTC()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           db.DBHost,
		DBPort:           db.DBPort,
		DBUser:           db.DBUser,
		DBPasswordCipher: []byte{},
		DBName:           db.DBName,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-schema-init-event",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpsertTenantTiDBCloudOrgBinding(context.Background(), &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: orgID,
		ClusterID:      "cluster-schema-init-event",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}

	srv := &Server{
		meta:        metaStore,
		pool:        db.Pool,
		provisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		metrics:     newServerMetrics(),
	}
	dsn := tenantDSN(db.DBUser, db.DBPass, db.DBHost, db.DBPort, db.DBName, false, tenant.ProviderTiDBCloudNative)
	srv.initTenantSchemaAsync(context.Background(), tenantID, dsn, tenant.ProviderTiDBCloudNative, func(context.Context, string) error {
		return nil
	})

	recorder := httptest.NewRecorder()
	srv.metrics.writePrometheus(recorder)
	want := `drive9_business_events_total{event="tenant_schema_init",provider="tidb_cloud_native",result="ok",tenant_id="tenant-schema-init-event",tidbcloud_org_id="org-schema-init-event"} 1`
	if !strings.Contains(recorder.Body.String(), want) {
		t.Errorf("missing organization-scoped schema-init event %q:\n%s", want, recorder.Body.String())
	}
}

func TestAutoEmbeddingProfileForTenantEnsuresDefaultProfile(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	srv := NewWithConfig(Config{Meta: metaStore})
	defer srv.Close()

	profile, err := srv.autoEmbeddingProfileForTenant(context.Background(), "tenant-default-profile")
	if err != nil {
		t.Fatalf("autoEmbeddingProfileForTenant: %v", err)
	}
	if profile.schemaProfile.Model != tenantschema.DefaultTiDBAutoEmbeddingModel {
		t.Fatalf("profile model = %q", profile.schemaProfile.Model)
	}
	if profile.schemaProfile.Dimensions != tenantschema.DefaultTiDBAutoEmbeddingDimensions {
		t.Fatalf("profile dimensions = %d", profile.schemaProfile.Dimensions)
	}
	if profile.schemaProfile.OptionsJSON != `{"dimensions":1024}` {
		t.Fatalf("profile options_json = %q", profile.schemaProfile.OptionsJSON)
	}

	stored, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), "tenant-default-profile")
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if stored.Model != tenantschema.DefaultTiDBAutoEmbeddingModel || stored.Dimensions != tenantschema.DefaultTiDBAutoEmbeddingDimensions {
		t.Fatalf("stored default profile = %+v", stored)
	}
}

func TestAutoEmbeddingProfileForTenantWithoutMetaUsesConfiguredDefault(t *testing.T) {
	srv := NewWithConfig(Config{
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	profile, err := srv.autoEmbeddingProfileForTenant(context.Background(), "tenant-without-meta")
	if err != nil {
		t.Fatalf("autoEmbeddingProfileForTenant: %v", err)
	}
	if profile.schemaProfile.Model != "openai/text-embedding-3-small" {
		t.Fatalf("profile model = %q", profile.schemaProfile.Model)
	}
	if profile.schemaProfile.Dimensions != 1536 {
		t.Fatalf("profile dimensions = %d", profile.schemaProfile.Dimensions)
	}
}

func TestProvisionPersistsTenantBeforeProvisionFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{
		provider:     tenant.ProviderTiDBZero,
		cluster:      &tenant.ClusterInfo{},
		provisionErr: fmt.Errorf("boom"),
	}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadGateway)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants LIMIT 1").Scan(&tenantID, &status); err != nil {
		t.Fatalf("QueryRow tenant: %v", err)
	}
	if tenantID == "" {
		t.Fatal("expected tenant row to be persisted")
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	var keyCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys").Scan(&keyCount); err != nil {
		t.Fatal(err)
	}
	if keyCount != 0 {
		t.Fatalf("api key count = %d, want 0", keyCount)
	}
}

func TestProvisionCleansPartialClusterBeforeMarkingFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative,
		cluster: &tenant.ClusterInfo{
			ClusterID: "cluster-after-takeover",
			Host:      "db.example",
			Port:      4000,
			Username:  "u1.root",
			Password:  "secret",
			DBName:    "test",
		},
		provisionErr:      fmt.Errorf("provision native cluster cluster-after-takeover: limit rejected"),
		defaultPublicKey:  "default-public",
		defaultPrivateKey: "default-private",
	}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadGateway)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow(`
		SELECT id, status
		FROM tenants LIMIT 1`,
	).Scan(&tenantID, &status); err != nil {
		t.Fatalf("QueryRow tenant: %v", err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForDeprovisionCalls(t, prov, 1)
	waitForTenantClusterReference(t, metaStore, tenantID, "")
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "cluster-after-takeover" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "default-public" || prov.lastCredentialReq.PrivateKey != "default-private" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}
}

func TestStartupResumesProvisioningTenantInit(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host := "127.0.0.1"
	port := 3306
	if parsed.Addr != "" {
		h, p, ok := strings.Cut(parsed.Addr, ":")
		if ok {
			host = h
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}

	passCipher, err := pool.Encrypt(context.Background(), []byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := token.NewID()
	now := time.Now().UTC()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	defer srv.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active after restart resume, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStartupMarksPendingTenantFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter, origSweepEvery := pendingTenantStaleAfter, pendingTenantSweepEvery
	pendingTenantStaleAfter = time.Minute
	pendingTenantSweepEvery = time.Hour
	defer func() {
		pendingTenantStaleAfter = origStaleAfter
		pendingTenantSweepEvery = origSweepEvery
	}()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	defer srv.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantFailed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending tenant did not become failed after startup resume, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStartupKeepsFreshPendingTenant(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tenantID := token.NewID()
	now := time.Now().UTC()
	origStaleAfter, origSweepEvery := pendingTenantStaleAfter, pendingTenantSweepEvery
	pendingTenantStaleAfter = time.Minute
	pendingTenantSweepEvery = time.Hour
	defer func() {
		pendingTenantStaleAfter = origStaleAfter
		pendingTenantSweepEvery = origSweepEvery
	}()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	defer srv.Close()

	time.Sleep(100 * time.Millisecond)
	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantPending) {
		t.Fatalf("fresh pending tenant status = %s, want %s", status, meta.TenantPending)
	}
}

func TestReconcilePendingDirectNativeClusterWithoutPoolOwnershipBecomesFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantFailed)
	}
}

func TestReconcilePendingNativePoolBindingWithoutConnectionStaysPending(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-pool-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpsertTenantTiDBCloudOrgBinding(context.Background(), &meta.TenantTiDBCloudOrgBinding{
		TenantID: tenantID, OrganizationID: "org-pool-1", ClusterID: "cluster-pool-1",
		PoolID: "pool-1", PoolStatus: meta.TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantPending) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantPending)
	}
}

func TestReconcilePendingReservationOnlyFreeTenantBecomesDeleted(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	if err := metaStore.SetQuotaConfigPatch(context.Background(), tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &zero}); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleted) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantDeleted)
	}
}

func TestReconcilePendingReloadsTenantAfterConcurrentEarlyBinding(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	staleAt := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	staleSnapshot := meta.Tenant{
		ID: tenantID, Status: meta.TenantPending, Kind: meta.TenantKindLive,
		DBPasswordCipher: []byte{}, DBTLS: true, Provider: tenant.ProviderTiDBCloudNative,
		SchemaVersion: 1, CreatedAt: staleAt, UpdatedAt: staleAt,
	}
	if err := metaStore.InsertTenant(context.Background(), &staleSnapshot); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	if err := metaStore.SetQuotaConfigPatch(context.Background(), tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &zero}); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.PersistTiDBCloudTenantClusterReference(context.Background(), tenantID, "org-concurrent-binding", &meta.Tenant{
		Provider: tenant.ProviderTiDBCloudNative, ClusterID: "cluster-concurrent-binding",
	}); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), staleSnapshot)

	got, err := metaStore.GetTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantPending || got.ClusterID != "cluster-concurrent-binding" {
		t.Fatalf("tenant after reconcile = status:%s cluster:%q, want pending early binding", got.Status, got.ClusterID)
	}
}

func TestReconcilePendingNativeTenantWithConnectionResumesSchemaInit(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	passCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "db.example",
		DBPort:           4000,
		DBUser:           "u1.root",
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending native tenant did not resume schema init, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if prov.systemUserCalls.Load() != 1 {
		t.Fatalf("system user calls = %d, want 1", prov.systemUserCalls.Load())
	}
}

func TestReconcilePendingTenantDoesNotOverwriteChangedStatus(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateTenantStatus(context.Background(), tenantID, meta.TenantProvisioning); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantProvisioning) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantProvisioning)
	}
}

func TestServerCloseCancelsSchemaInitRetryWorker(t *testing.T) {
	origWindow, origInitBackoff, origMaxBackoff := schemaInitRetryWindow, schemaInitInitialBackoff, schemaInitMaxBackoff
	schemaInitRetryWindow = time.Minute
	schemaInitInitialBackoff = 5 * time.Second
	schemaInitMaxBackoff = 5 * time.Second
	defer func() {
		schemaInitRetryWindow = origWindow
		schemaInitInitialBackoff = origInitBackoff
		schemaInitMaxBackoff = origMaxBackoff
	}()

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBZero,
		cluster:  &tenant.ClusterInfo{},
		initErr:  fmt.Errorf("boom"),
	}
	srv := NewWithConfig(Config{
		Provisioner: prov,
		TokenSecret: []byte("abc"),
	})

	srv.startProvisionedTenantSchemaInit(context.Background(), &provisionTenantResult{
		TenantID:  "tenant-close-test",
		TenantDSN: "user:pass@tcp(localhost:3306)/db?parseTime=true",
		Provider:  tenant.ProviderTiDBZero,
	})

	// Let the worker enter the retry backoff path before closing the server.
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		srv.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not cancel schema init retry worker promptly")
	}
}

func TestProvisionTiDBCloudNativeRejectsPartialCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{"public_key": "only-pk"})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestProvisionTiDBCloudNativeUsesDefaultCredentialsWhenOmitted(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testtidb.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:          tenant.ProviderTiDBCloudNative,
		cluster:           &tenant.ClusterInfo{ClusterID: "native-cluster-default", OrganizationID: "org-default"},
		defaultPublicKey:  "default-pk",
		defaultPrivateKey: "default-sk",
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if prov.lastCredentialReq.PublicKey != "default-pk" || prov.lastCredentialReq.PrivateKey != "default-sk" {
		t.Fatalf("credentials = %s/%s, want default-pk/default-sk", prov.lastCredentialReq.PublicKey, prov.lastCredentialReq.PrivateKey)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	binding, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get tidbcloud org binding: %v", err)
	}
	if binding.OrganizationID != "org-default" || binding.ClusterID != "native-cluster-default" {
		t.Fatalf("binding = %#v", binding)
	}
}
