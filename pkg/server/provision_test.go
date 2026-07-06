package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/internal/testmysql"
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

func (f *fakeProvisioner) ProviderType() string { return f.provider }

func (f *fakeProvisioner) ProvisioningCloudProvider() string { return f.cloudProvider }

func (f *fakeProvisioner) ProvisioningRegion() string { return f.region }

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

func (f *fakeProvisioner) ProvisionWithCredentials(_ context.Context, tenantID string, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	f.credentialCalls.Add(1)
	f.lastCredentialReq = req
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

func (f *fakeProvisioner) ProvisionWithCredentialsAndQuota(_ context.Context, tenantID string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	f.credentialQuotaCalls.Add(1)
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

type credentialOnlyProvisioner struct {
	provider string
	cluster  *tenant.ClusterInfo
}

func (f *credentialOnlyProvisioner) ProviderType() string { return f.provider }

func (f *credentialOnlyProvisioner) InitSchema(_ context.Context, _ string) error { return nil }

func (f *credentialOnlyProvisioner) Provision(_ context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

func (f *credentialOnlyProvisioner) ProvisionWithCredentials(_ context.Context, tenantID string, _ tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

type profileAwareFakeProvisioner struct {
	fakeProvisioner
	mu               sync.Mutex
	profileInitCalls atomic.Int32
	ensureDBCalls    atomic.Int32
	lastProfile      tenantschema.TiDBAutoEmbeddingProfile
	ensureDBErr      error
	lastEnsureDSN    string
	callOrder        []string
}

func (f *profileAwareFakeProvisioner) EnsureDatabase(_ context.Context, dsn string) error {
	f.ensureDBCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastEnsureDSN = dsn
	f.callOrder = append(f.callOrder, "ensure")
	return f.ensureDBErr
}

func (f *profileAwareFakeProvisioner) InitSchemaForAutoEmbeddingProfile(_ context.Context, _ string, profile tenantschema.TiDBAutoEmbeddingProfile) error {
	f.profileInitCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastProfile = profile
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
}

func TestProvisionMarksTenantFailedWhenInitKeepsFailing(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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

func TestProvisionTiDBCloudNativeCreateQuotaSkipsQuotaPatch(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

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

func TestProvisionTiDBCloudNativeCreateTimeQuotaRequiresQuotaProvisioner(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	prov := &credentialOnlyProvisioner{
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

	spendingLimit := int64(10000)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
		Quota: &quotaRequest{quotaFields: quotaFields{
			TiDBCloudSpendingLimit: &spendingLimit,
		}},
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want unsupported create-time quota error")
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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	if len(profile.APIKeyCipher) != 0 {
		t.Fatal("disabled auto-embedding profile should not store an empty API key cipher")
	}
}

func TestSchemaInitForTenantUsesAutoEmbeddingProfileWhenDatabaseAutoEmbeddingDisabled(t *testing.T) {
	prov := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBZero},
	}
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
	if prov.profileInitCalls.Load() != 1 {
		t.Fatalf("profile init calls = %d, want 1", prov.profileInitCalls.Load())
	}
	if prov.lastProfile.Model != "openai/text-embedding-3-small" || prov.lastProfile.Dimensions != 1536 {
		t.Fatalf("profile init profile = %+v", prov.lastProfile)
	}
}

func TestAutoEmbeddingProfileForTenantEnsuresDefaultProfile(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	srv := NewWithConfig(Config{Meta: metaStore})
	defer srv.Close()

	profile, err := srv.autoEmbeddingProfileForTenant(context.Background(), "tenant-default-profile")
	if err != nil {
		t.Fatalf("autoEmbeddingProfileForTenant: %v", err)
	}
	if profile.Model != tenantschema.DefaultTiDBAutoEmbeddingModel {
		t.Fatalf("profile model = %q", profile.Model)
	}
	if profile.Dimensions != tenantschema.DefaultTiDBAutoEmbeddingDimensions {
		t.Fatalf("profile dimensions = %d", profile.Dimensions)
	}
	if profile.OptionsJSON != `{"dimensions":1024}` {
		t.Fatalf("profile options_json = %q", profile.OptionsJSON)
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
	if profile.Model != "openai/text-embedding-3-small" {
		t.Fatalf("profile model = %q", profile.Model)
	}
	if profile.Dimensions != 1536 {
		t.Fatalf("profile dimensions = %d", profile.Dimensions)
	}
}

func TestProvisionPersistsTenantBeforeProvisionFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	_ = NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	_ = NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	t.Cleanup(srv.Close)

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

func TestReconcilePendingNativePoolTenantWithoutConnectionStaysPending(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	if status != string(meta.TenantPending) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantPending)
	}
}

func TestReconcilePendingNativeTenantWithConnectionResumesSchemaInit(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
	testmysql.ResetMetaDB(t, metaStore.DB())

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
