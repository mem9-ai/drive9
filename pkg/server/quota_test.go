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
	neturl "net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type quotaTestProvisioner struct {
	provider                 string
	updateErr                error
	markErr                  error
	getErr                   error
	deprovisionErr           error
	cloudCfg                 *tenant.QuotaCloudConfig
	defaultPublicKey         string
	defaultPrivateKey        string
	markHook                 func() error
	deprovisionHook          func(call int, cluster *tenant.ClusterInfo) error
	updateCalls              atomic.Int32
	markCalls                atomic.Int32
	getCalls                 atomic.Int32
	listCalls                atomic.Int32
	deprovisionCalls         atomic.Int32
	batchPoolCalls           atomic.Int32
	markPoolUsedCalls        atomic.Int32
	markPoolFreeCalls        atomic.Int32
	lastCluster              *tenant.ClusterInfo
	lastCredentials          tenant.CredentialProvisionRequest
	lastOptions              tenant.QuotaUpdateOptions
	lastListOptions          tenant.ManagedClusterListOptions
	lastDeprovision          *tenant.ClusterInfo
	listErr                  error
	listPages                []*tenant.ManagedClusterListResult
	batchPoolErr             error
	batchPoolConnectionReady bool
	batchPoolEmptyPassword   bool
	batchPoolMissingOrg      map[int]bool
	batchPoolMissingTenant   map[int]bool
	metadataWaitCalls        atomic.Int32
	metadataWaitErr          error
	calls                    []string
}

func (p *quotaTestProvisioner) ProviderType() string { return p.provider }

func (p *quotaTestProvisioner) DefaultCredentials() (tenant.CredentialProvisionRequest, bool) {
	if p.defaultPublicKey == "" || p.defaultPrivateKey == "" {
		return tenant.CredentialProvisionRequest{}, false
	}
	return tenant.CredentialProvisionRequest{
		PublicKey:  p.defaultPublicKey,
		PrivateKey: p.defaultPrivateKey,
	}, true
}

func (p *quotaTestProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, errors.New("not implemented")
}

func (p *quotaTestProvisioner) InitSchema(context.Context, string) error { return nil }

func (p *quotaTestProvisioner) EnsureSystemUser(context.Context, string, string) (string, string, error) {
	return "u.tdc_fs_sys", "pool-pass", nil
}

func (p *quotaTestProvisioner) UpdateQuota(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	p.updateCalls.Add(1)
	p.calls = append(p.calls, "update")
	p.lastCredentials = req
	p.lastOptions = opts
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
	if p.updateErr != nil {
		return nil, p.updateErr
	}
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		return &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: opts.TiDBCloudSpendingLimitMonthly}, nil
	}
	return nil, nil
}

func (p *quotaTestProvisioner) MarkQuotaUpdateStarted(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.QuotaCloudConfig, error) {
	p.markCalls.Add(1)
	p.calls = append(p.calls, "mark")
	p.lastCredentials = req
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
	if p.markHook != nil {
		if err := p.markHook(); err != nil {
			return nil, err
		}
	}
	if p.markErr != nil {
		return nil, p.markErr
	}
	return p.cloudCfg, nil
}

func (p *quotaTestProvisioner) GetQuota(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.QuotaCloudConfig, error) {
	p.getCalls.Add(1)
	p.calls = append(p.calls, "get")
	p.lastCredentials = req
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
	if p.getErr != nil {
		return nil, p.getErr
	}
	return p.cloudCfg, nil
}

func (p *quotaTestProvisioner) DeprovisionWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	call := int(p.deprovisionCalls.Add(1))
	p.calls = append(p.calls, "deprovision")
	p.lastCredentials = req
	if cluster != nil {
		out := *cluster
		p.lastDeprovision = &out
	}
	if p.deprovisionHook != nil {
		if err := p.deprovisionHook(call, cluster); err != nil {
			return err
		}
	}
	return p.deprovisionErr
}

func (p *quotaTestProvisioner) ListManagedClusters(_ context.Context, req tenant.CredentialProvisionRequest, opts tenant.ManagedClusterListOptions) (*tenant.ManagedClusterListResult, error) {
	call := int(p.listCalls.Add(1))
	p.calls = append(p.calls, "list")
	p.lastCredentials = req
	p.lastListOptions = opts
	if p.listErr != nil {
		return nil, p.listErr
	}
	if len(p.listPages) == 0 {
		return &tenant.ManagedClusterListResult{}, nil
	}
	idx := call - 1
	if idx >= len(p.listPages) {
		return &tenant.ManagedClusterListResult{}, nil
	}
	return p.listPages[idx], nil
}

func (p *quotaTestProvisioner) BatchProvisionFreeClustersWithCredentialsAndQuota(_ context.Context, tenantIDs []string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) ([]*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	p.batchPoolCalls.Add(1)
	p.calls = append(p.calls, "batch_pool")
	p.lastCredentials = req
	p.lastOptions = opts
	out := make([]*tenant.ClusterInfo, 0, len(tenantIDs))
	for i, tenantID := range tenantIDs {
		password := "pool-pass"
		if p.batchPoolEmptyPassword {
			password = ""
		}
		out = append(out, &tenant.ClusterInfo{
			TenantID:       tenantID,
			ClusterID:      fmt.Sprintf("pool-cluster-%d", i+1),
			OrganizationID: "org-1",
			Password:       password,
			DBName:         "tidbcloud_fs",
			Provider:       tenant.ProviderTiDBCloudNative,
		})
		if p.batchPoolMissingTenant[i] {
			out[len(out)-1].TenantID = ""
		}
		if p.batchPoolMissingOrg[i] {
			out[len(out)-1].OrganizationID = ""
		}
		if p.batchPoolConnectionReady {
			out[len(out)-1].Host = "db.example.com"
			out[len(out)-1].Port = 4000
			out[len(out)-1].Username = "u.root"
		}
	}
	if !p.batchPoolConnectionReady && p.batchPoolErr == nil {
		for _, cluster := range out {
			cluster.Host = "db.example.com"
			cluster.Port = 4000
			cluster.Username = "u.root"
		}
	}
	return out, nil, p.batchPoolErr
}

func (p *quotaTestProvisioner) WaitForPoolClusterMetadata(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	p.metadataWaitCalls.Add(1)
	p.calls = append(p.calls, "wait_pool_metadata")
	p.lastCredentials = req
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
	if p.metadataWaitErr != nil {
		return nil, p.metadataWaitErr
	}
	if cluster == nil {
		return nil, fmt.Errorf("cluster is required")
	}
	out := *cluster
	out.OrganizationID = "org-1"
	out.Host = "db.example.com"
	out.Port = 4000
	out.Username = "u.root"
	if out.DBName == "" {
		out.DBName = "tidbcloud_fs"
	}
	return &out, nil
}

func (p *quotaTestProvisioner) MarkClusterPoolUsed(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, _ time.Time, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	p.markPoolUsedCalls.Add(1)
	p.calls = append(p.calls, "mark_pool_used")
	p.lastCredentials = req
	p.lastOptions = opts
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
	return &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: opts.TiDBCloudSpendingLimitMonthly}, nil
}

func (p *quotaTestProvisioner) MarkClusterPoolFree(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	p.markPoolFreeCalls.Add(1)
	p.calls = append(p.calls, "mark_pool_free")
	p.lastCredentials = req
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
	return nil
}

type quotaRuntime struct {
	meta     *meta.Store
	tenantID string
	apiKey   string
	prov     *quotaTestProvisioner
	server   *Server
}

type tenantPoolNoListProvisioner struct {
	fakeProvisioner
	batchPoolCalls    atomic.Int32
	markPoolUsedCalls atomic.Int32
	markPoolFreeCalls atomic.Int32
}

func (p *tenantPoolNoListProvisioner) BatchProvisionFreeClustersWithCredentialsAndQuota(context.Context, []string, tenant.CredentialProvisionRequest, tenant.QuotaUpdateOptions) ([]*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	p.batchPoolCalls.Add(1)
	return nil, nil, nil
}

func (p *tenantPoolNoListProvisioner) MarkClusterPoolUsed(context.Context, *tenant.ClusterInfo, tenant.CredentialProvisionRequest, time.Time, tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	p.markPoolUsedCalls.Add(1)
	return nil, nil
}

func (p *tenantPoolNoListProvisioner) MarkClusterPoolFree(context.Context, *tenant.ClusterInfo, tenant.CredentialProvisionRequest) error {
	p.markPoolFreeCalls.Add(1)
	return nil
}

func newQuotaRuntime(t *testing.T, provider string) *quotaRuntime {
	t.Helper()
	db := newTenantDeleteDBInfo(t)
	testmysql.ResetMetaDB(t, db.Meta.DB())
	t.Cleanup(func() {
		testmysql.ResetMetaDB(t, db.Meta.DB())
	})

	tenantID := token.NewID()
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	apiKey, err := token.IssueToken(tokenSecret, tenantID, 1)
	if err != nil {
		t.Fatal(err)
	}
	apiKeyCipher, err := db.Pool.Encrypt(context.Background(), []byte(apiKey))
	if err != nil {
		t.Fatal(err)
	}
	dbPassCipher, err := db.Pool.Encrypt(context.Background(), []byte(db.DBPass))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.Meta.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           db.DBHost,
		DBPort:           db.DBPort,
		DBUser:           db.DBUser,
		DBPasswordCipher: dbPassCipher,
		DBName:           db.DBName,
		DBTLS:            false,
		Provider:         provider,
		ClusterID:        "cluster-quota-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Meta.InsertAPIKey(context.Background(), &meta.APIKey{
		ID:            token.NewID(),
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: apiKeyCipher,
		JWTHash:       token.HashToken(apiKey),
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		ScopeKind:     meta.APIKeyScopeKindOwner,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	prov := &quotaTestProvisioner{provider: provider}
	server := NewWithConfig(Config{Meta: db.Meta, Pool: db.Pool, Provisioner: prov, TokenSecret: tokenSecret})
	t.Cleanup(server.Close)
	return &quotaRuntime{meta: db.Meta, tenantID: tenantID, apiKey: apiKey, prov: prov, server: server}
}

func TestQuotaGetRejectsDrive9Key(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/quota", nil)
	req.Header.Set("Authorization", "Bearer "+rt.apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if got := rt.prov.getCalls.Load(); got != 0 {
		t.Fatalf("get calls = %d, want 0", got)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
}

func TestQuotaGetDoesNotFallbackToDefaultTiDBCloudCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.defaultPublicKey = "default-pk"
	rt.prov.defaultPrivateKey = "default-sk"
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "", "", "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), tenant.ErrCredentialsRequired.Error()) {
		t.Fatalf("body = %q, want missing credential error", raw)
	}
	if got := rt.prov.getCalls.Load(); got != 0 {
		t.Fatalf("get calls = %d, want 0", got)
	}
	if rt.prov.lastCredentials.PublicKey == "default-pk" || rt.prov.lastCredentials.PrivateKey == "default-sk" {
		t.Fatalf("quota get used default credentials: %#v", rt.prov.lastCredentials)
	}
}

func TestQuotaGetAllowsExplicitDefaultTiDBCloudCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.defaultPublicKey = "default-pk"
	rt.prov.defaultPrivateKey = "default-sk"
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "default-pk", "default-sk", "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := rt.prov.getCalls.Load(); got != 1 {
		t.Fatalf("get calls = %d, want 1", got)
	}
	if rt.prov.lastCredentials.PublicKey != "default-pk" || rt.prov.lastCredentials.PrivateKey != "default-sk" {
		t.Fatalf("last credentials = %#v", rt.prov.lastCredentials)
	}
}

func TestQuotaGetReturnsConfigStorageUsageAndSpendingLimit(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	spendingLimit := int64(10000)
	rt.prov.cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: &spendingLimit}
	ctx := context.Background()
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:         rt.tenantID,
		MaxStorageBytes:  123 * quotaStorageSizeBytes,
		MaxFileSizeBytes: 12 * quotaStorageSizeBytes,
		MaxFileCount:     34,
		MaxMediaLLMFiles: 56,
		MaxMonthlyCostMC: 789,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnsureQuotaUsageRow(ctx, rt.tenantID); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.SetQuotaCounters(ctx, rt.tenantID, 321, 7, 9); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.IncrReservedBytes(ctx, rt.tenantID, 11); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "public-1", "private-1", "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out quotaResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.TenantID != rt.tenantID || !out.SupportsUpdate {
		t.Fatalf("response tenant/update = %#v", out)
	}
	if out.Config.MaxStorageSize != 123 || out.Config.MaxFileSize != 12 || out.Config.MaxFileCount != 34 || out.Config.TiDBCloudSpendingLimit == nil || *out.Config.TiDBCloudSpendingLimit != spendingLimit {
		t.Fatalf("config = %#v", out.Config)
	}
	if out.Usage.StorageBytes != 321 || out.Usage.ReservedBytes != 11 || out.Usage.FileCount != 9 {
		t.Fatalf("usage = %#v", out.Usage)
	}
	if strings.Contains(string(raw), "media_file_count") || strings.Contains(string(raw), "monthly_cost_mc") {
		t.Fatalf("response should not expose media or cost counters: %s", raw)
	}
}

func TestQuotaGetUsesTiDBCloudAuthorization(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "public-1", "private-1", "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := rt.prov.getCalls.Load(); got != 1 {
		t.Fatalf("get calls = %d, want 1", got)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if rt.prov.lastCluster == nil || rt.prov.lastCluster.ClusterID != "cluster-quota-1" || rt.prov.lastCluster.TenantID != rt.tenantID {
		t.Fatalf("last cluster = %#v", rt.prov.lastCluster)
	}
	if rt.prov.lastCredentials.PublicKey != "public-1" || rt.prov.lastCredentials.PrivateKey != "private-1" {
		t.Fatalf("last credentials = %#v", rt.prov.lastCredentials)
	}
}

func TestDeprecatedQuotaGetWorksWithoutTiDBCloudOrgBinding(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "public-1", "private-1", "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 without tenant-org binding: %s", resp.StatusCode, body)
	}
	if got := rt.prov.getCalls.Load(); got != 1 {
		t.Fatalf("get calls = %d, want 1", got)
	}
	if got := rt.prov.listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0", got)
	}
}

func TestQuotaSetChecksClusterWritePermissionBeforeMaxStorageUpdate(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:         rt.tenantID,
		MaxStorageBytes:  100,
		MaxFileSizeBytes: 101,
		MaxFileCount:     102,
		MaxMediaLLMFiles: 200,
		MaxMonthlyCostMC: 300,
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.markHook = func() error {
		cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
		if err != nil {
			return err
		}
		if cfg.MaxStorageBytes != 100 {
			return fmt.Errorf("max storage bytes before label patch = %d, want old value 100", cfg.MaxStorageBytes)
		}
		return nil
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"public_key":       "public-1",
		"private_key":      "private-1",
		"max_storage_size": int64(1000),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if len(rt.prov.calls) != 1 || rt.prov.calls[0] != "mark" {
		t.Fatalf("calls = %#v, want mark only", rt.prov.calls)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 1000*quotaStorageSizeBytes || cfg.MaxFileSizeBytes != 101 || cfg.MaxFileCount != 102 || cfg.MaxMediaLLMFiles != 200 || cfg.MaxMonthlyCostMC != 300 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestDeprecatedQuotaSetWorksWithoutTiDBCloudOrgBinding(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"public_key":       "public-1",
		"private_key":      "private-1",
		"max_storage_size": int64(1000),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 without tenant-org binding: %s", resp.StatusCode, body)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if got := rt.prov.listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0", got)
	}
}

func TestQuotaSetRejectsProvisioningTenant(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	if err := rt.meta.UpdateTenantStatus(context.Background(), rt.tenantID, meta.TenantProvisioning); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"public_key":       "public-1",
		"private_key":      "private-1",
		"max_storage_size": int64(1000),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 409: %s", resp.StatusCode, body)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
}

func TestQuotaSetChecksClusterWritePermissionBeforeFileLimitUpdate(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:         rt.tenantID,
		MaxStorageBytes:  100,
		MaxFileSizeBytes: 200,
		MaxFileCount:     300,
		MaxMediaLLMFiles: 400,
		MaxMonthlyCostMC: 500,
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.markHook = func() error {
		cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
		if err != nil {
			return err
		}
		if cfg.MaxFileSizeBytes != 200 || cfg.MaxFileCount != 300 {
			return fmt.Errorf("file limits before label patch = size:%d count:%d, want old values", cfg.MaxFileSizeBytes, cfg.MaxFileCount)
		}
		return nil
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":      rt.tenantID,
		"public_key":     "public-1",
		"private_key":    "private-1",
		"max_file_size":  int64(64),
		"max_file_count": int64(42),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if len(rt.prov.calls) != 1 || rt.prov.calls[0] != "mark" {
		t.Fatalf("calls = %#v, want mark only", rt.prov.calls)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 100 || cfg.MaxFileSizeBytes != 64*quotaStorageSizeBytes || cfg.MaxFileCount != 42 || cfg.MaxMediaLLMFiles != 400 || cfg.MaxMonthlyCostMC != 500 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestQuotaSetSpendingLimitOnlyDoesNotWriteStorageConfig(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	spendingLimit := int64(0)
	rt.prov.cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: &spendingLimit}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":                rt.tenantID,
		"public_key":               "public-1",
		"private_key":              "private-1",
		"tidbcloud_spending_limit": spendingLimit,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 1 {
		t.Fatalf("update calls = %d, want 1", got)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if len(rt.prov.calls) != 2 || rt.prov.calls[0] != "mark" || rt.prov.calls[1] != "update" {
		t.Fatalf("calls = %#v, want mark before update", rt.prov.calls)
	}
	if rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly == nil || *rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly != spendingLimit {
		t.Fatalf("spending limit option = %#v, want %d", rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly, spendingLimit)
	}
	version, err := rt.meta.GetQuotaConfigVersion(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		t.Fatalf("quota config version = %q, want empty", version)
	}
	var out quotaResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Config.TiDBCloudSpendingLimit == nil || *out.Config.TiDBCloudSpendingLimit != spendingLimit {
		t.Fatalf("config = %#v", out.Config)
	}
}

func TestQuotaSetRejectsDrive9KeyWithoutTiDBCloudCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"max_storage_size": int64(1000),
	}, rt.apiKey)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
	version, err := rt.meta.GetQuotaConfigVersion(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		t.Fatalf("quota config version = %q, want empty", version)
	}
}

func TestQuotaSetDoesNotFallbackToDefaultTiDBCloudCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.defaultPublicKey = "default-pk"
	rt.prov.defaultPrivateKey = "default-sk"
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"max_storage_size": int64(1000),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), tenant.ErrCredentialsRequired.Error()) {
		t.Fatalf("body = %q, want missing credential error", raw)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
	if rt.prov.lastCredentials.PublicKey == "default-pk" || rt.prov.lastCredentials.PrivateKey == "default-sk" {
		t.Fatalf("quota set used default credentials: %#v", rt.prov.lastCredentials)
	}
}

func TestQuotaSetAllowsExplicitDefaultTiDBCloudCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.defaultPublicKey = "default-pk"
	rt.prov.defaultPrivateKey = "default-sk"
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"public_key":       "default-pk",
		"private_key":      "default-sk",
		"max_storage_size": int64(1000),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if len(rt.prov.calls) != 1 || rt.prov.calls[0] != "mark" {
		t.Fatalf("calls = %#v, want mark only", rt.prov.calls)
	}
	if rt.prov.lastCredentials.PublicKey != "default-pk" || rt.prov.lastCredentials.PrivateKey != "default-sk" {
		t.Fatalf("last credentials = %#v", rt.prov.lastCredentials)
	}
}

func TestQuotaSetRejectsMissingQuotaKnobs(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":   rt.tenantID,
		"public_key":  "public-1",
		"private_key": "private-1",
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
}

func TestQuotaSetRejectsInvalidQuotaValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		field   string
		value   int64
		wantErr string
	}{
		{name: "zero_storage_size", field: "max_storage_size", value: 0, wantErr: "max_storage_size must be positive"},
		{name: "negative_storage_size", field: "max_storage_size", value: -1, wantErr: "max_storage_size must be positive"},
		{name: "zero_file_size", field: "max_file_size", value: 0, wantErr: "max_file_size must be positive"},
		{name: "negative_file_size", field: "max_file_size", value: -1, wantErr: "max_file_size must be positive"},
		{name: "file_size_above_server_max", field: "max_file_size", value: 10241, wantErr: "max_file_size must be less than or equal to server max upload size"},
		{name: "negative_file_count", field: "max_file_count", value: -1, wantErr: "max_file_count must be non-negative"},
		{name: "negative_spending_limit", field: "tidbcloud_spending_limit", value: -1, wantErr: "tidbcloud_spending_limit must be non-negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
			ts := httptest.NewServer(rt.server)
			defer ts.Close()

			body := map[string]any{
				"tenant_id":   rt.tenantID,
				"public_key":  "public-1",
				"private_key": "private-1",
				tc.field:      tc.value,
			}
			resp := postJSON(t, ts.URL+"/v1/quota", body, "")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), tc.wantErr) {
				t.Fatalf("body = %q, want %q", raw, tc.wantErr)
			}
			if got := rt.prov.updateCalls.Load(); got != 0 {
				t.Fatalf("update calls = %d, want 0", got)
			}
			if got := rt.prov.markCalls.Load(); got != 0 {
				t.Fatalf("mark calls = %d, want 0", got)
			}
		})
	}
}

func TestQuotaSetRejectsNonCloudNativeTenant(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudStarter)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"public_key":       "public-1",
		"private_key":      "private-1",
		"max_storage_size": int64(1000),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
}

func TestQuotaGetMapsTiDBCloudCredentialErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "forbidden", err: tenant.ErrQuotaPermissionDenied, wantStatus: http.StatusForbidden},
		{name: "not_found", err: tenant.ErrQuotaBackendNotFound, wantStatus: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
			rt.prov.getErr = tc.err
			ts := httptest.NewServer(rt.server)
			defer ts.Close()

			resp := getQuota(t, ts.URL, rt.tenantID, "public-1", "private-1", "")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if got := rt.prov.getCalls.Load(); got != 1 {
				t.Fatalf("get calls = %d, want 1", got)
			}
		})
	}
}

func TestQuotaSetMapsTiDBCloudCredentialErrorsWithoutWritingConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "forbidden", err: tenant.ErrQuotaPermissionDenied, wantStatus: http.StatusForbidden},
		{name: "not_found", err: tenant.ErrQuotaBackendNotFound, wantStatus: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
			rt.prov.markErr = tc.err
			ts := httptest.NewServer(rt.server)
			defer ts.Close()

			resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
				"tenant_id":        rt.tenantID,
				"public_key":       "public-1",
				"private_key":      "private-1",
				"max_storage_size": int64(1000),
			}, "")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			version, err := rt.meta.GetQuotaConfigVersion(context.Background(), rt.tenantID)
			if err != nil {
				t.Fatal(err)
			}
			if version != "" {
				t.Fatalf("quota config version = %q, want empty", version)
			}
			if got := rt.prov.markCalls.Load(); got != 1 {
				t.Fatalf("mark calls = %d, want 1", got)
			}
			if got := rt.prov.updateCalls.Load(); got != 0 {
				t.Fatalf("update calls = %d, want 0", got)
			}
		})
	}
}

func TestQuotaSetHidesGenericTiDBCloudQuotaError(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.markErr = errors.New("upstream leaked detail")
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id":        rt.tenantID,
		"public_key":       "public-1",
		"private_key":      "private-1",
		"max_storage_size": int64(1000),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "tidbcloud quota update failed") {
		t.Fatalf("body = %q, want stable quota failure message", raw)
	}
	if strings.Contains(string(raw), "upstream leaked detail") {
		t.Fatalf("body leaked upstream detail: %q", raw)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
}

func TestAdminTenantListReturnsEmptyWithoutDBLookupWhenNoManagedClusters(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	var out adminTenantListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Tenants) != 0 {
		t.Fatalf("tenants = %#v, want empty", out.Tenants)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
	if rt.prov.lastListOptions.ClusterID != "" {
		t.Fatalf("cluster filter = %q, want empty", rt.prov.lastListOptions.ClusterID)
	}
}

func TestAdminTenantListFiltersByAuthorizedClusters(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-allowed",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	otherTenantID := "tenant-other-cluster"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               otherTenantID,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_other",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-denied",
		SchemaVersion:    1,
		CreatedAt:        now.Add(time.Second),
		UpdatedAt:        now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       otherTenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-denied",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	freeTenantID := "tenant-free-pool"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               freeTenantID,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_free",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-free",
		SchemaVersion:    1,
		CreatedAt:        now.Add(2 * time.Second),
		UpdatedAt:        now.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       freeTenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-free",
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-allowed",
			OrganizationID: "org-1",
		}, {
			ClusterID:      "cluster-free",
			OrganizationID: "org-1",
		}},
	}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants?page_size=10&page=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	var out adminTenantListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Tenants) != 1 || out.Tenants[0].TenantID != rt.tenantID {
		t.Fatalf("tenants = %#v, want only authorized tenant %s", out.Tenants, rt.tenantID)
	}
}

func TestAdminTenantGetHidesFreePoolTenant(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	now := time.Now().UTC()
	freeTenantID := "tenant-free-pool"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               freeTenantID,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_free",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-free",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       freeTenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-free",
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-free",
			OrganizationID: "org-1",
		}},
	}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+freeTenantID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 404: %s", resp.StatusCode, body)
	}
	if got := rt.prov.listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0 because free tenant is rejected before cloud lookup", got)
	}
}

func TestAdminTenantGetWithoutOrgBindingReturnsNotFound(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 404 for missing binding: %s", resp.StatusCode, body)
	}
	if got := rt.prov.listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0", got)
	}
}

func TestAdminTenantGetRejectsUnauthorizedCluster(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-quota-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-other",
			OrganizationID: "org-1",
		}},
	}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 403 for unauthorized cluster: %s", resp.StatusCode, body)
	}
}

func TestAdminTenantGetUsesListClusterAuthorizationAndReturnsQuota(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-quota-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:         rt.tenantID,
		MaxStorageBytes:  100 * quotaStorageSizeBytes,
		MaxFileSizeBytes: 10 * quotaStorageSizeBytes,
		MaxFileCount:     7,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnsureQuotaUsageRow(ctx, rt.tenantID); err != nil {
		t.Fatal(err)
	}
	spendingLimit := int64(12345)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:                     "cluster-quota-1",
			OrganizationID:                "org-1",
			TiDBCloudSpendingLimitMonthly: &spendingLimit,
		}},
	}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var rawOut map[string]any
	if err := json.Unmarshal(raw, &rawOut); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"provider", "cluster_id", "organization_id", "created_at", "updated_at"} {
		if _, ok := rawOut[field]; ok {
			t.Fatalf("tenant response exposed internal field %q: %s", field, raw)
		}
	}
	var out adminTenantResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.TenantID != rt.tenantID || out.Status != string(meta.TenantActive) || out.Kind != string(meta.TenantKindLive) {
		t.Fatalf("tenant response = %#v", out)
	}
	if out.Quota == nil || out.Quota.Config.MaxStorageSize != 100 || out.Quota.Config.MaxFileSize != 10 || out.Quota.Config.MaxFileCount != 7 {
		t.Fatalf("quota = %#v, want config in response", out.Quota)
	}
	if out.Quota.Config.TiDBCloudSpendingLimit == nil || *out.Quota.Config.TiDBCloudSpendingLimit != spendingLimit {
		t.Fatalf("spending limit = %#v, want %d", out.Quota.Config.TiDBCloudSpendingLimit, spendingLimit)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
	if rt.prov.lastListOptions.ClusterID != "cluster-quota-1" {
		t.Fatalf("cluster filter = %q, want cluster-quota-1", rt.prov.lastListOptions.ClusterID)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
}

func TestAdminTenantQuotaGetIsNotExposed(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID+"/quota", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 405: %s", resp.StatusCode, body)
	}
	if got := rt.prov.listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0", got)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
}

func TestAdminTenantPoolCreateBatchProvisionsFreeTenants(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   2,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var out adminTenantPoolResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 2 {
		t.Fatalf("pool response = %#v", out)
	}
	if rt.prov.batchPoolCalls.Load() != 1 {
		t.Fatalf("batch pool calls = %d, want 1", rt.prov.batchPoolCalls.Load())
	}
	if rt.prov.lastOptions.TenantPoolID != out.PoolID {
		t.Fatalf("batch pool id option = %q, want %q", rt.prov.lastOptions.TenantPoolID, out.PoolID)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.PoolID != out.PoolID || pool.Size != 2 {
		t.Fatalf("stored pool = %#v", pool)
	}
	free, err := rt.meta.CountFreeTenantPoolBindings(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("count free: %v", err)
	}
	if free != 2 {
		t.Fatalf("free = %d, want 2", free)
	}
}

func TestAdminTenantPoolCreatePersistsClustersWhenMetadataWaitFails(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	rt.prov.batchPoolErr = errors.New("tidbcloud native cluster get status 429")
	rt.prov.metadataWaitErr = errors.New("metadata still unavailable")
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   2,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var out adminTenantPoolResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 2 {
		t.Fatalf("pool response = %#v", out)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.PoolID != out.PoolID {
		t.Fatalf("stored pool id = %q, want %q", pool.PoolID, out.PoolID)
	}
	rows, err := rt.meta.ListFreeTenantPoolBindings(context.Background(), "org-1", false, 10)
	if err != nil {
		t.Fatalf("list free bindings: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("free bindings = %d, want 2", len(rows))
	}
	for _, row := range rows {
		if row.Tenant.Status != meta.TenantProvisioning {
			t.Fatalf("tenant %s status = %s, want provisioning", row.Tenant.ID, row.Tenant.Status)
		}
		if row.Tenant.DBUser != "" || row.Tenant.DBHost != "" {
			t.Fatalf("tenant %s connection = host %q user %q, want incomplete", row.Tenant.ID, row.Tenant.DBHost, row.Tenant.DBUser)
		}
		if row.Tenant.ClusterID == "" || len(row.Tenant.DBPasswordCipher) == 0 {
			t.Fatalf("tenant %s cluster/password not persisted: %#v", row.Tenant.ID, row.Tenant)
		}
		if row.Binding.PoolStatus != meta.TenantPoolBindingFree {
			t.Fatalf("tenant %s pool status = %s, want free", row.Tenant.ID, row.Binding.PoolStatus)
		}
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for rt.prov.metadataWaitCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rt.prov.metadataWaitCalls.Load(); got < 2 {
		t.Fatalf("metadata wait calls = %d, want at least 2", got)
	}
}

func TestAdminTenantPoolCreatePreservesPersistedClustersWhenOneOrganizationMissing(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	rt.prov.batchPoolErr = errors.New("tidbcloud native cluster get status 429")
	rt.prov.batchPoolMissingOrg = map[int]bool{1: true}
	rt.prov.metadataWaitErr = errors.New("metadata still unavailable")
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   2,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var out adminTenantPoolResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 1 {
		t.Fatalf("pool response = %#v", out)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.PoolID != out.PoolID {
		t.Fatalf("stored pool id = %q, want %q", pool.PoolID, out.PoolID)
	}
	rows, err := rt.meta.ListFreeTenantPoolBindings(context.Background(), "org-1", false, 10)
	if err != nil {
		t.Fatalf("list free bindings: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("free bindings = %d, want 1", len(rows))
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	if rt.prov.lastDeprovision == nil || rt.prov.lastDeprovision.ClusterID != "pool-cluster-2" {
		t.Fatalf("last deprovision = %#v, want pool-cluster-2", rt.prov.lastDeprovision)
	}
}

func TestAdminTenantPoolCreatePreservesPersistedClustersWhenOneTenantLabelMissing(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	rt.prov.batchPoolErr = errors.New("tidbcloud native cluster get status 429")
	rt.prov.batchPoolMissingTenant = map[int]bool{1: true}
	rt.prov.metadataWaitErr = errors.New("metadata still unavailable")
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   2,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var out adminTenantPoolResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 1 {
		t.Fatalf("pool response = %#v", out)
	}
	rows, err := rt.meta.ListFreeTenantPoolBindings(context.Background(), "org-1", false, 10)
	if err != nil {
		t.Fatalf("list free bindings: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("free bindings = %d, want 1", len(rows))
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	if rt.prov.lastDeprovision == nil || rt.prov.lastDeprovision.ClusterID != "pool-cluster-2" {
		t.Fatalf("last deprovision = %#v, want pool-cluster-2", rt.prov.lastDeprovision)
	}
}

func TestAdminTenantPoolCreateCleansUpWhenPartialMetadataPersistenceFails(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	rt.prov.batchPoolErr = errors.New("tidbcloud native cluster get status 429")
	rt.prov.batchPoolEmptyPassword = true
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   2,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502: %s", resp.StatusCode, body)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 2 {
		t.Fatalf("deprovision calls = %d, want 2", got)
	}
	if _, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("pool lookup err = %v, want not found", err)
	}
}

func TestResumeProvisioningNativeWithoutConnectionUsesDefaultCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.defaultPublicKey = "default-public"
	rt.prov.defaultPrivateKey = "default-private"
	ctx := context.Background()
	now := time.Now().UTC()
	tenantID := "pool-resume-native"
	cipherPass, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		Kind:             meta.TenantKindLive,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: cipherPass,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "pool-cluster-resume",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	rt.server.resumeProvisioningTenantsWithCtx(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		tnt, err := rt.meta.GetTenant(ctx, tenantID)
		if err != nil {
			t.Fatal(err)
		}
		if tnt.Status == meta.TenantActive {
			if tnt.DBHost != "db.example.com" || tnt.DBUser != "u.tdc_fs_sys" {
				t.Fatalf("tenant connection = host %q user %q", tnt.DBHost, tnt.DBUser)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant status = %s, want active", tnt.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := rt.prov.metadataWaitCalls.Load(); got != 1 {
		t.Fatalf("metadata wait calls = %d, want 1", got)
	}
	if rt.prov.lastCredentials.PublicKey != "default-public" || rt.prov.lastCredentials.PrivateKey != "default-private" {
		t.Fatalf("credentials = %#v, want default credentials", rt.prov.lastCredentials)
	}
}

func TestAdminTenantPoolCreateRejectsAboveMaxSize(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.server.tenantPoolMaxSize = 2
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   3,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400: %s", resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "pool_size must be less than or equal to 2") {
		t.Fatalf("body = %s", body)
	}
	if got := rt.prov.listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0", got)
	}
	if got := rt.prov.batchPoolCalls.Load(); got != 0 {
		t.Fatalf("batch pool calls = %d, want 0", got)
	}
}

func TestAdminTenantPoolUpdateRejectsAboveMaxSize(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.server.tenantPoolMaxSize = 2
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := patchJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   3,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400: %s", resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "pool_size must be less than or equal to 2") {
		t.Fatalf("body = %s", body)
	}
	if got := rt.prov.listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0", got)
	}
}

func TestAdminTenantPoolCreateCleansUpWhenOrganizationBackfillConflicts(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "existing-pool",
		OrganizationID: "org-1",
		Size:           1,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert existing pool: %v", err)
	}

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   2,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 409: %s", resp.StatusCode, body)
	}
	if got := rt.prov.batchPoolCalls.Load(); got != 1 {
		t.Fatalf("batch pool calls = %d, want 1", got)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 2 {
		t.Fatalf("deprovision calls = %d, want 2", got)
	}
	free, err := rt.meta.CountFreeTenantPoolBindings(ctx, "org-1")
	if err != nil {
		t.Fatalf("count free: %v", err)
	}
	if free != 0 {
		t.Fatalf("free = %d, want 0 after cleanup", free)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(ctx, "org-1")
	if err != nil {
		t.Fatalf("get existing pool: %v", err)
	}
	if pool.PoolID != "existing-pool" {
		t.Fatalf("pool id = %q, want existing-pool", pool.PoolID)
	}
}

func TestAdminTenantPoolReplenishSkipsDeletedOrShrunkPool(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	now := time.Now().UTC()
	stalePool := &meta.TenantPool{
		PoolID:         "pool-stale",
		OrganizationID: "org-1",
		Size:           2,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := rt.meta.CreateTenantPool(ctx, stalePool); err != nil {
		t.Fatalf("upsert pool: %v", err)
	}
	if err := rt.meta.UpdateTenantPoolSize(ctx, stalePool.PoolID, 0); err != nil {
		t.Fatalf("shrink pool: %v", err)
	}

	rt.server.replenishTenantPoolAsync(ctx, stalePool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	if got := rt.prov.batchPoolCalls.Load(); got != 0 {
		t.Fatalf("batch pool calls = %d, want 0", got)
	}
}

func TestAdminTenantPoolUpdateRequiresPoolSize(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-1",
			OrganizationID: "org-1",
		}},
	}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           2,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create pool: %v", err)
	}

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	resp := patchJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
	}, "")
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "pool_size is required") {
		t.Fatalf("body = %s, want pool_size validation error", body)
	}
	if got := rt.prov.batchPoolCalls.Load(); got != 0 {
		t.Fatalf("batch pool calls = %d, want 0", got)
	}
	pool, err := rt.meta.GetTenantPoolByID(ctx, "pool-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.Size != 2 {
		t.Fatalf("pool size = %d, want 2", pool.Size)
	}
}

func TestAdminTenantPoolUpdateShrinkPartialFailureReconcilesSize(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-free-1",
			OrganizationID: "org-1",
		}},
	}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           3,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	for i := 1; i <= 3; i++ {
		tenantID := fmt.Sprintf("pool-tenant-shrink-%d", i)
		clusterID := fmt.Sprintf("cluster-free-%d", i)
		createdAt := now.Add(time.Duration(i) * time.Minute)
		if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
			ID:               tenantID,
			Status:           meta.TenantActive,
			DBHost:           "db.example.com",
			DBPort:           4000,
			DBUser:           "u.root",
			DBPasswordCipher: []byte("cipher"),
			DBName:           "tidbcloud_fs",
			DBTLS:            true,
			Provider:         tenant.ProviderTiDBCloudNative,
			ClusterID:        clusterID,
			SchemaVersion:    1,
			CreatedAt:        createdAt,
			UpdatedAt:        createdAt,
		}); err != nil {
			t.Fatalf("insert tenant %s: %v", tenantID, err)
		}
		if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
			TenantID:       tenantID,
			OrganizationID: "org-1",
			ClusterID:      clusterID,
			PoolID:         "pool-1",
			PoolStatus:     meta.TenantPoolBindingFree,
			CreatedAt:      createdAt,
			UpdatedAt:      createdAt,
		}); err != nil {
			t.Fatalf("upsert binding %s: %v", tenantID, err)
		}
	}
	rt.prov.deprovisionHook = func(call int, _ *tenant.ClusterInfo) error {
		if call == 2 {
			return fmt.Errorf("tidbcloud deprovision failed")
		}
		return nil
	}

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	resp := patchJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   1,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502 body=%s", resp.StatusCode, body)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 2 {
		t.Fatalf("deprovision calls = %d, want 2", got)
	}
	pool, err := rt.meta.GetTenantPoolByID(ctx, "pool-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.Size != 2 {
		t.Fatalf("pool size = %d, want reconciled actual free size 2", pool.Size)
	}
	free, err := rt.meta.CountFreeTenantPoolBindings(ctx, "org-1")
	if err != nil {
		t.Fatalf("count free: %v", err)
	}
	if free != 2 {
		t.Fatalf("free size = %d, want 2", free)
	}
	deleted, err := rt.meta.GetTenant(ctx, "pool-tenant-shrink-3")
	if err != nil {
		t.Fatalf("get deleted tenant: %v", err)
	}
	if deleted.Status != meta.TenantDeleted {
		t.Fatalf("newest tenant status = %s, want %s", deleted.Status, meta.TenantDeleted)
	}
	reverted, err := rt.meta.GetTenant(ctx, "pool-tenant-shrink-2")
	if err != nil {
		t.Fatalf("get reverted tenant: %v", err)
	}
	if reverted.Status != meta.TenantActive {
		t.Fatalf("failed tenant status = %s, want %s", reverted.Status, meta.TenantActive)
	}
}

func TestAdminTenantPoolReplenishUsesDefaultSpendingLimit(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           1,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := rt.meta.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}

	rt.server.replenishTenantPoolAsync(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	if got := rt.prov.batchPoolCalls.Load(); got != 1 {
		t.Fatalf("batch pool calls = %d, want 1", got)
	}
	if rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly != nil {
		t.Fatalf("replenish spending limit = %#v, want nil", rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly)
	}
}

func TestAdminTenantCreateDoesNotIssueAPIKeyWhenPoolPasswordDecryptFails(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-free-1",
			OrganizationID: "org-1",
		}},
	}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           1,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert pool: %v", err)
	}
	tenantID := "pool-tenant-bad-pass"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: []byte("not-valid-ciphertext"),
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-free-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-free-1",
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	resp := postJSON(t, ts.URL+"/v1/admin/tenants", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502: %s", resp.StatusCode, body)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, tenantID)
	if err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if binding.PoolStatus != meta.TenantPoolBindingFree || binding.UsedAt != nil {
		t.Fatalf("binding = %#v, want released free", binding)
	}
	var activeKeys int
	if err := rt.meta.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?`, tenantID, meta.APIKeyActive).Scan(&activeKeys); err != nil {
		t.Fatalf("count active api keys: %v", err)
	}
	if activeKeys != 0 {
		t.Fatalf("active api keys = %d, want 0", activeKeys)
	}
}

func TestAdminTenantCreateClaimsFreePoolTenant(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-free-1",
			OrganizationID: "org-1",
		}},
	}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           0,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert pool: %v", err)
	}
	passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	tenantID := "pool-tenant-1"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-free-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-free-1",
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	resp := postJSON(t, ts.URL+"/v1/admin/tenants", map[string]any{
		"public_key":               "public-1",
		"private_key":              "private-1",
		"tidbcloud_spending_limit": 123,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var out adminTenantCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.TenantID != tenantID || out.APIKey == "" || out.Status != string(meta.TenantActive) {
		t.Fatalf("create response = %#v", out)
	}
	if rt.prov.markPoolUsedCalls.Load() != 1 {
		t.Fatalf("mark pool used calls = %d, want 1", rt.prov.markPoolUsedCalls.Load())
	}
	if rt.prov.lastCluster == nil || rt.prov.lastCluster.ClusterID != "cluster-free-1" {
		t.Fatalf("last cluster = %#v", rt.prov.lastCluster)
	}
	if rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly == nil || *rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly != 123 {
		t.Fatalf("last spending limit = %#v", rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, tenantID)
	if err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if binding.PoolStatus != meta.TenantPoolBindingUsed || binding.UsedAt == nil {
		t.Fatalf("binding = %#v, want used with used_at", binding)
	}
	resolved, err := rt.meta.ResolveByAPIKeyHash(ctx, token.HashToken(out.APIKey))
	if err != nil {
		t.Fatalf("resolve issued api key: %v", err)
	}
	if resolved.Tenant.ID != tenantID {
		t.Fatalf("resolved tenant = %q, want %q", resolved.Tenant.ID, tenantID)
	}
}

func TestProvisionClaimsFreePoolTenant(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-free-1",
			OrganizationID: "org-1",
		}},
	}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           0,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert pool: %v", err)
	}
	passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	tenantID := "pool-tenant-provision-1"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-free-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-free-1",
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	maxStorageSize := int64(100)
	resp := postJSON(t, ts.URL+"/v1/provision", map[string]any{
		"public_key":               "public-1",
		"private_key":              "private-1",
		"max_storage_size":         maxStorageSize,
		"tidbcloud_spending_limit": 123,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out["tenant_id"] != tenantID || out["api_key"] == "" || out["status"] != string(meta.TenantActive) {
		t.Errorf("provision response = %#v", out)
	}
	if got := rt.prov.markPoolUsedCalls.Load(); got != 1 {
		t.Errorf("mark pool used calls = %d, want 1", got)
	}
	if rt.prov.batchPoolCalls.Load() != 0 {
		t.Errorf("batch pool calls = %d, want 0", rt.prov.batchPoolCalls.Load())
	}
	if rt.prov.lastCluster == nil || rt.prov.lastCluster.ClusterID != "cluster-free-1" {
		t.Errorf("last cluster = %#v", rt.prov.lastCluster)
	}
	if rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly == nil || *rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly != 123 {
		t.Errorf("last spending limit = %#v", rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly)
	}
	quotaCfg, err := rt.meta.GetQuotaConfig(ctx, tenantID)
	if err != nil {
		t.Fatalf("get quota config: %v", err)
	}
	if quotaCfg.MaxStorageBytes != maxStorageSize*quotaStorageSizeBytes {
		t.Errorf("quota max storage = %d, want %d", quotaCfg.MaxStorageBytes, maxStorageSize*quotaStorageSizeBytes)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, tenantID)
	if err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if binding.PoolStatus != meta.TenantPoolBindingUsed || binding.UsedAt == nil {
		t.Fatalf("binding = %#v, want used with used_at", binding)
	}
	resolved, err := rt.meta.ResolveByAPIKeyHash(ctx, token.HashToken(out["api_key"]))
	if err != nil {
		t.Fatalf("resolve issued api key: %v", err)
	}
	if resolved.Tenant.ID != tenantID {
		t.Fatalf("resolved tenant = %q, want %q", resolved.Tenant.ID, tenantID)
	}
}

func TestProvisionFallsBackWhenTenantPoolClaimCannotListManagedClusters(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	prov := &tenantPoolNoListProvisioner{
		fakeProvisioner: fakeProvisioner{
			provider:      tenant.ProviderTiDBCloudNative,
			cloudProvider: "aws",
			region:        "us-east-1",
			cluster: &tenant.ClusterInfo{
				ClusterID:      "native-cluster-fallback",
				OrganizationID: "org-1",
				Host:           "db.example.com",
				Port:           4000,
				Username:       "u.root",
				Password:       "db-pass",
				DBName:         "tidbcloud_fs",
			},
		},
	}
	rt.server.provisioner = prov

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	resp := postJSON(t, ts.URL+"/v1/provision", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" || out["status"] != string(meta.TenantProvisioning) {
		t.Errorf("provision response = %#v", out)
	}
	if got := prov.credentialCalls.Load(); got != 1 {
		t.Errorf("credential provision calls = %d, want 1", got)
	}
	if got := prov.markPoolUsedCalls.Load(); got != 0 {
		t.Errorf("mark pool used calls = %d, want 0", got)
	}
	if got := prov.batchPoolCalls.Load(); got != 0 {
		t.Errorf("batch pool calls = %d, want 0", got)
	}
}

func TestAdminTenantQuotaSetRequiresPatchLabelAuthorization(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-quota-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:         rt.tenantID,
		MaxStorageBytes:  50 * quotaStorageSizeBytes,
		MaxFileSizeBytes: 5 * quotaStorageSizeBytes,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnsureQuotaUsageRow(ctx, rt.tenantID); err != nil {
		t.Fatal(err)
	}
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-quota-1",
			OrganizationID: "org-1",
		}},
	}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/admin/tenants/"+rt.tenantID+"/quota", map[string]any{
		"public_key":       "public-1",
		"private_key":      "private-1",
		"max_storage_size": int64(200),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var rawOut map[string]any
	if err := json.Unmarshal(raw, &rawOut); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"provider", "supports_update"} {
		if _, ok := rawOut[field]; ok {
			t.Fatalf("admin quota response exposed field %q: %s", field, raw)
		}
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 200*quotaStorageSizeBytes {
		t.Fatalf("max storage bytes = %d, want %d", cfg.MaxStorageBytes, 200*quotaStorageSizeBytes)
	}
	if len(rt.prov.calls) < 2 || rt.prov.calls[0] != "list" || rt.prov.calls[1] != "mark" {
		t.Fatalf("calls = %#v, want list then mark", rt.prov.calls)
	}
}

func TestAdminTenantQuotaSetRejectsProvisioningTenant(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpdateTenantStatus(ctx, rt.tenantID, meta.TenantProvisioning); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-quota-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-quota-1",
			OrganizationID: "org-1",
		}},
	}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/admin/tenants/"+rt.tenantID+"/quota", map[string]any{
		"public_key":       "public-1",
		"private_key":      "private-1",
		"max_storage_size": int64(200),
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 409: %s", resp.StatusCode, body)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}
	if got := rt.prov.updateCalls.Load(); got != 0 {
		t.Fatalf("update calls = %d, want 0", got)
	}
}

func TestAdminTenantDeleteRetryWithCleanupJobSkipsPatchAfterClusterGone(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpdateTenantStatus(ctx, rt.tenantID, meta.TenantDeleting); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-quota-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnqueueTenantDeleteJob(ctx, &meta.TenantDeleteJob{
		TenantID:    rt.tenantID,
		NamespaceID: rt.tenantID,
		Backend:     "local",
		Prefix:      rt.tenantID + "/",
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.markErr = tenant.ErrQuotaBackendNotFound
	rt.prov.listPages = []*tenant.ManagedClusterListResult{
		{},
		{Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-other-in-org",
			OrganizationID: "org-1",
		}}},
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/admin/tenants/"+rt.tenantID, strings.NewReader(`{"public_key":"public-1","private_key":"private-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 202: %s", resp.StatusCode, body)
	}
	if got := rt.prov.listCalls.Load(); got != 2 {
		t.Fatalf("list calls = %d, want cluster lookup then org fallback", got)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0 for already-enqueued delete retry", got)
	}
}

func TestAdminTenantDeleteRemovesTiDBCloudOrgBinding(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-quota-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	rt.prov.listPages = []*tenant.ManagedClusterListResult{
		{Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-quota-1",
			OrganizationID: "org-1",
		}}},
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/admin/tenants/"+rt.tenantID, strings.NewReader(`{"public_key":"public-1","private_key":"private-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 202: %s", resp.StatusCode, body)
	}
	if got := rt.prov.markCalls.Load(); got != 1 {
		t.Fatalf("mark calls = %d, want 1", got)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	if rt.prov.lastDeprovision == nil || rt.prov.lastDeprovision.ClusterID != "cluster-quota-1" {
		t.Fatalf("deprovision cluster = %#v", rt.prov.lastDeprovision)
	}
	got, err := rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleted)
	}
	if _, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, rt.tenantID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("binding err = %v, want %v", err, meta.ErrNotFound)
	}
}

func TestAdminPaginationRejectsOffsetOverflow(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/tenants?page_size=100&page=9223372036854775807", nil)
	_, _, _, err := adminPagination(req)
	if err == nil {
		t.Fatal("adminPagination error = nil, want page overflow error")
	}
	if !strings.Contains(err.Error(), "page is too large") {
		t.Fatalf("error = %q", err)
	}
}

func postJSON(t *testing.T, url string, body map[string]any, apiKey string) *http.Response {
	t.Helper()
	return sendJSON(t, http.MethodPost, url, body, apiKey)
}

func patchJSON(t *testing.T, url string, body map[string]any, apiKey string) *http.Response {
	t.Helper()
	return sendJSON(t, http.MethodPatch, url, body, apiKey)
}

func sendJSON(t *testing.T, method, url string, body map[string]any, apiKey string) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func getQuota(t *testing.T, baseURL, tenantID, publicKey, privateKey, apiKey string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/quota?tenant_id="+neturl.QueryEscape(tenantID), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, publicKey)
	req.Header.Set(quotaPrivateKeyHeader, privateKey)
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
