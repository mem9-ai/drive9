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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type quotaTestProvisioner struct {
	provider                    string
	updateErr                   error
	markErr                     error
	getErr                      error
	deprovisionErr              error
	cloudCfg                    *tenant.QuotaCloudConfig
	batchPoolCloudCfg           *tenant.QuotaCloudConfig
	defaultPublicKey            string
	defaultPrivateKey           string
	markHook                    func() error
	deprovisionHook             func(call int, cluster *tenant.ClusterInfo) error
	updateCalls                 atomic.Int32
	markCalls                   atomic.Int32
	getCalls                    atomic.Int32
	listCalls                   atomic.Int32
	deprovisionCalls            atomic.Int32
	batchPoolCalls              atomic.Int32
	markPoolUsedCalls           atomic.Int32
	markPoolFreeCalls           atomic.Int32
	ensureSystemUserCalls       atomic.Int32
	mu                          sync.Mutex
	lastCluster                 *tenant.ClusterInfo
	lastCredentials             tenant.CredentialProvisionRequest
	lastOptions                 tenant.QuotaUpdateOptions
	lastListOptions             tenant.ManagedClusterListOptions
	lastDeprovision             *tenant.ClusterInfo
	listErr                     error
	listPages                   []*tenant.ManagedClusterListResult
	batchPoolErr                error
	batchPoolOmitConnectionInfo bool
	batchPoolEmptyPassword      bool
	batchPoolMissingOrg         map[int]bool
	batchPoolMissingTenant      map[int]bool
	metadataWaitCalls           atomic.Int32
	metadataBatchWaitCalls      atomic.Int32
	metadataBatchWaitHook       func(call int, clusters []*tenant.ClusterInfo)
	metadataWaitErr             error
	calls                       []string
}

func (p *quotaTestProvisioner) recordCall(name string, req tenant.CredentialProvisionRequest, cluster *tenant.ClusterInfo, opts *tenant.QuotaUpdateOptions) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, name)
	p.lastCredentials = req
	if opts != nil {
		p.lastOptions = *opts
	}
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
}

func (p *quotaTestProvisioner) recordListCall(req tenant.CredentialProvisionRequest, opts tenant.ManagedClusterListOptions) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, "list")
	p.lastCredentials = req
	p.lastListOptions = opts
}

func (p *quotaTestProvisioner) recordDeprovisionCall(req tenant.CredentialProvisionRequest, cluster *tenant.ClusterInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, "deprovision")
	p.lastCredentials = req
	if cluster != nil {
		out := *cluster
		p.lastDeprovision = &out
	}
}

func (p *quotaTestProvisioner) callsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func (p *quotaTestProvisioner) lastCredentialsSnapshot() tenant.CredentialProvisionRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastCredentials
}

func (p *quotaTestProvisioner) lastOptionsSnapshot() tenant.QuotaUpdateOptions {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastOptions
}

func (p *quotaTestProvisioner) lastListOptionsSnapshot() tenant.ManagedClusterListOptions {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastListOptions
}

func (p *quotaTestProvisioner) lastClusterSnapshot() *tenant.ClusterInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastCluster == nil {
		return nil
	}
	out := *p.lastCluster
	return &out
}

func (p *quotaTestProvisioner) lastDeprovisionSnapshot() *tenant.ClusterInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastDeprovision == nil {
		return nil
	}
	out := *p.lastDeprovision
	return &out
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
	p.ensureSystemUserCalls.Add(1)
	return "u.tdc_fs_sys", "pool-pass", nil
}

func (p *quotaTestProvisioner) UpdateQuota(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	p.updateCalls.Add(1)
	p.recordCall("update", req, cluster, &opts)
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
	p.recordCall("mark", req, cluster, nil)
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
	p.recordCall("get", req, cluster, nil)
	if p.getErr != nil {
		return nil, p.getErr
	}
	return p.cloudCfg, nil
}

func (p *quotaTestProvisioner) DeprovisionWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	call := int(p.deprovisionCalls.Add(1))
	p.recordDeprovisionCall(req, cluster)
	if p.deprovisionHook != nil {
		if err := p.deprovisionHook(call, cluster); err != nil {
			return err
		}
	}
	return p.deprovisionErr
}

func (p *quotaTestProvisioner) ListManagedClusters(_ context.Context, req tenant.CredentialProvisionRequest, opts tenant.ManagedClusterListOptions) (*tenant.ManagedClusterListResult, error) {
	call := int(p.listCalls.Add(1))
	p.recordListCall(req, opts)
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
	p.recordCall("batch_pool", req, nil, &opts)
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
		if !p.batchPoolOmitConnectionInfo {
			out[len(out)-1].Host = "db.example.com"
			out[len(out)-1].Port = 4000
			out[len(out)-1].Username = "u.root"
		}
	}
	return out, p.batchPoolCloudCfg, p.batchPoolErr
}

func (p *quotaTestProvisioner) WaitForPoolClusterMetadata(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	p.metadataWaitCalls.Add(1)
	p.recordCall("wait_pool_metadata", req, cluster, nil)
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

func (p *quotaTestProvisioner) WaitForPoolClustersMetadata(_ context.Context, clusters []*tenant.ClusterInfo, req tenant.CredentialProvisionRequest) ([]*tenant.ClusterInfo, error) {
	call := int(p.metadataBatchWaitCalls.Add(1))
	p.recordCall("wait_pool_metadata_batch", req, nil, nil)
	if p.metadataBatchWaitHook != nil {
		p.metadataBatchWaitHook(call, clusters)
	}
	if p.metadataWaitErr != nil {
		return nil, p.metadataWaitErr
	}
	out := make([]*tenant.ClusterInfo, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster == nil {
			continue
		}
		next := *cluster
		next.OrganizationID = "org-1"
		next.Host = "db.example.com"
		next.Port = 4000
		next.Username = "u.root"
		if next.DBName == "" {
			next.DBName = "tidbcloud_fs"
		}
		out = append(out, &next)
	}
	return out, nil
}

func (p *quotaTestProvisioner) MarkClusterPoolUsed(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, _ time.Time, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	p.markPoolUsedCalls.Add(1)
	p.recordCall("mark_pool_used", req, cluster, &opts)
	if p.cloudCfg != nil {
		return p.cloudCfg, nil
	}
	if opts.TiDBCloudSpendingLimitMonthly == nil {
		return nil, nil
	}
	return &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: opts.TiDBCloudSpendingLimitMonthly}, nil
}

func (p *quotaTestProvisioner) MarkClusterPoolFree(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	p.markPoolFreeCalls.Add(1)
	p.recordCall("mark_pool_free", req, cluster, nil)
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
	defer func(body io.Closer) { _ = body.Close() }(resp.Body)
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
	defer func(body io.Closer) { _ = body.Close() }(resp.Body)
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
	lastCredentials := rt.prov.lastCredentialsSnapshot()
	if lastCredentials.PublicKey == "default-pk" || lastCredentials.PrivateKey == "default-sk" {
		t.Fatalf("quota get used default credentials: %#v", lastCredentials)
	}
}

func TestQuotaGetAllowsExplicitDefaultTiDBCloudCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.defaultPublicKey = "default-pk"
	rt.prov.defaultPrivateKey = "default-sk"
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "default-pk", "default-sk", "")
	defer func(body io.Closer) { _ = body.Close() }(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := rt.prov.getCalls.Load(); got != 1 {
		t.Fatalf("get calls = %d, want 1", got)
	}
	lastCredentials := rt.prov.lastCredentialsSnapshot()
	if lastCredentials.PublicKey != "default-pk" || lastCredentials.PrivateKey != "default-sk" {
		t.Fatalf("last credentials = %#v", lastCredentials)
	}
}

func TestQuotaGetReturnsConfigStorageUsageAndSpendingLimit(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	localSpendingLimit := int64(9000)
	cloudSpendingLimit := int64(10000)
	rt.prov.cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: &cloudSpendingLimit}
	ctx := context.Background()
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:               rt.tenantID,
		MaxStorageBytes:        123 * quotaStorageSizeBytes,
		MaxFileSizeBytes:       12 * quotaStorageSizeBytes,
		MaxFileCount:           34,
		MaxMediaLLMFiles:       56,
		MaxMonthlyCostMC:       789,
		TiDBCloudSpendingLimit: &localSpendingLimit,
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
	defer func(body io.Closer) { _ = body.Close() }(resp.Body)
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
	if out.Config.MaxStorageSize != 123 || out.Config.MaxFileSize != 12 || out.Config.MaxFileCount != 34 || out.Config.TiDBCloudSpendingLimit == nil || *out.Config.TiDBCloudSpendingLimit != localSpendingLimit {
		t.Fatalf("config = %#v", out.Config)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != localSpendingLimit {
		t.Fatalf("persisted spending limit = %#v, want %d", cfg.TiDBCloudSpendingLimit, localSpendingLimit)
	}
	if cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("checked_at = %v, want nil after GET", cfg.TiDBCloudSpendingLimitCheckedAt)
	}
	if out.Usage.StorageBytes != 321 || out.Usage.ReservedBytes != 11 || out.Usage.FileCount != 9 {
		t.Fatalf("usage = %#v", out.Usage)
	}
	if strings.Contains(string(raw), "media_file_count") || strings.Contains(string(raw), "monthly_cost_mc") {
		t.Fatalf("response should not expose media or cost counters: %s", raw)
	}
}

func TestSharedQuotaGetAndSetUseLocalVirtualValueWithoutCloudMutation(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNativeShared)
	ctx := context.Background()
	if _, err := rt.meta.DB().ExecContext(ctx, `UPDATE tenants SET cluster_id = '' WHERE id = ?`, rt.tenantID); err != nil {
		t.Fatalf("clear tenant cluster id: %v", err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := rt.meta.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared-quota", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	fsID, err := rt.meta.ResolveFsID(ctx, rt.tenantID)
	if err != nil {
		t.Fatalf("ResolveFsID: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}
	if err := rt.meta.IncrSharedDBTenantCount(ctx, dbID, 1); err != nil {
		t.Fatalf("IncrSharedDBTenantCount: %v", err)
	}
	if err := rt.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-shared-quota", ClusterID: "cluster-shared-quota",
	}); err != nil {
		t.Fatalf("UpdateManagedSharedDBPoolCloudResult: %v", err)
	}
	initialLimit := int64(1000)
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID: rt.tenantID, MaxStorageBytes: 100 * quotaStorageSizeBytes,
		MaxFileSizeBytes: 10 * quotaStorageSizeBytes, MaxMediaLLMFiles: 1,
		MaxVideoLLMFiles: 1, TiDBCloudSpendingLimit: &initialLimit,
	}); err != nil {
		t.Fatalf("SetQuotaConfig: %v", err)
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	getResp := getQuota(t, ts.URL, rt.tenantID, "", "", rt.apiKey)
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(getResp.Body)
		t.Fatalf("GET status = %d body=%s", getResp.StatusCode, raw)
	}
	var getOut quotaResponse
	if err := json.NewDecoder(getResp.Body).Decode(&getOut); err != nil {
		t.Fatal(err)
	}
	if !getOut.SupportsUpdate || getOut.Config.TiDBCloudSpendingLimit == nil || *getOut.Config.TiDBCloudSpendingLimit != initialLimit {
		t.Fatalf("GET response = %+v", getOut)
	}

	updatedLimit := int64(2000)
	setResp := postJSON(t, ts.URL+"/v1/quota", map[string]any{
		"tenant_id": rt.tenantID, "public_key": "public", "private_key": "private",
		"tidbcloud_spending_limit": updatedLimit,
	}, "")
	defer func() { _ = setResp.Body.Close() }()
	if setResp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(setResp.Body)
		t.Fatalf("SET status = %d body=%s", setResp.StatusCode, raw)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != updatedLimit {
		t.Fatalf("persisted shared limit = %#v, want %d", cfg.TiDBCloudSpendingLimit, updatedLimit)
	}
	if rt.prov.getCalls.Load() != 1 || rt.prov.markCalls.Load() != 0 || rt.prov.updateCalls.Load() != 0 {
		t.Fatalf("shared quota Cloud calls: get=%d mark=%d update=%d; want one read-only authorization call",
			rt.prov.getCalls.Load(), rt.prov.markCalls.Load(), rt.prov.updateCalls.Load())
	}
}

func TestQuotaGetUsesDrive9APIKeyWhenLocalSpendingLimitExists(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	spendingLimit := int64(10000)
	ctx := context.Background()
	if err := rt.meta.SetQuotaConfigPatch(ctx, rt.tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &spendingLimit}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnsureQuotaUsageRow(ctx, rt.tenantID); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.SetQuotaCounters(ctx, rt.tenantID, 321, 7, 9); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.IncrReservedBytes(ctx, rt.tenantID, 7); err != nil {
		t.Fatal(err)
	}
	rt.prov.getErr = errors.New("should not call TiDB Cloud when Drive9 API key and local spending limit are present")
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "", "", rt.apiKey)
	defer func(body io.Closer) { _ = body.Close() }(resp.Body)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	var out quotaResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Config.TiDBCloudSpendingLimit == nil || *out.Config.TiDBCloudSpendingLimit != spendingLimit {
		t.Fatalf("spending limit = %#v, want %d", out.Config.TiDBCloudSpendingLimit, spendingLimit)
	}
	if out.Usage.StorageBytes != 321 || out.Usage.ReservedBytes != 7 || out.Usage.FileCount != 9 {
		t.Fatalf("usage = %#v", out.Usage)
	}
	if got := rt.prov.getCalls.Load(); got != 0 {
		t.Fatalf("get calls = %d, want 0", got)
	}
}

func TestSyncTiDBCloudSpendingLimitSkipsNewerLocalAtSameTimestampTick(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()

	newLimit := int64(200)
	if err := rt.meta.SetQuotaConfigPatch(ctx, rt.tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &newLimit}); err != nil {
		t.Fatal(err)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}

	staleLimit := int64(100)
	if err := rt.server.syncTiDBCloudSpendingLimit(ctx, "quota_get", rt.tenantID, &tenant.QuotaCloudConfig{
		TiDBCloudSpendingLimitMonthly: &staleLimit,
	}, cfg.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	cfg, err = rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != newLimit {
		t.Fatalf("spending limit = %#v, want %d", cfg.TiDBCloudSpendingLimit, newLimit)
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
	lastCluster := rt.prov.lastClusterSnapshot()
	if lastCluster == nil || lastCluster.ClusterID != "cluster-quota-1" || lastCluster.TenantID != rt.tenantID {
		t.Fatalf("last cluster = %#v", lastCluster)
	}
	lastCredentials := rt.prov.lastCredentialsSnapshot()
	if lastCredentials.PublicKey != "public-1" || lastCredentials.PrivateKey != "private-1" {
		t.Fatalf("last credentials = %#v", lastCredentials)
	}
}

func TestQuotaGetCachesTiDBCloudRBACWithoutBackfillingSpendingLimit(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	spendingLimit := int64(222)
	rt.prov.cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: &spendingLimit}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	resp := getQuota(t, ts.URL, rt.tenantID, "public-1", "private-1", "")
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("first status = %d, want 200: %s", resp.StatusCode, body)
	}
	_ = resp.Body.Close()
	if got := rt.prov.getCalls.Load(); got != 1 {
		t.Fatalf("get calls after first request = %d, want 1", got)
	}
	cfg, err := rt.meta.GetQuotaConfig(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit != nil {
		t.Fatalf("persisted spending limit = %#v, want nil", cfg.TiDBCloudSpendingLimit)
	}
	if cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("checked_at = %v, want nil after GET", cfg.TiDBCloudSpendingLimitCheckedAt)
	}

	rt.prov.getErr = errors.New("should not call TiDB Cloud while RBAC cache and local spending limit are present")
	resp2 := getQuota(t, ts.URL, rt.tenantID, "public-1", "private-1", "")
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("second status = %d, want 200: %s", resp2.StatusCode, body)
	}
	var out quotaResponse
	if err := json.NewDecoder(resp2.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Config.TiDBCloudSpendingLimit != nil {
		t.Fatalf("response spending limit = %#v, want nil", out.Config.TiDBCloudSpendingLimit)
	}
	if got := rt.prov.getCalls.Load(); got != 1 {
		t.Fatalf("get calls after cached request = %d, want 1", got)
	}

	rt.prov.getErr = nil
	updatedLimit := int64(333)
	rt.prov.cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: &updatedLimit}
	resp3 := getQuota(t, ts.URL, rt.tenantID, "public-1", "private-2", "")
	defer func() { _ = resp3.Body.Close() }()
	if resp3.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp3.Body)
		t.Fatalf("third status = %d, want 200: %s", resp3.StatusCode, body)
	}
	if got := rt.prov.getCalls.Load(); got != 2 {
		t.Fatalf("get calls after new credential = %d, want 2", got)
	}
	cfg, err = rt.meta.GetQuotaConfig(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit != nil {
		t.Fatalf("updated spending limit = %#v, want nil", cfg.TiDBCloudSpendingLimit)
	}
	if cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("checked_at = %v, want nil after GET with new credentials", cfg.TiDBCloudSpendingLimitCheckedAt)
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
	calls := rt.prov.callsSnapshot()
	if len(calls) != 1 || calls[0] != "mark" {
		t.Fatalf("calls = %#v, want mark only", calls)
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
	calls := rt.prov.callsSnapshot()
	if len(calls) != 1 || calls[0] != "mark" {
		t.Fatalf("calls = %#v, want mark only", calls)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 100 || cfg.MaxFileSizeBytes != 64*quotaStorageSizeBytes || cfg.MaxFileCount != 42 || cfg.MaxMediaLLMFiles != 400 || cfg.MaxMonthlyCostMC != 500 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestQuotaSetSpendingLimitOnlyPersistsSpendingLimitConfig(t *testing.T) {
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
	calls := rt.prov.callsSnapshot()
	if len(calls) != 2 || calls[0] != "mark" || calls[1] != "update" {
		t.Fatalf("calls = %#v, want mark before update", calls)
	}
	lastOptions := rt.prov.lastOptionsSnapshot()
	if lastOptions.TiDBCloudSpendingLimitMonthly == nil || *lastOptions.TiDBCloudSpendingLimitMonthly != spendingLimit {
		t.Fatalf("spending limit option = %#v, want %d", lastOptions.TiDBCloudSpendingLimitMonthly, spendingLimit)
	}
	cfg, err := rt.meta.GetQuotaConfig(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != spendingLimit {
		t.Fatalf("persisted spending limit = %#v, want %d", cfg.TiDBCloudSpendingLimit, spendingLimit)
	}
	if cfg.MaxStorageBytes != meta.DefaultMaxStorageBytes() || cfg.MaxFileSizeBytes != meta.DefaultMaxFileSizeBytes() || cfg.MaxFileCount != 0 {
		t.Fatalf("storage quota fields = %#v, want defaults", cfg)
	}
	version, err := rt.meta.GetQuotaConfigVersion(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if version == "" {
		t.Fatalf("storage quota config version should be non-empty when config row exists")
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
	lastCredentials := rt.prov.lastCredentialsSnapshot()
	if lastCredentials.PublicKey == "default-pk" || lastCredentials.PrivateKey == "default-sk" {
		t.Fatalf("quota set used default credentials: %#v", lastCredentials)
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
	calls := rt.prov.callsSnapshot()
	if len(calls) != 1 || calls[0] != "mark" {
		t.Fatalf("calls = %#v, want mark only", calls)
	}
	lastCredentials := rt.prov.lastCredentialsSnapshot()
	if lastCredentials.PublicKey != "default-pk" || lastCredentials.PrivateKey != "default-sk" {
		t.Fatalf("last credentials = %#v", lastCredentials)
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
		{name: "small_spending_limit", field: "tidbcloud_spending_limit", value: 9, wantErr: "tidbcloud_spending_limit must be 0 or at least 10 RMB"},
		{name: "spending_limit_above_cloud_maximum", field: "tidbcloud_spending_limit", value: meta.MaxTiDBCloudSpendingLimit + 1, wantErr: "tidbcloud_spending_limit is too large"},
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
	rt := newQuotaRuntime(t, tenant.ProviderTiDBZero)
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
	lastListOptions := rt.prov.lastListOptionsSnapshot()
	if lastListOptions.ClusterID != "" {
		t.Fatalf("cluster filter = %q, want empty", lastListOptions.ClusterID)
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
	sharedTenantID := "tenant-shared-authorized"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID: sharedTenantID, Status: meta.TenantActive, Kind: meta.TenantKindLive,
		Provider: tenant.ProviderTiDBCloudNativeShared, SchemaVersion: 1, DBPasswordCipher: []byte{},
		CreatedAt: now.Add(3 * time.Second), UpdatedAt: now.Add(3 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	sharedFsID, err := rt.meta.ResolveFsID(ctx, sharedTenantID)
	if err != nil {
		t.Fatal(err)
	}
	sharedDBID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-1", Host: "shared-list.example.com", Port: 4000,
		User: "root", PasswordCipher: []byte("cipher"), Name: "shared_db",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.meta.DB().ExecContext(ctx, "UPDATE db_pool SET cluster_id = ? WHERE db_id = ?", "cluster-allowed", sharedDBID); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID: sharedFsID, DbID: sharedDBID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared,
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
	if len(out.Tenants) != 2 || out.Tenants[0].TenantID != sharedTenantID || out.Tenants[1].TenantID != rt.tenantID {
		t.Fatalf("tenants = %#v, want authorized shared tenant %s and dedicated tenant %s", out.Tenants, sharedTenantID, rt.tenantID)
	}
}

func TestAdminTenantListIncludeQuotaDoesNotBackfillSpendingLimit(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       rt.tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-quota-1",
		CreatedAt:      now,
		UpdatedAt:      now,
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
	cloudSpendingLimit := int64(12345)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:                     "cluster-quota-1",
			OrganizationID:                "org-1",
			TiDBCloudSpendingLimitMonthly: &cloudSpendingLimit,
		}},
	}}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants?include_quota=true", nil)
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
	if len(out.Tenants) != 1 || out.Tenants[0].Quota == nil {
		t.Fatalf("tenants = %#v, want one tenant with quota", out.Tenants)
	}
	if out.Tenants[0].Quota.Config.TiDBCloudSpendingLimit != nil {
		t.Fatalf("response spending limit = %#v, want local nil value", out.Tenants[0].Quota.Config.TiDBCloudSpendingLimit)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit != nil || cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("quota config mutated by GET: %#v", cfg)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
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
	localSpendingLimit := int64(7000)
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:               rt.tenantID,
		MaxStorageBytes:        100 * quotaStorageSizeBytes,
		MaxFileSizeBytes:       10 * quotaStorageSizeBytes,
		MaxFileCount:           7,
		TiDBCloudSpendingLimit: &localSpendingLimit,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnsureQuotaUsageRow(ctx, rt.tenantID); err != nil {
		t.Fatal(err)
	}
	cloudSpendingLimit := int64(12345)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:                     "cluster-quota-1",
			OrganizationID:                "org-1",
			TiDBCloudSpendingLimitMonthly: &cloudSpendingLimit,
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
	defer func(body io.Closer) { _ = body.Close() }(resp.Body)
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
	if out.Quota.Config.TiDBCloudSpendingLimit == nil || *out.Quota.Config.TiDBCloudSpendingLimit != localSpendingLimit {
		t.Fatalf("spending limit = %#v, want local value %d", out.Quota.Config.TiDBCloudSpendingLimit, localSpendingLimit)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != localSpendingLimit || cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("quota config mutated by GET: %#v", cfg)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
	lastListOptions := rt.prov.lastListOptionsSnapshot()
	if lastListOptions.ClusterID != "cluster-quota-1" {
		t.Fatalf("cluster filter = %q, want cluster-quota-1", lastListOptions.ClusterID)
	}
	if got := rt.prov.markCalls.Load(); got != 0 {
		t.Fatalf("mark calls = %d, want 0", got)
	}

	rt.prov.listErr = errors.New("should not call TiDB Cloud while admin RBAC cache and local spending limit are present")
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func(body io.Closer) { _ = body.Close() }(resp.Body)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("cached status = %d, want 200: %s", resp.StatusCode, body)
	}
	if got := rt.prov.listCalls.Load(); got != 1 {
		t.Fatalf("list calls after cached request = %d, want 1", got)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Quota == nil || out.Quota.Config.TiDBCloudSpendingLimit == nil || *out.Quota.Config.TiDBCloudSpendingLimit != localSpendingLimit {
		t.Fatalf("cached spending limit = %#v, want local value %d", out.Quota, localSpendingLimit)
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
	rt.prov.listPages = []*tenant.ManagedClusterListResult{
		{},
		{Clusters: []tenant.CloudClusterInfo{{OrganizationID: "org-1"}}},
	}
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
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 0 || out.Status != adminTenantPoolStatusCreating {
		t.Fatalf("pool response = %#v", out)
	}
	if rt.prov.batchPoolCalls.Load() != 1 {
		t.Fatalf("batch pool calls = %d, want 1", rt.prov.batchPoolCalls.Load())
	}
	lastOptions := rt.prov.lastOptionsSnapshot()
	if lastOptions.TenantPoolID != out.PoolID {
		t.Fatalf("batch pool id option = %q, want %q", lastOptions.TenantPoolID, out.PoolID)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.PoolID != out.PoolID || pool.Size != 2 || pool.Status != meta.TenantPoolActive {
		t.Fatalf("stored pool = %#v", pool)
	}
	free, err := rt.meta.CountFreeTenantPoolBindings(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("count free: %v", err)
	}
	if free != 0 {
		t.Fatalf("active free = %d, want 0 before schema init", free)
	}
	slots, err := rt.meta.CountTenantPoolFreeSlots(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("count free slots: %v", err)
	}
	if slots != 2 {
		t.Fatalf("free slots = %d, want 2", slots)
	}

}

func TestAdminTenantPoolDisplayStatusCreating(t *testing.T) {
	if got := adminTenantPoolDisplayStatus(meta.TenantPoolActive, 0, 2); got != adminTenantPoolStatusCreating {
		t.Fatalf("pending slots status = %q, want %q", got, adminTenantPoolStatusCreating)
	}
	if got := adminTenantPoolDisplayStatus(meta.TenantPoolActive, 1, 2); got != adminTenantPoolStatus(meta.TenantPoolActive) {
		t.Fatalf("free slots status = %q, want %q", got, meta.TenantPoolActive)
	}
	if got := adminTenantPoolDisplayStatus(meta.TenantPoolActive, 0, 0); got != adminTenantPoolStatus(meta.TenantPoolActive) {
		t.Fatalf("empty slots status = %q, want %q", got, meta.TenantPoolActive)
	}
	if got := adminTenantPoolDisplayStatus(meta.TenantPoolDeleting, 0, 2); got != adminTenantPoolStatus(meta.TenantPoolDeleting) {
		t.Fatalf("deleting status = %q, want %q", got, meta.TenantPoolDeleting)
	}
}

func TestAdminTenantPoolGetShowsCreatingForPendingSlots(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{OrganizationID: "org-1"}},
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
		t.Fatalf("create pool: %v", err)
	}
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               "pool-tenant-pending",
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-pending",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       "pool-tenant-pending",
		OrganizationID: "org-1",
		ClusterID:      "cluster-pending",
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)
	resp := getAdminTenantPool(t, ts.URL, "public-1", "private-1")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("get pool status = %d body=%s", resp.StatusCode, body)
	}
	var got adminTenantPoolResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if got.PoolID != "pool-1" || got.FreeSize != 0 || got.Status != adminTenantPoolStatusCreating {
		t.Fatalf("get pool response = %#v", got)
	}
}

func TestTenantSchemaInitStopsWhenTenantLeavesProvisioning(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	now := time.Now().UTC()
	tenantID := "schema-init-shrunk-tenant"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-schema-init",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		rt.server.initTenantSchemaAsync(ctx, tenantID, "u.root:pass@tcp(db.example.com:4000)/tidbcloud_fs", tenant.ProviderTiDBCloudNative, func(ctx context.Context, _ string) error {
			updated, err := rt.meta.UpdateTenantStatusIf(ctx, tenantID, meta.TenantProvisioning, meta.TenantDeleting)
			if err != nil {
				errCh <- err
				return err
			}
			if !updated {
				err := fmt.Errorf("tenant status was not provisioning")
				errCh <- err
				return err
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("schema init did not stop after tenant left provisioning")
	}
	select {
	case err := <-errCh:
		t.Fatalf("schema init setup failed: %v", err)
	default:
	}
	if calls := rt.prov.ensureSystemUserCalls.Load(); calls != 0 {
		t.Fatalf("ensure system user calls = %d, want 0", calls)
	}
	got, err := rt.meta.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	if got.Status != meta.TenantDeleting {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleting)
	}
}

func TestAdminTenantPoolCreatePersistsClustersWhenMetadataWaitFails(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	rt.prov.batchPoolErr = errors.New("tidbcloud native cluster get status 429")
	rt.prov.batchPoolOmitConnectionInfo = true
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
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 0 || out.Status != adminTenantPoolStatusCreating {
		t.Fatalf("pool response = %#v", out)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.PoolID != out.PoolID {
		t.Fatalf("stored pool id = %q, want %q", pool.PoolID, out.PoolID)
	}
	if pool.Status != meta.TenantPoolActive {
		t.Fatalf("stored pool status = %s, want %s", pool.Status, meta.TenantPoolActive)
	}
	rows, err := rt.meta.ListPendingTenantPoolBindingsForResume(context.Background(), "org-1", 10)
	if err != nil {
		t.Fatalf("list pending bindings: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("pending bindings = %d, want 2", len(rows))
	}
	for _, row := range rows {
		if row.Tenant.Status != meta.TenantPending {
			t.Fatalf("tenant %s status = %s, want pending", row.Tenant.ID, row.Tenant.Status)
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
	for rt.prov.metadataBatchWaitCalls.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rt.prov.metadataBatchWaitCalls.Load(); got < 1 {
		t.Fatalf("metadata batch wait calls = %d, want at least 1", got)
	}
	if got := rt.prov.metadataWaitCalls.Load(); got != 0 {
		t.Fatalf("single metadata wait calls = %d, want 0", got)
	}
}

func TestAdminTenantPoolCreatePreservesPersistedClustersWhenOneOrganizationMissing(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	rt.prov.batchPoolErr = errors.New("tidbcloud native cluster get status 429")
	rt.prov.batchPoolOmitConnectionInfo = true
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
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 0 || out.Status != adminTenantPoolStatusCreating {
		t.Fatalf("pool response = %#v", out)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.PoolID != out.PoolID {
		t.Fatalf("stored pool id = %q, want %q", pool.PoolID, out.PoolID)
	}
	if pool.Status != meta.TenantPoolActive {
		t.Fatalf("stored pool status = %s, want %s", pool.Status, meta.TenantPoolActive)
	}
	rows, err := rt.meta.ListPendingTenantPoolBindingsForResume(context.Background(), "org-1", 10)
	if err != nil {
		t.Fatalf("list pending bindings: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("pending bindings = %d, want 1", len(rows))
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	lastDeprovision := rt.prov.lastDeprovisionSnapshot()
	if lastDeprovision == nil || lastDeprovision.ClusterID != "pool-cluster-2" {
		t.Fatalf("last deprovision = %#v, want pool-cluster-2", lastDeprovision)
	}
}

func TestAdminTenantPoolCreatePreservesPersistedClustersWhenOneTenantLabelMissing(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{}}
	rt.prov.batchPoolErr = errors.New("tidbcloud native cluster get status 429")
	rt.prov.batchPoolOmitConnectionInfo = true
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
	if out.PoolID == "" || out.OrganizationID != "org-1" || out.PoolSize != 2 || out.FreeSize != 0 || out.Status != adminTenantPoolStatusCreating {
		t.Fatalf("pool response = %#v", out)
	}
	pool, err := rt.meta.GetTenantPoolByOrganization(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.Status != meta.TenantPoolActive {
		t.Fatalf("stored pool status = %s, want %s", pool.Status, meta.TenantPoolActive)
	}
	rows, err := rt.meta.ListPendingTenantPoolBindingsForResume(context.Background(), "org-1", 10)
	if err != nil {
		t.Fatalf("list pending bindings: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("pending bindings = %d, want 1", len(rows))
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	lastDeprovision := rt.prov.lastDeprovisionSnapshot()
	if lastDeprovision == nil || lastDeprovision.ClusterID != "pool-cluster-2" {
		t.Fatalf("last deprovision = %#v, want pool-cluster-2", lastDeprovision)
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

func TestResumeProvisioningNativeWithoutConnectionSkipsMetadataResume(t *testing.T) {
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
	tnt, err := rt.meta.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if tnt.Status != meta.TenantProvisioning || tnt.DBUser != "" || tnt.DBHost != "" {
		t.Fatalf("tenant after resume = status %s host %q user %q, want unchanged provisioning without connection", tnt.Status, tnt.DBHost, tnt.DBUser)
	}
	if got := rt.prov.metadataWaitCalls.Load(); got != 0 {
		t.Fatalf("metadata wait calls = %d, want 0", got)
	}
	if got := rt.prov.metadataBatchWaitCalls.Load(); got != 0 {
		t.Fatalf("metadata batch wait calls = %d, want 0", got)
	}
}

func TestTenantPoolClaimMissTriggersPendingMetadataResume(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-pending-1",
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
		t.Fatalf("create pool: %v", err)
	}
	passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	tenantID := "pool-pending-resume-1"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-pending-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-pending-1",
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	res, pool, claimed, _, err := rt.server.claimAdminTenantFromPool(ctx, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}, nil)
	if err != nil {
		t.Fatalf("claim from pool: %v", err)
	}
	if claimed || res != nil || pool == nil || pool.PoolID != "pool-1" {
		t.Fatalf("claim result res=%#v pool=%#v claimed=%v, want miss with pool", res, pool, claimed)
	}
	if got := rt.prov.markPoolUsedCalls.Load(); got != 0 {
		t.Fatalf("mark pool used calls = %d, want 0 for pending tenant", got)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		tnt, err := rt.meta.GetTenant(ctx, tenantID)
		if err != nil {
			t.Fatal(err)
		}
		if rt.prov.metadataBatchWaitCalls.Load() >= 1 && tnt.DBHost == "db.example.com" && tnt.DBUser == "u.tdc_fs_sys" && tnt.Status == meta.TenantActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant after pending resume = status %s host %q user %q, metadata batch waits=%d", tnt.Status, tnt.DBHost, tnt.DBUser, rt.prov.metadataBatchWaitCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTenantPoolClaimSeedsQuotaConfigWithoutExplicitQuota(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{
			ClusterID:      "cluster-pool-claim-seed",
			OrganizationID: "org-1",
		}},
	}}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-seed-quota",
		OrganizationID: "org-1",
		Size:           1,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	tenantID := "pool-claim-seed-quota"
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
		ClusterID:        "cluster-pool-claim-seed",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-1",
		ClusterID:      "cluster-pool-claim-seed",
		PoolID:         "pool-seed-quota",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	res, _, claimed, _, err := rt.server.claimAdminTenantFromPool(ctx, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}, nil)
	if err != nil {
		t.Fatalf("claim from pool: %v", err)
	}
	if !claimed || res == nil {
		t.Fatalf("claim result res=%#v claimed=%v, want claimed", res, claimed)
	}

	cfg, err := rt.meta.GetQuotaConfig(ctx, res.TenantID)
	if err != nil {
		t.Fatalf("get quota config: %v", err)
	}
	if cfg.TiDBCloudSpendingLimit != nil {
		t.Fatalf("spending limit = %d, want nil", *cfg.TiDBCloudSpendingLimit)
	}
	if cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("checked_at = %v, want nil without a cloud observation", cfg.TiDBCloudSpendingLimitCheckedAt)
	}
}

func TestTenantPoolMetadataResumeRerunsWhenTriggeredWhileActive(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
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
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var closeFirstStarted sync.Once
	rt.prov.metadataBatchWaitHook = func(call int, _ []*tenant.ClusterInfo) {
		if call != 1 {
			return
		}
		closeFirstStarted.Do(func() { close(firstStarted) })
		<-releaseFirst
	}
	makePending := func(tenantID, clusterID string) *tenant.ClusterInfo {
		t.Helper()
		passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
		if err != nil {
			t.Fatalf("encrypt password: %v", err)
		}
		if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
			ID:               tenantID,
			Status:           meta.TenantPending,
			DBPasswordCipher: passCipher,
			DBName:           "tidbcloud_fs",
			DBTLS:            true,
			Provider:         tenant.ProviderTiDBCloudNative,
			ClusterID:        clusterID,
			SchemaVersion:    1,
			CreatedAt:        now,
			UpdatedAt:        now,
		}); err != nil {
			t.Fatalf("insert tenant %s: %v", tenantID, err)
		}
		if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
			TenantID:       tenantID,
			OrganizationID: "org-1",
			ClusterID:      clusterID,
			PoolID:         "pool-1",
			PoolStatus:     meta.TenantPoolBindingFree,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatalf("upsert binding %s: %v", tenantID, err)
		}
		return &tenant.ClusterInfo{
			TenantID:       tenantID,
			ClusterID:      clusterID,
			OrganizationID: "org-1",
			Password:       "pool-pass",
			DBName:         "tidbcloud_fs",
			Provider:       tenant.ProviderTiDBCloudNative,
		}
	}
	first := makePending("pool-pending-rerun-1", "cluster-pending-rerun-1")
	rt.server.startPoolClustersMetadataResume(ctx, "pool-1", []*tenant.ClusterInfo{first}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first metadata resume did not start")
	}
	second := makePending("pool-pending-rerun-2", "cluster-pending-rerun-2")
	rt.server.startPoolClustersMetadataResume(ctx, "pool-1", []*tenant.ClusterInfo{second}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	close(releaseFirst)

	deadline := time.Now().Add(5 * time.Second)
	for {
		secondTenant, err := rt.meta.GetTenant(ctx, second.TenantID)
		if err != nil {
			t.Fatal(err)
		}
		if rt.prov.metadataBatchWaitCalls.Load() >= 2 && secondTenant.DBHost == "db.example.com" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second tenant after rerun = status %s host %q, metadata batch waits=%d", secondTenant.Status, secondTenant.DBHost, rt.prov.metadataBatchWaitCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
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
	if !strings.Contains(string(body), "pool_size 3 exceeds maximum 2") {
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
	if !strings.Contains(string(body), "pool_size 3 exceeds maximum 2") {
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
	metricsText := readServerMetrics(t, rt.server)
	if !strings.Contains(metricsText, `drive9_service_operations_total{component="admin_tenant_pool",operation="update",result="cluster_error"}`) {
		t.Fatalf("metrics missing admin tenant pool update cluster_error: %s", metricsText)
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

	defaultLimit := int64(1000)
	rt.prov.batchPoolCloudCfg = &tenant.QuotaCloudConfig{
		TiDBCloudSpendingLimitMonthly: &defaultLimit,
	}

	rt.server.replenishTenantPoolAsync(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	if got := rt.prov.batchPoolCalls.Load(); got != 1 {
		t.Fatalf("batch pool calls = %d, want 1", got)
	}
	lastOptions := rt.prov.lastOptionsSnapshot()
	if lastOptions.TiDBCloudSpendingLimitMonthly != nil {
		t.Fatalf("replenish spending limit = %#v, want nil", lastOptions.TiDBCloudSpendingLimitMonthly)
	}

	tenants, err := rt.meta.ListTenantsByStatus(ctx, meta.TenantActive, 10)
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	var poolTenantID string
	for _, tenant := range tenants {
		if tenant.ID != rt.tenantID {
			poolTenantID = tenant.ID
			break
		}
	}
	if poolTenantID == "" {
		t.Fatal("no pool tenant created by replenish")
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, poolTenantID)
	if err != nil {
		t.Fatalf("get quota config: %v", err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != defaultLimit {
		t.Fatalf("persisted spending limit = %#v, want %d", cfg.TiDBCloudSpendingLimit, defaultLimit)
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
	lastCluster := rt.prov.lastClusterSnapshot()
	if lastCluster == nil || lastCluster.ClusterID != "cluster-free-1" {
		t.Fatalf("last cluster = %#v", lastCluster)
	}
	lastOptions := rt.prov.lastOptionsSnapshot()
	if lastOptions.TiDBCloudSpendingLimitMonthly == nil || *lastOptions.TiDBCloudSpendingLimitMonthly != 123 {
		t.Fatalf("last spending limit = %#v", lastOptions.TiDBCloudSpendingLimitMonthly)
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
	lastCluster := rt.prov.lastClusterSnapshot()
	if lastCluster == nil || lastCluster.ClusterID != "cluster-free-1" {
		t.Errorf("last cluster = %#v", lastCluster)
	}
	lastOptions := rt.prov.lastOptionsSnapshot()
	if lastOptions.TiDBCloudSpendingLimitMonthly == nil || *lastOptions.TiDBCloudSpendingLimitMonthly != 123 {
		t.Errorf("last spending limit = %#v", lastOptions.TiDBCloudSpendingLimitMonthly)
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
	calls := rt.prov.callsSnapshot()
	if len(calls) < 2 || calls[0] != "list" || calls[1] != "mark" {
		t.Fatalf("calls = %#v, want list then mark", calls)
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
	lastDeprovision := rt.prov.lastDeprovisionSnapshot()
	if lastDeprovision == nil || lastDeprovision.ClusterID != "cluster-quota-1" {
		t.Fatalf("deprovision cluster = %#v", lastDeprovision)
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

func getAdminTenantPool(t *testing.T, baseURL, publicKey, privateKey string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/admin/tenant-pool", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, publicKey)
	req.Header.Set(quotaPrivateKeyHeader, privateKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
