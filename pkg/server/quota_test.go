package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type quotaTestProvisioner struct {
	provider        string
	updateErr       error
	getErr          error
	cloudCfg        *tenant.QuotaCloudConfig
	updateCalls     atomic.Int32
	getCalls        atomic.Int32
	lastCluster     *tenant.ClusterInfo
	lastCredentials tenant.CredentialProvisionRequest
	lastOptions     tenant.QuotaUpdateOptions
}

func (p *quotaTestProvisioner) ProviderType() string { return p.provider }

func (p *quotaTestProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, errors.New("not implemented")
}

func (p *quotaTestProvisioner) InitSchema(context.Context, string) error { return nil }

func (p *quotaTestProvisioner) UpdateQuota(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	p.updateCalls.Add(1)
	p.lastCredentials = req
	p.lastOptions = opts
	if cluster != nil {
		out := *cluster
		p.lastCluster = &out
	}
	if p.updateErr != nil {
		return nil, p.updateErr
	}
	return p.cloudCfg, nil
}

func (p *quotaTestProvisioner) GetQuota(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.QuotaCloudConfig, error) {
	p.getCalls.Add(1)
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

type quotaRuntime struct {
	meta     *meta.Store
	tenantID string
	apiKey   string
	prov     *quotaTestProvisioner
	server   *Server
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
}

func TestQuotaGetReturnsConfigUsageAndSpendingLimit(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	spendingLimit := int64(10000)
	rt.prov.cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: &spendingLimit}
	ctx := context.Background()
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:         rt.tenantID,
		MaxStorageBytes:  123 * quotaStorageSizeBytes,
		MaxMediaLLMFiles: 56,
		MaxMonthlyCostMC: 789,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnsureQuotaUsageRow(ctx, rt.tenantID); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.SetQuotaCounters(ctx, rt.tenantID, 321, 7); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.IncrReservedBytes(ctx, rt.tenantID, 11); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.IncrMonthlyLLMCost(ctx, rt.tenantID, 99); err != nil {
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
	if out.Config.MaxStorageSize != 123 || out.Config.TiDBCloudSpendingLimit == nil || *out.Config.TiDBCloudSpendingLimit != spendingLimit {
		t.Fatalf("config = %#v", out.Config)
	}
	if out.Usage.StorageBytes != 321 || out.Usage.ReservedBytes != 11 || out.Usage.MediaFileCount != 7 || out.Usage.MonthlyCostMC != 99 {
		t.Fatalf("usage = %#v", out.Usage)
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

func TestQuotaSetCloudNativeUpdatesCredentialLabelBeforeConfig(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.SetQuotaConfig(ctx, &meta.QuotaConfig{
		TenantID:         rt.tenantID,
		MaxStorageBytes:  100,
		MaxMediaLLMFiles: 200,
		MaxMonthlyCostMC: 300,
	}); err != nil {
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := rt.prov.updateCalls.Load(); got != 1 {
		t.Fatalf("update calls = %d, want 1", got)
	}
	if rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly != nil {
		t.Fatalf("spending limit option = %v, want nil", *rt.prov.lastOptions.TiDBCloudSpendingLimitMonthly)
	}
	cfg, err := rt.meta.GetQuotaConfig(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 1000*quotaStorageSizeBytes || cfg.MaxMediaLLMFiles != 200 || cfg.MaxMonthlyCostMC != 300 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestQuotaSetSpendingLimitOnlyDoesNotWriteStorageConfig(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	spendingLimit := int64(20000)
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
	version, err := rt.meta.GetQuotaConfigVersion(context.Background(), rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		t.Fatalf("quota config version = %q, want empty", version)
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
			rt.prov.updateErr = tc.err
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
		})
	}
}

func postJSON(t *testing.T, url string, body map[string]any, apiKey string) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
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
