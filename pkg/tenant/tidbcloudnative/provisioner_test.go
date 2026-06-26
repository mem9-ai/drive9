package tidbcloudnative

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestNewProvisionerFromEnvReadsServerSideConfigOnly(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "aws")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")
	t.Setenv(EnvTiDBCloudNativeDefaultDatabaseName, "drive9_db")
	t.Setenv(EnvTiDBCloudDefaultSpendingLimit, "5000")
	t.Setenv("DRIVE9_TIDBCLOUD_NATIVE_PUBLIC_KEY", "must-not-be-read")
	t.Setenv("DRIVE9_TIDBCLOUD_NATIVE_PRIVATE_KEY", "must-not-be-read")

	p, err := NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv: %v", err)
	}
	if p.apiURL != "https://serverless.tidbapi.com" {
		t.Fatalf("apiURL = %q", p.apiURL)
	}
	if p.cloudProvider != "aws" || p.region != "us-east-1" {
		t.Fatalf("cloud/region = %s/%s", p.cloudProvider, p.region)
	}
	if p.defaultDatabaseName != "drive9_db" {
		t.Fatalf("default db = %q", p.defaultDatabaseName)
	}
	if p.defaultSpendLimit == nil || *p.defaultSpendLimit != 5000 {
		t.Fatalf("default spend limit = %v, want 5000", p.defaultSpendLimit)
	}
}

func TestNewProvisionerFromEnvUsesBuiltinDefaultSpendingLimit(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "aws")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")
	t.Setenv(EnvTiDBCloudDefaultSpendingLimit, "")

	p, err := NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv: %v", err)
	}
	if p.defaultSpendLimit == nil || *p.defaultSpendLimit != DefaultSpendLimit {
		t.Fatalf("defaultSpendLimit = %v, want %d", p.defaultSpendLimit, DefaultSpendLimit)
	}
}

func TestNewProvisionerFromEnvRequiresCloudProviderAndRegion(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")

	_, err := NewProvisionerFromEnv()
	if err == nil {
		t.Fatal("expected missing cloud provider error")
	}
	if !strings.Contains(err.Error(), EnvTiDBCloudNativeCloudProvider) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewProvisionerFromEnvRejectsNonHTTPSAPIURL(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "http://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "aws")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")

	_, err := NewProvisionerFromEnv()
	if err == nil {
		t.Fatal("expected invalid api URL error")
	}
	if !strings.Contains(err.Error(), "valid https URL") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewProvisionerFromEnvRejectsInvalidDefaultDatabaseName(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "aws")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")
	t.Setenv(EnvTiDBCloudNativeDefaultDatabaseName, "test")

	_, err := NewProvisionerFromEnv()
	if err == nil {
		t.Fatal("expected invalid database name error")
	}
	if !strings.Contains(err.Error(), EnvTiDBCloudNativeDefaultDatabaseName) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewProvisionerFromEnvRejectsInvalidDefaultSpendingLimit(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "aws")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")
	t.Setenv(EnvTiDBCloudDefaultSpendingLimit, "-1")

	_, err := NewProvisionerFromEnv()
	if err == nil {
		t.Fatal("expected invalid default spending limit error")
	}
	if !strings.Contains(err.Error(), EnvTiDBCloudDefaultSpendingLimit) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestProvisionWithCredentialsUsesRequestCredentialsAndServerConfig(t *testing.T) {
	var pollCount int
	origEnsureDatabase := ensureDatabaseFunc
	ensureDatabaseFunc = func(context.Context, string, string, string, int, string) error {
		t.Fatal("ensure database should not run during provision")
		return nil
	}
	t.Cleanup(func() { ensureDatabaseFunc = origEnsureDatabase })

	var gotAuth string
	var gotBody struct {
		DisplayName  string `json:"displayName"`
		RootPassword string `json:"rootPassword"`
		Region       struct {
			Name string `json:"name"`
		} `json:"region"`
		Labels        map[string]string `json:"labels"`
		SpendingLimit struct {
			Monthly int32 `json:"monthly"`
		} `json:"spendingLimit"`
	}
	var requestCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/v1beta1/clusters/cluster-1" {
			pollCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId":  "cluster-1",
				"state":      "CREATING",
				"labels":     map[string]string{TiDBCloudOrganizationLabel: "org-1"},
				"userPrefix": "u1",
				"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
			})
			return
		}
		if r.URL.Path != "/v1beta1/clusters" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		pollCount++
		if pollCount == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId": "cluster-1",
				"state":     "CREATING",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"clusterId":  "cluster-1",
			"state":      "ACTIVE",
			"userPrefix": "u1",
			"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		cloudProvider:       "aws",
		region:              "us-east-1",
		defaultDatabaseName: DefaultDatabaseName,
		defaultSpendLimit:   int32Ptr(5000),
		client:              ts.Client(),
	}
	out, err := p.ProvisionWithCredentials(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("ProvisionWithCredentials: %v", err)
	}
	if !strings.Contains(gotAuth, `username="public-1"`) {
		t.Fatalf("Authorization header did not use request public key: %q", gotAuth)
	}
	if strings.Contains(gotAuth, "private-1") {
		t.Fatalf("Authorization header leaked private key: %q", gotAuth)
	}
	if gotBody.DisplayName != "tidbcloud-fs-tenant-1" {
		t.Fatalf("displayName = %q", gotBody.DisplayName)
	}
	if gotBody.Region.Name != "regions/aws-us-east-1" {
		t.Fatalf("region.name = %q", gotBody.Region.Name)
	}
	if gotBody.RootPassword == "" {
		t.Fatal("rootPassword was empty")
	}
	if gotBody.Labels[Drive9ManagedLabel] != "true" || gotBody.Labels[Drive9TenantIDLabel] != "tenant-1" {
		t.Fatalf("labels = %#v", gotBody.Labels)
	}
	if _, ok := gotBody.Labels[Drive9QuotaUpdateAtLabel]; ok {
		t.Fatalf("create labels unexpectedly included %s: %#v", Drive9QuotaUpdateAtLabel, gotBody.Labels)
	}
	if gotBody.SpendingLimit.Monthly != 5000 {
		t.Fatalf("spendingLimit.monthly = %d, want 5000", gotBody.SpendingLimit.Monthly)
	}
	if out.ClusterID != "cluster-1" || out.OrganizationID != "org-1" || out.Username != "u1.root" || out.DBName != DefaultDatabaseName || out.Provider != tenant.ProviderTiDBCloudNative {
		t.Fatalf("unexpected cluster info: %#v", out)
	}
	if pollCount < 2 {
		t.Fatalf("poll count = %d, want at least 2", pollCount)
	}
}

func TestProvisionWithCredentialsAndQuotaSendsCreateTimeSpendingLimit(t *testing.T) {
	var gotBody struct {
		Labels        map[string]string `json:"labels"`
		SpendingLimit struct {
			Monthly int32 `json:"monthly"`
		} `json:"spendingLimit"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1beta1/clusters" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"clusterId":  "cluster-1",
			"state":      "CREATING",
			"labels":     map[string]string{TiDBCloudOrganizationLabel: "org-1"},
			"userPrefix": "u1",
			"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		cloudProvider:       "aws",
		region:              "us-east-1",
		defaultDatabaseName: DefaultDatabaseName,
		defaultSpendLimit:   int32Ptr(5000),
		client:              ts.Client(),
	}
	monthly := int64(10000)
	_, cloudCfg, err := p.ProvisionWithCredentialsAndQuota(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}, tenant.QuotaUpdateOptions{TiDBCloudSpendingLimitMonthly: &monthly})
	if err != nil {
		t.Fatalf("ProvisionWithCredentialsAndQuota: %v", err)
	}
	if gotBody.SpendingLimit.Monthly != int32(monthly) {
		t.Fatalf("spendingLimit.monthly = %d, want %d", gotBody.SpendingLimit.Monthly, monthly)
	}
	if gotBody.Labels[Drive9ManagedLabel] != "true" || gotBody.Labels[Drive9TenantIDLabel] != "tenant-1" {
		t.Fatalf("labels = %#v", gotBody.Labels)
	}
	if _, ok := gotBody.Labels[Drive9QuotaUpdateAtLabel]; ok {
		t.Fatalf("create labels unexpectedly included %s: %#v", Drive9QuotaUpdateAtLabel, gotBody.Labels)
	}
	if cloudCfg == nil || cloudCfg.TiDBCloudSpendingLimitMonthly == nil || *cloudCfg.TiDBCloudSpendingLimitMonthly != monthly {
		t.Fatalf("cloud config = %#v, want spending limit %d", cloudCfg, monthly)
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}

func TestProvisionWithCredentialsDefaultsDatabaseName(t *testing.T) {
	origEnsureDatabase := ensureDatabaseFunc
	ensureDatabaseFunc = func(context.Context, string, string, string, int, string) error {
		t.Fatal("ensure database should not run during provision")
		return nil
	}
	t.Cleanup(func() { ensureDatabaseFunc = origEnsureDatabase })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"clusterId":  "cluster-1",
			"state":      "ACTIVE",
			"labels":     map[string]string{TiDBCloudOrganizationLabel: "org-1"},
			"userPrefix": "u1",
			"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		cloudProvider:       "aws",
		region:              "us-east-1",
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	out, err := p.ProvisionWithCredentials(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("ProvisionWithCredentials: %v", err)
	}
	if out.DBName != DefaultDatabaseName {
		t.Fatalf("database name = %q, want %q", out.DBName, DefaultDatabaseName)
	}
}

func TestEnsureDatabaseFromDSNUsesTenantConnection(t *testing.T) {
	var gotUser, gotPassword, gotHost, gotDBName string
	var gotPort int
	origEnsureDatabase := ensureDatabaseFunc
	ensureDatabaseFunc = func(_ context.Context, user, password, host string, port int, dbName string) error {
		gotUser = user
		gotPassword = password
		gotHost = host
		gotPort = port
		gotDBName = dbName
		return nil
	}
	t.Cleanup(func() { ensureDatabaseFunc = origEnsureDatabase })

	err := ensureDatabaseFromDSN(context.Background(), "u1.root:db-pass@tcp(db.example:4000)/tidbcloud_fs?parseTime=true&tls=true")
	if err != nil {
		t.Fatalf("ensureDatabaseFromDSN: %v", err)
	}
	if gotUser != "u1.root" || gotPassword != "db-pass" || gotHost != "db.example" || gotPort != 4000 || gotDBName != DefaultDatabaseName {
		t.Fatalf("ensure database args = %s/%s %s:%d %s", gotUser, gotPassword, gotHost, gotPort, gotDBName)
	}
}

func TestEnsureDatabaseFromDSNRejectsNonTCPNetwork(t *testing.T) {
	err := ensureDatabaseFromDSN(context.Background(), "u1.root:db-pass@unix(/tmp/mysql.sock)/tidbcloud_fs?parseTime=true")
	if err == nil {
		t.Fatal("expected non-tcp DSN error")
	}
	if !strings.Contains(err.Error(), `network must be tcp`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureDatabaseFromDSNHandlesIPv6Host(t *testing.T) {
	var gotHost string
	var gotPort int
	origEnsureDatabase := ensureDatabaseFunc
	ensureDatabaseFunc = func(_ context.Context, _ string, _ string, host string, port int, _ string) error {
		gotHost = host
		gotPort = port
		return nil
	}
	t.Cleanup(func() { ensureDatabaseFunc = origEnsureDatabase })

	err := ensureDatabaseFromDSN(context.Background(), "u1.root:db-pass@tcp([::1]:4000)/tidbcloud_fs?parseTime=true&tls=true")
	if err != nil {
		t.Fatalf("ensureDatabaseFromDSN: %v", err)
	}
	if gotHost != "[::1]" || gotPort != 4000 {
		t.Fatalf("ensure database address = %s:%d, want [::1]:4000", gotHost, gotPort)
	}
}

func TestEnsureDatabaseFromDSNRejectsNonPositivePort(t *testing.T) {
	err := ensureDatabaseFromDSN(context.Background(), "u1.root:db-pass@tcp(db.example:0)/tidbcloud_fs?parseTime=true")
	if err == nil {
		t.Fatal("expected non-positive port error")
	}
	if !strings.Contains(err.Error(), "port must be positive") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), "%!w") {
		t.Fatalf("error wrapped nil: %v", err)
	}
}

func TestProvisionWithCredentialsIncludesUpstreamBodyOnError(t *testing.T) {
	longBody := strings.Repeat("x", upstreamErrorBodyLimit+100)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Error(w, longBody, http.StatusBadRequest)
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		cloudProvider:       "aws",
		region:              "us-east-1",
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	_, err := p.ProvisionWithCredentials(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err == nil {
		t.Fatal("expected upstream error")
	}
	if !strings.Contains(err.Error(), "tidbcloud native provision status 400") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "(truncated)") {
		t.Fatalf("expected truncated upstream body, got: %v", err)
	}
}

func TestBranchWithCredentialsUsesRequestCredentials(t *testing.T) {
	var gotAuth []string
	var gotCreateBody struct {
		DisplayName  string `json:"displayName"`
		ParentID     string `json:"parentId"`
		RootPassword string `json:"rootPassword"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		gotAuth = append(gotAuth, r.Header.Get("Authorization"))
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1beta1/clusters/cluster-1/branches":
			if err := json.NewDecoder(r.Body).Decode(&gotCreateBody); err != nil {
				t.Fatalf("decode create branch body: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"branchId": "branch-1",
				"state":    "CREATING",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1beta1/clusters/cluster-1/branches/branch-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"branchId":   "branch-1",
				"state":      "ACTIVE",
				"userPrefix": "u2",
				"endpoints":  map[string]any{"public": map[string]any{"host": "branch.example", "port": 4000}},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1beta1/clusters/cluster-1/branches/branch-1":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	req := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	out, err := p.ProvisionBranchWithCredentials(context.Background(), "fork-tenant", &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		BranchID:  "source-branch",
		Password:  "branch-pass",
		DBName:    "tenant_db",
	}, req)
	if err != nil {
		t.Fatalf("ProvisionBranchWithCredentials: %v", err)
	}
	if out.ClusterID != "cluster-1" || out.BranchID != "branch-1" || out.Host != "branch.example" || out.Port != 4000 || out.Username != "u2.root" {
		t.Fatalf("branch info = %#v", out)
	}
	if gotCreateBody.ParentID != "source-branch" || gotCreateBody.RootPassword != "branch-pass" {
		t.Fatalf("create branch body = %+v", gotCreateBody)
	}
	if err := p.DeleteBranchWithCredentials(context.Background(), "cluster-1", "branch-1", req); err != nil {
		t.Fatalf("DeleteBranchWithCredentials: %v", err)
	}
	if len(gotAuth) != 3 {
		t.Fatalf("authorized request count = %d, want 3", len(gotAuth))
	}
	for _, auth := range gotAuth {
		if !strings.Contains(auth, `username="public-1"`) {
			t.Fatalf("Authorization header did not use request public key: %q", auth)
		}
		if strings.Contains(auth, "private-1") {
			t.Fatalf("Authorization header leaked private key: %q", auth)
		}
	}
}

func TestCreateBranchWithCredentialsRejectsMissingStateAndEndpoint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"branchId": "branch-1"})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	_, err := p.CreateBranchWithCredentials(context.Background(), "fork-tenant", &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		DBName:    "tenant_db",
	}, tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"})
	if err == nil || !strings.Contains(err.Error(), "missing state and endpoint") {
		t.Fatalf("CreateBranchWithCredentials error = %v, want missing state and endpoint", err)
	}
}

func TestCreateBranchWithCredentialsReturnsEndpointWhenPOSTIncludesIt(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1beta1/clusters/cluster-1/branches" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"branchId":   "branch-1",
				"state":      "CREATING",
				"userPrefix": "u2",
				"endpoints":  map[string]any{"public": map[string]any{"host": "branch.example", "port": 4000}},
			})
			return
		}
		t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	out, err := p.CreateBranchWithCredentials(context.Background(), "fork-tenant", &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		DBName:    "tenant_db",
	}, tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"})
	if err != nil {
		t.Fatalf("CreateBranchWithCredentials: %v", err)
	}
	if out.Host != "branch.example" || out.Port != 4000 || out.Username != "u2.root" {
		t.Fatalf("branch endpoint = %s:%d (user=%s), want branch.example:4000 (u2.root)", out.Host, out.Port, out.Username)
	}
	if out.BranchID != "branch-1" || out.ClusterID != "cluster-1" {
		t.Fatalf("branch metadata = cluster=%s branch=%s, want cluster-1/branch-1", out.ClusterID, out.BranchID)
	}
}

func TestCreateBranchWithCredentialsDefersToWaitWhenPOSTMissingEndpoint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"branchId": "branch-1",
			"state":    "CREATING",
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	out, err := p.CreateBranchWithCredentials(context.Background(), "fork-tenant", &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		DBName:    "tenant_db",
	}, tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"})
	if err != nil {
		t.Fatalf("CreateBranchWithCredentials: %v", err)
	}
	if out.Host != "" || out.Port != 0 || out.Username != "" {
		t.Fatalf("branch endpoint = %s:%d (user=%s), want empty (deferred to poll)", out.Host, out.Port, out.Username)
	}
	if out.BranchID != "branch-1" || out.ClusterID != "cluster-1" {
		t.Fatalf("branch metadata = cluster=%s branch=%s, want cluster-1/branch-1", out.ClusterID, out.BranchID)
	}
}

func TestWaitForBranchActiveRequiresConnectionInfo(t *testing.T) {
	origPollInterval := tidbCloudNativePollInterval
	tidbCloudNativePollInterval = time.Millisecond
	t.Cleanup(func() { tidbCloudNativePollInterval = origPollInterval })

	var getCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1beta1/clusters/cluster-1/branches/branch-1" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		getCount++
		if getCount == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"branchId": "branch-1",
				"state":    "ACTIVE",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"branchId":   "branch-1",
			"state":      "ACTIVE",
			"userPrefix": "u2",
			"endpoints":  map[string]any{"public": map[string]any{"host": "branch.example", "port": 4000}},
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL: ts.URL,
		client: ts.Client(),
	}
	out, err := p.WaitForBranchActiveWithCredentials(context.Background(), &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		BranchID:  "branch-1",
	}, tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"})
	if err != nil {
		t.Fatalf("WaitForBranchActiveWithCredentials: %v", err)
	}
	if getCount != 2 {
		t.Fatalf("get count = %d, want 2", getCount)
	}
	if out.Host != "branch.example" || out.Username != "u2.root" {
		t.Fatalf("branch connection = %#v", out)
	}
}

func TestDeprovisionWithCredentialsDeletesCluster(t *testing.T) {
	var gotAuth string
	var deleteCalled bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodDelete || r.URL.Path != "/v1beta1/clusters/cluster-1" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		deleteCalled = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL: ts.URL,
		client: ts.Client(),
	}
	if err := p.DeprovisionWithCredentials(context.Background(), &tenant.ClusterInfo{ClusterID: "cluster-1"}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}); err != nil {
		t.Fatalf("DeprovisionWithCredentials: %v", err)
	}
	if !deleteCalled {
		t.Fatal("delete was not called")
	}
	if !strings.Contains(gotAuth, `username="public-1"`) {
		t.Fatalf("Authorization header did not use request public key: %q", gotAuth)
	}
	if strings.Contains(gotAuth, "private-1") {
		t.Fatalf("Authorization header leaked private key: %q", gotAuth)
	}
}

func TestMarkQuotaUpdateStartedMergesDrive9Labels(t *testing.T) {
	var patchCalled bool
	var gotAuth string
	var order []string
	var gotPatch struct {
		Cluster struct {
			Labels map[string]string `json:"labels"`
		} `json:"cluster"`
		UpdateMask string `json:"updateMask"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/v1beta1/clusters/cluster-1" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		switch r.Method {
		case http.MethodGet:
			order = append(order, "GET")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId": "cluster-1",
				"labels": map[string]string{
					"environment":             "prod",
					Drive9ManagedLabel:        "old",
					Drive9TenantIDLabel:       "old-tenant",
					"drive9.ai/unrelated":     "keep",
					"tidb.cloud/project":      "123",
					"tidb.cloud/organization": "456",
				},
				"spendingLimit": map[string]int32{
					"monthly": 15000,
				},
			})
			return
		case http.MethodPatch:
			order = append(order, "PATCH")
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
		patchCalled = true
		if err := json.NewDecoder(r.Body).Decode(&gotPatch); err != nil {
			t.Fatalf("decode patch: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"clusterId": "cluster-1"})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL: ts.URL,
		client: ts.Client(),
	}
	cfg, err := p.MarkQuotaUpdateStarted(context.Background(), &tenant.ClusterInfo{
		TenantID:  "tenant-1",
		ClusterID: "cluster-1",
	}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("MarkQuotaUpdateStarted: %v", err)
	}
	if !patchCalled {
		t.Fatal("PATCH was not called")
	}
	if len(order) != 2 || order[0] != "GET" || order[1] != "PATCH" {
		t.Fatalf("order = %#v, want GET then PATCH", order)
	}
	if !strings.Contains(gotAuth, `username="public-1"`) {
		t.Fatalf("Authorization header did not use request public key: %q", gotAuth)
	}
	if gotPatch.UpdateMask != "labels" {
		t.Fatalf("updateMask = %q, want labels", gotPatch.UpdateMask)
	}
	labels := gotPatch.Cluster.Labels
	if labels["environment"] != "prod" || labels["drive9.ai/unrelated"] != "keep" {
		t.Fatalf("existing labels were not preserved: %#v", labels)
	}
	if _, ok := labels["tidb.cloud/project"]; ok {
		t.Fatalf("immutable label tidb.cloud/project was not stripped: %#v", labels)
	}
	if _, ok := labels["tidb.cloud/organization"]; ok {
		t.Fatalf("immutable label tidb.cloud/organization was not stripped: %#v", labels)
	}
	if labels[Drive9ManagedLabel] != "true" || labels[Drive9TenantIDLabel] != "tenant-1" {
		t.Fatalf("drive9 labels = %#v", labels)
	}
	if labels[Drive9QuotaUpdateAtLabel] == "" {
		t.Fatalf("%s label was empty: %#v", Drive9QuotaUpdateAtLabel, labels)
	}
	if cfg == nil || cfg.TiDBCloudSpendingLimitMonthly == nil || *cfg.TiDBCloudSpendingLimitMonthly != 15000 {
		t.Fatalf("cloud config = %#v, want spending limit 15000", cfg)
	}
	if cfg.Labels[Drive9ManagedLabel] != "true" || cfg.Labels[Drive9TenantIDLabel] != "tenant-1" {
		t.Fatalf("cloud config labels = %#v", cfg.Labels)
	}
}

func TestUpdateQuotaPatchesSpendingLimitWithoutLabels(t *testing.T) {
	monthly := int64(0)
	var patchCalls int
	var gotSpendingPatch struct {
		Cluster struct {
			SpendingLimit struct {
				Monthly int32 `json:"monthly"`
			} `json:"spendingLimit"`
		} `json:"cluster"`
		UpdateMask string `json:"updateMask"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/v1beta1/clusters/cluster-1" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		switch r.Method {
		case http.MethodPatch:
			patchCalls++
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read patch body: %v", err)
			}
			if err := json.Unmarshal(raw, &gotSpendingPatch); err != nil {
				t.Fatalf("decode spending patch: %v", err)
			}
			if gotSpendingPatch.UpdateMask != "spendingLimit.monthly" {
				t.Fatalf("unexpected update mask %q", gotSpendingPatch.UpdateMask)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"clusterId": "cluster-1"})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL: ts.URL,
		client: ts.Client(),
	}
	cfg, err := p.UpdateQuota(context.Background(), &tenant.ClusterInfo{
		TenantID:  "tenant-1",
		ClusterID: "cluster-1",
	}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}, tenant.QuotaUpdateOptions{
		TiDBCloudSpendingLimitMonthly: &monthly,
	})
	if err != nil {
		t.Fatalf("UpdateQuota: %v", err)
	}
	if patchCalls != 1 {
		t.Errorf("patch calls = %d, want 1", patchCalls)
	}
	if gotSpendingPatch.UpdateMask != "spendingLimit.monthly" || gotSpendingPatch.Cluster.SpendingLimit.Monthly != int32(monthly) {
		t.Errorf("spending patch = %#v", gotSpendingPatch)
	}
	if cfg == nil || cfg.TiDBCloudSpendingLimitMonthly == nil || *cfg.TiDBCloudSpendingLimitMonthly != monthly {
		t.Errorf("cloud config = %#v, want spending limit %d", cfg, monthly)
	}
}

func TestUpdateQuotaReturnsSpendingLimitPatchFailure(t *testing.T) {
	monthly := int64(0)
	var order []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/v1beta1/clusters/cluster-1" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		switch r.Method {
		case http.MethodPatch:
			var probe struct {
				UpdateMask string `json:"updateMask"`
			}
			if err := json.NewDecoder(r.Body).Decode(&probe); err != nil {
				t.Fatalf("decode patch probe: %v", err)
			}
			order = append(order, "PATCH "+probe.UpdateMask)
			if probe.UpdateMask == "labels" {
				t.Fatalf("UpdateQuota should not patch labels")
			}
			http.Error(w, "invalid spending limit", http.StatusBadRequest)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL: ts.URL,
		client: ts.Client(),
	}
	_, err := p.UpdateQuota(context.Background(), &tenant.ClusterInfo{
		TenantID:  "tenant-1",
		ClusterID: "cluster-1",
	}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}, tenant.QuotaUpdateOptions{
		TiDBCloudSpendingLimitMonthly: &monthly,
	})
	if err == nil {
		t.Fatal("UpdateQuota error = nil, want spending limit patch error")
	}
	if len(order) != 1 || order[0] != "PATCH spendingLimit.monthly" {
		t.Fatalf("order = %#v", order)
	}
}

func TestUpdateQuotaRejectsInvalidSpendingLimitBeforeRequest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		monthly int64
	}{
		{name: "negative", monthly: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hit bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hit = true
				http.Error(w, "unexpected request", http.StatusInternalServerError)
			}))
			defer ts.Close()

			p := &Provisioner{
				apiURL: ts.URL,
				client: ts.Client(),
			}
			_, err := p.UpdateQuota(context.Background(), &tenant.ClusterInfo{
				TenantID:  "tenant-1",
				ClusterID: "cluster-1",
			}, tenant.CredentialProvisionRequest{
				PublicKey:  "public-1",
				PrivateKey: "private-1",
			}, tenant.QuotaUpdateOptions{
				TiDBCloudSpendingLimitMonthly: &tc.monthly,
			})
			if err == nil {
				t.Fatal("UpdateQuota error = nil, want spending limit validation error")
			}
			if !strings.Contains(err.Error(), "tidbcloud_spending_limit must be non-negative") {
				t.Fatalf("error = %q", err)
			}
			if hit {
				t.Fatal("UpdateQuota dispatched request after local validation failed")
			}
		})
	}
}

func TestGetQuotaOnlyGetsCluster(t *testing.T) {
	var patchCalled bool
	var getCalled bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.Method {
		case http.MethodGet:
			getCalled = true
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId": "cluster-1",
				"labels":    map[string]string{Drive9ManagedLabel: "true"},
			})
		case http.MethodPatch:
			patchCalled = true
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL: ts.URL,
		client: ts.Client(),
	}
	_, err := p.GetQuota(context.Background(), &tenant.ClusterInfo{ClusterID: "cluster-1"}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("GetQuota: %v", err)
	}
	if !getCalled {
		t.Fatal("GET was not called")
	}
	if patchCalled {
		t.Fatal("PATCH should not be called for read-only quota authorization")
	}
}

func TestGetQuotaReadsSpendingLimit(t *testing.T) {
	var patchCalled bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId": "cluster-1",
				"labels":    map[string]string{Drive9ManagedLabel: "true"},
				"spendingLimit": map[string]int32{
					"monthly": 15000,
				},
			})
		case http.MethodPatch:
			patchCalled = true
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL: ts.URL,
		client: ts.Client(),
	}
	cfg, err := p.GetQuota(context.Background(), &tenant.ClusterInfo{ClusterID: "cluster-1"}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("GetQuota: %v", err)
	}
	if patchCalled {
		t.Fatal("PATCH should not be called for quota query")
	}
	if cfg == nil || cfg.TiDBCloudSpendingLimitMonthly == nil || *cfg.TiDBCloudSpendingLimitMonthly != 15000 {
		t.Fatalf("quota cloud config = %#v, want spending limit 15000", cfg)
	}
}

func TestQuotaCredentialErrorsMapForbiddenAndNotFound(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		target     error
	}{
		{name: "unauthorized", statusCode: http.StatusUnauthorized, target: tenant.ErrQuotaPermissionDenied},
		{name: "forbidden", statusCode: http.StatusForbidden, target: tenant.ErrQuotaPermissionDenied},
		{name: "not_found", statusCode: http.StatusNotFound, target: tenant.ErrQuotaBackendNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") == "" {
					w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				http.Error(w, "upstream says no", tc.statusCode)
			}))
			defer ts.Close()

			p := &Provisioner{
				apiURL: ts.URL,
				client: ts.Client(),
			}
			_, err := p.GetQuota(context.Background(), &tenant.ClusterInfo{ClusterID: "cluster-1"}, tenant.CredentialProvisionRequest{
				PublicKey:  "public-1",
				PrivateKey: "private-1",
			})
			if !errors.Is(err, tc.target) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, tc.target)
			}
		})
	}
}

func TestRegionNameAcceptsFullRegionResourceName(t *testing.T) {
	p := &Provisioner{cloudProvider: "alicloud", region: "regions/alicloud-ap-southeast-1"}
	if got := p.regionName(); got != "regions/alicloud-ap-southeast-1" {
		t.Fatalf("regionName = %q", got)
	}
}

func TestClusterDisplayNameMatchesSwaggerContract(t *testing.T) {
	got := clusterDisplayName("tenant_with_invalid_chars_and_a_very_long_suffix_that_exceeds_the_swagger_limit_1234567890")
	if len(got) < 4 || len(got) > 64 {
		t.Fatalf("display name length = %d for %q", len(got), got)
	}
	if !strings.HasPrefix(got, "tidbcloud-fs-") {
		t.Fatalf("display name = %q, want tidbcloud-fs prefix", got)
	}
	matched, err := regexp.MatchString(`^[A-Za-z0-9][-A-Za-z0-9]{2,62}[A-Za-z0-9]$`, got)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatalf("display name %q does not match swagger pattern", got)
	}
}

func TestSystemUsernameForCurrent(t *testing.T) {
	got, needsSetup, err := systemUsernameForCurrent("22ipQWBXXq2wN2S.root")
	if err != nil {
		t.Fatalf("systemUsernameForCurrent: %v", err)
	}
	if !needsSetup || got != "22ipQWBXXq2wN2S.tdc_fs_sys" {
		t.Fatalf("system username = %q setup=%v", got, needsSetup)
	}
	got, needsSetup, err = systemUsernameForCurrent("22ipQWBXXq2wN2S.tdc_fs_sys")
	if err != nil {
		t.Fatalf("systemUsernameForCurrent existing: %v", err)
	}
	if needsSetup || got != "22ipQWBXXq2wN2S.tdc_fs_sys" {
		t.Fatalf("existing system username = %q setup=%v", got, needsSetup)
	}
	if _, _, err := systemUsernameForCurrent(""); err == nil {
		t.Fatal("expected empty username error")
	}
	for _, username := range []string{"root", "u1.admin"} {
		if _, _, err := systemUsernameForCurrent(username); err == nil {
			t.Fatalf("expected unexpected username %q to be rejected", username)
		}
	}
}

func TestSystemUserStatements(t *testing.T) {
	got := systemUserStatements("tidbcloud_fs", "22ipQWBXXq2wN2S.tdc_fs_sys", "pass123")
	want := []string{
		"CREATE DATABASE IF NOT EXISTS `tidbcloud_fs`",
		"CREATE ROLE IF NOT EXISTS 'tdc_fs_admin'",
		"GRANT CREATE, ALTER, DROP, INDEX, SELECT, INSERT, UPDATE, DELETE ON `tidbcloud_fs`.* TO 'tdc_fs_admin'",
		"CREATE USER IF NOT EXISTS '22ipQWBXXq2wN2S.tdc_fs_sys' IDENTIFIED BY 'pass123'",
		"ALTER USER '22ipQWBXXq2wN2S.tdc_fs_sys' IDENTIFIED BY 'pass123'",
		"GRANT 'tdc_fs_admin' TO '22ipQWBXXq2wN2S.tdc_fs_sys'",
		"SET DEFAULT ROLE 'tdc_fs_admin' TO '22ipQWBXXq2wN2S.tdc_fs_sys'",
	}
	if len(got) != len(want) {
		t.Fatalf("statement count = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statement %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSQLQuoting(t *testing.T) {
	if got := quoteIdent("db`name"); got != "`db``name`" {
		t.Fatalf("quoteIdent = %q", got)
	}
	if got := quoteString(`u'ser\name`); got != `'u''ser\\name'` {
		t.Fatalf("quoteString = %q", got)
	}
}

func TestWaitForBranchUserWithCredentialsPollsUserPrefix(t *testing.T) {
	polls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		polls++
		if polls < 3 {
			_ = json.NewEncoder(w).Encode(map[string]string{
				"branchId": "branch-1",
				"state":    "CREATING",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"branchId":   "branch-1",
			"state":      "ACTIVE",
			"userPrefix": "u3",
			"endpoints":  map[string]any{"public": map[string]any{"host": "branch.example", "port": 4000}},
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	username, err := p.WaitForBranchUserWithCredentials(context.Background(), "cluster-1", "branch-1", tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"})
	if err != nil {
		t.Fatalf("WaitForBranchUserWithCredentials: %v", err)
	}
	if username != "u3.root" {
		t.Fatalf("username = %q, want u3.root", username)
	}
	if polls != 3 {
		t.Fatalf("polls = %d, want 3", polls)
	}
}

func TestWaitForBranchUserWithCredentialsUsesUserPrefix(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"branchId":   "branch-1",
			"state":      "ACTIVE",
			"userPrefix": "u2",
			"endpoints": map[string]any{
				"public": map[string]any{"host": "branch.example", "port": 4000},
			},
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		defaultDatabaseName: DefaultDatabaseName,
		client:              ts.Client(),
	}
	username, err := p.WaitForBranchUserWithCredentials(context.Background(), "cluster-1", "branch-1", tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"})
	if err != nil {
		t.Fatalf("WaitForBranchUserWithCredentials: %v", err)
	}
	if username != "u2.root" {
		t.Fatalf("username = %q, want u2.root", username)
	}
}

func TestNewProvisionerFromEnvReadsPrivateEndpointFlag(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "aws")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")

	defaultUse := false
	p, err := NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv: %v", err)
	}
	if p.usePrivateEndpoint != defaultUse {
		t.Fatalf("usePrivateEndpoint = %v, want %v (default)", p.usePrivateEndpoint, defaultUse)
	}

	t.Setenv(EnvTiDBCloudNativeUsePrivateEndpoint, "true")
	p, err = NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv with true: %v", err)
	}
	if !p.usePrivateEndpoint {
		t.Fatalf("usePrivateEndpoint = %v, want true", p.usePrivateEndpoint)
	}

	t.Setenv(EnvTiDBCloudNativeUsePrivateEndpoint, "1")
	p, err = NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv with 1: %v", err)
	}
	if !p.usePrivateEndpoint {
		t.Fatalf("usePrivateEndpoint = %v, want true", p.usePrivateEndpoint)
	}

	t.Setenv(EnvTiDBCloudNativeUsePrivateEndpoint, "no")
	p, err = NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv with no: %v", err)
	}
	if p.usePrivateEndpoint {
		t.Fatalf("usePrivateEndpoint = %v, want false", p.usePrivateEndpoint)
	}
}

func TestClusterConnectionIncompleteWhenPrivateEndpointMissing(t *testing.T) {
	info := &clusterInfo{
		ClusterID:  "cluster-1",
		UserPrefix: "u1",
	}
	info.Endpoints.Public.Host = "public.example"
	info.Endpoints.Public.Port = 4000
	info.Endpoints.Private.Host = ""
	info.Endpoints.Private.Port = 4000

	if clusterConnectionIncomplete(info, false) {
		t.Fatalf("public mode should report complete")
	}
	if !clusterConnectionIncomplete(info, true) {
		t.Fatalf("private mode should report incomplete when private host is empty")
	}
}

func TestProvisionWithCredentialsUsesPrivateEndpoint(t *testing.T) {
	var pollCount int
	origEnsureDatabase := ensureDatabaseFunc
	ensureDatabaseFunc = func(context.Context, string, string, string, int, string) error {
		return nil
	}
	t.Cleanup(func() { ensureDatabaseFunc = origEnsureDatabase })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		pollCount++
		if r.URL.Path != "/v1beta1/clusters" && pollCount == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId": "cluster-1",
				"state":     "CREATING",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"clusterId":  "cluster-1",
			"state":      "ACTIVE",
			"userPrefix": "u1",
			"labels":     map[string]string{TiDBCloudOrganizationLabel: "org-1"},
			"endpoints": map[string]any{
				"public":  map[string]any{"host": "public.example", "port": 4000},
				"private": map[string]any{"host": "private.internal", "port": 4001},
			},
		})
	}))
	defer ts.Close()

	p := &Provisioner{
		apiURL:              ts.URL,
		defaultDatabaseName: DefaultDatabaseName,
		usePrivateEndpoint:  true,
		client:              ts.Client(),
	}
	res, _, err := p.ProvisionWithCredentialsAndQuota(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey: "public-1", PrivateKey: "private-1",
	}, tenant.QuotaUpdateOptions{})
	if err != nil {
		t.Fatalf("ProvisionWithCredentialsAndQuota: %v", err)
	}
	if res.Host != "private.internal" {
		t.Fatalf("Host = %q, want private.internal", res.Host)
	}
	if res.Port != 4001 {
		t.Fatalf("Port = %d, want 4001", res.Port)
	}
}

func TestBranchConnectionIncompleteWhenPrivateEndpointMissing(t *testing.T) {
	branch := &branchInfo{
		BranchID:   "branch-1",
		UserPrefix: "u1",
	}
	branch.Endpoints.Public.Host = "public.example"
	branch.Endpoints.Public.Port = 4000
	branch.Endpoints.Private.Host = ""
	branch.Endpoints.Private.Port = 4000

	if branchConnectionIncomplete(branch, false) {
		t.Fatalf("public mode should report complete")
	}
	if !branchConnectionIncomplete(branch, true) {
		t.Fatalf("private mode should report incomplete when private host is empty")
	}
}

func TestFillBranchEndpointUsesPrivateEndpoint(t *testing.T) {
	branch := &branchInfo{
		BranchID:   "branch-1",
		UserPrefix: "u1",
	}
	branch.Endpoints.Public.Host = "public.example"
	branch.Endpoints.Public.Port = 4000
	branch.Endpoints.Private.Host = "private.internal"
	branch.Endpoints.Private.Port = 4001

	p := &Provisioner{usePrivateEndpoint: true}
	out := &tenant.ClusterInfo{}
	if err := p.fillBranchEndpoint(out, branch); err != nil {
		t.Fatalf("fillBranchEndpoint: %v", err)
	}
	if out.Host != "private.internal" {
		t.Fatalf("Host = %q, want private.internal", out.Host)
	}
	if out.Port != 4001 {
		t.Fatalf("Port = %d, want 4001", out.Port)
	}
	if out.Username != "u1.root" {
		t.Fatalf("Username = %q, want u1.root", out.Username)
	}
}
