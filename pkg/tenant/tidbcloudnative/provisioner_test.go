package tidbcloudnative

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/tenant"
)

func TestNewProvisionerFromEnvReadsServerSideConfigOnly(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudNativeCloudProvider, "aws")
	t.Setenv(EnvTiDBCloudNativeRegion, "us-east-1")
	t.Setenv(EnvTiDBCloudNativeDefaultDatabaseName, "drive9_db")
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

func TestProvisionWithCredentialsUsesRequestCredentialsAndServerConfig(t *testing.T) {
	var ensureDBCalled bool
	var pollCount int
	origEnsureDatabase := ensureDatabaseFunc
	ensureDatabaseFunc = func(_ context.Context, user, password, host string, port int, dbName string) error {
		ensureDBCalled = true
		if user != "u1.root" || password == "" || host != "db.example" || port != 4000 || dbName != "customer_db" {
			t.Fatalf("ensure database args = %s/%s %s:%d %s", user, password, host, port, dbName)
		}
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
				"state":      "ACTIVE",
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
		client:              ts.Client(),
	}
	out, err := p.ProvisionWithCredentials(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey:    "public-1",
		PrivateKey:   "private-1",
		DatabaseName: "customer_db",
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
	if gotBody.DisplayName != "drive9-tenant-1" {
		t.Fatalf("displayName = %q", gotBody.DisplayName)
	}
	if gotBody.Region.Name != "regions/aws-us-east-1" {
		t.Fatalf("region.name = %q", gotBody.Region.Name)
	}
	if gotBody.RootPassword == "" {
		t.Fatal("rootPassword was empty")
	}
	if out.ClusterID != "cluster-1" || out.Username != "u1.root" || out.DBName != "customer_db" || out.Provider != tenant.ProviderTiDBCloudNative {
		t.Fatalf("unexpected cluster info: %#v", out)
	}
	if !ensureDBCalled {
		t.Fatal("ensure database was not called")
	}
	if pollCount < 2 {
		t.Fatalf("poll count = %d, want at least 2", pollCount)
	}
}

func TestProvisionWithCredentialsDefaultsDatabaseName(t *testing.T) {
	var ensuredDB string
	origEnsureDatabase := ensureDatabaseFunc
	ensureDatabaseFunc = func(_ context.Context, _ string, _ string, _ string, _ int, dbName string) error {
		ensuredDB = dbName
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
	if _, err := p.ProvisionWithCredentials(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}); err != nil {
		t.Fatalf("ProvisionWithCredentials: %v", err)
	}
	if ensuredDB != DefaultDatabaseName {
		t.Fatalf("ensured database = %q, want %q", ensuredDB, DefaultDatabaseName)
	}
}

func TestProvisionWithCredentialsRejectsReservedDatabaseName(t *testing.T) {
	p := &Provisioner{defaultDatabaseName: DefaultDatabaseName}
	_, err := p.ProvisionWithCredentials(context.Background(), "tenant-1", tenant.CredentialProvisionRequest{
		PublicKey:    "public-1",
		PrivateKey:   "private-1",
		DatabaseName: "test",
	})
	if err == nil {
		t.Fatal("expected reserved database_name error")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("unexpected error: %v", err)
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
	matched, err := regexp.MatchString(`^[A-Za-z0-9][-A-Za-z0-9]{2,62}[A-Za-z0-9]$`, got)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatalf("display name %q does not match swagger pattern", got)
	}
}
