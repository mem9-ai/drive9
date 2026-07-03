package starter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestNewLegacyProvisionerFromEnvReadsNativeURLAndDAT9Secret(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com/")
	t.Setenv(EnvTiDBCloudAPIKey, "key-1")
	t.Setenv(EnvTiDBCloudDAT9APISecret, "secret-1")
	t.Setenv(EnvTiDBCloudLegacyAPISecret, "legacy-secret")

	p, err := NewLegacyProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewLegacyProvisionerFromEnv: %v", err)
	}
	if p.apiURL != "https://serverless.tidbapi.com" {
		t.Fatalf("apiURL = %q", p.apiURL)
	}
	if p.apiKey != "key-1" || p.apiSecret != "secret-1" {
		t.Fatalf("credentials = %q/%q", p.apiKey, p.apiSecret)
	}
}

func TestNewLegacyProvisionerFromEnvFallsBackToHistoricalSecret(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudAPIKey, "key-1")
	t.Setenv(EnvTiDBCloudDAT9APISecret, "")
	t.Setenv(EnvTiDBCloudLegacyAPISecret, "legacy-secret")

	p, err := NewLegacyProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewLegacyProvisionerFromEnv: %v", err)
	}
	if p.apiSecret != "legacy-secret" {
		t.Fatalf("apiSecret = %q", p.apiSecret)
	}
}

func TestNewLegacyProvisionerFromEnvFallsBackToHistoricalURL(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "")
	t.Setenv(EnvTiDBCloudLegacyAPIURL, "https://starter.tidbapi.com/")
	t.Setenv(EnvTiDBCloudAPIKey, "key-1")
	t.Setenv(EnvTiDBCloudDAT9APISecret, "secret-1")
	t.Setenv(EnvTiDBCloudLegacyAPISecret, "")

	p, err := NewLegacyProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewLegacyProvisionerFromEnv: %v", err)
	}
	if p.apiURL != "https://starter.tidbapi.com" {
		t.Fatalf("apiURL = %q", p.apiURL)
	}
}

func TestNewLegacyProvisionerFromEnvRequiresOldCredentials(t *testing.T) {
	t.Setenv(EnvTiDBCloudNativeAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(EnvTiDBCloudAPIKey, "")
	t.Setenv(EnvTiDBCloudDAT9APISecret, "secret-1")
	t.Setenv(EnvTiDBCloudLegacyAPISecret, "")

	if _, err := NewLegacyProvisionerFromEnv(); err == nil {
		t.Fatal("NewLegacyProvisionerFromEnv error = nil, want missing key error")
	}
	if !LegacyEnvPresent() {
		t.Fatal("LegacyEnvPresent should report partial legacy env")
	}
}

func TestLegacyDeprovisionUsesDigestDelete(t *testing.T) {
	var sawChallenge bool
	var sawAuthorized bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta1/clusters/cluster-a" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("Authorization") == "" {
			sawChallenge = true
			w.Header().Set("WWW-Authenticate", `Digest realm="tidb", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		sawAuthorized = true
		if !strings.Contains(r.Header.Get("Authorization"), `username="key-1"`) {
			t.Fatalf("authorization = %s", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	p := &LegacyProvisioner{
		apiURL:    ts.URL,
		apiKey:    "key-1",
		apiSecret: "secret-1",
		client:    ts.Client(),
	}
	err := p.Deprovision(context.Background(), &tenant.ClusterInfo{ClusterID: "cluster-a"})
	if err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if !sawChallenge || !sawAuthorized {
		t.Fatalf("digest flow sawChallenge=%v sawAuthorized=%v", sawChallenge, sawAuthorized)
	}
}
