package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQuotaGetUsesOwnerAPIKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(quotaTestResponse("tenant-1"))
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	t.Setenv(EnvAPIKey, "owner-key")
	resetCredentialCacheForTest()

	out, err := captureStdoutE(t, func() error {
		return Quota([]string{"get", "--json"})
	})
	if err != nil {
		t.Fatalf("Quota get: %v", err)
	}
	if gotAuth != "Bearer owner-key" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if !strings.Contains(out, `"tenant_id": "tenant-1"`) {
		t.Fatalf("output = %q", out)
	}
}

func TestQuotaGetWithTenantIDUsesCredentialQuery(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/quota/query" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(quotaTestResponse("tenant-1"))
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Quota([]string{
			"get",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	}); err != nil {
		t.Fatalf("Quota get credential query: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
	if gotBody["tenant_id"] != "tenant-1" || gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("body = %#v", gotBody)
	}
}

func TestQuotaGetIgnoresEnvTiDBCloudCredentialsWithoutTenantID(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotPath string
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if r.Method != http.MethodGet || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(quotaTestResponse("tenant-1"))
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	t.Setenv(EnvAPIKey, "owner-key")
	t.Setenv(EnvTiDBCloudPublicKey, "env-public")
	t.Setenv(EnvTiDBCloudPrivateKey, "env-private")
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Quota([]string{"get", "--json"})
	}); err != nil {
		t.Fatalf("Quota get: %v", err)
	}
	if gotPath != "/v1/quota" {
		t.Fatalf("path = %q, want /v1/quota", gotPath)
	}
	if gotAuth != "Bearer owner-key" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
}

func TestQuotaGetRejectsTiDBCloudCredentialFlagsWithoutTenantID(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)
	resetCredentialCacheForTest()

	err := Quota([]string{"get", "--tidbcloud-public-key", "public-1", "--tidbcloud-private-key", "private-1"})
	if err == nil {
		t.Fatal("Quota get error = nil, want tenant-id error")
	}
	if !strings.Contains(err.Error(), "--tenant-id is required") {
		t.Fatalf("error = %q", err)
	}
}

func TestQuotaSetSendsTiDBCloudCredentialBody(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(quotaTestResponse("tenant-1"))
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	t.Setenv(EnvAPIKey, "owner-key-should-not-be-used")
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Quota([]string{
			"set",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--max-storage-bytes", "1000",
			"--max-media-llm-files", "50",
			"--max-monthly-cost-mc", "0",
		})
	}); err != nil {
		t.Fatalf("Quota set: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
	if gotBody["tenant_id"] != "tenant-1" || gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("body credentials = %#v", gotBody)
	}
	if gotBody["max_storage_bytes"] != float64(1000) || gotBody["max_media_llm_files"] != float64(50) || gotBody["max_monthly_cost_mc"] != float64(0) {
		t.Fatalf("body quota = %#v", gotBody)
	}
}

func TestQuotaSetRejectsMissingTiDBCloudCredentials(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	err := Quota([]string{
		"set",
		"--tenant-id", "tenant-1",
		"--max-storage-bytes", "1000",
	})
	if err == nil {
		t.Fatal("Quota set error = nil, want missing credential error")
	}
	if !strings.Contains(err.Error(), "TiDB Cloud credentials are required") {
		t.Fatalf("error = %q", err)
	}
}

func quotaTestResponse(tenantID string) map[string]any {
	return map[string]any{
		"tenant_id":       tenantID,
		"provider":        "tidb_cloud_native",
		"status":          "active",
		"supports_update": true,
		"config": map[string]any{
			"max_storage_bytes":   1000,
			"max_media_llm_files": 50,
			"max_monthly_cost_mc": 0,
		},
		"usage": map[string]any{
			"storage_bytes":    1,
			"reserved_bytes":   2,
			"media_file_count": 3,
			"monthly_cost_mc":  4,
		},
	}
}
