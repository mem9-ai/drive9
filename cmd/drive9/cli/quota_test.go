package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestQuotaGetRequiresTenantIDAndTiDBCloudCredentials(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)
	resetCredentialCacheForTest()

	err := Quota([]string{"get", "--json"})
	if err == nil {
		t.Fatal("Quota get error = nil, want tenant-id error")
	}
	if !strings.Contains(err.Error(), "--tenant-id is required") {
		t.Fatalf("error = %q", err)
	}
}

func TestQuotaGetUsesTenantIDAndTiDBCloudHeaders(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotAuth string
	var gotPublicKey string
	var gotPrivateKey string
	var gotTenantID string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		gotPublicKey = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPrivateKey = r.Header.Get("X-TiDBCloud-Private-Key")
		gotTenantID = r.URL.Query().Get("tenant_id")
		_ = json.NewEncoder(w).Encode(quotaTestResponse("tenant-1"))
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	resetCredentialCacheForTest()

	stdout, err := captureStdoutE(t, func() error {
		return Quota([]string{
			"get",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Quota get: %v", err)
	}
	if !strings.Contains(stdout, "TenantID:") || !strings.Contains(stdout, "tenant-1") {
		t.Fatalf("stdout should include tenant ID: %s", stdout)
	}
	if !strings.Contains(stdout, "MaxStorage:") || !strings.Contains(stdout, "1000 Mi") {
		t.Fatalf("stdout should include max storage: %s", stdout)
	}
	if !strings.Contains(stdout, "MaxFileSize:") || !strings.Contains(stdout, "64 Mi") {
		t.Fatalf("stdout should include max file size: %s", stdout)
	}
	if !strings.Contains(stdout, "MaxFileCount:") || !strings.Contains(stdout, "42") {
		t.Fatalf("stdout should include max file count: %s", stdout)
	}
	if !strings.Contains(stdout, "SpendingLimit:") || !strings.Contains(stdout, "100.00") {
		t.Fatalf("stdout should include spending limit: %s", stdout)
	}
	if !strings.Contains(stdout, "StorageUsed:") || !strings.Contains(stdout, "1 B") {
		t.Fatalf("stdout should include storage used: %s", stdout)
	}
	if !strings.Contains(stdout, "Reserved:") || !strings.Contains(stdout, "2 B") {
		t.Fatalf("stdout should include reserved: %s", stdout)
	}
	if !strings.Contains(stdout, "FileCount:") || !strings.Contains(stdout, "3") {
		t.Fatalf("stdout should include file count: %s", stdout)
	}
	if strings.Contains(stdout, "media_file_count") || strings.Contains(stdout, "monthly_cost_mc") {
		t.Fatalf("stdout should not include media or cost counters: %s", stdout)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
	if gotTenantID != "tenant-1" || gotPublicKey != "public-1" || gotPrivateKey != "private-1" {
		t.Fatalf("request credentials tenant=%q public=%q private=%q", gotTenantID, gotPublicKey, gotPrivateKey)
	}
}

func TestQuotaGetRejectsEnvTiDBCloudCredentialsWithoutTenantID(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	t.Setenv(EnvAPIKey, "drive9-key-should-not-be-used")
	t.Setenv(EnvTiDBCloudPublicKey, "env-public")
	t.Setenv(EnvTiDBCloudPrivateKey, "env-private")
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Quota([]string{"get", "--json"})
	}); err == nil || !strings.Contains(err.Error(), "--tenant-id is required") {
		t.Fatalf("Quota get error = %v, want tenant-id error", err)
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
	t.Setenv(EnvAPIKey, "drive9-key-should-not-be-used")
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Quota([]string{
			"set",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--max-storage-size", "1000",
			"--tidbcloud-spending-limit", "20000",
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
	if gotBody["max_storage_size"] != float64(1000) {
		t.Fatalf("body quota = %#v", gotBody)
	}
	if gotBody["tidbcloud_spending_limit"] != float64(20000) {
		t.Fatalf("body spending limit = %#v", gotBody)
	}
}

func TestQuotaSetAllowsSpendingLimitOnly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	spendingLimit := int64(0)
	var gotBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
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
			"set",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--tidbcloud-spending-limit", strconv.FormatInt(spendingLimit, 10),
		})
	}); err != nil {
		t.Fatalf("Quota set: %v", err)
	}
	if _, ok := gotBody["max_storage_size"]; ok {
		t.Fatalf("body should not contain max_storage_size: %#v", gotBody)
	}
	if gotBody["tidbcloud_spending_limit"] != float64(spendingLimit) {
		t.Fatalf("body spending limit = %#v", gotBody)
	}
}

func TestQuotaSetAllowsStorageSizeOnly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
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
			"set",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--max-storage-size", "1000",
		})
	}); err != nil {
		t.Fatalf("Quota set: %v", err)
	}
	if gotBody["max_storage_size"] != float64(1000) {
		t.Fatalf("body quota = %#v", gotBody)
	}
	if _, ok := gotBody["tidbcloud_spending_limit"]; ok {
		t.Fatalf("body should not contain tidbcloud_spending_limit: %#v", gotBody)
	}
}

func TestQuotaSetAllowsFileLimitsOnly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
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
			"set",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--max-file-size", "64",
			"--max-file-count", "42",
		})
	}); err != nil {
		t.Fatalf("Quota set: %v", err)
	}
	if gotBody["max_file_size"] != float64(64) || gotBody["max_file_count"] != float64(42) {
		t.Fatalf("body file limits = %#v", gotBody)
	}
	if _, ok := gotBody["max_storage_size"]; ok {
		t.Fatalf("body should not contain max_storage_size: %#v", gotBody)
	}
}

func TestQuotaSetAllowsZeroFileCount(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(quotaTestResponse("tenant-1"))
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Quota([]string{
			"set",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--max-file-count", "0",
		})
	}); err != nil {
		t.Fatalf("Quota set: %v", err)
	}
	if gotBody["max_file_count"] != float64(0) {
		t.Fatalf("body = %#v", gotBody)
	}
}

func TestQuotaSetRegionCodeSelectsTiDBCloudServer(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var nativeHits int32
	var starterHits int32
	var gotBody map[string]any
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nativeHits, 1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/quota" {
			t.Fatalf("unexpected native request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(quotaTestResponse("tenant-1"))
	}))
	defer native.Close()
	starter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&starterHits, 1)
		http.Error(w, "starter server should not be used", http.StatusInternalServerError)
	}))
	defer starter.Close()
	manifest := newRegionManifestTestServer(t, []RegionManifestEntry{
		{
			RegionCode: "aws-us-east-1",
			Mode:       RegionModeTiDBCloudStarter,
			ServerURL:  starter.URL,
		},
		{
			RegionCode: "aws-us-east-1",
			Mode:       RegionModeTiDBCloudNative,
			ServerURL:  native.URL,
		},
	})
	defer manifest.Close()

	t.Setenv(EnvRegionManifestURL, manifest.URL)
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Quota([]string{
			"set",
			"--region-code", "aws-us-east-1",
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--max-storage-size", "1000",
		})
	}); err != nil {
		t.Fatalf("Quota set: %v", err)
	}
	if atomic.LoadInt32(&nativeHits) != 1 {
		t.Fatalf("native hits = %d, want 1", nativeHits)
	}
	if atomic.LoadInt32(&starterHits) != 0 {
		t.Fatalf("starter hits = %d, want 0", starterHits)
	}
	if gotBody["tenant_id"] != "tenant-1" || gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("body credentials = %#v", gotBody)
	}
	if gotBody["max_storage_size"] != float64(1000) {
		t.Fatalf("body quota = %#v", gotBody)
	}
}

func TestQuotaSetRejectsMissingTiDBCloudCredentials(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	err := Quota([]string{
		"set",
		"--tenant-id", "tenant-1",
		"--max-storage-size", "1000",
	})
	if err == nil {
		t.Fatal("Quota set error = nil, want missing credential error")
	}
	if !strings.Contains(err.Error(), "TiDB Cloud credentials are required") {
		t.Fatalf("error = %q", err)
	}
}

func TestQuotaSetRejectsMissingQuotaKnob(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	err := Quota([]string{
		"set",
		"--tenant-id", "tenant-1",
		"--tidbcloud-public-key", "public-1",
		"--tidbcloud-private-key", "private-1",
	})
	if err == nil {
		t.Fatal("Quota set error = nil, want missing quota knob error")
	}
	if !strings.Contains(err.Error(), "--max-storage-size, --max-file-size, --max-file-count, or --tidbcloud-spending-limit") {
		t.Fatalf("error = %q", err)
	}
}

func TestQuotaSetRejectsInvalidQuotaValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flag    string
		value   string
		wantErr string
	}{
		{name: "zero_storage_size", flag: "--max-storage-size", value: "0", wantErr: "--max-storage-size must be positive"},
		{name: "negative_storage_size", flag: "--max-storage-size", value: "-1", wantErr: "--max-storage-size must be positive"},
		{name: "zero_file_size", flag: "--max-file-size", value: "0", wantErr: "--max-file-size must be positive"},
		{name: "negative_file_size", flag: "--max-file-size", value: "-1", wantErr: "--max-file-size must be positive"},
		{name: "negative_file_count", flag: "--max-file-count", value: "-1", wantErr: "--max-file-count must be non-negative"},
		{name: "negative_spending_limit", flag: "--tidbcloud-spending-limit", value: "-1", wantErr: "--tidbcloud-spending-limit must be non-negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			clearProvisionEnv(t)
			resetCredentialCacheForTest()

			var hit bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hit = true
				http.Error(w, "unexpected request", http.StatusInternalServerError)
			}))
			defer ts.Close()
			t.Setenv(EnvServer, ts.URL)

			err := Quota([]string{
				"set",
				"--tenant-id", "tenant-1",
				"--tidbcloud-public-key", "public-1",
				"--tidbcloud-private-key", "private-1",
				tc.flag, tc.value,
			})
			if err == nil {
				t.Fatal("Quota set error = nil, want positive quota value error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q", err)
			}
			if hit {
				t.Fatal("quota set dispatched request after local validation failed")
			}
		})
	}
}

func TestQuotaGetStatusErrorIncludesHTTPCode(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"no permission to query quota with TiDB Cloud API key"}`))
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	resetCredentialCacheForTest()

	err := Quota([]string{
		"get",
		"--tenant-id", "tenant-1",
		"--tidbcloud-public-key", "public-1",
		"--tidbcloud-private-key", "private-1",
	})
	if err == nil {
		t.Fatal("Quota get error = nil, want status error")
	}
	if !strings.Contains(err.Error(), "query quota failed (HTTP 403): no permission to query quota with TiDB Cloud API key") {
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
			"max_storage_size":         1000,
			"max_file_size":            64,
			"max_file_count":           42,
			"tidbcloud_spending_limit": 10000,
		},
		"usage": map[string]any{
			"storage_bytes":  1,
			"reserved_bytes": 2,
			"file_count":     3,
		},
	}
}
