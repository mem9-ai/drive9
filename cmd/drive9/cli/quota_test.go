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

func TestQuotaSetSendsTiDBCloudCredentialBody(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotBody map[string]any
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/tenants/tenant-1/quota" {
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
		return Admin([]string{"tenant",
			"set-quota",
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
	if gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
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
		if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/tenants/tenant-1/quota" {
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
		return Admin([]string{"tenant",
			"set-quota",
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
		if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/tenants/tenant-1/quota" {
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
		return Admin([]string{"tenant",
			"set-quota",
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
		if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/tenants/tenant-1/quota" {
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
		return Admin([]string{"tenant",
			"set-quota",
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
		return Admin([]string{"tenant",
			"set-quota",
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
		if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/tenants/tenant-1/quota" {
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
		return Admin([]string{"tenant",
			"set-quota",
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
	if gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("body credentials = %#v", gotBody)
	}
	if gotBody["max_storage_size"] != float64(1000) {
		t.Fatalf("body quota = %#v", gotBody)
	}
}

func TestQuotaSetRejectsMissingTiDBCloudCredentials(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	err := Admin([]string{"tenant",
		"set-quota",
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

	err := Admin([]string{"tenant",
		"set-quota",
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

			err := Admin([]string{"tenant",
				"set-quota",
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

func quotaTestResponse(tenantID string) map[string]any {
	return map[string]any{
		"tenant_id": tenantID,
		"status":    "active",
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
