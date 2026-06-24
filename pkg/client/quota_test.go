package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetQuotaSendsHeadersAndDecodesResponse(t *testing.T) {
	t.Parallel()

	var gotAuth string
	var gotPublicKey string
	var gotPrivateKey string
	var gotTenantID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/v1/quota" {
			t.Fatalf("path = %q, want /v1/quota", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		gotPublicKey = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPrivateKey = r.Header.Get("X-TiDBCloud-Private-Key")
		gotTenantID = r.URL.Query().Get("tenant_id")
		_ = json.NewEncoder(w).Encode(quotaClientTestResponse("tenant-1"))
	}))
	defer srv.Close()

	resp, err := New(srv.URL, "").GetQuota(context.Background(), QuotaRequest{
		TenantID:   "tenant-1",
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("GetQuota: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
	if gotTenantID != "tenant-1" || gotPublicKey != "public-1" || gotPrivateKey != "private-1" {
		t.Fatalf("request credentials tenant=%q public=%q private=%q", gotTenantID, gotPublicKey, gotPrivateKey)
	}
	if resp.Provider != "tidb_cloud_native" {
		t.Fatalf("provider = %q", resp.Provider)
	}
	if resp.Config.TiDBCloudSpendingLimit == nil || *resp.Config.TiDBCloudSpendingLimit != 10000 {
		t.Fatalf("spending limit = %#v, want 10000", resp.Config.TiDBCloudSpendingLimit)
	}
	if resp.Config.MaxStorageSize != 1000 || resp.Config.MaxFileSize != 64 || resp.Config.MaxFileCount != 42 {
		t.Fatalf("config = %#v", resp.Config)
	}
	if resp.Usage.StorageBytes != 1 || resp.Usage.ReservedBytes != 2 || resp.Usage.FileCount != 3 {
		t.Fatalf("usage = %#v", resp.Usage)
	}
}

func TestSetQuotaPostsPartialFieldsAndDecodesResponse(t *testing.T) {
	t.Parallel()

	storageSize := int64(1000)
	var gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/quota" {
			t.Fatalf("path = %q, want /v1/quota", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(quotaClientTestResponse("tenant-1"))
	}))
	defer srv.Close()

	resp, err := New(srv.URL, "").SetQuota(context.Background(), QuotaSetRequest{
		TenantID:       "tenant-1",
		PublicKey:      "public-1",
		PrivateKey:     "private-1",
		MaxStorageSize: &storageSize,
	})
	if err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
	if gotBody["max_storage_size"] != float64(1000) {
		t.Fatalf("request body = %#v", gotBody)
	}
	if len(gotBody) != 4 {
		t.Fatalf("request body = %#v, want only tenant_id, public_key, private_key, and max_storage_size", gotBody)
	}
	if resp.Config.MaxStorageSize != 1000 {
		t.Fatalf("max storage size = %d, want 1000", resp.Config.MaxStorageSize)
	}
	if resp.Usage.StorageBytes != 1 || resp.Usage.ReservedBytes != 2 || resp.Usage.FileCount != 3 {
		t.Fatalf("usage = %#v", resp.Usage)
	}
}

func TestSetQuotaPostsFileLimits(t *testing.T) {
	t.Parallel()

	fileSize := int64(64)
	fileCount := int64(42)
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(quotaClientTestResponse("tenant-1"))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").SetQuota(context.Background(), QuotaSetRequest{
		TenantID:     "tenant-1",
		PublicKey:    "public-1",
		PrivateKey:   "private-1",
		MaxFileSize:  &fileSize,
		MaxFileCount: &fileCount,
	})
	if err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	if gotBody["max_file_size"] != float64(64) || gotBody["max_file_count"] != float64(42) {
		t.Fatalf("request body = %#v", gotBody)
	}
	if len(gotBody) != 5 {
		t.Fatalf("request body = %#v, want tenant_id, public_key, private_key, max_file_size, and max_file_count", gotBody)
	}
}

func TestSetQuotaPostsSpendingLimit(t *testing.T) {
	t.Parallel()

	storageSize := int64(1000)
	spendingLimit := int64(20000)
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/quota" {
			t.Fatalf("path = %q, want /v1/quota", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(quotaClientTestResponse("tenant-1"))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").SetQuota(context.Background(), QuotaSetRequest{
		TenantID:               "tenant-1",
		PublicKey:              "public-1",
		PrivateKey:             "private-1",
		MaxStorageSize:         &storageSize,
		TiDBCloudSpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	if gotBody["tidbcloud_spending_limit"] != float64(20000) {
		t.Fatalf("request body = %#v", gotBody)
	}
	if len(gotBody) != 5 {
		t.Fatalf("request body = %#v, want tenant_id, public_key, private_key, max_storage_size, and tidbcloud_spending_limit", gotBody)
	}
}

func TestGetQuotaReturnsStatusError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"no permission to query quota with TiDB Cloud API key"}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").GetQuota(context.Background(), QuotaRequest{
		TenantID:   "tenant-1",
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err == nil {
		t.Fatal("GetQuota error = nil, want status error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf("statusErr = %#v", statusErr)
	}
	if statusErr.Message != "no permission to query quota with TiDB Cloud API key" {
		t.Fatalf("message = %q", statusErr.Message)
	}
}

func quotaClientTestResponse(tenantID string) map[string]any {
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
