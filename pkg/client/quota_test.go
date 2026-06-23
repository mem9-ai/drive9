package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetQuotaSendsOwnerBearerAndDecodesResponse(t *testing.T) {
	t.Parallel()

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/v1/quota" {
			t.Fatalf("path = %q, want /v1/quota", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(quotaClientTestResponse("tenant-1"))
	}))
	defer srv.Close()

	c := New(srv.URL, "owner-key")
	resp, err := c.GetQuota(context.Background())
	if err != nil {
		t.Fatalf("GetQuota: %v", err)
	}
	if gotAuth != "Bearer owner-key" {
		t.Fatalf("Authorization = %q, want owner bearer", gotAuth)
	}
	if resp.TenantID != "tenant-1" || !resp.SupportsUpdate {
		t.Fatalf("response = %+v", resp)
	}
	if resp.Config.MaxStorageSize != 1000 || resp.Usage.ReservedBytes != 2 {
		t.Fatalf("quota response = %+v", resp)
	}
}

func TestQueryQuotaWithCredentialsPostsBody(t *testing.T) {
	t.Parallel()

	var gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/quota/query" {
			t.Fatalf("path = %q, want /v1/quota/query", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(quotaClientTestResponse("tenant-1"))
	}))
	defer srv.Close()

	resp, err := New(srv.URL, "").QueryQuotaWithCredentials(context.Background(), QuotaCredentialRequest{
		TenantID:   "tenant-1",
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("QueryQuotaWithCredentials: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
	if gotBody["tenant_id"] != "tenant-1" || gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("request body = %#v", gotBody)
	}
	if resp.Provider != "tidb_cloud_native" {
		t.Fatalf("provider = %q", resp.Provider)
	}
}

func TestSetQuotaWithCredentialsPostsPartialFieldsAndDecodesResponse(t *testing.T) {
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

	resp, err := New(srv.URL, "").SetQuotaWithCredentials(context.Background(), QuotaSetRequest{
		TenantID:       "tenant-1",
		PublicKey:      "public-1",
		PrivateKey:     "private-1",
		MaxStorageSize: &storageSize,
	})
	if err != nil {
		t.Fatalf("SetQuotaWithCredentials: %v", err)
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
	if resp.Usage.MonthlyCostMC != 4 {
		t.Fatalf("monthly cost = %d, want 4", resp.Usage.MonthlyCostMC)
	}
}

func TestGetQuotaReturnsStatusError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"no permission to query quota with TiDB Cloud API key"}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "owner-key").GetQuota(context.Background())
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
			"max_storage_size": 1000,
		},
		"usage": map[string]any{
			"storage_bytes":    1,
			"reserved_bytes":   2,
			"media_file_count": 3,
			"monthly_cost_mc":  4,
		},
	}
}
