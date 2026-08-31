package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdminGetTenantEmbeddingConfigSendsHeadersAndDecodesResponse(t *testing.T) {
	t.Parallel()

	var gotEscapedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		gotEscapedPath = r.URL.EscapedPath()
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud credential headers")
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want empty", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":    true,
			"api_base":   "https://provider.example.com/v1",
			"api_key":    "sk-a********",
			"model":      "embedding-model",
			"source":     "custom",
			"generation": 3,
			"updated_at": "2026-08-28T01:02:03Z",
		})
	}))
	defer srv.Close()

	out, err := New(srv.URL, "tenant-api-key").AdminGetTenantEmbeddingConfig(context.Background(), AdminTenantEmbeddingConfigGetRequest{
		TenantID: " tenant/1 ", PublicKey: "public-1", PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("AdminGetTenantEmbeddingConfig: %v", err)
	}
	if gotEscapedPath != "/v1/admin/tenants/tenant%2F1/embedding-config" {
		t.Fatalf("escaped path = %q", gotEscapedPath)
	}
	updatedAt, err := time.Parse(time.RFC3339, "2026-08-28T01:02:03Z")
	if err != nil {
		t.Fatal(err)
	}
	if !out.Enabled || out.APIBase == nil || *out.APIBase != "https://provider.example.com/v1" || out.APIKey == nil || *out.APIKey != "sk-a********" || out.Model == nil || *out.Model != "embedding-model" || out.Source != "custom" || out.Generation != 3 || out.UpdatedAt == nil || !out.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("response = %#v", out)
	}
}

func TestAdminSetTenantEmbeddingConfigSendsFullReplacement(t *testing.T) {
	t.Parallel()

	apiBase := "https://provider.example.com/v1"
	apiKey := "provider-secret"
	model := "embedding-model"
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/admin/tenants/tenant-1/embedding-config" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud credential headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"enabled": true, "source": "custom", "generation": 2})
	}))
	defer srv.Close()

	out, err := New(srv.URL, "").AdminSetTenantEmbeddingConfig(context.Background(), AdminTenantEmbeddingConfigSetRequest{
		TenantID: "tenant-1", PublicKey: "public-1", PrivateKey: "private-1", Enabled: true,
		APIBase: &apiBase, APIKey: &apiKey, Model: &model,
	})
	if err != nil {
		t.Fatalf("AdminSetTenantEmbeddingConfig: %v", err)
	}
	if len(gotBody) != 4 || gotBody["enabled"] != true || gotBody["api_base"] != apiBase || gotBody["api_key"] != apiKey || gotBody["model"] != model {
		t.Fatalf("request body = %#v", gotBody)
	}
	if !out.Enabled || out.Source != "custom" || out.Generation != 2 {
		t.Fatalf("response = %#v", out)
	}
}

func TestAdminSetTenantEmbeddingConfigDisableOmitsProviderFields(t *testing.T) {
	t.Parallel()

	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/admin/tenants/tenant-1/embedding-config" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud credential headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"enabled": false, "source": "custom"})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").AdminSetTenantEmbeddingConfig(context.Background(), AdminTenantEmbeddingConfigSetRequest{
		TenantID: "tenant-1", PublicKey: "public-1", PrivateKey: "private-1", Enabled: false,
	})
	if err != nil {
		t.Fatalf("AdminSetTenantEmbeddingConfig: %v", err)
	}
	if len(gotBody) != 1 || gotBody["enabled"] != false {
		t.Fatalf("request body = %#v, want enabled=false only", gotBody)
	}
}

func TestAdminTenantEmbeddingConfigRejectsEmptyTenantID(t *testing.T) {
	t.Parallel()

	c := New("https://drive9.example.com", "")
	if _, err := c.AdminGetTenantEmbeddingConfig(context.Background(), AdminTenantEmbeddingConfigGetRequest{}); err == nil {
		t.Fatal("get empty tenant ID error = nil")
	}
	if _, err := c.AdminSetTenantEmbeddingConfig(context.Background(), AdminTenantEmbeddingConfigSetRequest{}); err == nil {
		t.Fatal("set empty tenant ID error = nil")
	}
}
