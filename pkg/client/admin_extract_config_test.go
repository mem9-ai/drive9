package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminGetTenantExtractConfigSendsHeadersAndDecodesResponse(t *testing.T) {
	t.Parallel()

	var gotEscapedPath string
	var gotPublicKey string
	var gotPrivateKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		gotEscapedPath = r.URL.EscapedPath()
		gotPublicKey = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPrivateKey = r.Header.Get("X-TiDBCloud-Private-Key")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":    true,
			"api_base":   "https://provider.example.com",
			"api_key":    "sk-a********",
			"model":      "vision-model",
			"prompt":     "custom prompt",
			"source":     "custom",
			"updated_at": "2026-08-21T01:02:03Z",
		})
	}))
	defer srv.Close()

	out, err := New(srv.URL, "").AdminGetTenantExtractConfig(context.Background(), AdminTenantExtractConfigGetRequest{
		TenantID:   "tenant/1",
		MediaType:  ExtractMediaType("future/type"),
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("AdminGetTenantExtractConfig: %v", err)
	}
	if gotEscapedPath != "/v1/admin/tenants/tenant%2F1/extract-config/future%2Ftype" {
		t.Fatalf("escaped path = %q", gotEscapedPath)
	}
	if gotPublicKey != "public-1" || gotPrivateKey != "private-1" {
		t.Fatalf("credentials public=%q private=%q", gotPublicKey, gotPrivateKey)
	}
	if !out.Enabled || out.Source != "custom" || out.APIBase != "https://provider.example.com" || out.APIKey != "sk-a********" || out.Model != "vision-model" || out.Prompt != "custom prompt" || out.UpdatedAt != "2026-08-21T01:02:03Z" {
		t.Fatalf("response = %#v", out)
	}
}

func TestAdminSetTenantExtractConfigPreservesPartialFieldPresence(t *testing.T) {
	t.Parallel()

	enabled := false
	prompt := ""
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/v1/admin/tenants/tenant-1/extract-config/audio" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Fatalf("missing TiDB Cloud credential headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled": false,
			"source":  "custom",
		})
	}))
	defer srv.Close()

	out, err := New(srv.URL, "").AdminSetTenantExtractConfig(context.Background(), AdminTenantExtractConfigSetRequest{
		TenantID:   "tenant-1",
		MediaType:  ExtractMediaTypeAudio,
		PublicKey:  "public-1",
		PrivateKey: "private-1",
		Enabled:    &enabled,
		Prompt:     &prompt,
	})
	if err != nil {
		t.Fatalf("AdminSetTenantExtractConfig: %v", err)
	}
	if len(gotBody) != 2 || gotBody["enabled"] != false || gotBody["prompt"] != "" {
		t.Fatalf("request body = %#v, want explicit false and empty prompt only", gotBody)
	}
	for _, omitted := range []string{"api_base", "api_key", "model", "tenant_id", "media_type", "public_key", "private_key"} {
		if _, ok := gotBody[omitted]; ok {
			t.Fatalf("request body unexpectedly contains %q: %#v", omitted, gotBody)
		}
	}
	if out.Enabled || out.Source != "custom" {
		t.Fatalf("response = %#v", out)
	}
}

func TestAdminTenantExtractConfigRejectsEmptyIdentity(t *testing.T) {
	t.Parallel()

	c := New("https://drive9.example.com", "")
	if _, err := c.AdminGetTenantExtractConfig(context.Background(), AdminTenantExtractConfigGetRequest{MediaType: ExtractMediaTypeImage}); err == nil {
		t.Fatal("empty tenant ID error = nil")
	}
	if _, err := c.AdminGetTenantExtractConfig(context.Background(), AdminTenantExtractConfigGetRequest{TenantID: "tenant-1"}); err == nil {
		t.Fatal("empty media type error = nil")
	}
}

func TestAdminGetTenantExtractConfigReturnsStatusError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"media type is not supported"}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").AdminGetTenantExtractConfig(context.Background(), AdminTenantExtractConfigGetRequest{
		TenantID:  "tenant-1",
		MediaType: ExtractMediaTypeVideo,
	})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest || statusErr.Message != "media type is not supported" {
		t.Fatalf("error = %#v", err)
	}
}
