package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestParseMintObjectURI(t *testing.T) {
	scheme, bucket, key, err := parseMintObjectURI("s3://example/customer/a.txt")
	if err != nil || scheme != "s3" || bucket != "example" || key != "customer/a.txt" {
		t.Fatalf("got %s %s %s err=%v", scheme, bucket, key, err)
	}
	if _, _, _, err := parseMintObjectURI("not-a-uri"); err == nil {
		t.Fatal("expected invalid uri")
	}
	if _, _, _, err := parseMintObjectURI("webdav://x/y"); err == nil {
		t.Fatal("expected unsupported scheme")
	}
}

func TestObjectKeyInNamespace(t *testing.T) {
	if !objectKeyInNamespace("cust", "cust") || !objectKeyInNamespace("cust/a", "cust") {
		t.Fatal("expected in-namespace keys")
	}
	if objectKeyInNamespace("cust-evil", "cust") || objectKeyInNamespace("", "cust") || objectKeyInNamespace("other/a", "cust") {
		t.Fatal("expected out-of-namespace keys")
	}
	if objectKeyInNamespace("cust/a", "") {
		t.Fatal("empty namespace must deny")
	}
}

func TestNormalizeObjectNamespaceID(t *testing.T) {
	got, err := normalizeObjectNamespaceID(" customer-1 ")
	if err != nil || got != "customer-1" {
		t.Fatalf("got %q err=%v", got, err)
	}
	if _, err := normalizeObjectNamespaceID("a/b"); err == nil {
		t.Fatal("slash should fail")
	}
	if _, err := normalizeObjectNamespaceID(".."); err == nil {
		t.Fatal(".. should fail")
	}
}

func TestObjectSessionPolicyReadVsWrite(t *testing.T) {
	read := objectSessionPolicy("bkt", "cust", false)
	if strings.Contains(read, "s3:PutObject") || !strings.Contains(read, "s3:GetObject") {
		t.Fatalf("read policy=%s", read)
	}
	write := objectSessionPolicy("bkt", "cust", true)
	if !strings.Contains(write, "s3:PutObject") || !strings.Contains(write, "s3:DeleteObject") {
		t.Fatalf("write policy=%s", write)
	}
}

func TestHandleObjectCredentialsRequiresAuthAndNamespace(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)

	req := httptest.NewRequest(http.MethodPost, "/v1/object-credentials", strings.NewReader(`{"uri":"s3://example/cust/a.txt"}`))
	rr := httptest.NewRecorder()
	rt.server.handleObjectCredentials(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauth status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/object-credentials", strings.NewReader(`{"uri":"s3://example/cust/a.txt"}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: rt.tenantID, TiDBCloudOrgID: "org-1"}))
	rr = httptest.NewRecorder()
	rt.server.handleObjectCredentials(rr, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "object namespace is not configured") {
		t.Fatalf("missing namespace status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleObjectCredentialsRejectsOutsideNamespaceAndUnsupportedScheme(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	if err := rt.meta.SetTenantObjectNamespaceID(ctx, rt.tenantID, "cust"); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.InsertOrgObjectBackend(ctx, &meta.OrgObjectBackend{
		ID: "obb_mint_deny", OrganizationID: "org-1", Scheme: "s3", Bucket: "mint-deny",
		CredentialKind: meta.ObjectCredentialStatic, AccessKeyID: "AKIATEST",
		SecretCipher: []byte("x"),
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/object-credentials", strings.NewReader(`{"uri":"s3://mint-deny/other/a.txt"}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: rt.tenantID, TiDBCloudOrgID: "org-1"}))
	rr := httptest.NewRecorder()
	rt.server.handleObjectCredentials(rr, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "outside the tenant object namespace") {
		t.Fatalf("outside ns status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/object-credentials", strings.NewReader(`{"uri":"gs://mint-deny/cust/a.txt"}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: rt.tenantID, TiDBCloudOrgID: "org-1"}))
	rr = httptest.NewRecorder()
	rt.server.handleObjectCredentials(rr, req)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "--auth=local") {
		t.Fatalf("gs mint status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestAdminObjectBackendCreateListDelete(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	body := map[string]any{
		"public_key":        "public-1",
		"private_key":       "private-1",
		"scheme":            "s3",
		"bucket":            "admin-create",
		"credential_kind":   "static",
		"access_key_id":     "AKIATEST",
		"secret_access_key": "secret",
		"region":            "us-east-1",
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/admin/object-backends", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status=%d body=%s", resp.StatusCode, b)
	}
	var created adminObjectBackendView
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.HasSecret == false || created.AccessKeyID != "AKIATEST" {
		t.Fatalf("created=%+v", created)
	}

	req, err = http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/object-backends", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("list status=%d body=%s", resp.StatusCode, b)
	}

	req, err = http.NewRequest(http.MethodDelete, ts.URL+"/v1/admin/object-backends/"+created.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete status=%d body=%s", resp.StatusCode, b)
	}
}

func TestAdminObjectNamespaceSetGetClear(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	body := map[string]any{
		"public_key":   "public-1",
		"private_key":  "private-1",
		"namespace_id": "customer-1",
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/admin/tenants/"+rt.tenantID+"/object-namespace", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("set status=%d body=%s", resp.StatusCode, b)
	}

	req, err = http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID+"/object-namespace", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("get status=%d body=%s", resp.StatusCode, b)
	}
	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["namespace_id"] != "customer-1" {
		t.Fatalf("got=%v", got)
	}

	req, err = http.NewRequest(http.MethodDelete, ts.URL+"/v1/admin/tenants/"+rt.tenantID+"/object-namespace", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("clear status=%d body=%s", resp.StatusCode, b)
	}
}

func TestAdminObjectBackendRejectsStaticWithoutSecret(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()
	body := map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"scheme": "s3", "bucket": "admin-static-missing-secret", "credential_kind": "static",
		"access_key_id": "AKIATEST",
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/admin/object-backends", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
}
