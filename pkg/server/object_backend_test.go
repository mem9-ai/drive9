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
	scheme, bucket, key, endpoint, err := parseMintObjectURI("s3://example/customer/a.txt?endpoint=https://minio.example")
	if err != nil || scheme != "s3" || bucket != "example" || key != "customer/a.txt" || endpoint != "https://minio.example" {
		t.Fatalf("got %s %s %s %s err=%v", scheme, bucket, key, endpoint, err)
	}
	if _, _, _, _, err := parseMintObjectURI("not-a-uri"); err == nil {
		t.Fatal("expected invalid uri")
	}
	if _, _, _, _, err := parseMintObjectURI("webdav://x/y"); err == nil {
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
	if _, err := normalizeObjectNamespaceID("tenant*"); err == nil {
		t.Fatal("wildcard should fail")
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

	req = httptest.NewRequest(http.MethodPost, "/v1/object-credentials", strings.NewReader(`{"uri":"webdav://mint-deny/cust/a.txt"}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: rt.tenantID, TiDBCloudOrgID: "org-1"}))
	rr = httptest.NewRecorder()
	rt.server.handleObjectCredentials(rr, req)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "unsupported") {
		t.Fatalf("webdav mint status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/object-credentials", strings.NewReader(`{"uri":"s3://mint-deny/cust/a.txt"}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: rt.tenantID, TiDBCloudOrgID: "org-1", IsScoped: true}))
	rr = httptest.NewRecorder()
	rt.server.handleObjectCredentials(rr, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "scoped token") {
		t.Fatalf("scoped mint status=%d body=%s", rr.Code, rr.Body.String())
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

func TestAdminObjectBackendRejectsUnmintableConfigs(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()
	post := func(body map[string]any) int {
		t.Helper()
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
		return resp.StatusCode
	}
	if code := post(map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"scheme": "tos", "bucket": "tb", "region": "cn-beijing",
		"credential_kind": "static", "access_key_id": "ak", "secret_access_key": "sk",
	}); code != http.StatusBadRequest {
		t.Fatalf("tos without role status=%d", code)
	}
	if code := post(map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"scheme": "gs", "bucket": "gb", "prefix": "nested",
		"credential_kind": "static", "secret_access_key": `{"type":"service_account"}`,
	}); code != http.StatusBadRequest {
		t.Fatalf("gs prefix status=%d", code)
	}
	if code := post(map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"scheme": "az", "bucket": "cb", "credential_kind": "role",
		"role_arn": "arn:azure:unused", "access_key_id": "acct", "secret_access_key": "key",
	}); code != http.StatusBadRequest {
		t.Fatalf("az role status=%d", code)
	}
	if code := post(map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"scheme": "tos", "bucket": "tb2", "region": "cn-beijing",
		"credential_kind": "role", "role_arn": "trn:iam::1:role/r", "access_key_id": "ak",
	}); code != http.StatusBadRequest {
		t.Fatalf("tos role without secret status=%d", code)
	}
	if code := post(map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"scheme": "oss", "bucket": "ob", "region": "cn-hangzhou",
		"credential_kind": "role", "role_arn": "acs:ram::1:role/r", "access_key_id": "ak",
	}); code != http.StatusBadRequest {
		t.Fatalf("oss role without secret status=%d", code)
	}
	if code := post(map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"scheme": "cos", "bucket": "cbkt-123", "region": "ap-guangzhou", "account_id": "123",
		"credential_kind": "role", "role_arn": "qcs::cam::uin/1:roleName/r", "access_key_id": "ak",
	}); code != http.StatusBadRequest {
		t.Fatalf("cos role without secret status=%d", code)
	}
}

func TestAdminObjectBackendUpdateAndMultiple(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	create := func(bucket, prefix string) adminObjectBackendView {
		t.Helper()
		body := map[string]any{
			"public_key": "public-1", "private_key": "private-1",
			"scheme": "s3", "bucket": bucket, "prefix": prefix,
			"credential_kind": "static", "access_key_id": "AKIATEST",
			"secret_access_key": "secret", "region": "us-east-1",
			"sts_endpoint": "https://sts.example.com",
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
		return created
	}

	a := create("multi-bkt", "east")
	b := create("multi-bkt", "west")
	if a.ID == b.ID || a.Prefix != "east" || b.Prefix != "west" {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
	if a.STSEndpoint != "https://sts.example.com" {
		t.Fatalf("sts endpoint=%q", a.STSEndpoint)
	}

	patch := map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"name": "rotated", "secret_access_key": "new-secret", "region": "us-west-2",
	}
	raw, _ := json.Marshal(patch)
	req, err := http.NewRequest(http.MethodPatch, ts.URL+"/v1/admin/object-backends/"+a.ID, bytes.NewReader(raw))
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
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("patch status=%d body=%s", resp.StatusCode, body)
	}
	var updated adminObjectBackendView
	if err := json.NewDecoder(resp.Body).Decode(&updated); err != nil {
		t.Fatal(err)
	}
	if updated.Name != "rotated" || updated.Region != "us-west-2" || !updated.HasSecret {
		t.Fatalf("updated=%+v", updated)
	}

	clear := map[string]any{
		"public_key": "public-1", "private_key": "private-1",
		"secret_access_key": "",
	}
	raw, _ = json.Marshal(clear)
	req, err = http.NewRequest(http.MethodPatch, ts.URL+"/v1/admin/object-backends/"+a.ID, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(quotaPublicKeyHeader, "public-1")
	req.Header.Set(quotaPrivateKeyHeader, "private-1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("clear secret status=%d body=%s", resp.StatusCode, body)
	}

	req, err = http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/object-backends/"+a.ID, nil)
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
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("get status=%d body=%s", resp.StatusCode, body)
	}
}
