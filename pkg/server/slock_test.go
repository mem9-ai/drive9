package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/slockoauth"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

type fakeSlockOAuth struct {
	info slockoauth.UserInfo
}

func (f *fakeSlockOAuth) LoginURL() string {
	return "https://slock.example/login-with-slock/setup?client_id=drive9"
}

func (f *fakeSlockOAuth) ExchangeCode(_ context.Context, code string) (slockoauth.Token, error) {
	return slockoauth.Token{AccessToken: "tok-" + code}, nil
}

func (f *fakeSlockOAuth) Userinfo(_ context.Context, _ string) (slockoauth.UserInfo, error) {
	return f.info, nil
}

func newSlockTestServer(t *testing.T, info slockoauth.UserInfo) (*Server, *meta.Store, []byte) {
	t.Helper()
	dbi := newTestDBInfo(t)
	testmysql.ResetMetaDB(t, dbi.Meta.DB())
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "slock-cluster",
		Host:      dbi.DBHost,
		Port:      dbi.DBPort,
		Username:  dbi.DBUser,
		Password:  dbi.DBPass,
		DBName:    dbi.DBName,
	}}
	srv := NewWithConfig(Config{
		Meta:        dbi.Meta,
		Pool:        dbi.Pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		PublicURL:   "http://localhost:9009",
		SlockOAuth:  &fakeSlockOAuth{info: info},
	})
	t.Cleanup(srv.Close)
	return srv, dbi.Meta, tokenSecret
}

func TestSlockRoutesNotConfigured(t *testing.T) {
	srv := NewWithConfig(Config{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/auth/slock/login")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}

func TestSlockCallbackRequiresCode(t *testing.T) {
	info := slockoauth.UserInfo{Sub: "sub-1", Type: "agent", ClientID: "drive9", ServerID: "server-1"}
	srv, _, _ := newSlockTestServer(t, info)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/auth/slock/callback?format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestSlockCallbackCreatesTenantBindingAndOwnerKey(t *testing.T) {
	info := slockoauth.UserInfo{
		Sub:               "sub-1",
		Type:              "agent",
		ClientID:          "drive9",
		ServerID:          "server-1",
		ServerSlug:        "dev",
		PreferredUsername: "assistant",
		Name:              "Assistant",
	}
	srv, metaStore, tokenSecret := newSlockTestServer(t, info)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/auth/slock/callback?code=abc&format=json", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := resp.Header.Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma = %q, want no-cache", got)
	}
	var out slockCallbackResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.TenantID == "" || out.APIKey == "" || out.Status != string(meta.TenantProvisioning) {
		t.Fatalf("unexpected response: %+v", out)
	}
	if out.Principal.Provider != "slock" || out.Principal.ServerID != "server-1" || out.Principal.Sub != "sub-1" {
		t.Fatalf("unexpected principal: %+v", out.Principal)
	}
	if _, err := token.ParseAndVerifyToken(tokenSecret, out.APIKey); err != nil {
		t.Fatalf("ParseAndVerifyToken: %v", err)
	}
	if out.ServerURL == "" {
		t.Fatal("server_url should not be empty")
	}
	if out.Message == "" {
		t.Fatal("message should not be empty")
	}
	wantSubjectKey := slockSubjectKey(info)
	binding, err := metaStore.GetExternalBinding(context.Background(), "slock", wantSubjectKey)
	if err != nil {
		t.Fatalf("GetExternalBinding: %v", err)
	}
	var bindingMeta map[string]any
	if err := json.Unmarshal(binding.MetadataJSON, &bindingMeta); err != nil {
		t.Fatalf("Unmarshal(binding.MetadataJSON): %v", err)
	}
	if binding.TenantID != out.TenantID || bindingMeta["principal_type"] != "agent" {
		t.Fatalf("unexpected binding: %+v metadata=%s", binding, string(binding.MetadataJSON))
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(out.APIKey))
	if err != nil {
		t.Fatalf("ResolveByAPIKeyHash: %v", err)
	}
	if resolved.APIKey.IssuedByProvider != "slock" || resolved.APIKey.IssuedBySubjectKey != wantSubjectKey {
		t.Fatalf("issued-by metadata not set: %+v", resolved.APIKey)
	}
}

func TestSlockCallbackReusesExistingBinding(t *testing.T) {
	info := slockoauth.UserInfo{Sub: "sub-1", Type: "agent", ClientID: "drive9", ServerID: "server-1"}
	srv, metaStore, _ := newSlockTestServer(t, info)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	call := func(code string) slockCallbackResponse {
		t.Helper()
		resp, err := http.Get(ts.URL + "/v1/auth/slock/callback?code=" + code + "&format=json")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var out slockCallbackResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := call("first")
	second := call("second")
	if second.TenantID != first.TenantID {
		t.Fatalf("second tenant = %s, want %s", second.TenantID, first.TenantID)
	}
	if second.APIKey != first.APIKey {
		t.Fatalf("repeat callback should reuse the same api key; got %s, want %s", second.APIKey, first.APIKey)
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(first.APIKey))
	if err != nil {
		t.Fatalf("ResolveByAPIKeyHash: %v", err)
	}
	if resolved.APIKey.Status != meta.APIKeyActive {
		t.Fatalf("api key status = %s, want %s", resolved.APIKey.Status, meta.APIKeyActive)
	}
	var tenantCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if tenantCount != 1 {
		t.Fatalf("tenant count = %d, want 1", tenantCount)
	}
	var activeKeyCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?", first.TenantID, meta.APIKeyActive).Scan(&activeKeyCount); err != nil {
		t.Fatal(err)
	}
	if activeKeyCount != 1 {
		t.Fatalf("active api key count = %d, want 1", activeKeyCount)
	}
}

func TestSlockCallbackReprovisionsFailedBinding(t *testing.T) {
	info := slockoauth.UserInfo{Sub: "sub-1", Type: "agent", ClientID: "drive9", ServerID: "server-1"}
	srv, metaStore, _ := newSlockTestServer(t, info)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	call := func(code string) slockCallbackResponse {
		t.Helper()
		resp, err := http.Get(ts.URL + "/v1/auth/slock/callback?code=" + code + "&format=json")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var out slockCallbackResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	first := call("first")
	if err := metaStore.UpdateTenantStatus(context.Background(), first.TenantID, meta.TenantFailed); err != nil {
		t.Fatalf("UpdateTenantStatus: %v", err)
	}
	second := call("second")
	if second.TenantID == first.TenantID {
		t.Fatalf("second tenant = %s, want new tenant after failed binding", second.TenantID)
	}
	binding, err := metaStore.GetExternalBinding(context.Background(), "slock", slockSubjectKey(info))
	if err != nil {
		t.Fatalf("GetExternalBinding: %v", err)
	}
	if binding.TenantID != second.TenantID {
		t.Fatalf("binding tenant = %s, want %s", binding.TenantID, second.TenantID)
	}
}

func TestSlockSubjectKeyIsUnambiguous(t *testing.T) {
	a := slockSubjectKey(slockoauth.UserInfo{ServerID: "a:b", Sub: "c"})
	b := slockSubjectKey(slockoauth.UserInfo{ServerID: "a", Sub: "b:c"})
	if a == b {
		t.Fatalf("slockSubjectKey collision: %q", a)
	}
}

func TestWantsJSONAcceptHeaderCaseInsensitive(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/slock/callback", nil)
	req.Header.Set("Accept", "Application/JSON")
	if !wantsJSON(req) {
		t.Fatal("wantsJSON = false, want true")
	}
}

func TestSlockCallbackRejectsOversizedSubjectBeforeProvision(t *testing.T) {
	dbi := newTestDBInfo(t)
	testmysql.ResetMetaDB(t, dbi.Meta.DB())
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "slock-cluster",
		Host:      dbi.DBHost,
		Port:      dbi.DBPort,
		Username:  dbi.DBUser,
		Password:  dbi.DBPass,
		DBName:    dbi.DBName,
	}}
	info := slockoauth.UserInfo{
		Sub:      strings.Repeat("sub", 160),
		Type:     "agent",
		ClientID: "drive9",
		ServerID: strings.Repeat("server", 80),
	}
	srv := NewWithConfig(Config{
		Meta:        dbi.Meta,
		Pool:        dbi.Pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		SlockOAuth:  &fakeSlockOAuth{info: info},
	})
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/auth/slock/callback?code=oversized&format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if prov.ProvisionCallCount() != 0 {
		t.Fatalf("ProvisionCallCount = %d, want 0", prov.ProvisionCallCount())
	}
}

func TestSlockCallbackRejectsOversizedMetadataBeforeProvision(t *testing.T) {
	dbi := newTestDBInfo(t)
	testmysql.ResetMetaDB(t, dbi.Meta.DB())
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "slock-cluster",
		Host:      dbi.DBHost,
		Port:      dbi.DBPort,
		Username:  dbi.DBUser,
		Password:  dbi.DBPass,
		DBName:    dbi.DBName,
	}}
	info := slockoauth.UserInfo{
		Sub:               "sub-1",
		Type:              "agent",
		ClientID:          "drive9",
		ServerID:          "server-1",
		Name:              strings.Repeat("n", maxExternalMetadataBytes),
		PreferredUsername: "assistant",
	}
	srv := NewWithConfig(Config{
		Meta:        dbi.Meta,
		Pool:        dbi.Pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		SlockOAuth:  &fakeSlockOAuth{info: info},
	})
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/auth/slock/callback?code=oversized-metadata&format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if prov.ProvisionCallCount() != 0 {
		t.Fatalf("ProvisionCallCount = %d, want 0", prov.ProvisionCallCount())
	}
}

func TestSlockCallbackHTMLResponse(t *testing.T) {
	info := slockoauth.UserInfo{Sub: "sub-1", Type: "agent", ClientID: "drive9", ServerID: "server-1"}
	srv, _, _ := newSlockTestServer(t, info)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, err := http.NewRequest("GET", ts.URL+"/v1/auth/slock/callback?code=html", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/html")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if !strings.Contains(body, "API Key") {
		t.Fatal("html response should show API Key section")
	}
	if !strings.Contains(body, "copy-text") {
		t.Fatal("html response should render api_key for easy copy")
	}
	if !strings.Contains(body, "Identity") {
		t.Fatal("html response should show identity section")
	}
	if !strings.Contains(body, "Server URL") {
		t.Fatal("html response should show server URL section")
	}
}
