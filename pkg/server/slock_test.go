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

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/slockoauth"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
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
	testtidb.ResetMetaDB(t, dbi.Meta.DB())
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

// slockLoginState performs the login hop and returns the state cookie and the
// nonce the provider was given, which is what a real browser round trip carries
// into the callback.
func slockLoginState(t *testing.T, baseURL string) (*http.Cookie, string) {
	t.Helper()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Get(baseURL + "/v1/auth/slock/login")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("login status = %d, want 302", resp.StatusCode)
	}
	var stateCookie *http.Cookie
	for _, cookie := range resp.Cookies() {
		if cookie.Name == slockOAuthStateCookie {
			stateCookie = cookie
		}
	}
	if stateCookie == nil {
		t.Fatal("login did not set the state cookie")
	}
	location, err := resp.Location()
	if err != nil {
		t.Fatalf("login redirect: %v", err)
	}
	nonce := location.Query().Get(slockOAuthStateParam)
	if nonce == "" {
		t.Fatal("login did not pass a state nonce to the provider")
	}
	return stateCookie, nonce
}

// slockCallbackGet issues a callback request carrying the browser-bound login
// state, as a real browser would.
func slockCallbackGet(t *testing.T, baseURL, query string) *http.Response {
	t.Helper()
	stateCookie, nonce := slockLoginState(t, baseURL)
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/auth/slock/callback?"+query+"&state="+nonce, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(stateCookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	return resp
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

	resp := slockCallbackGet(t, ts.URL, "format=json")
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

	resp := slockCallbackGet(t, ts.URL, "code=abc&format=json")
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
		resp := slockCallbackGet(t, ts.URL, "code="+code+"&format=json")
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
		resp := slockCallbackGet(t, ts.URL, "code="+code+"&format=json")
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
	testtidb.ResetMetaDB(t, dbi.Meta.DB())
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

	resp := slockCallbackGet(t, ts.URL, "code=oversized&format=json")
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
	testtidb.ResetMetaDB(t, dbi.Meta.DB())
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

	resp := slockCallbackGet(t, ts.URL, "code=oversized-metadata&format=json")
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

	stateCookie, nonce := slockLoginState(t, ts.URL)
	req, err := http.NewRequest("GET", ts.URL+"/v1/auth/slock/callback?code=html&state="+nonce, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/html")
	req.AddCookie(stateCookie)
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

// The signed cookie only proves that this browser started some login. The
// authorization code must come back with the matching nonce, or an attacker who
// can make the victim's browser start a login can hand it their own code.
func TestSlockCallbackRequiresMatchingState(t *testing.T) {
	info := slockoauth.UserInfo{Sub: "sub-1", Type: "agent", ClientID: "drive9", ServerID: "server-1"}
	srv, _, _ := newSlockTestServer(t, info)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	stateCookie, nonce := slockLoginState(t, ts.URL)

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{name: "missing state", query: "code=attacker&format=json", want: http.StatusBadRequest},
		{name: "wrong state", query: "code=attacker&format=json&state=not-the-nonce", want: http.StatusBadRequest},
		{name: "matching state", query: "code=abc&format=json&state=" + nonce, want: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/auth/slock/callback?"+tc.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.AddCookie(stateCookie)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

// A cookie that is not Secure can leak over a plaintext request, so a forwarded
// https scheme has to be honoured when TLS terminates in front of this server.
func TestSlockOAuthCookieSecureHonoursForwardedProto(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/slock/login", nil)
	if slockOAuthCookieSecure(req, "") {
		t.Fatal("a plain request with no public URL must not be treated as secure")
	}
	req.Header.Set("X-Forwarded-Proto", "https")
	if !slockOAuthCookieSecure(req, "") {
		t.Fatal("a forwarded https scheme must mark the cookie Secure")
	}
	req.Header.Set("X-Forwarded-Proto", "http, https")
	if !slockOAuthCookieSecure(req, "") {
		t.Fatal("an appended https hop must mark the cookie Secure")
	}
	req.Header.Set("X-Forwarded-Proto", "http")
	if !slockOAuthCookieSecure(req, "https://drive9.example.com") {
		t.Fatal("an https public URL must already make the cookie Secure")
	}
}
