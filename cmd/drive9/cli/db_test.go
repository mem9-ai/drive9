package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func clearProvisionEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		EnvVaultToken,
		EnvAPIKey,
		EnvServer,
		EnvRegionCode,
		EnvRegionManifestURL,
		EnvTiDBCloudPublicKey,
		EnvTiDBCloudPrivateKey,
	} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
}

func TestCreateUsesResolvedServerWithoutNativeBody(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var called bool
	var requestBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodPost || r.URL.Path != "/v1/provision" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Fatalf("Authorization = %q, want empty for provision", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		requestBody = string(raw)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"tenant_id": "tenant-1",
			"api_key":   "owner-key-1",
			"status":    "active",
		})
	}))
	defer ts.Close()

	t.Setenv(EnvServer, ts.URL)
	t.Setenv(EnvRegionManifestURL, "http://127.0.0.1:1/not-used")
	resetCredentialCacheForTest()

	out, err := captureStdoutE(t, func() error {
		return Create([]string{"--name", "starter"})
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !called {
		t.Fatal("provision server was not called")
	}
	if requestBody != "" {
		t.Fatalf("request body = %q, want empty starter body", requestBody)
	}
	if !strings.Contains(out, `created "starter"`) {
		t.Fatalf("output = %q, want created message", out)
	}
	cfg := loadConfig()
	if cfg.CurrentContext != "starter" {
		t.Fatalf("current context = %q, want starter", cfg.CurrentContext)
	}
	ctx := cfg.Contexts["starter"]
	if ctx == nil || ctx.APIKey != "owner-key-1" || ctx.Server != ts.URL || ctx.Type != PrincipalOwner {
		t.Fatalf("saved context = %#v", ctx)
	}
}

func TestCreateServerOverrideSendsNativeBodyAndSkipsManifest(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var manifestHits int32
	manifest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&manifestHits, 1)
		http.Error(w, "manifest should not be fetched", http.StatusInternalServerError)
	}))
	defer manifest.Close()

	var gotBody map[string]string
	provision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/provision" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"tenant_id": "tenant-native",
			"api_key":   "owner-native",
			"status":    "provisioning",
		})
	}))
	defer provision.Close()

	t.Setenv(EnvRegionManifestURL, manifest.URL)
	t.Setenv(EnvRegionCode, "aws-us-east-1")
	t.Setenv(EnvTiDBCloudPublicKey, "env-public-should-be-consumed")
	t.Setenv(EnvTiDBCloudPrivateKey, "env-private-should-be-consumed")
	resetCredentialCacheForTest()

	out, err := captureStdoutE(t, func() error {
		return Create([]string{
			"--name", "native",
			"--server", provision.URL,
			"--region-code", "aws-us-east-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if atomic.LoadInt32(&manifestHits) != 0 {
		t.Fatalf("manifest hits = %d, want 0 when --server is set", manifestHits)
	}
	if gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("request body = %#v", gotBody)
	}
	if _, ok := gotBody["database_name"]; ok {
		t.Fatalf("request body unexpectedly included database_name: %#v", gotBody)
	}
	if _, ok := os.LookupEnv(EnvTiDBCloudPublicKey); ok {
		t.Fatalf("%s was not consumed", EnvTiDBCloudPublicKey)
	}
	if _, ok := os.LookupEnv(EnvTiDBCloudPrivateKey); ok {
		t.Fatalf("%s was not consumed", EnvTiDBCloudPrivateKey)
	}
	var result map[string]string
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, out)
	}
	if result["server"] != provision.URL {
		t.Fatalf("json server = %q, want %q", result["server"], provision.URL)
	}
	if _, ok := result["region_code"]; ok {
		t.Fatalf("json output included ignored region_code: %#v", result)
	}
	if result["mode"] != RegionModeTiDBCloudNative {
		t.Fatalf("json mode = %q, want %q", result["mode"], RegionModeTiDBCloudNative)
	}
}

func TestCreateRegionCodeSelectsNativeServer(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var nativeHits int32
	var starterHits int32
	var gotBody map[string]string
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nativeHits, 1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/provision" {
			t.Fatalf("unexpected native request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode native body: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"tenant_id": "tenant-native",
			"api_key":   "owner-native",
			"status":    "active",
		})
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
		return Create([]string{
			"--name", "native-region",
			"--region-code", "aws-us-east-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if atomic.LoadInt32(&nativeHits) != 1 {
		t.Fatalf("native hits = %d, want 1", nativeHits)
	}
	if atomic.LoadInt32(&starterHits) != 0 {
		t.Fatalf("starter hits = %d, want 0", starterHits)
	}
	if gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("native body = %#v", gotBody)
	}
	if _, ok := gotBody["database_name"]; ok {
		t.Fatalf("native body unexpectedly included database_name: %#v", gotBody)
	}
	cfg := loadConfig()
	if got := cfg.Contexts["native-region"].Server; got != native.URL {
		t.Fatalf("saved server = %q, want native %q", got, native.URL)
	}
}

func TestCreateRegionCodeSelectsStarterWithoutBody(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var starterHits int32
	var requestBody string
	starter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&starterHits, 1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/provision" {
			t.Fatalf("unexpected starter request %s %s", r.Method, r.URL.Path)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read starter body: %v", err)
		}
		requestBody = string(raw)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"tenant_id": "tenant-starter",
			"api_key":   "owner-starter",
			"status":    "active",
		})
	}))
	defer starter.Close()
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "native server should not be used", http.StatusInternalServerError)
	}))
	defer native.Close()
	manifest := newRegionManifestTestServer(t, []RegionManifestEntry{
		{
			RegionCode: "aws-us-east-1",
			Mode:       RegionModeTiDBCloudNative,
			ServerURL:  native.URL,
		},
		{
			RegionCode: "aws-us-east-1",
			Mode:       RegionModeTiDBCloudStarter,
			ServerURL:  starter.URL,
		},
	})
	defer manifest.Close()

	t.Setenv(EnvRegionManifestURL, manifest.URL)
	t.Setenv(EnvRegionCode, "aws-us-east-1")
	resetCredentialCacheForTest()

	if _, err := captureStdoutE(t, func() error {
		return Create([]string{"--name", "starter-region"})
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if atomic.LoadInt32(&starterHits) != 1 {
		t.Fatalf("starter hits = %d, want 1", starterHits)
	}
	if requestBody != "" {
		t.Fatalf("starter body = %q, want empty", requestBody)
	}
	if got := readTrimEnv(EnvRegionCode); got != "aws-us-east-1" {
		t.Fatalf("%s = %q, want preserved region code", EnvRegionCode, got)
	}
}

func TestCreateRejectsHalfTiDBCloudKeyBeforeManifestFetch(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var manifestHits int32
	manifest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&manifestHits, 1)
		_ = json.NewEncoder(w).Encode(RegionManifest{Service: "drive9", Regions: []RegionManifestEntry{
			{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudNative, ServerURL: "https://native.example"},
		}})
	}))
	defer manifest.Close()

	t.Setenv(EnvRegionManifestURL, manifest.URL)
	resetCredentialCacheForTest()

	err := Create([]string{
		"--name", "bad-native",
		"--region-code", "aws-us-east-1",
		"--tidbcloud-public-key", "public-1",
	})
	if err == nil {
		t.Fatal("Create error = nil, want missing private key error")
	}
	if !strings.Contains(err.Error(), "tidb_cloud_native create requires") {
		t.Fatalf("Create error = %q", err)
	}
	if atomic.LoadInt32(&manifestHits) != 0 {
		t.Fatalf("manifest hits = %d, want 0 before key validation succeeds", manifestHits)
	}
}

func TestCreateRejectsDatabaseNameFlag(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	err := Create([]string{
		"--server", "https://drive9.example",
		"--tidbcloud-public-key", "public-1",
		"--tidbcloud-private-key", "private-1",
		"--database-name", "appdb",
	})
	if err == nil {
		t.Fatal("Create error = nil, want unknown flag error")
	}
	if !strings.Contains(err.Error(), `unknown flag "--database-name"`) {
		t.Fatalf("Create error = %q", err)
	}
}

func TestDeleteTenantUsesOwnerContext(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var called bool
	var requestBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodDelete || r.URL.Path != "/v1/tenant" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer owner-key" {
			t.Fatalf("Authorization = %q, want Bearer owner-key", got)
		}
		if got := r.Header.Get("Content-Type"); got != "" {
			t.Fatalf("Content-Type = %q, want empty for nil delete body", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		requestBody = string(raw)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "delete_queued"})
	}))
	defer ts.Close()

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "owner", &Context{Type: PrincipalOwner, APIKey: "owner-key", Server: ts.URL}); err != nil {
		t.Fatalf("ctxAdd: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	resetCredentialCacheForTest()

	out, err := captureStdoutE(t, func() error {
		return DeleteTenant(nil)
	})
	if err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	if !called {
		t.Fatal("delete server was not called")
	}
	if requestBody != "" {
		t.Fatalf("delete body = %q, want empty", requestBody)
	}
	if !strings.Contains(out, "delete accepted (status: delete_queued)") {
		t.Fatalf("output = %q", out)
	}
}

func TestDeleteTenantServerOverrideSendsNativeBody(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	var gotBody map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/v1/tenant" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer owner-override" {
			t.Fatalf("Authorization = %q, want Bearer owner-override", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleting"})
	}))
	defer ts.Close()

	out, err := captureStdoutE(t, func() error {
		return DeleteTenant([]string{
			"--server", ts.URL,
			"--api-key", "owner-override",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	if gotBody["public_key"] != "public-1" || gotBody["private_key"] != "private-1" {
		t.Fatalf("delete body = %#v", gotBody)
	}
	var result map[string]string
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, out)
	}
	if result["status"] != "deleting" || result["server"] != ts.URL {
		t.Fatalf("delete json = %#v", result)
	}
}

func TestDeleteTenantRejectsFSScopedContext(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "scoped", &Context{Type: PrincipalFSScoped, APIKey: "scoped-key", Server: "https://drive9.example"}); err != nil {
		t.Fatalf("ctxAdd: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	resetCredentialCacheForTest()

	err := DeleteTenant(nil)
	if err == nil {
		t.Fatal("DeleteTenant error = nil, want owner API key error")
	}
	if !strings.Contains(err.Error(), "owner API key") {
		t.Fatalf("DeleteTenant error = %q", err)
	}
}

func TestDeleteTenantRejectsHalfTiDBCloudKey(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "owner", &Context{Type: PrincipalOwner, APIKey: "owner-key", Server: "https://drive9.example"}); err != nil {
		t.Fatalf("ctxAdd: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	resetCredentialCacheForTest()

	err := DeleteTenant([]string{"--tidbcloud-public-key", "public-1"})
	if err == nil {
		t.Fatal("DeleteTenant error = nil, want missing private key error")
	}
	if !strings.Contains(err.Error(), "requires both public and private keys") {
		t.Fatalf("DeleteTenant error = %q", err)
	}
}

func newRegionManifestTestServer(t *testing.T, entries []RegionManifestEntry) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("manifest method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RegionManifest{
			Service: "drive9",
			Default: &RegionManifestDefault{
				RegionCode: "aws-ap-southeast-1",
				Mode:       RegionModeTiDBCloudStarter,
			},
			Regions: entries,
		})
	}))
}
