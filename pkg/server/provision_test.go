package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	tenantschema "github.com/mem9-ai/dat9/pkg/tenant/schema"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

type fakeProvisioner struct {
	provider       string
	cluster        *tenant.ClusterInfo
	initErr        error
	provisionErr   error
	provisionCalls atomic.Int32
}

func (f *fakeProvisioner) ProviderType() string { return f.provider }

func (f *fakeProvisioner) InitSchema(_ context.Context, dsn string) error {
	if f.initErr != nil {
		return f.initErr
	}
	return nil
}

func (f *fakeProvisioner) Provision(_ context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	f.provisionCalls.Add(1)
	if f.provisionErr != nil {
		if f.cluster == nil {
			return nil, f.provisionErr
		}
		out := *f.cluster
		out.TenantID = tenantID
		out.Provider = f.provider
		return &out, f.provisionErr
	}
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

func (f *fakeProvisioner) ProvisionCallCount() int {
	return int(f.provisionCalls.Load())
}

func TestProvisionMarksTenantFailedWhenInitKeepsFailing(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, initErr: fmt.Errorf("boom"), cluster: &tenant.ClusterInfo{
		ClusterID: "bad-cluster",
		Host:      "127.0.0.1",
		Port:      3306,
		Username:  "root",
		Password:  "bad",
		DBName:    "bad",
	}}

	origWindow, origInitBackoff, origMaxBackoff := schemaInitRetryWindow, schemaInitInitialBackoff, schemaInitMaxBackoff
	schemaInitRetryWindow = 120 * time.Millisecond
	schemaInitInitialBackoff = 10 * time.Millisecond
	schemaInitMaxBackoff = 20 * time.Millisecond
	defer func() {
		schemaInitRetryWindow = origWindow
		schemaInitInitialBackoff = origInitBackoff
		schemaInitMaxBackoff = origMaxBackoff
	}()

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" {
		t.Fatalf("unexpected provision response: %+v", out)
	}
	apiKey := out["api_key"]
	if apiKey == "" {
		t.Fatal("empty api_key")
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(apiKey))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := resolved.Tenant.ID

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantFailed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become failed in time, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestProvisionUsesConfiguredProvisioner(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host := "127.0.0.1"
	port := 3306
	if parsed.Addr != "" {
		h, p, ok := strings.Cut(parsed.Addr, ":")
		if ok {
			host = h
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		Host:      host,
		Port:      port,
		Username:  parsed.User,
		Password:  parsed.Passwd,
		DBName:    parsed.DBName,
	}}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero, "db_tls": false})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" {
		t.Fatalf("unexpected provision response: %+v", out)
	}
	claims, err := token.ParseAndVerifyToken(tokenSecret, out["api_key"])
	if err != nil {
		t.Fatalf("ParseAndVerifyToken provision api key: %v", err)
	}
	hasAdmin := false
	for _, permission := range claims.JournalPermissions {
		if permission == JournalPermissionAdmin {
			hasAdmin = true
			break
		}
	}
	if !hasAdmin {
		t.Fatalf("provision api key journal_permissions = %#v, want %s", claims.JournalPermissions, JournalPermissionAdmin)
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(out["api_key"]))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := resolved.Tenant.ID
	if out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("expected provisioning response status, got %q", out["status"])
	}

	deadline := time.Now().Add(3 * time.Second)
	var status, provider, clusterID string
	for {
		row := metaStore.DB().QueryRow("SELECT status, provider, cluster_id FROM tenants WHERE id = ?", tenantID)
		if err := row.Scan(&status, &provider, &clusterID); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active in time: status=%s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if provider != tenant.ProviderTiDBZero || clusterID != "cluster-1" {
		t.Fatalf("unexpected tenant row: status=%s provider=%s cluster_id=%s", status, provider, clusterID)
	}
}

func TestProvisionPersistsEncryptedAutoEmbeddingProfile(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-profile",
		Host:      "127.0.0.1",
		Port:      4000,
		Username:  "root",
		Password:  "db-pass",
		DBName:    "tenant_db",
	}}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
		TiDBAutoEmbeddingAPIKey: "sk-profile-test",
	})
	defer srv.Close()

	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{KeyName: "default"})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	profile, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if profile.Model != "openai/text-embedding-3-small" {
		t.Fatalf("profile model = %q", profile.Model)
	}
	if profile.Dimensions != 1536 {
		t.Fatalf("profile dimensions = %d", profile.Dimensions)
	}
	if profile.OptionsJSON != `{"dimensions":1536}` {
		t.Fatalf("profile options_json = %q", profile.OptionsJSON)
	}
	if profile.APIBase != "" {
		t.Fatalf("profile api_base = %q", profile.APIBase)
	}
	if string(profile.APIKeyCipher) == "sk-profile-test" {
		t.Fatal("profile API key was stored in plaintext")
	}
	plain, err := pool.Decrypt(context.Background(), profile.APIKeyCipher)
	if err != nil {
		t.Fatalf("decrypt profile API key: %v", err)
	}
	if string(plain) != "sk-profile-test" {
		t.Fatalf("decrypted API key = %q", string(plain))
	}
}

func TestAutoEmbeddingProfileForTenantEnsuresDefaultProfile(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	srv := NewWithConfig(Config{Meta: metaStore})
	defer srv.Close()

	profile, err := srv.autoEmbeddingProfileForTenant(context.Background(), "tenant-default-profile")
	if err != nil {
		t.Fatalf("autoEmbeddingProfileForTenant: %v", err)
	}
	if profile.Model != tenantschema.DefaultTiDBAutoEmbeddingModel {
		t.Fatalf("profile model = %q", profile.Model)
	}
	if profile.Dimensions != tenantschema.DefaultTiDBAutoEmbeddingDimensions {
		t.Fatalf("profile dimensions = %d", profile.Dimensions)
	}
	if profile.OptionsJSON != `{"dimensions":1024}` {
		t.Fatalf("profile options_json = %q", profile.OptionsJSON)
	}

	stored, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), "tenant-default-profile")
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if stored.Model != tenantschema.DefaultTiDBAutoEmbeddingModel || stored.Dimensions != tenantschema.DefaultTiDBAutoEmbeddingDimensions {
		t.Fatalf("stored default profile = %+v", stored)
	}
}

func TestProvisionPersistsTenantBeforeProvisionFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{
		provider:     tenant.ProviderTiDBZero,
		cluster:      &tenant.ClusterInfo{},
		provisionErr: fmt.Errorf("boom"),
	}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadGateway)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants LIMIT 1").Scan(&tenantID, &status); err != nil {
		t.Fatalf("QueryRow tenant: %v", err)
	}
	if tenantID == "" {
		t.Fatal("expected tenant row to be persisted")
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	var keyCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys").Scan(&keyCount); err != nil {
		t.Fatal(err)
	}
	if keyCount != 0 {
		t.Fatalf("api key count = %d, want 0", keyCount)
	}
}

func TestProvisionPersistsPartialClusterBeforeMarkingFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudStarter,
		cluster: &tenant.ClusterInfo{
			ClusterID: "cluster-after-takeover",
			Host:      "db.example",
			Port:      4000,
			Username:  "u1.root",
			Password:  "secret",
			DBName:    "test",
		},
		provisionErr: fmt.Errorf("update starter spending limit for cluster cluster-after-takeover: limit rejected"),
	}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBCloudStarter})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadGateway)
	}

	var status, provider, clusterID, host, user, dbName string
	var port int
	var passCipher []byte
	if err := metaStore.DB().QueryRow(`
		SELECT status, provider, cluster_id, db_host, db_port, db_user, db_password, db_name
		FROM tenants LIMIT 1`,
	).Scan(&status, &provider, &clusterID, &host, &port, &user, &passCipher, &dbName); err != nil {
		t.Fatalf("QueryRow tenant: %v", err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	if provider != tenant.ProviderTiDBCloudStarter || clusterID != "cluster-after-takeover" {
		t.Fatalf("tenant provider/cluster = %s/%s, want %s/cluster-after-takeover", provider, clusterID, tenant.ProviderTiDBCloudStarter)
	}
	if host != "db.example" || port != 4000 || user != "u1.root" || dbName != "test" {
		t.Fatalf("tenant connection = %s:%d %s/%s, want db.example:4000 u1.root/test", host, port, user, dbName)
	}
	plain, err := pool.Decrypt(context.Background(), passCipher)
	if err != nil {
		t.Fatalf("decrypt persisted password: %v", err)
	}
	if string(plain) != "secret" {
		t.Fatalf("persisted password = %q, want secret", plain)
	}
}

func TestStartupResumesProvisioningTenantInit(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host := "127.0.0.1"
	port := 3306
	if parsed.Addr != "" {
		h, p, ok := strings.Cut(parsed.Addr, ":")
		if ok {
			host = h
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}

	passCipher, err := pool.Encrypt(context.Background(), []byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := token.NewID()
	now := time.Now().UTC()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	_ = NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active after restart resume, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStartupMarksPendingTenantFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter, origSweepEvery := pendingTenantStaleAfter, pendingTenantSweepEvery
	pendingTenantStaleAfter = time.Minute
	pendingTenantSweepEvery = time.Hour
	defer func() {
		pendingTenantStaleAfter = origStaleAfter
		pendingTenantSweepEvery = origSweepEvery
	}()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	_ = NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantFailed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending tenant did not become failed after startup resume, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStartupKeepsFreshPendingTenant(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tenantID := token.NewID()
	now := time.Now().UTC()
	origStaleAfter, origSweepEvery := pendingTenantStaleAfter, pendingTenantSweepEvery
	pendingTenantStaleAfter = time.Minute
	pendingTenantSweepEvery = time.Hour
	defer func() {
		pendingTenantStaleAfter = origStaleAfter
		pendingTenantSweepEvery = origSweepEvery
	}()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	t.Cleanup(srv.Close)

	time.Sleep(100 * time.Millisecond)
	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantPending) {
		t.Fatalf("fresh pending tenant status = %s, want %s", status, meta.TenantPending)
	}
}

func TestReconcilePendingTenantDoesNotOverwriteChangedStatus(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateTenantStatus(context.Background(), tenantID, meta.TenantProvisioning); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantProvisioning) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantProvisioning)
	}
}

func TestServerCloseCancelsSchemaInitRetryWorker(t *testing.T) {
	origWindow, origInitBackoff, origMaxBackoff := schemaInitRetryWindow, schemaInitInitialBackoff, schemaInitMaxBackoff
	schemaInitRetryWindow = time.Minute
	schemaInitInitialBackoff = 5 * time.Second
	schemaInitMaxBackoff = 5 * time.Second
	defer func() {
		schemaInitRetryWindow = origWindow
		schemaInitInitialBackoff = origInitBackoff
		schemaInitMaxBackoff = origMaxBackoff
	}()

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBZero,
		cluster:  &tenant.ClusterInfo{},
		initErr:  fmt.Errorf("boom"),
	}
	srv := NewWithConfig(Config{
		Provisioner: prov,
		TokenSecret: []byte("abc"),
	})

	srv.startProvisionedTenantSchemaInit(context.Background(), &provisionTenantResult{
		TenantID:  "tenant-close-test",
		TenantDSN: "user:pass@tcp(localhost:3306)/db?parseTime=true",
		Provider:  tenant.ProviderTiDBZero,
	})

	// Let the worker enter the retry backoff path before closing the server.
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		srv.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not cancel schema init retry worker promptly")
	}
}
