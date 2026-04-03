package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

type fakeProvisioner struct {
	provider string
	cluster  *tenant.ClusterInfo
	initErr  error
}

func (f *fakeProvisioner) ProviderType() string { return f.provider }

func (f *fakeProvisioner) InitSchema(_ context.Context, dsn string) error {
	if f.initErr != nil {
		return f.initErr
	}
	return nil
}

func (f *fakeProvisioner) Provision(_ context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

func TestProvisionMarksTenantFailedWhenInitKeepsFailing(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	resetServerTestState(t, testDSN, metaStore.DB())

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
	t.Cleanup(srv.waitBackgroundTasks)

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
	apiKey := out["api_key"]
	if apiKey == "" {
		t.Fatal("empty api_key")
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), tenant.HashToken(apiKey))
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
	resetServerTestState(t, testDSN, metaStore.DB())

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

	conn := parseTestTenantConnInfo(t, testDSN)

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		Host:      conn.Host,
		Port:      conn.Port,
		Username:  conn.User,
		Password:  conn.Password,
		DBName:    conn.DBName,
	}}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})
	t.Cleanup(srv.waitBackgroundTasks)

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero, "db_tls": conn.TLS})
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
	if out["api_key"] == "" {
		t.Fatalf("unexpected provision response: %+v", out)
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), tenant.HashToken(out["api_key"]))
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

func TestStartupResumesProvisioningTenantInit(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	resetServerTestState(t, testDSN, metaStore.DB())

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

	conn := parseTestTenantConnInfo(t, testDSN)

	passCipher, err := pool.Encrypt(context.Background(), []byte(conn.Password))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := tenant.NewID()
	now := time.Now().UTC()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           conn.Host,
		DBPort:           conn.Port,
		DBUser:           conn.User,
		DBPasswordCipher: passCipher,
		DBName:           conn.DBName,
		DBTLS:            conn.TLS,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	t.Cleanup(srv.waitBackgroundTasks)

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
