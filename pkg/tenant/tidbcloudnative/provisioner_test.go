package tidbcloudnative

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/tidbcloud"
)

// --- mock GlobalClient ---

type mockGlobalClient struct {
	getClusterInfoFn         func(ctx context.Context, clusterID string) (*tidbcloud.ClusterInfo, error)
	getEncryptedCloudAdminFn func(ctx context.Context, clusterID string) (string, error)
	getZeroInstanceFn        func(ctx context.Context, instanceID string) (*tidbcloud.ZeroInstanceInfo, error)
}

func (m *mockGlobalClient) GetClusterInfo(ctx context.Context, clusterID string) (*tidbcloud.ClusterInfo, error) {
	return m.getClusterInfoFn(ctx, clusterID)
}

func (m *mockGlobalClient) GetEncryptedCloudAdminPwd(ctx context.Context, clusterID string) (string, error) {
	return m.getEncryptedCloudAdminFn(ctx, clusterID)
}

func (m *mockGlobalClient) GetZeroInstance(ctx context.Context, instanceID string) (*tidbcloud.ZeroInstanceInfo, error) {
	return m.getZeroInstanceFn(ctx, instanceID)
}

// --- mock Encryptor ---

type mockEncryptor struct {
	decryptFn func(ctx context.Context, ciphertext []byte) ([]byte, error)
}

func (m *mockEncryptor) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	return plaintext, nil
}

func (m *mockEncryptor) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	return m.decryptFn(ctx, ciphertext)
}

func TestProvision_Success(t *testing.T) {
	password := "s3cret"
	encrypted := base64.StdEncoding.EncodeToString([]byte("encrypted-pwd"))

	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, clusterID string) (*tidbcloud.ClusterInfo, error) {
			if clusterID != "12345" {
				t.Fatalf("unexpected cluster ID: %s", clusterID)
			}
			return &tidbcloud.ClusterInfo{
				ClusterID: "12345",
				Host:      "cluster.tidbcloud.com",
				Port:      4000,
				Username:  "cloud_admin",
			}, nil
		},
		getEncryptedCloudAdminFn: func(_ context.Context, _ string) (string, error) {
			return encrypted, nil
		},
	}

	enc := &mockEncryptor{
		decryptFn: func(_ context.Context, _ []byte) ([]byte, error) {
			return []byte(password), nil
		},
	}

	p := NewProvisioner(global, nil, enc)
	info, err := p.Provision(context.Background(), "12345")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.TenantID != "12345" {
		t.Fatalf("got tenant ID %s, want 12345", info.TenantID)
	}
	if info.Host != "cluster.tidbcloud.com" {
		t.Fatalf("got host %s, want cluster.tidbcloud.com", info.Host)
	}
	if info.Port != 4000 {
		t.Fatalf("got port %d, want 4000", info.Port)
	}
	if info.Password != password {
		t.Fatalf("got password %s, want %s", info.Password, password)
	}
	if info.DBName != "_drive9_fs" {
		t.Fatalf("got db %s, want _drive9_fs", info.DBName)
	}
}

func TestProvision_ClusterInfoError(t *testing.T) {
	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return nil, fmt.Errorf("get cluster: %w", tidbcloud.ErrClusterNotFound)
		},
	}

	p := NewProvisioner(global, nil, nil)
	_, err := p.Provision(context.Background(), "99999")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, tidbcloud.ErrClusterNotFound) {
		t.Fatalf("expected ErrClusterNotFound in chain, got: %v", err)
	}
}

func TestProvision_DecryptError(t *testing.T) {
	encrypted := base64.StdEncoding.EncodeToString([]byte("bad"))

	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{ClusterID: "1", Host: "h", Port: 4000, Username: "u"}, nil
		},
		getEncryptedCloudAdminFn: func(_ context.Context, _ string) (string, error) {
			return encrypted, nil
		},
	}

	enc := &mockEncryptor{
		decryptFn: func(_ context.Context, _ []byte) ([]byte, error) {
			return nil, errors.New("kms failure")
		},
	}

	p := NewProvisioner(global, nil, enc)
	_, err := p.Provision(context.Background(), "1")
	if err == nil {
		t.Fatal("expected error from decrypt")
	}
}

func TestVerifyZeroInstance_Success(t *testing.T) {
	global := &mockGlobalClient{
		getZeroInstanceFn: func(_ context.Context, id string) (*tidbcloud.ZeroInstanceInfo, error) {
			if id != "inst-abc" {
				t.Fatalf("unexpected instance ID: %s", id)
			}
			return &tidbcloud.ZeroInstanceInfo{ID: id, Host: "h", Port: 4000}, nil
		},
	}

	p := NewProvisioner(global, nil, nil)
	if err := p.VerifyZeroInstance(context.Background(), "inst-abc"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVerifyZeroInstance_NotFound(t *testing.T) {
	global := &mockGlobalClient{
		getZeroInstanceFn: func(_ context.Context, _ string) (*tidbcloud.ZeroInstanceInfo, error) {
			return nil, fmt.Errorf("get zero instance: %w", tidbcloud.ErrInstanceNotFound)
		},
	}

	p := NewProvisioner(global, nil, nil)
	err := p.VerifyZeroInstance(context.Background(), "fake-id")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestProviderType(t *testing.T) {
	p := NewProvisioner(nil, nil, nil)
	if got := p.ProviderType(); got != "tidbcloud-native" {
		t.Fatalf("got %s, want tidbcloud-native", got)
	}
}

// --- mock AccountClient ---

type mockAccountClientForProvisioner struct {
	authorizeFn func(ctx context.Context, r *http.Request, clusterID string) (uint64, error)
}

func (m *mockAccountClientForProvisioner) Authorize(ctx context.Context, r *http.Request, clusterID string) (uint64, error) {
	return m.authorizeFn(ctx, r, clusterID)
}

func TestAuthorize_OrgMatch(t *testing.T) {
	account := &mockAccountClientForProvisioner{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 100, nil
		},
	}
	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, clusterID string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{ClusterID: clusterID, OrgID: 100, Host: "h", Port: 4000, Username: "u"}, nil
		},
	}

	p := NewProvisioner(global, account, nil)
	r, _ := http.NewRequest("GET", "/", nil)
	if err := p.Authorize(context.Background(), r, "12345"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthorize_OrgMismatch(t *testing.T) {
	account := &mockAccountClientForProvisioner{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 100, nil
		},
	}
	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, clusterID string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{ClusterID: clusterID, OrgID: 999, Host: "h", Port: 4000, Username: "u"}, nil
		},
	}

	p := NewProvisioner(global, account, nil)
	r, _ := http.NewRequest("GET", "/", nil)
	err := p.Authorize(context.Background(), r, "12345")
	if err == nil {
		t.Fatal("expected error for org mismatch")
	}
	if !errors.Is(err, tidbcloud.ErrAuthForbidden) {
		t.Fatalf("expected ErrAuthForbidden, got: %v", err)
	}
}

func TestAuthorize_AccountError(t *testing.T) {
	account := &mockAccountClientForProvisioner{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 0, fmt.Errorf("%w: bad token", tidbcloud.ErrAuthMissing)
		},
	}

	p := NewProvisioner(nil, account, nil)
	r, _ := http.NewRequest("GET", "/", nil)
	err := p.Authorize(context.Background(), r, "12345")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, tidbcloud.ErrAuthMissing) {
		t.Fatalf("expected ErrAuthMissing, got: %v", err)
	}
}

func TestAuthorize_ClusterNotFound(t *testing.T) {
	account := &mockAccountClientForProvisioner{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 100, nil
		},
	}
	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return nil, fmt.Errorf("get cluster: %w", tidbcloud.ErrClusterNotFound)
		},
	}

	p := NewProvisioner(global, account, nil)
	r, _ := http.NewRequest("GET", "/", nil)
	err := p.Authorize(context.Background(), r, "12345")
	if err == nil {
		t.Fatal("expected error for cluster not found")
	}
	if !errors.Is(err, tidbcloud.ErrClusterNotFound) {
		t.Fatalf("expected ErrClusterNotFound, got: %v", err)
	}
}

func TestProvisionWithRootCreds_Success(t *testing.T) {
	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, clusterID string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{
				ClusterID:     clusterID,
				Host:          "host.example.com",
				Port:          4000,
				Username:      "pfx.cloud_admin",
				ProxyEndpoint: "http://proxy:8080",
				UserPrefix:    "pfx",
			}, nil
		},
	}

	p := NewProvisioner(global, nil, nil)
	info, err := p.ProvisionWithRootCreds(context.Background(), "99", "pfx.root", "rootpw")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Username != "pfx.root" {
		t.Fatalf("Username=%q, want pfx.root", info.Username)
	}
	if info.Password != "rootpw" {
		t.Fatalf("Password=%q, want rootpw", info.Password)
	}
	if info.ProxyEndpoint != "http://proxy:8080" {
		t.Fatalf("ProxyEndpoint=%q, want http://proxy:8080", info.ProxyEndpoint)
	}
	if info.UserPrefix != "pfx" {
		t.Fatalf("UserPrefix=%q, want pfx", info.UserPrefix)
	}
}

func TestCreateServiceUser_Success(t *testing.T) {
	// Start a fake proxy server.
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"errNumber": 0, "errMessage": ""})
	}))
	defer proxySrv.Close()

	p := NewProvisioner(nil, nil, nil)
	err := p.CreateServiceUser(context.Background(), "12345", proxySrv.URL, "pfx.root", "root-pass", "pfx.fs_admin", "fs-pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateServiceUser_ProxyError(t *testing.T) {
	// Fake proxy that returns HTTP 500.
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer proxySrv.Close()

	p := NewProvisioner(nil, nil, nil)
	err := p.CreateServiceUser(context.Background(), "12345", proxySrv.URL, "pfx.root", "root-pass", "pfx.fs_admin", "pw")
	if err == nil {
		t.Fatal("expected error from proxy HTTP 500")
	}
}

func TestCreateServiceUser_InvalidClusterID(t *testing.T) {
	p := NewProvisioner(nil, nil, nil)
	err := p.CreateServiceUser(context.Background(), "not-a-number", "http://proxy", "root", "pw", "fs_admin", "pw")
	if err == nil {
		t.Fatal("expected error for invalid cluster ID")
	}
}

func TestFetchProxyInfo_Success(t *testing.T) {
	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, clusterID string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{
				ClusterID:     clusterID,
				ProxyEndpoint: "http://proxy:8080",
				UserPrefix:    "pfx",
			}, nil
		},
	}

	p := NewProvisioner(global, nil, nil)
	ep, prefix, err := p.FetchProxyInfo(context.Background(), "12345")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ep != "http://proxy:8080" {
		t.Fatalf("ProxyEndpoint=%q, want http://proxy:8080", ep)
	}
	if prefix != "pfx" {
		t.Fatalf("UserPrefix=%q, want pfx", prefix)
	}
}

func TestFetchProxyInfo_Error(t *testing.T) {
	global := &mockGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return nil, errors.New("rpc failure")
		},
	}

	p := NewProvisioner(global, nil, nil)
	_, _, err := p.FetchProxyInfo(context.Background(), "12345")
	if err == nil {
		t.Fatal("expected error")
	}
}
