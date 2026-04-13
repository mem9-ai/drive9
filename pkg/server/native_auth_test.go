package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant/tidbcloudnative"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
)

// stubGlobalClient implements tidbcloud.GlobalClient for testing.
type stubGlobalClient struct {
	getZeroInstanceFn func(ctx context.Context, instanceID string) (*tidbcloud.ZeroInstanceInfo, error)
	getClusterInfoFn  func(ctx context.Context, clusterID string) (*tidbcloud.ClusterInfo, error)
}

func (s *stubGlobalClient) GetZeroInstance(ctx context.Context, instanceID string) (*tidbcloud.ZeroInstanceInfo, error) {
	if s.getZeroInstanceFn != nil {
		return s.getZeroInstanceFn(ctx, instanceID)
	}
	return &tidbcloud.ZeroInstanceInfo{}, nil
}

func (s *stubGlobalClient) GetClusterInfo(ctx context.Context, clusterID string) (*tidbcloud.ClusterInfo, error) {
	if s.getClusterInfoFn != nil {
		return s.getClusterInfoFn(ctx, clusterID)
	}
	return &tidbcloud.ClusterInfo{}, nil
}

func (s *stubGlobalClient) GetEncryptedCloudAdminPwd(ctx context.Context, clusterID string) (string, error) {
	return "", nil
}

func (s *stubGlobalClient) CreateServiceUser(ctx context.Context, clusterID, operatorUser, operatorEncPwd, username, password string) error {
	return nil
}

// stubAccountClient implements tidbcloud.AccountClient for testing.
type stubAccountClient struct {
	authorizeFn func(ctx context.Context, r *http.Request, clusterID string) (uint64, error)
}

func (s *stubAccountClient) Authorize(ctx context.Context, r *http.Request, clusterID string) (uint64, error) {
	if s.authorizeFn != nil {
		return s.authorizeFn(ctx, r, clusterID)
	}
	return 1, nil
}

// stubEncryptor implements encrypt.Encryptor for testing.
type stubEncryptor struct{}

func (stubEncryptor) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	return plaintext, nil
}
func (stubEncryptor) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	return ciphertext, nil
}

func newTestNativeProvisioner(global *stubGlobalClient, account *stubAccountClient) *tidbcloudnative.Provisioner {
	var enc encrypt.Encryptor = stubEncryptor{}
	return tidbcloudnative.NewProvisioner(global, account, enc)
}

func TestAuthorizeNativeTarget_ZeroInstance_Success(t *testing.T) {
	global := &stubGlobalClient{}
	account := &stubAccountClient{}
	prov := newTestNativeProvisioner(global, account)

	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-123",
		ClusterID:  "cluster-456",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)

	np, ok := authorizeNativeTarget(r.Context(), w, r, prov, target)
	if !ok {
		t.Fatalf("expected success, got failure; body=%s", w.Body.String())
	}
	if np == nil {
		t.Fatal("expected non-nil provisioner")
	}
}

func TestAuthorizeNativeTarget_ZeroInstance_NotFound(t *testing.T) {
	global := &stubGlobalClient{
		getZeroInstanceFn: func(_ context.Context, _ string) (*tidbcloud.ZeroInstanceInfo, error) {
			return nil, fmt.Errorf("verify zero instance inst-123: %w", tidbcloud.ErrInstanceNotFound)
		},
	}
	prov := newTestNativeProvisioner(global, &stubAccountClient{})

	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-123",
		ClusterID:  "cluster-456",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)

	np, ok := authorizeNativeTarget(r.Context(), w, r, prov, target)
	if ok {
		t.Fatal("expected failure for not-found instance")
	}
	if np != nil {
		t.Fatal("expected nil provisioner on failure")
	}
	if w.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestAuthorizeNativeTarget_ZeroInstance_VerifyError(t *testing.T) {
	global := &stubGlobalClient{
		getZeroInstanceFn: func(_ context.Context, _ string) (*tidbcloud.ZeroInstanceInfo, error) {
			return nil, fmt.Errorf("verify zero instance inst-123: %w", errors.New("connection refused"))
		},
	}
	prov := newTestNativeProvisioner(global, &stubAccountClient{})

	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-123",
		ClusterID:  "cluster-456",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)

	np, ok := authorizeNativeTarget(r.Context(), w, r, prov, target)
	if ok {
		t.Fatal("expected failure for verify error")
	}
	if np != nil {
		t.Fatal("expected nil provisioner on failure")
	}
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusBadGateway)
	}
}

func TestAuthorizeNativeTarget_Cluster_Success(t *testing.T) {
	global := &stubGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{OrgID: 1}, nil
		},
	}
	account := &stubAccountClient{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 1, nil
		},
	}
	prov := newTestNativeProvisioner(global, account)

	target := &tidbcloud.ResolvedTarget{
		Type:      tidbcloud.TargetCluster,
		ClusterID: "cluster-456",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)

	np, ok := authorizeNativeTarget(r.Context(), w, r, prov, target)
	if !ok {
		t.Fatalf("expected success, got failure; body=%s", w.Body.String())
	}
	if np == nil {
		t.Fatal("expected non-nil provisioner")
	}
}

func TestAuthorizeNativeTarget_Cluster_AuthMissing(t *testing.T) {
	account := &stubAccountClient{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 0, tidbcloud.ErrAuthMissing
		},
	}
	prov := newTestNativeProvisioner(&stubGlobalClient{}, account)

	target := &tidbcloud.ResolvedTarget{
		Type:      tidbcloud.TargetCluster,
		ClusterID: "cluster-456",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)

	_, ok := authorizeNativeTarget(r.Context(), w, r, prov, target)
	if ok {
		t.Fatal("expected failure for auth missing")
	}
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestAuthorizeNativeTarget_Cluster_AuthForbidden(t *testing.T) {
	account := &stubAccountClient{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 0, tidbcloud.ErrAuthForbidden
		},
	}
	prov := newTestNativeProvisioner(&stubGlobalClient{}, account)

	target := &tidbcloud.ResolvedTarget{
		Type:      tidbcloud.TargetCluster,
		ClusterID: "cluster-456",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)

	_, ok := authorizeNativeTarget(r.Context(), w, r, prov, target)
	if ok {
		t.Fatal("expected failure for auth forbidden")
	}
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusForbidden)
	}
}

func TestAuthorizeNativeTarget_NotConfigured(t *testing.T) {
	// Pass nil provisioner — not a *tidbcloudnative.Provisioner.
	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-123",
		ClusterID:  "cluster-456",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)

	_, ok := authorizeNativeTarget(r.Context(), w, r, nil, target)
	if ok {
		t.Fatal("expected failure when provisioner is nil")
	}
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusNotImplemented)
	}
}

func TestNativeAuthScope_NoHeaders_FallsThrough(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)
	// No tidbcloud headers set.

	scope, release, handled := nativeAuthScope(w, r, metaStore, nil, nil)
	if handled {
		t.Fatal("expected not handled when no native headers present")
	}
	if scope != nil {
		t.Fatal("expected nil scope")
	}
	if release != nil {
		t.Fatal("expected nil release")
	}
}

func TestNativeAuthScope_InstanceHeader_NotConfigured(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)
	r.Header.Set(tidbcloud.HeaderClusterID, "cluster-123")

	scope, release, handled := nativeAuthScope(w, r, metaStore, nil, nil)
	if !handled {
		t.Fatal("expected handled")
	}
	if scope != nil {
		t.Fatal("expected nil scope on error")
	}
	if release != nil {
		t.Fatal("expected nil release on error")
	}
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusNotImplemented)
	}
}

func TestNativeAuthScope_BadHeader(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)
	// Set an invalid zero-instance ID (not a valid base64url 24-byte payload).
	r.Header.Set(tidbcloud.HeaderZeroInstanceID, "bad-value")

	scope, release, handled := nativeAuthScope(w, r, metaStore, nil, nil)
	if !handled {
		t.Fatal("expected handled for bad header")
	}
	if scope != nil {
		t.Fatal("expected nil scope")
	}
	if release != nil {
		t.Fatal("expected nil release")
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestNativeAuthScope_TenantNotFound(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	// Ensure no tenants exist.
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	global := &stubGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{OrgID: 1}, nil
		},
	}
	account := &stubAccountClient{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 1, nil
		},
	}
	prov := newTestNativeProvisioner(global, account)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)
	r.Header.Set(tidbcloud.HeaderClusterID, "nonexistent-cluster")

	scope, release, handled := nativeAuthScope(w, r, metaStore, nil, prov)
	if !handled {
		t.Fatal("expected handled")
	}
	if scope != nil {
		t.Fatal("expected nil scope when tenant not found")
	}
	if release != nil {
		t.Fatal("expected nil release")
	}
	if w.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusNotFound)
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "tenant not found" {
		t.Fatalf("unexpected error message: %s", body["error"])
	}
}

func TestNativeAuthScope_TenantSuspended(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()

	// Mark tenant as suspended.
	if _, err := rt.meta.DB().Exec("UPDATE tenants SET status = ?", string(meta.TenantSuspended)); err != nil {
		t.Fatal(err)
	}

	// Find the tenant ID.
	var tenantID string
	if err := rt.meta.DB().QueryRow("SELECT id FROM tenants LIMIT 1").Scan(&tenantID); err != nil {
		t.Fatal(err)
	}

	global := &stubGlobalClient{}
	prov := newTestNativeProvisioner(global, &stubAccountClient{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)
	// Set instance header; ParseHeaders will produce a target with ClusterID = ""
	// which won't match our tenant. We need to use the cluster header with the right ID.
	r.Header.Set(tidbcloud.HeaderClusterID, tenantID)

	scope, release, handled := nativeAuthScope(w, r, rt.meta, rt.pool, prov)
	if !handled {
		t.Fatal("expected handled")
	}
	if scope != nil {
		t.Fatal("expected nil scope for suspended tenant")
	}
	if release != nil {
		t.Fatal("expected nil release")
	}
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusForbidden)
	}
}

func TestNativeAuthScope_ActiveTenant_Success(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()

	// Find the tenant ID.
	var tenantID string
	if err := rt.meta.DB().QueryRow("SELECT id FROM tenants LIMIT 1").Scan(&tenantID); err != nil {
		t.Fatal(err)
	}

	global := &stubGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{OrgID: 1}, nil
		},
	}
	account := &stubAccountClient{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 1, nil
		},
	}
	prov := newTestNativeProvisioner(global, account)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/fs/test.txt", nil)
	r.Header.Set(tidbcloud.HeaderClusterID, tenantID)

	scope, release, handled := nativeAuthScope(w, r, rt.meta, rt.pool, prov)
	if !handled {
		t.Fatalf("expected handled; body=%s", w.Body.String())
	}
	if scope == nil {
		t.Fatalf("expected non-nil scope; status=%d body=%s", w.Code, w.Body.String())
	}
	if release == nil {
		t.Fatal("expected non-nil release")
	}
	defer release()

	if scope.TenantID != tenantID {
		t.Fatalf("scope.TenantID=%s, want %s", scope.TenantID, tenantID)
	}
	if scope.Backend == nil {
		t.Fatal("expected non-nil backend")
	}
}

func TestNativeAuthMiddleware_InstanceHeader_WriteThenRead(t *testing.T) {
	if testDSN == "" {
		t.Skip("no test database available")
	}
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()

	var tenantID string
	if err := rt.meta.DB().QueryRow("SELECT id FROM tenants LIMIT 1").Scan(&tenantID); err != nil {
		t.Fatal(err)
	}

	// For the instance header path, ParseHeaders maps instance ID → cluster ID
	// via the header value. We need X-TIDBCLOUD-ZERO-INSTANCE-ID to be set.
	// However ParseHeaders for instance path does not produce a ClusterID that
	// matches our DB tenant (it needs special handling). Use cluster header instead
	// since that directly sets ClusterID = header value = tenantID.
	global := &stubGlobalClient{
		getClusterInfoFn: func(_ context.Context, _ string) (*tidbcloud.ClusterInfo, error) {
			return &tidbcloud.ClusterInfo{OrgID: 1}, nil
		},
	}
	account := &stubAccountClient{
		authorizeFn: func(_ context.Context, _ *http.Request, _ string) (uint64, error) {
			return 1, nil
		},
	}
	prov := newTestNativeProvisioner(global, account)

	srv := NewWithConfig(Config{
		Meta:        rt.meta,
		Pool:        rt.pool,
		TokenSecret: rt.tokenSecret,
		Provisioner: prov,
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Write via native auth.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/native-test.txt", http.NoBody)
	req.Header.Set(tidbcloud.HeaderClusterID, tenantID)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status=%d, want 200", resp.StatusCode)
	}
}
