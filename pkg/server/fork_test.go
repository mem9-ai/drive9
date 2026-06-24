package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

type fakeBranchProvisioner struct {
	cluster      *tenant.ClusterInfo
	provisionErr error
	initErr      error
	deleteErr    error
	createErr    error
	provider     string

	mu                 sync.Mutex
	deleted            []string
	createBranchInputs []*tenant.ClusterInfo
	credentialReqs     []tenant.CredentialProvisionRequest
	systemUsername     string
	systemPassword     string
	systemUserErr      error
	systemUserCalls    int
}

func (f *fakeBranchProvisioner) ProviderType() string {
	if f.provider != "" {
		return f.provider
	}
	return tenant.ProviderTiDBCloudStarter
}

func (f *fakeBranchProvisioner) InitSchema(context.Context, string) error { return f.initErr }

func (f *fakeBranchProvisioner) EnsureSystemUser(context.Context, string, string) (string, string, error) {
	f.mu.Lock()
	f.systemUserCalls++
	username := f.systemUsername
	password := f.systemPassword
	err := f.systemUserErr
	f.mu.Unlock()
	if err != nil {
		return "", "", err
	}
	if username == "" {
		username = "u1.tdc_fs_sys"
	}
	if password == "" {
		password = "system-pass"
	}
	return username, password, nil
}

func (f *fakeBranchProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeBranchProvisioner) ProvisionBranch(ctx context.Context, forkTenantID string, _ *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	return f.CreateBranch(ctx, forkTenantID, nil)
}

func (f *fakeBranchProvisioner) ProvisionBranchWithCredentials(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	out, err := f.CreateBranchWithCredentials(ctx, forkTenantID, source, req)
	if err != nil {
		return out, err
	}
	return f.WaitForBranchActiveWithCredentials(ctx, out, req)
}

func (f *fakeBranchProvisioner) CreateBranch(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	f.mu.Lock()
	if source != nil {
		copySource := *source
		f.createBranchInputs = append(f.createBranchInputs, &copySource)
	} else {
		f.createBranchInputs = append(f.createBranchInputs, nil)
	}
	f.mu.Unlock()
	if f.cluster == nil {
		return nil, errors.New("missing cluster")
	}
	out := *f.cluster
	out.TenantID = forkTenantID
	if f.createErr != nil {
		return &out, f.createErr
	}
	return &out, nil
}

func (f *fakeBranchProvisioner) CreateBranchWithCredentials(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	f.mu.Lock()
	f.credentialReqs = append(f.credentialReqs, req)
	f.mu.Unlock()
	return f.CreateBranch(ctx, forkTenantID, source)
}

func (f *fakeBranchProvisioner) WaitForBranchActive(ctx context.Context, branch *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if f.provisionErr != nil {
		return branch, f.provisionErr
	}
	if f.cluster == nil {
		return nil, errors.New("missing cluster")
	}
	out := *f.cluster
	out.TenantID = branch.TenantID
	out.Password = branch.Password
	if out.DBName == "" {
		out.DBName = branch.DBName
	}
	return &out, nil
}

func (f *fakeBranchProvisioner) WaitForBranchActiveWithCredentials(ctx context.Context, branch *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	f.mu.Lock()
	f.credentialReqs = append(f.credentialReqs, req)
	f.mu.Unlock()
	return f.WaitForBranchActive(ctx, branch)
}

func (f *fakeBranchProvisioner) WaitForBranchUserWithCredentials(_ context.Context, clusterID, branchID string, req tenant.CredentialProvisionRequest) (string, error) {
	f.mu.Lock()
	f.credentialReqs = append(f.credentialReqs, req)
	f.mu.Unlock()
	return f.systemUsername, nil
}

func (f *fakeBranchProvisioner) DeleteBranch(_ context.Context, clusterID, branchID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, clusterID+"/"+branchID)
	return f.deleteErr
}

func (f *fakeBranchProvisioner) DeleteBranchWithCredentials(ctx context.Context, clusterID, branchID string, req tenant.CredentialProvisionRequest) error {
	f.mu.Lock()
	f.credentialReqs = append(f.credentialReqs, req)
	f.mu.Unlock()
	return f.DeleteBranch(ctx, clusterID, branchID)
}

func (f *fakeBranchProvisioner) deletedBranches() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

func (f *fakeBranchProvisioner) createBranchInput(i int) *tenant.ClusterInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i < 0 || i >= len(f.createBranchInputs) || f.createBranchInputs[i] == nil {
		return nil
	}
	out := *f.createBranchInputs[i]
	return &out
}

func (f *fakeBranchProvisioner) credentialRequests() []tenant.CredentialProvisionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tenant.CredentialProvisionRequest(nil), f.credentialReqs...)
}

func (f *fakeBranchProvisioner) systemUserCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.systemUserCalls
}

type nonBranchOnlyProvisioner struct{}

func (nonBranchOnlyProvisioner) ProviderType() string { return tenant.ProviderTiDBCloudStarter }

func (nonBranchOnlyProvisioner) InitSchema(context.Context, string) error { return nil }

func (nonBranchOnlyProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

type forkCleanupTestRuntime struct {
	server *Server
	meta   *meta.Store
	pool   *tenant.Pool
	prov   *fakeBranchProvisioner
	dbHost string
	dbPort int
	dbUser string
	dbPass string
	dbName string
}

func newForkCleanupTestRuntime(t *testing.T) *forkCleanupTestRuntime {
	t.Helper()
	db := newTestDBInfo(t)
	cleanupForkTestTables(t, db.Meta)
	prov := &fakeBranchProvisioner{}
	server := NewWithConfig(Config{Meta: db.Meta, Pool: db.Pool, Provisioner: prov, TokenSecret: []byte("ctx-fork-test-secret")})
	t.Cleanup(server.Close)
	return &forkCleanupTestRuntime{server: server, meta: db.Meta, pool: db.Pool, prov: prov, dbHost: db.DBHost, dbPort: db.DBPort, dbUser: db.DBUser, dbPass: db.DBPass, dbName: db.DBName}
}

func cleanupForkTestTables(t *testing.T, s *meta.Store) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM object_gc_candidates`,
		`DELETE FROM tenant_api_keys`,
		`DELETE FROM tenants`,
		`DELETE FROM storage_namespaces`,
		`DELETE FROM file_gc_tasks`,
		`DELETE FROM file_nodes`,
		`DELETE FROM file_tags`,
		`DELETE FROM semantic`,
		`DELETE FROM contents`,
		`DELETE FROM inodes`,
		// files may not exist for new-tenant-only deployments.
		`DELETE FROM files`,
		`DELETE FROM uploads`,
		`DELETE FROM semantic_tasks`,
	} {
		if _, err := s.DB().Exec(stmt); err != nil {
			if !isTableNotFoundForCleanup(err) {
				t.Fatalf("cleanupForkTestTables: %s: %v", stmt, err)
			}
		}
	}
}

func (rt *forkCleanupTestRuntime) insertForkTenant(t *testing.T, id string, status meta.TenantStatus, branchID string) {
	t.Helper()
	rt.insertTenant(t, id, status, meta.TenantKindFork, "parent", "ns-parent", branchID)
}

func (rt *forkCleanupTestRuntime) insertLiveTenant(t *testing.T, id string) {
	t.Helper()
	rt.insertLiveTenantWithProvider(t, id, tenant.ProviderTiDBCloudStarter)
}

func (rt *forkCleanupTestRuntime) insertLiveTenantWithProvider(t *testing.T, id, provider string) {
	t.Helper()
	rt.insertTenantWithProvider(t, id, meta.TenantActive, meta.TenantKindLive, "", "ns-parent", "source-branch", provider)
}

func (rt *forkCleanupTestRuntime) insertTenant(t *testing.T, id string, status meta.TenantStatus, kind meta.TenantKind, parentID, namespaceID, branchID string) {
	t.Helper()
	rt.insertTenantWithProvider(t, id, status, kind, parentID, namespaceID, branchID, tenant.ProviderTiDBCloudStarter)
}

func (rt *forkCleanupTestRuntime) insertTenantWithProvider(t *testing.T, id string, status meta.TenantStatus, kind meta.TenantKind, parentID, namespaceID, branchID, provider string) {
	t.Helper()
	ctx := context.Background()
	passCipher, err := rt.pool.Encrypt(ctx, []byte(rt.dbPass))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := rt.meta.UpsertStorageNamespace(ctx, &meta.StorageNamespace{ID: "ns-parent", OwnerTenantID: "parent", Backend: "local", Prefix: "parent/", State: meta.StorageNamespaceActive}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:                 id,
		Status:             status,
		Kind:               kind,
		ParentTenantID:     parentID,
		StorageNamespaceID: namespaceID,
		DBHost:             rt.dbHost,
		DBPort:             rt.dbPort,
		DBUser:             rt.dbUser,
		DBPasswordCipher:   passCipher,
		DBName:             rt.dbName,
		DBTLS:              false,
		Provider:           provider,
		ClusterID:          "cluster-a",
		BranchID:           branchID,
		SchemaVersion:      1,
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatal(err)
	}
}

func waitForCondition(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestDecodeForkRequestIgnoresUnknownFieldsAndRejectsTrailingData(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		reject  bool
		wantMsg string
	}{
		{name: "unknown field", body: `{"name":"fork","publicKey":"typo"}`, reject: false},
		{name: "trailing data", body: `{"name":"fork"} {"name":"other"}`, reject: true, wantMsg: "trailing data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/fork", strings.NewReader(tc.body))
			_, err := decodeForkRequest(httptest.NewRecorder(), req)
			if tc.reject && (err == nil || !strings.Contains(err.Error(), tc.wantMsg)) {
				t.Fatalf("decodeForkRequest error = %v, want %q", err, tc.wantMsg)
			}
			if !tc.reject && err != nil {
				t.Fatalf("decodeForkRequest error = %v, want nil", err)
			}
		})
	}
}

func TestCreateForkPartialBranchProvisionErrorPersistsBranchAndKeepsRoot(t *testing.T) {
	origWindow, origInitialBackoff, origMaxBackoff := forkProvisionRetryWindow, forkProvisionInitialBackoff, forkProvisionMaxBackoff
	forkProvisionRetryWindow = 500 * time.Millisecond
	forkProvisionInitialBackoff = 5 * time.Millisecond
	forkProvisionMaxBackoff = 5 * time.Millisecond
	t.Cleanup(func() {
		forkProvisionRetryWindow = origWindow
		forkProvisionInitialBackoff = origInitialBackoff
		forkProvisionMaxBackoff = origMaxBackoff
	})

	rt := newForkCleanupTestRuntime(t)
	rt.insertLiveTenant(t, "source")
	rt.prov.cluster = &tenant.ClusterInfo{ClusterID: "cluster-a", BranchID: "branch-created"}
	rt.prov.provisionErr = errors.New("starter branch not active before timeout")
	rt.prov.deleteErr = errors.New("delete branch failed")

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork", nil)
	if err != nil {
		t.Fatalf("createForkTenant: %v", err)
	}
	if resp.Status != string(meta.TenantProvisioning) || resp.APIKey == "" {
		t.Fatalf("unexpected fork response: %+v", resp)
	}
	if resp.Message != "Migrating fork data. Large tenants may take a few minutes." {
		t.Fatalf("message = %q", resp.Message)
	}
	waitForCondition(t, func() bool {
		failed, err := rt.meta.ListTenantsByStatus(context.Background(), meta.TenantFailed, 10)
		return err == nil && len(failed) == 1
	})

	failed, err := rt.meta.ListTenantsByStatus(context.Background(), meta.TenantFailed, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 {
		t.Fatalf("failed tenants = %+v, want exactly one fork root", failed)
	}
	if failed[0].BranchID != "branch-created" || failed[0].ClusterID != "cluster-a" {
		t.Fatalf("failed fork branch metadata = cluster:%q branch:%q", failed[0].ClusterID, failed[0].BranchID)
	}
}

func TestCreateForkTenantPostAPIKeyFailureMarksFailed(t *testing.T) {
	origWindow, origInitialBackoff, origMaxBackoff := forkProvisionRetryWindow, forkProvisionInitialBackoff, forkProvisionMaxBackoff
	forkProvisionRetryWindow = 500 * time.Millisecond
	forkProvisionInitialBackoff = 5 * time.Millisecond
	forkProvisionMaxBackoff = 5 * time.Millisecond
	t.Cleanup(func() {
		forkProvisionRetryWindow = origWindow
		forkProvisionInitialBackoff = origInitialBackoff
		forkProvisionMaxBackoff = origMaxBackoff
	})

	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-created",
		Host:      rt.dbHost,
		Port:      rt.dbPort,
		Username:  rt.dbUser,
		DBName:    rt.dbName,
		Provider:  tenant.ProviderTiDBCloudNative,
	}
	rt.prov.initErr = errors.New("init failed")

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork", &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("createForkTenant error = %v, want nil (provision is async)", err)
	}
	if resp.Status != string(meta.TenantProvisioning) || resp.APIKey == "" {
		t.Fatalf("unexpected fork response: %+v", resp)
	}

	waitForCondition(t, func() bool {
		failed, err := rt.meta.ListTenantsByStatus(context.Background(), meta.TenantFailed, 10)
		return err == nil && len(failed) == 1
	})

	failed, err := rt.meta.ListTenantsByStatus(context.Background(), meta.TenantFailed, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 {
		t.Fatalf("failed tenants = %d, want 1 (post-API-key failure should mark failed, not deleted)", len(failed))
	}
}

func TestCreateForkTenantPersistsGeneratedBranchPassword(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.insertLiveTenant(t, "source")
	rt.prov.cluster = &tenant.ClusterInfo{ClusterID: "cluster-a", BranchID: "branch-created"}
	rt.prov.provisionErr = context.Canceled

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork", nil)
	if err != nil {
		t.Fatalf("createForkTenant: %v", err)
	}
	if resp.Message != "Migrating fork data. Large tenants may take a few minutes." {
		t.Fatalf("message = %q", resp.Message)
	}

	forkTenant, err := rt.meta.GetTenant(context.Background(), resp.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if forkTenant.DBPasswordCipher == nil {
		t.Fatal("fork DB password cipher is nil")
	}
	gotPassword, err := rt.pool.Decrypt(context.Background(), forkTenant.DBPasswordCipher)
	if err != nil {
		t.Fatalf("decrypt fork password: %v", err)
	}
	if string(gotPassword) == rt.dbPass {
		t.Fatal("fork password unexpectedly matches source password")
	}

	createInput := rt.prov.createBranchInput(0)
	if createInput == nil {
		t.Fatal("CreateBranch was not called with source cluster info")
		return
	}
	if createInput.Password != string(gotPassword) {
		t.Fatalf("CreateBranch password = %q, want persisted fork password", createInput.Password)
	}
}

func TestCreateForkTenantNativeUsesCredentialsAndDeletesBranchOnFailure(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-created",
		Host:      rt.dbHost,
		Port:      rt.dbPort,
		Username:  rt.dbUser,
		DBName:    rt.dbName,
		Provider:  tenant.ProviderTiDBCloudNative,
	}
	rt.prov.provisionErr = context.Canceled

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork", &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("createForkTenant error = %v, want nil (provision is async)", err)
	}
	if resp.Status != string(meta.TenantProvisioning) {
		t.Fatalf("status = %q, want %q", resp.Status, meta.TenantProvisioning)
	}
	if resp.APIKey == "" {
		t.Fatal("api_key is empty")
	}

	createInput := rt.prov.createBranchInput(0)
	if createInput == nil {
		t.Fatal("CreateBranch was not called with source cluster info")
	}
	if createInput.Provider != tenant.ProviderTiDBCloudNative {
		t.Fatalf("CreateBranch source provider = %q, want %q", createInput.Provider, tenant.ProviderTiDBCloudNative)
	}
	credentialReqs := rt.prov.credentialRequests()
	if len(credentialReqs) == 0 || credentialReqs[0].PublicKey != "public-1" || credentialReqs[0].PrivateKey != "private-1" {
		t.Fatalf("credential requests = %+v", credentialReqs)
	}
}

func TestCreateForkTenantNativeDeletesBranchOnSchemaInitFailure(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-created",
		Host:      rt.dbHost,
		Port:      rt.dbPort,
		Username:  rt.dbUser,
		DBName:    rt.dbName,
		Provider:  tenant.ProviderTiDBCloudNative,
	}
	rt.prov.initErr = errors.New("schema init failed")
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork", &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("createForkTenant error = %v, want nil (provision is async)", err)
	}
	if resp.Status != string(meta.TenantProvisioning) {
		t.Fatalf("status = %q, want %q", resp.Status, meta.TenantProvisioning)
	}
	if resp.APIKey == "" {
		t.Fatal("api_key is empty")
	}

	if err := rt.server.provisionForkTenantOnceWithCredentials(context.Background(), resp.TenantID, &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}); err == nil || !strings.Contains(err.Error(), "schema init failed") {
		t.Fatalf("provision error = %v, want schema init failure", err)
	}
}

func TestProvisionForkTenantNativeFinalizesSystemUserCredential(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.prov.systemUsername = "u1.tdc_fs_sys"
	rt.prov.systemPassword = "system-pass"
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.insertTenantWithProvider(t, "fork-provisioning", meta.TenantProvisioning, meta.TenantKindFork, "source", "ns-parent", "branch-a", tenant.ProviderTiDBCloudNative)

	if err := rt.server.provisionForkTenantOnceWithCredentials(context.Background(), "fork-provisioning", &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}); err != nil {
		t.Fatalf("provisionForkTenantOnceWithCredentials: %v", err)
	}

	got, err := rt.meta.GetTenant(context.Background(), "fork-provisioning")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantActive {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantActive)
	}
	if got.DBUser != "u1.tdc_fs_sys" {
		t.Fatalf("tenant DB user = %q, want system user", got.DBUser)
	}
	plain, err := rt.pool.Decrypt(context.Background(), got.DBPasswordCipher)
	if err != nil {
		t.Fatalf("decrypt system password: %v", err)
	}
	if string(plain) != "system-pass" {
		t.Fatalf("tenant DB password = %q, want system password", plain)
	}
	if calls := rt.prov.systemUserCallCount(); calls != 1 {
		t.Fatalf("EnsureSystemUser calls = %d, want 1", calls)
	}
}

func TestCleanupFailedForkDoesNotMarkDeletedWhenBranchDeleteFails(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.deleteErr = errors.New("delete branch failed")
	rt.insertForkTenant(t, "fork-failed", meta.TenantFailed, "branch-a")

	rt.server.cleanupForkTenant(context.Background(), "fork-failed")

	got, err := rt.meta.GetTenant(context.Background(), "fork-failed")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantFailed {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantFailed)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 1 || deleted[0] != "cluster-a/branch-a" {
		t.Fatalf("deleted branches = %#v", deleted)
	}
}

func TestResumeProvisioningForkActivatesBranchBackedTenant(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-a",
		Host:      rt.dbHost,
		Port:      rt.dbPort,
		Username:  rt.dbUser,
		DBName:    rt.dbName,
	}
	rt.insertLiveTenant(t, "parent")
	rt.insertForkTenant(t, "fork-provisioning", meta.TenantProvisioning, "branch-a")

	rt.server.resumeProvisioningTenantsWithCtx(context.Background())

	waitForCondition(t, func() bool {
		got, err := rt.meta.GetTenant(context.Background(), "fork-provisioning")
		return err == nil && got.Status == meta.TenantActive
	})
}

func TestCleanupDeletingForkEnqueuesFileGCTaskRefsBeforeSanitize(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.insertForkTenant(t, "fork-deleting", meta.TenantDeleting, "branch-a")
	ctx := context.Background()
	if _, err := rt.meta.DB().ExecContext(ctx, `INSERT INTO file_gc_tasks
		(task_id, file_id, storage_type, storage_ref, size_bytes, content_type, status, attempt_count, max_attempts, available_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"task-1", "file-deleted", "s3", "blobs/fork-deleted-file", 123, "text/plain", "queued", 0, 0, time.Now().UTC(), time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	rt.server.cleanupForkTenant(ctx, "fork-deleting")

	got, err := rt.meta.GetTenant(ctx, "fork-deleting")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleted)
	}
	candidates, err := rt.meta.ListDueObjectGCCandidates(ctx, time.Now().UTC().Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1: %+v", len(candidates), candidates)
	}
	if candidates[0].StorageRef != "blobs/fork-deleted-file" || candidates[0].Reason != meta.ObjectGCReasonFileDelete {
		t.Fatalf("candidate = %+v", candidates[0])
	}
	var remaining int
	if err := rt.meta.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM file_gc_tasks`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("file_gc_tasks remaining = %d, want 0", remaining)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 1 || deleted[0] != "cluster-a/branch-a" {
		t.Fatalf("deleted branches = %#v", deleted)
	}
}

func TestCleanupDeletingNativeForkUsesCredentialsAfterEnqueueRefs(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertTenantWithProvider(t, "fork-deleting", meta.TenantDeleting, meta.TenantKindFork, "parent", "ns-parent", "branch-a", tenant.ProviderTiDBCloudNative)

	if err := rt.server.cleanupForkTenantOnce(context.Background(), "fork-deleting", &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	}); err != nil {
		t.Fatalf("cleanupForkTenantOnce: %v", err)
	}

	got, err := rt.meta.GetTenant(context.Background(), "fork-deleting")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleted)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 1 || deleted[0] != "cluster-a/branch-a" {
		t.Fatalf("deleted branches = %#v", deleted)
	}
	credentialReqs := rt.prov.credentialRequests()
	if len(credentialReqs) == 0 {
		t.Fatal("DeleteBranchWithCredentials was not called")
	}
	last := credentialReqs[len(credentialReqs)-1]
	if last.PublicKey != "public-1" || last.PrivateKey != "private-1" {
		t.Fatalf("last credential request = %+v", last)
	}
}

func TestHandleForkDeleteNativeFailedCleansBranchBeforeDeletingStatus(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertTenantWithProvider(t, "fork-failed", meta.TenantFailed, meta.TenantKindFork, "parent", "ns-parent", "branch-a", tenant.ProviderTiDBCloudNative)
	if _, err := rt.meta.DB().ExecContext(context.Background(), `UPDATE tenants SET db_host = '', db_port = 0, db_user = '' WHERE id = ?`, "fork-failed"); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/fork", strings.NewReader(`{"public_key":"public-1","private_key":"private-1"}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: "fork-failed"}))
	rr := httptest.NewRecorder()

	rt.server.handleForkDelete(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, body = %s", rr.Code, rr.Body.String())
	}
	got, err := rt.meta.GetTenant(context.Background(), "fork-failed")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleted)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 1 || deleted[0] != "cluster-a/branch-a" {
		t.Fatalf("deleted branches = %#v", deleted)
	}
	credentialReqs := rt.prov.credentialRequests()
	if len(credentialReqs) == 0 {
		t.Fatal("DeleteBranchWithCredentials was not called")
	}
	last := credentialReqs[len(credentialReqs)-1]
	if last.PublicKey != "public-1" || last.PrivateKey != "private-1" {
		t.Fatalf("last credential request = %+v", last)
	}
}

func TestCleanupFailedForkWithoutBranchIDMarksDeleted(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.insertForkTenant(t, "fork-missing-branch", meta.TenantFailed, "")

	rt.server.cleanupForkTenant(context.Background(), "fork-missing-branch")

	got, err := rt.meta.GetTenant(context.Background(), "fork-missing-branch")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleted)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 0 {
		t.Fatalf("deleted branches = %#v, want none", deleted)
	}
}

func TestCleanupNativeFailedForkWithoutBranchIDDoesNotRequireCredentials(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertTenantWithProvider(t, "fork-native-missing-branch", meta.TenantFailed, meta.TenantKindFork, "parent", "ns-parent", "", tenant.ProviderTiDBCloudNative)

	if err := rt.server.cleanupForkTenantOnce(context.Background(), "fork-native-missing-branch", nil); err != nil {
		t.Fatalf("cleanupForkTenantOnce: %v", err)
	}

	got, err := rt.meta.GetTenant(context.Background(), "fork-native-missing-branch")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleted)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 0 {
		t.Fatalf("deleted branches = %#v, want none", deleted)
	}
}

func TestCleanupBranchBackedForkWithoutProvisionerDoesNotMarkDeleted(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.server.provisioner = nonBranchOnlyProvisioner{}
	rt.insertForkTenant(t, "fork-no-provisioner", meta.TenantDeleting, "branch-a")

	rt.server.cleanupForkTenant(context.Background(), "fork-no-provisioner")

	got, err := rt.meta.GetTenant(context.Background(), "fork-no-provisioner")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleting {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleting)
	}
}

func isTableNotFoundForCleanup(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1146
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "error 1146") ||
		strings.Contains(msg, "table") && strings.Contains(msg, "doesn't exist")
}

func TestHandleForkDeleteNativeFailedBranchIDEmptyNoCredentialsDeletesLocally(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertTenantWithProvider(t, "fork-native-failed-empty-branch", meta.TenantFailed, meta.TenantKindFork, "parent", "ns-parent", "", tenant.ProviderTiDBCloudNative)

	req := httptest.NewRequest(http.MethodDelete, "/v1/fork", strings.NewReader("{}"))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: "fork-native-failed-empty-branch"}))
	rr := httptest.NewRecorder()

	rt.server.handleForkDelete(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, body = %s", rr.Code, rr.Body.String())
	}
	got, err := rt.meta.GetTenant(context.Background(), "fork-native-failed-empty-branch")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantDeleted)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 0 {
		t.Fatalf("deleted branches = %#v, want none", deleted)
	}
}

func TestHandleForkDeleteNativeRejectedWhenRequestLacksKey(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertTenantWithProvider(t, "fork-native-def", meta.TenantFailed, meta.TenantKindFork, "parent", "ns-parent", "branch-a", tenant.ProviderTiDBCloudNative)
	if _, err := rt.meta.DB().ExecContext(context.Background(),
		`UPDATE tenants SET db_host = '', db_port = 0, db_user = '' WHERE id = ?`, "fork-native-def"); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/fork", strings.NewReader(`{}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: "fork-native-def"}))
	rr := httptest.NewRecorder()
	rt.server.handleForkDelete(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "invalid or missing TiDB Cloud credentials") {
		t.Fatalf("body missing expected error: %s", body)
	}
}

func TestHandleForkDeleteNativeRejectsWhenNoCredentialsAvailable(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertTenantWithProvider(t, "fork-native-noc", meta.TenantFailed, meta.TenantKindFork, "parent", "ns-parent", "branch-a", tenant.ProviderTiDBCloudNative)
	if _, err := rt.meta.DB().ExecContext(context.Background(),
		`UPDATE tenants SET db_host = '', db_port = 0, db_user = '' WHERE id = ?`, "fork-native-noc"); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/fork", strings.NewReader(`{}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: "fork-native-noc"}))
	rr := httptest.NewRecorder()
	rt.server.handleForkDelete(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if strings.Contains(body, "private") || strings.Contains(body, "default") {
		t.Fatalf("response body = %s", body)
	}
	if deleted := rt.prov.deletedBranches(); len(deleted) != 0 {
		t.Fatalf("deleted branches = %#v", deleted)
	}
}

func TestCreateForkTenantNativeProvisionsWhenCredentialsPresent(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.prov.systemUsername = "u1.root"
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-created",
		Provider:  tenant.ProviderTiDBCloudNative,
	}

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork", &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err != nil {
		t.Fatalf("createForkTenant error = %v, want nil", err)
	}
	if resp == nil {
		t.Fatal("createForkTenant response is nil")
	}
	if resp.Status != string(meta.TenantProvisioning) {
		t.Fatalf("createForkTenant status = %s, want %s", resp.Status, meta.TenantProvisioning)
	}
	if resp.APIKey == "" {
		t.Fatal("createForkTenant response missing api_key")
	}
}

func TestCreateForkTenantNativeNoDefaultCredentialReturnsError(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-created",
		Provider:  tenant.ProviderTiDBCloudNative,
	}

	_, err := rt.server.createForkTenant(context.Background(), "source", "fork", nil)
	if err == nil {
		t.Fatal("createForkTenant error = nil, want error")
	}
	if !strings.Contains(err.Error(), tenant.ErrCredentialsRequired.Error()) {
		t.Fatalf("createForkTenant error = %v, want ErrCredentialsRequired", err)
	}
}

func TestHandleForkCreateNativeReturnsProvisioningWhenCredentialsPresent(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.prov.systemUsername = "u1.root"
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-created",
		Provider:  tenant.ProviderTiDBCloudNative,
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/fork", strings.NewReader(`{"name":"test-fork","public_key":"public-1","private_key":"private-1"}`))
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: "source"}))
	rr := httptest.NewRecorder()
	rt.server.handleForkCreate(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"status":"provisioning"`) {
		t.Fatalf("body missing status provisioning: %s", body)
	}
	if !strings.Contains(body, `"api_key"`) {
		t.Fatalf("body missing api_key: %s", body)
	}
	if strings.Contains(body, `"status":"failed"`) {
		t.Fatalf("body should not contain failed status: %s", body)
	}
}

func TestCreateForkTenantNativeBranchCreateErrorReturnsAPIKey(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-created",
		Provider:  tenant.ProviderTiDBCloudNative,
	}
	rt.prov.createErr = fmt.Errorf("branch connection wait failed")
	rt.prov.deleteErr = fmt.Errorf("delete branch failed")

	_, err := rt.server.createForkTenant(context.Background(), "source", "fork", &tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	if err == nil {
		t.Fatal("createForkTenant error = nil, want forkProvisionFailedError")
	}
	var provisionErr *forkProvisionFailedError
	if !errors.As(err, &provisionErr) {
		t.Fatalf("error type = %T, want forkProvisionFailedError", err)
	}
	if provisionErr.APIKey == "" {
		t.Fatal("forkProvisionFailedError.APIKey is empty")
	}
	if provisionErr.TenantID == "" {
		t.Fatal("forkProvisionFailedError.TenantID is empty")
	}
}

func TestProvisionForkTenantWithCredentialsUsesRequestKeyForWait(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.prov.provider = tenant.ProviderTiDBCloudNative
	rt.insertLiveTenantWithProvider(t, "source", tenant.ProviderTiDBCloudNative)
	rt.prov.cluster = &tenant.ClusterInfo{
		ClusterID: "cluster-a",
		BranchID:  "branch-a",
		Host:      rt.dbHost,
		Port:      rt.dbPort,
		Username:  rt.dbUser,
		DBName:    rt.dbName,
		Provider:  tenant.ProviderTiDBCloudNative,
	}
	rt.insertTenantWithProvider(t, "fork-nc", meta.TenantProvisioning, meta.TenantKindFork, "source", "ns-parent", "branch-a", tenant.ProviderTiDBCloudNative)
	if _, err := rt.meta.DB().ExecContext(context.Background(),
		`UPDATE tenants SET db_host = '', db_port = 0, db_user = '' WHERE id = ?`, "fork-nc"); err != nil {
		t.Fatal(err)
	}

	_ = rt.server.provisionForkTenantOnceWithCredentials(context.Background(), "fork-nc", &tenant.CredentialProvisionRequest{
		PublicKey:  "creds-pk",
		PrivateKey: "creds-sk",
	})
	reqs := rt.prov.credentialRequests()
	found := false
	for _, r := range reqs {
		if r.PublicKey == "creds-pk" && r.PrivateKey == "creds-sk" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("credential requests = %+v, want creds-pk/creds-sk", reqs)
	}
}

func TestProvisionForkTenantAsyncWithProvisionCallsProvisionClosure(t *testing.T) {
	var called bool
	rt := newForkCleanupTestRuntime(t)

	wdCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan struct{})
	rt.server.provisionForkTenantAsyncWithProvision(wdCtx, "test-fork", func() error {
		called = true
		close(done)
		return context.Canceled
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("provision closure was not called")
	}
	if !called {
		t.Fatal("provision closure was not invoked")
	}
}
