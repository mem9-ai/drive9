package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

type fakeBranchProvisioner struct {
	cluster      *tenant.ClusterInfo
	provisionErr error
	deleteErr    error

	mu                 sync.Mutex
	deleted            []string
	createBranchInputs []*tenant.ClusterInfo
}

func (f *fakeBranchProvisioner) ProviderType() string { return tenant.ProviderTiDBCloudStarter }

func (f *fakeBranchProvisioner) InitSchema(context.Context, string) error { return nil }

func (f *fakeBranchProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeBranchProvisioner) ProvisionBranch(ctx context.Context, forkTenantID string, _ *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	return f.CreateBranch(ctx, forkTenantID, nil)
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
	out.Host = ""
	out.Port = 0
	out.Username = ""
	return &out, nil
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

func (f *fakeBranchProvisioner) DeleteBranch(_ context.Context, clusterID, branchID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, clusterID+"/"+branchID)
	return f.deleteErr
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
		`DELETE FROM files`,
		`DELETE FROM uploads`,
		`DELETE FROM semantic_tasks`,
	} {
		if _, err := s.DB().Exec(stmt); err != nil {
			t.Fatalf("cleanupForkTestTables: %s: %v", stmt, err)
		}
	}
}

func (rt *forkCleanupTestRuntime) insertForkTenant(t *testing.T, id string, status meta.TenantStatus, branchID string) {
	t.Helper()
	rt.insertTenant(t, id, status, meta.TenantKindFork, "parent", "ns-parent", branchID)
}

func (rt *forkCleanupTestRuntime) insertLiveTenant(t *testing.T, id string) {
	t.Helper()
	rt.insertTenant(t, id, meta.TenantActive, meta.TenantKindLive, "", "ns-parent", "source-branch")
}

func (rt *forkCleanupTestRuntime) insertTenant(t *testing.T, id string, status meta.TenantStatus, kind meta.TenantKind, parentID, namespaceID, branchID string) {
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
		Provider:           tenant.ProviderTiDBCloudStarter,
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

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork")
	if err != nil {
		t.Fatalf("createForkTenant: %v", err)
	}
	if resp.Status != string(meta.TenantProvisioning) || resp.APIKey == "" {
		t.Fatalf("unexpected fork response: %+v", resp)
	}
	waitForCondition(t, func() bool {
		return len(rt.prov.deletedBranches()) >= 1
	})
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
	if deleted := rt.prov.deletedBranches(); len(deleted) == 0 || deleted[0] != "cluster-a/branch-created" {
		t.Fatalf("deleted branches = %#v", deleted)
	}
}

func TestCreateForkTenantPersistsGeneratedBranchPassword(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.insertLiveTenant(t, "source")
	rt.prov.cluster = &tenant.ClusterInfo{ClusterID: "cluster-a", BranchID: "branch-created"}
	rt.prov.provisionErr = context.Canceled

	resp, err := rt.server.createForkTenant(context.Background(), "source", "fork")
	if err != nil {
		t.Fatalf("createForkTenant: %v", err)
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
	}
	if createInput.Password != string(gotPassword) {
		t.Fatalf("CreateBranch password = %q, want persisted fork password", createInput.Password)
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

	rt.server.resumeProvisioningTenants()

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

func TestCleanupForkWithoutBranchIDDoesNotMarkDeleted(t *testing.T) {
	rt := newForkCleanupTestRuntime(t)
	rt.insertForkTenant(t, "fork-missing-branch", meta.TenantFailed, "")

	rt.server.cleanupForkTenant(context.Background(), "fork-missing-branch")

	got, err := rt.meta.GetTenant(context.Background(), "fork-missing-branch")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantFailed {
		t.Fatalf("tenant status = %s, want %s", got.Status, meta.TenantFailed)
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
