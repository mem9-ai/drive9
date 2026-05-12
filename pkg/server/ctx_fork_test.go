package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

type fakeBranchProvisioner struct {
	cluster   *tenant.ClusterInfo
	deleteErr error

	mu      sync.Mutex
	deleted []string
}

func (f *fakeBranchProvisioner) ProviderType() string { return tenant.ProviderTiDBCloudStarter }

func (f *fakeBranchProvisioner) InitSchema(context.Context, string) error { return nil }

func (f *fakeBranchProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeBranchProvisioner) ProvisionBranch(_ context.Context, forkTenantID string, _ *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	if f.cluster == nil {
		return nil, errors.New("missing cluster")
	}
	out := *f.cluster
	out.TenantID = forkTenantID
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
	if testDSN == "" {
		t.Skip("no test database available")
	}
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	cleanupForkTestTables(t, metaStore)

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host, port := "127.0.0.1", 3306
	if parsed.Addr != "" {
		h, p, _ := strings.Cut(parsed.Addr, ":")
		if h != "" {
			host = h
		}
		if p != "" {
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost", SkipTiDBSchemaCheck: true}, enc)
	pool.SetMetaStore(metaStore)
	t.Cleanup(pool.Close)
	prov := &fakeBranchProvisioner{}
	server := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("ctx-fork-test-secret")})
	t.Cleanup(server.Close)
	return &forkCleanupTestRuntime{server: server, meta: metaStore, pool: pool, prov: prov, dbHost: host, dbPort: port, dbUser: parsed.User, dbPass: parsed.Passwd, dbName: parsed.DBName}
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
		`DELETE FROM files`,
		`DELETE FROM uploads`,
		`DELETE FROM semantic_tasks`,
	} {
		_, _ = s.DB().Exec(stmt)
	}
}

func (rt *forkCleanupTestRuntime) insertForkTenant(t *testing.T, id string, status meta.TenantStatus, branchID string) {
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
		Kind:               meta.TenantKindFork,
		ParentTenantID:     "parent",
		StorageNamespaceID: "ns-parent",
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
