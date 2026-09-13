package server

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/s3client"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

func TestMintDataCredentialLocalFilePassThrough(t *testing.T) {
	dir := t.TempDir()
	local, err := s3client.NewLocal(dir, "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	cred, err := mintDataCredential(local, "tenant-z")
	if err != nil {
		t.Fatal(err)
	}
	if cred.Scheme != extent.SchemeFile {
		t.Fatalf("scheme=%s, want file", cred.Scheme)
	}
	if cred.Prefix != "t/tenant-z/" {
		t.Fatalf("prefix=%s", cred.Prefix)
	}
	if cred.Endpoint != local.ObjectsDir() {
		t.Fatalf("endpoint=%s, want %s", cred.Endpoint, local.ObjectsDir())
	}
	if cred.TenantID != "tenant-z" {
		t.Fatalf("tenant_id=%q, want tenant-z (client logs/metrics label the tenant)", cred.TenantID)
	}
}

func TestMintDataCredentialRequiresSTSOrStatic(t *testing.T) {
	_, err := mintDataCredential(nil, "t1")
	if err == nil {
		t.Fatal("expected error without local mock, STS, or static keys")
	}
	if !errors.Is(err, errNoExtentCredentials) {
		t.Fatalf("err=%v, want errNoExtentCredentials", err)
	}
}

func TestMintDataCredentialStaticMinIORefusedByDefault(t *testing.T) {
	aws, err := s3client.New(context.Background(), s3client.AWSConfig{
		Region:          "us-east-1",
		Bucket:          "drive9-local",
		Endpoint:        "http://127.0.0.1:19000",
		ForcePathStyle:  true,
		AccessKeyID:     "drive9minio",
		SecretAccessKey: "drive9minio",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS", "")
	t.Setenv("DRIVE9_TENANT_PROVIDER", "")
	if _, err := mintDataCredential(aws, "tenant-z"); err == nil {
		t.Fatal("static credentials must not be handed to a tenant without an explicit opt-in")
	}
	// A provisioning mode is not a trust boundary: the local provider alone
	// must stay refused, so the operator's opt-in is always explicit.
	t.Setenv("DRIVE9_TENANT_PROVIDER", "local")
	if _, err := mintDataCredential(aws, "tenant-z"); err == nil {
		t.Fatal("DRIVE9_TENANT_PROVIDER=local must not authorise unscoped static keys")
	}
	t.Setenv("DRIVE9_TENANT_PROVIDER", "")
}

func TestMintDataCredentialStaticMinIO(t *testing.T) {
	t.Setenv("DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS", "1")
	aws, err := s3client.New(context.Background(), s3client.AWSConfig{
		Region:          "us-east-1",
		Bucket:          "drive9-local",
		Endpoint:        "http://127.0.0.1:19000",
		ForcePathStyle:  true,
		AccessKeyID:     "drive9minio",
		SecretAccessKey: "drive9minio",
	})
	if err != nil {
		t.Fatal(err)
	}
	cred, err := mintDataCredential(aws, "tenant-z")
	if err != nil {
		t.Fatal(err)
	}
	if cred.Scheme != extent.SchemeS3 {
		t.Fatalf("scheme=%s, want s3", cred.Scheme)
	}
	if cred.AccessKeyID != "drive9minio" || cred.SecretAccessKey != "drive9minio" {
		t.Fatalf("static keys not passed through: ak=%q", cred.AccessKeyID)
	}
	if cred.ExpiresAt != "" {
		t.Fatalf("static minio creds must not expire, got %q", cred.ExpiresAt)
	}
	if cred.Prefix != "t/tenant-z/" {
		t.Fatalf("prefix=%s", cred.Prefix)
	}
	if cred.Endpoint != "http://127.0.0.1:19000" {
		t.Fatalf("endpoint=%s", cred.Endpoint)
	}
	if cred.Bucket != "drive9-local" {
		t.Fatalf("bucket=%s", cred.Bucket)
	}
	if !cred.ForcePathStyle {
		t.Fatal("force_path_style=false, want true for minio")
	}
	if cred.TenantID != "tenant-z" {
		t.Fatalf("tenant_id=%q, want tenant-z", cred.TenantID)
	}
}

type failDeleteS3 struct {
	*s3client.LocalS3Client
	err error
}

func (f failDeleteS3) DeleteObject(ctx context.Context, key string) error {
	if f.err != nil {
		return f.err
	}
	return f.LocalS3Client.DeleteObject(ctx, key)
}

func newExtentGCStore(t *testing.T) *datastore.Store {
	t.Helper()
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stmts := schema.TiDBAppEmbeddingTenantSchemaStatements()
	if err := schema.ExecSchemaStatements(db, stmts); err != nil {
		t.Fatal(err)
	}
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	testtidb.ResetDB(t, store.DB())
	return store
}

// makeBlockGCClaimable drops the block GC grace period so a task enqueued by
// the operation under test is immediately listable. The grace
// (pkg/datastore extentBlockGCGraceSQL) delays deletion so a reader holding a
// pre-compaction slice list cannot hit a deleted object; it is orthogonal to
// what these tests assert.
func makeBlockGCClaimable(t *testing.T, store *datastore.Store) {
	t.Helper()
	if _, err := store.DB().ExecContext(context.Background(),
		`UPDATE block_gc_tasks SET available_at = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
			WHERE status = 'PENDING'`); err != nil {
		t.Fatalf("make block GC tasks claimable: %v", err)
	}
}

func TestRunExtentBlockGCDeletesThenCompletes(t *testing.T) {
	store := newExtentGCStore(t)
	ctx := context.Background()
	dir := t.TempDir()
	local, err := s3client.NewLocal(dir, "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if _, errno, err := store.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":99,"size":4}`), nil); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
	makeBlockGCClaimable(t, store)
	keys, err := store.ListPendingBlockGC(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("expected pending block GC keys")
	}
	key := keys[0]
	full := extentBlockKey("tenant-a", key)
	if err := local.PutObject(ctx, full, bytes.NewReader([]byte("data")), 4, s3client.EncryptionOpts{}); err != nil {
		t.Fatal(err)
	}
	runExtentBlockGC(ctx, store, local, "tenant-a")
	status, err := store.BlockGCTaskStatus(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if status != "COMPLETED" {
		t.Fatalf("status=%s, want COMPLETED after successful delete", status)
	}
	if _, err := os.Stat(filepath.Join(local.ObjectsDir(), full)); !os.IsNotExist(err) {
		t.Fatalf("object still present after GC: %v", err)
	}
}

func TestRunExtentBlockGCKeepsPendingOnDeleteError(t *testing.T) {
	store := newExtentGCStore(t)
	ctx := context.Background()
	dir := t.TempDir()
	local, err := s3client.NewLocal(dir, "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if _, errno, err := store.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":77,"size":4}`), nil); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
	makeBlockGCClaimable(t, store)
	keys, err := store.ListPendingBlockGC(ctx, 8)
	if err != nil || len(keys) == 0 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	key := keys[0]
	full := extentBlockKey("tenant-a", key)
	if err := local.PutObject(ctx, full, bytes.NewReader([]byte("data")), 4, s3client.EncryptionOpts{}); err != nil {
		t.Fatal(err)
	}
	runExtentBlockGC(ctx, store, failDeleteS3{LocalS3Client: local, err: errors.New("s3 boom")}, "tenant-a")
	status, err := store.BlockGCTaskStatus(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" {
		t.Fatalf("status=%s, want PENDING after failed delete", status)
	}
	if _, err := os.Stat(filepath.Join(local.ObjectsDir(), full)); err != nil {
		t.Fatalf("object should remain after failed GC delete: %v", err)
	}
}

func TestRunExtentBlockGCCompletesWhenObjectAlreadyGone(t *testing.T) {
	store := newExtentGCStore(t)
	ctx := context.Background()
	dir := t.TempDir()
	local, err := s3client.NewLocal(dir, "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if _, errno, err := store.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":55,"size":4}`), nil); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
	makeBlockGCClaimable(t, store)
	keys, err := store.ListPendingBlockGC(ctx, 8)
	if err != nil || len(keys) == 0 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	runExtentBlockGC(ctx, store, local, "tenant-a")
	status, err := store.BlockGCTaskStatus(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if status != "COMPLETED" {
		t.Fatalf("status=%s, want COMPLETED for already-gone object", status)
	}
}

// The extent drain re-kicks itself only on real progress, so a failing delete
// must not count as completed work (otherwise a stuck object would spin the
// worker). The task row is seeded directly: a freshly enqueued block is inside
// its grace period and ListPendingBlockGC deliberately hides it.
func TestRunExtentBlockGCCountsOnlyCompletedDeletes(t *testing.T) {
	store := newExtentGCStore(t)
	ctx := context.Background()
	dir := t.TempDir()
	local, err := s3client.NewLocal(dir, "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if _, errno, err := store.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":123,"size":4}`), nil); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
	makeBlockGCClaimable(t, store)
	keys, err := store.ListPendingBlockGC(ctx, 8)
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys=%v err=%v, want the seeded key", keys, err)
	}
	full := extentBlockKey("tenant-a", keys[0])
	if err := local.PutObject(ctx, full, bytes.NewReader([]byte("data")), 4, s3client.EncryptionOpts{}); err != nil {
		t.Fatal(err)
	}
	if got := runExtentBlockGC(ctx, store, failDeleteS3{LocalS3Client: local, err: errors.New("s3 boom")}, "tenant-a"); got != 0 {
		t.Fatalf("completed = %d, want 0 when the delete fails", got)
	}
	if got := runExtentBlockGC(ctx, store, local, "tenant-a"); got != 1 {
		t.Fatalf("completed = %d, want 1 after the delete succeeds", got)
	}
	if got := runExtentBlockGC(ctx, store, local, "tenant-a"); got != 0 {
		t.Fatalf("completed = %d, want 0 when nothing is pending", got)
	}
}

// The extent quota report is the one extent job that must not run on two pods at
// once; this pins the install path that gates it.
func TestOwnsTenantWorkGatesShardedReports(t *testing.T) {
	m := &tenantWorkerManager{}
	if !m.ownsTenantWork("tenant-a") {
		t.Fatal("without a predicate the worker must own every tenant")
	}
	m.SetOwnsTenant(nil)
	if !m.ownsTenantWork("tenant-a") {
		t.Fatal("a nil predicate must mean owns-everything, not owns-nothing")
	}
	m.SetOwnsTenant(func(tenantID string) bool { return tenantID == "tenant-a" })
	if !m.ownsTenantWork("tenant-a") {
		t.Fatal("tenant-a is owned by this pod")
	}
	if m.ownsTenantWork("tenant-b") {
		t.Fatal("tenant-b is not owned by this pod")
	}
}

// A tenant that was forgotten must not have a Runtime rebuilt for it. A worker
// that resolved the entry just before the deletion would otherwise re-create one
// through entryFor and park a JuiceFS session plus its cache goroutine for a
// tenant nothing will drain again.
func TestExtentRuntimePoolRefusesForgottenTenant(t *testing.T) {
	p := newExtentRuntimePool()
	var built int
	p.newRuntime = func(extent.RuntimeConfig) (*extent.Runtime, error) {
		built++
		return &extent.Runtime{}, nil
	}
	p.closeRuntime = func(*extent.Runtime) error { return nil }
	store := &datastore.Store{}
	s3 := &fakeS3ForPool{}

	p.forget("tenant-a")
	err := p.withRuntime("tenant-a", store, s3, func(*extent.Runtime) error {
		t.Fatal("the callback must not run for a forgotten tenant")
		return nil
	})
	if err == nil {
		t.Fatal("withRuntime on a forgotten tenant must fail, not rebuild a Runtime")
	}
	if built != 0 {
		t.Fatalf("built %d runtimes for a forgotten tenant, want 0", built)
	}
}

// fakeS3ForPool is the minimal S3Client extent.WrapS3 touches.
type fakeS3ForPool struct{ s3client.S3Client }
