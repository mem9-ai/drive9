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

func TestMintDataCredentialStaticMinIO(t *testing.T) {
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

func TestRunExtentBlockGCDeletesThenCompletes(t *testing.T) {
	store := newExtentGCStore(t)
	ctx := context.Background()
	dir := t.TempDir()
	local, err := s3client.NewLocal(dir, "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if _, errno, err := store.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":99,"size":4}`)); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
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
	if _, errno, err := store.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":77,"size":4}`)); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
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
	if _, errno, err := store.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":55,"size":4}`)); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
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
