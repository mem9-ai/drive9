package backend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

type recordingS3Client struct {
	s3client.S3Client
	lastPutEncryption s3client.EncryptionOpts
	lastMPUEncryption s3client.EncryptionOpts
	failPut           bool
	failMPU           bool
}

func (c *recordingS3Client) PutObject(ctx context.Context, key string, body io.Reader, size int64, encOpts s3client.EncryptionOpts) error {
	c.lastPutEncryption = encOpts
	if c.failPut {
		return errors.New("kms access denied")
	}
	return c.S3Client.PutObject(ctx, key, body, size, encOpts)
}

func (c *recordingS3Client) CreateMultipartUpload(ctx context.Context, key string, algo s3client.ChecksumAlgo, encOpts s3client.EncryptionOpts) (*s3client.MultipartUpload, error) {
	c.lastMPUEncryption = encOpts
	if c.failMPU {
		return nil, errors.New("kms access denied")
	}
	return c.S3Client.CreateMultipartUpload(ctx, key, algo, encOpts)
}

func newTestBackendWithRecordingS3(t *testing.T, smallInDB bool, opts Options) (*Dat9Backend, *recordingS3Client) {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-s3-encryption-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initBackendSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	localS3, err := s3client.NewLocal(s3Dir, "http://localhost:9091/s3")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingS3Client{S3Client: localS3}
	b, err := NewWithS3ModeAndOptions(store, rec, smallInDB, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, rec
}

func resolvedSSEKMSTestPolicy() meta.ResolvedS3EncryptionPolicy {
	return meta.ResolvedS3EncryptionPolicy{
		Mode:             s3client.EncryptionModeSSEKMS,
		KMSKeyID:         "arn:aws:kms:ap-southeast-1:123456789012:key/test",
		BucketKeyEnabled: true,
	}
}

func TestS3EncryptionPolicyDrivesPutObjectAndFileMetadata(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, false, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})

	_, _, err := b.WriteCtxIfRevisionWithTagsResult(
		context.Background(),
		"/kms.txt",
		[]byte("encrypted"),
		0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate,
		-1,
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	if rec.lastPutEncryption.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("PutObject mode=%q, want %q", rec.lastPutEncryption.Mode, s3client.EncryptionModeSSEKMS)
	}
	if rec.lastPutEncryption.KMSKeyID != resolvedSSEKMSTestPolicy().KMSKeyID {
		t.Fatalf("PutObject key=%q, want %q", rec.lastPutEncryption.KMSKeyID, resolvedSSEKMSTestPolicy().KMSKeyID)
	}
	if !rec.lastPutEncryption.BucketKeyEnabled {
		t.Fatal("PutObject bucket key disabled, want enabled")
	}
	if rec.lastPutEncryption.EncryptionContext["tenant_id"] != "tenant-a" {
		t.Fatalf("PutObject tenant context=%q, want tenant-a", rec.lastPutEncryption.EncryptionContext["tenant_id"])
	}

	nf, err := b.store.Stat(context.Background(), "/kms.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if rec.lastPutEncryption.EncryptionContext["object_key"] != nf.File.StorageRef {
		t.Fatalf("PutObject object_key context=%q, want %q", rec.lastPutEncryption.EncryptionContext["object_key"], nf.File.StorageRef)
	}
	if nf.File.StorageEncryptionMode != datastore.StorageEncryptionSSEKMS {
		t.Fatalf("file encryption mode=%q, want %q", nf.File.StorageEncryptionMode, datastore.StorageEncryptionSSEKMS)
	}
	if nf.File.StorageEncryptionKeyID != resolvedSSEKMSTestPolicy().KMSKeyID {
		t.Fatalf("file encryption key=%q, want %q", nf.File.StorageEncryptionKeyID, resolvedSSEKMSTestPolicy().KMSKeyID)
	}
}

func TestDB9WriteRecordsExplicitNoneEvenWhenS3PolicyIsKMS(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, true, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})

	_, _, err := b.WriteCtxIfRevisionWithTagsResult(
		context.Background(),
		"/small.txt",
		[]byte("small"),
		0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate,
		-1,
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if rec.lastPutEncryption.Mode != "" {
		t.Fatalf("PutObject was called with mode=%q for db9 write", rec.lastPutEncryption.Mode)
	}

	nf, err := b.store.Stat(context.Background(), "/small.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if nf.File.StorageType != datastore.StorageDB9 {
		t.Fatalf("storage type=%q, want %q", nf.File.StorageType, datastore.StorageDB9)
	}
	if nf.File.StorageEncryptionMode != datastore.StorageEncryptionNone {
		t.Fatalf("file encryption mode=%q, want %q", nf.File.StorageEncryptionMode, datastore.StorageEncryptionNone)
	}
	if nf.File.StorageEncryptionKeyID != "" {
		t.Fatalf("file encryption key=%q, want empty", nf.File.StorageEncryptionKeyID)
	}
}

func TestS3EncryptionPolicyDrivesMultipartAndUploadMetadata(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, true, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})

	plan, err := b.InitiateUploadV2IfRevision(context.Background(), "/large.bin", int64(s3client.MinPartSize), -1, "")
	if err != nil {
		t.Fatalf("initiate upload: %v", err)
	}
	if rec.lastMPUEncryption.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("CreateMultipartUpload mode=%q, want %q", rec.lastMPUEncryption.Mode, s3client.EncryptionModeSSEKMS)
	}
	if rec.lastMPUEncryption.KMSKeyID != resolvedSSEKMSTestPolicy().KMSKeyID {
		t.Fatalf("CreateMultipartUpload key=%q, want %q", rec.lastMPUEncryption.KMSKeyID, resolvedSSEKMSTestPolicy().KMSKeyID)
	}

	upload, err := b.store.GetUpload(context.Background(), plan.UploadID)
	if err != nil {
		t.Fatalf("get upload: %v", err)
	}
	if rec.lastMPUEncryption.EncryptionContext["object_key"] != upload.S3Key {
		t.Fatalf("CreateMultipartUpload object_key context=%q, want %q", rec.lastMPUEncryption.EncryptionContext["object_key"], upload.S3Key)
	}
	if upload.StorageEncryptionMode != datastore.StorageEncryptionSSEKMS {
		t.Fatalf("upload encryption mode=%q, want %q", upload.StorageEncryptionMode, datastore.StorageEncryptionSSEKMS)
	}
	if upload.StorageEncryptionKeyID != resolvedSSEKMSTestPolicy().KMSKeyID {
		t.Fatalf("upload encryption key=%q, want %q", upload.StorageEncryptionKeyID, resolvedSSEKMSTestPolicy().KMSKeyID)
	}

	file, err := b.store.GetFile(context.Background(), upload.FileID)
	if err != nil {
		t.Fatalf("get pending file: %v", err)
	}
	if file.StorageEncryptionMode != upload.StorageEncryptionMode ||
		file.StorageEncryptionKeyID != upload.StorageEncryptionKeyID {
		t.Fatalf("file encryption=%q/%q, upload encryption=%q/%q",
			file.StorageEncryptionMode, file.StorageEncryptionKeyID,
			upload.StorageEncryptionMode, upload.StorageEncryptionKeyID)
	}
}

func TestPatchUploadUsesResolvedEncryptionForNewTarget(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, true, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})

	now := time.Now().UTC()
	if err := b.store.InsertFile(context.Background(), &datastore.File{
		FileID:                "source-file",
		StorageType:           datastore.StorageS3,
		StorageRef:            "blobs/source-file",
		StorageEncryptionMode: datastore.StorageEncryptionLegacy,
		SizeBytes:             1,
		Revision:              1,
		Status:                datastore.StatusConfirmed,
		CreatedAt:             now,
		ConfirmedAt:           &now,
	}); err != nil {
		t.Fatalf("insert source file: %v", err)
	}
	if err := b.store.InsertNode(context.Background(), &datastore.FileNode{
		NodeID:     "node-source-file",
		Path:       "/patch.bin",
		ParentPath: pathutil.ParentPath("/patch.bin"),
		Name:       pathutil.BaseName("/patch.bin"),
		FileID:     "source-file",
		CreatedAt:  now,
	}); err != nil {
		t.Fatalf("insert source node: %v", err)
	}

	plan, err := b.InitiatePatchUploadIfRevision(context.Background(), "/patch.bin", int64(s3client.MinPartSize), []int{1}, s3client.MinPartSize, 1)
	if err != nil {
		t.Fatalf("initiate patch: %v", err)
	}
	if rec.lastMPUEncryption.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("CreateMultipartUpload mode=%q, want %q", rec.lastMPUEncryption.Mode, s3client.EncryptionModeSSEKMS)
	}

	upload, err := b.store.GetUpload(context.Background(), plan.UploadID)
	if err != nil {
		t.Fatalf("get upload: %v", err)
	}
	if rec.lastMPUEncryption.EncryptionContext["object_key"] != upload.S3Key {
		t.Fatalf("patch upload object_key context=%q, want %q", rec.lastMPUEncryption.EncryptionContext["object_key"], upload.S3Key)
	}
	if upload.StorageEncryptionMode != datastore.StorageEncryptionSSEKMS {
		t.Fatalf("patch upload encryption mode=%q, want %q", upload.StorageEncryptionMode, datastore.StorageEncryptionSSEKMS)
	}
}

func TestOverwriteLegacyS3FileRecordsResolvedEncryption(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, false, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})
	insertLegacyS3File(t, b, "/overwrite.bin", "legacy-file")

	_, rev, err := b.WriteCtxIfRevisionWithTagsResult(
		context.Background(),
		"/overwrite.bin",
		[]byte("encrypted overwrite"),
		0,
		filesystem.WriteFlagTruncate,
		1,
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if rev != 2 {
		t.Fatalf("revision=%d, want 2", rev)
	}
	if rec.lastPutEncryption.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("PutObject mode=%q, want %q", rec.lastPutEncryption.Mode, s3client.EncryptionModeSSEKMS)
	}

	nf, err := b.store.Stat(context.Background(), "/overwrite.bin")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if rec.lastPutEncryption.EncryptionContext["object_key"] != nf.File.StorageRef {
		t.Fatalf("PutObject object_key context=%q, want %q", rec.lastPutEncryption.EncryptionContext["object_key"], nf.File.StorageRef)
	}
	if nf.File.StorageEncryptionMode != datastore.StorageEncryptionSSEKMS {
		t.Fatalf("file encryption mode=%q, want %q", nf.File.StorageEncryptionMode, datastore.StorageEncryptionSSEKMS)
	}
	if nf.File.StorageEncryptionKeyID != resolvedSSEKMSTestPolicy().KMSKeyID {
		t.Fatalf("file encryption key=%q, want %q", nf.File.StorageEncryptionKeyID, resolvedSSEKMSTestPolicy().KMSKeyID)
	}
}

func TestMultipartOverwriteLegacyS3FileRecordsResolvedEncryption(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, true, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})
	insertLegacyS3File(t, b, "/upload-overwrite.bin", "legacy-upload-file")

	ctx := context.Background()
	totalSize := int64(s3client.PartSize)
	plan, err := b.InitiateUploadWithChecksumsIfRevision(ctx, "/upload-overwrite.bin", totalSize, nil, 1, "")
	if err != nil {
		t.Fatalf("initiate upload: %v", err)
	}
	upload, err := b.store.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatalf("get upload: %v", err)
	}
	partData := bytes.Repeat([]byte("x"), int(totalSize))
	if _, err := b.S3().(*recordingS3Client).S3Client.(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, 1, bytes.NewReader(partData)); err != nil {
		t.Fatalf("upload part: %v", err)
	}
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatalf("confirm upload: %v", err)
	}

	if rec.lastMPUEncryption.EncryptionContext["object_key"] != upload.S3Key {
		t.Fatalf("CreateMultipartUpload object_key context=%q, want %q", rec.lastMPUEncryption.EncryptionContext["object_key"], upload.S3Key)
	}
	nf, err := b.store.Stat(ctx, "/upload-overwrite.bin")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if nf.File.FileID != "legacy-upload-file" {
		t.Fatalf("file_id=%q, want existing file id", nf.File.FileID)
	}
	if nf.File.Revision != 2 {
		t.Fatalf("revision=%d, want 2", nf.File.Revision)
	}
	if nf.File.StorageEncryptionMode != datastore.StorageEncryptionSSEKMS {
		t.Fatalf("file encryption mode=%q, want %q", nf.File.StorageEncryptionMode, datastore.StorageEncryptionSSEKMS)
	}
	if nf.File.StorageEncryptionKeyID != resolvedSSEKMSTestPolicy().KMSKeyID {
		t.Fatalf("file encryption key=%q, want %q", nf.File.StorageEncryptionKeyID, resolvedSSEKMSTestPolicy().KMSKeyID)
	}
}

func TestKMSWriteFailureDoesNotFallbackToPlaintext(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, false, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})
	rec.failPut = true

	_, _, err := b.WriteCtxIfRevisionWithTagsResult(
		context.Background(),
		"/kms-fail.txt",
		[]byte("data"),
		0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate,
		-1,
		nil,
		"",
	)
	if err == nil {
		t.Fatal("write error = nil, want KMS failure")
	}
	if rec.lastPutEncryption.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("failed PutObject mode=%q, want %q", rec.lastPutEncryption.Mode, s3client.EncryptionModeSSEKMS)
	}
	if _, statErr := b.store.Stat(context.Background(), "/kms-fail.txt"); !errors.Is(statErr, datastore.ErrNotFound) {
		t.Fatalf("stat after failed write error=%v, want ErrNotFound", statErr)
	}
}

func TestKMSMultipartFailureDoesNotFallbackToPlaintext(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, true, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})
	rec.failMPU = true

	_, err := b.InitiateUploadV2IfRevision(context.Background(), "/kms-fail.bin", int64(s3client.MinPartSize), -1, "")
	if err == nil {
		t.Fatal("initiate upload error = nil, want KMS failure")
	}
	if rec.lastMPUEncryption.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("failed CreateMultipartUpload mode=%q, want %q", rec.lastMPUEncryption.Mode, s3client.EncryptionModeSSEKMS)
	}
	if _, statErr := b.store.Stat(context.Background(), "/kms-fail.bin"); !errors.Is(statErr, datastore.ErrNotFound) {
		t.Fatalf("stat after failed initiate error=%v, want ErrNotFound", statErr)
	}
}

func TestReadPlanDoesNotUseEncryptionHeaders(t *testing.T) {
	b, rec := newTestBackendWithRecordingS3(t, false, Options{
		TenantID:           "tenant-a",
		S3EncryptionPolicy: resolvedSSEKMSTestPolicy(),
	})
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(
		context.Background(),
		"/read.bin",
		bytes.Repeat([]byte("x"), smallFileThreshold),
		0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate,
		-1,
		nil,
		"",
	); err != nil {
		t.Fatalf("write: %v", err)
	}
	rec.lastPutEncryption = s3client.EncryptionOpts{}

	plan, err := b.ReadPlanCtx(context.Background(), "/read.bin")
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	if plan.PresignURL == "" {
		t.Fatal("read plan PresignURL is empty")
	}
	if rec.lastPutEncryption.Mode != "" {
		t.Fatalf("read path mutated write encryption mode=%q", rec.lastPutEncryption.Mode)
	}
}

func insertLegacyS3File(t *testing.T, b *Dat9Backend, path, fileID string) {
	t.Helper()
	now := time.Now().UTC()
	if err := b.store.InsertFile(context.Background(), &datastore.File{
		FileID:                fileID,
		StorageType:           datastore.StorageS3,
		StorageRef:            "blobs/" + fileID,
		StorageEncryptionMode: datastore.StorageEncryptionLegacy,
		SizeBytes:             1,
		Revision:              1,
		Status:                datastore.StatusConfirmed,
		CreatedAt:             now,
		ConfirmedAt:           &now,
	}); err != nil {
		t.Fatalf("insert legacy file: %v", err)
	}
	if err := b.store.InsertNode(context.Background(), &datastore.FileNode{
		NodeID:     "node-" + fileID,
		Path:       path,
		ParentPath: pathutil.ParentPath(path),
		Name:       pathutil.BaseName(path),
		FileID:     fileID,
		CreatedAt:  now,
	}); err != nil {
		t.Fatalf("insert legacy node: %v", err)
	}
}
