package backend

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

func newTestBackendWithS3(t *testing.T) *Dat9Backend {
	t.Helper()
	dbFile, err := os.CreateTemp("", "dat9-*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbFile.Close()
	t.Cleanup(func() { os.Remove(dbFile.Name()) })

	blobDir, err := os.MkdirTemp("", "dat9-blobs-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(blobDir) })

	s3Dir, err := os.MkdirTemp("", "dat9-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(s3Dir) })

	store, err := meta.Open(dbFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	s3c, err := s3client.NewLocal(s3Dir, "http://localhost:9091/s3")
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewWithS3(store, blobDir, s3c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCapabilityProviderNoS3(t *testing.T) {
	b := newTestBackend(t)
	caps := b.GetCapabilities()
	if caps.IsObjectStore {
		t.Error("expected IsObjectStore=false without S3")
	}
}

func TestCapabilityProviderWithS3(t *testing.T) {
	b := newTestBackendWithS3(t)
	caps := b.GetCapabilities()
	if !caps.IsObjectStore {
		t.Error("expected IsObjectStore=true with S3")
	}

	// Verify interface compliance
	var _ filesystem.CapabilityProvider = b
}

func TestIsLargeFile(t *testing.T) {
	b := newTestBackendWithS3(t)
	if b.IsLargeFile(100) {
		t.Error("100 bytes should not be large")
	}
	if !b.IsLargeFile(1 << 20) {
		t.Error("1MB should be large")
	}

	// Without S3, nothing is large
	bNoS3 := newTestBackend(t)
	if bNoS3.IsLargeFile(10 << 20) {
		t.Error("without S3, nothing should be large")
	}
}

func TestInitiateAndConfirmUpload(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	// Initiate upload for a 2MB file
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/bigfile.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	if plan.UploadID == "" || plan.Key == "" {
		t.Fatalf("empty plan: %+v", plan)
	}
	if len(plan.Parts) == 0 {
		t.Fatal("expected parts in plan")
	}

	// Verify upload record exists
	upload, err := b.GetUpload(plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Status != meta.UploadUploading {
		t.Errorf("expected UPLOADING, got %s", upload.Status)
	}
	if upload.TargetPath != "/bigfile.bin" {
		t.Errorf("expected /bigfile.bin, got %s", upload.TargetPath)
	}

	// Simulate uploading all parts via the S3 client directly
	partData := make([]byte, totalSize)
	for i := range partData {
		partData[i] = byte(i % 256)
	}

	for _, p := range plan.Parts {
		start := int64(p.Number-1) * s3client.PartSize
		end := start + p.Size
		if end > totalSize {
			end = totalSize
		}
		_, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, p.Number, bytes.NewReader(partData[start:end]))
		if err != nil {
			t.Fatalf("upload part %d: %v", p.Number, err)
		}
	}

	// Confirm upload
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}

	// Verify upload is completed
	upload, _ = b.GetUpload(plan.UploadID)
	if upload.Status != meta.UploadCompleted {
		t.Errorf("expected COMPLETED, got %s", upload.Status)
	}

	// Verify file node exists and can be stat'd
	info, err := b.Stat("/bigfile.bin")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != totalSize {
		t.Errorf("expected size %d, got %d", totalSize, info.Size)
	}

	// Verify presigned GET URL
	url, err := b.PresignGetObject(ctx, "/bigfile.bin")
	if err != nil {
		t.Fatal(err)
	}
	if url == "" {
		t.Error("expected non-empty presigned URL")
	}
}

func TestResumeUpload(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	// Initiate upload for a 20MB file (3 parts: 8MB + 8MB + 4MB)
	totalSize := int64(20 << 20)
	plan, err := b.InitiateUpload(ctx, "/resume-test.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}

	upload, _ := b.GetUpload(plan.UploadID)

	// Upload only part 1 (simulate partial upload)
	data := make([]byte, s3client.PartSize)
	b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, 1, bytes.NewReader(data))

	// Resume should return parts 2 and 3
	resumed, err := b.ResumeUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.Parts) != 2 {
		t.Fatalf("expected 2 missing parts, got %d", len(resumed.Parts))
	}
	if resumed.Parts[0].Number != 2 || resumed.Parts[1].Number != 3 {
		t.Errorf("unexpected part numbers: %d, %d", resumed.Parts[0].Number, resumed.Parts[1].Number)
	}
}

func TestAbortUpload(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUpload(ctx, "/abort-test.bin", 2<<20)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.AbortUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}

	upload, _ := b.GetUpload(plan.UploadID)
	if upload.Status != meta.UploadAborted {
		t.Errorf("expected ABORTED, got %s", upload.Status)
	}
}

func TestListUploads(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	b.InitiateUpload(ctx, "/list-test.bin", 2<<20)
	b.InitiateUpload(ctx, "/list-test.bin", 3<<20)

	uploads, err := b.ListUploads("/list-test.bin", meta.UploadUploading)
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 2 {
		t.Errorf("expected 2 uploads, got %d", len(uploads))
	}
}
