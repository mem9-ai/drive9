package backend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

func TestPatchAndAppendRejectDBBackedFilesWithSentinel(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	if _, err := b.Write("/small.txt", []byte("hello"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("Write: %v", err)
	}

	t.Run("append", func(t *testing.T) {
		_, err := b.InitiateAppendUploadIfRevision(ctx, "/small.txt", 1, s3client.PartSize, -1)
		if !errors.Is(err, ErrNotS3Stored) {
			t.Fatalf("InitiateAppendUploadIfRevision error = %v, want ErrNotS3Stored", err)
		}
	})

	t.Run("patch", func(t *testing.T) {
		_, err := b.InitiatePatchUploadIfRevision(ctx, "/small.txt", 6, []int{1}, s3client.PartSize, -1)
		if !errors.Is(err, ErrNotS3Stored) {
			t.Fatalf("InitiatePatchUploadIfRevision error = %v, want ErrNotS3Stored", err)
		}
	})
}

// patchChecksumRecordingS3Client wraps an S3Client and records the
// ChecksumAlgo argument passed to CreateMultipartUpload and
// PresignUploadPart.  This is a regression test helper for issue #555:
// the patch path must use ChecksumAlgoNone so that the presigned URL
// contract is consistent with the MPU checksum declaration.
type patchChecksumRecordingS3Client struct {
	s3client.S3Client
	createMPUAlgos      []s3client.ChecksumAlgo
	presignPartAlgos    []s3client.ChecksumAlgo
	presignPartChecksum []string
}

func (c *patchChecksumRecordingS3Client) CreateMultipartUpload(ctx context.Context, key string, algo s3client.ChecksumAlgo, encOpts s3client.EncryptionOpts) (*s3client.MultipartUpload, error) {
	c.createMPUAlgos = append(c.createMPUAlgos, algo)
	return c.S3Client.CreateMultipartUpload(ctx, key, algo, encOpts)
}

func (c *patchChecksumRecordingS3Client) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, algo s3client.ChecksumAlgo, checksumValue string, ttl time.Duration) (*s3client.UploadPartURL, error) {
	c.presignPartAlgos = append(c.presignPartAlgos, algo)
	c.presignPartChecksum = append(c.presignPartChecksum, checksumValue)
	return c.S3Client.PresignUploadPart(ctx, key, uploadID, partNumber, partSize, algo, checksumValue, ttl)
}

// TestPatchUploadUsesChecksumAlgoNone verifies that InitiatePatchUploadIfRevision
// calls CreateMultipartUpload and PresignUploadPart with ChecksumAlgoNone.
// This is a regression test for issue #555: declaring ChecksumAlgoSHA256 at
// MPU creation forces S3 to require a checksum header on every UploadPart,
// but patch parts are assembled client-side after presigning so the checksum
// cannot be included in the signature — causing S3 403 or 400.
func TestPatchUploadUsesChecksumAlgoNone(t *testing.T) {
	// Set up backend with recording S3 client.
	s3Dir, err := os.MkdirTemp("", "dat9-s3-patch-checksum-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initBackendSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("Open datastore: %v", err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	localS3, err := s3client.NewLocal(s3Dir, "http://localhost:9091/s3")
	if err != nil {
		t.Fatalf("NewLocal S3: %v", err)
	}
	rec := &patchChecksumRecordingS3Client{S3Client: localS3}

	b, err := NewWithS3ModeAndOptions(store, rec, true, Options{})
	if err != nil {
		t.Fatalf("NewWithS3ModeAndOptions: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	ctx := context.Background()

	// Step 1: Create an S3-backed file via v1 upload + confirm.
	totalSize := int64(2 * s3client.PartSize) // 2 parts
	plan, err := b.InitiateUpload(ctx, "/patch-checksum-test.bin", totalSize)
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	if len(plan.Parts) == 0 {
		t.Fatal("InitiateUpload returned no parts")
	}

	// Get S3 upload ID for direct part upload.
	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}

	// Upload parts via the underlying local S3 client directly.
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
		_, err := localS3.UploadPart(ctx, upload.S3UploadID, p.Number, bytes.NewReader(partData[start:end]))
		if err != nil {
			t.Fatalf("upload part %d: %v", p.Number, err)
		}
	}
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatalf("ConfirmUpload: %v", err)
	}

	// Reset recording — we only care about the patch path calls.
	rec.createMPUAlgos = nil
	rec.presignPartAlgos = nil
	rec.presignPartChecksum = nil

	// Step 2: Initiate a patch upload marking part 1 as dirty.
	patchPlan, err := b.InitiatePatchUploadIfRevision(ctx, "/patch-checksum-test.bin", totalSize, []int{1}, s3client.PartSize, -1)
	if err != nil {
		t.Fatalf("InitiatePatchUploadIfRevision: %v", err)
	}
	if patchPlan == nil {
		t.Fatal("InitiatePatchUploadIfRevision returned nil plan")
	}

	// Step 3: Assert CreateMultipartUpload used ChecksumAlgoNone.
	if len(rec.createMPUAlgos) != 1 {
		t.Fatalf("CreateMultipartUpload calls = %d, want 1", len(rec.createMPUAlgos))
	}
	if rec.createMPUAlgos[0] != s3client.ChecksumAlgoNone {
		t.Errorf("patch CreateMultipartUpload algo = %v, want %v", rec.createMPUAlgos[0], s3client.ChecksumAlgoNone)
	}

	// Step 4: Assert PresignUploadPart used ChecksumAlgoNone with empty checksum.
	// Part 1 is dirty → should have a presigned upload URL.
	// Part 2 is clean → server-side copy, no presign.
	if len(rec.presignPartAlgos) == 0 {
		t.Fatal("expected at least 1 PresignUploadPart call for dirty part")
	}
	for i, algo := range rec.presignPartAlgos {
		if algo != s3client.ChecksumAlgoNone {
			t.Errorf("patch PresignUploadPart[%d] algo = %v, want %v", i, algo, s3client.ChecksumAlgoNone)
		}
		if rec.presignPartChecksum[i] != "" {
			t.Errorf("patch PresignUploadPart[%d] checksum = %q, want empty", i, rec.presignPartChecksum[i])
		}
	}
}
