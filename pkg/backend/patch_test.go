package backend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

type patchCopyRecordingS3Client struct {
	s3client.S3Client

	mu sync.Mutex

	active        int
	maxActive     int
	started       chan int
	release       chan struct{}
	failPart      int
	failErr       error
	abortCalls    int
	activeAtAbort int
	abortedKey    string
	abortedID     string
}

func (c *patchCopyRecordingS3Client) UploadPartCopy(
	ctx context.Context,
	destKey string,
	uploadID string,
	partNumber int,
	sourceKey string,
	startByte int64,
	endByte int64,
) (string, error) {
	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()

	if c.started != nil {
		c.started <- partNumber
	}
	if partNumber == c.failPart {
		return "", c.failErr
	}
	if c.release != nil {
		select {
		case <-c.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return c.S3Client.UploadPartCopy(ctx, destKey, uploadID, partNumber, sourceKey, startByte, endByte)
}

func (c *patchCopyRecordingS3Client) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	c.mu.Lock()
	c.abortCalls++
	c.activeAtAbort = c.active
	c.abortedKey = key
	c.abortedID = uploadID
	c.mu.Unlock()
	return c.S3Client.AbortMultipartUpload(ctx, key, uploadID)
}

func uploadPatchTestFile(t *testing.T, b *Dat9Backend, localS3 *s3client.LocalS3Client, path string, partCount int) {
	t.Helper()
	ctx := context.Background()
	totalSize := int64(partCount) * s3client.PartSize
	plan, err := b.InitiateUpload(ctx, path, totalSize)
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	for _, part := range plan.Parts {
		data := bytes.Repeat([]byte{byte(part.Number)}, int(part.Size))
		if _, err := localS3.UploadPart(ctx, upload.S3UploadID, part.Number, bytes.NewReader(data)); err != nil {
			t.Fatalf("upload part %d: %v", part.Number, err)
		}
	}
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatalf("ConfirmUpload: %v", err)
	}
}

func waitForPatchCopyStarts(t *testing.T, started <-chan int, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("started %d copies, want %d", i, count)
		}
	}
}

func TestPatchUploadCopiesRetainedPartsConcurrentlyAndCompletesInPartOrder(t *testing.T) {
	b := newTestBackendWithS3(t)
	localS3, ok := b.s3.(*s3client.LocalS3Client)
	if !ok {
		t.Fatalf("S3 client = %T, want *LocalS3Client", b.s3)
	}
	const path = "/patch-copy-concurrency.bin"
	uploadPatchTestFile(t, b, localS3, path, 4)

	recorder := &patchCopyRecordingS3Client{
		S3Client: localS3,
		started:  make(chan int, 4),
		release:  make(chan struct{}),
	}
	b.s3 = recorder

	ctx := context.Background()
	planDone := make(chan *PatchPlan, 1)
	errDone := make(chan error, 1)
	go func() {
		plan, err := b.InitiatePatchUploadIfRevision(
			ctx,
			path,
			4*s3client.PartSize,
			[]int{4},
			s3client.PartSize,
			-1,
		)
		planDone <- plan
		errDone <- err
	}()

	waitForPatchCopyStarts(t, recorder.started, 3)
	recorder.mu.Lock()
	maxActive := recorder.maxActive
	recorder.mu.Unlock()
	close(recorder.release)
	if maxActive != 3 {
		t.Fatalf("max active copies = %d, want 3", maxActive)
	}

	plan := <-planDone
	if err := <-errDone; err != nil {
		t.Fatalf("InitiatePatchUploadIfRevision: %v", err)
	}
	if len(plan.CopiedParts) != 3 || plan.CopiedParts[0] != 1 || plan.CopiedParts[1] != 2 || plan.CopiedParts[2] != 3 {
		t.Fatalf("copied parts = %v, want [1 2 3]", plan.CopiedParts)
	}

	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	dirty := bytes.Repeat([]byte{9}, s3client.PartSize)
	if _, err := localS3.UploadPart(ctx, upload.S3UploadID, 4, bytes.NewReader(dirty)); err != nil {
		t.Fatalf("upload dirty part: %v", err)
	}
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatalf("ConfirmUpload: %v", err)
	}

	for partNumber := 1; partNumber <= 4; partNumber++ {
		got, err := b.ReadCtx(ctx, path, int64(partNumber-1)*s3client.PartSize, 1)
		if err != nil {
			t.Fatalf("ReadCtx part %d: %v", partNumber, err)
		}
		want := byte(partNumber)
		if partNumber == 4 {
			want = 9
		}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("part %d first byte = %v, want [%d]", partNumber, got, want)
		}
	}

	recorder.mu.Lock()
	abortCalls := recorder.abortCalls
	recorder.mu.Unlock()
	if abortCalls != 0 {
		t.Fatalf("abort calls = %d, want 0", abortCalls)
	}
}

func TestPatchUploadCopyFailureWaitsThenAbortsWithoutMetadata(t *testing.T) {
	b := newTestBackendWithS3(t)
	localS3, ok := b.s3.(*s3client.LocalS3Client)
	if !ok {
		t.Fatalf("S3 client = %T, want *LocalS3Client", b.s3)
	}
	const path = "/patch-copy-failure.bin"
	uploadPatchTestFile(t, b, localS3, path, 3)

	copyErr := errors.New("copy failed")
	recorder := &patchCopyRecordingS3Client{
		S3Client: localS3,
		failPart: 2,
		failErr:  copyErr,
	}
	b.s3 = recorder

	_, err := b.InitiatePatchUploadIfRevision(
		context.Background(),
		path,
		3*s3client.PartSize,
		[]int{3},
		s3client.PartSize,
		-1,
	)
	if !errors.Is(err, copyErr) {
		t.Fatalf("InitiatePatchUploadIfRevision error = %v, want copy error", err)
	}

	recorder.mu.Lock()
	abortCalls := recorder.abortCalls
	activeAtAbort := recorder.activeAtAbort
	abortedKey := recorder.abortedKey
	abortedID := recorder.abortedID
	recorder.mu.Unlock()
	if abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", abortCalls)
	}
	if activeAtAbort != 0 {
		t.Fatalf("active copies when abort started = %d, want 0", activeAtAbort)
	}
	if _, err := localS3.ListParts(context.Background(), abortedKey, abortedID); err == nil {
		t.Fatal("ListParts after abort = nil error, want missing multipart upload")
	}
	if upload, err := b.activeUploadByPath(context.Background(), path); err != nil {
		t.Fatalf("activeUploadByPath: %v", err)
	} else if upload != nil {
		t.Fatalf("active upload after copy failure = %+v, want nil", upload)
	}
}

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
