package backend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

// entriesFromInts converts a slice of part numbers to PresignPartEntry slice (no checksums).
func entriesFromInts(nums []int) []PresignPartEntry {
	entries := make([]PresignPartEntry, len(nums))
	for i, n := range nums {
		entries[i] = PresignPartEntry{PartNumber: n}
	}
	return entries
}

func newTestBackendWithS3(t *testing.T) *Dat9Backend {
	return newTestBackendWithS3AndOptions(t, Options{})
}

func newTestBackendWithS3AndOptions(t *testing.T, opts Options) *Dat9Backend {
	t.Helper()
	s3Dir, err := os.MkdirTemp("", "dat9-s3-*")
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

	s3c, err := s3client.NewLocal(s3Dir, "http://localhost:9091/s3")
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewWithS3ModeAndOptions(store, s3c, true, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func newTestBackendNoS3(t *testing.T) *Dat9Backend {
	t.Helper()
	initBackendSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })
	b, err := New(store)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCapabilityProviderNoS3(t *testing.T) {
	b := newTestBackendNoS3(t)
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
	bNoS3 := newTestBackendNoS3(t)
	if bNoS3.IsLargeFile(10 << 20) {
		t.Error("without S3, nothing should be large")
	}
}

func TestInitiateUploadPropagatesActiveUploadLookupError(t *testing.T) {
	b := newTestBackendWithS3(t)
	origLookup := lookupActiveUploadByPath
	t.Cleanup(func() { lookupActiveUploadByPath = origLookup })

	lookupActiveUploadByPath = func(*datastore.Store, context.Context, string) (*datastore.Upload, error) {
		return nil, errors.New("lookup failed")
	}

	_, err := b.InitiateUpload(context.Background(), "/lookup-error.bin", 2<<20)
	if err == nil {
		t.Fatal("expected initiate upload to fail")
	}
	if !strings.Contains(err.Error(), "lookup active upload") || !strings.Contains(err.Error(), "lookup failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitiateUploadV2PropagatesActiveUploadLookupError(t *testing.T) {
	b := newTestBackendWithS3(t)
	origLookup := lookupActiveUploadByPath
	t.Cleanup(func() { lookupActiveUploadByPath = origLookup })

	lookupActiveUploadByPath = func(*datastore.Store, context.Context, string) (*datastore.Upload, error) {
		return nil, errors.New("lookup failed")
	}

	_, err := b.InitiateUploadV2(context.Background(), "/lookup-error-v2.bin", 2<<20)
	if err == nil {
		t.Fatal("expected initiate upload v2 to fail")
	}
	if !strings.Contains(err.Error(), "lookup active upload") || !strings.Contains(err.Error(), "lookup failed") {
		t.Fatalf("unexpected error: %v", err)
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
	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Status != datastore.UploadUploading {
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
		start := int64(p.Number-1) * plan.PartSize
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
	upload, _ = b.GetUpload(ctx, plan.UploadID)
	if upload.Status != datastore.UploadCompleted {
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

	upload, _ := b.GetUpload(ctx, plan.UploadID)

	// Upload only part 1 (simulate partial upload)
	data := make([]byte, upload.PartSize)
	if _, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, 1, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

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

	upload, _ := b.GetUpload(ctx, plan.UploadID)
	if upload.Status != datastore.UploadAborted {
		t.Errorf("expected ABORTED, got %s", upload.Status)
	}
}

func TestListUploads(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	// One upload per path — use different paths
	if _, err := b.InitiateUpload(ctx, "/list-a.bin", 2<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := b.InitiateUpload(ctx, "/list-b.bin", 3<<20); err != nil {
		t.Fatal(err)
	}

	uploadsA, err := b.ListUploads(ctx, "/list-a.bin", datastore.UploadUploading)
	if err != nil {
		t.Fatal(err)
	}
	if len(uploadsA) != 1 {
		t.Errorf("expected 1 upload for /list-a.bin, got %d", len(uploadsA))
	}
}

func TestOneUploadPerPath(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	_, err := b.InitiateUpload(ctx, "/dup.bin", 2<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Second upload for same path should fail
	_, err = b.InitiateUpload(ctx, "/dup.bin", 3<<20)
	if err == nil {
		t.Error("expected error for duplicate active upload")
	}
}

func TestInitiateUploadRejectsReservedQuotaOverflow(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{MaxTenantStorageBytes: 100_000})
	ctx := context.Background()

	if _, err := b.InitiateUpload(ctx, "/reserved-a.bin", 60_000); err != nil {
		t.Fatal(err)
	}
	_, err := b.InitiateUpload(ctx, "/reserved-b.bin", 50_000)
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected ErrStorageQuotaExceeded, got %v", err)
	}
}

func TestInitiatePatchUploadRejectsQuotaGrowth(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{MaxTenantStorageBytes: 100_000})
	ctx := context.Background()

	data := bytes.Repeat([]byte("a"), 80_000)
	if _, err := b.Write("/patch-quota.bin", data, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	_, err := b.InitiatePatchUpload(ctx, "/patch-quota.bin", 130_000, []int{1}, s3client.PartSize)
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected ErrStorageQuotaExceeded, got %v", err)
	}
}

// --- v2 presign tests (T2) ---

func TestInitiateUploadV2(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	totalSize := int64(100 << 20) // 100 MB
	plan, err := b.InitiateUploadV2(ctx, "/v2-test.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	if plan.UploadID == "" || plan.Key == "" {
		t.Fatalf("empty plan: %+v", plan)
	}
	if plan.TotalParts == 0 {
		t.Fatal("expected total_parts > 0")
	}
	if plan.PartSize < s3client.MinPartSize {
		t.Errorf("part_size %d below minimum %d", plan.PartSize, s3client.MinPartSize)
	}
	if plan.Resumable {
		t.Error("expected resumable=false for phase 1")
	}
	if plan.ChecksumContract.Required {
		t.Error("expected checksum_contract.required=false for phase 1")
	}
	if len(plan.ChecksumContract.Supported) == 0 {
		t.Error("expected checksum_contract.supported to be non-empty")
	}

	// Verify upload starts in INITIATED status
	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Status != datastore.UploadInitiated {
		t.Errorf("expected INITIATED, got %s", upload.Status)
	}
	if upload.PartSize != plan.PartSize {
		t.Errorf("stored part_size %d != plan part_size %d", upload.PartSize, plan.PartSize)
	}
}

func TestPresignPartSingle(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/presign-single.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Valid part number
	u, err := b.PresignPart(ctx, plan.UploadID, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if u.URL == "" {
		t.Error("expected non-empty presigned URL")
	}
	if u.Number != 1 {
		t.Errorf("expected part number 1, got %d", u.Number)
	}

	// After first presign, status should be UPLOADING
	upload, _ := b.GetUpload(ctx, plan.UploadID)
	if upload.Status != datastore.UploadUploading {
		t.Errorf("expected UPLOADING after first presign, got %s", upload.Status)
	}

	// Invalid part number: 0
	if _, err := b.PresignPart(ctx, plan.UploadID, 0, nil); err == nil {
		t.Error("expected error for part_number=0")
	}

	// Invalid part number: too large
	if _, err := b.PresignPart(ctx, plan.UploadID, plan.TotalParts+1, nil); err == nil {
		t.Error("expected error for part_number > total_parts")
	}

	// Last valid part
	u, err = b.PresignPart(ctx, plan.UploadID, plan.TotalParts, nil)
	if err != nil {
		t.Fatalf("presign last part: %v", err)
	}
	if u.Number != plan.TotalParts {
		t.Errorf("expected part number %d, got %d", plan.TotalParts, u.Number)
	}
}

func TestPresignPartConcurrentTransition(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/presign-concurrent.bin", 256<<20)
	if err != nil {
		t.Fatal(err)
	}

	workers := plan.TotalParts
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		partNumber := i + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			u, err := b.PresignPart(ctx, plan.UploadID, partNumber, nil)
			if err != nil {
				errCh <- err
				return
			}
			if u == nil || u.URL == "" || u.Number != partNumber {
				errCh <- errors.New("unexpected presign response")
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent presign failed: %v", err)
	}

	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Status != datastore.UploadUploading {
		t.Fatalf("expected UPLOADING after concurrent presign, got %s", upload.Status)
	}
}

func TestPresignPartsBatch(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/presign-batch.bin", 50<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Presign parts 1, 2, 3
	urls, err := b.PresignParts(ctx, plan.UploadID, entriesFromInts([]int{1, 2, 3}))
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 3 {
		t.Fatalf("expected 3 urls, got %d", len(urls))
	}
	for i, u := range urls {
		if u.URL == "" {
			t.Errorf("part %d: empty URL", i+1)
		}
	}

	// Verify status transitioned
	upload, _ := b.GetUpload(ctx, plan.UploadID)
	if upload.Status != datastore.UploadUploading {
		t.Errorf("expected UPLOADING, got %s", upload.Status)
	}
}

func TestPresignBatchDuplicateRejected(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/presign-dup.bin", 50<<20)
	if err != nil {
		t.Fatal(err)
	}

	_, err = b.PresignParts(ctx, plan.UploadID, entriesFromInts([]int{1, 2, 1}))
	if err == nil {
		t.Fatal("expected error for duplicate part numbers")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate error, got: %v", err)
	}
}

func TestPresignBatchLimitExceeded(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	// Need a large enough file for >500 parts
	// With 8 MiB default part size, 500*8MiB = 4 GiB → use a file that yields >500 parts
	totalSize := int64(5000 << 20) // 5000 MB ≈ ~625 parts at 8 MiB each
	plan, err := b.InitiateUploadV2(ctx, "/presign-limit.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}

	// Build a batch of 501 parts
	parts := make([]int, MaxPresignBatch+1)
	for i := range parts {
		parts[i] = i + 1
	}
	// Ensure we don't exceed total parts
	if parts[len(parts)-1] > plan.TotalParts {
		t.Skipf("not enough parts (%d) to test batch limit", plan.TotalParts)
	}

	_, err = b.PresignParts(ctx, plan.UploadID, entriesFromInts(parts))
	if err == nil {
		t.Fatal("expected error for batch > MaxPresignBatch")
	}
	if !strings.Contains(err.Error(), "batch too large") {
		t.Errorf("expected batch too large error, got: %v", err)
	}
}

func TestPresignAfterAbortFails(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/presign-abort.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.AbortUploadV2(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}

	// Single presign should fail
	_, err = b.PresignPart(ctx, plan.UploadID, 1, nil)
	if err == nil {
		t.Error("expected error presigning after abort")
	}

	// Batch presign should fail
	_, err = b.PresignParts(ctx, plan.UploadID, entriesFromInts([]int{1, 2}))
	if err == nil {
		t.Error("expected error batch presigning after abort")
	}
}

func TestPresignPartsInactiveUploadTakesPrecedenceOverBatchValidation(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/presign-inactive-priority.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.AbortUploadV2(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}

	_, err = b.PresignParts(ctx, plan.UploadID, entriesFromInts([]int{1, 1}))
	if !errors.Is(err, datastore.ErrUploadNotActive) {
		t.Fatalf("expected ErrUploadNotActive, got %v", err)
	}
}

func TestPresignPartsExpiredUploadTakesPrecedenceOverBatchValidation(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	totalSize := int64(5000 << 20)
	plan, err := b.InitiateUploadV2(ctx, "/presign-expired-priority.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.store.DB().ExecContext(ctx, `UPDATE uploads SET expires_at = ? WHERE upload_id = ?`,
		time.Now().Add(-1*time.Hour), plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	parts := make([]int, MaxPresignBatch+1)
	for i := range parts {
		parts[i] = i + 1
	}
	if parts[len(parts)-1] > plan.TotalParts {
		t.Skipf("not enough parts (%d) to test batch limit precedence", plan.TotalParts)
	}

	_, err = b.PresignParts(ctx, plan.UploadID, entriesFromInts(parts))
	if !errors.Is(err, datastore.ErrUploadExpired) {
		t.Fatalf("expected ErrUploadExpired, got %v", err)
	}
}

func TestPresignExpiredUpload(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/presign-expired.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Manually expire the upload by setting expires_at to the past
	_, err = b.store.DB().ExecContext(ctx, `UPDATE uploads SET expires_at = ? WHERE upload_id = ?`,
		time.Now().Add(-1*time.Hour), plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	// Single presign should fail with expired error
	_, err = b.PresignPart(ctx, plan.UploadID, 1, nil)
	if err == nil {
		t.Fatal("expected error for expired upload")
	}
	if err != datastore.ErrUploadExpired {
		t.Errorf("expected ErrUploadExpired, got: %v", err)
	}

	// Batch presign on a new expired upload
	plan2, err := b.InitiateUploadV2(ctx, "/presign-expired2.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.store.DB().ExecContext(ctx, `UPDATE uploads SET expires_at = ? WHERE upload_id = ?`,
		time.Now().Add(-1*time.Hour), plan2.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.PresignParts(ctx, plan2.UploadID, entriesFromInts([]int{1}))
	if err == nil {
		t.Fatal("expected error for expired upload batch")
	}
	if err != datastore.ErrUploadExpired {
		t.Errorf("expected ErrUploadExpired, got: %v", err)
	}
}

func TestExpiredUploadNoLongerBlocksSamePathInitiate(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/expired-retry.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}

	_, err = b.store.DB().ExecContext(ctx, `UPDATE uploads SET expires_at = ? WHERE upload_id = ?`,
		time.Now().Add(-1*time.Hour), plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	plan2, err := b.InitiateUploadV2(ctx, "/expired-retry.bin", 20<<20)
	if err != nil {
		t.Fatalf("expected expired upload to stop blocking the path, got %v", err)
	}
	if plan2.UploadID == plan.UploadID {
		t.Fatal("expected a new upload record after the previous one expired")
	}
}

func TestExpiredUploadReservationNoLongerCountsTowardQuota(t *testing.T) {
	b := newTestBackendWithS3AndOptions(t, Options{MaxTenantStorageBytes: 30 << 20})
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/expired-quota.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := b.InitiateUploadV2(ctx, "/fresh-quota.bin", 20<<20); !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected quota rejection while first reservation is active, got %v", err)
	}

	_, err = b.store.DB().ExecContext(ctx, `UPDATE uploads SET expires_at = ? WHERE upload_id = ?`,
		time.Now().Add(-1*time.Hour), plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	plan2, err := b.InitiateUploadV2(ctx, "/fresh-quota.bin", 20<<20)
	if err != nil {
		t.Fatalf("expected expired reservation to stop consuming quota, got %v", err)
	}
	if plan2.UploadID == "" {
		t.Fatal("expected a new upload plan")
	}
}

func TestV2FullUploadFlow(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	totalSize := int64(20 << 20) // 20 MB
	plan, err := b.InitiateUploadV2(ctx, "/v2-full.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}

	// Status starts as INITIATED
	upload, _ := b.GetUpload(ctx, plan.UploadID)
	if upload.Status != datastore.UploadInitiated {
		t.Fatalf("expected INITIATED, got %s", upload.Status)
	}

	// Presign all parts
	partNums := make([]int, plan.TotalParts)
	for i := range partNums {
		partNums[i] = i + 1
	}
	urls, err := b.PresignParts(ctx, plan.UploadID, entriesFromInts(partNums))
	if err != nil {
		t.Fatal(err)
	}

	// Status should now be UPLOADING
	upload, _ = b.GetUpload(ctx, plan.UploadID)
	if upload.Status != datastore.UploadUploading {
		t.Fatalf("expected UPLOADING, got %s", upload.Status)
	}

	// Upload all parts via S3 client, collecting ETags for v2 complete
	partData := make([]byte, totalSize)
	for i := range partData {
		partData[i] = byte(i % 256)
	}
	completeParts := make([]CompletePart, len(urls))
	for i, u := range urls {
		start := int64(u.Number-1) * upload.PartSize
		end := start + u.Size
		if end > totalSize {
			end = totalSize
		}
		etag, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, u.Number, bytes.NewReader(partData[start:end]))
		if err != nil {
			t.Fatalf("upload part %d: %v", u.Number, err)
		}
		completeParts[i] = CompletePart{Number: u.Number, ETag: etag}
	}

	// Confirm upload via v2 (with client-supplied parts)
	if err := b.ConfirmUploadV2(ctx, plan.UploadID, completeParts); err != nil {
		t.Fatal(err)
	}

	// Verify completed
	upload, _ = b.GetUpload(ctx, plan.UploadID)
	if upload.Status != datastore.UploadCompleted {
		t.Errorf("expected COMPLETED, got %s", upload.Status)
	}

	// Verify file node
	info, err := b.Stat("/v2-full.bin")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != totalSize {
		t.Errorf("expected size %d, got %d", totalSize, info.Size)
	}
}

func TestV2CompleteETagMismatch(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	totalSize := int64(20 << 20)
	plan, err := b.InitiateUploadV2(ctx, "/v2-etag-mismatch.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}

	// Presign and upload all parts
	partNums := make([]int, plan.TotalParts)
	for i := range partNums {
		partNums[i] = i + 1
	}
	urls, err := b.PresignParts(ctx, plan.UploadID, entriesFromInts(partNums))
	if err != nil {
		t.Fatal(err)
	}

	upload, _ := b.GetUpload(ctx, plan.UploadID)
	partData := make([]byte, totalSize)
	completeParts := make([]CompletePart, len(urls))
	for i, u := range urls {
		start := int64(u.Number-1) * upload.PartSize
		end := start + u.Size
		if end > totalSize {
			end = totalSize
		}
		etag, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, u.Number, bytes.NewReader(partData[start:end]))
		if err != nil {
			t.Fatalf("upload part %d: %v", u.Number, err)
		}
		completeParts[i] = CompletePart{Number: u.Number, ETag: etag}
	}

	// Tamper with the first part's ETag
	completeParts[0].ETag = "bad-etag"

	err = b.ConfirmUploadV2(ctx, plan.UploadID, completeParts)
	if err == nil {
		t.Fatal("expected error for ETag mismatch")
	}
	if !strings.Contains(err.Error(), "ETag mismatch") {
		t.Errorf("expected ETag mismatch error, got: %v", err)
	}
}

func TestV2CompleteAcceptsQuotedClientETag(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	totalSize := int64(20 << 20)
	plan, err := b.InitiateUploadV2(ctx, "/v2-quoted-client-etag.bin", totalSize)
	if err != nil {
		t.Fatal(err)
	}

	partNums := make([]int, plan.TotalParts)
	for i := range partNums {
		partNums[i] = i + 1
	}
	urls, err := b.PresignParts(ctx, plan.UploadID, entriesFromInts(partNums))
	if err != nil {
		t.Fatal(err)
	}

	upload, _ := b.GetUpload(ctx, plan.UploadID)
	partData := make([]byte, totalSize)
	completeParts := make([]CompletePart, len(urls))
	for i, u := range urls {
		start := int64(u.Number-1) * upload.PartSize
		end := start + u.Size
		if end > totalSize {
			end = totalSize
		}
		etag, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, u.Number, bytes.NewReader(partData[start:end]))
		if err != nil {
			t.Fatalf("upload part %d: %v", u.Number, err)
		}
		completeParts[i] = CompletePart{Number: u.Number, ETag: "\"" + etag + "\""}
	}

	if err := b.ConfirmUploadV2(ctx, plan.UploadID, completeParts); err != nil {
		t.Fatalf("ConfirmUploadV2 with quoted client ETags: %v", err)
	}
}

func TestNormalizeETag(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "bare", in: "abc", want: "abc"},
		{name: "quoted", in: "\"abc\"", want: "abc"},
		{name: "double quoted empty", in: "\"\"", want: ""},
	}
	for _, tt := range tests {
		if got := normalizeETag(tt.in); got != tt.want {
			t.Fatalf("%s: normalizeETag(%q) = %q, want %q", tt.name, tt.in, got, tt.want)
		}
	}
}

func TestV2CompletePartCountMismatch(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	plan, err := b.InitiateUploadV2(ctx, "/v2-partcount.bin", 20<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Transition to UPLOADING
	if _, err := b.PresignPart(ctx, plan.UploadID, 1, nil); err != nil {
		t.Fatal(err)
	}

	// Try to complete with wrong number of parts
	err = b.ConfirmUploadV2(ctx, plan.UploadID, []CompletePart{{Number: 1, ETag: "x"}})
	if err == nil {
		t.Fatal("expected error for part count mismatch")
	}
	if !strings.Contains(err.Error(), "part count mismatch") {
		t.Errorf("expected part count mismatch error, got: %v", err)
	}
}
