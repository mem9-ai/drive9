package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/meta"
)

func newServerQuotaBackend(t *testing.T, opts Options) (*Dat9Backend, *fakeMetaQuotaStore) {
	t.Helper()
	opts.QuotaSource = QuotaSourceServer
	b := newTestBackendWithOptions(t, opts)
	fake := newFakeMetaQuotaStore()
	fake.config["tenant-a"] = &QuotaConfigView{
		TenantID:         "tenant-a",
		MaxStorageBytes:  1 << 30,
		MaxFileSizeBytes: meta.DefaultMaxFileSizeBytes(),
		MaxFileCount:     0,
		MaxMediaLLMFiles: 1000,
		MaxMonthlyCostMC: 1 << 30,
	}
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)
	return b, fake
}

func waitForFakeCentralLLMUsage(t *testing.T, fake *fakeMetaQuotaStore, tenantID string, wantMonthly int64, wantUsageLen int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		gotMonthly := fake.monthly[tenantID]
		gotUsageLen := len(fake.llmUsage)
		fake.mu.Unlock()
		if gotMonthly == wantMonthly && gotUsageLen == wantUsageLen {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("central llm usage monthly=%d len=%d, want monthly=%d len=%d",
				gotMonthly, gotUsageLen, wantMonthly, wantUsageLen)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServerQuotaFeatureFlagRejectsOverLimitWrite(t *testing.T) {
	opts := Options{}
	opts.QuotaSource = QuotaSourceServer
	b := newTestBackendWithOptions(t, opts)
	fake := newFakeMetaQuotaStore()
	fake.config["tenant-a"] = &QuotaConfigView{
		TenantID:         "tenant-a",
		MaxStorageBytes:  10,
		MaxMediaLLMFiles: 1000,
		MaxMonthlyCostMC: 1 << 30,
	}
	// Pre-seed usage near the limit so the cache snapshot (loaded
	// synchronously in SetMetaQuotaStore) already reflects near-full state.
	// This avoids depending on async mutation timing from a prior write.
	fake.usage["tenant-a"] = &QuotaUsageView{
		TenantID:     "tenant-a",
		StorageBytes: 8,
	}
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)

	if _, err := b.Write("/beta.txt", []byte("xyz"), 0, filesystem.WriteFlagCreate); !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected ErrStorageQuotaExceeded from server quota path, got %v", err)
	}

	// Verify usage unchanged after rejected write.
	usage, err := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("get central usage: %v", err)
	}
	if usage.StorageBytes != 8 || usage.ReservedBytes != 0 {
		t.Fatalf("usage after rejected write = %+v", usage)
	}
}

func TestCreateIfAbsentExistingPathReturnsConflictBeforeStorageQuota(t *testing.T) {
	opts := Options{}
	opts.QuotaSource = QuotaSourceServer
	b := newTestBackendWithOptions(t, opts)
	fake := newFakeMetaQuotaStore()
	fake.config["tenant-a"] = &QuotaConfigView{
		TenantID:         "tenant-a",
		MaxStorageBytes:  10,
		MaxFileSizeBytes: meta.DefaultMaxFileSizeBytes(),
		MaxMediaLLMFiles: 1000,
		MaxMonthlyCostMC: 1 << 30,
	}
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)

	if _, err := b.WriteCtxIfRevision(context.Background(), "/exists.txt", []byte("ok"), 0, filesystem.WriteFlagCreate, 0); err != nil {
		t.Fatalf("seed create-if-absent: %v", err)
	}
	fake.mu.Lock()
	fake.usage["tenant-a"] = &QuotaUsageView{
		TenantID:     "tenant-a",
		StorageBytes: 10,
	}
	fake.mu.Unlock()
	b.quotaUsageCache.invalidate()

	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(context.Background(), "/exists.txt", []byte("too-large"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, 0, nil, ""); !errors.Is(err, datastore.ErrRevisionConflict) {
		t.Fatalf("duplicate create-if-absent error = %v, want ErrRevisionConflict", err)
	}
}

func TestServerQuotaRejectsOverFileSizeLimit(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxFileSizeBytes = 4
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	if _, err := b.WriteCtx(ctx, "/too-large.txt", []byte("12345"), 0, filesystem.WriteFlagCreate); !errors.Is(err, ErrFileSizeQuotaExceeded) {
		t.Fatalf("write error = %v, want ErrFileSizeQuotaExceeded", err)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 0 || usage.FileCount != 0 {
		t.Fatalf("usage after rejected write = %+v, want zero", usage)
	}
}

func TestCreateIfAbsentExistingPathReturnsConflictBeforeFileSizeQuota(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	ctx := context.Background()

	if _, err := b.WriteCtxIfRevision(ctx, "/size-exists.txt", []byte("ok"), 0, filesystem.WriteFlagCreate, 0); err != nil {
		t.Fatalf("seed create-if-absent: %v", err)
	}
	fake.mu.Lock()
	fake.config["tenant-a"].MaxFileSizeBytes = 4
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/size-exists.txt", []byte("12345"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, 0, nil, ""); !errors.Is(err, datastore.ErrRevisionConflict) {
		t.Fatalf("duplicate create-if-absent error = %v, want ErrRevisionConflict", err)
	}
}

func TestCreateBatchConcurrentCreatesCommit(t *testing.T) {
	b, _ := newServerQuotaBackend(t, Options{
		CreateBatch: CreateBatchOptions{
			MaxEntries: 4,
			Linger:     50 * time.Millisecond,
		},
	})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	const files = 12
	start := make(chan struct{})
	errs := make(chan error, files)
	for i := 0; i < files; i++ {
		i := i
		go func() {
			<-start
			path := fmt.Sprintf("/batch/file-%02d.txt", i)
			data := []byte(fmt.Sprintf("payload-%02d", i))
			n, rev, err := b.WriteCtxIfRevisionWithTagsResult(ctx, path, data, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, 0, nil, "")
			if err != nil {
				errs <- err
				return
			}
			if n != int64(len(data)) || rev != 1 {
				errs <- fmt.Errorf("%s write n=%d rev=%d, want n=%d rev=1", path, n, rev, len(data))
				return
			}
			errs <- nil
		}()
	}
	close(start)
	for i := 0; i < files; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("batched create %d failed: %v", i, err)
		}
	}
	for i := 0; i < files; i++ {
		path := fmt.Sprintf("/batch/file-%02d.txt", i)
		info, err := b.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Size != int64(len(fmt.Sprintf("payload-%02d", i))) {
			t.Fatalf("stat %s size = %d", path, info.Size)
		}
	}
	var outboxRows int
	if err := b.store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox WHERE mutation_type = 'file_create'`).Scan(&outboxRows); err != nil {
		t.Fatalf("count quota outbox: %v", err)
	}
	if outboxRows != files {
		t.Fatalf("quota outbox rows = %d, want %d", outboxRows, files)
	}
}

func TestCreateBatchDuplicatePathDoesNotAbortBatch(t *testing.T) {
	b, _ := newServerQuotaBackend(t, Options{
		CreateBatch: CreateBatchOptions{
			MaxEntries: 3,
			Linger:     50 * time.Millisecond,
		},
	})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	start := make(chan struct{})
	type result struct {
		path string
		err  error
	}
	results := make(chan result, 3)
	paths := []string{"/batch-dupe/same.txt", "/batch-dupe/same.txt", "/batch-dupe/other.txt"}
	for _, path := range paths {
		path := path
		go func() {
			<-start
			_, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, path, []byte(path), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, 0, nil, "")
			results <- result{path: path, err: err}
		}()
	}
	close(start)

	successSame := 0
	conflictSame := 0
	otherOK := false
	for i := 0; i < len(paths); i++ {
		res := <-results
		switch res.path {
		case "/batch-dupe/same.txt":
			if res.err == nil {
				successSame++
			} else if errors.Is(res.err, datastore.ErrRevisionConflict) {
				conflictSame++
			} else {
				t.Fatalf("same path error = %v, want nil or ErrRevisionConflict", res.err)
			}
		case "/batch-dupe/other.txt":
			if res.err != nil {
				t.Fatalf("other path error = %v, want nil", res.err)
			}
			otherOK = true
		}
	}
	if successSame != 1 || conflictSame != 1 || !otherOK {
		t.Fatalf("successSame=%d conflictSame=%d otherOK=%v, want 1/1/true", successSame, conflictSame, otherOK)
	}
	if _, err := b.Stat("/batch-dupe/other.txt"); err != nil {
		t.Fatalf("other path should commit despite duplicate in same batch: %v", err)
	}
}

func TestCreateBatchAppliesCumulativeStorageQuota(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{
		CreateBatch: CreateBatchOptions{
			MaxEntries: 3,
			Linger:     50 * time.Millisecond,
		},
	})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()
	fake.mu.Lock()
	fake.config["tenant-a"].MaxStorageBytes = 8
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	start := make(chan struct{})
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		i := i
		go func() {
			<-start
			_, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, fmt.Sprintf("/batch-quota/file-%d.txt", i), []byte("1234"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, 0, nil, "")
			errs <- err
		}()
	}
	close(start)

	okCount := 0
	exceededCount := 0
	for i := 0; i < 3; i++ {
		err := <-errs
		if err == nil {
			okCount++
		} else if errors.Is(err, ErrStorageQuotaExceeded) {
			exceededCount++
		} else {
			t.Fatalf("create error = %v, want nil or ErrStorageQuotaExceeded", err)
		}
	}
	if okCount != 2 || exceededCount != 1 {
		t.Fatalf("ok=%d exceeded=%d, want 2/1", okCount, exceededCount)
	}
	var outboxRows int
	if err := b.store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox WHERE mutation_type = 'file_create'`).Scan(&outboxRows); err != nil {
		t.Fatalf("count quota outbox: %v", err)
	}
	if outboxRows != 2 {
		t.Fatalf("quota outbox rows = %d, want 2", outboxRows)
	}
}

func TestServerQuotaPendingOutboxFileDeltaRejectsOverFileCount(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxFileCount = 1
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	if _, err := b.WriteCtx(ctx, "/first.txt", []byte("a"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("first write: %v", err)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if usage.FileCount != 0 {
		t.Fatalf("central file count before outbox drain = %d, want 0", usage.FileCount)
	}
	if _, err := b.WriteCtx(ctx, "/second.txt", []byte("b"), 0, filesystem.WriteFlagCreate); !errors.Is(err, ErrFileCountQuotaExceeded) {
		t.Fatalf("second write error = %v, want ErrFileCountQuotaExceeded", err)
	}
}

func TestServerModeBudgetGateWritesCentralOnly(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{
		LLMCostBudget: LLMCostBudgetOptions{
			MaxMonthlyMillicents:          100,
			VisionCostPerKTokenMillicents: 1000,
		},
	})

	// Override per-tenant config to match the test's budget limit; otherwise the
	// server-side check uses the shared default (1 << 30) from newServerQuotaBackend.
	fake.mu.Lock()
	fake.config["tenant-a"].MaxMonthlyCostMC = 100
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(context.Background())

	b.recordImageExtractUsage("task-server-budget", ImageExtractUsage{
		PromptTokens:     120,
		CompletionTokens: 80,
	})

	total, err := b.store.MonthlyLLMCostMillicents()
	if err != nil {
		t.Fatalf("tenant monthly llm cost: %v", err)
	}
	if total != 0 {
		t.Fatalf("tenant monthly llm cost = %d, want 0 in server mode", total)
	}

	waitForFakeCentralLLMUsage(t, fake, "tenant-a", 200, 1)
	if !b.monthlyLLMCostExceededCheck(context.Background()) {
		t.Fatal("expected central monthly budget check to trip in server mode")
	}
}

func TestServerQuotaUploadOverwriteDeltaIntegration(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	ctx := context.Background()

	oldData := []byte("old-wav")
	if _, err := b.Write("/clip.wav", oldData, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("seed original file: %v", err)
	}

	totalSize := int64(32)
	plan, err := b.InitiateUpload(ctx, "/clip.wav", totalSize)
	if err != nil {
		t.Fatalf("initiate overwrite upload: %v", err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatalf("confirm overwrite upload: %v", err)
	}
	b.processQuotaOutboxAvailable(ctx)

	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get central usage: %v", err)
	}
	if usage.StorageBytes != totalSize || usage.ReservedBytes != 0 || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after overwrite upload = %+v", usage)
	}

	nf, err := b.Store().Stat(ctx, "/clip.wav")
	if err != nil {
		t.Fatalf("stat overwritten file: %v", err)
	}
	fm, err := fake.GetFileMeta(ctx, "tenant-a", nf.File.FileID)
	if err != nil {
		t.Fatalf("get central file meta: %v", err)
	}
	if fm.SizeBytes != totalSize || !fm.IsMedia {
		t.Fatalf("file meta after overwrite upload = %+v", fm)
	}

	reservation, err := fake.GetUploadReservation(ctx, "tenant-a", plan.UploadID)
	if err != nil {
		t.Fatalf("get upload reservation: %v", err)
	}
	if reservation.Status != "completed" {
		t.Fatalf("reservation status = %q, want completed", reservation.Status)
	}
}

func TestConcurrentServerQuotaReservationsRejectOverLimit(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxStorageBytes = 30 << 20
	fake.mu.Unlock()

	type result struct {
		err error
	}
	results := make(chan result, 2)
	paths := []string{"/concurrent-a.bin", "/concurrent-b.bin"}

	var wg sync.WaitGroup
	for _, path := range paths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			_, err := b.InitiateUploadV2(ctx, path, 20<<20)
			results <- result{err: err}
		}(path)
	}
	wg.Wait()
	close(results)

	var successCount int
	var quotaErrCount int
	for res := range results {
		switch {
		case res.err == nil:
			successCount++
		case errors.Is(res.err, ErrStorageQuotaExceeded):
			quotaErrCount++
		default:
			t.Fatalf("unexpected initiate error: %v", res.err)
		}
	}
	if successCount != 1 || quotaErrCount != 1 {
		t.Fatalf("success=%d quota_err=%d, want 1/1", successCount, quotaErrCount)
	}

	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get central usage: %v", err)
	}
	if usage.ReservedBytes != 20<<20 {
		t.Fatalf("reserved bytes = %d, want %d", usage.ReservedBytes, 20<<20)
	}

	fake.mu.Lock()
	activeReservations := 0
	for _, r := range fake.reservations {
		if r.Status == "active" {
			activeReservations++
		}
	}
	fake.mu.Unlock()
	if activeReservations != 1 {
		t.Fatalf("active reservations = %d, want 1", activeReservations)
	}
}

func TestMutationReplayWorkerAppliesPendingUploadComplete(t *testing.T) {
	fake := newFakeMetaQuotaStore()

	fake.mu.Lock()
	fake.usage["tenant-a"] = &QuotaUsageView{
		TenantID:       "tenant-a",
		StorageBytes:   7,
		ReservedBytes:  32,
		FileCount:      1,
		MediaFileCount: 1,
	}
	fake.fileMeta[metaKey("tenant-a", "file-1")] = &FileMetaView{
		TenantID:  "tenant-a",
		FileID:    "file-1",
		SizeBytes: 7,
		IsMedia:   true,
	}
	fake.reservations[metaKey("tenant-a", "upload-1")] = &UploadReservationView{
		TenantID:      "tenant-a",
		UploadID:      "upload-1",
		ReservedBytes: 32,
		Status:        "active",
		ExpiresAt:     time.Now().Add(time.Hour),
	}
	data, err := json.Marshal(uploadCompleteMutationData{
		UploadID:      "upload-1",
		FileID:        "file-1",
		ReservedBytes: 32,
		OldSizeBytes:  7,
		OldIsMedia:    true,
		NewSizeBytes:  32,
		NewIsMedia:    true,
	})
	if err != nil {
		fake.mu.Unlock()
		t.Fatalf("marshal pending upload_complete mutation: %v", err)
	}
	fake.mutations = append(fake.mutations, fakeMutationRecord{
		tenantID: "tenant-a",
		id:       1,
		typ:      "upload_complete",
		status:   "pending",
		data:     data,
	})
	fake.nextID = 2
	fake.mu.Unlock()

	w := &MutationReplayWorker{store: fake}
	w.replayBatch(context.Background())

	usage, err := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("get central usage: %v", err)
	}
	if usage.StorageBytes != 32 || usage.ReservedBytes != 0 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after replay = %+v", usage)
	}
	reservation, err := fake.GetUploadReservation(context.Background(), "tenant-a", "upload-1")
	if err != nil {
		t.Fatalf("get reservation after replay: %v", err)
	}
	if reservation.Status != "completed" {
		t.Fatalf("reservation status after replay = %q, want completed", reservation.Status)
	}
	fm, err := fake.GetFileMeta(context.Background(), "tenant-a", "file-1")
	if err != nil {
		t.Fatalf("get file meta after replay: %v", err)
	}
	if fm.SizeBytes != 32 || !fm.IsMedia {
		t.Fatalf("file meta after replay = %+v", fm)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.mutations) != 1 || fake.mutations[0].status != "applied" {
		t.Fatalf("mutations after replay = %+v", fake.mutations)
	}
}

func TestExpirySweepWorkerReleasesExpiredReservation(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.mu.Lock()
	fake.usage["tenant-a"] = &QuotaUsageView{
		TenantID:      "tenant-a",
		ReservedBytes: 20,
	}
	fake.reservations[metaKey("tenant-a", "upload-expired")] = &UploadReservationView{
		TenantID:      "tenant-a",
		UploadID:      "upload-expired",
		ReservedBytes: 20,
		Status:        "active",
		ExpiresAt:     time.Now().Add(-time.Minute),
	}
	fake.mu.Unlock()

	w := &ExpirySweepWorker{store: fake}
	w.sweep(context.Background())

	usage, err := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("get central usage: %v", err)
	}
	if usage.ReservedBytes != 0 {
		t.Fatalf("reserved bytes after expiry sweep = %d, want 0", usage.ReservedBytes)
	}
	reservation, err := fake.GetUploadReservation(context.Background(), "tenant-a", "upload-expired")
	if err != nil {
		t.Fatalf("get reservation after expiry sweep: %v", err)
	}
	if reservation.Status != "aborted" {
		t.Fatalf("reservation status after expiry sweep = %q, want aborted", reservation.Status)
	}
}
