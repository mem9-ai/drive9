package backend

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

func newServerQuotaBackend(t *testing.T, opts Options) (*Dat9Backend, *fakeMetaQuotaStore) {
	t.Helper()
	opts.QuotaSource = QuotaSourceServer
	b := newTestBackendWithOptions(t, opts)
	fake := newFakeMetaQuotaStore()
	fake.config["tenant-a"] = &QuotaConfigView{
		TenantID:         "tenant-a",
		MaxStorageBytes:  1 << 30,
		MaxMediaLLMFiles: 1000,
		MaxMonthlyCostMC: 1 << 30,
	}
	b.SetMetaQuotaStore("tenant-a", fake)
	return b, fake
}

func TestServerQuotaFeatureFlagRejectsOverLimitWrite(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxStorageBytes = 10
	fake.mu.Unlock()

	if _, err := b.Write("/alpha.txt", []byte("12345678"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create within central quota: %v", err)
	}
	if _, err := b.Write("/beta.txt", []byte("xyz"), 0, filesystem.WriteFlagCreate); !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected ErrStorageQuotaExceeded from server quota path, got %v", err)
	}

	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get central usage: %v", err)
	}
	if usage.StorageBytes != 8 || usage.ReservedBytes != 0 {
		t.Fatalf("usage after rejected write = %+v", usage)
	}
}

func TestServerModeBudgetGateWritesCentralOnly(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{
		LLMCostBudget: LLMCostBudgetOptions{
			MaxMonthlyMillicents:          100,
			VisionCostPerKTokenMillicents: 1000,
		},
	})

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

	fake.mu.Lock()
	gotMonthly := fake.monthly["tenant-a"]
	gotUsageLen := len(fake.llmUsage)
	fake.mu.Unlock()
	if gotMonthly != 200 {
		t.Fatalf("central monthly llm cost = %d, want 200", gotMonthly)
	}
	if gotUsageLen != 1 {
		t.Fatalf("central llm usage len = %d, want 1", gotUsageLen)
	}
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

	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get central usage: %v", err)
	}
	if usage.StorageBytes != totalSize || usage.ReservedBytes != 0 || usage.MediaFileCount != 1 {
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
