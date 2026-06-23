package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

func TestServerQuotaSmallWriteUsesTenantOutbox(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	if _, err := b.WriteCtx(ctx, "/outbox.png", []byte("png-data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("write: %v", err)
	}
	nf, err := b.Store().Stat(ctx, "/outbox.png")
	if err != nil {
		t.Fatalf("stat written file: %v", err)
	}
	fileID := nf.File.FileID

	fake.mu.Lock()
	mutationCount := len(fake.mutations)
	fake.mu.Unlock()
	if mutationCount != 0 {
		t.Fatalf("central mutation log writes = %d, want 0", mutationCount)
	}

	if got := countQuotaOutboxRowsByFile(t, ctx, b, fileID, datastore.QuotaOutboxQueued); got != 1 {
		t.Fatalf("queued quota outbox rows = %d, want 1", got)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.StorageBytes != 0 || usage.MediaFileCount != 0 {
		t.Fatalf("central usage before outbox drain = %+v, want zero", usage)
	}

	processed, err := b.ProcessOneQuotaOutbox(ctx)
	if err != nil {
		t.Fatalf("process quota outbox: %v", err)
	}
	if !processed {
		t.Fatal("expected one quota outbox row to be processed")
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, fileID, datastore.QuotaOutboxSucceeded); got != 1 {
		t.Fatalf("succeeded quota outbox rows = %d, want 1", got)
	}
	usage, err = fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage after drain: %v", err)
	}
	if usage.StorageBytes != int64(len("png-data")) || usage.MediaFileCount != 1 {
		t.Fatalf("central usage after outbox drain = %+v", usage)
	}
}

func TestQuotaOutboxWorkerHoldsAdmissionLockAcrossApplyAndAck(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	if _, err := b.WriteCtx(ctx, "/locked-outbox.txt", []byte("payload"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("write: %v", err)
	}

	lockAttempted := make(chan struct{})
	lockResult := make(chan error, 1)
	fake.inTxHook = func(hookCtx context.Context) error {
		go func() {
			close(lockAttempted)
			lockResult <- b.store.InTx(hookCtx, func(tx *sql.Tx) error {
				return b.store.LockQuotaAdmissionTx(tx)
			})
		}()
		<-lockAttempted
		select {
		case err := <-lockResult:
			if err != nil {
				return err
			}
			return errors.New("quota admission lock was available while outbox apply was in progress")
		case <-time.After(150 * time.Millisecond):
			return nil
		}
	}

	processed, err := b.ProcessOneQuotaOutbox(ctx)
	if err != nil {
		t.Fatalf("process quota outbox: %v", err)
	}
	if !processed {
		t.Fatal("expected one quota outbox row to be processed")
	}
	select {
	case err := <-lockResult:
		if err != nil {
			t.Fatalf("lock waiter: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("quota admission lock waiter did not finish after outbox processing")
	}
}

func TestQuotaOutboxWorkerCommitsRetryAfterApplyError(t *testing.T) {
	b, _ := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	id, err := b.store.EnqueueQuotaOutboxTx(b.store.DB(), &datastore.QuotaOutboxEntry{
		FileID:       "bad-json",
		MutationType: quotaMutationTypeFileCreate,
		MutationData: json.RawMessage(`{"file_id":"bad-json","size_bytes":"bad","is_media":false}`),
		StorageDelta: 1,
	})
	if err != nil {
		t.Fatalf("enqueue quota outbox: %v", err)
	}

	processed, err := b.ProcessOneQuotaOutbox(ctx)
	if err == nil {
		t.Fatal("process quota outbox error = nil, want apply error")
	}
	if !processed {
		t.Fatal("expected one quota outbox row to be processed")
	}

	var status datastore.QuotaOutboxStatus
	var attemptCount int
	var lastError sql.NullString
	if err := b.store.DB().QueryRowContext(ctx, `SELECT status, attempt_count, last_error FROM quota_outbox WHERE id = ?`, id).
		Scan(&status, &attemptCount, &lastError); err != nil {
		t.Fatalf("load quota outbox row: %v", err)
	}
	if status != datastore.QuotaOutboxQueued {
		t.Fatalf("status = %q, want queued retry", status)
	}
	if attemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", attemptCount)
	}
	if !lastError.Valid || lastError.String == "" {
		t.Fatalf("last_error = %q valid=%v, want recorded apply error", lastError.String, lastError.Valid)
	}
}

func TestServerQuotaPendingOutboxDeltaRejectsOverLimitWrite(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxStorageBytes = 10
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	if _, err := b.WriteCtx(ctx, "/first.txt", []byte("12345678"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("first write: %v", err)
	}
	first, err := b.Store().Stat(ctx, "/first.txt")
	if err != nil {
		t.Fatalf("stat first file: %v", err)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.StorageBytes != 0 {
		t.Fatalf("central storage before outbox drain = %d, want 0", usage.StorageBytes)
	}

	_, err = b.WriteCtx(ctx, "/second.txt", []byte("abc"), 0, filesystem.WriteFlagCreate)
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("second write error = %v, want ErrStorageQuotaExceeded", err)
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, first.File.FileID, datastore.QuotaOutboxQueued); got != 1 {
		t.Fatalf("queued quota outbox rows after rejected write = %d, want 1", got)
	}
}

func TestServerQuotaPendingOutboxDeltaRejectsUploadReserve(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxStorageBytes = 10
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	if _, err := b.WriteCtx(ctx, "/first.txt", []byte("12345678"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("first write: %v", err)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.StorageBytes != 0 || usage.ReservedBytes != 0 {
		t.Fatalf("central usage before outbox drain = %+v, want zero", usage)
	}

	reserved, err := b.reserveUploadOnServer(ctx, "upload-pending", "/upload.bin", 3)
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("reserve error = %v, want ErrStorageQuotaExceeded", err)
	}
	if reserved {
		t.Fatal("reserve should be false when pending outbox pushes tenant over limit")
	}
	usage, err = fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage after rejected reserve: %v", err)
	}
	if usage.ReservedBytes != 0 {
		t.Fatalf("reserved bytes after rejected reserve = %d, want 0", usage.ReservedBytes)
	}
}

func TestServerQuotaReserveUploadRetrySkipsPendingPrecheck(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxStorageBytes = 8
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	reserved, err := b.reserveUploadOnServer(ctx, "upload-retry", "/upload.bin", 8)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if !reserved {
		t.Fatal("first reserve returned false, want true")
	}
	reserved, err = b.reserveUploadOnServer(ctx, "upload-retry", "/upload.bin", 8)
	if err != nil {
		t.Fatalf("retry reserve: %v", err)
	}
	if !reserved {
		t.Fatal("retry reserve returned false, want true")
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.ReservedBytes != 8 {
		t.Fatalf("reserved bytes = %d, want 8", usage.ReservedBytes)
	}
}

func TestServerQuotaOverwriteOutboxUsesLockedCurrentMeta(t *testing.T) {
	b, _ := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	if _, err := b.WriteCtx(ctx, "/stale.txt", []byte("1234567890"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	stale, err := b.Store().Stat(ctx, "/stale.txt")
	if err != nil {
		t.Fatalf("stat stale file: %v", err)
	}
	if _, err := b.WriteCtx(ctx, "/stale.txt", []byte("12345678901234567890"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("fresh overwrite: %v", err)
	}
	if _, _, err := b.overwriteFileCtxWithRev(ctx, stale, []byte("12345"), 0, filesystem.WriteFlagTruncate, 0, nil, ""); err != nil {
		t.Fatalf("stale overwrite: %v", err)
	}

	delta := latestQuotaOutboxStorageDeltaByFile(t, ctx, b, stale.File.FileID)
	if delta != -15 {
		t.Fatalf("latest outbox storage_delta = %d, want -15", delta)
	}
}

func TestServerQuotaMediaCheckIncludesCurrentWrite(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "transcript"},
		},
	})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxMediaLLMFiles = 1
	fake.usage["tenant-a"] = &QuotaUsageView{
		TenantID:       "tenant-a",
		MediaFileCount: 1,
	}
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	if _, err := b.WriteCtx(ctx, "/over-limit.mp3", []byte("audio-data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("write: %v", err)
	}
	nf, err := b.Store().Stat(ctx, "/over-limit.mp3")
	if err != nil {
		t.Fatalf("stat written file: %v", err)
	}
	tasks := loadSemanticTasksForFile(t, b, nf.File.FileID)
	if len(tasks) != 0 {
		t.Fatalf("semantic task count = %d, want 0", len(tasks))
	}
}

func TestApplyCentralFileMutationIsIdempotent(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	if err := fake.EnsureQuotaUsageRow(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("ensure usage: %v", err)
	}
	raw, err := json.Marshal(fileCreateMutationData{
		FileID:    "file-1",
		SizeBytes: 9,
		IsMedia:   true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := fake.InTx(context.Background(), func(tx *sql.Tx) error {
			return applyCentralQuotaMutationTx(fake, tx, "tenant-a", quotaMutationTypeFileCreate, raw, int64(i+1))
		}); err != nil {
			t.Fatalf("apply central mutation %d: %v", i+1, err)
		}
	}
	usage, err := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.StorageBytes != 9 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after duplicate apply = %+v, want storage=9 media=1", usage)
	}
}

func countQuotaOutboxRowsByFile(t *testing.T, ctx context.Context, b *Dat9Backend, fileID string, status datastore.QuotaOutboxStatus) int {
	t.Helper()
	var count int
	if err := b.Store().DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox WHERE file_id = ? AND status = ?`, fileID, status).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func latestQuotaOutboxStorageDeltaByFile(t *testing.T, ctx context.Context, b *Dat9Backend, fileID string) int64 {
	t.Helper()
	var delta int64
	if err := b.Store().DB().QueryRowContext(ctx, `SELECT storage_delta FROM quota_outbox
		WHERE file_id = ? AND mutation_type = ?
		ORDER BY id DESC LIMIT 1`, fileID, quotaMutationTypeOverwrite).Scan(&delta); err != nil {
		t.Fatal(err)
	}
	return delta
}
