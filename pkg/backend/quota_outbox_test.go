package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

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

	if got := countQuotaOutboxRowsByFile(t, b, fileID, datastore.QuotaOutboxQueued); got != 1 {
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
	if got := countQuotaOutboxRowsByFile(t, b, fileID, datastore.QuotaOutboxSucceeded); got != 1 {
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
	if got := countQuotaOutboxRowsByFile(t, b, first.File.FileID, datastore.QuotaOutboxQueued); got != 1 {
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

func countQuotaOutboxRowsByFile(t *testing.T, b *Dat9Backend, fileID string, status datastore.QuotaOutboxStatus) int {
	t.Helper()
	var count int
	if err := b.Store().DB().QueryRow(`SELECT COUNT(*) FROM quota_outbox WHERE file_id = ? AND status = ?`, fileID, status).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
