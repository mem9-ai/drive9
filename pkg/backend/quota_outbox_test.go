package backend

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/s3client"
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
	if usage.StorageBytes != 0 || usage.FileCount != 0 || usage.MediaFileCount != 0 {
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
	if usage.StorageBytes != int64(len("png-data")) || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("central usage after outbox drain = %+v", usage)
	}
}

func TestServerQuotaOutboxBatchProcessesDifferentFiles(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	if _, err := b.WriteCtx(ctx, "/batch-a.txt", []byte("aaaa"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("write a: %v", err)
	}
	a, err := b.Store().Stat(ctx, "/batch-a.txt")
	if err != nil {
		t.Fatalf("stat a: %v", err)
	}
	if _, err := b.WriteCtx(ctx, "/batch-b.png", []byte("bb"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("write b: %v", err)
	}
	bb, err := b.Store().Stat(ctx, "/batch-b.png")
	if err != nil {
		t.Fatalf("stat b: %v", err)
	}

	processed, err := b.ProcessQuotaOutboxBatch(ctx, 100)
	if err != nil {
		t.Fatalf("process quota outbox batch: %v", err)
	}
	if processed != 2 {
		t.Fatalf("processed rows = %d, want 2", processed)
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, a.File.FileID, datastore.QuotaOutboxSucceeded); got != 1 {
		t.Fatalf("a succeeded rows = %d, want 1", got)
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, bb.File.FileID, datastore.QuotaOutboxSucceeded); got != 1 {
		t.Fatalf("b succeeded rows = %d, want 1", got)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.StorageBytes != int64(len("aaaa")+len("bb")) || usage.FileCount != 2 || usage.MediaFileCount != 1 {
		t.Fatalf("central usage after batch = %+v", usage)
	}
}

func TestQuotaOutboxWorkerRepollsAfterUnderfilledSameFileBatch(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()
	fileID := "same-file-backlog"

	if _, err := b.store.EnqueueQuotaOutboxTx(b.store.DB(), &datastore.QuotaOutboxEntry{
		FileID:       fileID,
		MutationType: quotaMutationTypeFileCreate,
		MutationData: mustQuotaMutationJSON(t, fileCreateMutationData{
			FileID:    fileID,
			SizeBytes: 1,
		}),
		StorageDelta: 1,
		FileDelta:    1,
	}); err != nil {
		t.Fatalf("enqueue create: %v", err)
	}
	if _, err := b.store.EnqueueQuotaOutboxTx(b.store.DB(), &datastore.QuotaOutboxEntry{
		FileID:       fileID,
		MutationType: quotaMutationTypeOverwrite,
		MutationData: mustQuotaMutationJSON(t, fileOverwriteMutationData{
			FileID:       fileID,
			OldSizeBytes: 1,
			NewSizeBytes: 3,
		}),
		StorageDelta: 2,
	}); err != nil {
		t.Fatalf("enqueue overwrite: %v", err)
	}

	b.processQuotaOutboxAvailable(ctx)
	if got := countQuotaOutboxRowsByFile(t, ctx, b, fileID, datastore.QuotaOutboxSucceeded); got != 2 {
		t.Fatalf("succeeded same-file rows = %d, want 2", got)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.StorageBytes != 3 || usage.FileCount != 1 {
		t.Fatalf("usage after same-file backlog drain = %+v, want storage=3 files=1", usage)
	}
}

func TestServerQuotaSmallWriteDoesNotWaitForAdmissionLock(t *testing.T) {
	b, _ := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	lockTx, err := b.store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if err := b.store.LockQuotaAdmissionTx(lockTx); err != nil {
		_ = lockTx.Rollback()
		t.Fatalf("lock quota admission: %v", err)
	}
	t.Cleanup(func() { _ = lockTx.Rollback() })

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := b.WriteCtx(writeCtx, "/lock-free-small.txt", []byte("payload"), 0, filesystem.WriteFlagCreate)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("small write while admission lock is held: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("small write waited on quota admission lock")
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

func TestQuotaOutboxNotifyQuietHasMaxWait(t *testing.T) {
	b := &Dat9Backend{quotaOutboxNotify: make(chan struct{}, quotaOutboxNotifySize)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				select {
				case b.quotaOutboxNotify <- struct{}{}:
				default:
				}
			}
		}
	}()

	start := time.Now()
	if !b.waitQuotaOutboxNotifyQuiet(ctx) {
		t.Fatal("waitQuotaOutboxNotifyQuiet returned false")
	}
	if dur := time.Since(start); dur > 500*time.Millisecond {
		t.Fatalf("waitQuotaOutboxNotifyQuiet duration = %s, want bounded by max delay", dur)
	}
}

func TestQuotaOutboxNotifyQuietReturnsAfterQuietPeriod(t *testing.T) {
	b := &Dat9Backend{quotaOutboxNotify: make(chan struct{}, quotaOutboxNotifySize)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	if !b.waitQuotaOutboxNotifyQuiet(ctx) {
		t.Fatal("waitQuotaOutboxNotifyQuiet returned false")
	}
	dur := time.Since(start)
	if dur < quotaOutboxNotifyDelay/2 {
		t.Fatalf("waitQuotaOutboxNotifyQuiet duration = %s, want quiet-period wait", dur)
	}
	if dur > quotaOutboxNotifyMaxDelay+100*time.Millisecond {
		t.Fatalf("waitQuotaOutboxNotifyQuiet duration = %s, want before max delay", dur)
	}
}

func TestDrainQuotaOutboxForFileErrorsWhenPendingRowIsNotClaimable(t *testing.T) {
	b, _ := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	if _, err := b.WriteCtx(ctx, "/leased.txt", []byte("payload"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("write: %v", err)
	}
	nf, err := b.Store().Stat(ctx, "/leased.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	claimed, found, err := b.store.ClaimQuotaOutbox(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("claim quota outbox: %v", err)
	}
	if !found || claimed.FileID != nf.File.FileID {
		t.Fatalf("claimed = %+v found=%t, want leased row for target", claimed, found)
	}

	drainCtx, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
	defer cancel()
	err = b.drainQuotaOutboxForFile(drainCtx, nf.File.FileID, quotaOutboxUploadDrainLimit)
	if err == nil {
		t.Fatal("drain error = nil, want pending-not-claimable error")
	}
}

func TestUploadOverwriteQueuesCompleteBehindPendingFileMutation(t *testing.T) {
	b := newTestBackendWithS3AndOptions(t, Options{QuotaSource: QuotaSourceServer})
	fake := newFakeMetaQuotaStore()
	fake.config["tenant-a"] = &QuotaConfigView{
		TenantID:         "tenant-a",
		MaxStorageBytes:  1 << 30,
		MaxFileSizeBytes: meta.DefaultMaxFileSizeBytes(),
		MaxMediaLLMFiles: 1000,
		MaxMonthlyCostMC: 1 << 30,
	}
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	if _, err := b.WriteCtx(ctx, "/target.bin", []byte("old"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("write target: %v", err)
	}
	target, err := b.Store().Stat(ctx, "/target.bin")
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, target.File.FileID, datastore.QuotaOutboxQueued); got != 1 {
		t.Fatalf("queued rows before upload = %d, want 1", got)
	}

	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/target.bin", totalSize)
	if err != nil {
		t.Fatalf("initiate overwrite upload: %v", err)
	}
	if got := countQuotaOutboxRowsByFileAndMutation(t, ctx, b, target.File.FileID, quotaMutationTypeFileCreate, datastore.QuotaOutboxQueued); got != 1 {
		t.Fatalf("queued create rows after initiate = %d, want 1", got)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage after initiate: %v", err)
	}
	if usage.StorageBytes != 0 || usage.ReservedBytes != totalSize || usage.FileCount != 0 {
		t.Fatalf("usage after initiate = %+v, want storage=0 reserved=%d files=0", usage, totalSize)
	}

	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatalf("get upload: %v", err)
	}
	partData := bytes.Repeat([]byte{7}, int(totalSize))
	for _, p := range plan.Parts {
		start := int64(p.Number-1) * plan.PartSize
		end := start + p.Size
		if end > totalSize {
			end = totalSize
		}
		if _, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, p.Number, bytes.NewReader(partData[start:end])); err != nil {
			t.Fatalf("upload part %d: %v", p.Number, err)
		}
	}
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatalf("confirm upload: %v", err)
	}
	if got := countQuotaOutboxRowsByFileAndMutation(t, ctx, b, target.File.FileID, quotaMutationTypeFileCreate, datastore.QuotaOutboxQueued); got != 1 {
		t.Fatalf("queued create rows after confirm = %d, want 1", got)
	}
	if got := countQuotaOutboxRowsByFileAndMutation(t, ctx, b, target.File.FileID, quotaMutationTypeUploadComplete, datastore.QuotaOutboxQueued); got != 1 {
		t.Fatalf("queued upload-complete rows after confirm = %d, want 1", got)
	}

	b.processQuotaOutboxAvailable(ctx)
	usage, err = fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage after outbox: %v", err)
	}
	if usage.StorageBytes != totalSize || usage.ReservedBytes != 0 || usage.FileCount != 1 {
		t.Fatalf("usage after outbox = %+v, want storage=%d reserved=0 files=1", usage, totalSize)
	}
	fileMeta, err := fake.GetFileMeta(ctx, "tenant-a", target.File.FileID)
	if err != nil {
		t.Fatalf("get central file meta: %v", err)
	}
	if fileMeta.SizeBytes != totalSize {
		t.Fatalf("central file meta size = %d, want %d", fileMeta.SizeBytes, totalSize)
	}
	if got := countPendingQuotaOutboxRowsByFile(t, ctx, b, target.File.FileID); got != 0 {
		t.Fatalf("pending quota outbox rows after drain = %d, want 0", got)
	}
}

func TestUploadCompleteOutboxRetryAfterCentralApplyDoesNotDoubleCharge(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	const (
		uploadID = "upload-retry-after-central-apply"
		fileID   = "file-retry-after-central-apply"
		size     = int64(128)
	)
	if err := fake.AtomicReserveAndInsertUpload(ctx, &UploadReservationView{
		TenantID:       "tenant-a",
		UploadID:       uploadID,
		ReservedBytes:  size,
		FileCountDelta: 1,
		TargetPath:     "/retry.bin",
		Status:         "active",
		ExpiresAt:      time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("reserve upload: %v", err)
	}
	raw := mustQuotaMutationJSON(t, uploadCompleteMutationData{
		UploadID:      uploadID,
		FileID:        fileID,
		ReservedBytes: size,
		NewSizeBytes:  size,
	})
	if _, err := b.store.EnqueueQuotaOutboxTx(b.store.DB(), &datastore.QuotaOutboxEntry{
		FileID:       fileID,
		MutationType: quotaMutationTypeUploadComplete,
		MutationData: raw,
	}); err != nil {
		t.Fatalf("enqueue upload complete: %v", err)
	}

	claimAt := time.Now().UTC()
	entry, found, err := b.store.ClaimQuotaOutbox(ctx, claimAt, time.Second)
	if err != nil {
		t.Fatalf("claim upload complete: %v", err)
	}
	if !found {
		t.Fatal("claim upload complete: not found")
	}
	if err := b.applyQuotaOutboxEntry(ctx, entry); err != nil {
		t.Fatalf("first central apply: %v", err)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage after first apply: %v", err)
	}
	if usage.StorageBytes != size || usage.ReservedBytes != 0 || usage.FileCount != 1 {
		t.Fatalf("usage after first apply = %+v, want storage=%d reserved=0 files=1", usage, size)
	}

	recoverAt := claimAt.Add(2 * time.Second)
	recovered, err := b.store.RecoverExpiredQuotaOutbox(ctx, recoverAt, 10)
	if err != nil {
		t.Fatalf("recover expired outbox: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered rows = %d, want 1", recovered)
	}
	retryEntry, found, err := b.store.ClaimQuotaOutbox(ctx, recoverAt.Add(time.Second), quotaOutboxLeaseDuration)
	if err != nil {
		t.Fatalf("reclaim upload complete: %v", err)
	}
	if !found {
		t.Fatal("reclaim upload complete: not found")
	}
	applied, processed, err := b.processQuotaOutboxEntry(ctx, retryEntry)
	if err != nil {
		t.Fatalf("retry process upload complete: %v", err)
	}
	if !applied || !processed {
		t.Fatalf("retry process applied=%t processed=%t, want true/true", applied, processed)
	}
	usage, err = fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get usage after retry: %v", err)
	}
	if usage.StorageBytes != size || usage.ReservedBytes != 0 || usage.FileCount != 1 {
		t.Fatalf("usage after retry = %+v, want storage=%d reserved=0 files=1", usage, size)
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, fileID, datastore.QuotaOutboxSucceeded); got != 1 {
		t.Fatalf("succeeded upload-complete rows = %d, want 1", got)
	}
}

func TestDrainQuotaOutboxForFileContinuesAfterUnrelatedBatchError(t *testing.T) {
	b, _ := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	for i := 0; i < quotaOutboxBatchSize; i++ {
		fileID := "older-file"
		data := json.RawMessage(`{"file_id":"bad-json","size_bytes":"bad","is_media":false}`)
		if i > 0 {
			fileID = fmt.Sprintf("older-file-%03d", i)
			data = mustQuotaMutationJSON(t, fileCreateMutationData{
				FileID:    fileID,
				SizeBytes: 1,
			})
		}
		if _, err := b.store.EnqueueQuotaOutboxTx(b.store.DB(), &datastore.QuotaOutboxEntry{
			FileID:       fileID,
			MutationType: quotaMutationTypeFileCreate,
			MutationData: data,
			StorageDelta: 1,
			FileDelta:    1,
		}); err != nil {
			t.Fatalf("enqueue older %d: %v", i, err)
		}
	}
	targetFileID := "target-file-after-unrelated-error"
	if _, err := b.store.EnqueueQuotaOutboxTx(b.store.DB(), &datastore.QuotaOutboxEntry{
		FileID:       targetFileID,
		MutationType: quotaMutationTypeFileCreate,
		MutationData: mustQuotaMutationJSON(t, fileCreateMutationData{
			FileID:    targetFileID,
			SizeBytes: 1,
		}),
		StorageDelta: 1,
		FileDelta:    1,
	}); err != nil {
		t.Fatalf("enqueue target: %v", err)
	}

	if err := b.drainQuotaOutboxForFile(ctx, targetFileID, quotaOutboxUploadDrainLimit); err != nil {
		t.Fatalf("drain target: %v", err)
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, targetFileID, datastore.QuotaOutboxSucceeded); got != 1 {
		t.Fatalf("target succeeded rows = %d, want 1", got)
	}
	if got := countQuotaOutboxRowsByFile(t, ctx, b, "older-file", datastore.QuotaOutboxQueued); got != 1 {
		t.Fatalf("bad unrelated queued retry rows = %d, want 1", got)
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

	reserved, err := b.reserveUploadOnServer(ctx, "upload-pending", "/upload.bin", 3, 0)
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

func TestServerQuotaUploadReserveUsesLivePendingOutboxDeltas(t *testing.T) {
	b, fake := newServerQuotaBackend(t, Options{})
	b.stopQuotaOutboxWorker()
	ctx := context.Background()

	fake.mu.Lock()
	fake.config["tenant-a"].MaxStorageBytes = 10
	fake.mu.Unlock()
	b.quotaConfigCache.refresh(ctx)

	if b.quotaPendingCache == nil {
		t.Fatal("quota pending cache is nil")
	}
	cached, ok := b.quotaPendingCache.get(ctx)
	if !ok {
		t.Fatal("warm quota pending cache failed")
	}
	if cached.storageDelta != 0 || cached.fileDelta != 0 || cached.mediaDelta != 0 {
		t.Fatalf("initial cached pending deltas = %+v, want zero", cached)
	}

	if _, err := b.store.EnqueueQuotaOutboxTx(b.store.DB(), &datastore.QuotaOutboxEntry{
		FileID:       "other-server-file",
		MutationType: quotaMutationTypeFileCreate,
		MutationData: json.RawMessage(`{}`),
		StorageDelta: 8,
		FileDelta:    1,
	}); err != nil {
		t.Fatalf("enqueue quota outbox: %v", err)
	}

	reserved, err := b.reserveUploadOnServer(ctx, "upload-live-pending", "/upload.bin", 3, 0)
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("reserve error = %v, want ErrStorageQuotaExceeded", err)
	}
	if reserved {
		t.Fatal("reserve should be false when live pending outbox pushes tenant over limit")
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
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

	reserved, err := b.reserveUploadOnServer(ctx, "upload-retry", "/upload.bin", 8, 0)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if !reserved {
		t.Fatal("first reserve returned false, want true")
	}
	reserved, err = b.reserveUploadOnServer(ctx, "upload-retry", "/upload.bin", 8, 0)
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
	if usage.StorageBytes != 9 || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after duplicate apply = %+v, want storage=9 files=1 media=1", usage)
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

func countQuotaOutboxRowsByFileAndMutation(t *testing.T, ctx context.Context, b *Dat9Backend, fileID string, mutationType string, status datastore.QuotaOutboxStatus) int {
	t.Helper()
	var count int
	if err := b.Store().DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox
		WHERE file_id = ? AND mutation_type = ? AND status = ?`, fileID, mutationType, status).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func countPendingQuotaOutboxRowsByFile(t *testing.T, ctx context.Context, b *Dat9Backend, fileID string) int {
	t.Helper()
	var count int
	if err := b.Store().DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox
		WHERE file_id = ? AND status IN (?, ?)`,
		fileID, datastore.QuotaOutboxQueued, datastore.QuotaOutboxProcessing).Scan(&count); err != nil {
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

func mustQuotaMutationJSON(t *testing.T, payload any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal quota mutation: %v", err)
	}
	return raw
}
