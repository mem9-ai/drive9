package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQuotaOutboxLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]any{"file_id": "file-1"})
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		var err error
		id, err = s.EnqueueQuotaOutboxTx(tx, &QuotaOutboxEntry{
			FileID:       "file-1",
			MutationType: "file_create",
			MutationData: payload,
			StorageDelta: 7,
			FileDelta:    1,
			MediaDelta:   1,
		})
		return err
	}); err != nil {
		t.Fatalf("enqueue quota outbox: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero quota outbox id")
	}

	storageDelta, fileDelta, mediaDelta, err := s.PendingQuotaOutboxDeltas(ctx)
	if err != nil {
		t.Fatalf("pending deltas: %v", err)
	}
	if storageDelta != 7 || fileDelta != 1 || mediaDelta != 1 {
		t.Fatalf("pending deltas = %d/%d/%d, want 7/1/1", storageDelta, fileDelta, mediaDelta)
	}

	claimed, found, err := s.ClaimQuotaOutbox(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("claim quota outbox: %v", err)
	}
	if !found {
		t.Fatal("expected quota outbox row")
	}
	if claimed.ID != id || claimed.Status != QuotaOutboxProcessing || claimed.Receipt == "" {
		t.Fatalf("claimed row = %+v", claimed)
	}
	if claimed.MaxAttempts != defaultQuotaOutboxMaxAttempts {
		t.Fatalf("claimed max_attempts = %d, want default %d", claimed.MaxAttempts, defaultQuotaOutboxMaxAttempts)
	}

	if err := s.AckQuotaOutbox(ctx, claimed.ID, claimed.Receipt); err != nil {
		t.Fatalf("ack quota outbox: %v", err)
	}
	storageDelta, fileDelta, mediaDelta, err = s.PendingQuotaOutboxDeltas(ctx)
	if err != nil {
		t.Fatalf("pending deltas after ack: %v", err)
	}
	if storageDelta != 0 || fileDelta != 0 || mediaDelta != 0 {
		t.Fatalf("pending deltas after ack = %d/%d/%d, want 0/0/0", storageDelta, fileDelta, mediaDelta)
	}
}

func TestQuotaOutboxConcurrentBatchClaimNoDuplicates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	const totalRows = 20
	for i := 0; i < totalRows; i++ {
		if _, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
			FileID:       fmt.Sprintf("file-%02d", i),
			MutationType: "file_create",
			MutationData: json.RawMessage(`{"file_id":"file"}`),
			AvailableAt:  now.Add(-time.Second),
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	seen := make(map[int64]struct{})
	errCh := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < cap(errCh); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claimed, err := s.ClaimQuotaOutboxBatch(ctx, now, time.Minute, 5)
				if err != nil {
					errCh <- err
					return
				}
				if len(claimed) == 0 {
					return
				}
				mu.Lock()
				for _, entry := range claimed {
					if _, ok := seen[entry.ID]; ok {
						mu.Unlock()
						errCh <- fmt.Errorf("duplicate claim for row %d", entry.ID)
						return
					}
					seen[entry.ID] = struct{}{}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if len(seen) != totalRows {
		t.Fatalf("claimed rows = %d, want %d", len(seen), totalRows)
	}
}

func TestHasPendingQuotaOutboxFileMutationIncludesUploadComplete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	payload := json.RawMessage(`{"upload_id":"upload-1","file_id":"file-1"}`)
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		_, err := s.EnqueueQuotaOutboxTx(tx, &QuotaOutboxEntry{
			FileID:       "file-1",
			MutationType: "upload_complete",
			MutationData: payload,
		})
		return err
	}); err != nil {
		t.Fatalf("enqueue upload complete outbox: %v", err)
	}

	pending, err := s.HasPendingQuotaOutboxFileMutation(ctx, "file-1")
	if err != nil {
		t.Fatalf("has pending file mutation: %v", err)
	}
	if !pending {
		t.Fatal("pending file mutation = false, want true")
	}

	claimed, found, err := s.ClaimQuotaOutbox(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("claim quota outbox: %v", err)
	}
	if !found {
		t.Fatal("expected quota outbox row")
	}
	if err := s.AckQuotaOutbox(ctx, claimed.ID, claimed.Receipt); err != nil {
		t.Fatalf("ack quota outbox: %v", err)
	}
	pending, err = s.HasPendingQuotaOutboxFileMutation(ctx, "file-1")
	if err != nil {
		t.Fatalf("has pending file mutation after ack: %v", err)
	}
	if pending {
		t.Fatal("pending file mutation after ack = true, want false")
	}
}

func TestQuotaOutboxRecoverExpired(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]any{"file_id": "file-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		_, err := s.EnqueueQuotaOutboxTx(tx, &QuotaOutboxEntry{
			FileID:       "file-1",
			MutationType: "file_create",
			MutationData: payload,
			StorageDelta: 7,
		})
		return err
	}); err != nil {
		t.Fatalf("enqueue quota outbox: %v", err)
	}
	claimed, found, err := s.ClaimQuotaOutbox(ctx, time.Now().UTC(), time.Millisecond)
	if err != nil {
		t.Fatalf("claim quota outbox: %v", err)
	}
	if !found {
		t.Fatal("expected quota outbox row")
	}

	recovered, err := s.RecoverExpiredQuotaOutbox(ctx, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("recover expired quota outbox: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered rows = %d, want 1", recovered)
	}
	reclaimed, found, err := s.ClaimQuotaOutbox(ctx, time.Now().Add(time.Second), time.Minute)
	if err != nil {
		t.Fatalf("reclaim quota outbox: %v", err)
	}
	if !found || reclaimed.ID != claimed.ID {
		t.Fatalf("reclaimed row = %+v found=%v, want id %d", reclaimed, found, claimed.ID)
	}
	if err := s.AckQuotaOutbox(ctx, claimed.ID, claimed.Receipt); err != ErrQuotaOutboxLeaseMismatch {
		t.Fatalf("ack recovered row with old receipt error = %v, want lease mismatch", err)
	}
}

func TestQuotaOutboxBatchAckRejectsRecoveredOldReceipt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)

	id, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  now.Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimQuotaOutboxBatch(ctx, now, time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != id {
		t.Fatalf("claimed = %+v, want id %d", claimed, id)
	}
	if recovered, err := s.RecoverExpiredQuotaOutbox(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	} else if recovered != 1 {
		t.Fatalf("recovered rows = %d, want 1", recovered)
	}
	if _, err := s.ClaimQuotaOutboxBatch(ctx, time.Now().UTC(), time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.AckQuotaOutboxBatchTx(ctx, tx, claimed)
	}); err != ErrQuotaOutboxLeaseMismatch {
		t.Fatalf("batch ack recovered row with old receipt error = %v, want lease mismatch", err)
	}
}

func TestQuotaOutboxRetryRejectsRecoveredOldReceipt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)

	id, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  now.Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimQuotaOutbox(ctx, now, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.ID != id {
		t.Fatalf("claimed = %+v found=%v, want id %d", claimed, found, id)
	}
	if recovered, err := s.RecoverExpiredQuotaOutbox(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	} else if recovered != 1 {
		t.Fatalf("recovered rows = %d, want 1", recovered)
	}
	if _, _, err := s.ClaimQuotaOutbox(ctx, time.Now().UTC(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryQuotaOutbox(ctx, claimed.ID, claimed.Receipt, time.Now().UTC(), "temporary"); err != ErrQuotaOutboxLeaseMismatch {
		t.Fatalf("retry recovered row with old receipt error = %v, want lease mismatch", err)
	}
}

func TestQuotaOutboxAckAllowsExpiredLeaseWhenReceiptStillOwned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)

	id, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  now.Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimQuotaOutbox(ctx, now, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.ID != id {
		t.Fatalf("claimed = %+v found=%v, want id %d", claimed, found, id)
	}

	if err := s.AckQuotaOutbox(ctx, claimed.ID, claimed.Receipt); err != nil {
		t.Fatalf("ack expired-but-owned quota outbox: %v", err)
	}
	var status QuotaOutboxStatus
	if err := s.DB().QueryRowContext(ctx, `SELECT status FROM quota_outbox WHERE id = ?`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != QuotaOutboxSucceeded {
		t.Fatalf("status = %q, want %q", status, QuotaOutboxSucceeded)
	}
}

func TestQuotaOutboxBatchAckAllowsExpiredLeaseWhenReceiptStillOwned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	availableNow := now.Add(-time.Second)

	firstID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-2",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-2"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := s.ClaimQuotaOutboxBatch(ctx, now, time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 || claimed[0].ID != firstID || claimed[1].ID != secondID {
		t.Fatalf("claimed = %+v, want ids %d,%d", claimed, firstID, secondID)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.AckQuotaOutboxBatchTx(ctx, tx, claimed)
	}); err != nil {
		t.Fatalf("batch ack expired-but-owned quota outbox: %v", err)
	}
	var succeeded int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM quota_outbox WHERE status = ?`, QuotaOutboxSucceeded).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if succeeded != 2 {
		t.Fatalf("succeeded rows = %d, want 2", succeeded)
	}
}

func TestQuotaOutboxRetryAllowsExpiredLeaseWhenReceiptStillOwned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)

	id, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  now.Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimQuotaOutbox(ctx, now, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.ID != id {
		t.Fatalf("claimed = %+v found=%v, want id %d", claimed, found, id)
	}

	retryAt := now.Add(time.Minute)
	if err := s.RetryQuotaOutbox(ctx, claimed.ID, claimed.Receipt, retryAt, "temporary"); err != nil {
		t.Fatalf("retry expired-but-owned quota outbox: %v", err)
	}
	var status QuotaOutboxStatus
	if err := s.DB().QueryRowContext(ctx, `SELECT status FROM quota_outbox WHERE id = ?`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != QuotaOutboxQueued {
		t.Fatalf("status = %q, want %q", status, QuotaOutboxQueued)
	}
}

func TestQuotaOutboxClaimWaitsForDelayedOlderRetry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	availableNow := now.Add(-time.Second)

	firstID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		MutationType: "file_overwrite",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  availableNow,
	}); err != nil {
		t.Fatal(err)
	}

	claimed, found, err := s.ClaimQuotaOutbox(ctx, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.ID != firstID {
		t.Fatalf("claimed = %+v found=%v, want first id %d", claimed, found, firstID)
	}

	retryAt := now.Add(time.Hour)
	if err := s.RetryQuotaOutbox(ctx, claimed.ID, claimed.Receipt, retryAt, "temporary"); err != nil {
		t.Fatal(err)
	}
	if claimed, found, err = s.ClaimQuotaOutbox(ctx, now.Add(time.Minute), time.Minute); err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("claimed row while older retry delayed: %+v", claimed)
	}
	claimed, found, err = s.ClaimQuotaOutbox(ctx, retryAt.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.ID != firstID {
		t.Fatalf("claimed after retry = %+v found=%v, want first id %d", claimed, found, firstID)
	}
}

func TestQuotaOutboxBatchClaimPreservesPerFileOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	availableNow := now.Add(-time.Second)

	file1CreateID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	file2CreateID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-2",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-2"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	file1OverwriteID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_overwrite",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := s.ClaimQuotaOutboxBatch(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed rows = %d, want 2", len(claimed))
	}
	if claimed[0].ID != file1CreateID || claimed[1].ID != file2CreateID {
		t.Fatalf("claimed ids = %d,%d want %d,%d", claimed[0].ID, claimed[1].ID, file1CreateID, file2CreateID)
	}

	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.AckQuotaOutboxBatchTx(ctx, tx, claimed)
	}); err != nil {
		t.Fatalf("ack claimed batch: %v", err)
	}
	claimed, err = s.ClaimQuotaOutboxBatch(ctx, now.Add(time.Second), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != file1OverwriteID {
		t.Fatalf("claimed after ack = %+v, want file-1 overwrite id %d", claimed, file1OverwriteID)
	}
}

func TestQuotaOutboxBatchClaimTreatsNullFileIDAsGlobalBarrier(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	availableNow := now.Add(-time.Second)

	globalID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		MutationType: "global_legacy",
		MutationData: json.RawMessage(`{}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := s.ClaimQuotaOutboxBatch(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != globalID {
		t.Fatalf("claimed behind older NULL file_id = %+v, want only global id %d", claimed, globalID)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.AckQuotaOutboxBatchTx(ctx, tx, claimed)
	}); err != nil {
		t.Fatalf("ack global row: %v", err)
	}
	claimed, err = s.ClaimQuotaOutboxBatch(ctx, now.Add(time.Second), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != fileID {
		t.Fatalf("claimed after global ack = %+v, want file id %d", claimed, fileID)
	}
}

func TestQuotaOutboxBatchClaimBlocksNullFileIDBehindOlderPendingRow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	availableNow := now.Add(-time.Second)

	file1ID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	globalID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		MutationType: "global_legacy",
		MutationData: json.RawMessage(`{}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	file2ID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		FileID:       "file-2",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-2"}`),
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := s.ClaimQuotaOutboxBatch(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != file1ID {
		t.Fatalf("claimed with later NULL file_id = %+v, want only older file id %d", claimed, file1ID)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.AckQuotaOutboxBatchTx(ctx, tx, claimed)
	}); err != nil {
		t.Fatalf("ack file row: %v", err)
	}

	claimed, err = s.ClaimQuotaOutboxBatch(ctx, now.Add(time.Second), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != globalID {
		t.Fatalf("claimed after older file ack = %+v, want NULL file id %d", claimed, globalID)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.AckQuotaOutboxBatchTx(ctx, tx, claimed)
	}); err != nil {
		t.Fatalf("ack global row: %v", err)
	}

	claimed, err = s.ClaimQuotaOutboxBatch(ctx, now.Add(2*time.Second), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != file2ID {
		t.Fatalf("claimed after global ack = %+v, want later file id %d", claimed, file2ID)
	}
}

func TestQuotaOutboxDeadLetteredOlderRowUnblocksLaterClaim(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	availableNow := now.Add(-time.Second)

	firstID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		MutationType: "file_create",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		StorageDelta: 10,
		MaxAttempts:  1,
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := s.EnqueueQuotaOutboxTx(s.DB(), &QuotaOutboxEntry{
		MutationType: "file_overwrite",
		MutationData: json.RawMessage(`{"file_id":"file-1"}`),
		StorageDelta: 3,
		AvailableAt:  availableNow,
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, found, err := s.ClaimQuotaOutbox(ctx, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.ID != firstID {
		t.Fatalf("claimed = %+v found=%v, want first id %d", claimed, found, firstID)
	}

	if err := s.RetryQuotaOutbox(ctx, claimed.ID, claimed.Receipt, now.Add(time.Hour), "poison"); err != nil {
		t.Fatal(err)
	}
	var status QuotaOutboxStatus
	if err := s.DB().QueryRowContext(ctx, `SELECT status FROM quota_outbox WHERE id = ?`, firstID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != QuotaOutboxDeadLettered {
		t.Fatalf("first status = %q, want dead_lettered", status)
	}
	storageDelta, fileDelta, mediaDelta, err := s.PendingQuotaOutboxDeltas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if storageDelta != 3 || fileDelta != 0 || mediaDelta != 0 {
		t.Fatalf("pending deltas = %d/%d/%d, want only later row 3/0/0", storageDelta, fileDelta, mediaDelta)
	}

	claimed, found, err = s.ClaimQuotaOutbox(ctx, now.Add(time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.ID != secondID {
		t.Fatalf("claimed after dead-letter = %+v found=%v, want second id %d", claimed, found, secondID)
	}
}

func TestQuotaAdmissionLockCreatesStableLockRow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := s.InTx(ctx, func(tx *sql.Tx) error {
			return s.LockQuotaAdmissionTx(tx)
		}); err != nil {
			t.Fatalf("lock quota admission iteration %d: %v", i, err)
		}
	}

	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM quota_admission_locks WHERE name = ?`, quotaAdmissionLockName).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("quota admission lock rows = %d, want 1", count)
	}
}
