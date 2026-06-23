package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
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
			MediaDelta:   1,
		})
		return err
	}); err != nil {
		t.Fatalf("enqueue quota outbox: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero quota outbox id")
	}

	storageDelta, mediaDelta, err := s.PendingQuotaOutboxDeltas(ctx)
	if err != nil {
		t.Fatalf("pending deltas: %v", err)
	}
	if storageDelta != 7 || mediaDelta != 1 {
		t.Fatalf("pending deltas = %d/%d, want 7/1", storageDelta, mediaDelta)
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

	if err := s.AckQuotaOutbox(ctx, claimed.ID, claimed.Receipt); err != nil {
		t.Fatalf("ack quota outbox: %v", err)
	}
	storageDelta, mediaDelta, err = s.PendingQuotaOutboxDeltas(ctx)
	if err != nil {
		t.Fatalf("pending deltas after ack: %v", err)
	}
	if storageDelta != 0 || mediaDelta != 0 {
		t.Fatalf("pending deltas after ack = %d/%d, want 0/0", storageDelta, mediaDelta)
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
