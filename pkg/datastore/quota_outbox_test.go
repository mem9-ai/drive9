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
