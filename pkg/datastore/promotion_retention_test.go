package datastore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
	"github.com/stretchr/testify/require"
)

func allocatePromotionForRetention(t *testing.T, promotionStore *PromotionStore, key, target string) *PromotionAllocation {
	t.Helper()
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: target, ExpectedTargetAbsent: true,
		IdempotencyKey: key, AllocationLease: "allocation-lease",
	})
	require.NoError(t, err)
	return allocation
}

func allocationProofForImport(t *testing.T, store *Store, imported *PromotionImport) string {
	t.Helper()
	var proof string
	require.NoError(t, store.DB().QueryRow(`SELECT allocation_proof_blob
		FROM promotion_import_id_claims
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		imported.AllocationEpoch, imported.AllocationSequence).Scan(&proof))
	return proof
}

func promotionIdentityCounters(t *testing.T, store *Store) (live, tenantMaterialized, globalMaterialized, retiredThrough uint64) {
	t.Helper()
	require.NoError(t, store.DB().QueryRow(`SELECT live_claim_count, materialized_identity_count
		FROM promotion_import_identity_tenants WHERE tenant_id = 'tenant-a'`).
		Scan(&live, &tenantMaterialized))
	require.NoError(t, store.DB().QueryRow(`SELECT materialized_identity_count
		FROM promotion_import_identity_global WHERE capacity_key = ?`, promotionGlobalCapacityKey).
		Scan(&globalMaterialized))
	require.NoError(t, store.DB().QueryRow(`SELECT retired_through
		FROM promotion_import_identity_epochs WHERE tenant_id = 'tenant-a' AND allocation_epoch = 2`).
		Scan(&retiredThrough))
	return
}

func TestPromotionRetireAllocationIsIrreversibleAndProofRecoverable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, authorizer, _ := newTestPromotionStore(t, &now)
	allocation := allocatePromotionForRetention(t, promotionStore, "retire-unused", "/published")

	retired, err := promotionStore.RetireImportAllocation(context.Background(), PromotionRetireAllocationRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID,
		AllocationProof: allocation.AllocationProof, WriterLease: "writer-lease",
	})
	require.NoError(t, err)
	require.True(t, retired.Retired)
	require.Nil(t, retired.Import)

	retried, err := promotionStore.RetireImportAllocation(context.Background(), PromotionRetireAllocationRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID,
		AllocationProof: allocation.AllocationProof, WriterLease: "writer-lease",
	})
	require.NoError(t, err)
	require.True(t, retried.Retired)

	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID,
		AllocationProof: allocation.AllocationProof,
	})
	require.ErrorIs(t, err, ErrPromotionIDRetired)

	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	require.NoError(t, err)
	_, err = promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	})
	require.ErrorIs(t, err, ErrPromotionIDRetired)

	live, tenantCount, globalCount, watermark := promotionIdentityCounters(t, store)
	require.Zero(t, live)
	require.Zero(t, tenantCount)
	require.Zero(t, globalCount)
	require.Equal(t, allocation.AllocationSequence, watermark)

	authorizer.mu.Lock()
	authorizer.denied = true
	authorizer.mu.Unlock()
	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID,
		AllocationProof: allocation.AllocationProof,
	})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestPromotionCreateAndRetireAllocationHaveOneDurableWinner(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	_, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest, err := promotion.CanonicalizeManifest(nil, testPromotionLimits())
	require.NoError(t, err)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries,
	})
	require.NoError(t, err)
	allocation := allocatePromotionForRetention(t, promotionStore, "retire-create-race", "/published")
	createRequest := PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}
	retireRequest := PromotionRetireAllocationRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID,
		AllocationProof: allocation.AllocationProof, WriterLease: "writer-lease",
	}

	var created *PromotionImport
	var retired *PromotionRetireAllocationResult
	var createErr, retireErr error
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		created, createErr = promotionStore.CreateImport(context.Background(), createRequest)
	}()
	go func() {
		defer wg.Done()
		<-start
		retired, retireErr = promotionStore.RetireImportAllocation(context.Background(), retireRequest)
	}()
	close(start)
	wg.Wait()
	require.NoError(t, retireErr)

	if createErr == nil {
		require.NotNil(t, created)
		require.NotNil(t, retired.Import)
		require.False(t, retired.Retired)
		require.Equal(t, created.MigrationID, retired.Import.MigrationID)
		return
	}
	require.ErrorIs(t, createErr, ErrPromotionIDRetired)
	require.True(t, retired.Retired)
	require.Nil(t, retired.Import)
}

func TestPromotionGetValidProofForMissingMaterializedIdentityFailsClosed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	allocation := allocatePromotionForRetention(t, promotionStore, "missing-proof-gap", "/published")
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_id_claims
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		allocation.AllocationEpoch, allocation.AllocationSequence).Scan(new(int)))
	_, err := store.DB().Exec(`DELETE FROM promotion_import_id_claims
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		allocation.AllocationEpoch, allocation.AllocationSequence)
	require.NoError(t, err)

	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID,
		AllocationProof: allocation.AllocationProof,
	})
	require.ErrorIs(t, err, ErrPromotionRecoveryRequired)
	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID,
		RecoveryToken: "unknown-recovery-token",
	})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestPromotionAcknowledgeCompactsCompleteTerminalResult(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "retention-ack-compact")
	aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)

	_, err = promotionStore.AcknowledgeImportResult(context.Background(), PromotionAcknowledgeResultRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "wrong-token",
		TerminalState: "ABORTED", TerminalResultDigest: aborted.TerminalResultDigest, WriterLease: "writer-lease",
	})
	require.ErrorIs(t, err, ErrNotFound)
	_, err = promotionStore.AcknowledgeImportResult(context.Background(), PromotionAcknowledgeResultRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
		TerminalState: "ABORTED", TerminalResultDigest: promotionTestHash("wrong"), WriterLease: "writer-lease",
	})
	require.ErrorIs(t, err, ErrPromotionConflict)

	request := PromotionAcknowledgeResultRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
		TerminalState: "ABORTED", TerminalResultDigest: aborted.TerminalResultDigest, WriterLease: "writer-lease",
	}
	acknowledged, err := promotionStore.AcknowledgeImportResult(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, aborted.TerminalResultBlob, acknowledged.TerminalResultBlob)
	retried, err := promotionStore.AcknowledgeImportResult(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, acknowledged.TerminalResultDigest, retried.TerminalResultDigest)

	// An acknowledgement cannot bypass the durable minimum compaction time.
	swept, err := promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Zero(t, swept.Compacted)

	_, err = store.DB().Exec(`UPDATE promotion_imports
		SET full_row_compact_not_before = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)
	swept, err = promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, swept.Compacted)
	require.Zero(t, swept.Retired)

	var imports, tombstones, reservations int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&imports))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_tombstones
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&tombstones))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&reservations))
	require.Zero(t, imports)
	require.Equal(t, 1, tombstones)
	require.Zero(t, reservations)

	recovered, err := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
	})
	require.NoError(t, err)
	require.Equal(t, aborted.TerminalResultBlob, recovered.TerminalResultBlob)
	acknowledged, err = promotionStore.AcknowledgeImportResult(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, aborted.TerminalResultDigest, acknowledged.TerminalResultDigest)
}

func TestPromotionUnacknowledgedTerminalCompactsAtMaximum(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "retention-unacked-compact")
	_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	_, err = store.DB().Exec(`UPDATE promotion_imports
		SET terminal_at = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 2 HOUR),
		    full_row_compact_not_before = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 1 HOUR),
		    retire_after = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 1 DAY)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	swept, err := promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, swept.Compacted)
}

func TestPromotionOutOfOrderTerminalRetirementAdvancesWatermarkAndCounters(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	first, _ := createVerifiedPromotionImportOnStore(t, promotionStore, nil, "retention-first", "/first")
	second, _ := createVerifiedPromotionImportOnStore(t, promotionStore, nil, "retention-second", "/second")
	firstProof := allocationProofForImport(t, store, first)
	secondProof := allocationProofForImport(t, store, second)
	_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(first))
	require.NoError(t, err)
	_, err = promotionStore.AbortImport(context.Background(), abortPromotionRequest(second))
	require.NoError(t, err)
	_, err = store.DB().Exec(`UPDATE promotion_imports
		SET result_acknowledged_at = CURRENT_TIMESTAMP(3),
		    full_row_compact_not_before = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND),
		    retire_after = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 1 DAY)
		WHERE tenant_id = 'tenant-a'`)
	require.NoError(t, err)

	swept, err := promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 2, swept.Compacted)
	require.Zero(t, swept.Retired)

	_, err = store.DB().Exec(`UPDATE promotion_import_tombstones
		SET retire_after = CASE WHEN allocation_sequence = ?
			THEN DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 1 DAY)
			ELSE DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 2 SECOND) END
		WHERE tenant_id = 'tenant-a'`, first.AllocationSequence)
	require.NoError(t, err)
	swept, err = promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, swept.Retired)

	var sparse int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_retired_import_sequences
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		second.AllocationEpoch, second.AllocationSequence).Scan(&sparse))
	require.Equal(t, 1, sparse)
	live, tenantCount, globalCount, watermark := promotionIdentityCounters(t, store)
	require.Zero(t, live)
	require.Equal(t, uint64(2), tenantCount)
	require.Equal(t, uint64(2), globalCount)
	require.Zero(t, watermark)

	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: second.MigrationID, AllocationProof: secondProof,
	})
	require.ErrorIs(t, err, ErrPromotionIDRetired)
	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: first.MigrationID, AllocationProof: firstProof,
	})
	require.NoError(t, err)

	_, err = store.DB().Exec(`UPDATE promotion_import_tombstones
		SET retire_after = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		first.AllocationEpoch, first.AllocationSequence)
	require.NoError(t, err)
	swept, err = promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, swept.Retired)

	live, tenantCount, globalCount, watermark = promotionIdentityCounters(t, store)
	require.Zero(t, live)
	require.Zero(t, tenantCount)
	require.Zero(t, globalCount)
	require.Equal(t, second.AllocationSequence, watermark)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_retired_import_sequences
		WHERE tenant_id = 'tenant-a'`).Scan(&sparse))
	require.Zero(t, sparse)
	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: first.MigrationID, AllocationProof: firstProof,
	})
	require.ErrorIs(t, err, ErrPromotionIDRetired)
}

func TestPromotionCommittedSourceReleasePendingDoesNotRetire(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "retention-commit-pending")
	committed, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	_, err = promotionStore.AcknowledgeImportResult(context.Background(), PromotionAcknowledgeResultRequest{
		TenantID: "tenant-a", MigrationID: committed.MigrationID, RecoveryToken: "recovery-token",
		TerminalState: "COMMITTED", TerminalResultDigest: committed.TerminalResultDigest, WriterLease: "writer-lease",
	})
	require.NoError(t, err)
	_, err = store.DB().Exec(`UPDATE promotion_imports
		SET full_row_compact_not_before = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND),
		    retire_after = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		committed.AllocationEpoch, committed.AllocationSequence)
	require.NoError(t, err)

	swept, err := promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, swept.Compacted)
	require.Zero(t, swept.Retired)
	var tombstones, claims int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_tombstones
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		committed.AllocationEpoch, committed.AllocationSequence).Scan(&tombstones))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_id_claims
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		committed.AllocationEpoch, committed.AllocationSequence).Scan(&claims))
	require.Equal(t, 1, tombstones)
	require.Equal(t, 1, claims)
}

func TestPromotionRetentionRetiresExpiredUnusedAllocation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	allocation := allocatePromotionForRetention(t, promotionStore, "retention-expired-unused", "/published")
	_, err := store.DB().Exec(`UPDATE promotion_import_id_claims
		SET create_before = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 2 DAY)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		allocation.AllocationEpoch, allocation.AllocationSequence)
	require.NoError(t, err)

	swept, err := promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, swept.Retired)
	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
	})
	require.ErrorIs(t, err, ErrPromotionIDRetired)
	live, tenantCount, globalCount, _ := promotionIdentityCounters(t, store)
	require.Zero(t, live)
	require.Zero(t, tenantCount)
	require.Zero(t, globalCount)
}

func TestPromotionRetentionNeverRetiresActiveImport(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "retention-active")
	_, err := store.DB().Exec(`UPDATE promotion_import_id_claims
		SET create_before = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 2 DAY)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	swept, err := promotionStore.SweepPromotionRetention(context.Background(), PromotionRetentionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Zero(t, swept.Retired)
	got, err := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
	})
	require.NoError(t, err)
	require.Equal(t, created.State, got.State)
}

func TestPromotionRetireAllocationReturnsAcceptedWinner(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "retention-accepted-winner")
	proof := allocationProofForImport(t, store, created)
	result, err := promotionStore.RetireImportAllocation(context.Background(), PromotionRetireAllocationRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID,
		AllocationProof: proof, WriterLease: "writer-lease",
	})
	require.NoError(t, err)
	require.False(t, result.Retired)
	require.NotNil(t, result.Import)
	require.Equal(t, created.MigrationID, result.Import.MigrationID)
	require.False(t, errors.Is(err, ErrPromotionIDRetired))
}
