package datastore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
	"github.com/stretchr/testify/require"
)

func promotionTakeOverRequest(created *PromotionImport, newOwnerToken string) PromotionTakeOverRequest {
	return PromotionTakeOverRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID,
		ExpectedOwnerEpoch: created.OwnerEpoch, RecoveryToken: "recovery-token",
		NewOwnerToken: newOwnerToken, WriterLease: "writer-lease",
	}
}

func promotionRenewRequest(created *PromotionImport, ownerToken string) PromotionRenewRequest {
	return PromotionRenewRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: ownerToken, WriterLease: "writer-lease",
	}
}

func expirePromotionOwnerLease(t *testing.T, store *Store, created *PromotionImport) {
	t.Helper()
	_, err := store.DB().Exec(`UPDATE promotion_imports
		SET lease_expires_at = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)
}

func TestPromotionGetActiveByRecoveryTokenAndAllocationProof(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-get-active")

	byRecovery, err := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
	})
	require.NoError(t, err)
	require.Equal(t, created, byRecovery)

	var proof string
	require.NoError(t, store.DB().QueryRow(`SELECT allocation_proof_blob
		FROM promotion_import_id_claims
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&proof))
	byProof, err := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, AllocationProof: proof,
	})
	require.NoError(t, err)
	require.Equal(t, created, byProof)
}

func TestPromotionGetDeniedInvalidAndUnknownAreIndistinguishable(t *testing.T) {
	_, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-get-nondisclosure")
	authorizer := promotionStore.cfg.Authorizer.(*promotionTestAuthorizer)
	authorizer.mu.Lock()
	authorizer.denied = true
	authorizer.mu.Unlock()

	_, deniedErr := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
	})
	require.ErrorIs(t, deniedErr, ErrNotFound)

	authorizer.mu.Lock()
	authorizer.denied = false
	authorizer.mu.Unlock()
	unknownID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{
		AllocationEpoch: created.AllocationEpoch, AllocationSequence: created.AllocationSequence + 1000,
	})
	require.NoError(t, err)
	_, unknownErr := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: unknownID, RecoveryToken: "recovery-token",
	})
	require.ErrorIs(t, unknownErr, ErrNotFound)
	_, invalidErr := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "wrong-token",
	})
	require.ErrorIs(t, invalidErr, ErrNotFound)
	require.Equal(t, deniedErr, unknownErr)
	require.Equal(t, unknownErr, invalidErr)
}

func TestPromotionGetReturnsExactTerminalResultFromFullRowAndTombstone(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-get-terminal")
	aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)

	full, err := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
	})
	require.NoError(t, err)
	require.Equal(t, aborted.TerminalResultBlob, full.TerminalResultBlob)
	require.Equal(t, aborted.TerminalResultDigest, full.TerminalResultDigest)

	var proofDigest, requestDigest, ownerHash, recoveryHash, target string
	require.NoError(t, store.DB().QueryRow(`SELECT allocation_proof_digest, create_request_digest,
		owner_token_hash, recovery_token_hash, target_path FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).
		Scan(&proofDigest, &requestDigest, &ownerHash, &recoveryHash, &target))
	_, err = store.DB().Exec(`INSERT INTO promotion_import_tombstones
		(tenant_id, allocation_epoch, allocation_sequence, allocation_proof_digest,
		 target_path, target_path_hash, request_digest, owner_token_hash, recovery_token_hash,
		 terminal_state, terminal_result_blob, terminal_result_digest, retire_after)
		VALUES ('tenant-a', ?, ?, ?, ?, ?, ?, ?, ?, 'ABORTED', ?, ?, DATE_ADD(NOW(3), INTERVAL 1 DAY))`,
		created.AllocationEpoch, created.AllocationSequence, proofDigest, target,
		fileNodePathHash(target), requestDigest, ownerHash, recoveryHash,
		aborted.TerminalResultBlob, aborted.TerminalResultDigest)
	require.NoError(t, err)
	_, err = store.DB().Exec(`DELETE FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	tombstone, err := promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
	})
	require.NoError(t, err)
	require.Equal(t, "ABORTED", tombstone.State)
	require.Equal(t, aborted.TerminalResultBlob, tombstone.TerminalResultBlob)
	require.Equal(t, aborted.TerminalResultDigest, tombstone.TerminalResultDigest)
	require.NotZero(t, tombstone.RestoreGeneration)
	require.NotEmpty(t, tombstone.DatabaseIncarnation)
}

func TestPromotionGetCorruptTerminalResultFailsClosed(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-get-corrupt")
	_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	_, err = store.DB().Exec(`UPDATE promotion_imports SET terminal_result_digest = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		promotionTestHash("different"), created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	_, err = promotionStore.GetImport(context.Background(), PromotionGetRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, RecoveryToken: "recovery-token",
	})
	require.ErrorIs(t, err, ErrPromotionRecoveryRequired)
}

func TestPromotionTakeOverExpiredLeaseAndExactRetry(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-takeover")
	expirePromotionOwnerLease(t, store, created)
	req := promotionTakeOverRequest(created, "owner-token-2")

	taken, err := promotionStore.TakeOverImport(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, created.OwnerEpoch+1, taken.OwnerEpoch)
	require.Equal(t, created.StateVersion+1, taken.StateVersion)
	retried, err := promotionStore.TakeOverImport(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, taken, retried)

	_, err = promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	})
	require.ErrorIs(t, err, ErrPromotionConflict)
	verified, err := promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: taken.OwnerEpoch,
		OwnerToken: "owner-token-2", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	})
	require.NoError(t, err)
	require.Equal(t, "VERIFIED", verified.State)
}

func TestPromotionTakeOverRejectsLiveLeaseWithoutMutation(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-takeover-live")
	_, err := promotionStore.TakeOverImport(context.Background(), promotionTakeOverRequest(created, "owner-token-2"))
	require.ErrorIs(t, err, ErrPromotionConflict)

	var epoch, version uint64
	var ownerHash string
	require.NoError(t, store.DB().QueryRow(`SELECT owner_epoch, owner_token_hash, state_version
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&epoch, &ownerHash, &version))
	require.Equal(t, created.OwnerEpoch, epoch)
	require.Equal(t, promotionHashString("owner-token"), ownerHash)
	require.Equal(t, created.StateVersion, version)
}

func TestPromotionConcurrentTakeOverHasOneWinner(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-takeover-race")
	expirePromotionOwnerLease(t, store, created)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, token := range []string{"owner-token-a", "owner-token-b"} {
		token := token
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := promotionStore.TakeOverImport(context.Background(), promotionTakeOverRequest(created, token))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	var successes, conflicts int
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrPromotionConflict):
			conflicts++
		default:
			t.Fatalf("TakeOverImport error = %v", err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)

	var epoch uint64
	require.NoError(t, store.DB().QueryRow(`SELECT owner_epoch FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&epoch))
	require.Equal(t, created.OwnerEpoch+1, epoch)
}

func TestPromotionTakeOverDeadlineWinsWithDurableCleanupOwner(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-takeover-deadline")
	_, err := store.DB().Exec(`UPDATE promotion_imports
		SET lease_expires_at = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 2 SECOND),
		    activity_deadline = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	_, err = promotionStore.TakeOverImport(context.Background(), promotionTakeOverRequest(created, "owner-token-2"))
	require.ErrorIs(t, err, ErrPromotionDeadlineExceeded)
	var state, attempt, reason string
	var attemptGeneration uint64
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id,
		cleanup_writer_generation, terminal_reason FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).
		Scan(&state, &attempt, &attemptGeneration, &reason))
	require.Equal(t, "ABORTING", state)
	require.NotEmpty(t, attempt)
	require.NotZero(t, attemptGeneration)
	require.Equal(t, "activity_deadline_exceeded", reason)
}

func TestPromotionTakeOverRestoreFenceMakesNoMutation(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-takeover-fenced")
	expirePromotionOwnerLease(t, store, created)
	_, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
		SET admission_state = 'DRAINING' WHERE tenant_id = 'tenant-a'`)
	require.NoError(t, err)

	_, err = promotionStore.TakeOverImport(context.Background(), promotionTakeOverRequest(created, "owner-token-2"))
	require.ErrorIs(t, err, ErrPromotionRestoreFenced)
	var epoch, version uint64
	require.NoError(t, store.DB().QueryRow(`SELECT owner_epoch, state_version FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&epoch, &version))
	require.Equal(t, created.OwnerEpoch, epoch)
	require.Equal(t, created.StateVersion, version)
}

func TestPromotionTakeOverUsesFreshTimeAfterImportLock(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-takeover-final-time")
	var databaseNow time.Time
	require.NoError(t, store.DB().QueryRow(`SELECT CURRENT_TIMESTAMP(3)`).Scan(&databaseNow))
	_, err := store.DB().Exec(`UPDATE promotion_imports
		SET lease_expires_at = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND), activity_deadline = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		databaseNow.Add(1500*time.Millisecond), created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	blocker, err := store.DB().BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	var state string
	require.NoError(t, blocker.QueryRow(`SELECT state FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state))

	errs := make(chan error, 1)
	go func() {
		_, err := promotionStore.TakeOverImport(context.Background(), promotionTakeOverRequest(created, "owner-token-2"))
		errs <- err
	}()
	select {
	case err := <-errs:
		t.Fatalf("TakeOverImport returned before import-row lock release: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, blocker.Commit())

	select {
	case err := <-errs:
		require.ErrorIs(t, err, ErrPromotionDeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("TakeOverImport did not finish after import-row lock release")
	}
}

func TestPromotionRenewExtendsNearExpiryAndExactRetryIsStable(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-renew")
	var databaseNow time.Time
	require.NoError(t, store.DB().QueryRow(`SELECT CURRENT_TIMESTAMP(3)`).Scan(&databaseNow))
	nearExpiry := databaseNow.Add(10 * time.Second)
	_, err := store.DB().Exec(`UPDATE promotion_imports SET lease_expires_at = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		nearExpiry, created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	renewed, err := promotionStore.RenewImportLease(context.Background(), promotionRenewRequest(created, "owner-token"))
	require.NoError(t, err)
	require.Equal(t, created.StateVersion+1, renewed.StateVersion)
	require.True(t, renewed.LeaseExpiresAt.After(nearExpiry))
	retried, err := promotionStore.RenewImportLease(context.Background(), promotionRenewRequest(created, "owner-token"))
	require.NoError(t, err)
	require.Equal(t, renewed, retried)
}

func TestPromotionRenewWrongOrExpiredOwnerMakesNoMutation(t *testing.T) {
	t.Run("wrong owner", func(t *testing.T) {
		store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-renew-wrong")
		_, err := promotionStore.RenewImportLease(context.Background(), promotionRenewRequest(created, "wrong-owner"))
		require.ErrorIs(t, err, ErrPromotionConflict)
		var version uint64
		require.NoError(t, store.DB().QueryRow(`SELECT state_version FROM promotion_imports
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
			created.AllocationEpoch, created.AllocationSequence).Scan(&version))
		require.Equal(t, created.StateVersion, version)
	})

	t.Run("expired owner", func(t *testing.T) {
		store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-renew-expired")
		expirePromotionOwnerLease(t, store, created)
		_, err := promotionStore.RenewImportLease(context.Background(), promotionRenewRequest(created, "owner-token"))
		require.ErrorIs(t, err, ErrPromotionConflict)
		var version uint64
		require.NoError(t, store.DB().QueryRow(`SELECT state_version FROM promotion_imports
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
			created.AllocationEpoch, created.AllocationSequence).Scan(&version))
		require.Equal(t, created.StateVersion, version)
	})
}

func TestPromotionRenewDeadlineAtomicallyClaimsCleanup(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-renew-deadline")
	_, err := store.DB().Exec(`UPDATE promotion_imports SET activity_deadline = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	_, err = promotionStore.RenewImportLease(context.Background(), promotionRenewRequest(created, "owner-token"))
	require.ErrorIs(t, err, ErrPromotionDeadlineExceeded)
	var state, attempt, reason string
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &attempt, &reason))
	require.Equal(t, "ABORTING", state)
	require.NotEmpty(t, attempt)
	require.Equal(t, "activity_deadline_exceeded", reason)
}

func TestPromotionSweepExpiredOwnerCleansPartialStaging(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "one", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 3, ExpectedChecksumSHA256: promotionTestHash("one")},
		{RelativePath: "two", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 3, ExpectedChecksumSHA256: promotionTestHash("two")},
	}, "allocate-sweep-expired")
	_, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(
		created, manifest[0], "sweep-partial", bytes.NewBufferString("one"),
	))
	require.NoError(t, err)
	expirePromotionOwnerLease(t, store, created)

	completed, err := promotionStore.SweepPromotionImports(context.Background(), PromotionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, completed)
	var state, reason string
	require.NoError(t, store.DB().QueryRow(`SELECT state, terminal_reason FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &reason))
	require.Equal(t, "ABORTED", state)
	require.Equal(t, "owner_lease_expired", reason)
	assertPromotionAbortFullyReleased(t, store, created)

	completed, err = promotionStore.SweepPromotionImports(context.Background(), PromotionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Zero(t, completed)
	assertPromotionAbortFullyReleased(t, store, created)
}

func TestPromotionSweepDeadlineWinsOverExpiredOwner(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-sweep-deadline")
	_, err := store.DB().Exec(`UPDATE promotion_imports
		SET lease_expires_at = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 2 SECOND),
		    activity_deadline = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	completed, err := promotionStore.SweepPromotionImports(context.Background(), PromotionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, completed)
	var state, reason string
	require.NoError(t, store.DB().QueryRow(`SELECT state, terminal_reason FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &reason))
	require.Equal(t, "ABORTED", state)
	require.Equal(t, "activity_deadline_exceeded", reason)
}

func TestPromotionSweepResumesExistingCleanupAttempt(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-sweep-resume")
	promotionTestFailBeforeAbortFinalize = func() error { return errors.New("stop cleanup") }
	t.Cleanup(func() { promotionTestFailBeforeAbortFinalize = nil })
	_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.EqualError(t, err, "stop cleanup")
	promotionTestFailBeforeAbortFinalize = nil
	var firstAttempt string
	require.NoError(t, store.DB().QueryRow(`SELECT cleanup_attempt_id FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&firstAttempt))

	completed, err := promotionStore.SweepPromotionImports(context.Background(), PromotionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, completed)
	var state, secondAttempt string
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &secondAttempt))
	require.Equal(t, "ABORTED", state)
	require.Equal(t, firstAttempt, secondAttempt)
	assertPromotionAbortFullyReleased(t, store, created)
}

func TestPromotionSweepRestoreFenceLeavesCandidateUntouched(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-sweep-fenced")
	expirePromotionOwnerLease(t, store, created)
	_, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
		SET admission_state = 'DRAINING' WHERE tenant_id = 'tenant-a'`)
	require.NoError(t, err)

	completed, err := promotionStore.SweepPromotionImports(context.Background(), PromotionSweepRequest{
		TenantID: "tenant-a", WriterLease: "writer-lease", Limit: 10,
	})
	require.Zero(t, completed)
	require.ErrorIs(t, err, ErrPromotionRestoreFenced)
	var state string
	var attempt sql.NullString
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &attempt))
	require.Equal(t, "CREATED", state)
	require.False(t, attempt.Valid)
}
