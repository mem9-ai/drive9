package datastore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
	"github.com/stretchr/testify/require"
)

func abortPromotionRequest(created *PromotionImport) PromotionAbortRequest {
	return PromotionAbortRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease",
	}
}

func TestPromotionAbortReleasesQuotaAndRecoversExactResult(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-abort-round-trip")
	_, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(
		created, manifest[0], "abort-content", bytes.NewBufferString("hello"),
	))
	require.NoError(t, err)

	aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	require.Equal(t, "ABORTED", aborted.State)
	require.Equal(t, promotionHashString(aborted.TerminalResultBlob), aborted.TerminalResultDigest)
	require.Contains(t, aborted.TerminalResultBlob, `"terminal_reason":"client_aborted"`)

	var entries, contents int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_entries`).Scan(&entries))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents`).Scan(&contents))
	require.Zero(t, entries)
	require.Zero(t, contents)
	var reservedBytes, reservedFiles uint64
	require.NoError(t, store.DB().QueryRow(`SELECT reserved_bytes, reserved_files
		FROM promotion_quota_accounts WHERE tenant_id = 'tenant-a'`).Scan(&reservedBytes, &reservedFiles))
	require.Zero(t, reservedBytes)
	require.Zero(t, reservedFiles)
	var reservationState string
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "RELEASED", reservationState)

	retried, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	require.Equal(t, aborted.TerminalResultBlob, retried.TerminalResultBlob)
	require.Equal(t, aborted.TerminalResultDigest, retried.TerminalResultDigest)
	require.NoError(t, store.DB().QueryRow(`SELECT reserved_bytes, reserved_files
		FROM promotion_quota_accounts WHERE tenant_id = 'tenant-a'`).Scan(&reservedBytes, &reservedFiles))
	require.Zero(t, reservedBytes)
	require.Zero(t, reservedFiles)
}

func TestPromotionAbortMetadataOnlyReleasesZeroReservation(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-metadata")
	aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	require.Equal(t, "ABORTED", aborted.State)
	var state string
	var version uint64
	require.NoError(t, store.DB().QueryRow(`SELECT state, state_version FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID).Scan(&state, &version))
	require.Equal(t, "RELEASED", state)
	require.Equal(t, uint64(2), version)
}

func TestPromotionAbortDeniedAndUnknownAreIndistinguishable(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-nondisclosure")
	authorizer := promotionStore.cfg.Authorizer.(*promotionTestAuthorizer)
	authorizer.mu.Lock()
	authorizer.denied = true
	authorizer.mu.Unlock()

	_, deniedErr := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.ErrorIs(t, deniedErr, ErrNotFound)
	unknown := abortPromotionRequest(created)
	unknownID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{
		AllocationEpoch: created.AllocationEpoch, AllocationSequence: created.AllocationSequence + 1000,
	})
	require.NoError(t, err)
	unknown.MigrationID = unknownID
	_, unknownErr := promotionStore.AbortImport(context.Background(), unknown)
	require.ErrorIs(t, unknownErr, ErrNotFound)
	require.Equal(t, deniedErr, unknownErr)

	var state string
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state))
	require.Equal(t, "CREATED", state)
}

func TestPromotionAbortCleanupIgnoresShrunkAdmissionLimit(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-abort-shrunk-limit")
	_, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(
		created, manifest[0], "abort-shrunk-limit-content", bytes.NewBufferString("hello"),
	))
	require.NoError(t, err)

	// Admission limits apply to new work. A server-owned cleanup must still
	// release an existing reservation after the operator lowers those limits.
	_, err = store.DB().Exec(`UPDATE promotion_quota_accounts SET max_bytes = 0, max_files = 0
		WHERE tenant_id = 'tenant-a'`)
	require.NoError(t, err)
	aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	require.Equal(t, "ABORTED", aborted.State)

	var reservedBytes, reservedFiles uint64
	require.NoError(t, store.DB().QueryRow(`SELECT reserved_bytes, reserved_files
		FROM promotion_quota_accounts WHERE tenant_id = 'tenant-a'`).Scan(&reservedBytes, &reservedFiles))
	require.Zero(t, reservedBytes)
	require.Zero(t, reservedFiles)
}

func TestPromotionAbortDeadlineWinnerUsesStableReason(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-deadline")
	_, err := store.DB().Exec(`UPDATE promotion_imports SET activity_deadline = DATE_SUB(NOW(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)
	aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	require.Equal(t, "ABORTED", aborted.State)
	require.Contains(t, aborted.TerminalResultBlob, `"terminal_reason":"activity_deadline_exceeded"`)
}

func TestPromotionAbortUsesFreshDatabaseTimeAfterImportLock(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-final-time")
	var databaseNow time.Time
	require.NoError(t, store.DB().QueryRow(`SELECT CURRENT_TIMESTAMP(3)`).Scan(&databaseNow))
	_, err := store.DB().Exec(`UPDATE promotion_imports SET activity_deadline = ?
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
	require.Equal(t, "CREATED", state)

	result := make(chan *PromotionImport, 1)
	errs := make(chan error, 1)
	go func() {
		aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
		result <- aborted
		errs <- err
	}()
	select {
	case err := <-errs:
		t.Fatalf("AbortImport returned before import-row lock release: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, blocker.Commit())

	select {
	case err := <-errs:
		require.NoError(t, err)
		aborted := <-result
		require.Equal(t, "ABORTED", aborted.State)
		require.Contains(t, aborted.TerminalResultBlob, `"terminal_reason":"activity_deadline_exceeded"`)
	case <-time.After(5 * time.Second):
		t.Fatal("AbortImport did not finish after import-row lock release")
	}
}

func TestPromotionAbortAcceptanceSurvivesRequestCancellation(t *testing.T) {
	_, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	promotionTestAfterAbortAccepted = cancel
	t.Cleanup(func() { promotionTestAfterAbortAccepted = nil })
	aborted, err := promotionStore.AbortImport(ctx, abortPromotionRequest(created))
	require.NoError(t, err)
	require.Equal(t, "ABORTED", aborted.State)
}

func TestPromotionAbortFailureLeavesOneRecoverableAttempt(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-retry")
	promotionTestFailBeforeAbortFinalize = func() error { return errors.New("injected cleanup failure") }
	t.Cleanup(func() { promotionTestFailBeforeAbortFinalize = nil })
	_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.EqualError(t, err, "injected cleanup failure")
	var state, firstAttempt, reason, reservationState string
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &firstAttempt, &reason))
	require.Equal(t, "ABORTING", state)
	require.NotEmpty(t, firstAttempt)
	require.Equal(t, "client_aborted", reason)
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "RESERVED", reservationState)

	promotionTestFailBeforeAbortFinalize = nil
	aborted, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.NoError(t, err)
	require.Equal(t, "ABORTED", aborted.State)
	var secondAttempt string
	require.NoError(t, store.DB().QueryRow(`SELECT cleanup_attempt_id FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&secondAttempt))
	require.Equal(t, firstAttempt, secondAttempt)
}

func TestPromotionAbortStaleWriterAndCorruptAttemptFailClosed(t *testing.T) {
	t.Run("stale writer", func(t *testing.T) {
		store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-stale-writer")
		promotionTestFailBeforeAbortFinalize = func() error { return errors.New("stop after acceptance") }
		_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
		require.EqualError(t, err, "stop after acceptance")
		promotionTestFailBeforeAbortFinalize = nil
		t.Cleanup(func() { promotionTestFailBeforeAbortFinalize = nil })
		_, err = store.DB().Exec(`UPDATE promotion_namespace_capabilities SET writer_generation = 8 WHERE tenant_id = 'tenant-a'`)
		require.NoError(t, err)
		_, err = store.DB().Exec(`UPDATE promotion_import_identity_tenants SET installed_writer_generation = 8 WHERE tenant_id = 'tenant-a'`)
		require.NoError(t, err)
		_, err = promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
		require.ErrorIs(t, err, ErrPromotionRestoreFenced)
		var state, reservationState string
		require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_imports
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
			created.AllocationEpoch, created.AllocationSequence).Scan(&state))
		require.Equal(t, "ABORTING", state)
		require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
			WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID).Scan(&reservationState))
		require.Equal(t, "RESERVED", reservationState)
	})

	t.Run("missing tuple", func(t *testing.T) {
		store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-corrupt")
		promotionTestFailBeforeAbortFinalize = func() error { return errors.New("stop after acceptance") }
		_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
		require.EqualError(t, err, "stop after acceptance")
		promotionTestFailBeforeAbortFinalize = nil
		t.Cleanup(func() { promotionTestFailBeforeAbortFinalize = nil })
		_, err = store.DB().Exec(`UPDATE promotion_imports SET cleanup_attempt_id = NULL
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
			created.AllocationEpoch, created.AllocationSequence)
		require.NoError(t, err)
		_, err = promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
		require.ErrorIs(t, err, ErrPromotionRecoveryRequired)
	})
}

func TestPromotionAbortCorruptContentOwnershipFailsClosed(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-abort-corrupt-ownership")
	_, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(
		created, manifest[0], "abort-corrupt-content", bytes.NewBufferString("hello"),
	))
	require.NoError(t, err)
	_, err = store.DB().Exec(`UPDATE promotion_import_contents SET ownership_state = 'COMMITTED'
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence)
	require.NoError(t, err)

	_, err = promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
	require.ErrorIs(t, err, ErrPromotionRecoveryRequired)
	var state, reservationState, ownershipState string
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state))
	require.Equal(t, "ABORTING", state)
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "RESERVED", reservationState)
	require.NoError(t, store.DB().QueryRow(`SELECT ownership_state FROM promotion_import_contents
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&ownershipState))
	require.Equal(t, "COMMITTED", ownershipState)
}

func TestPromotionAbortCorruptManifestOrReservationFailsClosed(t *testing.T) {
	t.Run("missing metadata entry", func(t *testing.T) {
		store, promotionStore, created, _ := createPromotionImportForContent(t, []promotion.ManifestEntry{
			{RelativePath: "empty", Type: promotion.EntryTypeDirectory, Mode: 0o755},
		}, "allocate-abort-missing-entry")
		_, err := store.DB().Exec(`DELETE FROM promotion_import_entries
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
			created.AllocationEpoch, created.AllocationSequence)
		require.NoError(t, err)

		_, err = promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
		require.ErrorIs(t, err, ErrPromotionRecoveryRequired)
		assertPromotionAbortRemainsRecoverable(t, store, created)
	})

	t.Run("reservation file count mismatch", func(t *testing.T) {
		store, promotionStore, created, _ := createPromotionImportForContent(t, []promotion.ManifestEntry{
			{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
		}, "allocate-abort-reservation-files")
		_, err := store.DB().Exec(`UPDATE promotion_quota_reservations
			SET reserved_files = reserved_files + 1
			WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID)
		require.NoError(t, err)
		_, err = store.DB().Exec(`UPDATE promotion_quota_accounts
			SET reserved_files = reserved_files + 1 WHERE tenant_id = 'tenant-a'`)
		require.NoError(t, err)

		_, err = promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
		require.ErrorIs(t, err, ErrPromotionRecoveryRequired)
		assertPromotionAbortRemainsRecoverable(t, store, created)
	})
}

func assertPromotionAbortRemainsRecoverable(t *testing.T, store *Store, created *PromotionImport) {
	t.Helper()
	var state, reservationState string
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state))
	require.Equal(t, "ABORTING", state)
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "RESERVED", reservationState)
}

func TestPromotionConcurrentAbortCreatesOneCleanupAttempt(t *testing.T) {
	store, promotionStore, created, _ := createPromotionImportForContent(t, nil, "allocate-abort-concurrent")
	var wg sync.WaitGroup
	results := make(chan *PromotionImport, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(created))
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var digest string
	for result := range results {
		require.Equal(t, "ABORTED", result.State)
		if digest == "" {
			digest = result.TerminalResultDigest
		}
		require.Equal(t, digest, result.TerminalResultDigest)
	}
	var attempts, released int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(DISTINCT cleanup_attempt_id) FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&attempts))
	require.Equal(t, 1, attempts)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ? AND state = 'RELEASED'`,
		created.QuotaReservationID).Scan(&released))
	require.Equal(t, 1, released)
}

func TestPromotionCommitAndAbortHaveSingleWinners(t *testing.T) {
	t.Run("abort accepted first", func(t *testing.T) {
		store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-abort-wins-commit")
		accepted := make(chan struct{})
		release := make(chan struct{})
		promotionTestAfterAbortAccepted = func() {
			close(accepted)
			<-release
		}
		t.Cleanup(func() { promotionTestAfterAbortAccepted = nil })
		abortResult := make(chan *PromotionImport, 1)
		abortErr := make(chan error, 1)
		go func() {
			result, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(verified))
			abortResult <- result
			abortErr <- err
		}()
		<-accepted

		_, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
		require.ErrorIs(t, err, ErrPromotionConflict)
		close(release)
		require.NoError(t, <-abortErr)
		require.Equal(t, "ABORTED", (<-abortResult).State)
		var nodes int
		require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE '/published%'`).Scan(&nodes))
		require.Zero(t, nodes)
	})

	t.Run("commit accepted first", func(t *testing.T) {
		store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-wins-abort")
		accepted := make(chan struct{})
		release := make(chan struct{})
		promotionTestAfterCommitAccepted = func() {
			close(accepted)
			<-release
		}
		t.Cleanup(func() { promotionTestAfterCommitAccepted = nil })
		commitResult := make(chan *PromotionImport, 1)
		commitErr := make(chan error, 1)
		go func() {
			result, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
			commitResult <- result
			commitErr <- err
		}()
		<-accepted

		_, err := promotionStore.AbortImport(context.Background(), abortPromotionRequest(verified))
		require.ErrorIs(t, err, ErrPromotionConflict)
		close(release)
		require.NoError(t, <-commitErr)
		require.Equal(t, "COMMITTED", (<-commitResult).State)
		var nodes int
		require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE '/published%'`).Scan(&nodes))
		require.Positive(t, nodes)
	})
}
