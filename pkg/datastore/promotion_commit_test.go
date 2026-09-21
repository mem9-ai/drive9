package datastore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
	"github.com/stretchr/testify/require"
)

func createVerifiedPromotionImport(t *testing.T, entries []promotion.ManifestEntry, key string) (*Store, *PromotionStore, *PromotionImport, []promotion.ManifestEntry) {
	t.Helper()
	store, promotionStore, created, manifest := createPromotionImportForContent(t, entries, key)
	for i, entry := range manifest {
		if entry.Type != promotion.EntryTypeFile {
			continue
		}
		body := []byte(nil)
		if entry.ExpectedSizeBytes != 0 {
			body = []byte("hello")
		}
		_, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(
			created, entry, fmt.Sprintf("commit-content-%d-%s", i, entry.RelativePath), bytes.NewReader(body),
		))
		require.NoError(t, err)
	}
	verified, err := promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	})
	require.NoError(t, err)
	require.Equal(t, "VERIFIED", verified.State)
	return store, promotionStore, verified, manifest
}

func commitPromotionRequest(verified *PromotionImport) PromotionCommitRequest {
	return PromotionCommitRequest{
		TenantID: "tenant-a", MigrationID: verified.MigrationID, OwnerEpoch: verified.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease",
	}
}

func TestPromotionCommitPublishesWholeTreeAndRecoversExactResult(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, []promotion.ManifestEntry{
		{RelativePath: "dir", Type: promotion.EntryTypeDirectory, Mode: 0o750, MtimeNS: 1_001_000_000},
		{RelativePath: "dir/empty", Type: promotion.EntryTypeFile, Mode: 0o640, MtimeNS: 2_002_000_000, ExpectedChecksumSHA256: promotionTestHash("")},
		{RelativePath: "dir/file", Type: promotion.EntryTypeFile, Mode: 0o600, MtimeNS: 3_003_000_000, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
		{RelativePath: "link", Type: promotion.EntryTypeSymlink, Mode: 0o777, MtimeNS: 4_004_000_005, SymlinkTarget: "dir/file"},
	}, "allocate-commit-round-trip")

	var visibleBefore int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE '/published%'`).Scan(&visibleBefore))
	require.Zero(t, visibleBefore)

	committed, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	require.Equal(t, "COMMITTED", committed.State)
	require.NotEmpty(t, committed.TerminalResultBlob)
	require.Equal(t, promotionHashString(committed.TerminalResultBlob), committed.TerminalResultDigest)

	var nodeCount, inodeCount, contentCount, semanticCount int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE '/published%'`).Scan(&nodeCount))
	// root + directory + two regular files + symlink projection
	require.Equal(t, 5, nodeCount)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM inodes i
		JOIN file_nodes n ON n.inode_id = i.inode_id WHERE n.path LIKE '/published%'`).Scan(&inodeCount))
	require.Equal(t, 5, inodeCount)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM contents c
		JOIN file_nodes n ON n.inode_id = c.inode_id
		WHERE n.path IN ('/published/dir/empty', '/published/dir/file') AND c.storage_type = 'db9'`).Scan(&contentCount))
	require.Equal(t, 2, contentCount)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM semantic s
		JOIN file_nodes n ON n.inode_id = s.inode_id
		WHERE n.path IN ('/published/dir/empty', '/published/dir/file')`).Scan(&semanticCount))
	require.Equal(t, 2, semanticCount)
	root, err := store.Stat(context.Background(), "/published/")
	require.NoError(t, err)
	require.True(t, root.Node.IsDirectory)
	children, err := store.ListDir(context.Background(), "/published/")
	require.NoError(t, err)
	require.Len(t, children, 2)
	file, err := store.StatForRead(context.Background(), "/published/dir/file")
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), file.File.ContentBlob)
	require.Equal(t, uint32(0o600), file.Mode)
	require.True(t, file.HasMode)
	require.NotNil(t, file.File.ConfirmedAt)
	require.Equal(t, time.Unix(3, 3_000_000).UTC(), *file.File.ConfirmedAt)
	var directoryMode uint32
	var directoryMtime time.Time
	require.NoError(t, store.DB().QueryRow(`SELECT i.mode, n.created_at FROM file_nodes n
		JOIN inodes i ON n.inode_id = i.inode_id WHERE n.path = '/published/dir/'`).
		Scan(&directoryMode, &directoryMtime))
	require.Equal(t, uint32(0o750), directoryMode)
	require.Equal(t, time.Unix(1, 1_000_000).UTC(), directoryMtime.UTC())

	var symlinkTarget []byte
	require.NoError(t, store.DB().QueryRow(`SELECT s.target FROM jfs_symlink s
		JOIN file_nodes n ON n.extent_ino = s.inode WHERE n.path = '/published/link'`).Scan(&symlinkTarget))
	require.Equal(t, "dir/file", string(symlinkTarget))
	var symlinkMtimeMicros, symlinkMtimeNanoRemainder int64
	require.NoError(t, store.DB().QueryRow(`SELECT j.mtime, j.mtimensec FROM jfs_node j
		JOIN file_nodes n ON n.extent_ino = j.inode WHERE n.path = '/published/link'`).
		Scan(&symlinkMtimeMicros, &symlinkMtimeNanoRemainder))
	require.Equal(t, int64(4_004_000), symlinkMtimeMicros)
	require.Equal(t, int64(5), symlinkMtimeNanoRemainder)

	var reservedBytes, reservedFiles, committedBytes, committedFiles uint64
	require.NoError(t, store.DB().QueryRow(`SELECT reserved_bytes, reserved_files, committed_bytes, committed_files
		FROM promotion_quota_accounts WHERE tenant_id = 'tenant-a'`).
		Scan(&reservedBytes, &reservedFiles, &committedBytes, &committedFiles))
	require.Zero(t, reservedBytes)
	require.Zero(t, reservedFiles)
	require.Equal(t, uint64(5), committedBytes)
	require.Equal(t, uint64(2), committedFiles)

	var reservationState string
	var committedOwners, events int
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, verified.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "COMMITTED", reservationState)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents
		WHERE tenant_id = 'tenant-a' AND ownership_state = 'COMMITTED'`).Scan(&committedOwners))
	require.Equal(t, 2, committedOwners)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events
		WHERE path = '/' AND op = 'structural-reset' AND actor = ?
		  AND promotion_migration_id = ? AND promotion_target_path = '/published'
		  AND promotion_tree_generation = 1`, verified.MigrationID, verified.MigrationID).Scan(&events))
	require.Equal(t, 1, events)

	// Lost-response retry is a pure terminal read: no second tree, quota
	// settlement, content transfer, or structural reset.
	retried, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	require.Equal(t, committed.TerminalResultBlob, retried.TerminalResultBlob)
	require.Equal(t, committed.TerminalResultDigest, retried.TerminalResultDigest)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events
		WHERE path = '/' AND op = 'structural-reset' AND actor = ?
		  AND promotion_migration_id = ?`, verified.MigrationID, verified.MigrationID).Scan(&events))
	require.Equal(t, 1, events)

	_, err = store.DB().Exec(`UPDATE promotion_namespace_capabilities SET writer_generation = 8 WHERE tenant_id = 'tenant-a'`)
	require.NoError(t, err)
	_, err = store.DB().Exec(`UPDATE promotion_import_identity_tenants SET installed_writer_generation = 8 WHERE tenant_id = 'tenant-a'`)
	require.NoError(t, err)
	rolloutRetry, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	require.Equal(t, committed.TerminalResultDigest, rolloutRetry.TerminalResultDigest)
}

func TestPromotionCommitMetadataOnlyTreeSettlesZeroQuota(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-empty")
	committed, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	require.Equal(t, "COMMITTED", committed.State)

	var path string
	require.NoError(t, store.DB().QueryRow(`SELECT path FROM file_nodes WHERE parent_path = '/' AND name = 'published'`).Scan(&path))
	require.Equal(t, "/published/", path)
	var reserved, committedQuota uint64
	require.NoError(t, store.DB().QueryRow(`SELECT reserved_bytes + reserved_files,
		committed_bytes + committed_files FROM promotion_quota_accounts WHERE tenant_id = 'tenant-a'`).Scan(&reserved, &committedQuota))
	require.Zero(t, reserved)
	require.Zero(t, committedQuota)
}

func TestPromotionCommitPublishesUnderNonRootParentAndAdvancesItsGeneration(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	require.NoError(t, store.InsertInode(context.Background(), &Inode{
		InodeID: "parent-inode", Mode: 0o755, Revision: 1, Status: StatusConfirmed,
		CreatedAt: now, Mtime: now, ConfirmedAt: &now,
	}))
	require.NoError(t, store.InsertNode(context.Background(), &FileNode{
		NodeID: "parent-node", Path: "/parent/", ParentPath: "/", Name: "parent",
		IsDirectory: true, InodeID: "parent-inode", CreatedAt: now,
	}))
	_, err := store.DB().Exec(`UPDATE file_nodes
		SET path_edge_incarnation = 'parent-edge', children_generation = 17
		WHERE path_hash = ? AND path = '/parent/'`, fileNodePathHash("/parent/"))
	require.NoError(t, err)

	manifest, err := promotion.CanonicalizeManifest(nil, testPromotionLimits())
	require.NoError(t, err)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/parent/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries,
	})
	require.NoError(t, err)
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/parent/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "allocate-commit-nested", AllocationLease: "allocation-lease",
	})
	require.NoError(t, err)
	created, err := promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/parent/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	})
	require.NoError(t, err)
	verified, err := promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	})
	require.NoError(t, err)

	committed, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	require.Equal(t, "COMMITTED", committed.State)
	var generation uint64
	require.NoError(t, store.DB().QueryRow(`SELECT children_generation FROM file_nodes
		WHERE path_hash = ? AND path = '/parent/'`, fileNodePathHash("/parent/")).Scan(&generation))
	require.Equal(t, uint64(18), generation)
	var rootPath, eventPath string
	require.NoError(t, store.DB().QueryRow(`SELECT path FROM file_nodes
		WHERE parent_path = '/parent/' AND name = 'published'`).Scan(&rootPath))
	require.Equal(t, "/parent/published/", rootPath)
	require.NoError(t, store.DB().QueryRow(`SELECT path FROM fs_events
		WHERE promotion_migration_id = ?`, verified.MigrationID).Scan(&eventPath))
	require.Equal(t, "/parent/", eventPath)
}

func TestPromotionCreateTreatsCanonicalDirectorySpellingAsOccupiedTarget(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	require.NoError(t, store.InsertInode(context.Background(), &Inode{
		InodeID: "existing-dir", Mode: 0o755, Revision: 1, Status: StatusConfirmed,
		CreatedAt: now, Mtime: now, ConfirmedAt: &now,
	}))
	require.NoError(t, store.InsertNode(context.Background(), &FileNode{
		NodeID: "existing-node", Path: "/published/", ParentPath: "/", Name: "published",
		IsDirectory: true, InodeID: "existing-dir", CreatedAt: now,
	}))
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
	})
	require.NoError(t, err)
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "occupied-directory", AllocationLease: "allocation-lease",
	})
	require.NoError(t, err)
	_, err = promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	})
	require.ErrorIs(t, err, ErrPathConflict)
	var imports, reservations int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_imports`).Scan(&imports))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_quota_reservations`).Scan(&reservations))
	require.Zero(t, imports)
	require.Zero(t, reservations)
}

func TestPromotionCommitTargetGenerationChangeAtomicallyClaimsCleanup(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-target-change")
	_, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
		SET root_children_generation = root_children_generation + 1 WHERE tenant_id = 'tenant-a'`)
	require.NoError(t, err)

	_, err = promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.ErrorIs(t, err, ErrPromotionTargetChanged)
	var state string
	var cleanupID, reason sql.NullString
	var cleanupGeneration sql.NullInt64
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id, cleanup_writer_generation, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		verified.AllocationEpoch, verified.AllocationSequence).Scan(&state, &cleanupID, &cleanupGeneration, &reason))
	require.Equal(t, "ABORTING", state)
	require.True(t, cleanupID.Valid)
	require.Equal(t, int64(7), cleanupGeneration.Int64)
	require.Equal(t, "target_precondition_changed", reason.String)
	var nodes, events int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE '/published%'`).Scan(&nodes))
	require.Zero(t, nodes)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events WHERE actor = ?`, verified.MigrationID).Scan(&events))
	require.Zero(t, events)
	var reservationState string
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations WHERE reservation_id = ?`, verified.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "RESERVED", reservationState)
}

func TestPromotionCommitNamespaceEpochChangeAfterAcceptancePublishesNothing(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-namespace-epoch")
	promotionTestAfterCommitAccepted = func() {
		_, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
			SET namespace_cas_epoch = namespace_cas_epoch + 1 WHERE tenant_id = 'tenant-a'`)
		require.NoError(t, err)
	}
	t.Cleanup(func() { promotionTestAfterCommitAccepted = nil })

	_, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.ErrorIs(t, err, ErrPromotionTargetChanged)

	var state string
	var cleanupID, reason sql.NullString
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		verified.AllocationEpoch, verified.AllocationSequence).Scan(&state, &cleanupID, &reason))
	require.Equal(t, "ABORTING", state)
	require.True(t, cleanupID.Valid)
	require.Equal(t, "target_precondition_changed", reason.String)

	var roots, events int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path = '/published/'`).Scan(&roots))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events WHERE promotion_migration_id = ?`, verified.MigrationID).Scan(&events))
	require.Zero(t, roots)
	require.Zero(t, events)
	var reservationState string
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
		WHERE reservation_id = ?`, verified.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "RESERVED", reservationState)
}

func TestPromotionCommitTargetCreatedAfterAcceptancePublishesNothing(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-target-created")
	promotionTestAfterCommitAccepted = func() {
		now := time.Now().UTC().Truncate(time.Millisecond)
		require.NoError(t, store.InsertInode(context.Background(), &Inode{
			InodeID: "racing-target-inode", Mode: 0o644, Revision: 1, Status: StatusConfirmed,
			CreatedAt: now, Mtime: now, ConfirmedAt: &now,
		}))
		require.NoError(t, store.InsertNode(context.Background(), &FileNode{
			NodeID: "racing-target-node", Path: "/published", ParentPath: "/", Name: "published",
			InodeID: "racing-target-inode", CreatedAt: now,
		}))
	}
	t.Cleanup(func() { promotionTestAfterCommitAccepted = nil })

	_, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.ErrorIs(t, err, ErrPromotionTargetChanged)
	var state, reason string
	require.NoError(t, store.DB().QueryRow(`SELECT state, terminal_reason FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		verified.AllocationEpoch, verified.AllocationSequence).Scan(&state, &reason))
	require.Equal(t, "ABORTING", state)
	require.Equal(t, "target_precondition_changed", reason)
	var publishedRoot, events int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path = '/published/'`).Scan(&publishedRoot))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events WHERE promotion_migration_id = ?`, verified.MigrationID).Scan(&events))
	require.Zero(t, publishedRoot)
	require.Zero(t, events)
}

func TestPromotionCommitCorruptVerifiedManifestAtomicallyClaimsCleanup(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, []promotion.ManifestEntry{{
		RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644,
		ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello"),
	}}, "allocate-commit-corrupt-manifest")
	_, err := store.DB().Exec(`UPDATE promotion_import_contents SET checksum_sha256 = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		promotionTestHash("tampered"), verified.AllocationEpoch, verified.AllocationSequence)
	require.NoError(t, err)

	_, err = promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.ErrorIs(t, err, ErrPromotionRecoveryRequired)
	var state, cleanupID, reason string
	require.NoError(t, store.DB().QueryRow(`SELECT state, cleanup_attempt_id, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		verified.AllocationEpoch, verified.AllocationSequence).Scan(&state, &cleanupID, &reason))
	require.Equal(t, "ABORTING", state)
	require.NotEmpty(t, cleanupID)
	require.Equal(t, "manifest_invalid", reason)
	var roots, events int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path = '/published/'`).Scan(&roots))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events WHERE promotion_migration_id = ?`, verified.MigrationID).Scan(&events))
	require.Zero(t, roots)
	require.Zero(t, events)
}

func TestPromotionCommitTransactionFailurePublishesNothing(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-commit-rollback")
	promotionTestFailBeforeCommitFinalize = func() error { return errors.New("injected finalization failure") }
	t.Cleanup(func() { promotionTestFailBeforeCommitFinalize = nil })

	_, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.EqualError(t, err, "injected finalization failure")
	var state, reservationState, ownershipState string
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		verified.AllocationEpoch, verified.AllocationSequence).Scan(&state))
	require.Equal(t, "COMMITTING", state)
	require.NoError(t, store.DB().QueryRow(`SELECT state FROM promotion_quota_reservations
		WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, verified.QuotaReservationID).Scan(&reservationState))
	require.Equal(t, "RESERVED", reservationState)
	require.NoError(t, store.DB().QueryRow(`SELECT ownership_state FROM promotion_import_contents
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		verified.AllocationEpoch, verified.AllocationSequence).Scan(&ownershipState))
	require.Equal(t, "STAGED", ownershipState)
	var nodes, events int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE '/published%'`).Scan(&nodes))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events WHERE promotion_migration_id = ?`, verified.MigrationID).Scan(&events))
	require.Zero(t, nodes)
	require.Zero(t, events)

	promotionTestFailBeforeCommitFinalize = nil
	committed, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	require.Equal(t, "COMMITTED", committed.State)
}

func TestPromotionCommitDeadlineAndAcceptedAttemptHaveSingleWinners(t *testing.T) {
	t.Run("deadline wins before acceptance", func(t *testing.T) {
		store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-deadline")
		_, err := store.DB().Exec(`UPDATE promotion_imports SET activity_deadline = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`, verified.AllocationEpoch, verified.AllocationSequence)
		require.NoError(t, err)
		_, err = promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
		require.ErrorIs(t, err, ErrPromotionDeadlineExceeded)
		var state, reason string
		require.NoError(t, store.DB().QueryRow(`SELECT state, terminal_reason FROM promotion_imports
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`, verified.AllocationEpoch, verified.AllocationSequence).Scan(&state, &reason))
		require.Equal(t, "ABORTING", state)
		require.Equal(t, "activity_deadline_exceeded", reason)
	})

	t.Run("accepted commit ignores later deadline", func(t *testing.T) {
		store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-accepted")
		_, err := store.DB().Exec(`UPDATE promotion_imports
			SET state = 'COMMITTING', state_version = state_version + 1,
			    commit_attempt_id = 'pco_test', commit_writer_generation = 7,
			    activity_deadline = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ? AND state = 'VERIFIED'`,
			verified.AllocationEpoch, verified.AllocationSequence)
		require.NoError(t, err)
		committed, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
		require.NoError(t, err)
		require.Equal(t, "COMMITTED", committed.State)
		var reason sql.NullString
		require.NoError(t, store.DB().QueryRow(`SELECT terminal_reason FROM promotion_imports
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`, verified.AllocationEpoch, verified.AllocationSequence).Scan(&reason))
		require.False(t, reason.Valid)
	})
}

func TestPromotionCommitStaleWriterHasZeroMutation(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-stale-writer")
	promotionTestAfterCommitAccepted = func() {
		_, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
			SET writer_generation = 8 WHERE tenant_id = 'tenant-a'`)
		require.NoError(t, err)
		_, err = store.DB().Exec(`UPDATE promotion_import_identity_tenants
			SET installed_writer_generation = 8 WHERE tenant_id = 'tenant-a'`)
		require.NoError(t, err)
	}
	t.Cleanup(func() { promotionTestAfterCommitAccepted = nil })

	_, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.True(t, errors.Is(err, ErrPromotionRestoreFenced), "error = %v", err)
	var state string
	var version uint64
	var attemptID string
	require.NoError(t, store.DB().QueryRow(`SELECT state, state_version, commit_attempt_id FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`, verified.AllocationEpoch, verified.AllocationSequence).Scan(&state, &version, &attemptID))
	require.Equal(t, "COMMITTING", state)
	require.Equal(t, verified.StateVersion+1, version)
	require.NotEmpty(t, attemptID)
	var nodes, events int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path LIKE '/published%'`).Scan(&nodes))
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events WHERE actor = ?`, verified.MigrationID).Scan(&events))
	require.Zero(t, nodes)
	require.Zero(t, events)
}

func TestPromotionCommitCancellationAfterAcceptanceDoesNotOwnWorker(t *testing.T) {
	_, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	promotionTestAfterCommitAccepted = cancel
	t.Cleanup(func() { promotionTestAfterCommitAccepted = nil })
	result, err := promotionStore.CommitImport(ctx, commitPromotionRequest(verified))
	require.NoError(t, err)
	require.Equal(t, "COMMITTED", result.State)
}

func TestPromotionCommitTerminalRetryRequiresExactOwnerTuple(t *testing.T) {
	_, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-owner")
	_, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
	require.NoError(t, err)
	bad := commitPromotionRequest(verified)
	bad.OwnerToken = "different"
	_, err = promotionStore.CommitImport(context.Background(), bad)
	require.ErrorIs(t, err, ErrPromotionConflict)
}

func TestPromotionConcurrentCommitHasOneAttemptTreeQuotaAndOutbox(t *testing.T) {
	store, promotionStore, verified, _ := createVerifiedPromotionImport(t, nil, "allocate-commit-race")
	type result struct {
		value *PromotionImport
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			value, err := promotionStore.CommitImport(context.Background(), commitPromotionRequest(verified))
			results <- result{value: value, err: err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, first.value.TerminalResultDigest, second.value.TerminalResultDigest)

	var roots, events int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE path = '/published/'`).Scan(&roots))
	require.Equal(t, 1, roots)
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM fs_events WHERE promotion_migration_id = ?`, verified.MigrationID).Scan(&events))
	require.Equal(t, 1, events)
	var attempt string
	require.NoError(t, store.DB().QueryRow(`SELECT commit_attempt_id FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		verified.AllocationEpoch, verified.AllocationSequence).Scan(&attempt))
	require.NotEmpty(t, attempt)
}
