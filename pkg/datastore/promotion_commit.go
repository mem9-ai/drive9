package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

// PromotionCommitRequest accepts a verified import and synchronously drives
// its server-owned commit attempt to a durable result.
type PromotionCommitRequest struct {
	TenantID    string
	MigrationID string
	OwnerEpoch  uint64
	OwnerToken  string
	WriterLease string
}

type promotionCommitResult struct {
	Version        string `json:"version"`
	MigrationID    string `json:"migration_id"`
	Target         string `json:"target"`
	State          string `json:"state"`
	RootInode      string `json:"root_inode"`
	TreeGeneration uint64 `json:"tree_generation"`
}

var promotionTestFailBeforeCommitFinalize func() error
var promotionTestAfterCommitAccepted func()

// CommitImport first durably accepts VERIFIED -> COMMITTING and then performs
// one namespace-linearization transaction. Once acceptance commits, request
// cancellation no longer owns the work; a response-loss retry resumes the same
// durable attempt or returns the immutable terminal result.
func (s *PromotionStore) CommitImport(ctx context.Context, req PromotionCommitRequest) (*PromotionImport, error) {
	if req.TenantID == "" || req.OwnerEpoch == 0 || req.OwnerToken == "" || len(req.OwnerToken) > 512 || req.WriterLease == "" {
		return nil, ErrPromotionConflict
	}
	identity, err := promotion.DecodeMigrationID(req.MigrationID)
	if err != nil {
		return nil, ErrNotFound
	}
	target, err := s.authorizePromotionMutationTarget(ctx, req.TenantID, identity)
	if err != nil {
		return nil, err
	}
	if recovered, found, err := s.readPromotionCommitTerminal(ctx, req, identity, target); err != nil {
		return nil, err
	} else if found {
		return recovered, nil
	}

	deadlineWon := false
	terminalWon := false
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if row.target != target || row.ownerEpoch != req.OwnerEpoch ||
			!promotionHashEqual(row.ownerTokenHash, promotionHashString(req.OwnerToken)) {
			return ErrPromotionConflict
		}
		if row.state == "COMMITTED" || row.state == "ABORTED" {
			terminalWon = true
			return nil
		}
		if err := validatePromotionOwnerMutation(row, tenant, namespace, req.OwnerEpoch, req.OwnerToken); err != nil {
			return err
		}
		if row.state == "ABORTING" && row.terminalReason.Valid && row.terminalReason.String == "target_precondition_changed" {
			return ErrPromotionTargetChanged
		}
		if row.state == "COMMITTING" {
			if !row.commitAttemptID.Valid || row.commitAttemptID.String == "" ||
				!row.commitWriterGeneration.Valid || row.commitWriterGeneration.Int64 < 0 ||
				uint64(row.commitWriterGeneration.Int64) != namespace.writerGeneration {
				return ErrPromotionRecoveryRequired
			}
			return nil
		}
		if row.state != "VERIFIED" {
			return ErrPromotionConflict
		}
		lockedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if !lockedAt.Before(row.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, row, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, lockedAt); err != nil {
			return err
		}
		if !lockedAt.Before(row.leaseExpiresAt) {
			return ErrPromotionConflict
		}
		attemptID, err := newPromotionID("pco")
		if err != nil {
			return err
		}
		accepted, err := tx.ExecContext(ctx, `UPDATE promotion_imports
			SET state = 'COMMITTING', state_version = state_version + 1,
			    commit_attempt_id = ?, commit_writer_generation = ?
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND state = 'VERIFIED' AND state_version = ?`,
			attemptID, namespace.writerGeneration, req.TenantID, identity.AllocationEpoch,
			identity.AllocationSequence, row.stateVersion)
		if err != nil {
			return fmt.Errorf("accept promotion commit: %w", err)
		}
		if affected, err := accepted.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if deadlineWon {
		return nil, ErrPromotionDeadlineExceeded
	}
	if terminalWon {
		recovered, found, err := s.readPromotionCommitTerminal(context.WithoutCancel(ctx), req, identity, target)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrPromotionRecoveryRequired
		}
		return recovered, nil
	}
	if promotionTestAfterCommitAccepted != nil {
		promotionTestAfterCommitAccepted()
	}

	// The acceptance transaction, not the caller's context, owns the work.
	workerCtx := context.WithoutCancel(ctx)
	if err := s.finishPromotionCommit(workerCtx, req, identity, target); err != nil {
		// A concurrent worker may have committed before this worker acquired the
		// restore rows. Terminal recovery outranks current rollout admission.
		if recovered, found, recoveryErr := s.readPromotionCommitTerminal(workerCtx, req, identity, target); recoveryErr != nil {
			return nil, recoveryErr
		} else if found {
			return recovered, nil
		}
		return nil, err
	}
	recovered, found, err := s.readPromotionCommitTerminal(workerCtx, req, identity, target)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrPromotionRecoveryRequired
	}
	return recovered, nil
}

func (s *PromotionStore) readPromotionCommitTerminal(ctx context.Context, req PromotionCommitRequest, identity promotion.MigrationIdentity, target string) (*PromotionImport, bool, error) {
	var out PromotionImport
	var ownerHash string
	var blob, digest sql.NullString
	err := s.store.db.QueryRowContext(ctx, `SELECT target_path, manifest_hash, quota_reservation_id, state,
		state_version, owner_epoch, activity_deadline, lease_expires_at,
		accepted_restore_generation, accepted_database_incarnation, accepted_writer_generation,
		owner_token_hash, terminal_result_blob, terminal_result_digest
		FROM promotion_imports
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		req.TenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&out.Target, &out.ManifestHash, &out.QuotaReservationID, &out.State,
			&out.StateVersion, &out.OwnerEpoch, &out.ActivityDeadline, &out.LeaseExpiresAt,
			&out.RestoreGeneration, &out.DatabaseIncarnation, &out.WriterGeneration,
			&ownerHash, &blob, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, ErrNotFound
	}
	if err != nil {
		return nil, false, fmt.Errorf("read promotion commit result: %w", err)
	}
	if out.Target != target || out.OwnerEpoch != req.OwnerEpoch || !promotionHashEqual(ownerHash, promotionHashString(req.OwnerToken)) {
		return nil, false, ErrPromotionConflict
	}
	if out.State != "COMMITTED" && out.State != "ABORTED" {
		return nil, false, nil
	}
	if err := validatePromotionTerminalResult(out.State, blob, digest); err != nil {
		return nil, false, err
	}
	out.MigrationID = req.MigrationID
	out.AllocationEpoch = identity.AllocationEpoch
	out.AllocationSequence = identity.AllocationSequence
	out.TerminalResultBlob = blob.String
	out.TerminalResultDigest = digest.String
	return &out, true, nil
}

func (s *PromotionStore) finishPromotionCommit(ctx context.Context, req PromotionCommitRequest, identity promotion.MigrationIdentity, target string) error {
	var commitFailure error
	err := s.store.InTx(ctx, func(tx *sql.Tx) error {
		// Lock order is restore identity -> namespace capability -> import.
		tenant, err := s.lockPromotionIdentityTenant(ctx, tx, req.TenantID)
		if err != nil {
			return err
		}
		namespace, err := s.lockPromotionNamespaceCapability(ctx, tx, req.TenantID)
		if err != nil {
			return err
		}
		if err := s.requirePromotionWriterProtocol(namespace); err != nil {
			return err
		}
		now, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, now); err != nil {
			return err
		}
		row, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if row.state == "COMMITTED" {
			return nil
		}
		if row.state == "ABORTING" && row.terminalReason.Valid && row.terminalReason.String == "target_precondition_changed" {
			return ErrPromotionTargetChanged
		}
		if row.target != target || row.state != "COMMITTING" ||
			!row.commitAttemptID.Valid || row.commitAttemptID.String == "" ||
			!row.commitWriterGeneration.Valid || row.commitWriterGeneration.Int64 < 0 ||
			uint64(row.commitWriterGeneration.Int64) != namespace.writerGeneration ||
			row.acceptedRestoreGeneration != tenant.RestoreGeneration ||
			row.acceptedRestoreGeneration != namespace.restoreGeneration ||
			row.acceptedDatabaseIncarnation != tenant.DatabaseIncarnation ||
			row.acceptedDatabaseIncarnation != namespace.databaseIncarnation ||
			row.acceptedWriterGeneration != tenant.WriterGeneration ||
			row.acceptedWriterGeneration != namespace.writerGeneration {
			return ErrPromotionRestoreFenced
		}

		parent, parentOK, err := s.lockPromotionCommitParent(ctx, tx, target, namespace, row)
		if err != nil {
			return err
		}
		if !namespace.ready || namespace.epoch != row.namespaceCASEpoch || !parentOK {
			if err := s.transitionPromotionCommitFailureTx(ctx, tx, req.TenantID, row, namespace.writerGeneration, "target_precondition_changed"); err != nil {
				return err
			}
			commitFailure = ErrPromotionTargetChanged
			return nil
		}
		present, err := promotionTargetChildPresentTx(ctx, tx, parent.path, pathutil.BaseName(target))
		if err != nil {
			return err
		}
		if present {
			if err := s.transitionPromotionCommitFailureTx(ctx, tx, req.TenantID, row, namespace.writerGeneration, "target_precondition_changed"); err != nil {
				return err
			}
			commitFailure = ErrPromotionTargetChanged
			return nil
		}

		if err := validatePromotionReservationTx(ctx, tx, req.TenantID, row); err != nil {
			return err
		}
		entries, err := selectPromotionEntriesTx(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		contents, err := selectPromotionContentsTx(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if err := validatePromotionCompleteManifest(row, entries, contents); err != nil {
			if transitionErr := s.transitionPromotionCommitFailureTx(ctx, tx, req.TenantID, row, namespace.writerGeneration, "manifest_invalid"); transitionErr != nil {
				return transitionErr
			}
			commitFailure = ErrPromotionRecoveryRequired
			return nil
		}

		rootInode, treeGeneration, err := s.materializePromotionTreeTx(ctx, tx, req, row, entries, contents, now)
		if err != nil {
			return err
		}
		if err := s.settlePromotionQuotaTx(ctx, tx, req.TenantID, row); err != nil {
			return err
		}
		transferred, err := tx.ExecContext(ctx, `UPDATE promotion_import_contents
			SET ownership_state = 'COMMITTED'
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND ownership_state = 'STAGED'`, req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
		if err != nil {
			return fmt.Errorf("transfer promotion content ownership: %w", err)
		}
		if affected, err := transferred.RowsAffected(); err != nil || uint64(affected) != uint64(len(contents)) {
			return ErrPromotionRecoveryRequired
		}
		if err := s.advancePromotionParentGenerationTx(ctx, tx, req.TenantID, parent, namespace); err != nil {
			return err
		}
		if promotionTestFailBeforeCommitFinalize != nil {
			if err := promotionTestFailBeforeCommitFinalize(); err != nil {
				return err
			}
		}
		finishedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, finishedAt); err != nil {
			return err
		}
		resultRaw, err := json.Marshal(promotionCommitResult{
			Version: "p1", MigrationID: req.MigrationID, Target: target, State: "COMMITTED",
			RootInode: rootInode, TreeGeneration: treeGeneration,
		})
		if err != nil {
			return err
		}
		resultBlob := string(resultRaw)
		resultDigest := promotionHashString(resultBlob)
		committed, err := tx.ExecContext(ctx, `UPDATE promotion_imports
			SET state = 'COMMITTED', state_version = state_version + 1,
			    committed_root_inode = ?, committed_generation = ?,
			    terminal_result_blob = ?, terminal_result_digest = ?, terminal_at = ?
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND state = 'COMMITTING' AND state_version = ? AND commit_attempt_id = ?`,
			rootInode, treeGeneration, resultBlob, resultDigest, finishedAt,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence,
			row.stateVersion, row.commitAttemptID.String)
		if err != nil {
			return fmt.Errorf("finish promotion commit: %w", err)
		}
		if affected, err := committed.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fs_events
			(path, op, actor, ts, promotion_migration_id, promotion_target_path, promotion_tree_generation)
			VALUES (?, 'structural-reset', ?, ?, ?, ?, ?)`,
			parent.path, req.MigrationID, finishedAt.UnixNano(), req.MigrationID, target, treeGeneration); err != nil {
			return fmt.Errorf("insert promotion structural reset: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if commitFailure != nil {
		return commitFailure
	}
	return nil
}

func (s *PromotionStore) lockPromotionCommitParent(ctx context.Context, tx *sql.Tx, target string, namespace promotionNamespaceCapability, row *promotionOwnerImport) (promotionTargetParent, bool, error) {
	parent, err := s.resolvePromotionTargetParentTx(ctx, tx, target, namespace)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrPromotionDisabled) {
			return promotionTargetParent{}, false, nil
		}
		return promotionTargetParent{}, false, err
	}
	ok := parent.inode == row.targetParentInode && parent.path == row.targetParentPath &&
		parent.edgeIncarnation == row.targetParentEdgeIncarnation &&
		parent.childrenGeneration == row.targetParentChildrenGeneration
	return parent, ok, nil
}

func promotionTargetChildPresentTx(ctx context.Context, tx *sql.Tx, parentPath, childName string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM file_nodes
		WHERE parent_path_hash = ? AND parent_path = ? AND name = ? LIMIT 1 FOR UPDATE`,
		fileNodePathHash(parentPath), parentPath, childName).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock promotion target child absence: %w", err)
	}
	return true, nil
}

func (s *PromotionStore) transitionPromotionCommitFailureTx(ctx context.Context, tx *sql.Tx, tenantID string, row *promotionOwnerImport, writerGeneration uint64, reason string) error {
	attemptID, err := newPromotionID("pca")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE promotion_imports
		SET state = 'ABORTING', state_version = state_version + 1,
		    cleanup_attempt_id = ?, cleanup_writer_generation = ?, terminal_reason = ?
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
		  AND state = 'COMMITTING' AND state_version = ? AND commit_attempt_id = ?`,
		attemptID, writerGeneration, reason, tenantID, row.identity.AllocationEpoch,
		row.identity.AllocationSequence, row.stateVersion, row.commitAttemptID.String)
	if err != nil {
		return fmt.Errorf("claim failed promotion commit cleanup: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionConflict
	}
	return nil
}

func (s *PromotionStore) settlePromotionQuotaTx(ctx context.Context, tx *sql.Tx, tenantID string, row *promotionOwnerImport) error {
	var maxBytes, maxFiles, reservedBytes, reservedFiles, committedBytes, committedFiles uint64
	if err := tx.QueryRowContext(ctx, `SELECT max_bytes, max_files, reserved_bytes, reserved_files,
		committed_bytes, committed_files FROM promotion_quota_accounts
		WHERE tenant_id = ? FOR UPDATE`, tenantID).
		Scan(&maxBytes, &maxFiles, &reservedBytes, &reservedFiles, &committedBytes, &committedFiles); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPromotionRecoveryRequired
		}
		return fmt.Errorf("lock promotion quota settlement: %w", err)
	}
	var reservationBytes, reservationFiles uint64
	var reservationState string
	var reservationVersion uint64
	if err := tx.QueryRowContext(ctx, `SELECT reserved_bytes, reserved_files, state, state_version
		FROM promotion_quota_reservations WHERE tenant_id = ? AND reservation_id = ?
		  AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, row.quotaReservationID, row.identity.AllocationEpoch, row.identity.AllocationSequence).
		Scan(&reservationBytes, &reservationFiles, &reservationState, &reservationVersion); err != nil {
		return ErrPromotionRecoveryRequired
	}
	if reservationState != "RESERVED" || reservationBytes != row.byteTotal ||
		reservedBytes < reservationBytes || reservedFiles < reservationFiles ||
		committedBytes > maxBytes || reservationBytes > maxBytes-committedBytes ||
		committedFiles > maxFiles || reservationFiles > maxFiles-committedFiles {
		return ErrPromotionRecoveryRequired
	}
	if _, err := tx.ExecContext(ctx, `UPDATE promotion_quota_accounts
		SET reserved_bytes = reserved_bytes - ?, reserved_files = reserved_files - ?,
		    committed_bytes = committed_bytes + ?, committed_files = committed_files + ?
		WHERE tenant_id = ?`, reservationBytes, reservationFiles, reservationBytes, reservationFiles, tenantID); err != nil {
		return fmt.Errorf("settle promotion quota account: %w", err)
	}
	settled, err := tx.ExecContext(ctx, `UPDATE promotion_quota_reservations
		SET state = 'COMMITTED', state_version = state_version + 1
		WHERE tenant_id = ? AND reservation_id = ? AND state = 'RESERVED' AND state_version = ?`,
		tenantID, row.quotaReservationID, reservationVersion)
	if err != nil {
		return fmt.Errorf("settle promotion quota reservation: %w", err)
	}
	if affected, err := settled.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionRecoveryRequired
	}
	return nil
}

func (s *PromotionStore) materializePromotionTreeTx(ctx context.Context, tx *sql.Tx, req PromotionCommitRequest, row *promotionOwnerImport, entries []promotionEntryRow, contents []promotionContentRow, now time.Time) (string, uint64, error) {
	rootPath := strings.TrimSuffix(row.target, "/") + "/"
	rootInode, err := newPromotionID("pin")
	if err != nil {
		return "", 0, err
	}
	rootNode, err := newPromotionID("pnd")
	if err != nil {
		return "", 0, err
	}
	rootEdge, err := newPromotionID("ped")
	if err != nil {
		return "", 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO inodes
		(inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at)
		VALUES (?, 0, 1, ?, 'CONFIRMED', ?, ?, ?)`, rootInode, uint32(0o755), now, now, now); err != nil {
		return "", 0, fmt.Errorf("insert promotion root inode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO file_nodes
		(node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory,
		 inode_id, path_edge_incarnation, children_generation, created_at, content_layout)
		VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?, 1, ?, '')`, rootNode, rootPath,
		fileNodePathHash(rootPath), row.targetParentPath, fileNodePathHash(row.targetParentPath),
		pathutil.BaseName(row.target), rootInode, rootEdge, now); err != nil {
		return "", 0, fmt.Errorf("insert promotion root dentry: %w", err)
	}

	contentByID := make(map[string]promotionContentRow, len(contents))
	for _, content := range contents {
		contentByID[content.contentID] = content
	}
	for _, entry := range entries {
		fullPath := rootPath + entry.relativePath
		mtime := time.Unix(0, entry.mtimeNS).UTC()
		if entry.entryType == promotion.EntryTypeDirectory {
			fullPath += "/"
			inodeID, err := newPromotionID("pin")
			if err != nil {
				return "", 0, err
			}
			nodeID, err := newPromotionID("pnd")
			if err != nil {
				return "", 0, err
			}
			edgeID, err := newPromotionID("ped")
			if err != nil {
				return "", 0, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO inodes
				(inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at)
				VALUES (?, 0, 1, ?, 'CONFIRMED', ?, ?, ?)`, inodeID, entry.mode, now, mtime, mtime); err != nil {
				return "", 0, fmt.Errorf("insert promotion directory inode %q: %w", entry.relativePath, err)
			}
			parentPath := promotionPublishedParentPath(rootPath, entry.relativePath)
			if _, err := tx.ExecContext(ctx, `INSERT INTO file_nodes
				(node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory,
				 inode_id, path_edge_incarnation, children_generation, created_at, content_layout)
				VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?, 0, ?, '')`, nodeID, fullPath,
				fileNodePathHash(fullPath), parentPath, fileNodePathHash(parentPath),
				pathutil.BaseName(fullPath), inodeID, edgeID, mtime); err != nil {
				return "", 0, fmt.Errorf("insert promotion directory %q: %w", entry.relativePath, err)
			}
			continue
		}
		if entry.entryType == promotion.EntryTypeSymlink {
			parentIno, err := s.store.jfsParentInoForPathTx(tx, fullPath)
			if err != nil {
				return "", 0, fmt.Errorf("resolve promotion symlink parent %q: %w", entry.relativePath, err)
			}
			ino, _, eno, err := s.store.jfsMknodTx(ctx, tx, parentIno, pathutil.BaseName(fullPath), jfsTypeSymlink,
				uint16(entry.mode), 0, 0, ExtentAttr{}, fullPath, 0, 0, entry.symlinkTarget.String)
			if err != nil || eno != 0 {
				return "", 0, fmt.Errorf("insert promotion symlink %q (ino=%d errno=%d): %w", entry.relativePath, ino, eno, err)
			}
			mtimeSeconds, mtimeNanos := jfsSplitTime(entry.mtimeNS)
			if _, err := tx.ExecContext(ctx, `UPDATE jfs_node SET mtime = ?, mtimensec = ? WHERE inode = ?`,
				mtimeSeconds, mtimeNanos, ino); err != nil {
				return "", 0, fmt.Errorf("set promotion symlink mtime %q: %w", entry.relativePath, err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE inodes SET mtime = ?, confirmed_at = ?
				WHERE inode_id = (SELECT inode_id FROM file_nodes WHERE path_hash = ? AND path = ?)`,
				mtime, mtime, fileNodePathHash(fullPath), fullPath); err != nil {
				return "", 0, fmt.Errorf("set promotion symlink projection mtime %q: %w", entry.relativePath, err)
			}
			edgeID, err := newPromotionID("ped")
			if err != nil {
				return "", 0, err
			}
			updated, err := tx.ExecContext(ctx, `UPDATE file_nodes
				SET path_edge_incarnation = ?
				WHERE path_hash = ? AND path = ? AND extent_ino = ?`,
				edgeID, fileNodePathHash(fullPath), fullPath, ino)
			if err != nil {
				return "", 0, fmt.Errorf("bind promotion symlink edge %q: %w", entry.relativePath, err)
			}
			if affected, err := updated.RowsAffected(); err != nil || affected != 1 {
				return "", 0, ErrPromotionRecoveryRequired
			}
			continue
		}
		if entry.entryType != promotion.EntryTypeFile || !entry.contentID.Valid {
			return "", 0, ErrPromotionRecoveryRequired
		}
		content, ok := contentByID[entry.contentID.String]
		if !ok || content.size > math.MaxInt64 {
			return "", 0, ErrPromotionRecoveryRequired
		}
		inodeID, err := newPromotionID("pin")
		if err != nil {
			return "", 0, err
		}
		nodeID, err := newPromotionID("pnd")
		if err != nil {
			return "", 0, err
		}
		edgeID, err := newPromotionID("ped")
		if err != nil {
			return "", 0, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO inodes
			(inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at)
			VALUES (?, ?, 1, ?, 'CONFIRMED', ?, ?, ?)`, inodeID, content.size, entry.mode, now, mtime, mtime); err != nil {
			return "", 0, fmt.Errorf("insert promotion file inode %q: %w", entry.relativePath, err)
		}
		storageRef := "promotion://" + req.MigrationID + "/" + content.contentID
		if _, err := tx.ExecContext(ctx, `INSERT INTO contents
			(inode_id, storage_type, storage_ref, storage_ref_hash, storage_encryption_mode,
			 storage_encryption_key_id, content_blob, checksum_sha256, source_id, content_layout)
			VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?)`, inodeID, StorageDB9, storageRef,
			StorageRefHash(storageRef), StorageEncryptionNone, content.body, content.checksum,
			req.MigrationID, ContentLayoutSingle); err != nil {
			return "", 0, fmt.Errorf("insert promotion file content %q: %w", entry.relativePath, err)
		}
		if err := s.store.InsertSemanticTx(tx, &Semantic{InodeID: inodeID}); err != nil {
			return "", 0, fmt.Errorf("insert promotion semantic %q: %w", entry.relativePath, err)
		}
		parentPath := promotionPublishedParentPath(rootPath, entry.relativePath)
		if _, err := tx.ExecContext(ctx, `INSERT INTO file_nodes
			(node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory,
			 file_id, inode_id, path_edge_incarnation, children_generation, created_at, content_layout)
			VALUES (?, ?, ?, ?, ?, ?, FALSE, ?, ?, ?, 0, ?, ?)`, nodeID, fullPath,
			fileNodePathHash(fullPath), parentPath, fileNodePathHash(parentPath), pathutil.BaseName(fullPath),
			inodeID, inodeID, edgeID, now, ContentLayoutSingle); err != nil {
			return "", 0, fmt.Errorf("insert promotion file dentry %q: %w", entry.relativePath, err)
		}
	}
	return rootInode, 1, nil
}

func promotionPublishedParentPath(rootPath, relativePath string) string {
	idx := strings.LastIndexByte(relativePath, '/')
	if idx < 0 {
		return rootPath
	}
	return rootPath + relativePath[:idx+1]
}

func (s *PromotionStore) advancePromotionParentGenerationTx(ctx context.Context, tx *sql.Tx, tenantID string, parent promotionTargetParent, namespace promotionNamespaceCapability) error {
	if parent.childrenGeneration == math.MaxUint64 {
		return ErrPromotionRecoveryRequired
	}
	if parent.path == "/" {
		result, err := tx.ExecContext(ctx, `UPDATE promotion_namespace_capabilities
			SET root_children_generation = root_children_generation + 1
			WHERE tenant_id = ? AND root_edge_incarnation = ? AND root_children_generation = ?`,
			tenantID, namespace.rootEdgeIncarnation, parent.childrenGeneration)
		if err != nil {
			return fmt.Errorf("advance promotion root generation: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionTargetChanged
		}
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE file_nodes
		SET children_generation = children_generation + 1
		WHERE path_hash = ? AND path = ? AND is_directory = TRUE
		  AND path_edge_incarnation = ? AND children_generation = ?`,
		fileNodePathHash(parent.path), parent.path, parent.edgeIncarnation, parent.childrenGeneration)
	if err != nil {
		return fmt.Errorf("advance promotion parent generation: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionTargetChanged
	}
	return nil
}
