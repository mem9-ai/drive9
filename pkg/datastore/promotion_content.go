package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

// PromotionInlineContentRequest is the bounded DB-inline content mutation for
// one already-persisted regular-file manifest entry. Body is not read until
// the stored target and entry tuple have been checked; no database lock is
// held while the caller-controlled Reader is consumed.
type PromotionInlineContentRequest struct {
	TenantID       string
	MigrationID    string
	OwnerEpoch     uint64
	OwnerToken     string
	WriterLease    string
	RelativePath   string
	EntryHash      string
	SizeBytes      uint64
	ChecksumSHA256 string
	IdempotencyKey string
	Body           io.Reader
}

// PromotionInlineContent is the immutable content identity bound to a
// manifest entry.
type PromotionInlineContent struct {
	ContentID      string
	RelativePath   string
	SizeBytes      uint64
	ChecksumSHA256 string
	State          string
	StateVersion   uint64
}

// PromotionVerifyRequest freezes a complete, already-persisted manifest and
// its exact DB-inline content identities.
type PromotionVerifyRequest struct {
	TenantID     string
	MigrationID  string
	OwnerEpoch   uint64
	OwnerToken   string
	WriterLease  string
	ManifestHash string
}

type promotionOwnerImport struct {
	identity                       promotion.MigrationIdentity
	target                         string
	manifestHash                   string
	entryTotal                     uint64
	byteTotal                      uint64
	maxContentSize                 uint64
	storageMode                    string
	inlineThreshold                uint64
	namespaceCASEpoch              uint64
	acceptedRestoreGeneration      uint64
	acceptedDatabaseIncarnation    string
	acceptedWriterGeneration       uint64
	quotaReservationID             string
	targetParentInode              string
	targetParentPath               string
	targetParentEdgeIncarnation    string
	targetParentChildrenGeneration uint64
	state                          string
	stateVersion                   uint64
	ownerEpoch                     uint64
	ownerTokenHash                 string
	activityDeadline               time.Time
	leaseExpiresAt                 time.Time
	terminalReason                 sql.NullString
	commitAttemptID                sql.NullString
	commitWriterGeneration         sql.NullInt64
}

type promotionEntryRow struct {
	relativePath     string
	entryType        promotion.EntryType
	mode             uint32
	mtimeNS          int64
	symlinkTarget    sql.NullString
	expectedSize     uint64
	expectedChecksum string
	entryHash        string
	metadataBlob     sql.NullString
	contentID        sql.NullString
}

type promotionContentRow struct {
	contentID      string
	body           []byte
	idempotencyKey sql.NullString
	requestDigest  sql.NullString
	size           uint64
	checksum       string
	sealState      string
	ownershipState string
}

// PutInlineImportContent stores and binds one immutable inline content row.
// The content insert, entry bind, and first CREATED -> STAGING transition are
// one transaction, so response-loss retries discover the same content ID.
func (s *PromotionStore) PutInlineImportContent(ctx context.Context, req PromotionInlineContentRequest) (*PromotionInlineContent, error) {
	if req.TenantID == "" || req.OwnerEpoch == 0 || req.OwnerToken == "" || len(req.OwnerToken) > 512 || req.WriterLease == "" ||
		req.EntryHash == "" || req.IdempotencyKey == "" || len(req.IdempotencyKey) > 128 || req.Body == nil ||
		!validPromotionSHA256(req.EntryHash) || !validPromotionSHA256(req.ChecksumSHA256) {
		return nil, ErrPromotionConflict
	}
	identity, err := promotion.DecodeMigrationID(req.MigrationID)
	if err != nil {
		return nil, ErrNotFound
	}
	relativePath, err := canonicalPromotionRelativePath(req.RelativePath)
	if err != nil || relativePath != req.RelativePath {
		return nil, ErrPromotionConflict
	}
	target, err := s.authorizePromotionMutationTarget(ctx, req.TenantID, identity)
	if err != nil {
		return nil, err
	}

	// Check the complete stored tuple before accepting a body, then release
	// every database lock. A slow or malicious reader must not prevent the
	// deadline scanner or restore controller from acquiring their locks. The
	// final transaction repeats every check after the body is complete.
	deadlineWon, err := s.preflightPromotionInlineContent(ctx, req, identity, target)
	if err != nil {
		return nil, err
	}
	if deadlineWon {
		return nil, ErrPromotionDeadlineExceeded
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, int64(req.SizeBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("read promotion inline content: %w", err)
	}
	if uint64(len(body)) != req.SizeBytes || promotionHashBytes(body) != req.ChecksumSHA256 {
		return nil, ErrPromotionConflict
	}
	confirmedTarget, err := s.authorizePromotionMutationTarget(ctx, req.TenantID, identity)
	if err != nil {
		return nil, err
	}
	if confirmedTarget != target {
		return nil, ErrPromotionRecoveryRequired
	}
	requestDigest, err := promotionJSONHash(struct {
		Version        string `json:"version"`
		TenantID       string `json:"tenant_id"`
		MigrationID    string `json:"migration_id"`
		RelativePath   string `json:"relative_path"`
		EntryHash      string `json:"entry_hash"`
		SizeBytes      uint64 `json:"size_bytes"`
		ChecksumSHA256 string `json:"checksum_sha256"`
		IdempotencyKey string `json:"idempotency_key"`
	}{
		Version: "p1", TenantID: req.TenantID, MigrationID: req.MigrationID,
		RelativePath: relativePath, EntryHash: req.EntryHash,
		SizeBytes: req.SizeBytes, ChecksumSHA256: req.ChecksumSHA256, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		return nil, err
	}

	var out *PromotionInlineContent
	deadlineWon = false
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, now, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		importRow, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if err := validatePromotionOwnerMutation(importRow, tenant, namespace, req.OwnerEpoch, req.OwnerToken); err != nil {
			return err
		}
		if importRow.target != target {
			return ErrPromotionRecoveryRequired
		}
		if !now.Before(importRow.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, importRow, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if !now.Before(importRow.leaseExpiresAt) {
			return ErrPromotionConflict
		}
		entry, err := validatePromotionInlineTupleTx(ctx, tx, req, identity, importRow)
		if err != nil {
			return err
		}
		// Tuple/reservation reads above are intentionally before this second
		// database-time sample. The state CAS is the deadline winner, not the
		// transaction start timestamp.
		finishedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if !finishedAt.Before(importRow.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, importRow, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, finishedAt); err != nil {
			return err
		}
		if !finishedAt.Before(importRow.leaseExpiresAt) {
			return ErrPromotionConflict
		}
		existing, err := selectPromotionContentByIdempotencyTx(ctx, tx, req.TenantID, identity, req.IdempotencyKey)
		if err == nil {
			if !existing.requestDigest.Valid || existing.requestDigest.String != requestDigest ||
				existing.size != req.SizeBytes || existing.checksum != req.ChecksumSHA256 ||
				promotionHashBytes(existing.body) != req.ChecksumSHA256 {
				return ErrPromotionConflict
			}
			if !entry.contentID.Valid || entry.contentID.String != existing.contentID ||
				existing.sealState != "INLINE" || existing.ownershipState != "STAGED" {
				return ErrPromotionRecoveryRequired
			}
			out = &PromotionInlineContent{
				ContentID: existing.contentID, RelativePath: relativePath, SizeBytes: existing.size,
				ChecksumSHA256: existing.checksum, State: importRow.state, StateVersion: importRow.stateVersion,
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if importRow.state == "VERIFIED" {
			return ErrPromotionConflict
		}
		if entry.contentID.Valid {
			return ErrPromotionConflict
		}

		contentID, err := newPromotionID("pic")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO promotion_import_contents
			(tenant_id, allocation_epoch, allocation_sequence, import_content_id,
			 inline_content_blob, inline_content_idempotency_key, inline_request_digest,
			 size_bytes, checksum_sha256, seal_state, ownership_state)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'INLINE', 'STAGED')`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence, contentID,
			body, req.IdempotencyKey, requestDigest, req.SizeBytes, req.ChecksumSHA256); err != nil {
			return fmt.Errorf("insert promotion inline content: %w", err)
		}
		bound, err := tx.ExecContext(ctx, `UPDATE promotion_import_entries
			SET import_content_id = ?
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND relative_path_hash = ? AND relative_path = ? AND import_content_id IS NULL`,
			contentID, req.TenantID, identity.AllocationEpoch, identity.AllocationSequence,
			promotionHashString(relativePath), relativePath)
		if err != nil {
			return fmt.Errorf("bind promotion inline content: %w", err)
		}
		if affected, err := bound.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		state, version := importRow.state, importRow.stateVersion
		if state == "CREATED" {
			advanced, err := tx.ExecContext(ctx, `UPDATE promotion_imports
				SET state = 'STAGING', state_version = state_version + 1
				WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
				  AND state = 'CREATED' AND state_version = ?`, req.TenantID,
				identity.AllocationEpoch, identity.AllocationSequence, version)
			if err != nil {
				return fmt.Errorf("advance promotion import to staging: %w", err)
			}
			if affected, err := advanced.RowsAffected(); err != nil || affected != 1 {
				return ErrPromotionConflict
			}
			state = "STAGING"
			version++
		}
		out = &PromotionInlineContent{
			ContentID: contentID, RelativePath: relativePath, SizeBytes: req.SizeBytes,
			ChecksumSHA256: req.ChecksumSHA256, State: state, StateVersion: version,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if deadlineWon {
		return nil, ErrPromotionDeadlineExceeded
	}
	return out, nil
}

// VerifyImport proves that the complete persisted entry set and the complete
// inline content set are an exact one-to-one match before freezing the import.
func (s *PromotionStore) VerifyImport(ctx context.Context, req PromotionVerifyRequest) (*PromotionImport, error) {
	if req.TenantID == "" || req.OwnerEpoch == 0 || req.OwnerToken == "" || len(req.OwnerToken) > 512 || req.WriterLease == "" ||
		!validPromotionSHA256(req.ManifestHash) {
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

	var out *PromotionImport
	deadlineWon := false
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, _, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		importRow, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if err := validatePromotionOwnerMutation(importRow, tenant, namespace, req.OwnerEpoch, req.OwnerToken); err != nil {
			return err
		}
		if importRow.target != target {
			return ErrPromotionRecoveryRequired
		}
		if importRow.manifestHash != req.ManifestHash {
			return ErrPromotionConflict
		}
		// The guard locks precede the import-row lock. Re-sample time only after
		// the import lock is acquired: an exact VERIFIED retry may otherwise
		// wait across the deadline and recover using the stale pre-lock sample.
		lockedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if !lockedAt.Before(importRow.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, importRow, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, lockedAt); err != nil {
			return err
		}
		if !lockedAt.Before(importRow.leaseExpiresAt) {
			return ErrPromotionConflict
		}
		if importRow.state == "VERIFIED" {
			out = importRow.response(req.MigrationID)
			return nil
		}
		if importRow.storageMode != string(promotion.StorageModeDB9Inline) ||
			(importRow.state != "CREATED" && importRow.state != "STAGING") {
			return ErrPromotionConflict
		}
		if err := validatePromotionReservationTx(ctx, tx, req.TenantID, importRow); err != nil {
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
		if err := validatePromotionCompleteManifest(importRow, entries, contents); err != nil {
			return err
		}
		// Verification may scan and hash a bounded tree. Sample database time
		// again immediately before the state CAS so an already-expired worker
		// cannot win with the transaction-start timestamp.
		finishedAt, err := promotionDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if !finishedAt.Before(importRow.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, importRow, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if err := s.requirePromotionNormalWriterLease(ctx, req.TenantID, req.WriterLease, tenant, namespace, finishedAt); err != nil {
			return err
		}
		if !finishedAt.Before(importRow.leaseExpiresAt) {
			return ErrPromotionConflict
		}
		advanced, err := tx.ExecContext(ctx, `UPDATE promotion_imports
			SET state = 'VERIFIED', state_version = state_version + 1
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
			  AND state = ? AND state_version = ?`, req.TenantID,
			identity.AllocationEpoch, identity.AllocationSequence, importRow.state, importRow.stateVersion)
		if err != nil {
			return fmt.Errorf("verify promotion import: %w", err)
		}
		if affected, err := advanced.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		importRow.state = "VERIFIED"
		importRow.stateVersion++
		out = importRow.response(req.MigrationID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if deadlineWon {
		return nil, ErrPromotionDeadlineExceeded
	}
	return out, nil
}

func (s *PromotionStore) readPromotionMutationTarget(ctx context.Context, tenantID string, identity promotion.MigrationIdentity) (string, error) {
	var target string
	err := s.store.db.QueryRowContext(ctx, `SELECT target_path FROM promotion_imports
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&target)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("resolve promotion mutation target: %w", err)
	}
	return target, nil
}

// authorizePromotionMutationTarget deliberately maps an existing but
// out-of-scope import to the same public result as an unknown import. Content
// continuation APIs must not be usable as migration-ID existence oracles.
func (s *PromotionStore) authorizePromotionMutationTarget(ctx context.Context, tenantID string, identity promotion.MigrationIdentity) (string, error) {
	target, err := s.readPromotionMutationTarget(ctx, tenantID, identity)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	if err := s.cfg.Authorizer.AuthorizePromotionWrite(ctx, tenantID, target); err != nil {
		return "", ErrNotFound
	}
	return target, nil
}

// preflightPromotionInlineContent rejects an invalid stored tuple before
// reading the request body. Its transaction is intentionally short-lived:
// every lock is released before calling Reader.Read, and the mutation
// transaction repeats all checks after the body is complete.
func (s *PromotionStore) preflightPromotionInlineContent(ctx context.Context, req PromotionInlineContentRequest, identity promotion.MigrationIdentity, target string) (bool, error) {
	deadlineWon := false
	err := s.store.InTx(ctx, func(tx *sql.Tx) error {
		tenant, namespace, now, err := s.lockPromotionMutationGuard(ctx, tx, req.TenantID, req.WriterLease)
		if err != nil {
			return err
		}
		importRow, err := s.lockPromotionOwnerImport(ctx, tx, req.TenantID, identity)
		if err != nil {
			return err
		}
		if err := validatePromotionOwnerMutation(importRow, tenant, namespace, req.OwnerEpoch, req.OwnerToken); err != nil {
			return err
		}
		if importRow.target != target {
			return ErrPromotionRecoveryRequired
		}
		if !now.Before(importRow.activityDeadline) {
			if err := s.transitionPromotionDeadlineTx(ctx, tx, req.TenantID, importRow, namespace.writerGeneration); err != nil {
				return err
			}
			deadlineWon = true
			return nil
		}
		if !now.Before(importRow.leaseExpiresAt) {
			return ErrPromotionConflict
		}
		_, err = validatePromotionInlineTupleTx(ctx, tx, req, identity, importRow)
		return err
	})
	return deadlineWon, err
}

func validatePromotionInlineTupleTx(ctx context.Context, tx *sql.Tx, req PromotionInlineContentRequest, identity promotion.MigrationIdentity, importRow *promotionOwnerImport) (*promotionEntryRow, error) {
	if importRow.storageMode != string(promotion.StorageModeDB9Inline) || importRow.inlineThreshold == 0 ||
		req.SizeBytes >= importRow.inlineThreshold || req.SizeBytes > math.MaxInt64 {
		return nil, promotion.ErrStorageBackendUnsupported
	}
	if importRow.state != "CREATED" && importRow.state != "STAGING" && importRow.state != "VERIFIED" {
		return nil, ErrPromotionConflict
	}
	entry, err := lockPromotionEntryTx(ctx, tx, req.TenantID, identity, req.RelativePath)
	if err != nil {
		return nil, err
	}
	if entry.entryType != promotion.EntryTypeFile || entry.entryHash != req.EntryHash ||
		entry.expectedSize != req.SizeBytes || entry.expectedChecksum != req.ChecksumSHA256 {
		return nil, ErrPromotionConflict
	}
	if err := validatePromotionReservationTx(ctx, tx, req.TenantID, importRow); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *PromotionStore) lockPromotionMutationGuard(ctx context.Context, tx *sql.Tx, tenantID, writerLease string) (promotionIdentityTenant, promotionNamespaceCapability, time.Time, error) {
	tenant, err := s.lockPromotionIdentityTenant(ctx, tx, tenantID)
	if err != nil {
		return promotionIdentityTenant{}, promotionNamespaceCapability{}, time.Time{}, err
	}
	namespace, err := s.lockPromotionNamespaceCapability(ctx, tx, tenantID)
	if err != nil {
		return promotionIdentityTenant{}, promotionNamespaceCapability{}, time.Time{}, err
	}
	if err := s.requirePromotionWriterProtocol(namespace); err != nil {
		return promotionIdentityTenant{}, promotionNamespaceCapability{}, time.Time{}, err
	}
	now, err := promotionDatabaseTime(ctx, tx)
	if err != nil {
		return promotionIdentityTenant{}, promotionNamespaceCapability{}, time.Time{}, err
	}
	if err := s.requirePromotionNormalWriterLease(ctx, tenantID, writerLease, tenant, namespace, now); err != nil {
		return promotionIdentityTenant{}, promotionNamespaceCapability{}, time.Time{}, err
	}
	if !namespace.ready || namespace.epoch == 0 {
		return promotionIdentityTenant{}, promotionNamespaceCapability{}, time.Time{}, ErrPromotionRestoreFenced
	}
	return tenant, namespace, now, nil
}

func (s *PromotionStore) lockPromotionOwnerImport(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity) (*promotionOwnerImport, error) {
	row := &promotionOwnerImport{identity: identity}
	err := tx.QueryRowContext(ctx, `SELECT target_path, manifest_hash, entry_total, byte_total,
		max_content_size, storage_mode, inline_threshold, namespace_cas_epoch,
		accepted_restore_generation, accepted_database_incarnation, accepted_writer_generation,
		quota_reservation_id, target_parent_inode, target_parent_path,
		target_parent_edge_incarnation, target_parent_children_generation,
		state, state_version, owner_epoch, owner_token_hash,
		activity_deadline, lease_expires_at, terminal_reason,
		commit_attempt_id, commit_writer_generation
		FROM promotion_imports
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&row.target, &row.manifestHash, &row.entryTotal, &row.byteTotal,
			&row.maxContentSize, &row.storageMode, &row.inlineThreshold, &row.namespaceCASEpoch,
			&row.acceptedRestoreGeneration, &row.acceptedDatabaseIncarnation, &row.acceptedWriterGeneration,
			&row.quotaReservationID, &row.targetParentInode, &row.targetParentPath,
			&row.targetParentEdgeIncarnation, &row.targetParentChildrenGeneration,
			&row.state, &row.stateVersion, &row.ownerEpoch, &row.ownerTokenHash,
			&row.activityDeadline, &row.leaseExpiresAt, &row.terminalReason,
			&row.commitAttemptID, &row.commitWriterGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock promotion import mutation: %w", err)
	}
	return row, nil
}

func validatePromotionOwnerMutation(row *promotionOwnerImport, tenant promotionIdentityTenant, namespace promotionNamespaceCapability, ownerEpoch uint64, ownerToken string) error {
	if row.target == "" || row.ownerEpoch != ownerEpoch || !promotionHashEqual(row.ownerTokenHash, promotionHashString(ownerToken)) {
		return ErrPromotionConflict
	}
	if row.acceptedRestoreGeneration != tenant.RestoreGeneration ||
		row.acceptedRestoreGeneration != namespace.restoreGeneration ||
		row.acceptedDatabaseIncarnation != tenant.DatabaseIncarnation ||
		row.acceptedDatabaseIncarnation != namespace.databaseIncarnation ||
		row.acceptedWriterGeneration != tenant.WriterGeneration ||
		row.acceptedWriterGeneration != namespace.writerGeneration {
		return ErrPromotionRestoreFenced
	}
	if row.state == "ABORTING" && row.terminalReason.Valid && row.terminalReason.String == "activity_deadline_exceeded" {
		return ErrPromotionDeadlineExceeded
	}
	return nil
}

func (row *promotionOwnerImport) response(migrationID string) *PromotionImport {
	return &PromotionImport{
		MigrationID: migrationID, AllocationEpoch: row.identity.AllocationEpoch,
		AllocationSequence: row.identity.AllocationSequence, Target: row.target,
		ManifestHash: row.manifestHash, QuotaReservationID: row.quotaReservationID,
		State: row.state, StateVersion: row.stateVersion, OwnerEpoch: row.ownerEpoch,
		ActivityDeadline: row.activityDeadline, LeaseExpiresAt: row.leaseExpiresAt,
		RestoreGeneration:   row.acceptedRestoreGeneration,
		DatabaseIncarnation: row.acceptedDatabaseIncarnation,
		WriterGeneration:    row.acceptedWriterGeneration,
	}
}

func (s *PromotionStore) transitionPromotionDeadlineTx(ctx context.Context, tx *sql.Tx, tenantID string, row *promotionOwnerImport, writerGeneration uint64) error {
	if row.state != "CREATED" && row.state != "STAGING" && row.state != "VERIFIED" {
		return ErrPromotionConflict
	}
	attemptID, err := newPromotionID("pca")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE promotion_imports
		SET state = 'ABORTING', state_version = state_version + 1,
		    cleanup_attempt_id = ?, cleanup_writer_generation = ?,
		    terminal_reason = 'activity_deadline_exceeded'
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
		  AND state = ? AND state_version = ?`,
		attemptID, writerGeneration, tenantID, row.identity.AllocationEpoch,
		row.identity.AllocationSequence, row.state, row.stateVersion)
	if err != nil {
		return fmt.Errorf("claim promotion deadline cleanup: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionConflict
	}
	return nil
}

func lockPromotionEntryTx(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity, relativePath string) (*promotionEntryRow, error) {
	row := &promotionEntryRow{relativePath: relativePath}
	err := tx.QueryRowContext(ctx, `SELECT entry_type, mode, mtime_ns, symlink_target,
		expected_size_bytes, expected_checksum_sha256, entry_hash, metadata_blob, import_content_id
		FROM promotion_import_entries
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
		  AND relative_path_hash = ? AND relative_path = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence,
		promotionHashString(relativePath), relativePath).
		Scan(&row.entryType, &row.mode, &row.mtimeNS, &row.symlinkTarget,
			&row.expectedSize, &row.expectedChecksum, &row.entryHash, &row.metadataBlob, &row.contentID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock promotion manifest entry: %w", err)
	}
	return row, nil
}

func selectPromotionContentByIdempotencyTx(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity, key string) (*promotionContentRow, error) {
	row := &promotionContentRow{}
	err := tx.QueryRowContext(ctx, `SELECT import_content_id, inline_content_blob,
		inline_content_idempotency_key, inline_request_digest, size_bytes,
		checksum_sha256, seal_state, ownership_state
		FROM promotion_import_contents
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
		  AND inline_content_idempotency_key = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence, key).
		Scan(&row.contentID, &row.body, &row.idempotencyKey, &row.requestDigest,
			&row.size, &row.checksum, &row.sealState, &row.ownershipState)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read promotion inline content retry: %w", err)
	}
	return row, nil
}

func validatePromotionReservationTx(ctx context.Context, tx *sql.Tx, tenantID string, row *promotionOwnerImport) error {
	var bytes, files uint64
	var state string
	err := tx.QueryRowContext(ctx, `SELECT reserved_bytes, reserved_files, state
		FROM promotion_quota_reservations
		WHERE tenant_id = ? AND reservation_id = ?
		  AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, row.quotaReservationID, row.identity.AllocationEpoch, row.identity.AllocationSequence).
		Scan(&bytes, &files, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrPromotionRecoveryRequired
	}
	if err != nil {
		return fmt.Errorf("lock promotion quota reservation: %w", err)
	}
	if state != "RESERVED" || bytes != row.byteTotal {
		return ErrPromotionRecoveryRequired
	}
	var expectedFiles uint64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM promotion_import_entries
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? AND entry_type = 'file'`,
		tenantID, row.identity.AllocationEpoch, row.identity.AllocationSequence).Scan(&expectedFiles); err != nil {
		return fmt.Errorf("count promotion reserved files: %w", err)
	}
	if files != expectedFiles {
		return ErrPromotionRecoveryRequired
	}
	return nil
}

func selectPromotionEntriesTx(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity) ([]promotionEntryRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT relative_path, entry_type, mode, mtime_ns,
		symlink_target, expected_size_bytes, expected_checksum_sha256, entry_hash, metadata_blob, import_content_id
		FROM promotion_import_entries
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
		FOR UPDATE`, tenantID, identity.AllocationEpoch, identity.AllocationSequence)
	if err != nil {
		return nil, fmt.Errorf("lock promotion manifest entries: %w", err)
	}
	defer rows.Close()
	var entries []promotionEntryRow
	for rows.Next() {
		var entry promotionEntryRow
		if err := rows.Scan(&entry.relativePath, &entry.entryType, &entry.mode, &entry.mtimeNS,
			&entry.symlinkTarget, &entry.expectedSize, &entry.expectedChecksum, &entry.entryHash,
			&entry.metadataBlob, &entry.contentID); err != nil {
			return nil, fmt.Errorf("scan promotion manifest entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate promotion manifest entries: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].relativePath < entries[j].relativePath })
	return entries, nil
}

func selectPromotionContentsTx(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity) ([]promotionContentRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT import_content_id, inline_content_blob,
		inline_content_idempotency_key, inline_request_digest, size_bytes,
		checksum_sha256, seal_state, ownership_state
		FROM promotion_import_contents
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ?
		ORDER BY import_content_id FOR UPDATE`, tenantID, identity.AllocationEpoch, identity.AllocationSequence)
	if err != nil {
		return nil, fmt.Errorf("lock promotion inline contents: %w", err)
	}
	defer rows.Close()
	var contents []promotionContentRow
	for rows.Next() {
		var content promotionContentRow
		if err := rows.Scan(&content.contentID, &content.body, &content.idempotencyKey,
			&content.requestDigest, &content.size, &content.checksum,
			&content.sealState, &content.ownershipState); err != nil {
			return nil, fmt.Errorf("scan promotion inline content: %w", err)
		}
		contents = append(contents, content)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate promotion inline contents: %w", err)
	}
	return contents, nil
}

func validatePromotionCompleteManifest(importRow *promotionOwnerImport, entries []promotionEntryRow, contents []promotionContentRow) error {
	manifestEntries := make([]promotion.ManifestEntry, 0, len(entries))
	referenced := make(map[string]int, len(contents))
	for _, entry := range entries {
		if entry.metadataBlob.Valid {
			return ErrPromotionRecoveryRequired
		}
		manifestEntry := promotion.ManifestEntry{
			RelativePath: entry.relativePath, Type: entry.entryType, Mode: entry.mode,
			MtimeNS: entry.mtimeNS, ExpectedSizeBytes: entry.expectedSize,
			ExpectedChecksumSHA256: entry.expectedChecksum, EntryHash: entry.entryHash,
		}
		if entry.symlinkTarget.Valid {
			manifestEntry.SymlinkTarget = entry.symlinkTarget.String
		}
		manifestEntries = append(manifestEntries, manifestEntry)
		switch entry.entryType {
		case promotion.EntryTypeFile:
			if !entry.contentID.Valid || entry.contentID.String == "" {
				return ErrPromotionConflict
			}
			referenced[entry.contentID.String]++
		case promotion.EntryTypeDirectory, promotion.EntryTypeSymlink:
			if entry.contentID.Valid {
				return ErrPromotionRecoveryRequired
			}
		default:
			return ErrPromotionRecoveryRequired
		}
	}
	if uint64(len(entries)) != importRow.entryTotal || uint64(len(contents)) != uint64(len(referenced)) {
		return ErrPromotionRecoveryRequired
	}
	contentByID := make(map[string]promotionContentRow, len(contents))
	for _, content := range contents {
		if content.contentID == "" || contentByID[content.contentID].contentID != "" ||
			!content.idempotencyKey.Valid || content.idempotencyKey.String == "" ||
			!content.requestDigest.Valid || !validPromotionSHA256(content.requestDigest.String) ||
			content.sealState != "INLINE" || content.ownershipState != "STAGED" ||
			promotionHashBytes(content.body) != content.checksum {
			return ErrPromotionRecoveryRequired
		}
		contentByID[content.contentID] = content
	}
	for _, entry := range entries {
		if entry.entryType != promotion.EntryTypeFile {
			continue
		}
		if referenced[entry.contentID.String] != 1 {
			return ErrPromotionRecoveryRequired
		}
		content, ok := contentByID[entry.contentID.String]
		if !ok || content.size != entry.expectedSize || content.checksum != entry.expectedChecksum ||
			uint64(len(content.body)) != entry.expectedSize {
			return ErrPromotionRecoveryRequired
		}
	}
	limits := promotionStoredManifestLimits(manifestEntries, importRow.inlineThreshold)
	manifest, err := promotion.ValidateCanonicalManifest(manifestEntries, limits)
	if err != nil || manifest.ManifestHash != importRow.manifestHash ||
		manifest.EntryTotal != importRow.entryTotal || manifest.ByteTotal != importRow.byteTotal ||
		manifest.MaxContentSize != importRow.maxContentSize {
		return ErrPromotionRecoveryRequired
	}
	return nil
}

func promotionStoredManifestLimits(entries []promotion.ManifestEntry, inlineThreshold uint64) promotion.ManifestLimits {
	limits := promotion.ManifestLimits{
		MaxEntries: 1, MaxMetadataBytes: math.MaxUint64, MaxTotalInlineBytes: math.MaxUint64,
		MaxPathBytes: 1, MaxSymlinkBytes: 1, MaxDepth: 1, InlineThreshold: inlineThreshold,
	}
	if uint64(len(entries)) > limits.MaxEntries {
		limits.MaxEntries = uint64(len(entries))
	}
	for _, entry := range entries {
		if uint64(len(entry.RelativePath)) > limits.MaxPathBytes {
			limits.MaxPathBytes = uint64(len(entry.RelativePath))
		}
		if uint64(len(entry.SymlinkTarget)) > limits.MaxSymlinkBytes {
			limits.MaxSymlinkBytes = uint64(len(entry.SymlinkTarget))
		}
		depth := uint64(strings.Count(entry.RelativePath, "/") + 1)
		if depth > limits.MaxDepth {
			limits.MaxDepth = depth
		}
	}
	return limits
}

func canonicalPromotionRelativePath(raw string) (string, error) {
	if raw == "" || strings.HasPrefix(raw, "/") {
		return "", ErrPromotionConflict
	}
	canonical, err := pathutil.Canonicalize("/" + raw)
	if err != nil || canonical == "/" {
		return "", ErrPromotionConflict
	}
	return strings.TrimPrefix(canonical, "/"), nil
}
