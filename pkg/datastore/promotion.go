package datastore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

var (
	ErrPromotionDisabled                    = errors.New("promotion is disabled")
	ErrPromotionConflict                    = errors.New("promotion request conflict")
	ErrPromotionIdentityBudgetExceeded      = errors.New("promotion identity budget exceeded")
	ErrPromotionIdentityCapacityUnavailable = errors.New("promotion identity capacity unavailable")
	ErrPromotionIdentityEpochUnavailable    = errors.New("promotion identity epoch unavailable")
	ErrPromotionQuotaExceeded               = errors.New("promotion quota exceeded")
	ErrPromotionDeadlineExceeded            = errors.New("promotion activity deadline exceeded")
	ErrPromotionRestoreFenced               = errors.New("promotion writes are restore-fenced")
	ErrPromotionIDRetired                   = errors.New("promotion migration id is retired")
	ErrPromotionRecoveryRequired            = errors.New("promotion recovery is required")
	ErrPromotionTargetChanged               = errors.New("promotion target precondition changed")
)

const promotionGlobalCapacityKey = "promotion-imports"

// PromotionAuthorizer is invoked on every client-facing promotion operation.
// Implementations must enforce the current request's tenant and target write
// scope; possession of a plan, proof, or token is not authorization.
type PromotionAuthorizer interface {
	AuthorizePromotionRead(ctx context.Context, tenantID, canonicalTarget string) error
	AuthorizePromotionWrite(ctx context.Context, tenantID, canonicalTarget string) error
}

// PromotionAllocationLease is a verified lease from the non-rollback identity
// authority. AllocateImportID matches every claim against locked tenant state
// and checks NotAfter with database time in the claim transaction.
type PromotionAllocationLease struct {
	TenantID                  string
	AllocationEpoch           uint64
	RestoreGeneration         uint64
	DatabaseIncarnation       string
	AllocationLeaseGeneration uint64
	NotAfter                  time.Time
}

// PromotionWriterLease is the verified NORMAL lease used by client-facing
// promotion mutations. Restore drain/reconcile leases are operation-specific
// and intentionally cannot be passed through this type.
type PromotionWriterLease struct {
	TenantID            string
	AllocationEpoch     uint64
	RestoreGeneration   uint64
	DatabaseIncarnation string
	WriterGeneration    uint64
	NotAfter            time.Time
}

// PromotionLeaseVerifier authenticates opaque leases issued by the external
// authority. Verification is not sufficient on its own: datastore methods
// always compare the claims under tenant-row locks.
type PromotionLeaseVerifier interface {
	VerifyAllocationLease(ctx context.Context, token string) (PromotionAllocationLease, error)
	VerifyNormalWriterLease(ctx context.Context, token string) (PromotionWriterLease, error)
}

// PromotionStoreConfig contains process dependencies and operation deadlines
// for the first DB-inline implementation. Identity-admission bounds are read
// from versioned durable rows under the allocation transaction; they are not
// process-local knobs. RuntimeCapability must return the closed adapter
// classification for the serving process; disagreement with the durable row
// fails closed.
type PromotionStoreConfig struct {
	Planner           *promotion.Planner
	ProofSigner       *promotion.ProofSigner
	Authorizer        PromotionAuthorizer
	LeaseVerifier     PromotionLeaseVerifier
	RuntimeCapability func(tenantID string) promotion.StorageCapability
	// WriterProtocol is the namespace mutation protocol implemented by this
	// process. Deployment raises the durable minimum only after schema
	// migration and drains older processes/connections; every promotion entry
	// point then fails closed before mutation when this value is too old.
	WriterProtocol uint64
	Limits         promotion.ManifestLimits
	AllocationTTL  time.Duration
	ActivityTTL    time.Duration
	LeaseTTL       time.Duration
}

// PromotionStore owns the promotion admission and state-transition boundaries.
type PromotionStore struct {
	store *Store
	cfg   PromotionStoreConfig
}

type PromotionAllocationRequest struct {
	TenantID             string
	Target               string
	ExpectedTargetAbsent bool
	IdempotencyKey       string
	AllocationLease      string
}

type PromotionAllocation struct {
	MigrationID        string
	AllocationEpoch    uint64
	AllocationSequence uint64
	AllocationProof    string
	CreateBefore       time.Time
}

type PromotionCreateRequest struct {
	TenantID             string
	MigrationID          string
	AllocationProof      string
	Target               string
	ExpectedTargetAbsent bool
	Manifest             []promotion.ManifestEntry
	Plan                 promotion.ImportPlan
	OwnerToken           string
	RecoveryToken        string
	WriterLease          string
}

type PromotionImport struct {
	MigrationID          string
	AllocationEpoch      uint64
	AllocationSequence   uint64
	Target               string
	ManifestHash         string
	QuotaReservationID   string
	State                string
	StateVersion         uint64
	OwnerEpoch           uint64
	ActivityDeadline     time.Time
	LeaseExpiresAt       time.Time
	RestoreGeneration    uint64
	DatabaseIncarnation  string
	WriterGeneration     uint64
	TerminalResultBlob   string
	TerminalResultDigest string
}

type promotionRateBucket struct {
	TokensMilli      uint64 `json:"tokens_milli"`
	UpdatedUnixMilli int64  `json:"updated_unix_milli"`
}

type promotionIdentityTenant struct {
	AllocationEpoch           uint64
	RestoreGeneration         uint64
	DatabaseIncarnation       string
	WriterGeneration          uint64
	AllocationLeaseGeneration uint64
	AdmissionState            string
	LiveClaimCount            uint64
	MaterializedCount         uint64
	RateBucketRaw             sql.NullString
	MaxLiveClaims             uint64
	MaxMaterializedIdentities uint64
	MaxSequenceWindow         uint64
	AllocationRatePerMinute   uint64
	AllocationRateBurst       uint64
	ConfigGeneration          uint64
}

// NewPromotionStore constructs the P0 store. It rejects an incomplete runtime
// configuration rather than permitting a partially guarded implementation.
func NewPromotionStore(store *Store, cfg PromotionStoreConfig) (*PromotionStore, error) {
	if store == nil || cfg.Planner == nil || cfg.ProofSigner == nil || cfg.Authorizer == nil || cfg.LeaseVerifier == nil || cfg.RuntimeCapability == nil {
		return nil, fmt.Errorf("%w: promotion store dependencies are required", ErrPromotionDisabled)
	}
	if cfg.WriterProtocol == 0 {
		return nil, fmt.Errorf("%w: promotion writer protocol is required", ErrPromotionDisabled)
	}
	if cfg.AllocationTTL <= 0 || cfg.ActivityTTL <= 0 || cfg.LeaseTTL <= 0 || cfg.LeaseTTL > cfg.ActivityTTL {
		return nil, fmt.Errorf("%w: invalid promotion deadlines", ErrPromotionDisabled)
	}
	return &PromotionStore{store: store, cfg: cfg}, nil
}

// PlanImport performs no write. It binds the complete canonical manifest to
// the durable and runtime DB-inline capability generations.
func (s *PromotionStore) PlanImport(ctx context.Context, req promotion.PlanImportRequest) (*promotion.ImportPlan, error) {
	target, err := promotion.CanonicalTarget(req.Target)
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Authorizer.AuthorizePromotionWrite(ctx, req.TenantID, target); err != nil {
		return nil, err
	}
	namespace, err := s.readPromotionNamespaceCapability(ctx, req.TenantID)
	if err != nil {
		return nil, err
	}
	if err := s.requirePromotionWriterProtocol(namespace); err != nil {
		return nil, err
	}
	capability, err := s.readPromotionStorageCapability(ctx, req.TenantID)
	if err != nil {
		return nil, err
	}
	if err := s.requireRuntimeCapability(req.TenantID, capability); err != nil {
		return nil, err
	}
	req.Target = target
	return s.cfg.Planner.PlanImport(req, capability, s.cfg.Limits)
}

// AllocateImportID durably allocates a tenant-scoped epoch/sequence identity.
// Exact idempotency retries recover before capacity/rate admission.
func (s *PromotionStore) AllocateImportID(ctx context.Context, req PromotionAllocationRequest) (*PromotionAllocation, error) {
	if req.TenantID == "" || req.IdempotencyKey == "" || len(req.IdempotencyKey) > 256 || req.AllocationLease == "" {
		return nil, fmt.Errorf("%w: tenant and bounded idempotency key are required", ErrPromotionConflict)
	}
	if !req.ExpectedTargetAbsent {
		return nil, fmt.Errorf("%w: P0 requires expected_target_absent=true", ErrPromotionConflict)
	}
	target, err := promotion.CanonicalTarget(req.Target)
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Authorizer.AuthorizePromotionWrite(ctx, req.TenantID, target); err != nil {
		return nil, err
	}
	idempotencyHash := promotionHashString(req.IdempotencyKey)
	requestDigest, err := promotionJSONHash(struct {
		Version              string `json:"version"`
		TenantID             string `json:"tenant_id"`
		Target               string `json:"target"`
		ExpectedTargetAbsent bool   `json:"expected_target_absent"`
	}{Version: "p1", TenantID: req.TenantID, Target: target, ExpectedTargetAbsent: true})
	if err != nil {
		return nil, err
	}
	var out *PromotionAllocation
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
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
		if err := s.requirePromotionAllocationLease(ctx, req.TenantID, req.AllocationLease, tenant, now); err != nil {
			return err
		}
		if existing, err := s.selectPromotionAllocationByIdempotency(ctx, tx, req.TenantID, idempotencyHash); err == nil {
			if existing.requestDigest != requestDigest || existing.target != target || !existing.expectedTargetAbsent {
				return ErrPromotionConflict
			}
			out = existing.response
			return nil
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		if tenant.AllocationEpoch == 0 || tenant.DatabaseIncarnation == "" || tenant.WriterGeneration == 0 {
			return ErrPromotionDisabled
		}
		if tenant.ConfigGeneration == 0 || tenant.MaxLiveClaims == 0 || tenant.MaxMaterializedIdentities == 0 ||
			tenant.MaxSequenceWindow == 0 || tenant.AllocationRatePerMinute == 0 || tenant.AllocationRateBurst == 0 {
			return ErrPromotionDisabled
		}
		if tenant.LiveClaimCount >= tenant.MaxLiveClaims || tenant.MaterializedCount >= tenant.MaxMaterializedIdentities {
			return ErrPromotionIdentityBudgetExceeded
		}
		bucket, err := consumePromotionRate(tenant.RateBucketRaw, now, tenant.AllocationRatePerMinute, tenant.AllocationRateBurst)
		if err != nil {
			return err
		}
		var globalCount, globalMax, globalConfigGeneration uint64
		if err := tx.QueryRowContext(ctx, `SELECT materialized_identity_count,
			max_materialized_identities, config_generation
			FROM promotion_import_identity_global WHERE capacity_key = ? FOR UPDATE`, promotionGlobalCapacityKey).
			Scan(&globalCount, &globalMax, &globalConfigGeneration); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPromotionDisabled
			}
			return fmt.Errorf("lock promotion global capacity: %w", err)
		}
		if globalConfigGeneration == 0 || globalMax == 0 {
			return ErrPromotionDisabled
		}
		if globalConfigGeneration != tenant.ConfigGeneration {
			return ErrPromotionRecoveryRequired
		}
		if globalCount >= globalMax {
			return ErrPromotionIdentityCapacityUnavailable
		}
		var lastIssued, retiredThrough uint64
		var codecVersion, epochState string
		if err := tx.QueryRowContext(ctx, `SELECT last_issued_sequence, retired_through, id_codec_version, epoch_state
			FROM promotion_import_identity_epochs
			WHERE tenant_id = ? AND allocation_epoch = ? FOR UPDATE`, req.TenantID, tenant.AllocationEpoch).
			Scan(&lastIssued, &retiredThrough, &codecVersion, &epochState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPromotionDisabled
			}
			return fmt.Errorf("lock promotion allocation epoch: %w", err)
		}
		if codecVersion != "p1" || epochState != "ACTIVE" || lastIssued < retiredThrough || lastIssued == math.MaxUint64 {
			return ErrPromotionRecoveryRequired
		}
		if lastIssued-retiredThrough >= tenant.MaxSequenceWindow {
			return ErrPromotionIdentityBudgetExceeded
		}
		sequence := lastIssued + 1
		identity := promotion.MigrationIdentity{AllocationEpoch: tenant.AllocationEpoch, AllocationSequence: sequence}
		migrationID, err := promotion.EncodeMigrationID(identity)
		if err != nil {
			return err
		}
		decoded, err := promotion.DecodeMigrationID(migrationID)
		if err != nil || decoded != identity {
			return ErrPromotionRecoveryRequired
		}
		createBefore := now.Add(s.cfg.AllocationTTL)
		claims := promotion.AllocationProofClaims{
			Version:                      "p1",
			TenantID:                     req.TenantID,
			MigrationID:                  migrationID,
			AllocationEpoch:              identity.AllocationEpoch,
			AllocationSequence:           identity.AllocationSequence,
			Target:                       target,
			ExpectedTargetAbsent:         true,
			AllocationIdempotencyKeyHash: idempotencyHash,
			AllocationRequestDigest:      requestDigest,
			CreateBeforeUnixMilli:        createBefore.UnixMilli(),
		}
		proof, err := s.cfg.ProofSigner.Sign(claims)
		if err != nil {
			return err
		}
		proofDigest := promotionHashString(proof)
		if _, err := tx.ExecContext(ctx, `INSERT INTO promotion_import_id_claims
			(tenant_id, allocation_epoch, allocation_sequence, target_path, target_path_hash,
			 expected_target_absent, allocation_idempotency_key_hash, allocation_request_digest,
			 allocation_proof_blob, allocation_proof_digest, create_before, claim_state)
			VALUES (?, ?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, 'ALLOCATED')`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence, target, fileNodePathHash(target),
			idempotencyHash, requestDigest, proof, proofDigest, createBefore); err != nil {
			return fmt.Errorf("insert promotion allocation claim: %w", err)
		}
		bucketRaw, err := json.Marshal(bucket)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_epochs
			SET last_issued_sequence = ? WHERE tenant_id = ? AND allocation_epoch = ?`,
			sequence, req.TenantID, identity.AllocationEpoch); err != nil {
			return fmt.Errorf("advance promotion allocation sequence: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_tenants
			SET live_claim_count = live_claim_count + 1,
			    materialized_identity_count = materialized_identity_count + 1,
			    rate_bucket_state = ?
			WHERE tenant_id = ?`, string(bucketRaw), req.TenantID); err != nil {
			return fmt.Errorf("charge promotion tenant identity: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_global
			SET materialized_identity_count = materialized_identity_count + 1 WHERE capacity_key = ?`,
			promotionGlobalCapacityKey); err != nil {
			return fmt.Errorf("charge promotion global identity: %w", err)
		}
		out = &PromotionAllocation{
			MigrationID: migrationID, AllocationEpoch: identity.AllocationEpoch,
			AllocationSequence: identity.AllocationSequence, AllocationProof: proof, CreateBefore: createBefore,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CreateImport recovers an accepted row before reapplying first-admission
// checks. A first create atomically accepts the claim, reserves quota, and
// persists every hidden manifest entry.
func (s *PromotionStore) CreateImport(ctx context.Context, req PromotionCreateRequest) (*PromotionImport, error) {
	target, err := promotion.CanonicalTarget(req.Target)
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Authorizer.AuthorizePromotionWrite(ctx, req.TenantID, target); err != nil {
		return nil, err
	}
	if !req.ExpectedTargetAbsent || req.OwnerToken == "" || req.RecoveryToken == "" || req.WriterLease == "" || len(req.OwnerToken) > 512 || len(req.RecoveryToken) > 512 {
		return nil, ErrPromotionConflict
	}
	identity, err := promotion.DecodeMigrationID(req.MigrationID)
	if err != nil {
		return nil, ErrNotFound
	}
	claims, err := s.cfg.ProofSigner.Verify(req.AllocationProof)
	if err != nil || claims.TenantID != req.TenantID || claims.MigrationID != req.MigrationID ||
		claims.AllocationEpoch != identity.AllocationEpoch || claims.AllocationSequence != identity.AllocationSequence ||
		claims.Target != target || !claims.ExpectedTargetAbsent {
		return nil, ErrNotFound
	}
	ownerHash := promotionHashString(req.OwnerToken)
	recoveryHash := promotionHashString(req.RecoveryToken)
	proofDigest := promotionHashString(req.AllocationProof)
	createDigest, err := promotionCreateRequestDigest(req, target, proofDigest, ownerHash, recoveryHash)
	if err != nil {
		return nil, err
	}
	var out *PromotionImport
	err = s.store.InTx(ctx, func(tx *sql.Tx) error {
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
		if existing, existingDigest, existingOwner, existingRecovery, err := s.selectPromotionImportTx(ctx, tx, req.TenantID, identity); err == nil {
			if existingDigest != createDigest || !promotionHashEqual(existingOwner, ownerHash) || !promotionHashEqual(existingRecovery, recoveryHash) {
				return ErrPromotionConflict
			}
			out = existing
			return nil
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		if recovered, found, err := s.selectPromotionCreateTerminalRecoveryTx(ctx, tx, req.TenantID, identity, createDigest, ownerHash, recoveryHash); err != nil {
			return err
		} else if found {
			out = recovered
			return nil
		}
		if identity.AllocationEpoch != tenant.AllocationEpoch {
			return ErrPromotionRecoveryRequired
		}

		var claimState, claimProofDigest, claimRequestDigest, claimTarget string
		var expectedAbsent bool
		var createBefore time.Time
		if err := tx.QueryRowContext(ctx, `SELECT target_path, expected_target_absent, allocation_request_digest,
			allocation_proof_digest, create_before, claim_state
			FROM promotion_import_id_claims
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence).
			Scan(&claimTarget, &expectedAbsent, &claimRequestDigest, &claimProofDigest, &createBefore, &claimState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPromotionRecoveryRequired
			}
			return fmt.Errorf("lock promotion allocation claim: %w", err)
		}
		if claimState == "RETIRED" {
			return ErrPromotionIDRetired
		}
		if claimState == "ACCEPTED" {
			return ErrPromotionRecoveryRequired
		}
		if claimState != "ALLOCATED" || claimTarget != target || !expectedAbsent ||
			claimRequestDigest != claims.AllocationRequestDigest || !promotionHashEqual(claimProofDigest, proofDigest) ||
			claims.CreateBefore().UnixMilli() != createBefore.UnixMilli() {
			return ErrPromotionConflict
		}
		if !now.Before(createBefore) {
			return ErrPromotionIDRetired
		}

		capability, err := s.lockPromotionStorageCapability(ctx, tx, req.TenantID)
		if err != nil {
			return err
		}
		if err := s.requireRuntimeCapability(req.TenantID, capability); err != nil {
			return err
		}
		manifest, err := s.cfg.Planner.VerifyCreatePlan(promotion.PlanImportRequest{
			TenantID: req.TenantID, Target: target, ExpectedTargetAbsent: true, Manifest: req.Manifest,
		}, req.Plan, capability)
		if err != nil {
			return err
		}

		if !namespace.ready || namespace.admissionState != "ACTIVE" || namespace.epoch == 0 {
			return ErrPromotionDisabled
		}
		if namespace.restoreGeneration != tenant.RestoreGeneration || namespace.databaseIncarnation != tenant.DatabaseIncarnation ||
			namespace.writerGeneration != tenant.WriterGeneration {
			return ErrPromotionRestoreFenced
		}
		parent, err := s.resolvePromotionTargetParentTx(ctx, tx, target, namespace)
		if err != nil {
			return err
		}
		present, err := promotionTargetChildPresentTx(ctx, tx, parent.path, pathutil.BaseName(target))
		if err != nil {
			return err
		}
		if present {
			return ErrPathConflict
		}

		reservationID, err := newPromotionID("prq")
		if err != nil {
			return err
		}
		activityDeadline := now.Add(s.cfg.ActivityTTL)
		leaseExpiresAt := now.Add(s.cfg.LeaseTTL)
		reservedFiles := countRegularFiles(manifest.Entries)
		if err := s.reservePromotionQuotaTx(ctx, tx, req.TenantID, manifest.ByteTotal, reservedFiles); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO promotion_imports
			(tenant_id, allocation_epoch, allocation_sequence, allocation_proof_digest,
			 target_path, target_path_hash, target_parent_inode, target_parent_path,
			 target_parent_path_hash, target_parent_edge_incarnation, target_parent_children_generation,
			 manifest_hash, entry_total, byte_total, max_content_size, storage_mode,
			 storage_plan_digest, backend_capability_generation, inline_threshold, plan_expires_at,
			 create_request_digest, namespace_cas_epoch, accepted_restore_generation,
			 accepted_database_incarnation, accepted_writer_generation, quota_reservation_id,
			 state, state_version, owner_epoch, owner_token_hash, recovery_token_hash,
			 activity_deadline, lease_expires_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			        'CREATED', 1, 1, ?, ?, ?, ?)`,
			req.TenantID, identity.AllocationEpoch, identity.AllocationSequence, proofDigest,
			target, fileNodePathHash(target), parent.inode, parent.path, fileNodePathHash(parent.path),
			parent.edgeIncarnation, parent.childrenGeneration,
			manifest.ManifestHash, manifest.EntryTotal, manifest.ByteTotal, manifest.MaxContentSize,
			string(req.Plan.StorageMode), req.Plan.StoragePlanDigest, req.Plan.BackendCapabilityGeneration,
			req.Plan.Limits.InlineThreshold, req.Plan.PlanExpiresAt, createDigest, namespace.epoch,
			tenant.RestoreGeneration, tenant.DatabaseIncarnation, tenant.WriterGeneration, reservationID,
			ownerHash, recoveryHash, activityDeadline, leaseExpiresAt); err != nil {
			return fmt.Errorf("insert promotion import: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO promotion_quota_reservations
			(tenant_id, reservation_id, allocation_epoch, allocation_sequence, reserved_bytes, reserved_files, state, state_version)
			VALUES (?, ?, ?, ?, ?, ?, 'RESERVED', 1)`, req.TenantID, reservationID,
			identity.AllocationEpoch, identity.AllocationSequence, manifest.ByteTotal, reservedFiles); err != nil {
			return fmt.Errorf("insert promotion quota reservation: %w", err)
		}
		for _, entry := range manifest.Entries {
			if _, err := tx.ExecContext(ctx, `INSERT INTO promotion_import_entries
				(tenant_id, allocation_epoch, allocation_sequence, relative_path_hash, relative_path,
				 entry_type, mode, mtime_ns, symlink_target, expected_size_bytes,
				 expected_checksum_sha256, entry_hash)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				req.TenantID, identity.AllocationEpoch, identity.AllocationSequence,
				promotionHashString(entry.RelativePath), entry.RelativePath, string(entry.Type), entry.Mode,
				entry.MtimeNS, nullPromotionString(entry.SymlinkTarget), entry.ExpectedSizeBytes,
				entry.ExpectedChecksumSHA256, entry.EntryHash); err != nil {
				return fmt.Errorf("insert promotion manifest entry %q: %w", entry.RelativePath, err)
			}
		}
		result, err := tx.ExecContext(ctx, `UPDATE promotion_import_id_claims
			SET claim_state = 'ACCEPTED', accepted_create_request_digest = ?
			WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? AND claim_state = 'ALLOCATED'`,
			createDigest, req.TenantID, identity.AllocationEpoch, identity.AllocationSequence)
		if err != nil {
			return fmt.Errorf("accept promotion allocation claim: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionConflict
		}
		transfer, err := tx.ExecContext(ctx, `UPDATE promotion_import_identity_tenants
			SET live_claim_count = live_claim_count - 1
			WHERE tenant_id = ? AND live_claim_count > 0`, req.TenantID)
		if err != nil {
			return fmt.Errorf("transfer promotion live claim: %w", err)
		}
		if affected, err := transfer.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionRecoveryRequired
		}
		out = &PromotionImport{
			MigrationID: req.MigrationID, AllocationEpoch: identity.AllocationEpoch, AllocationSequence: identity.AllocationSequence,
			Target: target, ManifestHash: manifest.ManifestHash, QuotaReservationID: reservationID,
			State: "CREATED", StateVersion: 1, OwnerEpoch: 1, ActivityDeadline: activityDeadline,
			LeaseExpiresAt: leaseExpiresAt, RestoreGeneration: tenant.RestoreGeneration,
			DatabaseIncarnation: tenant.DatabaseIncarnation, WriterGeneration: tenant.WriterGeneration,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PromotionStore) reservePromotionQuotaTx(ctx context.Context, tx *sql.Tx, tenantID string, bytes, files uint64) error {
	var maxBytes, maxFiles, reservedBytes, reservedFiles, committedBytes, committedFiles uint64
	err := tx.QueryRowContext(ctx, `SELECT max_bytes, max_files, reserved_bytes, reserved_files,
		committed_bytes, committed_files FROM promotion_quota_accounts
		WHERE tenant_id = ? FOR UPDATE`, tenantID).
		Scan(&maxBytes, &maxFiles, &reservedBytes, &reservedFiles, &committedBytes, &committedFiles)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrPromotionDisabled
	}
	if err != nil {
		return fmt.Errorf("lock promotion quota account: %w", err)
	}
	if reservedBytes > maxBytes || committedBytes > maxBytes-reservedBytes || bytes > maxBytes-reservedBytes-committedBytes ||
		reservedFiles > maxFiles || committedFiles > maxFiles-reservedFiles || files > maxFiles-reservedFiles-committedFiles {
		return ErrPromotionQuotaExceeded
	}
	// The locked account row above is the quota admission point. A
	// metadata-only manifest has nothing to add to its counters, and MySQL may
	// report zero affected rows for an UPDATE that adds zero. Treat that valid
	// no-op as an admitted zero-sized reservation rather than a recovery fault.
	if bytes == 0 && files == 0 {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE promotion_quota_accounts
		SET reserved_bytes = reserved_bytes + ?, reserved_files = reserved_files + ?
		WHERE tenant_id = ?`, bytes, files, tenantID)
	if err != nil {
		return fmt.Errorf("reserve promotion quota: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrPromotionRecoveryRequired
	}
	return nil
}

func (s *PromotionStore) readPromotionStorageCapability(ctx context.Context, tenantID string) (promotion.StorageCapability, error) {
	var capability promotion.StorageCapability
	var allowedMode string
	err := s.store.db.QueryRowContext(ctx, `SELECT generation, config_generation, inline_enabled,
		inline_threshold_bytes, allowed_mode FROM promotion_storage_capabilities WHERE tenant_id = ?`, tenantID).
		Scan(&capability.Generation, &capability.ConfigGeneration, &capability.InlineEnabled, &capability.InlineThreshold, &allowedMode)
	if errors.Is(err, sql.ErrNoRows) {
		return promotion.StorageCapability{}, ErrPromotionDisabled
	}
	if err != nil {
		return promotion.StorageCapability{}, fmt.Errorf("read promotion storage capability: %w", err)
	}
	capability.AllowedMode = promotion.StorageMode(allowedMode)
	return capability, nil
}

func (s *PromotionStore) lockPromotionStorageCapability(ctx context.Context, tx *sql.Tx, tenantID string) (promotion.StorageCapability, error) {
	var capability promotion.StorageCapability
	var allowedMode string
	err := tx.QueryRowContext(ctx, `SELECT generation, config_generation, inline_enabled,
		inline_threshold_bytes, allowed_mode FROM promotion_storage_capabilities WHERE tenant_id = ? FOR UPDATE`, tenantID).
		Scan(&capability.Generation, &capability.ConfigGeneration, &capability.InlineEnabled, &capability.InlineThreshold, &allowedMode)
	if errors.Is(err, sql.ErrNoRows) {
		return promotion.StorageCapability{}, ErrPromotionDisabled
	}
	if err != nil {
		return promotion.StorageCapability{}, fmt.Errorf("lock promotion storage capability: %w", err)
	}
	capability.AllowedMode = promotion.StorageMode(allowedMode)
	return capability, nil
}

func (s *PromotionStore) requireRuntimeCapability(tenantID string, durable promotion.StorageCapability) error {
	runtime := s.cfg.RuntimeCapability(tenantID)
	if runtime != durable {
		return promotion.ErrPlanStale
	}
	if !runtime.InlineEnabled || runtime.AllowedMode != promotion.StorageModeDB9Inline {
		return promotion.ErrStorageBackendUnsupported
	}
	return nil
}

func (s *PromotionStore) lockPromotionIdentityTenant(ctx context.Context, tx *sql.Tx, tenantID string) (promotionIdentityTenant, error) {
	var out promotionIdentityTenant
	err := tx.QueryRowContext(ctx, `SELECT installed_allocation_epoch, installed_restore_generation,
		installed_database_incarnation, installed_writer_generation, installed_allocation_lease_generation, restore_admission_state,
		live_claim_count, materialized_identity_count, rate_bucket_state,
		max_live_claims, max_materialized_identities, max_sequence_window,
		allocation_rate_per_minute, allocation_rate_burst, config_generation
		FROM promotion_import_identity_tenants WHERE tenant_id = ? FOR UPDATE`, tenantID).
		Scan(&out.AllocationEpoch, &out.RestoreGeneration, &out.DatabaseIncarnation, &out.WriterGeneration,
			&out.AllocationLeaseGeneration, &out.AdmissionState, &out.LiveClaimCount, &out.MaterializedCount, &out.RateBucketRaw,
			&out.MaxLiveClaims, &out.MaxMaterializedIdentities, &out.MaxSequenceWindow,
			&out.AllocationRatePerMinute, &out.AllocationRateBurst, &out.ConfigGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return promotionIdentityTenant{}, ErrPromotionDisabled
	}
	if err != nil {
		return promotionIdentityTenant{}, fmt.Errorf("lock promotion identity tenant: %w", err)
	}
	return out, nil
}

func promotionDatabaseTime(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT CURRENT_TIMESTAMP(3)`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read promotion database time: %w", err)
	}
	return now.UTC().Truncate(time.Millisecond), nil
}

func (s *PromotionStore) requirePromotionAllocationLease(ctx context.Context, tenantID, token string, tenant promotionIdentityTenant, dbNow time.Time) error {
	lease, err := s.cfg.LeaseVerifier.VerifyAllocationLease(ctx, token)
	if err != nil {
		return ErrPromotionIdentityEpochUnavailable
	}
	if tenant.AdmissionState != "ACTIVE" || lease.TenantID != tenantID ||
		lease.AllocationEpoch != tenant.AllocationEpoch || lease.RestoreGeneration != tenant.RestoreGeneration ||
		lease.DatabaseIncarnation != tenant.DatabaseIncarnation ||
		lease.AllocationLeaseGeneration != tenant.AllocationLeaseGeneration ||
		!dbNow.Before(lease.NotAfter.UTC()) {
		return ErrPromotionIdentityEpochUnavailable
	}
	return nil
}

func (s *PromotionStore) requirePromotionNormalWriterLease(ctx context.Context, tenantID, token string, tenant promotionIdentityTenant, namespace promotionNamespaceCapability, dbNow time.Time) error {
	lease, err := s.cfg.LeaseVerifier.VerifyNormalWriterLease(ctx, token)
	if err != nil {
		return ErrPromotionRestoreFenced
	}
	if tenant.AdmissionState != "ACTIVE" || namespace.admissionState != "ACTIVE" ||
		lease.TenantID != tenantID || lease.AllocationEpoch != tenant.AllocationEpoch ||
		lease.RestoreGeneration != tenant.RestoreGeneration || lease.RestoreGeneration != namespace.restoreGeneration ||
		lease.DatabaseIncarnation != tenant.DatabaseIncarnation || lease.DatabaseIncarnation != namespace.databaseIncarnation ||
		lease.WriterGeneration != tenant.WriterGeneration || lease.WriterGeneration != namespace.writerGeneration ||
		!dbNow.Before(lease.NotAfter.UTC()) {
		return ErrPromotionRestoreFenced
	}
	return nil
}

type promotionNamespaceCapability struct {
	ready                  bool
	epoch                  uint64
	minimumWriterProtocol  uint64
	restoreGeneration      uint64
	databaseIncarnation    string
	writerGeneration       uint64
	admissionState         string
	rootInode              string
	rootEdgeIncarnation    string
	rootChildrenGeneration uint64
}

func (s *PromotionStore) readPromotionNamespaceCapability(ctx context.Context, tenantID string) (promotionNamespaceCapability, error) {
	var out promotionNamespaceCapability
	err := s.store.db.QueryRowContext(ctx, `SELECT namespace_cas_ready, namespace_cas_epoch,
		minimum_writer_protocol, restore_generation, database_incarnation, writer_generation,
		admission_state, root_inode, root_edge_incarnation, root_children_generation
		FROM promotion_namespace_capabilities WHERE tenant_id = ?`, tenantID).
		Scan(&out.ready, &out.epoch, &out.minimumWriterProtocol, &out.restoreGeneration,
			&out.databaseIncarnation, &out.writerGeneration, &out.admissionState, &out.rootInode,
			&out.rootEdgeIncarnation, &out.rootChildrenGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return promotionNamespaceCapability{}, ErrPromotionDisabled
	}
	if err != nil {
		return promotionNamespaceCapability{}, fmt.Errorf("read promotion namespace capability: %w", err)
	}
	return out, nil
}

func (s *PromotionStore) requirePromotionWriterProtocol(namespace promotionNamespaceCapability) error {
	if namespace.minimumWriterProtocol == 0 || s.cfg.WriterProtocol < namespace.minimumWriterProtocol {
		return ErrPromotionRestoreFenced
	}
	return nil
}

func (s *PromotionStore) lockPromotionNamespaceCapability(ctx context.Context, tx *sql.Tx, tenantID string) (promotionNamespaceCapability, error) {
	var out promotionNamespaceCapability
	err := tx.QueryRowContext(ctx, `SELECT namespace_cas_ready, namespace_cas_epoch, minimum_writer_protocol, restore_generation,
		database_incarnation, writer_generation, admission_state, root_inode,
		root_edge_incarnation, root_children_generation
		FROM promotion_namespace_capabilities WHERE tenant_id = ? FOR UPDATE`, tenantID).
		Scan(&out.ready, &out.epoch, &out.minimumWriterProtocol, &out.restoreGeneration, &out.databaseIncarnation, &out.writerGeneration,
			&out.admissionState, &out.rootInode, &out.rootEdgeIncarnation, &out.rootChildrenGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return promotionNamespaceCapability{}, ErrPromotionDisabled
	}
	if err != nil {
		return promotionNamespaceCapability{}, fmt.Errorf("lock promotion namespace capability: %w", err)
	}
	return out, nil
}

type promotionTargetParent struct {
	inode              string
	path               string
	edgeIncarnation    string
	childrenGeneration uint64
}

func (s *PromotionStore) resolvePromotionTargetParentTx(ctx context.Context, tx *sql.Tx, target string, namespace promotionNamespaceCapability) (promotionTargetParent, error) {
	parentPath := pathutil.ParentPath(target)
	if parentPath == "/" {
		if namespace.rootInode == "" || namespace.rootEdgeIncarnation == "" {
			return promotionTargetParent{}, ErrPromotionDisabled
		}
		return promotionTargetParent{inode: namespace.rootInode, path: "/", edgeIncarnation: namespace.rootEdgeIncarnation, childrenGeneration: namespace.rootChildrenGeneration}, nil
	}
	var parent promotionTargetParent
	var isDirectory bool
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(inode_id, ''), NULLIF(file_id, ''), node_id),
		path, is_directory, path_edge_incarnation, children_generation
		FROM file_nodes WHERE path_hash = ? AND path = ? FOR UPDATE`, fileNodePathHash(parentPath), parentPath).
		Scan(&parent.inode, &parent.path, &isDirectory, &parent.edgeIncarnation, &parent.childrenGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return promotionTargetParent{}, ErrNotFound
	}
	if err != nil {
		return promotionTargetParent{}, fmt.Errorf("lock promotion target parent: %w", err)
	}
	if !isDirectory || parent.edgeIncarnation == "" {
		return promotionTargetParent{}, ErrPromotionDisabled
	}
	return parent, nil
}

type promotionAllocationRow struct {
	target               string
	expectedTargetAbsent bool
	requestDigest        string
	response             *PromotionAllocation
}

func (s *PromotionStore) selectPromotionAllocationByIdempotency(ctx context.Context, tx *sql.Tx, tenantID, keyHash string) (*promotionAllocationRow, error) {
	var row promotionAllocationRow
	row.response = &PromotionAllocation{}
	err := tx.QueryRowContext(ctx, `SELECT allocation_epoch, allocation_sequence, target_path,
		expected_target_absent, allocation_request_digest, allocation_proof_blob, create_before
		FROM promotion_import_id_claims
		WHERE tenant_id = ? AND allocation_idempotency_key_hash = ? FOR UPDATE`, tenantID, keyHash).
		Scan(&row.response.AllocationEpoch, &row.response.AllocationSequence, &row.target,
			&row.expectedTargetAbsent, &row.requestDigest, &row.response.AllocationProof, &row.response.CreateBefore)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read promotion allocation retry: %w", err)
	}
	migrationID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{
		AllocationEpoch: row.response.AllocationEpoch, AllocationSequence: row.response.AllocationSequence,
	})
	if err != nil {
		return nil, ErrPromotionRecoveryRequired
	}
	row.response.MigrationID = migrationID
	claims, err := s.cfg.ProofSigner.Verify(row.response.AllocationProof)
	if err != nil || claims.TenantID != tenantID || claims.MigrationID != migrationID ||
		claims.AllocationEpoch != row.response.AllocationEpoch || claims.AllocationSequence != row.response.AllocationSequence ||
		claims.Target != row.target || claims.ExpectedTargetAbsent != row.expectedTargetAbsent ||
		claims.AllocationIdempotencyKeyHash != keyHash || claims.AllocationRequestDigest != row.requestDigest ||
		claims.CreateBeforeUnixMilli != row.response.CreateBefore.UnixMilli() {
		return nil, ErrPromotionRecoveryRequired
	}
	return &row, nil
}

func (s *PromotionStore) selectPromotionImportTx(ctx context.Context, tx *sql.Tx, tenantID string, identity promotion.MigrationIdentity) (*PromotionImport, string, string, string, error) {
	var out PromotionImport
	var createDigest, ownerHash, recoveryHash string
	var terminalResultBlob, terminalResultDigest sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT target_path, manifest_hash, quota_reservation_id, state,
		state_version, owner_epoch, activity_deadline, lease_expires_at,
		accepted_restore_generation, accepted_database_incarnation, accepted_writer_generation,
		create_request_digest, owner_token_hash, recovery_token_hash,
		terminal_result_blob, terminal_result_digest
		FROM promotion_imports
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&out.Target, &out.ManifestHash, &out.QuotaReservationID, &out.State, &out.StateVersion,
			&out.OwnerEpoch, &out.ActivityDeadline, &out.LeaseExpiresAt, &out.RestoreGeneration,
			&out.DatabaseIncarnation, &out.WriterGeneration, &createDigest, &ownerHash, &recoveryHash,
			&terminalResultBlob, &terminalResultDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", "", "", ErrNotFound
	}
	if err != nil {
		return nil, "", "", "", fmt.Errorf("read promotion import: %w", err)
	}
	out.AllocationEpoch = identity.AllocationEpoch
	out.AllocationSequence = identity.AllocationSequence
	out.MigrationID, _ = promotion.EncodeMigrationID(identity)
	if terminalResultBlob.Valid {
		out.TerminalResultBlob = terminalResultBlob.String
	}
	if terminalResultDigest.Valid {
		out.TerminalResultDigest = terminalResultDigest.String
	}
	if err := validatePromotionTerminalResult(out.State, terminalResultBlob, terminalResultDigest); err != nil {
		return nil, "", "", "", err
	}
	return &out, createDigest, ownerHash, recoveryHash, nil
}

func (s *PromotionStore) selectPromotionCreateTerminalRecoveryTx(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	identity promotion.MigrationIdentity,
	createDigest, ownerHash, recoveryHash string,
) (*PromotionImport, bool, error) {
	var target, requestDigest, storedOwnerHash, storedRecoveryHash, terminalState, terminalResultBlob, terminalResultDigest string
	err := tx.QueryRowContext(ctx, `SELECT target_path, request_digest, owner_token_hash,
		recovery_token_hash, terminal_state, terminal_result_blob, terminal_result_digest
		FROM promotion_import_tombstones
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).
		Scan(&target, &requestDigest, &storedOwnerHash, &storedRecoveryHash, &terminalState,
			&terminalResultBlob, &terminalResultDigest)
	if err == nil {
		if requestDigest != createDigest || !promotionHashEqual(storedOwnerHash, ownerHash) || !promotionHashEqual(storedRecoveryHash, recoveryHash) {
			return nil, false, ErrPromotionConflict
		}
		if err := validatePromotionTerminalResult(terminalState,
			sql.NullString{String: terminalResultBlob, Valid: true},
			sql.NullString{String: terminalResultDigest, Valid: true}); err != nil {
			return nil, false, err
		}
		migrationID, encodeErr := promotion.EncodeMigrationID(identity)
		if encodeErr != nil {
			return nil, false, ErrPromotionRecoveryRequired
		}
		return &PromotionImport{
			MigrationID: migrationID, AllocationEpoch: identity.AllocationEpoch,
			AllocationSequence: identity.AllocationSequence, Target: target, State: terminalState,
			TerminalResultBlob: terminalResultBlob, TerminalResultDigest: terminalResultDigest,
		}, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, fmt.Errorf("read promotion terminal recovery: %w", err)
	}

	var retiredThrough uint64
	err = tx.QueryRowContext(ctx, `SELECT retired_through
		FROM promotion_import_identity_epochs
		WHERE tenant_id = ? AND allocation_epoch = ? FOR UPDATE`, tenantID, identity.AllocationEpoch).
		Scan(&retiredThrough)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, ErrPromotionRecoveryRequired
	}
	if err != nil {
		return nil, false, fmt.Errorf("read promotion retirement watermark: %w", err)
	}
	if identity.AllocationSequence <= retiredThrough {
		return nil, false, ErrPromotionIDRetired
	}
	var retired int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM promotion_retired_import_sequences
		WHERE tenant_id = ? AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		tenantID, identity.AllocationEpoch, identity.AllocationSequence).Scan(&retired)
	if err == nil {
		return nil, false, ErrPromotionIDRetired
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, fmt.Errorf("read promotion retired sequence: %w", err)
	}
	return nil, false, nil
}

func validatePromotionTerminalResult(state string, blob, digest sql.NullString) error {
	switch state {
	case "COMMITTED", "ABORTED":
		if !blob.Valid || strings.TrimSpace(blob.String) == "" || !json.Valid([]byte(blob.String)) ||
			!digest.Valid || !validPromotionSHA256(digest.String) ||
			!promotionHashEqual(promotionHashString(blob.String), digest.String) {
			return ErrPromotionRecoveryRequired
		}
	case "CREATED", "STAGING", "VERIFIED", "COMMITTING", "ABORTING":
		if blob.Valid || digest.Valid {
			return ErrPromotionRecoveryRequired
		}
	default:
		return ErrPromotionRecoveryRequired
	}
	return nil
}

func consumePromotionRate(raw sql.NullString, now time.Time, ratePerMinute, burst uint64) (promotionRateBucket, error) {
	if ratePerMinute == 0 || burst == 0 || burst > math.MaxUint64/1000 {
		return promotionRateBucket{}, ErrPromotionDisabled
	}
	capacity := burst * 1000
	bucket := promotionRateBucket{TokensMilli: capacity, UpdatedUnixMilli: now.UnixMilli()}
	if raw.Valid && strings.TrimSpace(raw.String) != "" {
		if err := json.Unmarshal([]byte(raw.String), &bucket); err != nil {
			return promotionRateBucket{}, ErrPromotionRecoveryRequired
		}
		if bucket.UpdatedUnixMilli > now.UnixMilli() || bucket.TokensMilli > capacity {
			return promotionRateBucket{}, ErrPromotionRecoveryRequired
		}
		elapsed := uint64(now.UnixMilli() - bucket.UpdatedUnixMilli)
		var refill uint64
		if elapsed > math.MaxUint64/ratePerMinute {
			refill = capacity
		} else {
			refill = elapsed * ratePerMinute / 60
		}
		if refill >= capacity-bucket.TokensMilli {
			bucket.TokensMilli = capacity
		} else {
			bucket.TokensMilli += refill
		}
		bucket.UpdatedUnixMilli = now.UnixMilli()
	}
	if bucket.TokensMilli < 1000 {
		return promotionRateBucket{}, ErrPromotionIdentityBudgetExceeded
	}
	bucket.TokensMilli -= 1000
	return bucket, nil
}

func promotionCreateRequestDigest(req PromotionCreateRequest, target, proofDigest, ownerHash, recoveryHash string) (string, error) {
	return promotionJSONHash(struct {
		Version               string                    `json:"version"`
		TenantID              string                    `json:"tenant_id"`
		MigrationID           string                    `json:"migration_id"`
		AllocationProofDigest string                    `json:"allocation_proof_digest"`
		Target                string                    `json:"target"`
		ExpectedTargetAbsent  bool                      `json:"expected_target_absent"`
		Manifest              []promotion.ManifestEntry `json:"manifest"`
		Plan                  promotion.ImportPlan      `json:"plan"`
		OwnerTokenHash        string                    `json:"owner_token_hash"`
		RecoveryTokenHash     string                    `json:"recovery_token_hash"`
	}{
		Version: "p1", TenantID: req.TenantID, MigrationID: req.MigrationID,
		AllocationProofDigest: proofDigest, Target: target, ExpectedTargetAbsent: true,
		Manifest: req.Manifest, Plan: req.Plan, OwnerTokenHash: ownerHash, RecoveryTokenHash: recoveryHash,
	})
}

func promotionJSONHash(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal promotion digest input: %w", err)
	}
	return promotionHashBytes(raw), nil
}

func promotionHashString(value string) string { return promotionHashBytes([]byte(value)) }

func promotionHashBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func promotionHashEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func validPromotionSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func newPromotionID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate promotion id: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(raw[:]), nil
}

func countRegularFiles(entries []promotion.ManifestEntry) uint64 {
	var count uint64
	for _, entry := range entries {
		if entry.Type == promotion.EntryTypeFile {
			count++
		}
	}
	return count
}

func nullPromotionString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
