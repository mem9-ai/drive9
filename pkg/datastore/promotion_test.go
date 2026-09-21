package datastore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

type promotionTestAuthorizer struct {
	mu      sync.Mutex
	denied  bool
	targets []string
}

type promotionTestLeaseVerifier struct {
	allocation PromotionAllocationLease
	writer     PromotionWriterLease
}

func (v *promotionTestLeaseVerifier) VerifyAllocationLease(_ context.Context, token string) (PromotionAllocationLease, error) {
	if token != "allocation-lease" {
		return PromotionAllocationLease{}, errors.New("invalid allocation lease")
	}
	return v.allocation, nil
}

func (v *promotionTestLeaseVerifier) VerifyNormalWriterLease(_ context.Context, token string) (PromotionWriterLease, error) {
	if token != "writer-lease" {
		return PromotionWriterLease{}, errors.New("invalid writer lease")
	}
	return v.writer, nil
}

func (a *promotionTestAuthorizer) AuthorizePromotionWrite(_ context.Context, _, target string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.targets = append(a.targets, target)
	if a.denied {
		return errors.New("scope denied")
	}
	return nil
}

func testPromotionLimits() promotion.ManifestLimits {
	return promotion.ManifestLimits{
		MaxEntries: 64, MaxMetadataBytes: 64 << 10, MaxTotalInlineBytes: 64 << 10,
		MaxPathBytes: 512, MaxSymlinkBytes: 512, MaxDepth: 16, InlineThreshold: 1024,
	}
}

func testPromotionManifest(t *testing.T) []promotion.ManifestEntry {
	t.Helper()
	manifest, err := promotion.CanonicalizeManifest([]promotion.ManifestEntry{
		{RelativePath: "empty", Type: promotion.EntryTypeDirectory, Mode: 0o755, MtimeNS: 1},
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, MtimeNS: 2, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
		{RelativePath: "link", Type: promotion.EntryTypeSymlink, Mode: 0o777, MtimeNS: 3, SymlinkTarget: "file"},
	}, testPromotionLimits())
	if err != nil {
		t.Fatal(err)
	}
	return manifest.Entries
}

func promotionTestHash(value string) string { return promotionHashString(value) }

func newTestPromotionStore(t *testing.T, now *time.Time) (*Store, *PromotionStore, *promotionTestAuthorizer, *promotion.StorageCapability) {
	t.Helper()
	store := newTestStore(t)
	authorizer := &promotionTestAuthorizer{}
	capability := &promotion.StorageCapability{
		Generation: 7, ConfigGeneration: 9, InlineEnabled: true,
		InlineThreshold: testPromotionLimits().InlineThreshold, AllowedMode: promotion.StorageModeDB9Inline,
	}
	planner, err := promotion.NewPlannerWithClock([]byte("0123456789abcdef0123456789abcdef"), 10*time.Minute, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	proofSigner, err := promotion.NewProofSigner([]byte("abcdef0123456789abcdef0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	leaseVerifier := &promotionTestLeaseVerifier{
		allocation: PromotionAllocationLease{
			TenantID: "tenant-a", AllocationEpoch: 2, RestoreGeneration: 5,
			DatabaseIncarnation: "db-inc-1", AllocationLeaseGeneration: 13,
			NotAfter: time.Now().UTC().Add(24 * time.Hour),
		},
		writer: PromotionWriterLease{
			TenantID: "tenant-a", AllocationEpoch: 2, RestoreGeneration: 5,
			DatabaseIncarnation: "db-inc-1", WriterGeneration: 7,
			NotAfter: time.Now().UTC().Add(24 * time.Hour),
		},
	}
	promotionStore, err := NewPromotionStore(store, PromotionStoreConfig{
		Planner: planner, ProofSigner: proofSigner, Authorizer: authorizer, LeaseVerifier: leaseVerifier,
		RuntimeCapability: func(string) promotion.StorageCapability { return *capability },
		WriterProtocol:    1,
		Limits:            testPromotionLimits(), AllocationTTL: 5 * time.Minute,
		ActivityTTL: time.Hour, LeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	seedPromotionCapability(t, store, "tenant-a", *capability)
	return store, promotionStore, authorizer, capability
}

func seedPromotionCapability(t *testing.T, store *Store, tenantID string, capability promotion.StorageCapability) {
	t.Helper()
	for _, table := range []string{
		"promotion_import_contents", "promotion_import_entries", "promotion_quota_reservations",
		"promotion_quota_accounts",
		"promotion_imports", "promotion_import_tombstones", "promotion_retired_import_sequences",
		"promotion_import_id_claims", "promotion_import_identity_epochs",
		"promotion_import_identity_tenants", "promotion_namespace_capabilities",
		"promotion_storage_capabilities", "promotion_import_identity_global",
	} {
		if _, err := store.DB().Exec("DELETE FROM " + table); err != nil {
			t.Fatalf("reset %s: %v", table, err)
		}
	}
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO promotion_storage_capabilities
			(tenant_id, generation, inline_enabled, inline_threshold_bytes, allowed_mode, config_generation)
			VALUES (?, ?, ?, ?, ?, ?)`, []any{tenantID, capability.Generation, capability.InlineEnabled, capability.InlineThreshold, string(capability.AllowedMode), capability.ConfigGeneration}},
		{`INSERT INTO promotion_namespace_capabilities
			(tenant_id, namespace_cas_ready, namespace_cas_epoch, minimum_writer_protocol,
			 restore_generation, database_incarnation, writer_generation, admission_state,
			 root_inode, root_edge_incarnation, root_children_generation)
			VALUES (?, TRUE, 3, 1, 5, 'db-inc-1', 7, 'ACTIVE', 'root-inode', 'root-edge-1', 11)`, []any{tenantID}},
		{`INSERT INTO promotion_import_identity_tenants
			(tenant_id, installed_allocation_epoch, installed_restore_generation,
			 installed_database_incarnation, installed_backup_lineage_id, installed_writer_generation,
			 installed_allocation_lease_generation, restore_admission_state,
			 live_claim_count, materialized_identity_count, max_live_claims,
			 max_materialized_identities, max_sequence_window,
			 allocation_rate_per_minute, allocation_rate_burst, config_generation)
			VALUES (?, 2, 5, 'db-inc-1', 'backup-lineage-1', 7, 13, 'ACTIVE', 0, 0,
			        8, 32, 16, 60, 8, 1)`, []any{tenantID}},
		{`INSERT INTO promotion_import_identity_epochs
			(tenant_id, allocation_epoch, last_issued_sequence, retired_through, id_codec_version, epoch_state)
			VALUES (?, 2, 0, 0, 'p1', 'ACTIVE')`, []any{tenantID}},
		{`INSERT INTO promotion_import_identity_global
			(capacity_key, materialized_identity_count, max_materialized_identities, config_generation)
			VALUES ('promotion-imports', 0, 128, 1)`, nil},
		{`INSERT INTO promotion_quota_accounts
			(tenant_id, max_bytes, max_files, reserved_bytes, reserved_files, committed_bytes, committed_files, config_generation)
			VALUES (?, 1048576, 1024, 0, 0, 0, 0, 1)`, []any{tenantID}},
	}
	for _, statement := range statements {
		if _, err := store.DB().Exec(statement.query, statement.args...); err != nil {
			t.Fatalf("seed promotion capability: %v", err)
		}
	}
}

func TestPromotionPlanImportIsSideEffectFree(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: testPromotionManifest(t),
	})
	if err != nil {
		t.Fatalf("PlanImport: %v", err)
	}
	if plan.StorageMode != promotion.StorageModeDB9Inline || plan.StoragePlanDigest == "" {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	for _, table := range []string{"promotion_import_id_claims", "promotion_imports", "promotion_import_entries", "promotion_quota_reservations"} {
		var count int
		if err := store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows after PlanImport = %d, want 0", table, count)
		}
	}
}

func TestPromotionAllocateRecoversBeforeCapacityAndRejectsMismatch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	req := PromotionAllocationRequest{TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, IdempotencyKey: "allocate-1", AllocationLease: "allocation-lease"}
	first, err := promotionStore.AllocateImportID(context.Background(), req)
	if err != nil {
		t.Fatalf("AllocateImportID: %v", err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_import_identity_tenants SET live_claim_count = 8 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	retry, err := promotionStore.AllocateImportID(context.Background(), req)
	if err != nil {
		t.Fatalf("AllocateImportID retry at capacity: %v", err)
	}
	if *retry != *first {
		t.Fatalf("retry = %+v, want %+v", retry, first)
	}
	mismatched := req
	mismatched.Target = "/other"
	if _, err := promotionStore.AllocateImportID(context.Background(), mismatched); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("mismatched retry error = %v, want ErrPromotionConflict", err)
	}
}

func TestPromotionAllocationRateLimitStillAllowsExactRetry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	if _, err := store.DB().Exec(`UPDATE promotion_import_identity_tenants
		SET allocation_rate_burst = 1 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	firstReq := PromotionAllocationRequest{TenantID: "tenant-a", Target: "/one", ExpectedTargetAbsent: true, IdempotencyKey: "rate-one", AllocationLease: "allocation-lease"}
	first, err := promotionStore.AllocateImportID(context.Background(), firstReq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/two", ExpectedTargetAbsent: true, IdempotencyKey: "rate-two", AllocationLease: "allocation-lease",
	}); !errors.Is(err, ErrPromotionIdentityBudgetExceeded) {
		t.Fatalf("second allocation error = %v, want ErrPromotionIdentityBudgetExceeded", err)
	}
	retry, err := promotionStore.AllocateImportID(context.Background(), firstReq)
	if err != nil {
		t.Fatalf("exact retry after rate limit: %v", err)
	}
	if *retry != *first {
		t.Fatalf("retry = %+v, want %+v", retry, first)
	}
}

func TestPromotionAllocationLeaseGuardPrecedesIdempotentRecovery(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	req := PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "lease-fence", AllocationLease: "allocation-lease",
	}
	if _, err := promotionStore.AllocateImportID(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_import_identity_tenants
		SET installed_allocation_lease_generation = 14 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.AllocateImportID(context.Background(), req); !errors.Is(err, ErrPromotionIdentityEpochUnavailable) {
		t.Fatalf("stale allocation lease retry error = %v, want ErrPromotionIdentityEpochUnavailable", err)
	}
}

func TestPromotionAllocationSequenceWindowIsBounded(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	if _, err := store.DB().Exec(`UPDATE promotion_import_identity_tenants
		SET max_sequence_window = 1 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	first := PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/one", ExpectedTargetAbsent: true,
		IdempotencyKey: "window-one", AllocationLease: "allocation-lease",
	}
	if _, err := promotionStore.AllocateImportID(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/two", ExpectedTargetAbsent: true,
		IdempotencyKey: "window-two", AllocationLease: "allocation-lease",
	}); !errors.Is(err, ErrPromotionIdentityBudgetExceeded) {
		t.Fatalf("sequence-window error = %v, want ErrPromotionIdentityBudgetExceeded", err)
	}
	if _, err := promotionStore.AllocateImportID(context.Background(), first); err != nil {
		t.Fatalf("exact retry at sequence-window limit: %v", err)
	}
}

func TestPromotionDurableIdentityConfigControlsAdmission(t *testing.T) {
	t.Run("tenant materialized limit", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		store, promotionStore, _, _ := newTestPromotionStore(t, &now)
		if _, err := store.DB().Exec(`UPDATE promotion_import_identity_tenants
			SET max_materialized_identities = 1 WHERE tenant_id = 'tenant-a'`); err != nil {
			t.Fatal(err)
		}
		if _, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
			TenantID: "tenant-a", Target: "/one", ExpectedTargetAbsent: true,
			IdempotencyKey: "tenant-cap-one", AllocationLease: "allocation-lease",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
			TenantID: "tenant-a", Target: "/two", ExpectedTargetAbsent: true,
			IdempotencyKey: "tenant-cap-two", AllocationLease: "allocation-lease",
		}); !errors.Is(err, ErrPromotionIdentityBudgetExceeded) {
			t.Fatalf("tenant materialized limit error = %v, want ErrPromotionIdentityBudgetExceeded", err)
		}
	})

	t.Run("global materialized limit", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		store, promotionStore, _, _ := newTestPromotionStore(t, &now)
		if _, err := store.DB().Exec(`UPDATE promotion_import_identity_global
			SET max_materialized_identities = 1 WHERE capacity_key = 'promotion-imports'`); err != nil {
			t.Fatal(err)
		}
		first := PromotionAllocationRequest{
			TenantID: "tenant-a", Target: "/one", ExpectedTargetAbsent: true,
			IdempotencyKey: "global-cap-one", AllocationLease: "allocation-lease",
		}
		if _, err := promotionStore.AllocateImportID(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		if _, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
			TenantID: "tenant-a", Target: "/two", ExpectedTargetAbsent: true,
			IdempotencyKey: "global-cap-two", AllocationLease: "allocation-lease",
		}); !errors.Is(err, ErrPromotionIdentityCapacityUnavailable) {
			t.Fatalf("global materialized limit error = %v, want ErrPromotionIdentityCapacityUnavailable", err)
		}
		if _, err := promotionStore.AllocateImportID(context.Background(), first); err != nil {
			t.Fatalf("exact retry at durable global limit: %v", err)
		}
	})

	t.Run("partial config rollout fails new admission", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		store, promotionStore, _, _ := newTestPromotionStore(t, &now)
		first := PromotionAllocationRequest{
			TenantID: "tenant-a", Target: "/one", ExpectedTargetAbsent: true,
			IdempotencyKey: "config-rollout-one", AllocationLease: "allocation-lease",
		}
		original, err := promotionStore.AllocateImportID(context.Background(), first)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.DB().Exec(`UPDATE promotion_import_identity_tenants
			SET config_generation = 2 WHERE tenant_id = 'tenant-a'`); err != nil {
			t.Fatal(err)
		}
		recovered, err := promotionStore.AllocateImportID(context.Background(), first)
		if err != nil {
			t.Fatalf("exact retry during config rollout: %v", err)
		}
		if *recovered != *original {
			t.Fatalf("recovered allocation = %+v, want %+v", recovered, original)
		}
		if _, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
			TenantID: "tenant-a", Target: "/two", ExpectedTargetAbsent: true,
			IdempotencyKey: "config-rollout-two", AllocationLease: "allocation-lease",
		}); !errors.Is(err, ErrPromotionRecoveryRequired) {
			t.Fatalf("partial config rollout error = %v, want ErrPromotionRecoveryRequired", err)
		}
		var lastIssued uint64
		if err := store.DB().QueryRow(`SELECT last_issued_sequence FROM promotion_import_identity_epochs
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = 2`).Scan(&lastIssued); err != nil {
			t.Fatal(err)
		}
		if lastIssued != 1 {
			t.Fatalf("last issued sequence after rejected rollout = %d, want 1", lastIssued)
		}
	})
}

func TestPromotionProofKeyRotationPreservesRetryAndCreate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	_, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "proof-rotation", AllocationLease: "allocation-lease",
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	oldKey := []byte("abcdef0123456789abcdef0123456789")
	rotated, err := promotion.NewProofSignerWithKeyring(
		"k2", []byte("new-proof-key-0123456789abcdef012345"), map[string][]byte{"k1": oldKey},
	)
	if err != nil {
		t.Fatal(err)
	}
	promotionStore.cfg.ProofSigner = rotated
	recovered, err := promotionStore.AllocateImportID(context.Background(), request)
	if err != nil {
		t.Fatalf("allocation retry after proof-key rotation: %v", err)
	}
	if *recovered != *allocation {
		t.Fatalf("recovered allocation = %+v, want %+v", recovered, allocation)
	}
	if _, err := promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}); err != nil {
		t.Fatalf("CreateImport with retained old proof key: %v", err)
	}
}

func TestPromotionScopeRevocationPreventsFirstCreateSideEffects(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, authorizer, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, IdempotencyKey: "allocate-revoke", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	authorizer.mu.Lock()
	authorizer.denied = true
	authorizer.mu.Unlock()
	_, err = promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	})
	if err == nil || errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("CreateImport after scope revoke error = %v, want authorization error", err)
	}
	assertPromotionCreateRows(t, store, 0, 0, 0)
}

func TestPromotionCreateAtomicallyPersistsManifestAndRecoversAfterRollout(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, runtimeCapability := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, IdempotencyKey: "allocate-create", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	create := PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}
	created, err := promotionStore.CreateImport(context.Background(), create)
	if err != nil {
		t.Fatalf("CreateImport: %v", err)
	}
	if created.State != "CREATED" || created.ManifestHash != plan.ManifestHash {
		t.Fatalf("created import = %+v", created)
	}
	assertPromotionCreateRows(t, store, 1, len(manifest), 1)
	assertPromotionReservedQuota(t, store, 5, 1)

	// Recovery must precede new-admission plan expiry and capability checks.
	now = plan.PlanExpiresAt.Add(time.Minute)
	runtimeCapability.Generation++
	if _, err := store.DB().Exec(`UPDATE promotion_storage_capabilities SET generation = ? WHERE tenant_id = 'tenant-a'`, runtimeCapability.Generation); err != nil {
		t.Fatal(err)
	}
	recovered, err := promotionStore.CreateImport(context.Background(), create)
	if err != nil {
		t.Fatalf("CreateImport recovery after rollout: %v", err)
	}
	if recovered.QuotaReservationID != created.QuotaReservationID {
		t.Fatalf("recovered reservation = %q, want %q", recovered.QuotaReservationID, created.QuotaReservationID)
	}
	assertPromotionCreateRows(t, store, 1, len(manifest), 1)
	assertPromotionReservedQuota(t, store, 5, 1)

	changedToken := create
	changedToken.OwnerToken = "another-owner"
	if _, err := promotionStore.CreateImport(context.Background(), changedToken); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("changed token error = %v, want ErrPromotionConflict", err)
	}
}

func TestPromotionWriterLeaseGuardPrecedesCreateRecovery(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "create-writer-fence", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	create := PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}
	if _, err := promotionStore.CreateImport(context.Background(), create); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_import_identity_tenants
		SET installed_writer_generation = 8 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
		SET writer_generation = 8 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.CreateImport(context.Background(), create); !errors.Is(err, ErrPromotionRestoreFenced) {
		t.Fatalf("stale writer lease recovery error = %v, want ErrPromotionRestoreFenced", err)
	}
}

func TestPromotionWriterProtocolGateRejectsOldProcessBeforeMutation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "create-old-writer-protocol", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
		SET minimum_writer_protocol = 2 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}

	if _, err := promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}); !errors.Is(err, ErrPromotionRestoreFenced) {
		t.Fatalf("old writer protocol CreateImport error = %v, want ErrPromotionRestoreFenced", err)
	}
	assertPromotionCreateRows(t, store, 0, 0, 0)
	assertPromotionReservedQuota(t, store, 0, 0)

	if _, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/another", ExpectedTargetAbsent: true,
		IdempotencyKey: "allocate-old-writer-protocol", AllocationLease: "allocation-lease",
	}); !errors.Is(err, ErrPromotionRestoreFenced) {
		t.Fatalf("old writer protocol AllocateImportID error = %v, want ErrPromotionRestoreFenced", err)
	}
	var lastIssued uint64
	if err := store.DB().QueryRow(`SELECT last_issued_sequence FROM promotion_import_identity_epochs
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = 2`).Scan(&lastIssued); err != nil {
		t.Fatal(err)
	}
	if lastIssued != allocation.AllocationSequence {
		t.Fatalf("last issued sequence after rejected old writer = %d, want %d", lastIssued, allocation.AllocationSequence)
	}
}

func TestPromotionCreateRejectsManifestMutationBeforeAnyImportSideEffect(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, IdempotencyKey: "allocate-mutated", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := promotion.CanonicalizeManifest([]promotion.ManifestEntry{
		{RelativePath: "empty", Type: promotion.EntryTypeDirectory, Mode: 0o700, MtimeNS: 1},
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, MtimeNS: 2, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
		{RelativePath: "link", Type: promotion.EntryTypeSymlink, Mode: 0o777, MtimeNS: 3, SymlinkTarget: "file"},
	}, testPromotionLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: changed.Entries, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	})
	if !errors.Is(err, promotion.ErrInvalidPlan) {
		t.Fatalf("CreateImport changed manifest error = %v, want ErrInvalidPlan", err)
	}
	assertPromotionCreateRows(t, store, 0, 0, 0)
}

func TestPromotionCreateQuotaFailureRollsBackImportAndReservation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "allocate-over-quota", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_quota_accounts SET max_bytes = 4 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	_, err = promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	})
	if !errors.Is(err, ErrPromotionQuotaExceeded) {
		t.Fatalf("CreateImport quota error = %v, want ErrPromotionQuotaExceeded", err)
	}
	assertPromotionCreateRows(t, store, 0, 0, 0)
	var reservedBytes uint64
	if err := store.DB().QueryRow(`SELECT reserved_bytes FROM promotion_quota_accounts WHERE tenant_id = 'tenant-a'`).Scan(&reservedBytes); err != nil {
		t.Fatal(err)
	}
	if reservedBytes != 0 {
		t.Fatalf("reserved bytes after failed CreateImport = %d, want 0", reservedBytes)
	}
}

func TestPromotionConcurrentIdenticalCreateHasOneReservation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, IdempotencyKey: "allocate-race", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}
	type result struct {
		value *PromotionImport
		err   error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			value, err := promotionStore.CreateImport(context.Background(), req)
			results <- result{value: value, err: err}
		}()
	}
	close(start)
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent CreateImport errors = %v, %v", first.err, second.err)
	}
	if first.value.QuotaReservationID != second.value.QuotaReservationID {
		t.Fatalf("reservation IDs differ: %q vs %q", first.value.QuotaReservationID, second.value.QuotaReservationID)
	}
	assertPromotionCreateRows(t, store, 1, len(manifest), 1)
}

func TestPromotionCreateRecoversTerminalTombstoneAndRejectsRetiredIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "allocate-terminal-recovery", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}
	created, err := promotionStore.CreateImport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	var proofDigest, requestDigest, ownerHash, recoveryHash string
	if err := store.DB().QueryRow(`SELECT allocation_proof_digest, create_request_digest,
		owner_token_hash, recovery_token_hash FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).
		Scan(&proofDigest, &requestDigest, &ownerHash, &recoveryHash); err != nil {
		t.Fatal(err)
	}
	terminalResult := `{ "z": 2, "a": {"value": 1} }`
	if _, err := store.DB().Exec(`INSERT INTO promotion_import_tombstones
		(tenant_id, allocation_epoch, allocation_sequence, allocation_proof_digest,
		 target_path, target_path_hash, request_digest, owner_token_hash, recovery_token_hash,
		 terminal_state, terminal_result_blob, terminal_result_digest, retire_after)
		VALUES ('tenant-a', ?, ?, ?, '/published', ?, ?, ?, ?, 'COMMITTED', ?, ?, DATE_ADD(NOW(3), INTERVAL 1 DAY))`,
		created.AllocationEpoch, created.AllocationSequence, proofDigest, fileNodePathHash("/published"),
		requestDigest, ownerHash, recoveryHash, terminalResult, promotionTestHash(terminalResult)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`DELETE FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}

	recovered, err := promotionStore.CreateImport(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateImport tombstone recovery: %v", err)
	}
	if recovered.State != "COMMITTED" || recovered.MigrationID != created.MigrationID || recovered.Target != created.Target ||
		recovered.TerminalResultBlob != terminalResult || recovered.TerminalResultDigest != promotionTestHash(terminalResult) {
		t.Fatalf("terminal recovery = %+v, want complete durable terminal result", recovered)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_import_tombstones SET terminal_result_digest = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		promotionTestHash("different-result"), created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.CreateImport(context.Background(), request); !errors.Is(err, ErrPromotionRecoveryRequired) {
		t.Fatalf("corrupt tombstone digest error = %v, want ErrPromotionRecoveryRequired", err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_import_tombstones SET terminal_result_blob = '', terminal_result_digest = ''
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.CreateImport(context.Background(), request); !errors.Is(err, ErrPromotionRecoveryRequired) {
		t.Fatalf("empty tombstone result error = %v, want ErrPromotionRecoveryRequired", err)
	}
	mismatched := request
	mismatched.RecoveryToken = "different-recovery-token"
	if _, err := promotionStore.CreateImport(context.Background(), mismatched); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("mismatched tombstone recovery error = %v, want ErrPromotionConflict", err)
	}

	if _, err := store.DB().Exec(`DELETE FROM promotion_import_tombstones
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`INSERT INTO promotion_retired_import_sequences
		(tenant_id, allocation_epoch, allocation_sequence) VALUES ('tenant-a', ?, ?)`,
		created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.CreateImport(context.Background(), request); !errors.Is(err, ErrPromotionIDRetired) {
		t.Fatalf("retired CreateImport error = %v, want ErrPromotionIDRetired", err)
	}
}

func TestPromotionCreateMetadataOnlyManifestsReserveZeroQuota(t *testing.T) {
	tests := []struct {
		name    string
		entries []promotion.ManifestEntry
	}{
		{name: "empty"},
		{name: "directory", entries: []promotion.ManifestEntry{{RelativePath: "empty", Type: promotion.EntryTypeDirectory, Mode: 0o755}}},
		{name: "symlink", entries: []promotion.ManifestEntry{{RelativePath: "link", Type: promotion.EntryTypeSymlink, Mode: 0o777, SymlinkTarget: "target"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Millisecond)
			store, promotionStore, _, _ := newTestPromotionStore(t, &now)
			manifest, err := promotion.CanonicalizeManifest(test.entries, testPromotionLimits())
			if err != nil {
				t.Fatal(err)
			}
			plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
				TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries,
			})
			if err != nil {
				t.Fatal(err)
			}
			allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
				TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
				IdempotencyKey: "allocate-metadata-" + test.name, AllocationLease: "allocation-lease",
			})
			if err != nil {
				t.Fatal(err)
			}
			created, err := promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
				TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
				Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries, Plan: *plan,
				OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
			})
			if err != nil {
				t.Fatalf("CreateImport metadata-only manifest: %v", err)
			}
			if created.State != "CREATED" {
				t.Fatalf("created state = %q, want CREATED", created.State)
			}
			var reservationBytes, reservationFiles, accountBytes, accountFiles uint64
			if err := store.DB().QueryRow(`SELECT reserved_bytes, reserved_files FROM promotion_quota_reservations
				WHERE tenant_id = 'tenant-a' AND reservation_id = ?`, created.QuotaReservationID).
				Scan(&reservationBytes, &reservationFiles); err != nil {
				t.Fatal(err)
			}
			if err := store.DB().QueryRow(`SELECT reserved_bytes, reserved_files FROM promotion_quota_accounts
				WHERE tenant_id = 'tenant-a'`).Scan(&accountBytes, &accountFiles); err != nil {
				t.Fatal(err)
			}
			if reservationBytes != 0 || reservationFiles != 0 || accountBytes != 0 || accountFiles != 0 {
				t.Fatalf("zero-quota reservation/account = (%d,%d)/(%d,%d)",
					reservationBytes, reservationFiles, accountBytes, accountFiles)
			}
		})
	}
}

func TestPromotionCreateTerminalFullRowFailsClosedOnMissingOrCorruptResult(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest := testPromotionManifest(t)
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: "allocate-terminal-full-row", AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	}
	created, err := promotionStore.CreateImport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	setTerminalResult := func(blob, digest any) {
		t.Helper()
		if _, err := store.DB().Exec(`UPDATE promotion_imports
			SET state = 'COMMITTED', terminal_result_blob = ?, terminal_result_digest = ?
			WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
			blob, digest, created.AllocationEpoch, created.AllocationSequence); err != nil {
			t.Fatal(err)
		}
	}

	setTerminalResult(nil, nil)
	if _, err := promotionStore.CreateImport(context.Background(), request); !errors.Is(err, ErrPromotionRecoveryRequired) {
		t.Fatalf("missing terminal result error = %v, want ErrPromotionRecoveryRequired", err)
	}
	setTerminalResult("{}", promotionTestHash("different-result"))
	if _, err := promotionStore.CreateImport(context.Background(), request); !errors.Is(err, ErrPromotionRecoveryRequired) {
		t.Fatalf("corrupt terminal result error = %v, want ErrPromotionRecoveryRequired", err)
	}
	setTerminalResult("not-json", promotionTestHash("not-json"))
	if _, err := promotionStore.CreateImport(context.Background(), request); !errors.Is(err, ErrPromotionRecoveryRequired) {
		t.Fatalf("invalid terminal JSON error = %v, want ErrPromotionRecoveryRequired", err)
	}
	terminalResult := `{ "z": 2, "a": {"value": 1} }`
	setTerminalResult(terminalResult, promotionTestHash(terminalResult))
	recovered, err := promotionStore.CreateImport(context.Background(), request)
	if err != nil {
		t.Fatalf("valid full terminal recovery: %v", err)
	}
	if recovered.State != "COMMITTED" || recovered.TerminalResultBlob != terminalResult || recovered.TerminalResultDigest != promotionTestHash(terminalResult) {
		t.Fatalf("valid full terminal recovery = %+v", recovered)
	}
}

func assertPromotionCreateRows(t *testing.T, store *Store, imports, entries, reservations int) {
	t.Helper()
	for table, want := range map[string]int{
		"promotion_imports": imports, "promotion_import_entries": entries, "promotion_quota_reservations": reservations,
	} {
		var got int
		if err := store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d", table, got, want)
		}
	}
}

func assertPromotionReservedQuota(t *testing.T, store *Store, wantBytes, wantFiles uint64) {
	t.Helper()
	var gotBytes, gotFiles uint64
	if err := store.DB().QueryRow(`SELECT reserved_bytes, reserved_files FROM promotion_quota_accounts WHERE tenant_id = 'tenant-a'`).
		Scan(&gotBytes, &gotFiles); err != nil {
		t.Fatal(err)
	}
	if gotBytes != wantBytes || gotFiles != wantFiles {
		t.Fatalf("reserved quota = (%d, %d), want (%d, %d)", gotBytes, gotFiles, wantBytes, wantFiles)
	}
}
