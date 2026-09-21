package promotion

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func testCanonicalEntries(t *testing.T) []ManifestEntry {
	t.Helper()
	manifest, err := CanonicalizeManifest([]ManifestEntry{
		{RelativePath: "empty", Type: EntryTypeDirectory, Mode: 0o755, MtimeNS: 1},
		{RelativePath: "file", Type: EntryTypeFile, Mode: 0o644, MtimeNS: 2, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: checksum("hello")},
	}, testManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	return manifest.Entries
}

func testPlanner(t *testing.T, now time.Time) *Planner {
	t.Helper()
	planner, err := NewPlanner([]byte("0123456789abcdef0123456789abcdef"), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	planner.now = func() time.Time { return now }
	return planner
}

func testCapability() StorageCapability {
	return StorageCapability{Generation: 7, ConfigGeneration: 9, InlineEnabled: true, InlineThreshold: 1024, AllowedMode: StorageModeDB9Inline}
}

func TestPlannerPlanAndVerifyExactCreate(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	planner := testPlanner(t, now)
	req := PlanImportRequest{TenantID: "tenant-a", Target: "/published/tree", ExpectedTargetAbsent: true, Manifest: testCanonicalEntries(t)}
	plan, err := planner.PlanImport(req, testCapability(), testManifestLimits())
	if err != nil {
		t.Fatalf("PlanImport: %v", err)
	}
	if plan.StoragePlanDigest == "" || plan.StorageMode != StorageModeDB9Inline {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	if _, err := planner.VerifyCreatePlan(req, *plan, testCapability()); err != nil {
		t.Fatalf("VerifyCreatePlan: %v", err)
	}
}

func TestPlannerRejectsChangedManifestAndPlanClaims(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	planner := testPlanner(t, now)
	req := PlanImportRequest{TenantID: "tenant-a", Target: "/published/tree", ExpectedTargetAbsent: true, Manifest: testCanonicalEntries(t)}
	plan, err := planner.PlanImport(req, testCapability(), testManifestLimits())
	if err != nil {
		t.Fatal(err)
	}

	changed := req
	changed.Manifest = append([]ManifestEntry(nil), req.Manifest...)
	changed.Manifest[1].Mode = 0o600
	if _, err := planner.VerifyCreatePlan(changed, *plan, testCapability()); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("manifest mutation error = %v, want ErrInvalidManifest", err)
	}

	tampered := *plan
	tampered.ByteTotal++
	if _, err := planner.VerifyCreatePlan(req, tampered, testCapability()); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("plan mutation error = %v, want ErrInvalidPlan", err)
	}

	tampered = *plan
	tampered.StorageMode = StorageMode("forged")
	if _, err := planner.VerifyCreatePlan(req, tampered, testCapability()); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("MAC-bound claim mutation error = %v, want ErrInvalidPlan", err)
	}

	// A forged plan is rejected by its MAC before the server performs the
	// O(entries) manifest validation.
	forgedWithInvalidManifest := *plan
	forgedWithInvalidManifest.StorageMode = StorageMode("forged")
	invalidManifest := req
	invalidManifest.Manifest = []ManifestEntry{{RelativePath: "../escape", Type: EntryTypeFile}}
	if _, err := planner.VerifyCreatePlan(invalidManifest, forgedWithInvalidManifest, testCapability()); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("forged plan with invalid manifest error = %v, want ErrInvalidPlan", err)
	}
}

func TestPlannerTargetBoundAndAuthoritativeVerifyTime(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	planner := testPlanner(t, now)
	if _, err := CanonicalTarget("/" + strings.Repeat("a", MaxTargetPathBytes)); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("overlong target error = %v, want ErrInvalidPlan", err)
	}
	req := PlanImportRequest{TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: testCanonicalEntries(t)}
	plan, err := planner.PlanImport(req, testCapability(), testManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planner.VerifyCreatePlanAt(req, *plan, testCapability(), plan.PlanExpiresAt); !errors.Is(err, ErrPlanExpired) {
		t.Fatalf("database-time expiry error = %v, want ErrPlanExpired", err)
	}
}

func TestPlannerRejectsExpiredOrRolledCapability(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	planner := testPlanner(t, now)
	req := PlanImportRequest{TenantID: "tenant-a", Target: "/published/tree", ExpectedTargetAbsent: true, Manifest: testCanonicalEntries(t)}
	plan, err := planner.PlanImport(req, testCapability(), testManifestLimits())
	if err != nil {
		t.Fatal(err)
	}

	planner.now = func() time.Time { return plan.PlanExpiresAt }
	if _, err := planner.VerifyCreatePlan(req, *plan, testCapability()); !errors.Is(err, ErrPlanExpired) {
		t.Fatalf("expiry error = %v, want ErrPlanExpired", err)
	}
	planner.now = func() time.Time { return now }
	rolled := testCapability()
	rolled.Generation++
	if _, err := planner.VerifyCreatePlan(req, *plan, rolled); !errors.Is(err, ErrPlanStale) {
		t.Fatalf("rollout error = %v, want ErrPlanStale", err)
	}
}

func TestPlannerRejectsNonAbsentOrExternalPlanWithoutSideEffects(t *testing.T) {
	planner := testPlanner(t, time.Now())
	req := PlanImportRequest{TenantID: "tenant-a", Target: "/published/tree", Manifest: testCanonicalEntries(t)}
	if _, err := planner.PlanImport(req, testCapability(), testManifestLimits()); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("expected-target error = %v, want ErrInvalidPlan", err)
	}
	req.ExpectedTargetAbsent = true
	disabled := testCapability()
	disabled.InlineEnabled = false
	if _, err := planner.PlanImport(req, disabled, testManifestLimits()); !errors.Is(err, ErrStorageBackendUnsupported) {
		t.Fatalf("disabled inline error = %v, want ErrStorageBackendUnsupported", err)
	}
}
