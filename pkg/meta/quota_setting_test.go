package meta

import (
	"context"
	"testing"
	"time"
)

func TestDefaultMaxStorageBytesDefault(t *testing.T) {
	if got := DefaultMaxStorageBytes(); got != int64(50*(1<<30)) {
		t.Fatalf("default = %d, want 50 GiB", got)
	}
}

func TestSetDefaultMaxStorageBytes(t *testing.T) {
	orig := DefaultMaxStorageBytes()
	defer func() { SetDefaultMaxStorageBytes(orig) }()

	SetDefaultMaxStorageBytes(1 << 30)
	if got := DefaultMaxStorageBytes(); got != int64(1<<30) {
		t.Fatalf("got %d, want 1 GiB", got)
	}
}

func TestSetDefaultMaxStorageBytesRejectsZero(t *testing.T) {
	orig := DefaultMaxStorageBytes()
	defer func() { SetDefaultMaxStorageBytes(orig) }()

	SetDefaultMaxStorageBytes(0)
	if got := DefaultMaxStorageBytes(); got != orig {
		t.Fatalf("zero val changed default from %d to %d", orig, got)
	}
}

func TestGetQuotaConfigUsesConfiguredDefaultStorageBytes(t *testing.T) {
	orig := DefaultMaxStorageBytes()
	defer func() { SetDefaultMaxStorageBytes(orig) }()

	SetDefaultMaxStorageBytes(12345)
	s := newControlStore(t)

	cfg, err := s.GetQuotaConfig(context.Background(), "tenant-without-config")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 12345 {
		t.Fatalf("MaxStorageBytes = %d, want configured default 12345", cfg.MaxStorageBytes)
	}
	if cfg.MaxMediaLLMFiles != 500 {
		t.Fatalf("MaxMediaLLMFiles = %d, want default 500", cfg.MaxMediaLLMFiles)
	}
	if cfg.MaxMonthlyCostMC != DefaultMaxMonthlyCostMC {
		t.Fatalf("MaxMonthlyCostMC = %d, want default %d", cfg.MaxMonthlyCostMC, DefaultMaxMonthlyCostMC)
	}
	if !cfg.InheritMaxMonthlyCostMC {
		t.Fatal("InheritMaxMonthlyCostMC = false, want true for default config")
	}
	if cfg.Explicit {
		t.Fatal("Explicit = true, want false for default config")
	}
}

func TestGetQuotaConfigVersion(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	version, err := s.GetQuotaConfigVersion(ctx, "tenant-without-config")
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		t.Fatalf("version for missing config = %q, want empty", version)
	}

	if err := s.SetQuotaConfig(ctx, &QuotaConfig{
		TenantID:         "tenant-with-config",
		MaxStorageBytes:  123,
		MaxMediaLLMFiles: 456,
		MaxMonthlyCostMC: 789,
	}); err != nil {
		t.Fatal(err)
	}
	version, err = s.GetQuotaConfigVersion(ctx, "tenant-with-config")
	if err != nil {
		t.Fatal(err)
	}
	if version == "" {
		t.Fatal("version for explicit config is empty")
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-with-config")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Explicit {
		t.Fatal("Explicit = false, want true for configured tenant")
	}
}

func TestPatchQuotaConfigPreservesUnspecifiedFields(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	if err := s.SetQuotaConfig(ctx, &QuotaConfig{
		TenantID:         "tenant-patch",
		MaxStorageBytes:  100,
		MaxMediaLLMFiles: 200,
		MaxMonthlyCostMC: 300,
	}); err != nil {
		t.Fatal(err)
	}
	storage := int64(999)
	monthly := int64(0)
	if err := s.PatchQuotaConfig(ctx, &QuotaConfigPatch{
		TenantID:         "tenant-patch",
		MaxStorageBytes:  &storage,
		MaxMonthlyCostMC: &monthly,
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-patch")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 999 || cfg.MaxMediaLLMFiles != 200 || cfg.MaxMonthlyCostMC != 0 {
		t.Fatalf("cfg = %+v, want storage=999 media=200 monthly=0", cfg)
	}
}

func TestPatchQuotaConfigInsertsDefaultsForUnspecifiedFields(t *testing.T) {
	orig := DefaultMaxStorageBytes()
	defer func() { SetDefaultMaxStorageBytes(orig) }()
	SetDefaultMaxStorageBytes(12345)

	s := newControlStore(t)
	ctx := context.Background()

	media := int64(7)
	if err := s.PatchQuotaConfig(ctx, &QuotaConfigPatch{
		TenantID:         "tenant-patch-insert",
		MaxMediaLLMFiles: &media,
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-patch-insert")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 12345 || cfg.MaxMediaLLMFiles != 7 || cfg.MaxMonthlyCostMC != DefaultMaxMonthlyCostMC || !cfg.InheritMaxMonthlyCostMC || !cfg.Explicit {
		t.Fatalf("cfg = %+v, want defaults plus media patch", cfg)
	}
	var rawMonthly int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT max_monthly_cost_mc FROM tenant_quota_config WHERE tenant_id = ?`,
		"tenant-patch-insert").Scan(&rawMonthly); err != nil {
		t.Fatal(err)
	}
	if rawMonthly != InheritMaxMonthlyCostMC {
		t.Fatalf("raw max_monthly_cost_mc = %d, want inherit sentinel %d", rawMonthly, InheritMaxMonthlyCostMC)
	}
}

func TestPatchQuotaConfigCanExplicitlyDisableMonthlyCostOnInsert(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	storage := int64(12345)
	monthly := int64(0)
	if err := s.PatchQuotaConfig(ctx, &QuotaConfigPatch{
		TenantID:         "tenant-patch-explicit-monthly-zero",
		MaxStorageBytes:  &storage,
		MaxMonthlyCostMC: &monthly,
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-patch-explicit-monthly-zero")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 12345 || cfg.MaxMonthlyCostMC != 0 || cfg.InheritMaxMonthlyCostMC || !cfg.Explicit {
		t.Fatalf("cfg = %+v, want storage patch with explicit monthly zero", cfg)
	}
}

func TestAtomicReserveAndInsertUploadBootstrapsUsageRow(t *testing.T) {
	orig := DefaultMaxStorageBytes()
	defer func() { SetDefaultMaxStorageBytes(orig) }()

	SetDefaultMaxStorageBytes(100)
	s := newControlStore(t)
	ctx := context.Background()

	if err := s.AtomicReserveAndInsertUpload(ctx, &UploadReservation{
		TenantID:      "tenant-without-usage-row",
		UploadID:      "upload-1",
		ReservedBytes: 40,
		TargetPath:    "/large.bin",
		ExpiresAt:     time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	usage, err := s.GetQuotaUsage(ctx, "tenant-without-usage-row")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 0 || usage.ReservedBytes != 40 {
		t.Fatalf("usage = %+v, want storage=0 reserved=40", usage)
	}

	reservation, err := s.GetUploadReservation(ctx, "tenant-without-usage-row", "upload-1")
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Status != "active" || reservation.ReservedBytes != 40 {
		t.Fatalf("reservation = %+v, want active reserved=40", reservation)
	}
}
