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

func TestDefaultMaxFileSizeBytesDefault(t *testing.T) {
	if got := DefaultMaxFileSizeBytes(); got != int64(10*(1<<30)) {
		t.Fatalf("default = %d, want 10 GiB", got)
	}
}

func TestSetDefaultMaxFileSizeBytes(t *testing.T) {
	orig := DefaultMaxFileSizeBytes()
	defer func() { SetDefaultMaxFileSizeBytes(orig) }()

	SetDefaultMaxFileSizeBytes(2 << 30)
	if got := DefaultMaxFileSizeBytes(); got != int64(2<<30) {
		t.Fatalf("got %d, want 2 GiB", got)
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
		t.Errorf("MaxStorageBytes = %d, want configured default 12345", cfg.MaxStorageBytes)
	}
	if cfg.MaxFileSizeBytes != DefaultMaxFileSizeBytes() {
		t.Errorf("MaxFileSizeBytes = %d, want default %d", cfg.MaxFileSizeBytes, DefaultMaxFileSizeBytes())
	}
	if cfg.MaxFileCount != 0 {
		t.Errorf("MaxFileCount = %d, want default 0", cfg.MaxFileCount)
	}
	if cfg.MaxMediaLLMFiles != 500 {
		t.Errorf("MaxMediaLLMFiles = %d, want default 500", cfg.MaxMediaLLMFiles)
	}
	if cfg.MaxMonthlyCostMC != 0 {
		t.Errorf("MaxMonthlyCostMC = %d, want default 0", cfg.MaxMonthlyCostMC)
	}
}

func TestQuotaConfigStoresTiDBCloudSpendingLimitWithoutStorageVersion(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	cfg, err := s.GetQuotaConfig(ctx, "tenant-spending-only")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit != nil {
		t.Fatalf("default spending limit = %#v, want nil", cfg.TiDBCloudSpendingLimit)
	}

	zero := int64(0)
	if err := s.SetQuotaConfigPatch(ctx, "tenant-spending-only", QuotaConfigPatch{TiDBCloudSpendingLimit: &zero}); err != nil {
		t.Fatal(err)
	}
	cfg, err = s.GetQuotaConfig(ctx, "tenant-spending-only")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != 0 {
		t.Fatalf("spending limit = %#v, want 0", cfg.TiDBCloudSpendingLimit)
	}
	if cfg.MaxStorageBytes != DefaultMaxStorageBytes() || cfg.MaxFileSizeBytes != DefaultMaxFileSizeBytes() || cfg.MaxFileCount != 0 {
		t.Fatalf("storage quota fields = %#v, want defaults", cfg)
	}
	if cfg.QuotaLimitsOverridden {
		t.Fatalf("QuotaLimitsOverridden = true, want false for spending-only row")
	}
	version, err := s.GetQuotaConfigVersion(ctx, "tenant-spending-only")
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		t.Fatalf("storage quota version = %q, want empty", version)
	}

	updated := int64(123)
	if err := s.SetQuotaConfigPatch(ctx, "tenant-spending-only", QuotaConfigPatch{TiDBCloudSpendingLimit: &updated}); err != nil {
		t.Fatal(err)
	}
	cfg, err = s.GetQuotaConfig(ctx, "tenant-spending-only")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != updated {
		t.Fatalf("updated spending limit = %#v, want %d", cfg.TiDBCloudSpendingLimit, updated)
	}
	checkedAt := time.Now().UTC()
	if err := s.SetQuotaConfigPatch(ctx, "tenant-spending-only", QuotaConfigPatch{TiDBCloudSpendingLimitCheckedAt: &checkedAt}); err != nil {
		t.Fatal(err)
	}
	cfg, err = s.GetQuotaConfig(ctx, "tenant-spending-only")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimitCheckedAt == nil {
		t.Fatal("spending limit checked_at = nil, want timestamp")
	}
	if cfg.QuotaLimitsOverridden {
		t.Fatalf("QuotaLimitsOverridden after checked_at = true, want false")
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
		MaxFileSizeBytes: 234,
		MaxFileCount:     345,
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
	if err := s.SetQuotaStorageBytes(ctx, "tenant-with-config", 321); err != nil {
		t.Fatal(err)
	}
	nextVersion, err := s.GetQuotaConfigVersion(ctx, "tenant-with-config")
	if err != nil {
		t.Fatal(err)
	}
	if nextVersion == version {
		t.Fatalf("version after config value change = %q, want different from %q", nextVersion, version)
	}
}

func TestSetQuotaStorageBytesUpdatesStorageOnly(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	if err := s.SetQuotaConfig(ctx, &QuotaConfig{
		TenantID:         "tenant-patch",
		MaxStorageBytes:  100,
		MaxFileSizeBytes: 101,
		MaxFileCount:     102,
		MaxMediaLLMFiles: 200,
		MaxMonthlyCostMC: 300,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetQuotaStorageBytes(ctx, "tenant-patch", 999); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-patch")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 999 || cfg.MaxFileSizeBytes != 101 || cfg.MaxFileCount != 102 || cfg.MaxMediaLLMFiles != 200 || cfg.MaxMonthlyCostMC != 300 {
		t.Errorf("cfg = %+v, want storage=999 file_size=101 file_count=102 media=200 monthly=300", cfg)
	}
}

func TestSetQuotaStorageBytesInsertsInternalDefaults(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	if err := s.SetQuotaStorageBytes(ctx, "tenant-patch-insert", 12345); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-patch-insert")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 12345 || cfg.MaxFileSizeBytes != DefaultMaxFileSizeBytes() || cfg.MaxFileCount != 0 || cfg.MaxMediaLLMFiles != 500 || cfg.MaxMonthlyCostMC != 0 {
		t.Errorf("cfg = %+v, want storage patch with internal defaults", cfg)
	}
}

func TestSetQuotaConfigPatchUpdatesExternalFieldsOnly(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	if err := s.SetQuotaConfig(ctx, &QuotaConfig{
		TenantID:         "tenant-external-patch",
		MaxStorageBytes:  100,
		MaxFileSizeBytes: 200,
		MaxFileCount:     300,
		MaxMediaLLMFiles: 400,
		MaxMonthlyCostMC: 500,
	}); err != nil {
		t.Fatal(err)
	}
	fileSize := int64(222)
	fileCount := int64(333)
	if err := s.SetQuotaConfigPatch(ctx, "tenant-external-patch", QuotaConfigPatch{
		MaxFileSizeBytes: &fileSize,
		MaxFileCount:     &fileCount,
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-external-patch")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 100 || cfg.MaxFileSizeBytes != 222 || cfg.MaxFileCount != 333 || cfg.MaxMediaLLMFiles != 400 || cfg.MaxMonthlyCostMC != 500 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestAtomicReserveAndInsertUploadBootstrapsUsageRow(t *testing.T) {
	orig := DefaultMaxStorageBytes()
	defer func() { SetDefaultMaxStorageBytes(orig) }()

	SetDefaultMaxStorageBytes(100)
	s := newControlStore(t)
	ctx := context.Background()

	if err := s.AtomicReserveAndInsertUpload(ctx, &UploadReservation{
		TenantID:       "tenant-without-usage-row",
		UploadID:       "upload-1",
		ReservedBytes:  40,
		FileCountDelta: 1,
		TargetPath:     "/large.bin",
		ExpiresAt:      time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	usage, err := s.GetQuotaUsage(ctx, "tenant-without-usage-row")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 0 || usage.ReservedBytes != 40 || usage.FileCount != 1 {
		t.Fatalf("usage = %+v, want storage=0 reserved=40 file_count=1", usage)
	}

	reservation, err := s.GetUploadReservation(ctx, "tenant-without-usage-row", "upload-1")
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Status != "active" || reservation.ReservedBytes != 40 || reservation.FileCountDelta != 1 {
		t.Fatalf("reservation = %+v, want active reserved=40", reservation)
	}
}
