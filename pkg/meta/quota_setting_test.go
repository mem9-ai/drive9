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
		t.Errorf("MaxStorageBytes = %d, want configured default 12345", cfg.MaxStorageBytes)
	}
	if cfg.MaxMediaLLMFiles != 500 {
		t.Errorf("MaxMediaLLMFiles = %d, want default 500", cfg.MaxMediaLLMFiles)
	}
	if cfg.MaxMonthlyCostMC != 0 {
		t.Errorf("MaxMonthlyCostMC = %d, want default 0", cfg.MaxMonthlyCostMC)
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
	if cfg.MaxStorageBytes != 999 || cfg.MaxMediaLLMFiles != 200 || cfg.MaxMonthlyCostMC != 300 {
		t.Errorf("cfg = %+v, want storage=999 media=200 monthly=300", cfg)
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
	if cfg.MaxStorageBytes != 12345 || cfg.MaxMediaLLMFiles != 500 || cfg.MaxMonthlyCostMC != 0 {
		t.Errorf("cfg = %+v, want storage patch with internal defaults", cfg)
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
