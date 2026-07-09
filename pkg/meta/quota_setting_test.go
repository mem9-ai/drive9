package meta

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"
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

func TestRetryMetaLockConflictRetriesDeadlock(t *testing.T) {
	var calls int
	err := retryMetaLockConflict(context.Background(), "tenant-retry-metric", "reserve_upload", "test_op", func() error {
		calls++
		if calls < 3 {
			return errors.New("Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryMetaLockConflict returned error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_service_operations_total{component="central_quota",operation="reserve_upload",result="lock_conflict_retry",tenant_id="tenant-retry-metric"} 2`) {
		t.Fatalf("missing lock conflict retry metric:\n%s", rec.Body.String())
	}
}

func TestRetryMetaLockConflictDoesNotRetryBusinessError(t *testing.T) {
	var calls int
	err := retryMetaLockConflict(context.Background(), "tenant-business-error", "reserve_upload", "test_op", func() error {
		calls++
		return ErrStorageQuotaExceeded
	})
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("err = %v, want ErrStorageQuotaExceeded", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRetryMetaLockConflictExhaustedReturnsBusy(t *testing.T) {
	var calls int
	lockErr := errors.New("Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction")
	err := retryMetaLockConflict(context.Background(), "tenant-busy", "reserve_upload", "test_op", func() error {
		calls++
		return lockErr
	})
	if !errors.Is(err, ErrQuotaReservationBusy) {
		t.Fatalf("err = %v, want ErrQuotaReservationBusy", err)
	}
	if !errors.Is(err, lockErr) {
		t.Fatalf("err = %v, want wrapped lock conflict error", err)
	}
	if calls != metaLockConflictRetryAttempts {
		t.Fatalf("calls = %d, want %d", calls, metaLockConflictRetryAttempts)
	}
}

func TestIsMetaLockConflictErrorIncludesTiDBWriteConflict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "tidb write conflict code",
			err:  errors.New("ERROR 9007 (HY000): Write conflict, txnStartTS=123, conflictStartTS=456"),
			want: true,
		},
		{
			name: "tidb write conflict message",
			err:  errors.New("write conflict, retry txn"),
			want: true,
		},
		{
			name: "unrelated 9007",
			err:  errors.New("ERROR 9007 quota unavailable"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMetaLockConflictError(tt.err); got != tt.want {
				t.Fatalf("isMetaLockConflictError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestRetryMetaLockConflictHonorsContextDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	err := retryMetaLockConflict(ctx, "tenant-canceled", "reserve_upload", "test_op", func() error {
		calls++
		cancel()
		return errors.New("SQLSTATE 40001 serialization failure")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
