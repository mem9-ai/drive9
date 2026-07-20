package meta

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

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
	if cfg.MaxVideoLLMFiles != 50 {
		t.Errorf("MaxVideoLLMFiles = %d, want default 50", cfg.MaxVideoLLMFiles)
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
	if version == "" {
		t.Fatalf("storage quota version should be non-empty when config row exists")
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
		MaxVideoLLMFiles: 567,
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
		MaxVideoLLMFiles: 210,
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
	if cfg.MaxStorageBytes != 999 || cfg.MaxFileSizeBytes != 101 || cfg.MaxFileCount != 102 || cfg.MaxMediaLLMFiles != 200 || cfg.MaxVideoLLMFiles != 210 || cfg.MaxMonthlyCostMC != 300 {
		t.Errorf("cfg = %+v, want storage=999 file_size=101 file_count=102 media=200 video=210 monthly=300", cfg)
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
	if cfg.MaxStorageBytes != 12345 || cfg.MaxFileSizeBytes != DefaultMaxFileSizeBytes() || cfg.MaxFileCount != 0 || cfg.MaxMediaLLMFiles != 500 || cfg.MaxVideoLLMFiles != 50 || cfg.MaxMonthlyCostMC != 0 {
		t.Errorf("cfg = %+v, want storage patch with internal defaults", cfg)
	}
}

func TestSetQuotaStorageBytesOverridesSpendingOnlyRow(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	spendingLimit := int64(100)
	if err := s.SetQuotaConfigPatch(ctx, "tenant-spending-then-storage", QuotaConfigPatch{TiDBCloudSpendingLimit: &spendingLimit}); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-spending-then-storage")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QuotaLimitsOverridden {
		t.Fatal("QuotaLimitsOverridden after spending-only patch = true, want false")
	}

	if err := s.SetQuotaStorageBytes(ctx, "tenant-spending-then-storage", 12345); err != nil {
		t.Fatal(err)
	}
	cfg, err = s.GetQuotaConfig(ctx, "tenant-spending-then-storage")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 12345 {
		t.Fatalf("MaxStorageBytes = %d, want 12345", cfg.MaxStorageBytes)
	}
	if !cfg.QuotaLimitsOverridden {
		t.Fatal("QuotaLimitsOverridden = false, want true after storage override")
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != spendingLimit {
		t.Fatalf("TiDBCloudSpendingLimit = %#v, want %d", cfg.TiDBCloudSpendingLimit, spendingLimit)
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
		MaxVideoLLMFiles: 410,
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
	if cfg.MaxStorageBytes != 100 || cfg.MaxFileSizeBytes != 222 || cfg.MaxFileCount != 333 || cfg.MaxMediaLLMFiles != 400 || cfg.MaxVideoLLMFiles != 410 || cfg.MaxMonthlyCostMC != 500 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestSetTiDBCloudSpendingLimitIfNotUpdatedAfterSkipsNewerLocal(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()

	newLimit := int64(200)
	if err := s.SetQuotaConfigPatch(ctx, "tenant-spending-cas", QuotaConfigPatch{TiDBCloudSpendingLimit: &newLimit}); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.GetQuotaConfig(ctx, "tenant-spending-cas")
	if err != nil {
		t.Fatal(err)
	}

	applied, err := s.SetTiDBCloudSpendingLimitIfNotUpdatedAfter(ctx, "tenant-spending-cas", 100, time.Now().UTC(), cfg.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale spending limit sync applied, want skipped")
	}
	cfg, err = s.GetQuotaConfig(ctx, "tenant-spending-cas")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != newLimit {
		t.Fatalf("spending limit = %#v, want %d", cfg.TiDBCloudSpendingLimit, newLimit)
	}

	applied, err = s.SetTiDBCloudSpendingLimitIfNotUpdatedAfter(ctx, "tenant-spending-cas", 300, time.Now().UTC(), cfg.UpdatedAt.Add(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("fresh spending limit sync skipped, want applied")
	}
	cfg, err = s.GetQuotaConfig(ctx, "tenant-spending-cas")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != 300 {
		t.Fatalf("spending limit after fresh sync = %#v, want 300", cfg.TiDBCloudSpendingLimit)
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
	if !strings.Contains(rec.Body.String(), `drive9_service_operations_total{component="central_quota",operation="reserve_upload",result="lock_conflict_retry",tenant_id="tenant-retry-metric",tidbcloud_org_id="guest"} 2`) {
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
			name: "tidb write conflict mysql error",
			err:  &mysql.MySQLError{Number: 9007, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: "Write conflict, txnStartTS=123, conflictStartTS=456"},
			want: true,
		},
		{
			name: "mysql deadlock",
			err:  &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "Deadlock found when trying to get lock; try restarting transaction"},
			want: true,
		},
		{
			name: "mysql lock wait timeout",
			err:  &mysql.MySQLError{Number: 1205, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: "Lock wait timeout exceeded; try restarting transaction"},
			want: true,
		},
		{
			name: "sqlstate serialization failure",
			err:  &mysql.MySQLError{Number: 1105, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "transaction aborted"},
			want: true,
		},
		{
			name: "tidb write conflict db-shaped fallback",
			err:  errors.New("ERROR 9007 (HY000): Write conflict, txnStartTS=123, conflictStartTS=456"),
			want: true,
		},
		{
			name: "unrelated 9007",
			err:  errors.New("ERROR 9007 quota unavailable"),
			want: false,
		},
		{
			name: "tidb mysql error without write conflict",
			err:  &mysql.MySQLError{Number: 9007, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: "quota unavailable"},
			want: false,
		},
		{
			name: "unrelated 40001 string",
			err:  errors.New("quota request 40001 rejected by policy"),
			want: false,
		},
		{
			name: "unrelated write conflict string",
			err:  errors.New("application write conflict in cache layer"),
			want: false,
		},
		{
			name: "wrapped non-db context with 40001",
			err:  fmt.Errorf("quota admission 40001: %w", errors.New("application rejected write")),
			want: false,
		},
		{
			name: "wrapped non-db context with write conflict",
			err:  fmt.Errorf("quota admission write conflict: %w", errors.New("application rejected write")),
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

func TestRetryMetaLockConflictDoesNotRetryFalsePositiveLockText(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "unrelated 40001 string",
			err:  errors.New("quota request 40001 rejected by policy"),
		},
		{
			name: "unrelated write conflict string",
			err:  errors.New("application write conflict in cache layer"),
		},
		{
			name: "wrapped non-db context with 40001",
			err:  fmt.Errorf("quota admission 40001: %w", errors.New("application rejected write")),
		},
		{
			name: "wrapped non-db context with write conflict",
			err:  fmt.Errorf("quota admission write conflict: %w", errors.New("application rejected write")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			err := retryMetaLockConflict(context.Background(), "tenant-false-positive", "reserve_upload", "test_op", func() error {
				calls++
				return tt.err
			})
			if !errors.Is(err, tt.err) {
				t.Fatalf("err = %v, want original error %v", err, tt.err)
			}
			if errors.Is(err, ErrQuotaReservationBusy) {
				t.Fatalf("err = %v, want no busy fail-open sentinel", err)
			}
			if calls != 1 {
				t.Fatalf("calls = %d, want 1", calls)
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
