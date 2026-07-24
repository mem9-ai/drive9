package server

import (
	"errors"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestNormalizeTiDBCloudFreeProvisionQuota(t *testing.T) {
	s := &Server{tidbCloudFreePlanLimits: DefaultTiDBCloudFreePlanLimits()}
	zero := int64(0)
	positive := int64(10)
	storageBelow, fileSizeBelow, fileCountBelow := int64(1024), int64(100), int64(500)
	storageAbove, fileSizeAbove, fileCountAbove := int64(3073), int64(301), int64(1001)
	tests := []struct {
		name    string
		quota   *quotaRequest
		want    quotaFields
		wantErr error
	}{
		{
			name: "omitted quota uses free defaults",
			want: quotaFields{
				MaxStorageSize:         int64Ptr(3072),
				MaxFileSize:            int64Ptr(300),
				MaxFileCount:           int64Ptr(1000),
				TiDBCloudSpendingLimit: int64Ptr(0),
			},
		},
		{
			name: "smaller quota is preserved",
			quota: &quotaRequest{quotaFields: quotaFields{
				MaxStorageSize:         &storageBelow,
				MaxFileSize:            &fileSizeBelow,
				MaxFileCount:           &fileCountBelow,
				TiDBCloudSpendingLimit: &zero,
			}},
			want: quotaFields{
				MaxStorageSize:         &storageBelow,
				MaxFileSize:            &fileSizeBelow,
				MaxFileCount:           &fileCountBelow,
				TiDBCloudSpendingLimit: &zero,
			},
		},
		{
			name: "positive spending is forbidden",
			quota: &quotaRequest{quotaFields: quotaFields{
				TiDBCloudSpendingLimit: &positive,
			}},
			wantErr: tenant.ErrTiDBCloudFreeSpendingLimitForbidden,
		},
		{
			name: "unlimited file count is forbidden",
			quota: &quotaRequest{quotaFields: quotaFields{
				MaxFileCount: &zero,
			}},
			wantErr: tenant.ErrTiDBCloudFreeQuotaExceeded,
		},
		{
			name: "storage cap is enforced",
			quota: &quotaRequest{quotaFields: quotaFields{
				MaxStorageSize: &storageAbove,
			}},
			wantErr: tenant.ErrTiDBCloudFreeQuotaExceeded,
		},
		{
			name: "file size cap is enforced",
			quota: &quotaRequest{quotaFields: quotaFields{
				MaxFileSize: &fileSizeAbove,
			}},
			wantErr: tenant.ErrTiDBCloudFreeQuotaExceeded,
		},
		{
			name: "file count cap is enforced",
			quota: &quotaRequest{quotaFields: quotaFields{
				MaxFileCount: &fileCountAbove,
			}},
			wantErr: tenant.ErrTiDBCloudFreeQuotaExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.normalizeTiDBCloudFreeProvisionQuota(tt.quota)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("normalized quota is nil")
			}
			assertInt64PtrEqual(t, "max_storage_size", got.MaxStorageSize, tt.want.MaxStorageSize)
			assertInt64PtrEqual(t, "max_file_size", got.MaxFileSize, tt.want.MaxFileSize)
			assertInt64PtrEqual(t, "max_file_count", got.MaxFileCount, tt.want.MaxFileCount)
			assertInt64PtrEqual(t, "tidbcloud_spending_limit", got.TiDBCloudSpendingLimit, tt.want.TiDBCloudSpendingLimit)
		})
	}
}

func TestNormalizeTiDBCloudFreeProvisionQuotaRoundsPositiveByteLimitsUp(t *testing.T) {
	s := &Server{tidbCloudFreePlanLimits: TiDBCloudFreePlanLimits{
		TenantCount:      1,
		MaxStorageBytes:  1,
		MaxFileSizeBytes: 1,
		MaxFileCount:     1,
	}}
	got, err := s.normalizeTiDBCloudFreeProvisionQuota(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxStorageSize == nil || *got.MaxStorageSize != 1 {
		t.Fatalf("max storage size = %v, want 1 MiB unit", got.MaxStorageSize)
	}
	if got.MaxFileSize == nil || *got.MaxFileSize != 1 {
		t.Fatalf("max file size = %v, want 1 MiB unit", got.MaxFileSize)
	}
}

func int64Ptr(v int64) *int64 { return &v }

func assertInt64PtrEqual(t *testing.T, name string, got, want *int64) {
	t.Helper()
	if got == nil || want == nil || *got != *want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}
