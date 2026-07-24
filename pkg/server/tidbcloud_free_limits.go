package server

import "github.com/mem9-ai/drive9/pkg/tenant"

const (
	DefaultTiDBCloudFreeTenantCount      = 3
	DefaultTiDBCloudFreeMaxStorageBytes  = int64(3 * (1 << 30))
	DefaultTiDBCloudFreeMaxFileSizeBytes = int64(300 * (1 << 20))
	DefaultTiDBCloudFreeMaxFileCount     = int64(1000)
)

type TiDBCloudFreePlanLimits struct {
	TenantCount      int
	MaxStorageBytes  int64
	MaxFileSizeBytes int64
	MaxFileCount     int64
}

func DefaultTiDBCloudFreePlanLimits() TiDBCloudFreePlanLimits {
	return TiDBCloudFreePlanLimits{
		TenantCount:      DefaultTiDBCloudFreeTenantCount,
		MaxStorageBytes:  DefaultTiDBCloudFreeMaxStorageBytes,
		MaxFileSizeBytes: DefaultTiDBCloudFreeMaxFileSizeBytes,
		MaxFileCount:     DefaultTiDBCloudFreeMaxFileCount,
	}
}

func normalizeTiDBCloudFreePlanLimits(limits TiDBCloudFreePlanLimits) TiDBCloudFreePlanLimits {
	defaults := DefaultTiDBCloudFreePlanLimits()
	if limits.TenantCount <= 0 {
		limits.TenantCount = defaults.TenantCount
	}
	if limits.MaxStorageBytes <= 0 {
		limits.MaxStorageBytes = defaults.MaxStorageBytes
	}
	if limits.MaxFileSizeBytes <= 0 {
		limits.MaxFileSizeBytes = defaults.MaxFileSizeBytes
	}
	if limits.MaxFileCount <= 0 {
		limits.MaxFileCount = defaults.MaxFileCount
	}
	return limits
}

func (s *Server) normalizeTiDBCloudFreeProvisionQuota(req *quotaRequest) (*quotaRequest, error) {
	limits := normalizeTiDBCloudFreePlanLimits(s.tidbCloudFreePlanLimits)
	out := &quotaRequest{}
	if req != nil {
		*out = *req
	}
	maxStorageSize := limits.MaxStorageBytes / quotaStorageSizeBytes
	maxFileSize := limits.MaxFileSizeBytes / quotaStorageSizeBytes
	maxFileCount := limits.MaxFileCount
	zero := int64(0)

	if out.TiDBCloudSpendingLimit != nil && *out.TiDBCloudSpendingLimit > 0 {
		return nil, tenant.ErrTiDBCloudFreeSpendingLimitForbidden
	}
	if out.MaxStorageSize != nil && (*out.MaxStorageSize <= 0 || *out.MaxStorageSize > maxStorageSize) {
		return nil, tenant.ErrTiDBCloudFreeQuotaExceeded
	}
	if out.MaxFileSize != nil && (*out.MaxFileSize <= 0 || *out.MaxFileSize > maxFileSize) {
		return nil, tenant.ErrTiDBCloudFreeQuotaExceeded
	}
	if out.MaxFileCount != nil && (*out.MaxFileCount <= 0 || *out.MaxFileCount > maxFileCount) {
		return nil, tenant.ErrTiDBCloudFreeQuotaExceeded
	}
	if out.MaxStorageSize == nil {
		out.MaxStorageSize = &maxStorageSize
	}
	if out.MaxFileSize == nil {
		out.MaxFileSize = &maxFileSize
	}
	if out.MaxFileCount == nil {
		out.MaxFileCount = &maxFileCount
	}
	if out.TiDBCloudSpendingLimit == nil {
		out.TiDBCloudSpendingLimit = &zero
	}
	return out, nil
}
