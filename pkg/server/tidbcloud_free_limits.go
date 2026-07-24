package server

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
