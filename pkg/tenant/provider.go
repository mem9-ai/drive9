package tenant

import "fmt"

const (
	ProviderDB9             = "db9"
	ProviderTiDBZero        = "tidb_zero"
	ProviderTiDBCloudNative = "tidb_cloud_native"

	// ProviderTiDBCloudStarterLegacy is kept only for tenant rows persisted
	// before starter provisioning was removed. Do not accept it for new
	// provisioning configuration.
	ProviderTiDBCloudStarterLegacy = "tidb_cloud_starter"
)

func NormalizeProvider(provider string) (string, error) {
	switch provider {
	case ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudNative:
		return provider, nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", provider)
	}
}

func SmallInDB(provider string) bool {
	switch provider {
	case ProviderTiDBZero, ProviderTiDBCloudNative, ProviderTiDBCloudStarterLegacy:
		return true
	default:
		return false
	}
}

// UsesTiDBAutoEmbedding reports whether the provider should run the TiDB
// database-managed auto-embedding mode.
func UsesTiDBAutoEmbedding(provider string) bool {
	switch provider {
	case ProviderTiDBZero, ProviderTiDBCloudNative, ProviderTiDBCloudStarterLegacy:
		return true
	default:
		return false
	}
}

// SupportsClusterDelete reports whether a persisted tenant provider can delete
// its backing TiDB Cloud cluster. The legacy starter value is kept only so rows
// persisted before starter provisioning was removed keep their cleanup path.
func SupportsClusterDelete(provider string) bool {
	return provider == ProviderTiDBCloudNative || provider == ProviderTiDBCloudStarterLegacy
}
