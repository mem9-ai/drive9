package tenant

import "fmt"

const (
	ProviderDB9              = "db9"
	ProviderTiDBZero         = "tidb_zero"
	ProviderTiDBCloudStarter = "tidb_cloud_starter"
	ProviderTiDBCloudNative  = "tidbcloud-native"
)

func NormalizeProvider(provider string) (string, error) {
	switch provider {
	case ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudStarter, ProviderTiDBCloudNative:
		return provider, nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", provider)
	}
}

func SmallInDB(provider string) bool {
	return provider == ProviderTiDBZero || provider == ProviderTiDBCloudStarter || provider == ProviderTiDBCloudNative
}

// UsesTiDBAutoEmbedding reports whether the provider should run the TiDB
// database-managed auto-embedding mode.
func UsesTiDBAutoEmbedding(provider string) bool {
	switch provider {
	case ProviderTiDBZero, ProviderTiDBCloudStarter, ProviderTiDBCloudNative:
		return true
	default:
		return false
	}
}
