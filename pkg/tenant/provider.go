package tenant

import "fmt"

const (
	ProviderDB9              = "db9"
	ProviderTiDBZero         = "tidb_zero"
	ProviderTiDBCloudStarter = "tidb_cloud_starter"
)

func NormalizeProvider(provider string) (string, error) {
	switch provider {
	case ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudStarter:
		return provider, nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", provider)
	}
}

func SmallInDB(provider string) bool {
	return provider == ProviderTiDBZero || provider == ProviderTiDBCloudStarter
}

// UsesTiDBAutoEmbedding reports whether the provider should run the TiDB
// database-managed auto-embedding mode. The foundation phase keeps this
// disabled so provider defaults remain unchanged until the rollout switch
// lands in a dedicated follow-up commit.
func UsesTiDBAutoEmbedding(provider string) bool {
	switch provider {
	case ProviderTiDBZero, ProviderTiDBCloudStarter:
		return false
	default:
		return false
	}
}
