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
