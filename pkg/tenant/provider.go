package tenant

import "fmt"

const (
	ProviderDB9              = "db9"
	ProviderTiDBZero         = "tidb_zero"
	ProviderTiDBCloudStarter = "tidb_cloud_starter"

	currentDB9SchemaVersion              = 1
	currentTiDBZeroSchemaVersion         = 2
	currentTiDBCloudStarterSchemaVersion = 1
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

func CurrentSchemaVersion(provider string) int {
	switch provider {
	case ProviderDB9:
		return currentDB9SchemaVersion
	case ProviderTiDBZero:
		return currentTiDBZeroSchemaVersion
	case ProviderTiDBCloudStarter:
		return currentTiDBCloudStarterSchemaVersion
	default:
		return 1
	}
}
