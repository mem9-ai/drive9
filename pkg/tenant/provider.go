package tenant

import "fmt"

const (
	ProviderDB9             = "db9"
	ProviderTiDBZero        = "tidb_zero"
	ProviderTiDBCloudNative = "tidb_cloud_native"

	// ProviderTiDBCloudNativeShared is the persisted provider of a tenant
	// placed on a shared-schema (multi-tenant) TiDB database. It is assigned
	// as the independent new-tenant default while reusing the native TiDB
	// Cloud provisioner, and lets provider-driven capability checks distinguish shared tenants
	// from dedicated-cluster ones without an extra placement lookup:
	// TiDB-backed, but no per-tenant cluster to delete or fork, and no
	// database auto-embedding (the shared schema has no generated columns).
	ProviderTiDBCloudNativeShared = "tidb_cloud_native_shared"

	// ProviderTiDBCloudStarterLegacy is kept only for tenant rows persisted
	// before starter provisioning was removed. Do not accept it for new
	// provisioning configuration.
	ProviderTiDBCloudStarterLegacy = "tidb_cloud_starter"
)

func NormalizeProvider(provider string) (string, error) {
	switch provider {
	case ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudNative, ProviderTiDBCloudNativeShared:
		return provider, nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", provider)
	}
}

// IsSharedSchemaProvider reports whether provider is the persisted provider
// of a tenant placed on a shared-schema database.
func IsSharedSchemaProvider(provider string) bool {
	return provider == ProviderTiDBCloudNativeShared
}

// UsesTiDBCloudNativeCredentials reports whether provider uses the TiDB Cloud
// Native public/private-key request and default-credential contract.
func UsesTiDBCloudNativeCredentials(provider string) bool {
	return provider == ProviderTiDBCloudNative || provider == ProviderTiDBCloudNativeShared
}

func SmallInDB(provider string) bool {
	switch provider {
	case ProviderTiDBZero, ProviderTiDBCloudNative, ProviderTiDBCloudNativeShared, ProviderTiDBCloudStarterLegacy:
		return true
	default:
		return false
	}
}

// UsesTiDBAutoEmbedding reports whether the provider should run the TiDB
// database-managed auto-embedding mode. Shared-schema tenants never do: the
// shared tables carry plain VECTOR columns, not per-tenant EMBED_TEXT
// generated columns.
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
// Shared-schema tenants have no per-tenant cluster; their delete path purges
// rows by fs_id instead.
func SupportsClusterDelete(provider string) bool {
	return provider == ProviderTiDBCloudNative || provider == ProviderTiDBCloudStarterLegacy
}
