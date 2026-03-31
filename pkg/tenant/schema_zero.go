package tenant

// InitTiDBTenantSchema initializes the TiDB launch schema baseline with the
// shared database-managed auto-embedding contract used by TiDB tenants.
func InitTiDBTenantSchema(dsn string) error {
	return initZeroSchema(dsn)
}

func initZeroSchema(dsn string) error {
	return initTiDBAutoEmbeddingSchema(dsn, true)
}
