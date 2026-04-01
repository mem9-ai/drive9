package tenant

// InitTiDBTenantSchema initializes the TiDB launch schema baseline with the
// shared database-managed auto-embedding contract used by TiDB tenants.
func InitTiDBTenantSchema(dsn string) error {
	return InitTiDBTenantSchemaForMode(dsn, TiDBEmbeddingModeAuto)
}

// InitTiDBTenantSchemaForMode initializes the TiDB tenant schema for the
// requested local embedding mode.
func InitTiDBTenantSchemaForMode(dsn string, mode TiDBEmbeddingMode) error {
	switch mode {
	case TiDBEmbeddingModeAuto:
		return initZeroSchema(dsn)
	case TiDBEmbeddingModeApp:
		return initTiDBAppEmbeddingSchema(dsn, true)
	default:
		return validateTiDBSchemaMode(mode)
	}
}

func initZeroSchema(dsn string) error {
	return initTiDBAutoEmbeddingSchema(dsn, true)
}
