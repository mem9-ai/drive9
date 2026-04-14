package schema

// InitTiDBTenantSchema initializes the TiDB launch schema baseline with the
// shared database-managed auto-embedding contract used by TiDB tenants.
func InitTiDBTenantSchema(dsn string) error {
	return InitTiDBTenantSchemaForMode(dsn, TiDBEmbeddingModeAuto)
}

// InitTiDBTenantSchemaStatementsForMode returns the exact DDL statements used
// by TiDB tenant schema init for the requested embedding mode.
func InitTiDBTenantSchemaStatementsForMode(mode TiDBEmbeddingMode) ([]string, error) {
	switch mode {
	case TiDBEmbeddingModeAuto:
		return CloneStatements(tidbAutoEmbeddingSchemaStatements()), nil
	case TiDBEmbeddingModeApp:
		return CloneStatements(tidbAppEmbeddingSchemaStatements()), nil
	default:
		return nil, validateTiDBSchemaMode(mode)
	}
}

// InitTiDBTenantSchemaForMode initializes the TiDB tenant schema for the
// requested local embedding mode.
func InitTiDBTenantSchemaForMode(dsn string, mode TiDBEmbeddingMode) error {
	switch mode {
	case TiDBEmbeddingModeAuto:
		return initTiDBAutoEmbeddingSchema(dsn)
	case TiDBEmbeddingModeApp:
		return initTiDBAppEmbeddingSchema(dsn)
	default:
		return validateTiDBSchemaMode(mode)
	}
}
