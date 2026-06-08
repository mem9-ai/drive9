package schema

import "context"

type InitTiDBTenantSchemaOptions struct {
	// AllowUnsupportedOptionalIndexes is only for local bootstrap flows that need
	// the app-managed schema without requiring every TiDB deployment to support
	// FTS/vector optional DDL during init.
	AllowUnsupportedOptionalIndexes bool
}

// InitTiDBTenantSchema initializes the TiDB launch schema baseline with the
// shared database-managed auto-embedding contract used by TiDB tenants.
func InitTiDBTenantSchema(dsn string) error {
	return InitTiDBTenantSchemaForMode(dsn, TiDBEmbeddingModeAuto)
}

// InitTiDBTenantSchemaStatementsForMode returns the exact DDL statements used
// by TiDB tenant schema init for the requested embedding mode.
func InitTiDBTenantSchemaStatementsForMode(mode TiDBEmbeddingMode) ([]string, error) {
	return initTiDBTenantSchemaStatementsForModeWithConfig(mode, currentTiDBAutoEmbeddingRenderConfig())
}

func initTiDBTenantSchemaStatementsForModeWithConfig(mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) ([]string, error) {
	switch mode {
	case TiDBEmbeddingModeAuto:
		return CloneStatements(tidbAutoEmbeddingSchemaStatementsForConfig(cfg)), nil
	case TiDBEmbeddingModeApp:
		return CloneStatements(tidbAppEmbeddingSchemaStatements()), nil
	default:
		return nil, validateTiDBSchemaMode(mode)
	}
}

// InitTiDBTenantSchemaStatementsForAutoEmbeddingConfig returns tenant init DDL
// rendered from a tenant-persisted auto-embedding profile.
func InitTiDBTenantSchemaStatementsForAutoEmbeddingConfig(cfg TiDBAutoEmbeddingConfig) ([]string, error) {
	render, err := tidbAutoEmbeddingRenderConfigFor(cfg)
	if err != nil {
		return nil, err
	}
	return CloneStatements(tidbAutoEmbeddingSchemaStatementsForConfig(render)), nil
}

func InitTiDBTenantSchemaStatementsForAutoEmbeddingProfile(profile TiDBAutoEmbeddingProfile) ([]string, error) {
	render, err := tidbAutoEmbeddingRenderConfigForProfile(profile)
	if err != nil {
		return nil, err
	}
	return CloneStatements(tidbAutoEmbeddingSchemaStatementsForConfig(render)), nil
}

// InitTiDBTenantSchemaForMode initializes the TiDB tenant schema for the
// requested local embedding mode.
func InitTiDBTenantSchemaForMode(dsn string, mode TiDBEmbeddingMode) error {
	return InitTiDBTenantSchemaForModeWithOptions(dsn, mode, InitTiDBTenantSchemaOptions{})
}

// InitTiDBTenantSchemaForModeWithOptions initializes the TiDB tenant schema for
// the requested local embedding mode with caller-controlled compatibility
// toggles.
func InitTiDBTenantSchemaForModeWithOptions(dsn string, mode TiDBEmbeddingMode, opts InitTiDBTenantSchemaOptions) error {
	return InitTiDBTenantSchemaForModeWithOptionsContext(context.Background(), dsn, mode, opts)
}

// InitTiDBTenantSchemaForModeWithOptionsContext initializes the TiDB tenant
// schema for the requested local embedding mode with caller-controlled
// compatibility toggles and log context.
func InitTiDBTenantSchemaForModeWithOptionsContext(ctx context.Context, dsn string, mode TiDBEmbeddingMode, opts InitTiDBTenantSchemaOptions) error {
	switch mode {
	case TiDBEmbeddingModeAuto:
		return initTiDBAutoEmbeddingSchema(ctx, dsn)
	case TiDBEmbeddingModeApp:
		return initTiDBAppEmbeddingSchema(ctx, dsn, opts)
	default:
		return validateTiDBSchemaMode(mode)
	}
}

// InitTiDBTenantSchemaForAutoEmbeddingConfigContext initializes the TiDB auto
// embedding schema rendered from a tenant-persisted profile.
func InitTiDBTenantSchemaForAutoEmbeddingConfigContext(ctx context.Context, dsn string, cfg TiDBAutoEmbeddingConfig) error {
	return initTiDBAutoEmbeddingSchemaWithConfig(ctx, dsn, cfg)
}

func InitTiDBTenantSchemaForAutoEmbeddingProfileContext(ctx context.Context, dsn string, profile TiDBAutoEmbeddingProfile) error {
	return initTiDBAutoEmbeddingSchemaWithProfile(ctx, dsn, profile)
}
