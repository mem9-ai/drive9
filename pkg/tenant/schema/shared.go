package schema

import (
	"context"
	"database/sql"

	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
)

// SharedTiDBSchemaStatements returns the complete shared (multi-tenant) schema
// DDL for TiDB in one list: the Core FS, journal, vault, git workspace, and FS
// layer shared shapes concatenated in creation order. The shared schema has 31
// tables; llm_usage is deliberately omitted (the central meta DB ledger is
// authoritative in multi-tenant deployments — see
// CoreFSTiDBSharedSchemaStatements). There are no foreign keys, so statement
// order is informational only. For plain MySQL use SharedMySQLSchemaStatements.
//
// Schema versioning for shared databases is intentionally not wired up yet:
// per-physical-DB versioning arrives with the routing phase, so init is a
// plain idempotent apply of this statement list for now.
func SharedTiDBSchemaStatements() []string {
	stmts := CoreFSTiDBSharedSchemaStatements()
	stmts = append(stmts, JournalTiDBSharedSchemaStatements()...)
	stmts = append(stmts, VaultTiDBSharedSchemaStatements()...)
	stmts = append(stmts, GitWorkspaceTiDBSharedSchemaStatements()...)
	stmts = append(stmts, FSLayerTiDBSharedSchemaStatements()...)
	return stmts
}

// SharedMySQLSchemaStatements is the plain-MySQL variant of
// SharedTiDBSchemaStatements, derived by removing TiDB-only keywords. Use it
// for local development databases and MySQL-backed tests/e2e.
func SharedMySQLSchemaStatements() []string {
	return mysqlCompatibleSharedStatements(SharedTiDBSchemaStatements())
}

// SharedSchemaStatementsForDB selects the shared schema DDL matching the
// connected database's dialect: TiDB clusters get the CLUSTERED variant,
// anything else (plain MySQL, e.g. local e2e) the compatible variant.
func SharedSchemaStatementsForDB(ctx context.Context, db *sql.DB) []string {
	if IsTiDBCluster(ctx, db) {
		return SharedTiDBSchemaStatements()
	}
	return SharedMySQLSchemaStatements()
}

// InitSharedSchema initializes the shared (multi-tenant) schema on the
// database at dsn: it applies SharedSchemaStatementsForDB with the usual
// idempotent-DDL error tolerance (already-exists / duplicate key / duplicate
// column errors are skipped) and then, on TiDB clusters only, best-effort
// applies the optional FTS/vector indexes. Those optional indexes require
// TiDB Cloud features — plain TiDB without columnar support rejects them — so
// their failures are tolerated with a warning instead of failing init.
func InitSharedSchema(ctx context.Context, dsn string) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	if err := ExecSchemaStatementsParallelByTableContext(ctx, db, SharedSchemaStatementsForDB(ctx, db)); err != nil {
		return err
	}
	if !IsTiDBCluster(ctx, db) {
		return nil
	}
	skipped, err := ExecOptionalSchemaStatements(ctx, db, TiDBSharedOptionalSchemaStatements())
	if err != nil {
		logger.Warn(ctx, "shared_schema_optional_indexes_failed", zap.Error(err))
	} else if skipped > 0 {
		logger.Warn(ctx, "shared_schema_optional_indexes_skipped",
			zap.Int("skipped_count", skipped),
			zap.String("reason", "unsupported_tidb_features"))
	}
	return nil
}
