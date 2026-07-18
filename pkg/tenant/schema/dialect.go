package schema

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
)

// Shared-shape DDL is written once in TiDB form (the canonical list) and
// derived for plain MySQL mechanically, so the two dialects can never drift.
//
// Two TiDB-only constructs need rewriting for MySQL:
//
//   - the CLUSTERED marker on composite primary keys: TiDB creates composite
//     primary keys as NONCLUSTERED by default (tidb_enable_clustered_index =
//     INT_ONLY), so the shared shape must declare it explicitly to physically
//     co-locate rows of one tenant. MySQL's InnoDB primary key is always
//     clustered and the keyword is a syntax error there, so the MySQL variant
//     simply drops it — semantically a no-op on that engine.
//   - VECTOR(n) column types: plain MySQL 8.0 has no vector type, so the
//     MySQL variant maps them to LONGTEXT, mirroring the standalone
//     no-embedding MySQL schema (mysql_no_embedding.go). Vector search is
//     TiDB-only anyway; on MySQL these columns just carry text payloads.
func mysqlCompatibleSharedStatements(stmts []string) []string {
	out := make([]string, len(stmts))
	for i, stmt := range stmts {
		out[i] = mysqlCompatibleSharedStatement(stmt)
	}
	return out
}

func mysqlCompatibleSharedStatement(stmt string) string {
	return stripTiDBVectorColumnType(stripTiDBClusteredKeyword(stmt))
}

func stripTiDBClusteredKeyword(stmt string) string {
	return strings.ReplaceAll(stmt, " CLUSTERED", "")
}

var tidbVectorColumnType = regexp.MustCompile(`(?i)VECTOR\(\d+\)`)

func stripTiDBVectorColumnType(stmt string) string {
	return tidbVectorColumnType.ReplaceAllString(stmt, "LONGTEXT")
}

// JournalMySQLSharedSchemaStatements is the plain-MySQL variant of
// JournalTiDBSharedSchemaStatements, derived by removing TiDB-only keywords.
// Use it for local development databases and MySQL-backed tests/e2e.
func JournalMySQLSharedSchemaStatements() []string {
	return mysqlCompatibleSharedStatements(JournalTiDBSharedSchemaStatements())
}

// JournalSharedSchemaStatementsForDB selects the shared journal DDL matching
// the connected database's dialect: TiDB clusters get the CLUSTERED variant,
// anything else (plain MySQL, e.g. local e2e) the compatible variant.
func JournalSharedSchemaStatementsForDB(ctx context.Context, db *sql.DB) []string {
	if IsTiDBCluster(ctx, db) {
		return JournalTiDBSharedSchemaStatements()
	}
	return JournalMySQLSharedSchemaStatements()
}
