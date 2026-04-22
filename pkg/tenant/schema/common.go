package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
)

// CloneStatements returns a copy of the given schema statements so callers can
// format or extend them without mutating the init-schema source of truth.
func CloneStatements(stmts []string) []string {
	cloned := make([]string, len(stmts))
	copy(cloned, stmts)
	return cloned
}

// FormatStatementsSQL renders schema statements as executable SQL text.
func FormatStatementsSQL(stmts []string) string {
	var b strings.Builder
	for i, stmt := range stmts {
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(strings.TrimSpace(stmt))
		b.WriteString(";\n")
	}
	return b.String()
}

// ExecSchemaStatements executes a sequence of DDL statements, ignoring
// duplicate-key / already-exists errors that arise from racing migrations.
func ExecSchemaStatements(db *sql.DB, stmts []string) error {
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			if isIgnorableSchemaError(err) {
				continue
			}
			snippet := stmt
			if len(snippet) > 80 {
				snippet = snippet[:80]
			}
			return fmt.Errorf("exec %q: %w", snippet, err)
		}
	}
	return nil
}

// ExecOptionalSchemaStatements executes optional DDL statements and skips
// feature-specific failures that can happen on TiDB deployments without FTS,
// vector indexing, or columnar replica support. It returns the number of
// statements skipped for those unsupported features.
func ExecOptionalSchemaStatements(ctx context.Context, db *sql.DB, stmts []string) (int, error) {
	skipped := 0
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			if isIgnorableSchemaError(err) {
				continue
			}
			if isIgnorableOptionalSchemaError(err) {
				skipped++
				logger.Warn(ctx, "optional_schema_statement_skipped",
					zap.String("statement", schemaStatementSnippet(stmt)),
					zap.Error(err))
				continue
			}
			return skipped, fmt.Errorf("exec optional %q: %w", schemaStatementSnippet(stmt), err)
		}
	}
	return skipped, nil
}

func HasMultiStatements(dsn string) bool {
	lower := strings.ToLower(dsn)
	return strings.Contains(lower, "multistatements=true") || strings.Contains(lower, "multistatements=1")
}

func isIgnorableSchemaError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "already exist") ||
		strings.Contains(msg, "duplicate column")
}

func isIgnorableOptionalSchemaError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && !isIgnorableOptionalMySQLErrorCode(mysqlErr.Number) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "add_columnar_replica_on_demand") ||
		strings.Contains(msg, "with parser multilingual") ||
		strings.Contains(msg, "fulltext") ||
		strings.Contains(msg, "vector index") ||
		strings.Contains(msg, "vec_cosine_distance")
}

func isIgnorableOptionalMySQLErrorCode(code uint16) bool {
	switch code {
	case 1064, 1105, 1235, 8200:
		return true
	default:
		return false
	}
}

func schemaStatementSnippet(stmt string) string {
	snippet := stmt
	if len(snippet) > 80 {
		snippet = snippet[:80]
	}
	return snippet
}

func IsTiDBCluster(ctx context.Context, db *sql.DB) bool {
	var ver string
	if err := db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&ver); err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(ver), "tidb")
}
