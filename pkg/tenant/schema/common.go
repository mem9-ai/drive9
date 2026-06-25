package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/internal/schemaspec"
	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
)

const maxParallelSchemaStatementGroups = 8

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
	return ExecSchemaStatementsContext(context.Background(), db, stmts)
}

// ExecSchemaStatementsContext executes a sequence of DDL statements with
// contextual logging, ignoring duplicate-key / already-exists errors that
// arise from racing migrations.
func ExecSchemaStatementsContext(ctx context.Context, db *sql.DB, stmts []string) error {
	for i, stmt := range stmts {
		if err := execSchemaStatement(ctx, db, stmt, i+1, len(stmts)); err != nil {
			return err
		}
	}
	return nil
}

// ExecSchemaStatementsParallelByTableContext executes tenant bootstrap DDL with
// table-level parallelism. Statements for the same table keep source order, so a
// table's CREATE statement still precedes its indexes and ALTER statements.
func ExecSchemaStatementsParallelByTableContext(ctx context.Context, db *sql.DB, stmts []string) error {
	groups, ok := parallelSchemaStatementGroups(stmts)
	if !ok || len(groups) <= 1 {
		return ExecSchemaStatementsContext(ctx, db, stmts)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, maxParallelSchemaStatementGroups)
	errCh := make(chan error, len(groups))
	var wg sync.WaitGroup
	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			for _, stmt := range group.statements {
				if err := execSchemaStatement(ctx, db, stmt.sql, stmt.index+1, len(stmts)); err != nil {
					errCh <- err
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		// Return the first completed group failure; all groups see the cancelled
		// context before starting the next statement.
		return err
	}
	return nil
}

type parallelSchemaStatement struct {
	index int
	sql   string
}

type parallelSchemaStatementGroup struct {
	table      string
	statements []parallelSchemaStatement
}

func parallelSchemaStatementGroups(stmts []string) ([]parallelSchemaStatementGroup, bool) {
	byTable := make(map[string]int)
	groups := make([]parallelSchemaStatementGroup, 0)
	for i, stmt := range stmts {
		table, ok := schemaStatementTableName(stmt)
		if !ok {
			return nil, false
		}
		groupIdx, ok := byTable[table]
		if !ok {
			groupIdx = len(groups)
			byTable[table] = groupIdx
			groups = append(groups, parallelSchemaStatementGroup{table: table})
		}
		groups[groupIdx].statements = append(groups[groupIdx].statements, parallelSchemaStatement{index: i, sql: stmt})
	}
	return groups, true
}

func schemaStatementTableName(stmt string) (string, bool) {
	if table, _, ok, err := schemaspec.ParseCreateTableStatement(stmt); err == nil && ok {
		return table, true
	}
	fields := strings.Fields(schemaspec.NormalizeSQLFragment(stmt))
	if len(fields) < 3 {
		return "", false
	}
	if fields[0] == "alter" && fields[1] == "table" {
		return trimSQLIdentifier(fields[2]), true
	}
	if fields[0] != "create" {
		return "", false
	}
	onIdx := -1
	for i, field := range fields {
		if field == "on" {
			onIdx = i
			break
		}
	}
	if onIdx < 0 || onIdx+1 >= len(fields) {
		return "", false
	}
	table := fields[onIdx+1]
	if parenIdx := strings.Index(table, "("); parenIdx >= 0 {
		table = table[:parenIdx]
	}
	table = trimSQLIdentifier(table)
	return table, table != ""
}

func trimSQLIdentifier(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`")
}

func execSchemaStatement(ctx context.Context, db *sql.DB, stmt string, index, count int) error {
	start := time.Now()
	snippet := schemaStatementSnippet(stmt)
	logger.Info(ctx, "schema_statement_exec_started",
		zap.Int("statement_index", index),
		zap.Int("statement_count", count),
		zap.String("statement", snippet))
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		if isIgnorableSchemaError(err) {
			logger.Info(ctx, "schema_statement_exec_skipped_existing",
				zap.Int("statement_index", index),
				zap.Int("statement_count", count),
				zap.String("statement", snippet),
				zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
				zap.Error(err))
			return nil
		}
		logger.Error(ctx, "schema_statement_exec_failed",
			zap.Int("statement_index", index),
			zap.Int("statement_count", count),
			zap.String("statement", snippet),
			zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
			zap.Error(err))
		return fmt.Errorf("exec %q: %w", snippet, err)
	}
	logger.Info(ctx, "schema_statement_exec_finished",
		zap.Int("statement_index", index),
		zap.Int("statement_count", count),
		zap.String("statement", snippet),
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return nil
}

// ExecOptionalSchemaStatements executes optional DDL statements and skips
// feature-specific failures that can happen on TiDB deployments without FTS,
// vector indexing, or columnar replica support. It returns the number of
// statements skipped for those unsupported features.
func ExecOptionalSchemaStatements(ctx context.Context, db *sql.DB, stmts []string) (int, error) {
	skipped := 0
	for i, stmt := range stmts {
		start := time.Now()
		snippet := schemaStatementSnippet(stmt)
		logger.Info(ctx, "optional_schema_statement_exec_started",
			zap.Int("statement_index", i+1),
			zap.Int("statement_count", len(stmts)),
			zap.String("statement", snippet))
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if isIgnorableSchemaError(err) {
				logger.Info(ctx, "optional_schema_statement_exec_skipped_existing",
					zap.Int("statement_index", i+1),
					zap.Int("statement_count", len(stmts)),
					zap.String("statement", snippet),
					zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
					zap.Error(err))
				continue
			}
			if isIgnorableOptionalSchemaError(err) {
				skipped++
				logger.Warn(ctx, "optional_schema_statement_skipped",
					zap.Int("statement_index", i+1),
					zap.Int("statement_count", len(stmts)),
					zap.String("statement", snippet),
					zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
					zap.Error(err))
				continue
			}
			logger.Error(ctx, "optional_schema_statement_exec_failed",
				zap.Int("statement_index", i+1),
				zap.Int("statement_count", len(stmts)),
				zap.String("statement", snippet),
				zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
				zap.Error(err))
			return skipped, fmt.Errorf("exec optional %q: %w", snippet, err)
		}
		logger.Info(ctx, "optional_schema_statement_exec_finished",
			zap.Int("statement_index", i+1),
			zap.Int("statement_count", len(stmts)),
			zap.String("statement", snippet),
			zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
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
	return schemaspec.SQLSnippet(stmt)
}

func IsTiDBCluster(ctx context.Context, db *sql.DB) bool {
	var ver string
	if err := db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&ver); err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(ver), "tidb")
}
