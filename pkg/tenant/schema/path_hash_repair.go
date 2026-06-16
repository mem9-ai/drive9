package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
)

func repairMySQLPathHashSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	for _, stmt := range []string{
		`ALTER TABLE file_nodes ADD COLUMN path_hash VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE file_nodes ADD COLUMN parent_path_hash VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE uploads ADD COLUMN target_path_hash VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE uploads ADD COLUMN active_target_path_hash VARCHAR(64) AS (CASE WHEN status = 'UPLOADING' THEN target_path_hash ELSE NULL END) VIRTUAL`,
	} {
		if err := execPathHashRepairStatement(ctx, db, stmt); err != nil {
			return err
		}
	}
	if err := BackfillPathHashes(ctx, db); err != nil {
		return err
	}
	for _, idx := range []struct {
		table string
		name  string
	}{
		{"file_nodes", "idx_path"},
		{"file_nodes", "idx_parent"},
		{"uploads", "idx_upload_path"},
		{"uploads", "idx_uploads_active"},
	} {
		if err := dropMySQLPathHashIndexIfMismatched(ctx, db, idx.table, idx.name); err != nil {
			return err
		}
	}
	return nil
}

func dropMySQLPathHashIndexIfMismatched(ctx context.Context, db *sql.DB, tableName, indexName string) error {
	createStmt, err := loadShowCreateTable(ctx, db, tableName)
	if err != nil {
		if isMissingTableError(err) {
			return nil
		}
		return fmt.Errorf("show create %s for path-hash repair: %w", tableName, err)
	}
	observed, ok := parseObservedTiDBIndexColumns(createStmt)
	if !ok {
		return nil
	}
	got, ok := observed[strings.ToLower(indexName)]
	if !ok {
		return nil
	}
	want := expectedPathHashIndexColumns(tableName, indexName)
	if len(want) == 0 || equalStringSlices(got, want) {
		return nil
	}
	return execPathHashRepairStatement(ctx, db, dropPathHashIndexSQL(tableName, indexName))
}

func execPathHashRepairStatement(ctx context.Context, db *sql.DB, stmt string) error {
	if strings.TrimSpace(stmt) == "" {
		return nil
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		if isIgnorableTiDBSchemaError(err) || isMissingTableError(err) || isMissingColumnError(err) || isMissingIndexError(err) {
			return nil
		}
		return fmt.Errorf("repair path-hash schema %q: %w", schemaStatementSnippet(stmt), err)
	}
	return nil
}

func isMissingIndexError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1091
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "can't drop") && strings.Contains(msg, "check that column/key exists")
}
