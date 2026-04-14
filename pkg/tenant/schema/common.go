package schema

import (
	"database/sql"
	"fmt"
	"strings"
)

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

func IsTiDBCluster(db *sql.DB) bool {
	var ver string
	if err := db.QueryRow(`SELECT VERSION()`).Scan(&ver); err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(ver), "tidb")
}
