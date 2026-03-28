package tenant

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	autoEmbedTextModel  = "tidbcloud_free/amazon/titan-embed-text-v2"
	autoEmbedImageModel = "tidbcloud_free/amazon/titan-embed-image-v1"
	autoEmbedDims       = 1024
)

func execSchemaStatements(db *sql.DB, stmts []string) error {
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

func hasMultiStatements(dsn string) bool {
	lower := strings.ToLower(dsn)
	return strings.Contains(lower, "multistatements=true") || strings.Contains(lower, "multistatements=1")
}

func isIgnorableSchemaError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "duplicate column")
}

func isTiDBCluster(db *sql.DB) bool {
	var ver string
	if err := db.QueryRow(`SELECT VERSION()`).Scan(&ver); err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(ver), "tidb")
}
