package testmysql

import (
	"database/sql"
	"strings"
	"testing"
)

// EnsureContentBlobColumn repairs shared test schemas that were created by a
// provider variant without inline blob storage.
func EnsureContentBlobColumn(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`ALTER TABLE files ADD COLUMN content_blob LONGBLOB`); err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "duplicate column") || strings.Contains(msg, "already exists") || strings.Contains(msg, "already exist") {
			return
		}
		t.Fatalf("ensure content_blob column: %v", err)
	}
}
