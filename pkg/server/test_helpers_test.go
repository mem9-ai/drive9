package server

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
)

func resetServerTestState(t *testing.T, dsn string, db *sql.DB) {
	t.Helper()

	initServerTenantSchema(t, dsn)
	testmysql.ResetDB(t, db)
	resetServerMetaTables(t, db)
}

func resetServerMetaTables(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, query := range []string{"DELETE FROM tenant_api_keys", "DELETE FROM tenants"} {
		if _, err := db.Exec(query); err != nil && !isMissingTableErr(err) {
			t.Fatalf("reset server meta tables: %v", err)
		}
	}
}

func isMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "unknown table")
}
