package server

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

func initServerTenantSchema(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	stmts := schema.MySQLNoEmbeddingTenantSchemaStatements()
	stmts = append(stmts, schema.MySQLNoEmbeddingLegacyFilesStatements()...)
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "Duplicate key name") || strings.Contains(msg, "already exists") || strings.Contains(msg, "Duplicate column") {
				continue
			}
			t.Fatal(err)
		}
	}
}
