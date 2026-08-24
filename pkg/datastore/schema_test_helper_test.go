package datastore

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

func initDatastoreSchema(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	stmts := schema.TiDBAppEmbeddingTenantSchemaStatements()
	stmts = append(stmts, schema.TiDBAppEmbeddingLegacyFilesStatements()...)
	if err := schema.ExecSchemaStatements(db, stmts); err != nil {
		t.Fatal(err)
	}
}
