package schema

import (
	"testing"

	"github.com/mem9-ai/drive9/internal/testtidb"
)

func TestGitWorkspaceTiDBSchemaStatementsExecuteInMySQL(t *testing.T) {
	db := testtidb.OpenDB(t, testDSN)
	testtidb.ResetDB(t, db)

	for _, stmt := range GitWorkspaceTiDBSchemaStatements() {
		if _, err := db.Exec(stmt); err != nil {
			if isIgnorableTiDBSchemaError(err) {
				continue
			}
			t.Fatalf("exec git workspace schema %q: %v", stmt, err)
		}
	}
}
