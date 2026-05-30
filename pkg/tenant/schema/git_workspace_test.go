package schema

import (
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
)

func TestGitWorkspaceTiDBSchemaStatementsExecuteInMySQL(t *testing.T) {
	db := testmysql.OpenDB(t, testDSN)
	testmysql.ResetDB(t, db)

	for _, stmt := range GitWorkspaceTiDBSchemaStatements() {
		if _, err := db.Exec(stmt); err != nil {
			if isIgnorableTiDBSchemaError(err) {
				continue
			}
			t.Fatalf("exec git workspace schema %q: %v", stmt, err)
		}
	}
}
