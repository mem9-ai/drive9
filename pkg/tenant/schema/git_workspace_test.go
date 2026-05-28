package schema

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestGitWorkspaceTiDBSchemaStatementsExecuteInMySQL(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, stmt := range []string{
		"DROP TABLE IF EXISTS git_workspace_overlay",
		"DROP TABLE IF EXISTS git_workspace_object_packs",
		"DROP TABLE IF EXISTS git_workspace_git_state",
		"DROP TABLE IF EXISTS git_workspace_tree_nodes",
		"DROP TABLE IF EXISTS git_workspaces",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	for _, stmt := range GitWorkspaceTiDBSchemaStatements() {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec git workspace schema %q: %v", stmt, err)
		}
	}
}
