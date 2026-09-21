package db9

import (
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
)

func TestInitSchemaStatementsIncludePromotionTables(t *testing.T) {
	found := make(map[string]string)
	for _, stmt := range InitSchemaStatements() {
		table, _, ok, err := schemaspec.ParseCreateTableStatement(stmt)
		if err != nil {
			t.Fatalf("parse schema statement: %v", err)
		}
		if ok {
			found[table] = stmt
		}
	}
	for _, table := range []string{
		"promotion_storage_capabilities",
		"promotion_namespace_capabilities",
		"promotion_import_identity_global",
		"promotion_imports",
		"promotion_import_entries",
		"promotion_import_contents",
		"promotion_quota_accounts",
		"promotion_import_tombstones",
	} {
		if found[table] == "" {
			t.Fatalf("db9 init schema missing %s", table)
		}
	}
	for _, fact := range []string{"path_edge_incarnation", "children_generation"} {
		if !strings.Contains(found["file_nodes"], fact) {
			t.Fatalf("db9 file_nodes missing %s", fact)
		}
	}
}
