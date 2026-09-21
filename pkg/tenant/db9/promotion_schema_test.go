package db9

import (
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
)

func TestInitSchemaStatementsIncludePromotionTables(t *testing.T) {
	found := make(map[string]bool)
	for _, stmt := range InitSchemaStatements() {
		table, _, ok, err := schemaspec.ParseCreateTableStatement(stmt)
		if err != nil {
			t.Fatalf("parse schema statement: %v", err)
		}
		if ok {
			found[table] = true
		}
	}
	for _, table := range []string{
		"promotion_storage_capabilities",
		"promotion_namespace_capabilities",
		"promotion_imports",
		"promotion_import_entries",
		"promotion_import_contents",
		"promotion_import_tombstones",
	} {
		if !found[table] {
			t.Fatalf("db9 init schema missing %s", table)
		}
	}
}
