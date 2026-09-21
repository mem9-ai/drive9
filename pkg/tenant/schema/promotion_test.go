package schema

import (
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
	"github.com/mem9-ai/drive9/internal/testtidb"
)

func TestPromotionTiDBSchemaStatementsExecuteInMySQL(t *testing.T) {
	db := testtidb.OpenDB(t, testDSN)
	testtidb.ResetDB(t, db)

	for _, stmt := range PromotionTiDBSchemaStatements() {
		if _, err := db.Exec(stmt); err != nil {
			if isIgnorableTiDBSchemaError(err) {
				continue
			}
			t.Fatalf("exec promotion schema %q: %v", stmt, err)
		}
	}
}

func TestPromotionSchemaContainsP0DurableFacts(t *testing.T) {
	requiredTables := []string{
		"promotion_storage_capabilities",
		"promotion_namespace_capabilities",
		"promotion_import_identity_tenants",
		"promotion_import_identity_epochs",
		"promotion_import_id_claims",
		"promotion_imports",
		"promotion_import_entries",
		"promotion_import_contents",
		"promotion_quota_reservations",
		"promotion_import_tombstones",
		"promotion_retired_import_sequences",
	}

	for name, statements := range map[string][]string{
		"tidb": PromotionTiDBSchemaStatements(),
		"db9":  PromotionDB9SchemaStatements(),
	} {
		t.Run(name, func(t *testing.T) {
			creates := promotionCreateStatements(t, statements)
			for _, table := range requiredTables {
				if creates[table] == "" {
					t.Fatalf("missing %s CREATE TABLE statement", table)
				}
			}

			imports := creates["promotion_imports"]
			for _, fact := range []string{
				"target_parent_edge_incarnation",
				"target_parent_children_generation",
				"namespace_cas_epoch",
				"accepted_restore_generation",
				"accepted_database_incarnation",
				"accepted_writer_generation",
				"state_version",
				"owner_token_hash",
				"recovery_token_hash",
				"cleanup_attempt_id",
				"cleanup_writer_generation",
				"terminal_reason",
			} {
				if !strings.Contains(imports, fact) {
					t.Fatalf("promotion_imports missing %s", fact)
				}
			}

			for _, table := range []string{
				"promotion_import_id_claims",
				"promotion_imports",
				"promotion_import_entries",
				"promotion_import_contents",
				"promotion_import_tombstones",
				"promotion_retired_import_sequences",
			} {
				stmt := creates[table]
				for _, key := range []string{"tenant_id", "allocation_epoch", "allocation_sequence"} {
					if !strings.Contains(stmt, key) {
						t.Fatalf("%s missing identity key %s", table, key)
					}
				}
			}
		})
	}
}

func TestPromotionSchemaIsIncludedInTiDBInitModes(t *testing.T) {
	for name, statements := range map[string][]string{
		"auto": tidbAutoEmbeddingSchemaStatements(),
		"app":  tidbAppEmbeddingBaseSchemaStatements(),
	} {
		t.Run(name, func(t *testing.T) {
			creates := promotionCreateStatements(t, statements)
			if creates["promotion_imports"] == "" {
				t.Fatal("tenant init schema omits promotion_imports")
			}
		})
	}
}

func promotionCreateStatements(t *testing.T, statements []string) map[string]string {
	t.Helper()
	creates := make(map[string]string)
	for _, stmt := range statements {
		table, _, ok, err := schemaspec.ParseCreateTableStatement(stmt)
		if err != nil {
			t.Fatalf("parse schema statement: %v", err)
		}
		if ok {
			creates[table] = schemaspec.NormalizeSQLFragment(stmt)
		}
	}
	return creates
}
