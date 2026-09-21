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
		"promotion_import_identity_global",
		"promotion_import_id_claims",
		"promotion_imports",
		"promotion_import_entries",
		"promotion_import_contents",
		"promotion_quota_reservations",
		"promotion_quota_accounts",
		"promotion_import_tombstones",
		"promotion_retired_import_sequences",
	}

	for name, statements := range map[string][]string{
		"tidb": PromotionTiDBSchemaStatements(),
		"db9":  PromotionDB9SchemaStatements(),
	} {
		t.Run(name, func(t *testing.T) {
			creates := promotionCreateStatements(t, statements)
			allStatements := strings.ToLower(strings.Join(statements, "\n"))
			for _, fact := range []string{
				"promotion_migration_id",
				"promotion_target_path",
				"promotion_tree_generation",
				"uk_fs_events_promotion_migration",
			} {
				if !strings.Contains(allStatements, fact) {
					t.Fatalf("fs_events promotion outbox schema missing %s", fact)
				}
			}
			for _, table := range requiredTables {
				if creates[table] == "" {
					t.Fatalf("missing %s CREATE TABLE statement", table)
				}
			}

			imports := creates["promotion_imports"]
			if name == "db9" {
				for _, table := range []string{"promotion_imports", "promotion_import_tombstones"} {
					stmt := creates[table]
					if !strings.Contains(stmt, "terminal_result_blob text") || strings.Contains(stmt, "terminal_result_blob jsonb") {
						t.Fatalf("%s must preserve terminal result bytes in TEXT: %s", table, stmt)
					}
				}
			}
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
			for _, invariant := range []string{"chk_promotion_import_aborting_owner", "state <> 'ABORTING'", "cleanup_attempt_id IS NOT NULL"} {
				if !strings.Contains(imports, strings.ToLower(invariant)) && !strings.Contains(imports, invariant) {
					t.Fatalf("promotion_imports missing invariant %s", invariant)
				}
			}

			identityTenant := creates["promotion_import_identity_tenants"]
			for _, fact := range []string{
				"installed_allocation_lease_generation",
				"max_live_claims",
				"max_materialized_identities",
				"max_sequence_window",
				"allocation_rate_per_minute",
				"allocation_rate_burst",
				"config_generation",
			} {
				if !strings.Contains(identityTenant, fact) {
					t.Fatalf("promotion identity tenant missing %s", fact)
				}
			}
			identityGlobal := creates["promotion_import_identity_global"]
			for _, fact := range []string{"max_materialized_identities", "config_generation"} {
				if !strings.Contains(identityGlobal, fact) {
					t.Fatalf("promotion identity global missing %s", fact)
				}
			}

			namespace := creates["promotion_namespace_capabilities"]
			for _, fact := range []string{"root_inode", "root_edge_incarnation", "root_children_generation"} {
				if !strings.Contains(namespace, fact) {
					t.Fatalf("promotion_namespace_capabilities missing %s", fact)
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
			fileNodes := creates["file_nodes"]
			for _, fact := range []string{"path_edge_incarnation", "children_generation"} {
				if !strings.Contains(fileNodes, fact) {
					t.Fatalf("file_nodes missing %s", fact)
				}
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
