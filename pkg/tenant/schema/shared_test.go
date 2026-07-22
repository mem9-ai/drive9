package schema

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
	"github.com/mem9-ai/drive9/internal/testmysql"
)

// sharedSchemaTables lists the 30 tables of the shared (multi-tenant) schema
// shape across the five statement groups. llm_usage is intentionally absent:
// the central meta DB ledger is authoritative in multi-tenant deployments.
var sharedSchemaTables = []string{
	// Core FS (9).
	"file_nodes",
	"inodes",
	"contents",
	"semantic",
	"file_tags",
	"uploads",
	"semantic_tasks",
	"file_gc_tasks",
	"fs_events",
	// Journal (5).
	"journals",
	"journal_labels",
	"journal_append_requests",
	"journal_entries",
	"journal_entry_subjects",
	// Vault (6).
	"vault_deks",
	"vault_secrets",
	"vault_secret_fields",
	"vault_tokens",
	"vault_grants",
	"vault_audit_log",
	// Git workspace (5).
	"git_workspaces",
	"git_workspace_tree_nodes",
	"git_workspace_git_state",
	"git_workspace_object_packs",
	"git_workspace_overlay",
	// FS layer (5).
	"fs_layers",
	"fs_layer_tags",
	"fs_layer_entries",
	"fs_layer_events",
	"fs_layer_checkpoints",
}

// TestSharedTiDBSchemaStatementsContainsAllTables pins the aggregate list to
// exactly the 30 expected tables across the five groups: every statement must
// be a CREATE TABLE for one of them, with no duplicates and no extras.
func TestSharedTiDBSchemaStatementsContainsAllTables(t *testing.T) {
	stmts := SharedTiDBSchemaStatements()
	if len(stmts) != len(sharedSchemaTables) {
		t.Fatalf("shared schema has %d statements, want %d", len(stmts), len(sharedSchemaTables))
	}
	seen := make(map[string]bool, len(stmts))
	for i, stmt := range stmts {
		tableName, _, ok, err := schemaspec.ParseCreateTableStatement(stmt)
		if err != nil || !ok {
			t.Fatalf("statement %d is not a CREATE TABLE: ok=%t err=%v", i, ok, err)
		}
		if seen[tableName] {
			t.Errorf("duplicate CREATE TABLE for %s", tableName)
		}
		seen[tableName] = true
	}
	for _, tableName := range sharedSchemaTables {
		if !seen[tableName] {
			t.Errorf("shared schema missing table %s", tableName)
		}
	}
	if seen["llm_usage"] {
		t.Errorf("shared schema must not contain llm_usage")
	}
}

func TestCurrentSharedTiDBSchemaVersionIsDerivedFromSharedStatements(t *testing.T) {
	if CurrentSharedTiDBSchemaVersion <= 0 {
		t.Fatalf("CurrentSharedTiDBSchemaVersion = %d, want positive", CurrentSharedTiDBSchemaVersion)
	}
	if got, want := CurrentSharedTiDBSchemaVersion, currentTiDBTenantSchemaVersion(SharedTiDBSchemaStatements()); got != want {
		t.Fatalf("CurrentSharedTiDBSchemaVersion = %d, want derived value %d", got, want)
	}
	changed := append([]string(nil), SharedTiDBSchemaStatements()...)
	changed[0] += "\nCREATE INDEX idx_version_probe ON file_nodes (fs_id)"
	if got := currentTiDBTenantSchemaVersion(changed); got == CurrentSharedTiDBSchemaVersion {
		t.Fatalf("derived version did not change after schema content change: %d", got)
	}
}

func TestSharedSchemaContractUsesSharedStatementShape(t *testing.T) {
	spec, err := tidbSchemaSpecFromStatements(SharedTiDBSchemaStatements())
	if err != nil {
		t.Fatalf("tidbSchemaSpecFromStatements: %v", err)
	}
	foundFileNodes := false
	foundFileGCTasks := false
	for _, table := range spec.tables {
		switch table.name {
		case "file_nodes":
			foundFileNodes = true
			idx, ok := table.indexes["idx_path"]
			if !ok || !equalStringSlices(idx.columns, []string{"fs_id", "path_hash"}) {
				t.Fatalf("shared file_nodes.idx_path columns = %#v, want fs_id/path_hash", idx.columns)
			}
		case "file_gc_tasks":
			foundFileGCTasks = true
			if !equalStringSlices(table.primaryKey.columns, []string{"fs_id", "task_id"}) {
				t.Fatalf("shared file_gc_tasks primary key = %#v, want fs_id/task_id", table.primaryKey.columns)
			}
		}
	}
	if !foundFileNodes || !foundFileGCTasks {
		t.Fatalf("shared contract missing required core tables: file_nodes=%v file_gc_tasks=%v", foundFileNodes, foundFileGCTasks)
	}
}

// TestSharedMySQLSchemaStatementsDialect ensures the MySQL variant carries no
// TiDB-only constructs — no CLUSTERED keyword and no VECTOR(n) column types —
// while keeping the same 30 tables.
func TestSharedMySQLSchemaStatementsDialect(t *testing.T) {
	stmts := SharedMySQLSchemaStatements()
	if len(stmts) != len(sharedSchemaTables) {
		t.Fatalf("mysql shared schema has %d statements, want %d", len(stmts), len(sharedSchemaTables))
	}
	seen := make(map[string]bool, len(stmts))
	for i, stmt := range stmts {
		if strings.Contains(stmt, "CLUSTERED") {
			t.Errorf("mysql variant retains CLUSTERED keyword:\n%s", stmt)
		}
		if tidbVectorColumnType.MatchString(stmt) {
			t.Errorf("mysql variant retains VECTOR(n) column type:\n%s", stmt)
		}
		tableName, _, ok, err := schemaspec.ParseCreateTableStatement(stmt)
		if err != nil || !ok {
			t.Fatalf("statement %d is not a CREATE TABLE: ok=%t err=%v", i, ok, err)
		}
		seen[tableName] = true
	}
	for _, tableName := range sharedSchemaTables {
		if !seen[tableName] {
			t.Errorf("mysql shared schema missing table %s", tableName)
		}
	}
}

// TestSharedSchemaStatementsForDBSelectsMySQL verifies that the dialect
// selector returns the MySQL-compatible variant against a plain MySQL
// instance.
func TestSharedSchemaStatementsForDBSelectsMySQL(t *testing.T) {
	db := testmysql.OpenDB(t, testDSN)

	got, err := SharedSchemaStatementsForDB(context.Background(), db)
	if err != nil {
		t.Fatalf("SharedSchemaStatementsForDB: %v", err)
	}
	want := SharedMySQLSchemaStatements()
	if len(got) != len(want) {
		t.Fatalf("ForDB returned %d statements, want %d", len(got), len(want))
	}
	for i := range want {
		if schemaspec.NormalizeSQLFragment(got[i]) != schemaspec.NormalizeSQLFragment(want[i]) {
			t.Errorf("statement %d differs from the MySQL variant:\nForDB: %s\nMySQL: %s", i, got[i], want[i])
		}
	}
}

func TestEnsureSharedSchemaRejectsExistingStandaloneTableShape(t *testing.T) {
	db := testmysql.OpenDB(t, testDSN)
	testmysql.ResetDB(t, db)
	dropSharedSchemaTables(t, db)
	t.Cleanup(func() { dropSharedSchemaTables(t, db) })
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE file_gc_tasks (
		task_id VARCHAR(64) PRIMARY KEY,
		file_id VARCHAR(64) NOT NULL,
		storage_type VARCHAR(32) NOT NULL
	)`); err != nil {
		t.Fatalf("create standalone file_gc_tasks: %v", err)
	}

	err := EnsureSharedSchema(context.Background(), db)
	if err == nil {
		t.Fatal("EnsureSharedSchema succeeded with standalone file_gc_tasks shape")
	}
	var diffErr *sharedSchemaDiffError
	if !errors.As(err, &diffErr) {
		t.Fatalf("EnsureSharedSchema error = %v, want sharedSchemaDiffError", err)
	}
	if !strings.Contains(err.Error(), "file_gc_tasks") || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("EnsureSharedSchema error = %v, want file_gc_tasks primary-key mismatch", err)
	}
}

func TestEnsureSharedSchemaValidatesFreshSchema(t *testing.T) {
	db := testmysql.OpenDB(t, testDSN)
	testmysql.ResetDB(t, db)
	dropSharedSchemaTables(t, db)
	t.Cleanup(func() { dropSharedSchemaTables(t, db) })
	if err := EnsureSharedSchema(context.Background(), db); err != nil {
		t.Fatalf("EnsureSharedSchema fresh database: %v", err)
	}
}

func dropSharedSchemaTables(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}) {
	t.Helper()
	for _, tableName := range sharedSchemaTables {
		if _, err := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tableName); err != nil {
			t.Fatalf("drop shared table %s: %v", tableName, err)
		}
	}
}
