package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/mysqlutil"
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

// TestEnsureSharedSchemaReportsSchemaRoleMetrics pins the alerting contract for
// Drive9SharedDBOperationP99High: init-schema DDL targets the same physical
// shared database that serves tenant traffic, and it once ran on the
// role="shared" data-plane handle. It now gets a handle of its own, so the DDL
// must be observed under the shared_schema role and leave the shared
// data-plane latency series untouched.
func TestEnsureSharedSchemaReportsSchemaRoleMetrics(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSharedSchemaDB(ctx, testDSN, "pool-uuid-shared-schema-metrics", "org-shared-schema-metrics")
	if err != nil {
		t.Fatalf("open shared schema db: %v", err)
	}
	t.Cleanup(func() { _ = mysqlutil.CloseInstrumented(db) })
	dropSharedSchemaTables(t, db)
	t.Cleanup(func() { dropSharedSchemaTables(t, db) })

	sharedBefore := dbOperationCount(t, "exec", mysqlutil.RoleShared)
	schemaBefore := dbOperationCount(t, "exec", mysqlutil.RoleSharedSchema)

	if err := EnsureSharedSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSharedSchema: %v", err)
	}

	if got := dbOperationCount(t, "exec", mysqlutil.RoleSharedSchema) - schemaBefore; got <= 0 {
		t.Fatalf("shared_schema exec operations recorded = %v, want > 0", got)
	}
	if got := dbOperationCount(t, "exec", mysqlutil.RoleShared) - sharedBefore; got != 0 {
		t.Fatalf("shared exec operations recorded during schema ensure = %v, want 0", got)
	}
}

func TestPrometheusSampleValueUsesTheSampleNotTimestampOrExemplar(t *testing.T) {
	for _, test := range []struct {
		name string
		line string
		want float64
	}{
		{name: "plain", line: `drive9_db_operations_total{role="shared"} 3`, want: 3},
		{name: "timestamp", line: `drive9_db_operations_total{role="shared"} 3 1722247200000`, want: 3},
		{name: "exemplar", line: `drive9_db_operations_total{role="shared"} 3 # {trace_id="abc"} 1.5`, want: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := prometheusSampleValue(test.line)
			if err != nil {
				t.Fatalf("prometheusSampleValue(%q): %v", test.line, err)
			}
			if got != test.want {
				t.Fatalf("prometheusSampleValue(%q) = %v, want %v", test.line, got, test.want)
			}
		})
	}
}

// dbOperationCount sums drive9_db_operations_total across every result label for
// one operation/role pair.
func dbOperationCount(t *testing.T, operation, role string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	total := 0.0
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, "drive9_db_operations_total{") {
			continue
		}
		if !strings.Contains(line, `operation="`+operation+`"`) || !strings.Contains(line, `role="`+role+`"`) {
			continue
		}
		value, err := prometheusSampleValue(line)
		if err != nil {
			t.Fatalf("parse metric line %q: %v", line, err)
		}
		total += value
	}
	return total
}

func prometheusSampleValue(line string) (float64, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, fmt.Errorf("metric line has no sample value")
	}
	return strconv.ParseFloat(fields[1], 64)
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
