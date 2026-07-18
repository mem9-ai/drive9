package datastore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

// installSharedTables drops the named tables, applies the given shared
// (fs_id) DDL, and restores the standalone schema on cleanup. Tests in this
// package run sequentially, so the swap is safe. A second database cannot be
// used instead: the testcontainer MySQL user only has privileges on the test
// database. The restore cleanup is registered before applying the DDL so a
// failed apply cannot leak shared-shape tables into later tests.
func installSharedTables(t *testing.T, tables []string, stmts []string) {
	t.Helper()
	drop := func(db *sql.DB) {
		t.Helper()
		for _, tbl := range tables {
			if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
				t.Fatalf("drop %s: %v", tbl, err)
			}
		}
	}
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	drop(db)
	t.Cleanup(func() {
		db, err := sql.Open("mysql", testDSN)
		if err != nil {
			t.Errorf("reopen test db for schema restore: %v", err)
			return
		}
		defer func() { _ = db.Close() }()
		drop(db)
		initDatastoreSchema(t, testDSN)
	})
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply shared schema: %v", err)
		}
	}
}

var coreFSSharedTables = []string{
	"file_nodes", "inodes", "contents", "semantic", "file_tags",
	"uploads", "semantic_tasks", "file_gc_tasks", "llm_usage", "fs_events",
}

// installSharedCoreFSSchema swaps the 10 Core FS tables to the shared shape.
func installSharedCoreFSSchema(t *testing.T) {
	t.Helper()
	installSharedTables(t, coreFSSharedTables, schema.CoreFSMySQLSharedSchemaStatements())
}

var fsLayerSharedTables = []string{
	"fs_layer_checkpoints", "fs_layer_events", "fs_layer_tags", "fs_layer_entries", "fs_layers",
}

// installSharedFSLayerSchema swaps the 5 FS Layer tables to the shared shape.
func installSharedFSLayerSchema(t *testing.T) {
	t.Helper()
	installSharedTables(t, fsLayerSharedTables, schema.FSLayerMySQLSharedSchemaStatements())
}

var gitSharedTables = []string{
	"git_workspace_object_packs", "git_workspace_overlay", "git_workspace_git_state",
	"git_workspace_tree_nodes", "git_workspaces",
}

// installSharedGitSchema swaps the 5 Git Workspace tables to the shared shape.
func installSharedGitSchema(t *testing.T) {
	t.Helper()
	installSharedTables(t, gitSharedTables, schema.GitWorkspaceMySQLSharedSchemaStatements())
}

// newSharedStore opens a Store bound to the shared schema with the given
// fs_id as its tenant row key.
func newSharedStore(t *testing.T, fsID int64) *Store {
	t.Helper()
	store, err := OpenForTenantScoped(context.Background(), testDSN, "", "", SharedScope(fsID))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
