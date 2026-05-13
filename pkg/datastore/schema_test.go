package datastore

import (
	"testing"
)

func TestInitSchemaRequiredColumns(t *testing.T) {
	// TODO(async-embedding): This test only validates schema shape through the
	// MySQL fixture used by the current store/runtime path. It does not execute
	// initDB9Schema on a real Postgres-backed store or validate Postgres-specific
	// SQL/runtime behavior end-to-end.
	s1, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })

	dropDataPlaneTables(t, s1)
	initDatastoreSchema(t, testDSN)
	if !s1.columnExists("files", "content_blob") {
		t.Fatal("missing files.content_blob")
	}
	for _, col := range []string{"embedding", "embedding_revision"} {
		if !s1.columnExists("files", col) {
			t.Fatalf("missing files.%s", col)
		}
	}
	if !s1.columnExists("semantic_tasks", "task_id") {
		t.Fatal("missing semantic_tasks table")
	}
	if !s1.columnExists("file_gc_tasks", "task_id") {
		t.Fatal("missing file_gc_tasks table")
	}
	if !s1.columnExists("journals", "journal_id") {
		t.Fatal("missing journals table")
	}
	if !s1.columnExists("journal_entries", "entry_id") {
		t.Fatal("missing journal_entries table")
	}
}

func dropDataPlaneTables(t *testing.T, s *Store) {
	t.Helper()
	stmts := []string{
		"DROP TABLE IF EXISTS file_gc_tasks",
		"DROP TABLE IF EXISTS journal_entry_subjects",
		"DROP TABLE IF EXISTS journal_entries",
		"DROP TABLE IF EXISTS journal_append_requests",
		"DROP TABLE IF EXISTS journal_labels",
		"DROP TABLE IF EXISTS journals",
		"DROP TABLE IF EXISTS semantic_tasks",
		"DROP TABLE IF EXISTS uploads",
		"DROP TABLE IF EXISTS file_tags",
		"DROP TABLE IF EXISTS file_nodes",
		"DROP TABLE IF EXISTS files",
	}
	for _, stmt := range stmts {
		if _, err := s.DB().Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
}
