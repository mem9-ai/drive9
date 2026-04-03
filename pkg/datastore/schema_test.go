package datastore

import (
	"testing"
)

func TestInitSchemaProviderSplit(t *testing.T) {
	// TODO(async-embedding): This test only validates schema shape through the
	// MySQL fixture used by the current store/runtime path. It does not execute
	// initDB9Schema on a real Postgres-backed store or validate Postgres-specific
	// SQL/runtime behavior end-to-end.
	s1, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	providers := []string{"tidb_zero", "tidb_cloud_starter", "db9"}

	for _, provider := range providers {
		dropDataPlaneTables(t, s1)
		initDatastoreSchema(t, testDSN, provider)
		if !s1.columnExists("files", "content_blob") {
			t.Fatalf("provider %s missing files.content_blob", provider)
		}
		for _, col := range []string{"embedding", "embedding_revision"} {
			if !s1.columnExists("files", col) {
				t.Fatalf("provider %s missing files.%s", provider, col)
			}
		}
		if !s1.columnExists("semantic_tasks", "task_id") {
			t.Fatalf("provider %s missing semantic_tasks table", provider)
		}
	}
}

func dropDataPlaneTables(t *testing.T, s *Store) {
	t.Helper()
	stmts := []string{
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
