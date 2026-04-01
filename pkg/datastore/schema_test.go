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
	tests := []struct {
		provider        string
		wantContentBlob bool
	}{
		{provider: "tidb_zero", wantContentBlob: true},
		{provider: "tidb_cloud_starter", wantContentBlob: true},
		{provider: "db9", wantContentBlob: false},
	}

	for _, tc := range tests {
		dropDataPlaneTables(t, s1)
		initDatastoreSchema(t, testDSN, tc.provider)
		if got := s1.columnExists("files", "content_blob"); got != tc.wantContentBlob {
			t.Fatalf("provider %s content_blob=%v, want %v", tc.provider, got, tc.wantContentBlob)
		}
		for _, col := range []string{"embedding", "embedding_revision"} {
			if !s1.columnExists("files", col) {
				t.Fatalf("provider %s missing files.%s", tc.provider, col)
			}
		}
		if !s1.columnExists("semantic_tasks", "task_id") {
			t.Fatalf("provider %s missing semantic_tasks table", tc.provider)
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
