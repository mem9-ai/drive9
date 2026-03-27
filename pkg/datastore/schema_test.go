package datastore

import (
	"testing"
)

func TestInitSchemaProviderSplit(t *testing.T) {
	s1, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	dropDataPlaneTables(t, s1)
	initDatastoreSchema(t, testDSN, "tidb_zero")
	if !s1.columnExists("files", "content_blob") {
		t.Fatal("expected content_blob column for tidb_zero")
	}

	dropDataPlaneTables(t, s1)
	initDatastoreSchema(t, testDSN, "db9")
	if s1.columnExists("files", "content_blob") {
		t.Fatal("did not expect content_blob column for db9")
	}
}

func dropDataPlaneTables(t *testing.T, s *Store) {
	t.Helper()
	stmts := []string{
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
