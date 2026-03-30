package datastore

import (
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
)

func TestInitSchemaProviderSplit(t *testing.T) {
	tidbZeroDSN, err := testmysql.PrepareIsolatedDatabase(testDSN, "datastore_provider_split_tidb_zero")
	if err != nil {
		t.Fatal(err)
	}
	s1, err := Open(tidbZeroDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	dropDataPlaneTables(t, s1)
	initDatastoreSchema(t, tidbZeroDSN, "tidb_zero")
	if !s1.columnExists("files", "content_blob") {
		t.Fatal("expected content_blob column for tidb_zero")
	}

	db9DSN, err := testmysql.PrepareIsolatedDatabase(testDSN, "datastore_provider_split_db9")
	if err != nil {
		t.Fatal(err)
	}
	s2, err := Open(db9DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	dropDataPlaneTables(t, s2)
	initDatastoreSchema(t, db9DSN, "db9")
	if s2.columnExists("files", "content_blob") {
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
