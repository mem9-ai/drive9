package migrate

import (
	"database/sql"
	"testing"

	"github.com/mem9-ai/drive9/internal/testmysql"
)

// TestSplitTablesMigratorSkipGeneratedColumns verifies that when the semantic
// table has GENERATED columns (TiDB auto-embedding mode), the migration skips
// copying those columns and lets the database recompute them from
// content_text/description. This avoids triggering expensive LLM inference
// during INSERT ... SELECT.
func TestSplitTablesMigratorSkipGeneratedColumns(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	ctx := t.Context()
	testmysql.ResetDB(t, db)

	// Insert a file with content_text.
	if _, err := db.Exec(`
		INSERT INTO files (file_id, storage_type, storage_ref, status, content_text, description, embedding)
		VALUES ('file1', 'db9', 'ref1', 'CONFIRMED', 'hello', 'world', 'should_be_ignored')
	`); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	// Recreate semantic table with generated columns (simulating TiDB auto-embedding).
	if _, err := db.Exec(`DROP TABLE IF EXISTS semantic`); err != nil {
		t.Fatalf("drop semantic: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE semantic (
			inode_id                           VARCHAR(64) PRIMARY KEY,
			content_text                       LONGTEXT,
			description                        LONGTEXT,
			embedding                          VARCHAR(255) GENERATED ALWAYS AS (UPPER(content_text)) STORED,
			embedding_revision                 BIGINT,
			description_embedding              VARCHAR(255) GENERATED ALWAYS AS (UPPER(description)) STORED,
			description_embedding_revision     BIGINT
		)
	`); err != nil {
		t.Fatalf("create semantic with generated columns: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS semantic`)
		_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS semantic (
			inode_id                           VARCHAR(64) PRIMARY KEY,
			content_text                       LONGTEXT,
			description                        LONGTEXT,
			embedding                          LONGTEXT,
			embedding_revision                 BIGINT,
			description_embedding              LONGTEXT,
			description_embedding_revision     BIGINT
		)`)
		_ = db.Close()
	})

	m := NewSplitTablesMigrator(db)
	res, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}
	if res.SemanticMigrated != 1 {
		t.Errorf("semantic migrated = %d, want 1", res.SemanticMigrated)
	}

	// Verify that embedding was auto-generated from content_text,
	// not copied from files.embedding (which was 'should_be_ignored').
	var embedding, descEmbedding string
	if err := db.QueryRow(`
		SELECT embedding, description_embedding FROM semantic WHERE inode_id = 'file1'
	`).Scan(&embedding, &descEmbedding); err != nil {
		t.Fatalf("select semantic: %v", err)
	}
	if embedding != "HELLO" {
		t.Errorf("embedding = %q, want HELLO (should be generated, not copied)", embedding)
	}
	if descEmbedding != "WORLD" {
		t.Errorf("description_embedding = %q, want WORLD (should be generated, not copied)", descEmbedding)
	}
}

// TestSemanticHasGeneratedColumns verifies the detection logic directly.
func TestSemanticHasGeneratedColumns(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	ctx := t.Context()
	testmysql.ResetDB(t, db)

	// Ensure plain semantic table (previous tests may have left a generated-column version).
	if _, err := db.Exec(`DROP TABLE IF EXISTS semantic`); err != nil {
		t.Fatalf("drop semantic: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE semantic (
			inode_id                           VARCHAR(64) PRIMARY KEY,
			content_text                       LONGTEXT,
			description                        LONGTEXT,
			embedding                          LONGTEXT,
			embedding_revision                 BIGINT,
			description_embedding              LONGTEXT,
			description_embedding_revision     BIGINT
		)
	`); err != nil {
		t.Fatalf("create plain semantic: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS semantic`)
		_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS semantic (
			inode_id                           VARCHAR(64) PRIMARY KEY,
			content_text                       LONGTEXT,
			description                        LONGTEXT,
			embedding                          LONGTEXT,
			embedding_revision                 BIGINT,
			description_embedding              LONGTEXT,
			description_embedding_revision     BIGINT
		)`)
		_ = db.Close()
	})

	// Plain semantic table (no generated columns).
	m := NewSplitTablesMigrator(db)
	gen, err := m.semanticHasGeneratedColumns(ctx)
	if err != nil {
		t.Fatalf("detect generated columns on plain table: %v", err)
	}
	if gen {
		t.Error("expected false for plain semantic table")
	}

	// Recreate with generated columns.
	if _, err := db.Exec(`DROP TABLE IF EXISTS semantic`); err != nil {
		t.Fatalf("drop semantic: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE semantic (
			inode_id     VARCHAR(64) PRIMARY KEY,
			content_text LONGTEXT,
			embedding    VARCHAR(255) GENERATED ALWAYS AS (UPPER(content_text)) STORED
		)
	`); err != nil {
		t.Fatalf("create semantic with generated column: %v", err)
	}
	gen, err = m.semanticHasGeneratedColumns(ctx)
	if err != nil {
		t.Fatalf("detect generated columns on generated table: %v", err)
	}
	if !gen {
		t.Error("expected true for semantic table with generated column")
	}
}
