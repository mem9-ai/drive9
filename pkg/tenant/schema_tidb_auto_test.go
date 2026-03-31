package tenant

import (
	"strings"
	"testing"
)

func TestValidateTiDBAutoEmbeddingFilesDDLAcceptsGeneratedEmbedding(t *testing.T) {
	ddl := `
CREATE TABLE files (
	file_id VARCHAR(64) PRIMARY KEY,
	content_text LONGTEXT,
	embedding VECTOR(1024) GENERATED ALWAYS AS (EMBED_TEXT(
		'openai/text-embedding-3-small',
		content_text,
		'{"dimensions":1024}'
	)) STORED,
	embedding_revision BIGINT,
	FULLTEXT INDEX idx_fts_content(content_text) WITH PARSER MULTILINGUAL,
	VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
)`
	if err := validateTiDBAutoEmbeddingFilesDDL(ddl); err != nil {
		t.Fatalf("expected generated auto-embedding ddl to validate: %v", err)
	}
}

func TestValidateTiDBAutoEmbeddingFilesDDLRejectsWritableEmbedding(t *testing.T) {
	ddl := `
CREATE TABLE files (
	file_id VARCHAR(64) PRIMARY KEY,
	content_text LONGTEXT,
	embedding VECTOR(1024),
	embedding_revision BIGINT,
	VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
)`
	err := validateTiDBAutoEmbeddingFilesDDL(ddl)
	if err == nil {
		t.Fatal("expected writable embedding ddl to be rejected")
	}
	if !strings.Contains(err.Error(), "generated embedding column") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateTiDBAutoEmbeddingFilesDDLRejectsMissingFTSIndex(t *testing.T) {
	ddl := `
CREATE TABLE files (
	file_id VARCHAR(64) PRIMARY KEY,
	content_text LONGTEXT,
	embedding VECTOR(1024) GENERATED ALWAYS AS (EMBED_TEXT(
		'openai/text-embedding-3-small',
		content_text,
		'{"dimensions":1024}'
	)) STORED,
	embedding_revision BIGINT,
	VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
)`
	err := validateTiDBAutoEmbeddingFilesDDL(ddl)
	if err == nil {
		t.Fatal("expected missing fulltext index to be rejected")
	}
	if !strings.Contains(err.Error(), "idx_fts_content") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateTiDBAutoEmbeddingTableDDLRejectsMissingUploadsIndex(t *testing.T) {
	ddl := `
CREATE TABLE uploads (
	upload_id VARCHAR(64) PRIMARY KEY,
	target_path VARCHAR(512) NOT NULL,
	status VARCHAR(32) NOT NULL,
	active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED
)`
	err := validateTiDBAutoEmbeddingTableDDL("uploads", ddl)
	if err == nil {
		t.Fatal("expected uploads contract validation to fail")
	}
	if !strings.Contains(err.Error(), "idx_upload_path") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
