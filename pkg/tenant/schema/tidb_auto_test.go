package schema

import (
	"strings"
	"testing"
)

func TestDetectTiDBEmbeddingModeFromFilesMeta(t *testing.T) {
	autoMeta := testFilesTableMeta(TiDBEmbeddingModeAuto)
	mode, err := detectTiDBEmbeddingModeFromFilesMeta(autoMeta)
	if err != nil {
		t.Fatalf("detect auto mode: %v", err)
	}
	if mode != TiDBEmbeddingModeAuto {
		t.Fatalf("mode=%q, want %q", mode, TiDBEmbeddingModeAuto)
	}

	appMeta := testFilesTableMeta(TiDBEmbeddingModeApp)
	mode, err = detectTiDBEmbeddingModeFromFilesMeta(appMeta)
	if err != nil {
		t.Fatalf("detect app mode: %v", err)
	}
	if mode != TiDBEmbeddingModeApp {
		t.Fatalf("mode=%q, want %q", mode, TiDBEmbeddingModeApp)
	}
}

func TestValidateTiDBAutoEmbeddingFilesTableAcceptsRealTiDBMetadata(t *testing.T) {
	if err := validateTiDBAutoEmbeddingFilesTable(testFilesTableMeta(TiDBEmbeddingModeAuto)); err != nil {
		t.Fatalf("expected auto files table to validate: %v", err)
	}
}

func TestValidateTiDBAutoEmbeddingFilesTableRejectsWritableEmbedding(t *testing.T) {
	err := validateTiDBAutoEmbeddingFilesTable(testFilesTableMeta(TiDBEmbeddingModeApp))
	if err == nil {
		t.Fatal("expected writable embedding column to be rejected")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "generated") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateTiDBAppEmbeddingFilesTableRejectsGeneratedEmbedding(t *testing.T) {
	err := validateTiDBAppEmbeddingFilesTable(testFilesTableMeta(TiDBEmbeddingModeAuto))
	if err == nil {
		t.Fatal("expected generated embedding column to be rejected in app mode")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "writable") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateTiDBUploadsTableBaseAcceptsExpectedRevision(t *testing.T) {
	if err := validateTiDBUploadsTableBase(testUploadsTableMeta(true)); err != nil {
		t.Fatalf("expected uploads table to validate: %v", err)
	}
}

func TestValidateTiDBUploadsTableBaseRejectsMissingExpectedRevision(t *testing.T) {
	err := validateTiDBUploadsTableBase(testUploadsTableMeta(false))
	if err == nil {
		t.Fatal("expected missing expected_revision to fail validation")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "expected_revision") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestLegacyTiDBUploadsRepairStatements(t *testing.T) {
	if got := legacyTiDBUploadsRepairStatements(testUploadsTableMeta(true)); len(got) != 0 {
		t.Fatalf("expected no repair statements when expected_revision exists, got %#v", got)
	}
	got := legacyTiDBUploadsRepairStatements(testUploadsTableMeta(false))
	if len(got) != 1 {
		t.Fatalf("expected one repair statement, got %#v", got)
	}
	if !strings.Contains(strings.ToLower(got[0]), "add column expected_revision") {
		t.Fatalf("unexpected repair statement: %q", got[0])
	}
}

func testFilesTableMeta(mode TiDBEmbeddingMode) tidbTableMeta {
	meta := tidbTableMeta{
		tableName: "files",
		columns: map[string]tidbColumnMeta{
			"file_id":            {columnType: "varchar(64)"},
			"status":             {columnType: "varchar(32)"},
			"content_text":       {columnType: "longtext"},
			"embedding":          {columnType: "vector(1024)"},
			"embedding_revision": {columnType: "bigint"},
		},
	}
	if mode == TiDBEmbeddingModeAuto {
		meta.columns["embedding"] = tidbColumnMeta{
			columnType:           "vector(1024)",
			extra:                "STORED GENERATED",
			generationExpression: "embed_text(_utf8mb4'tidbcloud_free/amazon/titan-embed-text-v2', `content_text`, _utf8mb4'{\"dimensions\":1024}')",
		}
		return meta
	}
	return meta
}

func testUploadsTableMeta(includeExpectedRevision bool) tidbTableMeta {
	meta := tidbTableMeta{
		tableName: "uploads",
		columns: map[string]tidbColumnMeta{
			"upload_id":   {columnType: "varchar(64)"},
			"target_path": {columnType: "varchar(512)"},
			"status":      {columnType: "varchar(32)"},
		},
	}
	if includeExpectedRevision {
		meta.columns["expected_revision"] = tidbColumnMeta{columnType: "bigint"}
	}
	return meta
}
