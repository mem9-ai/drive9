package schema

import (
	"errors"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
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

func TestTiDBSchemaSpecForModeIncludesCreateStatements(t *testing.T) {
	spec, err := tidbSchemaSpecForMode(TiDBEmbeddingModeAuto)
	if err != nil {
		t.Fatalf("schema spec: %v", err)
	}

	createByTable := make(map[string]string, len(spec.tables))
	for _, table := range spec.tables {
		createByTable[table.name] = table.createStatement
	}

	for _, tableName := range []string{"file_nodes", "file_tags", "files", "uploads", "semantic_tasks", "vault_deks", "vault_audit_log"} {
		stmt := createByTable[tableName]
		if stmt == "" {
			t.Fatalf("missing create statement for %s", tableName)
		}
		if !strings.Contains(strings.ToLower(stmt), "create table if not exists "+tableName) {
			t.Fatalf("unexpected create statement for %s: %q", tableName, stmt)
		}
	}
}

func TestTiDBSchemaSpecFromStatementsParsesNewTableAutomatically(t *testing.T) {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS example_events (
			event_id VARCHAR(64) PRIMARY KEY,
			tenant_id VARCHAR(64) NOT NULL,
			payload JSON,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_example_events_tenant ON example_events(tenant_id)`,
	}

	spec, err := tidbSchemaSpecFromStatements(stmts)
	if err != nil {
		t.Fatalf("tidbSchemaSpecFromStatements: %v", err)
	}
	table := mustTableSpecFromSchemaSpec(t, spec, "example_events")
	if _, ok := table.columns["event_id"]; !ok {
		t.Fatal("expected event_id column in parsed schema spec")
	}
	if got := table.columns["created_at"].columnType; got != "datetime(3)" {
		t.Fatalf("created_at column type=%q, want datetime(3)", got)
	}
	if _, ok := table.indexes["idx_example_events_tenant"]; !ok {
		t.Fatal("expected idx_example_events_tenant index in parsed schema spec")
	}
}

func TestTiDBSchemaSpecFromStatementsAttachesExternalIndexesWithoutStalePointers(t *testing.T) {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS t1 (
			id VARCHAR(64) PRIMARY KEY
		)`,
		`CREATE TABLE IF NOT EXISTS t2 (
			id VARCHAR(64) PRIMARY KEY
		)`,
		`CREATE INDEX idx_t1_id ON t1(id)`,
	}

	spec, err := tidbSchemaSpecFromStatements(stmts)
	if err != nil {
		t.Fatalf("tidbSchemaSpecFromStatements: %v", err)
	}
	t1 := mustTableSpecFromSchemaSpec(t, spec, "t1")
	if _, ok := t1.indexes["idx_t1_id"]; !ok {
		t.Fatalf("expected idx_t1_id on t1, got indexes=%#v", t1.indexes)
	}
}

func TestMissingTableAndIndexDiffsIncludesExternalIndexes(t *testing.T) {
	table := tidbTableSpec{
		name:            "uploads",
		createStatement: "CREATE TABLE IF NOT EXISTS uploads (...)",
		indexes: map[string]tidbIndexSpec{
			"idx_upload_path": {createSQL: "CREATE INDEX idx_upload_path ON uploads(target_path, status)"},
			"idx_idempotency": {createSQL: "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)"},
		},
	}

	diffs := missingTableAndIndexDiffs(table)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingTable, "missing table") {
		t.Fatalf("expected missing table diff, got %#v", diffs)
	}
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_upload_path") {
		t.Fatalf("expected missing idx_upload_path diff, got %#v", diffs)
	}
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_idempotency") {
		t.Fatalf("expected missing idx_idempotency diff, got %#v", diffs)
	}
}

func TestPlannedTiDBSchemaRepairsIncludesSafeStatementsOnly(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{kind: tidbSchemaDiffMissingTable, tableName: "semantic_tasks", repairSQL: "CREATE TABLE IF NOT EXISTS semantic_tasks (...)"},
		{kind: tidbSchemaDiffMissingColumn, tableName: "uploads", columnName: "expected_revision", repairSQL: "ALTER TABLE uploads ADD COLUMN expected_revision BIGINT NULL"},
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_upload_path index", repairSQL: "CREATE INDEX idx_upload_path ON uploads(target_path, status)"},
		{kind: tidbSchemaDiffMissingColumn, tableName: "uploads", columnName: "expected_revision", repairSQL: "ALTER TABLE uploads ADD COLUMN expected_revision BIGINT NULL"},
		{kind: tidbSchemaDiffColumnType, tableName: "files", columnName: "embedding_revision", detail: "files schema contract: embedding_revision column type = \"int\", want bigint"},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 3 {
		t.Fatalf("plannedTiDBSchemaRepairs() len=%d, want 3 (%#v)", len(got), got)
	}
	if got[0] != "CREATE TABLE IF NOT EXISTS semantic_tasks (...)" {
		t.Fatalf("unexpected first repair statement: %q", got[0])
	}
	if got[1] != "ALTER TABLE uploads ADD COLUMN expected_revision BIGINT NULL" {
		t.Fatalf("unexpected second repair statement: %q", got[1])
	}
	if got[2] != "CREATE INDEX idx_upload_path ON uploads(target_path, status)" {
		t.Fatalf("unexpected third repair statement: %q", got[2])
	}
}

func TestPlannedTiDBSchemaRepairsSkipsUnsafeUniqueIndexOnExistingTable(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_upload_path index", repairSQL: "CREATE INDEX idx_upload_path ON uploads(target_path, status)"},
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_idempotency index", repairSQL: "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)"},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 1 {
		t.Fatalf("expected one safe repair statement, got %#v", got)
	}
	if got[0] != "CREATE INDEX idx_upload_path ON uploads(target_path, status)" {
		t.Fatalf("unexpected repair statement: %q", got[0])
	}
}

func TestValidateTiDBAutoEmbeddingFilesDiffsReportsGeneratedContractMismatch(t *testing.T) {
	meta := testFilesTableMeta(TiDBEmbeddingModeApp)
	diffs := validateTiDBAutoEmbeddingFilesDiffs(meta)
	if len(diffs) == 0 {
		t.Fatal("expected auto embedding diffs for writable embedding column")
	}
	if !strings.Contains(strings.ToLower(diffs[0].detail), "stored generated") {
		t.Fatalf("unexpected diff detail: %#v", diffs)
	}
}

func TestDiffTiDBTableMetaReportsMissingRequiredIndex(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	meta := testUploadsTableMeta(true)
	createStmt := `CREATE TABLE uploads (
		upload_id VARCHAR(64) PRIMARY KEY,
		target_path VARCHAR(512) NOT NULL,
		status VARCHAR(32) NOT NULL,
		expected_revision BIGINT NULL
	)`

	diffs := diffTiDBTableMeta(spec, meta, createStmt)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_upload_path") {
		t.Fatalf("expected missing idx_upload_path diff, got %#v", diffs)
	}
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_idempotency") {
		t.Fatalf("expected missing idx_idempotency diff, got %#v", diffs)
	}
}

func TestDiffTiDBTableMetaReportsFileNodesAndFileTagsMissingIndexes(t *testing.T) {
	nodesSpec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "file_nodes")
	nodesMeta := tidbTableMeta{
		tableName: "file_nodes",
		columns: map[string]tidbColumnMeta{
			"node_id":     {columnType: "varchar(64)"},
			"path":        {columnType: "varchar(512)"},
			"parent_path": {columnType: "varchar(512)"},
			"name":        {columnType: "varchar(255)"},
			"file_id":     {columnType: "varchar(64)"},
			"created_at":  {columnType: "datetime(3)"},
		},
	}
	nodesDiffs := diffTiDBTableMeta(nodesSpec, nodesMeta, `CREATE TABLE file_nodes (node_id VARCHAR(64) PRIMARY KEY)`)
	if !hasDiffKindAndDetail(nodesDiffs, tidbSchemaDiffMissingIndex, "idx_path") {
		t.Fatalf("expected missing idx_path diff, got %#v", nodesDiffs)
	}

	tagsSpec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "file_tags")
	tagsMeta := tidbTableMeta{
		tableName: "file_tags",
		columns: map[string]tidbColumnMeta{
			"file_id":   {columnType: "varchar(64)"},
			"tag_key":   {columnType: "varchar(255)"},
			"tag_value": {columnType: "varchar(255)"},
		},
	}
	tagsDiffs := diffTiDBTableMeta(tagsSpec, tagsMeta, `CREATE TABLE file_tags (file_id VARCHAR(64), tag_key VARCHAR(255), tag_value VARCHAR(255))`)
	if !hasDiffKindAndDetail(tagsDiffs, tidbSchemaDiffMissingIndex, "idx_kv") {
		t.Fatalf("expected missing idx_kv diff, got %#v", tagsDiffs)
	}
}

func TestDiffTiDBTableMetaTreatsBooleanAndTinyIntAsEquivalent(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "file_nodes")
	meta := tidbTableMeta{
		tableName: "file_nodes",
		columns: map[string]tidbColumnMeta{
			"node_id":      {columnType: "varchar(64)"},
			"path":         {columnType: "varchar(512)"},
			"parent_path":  {columnType: "varchar(512)"},
			"name":         {columnType: "varchar(255)"},
			"is_directory": {columnType: "tinyint(1)"},
			"file_id":      {columnType: "varchar(64)"},
			"created_at":   {columnType: "datetime(3)"},
		},
	}

	diffs := diffTiDBTableMeta(spec, meta, `CREATE TABLE file_nodes (
		node_id VARCHAR(64) PRIMARY KEY,
		path VARCHAR(512) NOT NULL,
		parent_path VARCHAR(512) NOT NULL,
		name VARCHAR(255) NOT NULL,
		is_directory TINYINT(1) NOT NULL DEFAULT 0,
		file_id VARCHAR(64),
		created_at DATETIME(3) NOT NULL
	)`)

	for _, diff := range diffs {
		if diff.kind == tidbSchemaDiffColumnType && diff.columnName == "is_directory" {
			t.Fatalf("unexpected boolean/tinyint(1) column type mismatch: %#v", diff)
		}
	}
}

func TestDiffTiDBTableMetaReportsSemanticTasksMissingKeyColumn(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "semantic_tasks")
	meta := tidbTableMeta{
		tableName: "semantic_tasks",
		columns: map[string]tidbColumnMeta{
			"task_id":          {columnType: "varchar(64)"},
			"task_type":        {columnType: "varchar(32)"},
			"resource_id":      {columnType: "varchar(64)"},
			"resource_version": {columnType: "bigint"},
			"status":           {columnType: "varchar(20)"},
		},
	}
	createStmt := `CREATE TABLE semantic_tasks (
		task_id VARCHAR(64) PRIMARY KEY,
		task_type VARCHAR(32) NOT NULL,
		resource_id VARCHAR(64) NOT NULL,
		resource_version BIGINT NOT NULL,
		status VARCHAR(20) NOT NULL
	)`

	diffs := diffTiDBTableMeta(spec, meta, createStmt)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingColumn, "available_at") {
		t.Fatalf("expected missing available_at diff, got %#v", diffs)
	}
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "uk_task_resource_version") {
		t.Fatalf("expected missing uk_task_resource_version diff, got %#v", diffs)
	}
}

func TestTiDBSchemaSpecForModeIncludesVaultIndexes(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "vault_secrets")
	if _, ok := spec.indexes["uk_vault_secrets_tenant_name"]; !ok {
		t.Fatal("vault_secrets missing uk_vault_secrets_tenant_name index spec")
	}
	if _, ok := spec.indexes["idx_vault_secrets_tenant"]; !ok {
		t.Fatal("vault_secrets missing idx_vault_secrets_tenant index spec")
	}
}

func TestTiDBSchemaSpecForModeIncludesAlterTableIndexes(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "files")
	if _, ok := spec.indexes["idx_fts_content"]; !ok {
		t.Fatal("files missing idx_fts_content index spec from ALTER TABLE statement")
	}
	if _, ok := spec.indexes["idx_files_cosine"]; !ok {
		t.Fatal("files missing idx_files_cosine index spec from ALTER TABLE statement")
	}
}

func TestTiDBSchemaSpecForAppModeExcludesOptionalIndexes(t *testing.T) {
	// Optional FULLTEXT/VECTOR indexes use ADD_COLUMNAR_REPLICA_ON_DEMAND which
	// is not supported on all TiDB versions. They must not appear in the app
	// mode schema contract so that validation does not fail when they are skipped.
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeApp, "files")
	if _, ok := spec.indexes["idx_fts_content"]; ok {
		t.Fatal("files app mode spec must not include optional idx_fts_content index")
	}
	if _, ok := spec.indexes["idx_files_cosine"]; ok {
		t.Fatal("files app mode spec must not include optional idx_files_cosine index")
	}
}

func TestPlannedTiDBSchemaRepairsIncludesAlterTableIndexRepairs(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: "files",
			detail:    "files schema contract: missing idx_fts_content index",
			repairSQL: "ALTER TABLE files ADD FULLTEXT INDEX idx_fts_content(content_text)",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 1 {
		t.Fatalf("expected one repair statement, got %#v", got)
	}
	if got[0] != "ALTER TABLE files ADD FULLTEXT INDEX idx_fts_content(content_text)" {
		t.Fatalf("unexpected repair statement: %q", got[0])
	}
}

func TestIsSafeAddColumnRepairSQLRejectsStoredAndVirtualGeneratedColumns(t *testing.T) {
	tests := []string{
		"ALTER TABLE uploads ADD COLUMN active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED",
		"ALTER TABLE files ADD COLUMN embedding VECTOR(1024) AS (EMBED_TEXT('m', content_text, '{\"dimensions\":1024}')) VIRTUAL",
	}

	for _, stmt := range tests {
		if isSafeAddColumnRepairSQL(stmt) {
			t.Fatalf("expected generated column repair to be unsafe: %s", stmt)
		}
	}
}

func TestInitTiDBTenantSchemaStatementsForModeIncludesVault(t *testing.T) {
	for _, mode := range []TiDBEmbeddingMode{TiDBEmbeddingModeAuto, TiDBEmbeddingModeApp} {
		t.Run(string(mode), func(t *testing.T) {
			stmts, err := InitTiDBTenantSchemaStatementsForMode(mode)
			if err != nil {
				t.Fatalf("init statements for mode %q: %v", mode, err)
			}

			sqlText := strings.Join(stmts, "\n")
			if !strings.Contains(sqlText, "CREATE TABLE IF NOT EXISTS vault_deks") {
				t.Fatalf("mode %q missing vault_deks in init schema", mode)
			}
			if !strings.Contains(sqlText, "CREATE TABLE IF NOT EXISTS vault_audit_log") {
				t.Fatalf("mode %q missing vault_audit_log in init schema", mode)
			}
		})
	}
}

func TestIsIgnorableOptionalSchemaError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "columnar replica syntax",
			err:  &mysql.MySQLError{Number: 1064, Message: "syntax error near \"ADD_COLUMNAR_REPLICA_ON_DEMAND\""},
			want: true,
		},
		{
			name: "fulltext unsupported",
			err:  &mysql.MySQLError{Number: 1105, Message: "FULLTEXT index is not supported"},
			want: true,
		},
		{
			name: "vector index unsupported",
			err:  &mysql.MySQLError{Number: 8200, Message: "VECTOR INDEX is not supported"},
			want: true,
		},
		{
			name: "vec cosine unsupported",
			err:  errors.New("vec_cosine_distance is not supported"),
			want: true,
		},
		{
			name: "parser multilingual unsupported",
			err:  errors.New("WITH PARSER multilingual is not supported"),
			want: true,
		},
		{
			name: "mysql syntax without optional markers",
			err:  &mysql.MySQLError{Number: 1064, Message: "syntax error near \"ALTER TABLE files ADD INDEX idx_status\""},
			want: false,
		},
		{
			name: "unrelated error",
			err:  errors.New("permission denied"),
			want: false,
		},
		{
			name: "mysql unrelated code with keyword should not skip",
			err:  &mysql.MySQLError{Number: 1146, Message: "FULLTEXT index is not supported"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isIgnorableOptionalSchemaError(tt.err); got != tt.want {
				t.Fatalf("isIgnorableOptionalSchemaError()=%v, want %v", got, tt.want)
			}
		})
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

func mustTiDBTableSpecByName(t *testing.T, mode TiDBEmbeddingMode, tableName string) tidbTableSpec {
	t.Helper()
	spec, err := tidbSchemaSpecForMode(mode)
	if err != nil {
		t.Fatalf("schema spec for %q: %v", mode, err)
	}
	for _, table := range spec.tables {
		if table.name == tableName {
			return table
		}
	}
	t.Fatalf("missing table spec %q", tableName)
	return tidbTableSpec{}
}

func hasDiffKindAndDetail(diffs []tidbSchemaDiff, kind tidbSchemaDiffKind, detailSubstring string) bool {
	for _, diff := range diffs {
		if diff.kind == kind && strings.Contains(strings.ToLower(diff.detail), strings.ToLower(detailSubstring)) {
			return true
		}
	}
	return false
}

func mustTableSpecFromSchemaSpec(t *testing.T, spec tidbSchemaSpec, tableName string) tidbTableSpec {
	t.Helper()
	for _, table := range spec.tables {
		if table.name == tableName {
			return table
		}
	}
	t.Fatalf("missing table spec %q", tableName)
	return tidbTableSpec{}
}
