package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
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
	if !equalStringSlices(table.primaryKey.columns, []string{"event_id"}) {
		t.Fatalf("primary key columns=%#v, want [event_id]", table.primaryKey.columns)
	}
}

func TestCurrentTiDBTenantSchemaVersionIgnoresFormattingOnlyChanges(t *testing.T) {
	base := []string{
		"CREATE TABLE IF NOT EXISTS example_events (event_id VARCHAR(64) PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL)",
		"CREATE INDEX idx_example_events_tenant ON example_events(tenant_id)",
	}
	formatted := []string{
		"\nCREATE TABLE IF NOT EXISTS example_events (\n    event_id VARCHAR(64) PRIMARY KEY,\n    tenant_id VARCHAR(64) NOT NULL\n)\n",
		"CREATE   INDEX idx_example_events_tenant   ON   example_events(tenant_id)",
	}

	if got, want := currentTiDBTenantSchemaVersion(formatted), currentTiDBTenantSchemaVersion(base); got != want {
		t.Fatalf("formatted schema version=%d, want %d", got, want)
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

func TestPlannedTiDBSchemaRepairsAllowsUniqueIndexOnExistingTable(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_upload_path index", repairSQL: "CREATE INDEX idx_upload_path ON uploads(target_path, status)"},
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_idempotency index", repairSQL: "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)"},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 2 {
		t.Fatalf("expected both missing indexes to be auto-repaired, got %#v", got)
	}
	if got[0] != "CREATE INDEX idx_upload_path ON uploads(target_path, status)" {
		t.Fatalf("unexpected first repair statement: %q", got[0])
	}
	if got[1] != "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)" {
		t.Fatalf("unexpected second repair statement: %q", got[1])
	}
}

func TestParseUniqueIndexRepairStatement(t *testing.T) {
	repair, ok := parseUniqueIndexRepairStatement("ALTER TABLE semantic_tasks ADD UNIQUE KEY uk_task_resource_version(task_type, resource_id, resource_version)")
	if !ok {
		t.Fatal("expected unique index repair statement to parse")
	}
	if repair.tableName != "semantic_tasks" || repair.indexName != "uk_task_resource_version" {
		t.Fatalf("unexpected repair target: %#v", repair)
	}
	if !equalStringSlices(repair.columns, []string{"task_type", "resource_id", "resource_version"}) {
		t.Fatalf("unexpected repair columns: %#v", repair.columns)
	}
}

func TestBuildUniqueIndexDuplicateCheckSQL(t *testing.T) {
	repair := tidbUniqueIndexRepair{
		tableName: "uploads",
		indexName: "idx_uploads_active",
		columns:   []string{"active_target_path"},
	}
	got := buildUniqueIndexDuplicateCheckSQL(repair)
	want := "SELECT 1 FROM `uploads` WHERE `active_target_path` IS NOT NULL GROUP BY `active_target_path` HAVING COUNT(*) > 1 LIMIT 1"
	if got != want {
		t.Fatalf("duplicate check SQL=%q, want %q", got, want)
	}
}

func TestApplyTiDBSchemaRepairsPreflightsUniqueIndexDuplicates(t *testing.T) {
	db := newTestRepairDB(t, func(query string) testRepairQueryResult {
		if strings.Contains(query, "GROUP BY `idempotency_key`") {
			return testRepairQueryResult{columns: []string{"1"}, rows: [][]driver.Value{{int64(1)}}}
		}
		return testRepairQueryResult{}
	}, nil)

	err := applyTiDBSchemaRepairs(context.Background(), db, []string{"CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)"})
	if err == nil {
		t.Fatal("expected duplicate preflight to reject repair")
	}
	if !strings.Contains(err.Error(), "duplicate rows exist") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyTiDBSchemaRepairsPreflightsUniqueIndexNoDuplicatesExecutesRepair(t *testing.T) {
	var executed atomic.Int32

	db := newTestRepairDB(t, func(query string) testRepairQueryResult {
		if strings.Contains(query, "GROUP BY `idempotency_key`") {
			return testRepairQueryResult{}
		}
		return testRepairQueryResult{}
	}, func(query string) error {
		if strings.Contains(query, "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)") {
			executed.Add(1)
		}
		return nil
	})

	err := applyTiDBSchemaRepairs(context.Background(), db, []string{"CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)"})
	if err != nil {
		t.Fatalf("expected repair to succeed when preflight finds no duplicates: %v", err)
	}
	if executed.Load() != 1 {
		t.Fatalf("expected repair statement to execute once, executed=%d", executed.Load())
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

func TestDiffTiDBTableMetaRecognizesUniqueIndexFromCreateStatement(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	meta := testUploadsTableMeta(true)
	createStmt := `CREATE TABLE uploads (
		upload_id VARCHAR(64) PRIMARY KEY,
		target_path VARCHAR(512) NOT NULL,
		status VARCHAR(32) NOT NULL,
		expected_revision BIGINT NULL,
		active_target_path VARCHAR(512),
		UNIQUE KEY idx_uploads_active (active_target_path)
	)`

	diffs := diffTiDBTableMeta(spec, meta, createStmt)
	if hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_uploads_active") {
		t.Fatalf("did not expect idx_uploads_active to be reported missing, got %#v", diffs)
	}
}

func TestDiffTiDBTableUsesInformationSchemaIndexesForUploads(t *testing.T) {
	if testDSN == "" {
		t.Skip("mysql test DSN not configured")
	}

	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("DROP TABLE IF EXISTS uploads"); err != nil {
		t.Fatalf("drop uploads: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS uploads")
	})

	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	if _, err := db.Exec(spec.createStatement); err != nil {
		t.Fatalf("create uploads table: %v", err)
	}
	for _, indexName := range sortedIndexNames(spec.indexes) {
		if _, err := db.Exec(spec.indexes[indexName].createSQL); err != nil {
			t.Fatalf("create %s: %v", indexName, err)
		}
	}

	diffs, err := diffTiDBTable(context.Background(), db, spec)
	if err != nil {
		t.Fatalf("diffTiDBTable: %v", err)
	}
	if hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_uploads_active") {
		t.Fatalf("did not expect idx_uploads_active to be reported missing via information_schema path, got %#v", diffs)
	}
	if len(diffs) != 0 {
		t.Fatalf("expected uploads table created from schema spec to have no diffs, got %#v", diffs)
	}
	observed, ok := loadObservedTiDBIndexes(context.Background(), db, "uploads", spec.createStatement)
	if !ok {
		t.Fatal("expected loadObservedTiDBIndexes to observe indexes from information_schema")
	}
	if !hasObservedTiDBIndex(observed, "idx_uploads_active") {
		t.Fatalf("expected idx_uploads_active in observed indexes, got %#v", observed)
	}
}

func TestDiffTiDBTableMetaReportsIndexInspectionFailureInsteadOfFalsePositives(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	meta := testUploadsTableMeta(true)
	diffs := diffTiDBTableMetaWithObservedIndexes(spec, meta, `CREATE TABLE uploads (upload_id VARCHAR(64) PRIMARY KEY)`, nil, false)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffTableContract, "unable to inspect indexes") {
		t.Fatalf("expected unable to inspect indexes diff, got %#v", diffs)
	}
	if hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_uploads_active") {
		t.Fatalf("did not expect false-positive missing index diff, got %#v", diffs)
	}
}

func TestParseObservedTiDBIndexesRecognizesConstraintUnique(t *testing.T) {
	createStmt := `CREATE TABLE uploads (
		upload_id VARCHAR(64) PRIMARY KEY,
		active_target_path VARCHAR(512),
		CONSTRAINT idx_uploads_active UNIQUE (active_target_path)
	)`
	observed, ok := parseObservedTiDBIndexes(createStmt)
	if !ok {
		t.Fatal("expected observed indexes parse to succeed")
	}
	if !hasObservedTiDBIndex(observed, "idx_uploads_active") {
		t.Fatalf("expected idx_uploads_active to be observed, got %#v", observed)
	}
}

func TestTiDBSchemaSpecFromStatementsParsesConstraintUniqueDefinition(t *testing.T) {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS uploads (
			upload_id VARCHAR(64) PRIMARY KEY,
			active_target_path VARCHAR(512),
			CONSTRAINT idx_uploads_active UNIQUE (active_target_path)
		)`,
	}
	spec, err := tidbSchemaSpecFromStatements(stmts)
	if err != nil {
		t.Fatalf("tidbSchemaSpecFromStatements: %v", err)
	}
	table := mustTableSpecFromSchemaSpec(t, spec, "uploads")
	if _, ok := table.indexes["idx_uploads_active"]; !ok {
		t.Fatalf("expected idx_uploads_active index in parsed schema spec, got %#v", table.indexes)
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

func TestTiDBSchemaSpecForModeCapturesCompositePrimaryKey(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "file_tags")
	if !equalStringSlices(spec.primaryKey.columns, []string{"file_id", "tag_key"}) {
		t.Fatalf("file_tags primary key=%#v, want [file_id tag_key]", spec.primaryKey.columns)
	}
}

func TestDiffTiDBTableMetaReportsMissingPrimaryKeyConstraint(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "file_tags")
	meta := tidbTableMeta{
		tableName: "file_tags",
		columns: map[string]tidbColumnMeta{
			"file_id":   {columnType: "varchar(64)"},
			"tag_key":   {columnType: "varchar(255)"},
			"tag_value": {columnType: "varchar(255)"},
		},
	}
	createStmt := `CREATE TABLE file_tags (
		file_id VARCHAR(64) NOT NULL,
		tag_key VARCHAR(255) NOT NULL,
		tag_value VARCHAR(255),
		KEY idx_kv (tag_key, tag_value)
	)`

	diffs := diffTiDBTableMeta(spec, meta, createStmt)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffTableContract, "missing primary key") {
		t.Fatalf("expected missing primary key diff, got %#v", diffs)
	}
}

func TestDiffTiDBTableMetaReportsPrimaryKeyColumnMismatch(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "file_tags")
	meta := tidbTableMeta{
		tableName: "file_tags",
		columns: map[string]tidbColumnMeta{
			"file_id":   {columnType: "varchar(64)"},
			"tag_key":   {columnType: "varchar(255)"},
			"tag_value": {columnType: "varchar(255)"},
		},
	}
	createStmt := `CREATE TABLE file_tags (
		file_id VARCHAR(64) NOT NULL,
		tag_key VARCHAR(255) NOT NULL,
		tag_value VARCHAR(255),
		PRIMARY KEY (tag_key, file_id),
		KEY idx_kv (tag_key, tag_value)
	)`

	diffs := diffTiDBTableMeta(spec, meta, createStmt)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffTableContract, "primary key columns") {
		t.Fatalf("expected primary key mismatch diff, got %#v", diffs)
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
	// In auto-embedding mode, FULLTEXT and VECTOR indexes are part of the
	// enforceable schema contract and must appear in the spec. TiDB Cloud
	// (the only platform where auto mode runs) supports ADD_COLUMNAR_REPLICA_ON_DEMAND.
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "files")
	if _, ok := spec.indexes["idx_fts_content_desc"]; !ok {
		t.Fatal("files auto mode spec must include idx_fts_content_desc index")
	}
	if _, ok := spec.indexes["idx_files_cosine"]; !ok {
		t.Fatal("files auto mode spec must include idx_files_cosine index")
	}
	if _, ok := spec.indexes["idx_files_desc_cosine"]; !ok {
		t.Fatal("files auto mode spec must include idx_files_desc_cosine index")
	}
}

func TestTiDBSchemaSpecForAppModeExcludesOptionalIndexes(t *testing.T) {
	// Optional FULLTEXT/VECTOR indexes use ADD_COLUMNAR_REPLICA_ON_DEMAND which
	// is not supported on all TiDB versions. They must not appear in the app
	// mode schema contract so that validation does not fail when they are skipped.
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeApp, "files")
	if _, ok := spec.indexes["idx_fts_content_desc"]; ok {
		t.Fatal("files app mode spec must not include optional idx_fts_content_desc index")
	}
	if _, ok := spec.indexes["idx_files_cosine"]; ok {
		t.Fatal("files app mode spec must not include optional idx_files_cosine index")
	}
}

func TestPlannedTiDBSchemaRepairsIncludesFulltextVectorIndexOnExistingTable(t *testing.T) {
	// FULLTEXT and VECTOR indexes must be repaired even when the table already
	// exists: TiDB Cloud (the platform for auto mode) supports the syntax, and
	// applyTiDBSchemaRepairs gracefully skips with a warning on unsupported
	// versions.
	diffs := []tidbSchemaDiff{
		{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: "files",
			detail:    "files schema contract: missing idx_fts_content_desc index",
			repairSQL: "ALTER TABLE files ADD FULLTEXT INDEX idx_fts_content_desc(content_text, description)",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 1 {
		t.Fatalf("expected fulltext index repair to be included for existing table, got %#v", got)
	}
	if got[0] != "ALTER TABLE files ADD FULLTEXT INDEX idx_fts_content_desc(content_text, description)" {
		t.Fatalf("unexpected repair statement: %q", got[0])
	}
}

func TestPlannedTiDBSchemaRepairsAllowsHeavyAlterTableIndexRepairsWhenTableMissing(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{
			kind:      tidbSchemaDiffMissingTable,
			tableName: "files",
			repairSQL: "CREATE TABLE IF NOT EXISTS files (...)",
		},
		{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: "files",
			detail:    "files schema contract: missing idx_fts_content_desc index",
			repairSQL: "ALTER TABLE files ADD FULLTEXT INDEX idx_fts_content_desc(content_text, description)",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 2 {
		t.Fatalf("expected create table and heavy index repair, got %#v", got)
	}
	if got[1] != "ALTER TABLE files ADD FULLTEXT INDEX idx_fts_content_desc(content_text, description)" {
		t.Fatalf("unexpected second repair statement: %q", got[1])
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

func TestIsSafeAddColumnRepairSQLAllowsStoredGeneratedVectorWithEmbedText(t *testing.T) {
	// description_embedding is a STORED GENERATED VECTOR column backed by EMBED_TEXT.
	// TiDB computes the value server-side so ALTER TABLE ADD COLUMN is safe on
	// existing tables even when embedding rows are absent.
	stmt := "ALTER TABLE files ADD COLUMN description_embedding VECTOR(1024) GENERATED ALWAYS AS (EMBED_TEXT('amazon.titan-embed-text-v2:0', description, '{\"dimensions\":1024}')) STORED"
	if !isSafeAddColumnRepairSQL(stmt) {
		t.Fatal("expected STORED GENERATED VECTOR with EMBED_TEXT to be safe to add")
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

func TestIsIgnorableTiDBSchemaError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "duplicate table",
			err:  &mysql.MySQLError{Number: 1050, Message: "Table 'files' already exists"},
			want: true,
		},
		{
			name: "duplicate column",
			err:  &mysql.MySQLError{Number: 1060, Message: "Duplicate column name 'embedding_revision'"},
			want: true,
		},
		{
			name: "duplicate key name",
			err:  &mysql.MySQLError{Number: 1061, Message: "Duplicate key name 'idx_files_status'"},
			want: true,
		},
		{
			name: "plain already exists",
			err:  errors.New("index already exists"),
			want: true,
		},
		{
			name: "plain duplicate",
			err:  errors.New("duplicate entry"),
			want: false,
		},
		{
			name: "non ignorable mysql",
			err:  &mysql.MySQLError{Number: 1146, Message: "Table 'missing' doesn't exist"},
			want: false,
		},
		{
			name: "non ignorable plain",
			err:  errors.New("permission denied"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isIgnorableTiDBSchemaError(tt.err); got != tt.want {
				t.Fatalf("isIgnorableTiDBSchemaError()=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseAlterTableAddIndexStatementAcceptsUniqueKey(t *testing.T) {
	tableName, indexName, createSQL, ok := parseAlterTableAddIndexStatement(
		"ALTER TABLE uploads ADD UNIQUE KEY uk_uploads_target (target_path)",
	)
	if !ok {
		t.Fatal("expected ALTER TABLE ... ADD UNIQUE KEY to parse")
	}
	if tableName != "uploads" {
		t.Fatalf("tableName=%q, want uploads", tableName)
	}
	if indexName != "uk_uploads_target" {
		t.Fatalf("indexName=%q, want uk_uploads_target", indexName)
	}
	if createSQL != "ALTER TABLE uploads ADD UNIQUE KEY uk_uploads_target (target_path)" {
		t.Fatalf("createSQL=%q", createSQL)
	}
}

func testFilesTableMeta(mode TiDBEmbeddingMode) tidbTableMeta {
	meta := tidbTableMeta{
		tableName: "files",
		columns: map[string]tidbColumnMeta{
			"file_id":                        {columnType: "varchar(64)"},
			"status":                         {columnType: "varchar(32)"},
			"content_text":                   {columnType: "longtext"},
			"embedding":                      {columnType: "vector(1024)"},
			"embedding_revision":             {columnType: "bigint"},
			"description":                    {columnType: "longtext"},
			"description_embedding":          {columnType: "vector(1024)"},
			"description_embedding_revision": {columnType: "bigint"},
		},
	}
	if mode == TiDBEmbeddingModeAuto {
		meta.columns["embedding"] = tidbColumnMeta{
			columnType:           "vector(1024)",
			extra:                "STORED GENERATED",
			generationExpression: "embed_text(_utf8mb4'tidbcloud_free/amazon/titan-embed-text-v2', `content_text`, _utf8mb4'{\"dimensions\":1024}')",
		}
		meta.columns["description_embedding"] = tidbColumnMeta{
			columnType:           "vector(1024)",
			extra:                "STORED GENERATED",
			generationExpression: "embed_text(_utf8mb4'tidbcloud_free/amazon/titan-embed-text-v2', `description`, _utf8mb4'{\"dimensions\":1024}')",
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

func TestParseConstraintUniqueIndexDefinitionUsesExplicitIndexName(t *testing.T) {
	indexName, columns, ok := parseConstraintUniqueIndexDefinition(
		"CONSTRAINT uploads_active_constraint UNIQUE KEY idx_uploads_active (active_target_path)",
	)
	if !ok {
		t.Fatal("expected constraint unique definition to parse")
	}
	if indexName != "idx_uploads_active" {
		t.Fatalf("indexName=%q, want idx_uploads_active", indexName)
	}
	if columns != "(active_target_path)" {
		t.Fatalf("columns=%q, want (active_target_path)", columns)
	}
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

type testRepairQueryResult struct {
	columns []string
	rows    [][]driver.Value
	err     error
}

type testRepairDriver struct {
	queryFn func(string) testRepairQueryResult
	execFn  func(string) error
}

type testRepairConn struct {
	queryFn func(string) testRepairQueryResult
	execFn  func(string) error
}

type testRepairRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

var testRepairDriverCounter uint64

func newTestRepairDB(t *testing.T, queryFn func(string) testRepairQueryResult, execFn func(string) error) *sql.DB {
	t.Helper()
	name := "test-repair-driver-" + strconv.FormatUint(atomic.AddUint64(&testRepairDriverCounter, 1), 10)
	sql.Register(name, testRepairDriver{queryFn: queryFn, execFn: execFn})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (d testRepairDriver) Open(string) (driver.Conn, error) {
	return testRepairConn(d), nil
}

func (c testRepairConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}
func (c testRepairConn) Close() error              { return nil }
func (c testRepairConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }

func (c testRepairConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	result := c.queryFn(query)
	if result.err != nil {
		return nil, result.err
	}
	if len(result.columns) == 0 {
		return &testRepairRows{}, nil
	}
	return &testRepairRows{columns: result.columns, rows: result.rows}, nil
}

func (c testRepairConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if c.execFn != nil {
		if err := c.execFn(query); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(1), nil
}

func (r *testRepairRows) Columns() []string { return r.columns }
func (r *testRepairRows) Close() error      { return nil }

func (r *testRepairRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}
