package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

func TestDetectTiDBEmbeddingModeFromMeta(t *testing.T) {
	autoMeta := testSemanticTableMeta(TiDBEmbeddingModeAuto)
	mode, err := detectTiDBEmbeddingModeFromMeta(autoMeta)
	if err != nil {
		t.Fatalf("detect auto mode: %v", err)
	}
	if mode != TiDBEmbeddingModeAuto {
		t.Fatalf("mode=%q, want %q", mode, TiDBEmbeddingModeAuto)
	}

	appMeta := testSemanticTableMeta(TiDBEmbeddingModeApp)
	mode, err = detectTiDBEmbeddingModeFromMeta(appMeta)
	if err != nil {
		t.Fatalf("detect app mode: %v", err)
	}
	if mode != TiDBEmbeddingModeApp {
		t.Fatalf("mode=%q, want %q", mode, TiDBEmbeddingModeApp)
	}
}

func TestDetectTiDBEmbeddingModeFallsBackToSemanticWhenFilesMetadataHasNoRows(t *testing.T) {
	db := newTestRepairDBWithArgs(t, func(query string, args []driver.NamedValue) testRepairQueryResult {
		normalized := normalizeSQLFragment(query)
		if strings.Contains(normalized, "select version()") {
			return testRepairQueryResult{columns: []string{"VERSION()"}, rows: [][]driver.Value{{"8.0.11-TiDB-v7.5.0"}}}
		}
		if strings.Contains(normalized, "from information_schema.columns") {
			tableName := queryArgString(args, 0)
			if tableName == "files" {
				return testRepairQueryResult{}
			}
			if tableName == "semantic" {
				return testRepairQueryResult{
					columns: []string{"column_name", "column_type", "extra", "generation_expression"},
					rows: [][]driver.Value{{
						"embedding",
						"vector(1024)",
						"STORED GENERATED",
						"embed_text(_utf8mb4'tidbcloud_free/amazon/titan-embed-text-v2', `content_text`, _utf8mb4'{\"dimensions\":1024}')",
					}},
				}
			}
		}
		return testRepairQueryResult{}
	}, nil)

	mode, err := DetectTiDBEmbeddingMode(db)
	if err != nil {
		t.Fatalf("DetectTiDBEmbeddingMode: %v", err)
	}
	if mode != TiDBEmbeddingModeAuto {
		t.Fatalf("mode=%q, want %q", mode, TiDBEmbeddingModeAuto)
	}
}

func TestValidateTiDBAutoEmbeddingSemanticTableAcceptsRealTiDBMetadata(t *testing.T) {
	if err := validateTiDBAutoEmbeddingSemanticTable(testSemanticTableMeta(TiDBEmbeddingModeAuto)); err != nil {
		t.Fatalf("expected auto semantic table to validate: %v", err)
	}
}

func TestValidateTiDBAutoEmbeddingSemanticTableRejectsWritableEmbedding(t *testing.T) {
	err := validateTiDBAutoEmbeddingSemanticTable(testSemanticTableMeta(TiDBEmbeddingModeApp))
	if err == nil {
		t.Fatal("expected writable embedding column to be rejected")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "generated") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateTiDBAppEmbeddingSemanticTableRejectsGeneratedEmbedding(t *testing.T) {
	err := validateTiDBAppEmbeddingSemanticTable(testSemanticTableMeta(TiDBEmbeddingModeAuto))
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

	for _, tableName := range []string{"file_nodes", "file_tags", "uploads", "semantic", "semantic_tasks", "file_gc_tasks", "vault_deks", "vault_audit_log"} {
		stmt := createByTable[tableName]
		if stmt == "" {
			t.Fatalf("missing create statement for %s", tableName)
		}
		if !strings.Contains(strings.ToLower(stmt), "create table if not exists "+tableName) {
			t.Fatalf("unexpected create statement for %s: %q", tableName, stmt)
		}
	}
	if _, ok := createByTable["files"]; ok {
		t.Fatal("new tenant schema spec must not create legacy files table")
	}
}

func TestLegacyTiDBFilesTableSpecRepairsExistingFilesColumnsAndIndexes(t *testing.T) {
	spec, err := legacyTiDBFilesTableSpecForMode(TiDBEmbeddingModeAuto)
	if err != nil {
		t.Fatalf("legacy files spec: %v", err)
	}
	meta := testLegacyFilesTableMeta(TiDBEmbeddingModeAuto)
	delete(meta.columns, "storage_ref_hash")

	diffs := diffTiDBTableMetaWithObservedIndexes(spec, meta, spec.createStatement, map[string]struct{}{}, true)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingColumn, "storage_ref_hash") {
		t.Fatalf("expected missing storage_ref_hash legacy files repair, got %#v", diffs)
	}
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_files_storage_ref_hash") {
		t.Fatalf("expected missing idx_files_storage_ref_hash legacy files repair, got %#v", diffs)
	}
	for _, diff := range diffs {
		if diff.kind == tidbSchemaDiffMissingTable {
			t.Fatalf("legacy compatibility diff must not recreate files table, got %#v", diffs)
		}
	}
}

func TestDiffLegacyTiDBFilesTableSkipsMissingFilesTable(t *testing.T) {
	db := newTestRepairDBWithArgs(t, func(query string, args []driver.NamedValue) testRepairQueryResult {
		if strings.Contains(normalizeSQLFragment(query), "from information_schema.columns") &&
			queryArgString(args, 0) == "files" {
			return testRepairQueryResult{}
		}
		return testRepairQueryResult{}
	}, nil)

	diffs, err := diffLegacyTiDBFilesTableIfExists(context.Background(), db, TiDBEmbeddingModeAuto)
	if err != nil {
		t.Fatalf("diff legacy files: %v", err)
	}
	if len(diffs) != 0 {
		t.Fatalf("expected missing legacy files table to be skipped, got %#v", diffs)
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

func TestParallelSchemaStatementGroupsKeepsTableOrder(t *testing.T) {
	groups, ok := parallelSchemaStatementGroups([]string{
		`CREATE TABLE IF NOT EXISTS t1 (id VARCHAR(64) PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS t2 (id VARCHAR(64) PRIMARY KEY)`,
		`CREATE INDEX idx_t1_id ON t1(id)`,
		`ALTER TABLE t2 ADD COLUMN name VARCHAR(64)`,
	})
	if !ok {
		t.Fatal("parallelSchemaStatementGroups did not classify statements")
	}
	if len(groups) != 2 {
		t.Fatalf("group count = %d, want 2", len(groups))
	}
	var t1, t2 []int
	for _, group := range groups {
		switch group.table {
		case "t1":
			for _, stmt := range group.statements {
				t1 = append(t1, stmt.index)
			}
		case "t2":
			for _, stmt := range group.statements {
				t2 = append(t2, stmt.index)
			}
		default:
			t.Fatalf("unexpected table group %q", group.table)
		}
	}
	if fmt.Sprint(t1) != "[0 2]" {
		t.Fatalf("t1 indexes = %v, want [0 2]", t1)
	}
	if fmt.Sprint(t2) != "[1 3]" {
		t.Fatalf("t2 indexes = %v, want [1 3]", t2)
	}
}

func TestParallelSchemaStatementGroupsRejectsUnclassifiedStatements(t *testing.T) {
	if _, ok := parallelSchemaStatementGroups([]string{
		`CREATE TABLE IF NOT EXISTS t1 (id VARCHAR(64) PRIMARY KEY)`,
		`SET @@GLOBAL.foo = 'bar'`,
	}); ok {
		t.Fatal("parallelSchemaStatementGroups classified non-DDL statement")
	}
}

func TestExecSchemaStatementsParallelByTableContextRunsIndependentTablesConcurrently(t *testing.T) {
	firstStarted := make(chan string, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	startOrder := make([]string, 0, 4)
	db := newTestRepairDB(t, func(string) testRepairQueryResult {
		return testRepairQueryResult{}
	}, func(query string) error {
		table, ok := schemaStatementTableName(query)
		if !ok {
			return fmt.Errorf("failed to classify query %q", query)
		}
		mu.Lock()
		startOrder = append(startOrder, table)
		mu.Unlock()
		if strings.Contains(normalizeSQLFragment(query), "create table") {
			select {
			case firstStarted <- table:
			default:
			}
			<-release
		}
		return nil
	})
	db.SetMaxOpenConns(4)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ExecSchemaStatementsParallelByTableContext(context.Background(), db, []string{
			`CREATE TABLE IF NOT EXISTS t1 (id VARCHAR(64) PRIMARY KEY)`,
			`CREATE INDEX idx_t1_id ON t1(id)`,
			`CREATE TABLE IF NOT EXISTS t2 (id VARCHAR(64) PRIMARY KEY)`,
			`CREATE INDEX idx_t2_id ON t2(id)`,
		})
	}()

	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case table := <-firstStarted:
			got[table] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for concurrent table creates; got %v", got)
		}
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("ExecSchemaStatementsParallelByTableContext: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(startOrder) != 4 {
		t.Fatalf("executed statement count = %d, want 4 (%v)", len(startOrder), startOrder)
	}
	seenCreate := map[string]bool{}
	for _, table := range startOrder {
		seenCreate[table] = true
	}
	if !seenCreate["t1"] || !seenCreate["t2"] {
		t.Fatalf("missing table create execution: %v", startOrder)
	}
}

func TestMissingTableAndIndexDiffsIncludesExternalIndexes(t *testing.T) {
	table := tidbTableSpec{
		name:            "uploads",
		createStatement: "CREATE TABLE IF NOT EXISTS uploads (...)",
		indexes: map[string]tidbIndexSpec{
			"idx_upload_path": {createSQL: "CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)"},
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
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_upload_path index", repairSQL: "CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)"},
		{kind: tidbSchemaDiffMissingColumn, tableName: "uploads", columnName: "expected_revision", repairSQL: "ALTER TABLE uploads ADD COLUMN expected_revision BIGINT NULL"},
		{kind: tidbSchemaDiffColumnType, tableName: "semantic", columnName: "embedding_revision", detail: "semantic schema contract: embedding_revision column type = \"int\", want bigint"},
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
	if got[2] != "CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)" {
		t.Fatalf("unexpected third repair statement: %q", got[2])
	}
}

func TestPlannedTiDBSchemaRepairsAllowsUniqueIndexOnExistingTable(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_upload_path index", repairSQL: "CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)"},
		{kind: tidbSchemaDiffMissingIndex, tableName: "uploads", detail: "uploads schema contract: missing idx_idempotency index", repairSQL: "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)"},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 2 {
		t.Fatalf("expected both missing indexes to be auto-repaired, got %#v", got)
	}
	if got[0] != "CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)" {
		t.Fatalf("unexpected first repair statement: %q", got[0])
	}
	if got[1] != "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)" {
		t.Fatalf("unexpected second repair statement: %q", got[1])
	}
}

func TestPlannedTiDBSchemaRepairsAllowsPathColumnWidening(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{
			kind:       tidbSchemaDiffColumnType,
			tableName:  "file_nodes",
			columnName: "path",
			repairSQL:  "ALTER TABLE file_nodes MODIFY COLUMN path TEXT NOT NULL",
		},
		{
			kind:       tidbSchemaDiffColumnType,
			tableName:  "uploads",
			columnName: "target_path",
			repairSQL:  "ALTER TABLE uploads MODIFY COLUMN target_path TEXT NOT NULL",
		},
		{
			kind:       tidbSchemaDiffColumnType,
			tableName:  "fs_events",
			columnName: "path",
			repairSQL:  "ALTER TABLE fs_events MODIFY COLUMN path TEXT NOT NULL",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 3 {
		t.Fatalf("expected path widening repairs, got %#v", got)
	}
}

func TestPlannedTiDBSchemaRepairsDefersPathHashIndexesUntilHashColumnsExist(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{
			kind:       tidbSchemaDiffMissingColumn,
			tableName:  "file_nodes",
			columnName: "path_hash",
			repairSQL:  "ALTER TABLE file_nodes ADD COLUMN path_hash VARCHAR(64) NOT NULL DEFAULT ''",
		},
		{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: "file_nodes",
			detail:    "file_nodes schema contract: missing idx_path index",
			repairSQL: "CREATE UNIQUE INDEX idx_path ON file_nodes(path_hash)",
		},
		{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: "uploads",
			detail:    "uploads schema contract: missing idx_idempotency index",
			repairSQL: "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 2 {
		t.Fatalf("expected hash index to be deferred while unrelated index remains, got %#v", got)
	}
	if got[0] != "ALTER TABLE file_nodes ADD COLUMN path_hash VARCHAR(64) NOT NULL DEFAULT ''" {
		t.Fatalf("unexpected first repair statement: %q", got[0])
	}
	if got[1] != "CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)" {
		t.Fatalf("unexpected second repair statement: %q", got[1])
	}
}

func TestPlannedTiDBSchemaRepairsDefersPathWideningUntilHashIndexesRebuilt(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{
			kind:       tidbSchemaDiffColumnType,
			tableName:  "file_nodes",
			columnName: "path",
			repairSQL:  "ALTER TABLE file_nodes MODIFY COLUMN path TEXT NOT NULL",
		},
		{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: "file_nodes",
			detail:    "file_nodes schema contract: idx_path index columns = (path), want (path_hash)",
			repairSQL: "ALTER TABLE file_nodes DROP INDEX idx_path",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 1 {
		t.Fatalf("expected only hash index repair before widening, got %#v", got)
	}
	if got[0] != "ALTER TABLE file_nodes DROP INDEX idx_path" {
		t.Fatalf("unexpected repair statement: %q", got[0])
	}
}

func TestPlannedTiDBSchemaRepairsDefersUploadTargetPathWideningUntilLegacyActivePathDropped(t *testing.T) {
	diffs := []tidbSchemaDiff{
		{
			kind:       tidbSchemaDiffColumnType,
			tableName:  "uploads",
			columnName: "target_path",
			repairSQL:  "ALTER TABLE uploads MODIFY COLUMN target_path TEXT NOT NULL",
		},
		{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: "uploads",
			detail:    "uploads schema contract: idx_uploads_active index columns = (active_target_path), want (active_target_path_hash)",
			repairSQL: "ALTER TABLE uploads DROP INDEX idx_uploads_active",
		},
		{
			kind:       tidbSchemaDiffExtraColumn,
			tableName:  "uploads",
			columnName: "active_target_path",
			repairSQL:  "ALTER TABLE uploads DROP COLUMN active_target_path",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 2 {
		t.Fatalf("expected old dependency repairs before widening, got %#v", got)
	}
	if got[0] != "ALTER TABLE uploads DROP INDEX idx_uploads_active" {
		t.Fatalf("unexpected first repair statement: %q", got[0])
	}
	if got[1] != "ALTER TABLE uploads DROP COLUMN active_target_path" {
		t.Fatalf("unexpected second repair statement: %q", got[1])
	}
}

func TestDiffTiDBTableMetaReportsLegacyUploadActiveTargetPath(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	meta := testUploadsTableMeta(true)
	meta.columns["target_path"] = tidbColumnMeta{columnType: "varchar(512)"}
	meta.columns["active_target_path"] = tidbColumnMeta{
		columnType:           "varchar(512)",
		extra:                "STORED GENERATED",
		generationExpression: "case when (`status` = _utf8mb4'UPLOADING') then `target_path` else NULL end",
	}
	delete(meta.columns, "active_target_path_hash")
	createStmt := `CREATE TABLE uploads (
		upload_id VARCHAR(64) PRIMARY KEY,
		target_path VARCHAR(512) NOT NULL,
		target_path_hash VARCHAR(64) NOT NULL DEFAULT '',
		status VARCHAR(32) NOT NULL,
		expected_revision BIGINT NULL,
		active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED,
		UNIQUE KEY idx_uploads_active (active_target_path)
	)`

	diffs := diffTiDBTableMeta(spec, meta, createStmt)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffExtraColumn, "legacy active_target_path") {
		t.Fatalf("expected legacy active_target_path diff, got %#v", diffs)
	}
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_uploads_active index columns") {
		t.Fatalf("expected idx_uploads_active column mismatch diff, got %#v", diffs)
	}

	plans := plannedTiDBSchemaRepairs(diffs)
	for _, plan := range plans {
		if plan == "ALTER TABLE uploads MODIFY COLUMN target_path TEXT NOT NULL" {
			t.Fatalf("target_path widening must wait until legacy generated column is dropped, plans=%#v", plans)
		}
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

func TestParseUniqueIndexRepairStatementAcceptsDropAndAddAlter(t *testing.T) {
	repair, ok := parseUniqueIndexRepairStatement("ALTER TABLE uploads DROP INDEX idx_uploads_active, ADD UNIQUE INDEX idx_uploads_active(active_target_path_hash)")
	if !ok {
		t.Fatal("expected combined drop/add unique index repair statement to parse")
	}
	if repair.tableName != "uploads" || repair.indexName != "idx_uploads_active" {
		t.Fatalf("unexpected repair target: %#v", repair)
	}
	if !equalStringSlices(repair.columns, []string{"active_target_path_hash"}) {
		t.Fatalf("unexpected repair columns: %#v", repair.columns)
	}
}

func TestBuildUniqueIndexDuplicateCheckSQL(t *testing.T) {
	repair := tidbUniqueIndexRepair{
		tableName: "uploads",
		indexName: "idx_uploads_active",
		columns:   []string{"active_target_path_hash"},
	}
	got := buildUniqueIndexDuplicateCheckSQL(repair)
	want := "SELECT 1 FROM `uploads` WHERE `active_target_path_hash` IS NOT NULL GROUP BY `active_target_path_hash` HAVING COUNT(*) > 1 LIMIT 1"
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

func TestValidateTiDBAutoEmbeddingDiffsReportsGeneratedContractMismatch(t *testing.T) {
	meta := testSemanticTableMeta(TiDBEmbeddingModeApp)
	diffs := validateTiDBAutoEmbeddingDiffs(meta)
	if len(diffs) == 0 {
		t.Fatal("expected auto embedding diffs for writable embedding column")
	}
	if !strings.Contains(strings.ToLower(diffs[0].detail), "stored generated") {
		t.Fatalf("unexpected diff detail: %#v", diffs)
	}
}

func TestValidateTiDBAutoEmbeddingDiffsAllowsWritableDescriptionEmbeddingCompat(t *testing.T) {
	meta := testSemanticTableMeta(TiDBEmbeddingModeAuto)
	meta.columns["description_embedding"] = tidbColumnMeta{columnType: "vector(1024)"}
	diffs := validateTiDBAutoEmbeddingDiffs(meta)
	for _, diff := range diffs {
		if diff.columnName == "description_embedding" {
			t.Fatalf("expected writable description_embedding compat column to be accepted, got %#v", diffs)
		}
	}
}

func TestValidateTiDBAutoEmbeddingDiffsAcceptsMixedCaseModelProfile(t *testing.T) {
	cfg, err := tidbAutoEmbeddingRenderConfigFor(TiDBAutoEmbeddingConfig{
		Model: "huggingface/BAAI/bge-m3",
	})
	if err != nil {
		t.Fatalf("render profile config: %v", err)
	}
	meta := testSemanticTableMeta(TiDBEmbeddingModeAuto)
	meta.columns["embedding"] = tidbColumnMeta{
		columnType:           "vector(1024)",
		extra:                "STORED GENERATED",
		generationExpression: "embed_text(_utf8mb4'huggingface/baai/bge-m3', `content_text`, _utf8mb4'{}')",
	}
	meta.columns["description_embedding"] = tidbColumnMeta{
		columnType:           "vector(1024)",
		extra:                "STORED GENERATED",
		generationExpression: "embed_text(_utf8mb4'huggingface/baai/bge-m3', `description`, _utf8mb4'{}')",
	}

	if diffs := validateTiDBAutoEmbeddingDiffsWithConfig(meta, cfg); len(diffs) != 0 {
		t.Fatalf("expected mixed-case model profile to validate, got %#v", diffs)
	}
}

func TestTiDBSQLStringLiteralEscapesBackslashesAndQuotes(t *testing.T) {
	got := tidbSQLStringLiteral("model\\with'quote\nline\rzero\x00tab\t")
	want := "'model\\\\with''quote\\nline\\rzero\\0tab\\t'"
	if got != want {
		t.Fatalf("tidbSQLStringLiteral()=%q, want %q", got, want)
	}
}

func TestTiDBAutoEmbeddingRenderConfigForProfileRejectsMismatchedOptionsJSON(t *testing.T) {
	_, err := tidbAutoEmbeddingRenderConfigForProfile(TiDBAutoEmbeddingProfile{
		Model:       "openai/text-embedding-3-small",
		Dimensions:  1536,
		OptionsJSON: `{"dimensions":512}`,
	})
	if err == nil {
		t.Fatal("expected mismatched options_json to fail")
	}
	if !strings.Contains(err.Error(), "options_json") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDiffTiDBTableMetaReportsMissingRequiredIndex(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	meta := testUploadsTableMeta(true)
	createStmt := `CREATE TABLE uploads (
		upload_id VARCHAR(64) PRIMARY KEY,
		target_path TEXT NOT NULL,
		target_path_hash VARCHAR(64) NOT NULL DEFAULT '',
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

func TestDiffTiDBTableMetaReportsPathHashIndexColumnMismatch(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "file_nodes")
	meta := tidbTableMeta{
		tableName: "file_nodes",
		columns: map[string]tidbColumnMeta{
			"node_id":          {columnType: "varchar(64)"},
			"path":             {columnType: "text"},
			"path_hash":        {columnType: "varchar(64)"},
			"parent_path":      {columnType: "text"},
			"parent_path_hash": {columnType: "varchar(64)"},
			"name":             {columnType: "varchar(255)"},
			"is_directory":     {columnType: "tinyint(1)"},
			"file_id":          {columnType: "varchar(64)"},
			"inode_id":         {columnType: "varchar(64)"},
			"created_at":       {columnType: "datetime(3)"},
		},
	}
	createStmt := `CREATE TABLE file_nodes (
		node_id VARCHAR(64) PRIMARY KEY,
		path TEXT NOT NULL,
		path_hash VARCHAR(64) NOT NULL DEFAULT '',
		parent_path TEXT NOT NULL,
		parent_path_hash VARCHAR(64) NOT NULL DEFAULT '',
		name VARCHAR(255) NOT NULL,
		is_directory TINYINT(1) NOT NULL DEFAULT 0,
		file_id VARCHAR(64),
		inode_id VARCHAR(64),
		created_at DATETIME(3) NOT NULL,
		UNIQUE KEY idx_path (path),
		KEY idx_parent (parent_path, name),
		KEY idx_file_id (file_id),
		KEY idx_inode_id (inode_id)
	)`

	diffs := diffTiDBTableMeta(spec, meta, createStmt)
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_path index columns") {
		t.Fatalf("expected idx_path column mismatch diff, got %#v", diffs)
	}
	if !hasDiffKindAndDetail(diffs, tidbSchemaDiffMissingIndex, "idx_parent index columns") {
		t.Fatalf("expected idx_parent column mismatch diff, got %#v", diffs)
	}
}

func TestDiffTiDBTableMetaRecognizesUniqueIndexFromCreateStatement(t *testing.T) {
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	meta := testUploadsTableMeta(true)
	createStmt := `CREATE TABLE uploads (
		upload_id VARCHAR(64) PRIMARY KEY,
		target_path TEXT NOT NULL,
		target_path_hash VARCHAR(64) NOT NULL DEFAULT '',
		status VARCHAR(32) NOT NULL,
		expected_revision BIGINT NULL,
		active_target_path_hash VARCHAR(64),
		UNIQUE KEY idx_uploads_active (active_target_path_hash)
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

func TestDiffTiDBTablesRunsIndependentTablesConcurrently(t *testing.T) {
	tables := []tidbTableSpec{
		{
			name:            "diff_parallel_t1",
			createStatement: "CREATE TABLE diff_parallel_t1 (id VARCHAR(64))",
			columns: map[string]tidbColumnSpec{
				"id": {columnType: "varchar(64)"},
			},
		},
		{
			name:            "diff_parallel_t2",
			createStatement: "CREATE TABLE diff_parallel_t2 (id VARCHAR(64))",
			columns: map[string]tidbColumnSpec{
				"id": {columnType: "varchar(64)"},
			},
		},
	}

	started := make(chan string, len(tables))
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	db := newTestRepairDBWithArgs(t, func(query string, args []driver.NamedValue) testRepairQueryResult {
		normalized := normalizeSQLFragment(query)
		switch {
		case strings.Contains(normalized, "from information_schema.columns"):
			tableName := fmt.Sprint(args[0].Value)
			select {
			case started <- tableName:
			default:
			}
			<-release
			return testRepairQueryResult{
				columns: []string{"column_name", "column_type", "extra", "generation_expression"},
				rows: [][]driver.Value{
					{"id", "varchar(64)", "", ""},
				},
			}
		case strings.HasPrefix(normalized, "show create table"):
			tableName := strings.TrimSpace(strings.TrimPrefix(query, "SHOW CREATE TABLE "))
			return testRepairQueryResult{
				columns: []string{"Table", "Create Table"},
				rows: [][]driver.Value{
					{tableName, fmt.Sprintf("CREATE TABLE %s (id VARCHAR(64))", tableName)},
				},
			}
		case strings.Contains(normalized, "from information_schema.statistics"):
			return testRepairQueryResult{columns: []string{"index_name"}}
		default:
			return testRepairQueryResult{err: fmt.Errorf("unexpected query: %s", query)}
		}
	}, nil)
	db.SetMaxOpenConns(2)

	errCh := make(chan error, 1)
	go func() {
		diffs, parallelism, err := diffTiDBTables(context.Background(), db, tables)
		if err != nil {
			errCh <- err
			return
		}
		if parallelism != 2 {
			errCh <- fmt.Errorf("parallelism = %d, want 2", parallelism)
			return
		}
		if len(diffs) != 0 {
			errCh <- fmt.Errorf("diff count = %d, want 0: %#v", len(diffs), diffs)
			return
		}
		errCh <- nil
	}()

	got := map[string]bool{}
	for len(got) < len(tables) {
		select {
		case tableName := <-started:
			got[tableName] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for concurrent table metadata queries; got %v", got)
		}
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-errCh; err != nil {
		t.Fatalf("diffTiDBTables: %v", err)
	}
}

func TestRepairMySQLPathHashSchemaUpgradesLegacyPathIndexes(t *testing.T) {
	if testDSN == "" {
		t.Skip("mysql test DSN not configured")
	}

	ctx := context.Background()
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	for _, stmt := range []string{
		"DROP TABLE IF EXISTS uploads",
		"DROP TABLE IF EXISTS file_nodes",
		`CREATE TABLE file_nodes (
			node_id VARCHAR(64) PRIMARY KEY,
			path VARCHAR(512) NOT NULL,
			parent_path VARCHAR(512) NOT NULL,
			name VARCHAR(255) NOT NULL,
			is_directory BOOLEAN NOT NULL DEFAULT FALSE,
			file_id VARCHAR(64),
			inode_id VARCHAR(64),
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			UNIQUE KEY idx_path (path),
			KEY idx_parent (parent_path, name)
		)`,
		`CREATE TABLE uploads (
			upload_id VARCHAR(64) PRIMARY KEY,
			file_id VARCHAR(64) NOT NULL,
			inode_id VARCHAR(64),
			target_path VARCHAR(512) NOT NULL,
			s3_upload_id VARCHAR(255) NOT NULL,
			s3_key VARCHAR(2048) NOT NULL,
			total_size BIGINT NOT NULL,
			part_size BIGINT NOT NULL,
			parts_total INT NOT NULL,
			expected_revision BIGINT NULL,
			status VARCHAR(32) NOT NULL DEFAULT 'UPLOADING',
			fingerprint_sha256 VARCHAR(128),
			idempotency_key VARCHAR(255),
			description LONGTEXT,
			active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) VIRTUAL,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			expires_at DATETIME(3) NOT NULL,
			KEY idx_upload_path (target_path, status),
			UNIQUE KEY idx_idempotency (idempotency_key),
			UNIQUE KEY idx_uploads_active (active_target_path)
		)`,
		`INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory)
			VALUES ('node-1', '/old/file.txt', '/old/', 'file.txt', FALSE),
			       ('node-2', '/old/dir/', '/old/', 'dir', TRUE)`,
		`INSERT INTO uploads (upload_id, file_id, target_path, s3_upload_id, s3_key, total_size, part_size, parts_total, idempotency_key, expires_at)
			VALUES ('upload-1', 'file-1', '/old/file.txt', 's3-upload-1', 's3-key-1', 3, 3, 1, 'idem-1', DATE_ADD(NOW(), INTERVAL 1 HOUR))`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec setup %q: %v", schemaStatementSnippet(stmt), err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS uploads")
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS file_nodes")
	})

	if err := repairMySQLPathHashSchema(ctx, db); err != nil {
		t.Fatalf("repairMySQLPathHashSchema: %v", err)
	}
	for _, stmt := range []string{
		"CREATE UNIQUE INDEX idx_path ON file_nodes(path_hash)",
		"CREATE INDEX idx_parent ON file_nodes(parent_path_hash, name)",
		"CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)",
		"CREATE UNIQUE INDEX idx_uploads_active ON uploads(active_target_path_hash)",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec repaired index %q: %v", stmt, err)
		}
	}

	var nodeHashes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM file_nodes
		WHERE path_hash = LOWER(SHA2(path, 256))
		  AND parent_path_hash = LOWER(SHA2(parent_path, 256))`).Scan(&nodeHashes); err != nil {
		t.Fatalf("query node hashes: %v", err)
	}
	if nodeHashes != 2 {
		t.Fatalf("backfilled file_nodes hashes = %d, want 2", nodeHashes)
	}

	var uploadHashes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM uploads
		WHERE target_path_hash = LOWER(SHA2(target_path, 256))`).Scan(&uploadHashes); err != nil {
		t.Fatalf("query upload hashes: %v", err)
	}
	if uploadHashes != 1 {
		t.Fatalf("backfilled upload hashes = %d, want 1", uploadHashes)
	}

	fileNodesCreate, err := loadShowCreateTable(ctx, db, "file_nodes")
	if err != nil {
		t.Fatalf("show create file_nodes: %v", err)
	}
	fileNodeIndexes, ok := parseObservedTiDBIndexColumns(fileNodesCreate)
	if !ok {
		t.Fatal("parse file_nodes indexes failed")
	}
	if got, want := fileNodeIndexes["idx_path"], []string{"path_hash"}; !equalStringSlices(got, want) {
		t.Fatalf("idx_path columns = %v, want %v", got, want)
	}
	if got, want := fileNodeIndexes["idx_parent"], []string{"parent_path_hash", "name"}; !equalStringSlices(got, want) {
		t.Fatalf("idx_parent columns = %v, want %v", got, want)
	}

	uploadsCreate, err := loadShowCreateTable(ctx, db, "uploads")
	if err != nil {
		t.Fatalf("show create uploads: %v", err)
	}
	uploadIndexes, ok := parseObservedTiDBIndexColumns(uploadsCreate)
	if !ok {
		t.Fatal("parse uploads indexes failed")
	}
	if got, want := uploadIndexes["idx_upload_path"], []string{"target_path_hash", "status"}; !equalStringSlices(got, want) {
		t.Fatalf("idx_upload_path columns = %v, want %v", got, want)
	}
	if got, want := uploadIndexes["idx_uploads_active"], []string{"active_target_path_hash"}; !equalStringSlices(got, want) {
		t.Fatalf("idx_uploads_active columns = %v, want %v", got, want)
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
		active_target_path_hash VARCHAR(64),
		CONSTRAINT idx_uploads_active UNIQUE (active_target_path_hash)
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
			active_target_path_hash VARCHAR(64),
			CONSTRAINT idx_uploads_active UNIQUE (active_target_path_hash)
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
			"node_id":          {columnType: "varchar(64)"},
			"path":             {columnType: "text"},
			"path_hash":        {columnType: "varchar(64)"},
			"parent_path":      {columnType: "text"},
			"parent_path_hash": {columnType: "varchar(64)"},
			"name":             {columnType: "varchar(255)"},
			"is_directory":     {columnType: "tinyint(1)"},
			"file_id":          {columnType: "varchar(64)"},
			"inode_id":         {columnType: "varchar(64)"},
			"created_at":       {columnType: "datetime(3)"},
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
			"node_id":          {columnType: "varchar(64)"},
			"path":             {columnType: "text"},
			"path_hash":        {columnType: "varchar(64)"},
			"parent_path":      {columnType: "text"},
			"parent_path_hash": {columnType: "varchar(64)"},
			"name":             {columnType: "varchar(255)"},
			"is_directory":     {columnType: "tinyint(1)"},
			"file_id":          {columnType: "varchar(64)"},
			"inode_id":         {columnType: "varchar(64)"},
			"created_at":       {columnType: "datetime(3)"},
		},
	}

	diffs := diffTiDBTableMeta(spec, meta, `CREATE TABLE file_nodes (
		node_id VARCHAR(64) PRIMARY KEY,
		path TEXT NOT NULL,
		path_hash VARCHAR(64) NOT NULL DEFAULT '',
		parent_path TEXT NOT NULL,
		parent_path_hash VARCHAR(64) NOT NULL DEFAULT '',
		name VARCHAR(255) NOT NULL,
		is_directory TINYINT(1) NOT NULL DEFAULT 0,
		file_id VARCHAR(64),
		inode_id VARCHAR(64),
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

func TestTiDBSchemaSpecIncludesStorageEncryptionColumns(t *testing.T) {
	contents := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "contents")
	for column, wantAddSQL := range map[string]string{
		"storage_encryption_mode":   "ALTER TABLE contents ADD COLUMN storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'legacy'",
		"storage_encryption_key_id": "ALTER TABLE contents ADD COLUMN storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT ''",
	} {
		spec, ok := contents.columns[column]
		if !ok {
			t.Fatalf("contents missing %s column", column)
		}
		if spec.addSQL != wantAddSQL {
			t.Fatalf("contents %s addSQL = %q, want %q", column, spec.addSQL, wantAddSQL)
		}
	}

	uploads := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "uploads")
	for column, wantAddSQL := range map[string]string{
		"storage_encryption_mode":   "ALTER TABLE uploads ADD COLUMN storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'none'",
		"storage_encryption_key_id": "ALTER TABLE uploads ADD COLUMN storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT ''",
	} {
		spec, ok := uploads.columns[column]
		if !ok {
			t.Fatalf("uploads missing %s column", column)
		}
		if spec.addSQL != wantAddSQL {
			t.Fatalf("uploads %s addSQL = %q, want %q", column, spec.addSQL, wantAddSQL)
		}
	}
}

func TestTiDBSchemaSpecForModeIncludesSemanticIndexes(t *testing.T) {
	// In auto-embedding mode, semantic table FULLTEXT and VECTOR indexes are
	// part of the enforceable schema contract and must appear in the spec.
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeAuto, "semantic")
	if _, ok := spec.indexes["idx_semantic_fts_content"]; !ok {
		t.Fatal("semantic missing idx_semantic_fts_content index spec from ALTER TABLE statement")
	}
	if _, ok := spec.indexes["idx_semantic_fts_description"]; !ok {
		t.Fatal("semantic missing idx_semantic_fts_description index spec from ALTER TABLE statement")
	}
	if _, ok := spec.indexes["idx_semantic_cosine"]; !ok {
		t.Fatal("semantic auto mode spec must include idx_semantic_cosine index")
	}
	if _, ok := spec.indexes["idx_semantic_desc_cosine"]; !ok {
		t.Fatal("semantic auto mode spec must include idx_semantic_desc_cosine index")
	}
}

func TestTiDBSchemaSpecForAppModeExcludesSemanticIndexes(t *testing.T) {
	// Optional semantic indexes use ADD_COLUMNAR_REPLICA_ON_DEMAND and must not
	// appear in the app mode schema contract so validation does not fail.
	spec := mustTiDBTableSpecByName(t, TiDBEmbeddingModeApp, "semantic")
	if _, ok := spec.indexes["idx_semantic_fts_content"]; ok {
		t.Fatal("semantic app mode spec must not include optional idx_semantic_fts_content index")
	}
	if _, ok := spec.indexes["idx_semantic_fts_description"]; ok {
		t.Fatal("semantic app mode spec must not include optional idx_semantic_fts_description index")
	}
	if _, ok := spec.indexes["idx_semantic_cosine"]; ok {
		t.Fatal("semantic app mode spec must not include optional idx_semantic_cosine index")
	}
	if _, ok := spec.indexes["idx_semantic_desc_cosine"]; ok {
		t.Fatal("semantic app mode spec must not include optional idx_semantic_desc_cosine index")
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
			tableName: "semantic",
			detail:    "semantic schema contract: missing idx_semantic_fts_content index",
			repairSQL: "ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_content(content_text)",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 1 {
		t.Fatalf("expected fulltext index repair to be included for existing table, got %#v", got)
	}
	if got[0] != "ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_content(content_text)" {
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
			tableName: "semantic",
			detail:    "semantic schema contract: missing idx_semantic_fts_content index",
			repairSQL: "ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_content(content_text)",
		},
	}

	got := plannedTiDBSchemaRepairs(diffs)
	if len(got) != 2 {
		t.Fatalf("expected create table and heavy index repair, got %#v", got)
	}
	if got[1] != "ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_content(content_text)" {
		t.Fatalf("unexpected second repair statement: %q", got[1])
	}
}

func TestIsSafeAddColumnRepairSQLRejectsGenericStoredAndVirtualGeneratedColumns(t *testing.T) {
	tests := []string{
		"ALTER TABLE uploads ADD COLUMN active_target_path_old TEXT AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED",
		"ALTER TABLE semantic ADD COLUMN embedding VECTOR(1024) AS (EMBED_TEXT('m', content_text, '{\"dimensions\":1024}')) VIRTUAL",
	}

	for _, stmt := range tests {
		if isSafeAddColumnRepairSQL(stmt) {
			t.Fatalf("expected generated column repair to be unsafe: %s", stmt)
		}
	}
}

func TestIsSafeAddColumnRepairSQLAllowsUploadActiveTargetHash(t *testing.T) {
	stmt := "ALTER TABLE uploads ADD COLUMN active_target_path_hash VARCHAR(64) AS (CASE WHEN status = 'UPLOADING' THEN target_path_hash ELSE NULL END) VIRTUAL"
	if !isSafeAddColumnRepairSQL(stmt) {
		t.Fatal("expected active_target_path_hash generated column to be safe to add")
	}
}

func TestIsSafeAddColumnRepairSQLAllowsStoredGeneratedVectorWithEmbedText(t *testing.T) {
	// description_embedding is a STORED GENERATED VECTOR column backed by EMBED_TEXT.
	// TiDB computes the value server-side so ALTER TABLE ADD COLUMN is safe on
	// existing tables even when embedding rows are absent.
	stmt := "ALTER TABLE semantic ADD COLUMN description_embedding VECTOR(1024) GENERATED ALWAYS AS (EMBED_TEXT('amazon.titan-embed-text-v2:0', description, '{\"dimensions\":1024}')) STORED"
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

func TestIsMissingTableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "wrapped no rows", err: errors.New("plain"), want: false},
		{name: "sql no rows", err: fmt.Errorf("load columns: %w", sql.ErrNoRows), want: true},
		{name: "mysql table missing", err: fmt.Errorf("wrapped: %w", &mysql.MySQLError{Number: 1146, Message: "Table 'files' doesn't exist"}), want: true},
		{name: "mysql syntax", err: &mysql.MySQLError{Number: 1064, Message: "syntax error"}, want: false},
		{name: "postgres relation missing", err: errors.New(`ERROR: relation "files" does not exist`), want: true},
		{name: "plain table missing", err: errors.New("Table 'files' doesn't exist"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMissingTableError(tt.err); got != tt.want {
				t.Fatalf("isMissingTableError()=%v, want %v", got, tt.want)
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

func TestStripColumnarReplicaOnDemand(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want string
	}{
		{
			name: "fulltext index with add_columnar_replica_on_demand",
			stmt: "ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_description(description) WITH PARSER MULTILINGUAL ADD_COLUMNAR_REPLICA_ON_DEMAND",
			want: "ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_description(description) WITH PARSER MULTILINGUAL",
		},
		{
			name: "vector index with add_columnar_replica_on_demand",
			stmt: "ALTER TABLE semantic ADD VECTOR INDEX idx_semantic_cosine((VEC_COSINE_DISTANCE(embedding))) ADD_COLUMNAR_REPLICA_ON_DEMAND",
			want: "ALTER TABLE semantic ADD VECTOR INDEX idx_semantic_cosine((VEC_COSINE_DISTANCE(embedding)))",
		},
		{
			name: "no add_columnar_replica_on_demand",
			stmt: "ALTER TABLE uploads ADD UNIQUE KEY uk_uploads_target (target_path)",
			want: "ALTER TABLE uploads ADD UNIQUE KEY uk_uploads_target (target_path)",
		},
		{
			name: "regular add index without columnar",
			stmt: "ALTER TABLE contents ADD INDEX idx_contents_ref (storage_ref_hash)",
			want: "ALTER TABLE contents ADD INDEX idx_contents_ref (storage_ref_hash)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripColumnarReplicaOnDemand(tt.stmt)
			if got != tt.want {
				t.Fatalf("stripColumnarReplicaOnDemand()=%q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseAlterTableAddFulltextIndexStripsColumnarReplicaOnDemand(t *testing.T) {
	stmt := `ALTER TABLE semantic
		ADD FULLTEXT INDEX idx_semantic_fts_description(description)
		WITH PARSER MULTILINGUAL
		ADD_COLUMNAR_REPLICA_ON_DEMAND`

	tableName, indexName, createSQL, ok := parseAlterTableAddIndexStatement(stmt)
	if !ok {
		t.Fatal("expected ALTER TABLE with ADD FULLTEXT INDEX to parse")
	}
	if tableName != "semantic" {
		t.Fatalf("tableName=%q, want semantic", tableName)
	}
	if indexName != "idx_semantic_fts_description" {
		t.Fatalf("indexName=%q, want idx_semantic_fts_description", indexName)
	}
	if strings.Contains(strings.ToLower(createSQL), "add_columnar_replica_on_demand") {
		t.Fatalf("createSQL must not contain ADD_COLUMNAR_REPLICA_ON_DEMAND: %q", createSQL)
	}
	if !strings.Contains(createSQL, "ADD FULLTEXT INDEX") {
		t.Fatalf("createSQL must contain ADD FULLTEXT INDEX: %q", createSQL)
	}
}

func TestParseAlterTableAddVectorIndexStripsColumnarReplicaOnDemand(t *testing.T) {
	stmt := `ALTER TABLE semantic
		ADD VECTOR INDEX idx_semantic_cosine((VEC_COSINE_DISTANCE(embedding)))
		ADD_COLUMNAR_REPLICA_ON_DEMAND`

	tableName, indexName, createSQL, ok := parseAlterTableAddIndexStatement(stmt)
	if !ok {
		t.Fatal("expected ALTER TABLE with ADD VECTOR INDEX to parse")
	}
	if tableName != "semantic" {
		t.Fatalf("tableName=%q, want semantic", tableName)
	}
	if indexName != "idx_semantic_cosine" {
		t.Fatalf("indexName=%q, want idx_semantic_cosine", indexName)
	}
	if strings.Contains(strings.ToLower(createSQL), "add_columnar_replica_on_demand") {
		t.Fatalf("createSQL must not contain ADD_COLUMNAR_REPLICA_ON_DEMAND: %q", createSQL)
	}
	if !strings.Contains(createSQL, "ADD VECTOR INDEX") {
		t.Fatalf("createSQL must contain ADD VECTOR INDEX: %q", createSQL)
	}
}

func TestParseObservedTiDBIndexesRecognizesFulltextAndVector(t *testing.T) {
	createStmt := `CREATE TABLE semantic (
		inode_id VARCHAR(64) PRIMARY KEY,
		content_text LONGTEXT,
		description LONGTEXT,
		embedding VECTOR(1024) GENERATED ALWAYS AS (EMBED_TEXT('m', content_text, '{}')) STORED,
		description_embedding VECTOR(1024) GENERATED ALWAYS AS (EMBED_TEXT('m', description, '{}')) STORED,
		FULLTEXT INDEX idx_semantic_fts_content (content_text),
		FULLTEXT KEY idx_semantic_fts_description (description),
		VECTOR INDEX idx_semantic_cosine ((VEC_COSINE_DISTANCE(embedding))),
		VECTOR INDEX idx_semantic_desc_cosine ((VEC_COSINE_DISTANCE(description_embedding)))
	)`
	observed, ok := parseObservedTiDBIndexes(createStmt)
	if !ok {
		t.Fatal("expected observed indexes parse to succeed")
	}
	if !hasObservedTiDBIndex(observed, "idx_semantic_fts_content") {
		t.Fatalf("expected idx_semantic_fts_content FULLTEXT INDEX to be observed, got %#v", observed)
	}
	if !hasObservedTiDBIndex(observed, "idx_semantic_fts_description") {
		t.Fatalf("expected idx_semantic_fts_description FULLTEXT KEY to be observed, got %#v", observed)
	}
	if !hasObservedTiDBIndex(observed, "idx_semantic_cosine") {
		t.Fatalf("expected idx_semantic_cosine VECTOR INDEX to be observed, got %#v", observed)
	}
	if !hasObservedTiDBIndex(observed, "idx_semantic_desc_cosine") {
		t.Fatalf("expected idx_semantic_desc_cosine VECTOR INDEX to be observed, got %#v", observed)
	}
	if !hasObservedTiDBIndex(observed, "primary") {
		t.Fatal("expected primary key to be observed from inline PRIMARY KEY column")
	}
}

func testSemanticTableMeta(mode TiDBEmbeddingMode) tidbTableMeta {
	meta := tidbTableMeta{
		tableName: "semantic",
		columns: map[string]tidbColumnMeta{
			"inode_id":                       {columnType: "varchar(64)"},
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

func testLegacyFilesTableMeta(mode TiDBEmbeddingMode) tidbTableMeta {
	meta := tidbTableMeta{
		tableName: "files",
		columns: map[string]tidbColumnMeta{
			"file_id":                        {columnType: "varchar(64)"},
			"storage_type":                   {columnType: "varchar(32)"},
			"storage_ref":                    {columnType: "text"},
			"storage_ref_hash":               {columnType: "varchar(64)"},
			"storage_encryption_mode":        {columnType: "varchar(16)"},
			"storage_encryption_key_id":      {columnType: "varchar(256)"},
			"content_blob":                   {columnType: "longblob"},
			"content_type":                   {columnType: "varchar(255)"},
			"size_bytes":                     {columnType: "bigint"},
			"checksum_sha256":                {columnType: "varchar(128)"},
			"revision":                       {columnType: "bigint"},
			"status":                         {columnType: "varchar(32)"},
			"source_id":                      {columnType: "varchar(255)"},
			"content_text":                   {columnType: "longtext"},
			"description":                    {columnType: "longtext"},
			"embedding":                      {columnType: "vector(1024)"},
			"embedding_revision":             {columnType: "bigint"},
			"description_embedding":          {columnType: "vector(1024)"},
			"description_embedding_revision": {columnType: "bigint"},
			"created_at":                     {columnType: "datetime(3)"},
			"confirmed_at":                   {columnType: "datetime(3)"},
			"expires_at":                     {columnType: "datetime(3)"},
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
	}
	return meta
}

func queryArgString(args []driver.NamedValue, idx int) string {
	if idx < 0 || idx >= len(args) {
		return ""
	}
	value, _ := args[idx].Value.(string)
	return value
}

func testUploadsTableMeta(includeExpectedRevision bool) tidbTableMeta {
	meta := tidbTableMeta{
		tableName: "uploads",
		columns: map[string]tidbColumnMeta{
			"upload_id":                 {columnType: "varchar(64)"},
			"target_path":               {columnType: "text"},
			"target_path_hash":          {columnType: "varchar(64)"},
			"active_target_path_hash":   {columnType: "varchar(64)"},
			"status":                    {columnType: "varchar(32)"},
			"storage_encryption_mode":   {columnType: "varchar(16)"},
			"storage_encryption_key_id": {columnType: "varchar(256)"},
		},
	}
	if includeExpectedRevision {
		meta.columns["expected_revision"] = tidbColumnMeta{columnType: "bigint"}
	}
	return meta
}

func TestParseConstraintUniqueIndexDefinitionUsesExplicitIndexName(t *testing.T) {
	indexName, columns, ok := parseConstraintUniqueIndexDefinition(
		"CONSTRAINT uploads_active_constraint UNIQUE KEY idx_uploads_active (active_target_path_hash)",
	)
	if !ok {
		t.Fatal("expected constraint unique definition to parse")
	}
	if indexName != "idx_uploads_active" {
		t.Fatalf("indexName=%q, want idx_uploads_active", indexName)
	}
	if columns != "(active_target_path_hash)" {
		t.Fatalf("columns=%q, want (active_target_path_hash)", columns)
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
	queryFn func(string, []driver.NamedValue) testRepairQueryResult
	execFn  func(string) error
}

type testRepairConn struct {
	queryFn func(string, []driver.NamedValue) testRepairQueryResult
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
	return newTestRepairDBWithArgs(t, func(query string, _ []driver.NamedValue) testRepairQueryResult {
		return queryFn(query)
	}, execFn)
}

func newTestRepairDBWithArgs(t *testing.T, queryFn func(string, []driver.NamedValue) testRepairQueryResult, execFn func(string) error) *sql.DB {
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

func (c testRepairConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	result := c.queryFn(query, args)
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

func TestIsFulltextOrVectorIndexRepairSQLCoversCreateIndexForms(t *testing.T) {
	cases := []struct {
		stmt string
		want bool
	}{
		{"ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_content(content_text) WITH PARSER MULTILINGUAL", true},
		{"ALTER TABLE semantic ADD VECTOR INDEX idx_semantic_cosine((VEC_COSINE_DISTANCE(embedding))) ADD_COLUMNAR_REPLICA_ON_DEMAND", true},
		// Repair-planner output form for a missing index (self-hosted engines
		// without FTS/vector support must be able to skip these).
		{"CREATE FULLTEXT INDEX idx_semantic_fts_content ON semantic(content_text) WITH PARSER MULTILINGUAL", true},
		{"CREATE VECTOR INDEX idx_semantic_cosine ON semantic(embedding)", true},
		{"CREATE UNIQUE INDEX idx_path ON file_nodes(path_hash)", false},
		{"CREATE INDEX idx_parent ON file_nodes(parent_path_hash, name)", false},
	}
	for _, tc := range cases {
		if got := isFulltextOrVectorIndexRepairSQL(tc.stmt); got != tc.want {
			t.Errorf("isFulltextOrVectorIndexRepairSQL(%q) = %v, want %v", tc.stmt, got, tc.want)
		}
	}
}
