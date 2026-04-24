package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/internal/schemaspec"
	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
)

type TiDBEmbeddingMode string

const (
	TiDBEmbeddingModeUnknown TiDBEmbeddingMode = "unknown"
	TiDBEmbeddingModeAuto    TiDBEmbeddingMode = "auto-embedding"
	TiDBEmbeddingModeApp     TiDBEmbeddingMode = "app-managed"
)

// Reference of Auto Embedding in TiDB Cloud: https://docs.pingcap.com/ai/vector-search-auto-embedding-amazon-titan/
const (
	tidbAutoEmbeddingModel      = "tidbcloud_free/amazon/titan-embed-text-v2"
	TiDBAutoEmbeddingDimensions = 1024
)

// CurrentTiDBTenantSchemaVersion is derived automatically from the content of
// the tenant auto-embedding init SQL statements. It changes whenever any
// statement in the Go source changes, so callers never have to maintain a
// manual counter.
//
// Tenants recorded with schema_version == CurrentTiDBTenantSchemaVersion in
// the meta store are skipped by EnsureTiDBSchemaForMode entirely.
//
// NOTE: this hash captures only changes to our Go-side SQL definitions, NOT
// changes to the TiDB server version.  Upgrading TiDB itself does not change
// the hash; existing tenant schemas therefore continue to be skipped
// correctly, because a TiDB version upgrade does not alter the user table
// structure that our init SQL created.  If a TiDB upgrade ever requires
// re-applying our schema (e.g., a required migration for a new major version),
// update any statement in tidbAutoEmbeddingSchemaStatements() to force a hash
// change and trigger a one-time re-Ensure for all tenants.
var CurrentTiDBTenantSchemaVersion = currentTiDBTenantSchemaVersion(tidbAutoEmbeddingSchemaStatements())

func currentTiDBTenantSchemaVersion(stmts []string) int {
	// Hash only statements that are parsed into the schema spec (CREATE TABLE,
	// CREATE [UNIQUE] INDEX, ALTER TABLE ... ADD ... INDEX).  Statements that
	// fall into none of these categories (e.g. SET, comments) do not affect
	// what ValidateTiDBSchemaForMode checks, so including them would cause
	// unnecessary re-Ensures on unrelated edits.
	//
	var specStmts []string
	for _, stmt := range stmts {
		n := normalizeSQLFragment(stmt)
		if strings.HasPrefix(n, "create table ") ||
			strings.HasPrefix(n, "create index ") ||
			strings.HasPrefix(n, "create unique index ") ||
			(strings.HasPrefix(n, "alter table ") && strings.Contains(n, " add ") && strings.Contains(n, " index ")) {
			specStmts = append(specStmts, schemaspec.CanonicalStatementForHash(stmt))
		}
	}
	return schemaspec.CRC32Version(specStmts)
}

var tidbAutoEmbeddingOptionsJSON = fmt.Sprintf(`{"dimensions":%d}`, TiDBAutoEmbeddingDimensions)

type tidbColumnMeta struct {
	columnType           string
	extra                string
	generationExpression string
}

type tidbTableMeta struct {
	tableName string
	columns   map[string]tidbColumnMeta
}

type tidbSchemaDiffKind string

const (
	tidbSchemaDiffMissingTable  tidbSchemaDiffKind = "missing_table"
	tidbSchemaDiffMissingColumn tidbSchemaDiffKind = "missing_column"
	tidbSchemaDiffMissingIndex  tidbSchemaDiffKind = "missing_index"
	tidbSchemaDiffColumnType    tidbSchemaDiffKind = "column_type_mismatch"
	tidbSchemaDiffTableContract tidbSchemaDiffKind = "table_contract_mismatch"
)

type tidbSchemaDiff struct {
	kind       tidbSchemaDiffKind
	tableName  string
	columnName string
	detail     string
	repairSQL  string
}

type tidbSchemaSpec struct {
	tables []tidbTableSpec
}

type tidbTableSpec struct {
	name            string
	createStatement string
	columns         map[string]tidbColumnSpec
	indexes         map[string]tidbIndexSpec
	primaryKey      tidbPrimaryKeySpec
	validate        func(tidbTableMeta) []tidbSchemaDiff
}

type tidbColumnSpec struct {
	columnType string
	addSQL     string
}

type tidbIndexSpec struct {
	createSQL string
}

type tidbPrimaryKeySpec struct {
	columns []string
}

type tidbUniqueIndexRepair struct {
	tableName string
	indexName string
	columns   []string
}

type tidbSchemaDiffError struct {
	mode  TiDBEmbeddingMode
	diffs []tidbSchemaDiff
}

func (e *tidbSchemaDiffError) Error() string {
	if e == nil || len(e.diffs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(e.diffs))
	for _, diff := range e.diffs {
		parts = append(parts, diff.detail)
	}
	return fmt.Sprintf("tidb schema contract mismatch for mode %q: %s", e.mode, strings.Join(parts, "; "))
}

// Keep this statement list aligned with the externally managed tidb_cloud_starter
// schema. If you change columns, indexes, generated expressions, or
// constraints here, rerun:
//
//	drive9-server schema dump-init-sql --provider tidb_cloud_starter
//
// and update tidb_cloud_starter with the exported SQL.
func tidbAutoEmbeddingSchemaStatements() []string {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (
			node_id      VARCHAR(64) PRIMARY KEY,
			path         VARCHAR(512) NOT NULL,
			parent_path  VARCHAR(512) NOT NULL,
			name         VARCHAR(255) NOT NULL,
			is_directory BOOLEAN NOT NULL DEFAULT FALSE,
			file_id      VARCHAR(64),
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE UNIQUE INDEX idx_path ON file_nodes(path)`,
		`CREATE INDEX idx_parent ON file_nodes(parent_path)`,
		`CREATE INDEX idx_file_id ON file_nodes(file_id)`,
		`CREATE TABLE IF NOT EXISTS files (
			file_id            VARCHAR(64) PRIMARY KEY,
			storage_type       VARCHAR(32) NOT NULL,
			storage_ref        TEXT NOT NULL,
			content_blob       LONGBLOB,
			content_type       VARCHAR(255),
			size_bytes         BIGINT NOT NULL DEFAULT 0,
			checksum_sha256    VARCHAR(128),
			revision           BIGINT NOT NULL DEFAULT 1,
			status             VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			source_id          VARCHAR(255),
			content_text       LONGTEXT,
			description        LONGTEXT,
			embedding          VECTOR(` + strconv.Itoa(TiDBAutoEmbeddingDimensions) + `) GENERATED ALWAYS AS (EMBED_TEXT(
				'` + tidbAutoEmbeddingModel + `',
				content_text,
				'` + tidbAutoEmbeddingOptionsJSON + `'
			)) STORED,
			embedding_revision BIGINT,
			description_embedding VECTOR(` + strconv.Itoa(TiDBAutoEmbeddingDimensions) + `) GENERATED ALWAYS AS (EMBED_TEXT(
				'` + tidbAutoEmbeddingModel + `',
				description,
				'` + tidbAutoEmbeddingOptionsJSON + `'
			)) STORED,
			description_embedding_revision BIGINT,
			created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			confirmed_at       DATETIME(3),
			expires_at         DATETIME(3)
		)`,
		`CREATE INDEX idx_status ON files(status, created_at)`,
		`ALTER TABLE files
			ADD FULLTEXT INDEX idx_fts_content(content_text)
			WITH PARSER MULTILINGUAL
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE files
			ADD FULLTEXT INDEX idx_fts_description(description)
			WITH PARSER MULTILINGUAL
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE files
			ADD VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE files
			ADD VECTOR INDEX idx_files_desc_cosine((VEC_COSINE_DISTANCE(description_embedding)))
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`CREATE TABLE IF NOT EXISTS file_tags (
			file_id   VARCHAR(64) NOT NULL,
			tag_key   VARCHAR(255) NOT NULL,
			tag_value VARCHAR(255) NOT NULL DEFAULT '',
			PRIMARY KEY (file_id, tag_key)
		)`,
		`CREATE INDEX idx_kv ON file_tags(tag_key, tag_value)`,
		`CREATE TABLE IF NOT EXISTS uploads (
			upload_id          VARCHAR(64) PRIMARY KEY,
			file_id            VARCHAR(64) NOT NULL,
			target_path        VARCHAR(512) NOT NULL,
			s3_upload_id       VARCHAR(255) NOT NULL,
			s3_key             VARCHAR(2048) NOT NULL,
			total_size         BIGINT NOT NULL,
			part_size          BIGINT NOT NULL,
			parts_total        INT NOT NULL,
			expected_revision  BIGINT NULL,
			status             VARCHAR(32) NOT NULL DEFAULT 'UPLOADING',
			fingerprint_sha256 VARCHAR(128),
			idempotency_key    VARCHAR(255),
			description        LONGTEXT,
			created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			expires_at         DATETIME(3) NOT NULL,
			active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED
		)`,
		`ALTER TABLE uploads ADD COLUMN expected_revision BIGINT NULL`,
		`CREATE INDEX idx_upload_path ON uploads(target_path, status)`,
		`CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)`,
		`CREATE UNIQUE INDEX idx_uploads_active ON uploads(active_target_path)`,
		`CREATE TABLE IF NOT EXISTS semantic_tasks (
			task_id           VARCHAR(64) PRIMARY KEY,
			task_type         VARCHAR(32) NOT NULL,
			resource_id       VARCHAR(64) NOT NULL,
			resource_version  BIGINT NOT NULL,
			status            VARCHAR(20) NOT NULL,
			attempt_count     INT NOT NULL DEFAULT 0,
			max_attempts      INT NOT NULL DEFAULT 5,
			receipt           VARCHAR(128) NULL,
			leased_at         DATETIME(3) NULL,
			lease_until       DATETIME(3) NULL,
			available_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			payload_json      JSON NULL,
			last_error        TEXT NULL,
			created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at      DATETIME(3) NULL
		)`,
		`CREATE UNIQUE INDEX uk_task_resource_version ON semantic_tasks(task_type, resource_id, resource_version)`,
		`CREATE INDEX idx_task_claim ON semantic_tasks(status, available_at, lease_until, created_at)`,
		`CREATE INDEX idx_task_claim_type ON semantic_tasks(status, task_type, available_at, created_at, task_id)`,
		`CREATE TABLE IF NOT EXISTS llm_usage (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			task_type       VARCHAR(32) NOT NULL,
			task_id         VARCHAR(64) NOT NULL,
			cost_millicents BIGINT NOT NULL DEFAULT 0,
			raw_units       BIGINT NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(16) NOT NULL,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_llm_usage_created ON llm_usage(created_at)`,
	}
	stmts = append(stmts, VaultTiDBSchemaStatements()...)
	return stmts
}

// DetectTiDBEmbeddingMode inspects the TiDB files-table embedding contract and
// reports whether the schema is in database-managed auto mode or app-managed mode.
func DetectTiDBEmbeddingMode(db *sql.DB) (TiDBEmbeddingMode, error) {
	ctx := context.Background()
	start := time.Now()
	logger.Info(ctx, "tenant_detect_tidb_embedding_mode_started")
	if db == nil {
		return TiDBEmbeddingModeUnknown, fmt.Errorf("nil db")
	}
	if !IsTiDBCluster(ctx, db) {
		return TiDBEmbeddingModeUnknown, fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	loadStart := time.Now()
	filesMeta, err := loadTiDBTableMeta(ctx, db, "files")
	if err != nil {
		logger.Warn(ctx, "tenant_detect_tidb_embedding_mode_load_files_failed",
			zap.Duration("elapsed", time.Since(loadStart)),
			zap.Duration("total_elapsed", time.Since(start)),
			zap.Error(err))
		return TiDBEmbeddingModeUnknown, fmt.Errorf("load files table metadata: %w", err)
	}
	logger.Info(ctx, "tenant_detect_tidb_embedding_mode_loaded_files",
		zap.Duration("elapsed", time.Since(loadStart)),
		zap.Duration("total_elapsed", time.Since(start)))
	detectStart := time.Now()
	mode, err := detectTiDBEmbeddingModeFromFilesMeta(filesMeta)
	if err != nil {
		logger.Warn(ctx, "tenant_detect_tidb_embedding_mode_failed",
			zap.Duration("elapsed", time.Since(detectStart)),
			zap.Duration("total_elapsed", time.Since(start)),
			zap.Error(err))
		return TiDBEmbeddingModeUnknown, fmt.Errorf("files schema contract: %w", err)
	}
	logger.Info(ctx, "tenant_detect_tidb_embedding_mode_finished",
		zap.String("mode", string(mode)),
		zap.Duration("elapsed", time.Since(detectStart)),
		zap.Duration("total_elapsed", time.Since(start)))
	return mode, nil
}

// ValidateTiDBSchemaForMode validates that an already-open TiDB connection
// matches exactly one supported dat9 embedding contract for the requested mode.
func ValidateTiDBSchemaForMode(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !IsTiDBCluster(ctx, db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := validateTiDBSchemaMode(mode); err != nil {
		return err
	}
	diffs, err := diffTiDBSchemaForMode(ctx, db, mode)
	if err != nil {
		return err
	}
	if len(diffs) > 0 {
		return &tidbSchemaDiffError{mode: mode, diffs: diffs}
	}
	return nil
}

// EnsureTiDBSchemaForMode repairs known launch-schema drift that can be fixed
// in place, then validates the full TiDB schema contract for the requested mode.
func EnsureTiDBSchemaForMode(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !IsTiDBCluster(ctx, db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := validateTiDBSchemaMode(mode); err != nil {
		return err
	}
	const maxRepairPasses = 3
	for i := 0; i < maxRepairPasses; i++ {
		diffs, err := diffTiDBSchemaForMode(ctx, db, mode)
		if err != nil {
			return err
		}
		if len(diffs) == 0 {
			return nil
		}
		repairs := plannedTiDBSchemaRepairs(diffs)
		if len(repairs) == 0 {
			// Drift remains but nothing in it is safe to repair automatically.
			break
		}
		if err := applyTiDBSchemaRepairs(ctx, db, repairs); err != nil {
			return err
		}
	}
	return ValidateTiDBSchemaForMode(ctx, db, mode)
}

func validateTiDBSchemaMode(mode TiDBEmbeddingMode) error {
	if mode != TiDBEmbeddingModeAuto && mode != TiDBEmbeddingModeApp {
		return fmt.Errorf("unsupported TiDB embedding mode %q", mode)
	}
	return nil
}

func initTiDBAutoEmbeddingSchema(dsn string) error {
	db, err := OpenTiDBSchemaDB(context.Background(), dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if !IsTiDBCluster(context.Background(), db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := ExecSchemaStatements(db, tidbAutoEmbeddingSchemaStatements()); err != nil {
		return err
	}
	return ValidateTiDBSchemaForMode(context.Background(), db, TiDBEmbeddingModeAuto)
}

// ValidateTiDBSchemaForModeDSN opens a DSN, validates the schema, and closes.
func ValidateTiDBSchemaForModeDSN(ctx context.Context, dsn string, mode TiDBEmbeddingMode) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return ValidateTiDBSchemaForMode(ctx, db, mode)
}

// EnsureTiDBSchemaForModeDSN opens a DSN, repairs known launch-schema drift,
// validates the schema contract, and closes.
func EnsureTiDBSchemaForModeDSN(ctx context.Context, dsn string, mode TiDBEmbeddingMode) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return EnsureTiDBSchemaForMode(ctx, db, mode)
}

func OpenTiDBSchemaDB(ctx context.Context, dsn string) (*sql.DB, error) {
	if HasMultiStatements(dsn) {
		return nil, fmt.Errorf("multiStatements is not allowed")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func detectTiDBEmbeddingModeFromFilesMeta(meta tidbTableMeta) (TiDBEmbeddingMode, error) {
	col, err := meta.requireColumn("embedding")
	if err != nil {
		return TiDBEmbeddingModeUnknown, err
	}
	if normalizeSQLFragment(col.columnType) != fmt.Sprintf("vector(%d)", TiDBAutoEmbeddingDimensions) {
		return TiDBEmbeddingModeUnknown, fmt.Errorf("unsupported embedding column type %q", col.columnType)
	}
	extra := normalizeSQLFragment(col.extra)
	expr := normalizeSQLFragment(col.generationExpression)
	if strings.Contains(extra, "generated") {
		if !strings.Contains(extra, "stored") {
			return TiDBEmbeddingModeUnknown, errors.New("embedding generated column must be stored")
		}
		if !strings.Contains(expr, "embed_text(") {
			return TiDBEmbeddingModeUnknown, errors.New("embedding generated expression must use EMBED_TEXT")
		}
		return TiDBEmbeddingModeAuto, nil
	}
	if expr != "" {
		return TiDBEmbeddingModeUnknown, errors.New("embedding generation expression present without generated column metadata")
	}
	return TiDBEmbeddingModeApp, nil
}

func validateTiDBAutoEmbeddingFilesTable(meta tidbTableMeta) error {
	if err := validateTiDBFilesTableBase(meta); err != nil {
		return err
	}
	return schemaDiffsToError(validateTiDBAutoEmbeddingFilesDiffs(meta))
}

func validateTiDBAppEmbeddingFilesTable(meta tidbTableMeta) error {
	if err := validateTiDBFilesTableBase(meta); err != nil {
		return err
	}
	return schemaDiffsToError(validateTiDBAppEmbeddingFilesDiffs(meta))
}

func validateTiDBFilesTableBase(meta tidbTableMeta) error {
	if err := meta.requireColumnType("file_id", "varchar(64)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("status", "varchar(32)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("content_text", "longtext"); err != nil {
		return err
	}
	if err := meta.requireColumnType("embedding", fmt.Sprintf("vector(%d)", TiDBAutoEmbeddingDimensions)); err != nil {
		return err
	}
	if err := meta.requireColumnType("embedding_revision", "bigint"); err != nil {
		return err
	}
	if err := meta.requireColumnType("description", "longtext"); err != nil {
		return err
	}
	if err := meta.requireColumnType("description_embedding", fmt.Sprintf("vector(%d)", TiDBAutoEmbeddingDimensions)); err != nil {
		return err
	}
	return meta.requireColumnType("description_embedding_revision", "bigint")
}

func validateTiDBUploadsTableBase(meta tidbTableMeta) error {
	if err := meta.requireColumnType("upload_id", "varchar(64)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("target_path", "varchar(512)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("status", "varchar(32)"); err != nil {
		return err
	}
	return meta.requireColumnType("expected_revision", "bigint")
}

func loadTiDBTableMeta(ctx context.Context, db *sql.DB, tableName string) (tidbTableMeta, error) {
	columns, err := loadTiDBColumnMeta(ctx, db, tableName)
	if err != nil {
		return tidbTableMeta{}, fmt.Errorf("load columns: %w", err)
	}
	return tidbTableMeta{tableName: tableName, columns: columns}, nil
}

func loadTiDBColumnMeta(ctx context.Context, db *sql.DB, tableName string) (map[string]tidbColumnMeta, error) {
	rows, err := db.QueryContext(ctx, `SELECT column_name, column_type, extra, generation_expression
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?`, tableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	columns := make(map[string]tidbColumnMeta)
	for rows.Next() {
		var name, columnType string
		var extra, generationExpression sql.NullString
		if err := rows.Scan(&name, &columnType, &extra, &generationExpression); err != nil {
			return nil, err
		}
		columns[strings.ToLower(name)] = tidbColumnMeta{
			columnType:           columnType,
			extra:                extra.String,
			generationExpression: generationExpression.String,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, sql.ErrNoRows
	}
	return columns, nil
}

func (m tidbTableMeta) requireColumn(name string) (tidbColumnMeta, error) {
	col, ok := m.columns[strings.ToLower(name)]
	if !ok {
		return tidbColumnMeta{}, fmt.Errorf("missing %s column", name)
	}
	return col, nil
}

func (m tidbTableMeta) requireColumnType(name, want string) error {
	col, err := m.requireColumn(name)
	if err != nil {
		return err
	}
	if normalizeColumnTypeForCompare(col.columnType) != normalizeColumnTypeForCompare(want) {
		return fmt.Errorf("%s column type = %q, want %s", name, col.columnType, want)
	}
	return nil
}

func loadShowCreateTable(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	var gotTable string
	var createStmt string
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	if err := db.QueryRowContext(ctx, query).Scan(&gotTable, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

func loadTiDBIndexNames(ctx context.Context, db *sql.DB, tableName string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT index_name
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ?`, tableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	indexNames := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		indexNames[strings.ToLower(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return indexNames, nil
}

func normalizeSQLFragment(s string) string {
	return schemaspec.NormalizeSQLFragment(s)
}

func normalizeColumnTypeForCompare(s string) string {
	normalized := normalizeSQLFragment(s)
	switch normalized {
	case "bool", "boolean", "tinyint(1)":
		return "boolean"
	default:
		return normalized
	}
}

func diffTiDBSchemaForMode(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode) ([]tidbSchemaDiff, error) {
	spec, err := tidbSchemaSpecForMode(mode)
	if err != nil {
		return nil, err
	}
	var diffs []tidbSchemaDiff
	for _, table := range spec.tables {
		tableDiffs, err := diffTiDBTable(ctx, db, table)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, tableDiffs...)
	}
	return diffs, nil
}

func diffTiDBTable(ctx context.Context, db *sql.DB, table tidbTableSpec) ([]tidbSchemaDiff, error) {
	meta, err := loadTiDBTableMeta(ctx, db, table.name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return missingTableAndIndexDiffs(table), nil
		}
		return nil, fmt.Errorf("load %s table metadata: %w", table.name, err)
	}
	createStmt, err := loadShowCreateTable(ctx, db, table.name)
	if err != nil {
		return nil, fmt.Errorf("show create %s: %w", table.name, err)
	}
	observedIndexes, indexesObserved := loadObservedTiDBIndexes(ctx, db, table.name, createStmt)
	return diffTiDBTableMetaWithObservedIndexes(table, meta, createStmt, observedIndexes, indexesObserved), nil
}

func loadObservedTiDBIndexes(ctx context.Context, db *sql.DB, tableName, createStmt string) (map[string]struct{}, bool) {
	indexNames, err := loadTiDBIndexNames(ctx, db, tableName)
	if err == nil {
		return indexNames, true
	}
	logger.Warn(ctx, "tenant_tidb_schema_load_index_metadata_failed",
		zap.String("table", tableName),
		zap.Error(err))
	observedIndexes, ok := parseObservedTiDBIndexes(createStmt)
	if ok {
		return observedIndexes, true
	}
	logger.Warn(ctx, "tenant_tidb_schema_parse_show_create_indexes_failed",
		zap.String("table", tableName))
	return nil, false
}

func tidbSchemaSpecForMode(mode TiDBEmbeddingMode) (tidbSchemaSpec, error) {
	// For app mode, use only the base (required) statements. The optional
	// indexes (FULLTEXT, VECTOR with ADD_COLUMNAR_REPLICA_ON_DEMAND) may be
	// silently skipped on TiDB versions that do not support that syntax, so
	// they must not be part of the enforceable schema contract.
	var stmts []string
	if mode == TiDBEmbeddingModeApp {
		stmts = tidbAppEmbeddingBaseSchemaStatements()
	} else {
		var err error
		stmts, err = InitTiDBTenantSchemaStatementsForMode(mode)
		if err != nil {
			return tidbSchemaSpec{}, err
		}
	}
	spec, err := tidbSchemaSpecFromStatements(stmts)
	if err != nil {
		return tidbSchemaSpec{}, err
	}
	for i := range spec.tables {
		if spec.tables[i].name != "files" {
			continue
		}
		spec.tables[i].validate = func(meta tidbTableMeta) []tidbSchemaDiff {
			switch mode {
			case TiDBEmbeddingModeAuto:
				return validateTiDBAutoEmbeddingFilesDiffs(meta)
			case TiDBEmbeddingModeApp:
				return validateTiDBAppEmbeddingFilesDiffs(meta)
			default:
				return []tidbSchemaDiff{{kind: tidbSchemaDiffTableContract, tableName: "files", detail: fmt.Sprintf("files schema contract: unsupported TiDB embedding mode %q", mode)}}
			}
		}
		break
	}
	return spec, nil
}

func tidbSchemaSpecFromStatements(stmts []string) (tidbSchemaSpec, error) {
	tables := make([]tidbTableSpec, 0)
	byName := make(map[string]int)
	for _, stmt := range stmts {
		table, ok, err := parseCreateTableSpec(stmt)
		if err != nil {
			return tidbSchemaSpec{}, err
		}
		if !ok {
			continue
		}
		tables = append(tables, table)
		byName[table.name] = len(tables) - 1
	}
	for _, stmt := range stmts {
		tableName, indexName, createSQL, ok := parseCreateIndexStatement(stmt)
		if !ok {
			tableName, indexName, createSQL, ok = parseAlterTableAddIndexStatement(stmt)
		}
		if !ok {
			continue
		}
		tableIndex, exists := byName[tableName]
		if !exists {
			continue
		}
		if tables[tableIndex].indexes == nil {
			tables[tableIndex].indexes = make(map[string]tidbIndexSpec)
		}
		tables[tableIndex].indexes[indexName] = tidbIndexSpec{createSQL: strings.TrimSpace(createSQL)}
	}
	return tidbSchemaSpec{tables: tables}, nil
}

func parseCreateTableSpec(stmt string) (tidbTableSpec, bool, error) {
	tableName, defs, ok, err := parseCreateTableStatement(stmt)
	if err != nil {
		return tidbTableSpec{}, false, err
	}
	if !ok {
		return tidbTableSpec{}, false, nil
	}
	columns := make(map[string]tidbColumnSpec)
	indexes := make(map[string]tidbIndexSpec)
	var primaryKey tidbPrimaryKeySpec
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		if pkSpec, ok := parsePrimaryKeyDefinition(def); ok {
			primaryKey = pkSpec
			continue
		}
		if indexName, createSQL, ok := parseInlineIndexDefinition(tableName, def); ok {
			indexes[indexName] = tidbIndexSpec{createSQL: createSQL}
			continue
		}
		if isConstraintDefinition(def) {
			continue
		}
		colName, colSpec, ok := parseColumnDefinition(tableName, def)
		if !ok {
			continue
		}
		columns[colName] = colSpec
		if pkCol, ok := parseInlinePrimaryKeyColumn(def); ok {
			primaryKey = tidbPrimaryKeySpec{columns: []string{pkCol}}
		}
	}
	return tidbTableSpec{
		name:            tableName,
		createStatement: strings.TrimSpace(stmt),
		columns:         columns,
		indexes:         indexes,
		primaryKey:      primaryKey,
	}, true, nil
}

func parseCreateTableStatement(stmt string) (tableName string, definitions string, ok bool, err error) {
	return schemaspec.ParseCreateTableStatement(stmt)
}

func splitTopLevelComma(definitions string) []string {
	return schemaspec.SplitTopLevelComma(definitions)
}

func parseInlineIndexDefinition(tableName, def string) (indexName, createSQL string, ok bool) {
	normalized := normalizeSQLFragment(def)
	if strings.HasPrefix(normalized, "unique index ") || strings.HasPrefix(normalized, "unique key ") {
		prefix := "UNIQUE INDEX"
		if strings.HasPrefix(normalized, "unique key ") {
			prefix = "UNIQUE KEY"
		}
		name, cols := parseIndexNameAndColumns(def, prefix)
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s%s", name, tableName, cols), true
	}
	if name, cols, ok := parseConstraintUniqueIndexDefinition(def); ok {
		return name, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s%s", name, tableName, cols), true
	}
	if strings.HasPrefix(normalized, "index ") || strings.HasPrefix(normalized, "key ") {
		prefix := "INDEX"
		if strings.HasPrefix(normalized, "key ") {
			prefix = "KEY"
		}
		name, cols := parseIndexNameAndColumns(def, prefix)
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE INDEX %s ON %s%s", name, tableName, cols), true
	}
	return "", "", false
}

func parseConstraintUniqueIndexDefinition(def string) (indexName, columns string, ok bool) {
	trimmed := strings.TrimSpace(def)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "CONSTRAINT ") {
		return "", "", false
	}
	rest := strings.TrimSpace(trimmed[len("CONSTRAINT "):])
	name, remainder := splitIdentifierAndRest(rest)
	if name == "" || remainder == "" {
		return "", "", false
	}
	remainder = strings.TrimSpace(remainder)
	upperRemainder := strings.ToUpper(remainder)
	switch {
	case strings.HasPrefix(upperRemainder, "UNIQUE KEY"):
		return parseConstraintUniqueIndexSuffix(name, remainder[len("UNIQUE KEY"):])
	case strings.HasPrefix(upperRemainder, "UNIQUE INDEX"):
		return parseConstraintUniqueIndexSuffix(name, remainder[len("UNIQUE INDEX"):])
	case strings.HasPrefix(upperRemainder, "UNIQUE"):
		return parseConstraintUniqueIndexSuffix(name, remainder[len("UNIQUE"):])
	default:
		return "", "", false
	}
}

func parseConstraintUniqueIndexSuffix(defaultName, suffix string) (indexName, columns string, ok bool) {
	suffix = strings.TrimSpace(suffix)
	if suffix == "" {
		return "", "", false
	}
	name, columnSuffix := splitIdentifierAndSuffix(suffix)
	if name == "" {
		name = defaultName
		columnSuffix = suffix
	}
	cols := parseIndexColumnsSuffix(columnSuffix)
	if cols == "" {
		return "", "", false
	}
	return strings.ToLower(name), cols, true
}

func parseIndexNameAndColumns(def, prefix string) (indexName, columns string) {
	trimmed := strings.TrimSpace(def)
	upper := strings.ToUpper(trimmed)
	p := strings.Index(upper, prefix)
	if p < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(trimmed[p+len(prefix):])
	if rest == "" {
		return "", ""
	}
	name, remainder := splitIdentifierAndRest(rest)
	if name == "" {
		return "", ""
	}
	open := strings.Index(remainder, "(")
	if open < 0 {
		return "", ""
	}
	return strings.ToLower(name), strings.TrimSpace(remainder[open:])
}

func parseIndexColumnsSuffix(s string) string {
	trimmed := strings.TrimSpace(s)
	open := strings.Index(trimmed, "(")
	if open < 0 {
		return ""
	}
	return strings.TrimSpace(trimmed[open:])
}

func parsePrimaryKeyDefinition(def string) (tidbPrimaryKeySpec, bool) {
	normalized := normalizeSQLFragment(def)
	isPrimaryConstraint := strings.HasPrefix(normalized, "primary key")
	isNamedPrimaryConstraint := strings.HasPrefix(normalized, "constraint ") && strings.Contains(normalized, " primary key ")
	if !isPrimaryConstraint && !isNamedPrimaryConstraint {
		return tidbPrimaryKeySpec{}, false
	}
	columns := parseKeyColumnList(def, "PRIMARY KEY")
	if len(columns) == 0 {
		return tidbPrimaryKeySpec{}, false
	}
	return tidbPrimaryKeySpec{columns: columns}, true
}

func parseInlinePrimaryKeyColumn(def string) (string, bool) {
	if !strings.Contains(normalizeSQLFragment(def), " primary key") {
		return "", false
	}
	name, rest := splitIdentifierAndRest(strings.TrimSpace(def))
	if name == "" || rest == "" {
		return "", false
	}
	return strings.ToLower(name), true
}

func parseKeyColumnList(def, token string) []string {
	upper := strings.ToUpper(def)
	pos := strings.Index(upper, token)
	if pos < 0 {
		return nil
	}
	rest := strings.TrimSpace(def[pos+len(token):])
	open := strings.Index(rest, "(")
	close := strings.LastIndex(rest, ")")
	if open < 0 || close <= open {
		return nil
	}
	inner := strings.TrimSpace(rest[open+1 : close])
	if inner == "" {
		return nil
	}
	parts := splitTopLevelComma(inner)
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		name := parseColumnReferenceName(part)
		if name == "" {
			return nil
		}
		columns = append(columns, name)
	}
	return columns
}

func parseColumnReferenceName(def string) string {
	trimmed := strings.TrimSpace(def)
	if trimmed == "" {
		return ""
	}
	if trimmed[0] == '`' {
		end := strings.Index(trimmed[1:], "`")
		if end < 0 {
			return ""
		}
		return strings.ToLower(trimmed[1 : 1+end])
	}
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case ' ', '\t', '\n', '\r', '(':
			return strings.ToLower(trimmed[:i])
		}
	}
	return strings.ToLower(trimmed)
}

func parseColumnDefinition(tableName, def string) (string, tidbColumnSpec, bool) {
	name, rest := splitIdentifierAndRest(strings.TrimSpace(def))
	if name == "" || rest == "" {
		return "", tidbColumnSpec{}, false
	}
	colType := parseColumnType(rest)
	if colType == "" {
		return "", tidbColumnSpec{}, false
	}
	return strings.ToLower(name), tidbColumnSpec{
		columnType: strings.ToLower(strings.TrimSpace(colType)),
		addSQL:     fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, name, strings.TrimSpace(rest)),
	}, true
}

func parseColumnType(rest string) string {
	return schemaspec.ParseColumnType(rest)
}

func splitIdentifierAndRest(s string) (identifier string, rest string) {
	return schemaspec.SplitIdentifierAndRest(s)
}

func splitIdentifierAndSuffix(s string) (identifier string, rest string) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return "", ""
	}
	if trimmed[0] == '`' {
		end := strings.Index(trimmed[1:], "`")
		if end < 0 {
			return "", ""
		}
		return trimmed[1 : 1+end], strings.TrimSpace(trimmed[1+end+1:])
	}
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case ' ', '\t', '\n', '\r', '(':
			return trimmed[:i], strings.TrimSpace(trimmed[i:])
		}
	}
	return trimmed, ""
}

func isConstraintDefinition(def string) bool {
	normalized := normalizeSQLFragment(def)
	return strings.HasPrefix(normalized, "primary key") ||
		strings.HasPrefix(normalized, "constraint ") ||
		strings.HasPrefix(normalized, "unique key ")
}

func parseCreateIndexStatement(stmt string) (tableName, indexName, createSQL string, ok bool) {
	normalized := normalizeSQLFragment(stmt)
	prefix := ""
	switch {
	case strings.HasPrefix(normalized, "create unique index "):
		prefix = "create unique index "
	case strings.HasPrefix(normalized, "create index "):
		prefix = "create index "
	default:
		return "", "", "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(normalized, prefix))
	parts := strings.SplitN(rest, " on ", 2)
	if len(parts) != 2 {
		return "", "", "", false
	}
	nameFields := strings.Fields(parts[0])
	if len(nameFields) == 0 {
		return "", "", "", false
	}
	afterOn := strings.TrimSpace(parts[1])
	if afterOn == "" {
		return "", "", "", false
	}
	tableEnd := strings.IndexAny(afterOn, " (")
	if tableEnd < 0 {
		tableEnd = len(afterOn)
	}
	table := strings.TrimSpace(afterOn[:tableEnd])
	if table == "" {
		return "", "", "", false
	}
	return strings.ToLower(table), strings.ToLower(nameFields[0]), strings.TrimSpace(stmt), true
}

func parseAlterTableAddIndexStatement(stmt string) (tableName, indexName, createSQL string, ok bool) {
	normalized := normalizeSQLFragment(stmt)
	const prefix = "alter table "
	if !strings.HasPrefix(normalized, prefix) {
		return "", "", "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(normalized, prefix))
	if rest == "" {
		return "", "", "", false
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", "", "", false
	}
	table := strings.ToLower(fields[0])

	markers := []string{" add fulltext index ", " add vector index ", " add unique index ", " add unique key ", " add index ", " add key "}
	for _, marker := range markers {
		pos := strings.Index(normalized, marker)
		if pos < 0 {
			continue
		}
		indexPart := strings.TrimSpace(normalized[pos+len(marker):])
		if indexPart == "" {
			return "", "", "", false
		}
		name := indexPart
		if open := strings.Index(name, "("); open >= 0 {
			name = name[:open]
		}
		nameFields := strings.Fields(name)
		if len(nameFields) == 0 {
			return "", "", "", false
		}
		return table, strings.ToLower(nameFields[0]), strings.TrimSpace(stmt), true
	}

	return "", "", "", false
}

func diffTiDBTableMeta(table tidbTableSpec, meta tidbTableMeta, createStmt string) []tidbSchemaDiff {
	observedIndexes, ok := parseObservedTiDBIndexes(createStmt)
	return diffTiDBTableMetaWithObservedIndexes(table, meta, createStmt, observedIndexes, ok)
}

func diffTiDBTableMetaWithObservedIndexes(table tidbTableSpec, meta tidbTableMeta, createStmt string, observedIndexes map[string]struct{}, indexesObserved bool) []tidbSchemaDiff {
	var diffs []tidbSchemaDiff
	for _, name := range sortedColumnNames(table.columns) {
		spec := table.columns[name]
		col, err := meta.requireColumn(name)
		if err != nil {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffMissingColumn,
				tableName:  table.name,
				columnName: name,
				detail:     fmt.Sprintf("%s schema contract: missing %s column", table.name, name),
				repairSQL:  spec.addSQL,
			})
			continue
		}
		if normalizeColumnTypeForCompare(col.columnType) != normalizeColumnTypeForCompare(spec.columnType) {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffColumnType,
				tableName:  table.name,
				columnName: name,
				detail:     fmt.Sprintf("%s schema contract: %s column type = %q, want %s", table.name, name, col.columnType, spec.columnType),
			})
		}
	}
	if len(table.primaryKey.columns) > 0 {
		actualPrimaryKey, ok := parsePrimaryKeyColumnsFromCreateStatement(createStmt)
		if !ok {
			diffs = append(diffs, tidbSchemaDiff{
				kind:      tidbSchemaDiffTableContract,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: missing primary key constraint", table.name),
			})
		} else if !equalStringSlices(actualPrimaryKey, table.primaryKey.columns) {
			diffs = append(diffs, tidbSchemaDiff{
				kind:      tidbSchemaDiffTableContract,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: primary key columns = (%s), want (%s)", table.name, strings.Join(actualPrimaryKey, ", "), strings.Join(table.primaryKey.columns, ", ")),
			})
		}
	}
	if !indexesObserved {
		if len(table.indexes) > 0 {
			diffs = append(diffs, tidbSchemaDiff{
				kind:      tidbSchemaDiffTableContract,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: unable to inspect indexes", table.name),
			})
		}
	} else {
		for _, name := range sortedIndexNames(table.indexes) {
			spec := table.indexes[name]
			if !hasObservedTiDBIndex(observedIndexes, name) {
				diffs = append(diffs, tidbSchemaDiff{
					kind:      tidbSchemaDiffMissingIndex,
					tableName: table.name,
					detail:    fmt.Sprintf("%s schema contract: missing %s index", table.name, name),
					repairSQL: spec.createSQL,
				})
			}
		}
	}
	if table.validate != nil {
		diffs = append(diffs, table.validate(meta)...)
	}
	return diffs
}

func hasObservedTiDBIndex(observedIndexes map[string]struct{}, indexName string) bool {
	if len(observedIndexes) == 0 {
		return false
	}
	_, ok := observedIndexes[strings.ToLower(indexName)]
	return ok
}

func parseObservedTiDBIndexes(createStmt string) (map[string]struct{}, bool) {
	_, defs, ok, err := parseCreateTableStatement(createStmt)
	if err != nil || !ok {
		return nil, false
	}
	observed := make(map[string]struct{})
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		normalized := normalizeSQLFragment(def)
		switch {
		case strings.HasPrefix(normalized, "primary key") || strings.Contains(normalized, " primary key"):
			observed["primary"] = struct{}{}
		case strings.HasPrefix(normalized, "constraint "):
			if name, _, ok := parseConstraintUniqueIndexDefinition(def); ok {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "unique key "):
			if name, _ := parseIndexNameAndColumns(def, "UNIQUE KEY"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "unique index "):
			if name, _ := parseIndexNameAndColumns(def, "UNIQUE INDEX"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "index "):
			if name, _ := parseIndexNameAndColumns(def, "INDEX"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "key "):
			if name, _ := parseIndexNameAndColumns(def, "KEY"); name != "" {
				observed[name] = struct{}{}
			}
		}
	}
	return observed, true
}

func parseIndexColumnList(def string) []string {
	inner, ok := parseParenthesizedList(def)
	if !ok {
		return nil
	}
	parts := splitTopLevelComma(inner)
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		name := parseColumnReferenceName(part)
		if name == "" {
			return nil
		}
		columns = append(columns, name)
	}
	return columns
}

func parseParenthesizedList(s string) (string, bool) {
	open := strings.Index(s, "(")
	if open < 0 {
		return "", false
	}
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(s[open+1 : i]), true
			}
		}
	}
	return "", false
}

func parsePrimaryKeyColumnsFromCreateStatement(createStmt string) ([]string, bool) {
	_, defs, ok, err := parseCreateTableStatement(createStmt)
	if err != nil || !ok {
		return nil, false
	}
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		if pkSpec, ok := parsePrimaryKeyDefinition(def); ok {
			return pkSpec.columns, true
		}
		if columnName, ok := parseInlinePrimaryKeyColumn(def); ok {
			return []string{columnName}, true
		}
	}
	return nil, false
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func missingTableDiff(table tidbTableSpec) tidbSchemaDiff {
	detail := fmt.Sprintf("%s schema contract: missing table", table.name)
	if table.createStatement == "" {
		detail = fmt.Sprintf("%s schema contract: missing table and no repair statement available", table.name)
	}
	return tidbSchemaDiff{
		kind:      tidbSchemaDiffMissingTable,
		tableName: table.name,
		detail:    detail,
		repairSQL: table.createStatement,
	}
}

func missingTableAndIndexDiffs(table tidbTableSpec) []tidbSchemaDiff {
	diffs := []tidbSchemaDiff{missingTableDiff(table)}
	for _, name := range sortedIndexNames(table.indexes) {
		spec := table.indexes[name]
		diffs = append(diffs, tidbSchemaDiff{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: table.name,
			detail:    fmt.Sprintf("%s schema contract: missing %s index", table.name, name),
			repairSQL: spec.createSQL,
		})
	}
	return diffs
}

func plannedTiDBSchemaRepairs(diffs []tidbSchemaDiff) []string {
	seen := make(map[string]struct{})
	plans := make([]string, 0, len(diffs))
	for _, diff := range diffs {
		if diff.repairSQL == "" {
			continue
		}
		if !isSafeTiDBRepairDiff(diff) {
			continue
		}
		if _, ok := seen[diff.repairSQL]; ok {
			continue
		}
		seen[diff.repairSQL] = struct{}{}
		plans = append(plans, diff.repairSQL)
	}
	return plans
}

func isSafeTiDBRepairDiff(diff tidbSchemaDiff) bool {
	switch diff.kind {
	case tidbSchemaDiffMissingTable:
		return true
	case tidbSchemaDiffMissingColumn:
		return isSafeAddColumnRepairSQL(diff.repairSQL)
	case tidbSchemaDiffMissingIndex:
		return isSafeAddIndexRepairSQL(diff.repairSQL)
	default:
		return false
	}
}

func isSafeAddColumnRepairSQL(sqlText string) bool {
	// STORED GENERATED VECTOR columns whose expression uses EMBED_TEXT are
	// safe to add to existing tables via ALTER TABLE in the correctness sense:
	// TiDB computes the values server-side rather than requiring the client to
	// backfill. Note that this still materializes one EMBED_TEXT call per
	// existing row at ALTER time, which can be slow and carry inference cost
	// for large tables. This covers the description_embedding column introduced
	// for auto-embedding mode.
	normalized := normalizeSQLFragment(sqlText)
	if strings.Contains(normalized, " generated ") &&
		strings.Contains(normalized, " stored") &&
		strings.Contains(normalized, " vector(") &&
		strings.Contains(normalized, "embed_text(") {
		return true
	}
	return schemaspec.IsSafeAddColumnRepairSQL(sqlText)
}

func isSafeAddIndexRepairSQL(sqlText string) bool {
	normalized := normalizeSQLFragment(sqlText)
	if strings.HasPrefix(normalized, "create index ") {
		return true
	}
	if strings.HasPrefix(normalized, "create unique index ") {
		return true
	}
	if strings.HasPrefix(normalized, "alter table ") {
		if strings.Contains(normalized, " add unique index ") || strings.Contains(normalized, " add unique key ") {
			return true
		}
		if strings.Contains(normalized, " add fulltext index ") || strings.Contains(normalized, " add vector index ") {
			// FULLTEXT and VECTOR indexes are always safe to add on an existing
			// table in auto-embedding mode: TiDB Cloud supports the syntax, and
			// applyTiDBSchemaRepairs will gracefully skip with a warning if the
			// current TiDB version does not.
			return true
		}
		if strings.Contains(normalized, " add index ") || strings.Contains(normalized, " add key ") {
			return true
		}
	}
	return false
}

func applyTiDBSchemaRepairs(ctx context.Context, db *sql.DB, statements []string) error {
	if len(statements) == 0 {
		return nil
	}
	for _, stmt := range statements {
		if isUniqueIndexRepairSQL(stmt) {
			repair, ok := parseUniqueIndexRepairStatement(stmt)
			if !ok {
				return fmt.Errorf("preflight unique index repair %q: unsupported statement", schemaStatementSnippet(stmt))
			}
			if err := ensureUniqueIndexRepairSafe(ctx, db, repair); err != nil {
				return err
			}
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if isIgnorableTiDBSchemaError(err) {
				continue
			}
			// FULLTEXT and VECTOR index repairs may fail with optional-feature
			// errors on TiDB versions or configurations that do not support
			// them (e.g. 8200: FULLTEXT index must specify one column name,
			// 1105: FULLTEXT index is not supported). Treat these the same as
			// when the statement was skipped during initial provisioning.
			if isFulltextOrVectorIndexRepairSQL(stmt) && isIgnorableOptionalSchemaError(err) {
				logger.Warn(ctx, "tidb_schema_repair_optional_index_skipped",
					zap.String("statement", schemaStatementSnippet(stmt)),
					zap.Error(err))
				continue
			}
			return fmt.Errorf("apply tidb schema repair %q: %w", schemaStatementSnippet(stmt), err)
		}
	}
	return nil
}

func isUniqueIndexRepairSQL(sqlText string) bool {
	normalized := normalizeSQLFragment(sqlText)
	if strings.HasPrefix(normalized, "create unique index ") {
		return true
	}
	return strings.HasPrefix(normalized, "alter table ") &&
		(strings.Contains(normalized, " add unique index ") || strings.Contains(normalized, " add unique key "))
}

func isFulltextOrVectorIndexRepairSQL(sqlText string) bool {
	normalized := normalizeSQLFragment(sqlText)
	return strings.Contains(normalized, " add fulltext index ") ||
		strings.Contains(normalized, " add vector index ")
}

func parseUniqueIndexRepairStatement(stmt string) (tidbUniqueIndexRepair, bool) {
	if tableName, indexName, columns, ok := parseCreateUniqueIndexRepairStatement(stmt); ok {
		return tidbUniqueIndexRepair{tableName: tableName, indexName: indexName, columns: columns}, true
	}
	if tableName, indexName, columns, ok := parseAlterTableAddUniqueIndexRepairStatement(stmt); ok {
		return tidbUniqueIndexRepair{tableName: tableName, indexName: indexName, columns: columns}, true
	}
	return tidbUniqueIndexRepair{}, false
}

func parseCreateUniqueIndexRepairStatement(stmt string) (tableName, indexName string, columns []string, ok bool) {
	trimmed := strings.TrimSpace(stmt)
	upper := strings.ToUpper(trimmed)
	const prefix = "CREATE UNIQUE INDEX "
	if !strings.HasPrefix(upper, prefix) {
		return "", "", nil, false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	name, remainder := splitIdentifierAndSuffix(rest)
	if name == "" || remainder == "" {
		return "", "", nil, false
	}
	remainder = strings.TrimSpace(remainder)
	upperRemainder := strings.ToUpper(remainder)
	if !strings.HasPrefix(upperRemainder, "ON ") {
		return "", "", nil, false
	}
	afterOn := strings.TrimSpace(remainder[len("ON "):])
	table, columnRemainder := splitIdentifierAndSuffix(afterOn)
	if table == "" || columnRemainder == "" {
		return "", "", nil, false
	}
	parsedColumns := parseIndexColumnList(columnRemainder)
	if len(parsedColumns) == 0 {
		return "", "", nil, false
	}
	return strings.ToLower(table), strings.ToLower(name), parsedColumns, true
}

func parseAlterTableAddUniqueIndexRepairStatement(stmt string) (tableName, indexName string, columns []string, ok bool) {
	trimmed := strings.TrimSpace(stmt)
	upper := strings.ToUpper(trimmed)
	const prefix = "ALTER TABLE "
	if !strings.HasPrefix(upper, prefix) {
		return "", "", nil, false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	table, remainder := splitIdentifierAndRest(rest)
	if table == "" || remainder == "" {
		return "", "", nil, false
	}
	remainder = strings.TrimSpace(remainder)
	upperRemainder := strings.ToUpper(remainder)
	for _, marker := range []string{"ADD UNIQUE INDEX ", "ADD UNIQUE KEY "} {
		if !strings.HasPrefix(upperRemainder, marker) {
			continue
		}
		afterMarker := strings.TrimSpace(remainder[len(marker):])
		name, columnRemainder := splitIdentifierAndSuffix(afterMarker)
		if name == "" || columnRemainder == "" {
			return "", "", nil, false
		}
		parsedColumns := parseIndexColumnList(columnRemainder)
		if len(parsedColumns) == 0 {
			return "", "", nil, false
		}
		return strings.ToLower(table), strings.ToLower(name), parsedColumns, true
	}
	return "", "", nil, false
}

func ensureUniqueIndexRepairSafe(ctx context.Context, db *sql.DB, repair tidbUniqueIndexRepair) error {
	var exists int
	err := db.QueryRowContext(ctx, buildUniqueIndexDuplicateCheckSQL(repair)).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("preflight unique index repair %s on %s: %w", repair.indexName, repair.tableName, err)
	}
	return fmt.Errorf("cannot auto-repair unique index %s on %s: duplicate rows exist for columns (%s)", repair.indexName, repair.tableName, strings.Join(repair.columns, ", "))
}

func buildUniqueIndexDuplicateCheckSQL(repair tidbUniqueIndexRepair) string {
	quotedColumns := make([]string, 0, len(repair.columns))
	nonNullPredicates := make([]string, 0, len(repair.columns))
	for _, column := range repair.columns {
		quoted := quoteSQLIdentifier(column)
		quotedColumns = append(quotedColumns, quoted)
		nonNullPredicates = append(nonNullPredicates, quoted+" IS NOT NULL")
	}
	return fmt.Sprintf("SELECT 1 FROM %s WHERE %s GROUP BY %s HAVING COUNT(*) > 1 LIMIT 1",
		quoteSQLIdentifier(repair.tableName),
		strings.Join(nonNullPredicates, " AND "),
		strings.Join(quotedColumns, ", "))
}

func quoteSQLIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func isIgnorableTiDBSchemaError(err error) bool {
	return schemaspec.IsIgnorableMySQLError(err)
}

func validateTiDBAutoEmbeddingFilesDiffs(meta tidbTableMeta) []tidbSchemaDiff {
	var diffs []tidbSchemaDiff
	for _, spec := range []struct {
		column string
		source string
	}{
		{"embedding", "content_text"},
		{"description_embedding", "description"},
	} {
		col, err := meta.requireColumn(spec.column)
		if err != nil {
			return nil
		}
		extra := normalizeSQLFragment(col.extra)
		if !strings.Contains(extra, "generated") || !strings.Contains(extra, "stored") {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffTableContract,
				tableName:  "files",
				columnName: spec.column,
				detail:     fmt.Sprintf("files schema contract: %s column must be a stored generated column", spec.column),
			})
		}
		expr := normalizeSQLFragment(col.generationExpression)
		checks := []struct {
			pattern string
			errMsg  string
		}{
			{"embed_text(", fmt.Sprintf("files schema contract: %s generated expression must use EMBED_TEXT", spec.column)},
			{tidbAutoEmbeddingModel, fmt.Sprintf("files schema contract: %s model contract mismatch", spec.column)},
			{spec.source, fmt.Sprintf("files schema contract: generated expression must derive from %s", spec.source)},
			{tidbAutoEmbeddingOptionsJSON, fmt.Sprintf("files schema contract: %s dimensions option mismatch", spec.column)},
		}
		for _, check := range checks {
			if !strings.Contains(expr, check.pattern) {
				diffs = append(diffs, tidbSchemaDiff{
					kind:       tidbSchemaDiffTableContract,
					tableName:  "files",
					columnName: spec.column,
					detail:     check.errMsg,
				})
			}
		}
	}
	return diffs
}

func validateTiDBAppEmbeddingFilesDiffs(meta tidbTableMeta) []tidbSchemaDiff {
	var diffs []tidbSchemaDiff
	for _, colName := range []string{"embedding", "description_embedding"} {
		col, err := meta.requireColumn(colName)
		if err != nil {
			return nil
		}
		extra := normalizeSQLFragment(col.extra)
		if strings.Contains(extra, "generated") {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffTableContract,
				tableName:  "files",
				columnName: colName,
				detail:     fmt.Sprintf("files schema contract: %s column must be writable in app mode", colName),
			})
		}
		if expr := normalizeSQLFragment(col.generationExpression); expr != "" {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffTableContract,
				tableName:  "files",
				columnName: colName,
				detail:     fmt.Sprintf("files schema contract: %s column must not define a generation expression in app mode", colName),
			})
		}
	}
	return diffs
}

func schemaDiffsToError(diffs []tidbSchemaDiff) error {
	if len(diffs) == 0 {
		return nil
	}
	return errors.New(diffs[0].detail)
}

func sortedColumnNames(columns map[string]tidbColumnSpec) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedIndexNames(indexes map[string]tidbIndexSpec) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
