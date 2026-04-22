package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
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
var CurrentTiDBTenantSchemaVersion = func() int {
	stmts := tidbAutoEmbeddingSchemaStatements()
	// Hash only statements that are parsed into the schema spec (CREATE TABLE,
	// CREATE [UNIQUE] INDEX, ALTER TABLE ... ADD ... INDEX).  Statements that
	// fall into none of these categories (e.g. SET, comments) do not affect
	// what ValidateTiDBSchemaForMode checks, so including them would cause
	// unnecessary re-Ensures on unrelated edits.
	//
	// int(h) is safe: uint32 → int on a 64-bit platform is always non-negative,
	// unlike int(int32(h)) which sign-extends for values with bit 31 set.
	var specStmts []string
	for _, stmt := range stmts {
		n := normalizeSQLFragment(stmt)
		if strings.HasPrefix(n, "create table ") ||
			strings.HasPrefix(n, "create index ") ||
			strings.HasPrefix(n, "create unique index ") ||
			(strings.HasPrefix(n, "alter table ") && strings.Contains(n, " add ") && strings.Contains(n, " index ")) {
			specStmts = append(specStmts, stmt)
		}
	}
	h := crc32.ChecksumIEEE([]byte(strings.Join(specStmts, "\n")))
	return int(h)
}()

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
	validate        func(tidbTableMeta) []tidbSchemaDiff
}

type tidbColumnSpec struct {
	columnType string
	addSQL     string
}

type tidbIndexSpec struct {
	createSQL string
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
			embedding          VECTOR(` + strconv.Itoa(TiDBAutoEmbeddingDimensions) + `) GENERATED ALWAYS AS (EMBED_TEXT(
				'` + tidbAutoEmbeddingModel + `',
				content_text,
				'` + tidbAutoEmbeddingOptionsJSON + `'
			)) STORED,
			embedding_revision BIGINT,
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
			ADD VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
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
	return meta.requireColumnType("embedding_revision", "bigint")
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

func legacyTiDBUploadsRepairStatements(meta tidbTableMeta) []string {
	if _, ok := meta.columns["expected_revision"]; ok {
		return nil
	}
	return []string{`ALTER TABLE uploads ADD COLUMN expected_revision BIGINT NULL`}
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

func normalizeSQLFragment(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "`", "")
	s = strings.ReplaceAll(s, "_utf8mb4", "")
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
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
	return diffTiDBTableMeta(table, meta, createStmt), nil
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
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
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
	}
	return tidbTableSpec{
		name:            tableName,
		createStatement: strings.TrimSpace(stmt),
		columns:         columns,
		indexes:         indexes,
	}, true, nil
}

func parseCreateTableStatement(stmt string) (tableName string, definitions string, ok bool, err error) {
	lower := strings.ToLower(stmt)
	const prefix = "create table if not exists"
	start := strings.Index(lower, prefix)
	if start < 0 {
		return "", "", false, nil
	}
	i := start + len(prefix)
	for i < len(stmt) && (stmt[i] == ' ' || stmt[i] == '\n' || stmt[i] == '\t' || stmt[i] == '\r') {
		i++
	}
	if i >= len(stmt) {
		return "", "", false, fmt.Errorf("parse create table: missing table name")
	}
	nameStart := i
	if stmt[i] == '`' {
		i++
		for i < len(stmt) && stmt[i] != '`' {
			i++
		}
		if i >= len(stmt) {
			return "", "", false, fmt.Errorf("parse create table: unterminated quoted table name")
		}
		tableName = strings.ToLower(stmt[nameStart+1 : i])
		i++
	} else {
		for i < len(stmt) && stmt[i] != ' ' && stmt[i] != '\n' && stmt[i] != '\t' && stmt[i] != '(' {
			i++
		}
		tableName = strings.ToLower(strings.TrimSpace(stmt[nameStart:i]))
	}
	for i < len(stmt) && stmt[i] != '(' {
		i++
	}
	if i >= len(stmt) || stmt[i] != '(' {
		return "", "", false, fmt.Errorf("parse create table %s: missing opening parenthesis", tableName)
	}
	bodyStart := i + 1
	depth := 1
	inSingle := false
	inDouble := false
	inBacktick := false
	for i = bodyStart; i < len(stmt); i++ {
		ch := stmt[i]
		switch ch {
		case '\\':
			i++
			continue
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick {
				depth--
				if depth == 0 {
					return tableName, stmt[bodyStart:i], true, nil
				}
			}
		}
	}
	return "", "", false, fmt.Errorf("parse create table %s: unbalanced parentheses", tableName)
}

func splitTopLevelComma(definitions string) []string {
	parts := make([]string, 0)
	start := 0
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(definitions); i++ {
		ch := definitions[i]
		switch ch {
		case '\\':
			i++
			continue
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick && depth > 0 {
				depth--
			}
		case ',':
			if !inSingle && !inDouble && !inBacktick && depth == 0 {
				parts = append(parts, strings.TrimSpace(definitions[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(definitions) {
		parts = append(parts, strings.TrimSpace(definitions[start:]))
	}
	return parts
}

func parseInlineIndexDefinition(tableName, def string) (indexName, createSQL string, ok bool) {
	normalized := normalizeSQLFragment(def)
	if strings.HasPrefix(normalized, "unique index ") {
		name, cols := parseIndexNameAndColumns(def, "UNIQUE INDEX")
		if name == "" || cols == "" {
			return "", "", false
		}
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
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return ""
	}
	keywords := []string{" not ", " null", " default ", " generated ", " as ", " primary ", " unique ", " comment ", " references ", " auto_increment", " on update"}
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(rest); i++ {
		ch := rest[i]
		switch ch {
		case '\\':
			i++
			continue
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick && depth > 0 {
				depth--
			}
		}
		if inSingle || inDouble || inBacktick || depth > 0 {
			continue
		}
		suffix := " " + strings.ToLower(rest[i:])
		for _, kw := range keywords {
			if strings.HasPrefix(suffix, kw) {
				return strings.TrimSpace(rest[:i])
			}
		}
	}
	return strings.TrimSpace(rest)
}

func splitIdentifierAndRest(s string) (identifier string, rest string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	if s[0] == '`' {
		end := strings.Index(s[1:], "`")
		if end < 0 {
			return "", ""
		}
		id := s[1 : 1+end]
		return id, strings.TrimSpace(s[1+end+1:])
	}
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' {
			return s[:i], strings.TrimSpace(s[i+1:])
		}
	}
	return s, ""
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

	markers := []string{" add fulltext index ", " add vector index ", " add unique index ", " add index ", " add key "}
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
	normalizedCreate := normalizeSQLFragment(createStmt)
	for _, name := range sortedIndexNames(table.indexes) {
		spec := table.indexes[name]
		if !showCreateHasIndex(normalizedCreate, name) {
			diffs = append(diffs, tidbSchemaDiff{
				kind:      tidbSchemaDiffMissingIndex,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: missing %s index", table.name, name),
				repairSQL: spec.createSQL,
			})
		}
	}
	if table.validate != nil {
		diffs = append(diffs, table.validate(meta)...)
	}
	return diffs
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
	tableMissing := make(map[string]bool)
	for _, diff := range diffs {
		if diff.kind == tidbSchemaDiffMissingTable {
			tableMissing[diff.tableName] = true
		}
	}

	seen := make(map[string]struct{})
	plans := make([]string, 0, len(diffs))
	for _, diff := range diffs {
		if diff.repairSQL == "" {
			continue
		}
		if !isSafeTiDBRepairDiff(diff, tableMissing) {
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

func isSafeTiDBRepairDiff(diff tidbSchemaDiff, tableMissing map[string]bool) bool {
	switch diff.kind {
	case tidbSchemaDiffMissingTable:
		return true
	case tidbSchemaDiffMissingColumn:
		return isSafeAddColumnRepairSQL(diff.repairSQL)
	case tidbSchemaDiffMissingIndex:
		return isSafeAddIndexRepairSQL(diff.repairSQL, tableMissing[diff.tableName])
	default:
		return false
	}
}

func isSafeAddColumnRepairSQL(sqlText string) bool {
	n := normalizeSQLFragment(sqlText)
	if !strings.HasPrefix(n, "alter table ") || !strings.Contains(n, " add column ") {
		return false
	}
	if isGeneratedColumnAddSQL(n) {
		return false
	}
	if strings.Contains(n, " not null") && !strings.Contains(n, " default ") {
		return false
	}
	return true
}

func isGeneratedColumnAddSQL(normalizedSQL string) bool {
	if strings.Contains(normalizedSQL, " generated ") {
		return true
	}
	hasAsExpr := strings.Contains(normalizedSQL, " as (")
	if !hasAsExpr {
		return false
	}
	return strings.Contains(normalizedSQL, " stored") || strings.Contains(normalizedSQL, " virtual")
}

func isSafeAddIndexRepairSQL(sqlText string, tableMissing bool) bool {
	normalized := normalizeSQLFragment(sqlText)
	if strings.HasPrefix(normalized, "create index ") {
		return true
	}
	if strings.HasPrefix(normalized, "create unique index ") {
		return tableMissing
	}
	if strings.HasPrefix(normalized, "alter table ") {
		if strings.Contains(normalized, " add unique index ") || strings.Contains(normalized, " add unique key ") {
			return tableMissing
		}
		if strings.Contains(normalized, " add fulltext index ") || strings.Contains(normalized, " add vector index ") || strings.Contains(normalized, " add index ") || strings.Contains(normalized, " add key ") {
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
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if isIgnorableTiDBSchemaError(err) {
				continue
			}
			return fmt.Errorf("apply tidb schema repair %q: %w", schemaStatementSnippet(stmt), err)
		}
	}
	return nil
}

func isIgnorableTiDBSchemaError(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1050, 1060, 1061:
			return true
		}
		msg := strings.ToLower(me.Message)
		if strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate") {
			return true
		}
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate")
}

func validateTiDBAutoEmbeddingFilesDiffs(meta tidbTableMeta) []tidbSchemaDiff {
	col, err := meta.requireColumn("embedding")
	if err != nil {
		return nil
	}
	var diffs []tidbSchemaDiff
	extra := normalizeSQLFragment(col.extra)
	if !strings.Contains(extra, "generated") || !strings.Contains(extra, "stored") {
		diffs = append(diffs, tidbSchemaDiff{
			kind:       tidbSchemaDiffTableContract,
			tableName:  "files",
			columnName: "embedding",
			detail:     "files schema contract: embedding column must be a stored generated column",
		})
	}
	expr := normalizeSQLFragment(col.generationExpression)
	checks := []struct {
		pattern string
		errMsg  string
	}{
		{"embed_text(", "files schema contract: embedding generated expression must use EMBED_TEXT"},
		{tidbAutoEmbeddingModel, "files schema contract: embedding model contract mismatch"},
		{"content_text", "files schema contract: generated expression must derive from content_text"},
		{tidbAutoEmbeddingOptionsJSON, "files schema contract: embedding dimensions option mismatch"},
	}
	for _, check := range checks {
		if !strings.Contains(expr, check.pattern) {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffTableContract,
				tableName:  "files",
				columnName: "embedding",
				detail:     check.errMsg,
			})
		}
	}
	return diffs
}

func validateTiDBAppEmbeddingFilesDiffs(meta tidbTableMeta) []tidbSchemaDiff {
	col, err := meta.requireColumn("embedding")
	if err != nil {
		return nil
	}
	var diffs []tidbSchemaDiff
	extra := normalizeSQLFragment(col.extra)
	if strings.Contains(extra, "generated") {
		diffs = append(diffs, tidbSchemaDiff{
			kind:       tidbSchemaDiffTableContract,
			tableName:  "files",
			columnName: "embedding",
			detail:     "files schema contract: embedding column must be writable in app mode",
		})
	}
	if expr := normalizeSQLFragment(col.generationExpression); expr != "" {
		diffs = append(diffs, tidbSchemaDiff{
			kind:       tidbSchemaDiffTableContract,
			tableName:  "files",
			columnName: "embedding",
			detail:     "files schema contract: embedding column must not define a generation expression in app mode",
		})
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

func showCreateHasIndex(normalizedCreate, indexName string) bool {
	needle := strings.ToLower(indexName) + " ("
	return strings.Contains(normalizedCreate, needle)
}
