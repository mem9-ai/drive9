package tenant

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	tidbAutoEmbeddingDimensions = 1024
)

var tidbAutoEmbeddingOptionsJSON = fmt.Sprintf(`{"dimensions":%d}`, tidbAutoEmbeddingDimensions)

type tidbColumnMeta struct {
	columnType           string
	extra                string
	generationExpression string
}

type tidbTableMeta struct {
	tableName string
	columns   map[string]tidbColumnMeta
}

func tidbAutoEmbeddingSchemaStatements() []string {
	return []string{
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
			embedding          VECTOR(` + strconv.Itoa(tidbAutoEmbeddingDimensions) + `) GENERATED ALWAYS AS (EMBED_TEXT(
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
			status             VARCHAR(32) NOT NULL DEFAULT 'UPLOADING',
			fingerprint_sha256 VARCHAR(128),
			idempotency_key    VARCHAR(255),
			created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			expires_at         DATETIME(3) NOT NULL,
			active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED
		)`,
		`CREATE INDEX idx_upload_path ON uploads(target_path, status)`,
		`CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)`,
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
	}
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
	if !isTiDBCluster(db) {
		return TiDBEmbeddingModeUnknown, fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	loadStart := time.Now()
	filesMeta, err := loadTiDBTableMeta(db, "files")
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
//
// The `mode` argument is a runtime behavior contract, not a cosmetic schema tag:
//
//   - `TiDBEmbeddingModeAuto` requires database-managed embedding, where
//     `files.embedding` is a stored generated column derived from `content_text`
//     via `EMBED_TEXT(...)`.
//   - `TiDBEmbeddingModeApp` requires application-managed embedding, where
//     `files.embedding` remains writable for the app-side embed worker and
//     query-embedding path.
//
// The validator is intentionally strict: if the schema does not clearly satisfy
// the requested mode, it returns an error rather than guessing or falling back
// to the other mode.
//
// At the moment this contract is intentionally scoped to the tables that drive
// embedding-mode behavior directly: `files` and `semantic_tasks`. Other TiDB
// tables are currently outside this mode validator.
//
// Within that scope, `files` is validated structurally for the requested mode,
// while `semantic_tasks` is currently checked only for table existence.
func ValidateTiDBSchemaForMode(db *sql.DB, mode TiDBEmbeddingMode) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !isTiDBCluster(db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := validateTiDBSchemaMode(mode); err != nil {
		return err
	}
	filesMeta, err := loadTiDBTableMeta(db, "files")
	if err != nil {
		return fmt.Errorf("load files table metadata: %w", err)
	}
	if err := validateTiDBTableForMode(mode, filesMeta); err != nil {
		return fmt.Errorf("files schema contract: %w", err)
	}
	if err := ensureTiDBTableExists(db, "semantic_tasks"); err != nil {
		return fmt.Errorf("semantic_tasks schema contract: %w", err)
	}
	return nil
}

func validateTiDBSchemaMode(mode TiDBEmbeddingMode) error {
	if mode != TiDBEmbeddingModeAuto && mode != TiDBEmbeddingModeApp {
		return fmt.Errorf("unsupported TiDB embedding mode %q", mode)
	}
	return nil
}

func initTiDBAutoEmbeddingSchema(dsn string) error {
	db, err := openTiDBSchemaDB(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if !isTiDBCluster(db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := execSchemaStatements(db, tidbAutoEmbeddingSchemaStatements()); err != nil {
		return err
	}
	return ValidateTiDBSchemaForMode(db, TiDBEmbeddingModeAuto)
}

func validateTiDBSchemaForModeDSN(dsn string, mode TiDBEmbeddingMode) error {
	db, err := openTiDBSchemaDB(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return ValidateTiDBSchemaForMode(db, mode)
}

func openTiDBSchemaDB(dsn string) (*sql.DB, error) {
	if hasMultiStatements(dsn) {
		return nil, fmt.Errorf("multiStatements is not allowed")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func validateTiDBTableForMode(mode TiDBEmbeddingMode, meta tidbTableMeta) error {
	switch meta.tableName {
	case "files":
		switch mode {
		case TiDBEmbeddingModeAuto:
			return validateTiDBAutoEmbeddingFilesTable(meta)
		case TiDBEmbeddingModeApp:
			return validateTiDBAppEmbeddingFilesTable(meta)
		default:
			return fmt.Errorf("unsupported TiDB embedding mode %q", mode)
		}
	default:
		return fmt.Errorf("unsupported table %q", meta.tableName)
	}
}

func detectTiDBEmbeddingModeFromFilesMeta(meta tidbTableMeta) (TiDBEmbeddingMode, error) {
	col, err := meta.requireColumn("embedding")
	if err != nil {
		return TiDBEmbeddingModeUnknown, err
	}
	if normalizeSQLFragment(col.columnType) != fmt.Sprintf("vector(%d)", tidbAutoEmbeddingDimensions) {
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
	col, err := meta.requireColumn("embedding")
	if err != nil {
		return err
	}
	extra := normalizeSQLFragment(col.extra)
	if !strings.Contains(extra, "generated") || !strings.Contains(extra, "stored") {
		return errors.New("embedding column must be a stored generated column")
	}
	expr := normalizeSQLFragment(col.generationExpression)
	checks := []struct {
		pattern string
		errMsg  string
	}{
		{"embed_text(", "embedding generated expression must use EMBED_TEXT"},
		{tidbAutoEmbeddingModel, "embedding model contract mismatch"},
		{"content_text", "generated expression must derive from content_text"},
		{tidbAutoEmbeddingOptionsJSON, "embedding dimensions option mismatch"},
	}
	for _, check := range checks {
		if !strings.Contains(expr, check.pattern) {
			return errors.New(check.errMsg)
		}
	}
	return nil
}

func validateTiDBAppEmbeddingFilesTable(meta tidbTableMeta) error {
	if err := validateTiDBFilesTableBase(meta); err != nil {
		return err
	}
	col, err := meta.requireColumn("embedding")
	if err != nil {
		return err
	}
	extra := normalizeSQLFragment(col.extra)
	if strings.Contains(extra, "generated") {
		return errors.New("embedding column must be writable in app mode")
	}
	if expr := normalizeSQLFragment(col.generationExpression); expr != "" {
		return errors.New("embedding column must not define a generation expression in app mode")
	}
	return nil
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
	if err := meta.requireColumnType("embedding", fmt.Sprintf("vector(%d)", tidbAutoEmbeddingDimensions)); err != nil {
		return err
	}
	return meta.requireColumnType("embedding_revision", "bigint")
}

func loadTiDBTableMeta(db *sql.DB, tableName string) (tidbTableMeta, error) {
	columns, err := loadTiDBColumnMeta(db, tableName)
	if err != nil {
		return tidbTableMeta{}, fmt.Errorf("load columns: %w", err)
	}
	return tidbTableMeta{tableName: tableName, columns: columns}, nil
}

func ensureTiDBTableExists(db *sql.DB, tableName string) error {
	if _, err := loadShowCreateTable(db, tableName); err != nil {
		return fmt.Errorf("show create table: %w", err)
	}
	return nil
}

func loadTiDBColumnMeta(db *sql.DB, tableName string) (map[string]tidbColumnMeta, error) {
	rows, err := db.Query(`SELECT column_name, column_type, extra, generation_expression
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
	if normalizeSQLFragment(col.columnType) != normalizeSQLFragment(want) {
		return fmt.Errorf("%s column type = %q, want %s", name, col.columnType, want)
	}
	return nil
}

func loadShowCreateTable(db *sql.DB, tableName string) (string, error) {
	var gotTable string
	var createStmt string
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	if err := db.QueryRow(query).Scan(&gotTable, &createStmt); err != nil {
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
