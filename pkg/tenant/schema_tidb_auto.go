package tenant

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Reference of Auto Embedding in TiDB Cloud: https://docs.pingcap.com/ai/vector-search-auto-embedding-amazon-titan/
const (
	tidbAutoEmbeddingModel      = "tidbcloud_free/amazon/titan-embed-text-v2"
	tidbAutoEmbeddingDimensions = 1024
)

var tidbAutoEmbeddingOptionsJSON = fmt.Sprintf(`{"dimensions":%d}`, tidbAutoEmbeddingDimensions)

func tidbAutoEmbeddingSchemaStatements(withContentBlob bool) []string {
	contentBlobCol := ""
	if withContentBlob {
		contentBlobCol = "content_blob       LONGBLOB,"
	}
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
			` + contentBlobCol + `
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
	}
}

// ValidateTiDBAutoEmbeddingSchema checks that an already-open TiDB connection
// exposes the launch-time auto-embedding contract required by TiDB tenants.
func ValidateTiDBAutoEmbeddingSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !isTiDBCluster(db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	for _, table := range []string{"file_nodes", "files", "file_tags", "uploads", "semantic_tasks"} {
		createStmt, err := loadShowCreateTable(db, table)
		if err != nil {
			return fmt.Errorf("show create table %s: %w", table, err)
		}
		if err := validateTiDBAutoEmbeddingTableDDL(table, createStmt); err != nil {
			return fmt.Errorf("%s schema contract: %w", table, err)
		}
	}
	return nil
}

func initTiDBAutoEmbeddingSchema(dsn string, withContentBlob bool) error {
	db, err := openTiDBSchemaDB(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if !isTiDBCluster(db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := execSchemaStatements(db, tidbAutoEmbeddingSchemaStatements(withContentBlob)); err != nil {
		return err
	}
	return ValidateTiDBAutoEmbeddingSchema(db)
}

func validateTiDBAutoEmbeddingSchemaDSN(dsn string) error {
	db, err := openTiDBSchemaDB(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return ValidateTiDBAutoEmbeddingSchema(db)
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

func validateTiDBAutoEmbeddingFilesDDL(createStmt string) error {
	norm := normalizeDDL(createStmt)
	checks := []struct {
		pattern string
		errMsg  string
	}{
		{"embedding vector(" + strconv.Itoa(tidbAutoEmbeddingDimensions) + ") generated always as", "missing generated embedding column"},
		{"embed_text(", "missing EMBED_TEXT generated expression"},
		{tidbAutoEmbeddingModel, "embedding model contract mismatch"},
		{"content_text", "generated expression must derive from content_text"},
		{tidbAutoEmbeddingOptionsJSON, "embedding dimensions option mismatch"},
		{"embedding_revision bigint", "embedding_revision compatibility column missing"},
		{"idx_fts_content", "missing fulltext index idx_fts_content"},
		{"with parser multilingual", "fulltext index must use multilingual parser"},
		{"idx_files_cosine", "missing vector index idx_files_cosine"},
		{"vec_cosine_distance(embedding)", "vector index must target embedding cosine distance"},
	}
	for _, check := range checks {
		if !strings.Contains(norm, check.pattern) {
			return errors.New(check.errMsg)
		}
	}
	return nil
}

func validateTiDBAutoEmbeddingTableDDL(tableName, createStmt string) error {
	switch tableName {
	case "file_nodes":
		return validateTiDBAutoEmbeddingFileNodesDDL(createStmt)
	case "files":
		return validateTiDBAutoEmbeddingFilesDDL(createStmt)
	case "file_tags":
		return validateTiDBAutoEmbeddingFileTagsDDL(createStmt)
	case "uploads":
		return validateTiDBAutoEmbeddingUploadsDDL(createStmt)
	case "semantic_tasks":
		return validateTiDBAutoEmbeddingSemanticTasksDDL(createStmt)
	default:
		return fmt.Errorf("unsupported table %q", tableName)
	}
}

func validateTiDBAutoEmbeddingFileNodesDDL(createStmt string) error {
	return validateDDLContains(createStmt,
		ddlCheck{pattern: "node_id varchar(64)", errMsg: "missing node_id column"},
		ddlCheck{pattern: "path varchar(512)", errMsg: "missing path column"},
		ddlCheck{pattern: "parent_path varchar(512)", errMsg: "missing parent_path column"},
		ddlCheck{pattern: "file_id varchar(64)", errMsg: "missing file_id column"},
		ddlCheck{pattern: "idx_path", errMsg: "missing unique index idx_path"},
		ddlCheck{pattern: "idx_parent", errMsg: "missing index idx_parent"},
		ddlCheck{pattern: "idx_file_id", errMsg: "missing index idx_file_id"},
	)
}

func validateTiDBAutoEmbeddingFileTagsDDL(createStmt string) error {
	return validateDDLContains(createStmt,
		ddlCheck{pattern: "primary key (file_id,tag_key)", errMsg: "missing file_tags primary key"},
		ddlCheck{pattern: "tag_key varchar(255)", errMsg: "missing tag_key column"},
		ddlCheck{pattern: "tag_value varchar(255)", errMsg: "missing tag_value column"},
		ddlCheck{pattern: "idx_kv", errMsg: "missing index idx_kv"},
	)
}

func validateTiDBAutoEmbeddingUploadsDDL(createStmt string) error {
	return validateDDLContains(createStmt,
		ddlCheck{pattern: "upload_id varchar(64)", errMsg: "missing upload_id column"},
		ddlCheck{pattern: "target_path varchar(512)", errMsg: "missing target_path column"},
		ddlCheck{pattern: "status varchar(32)", errMsg: "missing status column"},
		ddlCheck{pattern: "active_target_path varchar(512) as (case when status = 'uploading' then target_path else null end) stored", errMsg: "missing active_target_path generated column"},
		ddlCheck{pattern: "idx_upload_path", errMsg: "missing index idx_upload_path"},
		ddlCheck{pattern: "idx_idempotency", errMsg: "missing unique index idx_idempotency"},
	)
}

func validateTiDBAutoEmbeddingSemanticTasksDDL(createStmt string) error {
	return validateDDLContains(createStmt,
		ddlCheck{pattern: "task_id varchar(64)", errMsg: "missing task_id column"},
		ddlCheck{pattern: "task_type varchar(32)", errMsg: "missing task_type column"},
		ddlCheck{pattern: "resource_id varchar(64)", errMsg: "missing resource_id column"},
		ddlCheck{pattern: "resource_version bigint", errMsg: "missing resource_version column"},
		ddlCheck{pattern: "status varchar(20)", errMsg: "missing status column"},
		ddlCheck{pattern: "uk_task_resource_version", errMsg: "missing unique index uk_task_resource_version"},
		ddlCheck{pattern: "idx_task_claim", errMsg: "missing index idx_task_claim"},
	)
}

type ddlCheck struct {
	pattern string
	errMsg  string
}

func validateDDLContains(createStmt string, checks ...ddlCheck) error {
	norm := normalizeDDL(createStmt)
	for _, check := range checks {
		if !strings.Contains(norm, check.pattern) {
			return errors.New(check.errMsg)
		}
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

func normalizeDDL(s string) string {
	s = strings.ToLower(strings.ReplaceAll(s, "`", ""))
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}
