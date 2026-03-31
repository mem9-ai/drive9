package tenant

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	tidbAutoEmbeddingModel      = "openai/text-embedding-3-small"
	tidbAutoEmbeddingDimensions = 1024
)

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
			embedding          VECTOR(1024) GENERATED ALWAYS AS (EMBED_TEXT(
				'openai/text-embedding-3-small',
				content_text,
				'{"dimensions":1024}'
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
	var tableName string
	var createStmt string
	if err := db.QueryRow(`SHOW CREATE TABLE files`).Scan(&tableName, &createStmt); err != nil {
		return fmt.Errorf("show create table files: %w", err)
	}
	if err := validateTiDBAutoEmbeddingFilesDDL(createStmt); err != nil {
		return fmt.Errorf("files schema contract: %w", err)
	}
	return nil
}

func initTiDBAutoEmbeddingSchema(dsn string, withContentBlob bool) error {
	db, err := openTiDBSchemaDB(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
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
		{"embedding vector(1024) generated always as", "missing generated embedding column"},
		{"embed_text(", "missing EMBED_TEXT generated expression"},
		{tidbAutoEmbeddingModel, "embedding model contract mismatch"},
		{"content_text", "generated expression must derive from content_text"},
		{"dimensions", "embedding dimensions option missing"},
		{"1024", "embedding dimensions must stay at 1024"},
		{"embedding_revision bigint", "embedding_revision compatibility column missing"},
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

func normalizeDDL(s string) string {
	s = strings.ToLower(strings.ReplaceAll(s, "`", ""))
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}
