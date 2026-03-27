package tenant

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func initSchemaByProvider(dsn, provider string) error {
	if strings.Contains(dsn, "multiStatements=true") {
		return fmt.Errorf("multiStatements=true is not allowed")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}

	smallInDB := provider == ProviderTiDBZero || provider == ProviderTiDBCloudStarter
	for _, stmt := range baseSchemaStatements(smallInDB) {
		if _, err := db.Exec(stmt); err != nil {
			if isIndexStmt(stmt) && isDuplicateIndexError(err) {
				continue
			}
			snippet := stmt
			if len(snippet) > 60 {
				snippet = snippet[:60]
			}
			return fmt.Errorf("exec %q: %w", snippet, err)
		}
	}

	for _, stmt := range capabilityStatements(provider) {
		if _, err := db.Exec(stmt); err != nil {
			if isDuplicateIndexError(err) || isDuplicateColumnError(err) || isUnsupportedCapabilityError(err) {
				continue
			}
			snippet := stmt
			if len(snippet) > 60 {
				snippet = snippet[:60]
			}
			return fmt.Errorf("exec capability %q: %w", snippet, err)
		}
	}
	return nil
}

func InitSchemaForProvider(dsn, provider string) error {
	return initSchemaByProvider(dsn, provider)
}

func (p *ZeroProvisioner) InitSchema(ctx context.Context, dsn string) error {
	return initSchemaByProvider(dsn, p.ProviderType())
}

func (p *StarterProvisioner) InitSchema(ctx context.Context, dsn string) error {
	return initSchemaByProvider(dsn, p.ProviderType())
}

func (p *DB9Provisioner) InitSchema(ctx context.Context, dsn string) error {
	return initSchemaByProvider(dsn, p.ProviderType())
}

func baseSchemaStatements(smallInDB bool) []string {
	contentBlobCol := ""
	if smallInDB {
		contentBlobCol = "content_blob    LONGBLOB,"
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
			file_id         VARCHAR(64) PRIMARY KEY,
			storage_type    VARCHAR(32) NOT NULL,
			storage_ref     TEXT NOT NULL,
			` + contentBlobCol + `
			content_type    VARCHAR(255),
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			checksum_sha256 VARCHAR(128),
			revision        BIGINT NOT NULL DEFAULT 1,
			status          VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			source_id       VARCHAR(255),
			content_text    LONGTEXT,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			confirmed_at    DATETIME(3),
			expires_at      DATETIME(3)
		)`,
		`CREATE INDEX idx_status ON files(status, created_at)`,
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
	}
}

func capabilityStatements(provider string) []string {
	if provider != ProviderTiDBZero && provider != ProviderTiDBCloudStarter {
		return nil
	}
	return []string{
		`ALTER TABLE files ADD COLUMN embedding VECTOR(1536) NULL`,
		`ALTER TABLE files
			ADD FULLTEXT INDEX idx_fts_content(content_text)
			WITH PARSER MULTILINGUAL
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE files
			ADD VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
	}
}

func isIndexStmt(stmt string) bool {
	s := strings.TrimSpace(strings.ToUpper(stmt))
	return strings.HasPrefix(s, "CREATE INDEX") || strings.HasPrefix(s, "CREATE UNIQUE INDEX")
}

func isDuplicateIndexError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "1061") || strings.Contains(msg, "Duplicate key name") || strings.Contains(msg, "already exists")
}

func isDuplicateColumnError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "1060") || strings.Contains(msg, "Duplicate column name")
}

func isUnsupportedCapabilityError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "you have an error in your sql syntax") ||
		strings.Contains(msg, "unknown data type") ||
		strings.Contains(msg, "function 'multilingual' is not defined") ||
		strings.Contains(msg, "unsupported") ||
		strings.Contains(msg, "vector")
}
