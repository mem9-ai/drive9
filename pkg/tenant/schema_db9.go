package tenant

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func initDB9Schema(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err != nil {
		return err
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (
			node_id      VARCHAR(64) PRIMARY KEY,
			path         VARCHAR(512) NOT NULL,
			parent_path  VARCHAR(512) NOT NULL,
			name         VARCHAR(255) NOT NULL,
			is_directory BOOLEAN NOT NULL DEFAULT FALSE,
			file_id      VARCHAR(64),
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_path ON file_nodes(path)`,
		`CREATE INDEX IF NOT EXISTS idx_parent ON file_nodes(parent_path)`,
		`CREATE INDEX IF NOT EXISTS idx_file_id ON file_nodes(file_id)`,
		`CREATE TABLE IF NOT EXISTS files (
			file_id         VARCHAR(64) PRIMARY KEY,
			storage_type    VARCHAR(32) NOT NULL,
			storage_ref     TEXT NOT NULL,
			content_blob    BYTEA,
			content_type    VARCHAR(255),
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			checksum_sha256 VARCHAR(128),
			revision        BIGINT NOT NULL DEFAULT 1,
			status          VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			source_id       VARCHAR(255),
			content_text    TEXT,
			embedding       vector(1024) GENERATED ALWAYS AS (EMBED_TEXT('` + autoEmbedTextModel + `', content_text, '{"dimensions": 1024}')) STORED,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			confirmed_at    TIMESTAMPTZ,
			expires_at      TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_status ON files(status, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_files_cosine ON files USING hnsw (embedding vector_cosine_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_fts_content ON files USING gin (to_tsvector('simple', coalesce(content_text,'')))`,
		`CREATE TABLE IF NOT EXISTS file_tags (
			file_id   VARCHAR(64) NOT NULL,
			tag_key   VARCHAR(255) NOT NULL,
			tag_value VARCHAR(255) NOT NULL DEFAULT '',
			PRIMARY KEY (file_id, tag_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_kv ON file_tags(tag_key, tag_value)`,
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
			created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at         TIMESTAMPTZ NOT NULL,
			active_target_path VARCHAR(512) GENERATED ALWAYS AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED
		)`,
		`CREATE INDEX IF NOT EXISTS idx_upload_path ON uploads(target_path, status)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency ON uploads(idempotency_key)`,
	}

	return execSchemaStatements(db, stmts)
}
