package db9

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/mem9-ai/dat9/pkg/tenant/schema"
	"github.com/mem9-ai/dat9/pkg/vault"
)

// InitSchemaStatements returns the exact DDL statements used by db9 tenant
// schema initialization, including vault tables.
func InitSchemaStatements() []string {
	// Keep this statement list aligned with the externally managed tidb_cloud_starter
	// schema. If you change columns, indexes, generated expressions, or
	// constraints here, rerun:
	//   drive9-server schema dump-init-sql --provider db9
	// and update tidb_cloud_starter with the exported SQL.
	core := []string{
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
		// See docs/async-embedding/async-embedding-generation-proposal.md,
		// section "2) File schema: embedding must become mutable and revision-aware".
		`CREATE TABLE IF NOT EXISTS files (
			file_id            VARCHAR(64) PRIMARY KEY,
			storage_type       VARCHAR(32) NOT NULL,
			storage_ref        TEXT NOT NULL,
			content_blob       BYTEA,
			content_type       VARCHAR(255),
			size_bytes         BIGINT NOT NULL DEFAULT 0,
			checksum_sha256    VARCHAR(128),
			revision           BIGINT NOT NULL DEFAULT 1,
			status             VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			source_id          VARCHAR(255),
			content_text       TEXT,
			embedding          vector(1024),
			embedding_revision BIGINT,
			created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			confirmed_at       TIMESTAMPTZ,
			expires_at         TIMESTAMPTZ
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
			expected_revision  BIGINT,
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
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_uploads_active ON uploads(active_target_path)`,
		// semantic_tasks groups fields by responsibility:
		// - identity/resource binding: task_id, task_type, resource_id, resource_version
		// - delivery state: status, attempt_count, max_attempts
		// - lease/claim ownership: receipt, leased_at, lease_until, available_at
		// - diagnostics/audit: payload_json, last_error, created_at, updated_at, completed_at
		// payload_json is only for lightweight hints/debugging; worker correctness
		// must always re-read current file state via resource_id + resource_version.
		`CREATE TABLE IF NOT EXISTS semantic_tasks (
			task_id           VARCHAR(64) PRIMARY KEY,
			task_type         VARCHAR(32) NOT NULL,
			resource_id       VARCHAR(64) NOT NULL,
			resource_version  BIGINT NOT NULL,
			status            VARCHAR(20) NOT NULL,
			attempt_count     INT NOT NULL DEFAULT 0,
			max_attempts      INT NOT NULL DEFAULT 5,
			receipt           VARCHAR(128),
			leased_at         TIMESTAMPTZ,
			lease_until       TIMESTAMPTZ,
			available_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			payload_json      JSONB,
			last_error        TEXT,
			created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at      TIMESTAMPTZ,
			UNIQUE (task_type, resource_id, resource_version)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_task_claim ON semantic_tasks(status, available_at, lease_until, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_task_claim_type ON semantic_tasks(status, task_type, available_at, created_at, task_id)`,
		`CREATE TABLE IF NOT EXISTS llm_usage (
			id              BIGSERIAL PRIMARY KEY,
			task_type       VARCHAR(32) NOT NULL,
			task_id         VARCHAR(64) NOT NULL,
			cost_millicents BIGINT NOT NULL DEFAULT 0,
			raw_units       BIGINT NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(16) NOT NULL,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_llm_usage_created ON llm_usage(created_at)`,
	}
	stmts := make([]string, 0, len(core)+len(vault.SchemaStatements()))
	stmts = append(stmts, core...)
	stmts = append(stmts, vault.SchemaStatements()...)
	return stmts
}

func initDB9Schema(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err != nil {
		return err
	}

	if err := schema.ExecSchemaStatements(db, InitSchemaStatements()); err != nil {
		return err
	}
	return nil
}
