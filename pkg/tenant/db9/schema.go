package db9

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/mem9-ai/dat9/pkg/tenant/schema"
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
			inode_id     VARCHAR(64),
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_path ON file_nodes(path)`,
		`CREATE INDEX IF NOT EXISTS idx_parent ON file_nodes(parent_path)`,
		`CREATE INDEX IF NOT EXISTS idx_file_id ON file_nodes(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_inode_id ON file_nodes(inode_id)`,
		// See docs/async-embedding/async-embedding-generation-proposal.md,
		// section "2) File schema: embedding must become mutable and revision-aware".

		`CREATE TABLE IF NOT EXISTS inodes (
			inode_id     VARCHAR(64) PRIMARY KEY,
			size_bytes   BIGINT NOT NULL DEFAULT 0,
			revision     BIGINT NOT NULL DEFAULT 1,
			mode         INT NOT NULL DEFAULT 420,
			status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			mtime        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			confirmed_at TIMESTAMPTZ,
			expires_at   TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_inodes_status ON inodes(status, created_at)`,
		`CREATE TABLE IF NOT EXISTS contents (
			inode_id                   VARCHAR(64) PRIMARY KEY,
			storage_type               VARCHAR(32) NOT NULL,
			storage_ref                TEXT NOT NULL,
			storage_ref_hash           VARCHAR(64) NOT NULL DEFAULT '',
			storage_encryption_mode    VARCHAR(16) NOT NULL DEFAULT 'legacy',
			storage_encryption_key_id  VARCHAR(256) NOT NULL DEFAULT '',
			content_blob               BYTEA,
			content_type               VARCHAR(255),
			checksum_sha256            VARCHAR(128),
			source_id                  VARCHAR(255)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_contents_storage_ref_hash ON contents(storage_ref_hash)`,
		`CREATE TABLE IF NOT EXISTS semantic (
			inode_id                           VARCHAR(64) PRIMARY KEY,
			content_text                       TEXT,
			description                        TEXT,
			embedding                          vector(1024),
			embedding_revision                 BIGINT,
			description_embedding              vector(1024),
			description_embedding_revision     BIGINT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_semantic_cosine ON semantic USING hnsw (embedding vector_cosine_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_semantic_desc_cosine ON semantic USING hnsw (description_embedding vector_cosine_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_semantic_fts_content ON semantic USING gin (to_tsvector('simple', coalesce(content_text,'')))`,
		`CREATE INDEX IF NOT EXISTS idx_semantic_fts_description ON semantic USING gin (to_tsvector('simple', coalesce(description,'')))`,
		`CREATE TABLE IF NOT EXISTS file_tags (
			file_id   VARCHAR(64) NOT NULL,
			inode_id  VARCHAR(64),
			tag_key   VARCHAR(255) NOT NULL,
			tag_value VARCHAR(255) NOT NULL DEFAULT '',
			PRIMARY KEY (file_id, tag_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_kv ON file_tags(tag_key, tag_value)`,
		`CREATE TABLE IF NOT EXISTS uploads (
			upload_id          VARCHAR(64) PRIMARY KEY,
			file_id            VARCHAR(64) NOT NULL,
			inode_id           VARCHAR(64),
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
			description        TEXT,
			storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'none',
			storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '',
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
		`CREATE TABLE IF NOT EXISTS file_gc_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			file_id       VARCHAR(64) NOT NULL,
			inode_id      VARCHAR(64),
			storage_type  VARCHAR(32) NOT NULL,
			storage_ref   TEXT NOT NULL,
			size_bytes    BIGINT NOT NULL DEFAULT 0,
			content_type  VARCHAR(255),
			status        VARCHAR(20) NOT NULL,
			attempt_count INT NOT NULL DEFAULT 0,
			max_attempts  INT NOT NULL DEFAULT 0,
			receipt       VARCHAR(128),
			leased_at     TIMESTAMPTZ,
			lease_until   TIMESTAMPTZ,
			available_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_error    TEXT,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at  TIMESTAMPTZ,
			UNIQUE (file_id),
			UNIQUE (inode_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_file_gc_claim ON file_gc_tasks(status, available_at, lease_until, created_at)`,
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
		`CREATE TABLE IF NOT EXISTS fs_event_seq (
			id       SMALLINT PRIMARY KEY,
			next_seq BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fs_events (
			seq        BIGSERIAL PRIMARY KEY,
			path       VARCHAR(512) NOT NULL,
			op         VARCHAR(64) NOT NULL,
			actor      VARCHAR(255),
			ts         BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_events_created ON fs_events(created_at)`,
	}
	core = append(core, schema.GitWorkspaceDB9SchemaStatements()...)
	core = append(core, schema.FSLayerDB9SchemaStatements()...)
	core = append(core, schema.JournalDB9SchemaStatements()...)
	// Vault tables are TiDB/MySQL-only and are not created via the db9
	// PostgreSQL schema init path. They are initialized through the TiDB
	// tenant schema init (see pkg/tenant/schema/vault.go).
	return core
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
