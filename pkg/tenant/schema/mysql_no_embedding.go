package schema

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
)

// MySQLNoEmbeddingTenantSchemaStatements returns a local-development tenant
// schema that avoids TiDB-only FTS/VECTOR/EMBED_TEXT features.
//
// This schema is intentionally not used for production tenant provisioning. It
// exists to make drive9-server-local and e2e smoke tests runnable against an
// ordinary MySQL-compatible database while preserving the same core filesystem,
// layer, journal, git workspace, and vault tables.
func MySQLNoEmbeddingTenantSchemaStatements() []string {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (
			node_id      VARCHAR(64) PRIMARY KEY,
			path         VARCHAR(512) NOT NULL,
			parent_path  VARCHAR(512) NOT NULL,
			name         VARCHAR(255) NOT NULL,
			is_directory BOOLEAN NOT NULL DEFAULT FALSE,
			file_id      VARCHAR(64),
			inode_id     VARCHAR(64),
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE UNIQUE INDEX idx_path ON file_nodes(path)`,
		`CREATE INDEX idx_parent ON file_nodes(parent_path)`,
		`CREATE INDEX idx_file_id ON file_nodes(file_id)`,
		`CREATE INDEX idx_inode_id ON file_nodes(inode_id)`,

		`CREATE TABLE IF NOT EXISTS files (
			file_id                    VARCHAR(64) PRIMARY KEY,
			storage_type               VARCHAR(32) NOT NULL,
			storage_ref                TEXT NOT NULL,
			storage_ref_hash           VARCHAR(64) NOT NULL DEFAULT '',
			storage_encryption_mode    VARCHAR(16) NOT NULL DEFAULT 'legacy',
			storage_encryption_key_id  VARCHAR(256) NOT NULL DEFAULT '',
			content_blob               LONGBLOB,
			content_type               VARCHAR(255),
			size_bytes                 BIGINT NOT NULL DEFAULT 0,
			checksum_sha256            VARCHAR(128),
			revision                   BIGINT NOT NULL DEFAULT 1,
			status                     VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			source_id                  VARCHAR(255),
			content_text               LONGTEXT,
			description                LONGTEXT,
			embedding                  LONGTEXT,
			embedding_revision         BIGINT,
			description_embedding      LONGTEXT,
			description_embedding_revision BIGINT,
			created_at                 DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			confirmed_at               DATETIME(3),
			expires_at                 DATETIME(3)
		)`,
		`CREATE INDEX idx_status ON files(status, created_at)`,
		`CREATE INDEX idx_files_storage_ref_hash ON files(storage_ref_hash)`,

		`CREATE TABLE IF NOT EXISTS inodes (
			inode_id     VARCHAR(64) PRIMARY KEY,
			size_bytes   BIGINT NOT NULL DEFAULT 0,
			revision     BIGINT NOT NULL DEFAULT 1,
			mode         INT UNSIGNED NOT NULL DEFAULT 420,
			status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			mtime        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			confirmed_at DATETIME(3),
			expires_at   DATETIME(3)
		)`,
		`CREATE INDEX idx_inodes_status ON inodes(status, created_at)`,
		`CREATE TABLE IF NOT EXISTS contents (
			inode_id                   VARCHAR(64) PRIMARY KEY,
			storage_type               VARCHAR(32) NOT NULL,
			storage_ref                TEXT NOT NULL,
			storage_ref_hash           VARCHAR(64) NOT NULL DEFAULT '',
			storage_encryption_mode    VARCHAR(16) NOT NULL DEFAULT 'legacy',
			storage_encryption_key_id  VARCHAR(256) NOT NULL DEFAULT '',
			content_blob               LONGBLOB,
			content_type               VARCHAR(255),
			checksum_sha256            VARCHAR(128),
			source_id                  VARCHAR(255)
		)`,
		`CREATE INDEX idx_contents_storage_ref_hash ON contents(storage_ref_hash)`,
		`CREATE TABLE IF NOT EXISTS semantic (
			inode_id                           VARCHAR(64) PRIMARY KEY,
			content_text                       LONGTEXT,
			description                        LONGTEXT,
			embedding                          LONGTEXT,
			embedding_revision                 BIGINT,
			description_embedding              LONGTEXT,
			description_embedding_revision     BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS file_tags (
			file_id   VARCHAR(64) NOT NULL,
			inode_id  VARCHAR(64),
			tag_key   VARCHAR(255) NOT NULL,
			tag_value VARCHAR(255) NOT NULL DEFAULT '',
			PRIMARY KEY (file_id, tag_key)
		)`,
		`CREATE INDEX idx_kv ON file_tags(tag_key, tag_value)`,
		`CREATE TABLE IF NOT EXISTS uploads (
			upload_id          VARCHAR(64) PRIMARY KEY,
			file_id            VARCHAR(64) NOT NULL,
			inode_id           VARCHAR(64),
			target_path        VARCHAR(512) NOT NULL,
			s3_upload_id       VARCHAR(255) NOT NULL,
			s3_key             VARCHAR(2048) NOT NULL,
			storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'none',
			storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '',
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
		`CREATE TABLE IF NOT EXISTS file_gc_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			file_id       VARCHAR(64) NOT NULL,
			inode_id      VARCHAR(64),
			storage_type  VARCHAR(32) NOT NULL,
			storage_ref   TEXT NOT NULL,
			size_bytes    BIGINT NOT NULL DEFAULT 0,
			content_type  VARCHAR(255) NULL,
			status        VARCHAR(20) NOT NULL,
			attempt_count INT NOT NULL DEFAULT 0,
			max_attempts  INT NOT NULL DEFAULT 0,
			receipt       VARCHAR(128) NULL,
			leased_at     DATETIME(3) NULL,
			lease_until   DATETIME(3) NULL,
			available_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			last_error    TEXT NULL,
			created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at  DATETIME(3) NULL
		)`,
		`CREATE UNIQUE INDEX uk_file_gc_file_id ON file_gc_tasks(file_id)`,
		`CREATE UNIQUE INDEX uk_file_gc_inode_id ON file_gc_tasks(inode_id)`,
		`CREATE INDEX idx_file_gc_claim ON file_gc_tasks(status, available_at, lease_until, created_at)`,
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
		`CREATE TABLE IF NOT EXISTS fs_event_seq (
			id       TINYINT UNSIGNED PRIMARY KEY,
			next_seq BIGINT UNSIGNED NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fs_events (
			seq        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			path       VARCHAR(512) NOT NULL,
			op         VARCHAR(64) NOT NULL,
			actor      VARCHAR(255),
			ts         BIGINT NOT NULL,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_fs_events_created ON fs_events(created_at)`,
	}
	stmts = append(stmts, GitWorkspaceTiDBSchemaStatements()...)
	stmts = append(stmts, FSLayerTiDBSchemaStatements()...)
	stmts = append(stmts, JournalTiDBSchemaStatements()...)
	stmts = append(stmts, VaultTiDBSchemaStatements()...)
	return stmts
}

// InitMySQLNoEmbeddingTenantSchemaContext initializes the local no-embedding
// tenant schema on any MySQL-compatible database. It deliberately skips TiDB
// capability checks and TiDB-only optional indexes.
func InitMySQLNoEmbeddingTenantSchemaContext(ctx context.Context, dsn string) error {
	start := time.Now()
	logger.Info(ctx, "tenant_mysql_no_embedding_schema_init_started")
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	if err := ExecSchemaStatementsContext(ctx, db, MySQLNoEmbeddingTenantSchemaStatements()); err != nil {
		return err
	}
	if err := ValidateMySQLNoEmbeddingTenantSchema(ctx, db); err != nil {
		return err
	}
	logger.Info(ctx, "tenant_mysql_no_embedding_schema_init_finished",
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return nil
}

// ValidateMySQLNoEmbeddingTenantSchema performs a light contract check for the
// local no-embedding schema. Production TiDB paths still use
// ValidateTiDBSchemaForMode for exact schema validation.
func ValidateMySQLNoEmbeddingTenantSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	required := []string{
		"file_nodes",
		"inodes",
		"contents",
		"semantic",
		"uploads",
		"fs_layers",
		"fs_layer_entries",
		"fs_layer_tags",
		"fs_layer_events",
		"fs_layer_checkpoints",
		"journals",
		"fs_event_seq",
		"fs_events",
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_schema = DATABASE()
				AND table_name IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		required[0], required[1], required[2], required[3], required[4],
		required[5], required[6], required[7], required[8], required[9],
		required[10], required[11], required[12],
	).Scan(&count); err != nil {
		return fmt.Errorf("validate local no-embedding schema: %w", err)
	}
	if count != len(required) {
		return fmt.Errorf("local no-embedding schema missing required tables: got %d want %d", count, len(required))
	}
	return nil
}
