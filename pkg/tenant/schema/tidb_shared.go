package schema

import (
	"strconv"
)

// CoreFSTiDBSharedSchemaStatements returns the Core FS DDL for the shared
// (multi-tenant) schema shape on TiDB: the ten Core FS tables mirrored from
// tidbAppEmbeddingBaseSchemaStatements with an fs_id BIGINT NOT NULL
// discriminator as the first column of every table. Composite primary keys
// are prefixed with fs_id and declared CLUSTERED — TiDB creates composite
// primary keys NONCLUSTERED by default, and the shared shape relies on
// clustered keys to physically co-locate one tenant's rows. Every secondary
// index and unique constraint is likewise prefixed with fs_id. Column
// definitions otherwise mirror the standalone ones verbatim, so primary key
// member columns pick up their implicit NOT NULL from the key itself.
//
// Two documented exceptions keep their global AUTO_INCREMENT physical primary
// key unchanged (docs/TENANT_DB_REDESIGN.md §5.4) and only gain the fs_id
// column plus (fs_id, ...) lookup indexes:
//
//   - llm_usage: id stays the primary key; idx_llm_usage_created stays
//     unprefixed; idx_llm_usage_fs (fs_id, created_at) is new.
//   - fs_events: seq stays the primary key; idx_fs_events_created stays
//     unprefixed; idx_fs_events_fs_seq (fs_id, seq) is new.
//
// The uploads repair artifact (the standalone ALTER TABLE uploads ADD COLUMN
// expected_revision) is folded directly into the CREATE TABLE. The semantic
// table keeps app-managed plain VECTOR columns — no generated columns and no
// EMBED_TEXT — and carries no FTS/vector indexes here; those live in
// TiDBSharedOptionalSchemaStatements.
//
// For plain MySQL use CoreFSMySQLSharedSchemaStatements (same shape without
// the CLUSTERED keyword). Keep both in lockstep with
// tidbAppEmbeddingBaseSchemaStatements — the drift test in
// tidb_shared_test.go enforces parity.
func CoreFSTiDBSharedSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (
			fs_id            BIGINT       NOT NULL,
			node_id          VARCHAR(64),
			path             TEXT         NOT NULL,
			path_hash        VARCHAR(64)  NOT NULL DEFAULT '',
			parent_path      TEXT         NOT NULL,
			parent_path_hash VARCHAR(64)  NOT NULL DEFAULT '',
			name             VARCHAR(255) NOT NULL,
			is_directory     BOOLEAN      NOT NULL DEFAULT FALSE,
			file_id          VARCHAR(64),
			inode_id         VARCHAR(64),
			created_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (fs_id, node_id) CLUSTERED,
			UNIQUE KEY idx_path (fs_id, path_hash),
			KEY idx_parent (fs_id, parent_path_hash, name),
			KEY idx_file_id (fs_id, file_id),
			KEY idx_inode_id (fs_id, inode_id)
		)`,
		`CREATE TABLE IF NOT EXISTS inodes (
			fs_id        BIGINT       NOT NULL,
			inode_id     VARCHAR(64),
			size_bytes   BIGINT       NOT NULL DEFAULT 0,
			revision     BIGINT       NOT NULL DEFAULT 1,
			mode         INT UNSIGNED NOT NULL DEFAULT 420,
			status       VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
			created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			mtime        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			confirmed_at DATETIME(3),
			expires_at   DATETIME(3),
			PRIMARY KEY (fs_id, inode_id) CLUSTERED,
			KEY idx_inodes_status (fs_id, status, created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS contents (
			fs_id                     BIGINT       NOT NULL,
			inode_id                  VARCHAR(64),
			storage_type              VARCHAR(32)  NOT NULL,
			storage_ref               TEXT         NOT NULL,
			storage_ref_hash          VARCHAR(64)  NOT NULL DEFAULT '',
			storage_encryption_mode   VARCHAR(16)  NOT NULL DEFAULT 'legacy',
			storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '',
			content_blob              LONGBLOB,
			content_type              VARCHAR(255),
			checksum_sha256           VARCHAR(128),
			source_id                 VARCHAR(255),
			PRIMARY KEY (fs_id, inode_id) CLUSTERED,
			KEY idx_contents_storage_ref_hash (fs_id, storage_ref_hash)
		)`,
		`CREATE TABLE IF NOT EXISTS semantic (
			fs_id                          BIGINT      NOT NULL,
			inode_id                       VARCHAR(64),
			content_text                   LONGTEXT,
			description                    LONGTEXT,
			embedding                      VECTOR(` + strconv.Itoa(TiDBAutoEmbeddingDimensions) + `),
			embedding_revision             BIGINT,
			description_embedding          VECTOR(` + strconv.Itoa(TiDBAutoEmbeddingDimensions) + `),
			description_embedding_revision BIGINT,
			PRIMARY KEY (fs_id, inode_id) CLUSTERED
		)`,
		`CREATE TABLE IF NOT EXISTS file_tags (
			fs_id     BIGINT       NOT NULL,
			file_id   VARCHAR(64)  NOT NULL,
			inode_id  VARCHAR(64),
			tag_key   VARCHAR(255) NOT NULL,
			tag_value VARCHAR(255) NOT NULL DEFAULT '',
			PRIMARY KEY (fs_id, file_id, tag_key) CLUSTERED,
			KEY idx_kv (fs_id, tag_key, tag_value)
		)`,
		`CREATE TABLE IF NOT EXISTS uploads (
			fs_id                     BIGINT        NOT NULL,
			upload_id                 VARCHAR(64),
			file_id                   VARCHAR(64)   NOT NULL,
			inode_id                  VARCHAR(64),
			target_path               TEXT          NOT NULL,
			target_path_hash          VARCHAR(64)   NOT NULL DEFAULT '',
			s3_upload_id              VARCHAR(255)  NOT NULL,
			s3_key                    VARCHAR(2048) NOT NULL,
			storage_encryption_mode   VARCHAR(16)   NOT NULL DEFAULT 'none',
			storage_encryption_key_id VARCHAR(256)  NOT NULL DEFAULT '',
			total_size                BIGINT        NOT NULL,
			part_size                 BIGINT        NOT NULL,
			parts_total               INT           NOT NULL,
			expected_revision         BIGINT        NULL,
			status                    VARCHAR(32)   NOT NULL DEFAULT 'UPLOADING',
			fingerprint_sha256        VARCHAR(128),
			idempotency_key           VARCHAR(255),
			description               LONGTEXT,
			created_at                DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at                DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			expires_at                DATETIME(3)   NOT NULL,
			active_target_path_hash   VARCHAR(64)   AS (CASE WHEN status = 'UPLOADING' THEN target_path_hash ELSE NULL END) VIRTUAL,
			PRIMARY KEY (fs_id, upload_id) CLUSTERED,
			KEY idx_upload_path (fs_id, target_path_hash, status),
			UNIQUE KEY idx_idempotency (fs_id, idempotency_key),
			UNIQUE KEY idx_uploads_active (fs_id, active_target_path_hash)
		)`,
		`CREATE TABLE IF NOT EXISTS semantic_tasks (
			fs_id            BIGINT       NOT NULL,
			task_id          VARCHAR(64),
			task_type        VARCHAR(32)  NOT NULL,
			resource_id      VARCHAR(64)  NOT NULL,
			resource_version BIGINT       NOT NULL,
			status           VARCHAR(20)  NOT NULL,
			attempt_count    INT          NOT NULL DEFAULT 0,
			max_attempts     INT          NOT NULL DEFAULT 5,
			receipt          VARCHAR(128) NULL,
			leased_at        DATETIME(3)  NULL,
			lease_until      DATETIME(3)  NULL,
			available_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			payload_json     JSON         NULL,
			last_error       TEXT         NULL,
			created_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at     DATETIME(3)  NULL,
			PRIMARY KEY (fs_id, task_id) CLUSTERED,
			UNIQUE KEY uk_task_resource_version (fs_id, task_type, resource_id, resource_version),
			KEY idx_task_claim (fs_id, status, available_at, lease_until, created_at),
			KEY idx_task_claim_type (fs_id, status, task_type, available_at, created_at, task_id)
		)`,
		`CREATE TABLE IF NOT EXISTS file_gc_tasks (
			fs_id         BIGINT       NOT NULL,
			task_id       VARCHAR(64),
			file_id       VARCHAR(64)  NOT NULL,
			inode_id      VARCHAR(64),
			storage_type  VARCHAR(32)  NOT NULL,
			storage_ref   TEXT         NOT NULL,
			size_bytes    BIGINT       NOT NULL DEFAULT 0,
			content_type  VARCHAR(255) NULL,
			status        VARCHAR(20)  NOT NULL,
			attempt_count INT          NOT NULL DEFAULT 0,
			max_attempts  INT          NOT NULL DEFAULT 0,
			receipt       VARCHAR(128) NULL,
			leased_at     DATETIME(3)  NULL,
			lease_until   DATETIME(3)  NULL,
			available_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			last_error    TEXT         NULL,
			created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at  DATETIME(3)  NULL,
			PRIMARY KEY (fs_id, task_id) CLUSTERED,
			UNIQUE KEY uk_file_gc_file_id (fs_id, file_id),
			UNIQUE KEY uk_file_gc_inode_id (fs_id, inode_id),
			KEY idx_file_gc_claim (fs_id, status, available_at, lease_until, created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS llm_usage (
			fs_id           BIGINT      NOT NULL,
			id              BIGINT      AUTO_INCREMENT PRIMARY KEY,
			task_type       VARCHAR(32) NOT NULL,
			task_id         VARCHAR(64) NOT NULL,
			cost_millicents BIGINT      NOT NULL DEFAULT 0,
			raw_units       BIGINT      NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(16) NOT NULL,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			KEY idx_llm_usage_created (created_at),
			KEY idx_llm_usage_fs (fs_id, created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS fs_events (
			fs_id      BIGINT       NOT NULL,
			seq        BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			path       TEXT         NOT NULL,
			op         VARCHAR(64)  NOT NULL,
			actor      VARCHAR(255),
			ts         BIGINT       NOT NULL,
			created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			KEY idx_fs_events_created (created_at),
			KEY idx_fs_events_fs_seq (fs_id, seq)
		)`,
	}
}

// CoreFSMySQLSharedSchemaStatements is the plain-MySQL variant of
// CoreFSTiDBSharedSchemaStatements, derived by removing TiDB-only keywords.
// Use it for local development databases and MySQL-backed tests/e2e.
func CoreFSMySQLSharedSchemaStatements() []string {
	return mysqlCompatibleSharedStatements(CoreFSTiDBSharedSchemaStatements())
}

// TiDBSharedOptionalSchemaStatements returns the TiDB-only optional semantic
// indexes (full-text + vector) for the shared schema shape, mirroring
// tidbAppEmbeddingOptionalSchemaStatements verbatim: the statements reference
// columns, not tenant keys, so no fs_id changes are needed. There is no MySQL
// variant — plain MySQL has no comparable FTS parser or vector index support.
//
// Note: the vector index expression (VEC_COSINE_DISTANCE here versus the
// query-side VEC_EMBED_COSINE_DISTANCE) is pending Phase-0 verification (D3 in
// docs/TENANT_DB_REDESIGN.md), so this list may change once that lands.
func TiDBSharedOptionalSchemaStatements() []string {
	return []string{

		`ALTER TABLE semantic
			ADD FULLTEXT INDEX idx_semantic_fts_content(content_text)
			WITH PARSER MULTILINGUAL
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE semantic
			ADD FULLTEXT INDEX idx_semantic_fts_description(description)
			WITH PARSER MULTILINGUAL
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE semantic
			ADD VECTOR INDEX idx_semantic_cosine((VEC_COSINE_DISTANCE(embedding)))
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE semantic
			ADD VECTOR INDEX idx_semantic_desc_cosine((VEC_COSINE_DISTANCE(description_embedding)))
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
	}
}
