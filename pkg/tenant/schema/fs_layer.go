package schema

// FSLayerTiDBSchemaStatements returns tenant-local tables for generic Drive9
// filesystem layers. These tables sit beside file_nodes/inodes/contents: base
// data remains unchanged until an explicit layer commit applies overlay entries.
func FSLayerTiDBSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS fs_layers (
			layer_id        VARCHAR(64) PRIMARY KEY,
			base_root_path  VARCHAR(512) NOT NULL,
			name            VARCHAR(255) NOT NULL DEFAULT '',
			state           VARCHAR(32) NOT NULL DEFAULT 'active',
			durability_mode VARCHAR(32) NOT NULL DEFAULT 'restore-safe',
			actor_id        VARCHAR(255) NOT NULL DEFAULT '',
			durable_seq     BIGINT NOT NULL DEFAULT 0,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			sealed_at       DATETIME(3)
		)`,
		`CREATE INDEX idx_fs_layers_state ON fs_layers(state, updated_at)`,
		`CREATE INDEX idx_fs_layers_base_root ON fs_layers(base_root_path)`,
		`CREATE INDEX idx_fs_layers_name ON fs_layers(name, updated_at)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_tags (
			layer_id   VARCHAR(64) NOT NULL,
			tag_key    VARCHAR(255) NOT NULL,
			tag_value  VARCHAR(255) NOT NULL DEFAULT '',
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (layer_id, tag_key)
		)`,
		`CREATE INDEX idx_fs_layer_tags_kv ON fs_layer_tags(tag_key, tag_value)`,
		`CREATE INDEX idx_fs_layer_tags_key ON fs_layer_tags(tag_key)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_entries (
			layer_id         VARCHAR(64) NOT NULL,
			path             VARCHAR(1024) NOT NULL,
			path_hash        VARCHAR(64) NOT NULL,
			parent_path      VARCHAR(1024) NOT NULL,
			parent_path_hash VARCHAR(64) NOT NULL,
			name             VARCHAR(255) NOT NULL,
			op               VARCHAR(16) NOT NULL,
			kind             VARCHAR(16) NOT NULL DEFAULT 'file',
			base_inode_id    VARCHAR(64) NOT NULL DEFAULT '',
			base_revision    BIGINT NOT NULL DEFAULT 0,
			storage_type     VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref      TEXT NOT NULL,
			storage_ref_hash VARCHAR(64) NOT NULL DEFAULT '',
			storage_encryption_mode   VARCHAR(32) NOT NULL DEFAULT 'none',
			storage_encryption_key_id VARCHAR(255) NOT NULL DEFAULT '',
			content_blob     LONGBLOB,
			content_type     VARCHAR(255),
			content_text     LONGTEXT,
			checksum_sha256  VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes       BIGINT NOT NULL DEFAULT 0,
			mode             INT NOT NULL DEFAULT 420,
			entry_seq        BIGINT NOT NULL DEFAULT 0,
			created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (layer_id, path_hash, entry_seq)
		)`,
		`CREATE INDEX idx_fs_layer_entries_parent ON fs_layer_entries(layer_id, parent_path_hash)`,
		`CREATE INDEX idx_fs_layer_entries_seq ON fs_layer_entries(layer_id, entry_seq)`,
		`CREATE INDEX idx_fs_layer_entries_op ON fs_layer_entries(layer_id, op)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_events (
			event_id        VARCHAR(64) PRIMARY KEY,
			layer_id        VARCHAR(64) NOT NULL,
			seq             BIGINT NOT NULL,
			actor_id        VARCHAR(255) NOT NULL DEFAULT '',
			op              VARCHAR(32) NOT NULL,
			path            VARCHAR(1024) NOT NULL,
			before_json     JSON,
			after_json      JSON,
			idempotency_key VARCHAR(255) NOT NULL DEFAULT '',
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE UNIQUE INDEX uk_fs_layer_events_seq ON fs_layer_events(layer_id, seq)`,
		`CREATE INDEX idx_fs_layer_events_created ON fs_layer_events(layer_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_checkpoints (
			checkpoint_id VARCHAR(64) PRIMARY KEY,
			layer_id      VARCHAR(64) NOT NULL,
			durable_seq   BIGINT NOT NULL DEFAULT 0,
			label         VARCHAR(255) NOT NULL DEFAULT '',
			created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_fs_layer_checkpoints_layer ON fs_layer_checkpoints(layer_id, created_at)`,
	}
}

// FSLayerDB9SchemaStatements is the PostgreSQL/db9 equivalent of
// FSLayerTiDBSchemaStatements.
func FSLayerDB9SchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS fs_layers (
			layer_id        VARCHAR(64) PRIMARY KEY,
			base_root_path  VARCHAR(512) NOT NULL,
			name            VARCHAR(255) NOT NULL DEFAULT '',
			state           VARCHAR(32) NOT NULL DEFAULT 'active',
			durability_mode VARCHAR(32) NOT NULL DEFAULT 'restore-safe',
			actor_id        VARCHAR(255) NOT NULL DEFAULT '',
			durable_seq     BIGINT NOT NULL DEFAULT 0,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			sealed_at       TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layers_state ON fs_layers(state, updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layers_base_root ON fs_layers(base_root_path)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layers_name ON fs_layers(name, updated_at)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_tags (
			layer_id   VARCHAR(64) NOT NULL,
			tag_key    VARCHAR(255) NOT NULL,
			tag_value  VARCHAR(255) NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (layer_id, tag_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layer_tags_kv ON fs_layer_tags(tag_key, tag_value)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layer_tags_key ON fs_layer_tags(tag_key)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_entries (
			layer_id         VARCHAR(64) NOT NULL,
			path             VARCHAR(1024) NOT NULL,
			path_hash        VARCHAR(64) NOT NULL,
			parent_path      VARCHAR(1024) NOT NULL,
			parent_path_hash VARCHAR(64) NOT NULL,
			name             VARCHAR(255) NOT NULL,
			op               VARCHAR(16) NOT NULL,
			kind             VARCHAR(16) NOT NULL DEFAULT 'file',
			base_inode_id    VARCHAR(64) NOT NULL DEFAULT '',
			base_revision    BIGINT NOT NULL DEFAULT 0,
			storage_type     VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref      TEXT NOT NULL,
			storage_ref_hash VARCHAR(64) NOT NULL DEFAULT '',
			storage_encryption_mode   VARCHAR(32) NOT NULL DEFAULT 'none',
			storage_encryption_key_id VARCHAR(255) NOT NULL DEFAULT '',
			content_blob     BYTEA,
			content_type     VARCHAR(255),
			content_text     TEXT,
			checksum_sha256  VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes       BIGINT NOT NULL DEFAULT 0,
			mode             INT NOT NULL DEFAULT 420,
			entry_seq        BIGINT NOT NULL DEFAULT 0,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (layer_id, path_hash, entry_seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layer_entries_parent ON fs_layer_entries(layer_id, parent_path_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layer_entries_seq ON fs_layer_entries(layer_id, entry_seq)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layer_entries_op ON fs_layer_entries(layer_id, op)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_events (
			event_id        VARCHAR(64) PRIMARY KEY,
			layer_id        VARCHAR(64) NOT NULL,
			seq             BIGINT NOT NULL,
			actor_id        VARCHAR(255) NOT NULL DEFAULT '',
			op              VARCHAR(32) NOT NULL,
			path            VARCHAR(1024) NOT NULL,
			before_json     JSONB,
			after_json      JSONB,
			idempotency_key VARCHAR(255) NOT NULL DEFAULT '',
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uk_fs_layer_events_seq ON fs_layer_events(layer_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layer_events_created ON fs_layer_events(layer_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS fs_layer_checkpoints (
			checkpoint_id VARCHAR(64) PRIMARY KEY,
			layer_id      VARCHAR(64) NOT NULL,
			durable_seq   BIGINT NOT NULL DEFAULT 0,
			label         VARCHAR(255) NOT NULL DEFAULT '',
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fs_layer_checkpoints_layer ON fs_layer_checkpoints(layer_id, created_at)`,
	}
}
