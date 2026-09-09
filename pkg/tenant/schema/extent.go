package schema

// ExtentTiDBSchemaStatements returns tenant tables for content_layout=extent
// files: JuiceFS-style per-chunk append-only blobs in file_chunks, plus
// file_slices as a read fallback for inodes written before that table.
//
// contents.content_layout / contents.slice_generation are added next to the
// contents CREATE in tidb_app.go and tidb_auto.go (plus ALTER repair).
func ExtentTiDBSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS file_slices (
			inode_id         VARCHAR(64) NOT NULL,
			chunk            BIGINT NOT NULL,
			seq              BIGINT NOT NULL DEFAULT 0,
			file_off         BIGINT NOT NULL,
			len              BIGINT NOT NULL,
			block_key        VARCHAR(512) NOT NULL DEFAULT '',
			block_off        BIGINT NOT NULL DEFAULT 0,
			block_len        BIGINT NOT NULL DEFAULT 0,
			checksum_sha256  VARCHAR(128) NOT NULL DEFAULT '',
			kind             VARCHAR(16) NOT NULL DEFAULT 'data',
			born_gen         BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (inode_id, chunk, seq)
		)`,
		`ALTER TABLE file_slices ADD COLUMN seq BIGINT NOT NULL DEFAULT 0`,
		`UPDATE file_slices SET seq = file_off WHERE seq = 0`,
		`CREATE INDEX idx_file_slices_chunk_seq ON file_slices(inode_id, chunk, seq)`,
		`CREATE INDEX idx_file_slices_block_key ON file_slices(block_key)`,

		`CREATE TABLE IF NOT EXISTS file_chunks (
			inode_id VARCHAR(64) NOT NULL,
			chunk    BIGINT NOT NULL,
			slices   LONGBLOB NOT NULL,
			PRIMARY KEY (inode_id, chunk)
		)`,

		`CREATE TABLE IF NOT EXISTS file_slice_refs (
			block_key   VARCHAR(512) PRIMARY KEY,
			refs        BIGINT NOT NULL DEFAULT 0,
			size_bytes  BIGINT NOT NULL DEFAULT 0
		)`,

		`CREATE TABLE IF NOT EXISTS pending_blocks (
			block_key        VARCHAR(512) PRIMARY KEY,
			inode_id         VARCHAR(64) NOT NULL,
			size_bytes       BIGINT NOT NULL,
			checksum_sha256  VARCHAR(128) NOT NULL DEFAULT '',
			reserved_bytes   BIGINT NOT NULL DEFAULT 0,
			source           VARCHAR(16) NOT NULL DEFAULT 'fuse',
			created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_pending_blocks_inode ON pending_blocks(inode_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS slice_commit_ops (
			op_id             VARCHAR(64) PRIMARY KEY,
			inode_id          VARCHAR(64) NOT NULL,
			generation_after  BIGINT NOT NULL,
			revision_after    BIGINT NOT NULL,
			size_after        BIGINT NOT NULL,
			created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_slice_commit_ops_inode ON slice_commit_ops(inode_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS block_gc_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			block_key     VARCHAR(512) NOT NULL,
			inode_id      VARCHAR(64),
			size_bytes    BIGINT NOT NULL DEFAULT 0,
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
		`CREATE UNIQUE INDEX uk_block_gc_block_key ON block_gc_tasks(block_key)`,
		`CREATE INDEX idx_block_gc_claim ON block_gc_tasks(status, available_at, lease_until, created_at)`,

		`CREATE TABLE IF NOT EXISTS slice_compact_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			inode_id      VARCHAR(64) NOT NULL,
			chunk         BIGINT NOT NULL,
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
		`CREATE UNIQUE INDEX uk_slice_compact_inode_chunk ON slice_compact_tasks(inode_id, chunk)`,
		`CREATE INDEX idx_slice_compact_claim ON slice_compact_tasks(status, available_at, lease_until, created_at)`,
	}
}

// ExtentDB9SchemaStatements is the PostgreSQL/db9 equivalent of
// ExtentTiDBSchemaStatements.
func ExtentDB9SchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS file_slices (
			inode_id         VARCHAR(64) NOT NULL,
			chunk            BIGINT NOT NULL,
			seq              BIGINT NOT NULL DEFAULT 0,
			file_off         BIGINT NOT NULL,
			len              BIGINT NOT NULL,
			block_key        VARCHAR(512) NOT NULL DEFAULT '',
			block_off        BIGINT NOT NULL DEFAULT 0,
			block_len        BIGINT NOT NULL DEFAULT 0,
			checksum_sha256  VARCHAR(128) NOT NULL DEFAULT '',
			kind             VARCHAR(16) NOT NULL DEFAULT 'data',
			born_gen         BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (inode_id, chunk, seq)
		)`,
		`ALTER TABLE file_slices ADD COLUMN seq BIGINT NOT NULL DEFAULT 0`,
		`UPDATE file_slices SET seq = file_off WHERE seq = 0`,
		`CREATE INDEX IF NOT EXISTS idx_file_slices_chunk_seq ON file_slices(inode_id, chunk, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_file_slices_block_key ON file_slices(block_key)`,

		`CREATE TABLE IF NOT EXISTS file_chunks (
			inode_id VARCHAR(64) NOT NULL,
			chunk    BIGINT NOT NULL,
			slices   BYTEA NOT NULL,
			PRIMARY KEY (inode_id, chunk)
		)`,

		`CREATE TABLE IF NOT EXISTS file_slice_refs (
			block_key   VARCHAR(512) PRIMARY KEY,
			refs        BIGINT NOT NULL DEFAULT 0,
			size_bytes  BIGINT NOT NULL DEFAULT 0
		)`,

		`CREATE TABLE IF NOT EXISTS pending_blocks (
			block_key        VARCHAR(512) PRIMARY KEY,
			inode_id         VARCHAR(64) NOT NULL,
			size_bytes       BIGINT NOT NULL,
			checksum_sha256  VARCHAR(128) NOT NULL DEFAULT '',
			reserved_bytes   BIGINT NOT NULL DEFAULT 0,
			source           VARCHAR(16) NOT NULL DEFAULT 'fuse',
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pending_blocks_inode ON pending_blocks(inode_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS slice_commit_ops (
			op_id             VARCHAR(64) PRIMARY KEY,
			inode_id          VARCHAR(64) NOT NULL,
			generation_after  BIGINT NOT NULL,
			revision_after    BIGINT NOT NULL,
			size_after        BIGINT NOT NULL,
			created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_slice_commit_ops_inode ON slice_commit_ops(inode_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS block_gc_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			block_key     VARCHAR(512) NOT NULL,
			inode_id      VARCHAR(64),
			size_bytes    BIGINT NOT NULL DEFAULT 0,
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
			completed_at  TIMESTAMPTZ
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uk_block_gc_block_key ON block_gc_tasks(block_key)`,
		`CREATE INDEX IF NOT EXISTS idx_block_gc_claim ON block_gc_tasks(status, available_at, lease_until, created_at)`,

		`CREATE TABLE IF NOT EXISTS slice_compact_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			inode_id      VARCHAR(64) NOT NULL,
			chunk         BIGINT NOT NULL,
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
			completed_at  TIMESTAMPTZ
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uk_slice_compact_inode_chunk ON slice_compact_tasks(inode_id, chunk)`,
		`CREATE INDEX IF NOT EXISTS idx_slice_compact_claim ON slice_compact_tasks(status, available_at, lease_until, created_at)`,
	}
}
