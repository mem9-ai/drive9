package schema

// ExtentTiDBSchemaStatements returns tenant tables for content_layout=extent
// files: JuiceFS SQL-engine tables (prefixed jfs_) plus compact/GC queues.
// file_nodes.extent_ino / content_layout are added as ALTERs so existing
// tenants pick them up; ExecSchemaStatements ignores duplicate-column errors.
func ExtentTiDBSchemaStatements() []string {
	return []string{
		`ALTER TABLE file_nodes ADD COLUMN content_layout VARCHAR(32) NOT NULL DEFAULT ''`,
		`ALTER TABLE file_nodes ADD COLUMN extent_ino BIGINT UNSIGNED NULL`,
		`CREATE INDEX idx_file_nodes_extent_ino ON file_nodes(extent_ino)`,
		`ALTER TABLE contents ADD COLUMN content_layout VARCHAR(32) NOT NULL DEFAULT ''`,

		`CREATE TABLE IF NOT EXISTS jfs_setting (
			name  VARCHAR(255) NOT NULL PRIMARY KEY,
			value MEDIUMTEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_counter (
			name  VARCHAR(255) NOT NULL PRIMARY KEY,
			value BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_node (
			inode         BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			type          TINYINT UNSIGNED NOT NULL,
			flags         TINYINT UNSIGNED NOT NULL DEFAULT 0,
			mode          SMALLINT UNSIGNED NOT NULL,
			uid           INT UNSIGNED NOT NULL DEFAULT 0,
			gid           INT UNSIGNED NOT NULL DEFAULT 0,
			atime         BIGINT NOT NULL,
			mtime         BIGINT NOT NULL,
			ctime         BIGINT NOT NULL,
			atimensec     SMALLINT NOT NULL DEFAULT 0,
			mtimensec     SMALLINT NOT NULL DEFAULT 0,
			ctimensec     SMALLINT NOT NULL DEFAULT 0,
			nlink         INT UNSIGNED NOT NULL,
			length        BIGINT UNSIGNED NOT NULL DEFAULT 0,
			rdev          INT UNSIGNED NOT NULL DEFAULT 0,
			parent        BIGINT UNSIGNED NOT NULL DEFAULT 0,
			access_acl_id INT UNSIGNED NOT NULL DEFAULT 0,
			default_acl_id INT UNSIGNED NOT NULL DEFAULT 0,
			tier_id       TINYINT UNSIGNED NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_edge (
			id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			parent BIGINT UNSIGNED NOT NULL,
			name   VARBINARY(255) NOT NULL,
			inode  BIGINT UNSIGNED NOT NULL,
			type   TINYINT UNSIGNED NOT NULL,
			UNIQUE KEY uk_jfs_edge (parent, name),
			KEY idx_jfs_edge_inode (inode)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_symlink (
			inode  BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			target VARBINARY(4096) NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_chunk (
			id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			inode  BIGINT UNSIGNED NOT NULL,
			indx   INT UNSIGNED NOT NULL,
			slices LONGBLOB NOT NULL,
			UNIQUE KEY uk_jfs_chunk (inode, indx)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_chunk_ref (
			chunkid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			size    INT UNSIGNED NOT NULL,
			refs    INT NOT NULL,
			KEY idx_jfs_chunk_ref_refs (refs)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_delslices (
			chunkid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			deleted BIGINT NOT NULL,
			slices  LONGBLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_session2 (
			sid    BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			expire BIGINT NOT NULL,
			info   BLOB NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_sustained (
			id    BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			sid   BIGINT UNSIGNED NOT NULL,
			inode BIGINT UNSIGNED NOT NULL,
			UNIQUE KEY uk_jfs_sustained (sid, inode)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_delfile (
			inode  BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			length BIGINT UNSIGNED NOT NULL,
			expire BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_flock (
			id    BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			inode BIGINT UNSIGNED NOT NULL,
			sid   BIGINT UNSIGNED NOT NULL,
			owner BIGINT NOT NULL,
			ltype TINYINT UNSIGNED NOT NULL,
			UNIQUE KEY uk_jfs_flock (inode, sid, owner)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_plock (
			id      BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			inode   BIGINT UNSIGNED NOT NULL,
			sid     BIGINT UNSIGNED NOT NULL,
			owner   BIGINT NOT NULL,
			records BLOB NOT NULL,
			UNIQUE KEY uk_jfs_plock (inode, sid, owner)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_dir_stats (
			inode       BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			data_length BIGINT NOT NULL DEFAULT 0,
			used_space  BIGINT NOT NULL DEFAULT 0,
			used_inodes BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS slice_compact_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			extent_ino    BIGINT UNSIGNED NOT NULL,
			chunk         INT UNSIGNED NOT NULL,
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
		`CREATE UNIQUE INDEX uk_slice_compact_ino_chunk ON slice_compact_tasks(extent_ino, chunk)`,
		`CREATE INDEX idx_slice_compact_claim ON slice_compact_tasks(status, available_at, lease_until, created_at)`,
		`CREATE TABLE IF NOT EXISTS block_gc_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			block_key     VARCHAR(512) NOT NULL,
			extent_ino    BIGINT UNSIGNED NULL,
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
	}
}

// ExtentDB9SchemaStatements is the PostgreSQL/db9 equivalent of
// ExtentTiDBSchemaStatements.
func ExtentDB9SchemaStatements() []string {
	return []string{
		`ALTER TABLE file_nodes ADD COLUMN IF NOT EXISTS content_layout VARCHAR(32) NOT NULL DEFAULT ''`,
		`ALTER TABLE file_nodes ADD COLUMN IF NOT EXISTS extent_ino BIGINT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_file_nodes_extent_ino ON file_nodes(extent_ino)`,
		`ALTER TABLE contents ADD COLUMN IF NOT EXISTS content_layout VARCHAR(32) NOT NULL DEFAULT ''`,

		`CREATE TABLE IF NOT EXISTS jfs_setting (
			name  VARCHAR(255) NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_counter (
			name  VARCHAR(255) NOT NULL PRIMARY KEY,
			value BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_node (
			inode          BIGINT NOT NULL PRIMARY KEY,
			type           SMALLINT NOT NULL,
			flags          SMALLINT NOT NULL DEFAULT 0,
			mode           SMALLINT NOT NULL,
			uid            INT NOT NULL DEFAULT 0,
			gid            INT NOT NULL DEFAULT 0,
			atime          BIGINT NOT NULL,
			mtime          BIGINT NOT NULL,
			ctime          BIGINT NOT NULL,
			atimensec      SMALLINT NOT NULL DEFAULT 0,
			mtimensec      SMALLINT NOT NULL DEFAULT 0,
			ctimensec      SMALLINT NOT NULL DEFAULT 0,
			nlink          INT NOT NULL,
			length         BIGINT NOT NULL DEFAULT 0,
			rdev           INT NOT NULL DEFAULT 0,
			parent         BIGINT NOT NULL DEFAULT 0,
			access_acl_id  INT NOT NULL DEFAULT 0,
			default_acl_id INT NOT NULL DEFAULT 0,
			tier_id        SMALLINT NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_edge (
			id     BIGSERIAL PRIMARY KEY,
			parent BIGINT NOT NULL,
			name   BYTEA NOT NULL,
			inode  BIGINT NOT NULL,
			type   SMALLINT NOT NULL,
			UNIQUE (parent, name)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jfs_edge_inode ON jfs_edge(inode)`,
		`CREATE TABLE IF NOT EXISTS jfs_symlink (
			inode  BIGINT NOT NULL PRIMARY KEY,
			target BYTEA NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_chunk (
			id     BIGSERIAL PRIMARY KEY,
			inode  BIGINT NOT NULL,
			indx   INT NOT NULL,
			slices BYTEA NOT NULL,
			UNIQUE (inode, indx)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_chunk_ref (
			chunkid BIGINT NOT NULL PRIMARY KEY,
			size    INT NOT NULL,
			refs    INT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jfs_chunk_ref_refs ON jfs_chunk_ref(refs)`,
		`CREATE TABLE IF NOT EXISTS jfs_delslices (
			chunkid BIGINT NOT NULL PRIMARY KEY,
			deleted BIGINT NOT NULL,
			slices  BYTEA NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_session2 (
			sid    BIGINT NOT NULL PRIMARY KEY,
			expire BIGINT NOT NULL,
			info   BYTEA NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_sustained (
			id    BIGSERIAL PRIMARY KEY,
			sid   BIGINT NOT NULL,
			inode BIGINT NOT NULL,
			UNIQUE (sid, inode)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_delfile (
			inode  BIGINT NOT NULL PRIMARY KEY,
			length BIGINT NOT NULL,
			expire BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_flock (
			id    BIGSERIAL PRIMARY KEY,
			inode BIGINT NOT NULL,
			sid   BIGINT NOT NULL,
			owner BIGINT NOT NULL,
			ltype SMALLINT NOT NULL,
			UNIQUE (inode, sid, owner)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_plock (
			id      BIGSERIAL PRIMARY KEY,
			inode   BIGINT NOT NULL,
			sid     BIGINT NOT NULL,
			owner   BIGINT NOT NULL,
			records BYTEA NOT NULL,
			UNIQUE (inode, sid, owner)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_dir_stats (
			inode       BIGINT NOT NULL PRIMARY KEY,
			data_length BIGINT NOT NULL DEFAULT 0,
			used_space  BIGINT NOT NULL DEFAULT 0,
			used_inodes BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS slice_compact_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			extent_ino    BIGINT NOT NULL,
			chunk         INT NOT NULL,
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
		`CREATE UNIQUE INDEX IF NOT EXISTS uk_slice_compact_ino_chunk ON slice_compact_tasks(extent_ino, chunk)`,
		`CREATE INDEX IF NOT EXISTS idx_slice_compact_claim ON slice_compact_tasks(status, available_at, lease_until, created_at)`,
		`CREATE TABLE IF NOT EXISTS block_gc_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			block_key     VARCHAR(512) NOT NULL,
			extent_ino    BIGINT,
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
	}
}
