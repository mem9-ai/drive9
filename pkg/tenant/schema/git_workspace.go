package schema

// GitWorkspaceTiDBSchemaStatements returns the tenant-local tables used by
// drive9 git fast workspaces. They intentionally sit beside file_nodes rather
// than reusing it: clean git objects are not drive9 file contents until they
// become dirty overlay entries.
func GitWorkspaceTiDBSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS git_workspaces (
			workspace_id VARCHAR(64) PRIMARY KEY,
			root_path    VARCHAR(512) NOT NULL,
			repo_url     TEXT NOT NULL,
			remote_name  VARCHAR(128) NOT NULL DEFAULT 'origin',
			branch_name  VARCHAR(255) NOT NULL DEFAULT '',
			base_commit  VARCHAR(64) NOT NULL DEFAULT '',
			head_commit  VARCHAR(64) NOT NULL DEFAULT '',
			mode         VARCHAR(32) NOT NULL DEFAULT 'fast',
			workspace_kind VARCHAR(16) NOT NULL DEFAULT 'main',
			common_workspace_id VARCHAR(64) NOT NULL DEFAULT '',
			worktree_name VARCHAR(255) NOT NULL DEFAULT '',
			gitdir_rel VARCHAR(1024) NOT NULL DEFAULT '',
			status       VARCHAR(32) NOT NULL DEFAULT 'active',
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		// Repair existing git_workspaces tables created before linked worktree
		// metadata existed. ExecSchemaStatementsContext tolerates duplicate
		// column errors for fresh schemas.
		`ALTER TABLE git_workspaces ADD COLUMN workspace_kind VARCHAR(16) NOT NULL DEFAULT 'main'`,
		`ALTER TABLE git_workspaces ADD COLUMN common_workspace_id VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE git_workspaces ADD COLUMN worktree_name VARCHAR(255) NOT NULL DEFAULT ''`,
		`ALTER TABLE git_workspaces ADD COLUMN gitdir_rel VARCHAR(1024) NOT NULL DEFAULT ''`,
		`CREATE UNIQUE INDEX uk_git_workspaces_root ON git_workspaces(root_path)`,
		`CREATE INDEX idx_git_workspaces_status ON git_workspaces(status, updated_at)`,
		`CREATE INDEX idx_git_workspaces_common ON git_workspaces(common_workspace_id, status)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_tree_nodes (
			workspace_id VARCHAR(64) NOT NULL,
			commit_sha   VARCHAR(64) NOT NULL,
			path         VARCHAR(1024) NOT NULL,
			path_hash    VARCHAR(64) NOT NULL,
			parent_path  VARCHAR(1024) NOT NULL DEFAULT '',
			parent_path_hash VARCHAR(64) NOT NULL DEFAULT '',
			name         VARCHAR(255) NOT NULL,
			kind         VARCHAR(16) NOT NULL,
			mode         VARCHAR(16) NOT NULL,
			object_sha   VARCHAR(64) NOT NULL DEFAULT '',
			size_bytes   BIGINT NOT NULL DEFAULT -1,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (workspace_id, commit_sha, path_hash)
		)`,
		`CREATE INDEX idx_git_tree_parent ON git_workspace_tree_nodes(workspace_id, commit_sha, parent_path_hash)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_git_state (
			workspace_id      VARCHAR(64) PRIMARY KEY,
			checkpoint_commit VARCHAR(64) NOT NULL DEFAULT '',
			storage_type      VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref       TEXT NOT NULL,
			storage_ref_hash  VARCHAR(64) NOT NULL DEFAULT '',
			checksum_sha256   VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes        BIGINT NOT NULL DEFAULT 0,
			content_blob      LONGBLOB,
			created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_git_state_ref_hash ON git_workspace_git_state(storage_ref_hash)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_object_packs (
			workspace_id    VARCHAR(64) NOT NULL,
			pack_id         VARCHAR(64) NOT NULL,
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			content_blob    LONGBLOB,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (workspace_id, pack_id)
		)`,
		`CREATE INDEX idx_git_object_packs_created ON git_workspace_object_packs(workspace_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_overlay (
			workspace_id    VARCHAR(64) NOT NULL,
			path            VARCHAR(1024) NOT NULL,
			path_hash       VARCHAR(64) NOT NULL,
			op              VARCHAR(16) NOT NULL,
			kind            VARCHAR(16) NOT NULL DEFAULT 'file',
			mode            VARCHAR(16) NOT NULL DEFAULT '',
			storage_type    VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref     TEXT NOT NULL,
			storage_ref_hash VARCHAR(64) NOT NULL DEFAULT '',
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			base_object_sha VARCHAR(64) NOT NULL DEFAULT '',
			content_blob    LONGBLOB,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (workspace_id, path_hash)
		)`,
		`CREATE INDEX idx_git_overlay_op ON git_workspace_overlay(workspace_id, op)`,
	}
}

// GitWorkspaceTiDBSharedSchemaStatements returns the git workspace DDL for the
// shared (multi-tenant) schema shape on TiDB (docs/TENANT_DB_REDESIGN.md
// §5.2.3): the same tables as GitWorkspaceTiDBSchemaStatements, but every
// table gains an fs_id BIGINT discriminator as its first column and every
// primary key, unique key, and index is prefixed with fs_id so one tenant's
// rows stay physically co-located. Composite primary keys are declared
// CLUSTERED — TiDB creates them NONCLUSTERED by default. The standalone
// repair-style ALTER TABLE ... ADD COLUMN statements are folded into the
// CREATE TABLE here: shared schemas are created fresh, so there is nothing to
// repair. For plain MySQL use GitWorkspaceMySQLSharedSchemaStatements (same
// shape without the keyword). Keep both in lockstep with
// GitWorkspaceTiDBSchemaStatements — the drift test in git_shared_test.go
// enforces parity.
func GitWorkspaceTiDBSharedSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS git_workspaces (
			fs_id BIGINT NOT NULL,
			workspace_id VARCHAR(64) NOT NULL,
			root_path    VARCHAR(512) NOT NULL,
			repo_url     TEXT NOT NULL,
			remote_name  VARCHAR(128) NOT NULL DEFAULT 'origin',
			branch_name  VARCHAR(255) NOT NULL DEFAULT '',
			base_commit  VARCHAR(64) NOT NULL DEFAULT '',
			head_commit  VARCHAR(64) NOT NULL DEFAULT '',
			mode         VARCHAR(32) NOT NULL DEFAULT 'fast',
			workspace_kind VARCHAR(16) NOT NULL DEFAULT 'main',
			common_workspace_id VARCHAR(64) NOT NULL DEFAULT '',
			worktree_name VARCHAR(255) NOT NULL DEFAULT '',
			gitdir_rel VARCHAR(1024) NOT NULL DEFAULT '',
			status       VARCHAR(32) NOT NULL DEFAULT 'active',
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (fs_id, workspace_id) CLUSTERED,
			UNIQUE KEY uk_git_workspaces_root (fs_id, root_path),
			KEY idx_git_workspaces_status (fs_id, status, updated_at),
			KEY idx_git_workspaces_common (fs_id, common_workspace_id, status)
		)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_tree_nodes (
			fs_id BIGINT NOT NULL,
			workspace_id VARCHAR(64) NOT NULL,
			commit_sha   VARCHAR(64) NOT NULL,
			path         VARCHAR(1024) NOT NULL,
			path_hash    VARCHAR(64) NOT NULL,
			parent_path  VARCHAR(1024) NOT NULL DEFAULT '',
			parent_path_hash VARCHAR(64) NOT NULL DEFAULT '',
			name         VARCHAR(255) NOT NULL,
			kind         VARCHAR(16) NOT NULL,
			mode         VARCHAR(16) NOT NULL,
			object_sha   VARCHAR(64) NOT NULL DEFAULT '',
			size_bytes   BIGINT NOT NULL DEFAULT -1,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (fs_id, workspace_id, commit_sha, path_hash) CLUSTERED,
			KEY idx_git_tree_parent (fs_id, workspace_id, commit_sha, parent_path_hash)
		)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_git_state (
			fs_id BIGINT NOT NULL,
			workspace_id      VARCHAR(64) NOT NULL,
			checkpoint_commit VARCHAR(64) NOT NULL DEFAULT '',
			storage_type      VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref       TEXT NOT NULL,
			storage_ref_hash  VARCHAR(64) NOT NULL DEFAULT '',
			checksum_sha256   VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes        BIGINT NOT NULL DEFAULT 0,
			content_blob      LONGBLOB,
			created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (fs_id, workspace_id) CLUSTERED,
			KEY idx_git_state_ref_hash (fs_id, storage_ref_hash)
		)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_object_packs (
			fs_id BIGINT NOT NULL,
			workspace_id    VARCHAR(64) NOT NULL,
			pack_id         VARCHAR(64) NOT NULL,
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			content_blob    LONGBLOB,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (fs_id, workspace_id, pack_id) CLUSTERED,
			KEY idx_git_object_packs_created (fs_id, workspace_id, created_at)
		)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_overlay (
			fs_id BIGINT NOT NULL,
			workspace_id    VARCHAR(64) NOT NULL,
			path            VARCHAR(1024) NOT NULL,
			path_hash       VARCHAR(64) NOT NULL,
			op              VARCHAR(16) NOT NULL,
			kind            VARCHAR(16) NOT NULL DEFAULT 'file',
			mode            VARCHAR(16) NOT NULL DEFAULT '',
			storage_type    VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref     TEXT NOT NULL,
			storage_ref_hash VARCHAR(64) NOT NULL DEFAULT '',
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			base_object_sha VARCHAR(64) NOT NULL DEFAULT '',
			content_blob    LONGBLOB,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (fs_id, workspace_id, path_hash) CLUSTERED,
			KEY idx_git_overlay_op (fs_id, workspace_id, op)
		)`,
	}
}

// GitWorkspaceMySQLSharedSchemaStatements is the plain-MySQL variant of
// GitWorkspaceTiDBSharedSchemaStatements, derived by removing TiDB-only
// keywords. Use it for local development databases and MySQL-backed tests/e2e.
func GitWorkspaceMySQLSharedSchemaStatements() []string {
	return mysqlCompatibleSharedStatements(GitWorkspaceTiDBSharedSchemaStatements())
}

// GitWorkspaceDB9SchemaStatements is the PostgreSQL/db9 equivalent of
// GitWorkspaceTiDBSchemaStatements.
func GitWorkspaceDB9SchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS git_workspaces (
			workspace_id VARCHAR(64) PRIMARY KEY,
			root_path    VARCHAR(512) NOT NULL,
			repo_url     TEXT NOT NULL,
			remote_name  VARCHAR(128) NOT NULL DEFAULT 'origin',
			branch_name  VARCHAR(255) NOT NULL DEFAULT '',
			base_commit  VARCHAR(64) NOT NULL DEFAULT '',
			head_commit  VARCHAR(64) NOT NULL DEFAULT '',
			mode         VARCHAR(32) NOT NULL DEFAULT 'fast',
			workspace_kind VARCHAR(16) NOT NULL DEFAULT 'main',
			common_workspace_id VARCHAR(64) NOT NULL DEFAULT '',
			worktree_name VARCHAR(255) NOT NULL DEFAULT '',
			gitdir_rel VARCHAR(1024) NOT NULL DEFAULT '',
			status       VARCHAR(32) NOT NULL DEFAULT 'active',
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		// Repair existing git_workspaces tables created before linked worktree
		// metadata existed. ExecSchemaStatementsContext tolerates duplicate
		// column errors for fresh schemas.
		`ALTER TABLE git_workspaces ADD COLUMN workspace_kind VARCHAR(16) NOT NULL DEFAULT 'main'`,
		`ALTER TABLE git_workspaces ADD COLUMN common_workspace_id VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE git_workspaces ADD COLUMN worktree_name VARCHAR(255) NOT NULL DEFAULT ''`,
		`ALTER TABLE git_workspaces ADD COLUMN gitdir_rel VARCHAR(1024) NOT NULL DEFAULT ''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uk_git_workspaces_root ON git_workspaces(root_path)`,
		`CREATE INDEX IF NOT EXISTS idx_git_workspaces_status ON git_workspaces(status, updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_git_workspaces_common ON git_workspaces(common_workspace_id, status)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_tree_nodes (
			workspace_id VARCHAR(64) NOT NULL,
			commit_sha   VARCHAR(64) NOT NULL,
			path         VARCHAR(1024) NOT NULL,
			path_hash    VARCHAR(64) NOT NULL,
			parent_path  VARCHAR(1024) NOT NULL DEFAULT '',
			parent_path_hash VARCHAR(64) NOT NULL DEFAULT '',
			name         VARCHAR(255) NOT NULL,
			kind         VARCHAR(16) NOT NULL,
			mode         VARCHAR(16) NOT NULL,
			object_sha   VARCHAR(64) NOT NULL DEFAULT '',
			size_bytes   BIGINT NOT NULL DEFAULT -1,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (workspace_id, commit_sha, path_hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_git_tree_parent ON git_workspace_tree_nodes(workspace_id, commit_sha, parent_path_hash)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_git_state (
			workspace_id      VARCHAR(64) PRIMARY KEY,
			checkpoint_commit VARCHAR(64) NOT NULL DEFAULT '',
			storage_type      VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref       TEXT NOT NULL,
			storage_ref_hash  VARCHAR(64) NOT NULL DEFAULT '',
			checksum_sha256   VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes        BIGINT NOT NULL DEFAULT 0,
			content_blob      BYTEA,
			created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_git_state_ref_hash ON git_workspace_git_state(storage_ref_hash)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_object_packs (
			workspace_id    VARCHAR(64) NOT NULL,
			pack_id         VARCHAR(64) NOT NULL,
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			content_blob    BYTEA,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (workspace_id, pack_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_git_object_packs_created ON git_workspace_object_packs(workspace_id, created_at)`,

		`CREATE TABLE IF NOT EXISTS git_workspace_overlay (
			workspace_id    VARCHAR(64) NOT NULL,
			path            VARCHAR(1024) NOT NULL,
			path_hash       VARCHAR(64) NOT NULL,
			op              VARCHAR(16) NOT NULL,
			kind            VARCHAR(16) NOT NULL DEFAULT 'file',
			mode            VARCHAR(16) NOT NULL DEFAULT '',
			storage_type    VARCHAR(32) NOT NULL DEFAULT '',
			storage_ref     TEXT NOT NULL,
			storage_ref_hash VARCHAR(64) NOT NULL DEFAULT '',
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			base_object_sha VARCHAR(64) NOT NULL DEFAULT '',
			content_blob    BYTEA,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (workspace_id, path_hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_git_overlay_op ON git_workspace_overlay(workspace_id, op)`,
	}
}
