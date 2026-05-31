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
