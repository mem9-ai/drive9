package schema

// PromotionTiDBSchemaStatements returns the tenant-local tables used by the
// P0 LocalEphemeral-to-RemoteManaged import protocol. Hidden import rows live
// outside file_nodes/inodes/contents so ordinary namespace reads cannot observe
// a partial tree before CommitImport publishes it.
func PromotionTiDBSchemaStatements() []string {
	return []string{
		`ALTER TABLE fs_events ADD COLUMN promotion_migration_id VARCHAR(64) NULL`,
		`ALTER TABLE fs_events ADD COLUMN promotion_target_path TEXT NULL`,
		`ALTER TABLE fs_events ADD COLUMN promotion_tree_generation BIGINT UNSIGNED NULL`,
		`CREATE UNIQUE INDEX uk_fs_events_promotion_migration ON fs_events(promotion_migration_id)`,
		`CREATE TABLE IF NOT EXISTS promotion_storage_capabilities (
			tenant_id                    VARCHAR(64) PRIMARY KEY,
			generation                   BIGINT UNSIGNED NOT NULL DEFAULT 0,
			inline_enabled               BOOLEAN NOT NULL DEFAULT FALSE,
			inline_threshold_bytes       BIGINT UNSIGNED NOT NULL DEFAULT 0,
			allowed_mode                 VARCHAR(32) NOT NULL DEFAULT 'disabled',
			config_generation            BIGINT UNSIGNED NOT NULL DEFAULT 0,
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_namespace_capabilities (
			tenant_id                    VARCHAR(64) PRIMARY KEY,
			namespace_cas_ready          BOOLEAN NOT NULL DEFAULT FALSE,
			namespace_cas_epoch          BIGINT UNSIGNED NOT NULL DEFAULT 0,
			minimum_writer_protocol      BIGINT UNSIGNED NOT NULL DEFAULT 0,
			restore_generation           BIGINT UNSIGNED NOT NULL DEFAULT 0,
			database_incarnation         VARCHAR(128) NOT NULL DEFAULT '',
			writer_generation            BIGINT UNSIGNED NOT NULL DEFAULT 0,
			admission_state              VARCHAR(32) NOT NULL DEFAULT 'DISABLED',
			root_inode                   VARCHAR(64) NOT NULL DEFAULT '',
			root_edge_incarnation        VARCHAR(128) NOT NULL DEFAULT '',
			root_children_generation     BIGINT UNSIGNED NOT NULL DEFAULT 0,
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`ALTER TABLE promotion_namespace_capabilities ADD COLUMN root_inode VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE promotion_namespace_capabilities ADD COLUMN root_edge_incarnation VARCHAR(128) NOT NULL DEFAULT ''`,
		`ALTER TABLE promotion_namespace_capabilities ADD COLUMN root_children_generation BIGINT UNSIGNED NOT NULL DEFAULT 0`,

		`CREATE TABLE IF NOT EXISTS promotion_import_identity_tenants (
			tenant_id                    VARCHAR(64) PRIMARY KEY,
			installed_allocation_epoch   BIGINT UNSIGNED NOT NULL DEFAULT 0,
			installed_restore_generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
			installed_database_incarnation VARCHAR(128) NOT NULL DEFAULT '',
			installed_backup_lineage_id  VARCHAR(128) NOT NULL DEFAULT '',
			installed_writer_generation  BIGINT UNSIGNED NOT NULL DEFAULT 0,
			installed_allocation_lease_generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
			restore_admission_state      VARCHAR(32) NOT NULL DEFAULT 'DISABLED',
			live_claim_count             BIGINT UNSIGNED NOT NULL DEFAULT 0,
			materialized_identity_count  BIGINT UNSIGNED NOT NULL DEFAULT 0,
			rate_bucket_state            LONGTEXT,
			max_live_claims              BIGINT UNSIGNED NOT NULL DEFAULT 0,
			max_materialized_identities  BIGINT UNSIGNED NOT NULL DEFAULT 0,
			max_sequence_window          BIGINT UNSIGNED NOT NULL DEFAULT 0,
			allocation_rate_per_minute   BIGINT UNSIGNED NOT NULL DEFAULT 0,
			allocation_rate_burst        BIGINT UNSIGNED NOT NULL DEFAULT 0,
			config_generation            BIGINT UNSIGNED NOT NULL DEFAULT 0,
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`ALTER TABLE promotion_import_identity_tenants ADD COLUMN installed_allocation_lease_generation BIGINT UNSIGNED NOT NULL DEFAULT 0`,
		`ALTER TABLE promotion_import_identity_tenants ADD COLUMN max_live_claims BIGINT UNSIGNED NOT NULL DEFAULT 0`,
		`ALTER TABLE promotion_import_identity_tenants ADD COLUMN max_materialized_identities BIGINT UNSIGNED NOT NULL DEFAULT 0`,
		`ALTER TABLE promotion_import_identity_tenants ADD COLUMN max_sequence_window BIGINT UNSIGNED NOT NULL DEFAULT 0`,
		`ALTER TABLE promotion_import_identity_tenants ADD COLUMN allocation_rate_per_minute BIGINT UNSIGNED NOT NULL DEFAULT 0`,
		`ALTER TABLE promotion_import_identity_tenants ADD COLUMN allocation_rate_burst BIGINT UNSIGNED NOT NULL DEFAULT 0`,

		`CREATE TABLE IF NOT EXISTS promotion_import_identity_epochs (
			tenant_id                    VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			last_issued_sequence         BIGINT UNSIGNED NOT NULL DEFAULT 0,
			retired_through              BIGINT UNSIGNED NOT NULL DEFAULT 0,
			id_codec_version             VARCHAR(16) NOT NULL DEFAULT 'p1',
			epoch_state                  VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
			created_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			sealed_at                    DATETIME(3),
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, allocation_epoch)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_import_identity_global (
			capacity_key                  VARCHAR(64) PRIMARY KEY,
			materialized_identity_count  BIGINT UNSIGNED NOT NULL DEFAULT 0,
			max_materialized_identities  BIGINT UNSIGNED NOT NULL DEFAULT 0,
			config_generation            BIGINT UNSIGNED NOT NULL DEFAULT 0,
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`ALTER TABLE promotion_import_identity_global ADD COLUMN max_materialized_identities BIGINT UNSIGNED NOT NULL DEFAULT 0`,

		`CREATE TABLE IF NOT EXISTS promotion_import_id_claims (
			tenant_id                    VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			allocation_sequence          BIGINT UNSIGNED NOT NULL,
			target_path                  TEXT NOT NULL,
			target_path_hash             VARCHAR(64) NOT NULL,
			expected_target_absent       BOOLEAN NOT NULL DEFAULT TRUE,
			allocation_idempotency_key_hash VARCHAR(64) NOT NULL,
			allocation_request_digest    VARCHAR(64) NOT NULL,
			allocation_proof_blob        LONGTEXT NOT NULL,
			allocation_proof_digest      VARCHAR(64) NOT NULL,
			create_before                DATETIME(3) NOT NULL,
			claim_state                  VARCHAR(32) NOT NULL,
			accepted_create_request_digest VARCHAR(64),
			retired_at                   DATETIME(3),
			created_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence),
			UNIQUE KEY uk_promotion_claim_idempotency (tenant_id, allocation_idempotency_key_hash),
			KEY idx_promotion_claim_target (tenant_id, target_path_hash, claim_state),
			KEY idx_promotion_claim_expiry (tenant_id, claim_state, create_before)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_imports (
			tenant_id                    VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			allocation_sequence          BIGINT UNSIGNED NOT NULL,
			allocation_proof_digest      VARCHAR(64) NOT NULL,
			target_path                  TEXT NOT NULL,
			target_path_hash             VARCHAR(64) NOT NULL,
			target_parent_inode          VARCHAR(64) NOT NULL,
			target_parent_path           TEXT NOT NULL,
			target_parent_path_hash      VARCHAR(64) NOT NULL,
			target_parent_edge_incarnation VARCHAR(128) NOT NULL,
			target_parent_children_generation BIGINT UNSIGNED NOT NULL,
			manifest_hash                VARCHAR(64) NOT NULL,
			entry_total                  BIGINT UNSIGNED NOT NULL,
			byte_total                   BIGINT UNSIGNED NOT NULL,
			max_content_size             BIGINT UNSIGNED NOT NULL,
			storage_mode                 VARCHAR(32) NOT NULL,
			storage_plan_digest          VARCHAR(64) NOT NULL,
			backend_capability_generation BIGINT UNSIGNED NOT NULL,
			inline_threshold             BIGINT UNSIGNED NOT NULL,
			plan_expires_at              DATETIME(3) NOT NULL,
			create_request_digest        VARCHAR(64) NOT NULL,
			namespace_cas_epoch          BIGINT UNSIGNED NOT NULL,
			accepted_restore_generation  BIGINT UNSIGNED NOT NULL,
			accepted_database_incarnation VARCHAR(128) NOT NULL,
			accepted_writer_generation   BIGINT UNSIGNED NOT NULL,
			quota_reservation_id         VARCHAR(64) NOT NULL,
			state                        VARCHAR(32) NOT NULL,
			state_version                BIGINT UNSIGNED NOT NULL DEFAULT 1,
			owner_epoch                  BIGINT UNSIGNED NOT NULL DEFAULT 1,
			owner_token_hash             VARCHAR(64) NOT NULL,
			recovery_token_hash          VARCHAR(64) NOT NULL,
			activity_deadline            DATETIME(3) NOT NULL,
			lease_expires_at             DATETIME(3) NOT NULL,
			commit_attempt_id            VARCHAR(64),
			commit_writer_generation     BIGINT UNSIGNED,
			cleanup_attempt_id           VARCHAR(64),
			cleanup_writer_generation    BIGINT UNSIGNED,
			terminal_reason              VARCHAR(128),
			committed_root_inode         VARCHAR(64),
			committed_generation         BIGINT UNSIGNED,
			terminal_result_blob         LONGTEXT,
			terminal_result_digest       VARCHAR(64),
			commit_containment_lineage_id VARCHAR(128),
			commit_containment_position  VARCHAR(255),
			commit_containment_checkpoint_digest VARCHAR(64),
			source_release_state         VARCHAR(32) NOT NULL DEFAULT 'NONE',
			source_release_certificate_digest VARCHAR(64),
			source_release_floor_checkpoint LONGTEXT,
			terminal_at                  DATETIME(3),
			result_acknowledged_at       DATETIME(3),
			full_row_compact_not_before  DATETIME(3),
			retire_after                 DATETIME(3),
			created_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence),
			KEY idx_promotion_import_target (tenant_id, target_path_hash, state),
			KEY idx_promotion_import_worker (tenant_id, state, lease_expires_at),
			KEY idx_promotion_import_retire (tenant_id, state, retire_after),
			CONSTRAINT chk_promotion_import_aborting_owner CHECK (
				state <> 'ABORTING' OR
				(cleanup_attempt_id IS NOT NULL AND cleanup_writer_generation IS NOT NULL AND terminal_reason IS NOT NULL)
			)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_import_entries (
			tenant_id                    VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			allocation_sequence          BIGINT UNSIGNED NOT NULL,
			relative_path_hash           VARCHAR(64) NOT NULL,
			relative_path                TEXT NOT NULL,
			entry_type                   VARCHAR(16) NOT NULL,
			mode                         INT UNSIGNED NOT NULL,
			mtime_ns                     BIGINT NOT NULL,
			symlink_target               TEXT,
			expected_size_bytes          BIGINT UNSIGNED NOT NULL DEFAULT 0,
			expected_checksum_sha256     VARCHAR(64) NOT NULL DEFAULT '',
			metadata_blob                LONGTEXT,
			entry_hash                   VARCHAR(64) NOT NULL,
			import_content_id            VARCHAR(64),
			created_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence, relative_path_hash),
			KEY idx_promotion_entry_content (tenant_id, allocation_epoch, allocation_sequence, import_content_id)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_import_contents (
			tenant_id                    VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			allocation_sequence          BIGINT UNSIGNED NOT NULL,
			import_content_id            VARCHAR(64) NOT NULL,
			inline_content_blob          LONGBLOB,
			inline_content_idempotency_key VARCHAR(128),
			inline_request_digest        VARCHAR(64),
			sealed_storage_ref           TEXT,
			sealed_storage_version       VARCHAR(255),
			size_bytes                   BIGINT UNSIGNED NOT NULL,
			checksum_sha256              VARCHAR(64) NOT NULL,
			accepted_seal_attempt_id     VARCHAR(64),
			seal_state                   VARCHAR(32) NOT NULL DEFAULT 'INLINE',
			ownership_state              VARCHAR(32) NOT NULL DEFAULT 'STAGED',
			created_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence, import_content_id),
			UNIQUE KEY uk_promotion_content_idempotency (tenant_id, allocation_epoch, allocation_sequence, inline_content_idempotency_key),
			KEY idx_promotion_content_gc (tenant_id, ownership_state, updated_at)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_quota_reservations (
			tenant_id                    VARCHAR(64) NOT NULL,
			reservation_id               VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			allocation_sequence          BIGINT UNSIGNED NOT NULL,
			reserved_bytes               BIGINT UNSIGNED NOT NULL,
			reserved_files               BIGINT UNSIGNED NOT NULL,
			state                        VARCHAR(32) NOT NULL,
			state_version                BIGINT UNSIGNED NOT NULL DEFAULT 1,
			created_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, reservation_id),
			UNIQUE KEY uk_promotion_quota_import (tenant_id, allocation_epoch, allocation_sequence),
			KEY idx_promotion_quota_state (tenant_id, state, updated_at)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_quota_accounts (
			tenant_id                    VARCHAR(64) PRIMARY KEY,
			max_bytes                    BIGINT UNSIGNED NOT NULL,
			max_files                    BIGINT UNSIGNED NOT NULL,
			reserved_bytes               BIGINT UNSIGNED NOT NULL DEFAULT 0,
			reserved_files               BIGINT UNSIGNED NOT NULL DEFAULT 0,
			committed_bytes              BIGINT UNSIGNED NOT NULL DEFAULT 0,
			committed_files              BIGINT UNSIGNED NOT NULL DEFAULT 0,
			config_generation            BIGINT UNSIGNED NOT NULL DEFAULT 0,
			updated_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_import_tombstones (
			tenant_id                    VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			allocation_sequence          BIGINT UNSIGNED NOT NULL,
			allocation_proof_digest      VARCHAR(64) NOT NULL,
			target_path                  TEXT NOT NULL,
			target_path_hash             VARCHAR(64) NOT NULL,
			request_digest               VARCHAR(64) NOT NULL,
			owner_token_hash             VARCHAR(64) NOT NULL,
			recovery_token_hash          VARCHAR(64) NOT NULL,
			terminal_state               VARCHAR(32) NOT NULL,
			terminal_result_blob         LONGTEXT NOT NULL,
			terminal_result_digest       VARCHAR(64) NOT NULL,
			commit_containment_lineage_id VARCHAR(128),
			commit_containment_position  VARCHAR(255),
			commit_containment_checkpoint_digest VARCHAR(64),
			source_release_state         VARCHAR(32) NOT NULL DEFAULT 'NONE',
			source_release_certificate_digest VARCHAR(64),
			source_release_floor_checkpoint LONGTEXT,
			retire_after                 DATETIME(3) NOT NULL,
			created_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence),
			KEY idx_promotion_tombstone_retire (tenant_id, retire_after)
		)`,

		`CREATE TABLE IF NOT EXISTS promotion_retired_import_sequences (
			tenant_id                    VARCHAR(64) NOT NULL,
			allocation_epoch             BIGINT UNSIGNED NOT NULL,
			allocation_sequence          BIGINT UNSIGNED NOT NULL,
			retired_at                   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence)
		)`,
	}
}

// PromotionDB9SchemaStatements is the PostgreSQL/db9 equivalent of
// PromotionTiDBSchemaStatements.
func PromotionDB9SchemaStatements() []string {
	return []string{
		`ALTER TABLE IF EXISTS fs_events ADD COLUMN IF NOT EXISTS promotion_migration_id VARCHAR(64) NULL`,
		`ALTER TABLE IF EXISTS fs_events ADD COLUMN IF NOT EXISTS promotion_target_path TEXT NULL`,
		`ALTER TABLE IF EXISTS fs_events ADD COLUMN IF NOT EXISTS promotion_tree_generation BIGINT NULL`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uk_fs_events_promotion_migration ON fs_events(promotion_migration_id)`,
		`CREATE TABLE IF NOT EXISTS promotion_storage_capabilities (
			tenant_id VARCHAR(64) PRIMARY KEY,
			generation BIGINT NOT NULL DEFAULT 0,
			inline_enabled BOOLEAN NOT NULL DEFAULT FALSE,
			inline_threshold_bytes BIGINT NOT NULL DEFAULT 0,
			allowed_mode VARCHAR(32) NOT NULL DEFAULT 'disabled',
			config_generation BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS promotion_namespace_capabilities (
			tenant_id VARCHAR(64) PRIMARY KEY,
			namespace_cas_ready BOOLEAN NOT NULL DEFAULT FALSE,
			namespace_cas_epoch BIGINT NOT NULL DEFAULT 0,
			minimum_writer_protocol BIGINT NOT NULL DEFAULT 0,
			restore_generation BIGINT NOT NULL DEFAULT 0,
			database_incarnation VARCHAR(128) NOT NULL DEFAULT '',
			writer_generation BIGINT NOT NULL DEFAULT 0,
			admission_state VARCHAR(32) NOT NULL DEFAULT 'DISABLED',
			root_inode VARCHAR(64) NOT NULL DEFAULT '',
			root_edge_incarnation VARCHAR(128) NOT NULL DEFAULT '',
			root_children_generation BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE IF EXISTS promotion_namespace_capabilities ADD COLUMN IF NOT EXISTS root_inode VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE IF EXISTS promotion_namespace_capabilities ADD COLUMN IF NOT EXISTS root_edge_incarnation VARCHAR(128) NOT NULL DEFAULT ''`,
		`ALTER TABLE IF EXISTS promotion_namespace_capabilities ADD COLUMN IF NOT EXISTS root_children_generation BIGINT NOT NULL DEFAULT 0`,
		`CREATE TABLE IF NOT EXISTS promotion_import_identity_tenants (
			tenant_id VARCHAR(64) PRIMARY KEY,
			installed_allocation_epoch BIGINT NOT NULL DEFAULT 0,
			installed_restore_generation BIGINT NOT NULL DEFAULT 0,
			installed_database_incarnation VARCHAR(128) NOT NULL DEFAULT '',
			installed_backup_lineage_id VARCHAR(128) NOT NULL DEFAULT '',
			installed_writer_generation BIGINT NOT NULL DEFAULT 0,
			installed_allocation_lease_generation BIGINT NOT NULL DEFAULT 0,
			restore_admission_state VARCHAR(32) NOT NULL DEFAULT 'DISABLED',
			live_claim_count BIGINT NOT NULL DEFAULT 0,
			materialized_identity_count BIGINT NOT NULL DEFAULT 0,
			rate_bucket_state JSONB,
			max_live_claims BIGINT NOT NULL DEFAULT 0,
			max_materialized_identities BIGINT NOT NULL DEFAULT 0,
			max_sequence_window BIGINT NOT NULL DEFAULT 0,
			allocation_rate_per_minute BIGINT NOT NULL DEFAULT 0,
			allocation_rate_burst BIGINT NOT NULL DEFAULT 0,
			config_generation BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE IF EXISTS promotion_import_identity_tenants ADD COLUMN IF NOT EXISTS installed_allocation_lease_generation BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE IF EXISTS promotion_import_identity_tenants ADD COLUMN IF NOT EXISTS max_live_claims BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE IF EXISTS promotion_import_identity_tenants ADD COLUMN IF NOT EXISTS max_materialized_identities BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE IF EXISTS promotion_import_identity_tenants ADD COLUMN IF NOT EXISTS max_sequence_window BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE IF EXISTS promotion_import_identity_tenants ADD COLUMN IF NOT EXISTS allocation_rate_per_minute BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE IF EXISTS promotion_import_identity_tenants ADD COLUMN IF NOT EXISTS allocation_rate_burst BIGINT NOT NULL DEFAULT 0`,
		`CREATE TABLE IF NOT EXISTS promotion_import_identity_epochs (
			tenant_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			last_issued_sequence BIGINT NOT NULL DEFAULT 0,
			retired_through BIGINT NOT NULL DEFAULT 0,
			id_codec_version VARCHAR(16) NOT NULL DEFAULT 'p1',
			epoch_state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			sealed_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, allocation_epoch)
		)`,
		`CREATE TABLE IF NOT EXISTS promotion_import_identity_global (
			capacity_key VARCHAR(64) PRIMARY KEY,
			materialized_identity_count BIGINT NOT NULL DEFAULT 0,
			max_materialized_identities BIGINT NOT NULL DEFAULT 0,
			config_generation BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE IF EXISTS promotion_import_identity_global ADD COLUMN IF NOT EXISTS max_materialized_identities BIGINT NOT NULL DEFAULT 0`,
		`CREATE TABLE IF NOT EXISTS promotion_import_id_claims (
			tenant_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			allocation_sequence BIGINT NOT NULL,
			target_path VARCHAR(4096) NOT NULL,
			target_path_hash VARCHAR(64) NOT NULL,
			expected_target_absent BOOLEAN NOT NULL DEFAULT TRUE,
			allocation_idempotency_key_hash VARCHAR(64) NOT NULL,
			allocation_request_digest VARCHAR(64) NOT NULL,
			allocation_proof_blob TEXT NOT NULL,
			allocation_proof_digest VARCHAR(64) NOT NULL,
			create_before TIMESTAMPTZ NOT NULL,
			claim_state VARCHAR(32) NOT NULL,
			accepted_create_request_digest VARCHAR(64),
			retired_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence),
			UNIQUE (tenant_id, allocation_idempotency_key_hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_claim_target ON promotion_import_id_claims(tenant_id, target_path_hash, claim_state)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_claim_expiry ON promotion_import_id_claims(tenant_id, claim_state, create_before)`,
		`CREATE TABLE IF NOT EXISTS promotion_imports (
			tenant_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			allocation_sequence BIGINT NOT NULL,
			allocation_proof_digest VARCHAR(64) NOT NULL,
			target_path VARCHAR(4096) NOT NULL,
			target_path_hash VARCHAR(64) NOT NULL,
			target_parent_inode VARCHAR(64) NOT NULL,
			target_parent_path VARCHAR(4096) NOT NULL,
			target_parent_path_hash VARCHAR(64) NOT NULL,
			target_parent_edge_incarnation VARCHAR(128) NOT NULL,
			target_parent_children_generation BIGINT NOT NULL,
			manifest_hash VARCHAR(64) NOT NULL,
			entry_total BIGINT NOT NULL,
			byte_total BIGINT NOT NULL,
			max_content_size BIGINT NOT NULL,
			storage_mode VARCHAR(32) NOT NULL,
			storage_plan_digest VARCHAR(64) NOT NULL,
			backend_capability_generation BIGINT NOT NULL,
			inline_threshold BIGINT NOT NULL,
			plan_expires_at TIMESTAMPTZ NOT NULL,
			create_request_digest VARCHAR(64) NOT NULL,
			namespace_cas_epoch BIGINT NOT NULL,
			accepted_restore_generation BIGINT NOT NULL,
			accepted_database_incarnation VARCHAR(128) NOT NULL,
			accepted_writer_generation BIGINT NOT NULL,
			quota_reservation_id VARCHAR(64) NOT NULL,
			state VARCHAR(32) NOT NULL,
			state_version BIGINT NOT NULL DEFAULT 1,
			owner_epoch BIGINT NOT NULL DEFAULT 1,
			owner_token_hash VARCHAR(64) NOT NULL,
			recovery_token_hash VARCHAR(64) NOT NULL,
			activity_deadline TIMESTAMPTZ NOT NULL,
			lease_expires_at TIMESTAMPTZ NOT NULL,
			commit_attempt_id VARCHAR(64),
			commit_writer_generation BIGINT,
			cleanup_attempt_id VARCHAR(64),
			cleanup_writer_generation BIGINT,
			terminal_reason VARCHAR(128),
			committed_root_inode VARCHAR(64),
			committed_generation BIGINT,
			terminal_result_blob TEXT,
			terminal_result_digest VARCHAR(64),
			commit_containment_lineage_id VARCHAR(128),
			commit_containment_position VARCHAR(255),
			commit_containment_checkpoint_digest VARCHAR(64),
			source_release_state VARCHAR(32) NOT NULL DEFAULT 'NONE',
			source_release_certificate_digest VARCHAR(64),
			source_release_floor_checkpoint JSONB,
			terminal_at TIMESTAMPTZ,
			result_acknowledged_at TIMESTAMPTZ,
			full_row_compact_not_before TIMESTAMPTZ,
			retire_after TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence),
			CONSTRAINT chk_promotion_import_aborting_owner CHECK (
				state <> 'ABORTING' OR
				(cleanup_attempt_id IS NOT NULL AND cleanup_writer_generation IS NOT NULL AND terminal_reason IS NOT NULL)
			)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_import_target ON promotion_imports(tenant_id, target_path_hash, state)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_import_worker ON promotion_imports(tenant_id, state, lease_expires_at)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_import_retire ON promotion_imports(tenant_id, state, retire_after)`,
		`CREATE TABLE IF NOT EXISTS promotion_import_entries (
			tenant_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			allocation_sequence BIGINT NOT NULL,
			relative_path_hash VARCHAR(64) NOT NULL,
			relative_path VARCHAR(4096) NOT NULL,
			entry_type VARCHAR(16) NOT NULL,
			mode BIGINT NOT NULL,
			mtime_ns BIGINT NOT NULL,
			symlink_target TEXT,
			expected_size_bytes BIGINT NOT NULL DEFAULT 0,
			expected_checksum_sha256 VARCHAR(64) NOT NULL DEFAULT '',
			metadata_blob JSONB,
			entry_hash VARCHAR(64) NOT NULL,
			import_content_id VARCHAR(64),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence, relative_path_hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_entry_content ON promotion_import_entries(tenant_id, allocation_epoch, allocation_sequence, import_content_id)`,
		`CREATE TABLE IF NOT EXISTS promotion_import_contents (
			tenant_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			allocation_sequence BIGINT NOT NULL,
			import_content_id VARCHAR(64) NOT NULL,
			inline_content_blob BYTEA,
			inline_content_idempotency_key VARCHAR(128),
			inline_request_digest VARCHAR(64),
			sealed_storage_ref TEXT,
			sealed_storage_version VARCHAR(255),
			size_bytes BIGINT NOT NULL,
			checksum_sha256 VARCHAR(64) NOT NULL,
			accepted_seal_attempt_id VARCHAR(64),
			seal_state VARCHAR(32) NOT NULL DEFAULT 'INLINE',
			ownership_state VARCHAR(32) NOT NULL DEFAULT 'STAGED',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence, import_content_id),
			UNIQUE (tenant_id, allocation_epoch, allocation_sequence, inline_content_idempotency_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_content_gc ON promotion_import_contents(tenant_id, ownership_state, updated_at)`,
		`CREATE TABLE IF NOT EXISTS promotion_quota_reservations (
			tenant_id VARCHAR(64) NOT NULL,
			reservation_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			allocation_sequence BIGINT NOT NULL,
			reserved_bytes BIGINT NOT NULL,
			reserved_files BIGINT NOT NULL,
			state VARCHAR(32) NOT NULL,
			state_version BIGINT NOT NULL DEFAULT 1,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, reservation_id),
			UNIQUE (tenant_id, allocation_epoch, allocation_sequence)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_quota_state ON promotion_quota_reservations(tenant_id, state, updated_at)`,
		`CREATE TABLE IF NOT EXISTS promotion_quota_accounts (
			tenant_id VARCHAR(64) PRIMARY KEY,
			max_bytes BIGINT NOT NULL,
			max_files BIGINT NOT NULL,
			reserved_bytes BIGINT NOT NULL DEFAULT 0,
			reserved_files BIGINT NOT NULL DEFAULT 0,
			committed_bytes BIGINT NOT NULL DEFAULT 0,
			committed_files BIGINT NOT NULL DEFAULT 0,
			config_generation BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS promotion_import_tombstones (
			tenant_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			allocation_sequence BIGINT NOT NULL,
			allocation_proof_digest VARCHAR(64) NOT NULL,
			target_path VARCHAR(4096) NOT NULL,
			target_path_hash VARCHAR(64) NOT NULL,
			request_digest VARCHAR(64) NOT NULL,
			owner_token_hash VARCHAR(64) NOT NULL,
			recovery_token_hash VARCHAR(64) NOT NULL,
			terminal_state VARCHAR(32) NOT NULL,
			terminal_result_blob TEXT NOT NULL,
			terminal_result_digest VARCHAR(64) NOT NULL,
			commit_containment_lineage_id VARCHAR(128),
			commit_containment_position VARCHAR(255),
			commit_containment_checkpoint_digest VARCHAR(64),
			source_release_state VARCHAR(32) NOT NULL DEFAULT 'NONE',
			source_release_certificate_digest VARCHAR(64),
			source_release_floor_checkpoint JSONB,
			retire_after TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_promotion_tombstone_retire ON promotion_import_tombstones(tenant_id, retire_after)`,
		`CREATE TABLE IF NOT EXISTS promotion_retired_import_sequences (
			tenant_id VARCHAR(64) NOT NULL,
			allocation_epoch BIGINT NOT NULL,
			allocation_sequence BIGINT NOT NULL,
			retired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, allocation_epoch, allocation_sequence)
		)`,

		// Terminal result digests cover the exact stored JSON bytes. JSONB
		// rewrites whitespace and object-key order on round trip, so both fresh
		// and previously bootstrapped DB9 schemas must use byte-preserving TEXT.
		// A JSONB row that already contains a terminal result cannot be repaired:
		// the original response bytes are gone. Stop the upgrade and require
		// explicit operator reconciliation rather than silently re-digesting a
		// different result and breaking exact lost-response recovery.
		`DO $drive9$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'promotion_imports'
				  AND column_name = 'terminal_result_blob'
				  AND data_type = 'jsonb'
			) AND EXISTS (
				SELECT 1 FROM promotion_imports
				WHERE terminal_result_blob IS NOT NULL OR terminal_result_digest IS NOT NULL
			) THEN
				RAISE EXCEPTION 'drive9 refuses JSONB terminal-result upgrade for promotion_imports: operator reconciliation required';
			END IF;
		END
		$drive9$`,
		`DO $drive9$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'promotion_import_tombstones'
				  AND column_name = 'terminal_result_blob'
				  AND data_type = 'jsonb'
			) AND EXISTS (
				SELECT 1 FROM promotion_import_tombstones
				WHERE terminal_result_blob IS NOT NULL OR terminal_result_digest IS NOT NULL
			) THEN
				RAISE EXCEPTION 'drive9 refuses JSONB terminal-result upgrade for promotion_import_tombstones: operator reconciliation required';
			END IF;
		END
		$drive9$`,
		`ALTER TABLE promotion_imports ALTER COLUMN terminal_result_blob TYPE TEXT USING terminal_result_blob::text`,
		`ALTER TABLE promotion_import_tombstones ALTER COLUMN terminal_result_blob TYPE TEXT USING terminal_result_blob::text`,
	}
}
