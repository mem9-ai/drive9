package schema

// VaultTiDBSchemaStatements returns the vault DDL statements in TiDB/MySQL
// dialect. These are appended to the tenant init schema statement set for all
// TiDB providers, including drive9-server schema dump-init-sql output.
func VaultTiDBSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS vault_deks (
			tenant_id    VARCHAR(64) PRIMARY KEY,
			wrapped_dek  BLOB NOT NULL,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_secrets (
			secret_id    VARCHAR(64) PRIMARY KEY,
			tenant_id    VARCHAR(64) NOT NULL,
			name         VARCHAR(255) NOT NULL,
			secret_type  VARCHAR(32) NOT NULL DEFAULT 'generic',
			revision     BIGINT NOT NULL DEFAULT 1,
			created_by   VARCHAR(255) NOT NULL,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			deleted_at   DATETIME(3),
			UNIQUE INDEX uk_vault_secrets_tenant_name (tenant_id, name),
			INDEX idx_vault_secrets_tenant (tenant_id)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_secret_fields (
			secret_id       VARCHAR(64) NOT NULL,
			field_name      VARCHAR(255) NOT NULL,
			encrypted_value BLOB NOT NULL,
			nonce           BLOB NOT NULL,
			PRIMARY KEY (secret_id, field_name)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_tokens (
			token_id      VARCHAR(64) PRIMARY KEY,
			tenant_id     VARCHAR(64) NOT NULL,
			agent_id      VARCHAR(255) NOT NULL,
			task_id       VARCHAR(255),
			scope_json    JSON NOT NULL,
			issued_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			expires_at    DATETIME(3) NOT NULL,
			revoked_at    DATETIME(3),
			revoked_by    VARCHAR(255),
			revoke_reason VARCHAR(255),
			INDEX idx_vault_token_tenant (tenant_id),
			INDEX idx_vault_token_agent (agent_id)
		)`,

		// vault_grants — end-state token storage per docs/specs/vault-interaction-end-state.md §16.
		// Coexists with vault_tokens until PR-E per docs/specs/pr-e-removal-contract.md.
		`CREATE TABLE IF NOT EXISTS vault_grants (
			grant_id       VARCHAR(64) PRIMARY KEY,
			tenant_id      VARCHAR(64) NOT NULL,
			issuer         VARCHAR(256) NOT NULL,
			principal_type VARCHAR(16) NOT NULL,
			agent          VARCHAR(128) NOT NULL,
			scope_json     JSON NOT NULL,
			perm           VARCHAR(8) NOT NULL,
			label_hint     VARCHAR(128),
			issued_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			expires_at     DATETIME(3) NOT NULL,
			revoked_at     DATETIME(3),
			revoked_by     VARCHAR(128),
			revoke_reason  VARCHAR(256),
			INDEX idx_vault_grants_tenant (tenant_id),
			INDEX idx_vault_grants_expires (expires_at)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_audit_log (
			event_id     VARCHAR(64) PRIMARY KEY,
			tenant_id    VARCHAR(64) NOT NULL,
			event_type   VARCHAR(32) NOT NULL,
			token_id     VARCHAR(64),
			agent_id     VARCHAR(255),
			task_id      VARCHAR(255),
			secret_name  VARCHAR(255),
			field_name   VARCHAR(255),
			adapter      VARCHAR(16),
			detail_json  JSON,
			timestamp    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			INDEX idx_vault_audit_tenant_time (tenant_id, timestamp),
			INDEX idx_vault_audit_secret (secret_name, timestamp)
		)`,
	}
}

// VaultTiDBSharedSchemaStatements returns the vault DDL for the shared
// (multi-tenant) schema shape on TiDB: identical to VaultTiDBSchemaStatements
// except the tenant_id VARCHAR(64) discriminator column is replaced by
// fs_id BIGINT in every table, unique key, and index, and vault_secret_fields
// — which has no tenant column in the standalone shape — gains fs_id BIGINT
// as its first column. Every table's primary key leads with fs_id and is
// declared CLUSTERED, physically co-locating one tenant's rows like the
// other shared table groups do; composite keys need the keyword because TiDB
// creates composite primary keys NONCLUSTERED by default (vault_deks keeps
// its single-column integer primary key, which TiDB clusters by default).
// For plain MySQL use VaultMySQLSharedSchemaStatements (same shape without
// the keyword). Keep both in lockstep with VaultTiDBSchemaStatements — the
// drift test in vault_shared_test.go enforces parity.
func VaultTiDBSharedSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS vault_deks (
			fs_id        BIGINT PRIMARY KEY,
			wrapped_dek  BLOB NOT NULL,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_secrets (
			secret_id    VARCHAR(64) NOT NULL,
			fs_id        BIGINT NOT NULL,
			name         VARCHAR(255) NOT NULL,
			secret_type  VARCHAR(32) NOT NULL DEFAULT 'generic',
			revision     BIGINT NOT NULL DEFAULT 1,
			created_by   VARCHAR(255) NOT NULL,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			deleted_at   DATETIME(3),
			PRIMARY KEY (fs_id, secret_id) CLUSTERED,
			UNIQUE INDEX uk_vault_secrets_tenant_name (fs_id, name)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_secret_fields (
			fs_id           BIGINT NOT NULL,
			secret_id       VARCHAR(64) NOT NULL,
			field_name      VARCHAR(255) NOT NULL,
			encrypted_value BLOB NOT NULL,
			nonce           BLOB NOT NULL,
			PRIMARY KEY (fs_id, secret_id, field_name) CLUSTERED
		)`,

		`CREATE TABLE IF NOT EXISTS vault_tokens (
			token_id      VARCHAR(64) NOT NULL,
			fs_id         BIGINT NOT NULL,
			agent_id      VARCHAR(255) NOT NULL,
			task_id       VARCHAR(255),
			scope_json    JSON NOT NULL,
			issued_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			expires_at    DATETIME(3) NOT NULL,
			revoked_at    DATETIME(3),
			revoked_by    VARCHAR(255),
			revoke_reason VARCHAR(255),
			PRIMARY KEY (fs_id, token_id) CLUSTERED,
			INDEX idx_vault_token_agent (agent_id)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_grants (
			grant_id       VARCHAR(64) NOT NULL,
			fs_id          BIGINT NOT NULL,
			issuer         VARCHAR(256) NOT NULL,
			principal_type VARCHAR(16) NOT NULL,
			agent          VARCHAR(128) NOT NULL,
			scope_json     JSON NOT NULL,
			perm           VARCHAR(8) NOT NULL,
			label_hint     VARCHAR(128),
			issued_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			expires_at     DATETIME(3) NOT NULL,
			revoked_at     DATETIME(3),
			revoked_by     VARCHAR(128),
			revoke_reason  VARCHAR(256),
			PRIMARY KEY (fs_id, grant_id) CLUSTERED,
			INDEX idx_vault_grants_expires (expires_at)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_audit_log (
			event_id     VARCHAR(64) NOT NULL,
			fs_id        BIGINT NOT NULL,
			event_type   VARCHAR(32) NOT NULL,
			token_id     VARCHAR(64),
			agent_id     VARCHAR(255),
			task_id      VARCHAR(255),
			secret_name  VARCHAR(255),
			field_name   VARCHAR(255),
			adapter      VARCHAR(16),
			detail_json  JSON,
			timestamp    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (fs_id, event_id) CLUSTERED,
			INDEX idx_vault_audit_tenant_time (fs_id, timestamp),
			INDEX idx_vault_audit_secret (secret_name, timestamp)
		)`,
	}
}

// VaultMySQLSharedSchemaStatements is the plain-MySQL variant of
// VaultTiDBSharedSchemaStatements, derived by removing TiDB-only keywords.
// Use it for local development databases and MySQL-backed tests/e2e.
func VaultMySQLSharedSchemaStatements() []string {
	return mysqlCompatibleSharedStatements(VaultTiDBSharedSchemaStatements())
}
