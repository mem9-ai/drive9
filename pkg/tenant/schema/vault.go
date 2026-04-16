package schema

// VaultTiDBSchemaStatements returns the vault DDL statements in TiDB/MySQL
// dialect. These are appended to the tenant schema init for all TiDB providers.
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

		`CREATE TABLE IF NOT EXISTS vault_policies (
			policy_id   VARCHAR(64) PRIMARY KEY,
			tenant_id   VARCHAR(64) NOT NULL,
			name        VARCHAR(255) NOT NULL,
			rules_json  JSON NOT NULL,
			created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
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
