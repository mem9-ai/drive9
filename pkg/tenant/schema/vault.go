package schema

// VaultTiDBSchemaStatements returns the vault DDL statements in TiDB/MySQL
// dialect. These are appended to the tenant init schema statement set for all
// TiDB providers, including drive9-server schema dump-init-sql output.
//
// Schema shape follows spec 083aab8 §16: capability grants are identified by
// grant_id (stable revocation handle), bind to a single agent + issuer +
// principal_type + perm, and carry a scope list plus optional label_hint.
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
			grant_id       VARCHAR(64) PRIMARY KEY,
			tenant_id      VARCHAR(64) NOT NULL,
			issuer         VARCHAR(512) NOT NULL,
			principal_type VARCHAR(16) NOT NULL,
			agent          VARCHAR(255) NOT NULL,
			scope_json     JSON NOT NULL,
			perm           VARCHAR(16) NOT NULL,
			label_hint     VARCHAR(255),
			issued_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			expires_at     DATETIME(3) NOT NULL,
			revoked_at     DATETIME(3),
			revoked_by     VARCHAR(255),
			revoke_reason  VARCHAR(255),
			INDEX idx_vault_token_tenant (tenant_id),
			INDEX idx_vault_token_agent (agent)
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
			grant_id     VARCHAR(64),
			agent        VARCHAR(255),
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
