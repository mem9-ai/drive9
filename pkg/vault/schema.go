package vault

import (
	"database/sql"

	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

// InitSchema creates the vault tables in the tenant database.
func InitSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS vault_deks (
			tenant_id    VARCHAR(64) PRIMARY KEY,
			wrapped_dek  BYTEA NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,

		`CREATE TABLE IF NOT EXISTS vault_secrets (
			secret_id    VARCHAR(64) PRIMARY KEY,
			tenant_id    VARCHAR(64) NOT NULL,
			name         VARCHAR(255) NOT NULL,
			secret_type  VARCHAR(32) NOT NULL DEFAULT 'generic',
			revision     BIGINT NOT NULL DEFAULT 1,
			created_by   VARCHAR(255) NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			deleted_at   TIMESTAMPTZ,
			UNIQUE (tenant_id, name)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vault_secrets_tenant ON vault_secrets(tenant_id)`,

		`CREATE TABLE IF NOT EXISTS vault_secret_fields (
			secret_id       VARCHAR(64) NOT NULL,
			field_name      VARCHAR(255) NOT NULL,
			encrypted_value BYTEA NOT NULL,
			nonce           BYTEA NOT NULL,
			PRIMARY KEY (secret_id, field_name)
		)`,

		`CREATE TABLE IF NOT EXISTS vault_tokens (
			token_id      VARCHAR(64) PRIMARY KEY,
			tenant_id     VARCHAR(64) NOT NULL,
			agent_id      VARCHAR(255) NOT NULL,
			task_id       VARCHAR(255),
			scope_json    JSONB NOT NULL,
			issued_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at    TIMESTAMPTZ NOT NULL,
			revoked_at    TIMESTAMPTZ,
			revoked_by    VARCHAR(255),
			revoke_reason VARCHAR(255)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vault_token_tenant ON vault_tokens(tenant_id)`,
		`CREATE INDEX IF NOT EXISTS idx_vault_token_agent ON vault_tokens(agent_id)`,

		`CREATE TABLE IF NOT EXISTS vault_policies (
			policy_id   VARCHAR(64) PRIMARY KEY,
			tenant_id   VARCHAR(64) NOT NULL,
			name        VARCHAR(255) NOT NULL,
			rules_json  JSONB NOT NULL,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
			detail_json  JSONB,
			timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vault_audit_tenant_time ON vault_audit_log(tenant_id, timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_vault_audit_secret ON vault_audit_log(secret_name, timestamp)`,
	}
	return schema.ExecSchemaStatements(db, stmts)
}
