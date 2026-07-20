package schema

import (
	"context"
	"database/sql"
)

// JournalTiDBSchemaStatements returns the tenant journal DDL used by TiDB and
// MySQL-compatible tenants. The first implementation intentionally creates
// the Phase 1 tables only: artifacts, filesystem projection, and seals are
// separate rollout phases in docs/design/agent-journal-design.md.
func JournalTiDBSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS journals (
			tenant_id      VARCHAR(64)  NOT NULL,
			journal_id     VARCHAR(64)  NOT NULL,
			kind           VARCHAR(64)  NOT NULL,
			title          VARCHAR(255) NULL,
			actor_type     VARCHAR(64)  NULL,
			actor_id       VARCHAR(255) NULL,
			source         VARCHAR(64)  NULL,
			meta           JSON         NULL,
			retention      JSON         NULL,
			next_seq       BIGINT       NOT NULL DEFAULT 1,
			genesis        JSON         NOT NULL,
			create_hash    VARCHAR(128) NOT NULL,
			genesis_hash   VARCHAR(128) NOT NULL,
			head_hash      VARCHAR(128) NOT NULL,
			created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			closed_at      DATETIME(3)  NULL,
			PRIMARY KEY (tenant_id, journal_id),
			KEY idx_kind_created (tenant_id, kind, created_at, journal_id),
			KEY idx_actor_created (tenant_id, actor_type, actor_id, created_at, journal_id)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_labels (
			tenant_id    VARCHAR(64)  NOT NULL,
			label_key    VARCHAR(128) NOT NULL,
			label_hash   VARCHAR(128) NOT NULL,
			label_value  TEXT         NOT NULL,
			journal_id   VARCHAR(64)  NOT NULL,
			created_at   DATETIME(3)  NOT NULL,
			source_seq   BIGINT       NULL,
			PRIMARY KEY (tenant_id, label_key, label_hash, created_at, journal_id),
			UNIQUE KEY uk_label_journal (tenant_id, journal_id, label_key, label_hash),
			KEY idx_journal (tenant_id, journal_id)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_append_requests (
			tenant_id        VARCHAR(64)  NOT NULL,
			journal_id       VARCHAR(64)  NOT NULL,
			append_id        VARCHAR(128) NOT NULL,
			request_hash     VARCHAR(128) NOT NULL,
			writer_type      VARCHAR(64)  NOT NULL,
			writer_id        VARCHAR(255) NOT NULL,
			effective_source VARCHAR(64)  NOT NULL,
			first_seq        BIGINT       NOT NULL,
			last_seq         BIGINT       NOT NULL,
			count            INT          NOT NULL,
			head_hash        VARCHAR(128) NOT NULL,
			created_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			expires_at       DATETIME(3)  NULL,
			PRIMARY KEY (tenant_id, journal_id, append_id),
			KEY idx_created (tenant_id, created_at),
			KEY idx_expires (tenant_id, expires_at)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_entries (
			tenant_id        VARCHAR(64)  NOT NULL,
			journal_id       VARCHAR(64)  NOT NULL,
			seq              BIGINT       NOT NULL,
			entry_id         VARCHAR(64)  NOT NULL,
			type             VARCHAR(128) NOT NULL,
			schema_version   INT          NOT NULL DEFAULT 1,
			status           VARCHAR(64)  NULL,
			occurred_at      DATETIME(3)  NOT NULL,
			observed_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			actor_type       VARCHAR(64)  NULL,
			actor_id         VARCHAR(255) NULL,
			source           VARCHAR(64)  NOT NULL,
			parent_entry_id  VARCHAR(64)  NULL,
			correlation_id   VARCHAR(128) NULL,
			subjects         JSON         NULL,
			summary          JSON         NULL,
			artifact_refs    JSON         NULL,
			prev_hash        VARCHAR(128) NOT NULL,
			entry_hash       VARCHAR(128) NOT NULL,
			PRIMARY KEY (tenant_id, journal_id, seq),
			UNIQUE KEY uk_entry_id (tenant_id, entry_id),
			KEY idx_type_observed (tenant_id, type, observed_at, journal_id, seq),
			KEY idx_type_status_observed (tenant_id, type, status, observed_at, journal_id, seq),
			KEY idx_status_observed (tenant_id, status, observed_at, journal_id, seq),
			KEY idx_actor_observed (tenant_id, actor_type, actor_id, observed_at, journal_id, seq),
			KEY idx_parent_observed (tenant_id, parent_entry_id, observed_at, journal_id, seq),
			KEY idx_correlation_observed (tenant_id, correlation_id, observed_at, journal_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_entry_subjects (
			tenant_id      VARCHAR(64)  NOT NULL,
			subject_type   VARCHAR(64)  NOT NULL,
			subject_hash   VARCHAR(128) NOT NULL,
			subject_id     TEXT         NOT NULL,
			occurred_at    DATETIME(3)  NOT NULL,
			observed_at    DATETIME(3)  NOT NULL,
			journal_id     VARCHAR(64)  NOT NULL,
			seq            BIGINT       NOT NULL,
			entry_id       VARCHAR(64)  NOT NULL,
			PRIMARY KEY (tenant_id, subject_type, subject_hash, observed_at, journal_id, seq),
			KEY idx_entry (tenant_id, entry_id),
			KEY idx_journal_seq (tenant_id, journal_id, seq)
		)`,
	}
}

// JournalTiDBSharedSchemaStatements returns the journal DDL for the shared
// (multi-tenant) schema shape on TiDB: identical to
// JournalTiDBSchemaStatements except the tenant_id VARCHAR(64) discriminator
// column is replaced by fs_id BIGINT in every table, primary key, unique key,
// and index. Composite primary keys are declared CLUSTERED — TiDB creates
// them NONCLUSTERED by default, and the shared shape relies on clustered
// keys to physically co-locate one tenant's rows. For plain MySQL use
// JournalMySQLSharedSchemaStatements (same shape without the keyword).
// Keep both in lockstep with JournalTiDBSchemaStatements — the drift test in
// journal_shared_test.go enforces parity.
func JournalTiDBSharedSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS journals (
			fs_id          BIGINT        NOT NULL,
			journal_id     VARCHAR(64)  NOT NULL,
			kind           VARCHAR(64)  NOT NULL,
			title          VARCHAR(255) NULL,
			actor_type     VARCHAR(64)  NULL,
			actor_id       VARCHAR(255) NULL,
			source         VARCHAR(64)  NULL,
			meta           JSON         NULL,
			retention      JSON         NULL,
			next_seq       BIGINT       NOT NULL DEFAULT 1,
			genesis        JSON         NOT NULL,
			create_hash    VARCHAR(128) NOT NULL,
			genesis_hash   VARCHAR(128) NOT NULL,
			head_hash      VARCHAR(128) NOT NULL,
			created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			closed_at      DATETIME(3)  NULL,
			PRIMARY KEY (fs_id, journal_id) CLUSTERED,
			KEY idx_kind_created (fs_id, kind, created_at, journal_id),
			KEY idx_actor_created (fs_id, actor_type, actor_id, created_at, journal_id)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_labels (
			fs_id        BIGINT        NOT NULL,
			label_key    VARCHAR(128) NOT NULL,
			label_hash   VARCHAR(128) NOT NULL,
			label_value  TEXT         NOT NULL,
			journal_id   VARCHAR(64)  NOT NULL,
			created_at   DATETIME(3)  NOT NULL,
			source_seq   BIGINT       NULL,
			PRIMARY KEY (fs_id, label_key, label_hash, created_at, journal_id) CLUSTERED,
			UNIQUE KEY uk_label_journal (fs_id, journal_id, label_key, label_hash),
			KEY idx_journal (fs_id, journal_id)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_append_requests (
			fs_id            BIGINT        NOT NULL,
			journal_id       VARCHAR(64)  NOT NULL,
			append_id        VARCHAR(128) NOT NULL,
			request_hash     VARCHAR(128) NOT NULL,
			writer_type      VARCHAR(64)  NOT NULL,
			writer_id        VARCHAR(255) NOT NULL,
			effective_source VARCHAR(64)  NOT NULL,
			first_seq        BIGINT       NOT NULL,
			last_seq         BIGINT       NOT NULL,
			count            INT          NOT NULL,
			head_hash        VARCHAR(128) NOT NULL,
			created_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			expires_at       DATETIME(3)  NULL,
			PRIMARY KEY (fs_id, journal_id, append_id) CLUSTERED,
			KEY idx_created (fs_id, created_at),
			KEY idx_expires (fs_id, expires_at)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_entries (
			fs_id            BIGINT        NOT NULL,
			journal_id       VARCHAR(64)  NOT NULL,
			seq              BIGINT       NOT NULL,
			entry_id         VARCHAR(64)  NOT NULL,
			type             VARCHAR(128) NOT NULL,
			schema_version   INT          NOT NULL DEFAULT 1,
			status           VARCHAR(64)  NULL,
			occurred_at      DATETIME(3)  NOT NULL,
			observed_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			actor_type       VARCHAR(64)  NULL,
			actor_id         VARCHAR(255) NULL,
			source           VARCHAR(64)  NOT NULL,
			parent_entry_id  VARCHAR(64)  NULL,
			correlation_id   VARCHAR(128) NULL,
			subjects         JSON         NULL,
			summary          JSON         NULL,
			artifact_refs    JSON         NULL,
			prev_hash        VARCHAR(128) NOT NULL,
			entry_hash       VARCHAR(128) NOT NULL,
			PRIMARY KEY (fs_id, journal_id, seq) CLUSTERED,
			UNIQUE KEY uk_entry_id (fs_id, entry_id),
			KEY idx_type_observed (fs_id, type, observed_at, journal_id, seq),
			KEY idx_type_status_observed (fs_id, type, status, observed_at, journal_id, seq),
			KEY idx_status_observed (fs_id, status, observed_at, journal_id, seq),
			KEY idx_actor_observed (fs_id, actor_type, actor_id, observed_at, journal_id, seq),
			KEY idx_parent_observed (fs_id, parent_entry_id, observed_at, journal_id, seq),
			KEY idx_correlation_observed (fs_id, correlation_id, observed_at, journal_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS journal_entry_subjects (
			fs_id        BIGINT        NOT NULL,
			subject_type VARCHAR(64)  NOT NULL,
			subject_hash VARCHAR(128) NOT NULL,
			subject_id   TEXT         NOT NULL,
			occurred_at  DATETIME(3)  NOT NULL,
			observed_at  DATETIME(3)  NOT NULL,
			journal_id   VARCHAR(64)  NOT NULL,
			seq          BIGINT       NOT NULL,
			entry_id     VARCHAR(64)  NOT NULL,
			PRIMARY KEY (fs_id, subject_type, subject_hash, observed_at, journal_id, seq) CLUSTERED,
			KEY idx_entry (fs_id, entry_id),
			KEY idx_journal_seq (fs_id, journal_id, seq)
		)`,
	}
}

// JournalDB9SchemaStatements returns the PostgreSQL-shaped journal DDL for the
// db9 tenant init path. Runtime journal storage currently uses the MySQL/TiDB
// datastore implementation; this keeps exported schemas structurally aligned.
func JournalDB9SchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS journals (
			tenant_id      VARCHAR(64)  NOT NULL,
			journal_id     VARCHAR(64)  NOT NULL,
			kind           VARCHAR(64)  NOT NULL,
			title          VARCHAR(255),
			actor_type     VARCHAR(64),
			actor_id       VARCHAR(255),
			source         VARCHAR(64),
			meta           JSONB,
			retention      JSONB,
			next_seq       BIGINT       NOT NULL DEFAULT 1,
			genesis        JSONB        NOT NULL,
			create_hash    VARCHAR(128) NOT NULL,
			genesis_hash   VARCHAR(128) NOT NULL,
			head_hash      VARCHAR(128) NOT NULL,
			created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			closed_at      TIMESTAMPTZ,
			PRIMARY KEY (tenant_id, journal_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_kind_created ON journals(tenant_id, kind, created_at, journal_id)`,
		`CREATE INDEX IF NOT EXISTS idx_actor_created ON journals(tenant_id, actor_type, actor_id, created_at, journal_id)`,
		`CREATE TABLE IF NOT EXISTS journal_labels (
			tenant_id    VARCHAR(64)  NOT NULL,
			label_key    VARCHAR(128) NOT NULL,
			label_hash   VARCHAR(128) NOT NULL,
			label_value  TEXT         NOT NULL,
			journal_id   VARCHAR(64)  NOT NULL,
			created_at   TIMESTAMPTZ  NOT NULL,
			source_seq   BIGINT,
			PRIMARY KEY (tenant_id, label_key, label_hash, created_at, journal_id),
			UNIQUE (tenant_id, journal_id, label_key, label_hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_journal_labels_journal ON journal_labels(tenant_id, journal_id)`,
		`CREATE TABLE IF NOT EXISTS journal_append_requests (
			tenant_id        VARCHAR(64)  NOT NULL,
			journal_id       VARCHAR(64)  NOT NULL,
			append_id        VARCHAR(128) NOT NULL,
			request_hash     VARCHAR(128) NOT NULL,
			writer_type      VARCHAR(64)  NOT NULL,
			writer_id        VARCHAR(255) NOT NULL,
			effective_source VARCHAR(64)  NOT NULL,
			first_seq        BIGINT       NOT NULL,
			last_seq         BIGINT       NOT NULL,
			count            INT          NOT NULL,
			head_hash        VARCHAR(128) NOT NULL,
			created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			expires_at       TIMESTAMPTZ,
			PRIMARY KEY (tenant_id, journal_id, append_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_journal_append_requests_created ON journal_append_requests(tenant_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_journal_append_requests_expires ON journal_append_requests(tenant_id, expires_at)`,
		`CREATE TABLE IF NOT EXISTS journal_entries (
			tenant_id        VARCHAR(64)  NOT NULL,
			journal_id       VARCHAR(64)  NOT NULL,
			seq              BIGINT       NOT NULL,
			entry_id         VARCHAR(64)  NOT NULL,
			type             VARCHAR(128) NOT NULL,
			schema_version   INT          NOT NULL DEFAULT 1,
			status           VARCHAR(64),
			occurred_at      TIMESTAMPTZ  NOT NULL,
			observed_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			actor_type       VARCHAR(64),
			actor_id         VARCHAR(255),
			source           VARCHAR(64)  NOT NULL,
			parent_entry_id  VARCHAR(64),
			correlation_id   VARCHAR(128),
			subjects         JSONB,
			summary          JSONB,
			artifact_refs    JSONB,
			prev_hash        VARCHAR(128) NOT NULL,
			entry_hash       VARCHAR(128) NOT NULL,
			PRIMARY KEY (tenant_id, journal_id, seq),
			UNIQUE (tenant_id, entry_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_type_observed ON journal_entries(tenant_id, type, observed_at, journal_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_type_status_observed ON journal_entries(tenant_id, type, status, observed_at, journal_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_status_observed ON journal_entries(tenant_id, status, observed_at, journal_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_actor_observed ON journal_entries(tenant_id, actor_type, actor_id, observed_at, journal_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_parent_observed ON journal_entries(tenant_id, parent_entry_id, observed_at, journal_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_correlation_observed ON journal_entries(tenant_id, correlation_id, observed_at, journal_id, seq)`,
		`CREATE TABLE IF NOT EXISTS journal_entry_subjects (
			tenant_id      VARCHAR(64)  NOT NULL,
			subject_type   VARCHAR(64)  NOT NULL,
			subject_hash   VARCHAR(128) NOT NULL,
			subject_id     TEXT         NOT NULL,
			occurred_at    TIMESTAMPTZ  NOT NULL,
			observed_at    TIMESTAMPTZ  NOT NULL,
			journal_id     VARCHAR(64)  NOT NULL,
			seq            BIGINT       NOT NULL,
			entry_id       VARCHAR(64)  NOT NULL,
			PRIMARY KEY (tenant_id, subject_type, subject_hash, observed_at, journal_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_journal_entry_subjects_entry ON journal_entry_subjects(tenant_id, entry_id)`,
		`CREATE INDEX IF NOT EXISTS idx_journal_entry_subjects_journal_seq ON journal_entry_subjects(tenant_id, journal_id, seq)`,
	}
}

// JournalMySQLSharedSchemaStatements is the plain-MySQL variant of
// JournalTiDBSharedSchemaStatements, derived by removing TiDB-only keywords.
// Use it for local development databases and MySQL-backed tests/e2e.
func JournalMySQLSharedSchemaStatements() []string {
	return mysqlCompatibleSharedStatements(JournalTiDBSharedSchemaStatements())
}

// JournalSharedSchemaStatementsForDB selects the shared journal DDL matching
// the connected database's dialect: TiDB clusters get the CLUSTERED variant,
// anything else (plain MySQL, e.g. local e2e) the compatible variant. Dialect
// detection failures are returned, never silently treated as MySQL.
func JournalSharedSchemaStatementsForDB(ctx context.Context, db *sql.DB) ([]string, error) {
	isTiDB, err := IsTiDBClusterE(ctx, db)
	if err != nil {
		return nil, err
	}
	if isTiDB {
		return JournalTiDBSharedSchemaStatements(), nil
	}
	return JournalMySQLSharedSchemaStatements(), nil
}
