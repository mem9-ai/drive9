package schema

// QuotaOutboxTiDBSchemaStatements returns the tenant-local quota admission and
// mutation outbox schema for MySQL/TiDB-backed tenants.
func QuotaOutboxTiDBSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS quota_admission_locks (
			name       VARCHAR(64) PRIMARY KEY,
			updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS quota_outbox (
			id             BIGINT AUTO_INCREMENT PRIMARY KEY,
			file_id        VARCHAR(64) NULL,
			mutation_type  VARCHAR(32) NOT NULL,
			mutation_data  JSON NOT NULL,
			storage_delta  BIGINT NOT NULL DEFAULT 0,
			file_delta     BIGINT NOT NULL DEFAULT 0,
			media_delta    BIGINT NOT NULL DEFAULT 0,
			status         VARCHAR(20) NOT NULL DEFAULT 'queued',
			attempt_count  INT NOT NULL DEFAULT 0,
			max_attempts   INT NOT NULL DEFAULT 100,
			receipt        VARCHAR(128) NULL,
			leased_at      DATETIME(3) NULL,
			lease_until    DATETIME(3) NULL,
			available_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			last_error     TEXT NULL,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at   DATETIME(3) NULL
		)`,
		`CREATE INDEX idx_quota_outbox_claim ON quota_outbox(status, available_at, id)`,
		`CREATE INDEX idx_quota_outbox_processing ON quota_outbox(status, lease_until)`,
		`CREATE INDEX idx_quota_outbox_file_order ON quota_outbox(file_id, status, id)`,
		`CREATE INDEX idx_quota_outbox_file_pending ON quota_outbox(file_id, status, mutation_type)`,
		`CREATE INDEX idx_quota_outbox_pending_deltas ON quota_outbox(status, storage_delta, file_delta, media_delta)`,
	}
}

// QuotaOutboxDB9SchemaStatements returns the tenant-local quota admission and
// mutation outbox schema for db9/PostgreSQL-backed tenants.
func QuotaOutboxDB9SchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS quota_admission_locks (
			name       VARCHAR(64) PRIMARY KEY,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS quota_outbox (
			id             BIGSERIAL PRIMARY KEY,
			file_id        VARCHAR(64),
			mutation_type  VARCHAR(32) NOT NULL,
			mutation_data  JSONB NOT NULL,
			storage_delta  BIGINT NOT NULL DEFAULT 0,
			file_delta     BIGINT NOT NULL DEFAULT 0,
			media_delta    BIGINT NOT NULL DEFAULT 0,
			status         VARCHAR(20) NOT NULL DEFAULT 'queued',
			attempt_count  INT NOT NULL DEFAULT 0,
			max_attempts   INT NOT NULL DEFAULT 100,
			receipt        VARCHAR(128),
			leased_at      TIMESTAMPTZ,
			lease_until    TIMESTAMPTZ,
			available_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_error     TEXT,
			created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at   TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_quota_outbox_claim ON quota_outbox(status, available_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_quota_outbox_processing ON quota_outbox(status, lease_until)`,
		`CREATE INDEX IF NOT EXISTS idx_quota_outbox_file_order ON quota_outbox(file_id, status, id)`,
		`CREATE INDEX IF NOT EXISTS idx_quota_outbox_file_pending ON quota_outbox(file_id, status, mutation_type)`,
		`CREATE INDEX IF NOT EXISTS idx_quota_outbox_pending_deltas ON quota_outbox(status, storage_delta, file_delta, media_delta)`,
	}
}
