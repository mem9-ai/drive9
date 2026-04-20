// Package meta provides control-plane metadata storage for multi-tenant auth.
package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/mysqlutil"
	"go.uber.org/zap"
)

var (
	ErrNotFound                 = errors.New("not found")
	ErrDuplicate                = errors.New("duplicate entry")
	ErrStorageQuotaExceeded     = errors.New("tenant storage quota exceeded")
	ErrReservationAlreadyExists = errors.New("upload reservation already exists")
)

type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantFailed       TenantStatus = "failed"
	TenantSuspended    TenantStatus = "suspended"
	TenantDeleted      TenantStatus = "deleted"
)

type APIKeyStatus string

const (
	APIKeyActive  APIKeyStatus = "active"
	APIKeyRevoked APIKeyStatus = "revoked"
)

type Tenant struct {
	ID               string
	Status           TenantStatus
	DBHost           string
	DBPort           int
	DBUser           string
	DBPasswordCipher []byte
	DBName           string
	DBTLS            bool
	Provider         string
	ClusterID        string
	ClaimURL         string
	ClaimExpiresAt   *time.Time
	SchemaVersion    int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type APIKey struct {
	ID            string
	TenantID      string
	KeyName       string
	JWTCiphertext []byte
	JWTHash       string
	TokenVersion  int
	Status        APIKeyStatus
	IssuedAt      time.Time
	RevokedAt     *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type TenantWithAPIKey struct {
	Tenant Tenant
	APIKey APIKey
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	if strings.Contains(dsn, "multiStatements=true") {
		return nil, fmt.Errorf("multiStatements=true is not allowed in production DSN")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	applyMySQLPoolDefaults(db)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }

func applyMySQLPoolDefaults(db *sql.DB) {
	mysqlutil.ApplyPoolDefaults(db)
}

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			id               VARCHAR(64) PRIMARY KEY,
			status           VARCHAR(20) NOT NULL DEFAULT 'provisioning',
			db_host          VARCHAR(255) NOT NULL,
			db_port          INT NOT NULL,
			db_user          VARCHAR(255) NOT NULL,
			db_password      VARBINARY(2048) NOT NULL,
			db_name          VARCHAR(255) NOT NULL,
			db_tls           TINYINT(1) NOT NULL DEFAULT 1,
			provider         VARCHAR(50) NOT NULL,
			cluster_id       VARCHAR(255) NULL,
			claim_url        TEXT NULL,
			claim_expires_at DATETIME(3) NULL,
			schema_version   INT NOT NULL DEFAULT 1,
			created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			deleted_at       DATETIME(3) NULL,
			INDEX idx_tenant_status (status),
			INDEX idx_tenant_provider (provider)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_api_keys (
			id             VARCHAR(64) PRIMARY KEY,
			tenant_id      VARCHAR(64) NOT NULL,
			key_name       VARCHAR(64) NOT NULL DEFAULT 'default',
			jwt_ciphertext VARBINARY(4096) NOT NULL,
			jwt_hash       VARCHAR(128) NOT NULL,
			token_version  INT NOT NULL DEFAULT 1,
			status         VARCHAR(20) NOT NULL DEFAULT 'active',
			issued_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			revoked_at     DATETIME(3) NULL,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			UNIQUE INDEX idx_api_keys_hash (jwt_hash),
			INDEX idx_api_keys_tenant (tenant_id, status),
			UNIQUE INDEX idx_api_keys_tenant_name (tenant_id, key_name)
		)`,
		// --- LLM usage table (PR #245 migration) ---

		`CREATE TABLE IF NOT EXISTS llm_usage (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			tenant_id       VARCHAR(64) NOT NULL,
			task_type       VARCHAR(64) NOT NULL,
			task_id         VARCHAR(255) NOT NULL,
			cost_millicents BIGINT NOT NULL,
			raw_units       BIGINT NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(32) NOT NULL DEFAULT '',
			created_at      DATETIME(3) NOT NULL,
			INDEX idx_llm_usage_tenant_created (tenant_id, created_at),
			INDEX idx_llm_usage_created (created_at)
		)`,

		// --- Quota tables (Rev 4 migration) ---

		`CREATE TABLE IF NOT EXISTS tenant_quota_config (
			tenant_id             VARCHAR(64) PRIMARY KEY,
			max_storage_bytes     BIGINT NOT NULL DEFAULT 53687091200,
			max_media_llm_files   BIGINT NOT NULL DEFAULT 500,
			max_monthly_cost_mc   BIGINT NOT NULL DEFAULT 0,
			created_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,

		`CREATE TABLE IF NOT EXISTS tenant_quota_usage (
			tenant_id          VARCHAR(64) PRIMARY KEY,
			storage_bytes      BIGINT NOT NULL DEFAULT 0,
			reserved_bytes     BIGINT NOT NULL DEFAULT 0,
			media_file_count   BIGINT NOT NULL DEFAULT 0,
			updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,

		`CREATE TABLE IF NOT EXISTS tenant_file_meta (
			tenant_id    VARCHAR(64) NOT NULL,
			file_id      VARCHAR(64) NOT NULL,
			size_bytes   BIGINT NOT NULL DEFAULT 0,
			is_media     TINYINT(1) NOT NULL DEFAULT 0,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, file_id)
		)`,

		`CREATE TABLE IF NOT EXISTS tenant_upload_reservations (
			tenant_id      VARCHAR(64) NOT NULL,
			upload_id      VARCHAR(64) NOT NULL,
			reserved_bytes BIGINT NOT NULL,
			target_path    VARCHAR(4096) NOT NULL,
			status         VARCHAR(20) NOT NULL DEFAULT 'active',
			expires_at     DATETIME(3) NOT NULL,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, upload_id),
			INDEX idx_active_reservations (tenant_id, status, expires_at)
		)`,

		`CREATE TABLE IF NOT EXISTS tenant_llm_usage (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			tenant_id       VARCHAR(64) NOT NULL,
			task_type       VARCHAR(32) NOT NULL,
			task_id         VARCHAR(64) NOT NULL,
			cost_millicents BIGINT NOT NULL DEFAULT 0,
			raw_units       BIGINT NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(16) NOT NULL,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			INDEX idx_tenant_month (tenant_id, created_at)
		)`,

		`CREATE TABLE IF NOT EXISTS tenant_monthly_llm_cost (
			tenant_id    VARCHAR(64) NOT NULL,
			month_start  DATE NOT NULL,
			total_mc     BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (tenant_id, month_start)
		)`,

		`CREATE TABLE IF NOT EXISTS quota_mutation_log (
			id             BIGINT AUTO_INCREMENT PRIMARY KEY,
			tenant_id      VARCHAR(64) NOT NULL,
			mutation_type  VARCHAR(32) NOT NULL,
			mutation_data  JSON NOT NULL,
			status         VARCHAR(20) NOT NULL DEFAULT 'pending',
			retry_count    INT NOT NULL DEFAULT 0,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			applied_at     DATETIME(3) NULL,
			INDEX idx_pending (status, created_at),
			INDEX idx_tenant_order (tenant_id, id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) InsertTenant(ctx context.Context, t *Tenant) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_tenant", start, &err)
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenants
		(id, status, db_host, db_port, db_user, db_password, db_name, db_tls,
		 provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.Status, t.DBHost, t.DBPort, t.DBUser, t.DBPasswordCipher, t.DBName, boolToInt(t.DBTLS),
		t.Provider, nullStr(t.ClusterID), nullStr(t.ClaimURL), t.ClaimExpiresAt, t.SchemaVersion, t.CreatedAt.UTC(), t.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) InsertAPIKey(ctx context.Context, k *APIKey) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_api_key", start, &err)
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_api_keys
		(id, tenant_id, key_name, jwt_ciphertext, jwt_hash, token_version, status, issued_at, revoked_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		k.ID, k.TenantID, k.KeyName, k.JWTCiphertext, k.JWTHash, k.TokenVersion, k.Status,
		k.IssuedAt.UTC(), k.RevokedAt, k.CreatedAt.UTC(), k.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) ResolveByAPIKeyHash(ctx context.Context, hash string) (out *TenantWithAPIKey, err error) {
	start := time.Now()
	defer observeMeta(ctx, "resolve_api_key_hash", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT
			t.id, t.status, t.db_host, t.db_port, t.db_user, t.db_password, t.db_name, t.db_tls,
			t.provider, t.cluster_id, t.claim_url, t.claim_expires_at, t.schema_version, t.created_at, t.updated_at,
			k.id, k.tenant_id, k.key_name, k.jwt_ciphertext, k.jwt_hash, k.token_version, k.status, k.issued_at,
			k.revoked_at, k.created_at, k.updated_at
		FROM tenant_api_keys k
		JOIN tenants t ON t.id = k.tenant_id
		WHERE k.jwt_hash = ?`, hash)

	var rec TenantWithAPIKey
	var dbTLS int
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var clusterID sql.NullString
	var revokedAt sql.NullTime
	if err = row.Scan(
		&rec.Tenant.ID, &rec.Tenant.Status, &rec.Tenant.DBHost, &rec.Tenant.DBPort, &rec.Tenant.DBUser,
		&rec.Tenant.DBPasswordCipher, &rec.Tenant.DBName, &dbTLS, &rec.Tenant.Provider, &clusterID,
		&claimURL, &claimExp, &rec.Tenant.SchemaVersion, &rec.Tenant.CreatedAt, &rec.Tenant.UpdatedAt,
		&rec.APIKey.ID, &rec.APIKey.TenantID, &rec.APIKey.KeyName, &rec.APIKey.JWTCiphertext,
		&rec.APIKey.JWTHash, &rec.APIKey.TokenVersion, &rec.APIKey.Status, &rec.APIKey.IssuedAt,
		&revokedAt, &rec.APIKey.CreatedAt, &rec.APIKey.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	rec.Tenant.DBTLS = dbTLS == 1
	if clusterID.Valid {
		rec.Tenant.ClusterID = clusterID.String
	}
	if claimURL.Valid {
		rec.Tenant.ClaimURL = claimURL.String
	}
	if claimExp.Valid {
		t := claimExp.Time.UTC()
		rec.Tenant.ClaimExpiresAt = &t
	}
	if revokedAt.Valid {
		t := revokedAt.Time.UTC()
		rec.APIKey.RevokedAt = &t
	}
	out = &rec
	return out, nil
}

func (s *Store) GetTenant(ctx context.Context, id string) (out *Tenant, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT id, status, db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at
		FROM tenants WHERE id = ?`, id)
	var dbTLS int
	var clusterID sql.NullString
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var rec Tenant
	if err = row.Scan(&rec.ID, &rec.Status, &rec.DBHost, &rec.DBPort, &rec.DBUser, &rec.DBPasswordCipher,
		&rec.DBName, &dbTLS, &rec.Provider, &clusterID, &claimURL, &claimExp, &rec.SchemaVersion,
		&rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	rec.DBTLS = dbTLS == 1
	if clusterID.Valid {
		rec.ClusterID = clusterID.String
	}
	if claimURL.Valid {
		rec.ClaimURL = claimURL.String
	}
	if claimExp.Valid {
		t := claimExp.Time.UTC()
		rec.ClaimExpiresAt = &t
	}
	out = &rec
	return out, nil
}

func (s *Store) ListTenantsByStatus(ctx context.Context, status TenantStatus, limit int) (out []Tenant, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_tenants_by_status", start, &err)
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, status, db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at
		FROM tenants WHERE status = ? ORDER BY created_at ASC LIMIT ?`, status, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out = make([]Tenant, 0)
	for rows.Next() {
		var t Tenant
		var dbTLS int
		var clusterID sql.NullString
		var claimURL sql.NullString
		var claimExp sql.NullTime
		if err := rows.Scan(&t.ID, &t.Status, &t.DBHost, &t.DBPort, &t.DBUser, &t.DBPasswordCipher,
			&t.DBName, &dbTLS, &t.Provider, &clusterID, &claimURL, &claimExp, &t.SchemaVersion,
			&t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		t.DBTLS = dbTLS == 1
		if clusterID.Valid {
			t.ClusterID = clusterID.String
		}
		if claimURL.Valid {
			t.ClaimURL = claimURL.String
		}
		if claimExp.Valid {
			ts := claimExp.Time.UTC()
			t.ClaimExpiresAt = &ts
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) UpdateTenantStatus(ctx context.Context, id string, status TenantStatus) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_status", start, &err)
	res, err := s.db.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = ?`, status, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateTenantDBCredentials updates the DB user, encrypted password, and
// database name for a tenant. Used by the async provisioning flow after
// dedicated tenant DB credentials have been created.
func (s *Store) UpdateTenantDBCredentials(ctx context.Context, id, dbUser string, dbPasswordCipher []byte, dbName string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_db_credentials", start, &err)
	res, err := s.db.ExecContext(ctx,
		`UPDATE tenants SET db_user = ?, db_password = ?, db_name = ?, updated_at = ? WHERE id = ?`,
		dbUser, dbPasswordCipher, dbName, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func observeMeta(ctx context.Context, op string, start time.Time, errp *error) {
	result := "ok"
	if errp != nil && *errp != nil {
		switch {
		case errors.Is(*errp, ErrNotFound):
			result = "not_found"
		case errors.Is(*errp, ErrDuplicate):
			result = "duplicate"
		default:
			result = "error"
		}
		logger.Error(ctx, "meta_op_failed", zap.String("operation", op), zap.String("result", result), zap.Error(*errp))
	}
	metrics.RecordOperation("meta", op, result, time.Since(start))
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func isDuplicateEntry(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || strings.Contains(msg, "UNIQUE constraint failed")
}
