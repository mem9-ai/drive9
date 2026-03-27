// Package meta provides control-plane metadata storage for multi-tenant auth.
package meta

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ErrNotFound  = errors.New("not found")
	ErrDuplicate = errors.New("duplicate entry")
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
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) InsertTenant(t *Tenant) error {
	_, err := s.db.Exec(`INSERT INTO tenants
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

func (s *Store) InsertAPIKey(k *APIKey) error {
	_, err := s.db.Exec(`INSERT INTO tenant_api_keys
		(id, tenant_id, key_name, jwt_ciphertext, jwt_hash, token_version, status, issued_at, revoked_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		k.ID, k.TenantID, k.KeyName, k.JWTCiphertext, k.JWTHash, k.TokenVersion, k.Status,
		k.IssuedAt.UTC(), k.RevokedAt, k.CreatedAt.UTC(), k.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) ResolveByAPIKeyHash(hash string) (*TenantWithAPIKey, error) {
	row := s.db.QueryRow(`SELECT
			t.id, t.status, t.db_host, t.db_port, t.db_user, t.db_password, t.db_name, t.db_tls,
			t.provider, t.cluster_id, t.claim_url, t.claim_expires_at, t.schema_version, t.created_at, t.updated_at,
			k.id, k.tenant_id, k.key_name, k.jwt_ciphertext, k.jwt_hash, k.token_version, k.status, k.issued_at,
			k.revoked_at, k.created_at, k.updated_at
		FROM tenant_api_keys k
		JOIN tenants t ON t.id = k.tenant_id
		WHERE k.jwt_hash = ?`, hash)

	var out TenantWithAPIKey
	var dbTLS int
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var clusterID sql.NullString
	var revokedAt sql.NullTime
	if err := row.Scan(
		&out.Tenant.ID, &out.Tenant.Status, &out.Tenant.DBHost, &out.Tenant.DBPort, &out.Tenant.DBUser,
		&out.Tenant.DBPasswordCipher, &out.Tenant.DBName, &dbTLS, &out.Tenant.Provider, &clusterID,
		&claimURL, &claimExp, &out.Tenant.SchemaVersion, &out.Tenant.CreatedAt, &out.Tenant.UpdatedAt,
		&out.APIKey.ID, &out.APIKey.TenantID, &out.APIKey.KeyName, &out.APIKey.JWTCiphertext,
		&out.APIKey.JWTHash, &out.APIKey.TokenVersion, &out.APIKey.Status, &out.APIKey.IssuedAt,
		&revokedAt, &out.APIKey.CreatedAt, &out.APIKey.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	out.Tenant.DBTLS = dbTLS == 1
	if clusterID.Valid {
		out.Tenant.ClusterID = clusterID.String
	}
	if claimURL.Valid {
		out.Tenant.ClaimURL = claimURL.String
	}
	if claimExp.Valid {
		t := claimExp.Time.UTC()
		out.Tenant.ClaimExpiresAt = &t
	}
	if revokedAt.Valid {
		t := revokedAt.Time.UTC()
		out.APIKey.RevokedAt = &t
	}
	return &out, nil
}

func (s *Store) GetTenant(id string) (*Tenant, error) {
	row := s.db.QueryRow(`SELECT id, status, db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at
		FROM tenants WHERE id = ?`, id)
	var out Tenant
	var dbTLS int
	var clusterID sql.NullString
	var claimURL sql.NullString
	var claimExp sql.NullTime
	if err := row.Scan(&out.ID, &out.Status, &out.DBHost, &out.DBPort, &out.DBUser, &out.DBPasswordCipher,
		&out.DBName, &dbTLS, &out.Provider, &clusterID, &claimURL, &claimExp, &out.SchemaVersion,
		&out.CreatedAt, &out.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	out.DBTLS = dbTLS == 1
	if clusterID.Valid {
		out.ClusterID = clusterID.String
	}
	if claimURL.Valid {
		out.ClaimURL = claimURL.String
	}
	if claimExp.Valid {
		t := claimExp.Time.UTC()
		out.ClaimExpiresAt = &t
	}
	return &out, nil
}

func (s *Store) ListTenantsByStatus(status TenantStatus, limit int) ([]Tenant, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT id, status, db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at
		FROM tenants WHERE status = ? ORDER BY created_at ASC LIMIT ?`, status, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]Tenant, 0)
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

func (s *Store) UpdateTenantStatus(id string, status TenantStatus) error {
	res, err := s.db.Exec(`UPDATE tenants SET status = ?, updated_at = ? WHERE id = ?`, status, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
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
