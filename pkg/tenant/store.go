package tenant

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ErrNotFound  = errors.New("tenant not found")
	ErrDuplicate = errors.New("duplicate API key")
)

// Store manages tenants in the control plane database.
// This is separate from the per-tenant meta.Store.
type Store struct {
	db  *sql.DB
	enc *Encryptor
}

// OpenStore opens the control plane DB and runs migrations.
func OpenStore(dsn string, enc *Encryptor) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open control plane db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping control plane db: %w", err)
	}
	s := &Store{db: db, enc: enc}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate control plane: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			id               VARCHAR(64) PRIMARY KEY,
			api_key_prefix   VARCHAR(16) NOT NULL,
			api_key_hash     VARCHAR(128) NOT NULL,
			status           VARCHAR(32) NOT NULL DEFAULT 'provisioning',
			db_host          VARCHAR(255) NOT NULL DEFAULT '',
			db_port          INT NOT NULL DEFAULT 4000,
			db_user          VARCHAR(255) NOT NULL DEFAULT '',
			db_password_enc  VARBINARY(512),
			db_name          VARCHAR(255) NOT NULL DEFAULT '',
			s3_bucket        VARCHAR(255) NOT NULL DEFAULT '',
			s3_key_prefix    VARCHAR(255) NOT NULL DEFAULT '',
			cluster_id       VARCHAR(255) NOT NULL DEFAULT '',
			provisioner_type VARCHAR(64) NOT NULL DEFAULT '',
			created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`CREATE UNIQUE INDEX idx_tenant_api_key_hash ON tenants(api_key_hash)`,
		`CREATE INDEX idx_tenant_status ON tenants(status)`,
		`CREATE INDEX idx_tenant_prefix ON tenants(api_key_prefix)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			if isIndexStmt(stmt) && isDuplicateIndex(err) {
				continue
			}
			snippet := stmt
			if len(snippet) > 60 {
				snippet = snippet[:60]
			}
			return fmt.Errorf("exec %q: %w", snippet, err)
		}
	}
	return nil
}

// Insert persists a new tenant. The caller must set all fields including
// the encrypted password (via EncryptPassword).
func (s *Store) Insert(t *Tenant) error {
	_, err := s.db.Exec(`INSERT INTO tenants
		(id, api_key_prefix, api_key_hash, status, db_host, db_port, db_user,
		 db_password_enc, db_name, s3_bucket, s3_key_prefix, cluster_id,
		 provisioner_type, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.APIKeyPrefix, t.APIKeyHash, t.Status,
		t.DBHost, t.DBPort, t.DBUser, t.DBPasswordEnc, t.DBName,
		t.S3Bucket, t.S3KeyPrefix, t.ClusterID, t.ProvisionerType,
		t.CreatedAt.UTC(), t.UpdatedAt.UTC())
	if err != nil && isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

// GetByAPIKeyHash looks up a tenant by the SHA-256 hash of their API key.
func (s *Store) GetByAPIKeyHash(hash string) (*Tenant, error) {
	row := s.db.QueryRow(`SELECT id, api_key_prefix, api_key_hash, status,
		db_host, db_port, db_user, db_password_enc, db_name,
		s3_bucket, s3_key_prefix, cluster_id, provisioner_type,
		created_at, updated_at
		FROM tenants WHERE api_key_hash = ?`, hash)
	return s.scanTenant(row)
}

// Get looks up a tenant by ID.
func (s *Store) Get(id string) (*Tenant, error) {
	row := s.db.QueryRow(`SELECT id, api_key_prefix, api_key_hash, status,
		db_host, db_port, db_user, db_password_enc, db_name,
		s3_bucket, s3_key_prefix, cluster_id, provisioner_type,
		created_at, updated_at
		FROM tenants WHERE id = ?`, id)
	return s.scanTenant(row)
}

// UpdateStatus changes a tenant's status.
func (s *Store) UpdateStatus(id string, status Status) error {
	res, err := s.db.Exec(`UPDATE tenants SET status = ?, updated_at = ?
		WHERE id = ?`, status, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateClusterInfo sets the DB connection details after provisioning.
func (s *Store) UpdateClusterInfo(id, host string, port int, user string, passwordEnc []byte, dbName string) error {
	res, err := s.db.Exec(`UPDATE tenants SET db_host = ?, db_port = ?, db_user = ?,
		db_password_enc = ?, db_name = ?, updated_at = ?
		WHERE id = ?`, host, port, user, passwordEnc, dbName, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// EncryptPassword encrypts a plaintext password using the store's encryptor.
func (s *Store) EncryptPassword(plaintext string) ([]byte, error) {
	return s.enc.Encrypt([]byte(plaintext))
}

// DecryptPassword decrypts an encrypted password from the DB.
func (s *Store) DecryptPassword(ciphertext []byte) (string, error) {
	b, err := s.enc.Decrypt(ciphertext)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// List returns all tenants, optionally filtered by status.
func (s *Store) List(status Status) ([]*Tenant, error) {
	var rows *sql.Rows
	var err error
	if status == "" {
		rows, err = s.db.Query(`SELECT id, api_key_prefix, api_key_hash, status,
			db_host, db_port, db_user, db_password_enc, db_name,
			s3_bucket, s3_key_prefix, cluster_id, provisioner_type,
			created_at, updated_at
			FROM tenants ORDER BY created_at`)
	} else {
		rows, err = s.db.Query(`SELECT id, api_key_prefix, api_key_hash, status,
			db_host, db_port, db_user, db_password_enc, db_name,
			s3_bucket, s3_key_prefix, cluster_id, provisioner_type,
			created_at, updated_at
			FROM tenants WHERE status = ? ORDER BY created_at`, status)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tenants []*Tenant
	for rows.Next() {
		t, err := s.scanTenantRows(rows)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, rows.Err()
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func (s *Store) scanTenant(sc scanner) (*Tenant, error) {
	var t Tenant
	var passwordEnc []byte
	var createdAt, updatedAt time.Time
	err := sc.Scan(&t.ID, &t.APIKeyPrefix, &t.APIKeyHash, &t.Status,
		&t.DBHost, &t.DBPort, &t.DBUser, &passwordEnc, &t.DBName,
		&t.S3Bucket, &t.S3KeyPrefix, &t.ClusterID, &t.ProvisionerType,
		&createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.DBPasswordEnc = passwordEnc
	t.CreatedAt = createdAt.UTC()
	t.UpdatedAt = updatedAt.UTC()
	return &t, nil
}

func (s *Store) scanTenantRows(rows *sql.Rows) (*Tenant, error) {
	return s.scanTenant(rows)
}

// --- helpers ---

func isIndexStmt(stmt string) bool {
	s := strings.ToUpper(strings.TrimSpace(stmt))
	return strings.HasPrefix(s, "CREATE INDEX") || strings.HasPrefix(s, "CREATE UNIQUE INDEX")
}

func isDuplicateIndex(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Duplicate key name")
}

func isDuplicateEntry(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || strings.Contains(msg, "UNIQUE constraint failed")
}
