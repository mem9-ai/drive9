package vault

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Store provides CRUD operations for the vault data model.
type Store struct {
	db *sql.DB
	mk *MasterKey
}

// NewStore creates a new vault store.
func NewStore(db *sql.DB, mk *MasterKey) *Store {
	return &Store{db: db, mk: mk}
}

// DB returns the underlying database connection.
func (s *Store) DB() *sql.DB { return s.db }

// ---- DEK Management ----

// GetOrCreateDEK returns the plaintext DEK for a tenant, creating one if needed.
func (s *Store) GetOrCreateDEK(ctx context.Context, tenantID string) ([]byte, error) {
	var wrappedDEK []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT wrapped_dek FROM vault_deks WHERE tenant_id = ?`, tenantID,
	).Scan(&wrappedDEK)

	if err == nil {
		return s.mk.UnwrapDEK(wrappedDEK)
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("query DEK: %w", err)
	}

	// Generate new DEK.
	_, wrappedDEK, err = s.mk.GenerateDEK()
	if err != nil {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT IGNORE INTO vault_deks (tenant_id, wrapped_dek) VALUES (?, ?)`,
		tenantID, wrappedDEK,
	)
	if err != nil {
		return nil, fmt.Errorf("insert DEK: %w", err)
	}

	// Re-read in case of race (another process inserted first).
	err = s.db.QueryRowContext(ctx,
		`SELECT wrapped_dek FROM vault_deks WHERE tenant_id = ?`, tenantID,
	).Scan(&wrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("re-read DEK: %w", err)
	}
	return s.mk.UnwrapDEK(wrappedDEK)
}

// fieldEncryptor returns a FieldEncryptor for the given tenant.
func (s *Store) fieldEncryptor(ctx context.Context, tenantID string) (*FieldEncryptor, error) {
	dek, err := s.GetOrCreateDEK(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return NewFieldEncryptor(dek)
}

// ---- Secret CRUD ----

// CreateSecret creates a new secret with encrypted fields.
func (s *Store) CreateSecret(ctx context.Context, tenantID, name, createdBy string, secretType SecretType, fields map[string][]byte) (*Secret, error) {
	fe, err := s.fieldEncryptor(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	secretID := uuid.NewString()
	now := time.Now()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO vault_secrets (secret_id, tenant_id, name, secret_type, revision, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?, ?)`,
		secretID, tenantID, name, string(secretType), createdBy, now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("insert secret: %w", err)
	}

	for fieldName, plaintext := range fields {
		ciphertext, nonce, err := fe.Encrypt(plaintext)
		if err != nil {
			return nil, fmt.Errorf("encrypt field %s: %w", fieldName, err)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO vault_secret_fields (secret_id, field_name, encrypted_value, nonce)
			 VALUES (?, ?, ?, ?)`,
			secretID, fieldName, ciphertext, nonce,
		)
		if err != nil {
			return nil, fmt.Errorf("insert field %s: %w", fieldName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &Secret{
		SecretID:   secretID,
		TenantID:   tenantID,
		Name:       name,
		SecretType: secretType,
		Revision:   1,
		CreatedBy:  createdBy,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

// GetSecret returns secret metadata (no decrypted values).
func (s *Store) GetSecret(ctx context.Context, tenantID, name string) (*Secret, error) {
	var sec Secret
	err := s.db.QueryRowContext(ctx,
		`SELECT secret_id, tenant_id, name, secret_type, revision, created_by, created_at, updated_at, deleted_at
		 FROM vault_secrets WHERE tenant_id = ? AND name = ? AND deleted_at IS NULL`,
		tenantID, name,
	).Scan(&sec.SecretID, &sec.TenantID, &sec.Name, &sec.SecretType, &sec.Revision,
		&sec.CreatedBy, &sec.CreatedAt, &sec.UpdatedAt, &sec.DeletedAt)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &sec, nil
}

// ListSecrets returns metadata for all non-deleted secrets in a tenant.
func (s *Store) ListSecrets(ctx context.Context, tenantID string) ([]*Secret, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT secret_id, tenant_id, name, secret_type, revision, created_by, created_at, updated_at
		 FROM vault_secrets WHERE tenant_id = ? AND deleted_at IS NULL ORDER BY name`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var secrets []*Secret
	for rows.Next() {
		var sec Secret
		if err := rows.Scan(&sec.SecretID, &sec.TenantID, &sec.Name, &sec.SecretType,
			&sec.Revision, &sec.CreatedBy, &sec.CreatedAt, &sec.UpdatedAt); err != nil {
			return nil, err
		}
		secrets = append(secrets, &sec)
	}
	return secrets, rows.Err()
}

// UpdateSecret rotates a secret's field values and bumps revision.
func (s *Store) UpdateSecret(ctx context.Context, tenantID, name, updatedBy string, fields map[string][]byte) (*Secret, error) {
	fe, err := s.fieldEncryptor(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var secretID string
	var revision int64
	now := time.Now()

	err = tx.QueryRowContext(ctx,
		`SELECT secret_id, revision FROM vault_secrets
		 WHERE tenant_id = ? AND name = ? AND deleted_at IS NULL
		 FOR UPDATE`,
		tenantID, name,
	).Scan(&secretID, &revision)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	newRevision := revision + 1
	_, err = tx.ExecContext(ctx,
		`UPDATE vault_secrets SET revision = ?, updated_at = ? WHERE secret_id = ?`,
		newRevision, now, secretID,
	)
	if err != nil {
		return nil, err
	}

	// Delete old fields, insert new ones.
	_, err = tx.ExecContext(ctx, `DELETE FROM vault_secret_fields WHERE secret_id = ?`, secretID)
	if err != nil {
		return nil, err
	}
	for fieldName, plaintext := range fields {
		ciphertext, nonce, err := fe.Encrypt(plaintext)
		if err != nil {
			return nil, fmt.Errorf("encrypt field %s: %w", fieldName, err)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO vault_secret_fields (secret_id, field_name, encrypted_value, nonce)
			 VALUES (?, ?, ?, ?)`,
			secretID, fieldName, ciphertext, nonce,
		)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &Secret{
		SecretID:  secretID,
		TenantID:  tenantID,
		Name:      name,
		Revision:  newRevision,
		UpdatedAt: now,
	}, nil
}

// DeleteSecret soft-deletes a secret.
func (s *Store) DeleteSecret(ctx context.Context, tenantID, name string) error {
	now := time.Now()
	res, err := s.db.ExecContext(ctx,
		`UPDATE vault_secrets SET deleted_at = ? WHERE tenant_id = ? AND name = ? AND deleted_at IS NULL`,
		now, tenantID, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ReadSecretFields decrypts and returns all fields of a secret.
func (s *Store) ReadSecretFields(ctx context.Context, tenantID, name string) (map[string][]byte, error) {
	sec, err := s.GetSecret(ctx, tenantID, name)
	if err != nil {
		return nil, err
	}
	fe, err := s.fieldEncryptor(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT field_name, encrypted_value, nonce FROM vault_secret_fields WHERE secret_id = ?`,
		sec.SecretID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	fields := make(map[string][]byte)
	for rows.Next() {
		var fieldName string
		var ciphertext, nonce []byte
		if err := rows.Scan(&fieldName, &ciphertext, &nonce); err != nil {
			return nil, err
		}
		plaintext, err := fe.Decrypt(ciphertext, nonce)
		if err != nil {
			return nil, fmt.Errorf("decrypt field %s: %w", fieldName, err)
		}
		fields[fieldName] = plaintext
	}
	return fields, rows.Err()
}

// ReadSecretField decrypts and returns a single field of a secret.
func (s *Store) ReadSecretField(ctx context.Context, tenantID, name, fieldName string) ([]byte, error) {
	sec, err := s.GetSecret(ctx, tenantID, name)
	if err != nil {
		return nil, err
	}
	fe, err := s.fieldEncryptor(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	var ciphertext, nonce []byte
	err = s.db.QueryRowContext(ctx,
		`SELECT encrypted_value, nonce FROM vault_secret_fields WHERE secret_id = ? AND field_name = ?`,
		sec.SecretID, fieldName,
	).Scan(&ciphertext, &nonce)
	if err == sql.ErrNoRows {
		return nil, ErrFieldNotFound
	}
	if err != nil {
		return nil, err
	}
	return fe.Decrypt(ciphertext, nonce)
}

// ---- Capability Grant ----

// IssueCapTokenParams holds the parameters for issuing a capability grant (spec §6).
type IssueCapTokenParams struct {
	TenantID      string
	Issuer        string        // JWT iss claim — the server URL the delegatee will contact
	PrincipalType PrincipalType // owner or delegated; vault grant always emits delegated
	Agent         string
	Scope         []string
	Perm          Perm
	LabelHint     string
	TTL           time.Duration
}

// IssueCapToken creates a capability grant and persists its server-side row.
// Returns the signed JWT bearer string and the CapToken row.
func (s *Store) IssueCapToken(ctx context.Context, p IssueCapTokenParams) (string, *CapToken, error) {
	grantID := "grt_" + uuid.NewString()[:8]
	now := time.Now()
	expiresAt := now.Add(p.TTL)

	nonce, err := GenerateNonce()
	if err != nil {
		return "", nil, err
	}

	claims := &CapTokenClaims{
		Issuer:        p.Issuer,
		PrincipalType: p.PrincipalType,
		GrantID:       grantID,
		TenantID:      p.TenantID,
		Agent:         p.Agent,
		Scope:         p.Scope,
		Perm:          p.Perm,
		IssuedAt:      now.Unix(),
		ExpiresAt:     expiresAt.Unix(),
		LabelHint:     p.LabelHint,
		Nonce:         nonce,
	}

	csk := s.mk.DeriveCSK(p.TenantID)
	tokenStr, err := SignCapToken(csk, claims)
	if err != nil {
		return "", nil, err
	}

	scopeJSON, _ := json.Marshal(p.Scope)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO vault_tokens (grant_id, tenant_id, issuer, principal_type, agent, scope_json, perm, label_hint, issued_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		grantID, p.TenantID, p.Issuer, string(p.PrincipalType), p.Agent, scopeJSON, string(p.Perm), p.LabelHint, now, expiresAt,
	)
	if err != nil {
		return "", nil, fmt.Errorf("insert grant: %w", err)
	}

	return tokenStr, &CapToken{
		GrantID:       grantID,
		TenantID:      p.TenantID,
		Issuer:        p.Issuer,
		PrincipalType: p.PrincipalType,
		Agent:         p.Agent,
		Scope:         p.Scope,
		Perm:          p.Perm,
		LabelHint:     p.LabelHint,
		IssuedAt:      now,
		ExpiresAt:     expiresAt,
	}, nil
}

// VerifyAndResolveCapToken performs the full 4-step verification flow.
// 1. HMAC signature check  2. TTL check  3. DB revocation check  4. Returns claims for scope check
func (s *Store) VerifyAndResolveCapToken(ctx context.Context, tenantID, raw string) (*CapTokenClaims, error) {
	csk := s.mk.DeriveCSK(tenantID)
	claims, err := VerifyCapToken(csk, raw, time.Now())
	if err != nil {
		return nil, err
	}

	// DB revocation check — scoped to tenant for isolation.
	var revokedAt *time.Time
	err = s.db.QueryRowContext(ctx,
		`SELECT revoked_at FROM vault_tokens WHERE tenant_id = ? AND grant_id = ?`,
		tenantID, claims.GrantID,
	).Scan(&revokedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("grant not found")
	}
	if err != nil {
		return nil, fmt.Errorf("query grant: %w", err)
	}
	if revokedAt != nil {
		return nil, fmt.Errorf("grant revoked")
	}

	return claims, nil
}

// RevokeCapToken sets revoked_at on a capability grant (spec §8).
// tenantID is required to enforce tenant isolation.
func (s *Store) RevokeCapToken(ctx context.Context, tenantID, grantID, revokedBy, reason string) error {
	now := time.Now()
	res, err := s.db.ExecContext(ctx,
		`UPDATE vault_tokens SET revoked_at = ?, revoked_by = ?, revoke_reason = ?
		 WHERE tenant_id = ? AND grant_id = ? AND revoked_at IS NULL`,
		now, revokedBy, reason, tenantID, grantID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ---- Audit Log ----

// WriteAuditEvent appends an audit event.
func (s *Store) WriteAuditEvent(ctx context.Context, event *AuditEvent) error {
	if event.EventID == "" {
		event.EventID = uuid.NewString()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	var detailJSON []byte
	if event.Detail != nil {
		detailJSON, _ = json.Marshal(event.Detail)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO vault_audit_log (event_id, tenant_id, event_type, grant_id, agent, secret_name, field_name, adapter, detail_json, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.EventID, event.TenantID, event.EventType, event.GrantID, event.Agent,
		event.SecretName, event.FieldName, event.Adapter, detailJSON, event.Timestamp,
	)
	return err
}

// QueryAuditLog returns audit events for a tenant, with optional filters.
func (s *Store) QueryAuditLog(ctx context.Context, tenantID string, secretName string, limit int) ([]*AuditEvent, error) {
	var query string
	var args []any

	if secretName != "" {
		query = `SELECT event_id, tenant_id, event_type, grant_id, agent, secret_name, field_name, adapter, detail_json, timestamp
			FROM vault_audit_log WHERE tenant_id = ? AND secret_name = ? ORDER BY timestamp DESC LIMIT ?`
		args = []any{tenantID, secretName, limit}
	} else {
		query = `SELECT event_id, tenant_id, event_type, grant_id, agent, secret_name, field_name, adapter, detail_json, timestamp
			FROM vault_audit_log WHERE tenant_id = ? ORDER BY timestamp DESC LIMIT ?`
		args = []any{tenantID, limit}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var events []*AuditEvent
	for rows.Next() {
		var ev AuditEvent
		var detailJSON []byte
		var grantID, agent sql.NullString
		if err := rows.Scan(&ev.EventID, &ev.TenantID, &ev.EventType, &grantID, &agent,
			&ev.SecretName, &ev.FieldName, &ev.Adapter, &detailJSON, &ev.Timestamp); err != nil {
			return nil, err
		}
		ev.GrantID = grantID.String
		ev.Agent = agent.String
		if detailJSON != nil {
			_ = json.Unmarshal(detailJSON, &ev.Detail)
		}
		events = append(events, &ev)
	}
	return events, rows.Err()
}
