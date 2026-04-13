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
		`SELECT wrapped_dek FROM vault_deks WHERE tenant_id = $1`, tenantID,
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
		`INSERT INTO vault_deks (tenant_id, wrapped_dek) VALUES ($1, $2) ON CONFLICT (tenant_id) DO NOTHING`,
		tenantID, wrappedDEK,
	)
	if err != nil {
		return nil, fmt.Errorf("insert DEK: %w", err)
	}

	// Re-read in case of race (another process inserted first).
	err = s.db.QueryRowContext(ctx,
		`SELECT wrapped_dek FROM vault_deks WHERE tenant_id = $1`, tenantID,
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
		 VALUES ($1, $2, $3, $4, 1, $5, $6, $6)`,
		secretID, tenantID, name, string(secretType), createdBy, now,
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
			 VALUES ($1, $2, $3, $4)`,
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
		 FROM vault_secrets WHERE tenant_id = $1 AND name = $2 AND deleted_at IS NULL`,
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
		 FROM vault_secrets WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY name`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
		 WHERE tenant_id = $1 AND name = $2 AND deleted_at IS NULL
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
		`UPDATE vault_secrets SET revision = $1, updated_at = $2 WHERE secret_id = $3`,
		newRevision, now, secretID,
	)
	if err != nil {
		return nil, err
	}

	// Delete old fields, insert new ones.
	_, err = tx.ExecContext(ctx, `DELETE FROM vault_secret_fields WHERE secret_id = $1`, secretID)
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
			 VALUES ($1, $2, $3, $4)`,
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
		`UPDATE vault_secrets SET deleted_at = $1 WHERE tenant_id = $2 AND name = $3 AND deleted_at IS NULL`,
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
		`SELECT field_name, encrypted_value, nonce FROM vault_secret_fields WHERE secret_id = $1`,
		sec.SecretID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
		`SELECT encrypted_value, nonce FROM vault_secret_fields WHERE secret_id = $1 AND field_name = $2`,
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

// ---- Capability Token ----

// IssueCapToken creates a capability token and persists its server-side state.
func (s *Store) IssueCapToken(ctx context.Context, tenantID, agentID, taskID string, scope []string, ttl time.Duration) (string, *CapToken, error) {
	tokenID := "cap_" + uuid.NewString()[:8]
	now := time.Now()
	expiresAt := now.Add(ttl)

	nonce, err := GenerateNonce()
	if err != nil {
		return "", nil, err
	}

	claims := &CapTokenClaims{
		TokenID:   tokenID,
		TenantID:  tenantID,
		AgentID:   agentID,
		TaskID:    taskID,
		Scope:     scope,
		IssuedAt:  now.Unix(),
		ExpiresAt: expiresAt.Unix(),
		Nonce:     nonce,
	}

	csk := s.mk.DeriveCSK(tenantID)
	tokenStr, err := SignCapToken(csk, claims)
	if err != nil {
		return "", nil, err
	}

	scopeJSON, _ := json.Marshal(scope)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO vault_tokens (token_id, tenant_id, agent_id, task_id, scope_json, issued_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tokenID, tenantID, agentID, taskID, scopeJSON, now, expiresAt,
	)
	if err != nil {
		return "", nil, fmt.Errorf("insert token: %w", err)
	}

	return tokenStr, &CapToken{
		TokenID:   tokenID,
		TenantID:  tenantID,
		AgentID:   agentID,
		TaskID:    taskID,
		Scope:     scope,
		IssuedAt:  now,
		ExpiresAt: expiresAt,
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

	// DB revocation check.
	var revokedAt *time.Time
	err = s.db.QueryRowContext(ctx,
		`SELECT revoked_at FROM vault_tokens WHERE token_id = $1`,
		claims.TokenID,
	).Scan(&revokedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("token not found")
	}
	if err != nil {
		return nil, fmt.Errorf("query token: %w", err)
	}
	if revokedAt != nil {
		return nil, fmt.Errorf("token revoked")
	}

	return claims, nil
}

// RevokeCapToken sets revoked_at on a capability token.
func (s *Store) RevokeCapToken(ctx context.Context, tokenID, revokedBy, reason string) error {
	now := time.Now()
	res, err := s.db.ExecContext(ctx,
		`UPDATE vault_tokens SET revoked_at = $1, revoked_by = $2, revoke_reason = $3
		 WHERE token_id = $4 AND revoked_at IS NULL`,
		now, revokedBy, reason, tokenID,
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
		`INSERT INTO vault_audit_log (event_id, tenant_id, event_type, token_id, agent_id, task_id, secret_name, field_name, adapter, detail_json, timestamp)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		event.EventID, event.TenantID, event.EventType, event.TokenID, event.AgentID,
		event.TaskID, event.SecretName, event.FieldName, event.Adapter, detailJSON, event.Timestamp,
	)
	return err
}

// QueryAuditLog returns audit events for a tenant, with optional filters.
func (s *Store) QueryAuditLog(ctx context.Context, tenantID string, secretName string, limit int) ([]*AuditEvent, error) {
	var query string
	var args []any

	if secretName != "" {
		query = `SELECT event_id, tenant_id, event_type, token_id, agent_id, task_id, secret_name, field_name, adapter, detail_json, timestamp
			FROM vault_audit_log WHERE tenant_id = $1 AND secret_name = $2 ORDER BY timestamp DESC LIMIT $3`
		args = []any{tenantID, secretName, limit}
	} else {
		query = `SELECT event_id, tenant_id, event_type, token_id, agent_id, task_id, secret_name, field_name, adapter, detail_json, timestamp
			FROM vault_audit_log WHERE tenant_id = $1 ORDER BY timestamp DESC LIMIT $2`
		args = []any{tenantID, limit}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*AuditEvent
	for rows.Next() {
		var ev AuditEvent
		var detailJSON []byte
		if err := rows.Scan(&ev.EventID, &ev.TenantID, &ev.EventType, &ev.TokenID, &ev.AgentID,
			&ev.TaskID, &ev.SecretName, &ev.FieldName, &ev.Adapter, &detailJSON, &ev.Timestamp); err != nil {
			return nil, err
		}
		if detailJSON != nil {
			_ = json.Unmarshal(detailJSON, &ev.Detail)
		}
		events = append(events, &ev)
	}
	return events, rows.Err()
}
