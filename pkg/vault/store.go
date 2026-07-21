package vault

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mem9-ai/drive9/pkg/datastore"
)

// Store provides CRUD operations for the vault data model.
type Store struct {
	db    *sql.DB
	mk    *MasterKey
	scope datastore.Scope
}

// NewStore creates a new vault store for the standalone (per-tenant DB)
// schema shape.
func NewStore(db *sql.DB, mk *MasterKey) *Store {
	return NewStoreScoped(db, mk, datastore.StandaloneScope(0))
}

// NewStoreScoped creates a new vault store whose SQL is shaped by scope:
// standalone tables key tenant rows by tenant_id, the shared (multi-tenant)
// schema keys them by fs_id (see pkg/datastore.Scope).
func NewStoreScoped(db *sql.DB, mk *MasterKey, scope datastore.Scope) *Store {
	return &Store{db: db, mk: mk, scope: scope}
}

// DB returns the underlying database connection.
func (s *Store) DB() *sql.DB { return s.db }

// tenantCol returns the tenant-discriminator column name for vault tables
// under this Store's schema shape.
func (s *Store) tenantCol() string { return s.scope.TenantCol() }

// tenantArg returns the bind value for the tenant discriminator of vault
// tables: tenantID in standalone shape, the scope's fs_id in shared shape.
func (s *Store) tenantArg(tenantID string) any { return s.scope.TenantArg(tenantID) }

// ---- DEK Management ----

// GetOrCreateDEK returns the plaintext DEK for a tenant, creating one if needed.
func (s *Store) GetOrCreateDEK(ctx context.Context, tenantID string) ([]byte, error) {
	var wrappedDEK []byte
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT wrapped_dek FROM vault_deks WHERE %s = ?`, s.tenantCol()),
		s.tenantArg(tenantID),
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
		fmt.Sprintf(`INSERT IGNORE INTO vault_deks (%s, wrapped_dek) VALUES (?, ?)`, s.tenantCol()),
		s.tenantArg(tenantID), wrappedDEK,
	)
	if err != nil {
		return nil, fmt.Errorf("insert DEK: %w", err)
	}

	// Re-read in case of race (another process inserted first).
	err = s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT wrapped_dek FROM vault_deks WHERE %s = ?`, s.tenantCol()),
		s.tenantArg(tenantID),
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
		fmt.Sprintf(`INSERT INTO vault_secrets (secret_id, %s, name, secret_type, revision, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?, ?)`, s.tenantCol()),
		secretID, s.tenantArg(tenantID), name, string(secretType), createdBy, now, now,
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
			fmt.Sprintf(`INSERT INTO vault_secret_fields (%s)
			 VALUES (%s)`,
				s.scope.InsCols("secret_id, field_name, encrypted_value, nonce"),
				s.scope.InsVals("?, ?, ?, ?")),
			s.scope.Args(secretID, fieldName, ciphertext, nonce)...,
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
		fmt.Sprintf(`SELECT secret_id, %[1]s, name, secret_type, revision, created_by, created_at, updated_at, deleted_at
		 FROM vault_secrets WHERE %[1]s = ? AND name = ? AND deleted_at IS NULL`, s.tenantCol()),
		s.tenantArg(tenantID), name,
	).Scan(&sec.SecretID, &sec.TenantID, &sec.Name, &sec.SecretType, &sec.Revision,
		&sec.CreatedBy, &sec.CreatedAt, &sec.UpdatedAt, &sec.DeletedAt)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	// In shared shape the row stores fs_id, not the tenant UUID; the
	// app-layer tenant id comes from the request scope.
	sec.TenantID = tenantID
	return &sec, nil
}

// ListSecrets returns metadata for all non-deleted secrets in a tenant.
func (s *Store) ListSecrets(ctx context.Context, tenantID string) ([]*Secret, error) {
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT secret_id, %[1]s, name, secret_type, revision, created_by, created_at, updated_at
		 FROM vault_secrets WHERE %[1]s = ? AND deleted_at IS NULL ORDER BY name`, s.tenantCol()),
		s.tenantArg(tenantID),
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
		// In shared shape the row stores fs_id; stamp the app-layer tenant id.
		sec.TenantID = tenantID
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
		fmt.Sprintf(`SELECT secret_id, revision FROM vault_secrets
		 WHERE %s = ? AND name = ? AND deleted_at IS NULL
		 FOR UPDATE`, s.tenantCol()),
		s.tenantArg(tenantID), name,
	).Scan(&secretID, &revision)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	newRevision := revision + 1
	_, err = tx.ExecContext(ctx,
		fmt.Sprintf(`UPDATE vault_secrets SET revision = ?, updated_at = ? WHERE %s`, s.scope.And("secret_id = ?")),
		append([]any{newRevision, now}, s.scope.Args(secretID)...)...,
	)
	if err != nil {
		return nil, err
	}

	// Delete old fields, insert new ones.
	_, err = tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM vault_secret_fields WHERE %s`, s.scope.And("secret_id = ?")),
		s.scope.Args(secretID)...,
	)
	if err != nil {
		return nil, err
	}
	for fieldName, plaintext := range fields {
		ciphertext, nonce, err := fe.Encrypt(plaintext)
		if err != nil {
			return nil, fmt.Errorf("encrypt field %s: %w", fieldName, err)
		}
		_, err = tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO vault_secret_fields (%s)
			 VALUES (%s)`,
				s.scope.InsCols("secret_id, field_name, encrypted_value, nonce"),
				s.scope.InsVals("?, ?, ?, ?")),
			s.scope.Args(secretID, fieldName, ciphertext, nonce)...,
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

// DeleteSecret hard-deletes a secret and all of its fields.
//
// Previously this was a soft-delete (UPDATE ... SET deleted_at = NOW()), but
// the unique index uk_vault_secrets_tenant_name does not include deleted_at,
// so a subsequent `vault set <same-name>` collided with the tombstoned row and
// surfaced as "already exists" — a surprising and broken UX given the user
// just ran `vault rm`. Audit history lives in vault_audit_log, not in the
// secret row itself, so a hard delete preserves the audit trail while making
// rm + set work the way users expect.
//
// For backward compatibility with any historical soft-deleted rows, we drop
// the row regardless of deleted_at state (no `AND deleted_at IS NULL`), so a
// `vault rm` over a tombstoned row from the old scheme also clears the unique
// slot.
func (s *Store) DeleteSecret(ctx context.Context, tenantID, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var secretID string
	err = tx.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT secret_id FROM vault_secrets WHERE %s = ? AND name = ?`, s.tenantCol()),
		s.tenantArg(tenantID), name,
	).Scan(&secretID)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM vault_secret_fields WHERE %s`, s.scope.And("secret_id = ?")),
		s.scope.Args(secretID)...,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM vault_secrets WHERE %s`, s.scope.And("secret_id = ?")),
		s.scope.Args(secretID)...,
	); err != nil {
		return err
	}
	return tx.Commit()
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
		fmt.Sprintf(`SELECT field_name, encrypted_value, nonce FROM vault_secret_fields WHERE %s`, s.scope.And("secret_id = ?")),
		s.scope.Args(sec.SecretID)...,
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
		fmt.Sprintf(`SELECT encrypted_value, nonce FROM vault_secret_fields WHERE %s`, s.scope.And("secret_id = ? AND field_name = ?")),
		s.scope.Args(sec.SecretID, fieldName)...,
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
	tokenID := "cap_" + uuid.NewString()
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
		fmt.Sprintf(`INSERT INTO vault_tokens (token_id, %s, agent_id, task_id, scope_json, issued_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`, s.tenantCol()),
		tokenID, s.tenantArg(tenantID), agentID, taskID, scopeJSON, now, expiresAt,
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

	// DB revocation check — scoped to tenant for isolation.
	var revokedAt *time.Time
	err = s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT revoked_at FROM vault_tokens WHERE %s = ? AND token_id = ?`, s.tenantCol()),
		s.tenantArg(tenantID), claims.TokenID,
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
// tenantID is required to enforce tenant isolation.
func (s *Store) RevokeCapToken(ctx context.Context, tenantID, tokenID, revokedBy, reason string) error {
	now := time.Now()
	res, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`UPDATE vault_tokens SET revoked_at = ?, revoked_by = ?, revoke_reason = ?
		 WHERE %s = ? AND token_id = ? AND revoked_at IS NULL`, s.tenantCol()),
		now, revokedBy, reason, s.tenantArg(tenantID), tokenID,
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
		fmt.Sprintf(`INSERT INTO vault_audit_log (event_id, %s, event_type, token_id, agent_id, task_id, secret_name, field_name, adapter, detail_json, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.tenantCol()),
		event.EventID, s.tenantArg(event.TenantID), event.EventType, event.TokenID, event.AgentID,
		event.TaskID, event.SecretName, event.FieldName, event.Adapter, detailJSON, event.Timestamp,
	)
	return err
}

// QueryAuditLog returns audit events for a tenant, with optional filters.
func (s *Store) QueryAuditLog(ctx context.Context, tenantID string, secretName string, limit int) ([]*AuditEvent, error) {
	var query string
	var args []any

	col := s.tenantCol()
	if secretName != "" {
		query = fmt.Sprintf(`SELECT event_id, %[1]s, event_type, token_id, agent_id, task_id, secret_name, field_name, adapter, detail_json, timestamp
			FROM vault_audit_log WHERE %[1]s = ? AND secret_name = ? ORDER BY timestamp DESC LIMIT ?`, col)
		args = []any{s.tenantArg(tenantID), secretName, limit}
	} else {
		query = fmt.Sprintf(`SELECT event_id, %[1]s, event_type, token_id, agent_id, task_id, secret_name, field_name, adapter, detail_json, timestamp
			FROM vault_audit_log WHERE %[1]s = ? ORDER BY timestamp DESC LIMIT ?`, col)
		args = []any{s.tenantArg(tenantID), limit}
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
		if err := rows.Scan(&ev.EventID, &ev.TenantID, &ev.EventType, &ev.TokenID, &ev.AgentID,
			&ev.TaskID, &ev.SecretName, &ev.FieldName, &ev.Adapter, &detailJSON, &ev.Timestamp); err != nil {
			return nil, err
		}
		// In shared shape the row stores fs_id; stamp the app-layer tenant id.
		ev.TenantID = tenantID
		if detailJSON != nil {
			_ = json.Unmarshal(detailJSON, &ev.Detail)
		}
		events = append(events, &ev)
	}
	return events, rows.Err()
}
