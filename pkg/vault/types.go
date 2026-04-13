package vault

import "time"

// SecretType identifies the kind of secret stored.
type SecretType string

const (
	SecretTypeGeneric        SecretType = "generic"
	SecretTypeAWSCredentials SecretType = "aws_credentials"
	SecretTypeDatabase       SecretType = "database"
	SecretTypeAPIKey         SecretType = "api_key"
	SecretTypeSSHKey         SecretType = "ssh_key"
	SecretTypeTLSCert        SecretType = "tls_cert"
)

// Secret is the metadata for a stored secret (no plaintext values).
type Secret struct {
	SecretID   string     `json:"secret_id"`
	TenantID   string     `json:"tenant_id"`
	Name       string     `json:"name"`
	SecretType SecretType `json:"secret_type"`
	Revision   int64      `json:"revision"`
	CreatedBy  string     `json:"created_by"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	DeletedAt  *time.Time `json:"deleted_at,omitempty"`
}

// SecretField holds the encrypted value and nonce for one field.
type SecretField struct {
	SecretID       string `json:"-"`
	FieldName      string `json:"field_name"`
	EncryptedValue []byte `json:"-"`
	Nonce          []byte `json:"-"`
}

// CapToken is the server-side state for a capability token.
type CapToken struct {
	TokenID     string     `json:"token_id"`
	TenantID    string     `json:"tenant_id"`
	AgentID     string     `json:"agent_id"`
	TaskID      string     `json:"task_id,omitempty"`
	Scope       []string   `json:"scope"`
	IssuedAt    time.Time  `json:"issued_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
	RevokedBy   string     `json:"revoked_by,omitempty"`
	RevokeReason string    `json:"revoke_reason,omitempty"`
}

// CapTokenClaims is the payload signed into the bearer token.
type CapTokenClaims struct {
	TokenID   string   `json:"token_id"`
	TenantID  string   `json:"tenant_id"`
	AgentID   string   `json:"agent_id"`
	TaskID    string   `json:"task_id,omitempty"`
	Scope     []string `json:"scope"`
	IssuedAt  int64    `json:"iat"`
	ExpiresAt int64    `json:"exp"`
	Nonce     string   `json:"nonce"`
}

// AuditEvent is an append-only audit log entry.
type AuditEvent struct {
	EventID    string     `json:"event_id"`
	TenantID   string     `json:"tenant_id"`
	EventType  string     `json:"event_type"`
	TokenID    string     `json:"token_id,omitempty"`
	AgentID    string     `json:"agent_id,omitempty"`
	TaskID     string     `json:"task_id,omitempty"`
	SecretName string     `json:"secret_name,omitempty"`
	FieldName  string     `json:"field_name,omitempty"`
	Adapter    string     `json:"adapter,omitempty"`
	Detail     any        `json:"detail,omitempty"`
	Timestamp  time.Time  `json:"timestamp"`
}

// TenantDEK holds the wrapped (encrypted) data encryption key for a tenant.
type TenantDEK struct {
	TenantID   string    `json:"tenant_id"`
	WrappedDEK []byte    `json:"-"`
	CreatedAt  time.Time `json:"created_at"`
}
