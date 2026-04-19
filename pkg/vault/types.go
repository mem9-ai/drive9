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

// PrincipalType is the kind of principal bound to a credential (spec §16).
// "owner" credentials come from ctx add --api-key; "delegated" from vault grant.
type PrincipalType string

const (
	PrincipalOwner     PrincipalType = "owner"
	PrincipalDelegated PrincipalType = "delegated"
)

// Perm is the permission level carried by a delegated grant (spec §6).
type Perm string

const (
	PermRead  Perm = "read"
	PermWrite Perm = "write"
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

// CapToken is the server-side row for a capability grant (spec §16).
// GrantID is the stable identifier used by `vault revoke grt_...`.
type CapToken struct {
	GrantID       string        `json:"grant_id"`
	TenantID      string        `json:"tenant_id"`
	Issuer        string        `json:"iss"`
	PrincipalType PrincipalType `json:"principal_type"`
	Agent         string        `json:"agent"`
	Scope         []string      `json:"scope"`
	Perm          Perm          `json:"perm"`
	LabelHint     string        `json:"label_hint,omitempty"`
	IssuedAt      time.Time     `json:"issued_at"`
	ExpiresAt     time.Time     `json:"expires_at"`
	RevokedAt     *time.Time    `json:"revoked_at,omitempty"`
	RevokedBy     string        `json:"revoked_by,omitempty"`
	RevokeReason  string        `json:"revoke_reason,omitempty"`
}

// CapTokenClaims is the signed JWT-style payload (spec §16).
// Wire shape is fixed by spec; JSON tags MUST match exactly.
type CapTokenClaims struct {
	Issuer        string        `json:"iss"`
	PrincipalType PrincipalType `json:"principal_type"`
	GrantID       string        `json:"grant_id"`
	TenantID      string        `json:"tenant_id"`
	Agent         string        `json:"agent"`
	Scope         []string      `json:"scope"`
	Perm          Perm          `json:"perm"`
	IssuedAt      int64         `json:"iat"`
	ExpiresAt     int64         `json:"exp"`
	LabelHint     string        `json:"label_hint,omitempty"`
	Nonce         string        `json:"nonce"`
}

// AuditEvent is an append-only audit log entry.
type AuditEvent struct {
	EventID    string    `json:"event_id"`
	TenantID   string    `json:"tenant_id"`
	EventType  string    `json:"event_type"`
	GrantID    string    `json:"grant_id,omitempty"`
	Agent      string    `json:"agent,omitempty"`
	SecretName string    `json:"secret_name,omitempty"`
	FieldName  string    `json:"field_name,omitempty"`
	Adapter    string    `json:"adapter,omitempty"`
	Detail     any       `json:"detail,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

// TenantDEK holds the wrapped (encrypted) data encryption key for a tenant.
type TenantDEK struct {
	TenantID   string    `json:"tenant_id"`
	WrappedDEK []byte    `json:"-"`
	CreatedAt  time.Time `json:"created_at"`
}
