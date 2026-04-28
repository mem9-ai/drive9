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

// ---- Vault grant (terminal state per docs/specs/vault-interaction-end-state.md §16) ----
//
// VaultGrant and VaultGrantClaims implement the end-state token shape defined in
// the vault-interaction-end-state spec. They are added alongside the existing
// CapToken/CapTokenClaims types; the older types are deleted in PR-E per
// docs/specs/pr-e-removal-contract.md.

// PrincipalType enumerates the authorization principal behind a grant.
// Exactly one of "owner" or "delegated" — no other values are accepted.
type PrincipalType string

const (
	PrincipalOwner     PrincipalType = "owner"
	PrincipalDelegated PrincipalType = "delegated"
)

// GrantPerm enumerates the permission a grant carries.
// Exactly one of "read" or "write" — no other values are accepted.
type GrantPerm string

const (
	GrantPermRead  GrantPerm = "read"
	GrantPermWrite GrantPerm = "write"
)

// VaultGrant is the server-side state for a vault grant. The fields that are
// security-relevant (principal_type, perm, scope, issuer, exp) mirror the
// signed JWT claims, so a forged DB row cannot upgrade a grant's authority.
type VaultGrant struct {
	GrantID       string        `json:"grant_id"`
	TenantID      string        `json:"tenant_id"`
	Issuer        string        `json:"issuer"`
	PrincipalType PrincipalType `json:"principal_type"`
	Agent         string        `json:"agent"`
	Scope         []string      `json:"scope"`
	Perm          GrantPerm     `json:"perm"`
	LabelHint     string        `json:"label_hint,omitempty"`
	IssuedAt      time.Time     `json:"issued_at"`
	ExpiresAt     time.Time     `json:"expires_at"`
	RevokedAt     *time.Time    `json:"revoked_at,omitempty"`
	RevokedBy     string        `json:"revoked_by,omitempty"`
	RevokeReason  string        `json:"revoke_reason,omitempty"`
}

// VaultGrantClaims is the JWT payload signed into the grant token.
//
// Claim set is locked by spec §16:
//   - iss, grant_id, tenant_id, principal_type, agent, scope, perm, exp are required
//   - label_hint is optional and UX-only (Invariant #7 — never authz)
//
// tenant_id is a routing claim only — it allows the server to resolve the
// correct tenant backend before HMAC verification. It is NOT an authority
// claim: authorization is enforced by HMAC verification with the tenant-scoped
// CSK + DB grant row check. Tampering with tenant_id routes to the wrong
// tenant, where HMAC verification fails.
//
// task_id is deliberately absent (legacy Phase-0 concept; removed per §20).
type VaultGrantClaims struct {
	Issuer        string        `json:"iss"`
	GrantID       string        `json:"grant_id"`
	TenantID      string        `json:"tenant_id"`
	PrincipalType PrincipalType `json:"principal_type"`
	Agent         string        `json:"agent"`
	Scope         []string      `json:"scope"`
	Perm          GrantPerm     `json:"perm"`
	ExpiresAt     int64         `json:"exp"`
	LabelHint     string        `json:"label_hint,omitempty"`
}
