package vault

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// IssueGrant mints a grant token and persists its server-side row.
//
// Authorization: the caller is responsible for establishing that the
// originating principal has permission to mint this grant. IssueGrant only
// enforces structural invariants (scope validity, enum membership).
//
// The issuer parameter is the server URL that will appear as the `iss` claim;
// it is the server's own canonical URL, not a user-supplied value. The CSK is
// derived from tenantID so tokens minted under tenant A cannot be verified
// under tenant B even with DB access.
func (s *Store) IssueGrant(
	ctx context.Context,
	tenantID string,
	issuer string,
	principal PrincipalType,
	agent string,
	scope []string,
	perm GrantPerm,
	ttl time.Duration,
	labelHint string,
) (string, *VaultGrant, error) {
	if ttl <= 0 {
		return "", nil, fmt.Errorf("ttl must be positive")
	}
	if len(scope) == 0 {
		return "", nil, fmt.Errorf("scope must not be empty")
	}
	if err := ValidateScope(scope); err != nil {
		return "", nil, err
	}
	if principal != PrincipalOwner && principal != PrincipalDelegated {
		return "", nil, fmt.Errorf("invalid principal_type")
	}
	if perm != GrantPermRead && perm != GrantPermWrite {
		return "", nil, fmt.Errorf("invalid perm")
	}
	if agent == "" {
		return "", nil, fmt.Errorf("agent is required")
	}
	if issuer == "" {
		return "", nil, fmt.Errorf("issuer is required")
	}

	grantID := "grt_" + uuid.NewString()
	now := time.Now()
	expiresAt := now.Add(ttl)

	claims := &VaultGrantClaims{
		Issuer:        issuer,
		GrantID:       grantID,
		PrincipalType: principal,
		Agent:         agent,
		Scope:         scope,
		Perm:          perm,
		ExpiresAt:     expiresAt.Unix(),
		LabelHint:     labelHint,
	}

	csk := s.mk.DeriveCSK(tenantID)
	tokenStr, err := SignGrant(csk, claims)
	if err != nil {
		return "", nil, err
	}

	scopeJSON, _ := json.Marshal(scope)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO vault_grants (
			grant_id, tenant_id, issuer, principal_type, agent, scope_json,
			perm, label_hint, issued_at, expires_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		grantID, tenantID, issuer, string(principal), agent, scopeJSON,
		string(perm), nullableString(labelHint), now, expiresAt,
	)
	if err != nil {
		return "", nil, fmt.Errorf("insert grant: %w", err)
	}

	return tokenStr, &VaultGrant{
		GrantID:       grantID,
		TenantID:      tenantID,
		Issuer:        issuer,
		PrincipalType: principal,
		Agent:         agent,
		Scope:         scope,
		Perm:          perm,
		LabelHint:     labelHint,
		IssuedAt:      now,
		ExpiresAt:     expiresAt,
	}, nil
}

// VerifyAndResolveGrant runs the full 6-step fail-closed verification flow:
//  1. HMAC signature (via VerifyGrant)
//  2. exp with ±60s leeway (via VerifyGrant)
//  3. principal_type enum (via VerifyGrant)
//  4. perm enum (via VerifyGrant)
//  5. scope validity (via VerifyGrant)
//  6. DB revocation (scoped to tenant for isolation)
//
// Per pr-a-jwt-implementation.md §4 step 6: the DB query MUST filter on
// tenant_id to prevent cross-tenant token reuse.
//
// The iss claim is additionally checked against expectedIssuer; a mismatch
// fails closed. An empty expectedIssuer disables this check (useful for unit
// tests that do not have a canonical URL handy).
func (s *Store) VerifyAndResolveGrant(
	ctx context.Context,
	tenantID string,
	expectedIssuer string,
	raw string,
) (*VaultGrantClaims, error) {
	csk := s.mk.DeriveCSK(tenantID)
	claims, err := VerifyGrant(csk, raw, time.Now())
	if err != nil {
		return nil, err
	}

	if expectedIssuer != "" && claims.Issuer != expectedIssuer {
		return nil, fmt.Errorf("invalid issuer")
	}

	var revokedAt *time.Time
	err = s.db.QueryRowContext(ctx,
		`SELECT revoked_at FROM vault_grants WHERE tenant_id = ? AND grant_id = ?`,
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

// RevokeGrant sets revoked_at on a grant. tenantID is required for isolation.
// Returns ErrNotFound if the grant does not exist or is already revoked.
func (s *Store) RevokeGrant(ctx context.Context, tenantID, grantID, revokedBy, reason string) error {
	now := time.Now()
	res, err := s.db.ExecContext(ctx,
		`UPDATE vault_grants SET revoked_at = ?, revoked_by = ?, revoke_reason = ?
		 WHERE tenant_id = ? AND grant_id = ? AND revoked_at IS NULL`,
		now, revokedBy, reason, tenantID, grantID,
	)
	if err != nil {
		return fmt.Errorf("revoke grant: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// nullableString maps an empty string to a nil SQL value so that NULL/empty
// semantics match in the database. Used for optional claim fields like label_hint.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
