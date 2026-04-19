package vault

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// grantTokenPrefix is the display prefix on the wire for grant tokens (§16).
// Distinct from the legacy capTokenPrefix so the two surfaces never alias.
const grantTokenPrefix = "vt_"

// grantClockSkew is the ±leeway applied to exp verification (spec §16).
const grantClockSkew = 60 * time.Second

// SignGrant signs a VaultGrantClaims payload with the tenant-derived CSK.
//
// Wire format: "vt_" + base64url(payload) + "." + base64url(mac).
// Header-less JWT: v0 uses a fixed HS256/HMAC-SHA256 algorithm; there is no
// negotiation, so a header would only be attack surface.
func SignGrant(csk []byte, claims *VaultGrantClaims) (string, error) {
	if err := validateClaimsForSign(claims); err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)

	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(payloadB64))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return grantTokenPrefix + payloadB64 + "." + sig, nil
}

// VerifyGrant verifies the HMAC signature, unmarshals the claims, enforces the
// enum constraints on principal_type and perm, checks exp (with ±60s leeway),
// and validates scope. It does NOT perform DB revocation checks — the caller
// must do that (see VerifyAndResolveGrant).
//
// On any failure returns an error; callers map all failures to EACCES per §11.
// The returned error text must not leak tenant-specific information to HTTP
// clients; server handlers should translate these into opaque "invalid grant"
// / "grant expired" strings.
func VerifyGrant(csk []byte, raw string, now time.Time) (*VaultGrantClaims, error) {
	if !strings.HasPrefix(raw, grantTokenPrefix) {
		return nil, fmt.Errorf("invalid grant format")
	}
	body := raw[len(grantTokenPrefix):]

	// Split payload.sig on the last dot. Using LastIndex ensures we handle the
	// payload-first layout correctly even though our payloads do not contain '.'.
	dotIdx := strings.LastIndex(body, ".")
	if dotIdx < 0 {
		return nil, fmt.Errorf("invalid grant format")
	}
	payloadB64 := body[:dotIdx]
	sigB64 := body[dotIdx+1:]

	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(payloadB64))
	expectedSig := mac.Sum(nil)

	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, fmt.Errorf("decode signature: %w", err)
	}
	if !hmac.Equal(sig, expectedSig) {
		return nil, fmt.Errorf("invalid grant signature")
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	var claims VaultGrantClaims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal claims: %w", err)
	}

	if err := validateClaimsForVerify(&claims, now); err != nil {
		return nil, err
	}
	return &claims, nil
}

// validateClaimsForSign enforces minimum structural invariants before the
// server mints a token. These are redundant with validateClaimsForVerify but
// they prevent writing a malformed token to the DB.
func validateClaimsForSign(c *VaultGrantClaims) error {
	if c == nil {
		return fmt.Errorf("nil claims")
	}
	if c.Issuer == "" {
		return fmt.Errorf("iss is required")
	}
	if c.GrantID == "" {
		return fmt.Errorf("grant_id is required")
	}
	if c.PrincipalType != PrincipalOwner && c.PrincipalType != PrincipalDelegated {
		return fmt.Errorf("principal_type must be owner or delegated")
	}
	if c.Agent == "" {
		return fmt.Errorf("agent is required")
	}
	if c.Perm != GrantPermRead && c.Perm != GrantPermWrite {
		return fmt.Errorf("perm must be read or write")
	}
	if c.ExpiresAt <= 0 {
		return fmt.Errorf("exp is required")
	}
	if len(c.Scope) == 0 {
		return fmt.Errorf("scope must not be empty")
	}
	if err := ValidateScope(c.Scope); err != nil {
		return err
	}
	return nil
}

// validateClaimsForVerify enforces every fail-closed check listed in §16.
// Every failure here becomes EACCES at the HTTP boundary. The ordering matches
// docs/specs/pr-a-jwt-implementation.md §4:
//   1. enum principal_type  2. enum perm  3. scope validity  4. exp with leeway.
// iss / grant_id presence are checked implicitly via JSON unmarshal + emptiness
// (JSON unmarshal leaves empty strings for absent fields, which fail the checks).
func validateClaimsForVerify(c *VaultGrantClaims, now time.Time) error {
	if c.PrincipalType != PrincipalOwner && c.PrincipalType != PrincipalDelegated {
		return fmt.Errorf("malformed grant: bad principal_type")
	}
	if c.Perm != GrantPermRead && c.Perm != GrantPermWrite {
		return fmt.Errorf("malformed grant: bad perm")
	}
	if c.GrantID == "" || c.Issuer == "" || c.Agent == "" {
		return fmt.Errorf("malformed grant: missing required claim")
	}
	if len(c.Scope) == 0 {
		return fmt.Errorf("malformed grant: empty scope")
	}
	if err := ValidateScope(c.Scope); err != nil {
		return fmt.Errorf("malformed grant: %w", err)
	}
	if c.ExpiresAt <= 0 {
		return fmt.Errorf("malformed grant: missing exp")
	}
	// Clock skew leeway: we accept exp that is at most grantClockSkew in the past.
	if now.Unix() > c.ExpiresAt+int64(grantClockSkew/time.Second) {
		return fmt.Errorf("grant expired")
	}
	return nil
}
