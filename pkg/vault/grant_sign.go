package vault

import (
	"bytes"
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

// grantHeaderJSON is the fixed JWT header for grant tokens. v0 hardcodes
// alg=HS256/typ=JWT per vault-interaction-end-state.md §16; verify does NOT
// trust this header — HS256 is enforced regardless of what the header says,
// so alg=none / alg-confusion attacks are moot. The header ships because the
// merged spec's wire examples (`vt_eyJhbGc...`) require it and PR-B's
// `ctx import` decoder is the downstream consumer.
var grantHeaderJSON = []byte(`{"alg":"HS256","typ":"JWT"}`)

// SignGrant signs a VaultGrantClaims payload with the tenant-derived CSK.
//
// Wire format per spec §16: "vt_" + base64url(header) + "." +
// base64url(payload) + "." + base64url(mac). The MAC input is
// base64url(header) + "." + base64url(payload) (JWT convention).
func SignGrant(csk []byte, claims *VaultGrantClaims) (string, error) {
	if err := validateClaimsForSign(claims); err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(grantHeaderJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := headerB64 + "." + payloadB64

	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return grantTokenPrefix + signingInput + "." + sig, nil
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

	// Wire per §16: header.payload.sig. Verify is HS256-only regardless of
	// what the header claims — we do not parse `alg` to pick an algorithm.
	parts := strings.Split(body, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid grant format")
	}
	headerB64, payloadB64, sigB64 := parts[0], parts[1], parts[2]

	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(headerB64 + "." + payloadB64))
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
	// Reject unknown claims: a locked claim set (§16) means any extra
	// field is either a malformed forgery or a silently-introduced payload
	// from a newer signer we don't trust.
	var claims VaultGrantClaims
	dec := json.NewDecoder(bytes.NewReader(payloadJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&claims); err != nil {
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
