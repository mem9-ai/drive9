package cli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// jwtClaims holds the subset of JWT payload fields the CLI consumes to
// populate a delegated Context. Per spec §16 these are required on a
// delegated JWT; see §13.1 for how they map into Context fields.
//
// Decoding is UX-only. Authorization is server-side (Invariant #7); the server
// MUST re-validate signature, TTL, and revocation on every request.
type jwtClaims struct {
	Iss           string   `json:"iss"`
	GrantID       string   `json:"grant_id"`
	PrincipalType string   `json:"principal_type"`
	Agent         string   `json:"agent"`
	Scope         []string `json:"scope"`
	Perm          string   `json:"perm"`
	Exp           int64    `json:"exp"`
	LabelHint     string   `json:"label_hint,omitempty"`
}

// decodeJWTPayload returns the parsed payload of a JWT without verifying its
// signature. Signature verification is the issuing server's responsibility
// (Invariant #7); this decode is strictly for local UX purposes (populating
// `ctx ls` and running the §17 short-circuits).
//
// Returns an error with a stable prefix for each distinguishable failure
// class so that callers can surface actionable messages without string-match:
//   - "malformed": token shape (not three dot-separated base64url segments)
//   - "decode":    base64url or JSON parse failed on the payload
func decodeJWTPayload(raw string) (*jwtClaims, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("malformed: empty token")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed: expected 3 dot-separated segments, got %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode: base64 payload: %w", err)
	}
	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("decode: json payload: %w", err)
	}
	return &claims, nil
}

// expTime converts the JWT exp claim to a time.Time in UTC.
func (c *jwtClaims) expTime() time.Time {
	if c.Exp == 0 {
		return time.Time{}
	}
	return time.Unix(c.Exp, 0).UTC()
}
