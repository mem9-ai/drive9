package vault

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func randomCSK(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func validClaims(exp time.Time) *VaultGrantClaims {
	return &VaultGrantClaims{
		Issuer:        "https://example.invalid",
		GrantID:       "grt_test",
		PrincipalType: PrincipalDelegated,
		Agent:         "agent-a",
		Scope:         []string{"aws-prod"},
		Perm:          GrantPermRead,
		ExpiresAt:     exp.Unix(),
	}
}

// signHandCraftedClaims signs an arbitrary JSON payload with the grant wire
// format so tests can construct HMAC-valid tokens that bypass SignGrant's
// structural validation. Used to exercise VerifyGrant's reject paths for
// missing-claim / unknown-claim scenarios.
func signHandCraftedClaims(t *testing.T, csk []byte, payload map[string]any) string {
	t.Helper()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(grantHeaderJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)
	signingInput := headerB64 + "." + payloadB64
	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return grantTokenPrefix + signingInput + "." + sig
}

func TestSignVerifyGrantHappyPath(t *testing.T) {
	csk := randomCSK(t)
	tok, err := SignGrant(csk, validClaims(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("SignGrant: %v", err)
	}
	if !strings.HasPrefix(tok, grantTokenPrefix) {
		t.Fatalf("token missing %q prefix: %q", grantTokenPrefix, tok)
	}
	claims, err := VerifyGrant(csk, tok, time.Now())
	if err != nil {
		t.Fatalf("VerifyGrant: %v", err)
	}
	if claims.GrantID != "grt_test" {
		t.Fatalf("grant_id roundtrip: %q", claims.GrantID)
	}
	if claims.PrincipalType != PrincipalDelegated {
		t.Fatalf("principal_type roundtrip: %q", claims.PrincipalType)
	}
}

// TestSignGrantWireFormatHasHeader locks the §16 wire format: the token after
// the vt_ prefix MUST be header.payload.sig with a base64url-decodable header
// carrying {"alg":"HS256","typ":"JWT"}. PR-B's ctx import will depend on this
// shape; a regression here cascades into the downstream decoder.
func TestSignGrantWireFormatHasHeader(t *testing.T) {
	csk := randomCSK(t)
	tok, err := SignGrant(csk, validClaims(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("SignGrant: %v", err)
	}
	body := strings.TrimPrefix(tok, grantTokenPrefix)
	parts := strings.Split(body, ".")
	if len(parts) != 3 {
		t.Fatalf("wire format must be 3-segment header.payload.sig, got %d segments", len(parts))
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("header base64 decode: %v", err)
	}
	var h map[string]string
	if err := json.Unmarshal(headerJSON, &h); err != nil {
		t.Fatalf("header JSON decode: %v", err)
	}
	if h["alg"] != "HS256" || h["typ"] != "JWT" {
		t.Fatalf("header mismatch: got %v", h)
	}
}

func TestSignGrantRejectsEmptyScope(t *testing.T) {
	csk := randomCSK(t)
	c := validClaims(time.Now().Add(time.Hour))
	c.Scope = nil
	if _, err := SignGrant(csk, c); err == nil {
		t.Fatal("expected SignGrant to reject empty scope")
	}
}

func TestSignGrantRejectsBadPerm(t *testing.T) {
	csk := randomCSK(t)
	c := validClaims(time.Now().Add(time.Hour))
	c.Perm = "admin"
	if _, err := SignGrant(csk, c); err == nil {
		t.Fatal("expected SignGrant to reject bad perm")
	}
}

func TestSignGrantRejectsBadPrincipal(t *testing.T) {
	csk := randomCSK(t)
	c := validClaims(time.Now().Add(time.Hour))
	c.PrincipalType = "root"
	if _, err := SignGrant(csk, c); err == nil {
		t.Fatal("expected SignGrant to reject bad principal_type")
	}
}

func TestVerifyGrantRejectsTampering(t *testing.T) {
	csk := randomCSK(t)
	tok, err := SignGrant(csk, validClaims(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("SignGrant: %v", err)
	}
	// Replace the signature with a valid-looking but wrong one.
	dot := strings.LastIndex(tok, ".")
	fake := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	tampered := tok[:dot+1] + fake
	if _, err := VerifyGrant(csk, tampered, time.Now()); err == nil {
		t.Fatal("expected VerifyGrant to reject tampered signature")
	}
}

func TestVerifyGrantRejectsWrongCSK(t *testing.T) {
	csk1 := randomCSK(t)
	csk2 := randomCSK(t)
	tok, err := SignGrant(csk1, validClaims(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("SignGrant: %v", err)
	}
	if _, err := VerifyGrant(csk2, tok, time.Now()); err == nil {
		t.Fatal("expected VerifyGrant to reject token signed with different CSK (cross-tenant replay)")
	}
}

func TestVerifyGrantExpiredBeyondSkew(t *testing.T) {
	csk := randomCSK(t)
	exp := time.Now().Add(-2 * time.Minute)
	tok, err := SignGrant(csk, validClaims(exp))
	if err != nil {
		t.Fatalf("SignGrant: %v", err)
	}
	if _, err := VerifyGrant(csk, tok, time.Now()); err == nil {
		t.Fatal("expected VerifyGrant to reject token expired beyond skew")
	}
}

func TestVerifyGrantAcceptsWithinSkew(t *testing.T) {
	csk := randomCSK(t)
	// exp 30s in the past — within ±60s leeway.
	exp := time.Now().Add(-30 * time.Second)
	tok, err := SignGrant(csk, validClaims(exp))
	if err != nil {
		t.Fatalf("SignGrant: %v", err)
	}
	if _, err := VerifyGrant(csk, tok, time.Now()); err != nil {
		t.Fatalf("VerifyGrant should accept token within skew: %v", err)
	}
}

// TestVerifyGrantRejectsUnknownClaims ensures the locked §16 claim set is
// enforced on decode: extra JSON fields must make VerifyGrant fail, so a
// signer that sneaks in (for example) `"tenant_id": "other"` can't smuggle
// bonus authority into a verifier that ignores unknown keys.
func TestVerifyGrantRejectsUnknownClaims(t *testing.T) {
	csk := randomCSK(t)
	tok := signHandCraftedClaims(t, csk, map[string]any{
		"iss":            "https://example.invalid",
		"grant_id":       "grt_test",
		"principal_type": string(PrincipalDelegated),
		"agent":          "agent-a",
		"scope":          []string{"aws-prod"},
		"perm":           string(GrantPermRead),
		"exp":            time.Now().Add(time.Hour).Unix(),
		"bogus":          "smuggled",
	})
	if _, err := VerifyGrant(csk, tok, time.Now()); err == nil {
		t.Fatal("expected VerifyGrant to reject unknown claim field")
	}
}

// TestVerifyGrantRejectsMissingRequiredClaim covers adv-2 Block A: an
// HMAC-valid token that omits one required §16 claim must be rejected at
// verify time, not silently zero-valued. Each required claim is tested
// independently to prevent a future refactor from widening the acceptance
// set.
func TestVerifyGrantRejectsMissingRequiredClaim(t *testing.T) {
	csk := randomCSK(t)
	fullPayload := func() map[string]any {
		return map[string]any{
			"iss":            "https://example.invalid",
			"grant_id":       "grt_test",
			"principal_type": string(PrincipalDelegated),
			"agent":          "agent-a",
			"scope":          []string{"aws-prod"},
			"perm":           string(GrantPermRead),
			"exp":            time.Now().Add(time.Hour).Unix(),
		}
	}
	required := []string{"iss", "grant_id", "principal_type", "agent", "scope", "perm", "exp"}
	for _, claim := range required {
		t.Run("missing_"+claim, func(t *testing.T) {
			payload := fullPayload()
			delete(payload, claim)
			tok := signHandCraftedClaims(t, csk, payload)
			if _, err := VerifyGrant(csk, tok, time.Now()); err == nil {
				t.Fatalf("expected VerifyGrant to reject token missing %q", claim)
			}
		})
	}
}

func TestVerifyGrantRejectsMissingPrefix(t *testing.T) {
	csk := randomCSK(t)
	tok, err := SignGrant(csk, validClaims(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("SignGrant: %v", err)
	}
	stripped := strings.TrimPrefix(tok, grantTokenPrefix)
	if _, err := VerifyGrant(csk, stripped, time.Now()); err == nil {
		t.Fatal("expected VerifyGrant to reject token without vt_ prefix")
	}
}
