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
		PrincipalType: PrincipalOwner,
		Agent:         "agent-a",
		Scope:         []string{"aws-prod"},
		Perm:          GrantPermRead,
		ExpiresAt:     exp.Unix(),
	}
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
	if claims.PrincipalType != PrincipalOwner {
		t.Fatalf("principal_type roundtrip: %q", claims.PrincipalType)
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

	// Build a payload with a bogus extra field, using the same HMAC scheme
	// SignGrant uses so the signature still verifies; only the new
	// DisallowUnknownFields check should fail the verify.
	payload := map[string]any{
		"iss":            "https://example.invalid",
		"grant_id":       "grt_test",
		"principal_type": string(PrincipalOwner),
		"agent":          "agent-a",
		"scope":          []string{"aws-prod"},
		"perm":           string(GrantPermRead),
		"exp":            time.Now().Add(time.Hour).Unix(),
		"bogus":          "smuggled",
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)
	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(payloadB64))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	tok := grantTokenPrefix + payloadB64 + "." + sig

	if _, err := VerifyGrant(csk, tok, time.Now()); err == nil {
		t.Fatal("expected VerifyGrant to reject unknown claim field")
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
