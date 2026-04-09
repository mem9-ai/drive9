package token

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func testTokenSecret(t *testing.T) []byte {
	t.Helper()
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	return secret
}

func TestIssueTokenDefaultNeverExpires(t *testing.T) {
	secret := testTokenSecret(t)
	tok, err := IssueToken(secret, "tenant-1", 1)
	if err != nil {
		t.Fatal(err)
	}

	claims, err := ParseAndVerifyToken(secret, tok)
	if err != nil {
		t.Fatal(err)
	}
	if claims.ExpiresAt != 0 {
		t.Fatalf("expected default token without exp, got exp=%d", claims.ExpiresAt)
	}
	if strings.Count(tok, ".") != 0 {
		t.Fatalf("expected one-segment API key format, got token=%s", tok)
	}
}

func TestIssueTokenWithExpiryAddsExpClaim(t *testing.T) {
	secret := testTokenSecret(t)
	exp := time.Now().Add(2 * time.Minute).UTC().Truncate(time.Second)
	tok, err := IssueTokenWithExpiry(secret, "tenant-1", 1, exp)
	if err != nil {
		t.Fatal(err)
	}

	claims, err := ParseAndVerifyToken(secret, tok)
	if err != nil {
		t.Fatal(err)
	}
	if claims.ExpiresAt != exp.Unix() {
		t.Fatalf("expected exp=%d got=%d", exp.Unix(), claims.ExpiresAt)
	}
}

func TestParseAndVerifyTokenRejectsExpiredExpClaim(t *testing.T) {
	secret := testTokenSecret(t)
	tok, err := IssueTokenWithExpiry(secret, "tenant-1", 1, time.Now().Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	claims, err := ParseAndVerifyToken(secret, tok)
	if err != nil {
		t.Fatal(err)
	}
	_, err = parseAndVerifyTokenAt(secret, tok, claims.ExpiresAt)
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired error, got %v", err)
	}
}

func TestParseAndVerifyTokenRejectsLegacyThreeSegmentJWT(t *testing.T) {
	secret := testTokenSecret(t)
	tok, err := IssueToken(secret, "tenant-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	rawJWTBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(tok, tokenPrefix))
	if err != nil {
		t.Fatal(err)
	}
	rawJWT := string(rawJWTBytes)

	_, err = ParseAndVerifyToken(secret, rawJWT)
	if err == nil || !strings.Contains(err.Error(), "invalid token format") {
		t.Fatalf("expected invalid token format error, got %v", err)
	}
}
