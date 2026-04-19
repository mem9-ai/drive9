package vault

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"
)

func TestMasterKeyGenerateAndUnwrapDEK(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	mk, err := NewMasterKey(key)
	if err != nil {
		t.Fatal(err)
	}

	plainDEK, wrappedDEK, err := mk.GenerateDEK()
	if err != nil {
		t.Fatal(err)
	}
	if len(plainDEK) != 32 {
		t.Fatalf("expected 32-byte DEK, got %d", len(plainDEK))
	}
	if len(wrappedDEK) == 0 {
		t.Fatal("wrapped DEK is empty")
	}

	unwrapped, err := mk.UnwrapDEK(wrappedDEK)
	if err != nil {
		t.Fatal(err)
	}
	if string(unwrapped) != string(plainDEK) {
		t.Fatal("unwrapped DEK does not match original")
	}
}

func TestMasterKeyBadSize(t *testing.T) {
	_, err := NewMasterKey([]byte("too-short"))
	if err == nil {
		t.Fatal("expected error for short key")
	}
}

func TestFieldEncryptorRoundTrip(t *testing.T) {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	fe, err := NewFieldEncryptor(dek)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("my-secret-password")
	ciphertext, nonce, err := fe.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if len(ciphertext) == 0 || len(nonce) == 0 {
		t.Fatal("ciphertext or nonce is empty")
	}

	decrypted, err := fe.Decrypt(ciphertext, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(plaintext) {
		t.Fatalf("decrypted %q != original %q", decrypted, plaintext)
	}
}

func TestFieldEncryptorDifferentNonces(t *testing.T) {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	fe, err := NewFieldEncryptor(dek)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("same-value")
	ct1, n1, _ := fe.Encrypt(plaintext)
	ct2, n2, _ := fe.Encrypt(plaintext)

	// Same plaintext should produce different ciphertexts (different nonces).
	if string(ct1) == string(ct2) {
		t.Fatal("identical ciphertexts for same plaintext — nonce reuse")
	}
	if string(n1) == string(n2) {
		t.Fatal("identical nonces")
	}
}

func TestCapTokenSignVerify(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-1")

	claims := &CapTokenClaims{
		Issuer:        "https://drive9.example.com",
		PrincipalType: PrincipalDelegated,
		GrantID:       "grt_test123",
		TenantID:      "tenant-1",
		Agent:         "agent-1",
		Scope:         []string{"aws-prod", "db-prod/password"},
		Perm:          PermRead,
		IssuedAt:      time.Now().Unix(),
		ExpiresAt:     time.Now().Add(time.Hour).Unix(),
		LabelHint:     "agent-1-aws-prod",
		Nonce:         "deadbeef",
	}

	tokenStr, err := SignCapToken(csk, claims)
	if err != nil {
		t.Fatal(err)
	}

	// Verify should succeed.
	parsed, err := VerifyCapToken(csk, tokenStr, time.Now())
	if err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	if parsed.GrantID != claims.GrantID {
		t.Fatalf("grant_id mismatch: %s != %s", parsed.GrantID, claims.GrantID)
	}
	if parsed.Agent != claims.Agent {
		t.Fatalf("agent mismatch")
	}
	if parsed.Issuer != claims.Issuer {
		t.Fatalf("iss mismatch: %s != %s", parsed.Issuer, claims.Issuer)
	}
	if parsed.PrincipalType != PrincipalDelegated {
		t.Fatalf("principal_type mismatch: %s", parsed.PrincipalType)
	}
	if parsed.Perm != PermRead {
		t.Fatalf("perm mismatch: %s", parsed.Perm)
	}
	if parsed.LabelHint != claims.LabelHint {
		t.Fatalf("label_hint mismatch")
	}
	if len(parsed.Scope) != 2 {
		t.Fatalf("scope length mismatch")
	}
}

func TestCapTokenExpired(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-1")

	claims := &CapTokenClaims{
		Issuer:        "https://drive9.example.com",
		PrincipalType: PrincipalDelegated,
		GrantID:       "grt_expired",
		TenantID:      "tenant-1",
		Agent:         "agent-1",
		Scope:         []string{"test"},
		Perm:          PermRead,
		IssuedAt:      time.Now().Add(-2 * time.Hour).Unix(),
		ExpiresAt:     time.Now().Add(-1 * time.Hour).Unix(),
		Nonce:         "abc",
	}

	tokenStr, _ := SignCapToken(csk, claims)

	_, err := VerifyCapToken(csk, tokenStr, time.Now())
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestCapTokenBadSignature(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-1")

	claims := &CapTokenClaims{
		Issuer:        "https://drive9.example.com",
		PrincipalType: PrincipalDelegated,
		GrantID:       "grt_badsig",
		TenantID:      "tenant-1",
		Agent:         "agent-1",
		Scope:         []string{"test"},
		Perm:          PermRead,
		IssuedAt:      time.Now().Unix(),
		ExpiresAt:     time.Now().Add(time.Hour).Unix(),
		Nonce:         "xyz",
	}

	tokenStr, _ := SignCapToken(csk, claims)

	// Use wrong CSK.
	wrongCSK := mk.DeriveCSK("tenant-2")
	_, err := VerifyCapToken(wrongCSK, tokenStr, time.Now())
	if err == nil {
		t.Fatal("expected error for wrong signature")
	}
}

func TestPeekCapTokenTenantID(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-42")

	claims := &CapTokenClaims{
		Issuer:        "https://drive9.example.com",
		PrincipalType: PrincipalDelegated,
		GrantID:       "grt_peek",
		TenantID:      "tenant-42",
		Agent:         "agent-1",
		Scope:         []string{"secret-a"},
		Perm:          PermRead,
		IssuedAt:      time.Now().Unix(),
		ExpiresAt:     time.Now().Add(time.Hour).Unix(),
		Nonce:         "abc",
	}
	tokenStr, _ := SignCapToken(csk, claims)

	tenantID, err := PeekCapTokenTenantID(tokenStr)
	if err != nil {
		t.Fatalf("PeekCapTokenTenantID failed: %v", err)
	}
	if tenantID != "tenant-42" {
		t.Fatalf("expected tenant-42, got %s", tenantID)
	}

	// Invalid token.
	_, err = PeekCapTokenTenantID("garbage")
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
}

func TestDeriveCSKDifferentTenants(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	mk, _ := NewMasterKey(key)

	csk1 := mk.DeriveCSK("tenant-1")
	csk2 := mk.DeriveCSK("tenant-2")

	if string(csk1) == string(csk2) {
		t.Fatal("different tenants should have different CSKs")
	}
}

// TestVerifyCapToken_RejectsUnknownFields covers the locked-payload invariant:
// a forged payload carrying a silently-introduced field must be rejected even
// if the HMAC matches, to prevent attackers (or a compromised signer) from
// smuggling fields a future verifier might start honoring.
func TestVerifyCapToken_RejectsUnknownFields(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-unk")

	// Forge a payload with a valid MAC but an extra `admin` field the struct
	// doesn't declare. We do the MAC ourselves since SignCapToken marshals
	// CapTokenClaims directly and can't emit unknown fields.
	forgedPayload := []byte(`{"iss":"https://drive9.example.com","principal_type":"delegated","grant_id":"grt_x","tenant_id":"tenant-unk","agent":"a","scope":["s"],"perm":"read","iat":1,"exp":9999999999,"nonce":"n","admin":true}`)
	payloadB64 := base64.RawURLEncoding.EncodeToString(forgedPayload)
	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(payloadB64))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	tokenStr := capTokenPrefix + payloadB64 + "." + sigB64

	_, err := VerifyCapToken(csk, tokenStr, time.Now())
	if err == nil {
		t.Fatal("expected rejection of payload with unknown field, got nil")
	}
}

// TestVerifyCapToken_RejectsBadEnums covers fail-closed enum validation on the
// verify path: a forged payload with an out-of-spec principal_type or perm
// must not propagate to downstream callers (handlers / audit writers) that
// treat claims.Perm as a trusted string.
func TestVerifyCapToken_RejectsBadEnums(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-enum")

	forgeSigned := func(payload string) string {
		payloadB64 := base64.RawURLEncoding.EncodeToString([]byte(payload))
		mac := hmac.New(sha256.New, csk)
		mac.Write([]byte(payloadB64))
		sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		return capTokenPrefix + payloadB64 + "." + sigB64
	}

	// Bad principal_type.
	tokBadPrincipal := forgeSigned(`{"iss":"https://drive9.example.com","principal_type":"superuser","grant_id":"grt_x","tenant_id":"tenant-enum","agent":"a","scope":["s"],"perm":"read","iat":1,"exp":9999999999,"nonce":"n"}`)
	if _, err := VerifyCapToken(csk, tokBadPrincipal, time.Now()); err == nil {
		t.Fatal("expected rejection of bad principal_type, got nil")
	}

	// Bad perm.
	tokBadPerm := forgeSigned(`{"iss":"https://drive9.example.com","principal_type":"delegated","grant_id":"grt_x","tenant_id":"tenant-enum","agent":"a","scope":["s"],"perm":"admin","iat":1,"exp":9999999999,"nonce":"n"}`)
	if _, err := VerifyCapToken(csk, tokBadPerm, time.Now()); err == nil {
		t.Fatal("expected rejection of bad perm, got nil")
	}
}
