package vault

import (
	"crypto/rand"
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
		TokenID:   "cap_test123",
		TenantID:  "tenant-1",
		AgentID:   "agent-1",
		TaskID:    "task-1",
		Scope:     []string{"aws-prod", "db-prod/password"},
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
		Nonce:     "deadbeef",
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
	if parsed.TokenID != claims.TokenID {
		t.Fatalf("token_id mismatch: %s != %s", parsed.TokenID, claims.TokenID)
	}
	if parsed.AgentID != claims.AgentID {
		t.Fatalf("agent_id mismatch")
	}
	if len(parsed.Scope) != 2 {
		t.Fatalf("scope length mismatch")
	}
}

func TestCapTokenExpired(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-1")

	claims := &CapTokenClaims{
		TokenID:   "cap_expired",
		TenantID:  "tenant-1",
		AgentID:   "agent-1",
		Scope:     []string{"test"},
		IssuedAt:  time.Now().Add(-2 * time.Hour).Unix(),
		ExpiresAt: time.Now().Add(-1 * time.Hour).Unix(),
		Nonce:     "abc",
	}

	tokenStr, _ := SignCapToken(csk, claims)

	_, err := VerifyCapToken(csk, tokenStr, time.Now())
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestCapTokenBadSignature(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-1")

	claims := &CapTokenClaims{
		TokenID:   "cap_badsig",
		TenantID:  "tenant-1",
		AgentID:   "agent-1",
		Scope:     []string{"test"},
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
		Nonce:     "xyz",
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
	rand.Read(key)
	mk, _ := NewMasterKey(key)
	csk := mk.DeriveCSK("tenant-42")

	claims := &CapTokenClaims{
		TokenID:   "cap_test",
		TenantID:  "tenant-42",
		AgentID:   "agent-1",
		Scope:     []string{"secret-a"},
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
		Nonce:     "abc",
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
	rand.Read(key)
	mk, _ := NewMasterKey(key)

	csk1 := mk.DeriveCSK("tenant-1")
	csk2 := mk.DeriveCSK("tenant-2")

	if string(csk1) == string(csk2) {
		t.Fatal("different tenants should have different CSKs")
	}
}
