package vault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// MasterKey wraps the server-wide master key used to wrap/unwrap tenant DEKs
// and derive capability signing keys.
type MasterKey struct {
	key []byte
	gcm cipher.AEAD
}

// NewMasterKey creates a MasterKey from raw 32-byte key material.
func NewMasterKey(key []byte) (*MasterKey, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("vault master key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes.NewCipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cipher.NewGCM: %w", err)
	}
	return &MasterKey{key: key, gcm: gcm}, nil
}

// GenerateDEK creates a new random 32-byte DEK and returns it wrapped (encrypted by MK).
func (mk *MasterKey) GenerateDEK() (plainDEK []byte, wrappedDEK []byte, err error) {
	plainDEK = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, plainDEK); err != nil {
		return nil, nil, fmt.Errorf("generate DEK: %w", err)
	}
	nonce := make([]byte, mk.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("generate nonce: %w", err)
	}
	wrappedDEK = mk.gcm.Seal(nonce, nonce, plainDEK, nil)
	return plainDEK, wrappedDEK, nil
}

// UnwrapDEK decrypts a wrapped DEK using the master key.
func (mk *MasterKey) UnwrapDEK(wrappedDEK []byte) ([]byte, error) {
	ns := mk.gcm.NonceSize()
	if len(wrappedDEK) < ns {
		return nil, fmt.Errorf("wrapped DEK too short")
	}
	plainDEK, err := mk.gcm.Open(nil, wrappedDEK[:ns], wrappedDEK[ns:], nil)
	if err != nil {
		return nil, fmt.Errorf("unwrap DEK: %w", err)
	}
	return plainDEK, nil
}

// DeriveCSK derives the Capability Signing Key from MK + tenantID.
func (mk *MasterKey) DeriveCSK(tenantID string) []byte {
	mac := hmac.New(sha256.New, mk.key)
	mac.Write([]byte("vault-csk:" + tenantID))
	return mac.Sum(nil)
}

// FieldEncryptor encrypts/decrypts individual secret field values using a DEK.
type FieldEncryptor struct {
	gcm cipher.AEAD
}

// NewFieldEncryptor creates a field encryptor from a plaintext DEK.
func NewFieldEncryptor(dek []byte) (*FieldEncryptor, error) {
	if len(dek) != 32 {
		return nil, fmt.Errorf("DEK must be 32 bytes")
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &FieldEncryptor{gcm: gcm}, nil
}

// Encrypt encrypts a plaintext field value. Returns (ciphertext, nonce).
func (fe *FieldEncryptor) Encrypt(plaintext []byte) (ciphertext []byte, nonce []byte, err error) {
	nonce = make([]byte, fe.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	ciphertext = fe.gcm.Seal(nil, nonce, plaintext, nil)
	return ciphertext, nonce, nil
}

// Decrypt decrypts a field value given ciphertext and nonce.
func (fe *FieldEncryptor) Decrypt(ciphertext, nonce []byte) ([]byte, error) {
	return fe.gcm.Open(nil, nonce, ciphertext, nil)
}

// ---- Capability Token signing/verification ----

const capTokenPrefix = "vault_"

// SignCapToken signs a CapTokenClaims payload with the tenant's CSK.
func SignCapToken(csk []byte, claims *CapTokenClaims) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)

	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(payloadB64))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return capTokenPrefix + payloadB64 + "." + sig, nil
}

// VerifyCapToken verifies an HMAC-signed capability token and returns claims.
// This only checks signature and TTL — caller must also check DB revocation.
func VerifyCapToken(csk []byte, raw string, now time.Time) (*CapTokenClaims, error) {
	if len(raw) < len(capTokenPrefix) {
		return nil, fmt.Errorf("invalid capability token format")
	}
	body := raw[len(capTokenPrefix):]

	// Split payload.sig
	dotIdx := -1
	for i := len(body) - 1; i >= 0; i-- {
		if body[i] == '.' {
			dotIdx = i
			break
		}
	}
	if dotIdx < 0 {
		return nil, fmt.Errorf("invalid capability token format")
	}
	payloadB64 := body[:dotIdx]
	sigB64 := body[dotIdx+1:]

	// Verify HMAC signature.
	mac := hmac.New(sha256.New, csk)
	mac.Write([]byte(payloadB64))
	expectedSig := mac.Sum(nil)

	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, fmt.Errorf("decode signature: %w", err)
	}
	if !hmac.Equal(sig, expectedSig) {
		return nil, fmt.Errorf("invalid token signature")
	}

	// Decode claims.
	payloadJSON, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	var claims CapTokenClaims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal claims: %w", err)
	}

	// Check TTL.
	if now.Unix() > claims.ExpiresAt {
		return nil, fmt.Errorf("token expired")
	}

	return &claims, nil
}

// PeekCapTokenTenantID extracts the tenant_id from a capability token's payload
// WITHOUT verifying the HMAC signature. This is used only to resolve the tenant
// backend so that the full verification can proceed. The caller MUST still do
// full verification (SignCapToken/VerifyCapToken) before trusting any claims.
func PeekCapTokenTenantID(raw string) (string, error) {
	if len(raw) < len(capTokenPrefix) {
		return "", fmt.Errorf("invalid capability token format")
	}
	body := raw[len(capTokenPrefix):]

	dotIdx := -1
	for i := len(body) - 1; i >= 0; i-- {
		if body[i] == '.' {
			dotIdx = i
			break
		}
	}
	if dotIdx < 0 {
		return "", fmt.Errorf("invalid capability token format")
	}
	payloadB64 := body[:dotIdx]

	payloadJSON, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}
	var peek struct {
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(payloadJSON, &peek); err != nil {
		return "", fmt.Errorf("unmarshal claims: %w", err)
	}
	if peek.TenantID == "" {
		return "", fmt.Errorf("missing tenant_id in token")
	}
	return peek.TenantID, nil
}

// GenerateNonce returns a random 16-byte nonce as hex string.
func GenerateNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}
