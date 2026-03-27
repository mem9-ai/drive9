package tenant

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const (
	apiKeyPrefix = "dat9_"
	apiKeyBytes  = 32 // 256-bit random key
)

// GenerateAPIKey creates a new API key with format "dat9_<hex>".
// Returns (rawKey, prefix, sha256Hash).
// The raw key should be returned to the caller once and never stored.
func GenerateAPIKey() (raw, prefix, hash string, err error) {
	b := make([]byte, apiKeyBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", "", fmt.Errorf("generate API key: %w", err)
	}
	raw = apiKeyPrefix + hex.EncodeToString(b)
	prefix = raw[:8]
	h := sha256.Sum256([]byte(raw))
	hash = hex.EncodeToString(h[:])
	return raw, prefix, hash, nil
}

// HashAPIKey computes the SHA-256 hex hash of a raw API key for lookup.
func HashAPIKey(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}
