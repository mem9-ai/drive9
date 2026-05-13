package datastore

import (
	"crypto/sha256"
	"encoding/hex"
)

// StorageRefHash returns the stable lookup hash for a namespace-relative storage ref.
func StorageRefHash(ref string) string {
	if ref == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(ref))
	return hex.EncodeToString(sum[:])
}
