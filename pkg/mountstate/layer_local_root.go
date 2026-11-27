package mountstate

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// LayerLocalRoot scopes persistent overlay data to its canonical layer.
func LayerLocalRoot(base, layerID string) (string, error) {
	if layerID == "" {
		return base, nil
	}
	if err := viewprotocol.ValidateLayerID(layerID); err != nil {
		return "", err
	}
	if base == "" {
		return "", nil
	}
	digest := sha256.Sum256([]byte(layerID))
	return filepath.Join(base, "views", hex.EncodeToString(digest[:])), nil
}
