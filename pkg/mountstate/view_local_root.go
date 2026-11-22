package mountstate

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// ViewLocalRoot selects persistent local-only data beneath the mount's base
// local root. Main keeps base directly; each Layer gets a deterministic
// isolated directory keyed by canonical layer_id across generations.
func ViewLocalRoot(base string, binding viewprotocol.SwitchBinding) (string, error) {
	if err := binding.Validate(); err != nil {
		return "", err
	}
	if base == "" || binding.LayerID == "" {
		return base, nil
	}
	digest := sha256.Sum256([]byte(binding.LayerID))
	return filepath.Join(base, "views", hex.EncodeToString(digest[:])), nil
}
