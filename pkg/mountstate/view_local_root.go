package mountstate

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// ViewLocalRoot selects persistent local-only data beneath the mount's existing
// local root. Main keeps its old directory. A Layer keeps the same directory
// across work/fence/generation changes and process restarts; no data is copied.
// The caller still owns the base directory's filesystem/credential scope.
func ViewLocalRoot(base string, binding viewprotocol.Binding) (string, error) {
	if err := binding.Validate(); err != nil {
		return "", err
	}
	if base == "" || binding.LayerID == "" {
		return base, nil
	}
	digest := sha256.Sum256([]byte(binding.LayerID))
	return filepath.Join(base, "views", hex.EncodeToString(digest[:])), nil
}
