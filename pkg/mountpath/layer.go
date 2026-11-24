package mountpath

import (
	"fmt"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// LayerRoots returns the canonical layer base and the mount subtree within it.
func LayerRoots(base, requested string, defaulted bool) (string, string, error) {
	if base == "" {
		return "", "", fmt.Errorf("layer base root is empty")
	}
	base, err := NormalizeRoot(base)
	if err != nil {
		return "", "", err
	}
	if requested == "" || defaulted {
		return base, base, nil
	}
	root, err := NormalizeRoot(requested)
	if err != nil {
		return "", "", err
	}
	if !viewprotocol.PathWithinBaseRoot(base, root) {
		return "", "", fmt.Errorf("remote root %q is outside layer base_root_path %q", root, base)
	}
	return base, root, nil
}
