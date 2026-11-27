package mountstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"

	"github.com/mem9-ai/drive9/pkg/mountpath"
)

func SwitchScope(server, credScope, remoteRoot, mountPoint string) (string, error) {
	if strings.TrimSpace(server) == "" || credScope == "" || strings.TrimSpace(credScope) != credScope || !filepath.IsAbs(mountPoint) {
		return "", errors.New("incomplete mount switch scope")
	}
	root, err := mountpath.NormalizeRoot(remoteRoot)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal([4]string{strings.TrimRight(server, "/"), credScope, root, filepath.Clean(mountPoint)})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// SwitchJournalPath returns the per-mount-incarnation journal path inside stateDir.
func SwitchJournalPath(mountID string) (string, error) {
	if err := ValidateOperationID(mountID); err != nil {
		return "", err
	}
	return filepath.Join(stateDir(), "drive9-view-"+mountID+".log"), nil
}

// SwitchScopedJournalPath returns a scope-prefixed per-incarnation journal path
// inside stateDir so unrelated mount scopes can skip each other without reading
// or replaying foreign journal files.
func SwitchScopedJournalPath(scope, mountID string) (string, error) {
	if len(scope) < 16 {
		return "", errors.New("invalid mount switch scope")
	}
	for i := 0; i < 16; i++ {
		switch c := scope[i]; {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return "", errors.New("invalid mount switch scope hex prefix")
		}
	}
	if err := ValidateOperationID(mountID); err != nil {
		return "", err
	}
	return filepath.Join(stateDir(), "drive9-view-"+scope[:16]+"-"+mountID+".log"), nil
}
