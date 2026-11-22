package mountstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"

	"github.com/mem9-ai/drive9/pkg/mountpath"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// SwitchScope binds a local journal to one authenticated filesystem and mount.
// TenantID must come from the authenticated server, never CLI input. The server
// string is hashed so URLs or credentials are never persisted to disk.
func SwitchScope(server, tenantID, remoteRoot, mountPoint string) (string, error) {
	if strings.TrimSpace(server) == "" || tenantID == "" || strings.TrimSpace(tenantID) != tenantID || !filepath.IsAbs(mountPoint) {
		return "", errors.New("incomplete mount switch scope")
	}
	root, err := mountpath.NormalizeRoot(remoteRoot)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal([4]string{strings.TrimRight(server, "/"), tenantID, root, filepath.Clean(mountPoint)})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// SwitchJournalPath returns the per-mount-incarnation journal path inside stateDir.
func SwitchJournalPath(mountID string) (string, error) {
	if err := viewprotocol.ValidateOperationID(mountID); err != nil {
		return "", err
	}
	return filepath.Join(stateDir(), "drive9-view-"+mountID+".log"), nil
}
