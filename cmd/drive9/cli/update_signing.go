package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

// updateManifestPublicKey is the ed25519 public key the self-updater verifies the release
// manifest with. See internal/releasesign for why the manifest, and not the checksum match,
// is the thing that decides what gets installed.
//
// It is a var, not a const, so a fork or a self-hosted build can install its own key at link
// time:
//
//	go build -ldflags "-X github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<base64 public key>"
//
// A build with no key refuses to update instead of trusting whatever the origin serves.
var updateManifestPublicKey = "84uMULcNs9VhcuDjKg2SELH2RDgDBJ9Of03VnsU9faQ="

// releaseManifest is the authenticated part of a release: the version to compare against
// and the artifact checksums to verify downloaded bytes with.
type releaseManifest struct {
	version   string
	checksums map[string]string
}

// verifyUpdateManifestSignature is the updater's entry point into the shared signature
// contract.
func verifyUpdateManifestSignature(manifest, signature []byte, publicKey string) error {
	return releasesign.Verify(manifest, signature, publicKey)
}

// parseReleaseManifest parses the signed manifest: exactly one version line, then the
// artifact checksums. The version is required, because the manifest is what the updater
// trusts, and a manifest that cannot say which release it describes cannot be compared
// against the running binary.
func parseReleaseManifest(data []byte) (releaseManifest, error) {
	// The marker without its trailing space, so a line with an empty value is still
	// recognized as the version line instead of reading as a missing one.
	marker := strings.TrimSpace(releasesign.VersionPrefix)
	version := ""
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(trimmed, marker)
		if !ok || (rest != "" && !strings.HasPrefix(rest, " ")) {
			continue
		}
		if version != "" {
			return releaseManifest{}, errors.New("release manifest names more than one version")
		}
		version = strings.TrimSpace(rest)
		if version == "" {
			return releaseManifest{}, errors.New("release manifest names an empty version")
		}
	}
	if version == "" {
		return releaseManifest{}, fmt.Errorf("release manifest has no %q line", strings.TrimSpace(releasesign.VersionPrefix))
	}
	checksums, err := parseChecksums(data)
	if err != nil {
		return releaseManifest{}, err
	}
	if len(checksums) == 0 {
		return releaseManifest{}, errors.New("release manifest has an empty checksum list")
	}
	return releaseManifest{version: version, checksums: checksums}, nil
}
