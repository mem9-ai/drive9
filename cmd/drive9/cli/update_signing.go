package cli

import (
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

// updateManifestKeyFile is the release key set this build trusts: base64 ed25519 public
// keys, comma- or newline-separated (one per line reads best when a rotation pins two). It
// is embedded rather than duplicated in source so the release workflow can verify a manifest
// against the exact keys the shipped binary carries — the same bytes, not a copy that can
// drift or a normalization the binary never performs.
//
//go:embed update_key.txt
var updateManifestKeyFile string

// embeddedUpdateManifestKeys marks "no linker override", so the sentinel below is the value
// a plain build starts from.
const embeddedUpdateManifestKeys = "@embedded"

// updateManifestPublicKey is the key set the self-updater verifies with, and the value a fork
// or self-hosted build replaces at link time:
//
//	go build -ldflags "-X github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<key>[,<key>]"
//
// The initializer is a constant sentinel, not updateManifestKeyFile: -X only applies to a
// string variable initialized with a constant expression, so an initializer that referenced
// the embedded file would silently ignore the override. An explicit empty override means
// "trust no keys" and refuses to update; it must not fall back to the embedded keys.
var updateManifestPublicKey = embeddedUpdateManifestKeys

// trustedUpdateManifestKeys resolves the linker override, falling back to the embedded key
// file only when no override was applied.
func trustedUpdateManifestKeys() string {
	if updateManifestPublicKey == embeddedUpdateManifestKeys {
		return updateManifestKeyFile
	}
	return updateManifestPublicKey
}

// verifyUpdateRelease is the updater's entry point into the shared release contract: the same
// bundle verification the release tooling and the documented manual check run, plus the
// build-specific rebuild hint a key failure needs to be actionable.
func verifyUpdateRelease(manifest, manifestSignature, releaseInfo, releaseInfoSignature []byte, publicKeys string) (releasesign.Manifest, error) {
	release, err := releasesign.VerifyBundle(manifest, manifestSignature, releaseInfo, releaseInfoSignature, publicKeys)
	if err != nil {
		return releasesign.Manifest{}, updateSigningKeyHint(err, publicKeys)
	}
	return release, nil
}

// updateSigningKeyHint explains a failed signature check in terms of the key this build pins.
// Only key failures get the hint: a malformed manifest or a digest mismatch is not something
// rebuilding fixes, and telling the user to rebuild then would just mislead.
func updateSigningKeyHint(err error, publicKeys string) error {
	keyFailure := errors.Is(err, releasesign.ErrNoPublicKey) ||
		errors.Is(err, releasesign.ErrInvalidPublicKey) ||
		errors.Is(err, releasesign.ErrSignatureMismatch)
	if !keyFailure {
		return err
	}
	trusted := strings.Join(strings.Fields(publicKeys), ",")
	if trusted == "" {
		trusted = "(none)"
	}
	return fmt.Errorf("%w (this build trusts: %s; rebuild with -X "+
		"github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<key>[,<key>])",
		err, trusted)
}
