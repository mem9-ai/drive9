package cli

import (
	_ "embed"
	"fmt"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

// updateManifestKeyFile is the release key set this build trusts: base64 ed25519 public
// keys, comma-separated. It is embedded rather than duplicated in source so the release
// workflow can verify a manifest against the exact keys the shipped binary carries — the
// same file, not a copy that can drift.
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

// verifyUpdateManifestSignature is the updater's entry point into the shared signature
// contract: the trusted pin, then the caller-specific rebuild hint.
func verifyUpdateManifestSignature(manifest, signature []byte, publicKeys string) error {
	if err := releasesign.Verify(manifest, signature, publicKeys); err != nil {
		return fmt.Errorf("%w (this build trusts: %s; rebuild with -X "+
			"github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<key>[,<key>])",
			err, publicKeys)
	}
	return nil
}
