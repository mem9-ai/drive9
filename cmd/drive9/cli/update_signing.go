package cli

import (
	_ "embed"
	"fmt"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

// updateManifestKeyFile is the release key set this build trusts, one base64 ed25519 public
// key per line (or a single comma-separated list). It is embedded rather than duplicated in
// source so the release workflow can verify a manifest against the exact keys the shipped
// binary carries — the same file, not a copy that can drift.
//
//go:embed update_key.txt
var updateManifestKeyFile string

// updateManifestPublicKey is the key set the self-updater verifies the release manifest with.
// A set (comma-separated) is allowed so a rotation can overlap: pin "old,new" while releases
// are still signed by "old", then flip the signing secret and drop "old".
//
// It is a var, not a const, so a fork or a self-hosted build can install its own keys at link
// time:
//
//	go build -ldflags "-X github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<base64 public key[,more]>"
//
// A build with no key refuses to update instead of trusting whatever the origin serves.
var updateManifestPublicKey = updateManifestKeyFile

// verifyUpdateManifestSignature is the updater's entry point into the shared signature
// contract: the trusted pin, then the caller-specific rebuild hint.
func verifyUpdateManifestSignature(manifest, signature []byte, publicKeys string) error {
	if err := releasesign.Verify(manifest, signature, publicKeys); err != nil {
		return fmt.Errorf("%w (this build trusts: %s; rebuild with -X "+
			"github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<base64 public key[,more]>)",
			err, publicKeys)
	}
	return nil
}
