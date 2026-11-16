// Package releasesign holds the release-manifest signature contract shared by the
// self-updater (which verifies) and the release tooling (which signs).
//
// The updater downloads the checksum manifest and the binary from the same origin, so a
// checksum match proves only that the bytes arrived intact. Authenticity comes from a
// detached ed25519 signature over the manifest: the embedded public key decides what may be
// installed, and the origin — however it is reached — only supplies bytes.
package releasesign

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	// SignatureName is the detached signature file published next to the manifest.
	SignatureName = "checksums.txt.sig"
	// VersionPrefix is the manifest line binding the release version to the checksums.
	// Signing it stops an origin from pairing an older signed binary with a newer version.
	VersionPrefix = "# drive9-release-version "
)

// Sign returns the detached signature file body for a manifest, in the one encoding Verify
// accepts. A key of the wrong length is an error rather than a panic: the signer runs in
// release automation, where a misconfigured secret should fail the job.
func Sign(manifest []byte, privateKey ed25519.PrivateKey) ([]byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("signing key is %d bytes, want %d", len(privateKey), ed25519.PrivateKeySize)
	}
	return []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, manifest)) + "\n"), nil
}

// Verify checks a detached signature over the exact manifest bytes against a base64 ed25519
// public key. Every failure mode is fatal for the caller: an unsigned, wrongly signed, or
// unverifiable manifest is not a manifest to install from.
func Verify(manifest, signature []byte, publicKeyB64 string) error {
	publicKeyB64 = strings.TrimSpace(publicKeyB64)
	if publicKeyB64 == "" {
		return errors.New("this build carries no release-signing key; refusing to self-update (rebuild with -X " +
			"github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<base64 ed25519 public key>)")
	}
	rawKey, err := base64.StdEncoding.DecodeString(publicKeyB64)
	if err != nil || len(rawKey) != ed25519.PublicKeySize {
		return errors.New("this build carries an invalid release-signing key; refusing to self-update")
	}
	rawSignature, err := DecodeSignature(signature)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(rawKey), manifest, rawSignature) {
		return errors.New("release manifest signature does not verify against the release key; refusing to install")
	}
	return nil
}

// DecodeSignature parses a signature file. Only the bare base64 form is accepted: a file
// with extra structure is not something the release tooling writes, and guessing at formats
// is how a signature check turns into a formality.
func DecodeSignature(body []byte) ([]byte, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, fmt.Errorf("release manifest signature %s is missing or empty", SignatureName)
	}
	signature, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("release manifest signature is not valid base64: %w", err)
	}
	if len(signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("release manifest signature is %d bytes, want %d", len(signature), ed25519.SignatureSize)
	}
	return signature, nil
}

// ParsePrivateKey decodes a base64 ed25519 private key from its 32-byte seed or its 64-byte
// expanded form, so the release tooling does not care which shape the secret stores.
func ParsePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return nil, errors.New("release-signing key is empty")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("release-signing key is not valid base64: %w", err)
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(raw), nil
	default:
		return nil, fmt.Errorf("release-signing key is %d bytes, want %d (seed) or %d (private key)", len(raw), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
}
