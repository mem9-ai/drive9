// Package releasesign holds the release-manifest contract shared by the self-updater (which
// verifies), the release tooling (which signs and verifies), and the CLI build that pins the
// trusted keys.
//
// The updater downloads the checksum manifest and the binary from the same origin, so a
// checksum match proves only that the bytes arrived intact. Authenticity comes from a
// detached ed25519 signature over the manifest: the pinned public keys decide what may be
// installed, and the origin — however it is reached — only supplies bytes.
package releasesign

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"crypto/sha256"
)

const (
	// SignatureName is the detached signature file published next to the manifest.
	SignatureName = "checksums.txt.sig"
	// ManifestName is the signed checksum manifest.
	ManifestName = "checksums.txt"
	// VersionPrefix is the manifest line binding the release version to the checksums.
	// Signing it stops an origin from pairing an older signed binary with a newer version.
	VersionPrefix = "# drive9-release-version "
)

// Sentinel failures. They are intentionally caller-agnostic: the CLI adds "rebuild with -X",
// the release tool adds "-pubkey / DRIVE9_RELEASE_PUBLIC_KEY", and neither needs to parse a
// message to know which happened.
var (
	// ErrNoPublicKey means the caller supplied no trusted key at all.
	ErrNoPublicKey = errors.New("no release-signing public key configured")
	// ErrInvalidPublicKey means a configured key is not base64 or not a 32-byte ed25519 key.
	ErrInvalidPublicKey = errors.New("release-signing public key is invalid")
	// ErrNoSignature means the signature file is missing or empty.
	ErrNoSignature = errors.New("release manifest signature is missing or empty")
	// ErrInvalidSignature means the signature file is not a base64 64-byte ed25519 signature.
	ErrInvalidSignature = errors.New("release manifest signature is malformed")
	// ErrSignatureMismatch means no configured key verifies the manifest.
	ErrSignatureMismatch = errors.New("release manifest signature does not verify against any trusted release key")
	// ErrInvalidPrivateKey means the signing secret is not a usable ed25519 private key.
	ErrInvalidPrivateKey = errors.New("release-signing private key is invalid")
)

// Manifest is a parsed, signed release manifest: the version to compare against and the
// artifact checksums to verify downloaded bytes with.
type Manifest struct {
	Version   string
	Checksums map[string]string
}

// ParsePublicKeys decodes a comma-separated list of base64 ed25519 public keys. A set is
// allowed so a key rotation can overlap: the release that introduces a new key pins
// "old,new" and keeps being signed with the old one, so installed clients keep updating
// throughout the switch. Every entry is validated up front, so an empty or malformed element
// fails closed instead of silently shrinking the trusted set.
func ParsePublicKeys(encoded string) ([]ed25519.PublicKey, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return nil, ErrNoPublicKey
	}
	parts := strings.Split(encoded, ",")
	keys := make([]ed25519.PublicKey, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("%w: empty entry in the key list", ErrInvalidPublicKey)
		}
		raw, err := base64.StdEncoding.DecodeString(part)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("%w: %q is not a base64 %d-byte ed25519 public key", ErrInvalidPublicKey, part, ed25519.PublicKeySize)
		}
		keys = append(keys, ed25519.PublicKey(raw))
	}
	return keys, nil
}

// Verify checks a detached signature over the exact manifest bytes against a comma-separated
// key set, succeeding when any trusted key verifies it.
func Verify(manifest, signature []byte, publicKeys string) error {
	keys, err := ParsePublicKeys(publicKeys)
	if err != nil {
		return err
	}
	rawSignature, err := DecodeSignature(signature)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if ed25519.Verify(key, manifest, rawSignature) {
			return nil
		}
	}
	return ErrSignatureMismatch
}

// DecodeSignature parses a signature file. Only the bare base64 form is accepted: a file
// with extra structure is not something the release tooling writes, and guessing at formats
// is how a signature check turns into a formality.
func DecodeSignature(body []byte) ([]byte, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, fmt.Errorf("%w (%s)", ErrNoSignature, SignatureName)
	}
	signature, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("%w: not base64: %v", ErrInvalidSignature, err)
	}
	if len(signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: %d bytes, want %d", ErrInvalidSignature, len(signature), ed25519.SignatureSize)
	}
	return signature, nil
}

// Sign returns the detached signature file body for a manifest, in the one encoding Verify
// accepts. A key of the wrong length is an error rather than a panic: the signer runs in
// release automation, where a misconfigured secret should fail the job.
func Sign(manifest []byte, privateKey ed25519.PrivateKey) ([]byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: %d bytes, want %d", ErrInvalidPrivateKey, len(privateKey), ed25519.PrivateKeySize)
	}
	return []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, manifest)) + "\n"), nil
}

// ParsePrivateKey decodes a base64 ed25519 private key from its 32-byte seed or its 64-byte
// expanded form, so the release tooling does not care which shape the secret stores.
//
// The 64-byte form is checked against the seed it carries: crypto/ed25519 assumes the layout
// seed||publicKey and signs with the public half as given, so a 64-byte value from another
// tool (an OpenSSL scalar+prefix, a pasted signature) would sign happily and produce
// signatures no public key can verify. Deriving the expected key catches that at parse time.
func ParsePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return nil, fmt.Errorf("%w: empty", ErrInvalidPrivateKey)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: not base64: %v", ErrInvalidPrivateKey, err)
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		want := ed25519.NewKeyFromSeed(raw[:ed25519.SeedSize])
		if !want.Equal(ed25519.PrivateKey(raw)) {
			return nil, fmt.Errorf("%w: the 64-byte form is not seed||publicKey for the seed it carries", ErrInvalidPrivateKey)
		}
		return ed25519.PrivateKey(raw), nil
	default:
		return nil, fmt.Errorf("%w: %d bytes, want %d (seed) or %d (private key)", ErrInvalidPrivateKey, len(raw), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
}

// ParseManifest validates and parses a signed manifest in one pass: exactly one version line
// and at least one artifact checksum. The SAME parser runs in the updater (after the
// signature check) and in the release tool before signing, so a release cannot be signed
// with a shape the updater would reject, and the two sides cannot drift on the format.
func ParseManifest(data []byte) (Manifest, error) {
	// The marker without its trailing space, so a line with an empty value is still
	// recognized as the version line instead of reading as a missing one.
	marker := strings.TrimSpace(VersionPrefix)
	version := ""
	checksums := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(trimmed, marker); ok {
			if rest != "" && !strings.HasPrefix(rest, " ") {
				// A name that merely starts with the marker, not the version line.
				continue
			}
			if version != "" {
				return Manifest{}, errors.New("release manifest names more than one version")
			}
			version = strings.TrimSpace(rest)
			if version == "" {
				return Manifest{}, errors.New("release manifest names an empty version")
			}
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			// sha256sum --check ignores comment lines too, so the manifest can carry them.
			continue
		}
		name, sum, err := parseChecksumLine(trimmed)
		if err != nil {
			return Manifest{}, err
		}
		if _, exists := checksums[name]; exists {
			return Manifest{}, fmt.Errorf("duplicate checksum for %s", name)
		}
		checksums[name] = sum
	}
	if version == "" {
		return Manifest{}, fmt.Errorf("release manifest has no %q line", marker)
	}
	if len(checksums) == 0 {
		return Manifest{}, errors.New("release manifest has an empty checksum list")
	}
	return Manifest{Version: version, Checksums: checksums}, nil
}

func parseChecksumLine(line string) (name, sum string, err error) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", "", fmt.Errorf("invalid checksum line %q", line)
	}
	sum = strings.ToLower(fields[0])
	if len(sum) != sha256.Size*2 {
		return "", "", fmt.Errorf("invalid checksum for %s", fields[1])
	}
	clean := filepath.ToSlash(filepath.Clean(strings.TrimPrefix(fields[1], "*")))
	if clean == "." || clean == "" || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/") {
		return "", "", fmt.Errorf("checksum artifact %q must be a release artifact filename", fields[1])
	}
	return clean, sum, nil
}
