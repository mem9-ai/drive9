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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	// ManifestName is the signed checksum manifest, in the format every drive9 CLI has
	// parsed since before signing existed: "<sha256>  <artifact>" lines only.
	ManifestName = "checksums.txt"
	// SignatureName is its detached signature.
	SignatureName = "checksums.txt.sig"
	// ReleaseInfoName is the signed release description: the version plus the digest of the
	// manifest it describes.
	ReleaseInfoName = "release.txt"
	// ReleaseInfoSignatureName is the detached signature of release.txt.
	ReleaseInfoSignatureName = "release.txt.sig"

	// releaseVersionField and releaseManifestField are the release.txt keys.
	releaseVersionField  = "version"
	releaseManifestField = "checksums-sha256"
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

// ParseChecksums parses the manifest. The format is exactly what pre-signing CLIs already
// accept — "<sha256>  <artifact>" lines, nothing else — so a release stays installable by
// every CLI version: no comments, no headers, no version line. A manifest that carries any
// other line is rejected here, before it can be signed, because an old client's parser would
// reject it too and lose self-update entirely.
func ParseChecksums(data []byte) (map[string]string, error) {
	checksums := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		name, sum, err := parseChecksumLine(trimmed)
		if err != nil {
			return nil, err
		}
		if _, exists := checksums[name]; exists {
			return nil, fmt.Errorf("duplicate checksum for %s", name)
		}
		checksums[name] = sum
	}
	if len(checksums) == 0 {
		return nil, errors.New("release manifest has an empty checksum list")
	}
	return checksums, nil
}

// ManifestDigest is the digest release.txt binds to a manifest.
func ManifestDigest(manifest []byte) string {
	sum := sha256.Sum256(manifest)
	return hex.EncodeToString(sum[:])
}

// ReleaseInfo is what release.txt says: the version this release is, and the digest of the
// manifest it describes. Signing it binds the version to exactly one set of artifacts, which
// is what stops an origin from pairing an older signed binary with a newer version string.
type ReleaseInfo struct {
	Version        string
	ManifestSHA256 string
}

// ParseReleaseInfo parses and validates release.txt.
func ParseReleaseInfo(data []byte) (ReleaseInfo, error) {
	info := ReleaseInfo{}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		// A bare field name (no space) is the same key with an empty value, so the specific
		// "empty version" error fires instead of a generic malformed-line one.
		key, value, _ := strings.Cut(trimmed, " ")
		value = strings.TrimSpace(value)
		switch key {
		case releaseVersionField:
			if info.Version != "" {
				return ReleaseInfo{}, errors.New("release info names more than one version")
			}
			if value == "" {
				return ReleaseInfo{}, errors.New("release info names an empty version")
			}
			info.Version = value
		case releaseManifestField:
			if info.ManifestSHA256 != "" {
				return ReleaseInfo{}, errors.New("release info names more than one manifest digest")
			}
			raw, err := hex.DecodeString(value)
			if err != nil || len(raw) != sha256.Size {
				return ReleaseInfo{}, fmt.Errorf("release info manifest digest %q is not a sha256 hex digest", value)
			}
			info.ManifestSHA256 = strings.ToLower(value)
		default:
			return ReleaseInfo{}, fmt.Errorf("unknown release info field %q", key)
		}
	}
	if info.Version == "" {
		return ReleaseInfo{}, errors.New("release info has no version")
	}
	if info.ManifestSHA256 == "" {
		return ReleaseInfo{}, fmt.Errorf("release info has no %s", releaseManifestField)
	}
	return info, nil
}

// ValidateManifest checks that the manifest bytes are the ones this release info describes.
// Both files are signed independently, so this check is what keeps an origin from mixing a
// signed version with a different release's signed manifest.
func (ri ReleaseInfo) ValidateManifest(manifest []byte) error {
	if got := ManifestDigest(manifest); got != ri.ManifestSHA256 {
		return fmt.Errorf("release info describes manifest %s, but checksums.txt is %s", ri.ManifestSHA256, got)
	}
	return nil
}

func parseChecksumLine(line string) (name, sum string, err error) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", "", fmt.Errorf("invalid checksum line %q", line)
	}
	sum = strings.ToLower(fields[0])
	raw, err := hex.DecodeString(sum)
	if err != nil || len(raw) != sha256.Size {
		return "", "", fmt.Errorf("invalid checksum for %s: not a sha256 hex digest", fields[1])
	}
	clean := filepath.ToSlash(filepath.Clean(strings.TrimPrefix(fields[1], "*")))
	if clean == "." || clean == "" || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/") {
		return "", "", fmt.Errorf("checksum artifact %q must be a release artifact filename", fields[1])
	}
	return clean, sum, nil
}
