// Command drive9-release-sign signs and verifies the two files the drive9 CLI's self-updater
// authenticates before installing anything: checksums.txt and release.txt.
//
// Sign (default):
//
//	DRIVE9_RELEASE_SIGNING_KEY=<base64 seed or private key> drive9-release-sign <file>...
//
// For each input it writes <file>.sig next to it. Only the two files the updater reads may be
// signed, each is parsed the way the updater parses it, and a release.txt whose digest does
// not describe its sibling checksums.txt is refused — signing that shape would publish a
// version bound to the wrong artifacts. The key comes from the environment and is never read
// from a file on disk, so a release job passes it as a secret. Removing the signature check
// from the release pipeline is the way to ship an unsigned manifest, so the tool fails
// closed: no key, no signature, no release.
//
// Verify one signature:
//
//	DRIVE9_RELEASE_PUBLIC_KEY=<key[,key]> drive9-release-sign -verify <file>
//
// checks <file>.sig against <file> and exits non-zero when it does not verify.
//
// Verify a whole release:
//
//	DRIVE9_RELEASE_PUBLIC_KEY=<key[,key]> drive9-release-sign -verify-release checksums.txt
//
// runs the updater's own bundle check: both signatures, the digest binding release.txt to
// checksums.txt, and the manifest structure. This is what the release job runs before
// uploading and what the release guide tells a user to run by hand, and it is the same
// releasesign.VerifyBundle the shipped CLI calls, so neither can drift from what an install
// actually requires.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

const (
	signingKeyEnv = "DRIVE9_RELEASE_SIGNING_KEY"
	publicKeyEnv  = "DRIVE9_RELEASE_PUBLIC_KEY"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "drive9-release-sign: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("drive9-release-sign", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	verify := fs.Bool("verify", false, "verify <file>.sig against <file> instead of signing")
	verifyRelease := fs.Bool("verify-release", false, "verify checksums.txt, release.txt, their signatures, and the version-to-manifest binding (takes the checksums.txt path)")
	publicKey := fs.String("pubkey", "", "base64 ed25519 public key (one, or a comma- or newline-separated set) for -verify/-verify-release (default $"+publicKeyEnv+")")
	if err := fs.Parse(args); err != nil {
		return err
	}
	files := fs.Args()
	if len(files) == 0 {
		return errors.New("no input files (usage: drive9-release-sign [-verify|-verify-release] <file>...)")
	}
	if *verify && *verifyRelease {
		return errors.New("-verify and -verify-release are mutually exclusive")
	}
	if *verify {
		if len(files) != 1 {
			return errors.New("-verify takes exactly one manifest path")
		}
		return verifyManifest(files[0], firstNonEmpty(*publicKey, os.Getenv(publicKeyEnv)))
	}
	if *verifyRelease {
		if len(files) != 1 {
			return errors.New("-verify-release takes exactly one checksums.txt path")
		}
		return verifyReleaseBundle(files[0], firstNonEmpty(*publicKey, os.Getenv(publicKeyEnv)))
	}
	privateKey, err := releasesign.ParsePrivateKey(os.Getenv(signingKeyEnv))
	if err != nil {
		return fmt.Errorf("%s: %w", signingKeyEnv, err)
	}
	for _, file := range files {
		manifest, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}
		if len(manifest) == 0 {
			// An empty file would verify as a signature over nothing; refuse it here
			// instead of letting an empty release through.
			return fmt.Errorf("refusing to sign empty file %s", file)
		}
		// Sign only what the updater would accept: the same parsers run on both sides, so a
		// release cannot be signed with a shape (empty version, no checksums, a manifest
		// release.txt does not describe) that the CLI would reject after publishing it.
		label, err := validateReleaseFile(file, manifest)
		if err != nil {
			return fmt.Errorf("refusing to sign %s: %w", file, err)
		}
		fmt.Printf("signing %s for release %s\n", filepath.Base(file), label)
		signature, err := releasesign.Sign(manifest, privateKey)
		if err != nil {
			return err
		}
		signaturePath := file + ".sig"
		if err := os.WriteFile(signaturePath, signature, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", signaturePath, err)
		}
		fmt.Printf("signed %s -> %s\n", file, filepath.Base(signaturePath))
	}
	return nil
}

func verifyManifest(manifestPath, publicKey string) error {
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", manifestPath, err)
	}
	if err := verifySignedFile(manifestPath, manifest, publicKey); err != nil {
		return err
	}
	fmt.Printf("signature OK: %s\n", manifestPath)
	return nil
}

// validateReleaseFile parses a file the way the updater will, and cross-checks the pair:
// release.txt must describe the checksums.txt sitting next to it.
func validateReleaseFile(file string, body []byte) (string, error) {
	switch filepath.Base(file) {
	case releasesign.ManifestName:
		if _, err := releasesign.ParseChecksums(body); err != nil {
			return "", err
		}
		return "a checksum manifest", nil
	case releasesign.ReleaseInfoName:
		info, err := releasesign.ParseReleaseInfo(body)
		if err != nil {
			return "", err
		}
		manifest, err := os.ReadFile(filepath.Join(filepath.Dir(file), releasesign.ManifestName))
		if err != nil {
			return "", fmt.Errorf("read the manifest it describes: %w", err)
		}
		if err := info.ValidateManifest(manifest); err != nil {
			return "", err
		}
		return info.Version, nil
	default:
		return "", fmt.Errorf("not a release artifact the updater reads (%s, %s)", releasesign.ManifestName, releasesign.ReleaseInfoName)
	}
}

// verifyReleaseBundle runs the same check the updater runs before it installs anything, and
// the same one the release job runs before it publishes: both signatures, then the digest
// that binds the version to this exact manifest, then the manifest structure. A mixed pair
// (one release's signed version, another release's signed checksums) fails here, which is the
// point — printing a digest is not checking it.
func verifyReleaseBundle(manifestPath, publicKey string) error {
	dir := filepath.Dir(manifestPath)
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", manifestPath, err)
	}
	infoPath := filepath.Join(dir, releasesign.ReleaseInfoName)
	info, err := os.ReadFile(infoPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", infoPath, err)
	}
	manifestSignature, err := readSignatureFile(manifestPath)
	if err != nil {
		return err
	}
	infoSignature, err := readSignatureFile(infoPath)
	if err != nil {
		return err
	}
	release, err := releasesign.VerifyBundle(manifest, manifestSignature, info, infoSignature, publicKey)
	if err != nil {
		if isSignatureFailure(err) {
			return fmt.Errorf("%w (pass the release key with -pubkey or $%s; a comma- or newline-separated set is accepted)", err, publicKeyEnv)
		}
		return err
	}
	fmt.Printf("release OK: version %s, %d artifacts, manifest %s\n",
		release.Version, len(release.Checksums), releasesign.ManifestDigest(manifest))
	return nil
}

// verifySignedFile reads <path>.sig and verifies it against the given key set.
func verifySignedFile(path string, body []byte, publicKey string) error {
	signature, err := readSignatureFile(path)
	if err != nil {
		return err
	}
	if err := releasesign.Verify(body, signature, publicKey); err != nil {
		return fmt.Errorf("%s: %w (pass the release key with -pubkey or $%s; a comma- or newline-separated set is accepted)", filepath.Base(path), err, publicKeyEnv)
	}
	return nil
}

// readSignatureFile reads the detached signature next to a signed file.
func readSignatureFile(path string) ([]byte, error) {
	signaturePath := path + ".sig"
	signature, err := os.ReadFile(signaturePath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", signaturePath, err)
	}
	return signature, nil
}

// isSignatureFailure reports whether a bundle failure was about keys or signatures rather
// than the bytes themselves, so only those get the "-pubkey / $DRIVE9_RELEASE_PUBLIC_KEY" hint.
func isSignatureFailure(err error) bool {
	return errors.Is(err, releasesign.ErrNoPublicKey) ||
		errors.Is(err, releasesign.ErrInvalidPublicKey) ||
		errors.Is(err, releasesign.ErrNoSignature) ||
		errors.Is(err, releasesign.ErrInvalidSignature) ||
		errors.Is(err, releasesign.ErrSignatureMismatch)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
