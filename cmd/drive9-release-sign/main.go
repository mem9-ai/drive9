// Command drive9-release-sign signs and verifies the release manifest the drive9 CLI's
// self-updater authenticates before installing anything.
//
// Sign (default):
//
//	DRIVE9_RELEASE_SIGNING_KEY=<base64 seed or private key> drive9-release-sign <file>...
//
// For each input it writes <file>.sig next to it. The key comes from the environment and is
// never read from a file on disk, so a release job passes it as a secret. Removing the
// signature check from the release pipeline is the way to ship an unsigned manifest, so the
// tool fails closed: no key, no signature, no release.
//
// Verify:
//
//	DRIVE9_RELEASE_PUBLIC_KEY=<base64 public key> drive9-release-sign -verify <file>
//
// checks <file>.sig against <file> and exits non-zero when it does not verify. It uses the
// same verifier the shipped CLI embeds, so the documented manual check cannot drift from the
// code that actually installs updates.
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
	publicKey := fs.String("pubkey", "", "base64 ed25519 public key (one, or a comma-separated set) for -verify/-verify-release (default $"+publicKeyEnv+")")
	if err := fs.Parse(args); err != nil {
		return err
	}
	files := fs.Args()
	if len(files) == 0 {
		return errors.New("no input files (usage: drive9-release-sign [-verify] <file>...)")
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

// verifyReleaseBundle is the manual counterpart of what the updater does before it installs
// anything: both signatures, then the digest that binds the version to this exact manifest,
// then the manifest structure. A mixed pair (one release's signed version, another release's
// signed checksums) fails here, which is the point — printing a digest is not checking it.
func verifyReleaseBundle(manifestPath, publicKey string) error {
	dir := filepath.Dir(manifestPath)
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", manifestPath, err)
	}
	if err := verifySignedFile(manifestPath, manifest, publicKey); err != nil {
		return err
	}
	infoPath := filepath.Join(dir, releasesign.ReleaseInfoName)
	info, err := os.ReadFile(infoPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", infoPath, err)
	}
	if err := verifySignedFile(infoPath, info, publicKey); err != nil {
		return err
	}
	parsed, err := releasesign.ParseReleaseInfo(info)
	if err != nil {
		return err
	}
	if err := parsed.ValidateManifest(manifest); err != nil {
		return err
	}
	checksums, err := releasesign.ParseChecksums(manifest)
	if err != nil {
		return err
	}
	fmt.Printf("release OK: version %s, %d artifacts, manifest %s\n", parsed.Version, len(checksums), parsed.ManifestSHA256)
	return nil
}

// verifySignedFile reads <path>.sig and verifies it against the given key set.
func verifySignedFile(path string, body []byte, publicKey string) error {
	signaturePath := path + ".sig"
	signature, err := os.ReadFile(signaturePath)
	if err != nil {
		return fmt.Errorf("read %s: %w", signaturePath, err)
	}
	if err := releasesign.Verify(body, signature, publicKey); err != nil {
		return fmt.Errorf("%s: %w (pass the release key with -pubkey or $%s; a comma-separated set is accepted)", filepath.Base(path), err, publicKeyEnv)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
