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
	publicKey := fs.String("pubkey", "", "base64 ed25519 public key for -verify (default $"+publicKeyEnv+")")
	if err := fs.Parse(args); err != nil {
		return err
	}
	files := fs.Args()
	if len(files) == 0 {
		return errors.New("no input files (usage: drive9-release-sign [-verify] <file>...)")
	}
	if *verify {
		if len(files) != 1 {
			return errors.New("-verify takes exactly one manifest path")
		}
		return verifyManifest(files[0], firstNonEmpty(*publicKey, os.Getenv(publicKeyEnv)))
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
			// An empty manifest would verify as a signature over nothing; refuse it here
			// instead of letting an empty release through.
			return fmt.Errorf("refusing to sign empty file %s", file)
		}
		// Sign only what the updater would accept: the same parser runs on both sides, so a
		// release cannot be signed with a shape (empty version, no checksums) that the CLI
		// would reject after publishing it.
		parsed, err := releasesign.ParseManifest(manifest)
		if err != nil {
			return fmt.Errorf("refusing to sign %s: %w", file, err)
		}
		fmt.Printf("signing %s for release %s\n", filepath.Base(file), parsed.Version)
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
	signaturePath := manifestPath + ".sig"
	signature, err := os.ReadFile(signaturePath)
	if err != nil {
		return fmt.Errorf("read %s: %w", signaturePath, err)
	}
	if err := releasesign.Verify(manifest, signature, publicKey); err != nil {
		return fmt.Errorf("%w (pass the release key with -pubkey or $%s; a comma-separated list is accepted)", err, publicKeyEnv)
	}
	fmt.Printf("signature OK: %s\n", manifestPath)
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
