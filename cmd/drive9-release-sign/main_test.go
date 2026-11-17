package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

// testSigningKey is a throwaway key for this test file. The production key exists only as a
// repository secret and as the public half embedded in the CLI; a test fixture must never
// hold it, which is why this seed is unrelated to any published release.
var testSigningKey = ed25519.NewKeyFromSeed(bytes32(0x5a))

func bytes32(b byte) []byte {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = b
	}
	return seed
}

func testPublicKey() string {
	return base64.StdEncoding.EncodeToString(testSigningKey.Public().(ed25519.PublicKey))
}

func TestRunSignsManifestThatTheUpdaterVerifies(t *testing.T) {
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString(testSigningKey.Seed()))

	dir := t.TempDir()
	manifestPath := filepath.Join(dir, releasesign.ManifestName)
	infoPath := filepath.Join(dir, releasesign.ReleaseInfoName)
	manifest := []byte(strings.Repeat("a", 64) + "  drive9-linux-amd64\n")
	info := []byte("version v1.2.3\nchecksums-sha256 " + releasesign.ManifestDigest(manifest) + "\n")
	if err := os.WriteFile(manifestPath, manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(infoPath, info, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{manifestPath, infoPath}); err != nil {
		t.Fatalf("run: %v", err)
	}
	for path, body := range map[string][]byte{manifestPath: manifest, infoPath: info} {
		signature, err := os.ReadFile(path + ".sig")
		if err != nil {
			t.Fatalf("read signature %s: %v", path, err)
		}
		if err := releasesign.Verify(body, signature, testPublicKey()); err != nil {
			t.Fatalf("the signature this tool wrote for %s does not verify: %v", path, err)
		}
	}
}

func TestRunVerifyMode(t *testing.T) {
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString(testSigningKey.Seed()))
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, releasesign.ManifestName)
	manifest := []byte(strings.Repeat("b", 64) + "  drive9-linux-amd64\n")
	if err := os.WriteFile(manifestPath, manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{manifestPath}); err != nil {
		t.Fatalf("sign: %v", err)
	}
	t.Setenv(publicKeyEnv, testPublicKey())
	if err := run([]string{"-verify", manifestPath}); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// A different manifest under the same signature is not the signed one.
	if err := os.WriteFile(manifestPath, append(manifest, []byte(strings.Repeat("c", 64)+"  drive9-darwin-arm64\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-verify", manifestPath}); err == nil || !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("verify error = %v, want a verification failure", err)
	}
	// The wrong public key is a failure too, not a warning.
	t.Setenv(publicKeyEnv, base64.StdEncoding.EncodeToString(ed25519.NewKeyFromSeed(bytes32(0x6b)).Public().(ed25519.PublicKey)))
	if err := run([]string{"-verify", manifestPath}); err == nil || !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("verify with another key error = %v, want a verification failure", err)
	}
}

func TestRunRefusesWithoutKeyOrInput(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "checksums.txt")
	if err := os.WriteFile(manifestPath, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(signingKeyEnv, "")
	if err := run([]string{manifestPath}); err == nil || !strings.Contains(err.Error(), signingKeyEnv) {
		t.Fatalf("run without key error = %v, want %s", err, signingKeyEnv)
	}
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString(testSigningKey.Seed()))
	if err := run(nil); err == nil || !strings.Contains(err.Error(), "no input files") {
		t.Fatalf("run without files error = %v, want usage", err)
	}
	empty := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{empty}); err == nil || !strings.Contains(err.Error(), "refusing to sign empty") {
		t.Fatalf("run on empty file error = %v, want refusal", err)
	}
	// An unknown-length signing key is an error, not a panic.
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString([]byte("short")))
	if err := run([]string{manifestPath}); err == nil || !strings.Contains(err.Error(), signingKeyEnv) {
		t.Fatalf("run with a short key error = %v, want a key error", err)
	}
}

// The signer refuses a release the updater would reject: the same parsers run on both
// sides, and release.txt must describe the manifest next to it.
func TestRunRefusesManifestTheUpdaterWouldReject(t *testing.T) {
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString(testSigningKey.Seed()))
	dir := t.TempDir()

	// A manifest in the shape pre-change CLIs cannot parse.
	badManifest := filepath.Join(dir, releasesign.ManifestName)
	if err := os.WriteFile(badManifest, []byte("# drive9-release-version v1\n"+strings.Repeat("a", 64)+"  drive9-linux-amd64\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{badManifest}); err == nil || !strings.Contains(err.Error(), "refusing to sign") {
		t.Fatalf("run on a commented manifest error = %v, want a refusal", err)
	}
	if _, err := os.Stat(badManifest + ".sig"); !os.IsNotExist(err) {
		t.Fatalf("a rejected manifest was signed anyway: %v", err)
	}

	// release.txt that describes a different manifest.
	if err := os.WriteFile(badManifest, []byte(strings.Repeat("a", 64)+"  drive9-linux-amd64\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	infoPath := filepath.Join(dir, releasesign.ReleaseInfoName)
	pending := filepath.Join(dir, "pending.txt")
	info := []byte("version v1\nchecksums-sha256 " + releasesign.ManifestDigest([]byte("another manifest\n")) + "\n")
	if err := os.WriteFile(infoPath+".tmp", info, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(infoPath+".tmp", pending); err != nil {
		t.Fatal(err)
	}
	// A file that is not one of the two the updater reads is refused outright.
	if err := run([]string{pending}); err == nil || !strings.Contains(err.Error(), "not a release artifact") {
		t.Fatalf("run on an unknown file error = %v, want a refusal", err)
	}
}

// -verify-release is the manual counterpart of what the updater does: both signatures, then
// the digest that binds the version to this exact manifest. A mixed pair of genuine files
// must fail here, or the documented procedure would pass a release the updater refuses.
func TestRunVerifyReleaseBundle(t *testing.T) {
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString(testSigningKey.Seed()))
	t.Setenv(publicKeyEnv, testPublicKey())

	dir := t.TempDir()
	writeRelease := func(sub string, artifactBody string, version string) string {
		t.Helper()
		sub = filepath.Join(dir, sub)
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		manifest := []byte(releasesign.ManifestDigest([]byte(version)) + "  drive9-linux-amd64\n")
		info := []byte("version " + version + "\nchecksums-sha256 " + releasesign.ManifestDigest(manifest) + "\n")
		artifact := []byte(artifactBody)
		for name, body := range map[string][]byte{
			releasesign.ManifestName:    manifest,
			releasesign.ReleaseInfoName: info,
			"drive9-linux-amd64":        artifact,
		} {
			if err := os.WriteFile(filepath.Join(sub, name), body, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := run([]string{filepath.Join(sub, releasesign.ManifestName), filepath.Join(sub, releasesign.ReleaseInfoName)}); err != nil {
			t.Fatalf("sign %s: %v", sub, err)
		}
		return sub
	}
	older := writeRelease("older", "older", "v0.2.0")
	newer := writeRelease("newer", "newer", "v9.0.0")

	// A consistent release verifies.
	if err := run([]string{"-verify-release", filepath.Join(older, releasesign.ManifestName)}); err != nil {
		t.Fatalf("verify-release on a consistent pair: %v", err)
	}

	// Genuinely signed files from two releases do not form one release.
	for _, name := range []string{releasesign.ReleaseInfoName, releasesign.ReleaseInfoSignatureName} {
		body, err := os.ReadFile(filepath.Join(newer, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(older, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := run([]string{"-verify-release", filepath.Join(older, releasesign.ManifestName)}); err == nil || !strings.Contains(err.Error(), "release info describes manifest") {
		t.Fatalf("verify-release on a mixed pair error = %v, want a digest mismatch", err)
	}
}

// The guide's commands have to be runnable: no doubled continuations, and the manual route
// must delegate to the binding check rather than printing a digest.
func TestReleaseSigningGuideIsRunnable(t *testing.T) {
	guide, err := os.ReadFile("../../docs/guides/release-signing.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(guide)
	if strings.Contains(text, `\\`+"\n") {
		t.Fatal("the guide contains a doubled line continuation, so pasted commands break")
	}
	for _, want := range []string{"-verify-release checksums.txt", "checksums-sha256", "set -euo pipefail"} {
		if !strings.Contains(text, want) {
			t.Errorf("the manual verification section is missing %q", want)
		}
	}
	if strings.Contains(text, `'^checksums-sha256 '`) {
		t.Error("the guide still only prints the digest instead of comparing it")
	}
}
