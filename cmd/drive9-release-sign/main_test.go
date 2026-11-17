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
	manifestPath := filepath.Join(dir, "checksums.txt")
	manifest := []byte("# drive9-release-version v1.2.3\n" + strings.Repeat("a", 64) + "  drive9-linux-amd64\n")
	if err := os.WriteFile(manifestPath, manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{manifestPath}); err != nil {
		t.Fatalf("run: %v", err)
	}
	signature, err := os.ReadFile(manifestPath + ".sig")
	if err != nil {
		t.Fatalf("read signature: %v", err)
	}
	if err := releasesign.Verify(manifest, signature, testPublicKey()); err != nil {
		t.Fatalf("the signature this tool wrote does not verify: %v", err)
	}
}

func TestRunVerifyMode(t *testing.T) {
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString(testSigningKey.Seed()))
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "checksums.txt")
	manifest := []byte("# drive9-release-version v1.2.3\n" + strings.Repeat("b", 64) + "  drive9-linux-amd64\n")
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
	if err := os.WriteFile(manifestPath, append(manifest, []byte("# extra\n")...), 0o644); err != nil {
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

// The signer refuses a manifest the updater would reject: same parser on both sides, so a
// release cannot be published in a shape that no CLI will install.
func TestRunRefusesManifestTheUpdaterWouldReject(t *testing.T) {
	t.Setenv(signingKeyEnv, base64.StdEncoding.EncodeToString(testSigningKey.Seed()))
	dir := t.TempDir()
	versionless := filepath.Join(dir, "checksums.txt")
	if err := os.WriteFile(versionless, []byte(strings.Repeat("a", 64)+"  drive9-linux-amd64\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{versionless}); err == nil || !strings.Contains(err.Error(), "refusing to sign") {
		t.Fatalf("run on a versionless manifest error = %v, want a refusal", err)
	}
	if _, err := os.Stat(versionless + ".sig"); !os.IsNotExist(err) {
		t.Fatalf("a rejected manifest was signed anyway: %v", err)
	}
}
