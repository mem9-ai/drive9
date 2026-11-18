package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

// updateTestSigningPrivateKey is the throwaway key update tests sign manifests with: the
// real release key's private half is a repository secret, and every test must exercise the
// same verification path production does.
var updateTestSigningPrivateKey = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x37}, ed25519.SeedSize))

var updateTestSigningPublicKey = base64.StdEncoding.EncodeToString(updateTestSigningPrivateKey.Public().(ed25519.PublicKey))

// TestMain installs the throwaway signing key for every test in this package, so the code
// under test is the production one (embedded var -> deps -> verify) and no test can pin
// itself to the real release key.
func TestMain(m *testing.M) {
	updateManifestPublicKey = updateTestSigningPublicKey
	os.Exit(m.Run())
}

// useUpdateTestSigningKey points the build's release key at the given key for one test.
func useUpdateTestSigningKey(t *testing.T, publicKey string) {
	t.Helper()
	previous := updateManifestPublicKey
	updateManifestPublicKey = publicKey
	t.Cleanup(func() { updateManifestPublicKey = previous })
}

// updateTestReleaseFiles is one signed test release: the plain checksum manifest (the
// format every CLI already parses), the release description binding the version to that
// manifest, and both detached signatures.
type updateTestReleaseFiles struct {
	version     string
	artifact    string
	payload     []byte
	manifest    []byte
	info        []byte
	manifestSig []byte
	infoSig     []byte
}

func newUpdateTestReleaseFiles(t *testing.T, version, artifact, checksum string, payload []byte) *updateTestReleaseFiles {
	t.Helper()
	return newUpdateTestReleaseFilesSignedBy(t, version, artifact, checksum, payload, updateTestSigningPrivateKey)
}

func newUpdateTestReleaseFilesSignedBy(t *testing.T, version, artifact, checksum string, payload []byte, key ed25519.PrivateKey) *updateTestReleaseFiles {
	t.Helper()
	manifest := []byte(fmt.Sprintf("%s  %s\n", checksum, artifact))
	info := []byte(fmt.Sprintf("version %s\nchecksums-sha256 %s\n", version, releasesign.ManifestDigest(manifest)))
	manifestSig, err := releasesign.Sign(manifest, key)
	if err != nil {
		t.Fatalf("sign manifest: %v", err)
	}
	infoSig, err := releasesign.Sign(info, key)
	if err != nil {
		t.Fatalf("sign release info: %v", err)
	}
	return &updateTestReleaseFiles{
		version:     version,
		artifact:    artifact,
		payload:     payload,
		manifest:    manifest,
		info:        info,
		manifestSig: manifestSig,
		infoSig:     infoSig,
	}
}

func (rel *updateTestReleaseFiles) handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/releases/version":
		// Kept for older CLIs that still read it; this updater takes the signed version.
		_, _ = fmt.Fprintln(w, rel.version)
	case "/releases/" + releasesign.ManifestName:
		_, _ = w.Write(rel.manifest)
	case "/releases/" + releasesign.SignatureName:
		_, _ = w.Write(rel.manifestSig)
	case "/releases/" + releasesign.ReleaseInfoName:
		_, _ = w.Write(rel.info)
	case "/releases/" + releasesign.ReleaseInfoSignatureName:
		_, _ = w.Write(rel.infoSig)
	case "/releases/" + rel.artifact:
		_, _ = w.Write(rel.payload)
	default:
		http.NotFound(w, r)
	}
}

func updateTestReleaseHandler(t *testing.T, payload []byte, checksum string) http.HandlerFunc {
	return updateTestReleaseHandlerWithVersion(t, "v0.2.0", payload, checksum)
}

func updateTestReleaseHandlerWithVersion(t *testing.T, version string, payload []byte, checksum string) http.HandlerFunc {
	t.Helper()
	return newUpdateTestReleaseFiles(t, version, "drive9-linux-amd64", checksum, payload).handler
}

// The exploit this closes: an origin the user was talked into (or one an attacker controls)
// serves a payload and a matching checksum list. Without a signature the updater would
// install it, because the checksum only proves the two files agree with each other.
func TestUpdateRefusesUnsignedManifestFromCustomOrigin(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("attacker-controlled")
	sum := sha256.Sum256(payload)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("current-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/releases/" + releasesign.ManifestName:
			_, _ = fmt.Fprintf(w, "%x  drive9-linux-amd64\n", sum[:])
		case "/releases/" + releasesign.SignatureName, "/releases/" + releasesign.ReleaseInfoName, "/releases/" + releasesign.ReleaseInfoSignatureName:
			http.NotFound(w, r)
		case "/releases/drive9-linux-amd64":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	err := updateWithDeps([]string{"--base-url", srv.URL}, updateDeps{
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run for an unsigned manifest")
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("update error = %v, want a signature failure", err)
	}
	if got, readErr := os.ReadFile(target); readErr != nil || string(got) != "current-drive9" {
		t.Fatalf("target changed after an unsigned manifest: %q err=%v", got, readErr)
	}
}

// A signature that is well-formed but from another key is not a signature by drive9.
func TestUpdateRefusesManifestSignedByAnotherKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("attacker-controlled")
	sum := sha256.Sum256(payload)
	foreignKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x99}, ed25519.SeedSize))
	release := newUpdateTestReleaseFilesSignedBy(t, "v99.0.0", "drive9-linux-amd64", fmt.Sprintf("%x", sum[:]), payload, foreignKey)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("current-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(release.handler))
	defer srv.Close()

	err := updateWithDeps([]string{"--base-url", srv.URL}, updateDeps{
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run for a foreign signature")
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("update error = %v, want a verification failure", err)
	}
	if got, readErr := os.ReadFile(target); readErr != nil || string(got) != "current-drive9" {
		t.Fatalf("target changed after a foreign signature: %q err=%v", got, readErr)
	}
}

// The signature covers the exact manifest bytes: swapping the artifact hash underneath a
// valid signature must not install anything.
func TestUpdateRefusesTamperedManifest(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("attacker-controlled")
	sum := sha256.Sum256(payload)
	release := newUpdateTestReleaseFiles(t, "v0.2.0", "drive9-linux-amd64", strings.Repeat("a", sha256.Size*2), payload)
	tampered := fmt.Sprintf("%x  drive9-linux-amd64\n", sum[:])
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("current-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	handler := release.handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/releases/"+releasesign.ManifestName {
			_, _ = w.Write([]byte(tampered))
			return
		}
		handler(w, r)
	}))
	defer srv.Close()

	err := updateWithDeps([]string{"--base-url", srv.URL}, updateDeps{
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run for a tampered manifest")
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("update error = %v, want a verification failure", err)
	}
}

// A build with no release key cannot authenticate anything, so it must refuse to update
// rather than fall back to trusting the origin.
func TestUpdateRefusesWithoutBuildSigningKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		updateTestReleaseHandler(t, []byte("new-drive9"), strings.Repeat("a", sha256.Size*2))(w, r)
	}))
	defer srv.Close()
	useUpdateTestSigningKey(t, "")

	err := updateWithDeps([]string{"--check"}, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
	})
	if err == nil || !strings.Contains(err.Error(), "no release-signing public key configured") || !strings.Contains(err.Error(), "updateManifestPublicKey") {
		t.Fatalf("update error = %v, want the no-key refusal with the rebuild hint", err)
	}
	if requests == 0 {
		t.Fatal("expected the manifest to be fetched before the key check rejects it")
	}
}

// The signed manifest, not the sibling version file, decides the version: an origin that
// pairs an older signed binary with a newer version string cannot make it an upgrade.
func TestUpdateTakesVersionFromSignedManifest(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("older-signed-release")
	sum := sha256.Sum256(payload)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("current-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	release := newUpdateTestReleaseFiles(t, "v0.2.0", "drive9-linux-amd64", fmt.Sprintf("%x", sum[:]), payload)
	handler := release.handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/releases/version" {
			// The forged upgrade the origin would like the updater to believe.
			_, _ = fmt.Fprintln(w, "v99.0.0")
			return
		}
		handler(w, r)
	}))
	defer srv.Close()

	var stdout bytes.Buffer
	err := updateWithDeps([]string{"--base-url", srv.URL}, updateDeps{
		currentVersion:       "v0.9.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &stdout,
		allowInsecureBaseURL: true,
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run for a downgrade")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !strings.Contains(stdout.String(), "v0.2.0 is older than current v0.9.0") {
		t.Fatalf("stdout = %q, want the signed version to decide", stdout.String())
	}
}

// The build's embedded key file must hold at least one usable key: an empty or malformed
// pin would turn every update into a refusal, and a typo is easier to catch here than in a
// release job.
func TestUpdateEmbeddedKeyFileIsValid(t *testing.T) {
	keys, err := releasesign.ParsePublicKeys(updateManifestKeyFile)
	if err != nil {
		t.Fatalf("embedded key file: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("embedded key file names no keys")
	}
}

// A rotation overlaps: the build pins old,new while releases are still signed with the old
// key. A manifest signed by either key installs, which is what lets the secret switch
// without stranding clients.
func TestUpdateAcceptsManifestSignedByOverlapKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("new-drive9")
	sum := sha256.Sum256(payload)
	foreignKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x91}, ed25519.SeedSize))
	foreignPublic := base64.StdEncoding.EncodeToString(foreignKey.Public().(ed25519.PublicKey))
	// Pin the test key first, the rotating key second: the signature comes from the second.
	useUpdateTestSigningKey(t, updateTestSigningPublicKey+","+foreignPublic)

	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("old-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	release := newUpdateTestReleaseFilesSignedBy(t, "v0.2.0", "drive9-linux-amd64", fmt.Sprintf("%x", sum[:]), payload, foreignKey)
	srv := httptest.NewServer(http.HandlerFunc(release.handler))
	defer srv.Close()

	err := updateWithDeps([]string{"--base-url", srv.URL}, updateDeps{
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
		replaceExecutable: func(newPath, targetPath string) error {
			if err := os.Remove(targetPath); err != nil {
				return err
			}
			return os.Rename(newPath, targetPath)
		},
	})
	if err != nil {
		t.Fatalf("update signed by the overlap key: %v", err)
	}
	if got, readErr := os.ReadFile(target); readErr != nil || !bytes.Equal(got, payload) {
		t.Fatalf("target = %q err=%v, want the new binary", got, readErr)
	}
}

// An origin cannot combine one release's signed version with another release's signed
// manifest: the description names the exact manifest bytes it describes.
func TestUpdateRefusesCrossReleaseMix(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("older-signed-release")
	olderSum := sha256.Sum256(payload)
	newerPayload := []byte("newer-signed-release")
	newerSum := sha256.Sum256(newerPayload)

	older := newUpdateTestReleaseFiles(t, "v0.2.0", "drive9-linux-amd64", fmt.Sprintf("%x", olderSum[:]), payload)
	newer := newUpdateTestReleaseFiles(t, "v9.0.0", "drive9-linux-amd64", fmt.Sprintf("%x", newerSum[:]), newerPayload)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("current-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	// Genuine signature over the newer description, but the older manifest and binary are
	// what the origin serves: the digest check must refuse it.
	handler := older.handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/releases/" + releasesign.ReleaseInfoName:
			_, _ = w.Write(newer.info)
			return
		case "/releases/" + releasesign.ReleaseInfoSignatureName:
			_, _ = w.Write(newer.infoSig)
			return
		}
		handler(w, r)
	}))
	defer srv.Close()

	err := updateWithDeps([]string{"--base-url", srv.URL}, updateDeps{
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run for a mixed release")
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "release info describes manifest") {
		t.Fatalf("update error = %v, want a cross-release digest mismatch", err)
	}
	// A digest mismatch is not a key problem, so it must not carry the rebuild hint.
	if strings.Contains(err.Error(), "this build trusts") {
		t.Errorf("digest mismatch was reported as a signing-key failure: %v", err)
	}
	if got, readErr := os.ReadFile(target); readErr != nil || string(got) != "current-drive9" {
		t.Fatalf("target changed after a mixed release: %q err=%v", got, readErr)
	}
}

// A release without the signed description is refused: the version must be authenticated,
// not taken from an unauthenticated sibling file.
func TestUpdateRefusesMissingReleaseInfo(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("new-drive9")
	sum := sha256.Sum256(payload)
	release := newUpdateTestReleaseFiles(t, "v0.2.0", "drive9-linux-amd64", fmt.Sprintf("%x", sum[:]), payload)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("current-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	handler := release.handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/releases/"+releasesign.ReleaseInfoName || r.URL.Path == "/releases/"+releasesign.ReleaseInfoSignatureName {
			http.NotFound(w, r)
			return
		}
		handler(w, r)
	}))
	defer srv.Close()

	err := updateWithDeps([]string{"--check"}, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
	})
	if err == nil || !strings.Contains(err.Error(), "fetch release info") {
		t.Fatalf("update error = %v, want a missing release info failure", err)
	}
}

// The linker override must actually take effect: the trusted-key variable is initialized
// from a constant sentinel precisely so -X can replace it, and an explicit empty override
// means "trust nothing" rather than falling back to the embedded production keys.
func TestUpdateTrustedKeysResolveLinkerOverride(t *testing.T) {
	// -X only rewrites a string variable initialized with a constant expression, so the
	// initializer must be the sentinel literally (a var-to-var initializer would silently
	// ignore a linker override). TestMain replaces the value at run time, so pin the source.
	source, err := os.ReadFile("update_signing.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "var updateManifestPublicKey = embeddedUpdateManifestKeys") {
		t.Fatal("updateManifestPublicKey is not initialized from the constant sentinel, so -X cannot override it")
	}
	if _, err := releasesign.ParsePublicKeys(embeddedUpdateManifestKeys); err == nil {
		t.Fatal("the sentinel parses as a key set, so it cannot mark an unset override")
	}
	// TestMain has already replaced the variable with a test key, so restore the sentinel for
	// this assertion: otherwise the "no override" branch that resolves to the embedded file is
	// never exercised in this package.
	useUpdateTestSigningKey(t, embeddedUpdateManifestKeys)
	if got := trustedUpdateManifestKeys(); got != updateManifestKeyFile {
		t.Fatalf("no override should resolve to the embedded key file, got %q", got)
	}
	useUpdateTestSigningKey(t, updateTestSigningPublicKey+","+updateTestSigningPublicKey)
	if got, want := trustedUpdateManifestKeys(), updateTestSigningPublicKey+","+updateTestSigningPublicKey; got != want {
		t.Fatalf("override = %q, want %q", got, want)
	}
	useUpdateTestSigningKey(t, "")
	if got := trustedUpdateManifestKeys(); got != "" {
		t.Fatalf("an explicit empty override resolved to %q, want no keys", got)
	}
}
