package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

func TestUpdateCheckReportsAvailable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	srv := httptest.NewServer(updateTestReleaseHandler(t, []byte("new-drive9"), strings.Repeat("a", sha256.Size*2)))
	defer srv.Close()

	var stdout bytes.Buffer
	err := updateWithDeps([]string{"--check"}, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		httpClient:           srv.Client(),
		stdout:               &stdout,
		allowInsecureBaseURL: true,
	})
	if err != nil {
		t.Fatalf("update --check: %v", err)
	}
	if !strings.Contains(stdout.String(), "drive9 update available: v0.1.0 -> v0.2.0") {
		t.Errorf("stdout = %q, want update-available line", stdout.String())
	}
}

func TestUpdateHelpFlagAfterOptionPrintsUsage(t *testing.T) {
	var stdout bytes.Buffer
	err := updateWithDeps([]string{"--check", "-h"}, updateDeps{
		stdout: &stdout,
	})
	if err != nil {
		t.Fatalf("update --check -h: %v", err)
	}
	if !strings.Contains(stdout.String(), "usage: drive9 update") {
		t.Errorf("stdout = %q, want update usage", stdout.String())
	}
}

func TestUpdateDownloadsVerifiesAndReplacesExecutable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	payload := []byte("#!/bin/sh\necho new-drive9\n")
	sum := sha256.Sum256(payload)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("old-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}

	srv := httptest.NewServer(updateTestReleaseHandler(t, payload, fmt.Sprintf("%x", sum[:])))
	defer srv.Close()

	var stdout bytes.Buffer
	err := updateWithDeps(nil, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &stdout,
		allowInsecureBaseURL: true,
		replaceExecutable: func(newPath, targetPath string) error {
			if err := os.Remove(targetPath); err != nil {
				return err
			}
			return os.Rename(newPath, targetPath)
		},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("updated binary = %q, want %q", got, payload)
	}
	if !strings.Contains(stdout.String(), "drive9 updated: v0.1.0 -> v0.2.0") {
		t.Errorf("stdout = %q, want update success line", stdout.String())
	}
}

func TestUpdateRejectsChecksumMismatch(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("old-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}

	srv := httptest.NewServer(updateTestReleaseHandler(t, []byte("new-drive9"), strings.Repeat("0", 64)))
	defer srv.Close()

	err := updateWithDeps(nil, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run after checksum mismatch")
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("update error = %v, want checksum mismatch", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != "old-drive9" {
		t.Errorf("target changed after checksum mismatch: %q", got)
	}
}

func TestUpdateDoesNotInstallDowngradeWithoutForce(t *testing.T) {
	payload := []byte("older-drive9")
	sum := sha256.Sum256(payload)
	var requests []string
	release := updateTestReleaseHandlerWithVersion(t, "v0.8.0", payload, fmt.Sprintf("%x", sum[:]))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		release(w, r)
	}))
	defer srv.Close()

	var stdout bytes.Buffer
	err := updateWithDeps(nil, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.9.0",
		goos:                 "linux",
		goarch:               "amd64",
		httpClient:           srv.Client(),
		stdout:               &stdout,
		allowInsecureBaseURL: true,
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run for downgrade without --force")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("update downgrade: %v", err)
	}
	if !strings.Contains(stdout.String(), "v0.8.0 is older than current v0.9.0; use --force") {
		t.Errorf("stdout = %q, want downgrade message", stdout.String())
	}
	wantRequests := "/releases/checksums.txt,/releases/checksums.txt.sig,/releases/release.txt,/releases/release.txt.sig"
	if got := strings.Join(requests, ","); got != wantRequests {
		t.Errorf("requests = %q, want only the two signed pairs %q", got, wantRequests)
	}
}

func TestUpdateForceInstallsDowngrade(t *testing.T) {
	payload := []byte("older-drive9")
	sum := sha256.Sum256(payload)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("current-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}
	srv := httptest.NewServer(updateTestReleaseHandlerWithVersion(t, "v0.8.0", payload, fmt.Sprintf("%x", sum[:])))
	defer srv.Close()

	err := updateWithDeps([]string{"--force"}, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.9.0",
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
		t.Fatalf("update --force downgrade: %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("updated binary = %q, want %q", got, payload)
	}
}

func TestUpdatePreflightReplaceRunsBeforeDownload(t *testing.T) {
	errUnsupported := errors.New("unsupported platform")
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		updateTestReleaseHandler(t, []byte("new-drive9"), strings.Repeat("1", 64))(w, r)
	}))
	defer srv.Close()

	err := updateWithDeps(nil, updateDeps{
		baseURL:              srv.URL,
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		allowInsecureBaseURL: true,
		preflightReplace:     func() error { return errUnsupported },
		replaceExecutable: func(string, string) error {
			t.Fatal("replaceExecutable must not run after preflight failure")
			return nil
		},
	})
	if !errors.Is(err, errUnsupported) {
		t.Fatalf("update error = %v, want %v", err, errUnsupported)
	}
	wantRequests := "/releases/checksums.txt,/releases/checksums.txt.sig,/releases/release.txt,/releases/release.txt.sig"
	if got := strings.Join(requests, ","); got != wantRequests {
		t.Errorf("requests = %q, want only the two signed pairs %q", got, wantRequests)
	}
}

func TestUpdateRejectsInsecureBaseURLByDefault(t *testing.T) {
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		http.NotFound(w, r)
	}))
	defer srv.Close()

	err := updateWithDeps([]string{"--check"}, updateDeps{
		baseURL:        srv.URL,
		currentVersion: "v0.1.0",
		goos:           "linux",
		goarch:         "amd64",
		httpClient:     srv.Client(),
		stdout:         &bytes.Buffer{},
	})
	if err == nil || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("update error = %v, want HTTPS requirement", err)
	}
	if len(requests) != 0 {
		t.Fatalf("requests = %v, want none before HTTPS validation", requests)
	}
}

func TestCompareReleaseVersions(t *testing.T) {
	tests := []struct {
		name    string
		current string
		latest  string
		want    releaseRelation
	}{
		{name: "newer", current: "v0.1.0", latest: "v0.2.0", want: releaseNewer},
		{name: "same ignores build", current: "v0.2.0+local", latest: "0.2.0", want: releaseSame},
		{name: "older", current: "v0.9.0", latest: "v0.8.0", want: releaseOlder},
		{name: "release newer than prerelease", current: "v1.0.0-rc.1", latest: "v1.0.0", want: releaseNewer},
		{name: "semver latest updates non-semver current", current: "dev", latest: "v0.2.0", want: releaseNewer},
		{name: "unknown latest", current: "v0.2.0", latest: "new123", want: releaseUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareReleaseVersions(tt.current, tt.latest); got != tt.want {
				t.Fatalf("compareReleaseVersions(%q, %q) = %v, want %v", tt.current, tt.latest, got, tt.want)
			}
		})
	}
}

func TestUpdateRejectsEnvBaseURLWithoutRequests(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.NotFound(w, r)
	}))
	defer srv.Close()

	// The test server's 127.0.0.1 is not a pinned release host.
	t.Setenv("DRIVE9_UPDATE_BASE_URL", srv.URL)

	var stderr bytes.Buffer
	err := updateWithDeps([]string{"--check"}, updateDeps{
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		stderr:               &stderr,
		allowInsecureBaseURL: true,
	})
	if err == nil || !strings.Contains(err.Error(), "not a pinned drive9 release host") {
		t.Fatalf("update error = %v, want pinned-host rejection", err)
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want none before the pinned-host check", requests)
	}
}

func TestIsPinnedUpdateHost(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		want    bool
	}{
		{name: "default host", baseURL: "https://drive9.ai", want: true},
		{name: "www subdomain", baseURL: "https://www.drive9.ai", want: true},
		{name: "release subdomain", baseURL: "https://releases.drive9.ai", want: true},
		{name: "suffix lookalike", baseURL: "https://drive9.ai.attacker.example", want: false},
		{name: "hyphen lookalike", baseURL: "https://evil-drive9.ai", want: false},
		{name: "path lookalike", baseURL: "https://attacker.example/drive9.ai", want: false},
		{name: "userinfo lookalike", baseURL: "https://drive9.ai@attacker.example", want: false},
		{name: "fragment lookalike", baseURL: "https://attacker.example#.drive9.ai", want: false},
		{name: "uppercase pinned host", baseURL: "https://DRIVE9.AI", want: true},
		{name: "pinned host with port", baseURL: "https://drive9.ai:8443", want: true},
		{name: "empty", baseURL: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPinnedUpdateHost(tt.baseURL); got != tt.want {
				t.Fatalf("isPinnedUpdateHost(%q) = %v, want %v", tt.baseURL, got, tt.want)
			}
		})
	}
}

func TestUpdateExplicitBaseURLAllowsCustomHost(t *testing.T) {
	payload := []byte("new-drive9")
	sum := sha256.Sum256(payload)
	target := filepath.Join(t.TempDir(), "drive9")
	if err := os.WriteFile(target, []byte("old-drive9"), 0o755); err != nil {
		t.Fatalf("write target: %v", err)
	}

	srv := httptest.NewServer(updateTestReleaseHandler(t, payload, fmt.Sprintf("%x", sum[:])))
	defer srv.Close()

	var stderr bytes.Buffer
	err := updateWithDeps([]string{"--base-url", srv.URL}, updateDeps{
		currentVersion:       "v0.1.0",
		goos:                 "linux",
		goarch:               "amd64",
		executable:           func() (string, error) { return target, nil },
		httpClient:           srv.Client(),
		stdout:               &bytes.Buffer{},
		stderr:               &stderr,
		allowInsecureBaseURL: true,
		replaceExecutable: func(newPath, targetPath string) error {
			if err := os.Remove(targetPath); err != nil {
				return err
			}
			return os.Rename(newPath, targetPath)
		},
	})
	if err != nil {
		t.Fatalf("update with explicit --base-url: %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("updated binary = %q, want %q", got, payload)
	}
	if !strings.Contains(stderr.String(), "custom release root") {
		t.Errorf("stderr = %q, want custom release root warning", stderr.String())
	}
}

type updateRedirectTransport struct {
	redirectPath string
	target       string
	bodies       map[string]string
	hops         int
}

func (tr *updateRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.hops++
	if tr.target != "" && req.URL.Path == tr.redirectPath {
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{tr.target}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(tr.bodies[req.URL.Path])),
		Request:    req,
	}, nil
}

func TestUpdatePinsRedirectsForPinnedReleaseRoots(t *testing.T) {
	files := newUpdateTestReleaseFiles(t, "v0.2.0", "drive9-linux-amd64", strings.Repeat("a", sha256.Size*2), nil)
	bodies := map[string]string{
		"/latest/" + releasesign.ManifestName:               string(files.manifest),
		"/releases/" + releasesign.SignatureName:            string(files.manifestSig),
		"/releases/" + releasesign.ReleaseInfoName:          string(files.info),
		"/releases/" + releasesign.ReleaseInfoSignatureName: string(files.infoSig),
	}
	tests := []struct {
		name     string
		location string
		wantErr  string
	}{
		{name: "pinned subdomain", location: "https://releases.drive9.ai/latest/checksums.txt"},
		{name: "unpinned host", location: "https://releases.attacker.example/checksums.txt", wantErr: "not a pinned drive9 release host"},
		{name: "https downgrade", location: "http://drive9.ai/checksums.txt", wantErr: "require https"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &updateRedirectTransport{
				redirectPath: "/releases/checksums.txt",
				target:       tt.location,
				bodies:       bodies,
			}
			var stdout bytes.Buffer
			err := updateWithDeps([]string{"--check"}, updateDeps{
				baseURL:        "https://drive9.ai",
				currentVersion: "v0.1.0",
				goos:           "linux",
				goarch:         "amd64",
				httpClient:     &http.Client{Transport: transport},
				stdout:         &stdout,
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("update error = %v, want %q", err, tt.wantErr)
				}
				if transport.hops != 1 {
					t.Fatalf("hops = %d, want the redirect to be refused before it is followed", transport.hops)
				}
				return
			}
			if err != nil {
				t.Fatalf("update --check through a pinned redirect: %v", err)
			}
			// The redirected manifest plus the signature, release info and its signature:
			// every request in the chain is pinned.
			if transport.hops != 5 {
				t.Fatalf("hops = %d, want the redirected manifest and the three remaining fetches", transport.hops)
			}
			if !strings.Contains(stdout.String(), "update available: v0.1.0 -> v0.2.0") {
				t.Errorf("stdout = %q, want update-available line", stdout.String())
			}
		})
	}
}

func TestParseChecksumsRejectsPathQualifiedArtifactNames(t *testing.T) {
	sum := strings.Repeat("a", sha256.Size*2)
	_, err := releasesign.ParseChecksums([]byte(fmt.Sprintf("%s  releases/drive9-linux-amd64\n", sum)))
	if err == nil || !strings.Contains(err.Error(), "must be a release artifact filename") {
		t.Fatalf("ParseChecksums error = %v, want artifact filename error", err)
	}
}

func TestParseChecksumsRejectsDuplicateArtifactNames(t *testing.T) {
	first := strings.Repeat("a", sha256.Size*2)
	second := strings.Repeat("b", sha256.Size*2)
	_, err := releasesign.ParseChecksums([]byte(fmt.Sprintf("%s  drive9-linux-amd64\n%s  drive9-linux-amd64\n", first, second)))
	if err == nil || !strings.Contains(err.Error(), "duplicate checksum") {
		t.Fatalf("ParseChecksums error = %v, want duplicate checksum error", err)
	}
}

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

func TestParseChecksumsIsThePreSigningFormat(t *testing.T) {
	sum := strings.Repeat("a", sha256.Size*2)
	checksums, err := releasesign.ParseChecksums([]byte(fmt.Sprintf("%s  drive9-linux-amd64\n", sum)))
	if err != nil {
		t.Fatalf("ParseChecksums: %v", err)
	}
	if checksums["drive9-linux-amd64"] != sum {
		t.Fatalf("checksums = %v, want the artifact digest", checksums)
	}

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		// Pre-change CLIs reject any line that is not "<sha256>  <artifact>", which would
		// make them lose self-update entirely, so the manifest must never carry one.
		{name: "comment line", body: "# drive9-release-version v1.0.0\n" + sum + "  drive9-linux-amd64\n", want: "invalid checksum"},
		{name: "empty manifest", body: "\n", want: "empty checksum list"},
		{name: "duplicate artifact", body: sum + "  a\n" + strings.Repeat("b", 64) + "  a\n", want: "duplicate checksum"},
		{name: "non-hex digest", body: strings.Repeat("g", 64) + "  drive9-linux-amd64\n", want: "not a sha256 hex digest"},
		{name: "short digest", body: "abc  drive9-linux-amd64\n", want: "not a sha256 hex digest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := releasesign.ParseChecksums([]byte(tc.body)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseChecksums error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseReleaseInfo(t *testing.T) {
	manifest := []byte(strings.Repeat("a", 64) + "  drive9-linux-amd64\n")
	valid := []byte("version v0.2.0\nchecksums-sha256 " + releasesign.ManifestDigest(manifest) + "\n")
	info, err := releasesign.ParseReleaseInfo(valid)
	if err != nil {
		t.Fatalf("ParseReleaseInfo: %v", err)
	}
	if info.Version != "v0.2.0" {
		t.Fatalf("version = %q, want v0.2.0", info.Version)
	}
	if err := info.ValidateManifest(manifest); err != nil {
		t.Fatalf("ValidateManifest: %v", err)
	}
	// The description binds one exact manifest: a different release's manifest is refused.
	other := []byte(strings.Repeat("b", 64) + "  drive9-linux-amd64\n")
	if err := info.ValidateManifest(other); err == nil || !strings.Contains(err.Error(), "but checksums.txt is") {
		t.Fatalf("ValidateManifest(other) error = %v, want a digest mismatch", err)
	}

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing version", body: "checksums-sha256 " + releasesign.ManifestDigest(manifest) + "\n", want: "no version"},
		{name: "empty version", body: "version \nchecksums-sha256 " + releasesign.ManifestDigest(manifest) + "\n", want: "empty version"},
		{name: "duplicate version", body: "version v1\nversion v2\nchecksums-sha256 " + releasesign.ManifestDigest(manifest) + "\n", want: "more than one version"},
		{name: "missing digest", body: "version v1\n", want: "no checksums-sha256"},
		{name: "bad digest", body: "version v1\nchecksums-sha256 nothex\n", want: "not a sha256 hex digest"},
		{name: "unknown field", body: "version v1\nchecksums-sha256 " + releasesign.ManifestDigest(manifest) + "\nextra x\n", want: "unknown release info field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := releasesign.ParseReleaseInfo([]byte(tc.body)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseReleaseInfo error = %v, want %q", err, tc.want)
			}
		})
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
		t.Fatalf("update error = %v, want a cross-release digestion mismatch", err)
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
	if got := trustedUpdateManifestKeys(); got == "" {
		t.Fatalf("no override should resolve to the embedded file, got %q", got)
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
