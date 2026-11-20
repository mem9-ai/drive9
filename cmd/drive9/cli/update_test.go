package cli

import (
	"bytes"
	"crypto/sha256"
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
