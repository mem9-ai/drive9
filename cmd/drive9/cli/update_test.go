package cli

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpdateCheckReportsAvailable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/releases/version" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprintln(w, "v0.2.0")
	}))
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		updateTestReleaseHandlerWithVersion(t, "v0.8.0", payload, fmt.Sprintf("%x", sum[:]))(w, r)
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
	if got, want := strings.Join(requests, ","), "/releases/version"; got != want {
		t.Errorf("requests = %q, want only %q", got, want)
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
	if got, want := strings.Join(requests, ","), "/releases/version"; got != want {
		t.Errorf("requests = %q, want only %q", got, want)
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

func TestParseChecksumsRejectsPathQualifiedArtifactNames(t *testing.T) {
	sum := strings.Repeat("a", sha256.Size*2)
	_, err := parseChecksums([]byte(fmt.Sprintf("%s  releases/drive9-linux-amd64\n", sum)))
	if err == nil || !strings.Contains(err.Error(), "must be a release artifact filename") {
		t.Fatalf("parseChecksums error = %v, want artifact filename error", err)
	}
}

func TestParseChecksumsRejectsDuplicateArtifactNames(t *testing.T) {
	first := strings.Repeat("a", sha256.Size*2)
	second := strings.Repeat("b", sha256.Size*2)
	_, err := parseChecksums([]byte(fmt.Sprintf("%s  drive9-linux-amd64\n%s  drive9-linux-amd64\n", first, second)))
	if err == nil || !strings.Contains(err.Error(), "duplicate checksum") {
		t.Fatalf("parseChecksums error = %v, want duplicate checksum error", err)
	}
}

func updateTestReleaseHandler(t *testing.T, payload []byte, checksum string) http.HandlerFunc {
	return updateTestReleaseHandlerWithVersion(t, "v0.2.0", payload, checksum)
}

func updateTestReleaseHandlerWithVersion(t *testing.T, version string, payload []byte, checksum string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/releases/version":
			_, _ = fmt.Fprintln(w, version)
		case "/releases/checksums.txt":
			_, _ = fmt.Fprintf(w, "%s  drive9-linux-amd64\n", checksum)
		case "/releases/drive9-linux-amd64":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}
}
