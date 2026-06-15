package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUpdateCheckReportsAvailable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/releases/version" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprintln(w, "new123")
	}))
	defer srv.Close()

	var stdout bytes.Buffer
	err := updateWithDeps([]string{"--check"}, updateDeps{
		baseURL:        srv.URL,
		currentVersion: "old123",
		goos:           "linux",
		goarch:         "amd64",
		httpClient:     srv.Client(),
		stdout:         &stdout,
		now:            fixedUpdateNow,
	})
	if err != nil {
		t.Fatalf("update --check: %v", err)
	}
	if !strings.Contains(stdout.String(), "drive9 update available: old123 -> new123") {
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
		baseURL:        srv.URL,
		currentVersion: "old123",
		goos:           "linux",
		goarch:         "amd64",
		executable:     func() (string, error) { return target, nil },
		httpClient:     srv.Client(),
		stdout:         &stdout,
		now:            fixedUpdateNow,
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
	if !strings.Contains(stdout.String(), "drive9 updated: old123 -> new123") {
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
		baseURL:        srv.URL,
		currentVersion: "old123",
		goos:           "linux",
		goarch:         "amd64",
		executable:     func() (string, error) { return target, nil },
		httpClient:     srv.Client(),
		stdout:         &bytes.Buffer{},
		now:            fixedUpdateNow,
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

func TestMaybeNotifyUpdateUsesCachedResultOnNextRun(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var versionHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/releases/version" {
			http.NotFound(w, r)
			return
		}
		versionHits++
		_, _ = fmt.Fprintln(w, "new123")
	}))
	defer srv.Close()

	now := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	var stderr bytes.Buffer
	deps := updateDeps{
		baseURL:        srv.URL,
		currentVersion: "old123",
		goos:           "linux",
		goarch:         "amd64",
		httpClient:     srv.Client(),
		stderr:         &stderr,
		now:            func() time.Time { return now },
	}

	maybeNotifyUpdateWithDeps(deps)
	if stderr.String() != "" {
		t.Errorf("first run stderr = %q, want no same-run update notice", stderr.String())
	}
	if versionHits != 1 {
		t.Errorf("version hits after first run = %d, want 1", versionHits)
	}

	stderr.Reset()
	maybeNotifyUpdateWithDeps(deps)
	if !strings.Contains(stderr.String(), "drive9 update available: old123 -> new123") {
		t.Errorf("second run stderr = %q, want cached update notice", stderr.String())
	}
	if versionHits != 1 {
		t.Errorf("version hits after second run = %d, want stale check to be skipped", versionHits)
	}
}

func TestMaybeNotifyUpdateSkipsDevVersionNotice(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := writeUpdateCache(context.Background(), updateCache{
		LastCheckedAt: fixedUpdateNow().UTC(),
		LatestVersion: "new123",
		LatestURL:     "https://drive9.ai/releases/drive9-linux-amd64",
	}); err != nil {
		t.Fatalf("write cache: %v", err)
	}

	var stderr bytes.Buffer
	maybeNotifyUpdateWithDeps(updateDeps{
		currentVersion: "dev",
		stderr:         &stderr,
		now:            fixedUpdateNow,
	})
	if stderr.String() != "" {
		t.Errorf("stderr = %q, want no notice for dev builds", stderr.String())
	}
}

func updateTestReleaseHandler(t *testing.T, payload []byte, checksum string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/releases/version":
			_, _ = fmt.Fprintln(w, "new123")
		case "/releases/checksums.txt":
			_, _ = fmt.Fprintf(w, "%s  drive9-linux-amd64\n", checksum)
		case "/releases/drive9-linux-amd64":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}
}

func fixedUpdateNow() time.Time {
	return time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
}
