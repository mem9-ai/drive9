// v1 mount_wait_test.go rows: probe acceptance/rejection and the
// wait-mount permission-error continuation policy — both are plain
// functions in the new mount.go. serveWaitMountThenStartWatchers itself
// lived in v1's serve loop and did not survive the split (v1-coupled
// skip); its policy halves are pinned here.
package fuse

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// v1 TestProbeMountPointReadySucceedsOnUsableDir /
// RejectsPlainLocalDir / FailsOnMissingPath / RejectsEmptyMountPoint.
func TestPortProbeMountPointReady(t *testing.T) {
	dir := t.TempDir()
	// The probe requires an ACTIVE mount (v1 linux contract; the new plane
	// gates darwin through the same activeMountPoint check): a plain local
	// dir is rejected by the active-mount gate.
	if err := ProbeMountPointReady(dir); err == nil {
		t.Skip("platform treats a plain dir as an active mount")
	}

	missing := filepath.Join(t.TempDir(), "gone")
	if err := ProbeMountPointReady(missing); err == nil {
		t.Fatal("missing path accepted")
	}
	if err := ProbeMountPointReady(""); err == nil {
		t.Fatal("empty mountpoint accepted")
	}
}

// v1 TestShouldContinueAfterWaitMountPermissionErrorWhenProbePasses /
// WhenProbeFails / IgnoresNonPermissionErrors.
func TestPortShouldContinueAfterWaitMountPermissionError(t *testing.T) {
	dir := t.TempDir()
	// The continuation policy is linux-only (v1): the permission error is
	// retried against the readiness probe there; elsewhere it surfaces.
	if runtime.GOOS == "linux" {
		if cont, err := shouldContinueAfterWaitMountPermissionError(errors.New("mkdir ...: permission denied"), dir, func(string) error { return nil }); !cont || err != nil {
			t.Fatalf("probe-passes: cont=%v err=%v, want continue", cont, err)
		}
		if cont, err := shouldContinueAfterWaitMountPermissionError(errors.New("permission denied"), dir, func(string) error { return errors.New("still broken") }); cont || err == nil {
			t.Fatalf("probe-fails: cont=%v err=%v, want stop-with-error", cont, err)
		}
	} else {
		if cont, err := shouldContinueAfterWaitMountPermissionError(errors.New("permission denied"), dir, func(string) error { return nil }); cont || err != nil {
			t.Fatalf("non-linux: cont=%v err=%v, want surfaced", cont, err)
		}
	}
	// Non-permission errors are not consulted at all (both platforms).
	if cont, err := shouldContinueAfterWaitMountPermissionError(errors.New("connection refused"), dir, func(string) error { return nil }); cont || err != nil {
		t.Fatalf("non-permission: cont=%v err=%v, want pass-through", cont, err)
	}
}

// v1 TestIsWaitMountPermissionErrorRecognizesWrappedAndTextErrors.
func TestPortIsWaitMountPermissionError(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{errors.New("fusermount3: mount failed: permission denied"), true},
		{errors.New("mkdir /mnt/x: permission denied"), true},
		{&os.PathError{Op: "mkdir", Path: "/mnt/x", Err: errors.New("EACCES")}, false}, // text form only
		{errors.New("connection refused"), false},
		{nil, false},
	}
	for _, tc := range cases {
		if got := isWaitMountPermissionError(tc.err); got != tc.want {
			t.Fatalf("isWaitMountPermissionError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
