package fuse

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMountExitErrorExitCode(t *testing.T) {
	err := ExitServeAbnormalErr("boom")
	if err.ExitCode() != ExitServeAbnormal {
		t.Fatalf("ExitCode=%d want %d", err.ExitCode(), ExitServeAbnormal)
	}
	if err.Error() == "" {
		t.Fatal("empty error string")
	}
	if !errors.Is(err, err) {
		t.Fatal("self-is failed")
	}
}

func TestClassifyServeEndSignal(t *testing.T) {
	reason, detail := classifyServeEnd(true, "/tmp/not-a-real-mount")
	if reason != ExitReasonSignal {
		t.Fatalf("reason=%s want signal (%s)", reason, detail)
	}
}

func TestClassifyServeEndMissingPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gone")
	reason, _ := classifyServeEnd(false, dir)
	if reason != ExitReasonExternalUnmount && reason != ExitReasonServeAbnormal {
		// Missing path is external unmount; some platforms may report differently.
		t.Fatalf("reason=%s", reason)
	}
}

func TestClassifyServeEndLocalDirNotActive(t *testing.T) {
	dir := t.TempDir()
	reason, detail := classifyServeEnd(false, dir)
	// Local dir is typically not an active FUSE mount → external_unmount.
	if reason != ExitReasonExternalUnmount {
		t.Fatalf("reason=%s detail=%s want external_unmount", reason, detail)
	}
}

func TestEnsureCleanMountpointMissing(t *testing.T) {
	cleaned, err := EnsureCleanMountpoint(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatal(err)
	}
	if cleaned {
		t.Fatal("expected no clean on missing path")
	}
}

func TestEnsureCleanMountpointPlainDir(t *testing.T) {
	dir := t.TempDir()
	cleaned, err := EnsureCleanMountpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cleaned {
		t.Fatal("plain dir should not be cleaned")
	}
	// Ensure dir still exists.
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}
