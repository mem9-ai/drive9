package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVersionStringIncludesGitHash(t *testing.T) {
	origVersion := version
	origGitHash := gitHash
	version = "v1.2.3"
	gitHash = "abc123"
	t.Cleanup(func() {
		version = origVersion
		gitHash = origGitHash
	})

	got := versionString()
	if !strings.Contains(got, "drive9 v1.2.3\n") {
		t.Fatalf("versionString() missing version line: %q", got)
	}
	if !strings.Contains(got, "Git Commit Hash: abc123\n") {
		t.Fatalf("versionString() missing git hash line: %q", got)
	}
}

func TestStartCPUProfileFromEnv(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "drive9.cpu.pprof")
	t.Setenv("DRIVE9_PROF_CPU_PROFILE", profilePath)

	stopCPUProfile, err := startCPUProfileFromEnv()
	if err != nil {
		t.Fatalf("startCPUProfileFromEnv: %v", err)
	}

	deadline := time.Now().Add(20 * time.Millisecond)
	for time.Now().Before(deadline) {
	}

	stopCPUProfile()

	info, err := os.Stat(profilePath)
	if err != nil {
		t.Fatalf("Stat(profile): %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("profile file is empty: %s", profilePath)
	}
}

func TestExitWithCodeStopsCPUProfile(t *testing.T) {
	origStop := cpuProfileStop
	origExit := exitFunc
	t.Cleanup(func() {
		cpuProfileStop = origStop
		exitFunc = origExit
	})

	stopped := false
	exitCode := -1
	cpuProfileStop = func() { stopped = true }
	exitFunc = func(code int) { exitCode = code }

	exitWithCode(7)

	if !stopped {
		t.Fatal("expected exitWithCode to stop CPU profiling")
	}
	if exitCode != 7 {
		t.Fatalf("exit code = %d, want 7", exitCode)
	}
}
