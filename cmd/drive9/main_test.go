package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/buildinfo"
)

func TestVersionStringIncludesGitHash(t *testing.T) {
	origVersion := buildinfo.Version
	origGitHash := buildinfo.GitHash
	origGitBranch := buildinfo.GitBranch
	origBuildTime := buildinfo.BuildTime
	buildinfo.Version = "v1.2.3"
	buildinfo.GitHash = "abc123"
	buildinfo.GitBranch = "feature/cli"
	buildinfo.BuildTime = "2026-04-16T09:30:00Z"
	t.Cleanup(func() {
		buildinfo.Version = origVersion
		buildinfo.GitHash = origGitHash
		buildinfo.GitBranch = origGitBranch
		buildinfo.BuildTime = origBuildTime
	})

	got := versionString()
	if !strings.Contains(got, "component: drive9\n") {
		t.Fatalf("versionString() missing component line: %q", got)
	}
	if !strings.Contains(got, "version: v1.2.3\n") {
		t.Fatalf("versionString() missing version line: %q", got)
	}
	if !strings.Contains(got, "git_hash: abc123\n") {
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
