package buildinfo

import (
	"runtime"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStringIncludesAllFields(t *testing.T) {
	origVersion := Version
	origGitHash := GitHash
	origGitBranch := GitBranch
	origBuildTime := BuildTime
	t.Cleanup(func() {
		Version = origVersion
		GitHash = origGitHash
		GitBranch = origGitBranch
		BuildTime = origBuildTime
	})

	Version = "v1.2.3"
	GitHash = "abc123"
	GitBranch = "feature/build-info"
	BuildTime = "2026-04-16T08:15:00Z"

	got := String("drive9-server")
	checks := []string{
		"component: drive9-server\n",
		"version: v1.2.3\n",
		"git_hash: abc123\n",
		"git_branch: feature/build-info\n",
		"build_time: 2026-04-16T08:15:00Z\n",
		"go_version: " + runtime.Version() + "\n",
	}
	for _, want := range checks {
		if !strings.Contains(got, want) {
			t.Fatalf("String() missing %q in %q", want, got)
		}
	}
}

func TestFieldsIncludeAllValues(t *testing.T) {
	origVersion := Version
	origGitHash := GitHash
	origGitBranch := GitBranch
	origBuildTime := BuildTime
	t.Cleanup(func() {
		Version = origVersion
		GitHash = origGitHash
		GitBranch = origGitBranch
		BuildTime = origBuildTime
	})

	Version = "v9.9.9"
	GitHash = "deadbeef"
	GitBranch = "main"
	BuildTime = "2026-04-16T09:00:00Z"

	core, recorded := observer.New(zap.InfoLevel)
	zap.New(core).Info("build_info", Fields("drive9-server-local")...)

	entries := recorded.All()
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	ctx := entries[0].ContextMap()
	if ctx["component"] != "drive9-server-local" {
		t.Fatalf("component = %v, want drive9-server-local", ctx["component"])
	}
	if ctx["version"] != "v9.9.9" {
		t.Fatalf("version = %v, want v9.9.9", ctx["version"])
	}
	if ctx["git_hash"] != "deadbeef" {
		t.Fatalf("git_hash = %v, want deadbeef", ctx["git_hash"])
	}
	if ctx["git_branch"] != "main" {
		t.Fatalf("git_branch = %v, want main", ctx["git_branch"])
	}
	if ctx["build_time"] != "2026-04-16T09:00:00Z" {
		t.Fatalf("build_time = %v, want 2026-04-16T09:00:00Z", ctx["build_time"])
	}
	if ctx["go_version"] != runtime.Version() {
		t.Fatalf("go_version = %v, want %s", ctx["go_version"], runtime.Version())
	}
}
