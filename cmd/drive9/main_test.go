package main

import (
	"strings"
	"testing"
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
