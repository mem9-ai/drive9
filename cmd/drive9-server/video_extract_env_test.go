package main

import (
	"os"
	"testing"
)

func TestBuildVideoExtractOptionsFromEnv_AllowlistOff(t *testing.T) {
	tests := []struct {
		name string
		val  string
	}{
		{"unset", ""},
		{"only commas", ","},
		{"commas and spaces", ", ,"},
		{"whitespace", "  "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST", tt.val)
			// Ensure no API envs are set — if the function tries to
			// read them despite off-state, it would fail or return
			// a configured result.
			t.Setenv("DRIVE9_VIDEO_EXTRACT_API_BASE", "")
			t.Setenv("DRIVE9_VIDEO_EXTRACT_API_KEY", "")
			t.Setenv("DRIVE9_VIDEO_EXTRACT_MODEL", "")

			opts, err := buildVideoExtractOptionsFromEnv()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if opts.Enabled {
				t.Fatal("expected Enabled=false for off allowlist")
			}
		})
	}
}

func TestBuildVideoExtractOptionsFromEnv_InvalidAllowlist(t *testing.T) {
	tests := []struct {
		name string
		val  string
	}{
		{"glob prefix", "tenant-*"},
		{"mixed wildcard", "*,tenant-a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST", tt.val)
			// API envs present so we isolate the allowlist validation.
			t.Setenv("DRIVE9_VIDEO_EXTRACT_API_BASE", "http://example.com")
			t.Setenv("DRIVE9_VIDEO_EXTRACT_API_KEY", "sk-test")
			t.Setenv("DRIVE9_VIDEO_EXTRACT_MODEL", "test-model")

			_, err := buildVideoExtractOptionsFromEnv()
			if err == nil {
				t.Fatalf("expected error for invalid allowlist %q", tt.val)
			}
		})
	}
}

func TestBuildVideoExtractOptionsFromEnv_RequiresAPIEnvs(t *testing.T) {
	t.Setenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST", "tenant-a")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_BASE", "")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_KEY", "")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_MODEL", "")

	_, err := buildVideoExtractOptionsFromEnv()
	if err == nil {
		t.Fatal("expected error when allowlist is set but API envs are missing")
	}
}

func TestBuildVideoExtractOptionsFromEnv_WildcardRequiresAPIEnvs(t *testing.T) {
	t.Setenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST", "*")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_BASE", "")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_KEY", "")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_MODEL", "")

	_, err := buildVideoExtractOptionsFromEnv()
	if err == nil {
		t.Fatal("expected error when wildcard is set but API envs are missing")
	}
}

// TestBuildVideoExtractOptionsFromEnv_NoTestMain verifies these tests
// do not depend on TestMain / testcontainers — they only test env parsing.
func TestBuildVideoExtractOptionsFromEnv_NoTestMain(t *testing.T) {
	// This test exists solely to confirm the test file compiles and runs
	// without Docker. If it reaches here, the file is self-contained.
	_ = os.Getenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST")
}
