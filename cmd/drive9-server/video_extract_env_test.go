package main

import (
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

func TestBuildVideoExtractOptionsFromEnv_MaxFilesDefault(t *testing.T) {
	t.Setenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST", "tenant-a")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_BASE", "http://example.com")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_KEY", "sk-test")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_MODEL", "test-model")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_MAX_FILES", "")

	opts, err := buildVideoExtractOptionsFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.MaxVideoLLMFiles != 50 {
		t.Fatalf("MaxVideoLLMFiles=%d, want 50 (default)", opts.MaxVideoLLMFiles)
	}
}

func TestBuildVideoExtractOptionsFromEnv_MaxFilesCustom(t *testing.T) {
	t.Setenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST", "tenant-a")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_BASE", "http://example.com")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_API_KEY", "sk-test")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_MODEL", "test-model")
	t.Setenv("DRIVE9_VIDEO_EXTRACT_MAX_FILES", "25")

	opts, err := buildVideoExtractOptionsFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.MaxVideoLLMFiles != 25 {
		t.Fatalf("MaxVideoLLMFiles=%d, want 25", opts.MaxVideoLLMFiles)
	}
}
