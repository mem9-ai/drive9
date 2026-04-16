package main

import (
	"os"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/buildinfo"
)

func TestVersionTextIncludesBuildInfo(t *testing.T) {
	origVersion := buildinfo.Version
	origGitHash := buildinfo.GitHash
	origGitBranch := buildinfo.GitBranch
	origBuildTime := buildinfo.BuildTime
	t.Cleanup(func() {
		buildinfo.Version = origVersion
		buildinfo.GitHash = origGitHash
		buildinfo.GitBranch = origGitBranch
		buildinfo.BuildTime = origBuildTime
	})

	buildinfo.Version = "v2.0.0"
	buildinfo.GitHash = "feedface"
	buildinfo.GitBranch = "main"
	buildinfo.BuildTime = "2026-04-16T10:00:00Z"

	got := versionText()
	checks := []string{
		"component: drive9-server\n",
		"version: v2.0.0\n",
		"git_hash: feedface\n",
		"git_branch: main\n",
		"build_time: 2026-04-16T10:00:00Z\n",
	}
	for _, want := range checks {
		if !strings.Contains(got, want) {
			t.Fatalf("versionText() missing %q in %q", want, got)
		}
	}
}

func TestBuildBackendOptionsFromEnvAudioDisabled(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_API_BASE",
		"DRIVE9_AUDIO_EXTRACT_API_KEY",
		"DRIVE9_AUDIO_EXTRACT_MODEL",
		"DRIVE9_AUDIO_EXTRACT_PROMPT",
		"DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS",
		"DRIVE9_AUDIO_EXTRACT_MAX_BYTES",
		"DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	opts, err := buildBackendOptionsFromEnv()
	if err != nil {
		t.Fatalf("buildBackendOptionsFromEnv: %v", err)
	}
	if backend.AsyncAudioExtractWillWireRuntime(opts.AsyncAudioExtract) {
		t.Fatalf("expected audio runtime disabled, got %+v", opts.AsyncAudioExtract)
	}
}

func TestBuildBackendOptionsFromEnvAudioMissingRequiredConfig(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_API_BASE",
		"DRIVE9_AUDIO_EXTRACT_API_KEY",
		"DRIVE9_AUDIO_EXTRACT_MODEL",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	if err := os.Setenv("DRIVE9_AUDIO_EXTRACT_ENABLED", "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_AUDIO_EXTRACT_API_BASE", "https://example.com"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_AUDIO_EXTRACT_API_KEY", "secret"); err != nil {
		t.Fatal(err)
	}
	if _, err := buildBackendOptionsFromEnv(); err == nil {
		t.Fatal("expected missing model config to fail")
	}
}

func TestBuildBackendOptionsFromEnvAudioOpenAI(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_API_BASE",
		"DRIVE9_AUDIO_EXTRACT_API_KEY",
		"DRIVE9_AUDIO_EXTRACT_MODEL",
		"DRIVE9_AUDIO_EXTRACT_PROMPT",
		"DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS",
		"DRIVE9_AUDIO_EXTRACT_MAX_BYTES",
		"DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	setEnv(t, "DRIVE9_AUDIO_EXTRACT_ENABLED", "true")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_BASE", "https://example.com/v1")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_KEY", "secret")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MODEL", "whisper-1")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_PROMPT", "transcribe in zh")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS", "45")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MAX_BYTES", "1234")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES", "5678")

	opts, err := buildBackendOptionsFromEnv()
	if err != nil {
		t.Fatalf("buildBackendOptionsFromEnv: %v", err)
	}
	if !backend.AsyncAudioExtractWillWireRuntime(opts.AsyncAudioExtract) {
		t.Fatalf("expected audio runtime wired, got %+v", opts.AsyncAudioExtract)
	}
	if opts.AsyncAudioExtract.MaxAudioBytes != 1234 {
		t.Fatalf("MaxAudioBytes=%d, want 1234", opts.AsyncAudioExtract.MaxAudioBytes)
	}
	if opts.AsyncAudioExtract.MaxExtractTextBytes != 5678 {
		t.Fatalf("MaxExtractTextBytes=%d, want 5678", opts.AsyncAudioExtract.MaxExtractTextBytes)
	}
	if got := opts.AsyncAudioExtract.TaskTimeout.Seconds(); got != 45 {
		t.Fatalf("TaskTimeout=%v, want 45s", opts.AsyncAudioExtract.TaskTimeout)
	}
}

func snapshotEnv(t *testing.T, keys []string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		out[key] = os.Getenv(key)
	}
	return out
}

func restoreEnv(t *testing.T, snapshot map[string]string) {
	t.Helper()
	for key, value := range snapshot {
		if value == "" {
			if err := os.Unsetenv(key); err != nil {
				t.Fatalf("unset %s: %v", key, err)
			}
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("restore %s: %v", key, err)
		}
	}
}

func unsetEnv(t *testing.T, keys []string) {
	t.Helper()
	for _, key := range keys {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

func setEnv(t *testing.T, key, value string) {
	t.Helper()
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("set %s: %v", key, err)
	}
}
