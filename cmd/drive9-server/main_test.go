package main

import (
	"os"
	"testing"

	"github.com/mem9-ai/dat9/pkg/backend"
)

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
	restore := snapshotEnv(keys)
	t.Cleanup(func() { restoreEnv(restore) })
	unsetEnv(keys)

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
	restore := snapshotEnv(keys)
	t.Cleanup(func() { restoreEnv(restore) })
	unsetEnv(keys)

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
	restore := snapshotEnv(keys)
	t.Cleanup(func() { restoreEnv(restore) })
	unsetEnv(keys)

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

func snapshotEnv(keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		out[key] = os.Getenv(key)
	}
	return out
}

func restoreEnv(snapshot map[string]string) {
	for key, value := range snapshot {
		if value == "" {
			_ = os.Unsetenv(key)
			continue
		}
		_ = os.Setenv(key, value)
	}
}

func unsetEnv(keys []string) {
	for _, key := range keys {
		_ = os.Unsetenv(key)
	}
}

func setEnv(t *testing.T, key, value string) {
	t.Helper()
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("set %s: %v", key, err)
	}
}
