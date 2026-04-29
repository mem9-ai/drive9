package main

import (
	"os"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/backend"
)

func TestVersionTextUsesDrive9ServerComponent(t *testing.T) {
	got := versionText()
	if !strings.Contains(got, "component: drive9-server\n") {
		t.Fatalf("versionText() missing drive9-server component line: %q", got)
	}
}

func TestBuildBackendOptionsFromEnvAudioDisabled(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_MODE",
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
		"DRIVE9_AUDIO_EXTRACT_MODE",
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
		"DRIVE9_AUDIO_EXTRACT_MODE",
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

func TestBuildBackendOptionsFromEnvAudioQwenASR(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_MODE",
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
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MODE", "qwen-asr")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_BASE", "https://dashscope.aliyuncs.com/compatible-mode/v1")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_KEY", "secret")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MODEL", "qwen3-asr-flash")
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

func TestBuildAudioExtractOptionsFromEnvOpenAIDefaultMaxBytes(t *testing.T) {
	keys := []string{
		"DRIVE9_AUDIO_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_MODE",
		"DRIVE9_AUDIO_EXTRACT_API_BASE",
		"DRIVE9_AUDIO_EXTRACT_API_KEY",
		"DRIVE9_AUDIO_EXTRACT_MODEL",
		"DRIVE9_AUDIO_EXTRACT_PROMPT",
		"DRIVE9_AUDIO_EXTRACT_RESPONSE_FORMAT",
		"DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS",
		"DRIVE9_AUDIO_EXTRACT_MAX_BYTES",
		"DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	setEnv(t, "DRIVE9_AUDIO_EXTRACT_ENABLED", "true")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MODE", "openai")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_BASE", "https://example.com/v1")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_KEY", "secret")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MODEL", "whisper-1")

	opts, err := buildAudioExtractOptionsFromEnv()
	if err != nil {
		t.Fatalf("buildAudioExtractOptionsFromEnv: %v", err)
	}
	if opts.MaxAudioBytes != 33554432 {
		t.Fatalf("MaxAudioBytes=%d, want 33554432", opts.MaxAudioBytes)
	}
}

func TestBuildAudioExtractOptionsFromEnvQwenASRDefaultMaxBytes(t *testing.T) {
	keys := []string{
		"DRIVE9_AUDIO_EXTRACT_ENABLED",
		"DRIVE9_AUDIO_EXTRACT_MODE",
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
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MODE", "qwen-asr")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_BASE", "https://dashscope.aliyuncs.com/compatible-mode/v1")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_API_KEY", "secret")
	setEnv(t, "DRIVE9_AUDIO_EXTRACT_MODEL", "qwen3-asr-flash")

	opts, err := buildAudioExtractOptionsFromEnv()
	if err != nil {
		t.Fatalf("buildAudioExtractOptionsFromEnv: %v", err)
	}
	if opts.MaxAudioBytes != 7340032 {
		t.Fatalf("MaxAudioBytes=%d, want 7340032", opts.MaxAudioBytes)
	}
}

func TestS3ConfigFromEnv(t *testing.T) {
	keys := []string{
		"DRIVE9_S3_DIR",
		"DRIVE9_S3_BUCKET",
		"DRIVE9_S3_REGION",
		"DRIVE9_S3_PREFIX",
		"DRIVE9_S3_ROLE_ARN",
		"DRIVE9_S3_ENDPOINT",
		"DRIVE9_S3_FORCE_PATH_STYLE",
		"DRIVE9_S3_ACCESS_KEY_ID",
		"DRIVE9_S3_SECRET_ACCESS_KEY",
		"DRIVE9_S3_SESSION_TOKEN",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	cfg := s3ConfigFromEnv()
	if cfg.Dir != defaultS3Dir {
		t.Fatalf("default dir = %q, want %q", cfg.Dir, defaultS3Dir)
	}
	if cfg.Bucket != "" {
		t.Fatalf("default bucket = %q, want empty", cfg.Bucket)
	}

	setEnv(t, "DRIVE9_S3_DIR", " custom-s3 ")
	setEnv(t, "DRIVE9_S3_BUCKET", " bench-bucket ")
	setEnv(t, "DRIVE9_S3_REGION", " us-west-2 ")
	setEnv(t, "DRIVE9_S3_PREFIX", " uploads/ ")
	setEnv(t, "DRIVE9_S3_ROLE_ARN", " arn:aws:iam::123456789012:role/test ")
	setEnv(t, "DRIVE9_S3_ENDPOINT", " http://127.0.0.1:9000 ")
	setEnv(t, "DRIVE9_S3_FORCE_PATH_STYLE", " true ")
	setEnv(t, "DRIVE9_S3_ACCESS_KEY_ID", " minioadmin ")
	setEnv(t, "DRIVE9_S3_SECRET_ACCESS_KEY", " miniosecret ")
	setEnv(t, "DRIVE9_S3_SESSION_TOKEN", " session-token ")

	cfg = s3ConfigFromEnv()
	if cfg.Dir != "custom-s3" {
		t.Fatalf("Dir = %q, want %q", cfg.Dir, "custom-s3")
	}
	if cfg.Bucket != "bench-bucket" || cfg.Region != "us-west-2" {
		t.Fatalf("unexpected bucket/region config: %+v", cfg)
	}
	if cfg.Prefix != "uploads/" || cfg.RoleARN != "arn:aws:iam::123456789012:role/test" {
		t.Fatalf("unexpected prefix/role config: %+v", cfg)
	}
	if cfg.Endpoint != "http://127.0.0.1:9000" || !cfg.ForcePathStyle {
		t.Fatalf("unexpected endpoint config: %+v", cfg)
	}
	if cfg.AccessKeyID != "minioadmin" || cfg.SecretAccessKey != "miniosecret" || cfg.SessionToken != "session-token" {
		t.Fatalf("unexpected static credential config: %+v", cfg)
	}
}

func TestS3ConfigValidateRejectsInvalidStaticCredentialCombinations(t *testing.T) {
	tests := []struct {
		name    string
		cfg     s3Config
		wantErr bool
	}{
		{
			name: "local mode ignores static credential fields",
			cfg: s3Config{
				Dir:         defaultS3Dir,
				AccessKeyID: "ak",
			},
			wantErr: false,
		},
		{
			name: "access key without secret",
			cfg: s3Config{
				Bucket:      "bucket",
				Region:      "us-east-1",
				AccessKeyID: "ak",
			},
			wantErr: true,
		},
		{
			name: "secret without access key",
			cfg: s3Config{
				Bucket:          "bucket",
				Region:          "us-east-1",
				SecretAccessKey: "sk",
			},
			wantErr: true,
		},
		{
			name: "session token without static credentials",
			cfg: s3Config{
				Bucket:       "bucket",
				Region:       "us-east-1",
				SessionToken: "token",
			},
			wantErr: true,
		},
		{
			name: "valid static credentials",
			cfg: s3Config{
				Bucket:          "bucket",
				Region:          "us-east-1",
				AccessKeyID:     "ak",
				SecretAccessKey: "sk",
				SessionToken:    "token",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
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
