package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/server"
)

func TestVersionTextUsesDrive9ServerComponent(t *testing.T) {
	got := versionText()
	if !strings.Contains(got, "component: drive9-server\n") {
		t.Fatalf("versionText() missing drive9-server component line: %q", got)
	}
}

func TestSlockOAuthFromEnvDisabledByDefault(t *testing.T) {
	keys := []string{
		"DRIVE9_SLOCK_ORIGIN",
		"DRIVE9_SLOCK_API_ORIGIN",
		"DRIVE9_SLOCK_CLIENT_ID",
		"DRIVE9_SLOCK_CLIENT_SECRET",
		"DRIVE9_PUBLIC_URL",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	c, err := slockOAuthFromEnv()
	if err != nil {
		t.Fatalf("slockOAuthFromEnv: %v", err)
	}
	if c != nil {
		t.Fatal("expected nil Slock client when DRIVE9_SLOCK_ORIGIN is unset")
	}
}

func TestSlockOAuthFromEnvRequiresCompanionVars(t *testing.T) {
	keys := []string{
		"DRIVE9_SLOCK_ORIGIN",
		"DRIVE9_SLOCK_API_ORIGIN",
		"DRIVE9_SLOCK_CLIENT_ID",
		"DRIVE9_SLOCK_CLIENT_SECRET",
		"DRIVE9_PUBLIC_URL",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	setEnv(t, "DRIVE9_SLOCK_ORIGIN", "https://app.slock.ai")

	if _, err := slockOAuthFromEnv(); err == nil {
		t.Fatal("expected partial Slock config to fail")
	}
}

func TestSlockOAuthFromEnvEnabled(t *testing.T) {
	keys := []string{
		"DRIVE9_SLOCK_ORIGIN",
		"DRIVE9_SLOCK_API_ORIGIN",
		"DRIVE9_SLOCK_CLIENT_ID",
		"DRIVE9_SLOCK_CLIENT_SECRET",
		"DRIVE9_PUBLIC_URL",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	setEnv(t, "DRIVE9_SLOCK_ORIGIN", "https://app.slock.ai")
	setEnv(t, "DRIVE9_SLOCK_API_ORIGIN", "https://api.slock.ai")
	setEnv(t, "DRIVE9_SLOCK_CLIENT_ID", "drive9")
	setEnv(t, "DRIVE9_SLOCK_CLIENT_SECRET", "secret")
	setEnv(t, "DRIVE9_PUBLIC_URL", "https://drive9.example.com")

	c, err := slockOAuthFromEnv()
	if err != nil {
		t.Fatalf("slockOAuthFromEnv: %v", err)
	}
	if c == nil || !strings.Contains(c.LoginURL(), "client_id=drive9") {
		t.Fatalf("unexpected Slock client: %#v", c)
	}
}

func TestTenantPoolMaxSizeFromEnv(t *testing.T) {
	const key = "DRIVE9_TENANT_POOL_MAX_SIZE"
	restore := snapshotEnv(t, []string{key})
	t.Cleanup(func() { restoreEnv(t, restore) })

	unsetEnv(t, []string{key})
	got, err := tenantPoolMaxSizeFromEnv()
	if err != nil {
		t.Fatalf("tenantPoolMaxSizeFromEnv empty: %v", err)
	}
	if got != server.DefaultTenantPoolMaxSize {
		t.Fatalf("empty max size = %d, want %d", got, server.DefaultTenantPoolMaxSize)
	}

	setEnv(t, key, "25")
	got, err = tenantPoolMaxSizeFromEnv()
	if err != nil {
		t.Fatalf("tenantPoolMaxSizeFromEnv valid: %v", err)
	}
	if got != 25 {
		t.Fatalf("max size = %d, want 25", got)
	}

	for _, raw := range []string{"0", "-1", "bad"} {
		setEnv(t, key, raw)
		if _, err := tenantPoolMaxSizeFromEnv(); err == nil {
			t.Fatalf("tenantPoolMaxSizeFromEnv(%q) error = nil, want error", raw)
		}
	}
}

func TestTenantPoolRefillFreeRatioFromEnv(t *testing.T) {
	const key = "DRIVE9_TENANT_POOL_REFILL_FREE_RATIO"
	restore := snapshotEnv(t, []string{key})
	t.Cleanup(func() { restoreEnv(t, restore) })

	unsetEnv(t, []string{key})
	got, err := tenantPoolRefillFreeRatioFromEnv()
	if err != nil {
		t.Fatalf("tenantPoolRefillFreeRatioFromEnv empty: %v", err)
	}
	if got != server.DefaultTenantPoolRefillFreeRatio {
		t.Fatalf("empty refill ratio = %f, want %f", got, server.DefaultTenantPoolRefillFreeRatio)
	}

	setEnv(t, key, "0.75")
	got, err = tenantPoolRefillFreeRatioFromEnv()
	if err != nil {
		t.Fatalf("tenantPoolRefillFreeRatioFromEnv valid: %v", err)
	}
	if got != 0.75 {
		t.Fatalf("refill ratio = %f, want 0.75", got)
	}

	for _, raw := range []string{"0", "-0.1", "1.1", "NaN", "bad"} {
		setEnv(t, key, raw)
		if _, err := tenantPoolRefillFreeRatioFromEnv(); err == nil {
			t.Fatalf("tenantPoolRefillFreeRatioFromEnv(%q) error = nil, want error", raw)
		}
	}
}

func TestDBHealthProbeOptionsFromEnvDefaults(t *testing.T) {
	keys := []string{
		"DRIVE9_DB_HEALTH_PROBE_META_ENABLED",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	opts := dbHealthProbeOptionsFromEnv()
	if !opts.ProbeMeta {
		t.Fatal("ProbeMeta=false, want true")
	}
}

func TestDBHealthProbeOptionsFromEnvOverrides(t *testing.T) {
	keys := []string{
		"DRIVE9_DB_HEALTH_PROBE_META_ENABLED",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	setEnv(t, "DRIVE9_DB_HEALTH_PROBE_META_ENABLED", "false")

	opts := dbHealthProbeOptionsFromEnv()
	if opts.ProbeMeta {
		t.Fatal("ProbeMeta=true, want false")
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

func TestBuildTenantWorkerConfigFromEnvReadsWorkerOptionsWithoutEmbedder(t *testing.T) {
	keys := []string{
		"DRIVE9_EMBED_API_BASE",
		"DRIVE9_EMBED_API_KEY",
		"DRIVE9_EMBED_MODEL",
		"DRIVE9_SEMANTIC_WORKERS",
		"DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	setEnv(t, "DRIVE9_SEMANTIC_WORKERS", "8")
	setEnv(t, "DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY", "4")

	client, opts, err := buildTenantWorkerConfigFromEnv()
	if err != nil {
		t.Fatalf("buildTenantWorkerConfigFromEnv: %v", err)
	}
	if client != nil {
		t.Fatal("client configured without DRIVE9_EMBED_*")
	}
	if opts.Workers != 8 {
		t.Fatalf("Workers=%d, want 8", opts.Workers)
	}
	if opts.PerTenantConcurrency != 4 {
		t.Fatalf("PerTenantConcurrency=%d, want 4", opts.PerTenantConcurrency)
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
		"DRIVE9_S3_ENCRYPTION_MODE",
		"DRIVE9_S3_KMS_KEY_ID",
		"DRIVE9_S3_BUCKET_KEY_ENABLED",
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
	if cfg.EncryptionPolicy.Mode != meta.S3EncryptionModeNone {
		t.Fatalf("default encryption mode = %q, want none", cfg.EncryptionPolicy.Mode)
	}
	if !cfg.EncryptionPolicy.BucketKeyEnabled {
		t.Fatal("default bucket key enabled = false, want true")
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
	setEnv(t, "DRIVE9_S3_ENCRYPTION_MODE", " sse-kms ")
	setEnv(t, "DRIVE9_S3_KMS_KEY_ID", " arn:aws:kms:us-west-2:123456789012:key/test ")
	setEnv(t, "DRIVE9_S3_BUCKET_KEY_ENABLED", " false ")

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
	if cfg.EncryptionPolicy.Mode != meta.S3EncryptionModeSSEKMS ||
		cfg.EncryptionPolicy.KMSKeyID != "arn:aws:kms:us-west-2:123456789012:key/test" ||
		cfg.EncryptionPolicy.BucketKeyEnabled {
		t.Fatalf("unexpected encryption config: %+v", cfg.EncryptionPolicy)
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
		{
			name: "sse kms missing key",
			cfg: s3Config{
				EncryptionPolicy: meta.S3EncryptionPolicy{Mode: meta.S3EncryptionModeSSEKMS},
			},
			wantErr: true,
		},
		{
			name: "sse kms with key in local mode",
			cfg: s3Config{
				EncryptionPolicy: meta.S3EncryptionPolicy{
					Mode:             meta.S3EncryptionModeSSEKMS,
					KMSKeyID:         "key",
					BucketKeyEnabled: true,
				},
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

func TestEnvDurationCompat(t *testing.T) {
	const canonical = "DRIVE9_TEST_DURATION_COMPAT"
	const deprecated = "DRIVE9_TEST_DURATION_COMPAT_DEPRECATED_MS"
	fallback := 5 * time.Minute

	t.Run("fallback when neither is set", func(t *testing.T) {
		if got := envDurationCompat(canonical, deprecated, fallback); got != fallback {
			t.Fatalf("expected fallback %v, got %v", fallback, got)
		}
	})
	t.Run("canonical wins when both are set", func(t *testing.T) {
		t.Setenv(canonical, "10s")
		t.Setenv(deprecated, "20s")
		if got := envDurationCompat(canonical, deprecated, fallback); got != 10*time.Second {
			t.Fatalf("expected canonical 10s, got %v", got)
		}
	})
	t.Run("deprecated name honored when canonical is unset", func(t *testing.T) {
		t.Setenv(deprecated, "20s")
		if got := envDurationCompat(canonical, deprecated, fallback); got != 20*time.Second {
			t.Fatalf("expected deprecated 20s, got %v", got)
		}
	})
}

func TestStartupEnvsSetMetaMediaAndVideoDefaults(t *testing.T) {
	keys := []string{
		"DRIVE9_MEDIA_EXTRACT_MAX_FILES",
		"DRIVE9_VIDEO_EXTRACT_MAX_FILES",
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)

	origMedia := meta.DefaultMaxMediaLLMFiles()
	origVideo := meta.DefaultMaxVideoLLMFiles()
	defer func() {
		meta.SetDefaultMaxMediaLLMFiles(origMedia)
		meta.SetDefaultMaxVideoLLMFiles(origVideo)
	}()

	if meta.DefaultMaxMediaLLMFiles() != 500 {
		t.Errorf("default media = %d, want 500", meta.DefaultMaxMediaLLMFiles())
	}
	if meta.DefaultMaxVideoLLMFiles() != 50 {
		t.Errorf("default video = %d, want 50", meta.DefaultMaxVideoLLMFiles())
	}

	setEnv(t, "DRIVE9_MEDIA_EXTRACT_MAX_FILES", "10000")
	setEnv(t, "DRIVE9_VIDEO_EXTRACT_MAX_FILES", "10000")
	applyQuotaDefaultsFromEnv()

	if meta.DefaultMaxMediaLLMFiles() != 10000 {
		t.Errorf("default media = %d, want 10000", meta.DefaultMaxMediaLLMFiles())
	}
	if meta.DefaultMaxVideoLLMFiles() != 10000 {
		t.Errorf("default video = %d, want 10000", meta.DefaultMaxVideoLLMFiles())
	}
}
