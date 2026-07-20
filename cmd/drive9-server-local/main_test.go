package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

func TestVersionTextUsesDrive9ServerLocalComponent(t *testing.T) {
	got := versionText()
	if !strings.Contains(got, "component: drive9-server-local\n") {
		t.Fatalf("versionText() missing drive9-server-local component line: %q", got)
	}
}

func TestDetectLocalTiDBEmbeddingMode(t *testing.T) {
	origDetector := localTiDBEmbeddingModeDetector
	origValidator := localTiDBSchemaValidator
	origNoEmbeddingValidator := localNoEmbeddingSchemaValidator
	t.Cleanup(func() {
		localTiDBEmbeddingModeDetector = origDetector
		localTiDBSchemaValidator = origValidator
		localNoEmbeddingSchemaValidator = origNoEmbeddingValidator
	})
	ctx := context.Background()

	mode, err := detectLocalTiDBEmbeddingMode(ctx, nil, true, schema.TiDBEmbeddingModeApp, true)
	if err != nil {
		t.Fatalf("schema-initialized explicit app mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeApp {
		t.Fatalf("schema-initialized explicit mode=%q, want %q", mode, schema.TiDBEmbeddingModeApp)
	}

	mode, err = detectLocalTiDBEmbeddingMode(ctx, nil, true, localEmbeddingModeNone, true)
	if err != nil {
		t.Fatalf("schema-initialized explicit none mode returned error: %v", err)
	}
	if mode != localEmbeddingModeNone {
		t.Fatalf("schema-initialized none mode=%q, want %q", mode, localEmbeddingModeNone)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeUnknown, errors.New("should not be called")
	}
	mode, err = detectLocalTiDBEmbeddingMode(ctx, nil, true, schema.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("schema-initialized default mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeAuto {
		t.Fatalf("schema-initialized default mode=%q, want %q", mode, schema.TiDBEmbeddingModeAuto)
	}

	localTiDBSchemaValidator = func(context.Context, *sql.DB, schema.TiDBEmbeddingMode) error { return nil }
	mode, err = detectLocalTiDBEmbeddingMode(ctx, &sql.DB{}, false, schema.TiDBEmbeddingModeApp, true)
	if err != nil {
		t.Fatalf("explicit app mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeApp {
		t.Fatalf("explicit mode=%q, want %q", mode, schema.TiDBEmbeddingModeApp)
	}

	localNoEmbeddingSchemaValidator = func(context.Context, *sql.DB) error { return nil }
	mode, err = detectLocalTiDBEmbeddingMode(ctx, &sql.DB{}, false, localEmbeddingModeNone, true)
	if err != nil {
		t.Fatalf("explicit none mode returned error: %v", err)
	}
	if mode != localEmbeddingModeNone {
		t.Fatalf("explicit none mode=%q, want %q", mode, localEmbeddingModeNone)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeAuto, nil
	}
	mode, err = detectLocalTiDBEmbeddingMode(ctx, &sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("detect auto mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeAuto {
		t.Fatalf("detected mode=%q, want %q", mode, schema.TiDBEmbeddingModeAuto)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeApp, nil
	}
	mode, err = detectLocalTiDBEmbeddingMode(ctx, &sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("detect app mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeApp {
		t.Fatalf("detected mode=%q, want %q", mode, schema.TiDBEmbeddingModeApp)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeUnknown, nil
	}
	if _, err := detectLocalTiDBEmbeddingMode(ctx, &sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false); err == nil {
		t.Fatal("expected unknown mode to fail")
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeAuto, nil
	}
	localTiDBSchemaValidator = func(context.Context, *sql.DB, schema.TiDBEmbeddingMode) error {
		return errors.New("bad schema")
	}
	if _, err := detectLocalTiDBEmbeddingMode(ctx, &sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false); err == nil {
		t.Fatal("expected validation failure to propagate")
	}
}

func TestInitLocalTenantSchemaSelectsNoEmbeddingInitializer(t *testing.T) {
	origNoEmbeddingInitializer := localNoEmbeddingSchemaInitializer
	origTiDBInitializer := localTiDBSchemaInitializer
	t.Cleanup(func() {
		localNoEmbeddingSchemaInitializer = origNoEmbeddingInitializer
		localTiDBSchemaInitializer = origTiDBInitializer
	})

	calledNoEmbedding := false
	localNoEmbeddingSchemaInitializer = func(context.Context, string) error {
		calledNoEmbedding = true
		return nil
	}
	localTiDBSchemaInitializer = func(context.Context, string, schema.TiDBEmbeddingMode, schema.InitTiDBTenantSchemaOptions) error {
		return errors.New("tidb initializer should not be called")
	}

	if err := initLocalTenantSchema(context.Background(), "dsn", localEmbeddingModeNone); err != nil {
		t.Fatalf("init none schema: %v", err)
	}
	if !calledNoEmbedding {
		t.Fatal("expected no-embedding initializer to be called")
	}
}

func TestLocalEmbeddingModeFromEnv(t *testing.T) {
	orig := os.Getenv(envLocalEmbeddingMode)
	t.Cleanup(func() {
		if orig == "" {
			_ = os.Unsetenv(envLocalEmbeddingMode)
		} else {
			_ = os.Setenv(envLocalEmbeddingMode, orig)
		}
	})

	_ = os.Unsetenv(envLocalEmbeddingMode)
	mode, explicit, err := localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("unset mode returned error: %v", err)
	}
	if explicit || mode != schema.TiDBEmbeddingModeUnknown {
		t.Fatalf("unset mode=(%q,%v), want (unknown,false)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "auto"); err != nil {
		t.Fatalf("set auto mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("auto mode returned error: %v", err)
	}
	if !explicit || mode != schema.TiDBEmbeddingModeAuto {
		t.Fatalf("auto mode=(%q,%v), want (auto,true)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "app"); err != nil {
		t.Fatalf("set app mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("app mode returned error: %v", err)
	}
	if !explicit || mode != schema.TiDBEmbeddingModeApp {
		t.Fatalf("app mode=(%q,%v), want (app,true)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "none"); err != nil {
		t.Fatalf("set none mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("none mode returned error: %v", err)
	}
	if !explicit || mode != localEmbeddingModeNone {
		t.Fatalf("none mode=(%q,%v), want (none,true)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "skip"); err != nil {
		t.Fatalf("set skip mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("skip mode returned error: %v", err)
	}
	if !explicit || mode != localEmbeddingModeNone {
		t.Fatalf("skip mode=(%q,%v), want (none,true)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "detect"); err != nil {
		t.Fatalf("set detect mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("detect mode returned error: %v", err)
	}
	if explicit || mode != schema.TiDBEmbeddingModeUnknown {
		t.Fatalf("detect mode=(%q,%v), want (unknown,false)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "bogus"); err != nil {
		t.Fatalf("set bogus mode: %v", err)
	}
	if _, _, err := localEmbeddingModeFromEnv(); err == nil {
		t.Fatal("expected invalid mode to fail")
	}
}

func TestBuildSemanticWorkerConfigFromEnvReadsWorkerOptionsWithoutEmbedder(t *testing.T) {
	keys := []string{
		"DRIVE9_EMBED_API_BASE",
		"DRIVE9_EMBED_API_KEY",
		"DRIVE9_EMBED_MODEL",
		"DRIVE9_SEMANTIC_WORKERS",
		"DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY",
	}
	prev := make(map[string]string, len(keys))
	for _, key := range keys {
		prev[key] = os.Getenv(key)
	}
	t.Cleanup(func() {
		for _, key := range keys {
			if prev[key] == "" {
				_ = os.Unsetenv(key)
			} else {
				_ = os.Setenv(key, prev[key])
			}
		}
	})
	for _, key := range keys {
		_ = os.Unsetenv(key)
	}
	if err := os.Setenv("DRIVE9_SEMANTIC_WORKERS", "8"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY", "4"); err != nil {
		t.Fatal(err)
	}

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

func TestLocalS3ConfigFromEnv(t *testing.T) {
	keys := []string{
		"TMPDIR",
		"DRIVE9_S3_BUCKET",
		"DRIVE9_S3_DIR",
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
	prev := make(map[string]string, len(keys))
	for _, k := range keys {
		prev[k] = os.Getenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if prev[k] == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, prev[k])
			}
		}
	})

	unsetAll := func() {
		for _, k := range keys {
			_ = os.Unsetenv(k)
		}
	}

	unsetAll()
	if err := os.Setenv("TMPDIR", filepath.Join(string(os.PathSeparator), "tmp", "podman")); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_BUCKET", "  bench-bucket  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_DIR", filepath.Join(os.Getenv("TMPDIR"), "drive9-local-s3")); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_REGION", "  us-west-2  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_PREFIX", "  uploads/  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_ROLE_ARN", "  arn:aws:iam::123456789012:role/test  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_ENDPOINT", "  http://127.0.0.1:9000  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_FORCE_PATH_STYLE", " true "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_ACCESS_KEY_ID", "  minioadmin  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_SECRET_ACCESS_KEY", "  miniosecret  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_SESSION_TOKEN", "  session-token  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_ENCRYPTION_MODE", "  sse-kms  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_KMS_KEY_ID", "  arn:aws:kms:us-west-2:123456789012:key/test  "); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_BUCKET_KEY_ENABLED", " false "); err != nil {
		t.Fatal(err)
	}
	cfg, err := localS3ConfigFromEnv()
	if err != nil {
		t.Fatalf("aws config with default local dir should succeed: %v", err)
	}
	if cfg.Mode != "aws" || cfg.Bucket != "bench-bucket" || cfg.Region != "us-west-2" {
		t.Fatalf("unexpected aws config: %+v", cfg)
	}
	if cfg.Prefix != "uploads/" || cfg.RoleARN != "arn:aws:iam::123456789012:role/test" {
		t.Fatalf("unexpected trimmed aws fields: %+v", cfg)
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

	unsetAll()
	if err := os.Setenv("DRIVE9_S3_BUCKET", "bucket"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("DRIVE9_S3_DIR", " /tmp/custom-local-s3 "); err != nil {
		t.Fatal(err)
	}
	if _, err := localS3ConfigFromEnv(); err == nil {
		t.Fatal("expected explicit local dir to conflict with aws bucket")
	}

	unsetAll()
	if err := os.Setenv("DRIVE9_S3_DIR", " /tmp/local-s3//nested/ "); err != nil {
		t.Fatal(err)
	}
	cfg, err = localS3ConfigFromEnv()
	if err != nil {
		t.Fatalf("local config returned error: %v", err)
	}
	if cfg.Mode != "local" {
		t.Fatalf("mode = %q, want local", cfg.Mode)
	}
	if cfg.Dir != "/tmp/local-s3/nested" {
		t.Fatalf("dir = %q, want %q", cfg.Dir, "/tmp/local-s3/nested")
	}
	if cfg.EncryptionPolicy.Mode != meta.S3EncryptionModeNone || !cfg.EncryptionPolicy.BucketKeyEnabled {
		t.Fatalf("unexpected default encryption config: %+v", cfg.EncryptionPolicy)
	}

	unsetAll()
	if err := os.Setenv("DRIVE9_S3_ENCRYPTION_MODE", "sse-kms"); err != nil {
		t.Fatal(err)
	}
	if _, err := localS3ConfigFromEnv(); err == nil {
		t.Fatal("expected missing KMS key to fail")
	}
}

func TestBuildLocalAudioExtractOptionsFromEnv(t *testing.T) {
	keys := []string{
		envAudioExtractEnabled,
		envAudioExtractMode,
		envAudioExtractMaxBytes,
		envAudioExtractTimeoutSeconds,
		envAudioExtractMaxTextBytes,
		envAudioExtractAPIBase,
		envAudioExtractAPIKey,
		envAudioExtractModel,
		envAudioExtractPrompt,
	}
	prev := make(map[string]string, len(keys))
	for _, k := range keys {
		prev[k] = os.Getenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if prev[k] == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, prev[k])
			}
		}
	})

	unsetAll := func() {
		for _, k := range keys {
			_ = os.Unsetenv(k)
		}
	}

	unsetAll()
	opts, err := buildLocalAudioExtractOptionsFromEnv()
	if err != nil {
		t.Fatalf("disabled (unset): %v", err)
	}
	if backend.AsyncAudioExtractWillWireRuntime(opts) {
		t.Fatalf("expected audio runtime unwired when disabled, got %+v", opts)
	}

	unsetAll()
	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if _, err := buildLocalAudioExtractOptionsFromEnv(); err == nil {
		t.Fatal("expected error when mode missing")
	}

	unsetAll()
	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractMode, "whisper"); err != nil {
		t.Fatal(err)
	}
	if _, err := buildLocalAudioExtractOptionsFromEnv(); err == nil {
		t.Fatal("expected error for unsupported mode")
	}

	unsetAll()
	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractMode, "stub"); err != nil {
		t.Fatal(err)
	}
	opts, err = buildLocalAudioExtractOptionsFromEnv()
	if err != nil {
		t.Fatalf("stub mode: %v", err)
	}
	if !backend.AsyncAudioExtractWillWireRuntime(opts) {
		t.Fatalf("expected stub runtime wired, got %+v", opts)
	}
	if opts.Extractor == nil {
		t.Fatal("expected non-nil extractor")
	}

	unsetAll()
	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractMode, "openai"); err != nil {
		t.Fatal(err)
	}
	if _, err := buildLocalAudioExtractOptionsFromEnv(); err == nil {
		t.Fatal("expected openai mode without provider config to fail")
	}

	unsetAll()
	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractMode, "openai"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractAPIBase, "https://example.com/v1"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractAPIKey, "secret"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractModel, "whisper-1"); err != nil {
		t.Fatal(err)
	}
	opts, err = buildLocalAudioExtractOptionsFromEnv()
	if err != nil {
		t.Fatalf("openai mode: %v", err)
	}
	if !backend.AsyncAudioExtractWillWireRuntime(opts) {
		t.Fatalf("expected openai runtime wired, got %+v", opts)
	}
}

func TestBuildBackendOptionsFromEnvAudioStub(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		envAudioExtractEnabled,
		envAudioExtractMode,
		envAudioExtractAPIBase,
		envAudioExtractAPIKey,
		envAudioExtractModel,
	}
	prev := make(map[string]string, len(keys))
	for _, k := range keys {
		prev[k] = os.Getenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if prev[k] == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, prev[k])
			}
		}
	})
	for _, k := range keys {
		_ = os.Unsetenv(k)
	}

	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractMode, "stub"); err != nil {
		t.Fatal(err)
	}
	opts, err := buildBackendOptionsFromEnv()
	if err != nil {
		t.Fatalf("buildBackendOptionsFromEnv: %v", err)
	}
	if !backend.AsyncAudioExtractWillWireRuntime(opts.AsyncAudioExtract) {
		t.Fatalf("expected async audio in backend options, got %+v", opts.AsyncAudioExtract)
	}
}

func TestBuildBackendOptionsFromEnvAudioOpenAI(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		envAudioExtractEnabled,
		envAudioExtractMode,
		envAudioExtractAPIBase,
		envAudioExtractAPIKey,
		envAudioExtractModel,
		envAudioExtractPrompt,
	}
	prev := make(map[string]string, len(keys))
	for _, k := range keys {
		prev[k] = os.Getenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if prev[k] == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, prev[k])
			}
		}
	})
	for _, k := range keys {
		_ = os.Unsetenv(k)
	}

	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractMode, "openai"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractAPIBase, "https://example.com/v1"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractAPIKey, "secret"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractModel, "whisper-1"); err != nil {
		t.Fatal(err)
	}
	opts, err := buildBackendOptionsFromEnv()
	if err != nil {
		t.Fatalf("buildBackendOptionsFromEnv: %v", err)
	}
	if !backend.AsyncAudioExtractWillWireRuntime(opts.AsyncAudioExtract) {
		t.Fatalf("expected async audio in backend options, got %+v", opts.AsyncAudioExtract)
	}
}

func TestBuildBackendOptionsFromEnvAudioQwenASR(t *testing.T) {
	keys := []string{
		"DRIVE9_QUERY_EMBED_API_BASE",
		"DRIVE9_QUERY_EMBED_API_KEY",
		"DRIVE9_QUERY_EMBED_MODEL",
		"DRIVE9_IMAGE_EXTRACT_ENABLED",
		envAudioExtractEnabled,
		envAudioExtractMode,
		envAudioExtractAPIBase,
		envAudioExtractAPIKey,
		envAudioExtractModel,
		envAudioExtractPrompt,
		envAudioExtractTimeoutSeconds,
		envAudioExtractMaxBytes,
		envAudioExtractMaxTextBytes,
	}
	prev := make(map[string]string, len(keys))
	for _, k := range keys {
		prev[k] = os.Getenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if prev[k] == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, prev[k])
			}
		}
	})
	for _, k := range keys {
		_ = os.Unsetenv(k)
	}

	if err := os.Setenv(envAudioExtractEnabled, "true"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractMode, "qwen-asr"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractAPIBase, "https://dashscope.aliyuncs.com/compatible-mode/v1"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractAPIKey, "secret"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(envAudioExtractModel, "qwen3-asr-flash"); err != nil {
		t.Fatal(err)
	}
	opts, err := buildBackendOptionsFromEnv()
	if err != nil {
		t.Fatalf("buildBackendOptionsFromEnv: %v", err)
	}
	if !backend.AsyncAudioExtractWillWireRuntime(opts.AsyncAudioExtract) {
		t.Fatalf("expected async audio in backend options, got %+v", opts.AsyncAudioExtract)
	}
}

func TestLocalStubAudioTextExtractorTranscript(t *testing.T) {
	var ex localStubAudioTextExtractor
	got, _, err := ex.ExtractAudioText(context.Background(), backend.AudioExtractRequest{Path: "/audio/clip.mp3"})
	if err != nil {
		t.Fatal(err)
	}
	want := "audio transcript for clip.mp3"
	if got != want {
		t.Fatalf("transcript=%q, want %q", got, want)
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
