package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

func TestDetectLocalTiDBEmbeddingMode(t *testing.T) {
	origDetector := localTiDBEmbeddingModeDetector
	origValidator := localTiDBSchemaValidator
	t.Cleanup(func() {
		localTiDBEmbeddingModeDetector = origDetector
		localTiDBSchemaValidator = origValidator
	})

	mode, err := detectLocalTiDBEmbeddingMode(nil, true, schema.TiDBEmbeddingModeApp, true)
	if err != nil {
		t.Fatalf("schema-initialized explicit app mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeApp {
		t.Fatalf("schema-initialized explicit mode=%q, want %q", mode, schema.TiDBEmbeddingModeApp)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeUnknown, errors.New("should not be called")
	}
	mode, err = detectLocalTiDBEmbeddingMode(nil, true, schema.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("schema-initialized default mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeAuto {
		t.Fatalf("schema-initialized default mode=%q, want %q", mode, schema.TiDBEmbeddingModeAuto)
	}

	localTiDBSchemaValidator = func(*sql.DB, schema.TiDBEmbeddingMode) error { return nil }
	mode, err = detectLocalTiDBEmbeddingMode(&sql.DB{}, false, schema.TiDBEmbeddingModeApp, true)
	if err != nil {
		t.Fatalf("explicit app mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeApp {
		t.Fatalf("explicit mode=%q, want %q", mode, schema.TiDBEmbeddingModeApp)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeAuto, nil
	}
	mode, err = detectLocalTiDBEmbeddingMode(&sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("detect auto mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeAuto {
		t.Fatalf("detected mode=%q, want %q", mode, schema.TiDBEmbeddingModeAuto)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeApp, nil
	}
	mode, err = detectLocalTiDBEmbeddingMode(&sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("detect app mode returned error: %v", err)
	}
	if mode != schema.TiDBEmbeddingModeApp {
		t.Fatalf("detected mode=%q, want %q", mode, schema.TiDBEmbeddingModeApp)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeUnknown, nil
	}
	if _, err := detectLocalTiDBEmbeddingMode(&sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false); err == nil {
		t.Fatal("expected unknown mode to fail")
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (schema.TiDBEmbeddingMode, error) {
		return schema.TiDBEmbeddingModeAuto, nil
	}
	localTiDBSchemaValidator = func(*sql.DB, schema.TiDBEmbeddingMode) error {
		return errors.New("bad schema")
	}
	if _, err := detectLocalTiDBEmbeddingMode(&sql.DB{}, false, schema.TiDBEmbeddingModeUnknown, false); err == nil {
		t.Fatal("expected validation failure to propagate")
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

func TestLocalStubAudioTextExtractorTranscript(t *testing.T) {
	var ex localStubAudioTextExtractor
	got, err := ex.ExtractAudioText(context.Background(), backend.AudioExtractRequest{Path: "/audio/clip.mp3"})
	if err != nil {
		t.Fatal(err)
	}
	want := "audio transcript for clip.mp3"
	if got != want {
		t.Fatalf("transcript=%q, want %q", got, want)
	}
}
