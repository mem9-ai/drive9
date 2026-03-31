package main

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/mem9-ai/dat9/pkg/tenant"
)

func TestDetectLocalTiDBEmbeddingMode(t *testing.T) {
	origDetector := localTiDBEmbeddingModeDetector
	origValidator := localTiDBSchemaValidator
	t.Cleanup(func() {
		localTiDBEmbeddingModeDetector = origDetector
		localTiDBSchemaValidator = origValidator
	})

	mode, err := detectLocalTiDBEmbeddingMode(nil, true, tenant.TiDBEmbeddingModeApp, true)
	if err != nil {
		t.Fatalf("schema-initialized explicit app mode returned error: %v", err)
	}
	if mode != tenant.TiDBEmbeddingModeApp {
		t.Fatalf("schema-initialized explicit mode=%q, want %q", mode, tenant.TiDBEmbeddingModeApp)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (tenant.TiDBEmbeddingMode, error) {
		return tenant.TiDBEmbeddingModeUnknown, errors.New("should not be called")
	}
	mode, err = detectLocalTiDBEmbeddingMode(nil, true, tenant.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("schema-initialized default mode returned error: %v", err)
	}
	if mode != tenant.TiDBEmbeddingModeAuto {
		t.Fatalf("schema-initialized default mode=%q, want %q", mode, tenant.TiDBEmbeddingModeAuto)
	}

	localTiDBSchemaValidator = func(*sql.DB, tenant.TiDBEmbeddingMode) error { return nil }
	mode, err = detectLocalTiDBEmbeddingMode(&sql.DB{}, false, tenant.TiDBEmbeddingModeApp, true)
	if err != nil {
		t.Fatalf("explicit app mode returned error: %v", err)
	}
	if mode != tenant.TiDBEmbeddingModeApp {
		t.Fatalf("explicit mode=%q, want %q", mode, tenant.TiDBEmbeddingModeApp)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (tenant.TiDBEmbeddingMode, error) {
		return tenant.TiDBEmbeddingModeAuto, nil
	}
	mode, err = detectLocalTiDBEmbeddingMode(&sql.DB{}, false, tenant.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("detect auto mode returned error: %v", err)
	}
	if mode != tenant.TiDBEmbeddingModeAuto {
		t.Fatalf("detected mode=%q, want %q", mode, tenant.TiDBEmbeddingModeAuto)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (tenant.TiDBEmbeddingMode, error) {
		return tenant.TiDBEmbeddingModeApp, nil
	}
	mode, err = detectLocalTiDBEmbeddingMode(&sql.DB{}, false, tenant.TiDBEmbeddingModeUnknown, false)
	if err != nil {
		t.Fatalf("detect app mode returned error: %v", err)
	}
	if mode != tenant.TiDBEmbeddingModeApp {
		t.Fatalf("detected mode=%q, want %q", mode, tenant.TiDBEmbeddingModeApp)
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (tenant.TiDBEmbeddingMode, error) {
		return tenant.TiDBEmbeddingModeUnknown, nil
	}
	if _, err := detectLocalTiDBEmbeddingMode(&sql.DB{}, false, tenant.TiDBEmbeddingModeUnknown, false); err == nil {
		t.Fatal("expected unknown mode to fail")
	}

	localTiDBEmbeddingModeDetector = func(*sql.DB) (tenant.TiDBEmbeddingMode, error) {
		return tenant.TiDBEmbeddingModeAuto, nil
	}
	localTiDBSchemaValidator = func(*sql.DB, tenant.TiDBEmbeddingMode) error {
		return errors.New("bad schema")
	}
	if _, err := detectLocalTiDBEmbeddingMode(&sql.DB{}, false, tenant.TiDBEmbeddingModeUnknown, false); err == nil {
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
	if explicit || mode != tenant.TiDBEmbeddingModeUnknown {
		t.Fatalf("unset mode=(%q,%v), want (unknown,false)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "auto"); err != nil {
		t.Fatalf("set auto mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("auto mode returned error: %v", err)
	}
	if !explicit || mode != tenant.TiDBEmbeddingModeAuto {
		t.Fatalf("auto mode=(%q,%v), want (auto,true)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "app"); err != nil {
		t.Fatalf("set app mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("app mode returned error: %v", err)
	}
	if !explicit || mode != tenant.TiDBEmbeddingModeApp {
		t.Fatalf("app mode=(%q,%v), want (app,true)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "detect"); err != nil {
		t.Fatalf("set detect mode: %v", err)
	}
	mode, explicit, err = localEmbeddingModeFromEnv()
	if err != nil {
		t.Fatalf("detect mode returned error: %v", err)
	}
	if explicit || mode != tenant.TiDBEmbeddingModeUnknown {
		t.Fatalf("detect mode=(%q,%v), want (unknown,false)", mode, explicit)
	}

	if err := os.Setenv(envLocalEmbeddingMode, "bogus"); err != nil {
		t.Fatalf("set bogus mode: %v", err)
	}
	if _, _, err := localEmbeddingModeFromEnv(); err == nil {
		t.Fatal("expected invalid mode to fail")
	}
}
