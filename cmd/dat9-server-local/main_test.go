package main

import (
	"database/sql"
	"errors"
	"testing"
)

func TestShouldUseLocalDatabaseAutoEmbedding(t *testing.T) {
	orig := localAutoEmbeddingSchemaValidator
	t.Cleanup(func() { localAutoEmbeddingSchemaValidator = orig })

	localAutoEmbeddingSchemaValidator = func(*sql.DB) error {
		return errors.New("not auto schema")
	}
	if !shouldUseLocalDatabaseAutoEmbedding(nil, true) {
		t.Fatal("schema-initialized local mode should force database auto embedding")
	}

	localAutoEmbeddingSchemaValidator = func(*sql.DB) error { return nil }
	if !shouldUseLocalDatabaseAutoEmbedding(&sql.DB{}, false) {
		t.Fatal("validated auto schema should enable database auto embedding")
	}

	localAutoEmbeddingSchemaValidator = func(*sql.DB) error {
		return errors.New("not auto schema")
	}
	if shouldUseLocalDatabaseAutoEmbedding(&sql.DB{}, false) {
		t.Fatal("non-auto schema should keep app-managed embedding mode")
	}
}
