package tenant

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

func TestTiDBTenantSchemaVersionForEmbeddingMode(t *testing.T) {
	profile := schema.TiDBAutoEmbeddingProfile{
		Model:       "openai/text-embedding-v4",
		Dimensions:  1024,
		OptionsJSON: `{"dimensions":1024}`,
	}
	for _, mode := range []string{meta.TenantEmbeddingModeAuto, meta.TenantEmbeddingModeFTSOnly} {
		got, err := TiDBTenantSchemaVersionForEmbeddingMode(mode, profile)
		if err != nil {
			t.Fatalf("TiDBTenantSchemaVersionForEmbeddingMode(%q): %v", mode, err)
		}
		tidbMode, err := TiDBEmbeddingModeForTenantMode(mode)
		if err != nil {
			t.Fatalf("TiDBEmbeddingModeForTenantMode(%q): %v", mode, err)
		}
		want, err := schema.TiDBTenantSchemaVersionForEmbeddingModeProfile(tidbMode, profile)
		if err != nil {
			t.Fatalf("TiDBTenantSchemaVersionForEmbeddingModeProfile(%q): %v", mode, err)
		}
		if got != want {
			t.Fatalf("version for %q = %d, want %d", mode, got, want)
		}
	}

	if _, err := TiDBTenantSchemaVersionForEmbeddingMode("invalid", profile); err == nil {
		t.Fatal("TiDBTenantSchemaVersionForEmbeddingMode(invalid) succeeded, want error")
	}
}
