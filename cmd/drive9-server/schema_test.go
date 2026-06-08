package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	tenantschema "github.com/mem9-ai/dat9/pkg/tenant/schema"
)

func TestSchemaDumpInitSQLByProvider(t *testing.T) {
	for _, provider := range []string{"tidb_zero", "tidb_cloud_starter"} {
		t.Run(provider, func(t *testing.T) {
			out := captureSchemaStdout(t, func() {
				if err := runSchemaCommand([]string{"dump-init-sql", "--provider", provider}); err != nil {
					t.Fatalf("dump provider schema: %v", err)
				}
			})

			if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS inodes") {
				t.Fatalf("dump missing inodes table: %q", out)
			}
			if !strings.Contains(out, "GENERATED ALWAYS AS (EMBED_TEXT") {
				t.Fatalf("dump missing auto-embedding expression: %q", out)
			}
			if !strings.Contains(out, "CREATE INDEX idx_task_claim_type ON semantic_tasks") {
				t.Fatalf("dump missing semantic_tasks index: %q", out)
			}
			if !strings.Contains(out, ";\n") {
				t.Fatalf("dump missing SQL statement terminators: %q", out)
			}
		})
	}
}

func TestSchemaDumpInitSQLByProviderIncludesVault(t *testing.T) {
	for _, provider := range []string{"tidb_zero", "tidb_cloud_starter"} {
		t.Run(provider, func(t *testing.T) {
			out := captureSchemaStdout(t, func() {
				if err := runSchemaCommand([]string{"dump-init-sql", "--provider", provider}); err != nil {
					t.Fatalf("dump provider schema: %v", err)
				}
			})

			if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS vault_deks") {
				t.Fatalf("%s dump missing vault_deks: %q", provider, out)
			}
			if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS vault_audit_log") {
				t.Fatalf("%s dump missing vault_audit_log: %q", provider, out)
			}
		})
	}
}

func TestSchemaDumpInitSQLUsesTiDBAutoEmbeddingEnv(t *testing.T) {
	keys := []string{
		tenantschema.EnvTiDBAutoEmbeddingModel,
		tenantschema.EnvTiDBAutoEmbeddingDimensions,
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() {
		restoreEnv(t, restore)
		if err := tenantschema.ConfigureTiDBAutoEmbedding(tenantschema.TiDBAutoEmbeddingConfig{}); err != nil {
			t.Fatalf("reset TiDB auto-embedding config: %v", err)
		}
	})

	tests := []struct {
		name       string
		model      string
		dimensions string
		wantVector string
		wantOption string
	}{
		{
			name:       "openai",
			model:      "openai/text-embedding-3-small",
			dimensions: "1536",
			wantVector: "VECTOR(1536)",
			wantOption: `{"dimensions":1536}`,
		},
		{
			name:       "tidb cloud cohere",
			model:      "tidbcloud_free/cohere/embed-english-v3",
			wantVector: "VECTOR(1024)",
			wantOption: `"input_type@search":"search_query"`,
		},
		{
			name:       "gemini",
			model:      "gemini/gemini-embedding-001",
			dimensions: "256",
			wantVector: "VECTOR(256)",
			wantOption: `{"output_dimensionality":256}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetEnv(t, keys)
			setEnv(t, tenantschema.EnvTiDBAutoEmbeddingModel, tt.model)
			if tt.dimensions != "" {
				setEnv(t, tenantschema.EnvTiDBAutoEmbeddingDimensions, tt.dimensions)
			}
			if err := tenantschema.ConfigureTiDBAutoEmbeddingFromEnv(); err != nil {
				t.Fatalf("configure TiDB auto-embedding from env: %v", err)
			}

			out := captureSchemaStdout(t, func() {
				if err := runSchemaCommand([]string{"dump-init-sql", "--provider", "tidb_cloud_starter"}); err != nil {
					t.Fatalf("dump provider schema: %v", err)
				}
			})

			if !strings.Contains(out, tt.model) {
				t.Fatalf("dump missing configured auto-embedding model: %q", out)
			}
			if !strings.Contains(out, tt.wantVector) {
				t.Fatalf("dump missing configured vector dimensions %s: %q", tt.wantVector, out)
			}
			if !strings.Contains(out, tt.wantOption) {
				t.Fatalf("dump missing configured embedding option %s: %q", tt.wantOption, out)
			}
		})
	}
}

func TestConfigureTiDBAutoEmbeddingFromEnvRejectsUnsupportedDimensions(t *testing.T) {
	keys := []string{
		tenantschema.EnvTiDBAutoEmbeddingModel,
		tenantschema.EnvTiDBAutoEmbeddingDimensions,
	}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() {
		restoreEnv(t, restore)
		if err := tenantschema.ConfigureTiDBAutoEmbedding(tenantschema.TiDBAutoEmbeddingConfig{}); err != nil {
			t.Fatalf("reset TiDB auto-embedding config: %v", err)
		}
	})
	unsetEnv(t, keys)
	setEnv(t, tenantschema.EnvTiDBAutoEmbeddingModel, "openai/text-embedding-3-small")
	setEnv(t, tenantschema.EnvTiDBAutoEmbeddingDimensions, "3072")

	err := tenantschema.ConfigureTiDBAutoEmbeddingFromEnv()
	if err == nil {
		t.Fatal("expected unsupported dimensions to fail")
	}
	if !strings.Contains(err.Error(), tenantschema.EnvTiDBAutoEmbeddingDimensions) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTiDBAutoEmbeddingProviderConfigRequirements(t *testing.T) {
	tests := []struct {
		name    string
		model   string
		apiKey  string
		apiBase string
		wantErr string
	}{
		{
			name:  "tidb cloud hosted requires no credentials",
			model: tenantschema.DefaultTiDBAutoEmbeddingModel,
		},
		{
			name:    "openai requires api key",
			model:   "openai/text-embedding-3-small",
			wantErr: tenantschema.EnvTiDBAutoEmbeddingAPIKey,
		},
		{
			name:    "openai accepts api key and api base for azure endpoint",
			model:   "openai/text-embedding-3-small",
			apiKey:  "sk-test",
			apiBase: "https://example.openai.azure.com",
		},
		{
			name:   "openai accepts api key only",
			model:  "openai/text-embedding-3-small",
			apiKey: "sk-test",
		},
		{
			name:    "azure openai requires api base",
			model:   "azure_openai/text-embedding-3-small",
			apiKey:  "sk-test",
			wantErr: tenantschema.EnvTiDBAutoEmbeddingAPIBase,
		},
		{
			name:    "azure openai accepts api key and api base",
			model:   "azure_openai/text-embedding-3-small",
			apiKey:  "sk-test",
			apiBase: "https://example.openai.azure.com",
		},
		{
			name:    "cohere byok requires api key",
			model:   "cohere/embed-v4.0",
			wantErr: tenantschema.EnvTiDBAutoEmbeddingAPIKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tenantschema.ValidateTiDBAutoEmbeddingProviderConfig(tenantschema.TiDBAutoEmbeddingProviderConfig{
				Model:   tt.model,
				APIKey:  tt.apiKey,
				APIBase: tt.apiBase,
			})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTiDBAutoEmbeddingProviderConfig: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestTiDBAutoEmbeddingSchemaVersionUsesTenantProfile(t *testing.T) {
	defaultProfile, err := tenantschema.TiDBAutoEmbeddingProfileFromConfig(tenantschema.TiDBAutoEmbeddingConfig{
		Model:      tenantschema.DefaultTiDBAutoEmbeddingModel,
		Dimensions: tenantschema.DefaultTiDBAutoEmbeddingDimensions,
	})
	if err != nil {
		t.Fatal(err)
	}
	defaultProfileVersion, err := tenantschema.TiDBTenantSchemaVersionForAutoEmbeddingProfile(defaultProfile)
	if err != nil {
		t.Fatal(err)
	}
	if defaultProfileVersion != tenantschema.CurrentTiDBTenantSchemaVersion {
		t.Fatalf("default profile version = %d, want current default version %d", defaultProfileVersion, tenantschema.CurrentTiDBTenantSchemaVersion)
	}

	openAIProfile, err := tenantschema.TiDBAutoEmbeddingProfileFromConfig(tenantschema.TiDBAutoEmbeddingConfig{
		Model:      "openai/text-embedding-3-small",
		Dimensions: 1536,
	})
	if err != nil {
		t.Fatal(err)
	}
	openAIVersion, err := tenantschema.TiDBTenantSchemaVersionForAutoEmbeddingProfile(openAIProfile)
	if err != nil {
		t.Fatal(err)
	}
	if openAIVersion == defaultProfileVersion {
		t.Fatalf("profile-specific model/options did not change schema version: %d", openAIVersion)
	}
}

func TestSchemaDumpInitSQLRequiresProvider(t *testing.T) {
	err := runSchemaCommand([]string{"dump-init-sql"})
	if err == nil {
		t.Fatal("expected missing provider to fail")
	}
	if !strings.Contains(err.Error(), "--provider") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func captureSchemaStdout(t *testing.T, fn func()) string {
	t.Helper()

	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}

	os.Stdout = writer
	t.Cleanup(func() {
		os.Stdout = originalStdout
	})

	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, reader)
		done <- buf.String()
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	data := <-done
	if err := reader.Close(); err != nil {
		t.Fatalf("close stdout reader: %v", err)
	}
	return data
}
