package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/internal/schemaspec"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/mysqlutil"
	"go.uber.org/zap"
)

type TiDBEmbeddingMode string

const (
	TiDBEmbeddingModeUnknown TiDBEmbeddingMode = "unknown"
	TiDBEmbeddingModeAuto    TiDBEmbeddingMode = "auto-embedding"
	TiDBEmbeddingModeApp     TiDBEmbeddingMode = "app-managed"
)

// Reference of Auto Embedding in TiDB Cloud: https://docs.pingcap.com/ai/vector-search-auto-embedding-amazon-titan/
const (
	// DefaultTiDBAutoEmbeddingModel is the TiDB Cloud hosted free embedding
	// model used when no environment override is configured.
	DefaultTiDBAutoEmbeddingModel = "tidbcloud_free/amazon/titan-embed-text-v2"
	// DefaultTiDBAutoEmbeddingDimensions is the vector dimension for the
	// default TiDB Cloud hosted free embedding model.
	DefaultTiDBAutoEmbeddingDimensions = 1024

	// EnvTiDBAutoEmbeddingModel overrides the model used by TiDB EMBED_TEXT
	// generated columns in database-managed auto-embedding mode.
	EnvTiDBAutoEmbeddingModel = "DRIVE9_TIDB_AUTO_EMBEDDING_MODEL"
	// EnvTiDBAutoEmbeddingDimensions overrides VECTOR(n) and the provider
	// options for database-managed auto-embedding mode.
	EnvTiDBAutoEmbeddingDimensions = "DRIVE9_TIDB_AUTO_EMBEDDING_DIMENSIONS"
	EnvTiDBAutoEmbeddingAPIKey     = "DRIVE9_TIDB_AUTO_EMBEDDING_API_KEY"
	EnvTiDBAutoEmbeddingAPIBase    = "DRIVE9_TIDB_AUTO_EMBEDDING_API_BASE"
)

// TiDBAutoEmbeddingConfig describes the model contract baked into TiDB
// generated vector columns.
type TiDBAutoEmbeddingConfig struct {
	Model      string
	Dimensions int
}

type TiDBAutoEmbeddingProfile struct {
	Model       string
	Dimensions  int
	OptionsJSON string
}

type TiDBAutoEmbeddingProviderConfig struct {
	Model   string
	APIKey  string
	APIBase string
}

type TiDBAutoEmbeddingProviderRequirements struct {
	APIKeyRequired  bool
	APIBaseRequired bool
	APIBaseAllowed  bool
}

var (
	tidbAutoEmbeddingModel       = DefaultTiDBAutoEmbeddingModel
	TiDBAutoEmbeddingDimensions  = DefaultTiDBAutoEmbeddingDimensions
	tidbAutoEmbeddingOptionsJSON = mustTiDBAutoEmbeddingOptionsJSON(TiDBAutoEmbeddingConfig{
		Model:      DefaultTiDBAutoEmbeddingModel,
		Dimensions: DefaultTiDBAutoEmbeddingDimensions,
	})
)

// CurrentTiDBTenantSchemaVersion is derived automatically from the content of
// the tenant auto-embedding init SQL statements. It changes whenever any
// statement in the Go source changes, so callers never have to maintain a
// manual counter.
//
// Tenants recorded with schema_version == CurrentTiDBTenantSchemaVersion in
// the meta store are skipped by EnsureTiDBSchemaForMode entirely.
//
// NOTE: this hash captures only changes to our Go-side SQL definitions, NOT
// changes to the TiDB server version.  Upgrading TiDB itself does not change
// the hash; existing tenant schemas therefore continue to be skipped
// correctly, because a TiDB version upgrade does not alter the user table
// structure that our init SQL created.  If a TiDB upgrade ever requires
// re-applying our schema (e.g., a required migration for a new major version),
// update any statement in tidbAutoEmbeddingSchemaStatements() to force a hash
// change and trigger a one-time re-Ensure for all tenants.
var CurrentTiDBTenantSchemaVersion = currentTiDBTenantSchemaVersion(tidbAutoEmbeddingSchemaStatements())

type tidbAutoEmbeddingRenderConfig struct {
	model       string
	dimensions  int
	optionsJSON string
}

type tidbAutoEmbeddingProviderSetting struct {
	apiKeyVariable      string
	apiBaseVariable     string
	clearAPIBaseIfEmpty bool
}

// ConfigureTiDBAutoEmbeddingFromEnv applies the process environment overrides
// before tenant schema DDL, schema dump, and schema version checks run.
func ConfigureTiDBAutoEmbeddingFromEnv() error {
	cfg, err := TiDBAutoEmbeddingConfigFromEnv()
	if err != nil {
		return err
	}
	return ConfigureTiDBAutoEmbedding(cfg)
}

// TiDBAutoEmbeddingConfigFromEnv returns the normalized TiDB auto-embedding
// configuration from environment variables without mutating package globals.
func TiDBAutoEmbeddingConfigFromEnv() (TiDBAutoEmbeddingConfig, error) {
	cfg := TiDBAutoEmbeddingConfig{
		Model: DefaultTiDBAutoEmbeddingModel,
	}
	if raw := strings.TrimSpace(os.Getenv(EnvTiDBAutoEmbeddingModel)); raw != "" {
		cfg.Model = raw
	}

	if raw := strings.TrimSpace(os.Getenv(EnvTiDBAutoEmbeddingDimensions)); raw != "" {
		dimensions, err := strconv.Atoi(raw)
		if err != nil {
			return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s must be an integer: %w", EnvTiDBAutoEmbeddingDimensions, err)
		}
		cfg.Dimensions = dimensions
	}
	return normalizeTiDBAutoEmbeddingConfig(cfg)
}

// ConfigureTiDBAutoEmbedding replaces the process-wide TiDB auto-embedding
// contract and recomputes the derived tenant schema version.
func ConfigureTiDBAutoEmbedding(cfg TiDBAutoEmbeddingConfig) error {
	normalized, err := normalizeTiDBAutoEmbeddingConfig(cfg)
	if err != nil {
		return err
	}
	tidbAutoEmbeddingModel = normalized.Model
	TiDBAutoEmbeddingDimensions = normalized.Dimensions
	render, err := tidbAutoEmbeddingRenderConfigFor(normalized)
	if err != nil {
		return err
	}
	tidbAutoEmbeddingOptionsJSON = render.optionsJSON
	CurrentTiDBTenantSchemaVersion = currentTiDBTenantSchemaVersion(tidbAutoEmbeddingSchemaStatements())
	return nil
}

// CurrentTiDBAutoEmbeddingConfig returns the active process-wide TiDB
// auto-embedding contract.
func CurrentTiDBAutoEmbeddingConfig() TiDBAutoEmbeddingConfig {
	return TiDBAutoEmbeddingConfig{
		Model:      tidbAutoEmbeddingModel,
		Dimensions: TiDBAutoEmbeddingDimensions,
	}
}

func TiDBAutoEmbeddingProfileFromConfig(cfg TiDBAutoEmbeddingConfig) (TiDBAutoEmbeddingProfile, error) {
	render, err := tidbAutoEmbeddingRenderConfigFor(cfg)
	if err != nil {
		return TiDBAutoEmbeddingProfile{}, err
	}
	return TiDBAutoEmbeddingProfile{
		Model:       render.model,
		Dimensions:  render.dimensions,
		OptionsJSON: render.optionsJSON,
	}, nil
}

func TiDBAutoEmbeddingConfigFromProfile(profile TiDBAutoEmbeddingProfile) (TiDBAutoEmbeddingConfig, error) {
	render, err := tidbAutoEmbeddingRenderConfigForProfile(profile)
	if err != nil {
		return TiDBAutoEmbeddingConfig{}, err
	}
	return TiDBAutoEmbeddingConfig{
		Model:      render.model,
		Dimensions: render.dimensions,
	}, nil
}

type tidbAutoEmbeddingModelSpec struct {
	defaultDimensions int
	allowedDimensions []int
	minDimensions     int
	maxDimensions     int
	// dimensionOption is the provider-specific additional_json_options key
	// used to request a non-default output dimension. It is intentionally not
	// normalized to "dimensions": TiDB provider docs use different keys, for
	// example Gemini uses output_dimensionality and Cohere uses output_dimension.
	dimensionOption string
	// baseOptions contains provider-required options that are not exposed as
	// top-level env vars. TiDB Cloud hosted Cohere requires insert/search input
	// type options for generated document columns and search expressions.
	baseOptions map[string]any
}

func normalizeTiDBAutoEmbeddingConfig(cfg TiDBAutoEmbeddingConfig) (TiDBAutoEmbeddingConfig, error) {
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = DefaultTiDBAutoEmbeddingModel
	}
	if err := validateTiDBAutoEmbeddingModelName(cfg.Model); err != nil {
		return TiDBAutoEmbeddingConfig{}, err
	}
	spec, ok := tidbAutoEmbeddingModelSpecFor(cfg.Model)
	if !ok {
		return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s %q is unsupported", EnvTiDBAutoEmbeddingModel, cfg.Model)
	}
	if cfg.Dimensions == 0 {
		if spec.defaultDimensions == 0 {
			return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s is required for %s=%q", EnvTiDBAutoEmbeddingDimensions, EnvTiDBAutoEmbeddingModel, cfg.Model)
		}
		cfg.Dimensions = spec.defaultDimensions
	}
	if cfg.Dimensions <= 0 {
		return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s must be positive", EnvTiDBAutoEmbeddingDimensions)
	}
	if spec.dimensionOption == "" && spec.defaultDimensions > 0 && cfg.Dimensions != spec.defaultDimensions {
		return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s=%d is unsupported for fixed-dimension %s=%q", EnvTiDBAutoEmbeddingDimensions, cfg.Dimensions, EnvTiDBAutoEmbeddingModel, cfg.Model)
	}
	if len(spec.allowedDimensions) > 0 {
		for _, allowed := range spec.allowedDimensions {
			if cfg.Dimensions == allowed {
				return cfg, nil
			}
		}
		return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s=%d is unsupported for %s=%q", EnvTiDBAutoEmbeddingDimensions, cfg.Dimensions, EnvTiDBAutoEmbeddingModel, cfg.Model)
	}
	if spec.minDimensions > 0 && cfg.Dimensions < spec.minDimensions {
		return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s=%d is below the supported minimum %d for %s=%q", EnvTiDBAutoEmbeddingDimensions, cfg.Dimensions, spec.minDimensions, EnvTiDBAutoEmbeddingModel, cfg.Model)
	}
	if spec.maxDimensions > 0 && cfg.Dimensions > spec.maxDimensions {
		return TiDBAutoEmbeddingConfig{}, fmt.Errorf("%s=%d is above the supported maximum %d for %s=%q", EnvTiDBAutoEmbeddingDimensions, cfg.Dimensions, spec.maxDimensions, EnvTiDBAutoEmbeddingModel, cfg.Model)
	}
	return cfg, nil
}

func validateTiDBAutoEmbeddingModelName(model string) error {
	for _, r := range model {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			continue
		}
		switch r {
		case '/', '-', '_', '.', ':':
			continue
		default:
			return fmt.Errorf("%s contains unsupported character %q", EnvTiDBAutoEmbeddingModel, r)
		}
	}
	return nil
}

// tidbAutoEmbeddingModelSpecFor tracks the model names, dimensions, and
// additional_json_options keys documented by PingCAP TiDB Cloud Auto Embedding.
//
// Keep this list aligned with:
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-overview/
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-amazon-titan/
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-cohere/
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-openai/
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-jina-ai/
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-gemini/
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-hugging-face/
//   - https://docs.pingcap.com/ai/vector-search-auto-embedding-nvidia-nim/
//
// Unknown BYOK models are accepted for supported provider prefixes, but callers
// must supply dimensions explicitly so the VECTOR(n) schema matches the model.
func tidbAutoEmbeddingModelSpecFor(model string) (tidbAutoEmbeddingModelSpec, bool) {
	switch model {
	case DefaultTiDBAutoEmbeddingModel:
		return tidbAutoEmbeddingModelSpec{defaultDimensions: DefaultTiDBAutoEmbeddingDimensions, allowedDimensions: []int{256, 512, 1024}, dimensionOption: "dimensions"}, true
	case "tidbcloud_free/cohere/embed-english-v3", "tidbcloud_free/cohere/embed-multilingual-v3":
		return tidbAutoEmbeddingModelSpec{
			defaultDimensions: 1024,
			baseOptions: map[string]any{
				"input_type":        "search_document",
				"input_type@search": "search_query",
			},
		}, true
	case "cohere/embed-v4.0":
		return tidbAutoEmbeddingModelSpec{
			defaultDimensions: 1536,
			allowedDimensions: []int{256, 512, 1024, 1536},
			dimensionOption:   "output_dimension",
		}, true
	case "openai/text-embedding-3-small":
		return tidbAutoEmbeddingModelSpec{
			defaultDimensions: 1536,
			minDimensions:     512,
			maxDimensions:     1536,
			dimensionOption:   "dimensions",
		}, true
	case "openai/text-embedding-3-large":
		return tidbAutoEmbeddingModelSpec{
			defaultDimensions: 3072,
			minDimensions:     256,
			maxDimensions:     3072,
			dimensionOption:   "dimensions",
		}, true
	case "jina_ai/jina-embeddings-v4":
		return tidbAutoEmbeddingModelSpec{defaultDimensions: 2048, dimensionOption: "dimensions"}, true
	case "jina_ai/jina-embeddings-v3":
		return tidbAutoEmbeddingModelSpec{defaultDimensions: 1024, dimensionOption: "dimensions"}, true
	case "gemini/gemini-embedding-001":
		return tidbAutoEmbeddingModelSpec{
			defaultDimensions: 3072,
			minDimensions:     128,
			maxDimensions:     3072,
			dimensionOption:   "output_dimensionality",
		}, true
	case "huggingface/intfloat/multilingual-e5-large", "huggingface/BAAI/bge-m3", "huggingface/Qwen/Qwen3-Embedding-0.6B":
		return tidbAutoEmbeddingModelSpec{defaultDimensions: 1024}, true
	case "huggingface/sentence-transformers/all-MiniLM-L6-v2":
		return tidbAutoEmbeddingModelSpec{defaultDimensions: 384}, true
	case "huggingface/sentence-transformers/all-mpnet-base-v2":
		return tidbAutoEmbeddingModelSpec{defaultDimensions: 768}, true
	case "nvidia_nim/baai/bge-m3":
		return tidbAutoEmbeddingModelSpec{defaultDimensions: 1024}, true
	case "nvidia/nv-embed-v1":
		return tidbAutoEmbeddingModelSpec{defaultDimensions: 4096}, true
	default:
		if strings.HasPrefix(model, "openai/") {
			return tidbAutoEmbeddingModelSpec{dimensionOption: "dimensions"}, true
		}
		if strings.HasPrefix(model, "azure_openai/") {
			return tidbAutoEmbeddingModelSpec{dimensionOption: "dimensions"}, true
		}
		if strings.HasPrefix(model, "cohere/") {
			return tidbAutoEmbeddingModelSpec{dimensionOption: "output_dimension"}, true
		}
		if strings.HasPrefix(model, "jina_ai/") {
			return tidbAutoEmbeddingModelSpec{dimensionOption: "dimensions"}, true
		}
		if strings.HasPrefix(model, "gemini/") {
			return tidbAutoEmbeddingModelSpec{dimensionOption: "output_dimensionality"}, true
		}
		if strings.HasPrefix(model, "huggingface/") {
			return tidbAutoEmbeddingModelSpec{}, true
		}
		if strings.HasPrefix(model, "nvidia_nim/") || strings.HasPrefix(model, "nvidia/") {
			return tidbAutoEmbeddingModelSpec{}, true
		}
		return tidbAutoEmbeddingModelSpec{}, false
	}
}

func TiDBAutoEmbeddingProviderRequirementsForModel(model string) (TiDBAutoEmbeddingProviderRequirements, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		model = DefaultTiDBAutoEmbeddingModel
	}
	if _, ok := tidbAutoEmbeddingModelSpecFor(model); !ok {
		return TiDBAutoEmbeddingProviderRequirements{}, fmt.Errorf("%s %q is unsupported", EnvTiDBAutoEmbeddingModel, model)
	}
	switch {
	case strings.HasPrefix(model, "tidbcloud_free/"):
		return TiDBAutoEmbeddingProviderRequirements{}, nil
	case strings.HasPrefix(model, "azure_openai/"):
		return TiDBAutoEmbeddingProviderRequirements{APIKeyRequired: true, APIBaseRequired: true, APIBaseAllowed: true}, nil
	case strings.HasPrefix(model, "openai/"):
		return TiDBAutoEmbeddingProviderRequirements{APIKeyRequired: true, APIBaseAllowed: true}, nil
	case strings.HasPrefix(model, "cohere/"),
		strings.HasPrefix(model, "jina_ai/"),
		strings.HasPrefix(model, "gemini/"),
		strings.HasPrefix(model, "huggingface/"),
		strings.HasPrefix(model, "nvidia_nim/"),
		strings.HasPrefix(model, "nvidia/"):
		return TiDBAutoEmbeddingProviderRequirements{APIKeyRequired: true}, nil
	default:
		return TiDBAutoEmbeddingProviderRequirements{}, nil
	}
}

func ValidateTiDBAutoEmbeddingProviderConfig(cfg TiDBAutoEmbeddingProviderConfig) error {
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = DefaultTiDBAutoEmbeddingModel
	}
	requirements, err := TiDBAutoEmbeddingProviderRequirementsForModel(model)
	if err != nil {
		return err
	}
	apiKey := strings.TrimSpace(cfg.APIKey)
	apiBase := strings.TrimSpace(cfg.APIBase)
	if requirements.APIKeyRequired && apiKey == "" {
		return fmt.Errorf("%s is required for %s=%q", EnvTiDBAutoEmbeddingAPIKey, EnvTiDBAutoEmbeddingModel, model)
	}
	if requirements.APIBaseRequired && apiBase == "" {
		return fmt.Errorf("%s is required for %s=%q", EnvTiDBAutoEmbeddingAPIBase, EnvTiDBAutoEmbeddingModel, model)
	}
	if !requirements.APIBaseAllowed && apiBase != "" {
		return fmt.Errorf("%s is unsupported for %s=%q", EnvTiDBAutoEmbeddingAPIBase, EnvTiDBAutoEmbeddingModel, model)
	}
	return nil
}

func ApplyTiDBAutoEmbeddingProviderConfig(ctx context.Context, db *sql.DB, cfg TiDBAutoEmbeddingProviderConfig) error {
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = DefaultTiDBAutoEmbeddingModel
	}
	if err := ValidateTiDBAutoEmbeddingProviderConfig(TiDBAutoEmbeddingProviderConfig{
		Model:   model,
		APIKey:  cfg.APIKey,
		APIBase: cfg.APIBase,
	}); err != nil {
		return err
	}
	setting, ok, err := tidbAutoEmbeddingProviderSettingForModel(model)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if setting.apiKeyVariable != "" {
		if _, err := db.ExecContext(ctx, "SET @@GLOBAL."+setting.apiKeyVariable+" = ?", strings.TrimSpace(cfg.APIKey)); err != nil {
			return fmt.Errorf("set TiDB auto-embedding API key for %s: %w", model, err)
		}
	}
	apiBase := strings.TrimSpace(cfg.APIBase)
	if setting.apiBaseVariable != "" && (apiBase != "" || setting.clearAPIBaseIfEmpty) {
		if _, err := db.ExecContext(ctx, "SET @@GLOBAL."+setting.apiBaseVariable+" = ?", apiBase); err != nil {
			return fmt.Errorf("set TiDB auto-embedding API base for %s: %w", model, err)
		}
	}
	return nil
}

func tidbAutoEmbeddingProviderSettingForModel(model string) (tidbAutoEmbeddingProviderSetting, bool, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		model = DefaultTiDBAutoEmbeddingModel
	}
	if _, ok := tidbAutoEmbeddingModelSpecFor(model); !ok {
		return tidbAutoEmbeddingProviderSetting{}, false, fmt.Errorf("%s %q is unsupported", EnvTiDBAutoEmbeddingModel, model)
	}
	switch {
	case strings.HasPrefix(model, "tidbcloud_free/"):
		return tidbAutoEmbeddingProviderSetting{}, false, nil
	case strings.HasPrefix(model, "azure_openai/"):
		return tidbAutoEmbeddingProviderSetting{
			apiKeyVariable:  "TIDB_EXP_EMBED_OPENAI_API_KEY",
			apiBaseVariable: "TIDB_EXP_EMBED_OPENAI_API_BASE",
		}, true, nil
	case strings.HasPrefix(model, "openai/"):
		return tidbAutoEmbeddingProviderSetting{
			apiKeyVariable:      "TIDB_EXP_EMBED_OPENAI_API_KEY",
			apiBaseVariable:     "TIDB_EXP_EMBED_OPENAI_API_BASE",
			clearAPIBaseIfEmpty: true,
		}, true, nil
	case strings.HasPrefix(model, "cohere/"):
		return tidbAutoEmbeddingProviderSetting{apiKeyVariable: "TIDB_EXP_EMBED_COHERE_API_KEY"}, true, nil
	case strings.HasPrefix(model, "jina_ai/"):
		return tidbAutoEmbeddingProviderSetting{apiKeyVariable: "TIDB_EXP_EMBED_JINA_AI_API_KEY"}, true, nil
	case strings.HasPrefix(model, "gemini/"):
		return tidbAutoEmbeddingProviderSetting{apiKeyVariable: "TIDB_EXP_EMBED_GEMINI_API_KEY"}, true, nil
	case strings.HasPrefix(model, "huggingface/"):
		return tidbAutoEmbeddingProviderSetting{apiKeyVariable: "TIDB_EXP_EMBED_HUGGINGFACE_API_KEY"}, true, nil
	case strings.HasPrefix(model, "nvidia_nim/"), strings.HasPrefix(model, "nvidia/"):
		return tidbAutoEmbeddingProviderSetting{apiKeyVariable: "TIDB_EXP_EMBED_NVIDIA_NIM_API_KEY"}, true, nil
	default:
		return tidbAutoEmbeddingProviderSetting{}, false, nil
	}
}

func mustTiDBAutoEmbeddingOptionsJSON(cfg TiDBAutoEmbeddingConfig) string {
	raw, err := tidbAutoEmbeddingOptionsJSONFor(cfg)
	if err != nil {
		panic(err)
	}
	return raw
}

func tidbAutoEmbeddingOptionsJSONFor(cfg TiDBAutoEmbeddingConfig) (string, error) {
	spec, ok := tidbAutoEmbeddingModelSpecFor(cfg.Model)
	if !ok {
		return "", fmt.Errorf("%s %q is unsupported", EnvTiDBAutoEmbeddingModel, cfg.Model)
	}
	options := make(map[string]any, len(spec.baseOptions)+1)
	for key, value := range spec.baseOptions {
		options[key] = value
	}
	if spec.dimensionOption != "" {
		options[spec.dimensionOption] = cfg.Dimensions
	}
	if len(options) == 0 {
		return "{}", nil
	}
	raw, err := json.Marshal(options)
	if err != nil {
		return "", fmt.Errorf("encode TiDB auto-embedding options: %w", err)
	}
	return string(raw), nil
}

func tidbAutoEmbeddingRenderConfigFor(cfg TiDBAutoEmbeddingConfig) (tidbAutoEmbeddingRenderConfig, error) {
	normalized, err := normalizeTiDBAutoEmbeddingConfig(cfg)
	if err != nil {
		return tidbAutoEmbeddingRenderConfig{}, err
	}
	optionsJSON, err := tidbAutoEmbeddingOptionsJSONFor(normalized)
	if err != nil {
		return tidbAutoEmbeddingRenderConfig{}, err
	}
	return tidbAutoEmbeddingRenderConfig{
		model:       normalized.Model,
		dimensions:  normalized.Dimensions,
		optionsJSON: optionsJSON,
	}, nil
}

func tidbAutoEmbeddingRenderConfigForProfile(profile TiDBAutoEmbeddingProfile) (tidbAutoEmbeddingRenderConfig, error) {
	normalized, err := normalizeTiDBAutoEmbeddingConfig(TiDBAutoEmbeddingConfig{
		Model:      profile.Model,
		Dimensions: profile.Dimensions,
	})
	if err != nil {
		return tidbAutoEmbeddingRenderConfig{}, err
	}
	optionsJSON := strings.TrimSpace(profile.OptionsJSON)
	if optionsJSON == "" {
		optionsJSON, err = tidbAutoEmbeddingOptionsJSONFor(normalized)
		if err != nil {
			return tidbAutoEmbeddingRenderConfig{}, err
		}
	} else {
		var options map[string]any
		if err := json.Unmarshal([]byte(optionsJSON), &options); err != nil {
			return tidbAutoEmbeddingRenderConfig{}, fmt.Errorf("decode TiDB auto-embedding options_json: %w", err)
		}
		if err := validateTiDBAutoEmbeddingProfileOptions(normalized, options); err != nil {
			return tidbAutoEmbeddingRenderConfig{}, err
		}
		canonical, err := json.Marshal(options)
		if err != nil {
			return tidbAutoEmbeddingRenderConfig{}, fmt.Errorf("encode TiDB auto-embedding options_json: %w", err)
		}
		optionsJSON = string(canonical)
	}
	return tidbAutoEmbeddingRenderConfig{
		model:       normalized.Model,
		dimensions:  normalized.Dimensions,
		optionsJSON: optionsJSON,
	}, nil
}

func validateTiDBAutoEmbeddingProfileOptions(cfg TiDBAutoEmbeddingConfig, options map[string]any) error {
	expectedJSON, err := tidbAutoEmbeddingOptionsJSONFor(cfg)
	if err != nil {
		return err
	}
	var expected map[string]any
	if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		return fmt.Errorf("decode expected TiDB auto-embedding options_json: %w", err)
	}
	for key, want := range expected {
		got, ok := options[key]
		if !ok {
			return fmt.Errorf("TiDB auto-embedding options_json missing required key %q for %s=%q", key, EnvTiDBAutoEmbeddingModel, cfg.Model)
		}
		if !equalJSONValue(got, want) {
			return fmt.Errorf("TiDB auto-embedding options_json key %q does not match %s=%q and %s=%d", key, EnvTiDBAutoEmbeddingModel, cfg.Model, EnvTiDBAutoEmbeddingDimensions, cfg.Dimensions)
		}
	}
	return nil
}

func equalJSONValue(a, b any) bool {
	aRaw, aErr := json.Marshal(a)
	bRaw, bErr := json.Marshal(b)
	return aErr == nil && bErr == nil && string(aRaw) == string(bRaw)
}

func currentTiDBAutoEmbeddingRenderConfig() tidbAutoEmbeddingRenderConfig {
	return tidbAutoEmbeddingRenderConfig{
		model:       tidbAutoEmbeddingModel,
		dimensions:  TiDBAutoEmbeddingDimensions,
		optionsJSON: tidbAutoEmbeddingOptionsJSON,
	}
}

func TiDBTenantSchemaVersionForAutoEmbeddingConfig(cfg TiDBAutoEmbeddingConfig) (int, error) {
	render, err := tidbAutoEmbeddingRenderConfigFor(cfg)
	if err != nil {
		return 0, err
	}
	return currentTiDBTenantSchemaVersion(tidbAutoEmbeddingSchemaStatementsForConfig(render)), nil
}

func TiDBTenantSchemaVersionForAutoEmbeddingProfile(profile TiDBAutoEmbeddingProfile) (int, error) {
	render, err := tidbAutoEmbeddingRenderConfigForProfile(profile)
	if err != nil {
		return 0, err
	}
	return currentTiDBTenantSchemaVersion(tidbAutoEmbeddingSchemaStatementsForConfig(render)), nil
}

func currentTiDBTenantSchemaVersion(stmts []string) int {
	// Hash only statements that are parsed into the schema spec (CREATE TABLE,
	// CREATE [UNIQUE] INDEX, ALTER TABLE ... ADD ... INDEX).  Statements that
	// fall into none of these categories (e.g. SET, comments) do not affect
	// what ValidateTiDBSchemaForMode checks, so including them would cause
	// unnecessary re-Ensures on unrelated edits.
	//
	var specStmts []string
	for _, stmt := range stmts {
		n := normalizeSQLFragment(stmt)
		if strings.HasPrefix(n, "create table ") ||
			strings.HasPrefix(n, "create index ") ||
			strings.HasPrefix(n, "create unique index ") ||
			(strings.HasPrefix(n, "alter table ") && strings.Contains(n, " add ") && strings.Contains(n, " index ")) {
			specStmts = append(specStmts, schemaspec.CanonicalStatementForHash(stmt))
		}
	}
	return schemaspec.CRC32Version(specStmts)
}

type tidbColumnMeta struct {
	columnType           string
	extra                string
	generationExpression string
}

type tidbTableMeta struct {
	tableName string
	columns   map[string]tidbColumnMeta
}

type tidbSchemaDiffKind string

const (
	tidbSchemaDiffMissingTable  tidbSchemaDiffKind = "missing_table"
	tidbSchemaDiffMissingColumn tidbSchemaDiffKind = "missing_column"
	tidbSchemaDiffMissingIndex  tidbSchemaDiffKind = "missing_index"
	tidbSchemaDiffColumnType    tidbSchemaDiffKind = "column_type_mismatch"
	tidbSchemaDiffExtraColumn   tidbSchemaDiffKind = "extra_column"
	tidbSchemaDiffTableContract tidbSchemaDiffKind = "table_contract_mismatch"
)

type tidbSchemaDiff struct {
	kind       tidbSchemaDiffKind
	tableName  string
	columnName string
	detail     string
	repairSQL  string
}

type tidbSchemaSpec struct {
	tables []tidbTableSpec
}

type tidbTableSpec struct {
	name            string
	createStatement string
	columns         map[string]tidbColumnSpec
	indexes         map[string]tidbIndexSpec
	primaryKey      tidbPrimaryKeySpec
	validate        func(tidbTableMeta) []tidbSchemaDiff
}

type tidbColumnSpec struct {
	columnType string
	addSQL     string
	modifySQL  string
}

type tidbIndexSpec struct {
	createSQL string
}

type tidbPrimaryKeySpec struct {
	columns []string
}

type tidbUniqueIndexRepair struct {
	tableName string
	indexName string
	columns   []string
}

type tidbSchemaDiffError struct {
	mode  TiDBEmbeddingMode
	diffs []tidbSchemaDiff
}

func (e *tidbSchemaDiffError) Error() string {
	if e == nil || len(e.diffs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(e.diffs))
	for _, diff := range e.diffs {
		parts = append(parts, diff.detail)
	}
	return fmt.Sprintf("tidb schema contract mismatch for mode %q: %s", e.mode, strings.Join(parts, "; "))
}

// Keep this statement list aligned with the externally managed tidb_cloud_starter
// schema. If you change columns, indexes, generated expressions, or
// constraints here, rerun:
//
//	drive9-server schema dump-init-sql --provider tidb_cloud_starter
//
// and update tidb_cloud_starter with the exported SQL.
func tidbAutoEmbeddingSchemaStatements() []string {
	return tidbAutoEmbeddingSchemaStatementsForConfig(currentTiDBAutoEmbeddingRenderConfig())
}

func tidbAutoEmbeddingSchemaStatementsForConfig(cfg tidbAutoEmbeddingRenderConfig) []string {
	modelLiteral := tidbSQLStringLiteral(cfg.model)
	optionsLiteral := tidbSQLStringLiteral(cfg.optionsJSON)
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (
			node_id      VARCHAR(64) PRIMARY KEY,
			path         TEXT NOT NULL,
			path_hash    VARCHAR(64) NOT NULL DEFAULT '',
			parent_path  TEXT NOT NULL,
			parent_path_hash VARCHAR(64) NOT NULL DEFAULT '',
			name         VARCHAR(255) NOT NULL,
			is_directory BOOLEAN NOT NULL DEFAULT FALSE,
			file_id      VARCHAR(64),
			inode_id     VARCHAR(64),
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE UNIQUE INDEX idx_path ON file_nodes(path_hash)`,
		`CREATE INDEX idx_parent ON file_nodes(parent_path_hash, name)`,
		`CREATE INDEX idx_file_id ON file_nodes(file_id)`,
		`CREATE INDEX idx_inode_id ON file_nodes(inode_id)`,

		`CREATE TABLE IF NOT EXISTS inodes (
			inode_id     VARCHAR(64) PRIMARY KEY,
			size_bytes   BIGINT NOT NULL DEFAULT 0,
			revision     BIGINT NOT NULL DEFAULT 1,
			mode         INT UNSIGNED NOT NULL DEFAULT 420,
			status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			mtime        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			confirmed_at DATETIME(3),
			expires_at   DATETIME(3)
		)`,
		`CREATE INDEX idx_inodes_status ON inodes(status, created_at)`,
		`CREATE TABLE IF NOT EXISTS contents (
			inode_id                   VARCHAR(64) PRIMARY KEY,
			storage_type               VARCHAR(32) NOT NULL,
			storage_ref                TEXT NOT NULL,
			storage_ref_hash           VARCHAR(64) NOT NULL DEFAULT '',
			storage_encryption_mode    VARCHAR(16) NOT NULL DEFAULT 'legacy',
			storage_encryption_key_id  VARCHAR(256) NOT NULL DEFAULT '',
			content_blob               LONGBLOB,
			content_type               VARCHAR(255),
			checksum_sha256            VARCHAR(128),
			source_id                  VARCHAR(255)
		)`,
		`CREATE INDEX idx_contents_storage_ref_hash ON contents(storage_ref_hash)`,
		`CREATE TABLE IF NOT EXISTS semantic (
			inode_id                           VARCHAR(64) PRIMARY KEY,
			content_text                       LONGTEXT,
			description                        LONGTEXT,
			embedding                          VECTOR(` + strconv.Itoa(cfg.dimensions) + `) GENERATED ALWAYS AS (EMBED_TEXT(
				` + modelLiteral + `,
				content_text,
				` + optionsLiteral + `
			)) STORED,
			embedding_revision                 BIGINT,
			description_embedding              VECTOR(` + strconv.Itoa(cfg.dimensions) + `) GENERATED ALWAYS AS (EMBED_TEXT(
				` + modelLiteral + `,
				description,
				` + optionsLiteral + `
			)) STORED,
			description_embedding_revision     BIGINT,
			FULLTEXT INDEX idx_semantic_fts_content (content_text) WITH PARSER MULTILINGUAL,
			FULLTEXT INDEX idx_semantic_fts_description (description) WITH PARSER MULTILINGUAL
		)`,
		`ALTER TABLE semantic
			ADD VECTOR INDEX idx_semantic_cosine((VEC_COSINE_DISTANCE(embedding)))
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`ALTER TABLE semantic
			ADD VECTOR INDEX idx_semantic_desc_cosine((VEC_COSINE_DISTANCE(description_embedding)))
			ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		`CREATE TABLE IF NOT EXISTS file_tags (
			file_id   VARCHAR(64) NOT NULL,
			inode_id  VARCHAR(64),
			tag_key   VARCHAR(255) NOT NULL,
			tag_value VARCHAR(255) NOT NULL DEFAULT '',
			PRIMARY KEY (file_id, tag_key)
		)`,
		`CREATE INDEX idx_kv ON file_tags(tag_key, tag_value)`,
		`CREATE TABLE IF NOT EXISTS uploads (
			upload_id          VARCHAR(64) PRIMARY KEY,
			file_id            VARCHAR(64) NOT NULL,
			inode_id           VARCHAR(64),
			target_path        TEXT NOT NULL,
			target_path_hash   VARCHAR(64) NOT NULL DEFAULT '',
			s3_upload_id       VARCHAR(255) NOT NULL,
			s3_key             VARCHAR(2048) NOT NULL,
			storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'none',
			storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '',
			total_size         BIGINT NOT NULL,
			part_size          BIGINT NOT NULL,
			parts_total        INT NOT NULL,
			expected_revision  BIGINT NULL,
			status             VARCHAR(32) NOT NULL DEFAULT 'UPLOADING',
			fingerprint_sha256 VARCHAR(128),
			idempotency_key    VARCHAR(255),
			description        LONGTEXT,
			created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			expires_at         DATETIME(3) NOT NULL,
			active_target_path_hash VARCHAR(64) AS (CASE WHEN status = 'UPLOADING' THEN target_path_hash ELSE NULL END) VIRTUAL
		)`,
		`ALTER TABLE uploads ADD COLUMN expected_revision BIGINT NULL`,
		`CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)`,
		`CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)`,
		`CREATE UNIQUE INDEX idx_uploads_active ON uploads(active_target_path_hash)`,
		`CREATE TABLE IF NOT EXISTS semantic_tasks (
			task_id           VARCHAR(64) PRIMARY KEY,
			task_type         VARCHAR(32) NOT NULL,
			resource_id       VARCHAR(64) NOT NULL,
			resource_version  BIGINT NOT NULL,
			status            VARCHAR(20) NOT NULL,
			attempt_count     INT NOT NULL DEFAULT 0,
			max_attempts      INT NOT NULL DEFAULT 5,
			receipt           VARCHAR(128) NULL,
			leased_at         DATETIME(3) NULL,
			lease_until       DATETIME(3) NULL,
			available_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			payload_json      JSON NULL,
			last_error        TEXT NULL,
			created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at      DATETIME(3) NULL
		)`,
		`CREATE UNIQUE INDEX uk_task_resource_version ON semantic_tasks(task_type, resource_id, resource_version)`,
		`CREATE INDEX idx_task_claim ON semantic_tasks(status, available_at, lease_until, created_at)`,
		`CREATE INDEX idx_task_claim_type ON semantic_tasks(status, task_type, available_at, created_at, task_id)`,
		`CREATE TABLE IF NOT EXISTS file_gc_tasks (
			task_id       VARCHAR(64) PRIMARY KEY,
			file_id       VARCHAR(64) NOT NULL,
			inode_id      VARCHAR(64),
			storage_type  VARCHAR(32) NOT NULL,
			storage_ref   TEXT NOT NULL,
			size_bytes    BIGINT NOT NULL DEFAULT 0,
			content_type  VARCHAR(255) NULL,
			status        VARCHAR(20) NOT NULL,
			attempt_count INT NOT NULL DEFAULT 0,
			max_attempts  INT NOT NULL DEFAULT 0,
			receipt       VARCHAR(128) NULL,
			leased_at     DATETIME(3) NULL,
			lease_until   DATETIME(3) NULL,
			available_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			last_error    TEXT NULL,
			created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at  DATETIME(3) NULL
		)`,
		`CREATE UNIQUE INDEX uk_file_gc_file_id ON file_gc_tasks(file_id)`,
		`CREATE UNIQUE INDEX uk_file_gc_inode_id ON file_gc_tasks(inode_id)`,
		`CREATE INDEX idx_file_gc_claim ON file_gc_tasks(status, available_at, lease_until, created_at)`,
		`CREATE TABLE IF NOT EXISTS llm_usage (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			task_type       VARCHAR(32) NOT NULL,
			task_id         VARCHAR(64) NOT NULL,
			cost_millicents BIGINT NOT NULL DEFAULT 0,
			raw_units       BIGINT NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(16) NOT NULL,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_llm_usage_created ON llm_usage(created_at)`,
		`CREATE TABLE IF NOT EXISTS fs_event_seq (
			id       TINYINT UNSIGNED PRIMARY KEY,
			next_seq BIGINT UNSIGNED NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fs_events (
			seq        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			path       VARCHAR(512) NOT NULL,
			op         VARCHAR(64) NOT NULL,
			actor      VARCHAR(255),
			ts         BIGINT NOT NULL,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_fs_events_created ON fs_events(created_at)`,
	}
	stmts = append(stmts, GitWorkspaceTiDBSchemaStatements()...)
	stmts = append(stmts, FSLayerTiDBSchemaStatements()...)
	stmts = append(stmts, JournalTiDBSchemaStatements()...)
	stmts = append(stmts, VaultTiDBSchemaStatements()...)
	return stmts
}

func tidbSQLStringLiteral(s string) string {
	escaped := strings.NewReplacer(
		`\`, `\\`,
		`'`, `''`,
		"\x00", `\0`,
		"\n", `\n`,
		"\r", `\r`,
		"\t", `\t`,
	).Replace(s)
	return "'" + escaped + "'"
}

// DetectTiDBEmbeddingMode inspects the TiDB embedding contract and reports
// whether the schema is in database-managed auto mode or app-managed mode.
// It checks the files table first (legacy tenants); when the files table does
// not exist, it falls back to the semantic table (new tenants without legacy
// dual-write).
func DetectTiDBEmbeddingMode(db *sql.DB) (TiDBEmbeddingMode, error) {
	ctx := context.Background()
	start := time.Now()
	logger.Info(ctx, "tenant_detect_tidb_embedding_mode_started")
	if db == nil {
		return TiDBEmbeddingModeUnknown, fmt.Errorf("nil db")
	}
	if !IsTiDBCluster(ctx, db) {
		return TiDBEmbeddingModeUnknown, fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	loadStart := time.Now()
	tableName := "files"
	meta, err := loadTiDBTableMeta(ctx, db, tableName)
	if err != nil {
		// Only fall back to semantic when files table is genuinely absent
		// (new tenants without legacy dual-write).  Do not mask transient
		// errors such as connection failures.
		if !isMissingTableError(err) {
			logger.Warn(ctx, "tenant_detect_tidb_embedding_mode_load_files_failed",
				zap.Duration("elapsed", time.Since(loadStart)),
				zap.Duration("total_elapsed", time.Since(start)),
				zap.Error(err))
			return TiDBEmbeddingModeUnknown, fmt.Errorf("load files table metadata: %w", err)
		}
		semanticMeta, semErr := loadTiDBTableMeta(ctx, db, "semantic")
		if semErr != nil {
			logger.Warn(ctx, "tenant_detect_tidb_embedding_mode_load_both_failed",
				zap.Duration("elapsed", time.Since(loadStart)),
				zap.Duration("total_elapsed", time.Since(start)),
				zap.Error(err),
				zap.NamedError("semantic_err", semErr))
			return TiDBEmbeddingModeUnknown, fmt.Errorf("load table metadata: files: %w; semantic: %w", err, semErr)
		}
		meta = semanticMeta
		tableName = "semantic"
	}
	logger.Info(ctx, "tenant_detect_tidb_embedding_mode_loaded_table",
		zap.String("table", tableName),
		zap.Duration("elapsed", time.Since(loadStart)),
		zap.Duration("total_elapsed", time.Since(start)))
	detectStart := time.Now()
	mode, err := detectTiDBEmbeddingModeFromMeta(meta)
	if err != nil {
		logger.Warn(ctx, "tenant_detect_tidb_embedding_mode_failed",
			zap.Duration("elapsed", time.Since(detectStart)),
			zap.Duration("total_elapsed", time.Since(start)),
			zap.Error(err))
		return TiDBEmbeddingModeUnknown, fmt.Errorf("embedding schema contract: %w", err)
	}
	logger.Info(ctx, "tenant_detect_tidb_embedding_mode_finished",
		zap.String("mode", string(mode)),
		zap.Duration("elapsed", time.Since(detectStart)),
		zap.Duration("total_elapsed", time.Since(start)))
	return mode, nil
}

// ValidateTiDBSchemaForMode validates that an already-open TiDB connection
// matches exactly one supported dat9 embedding contract for the requested mode.
func ValidateTiDBSchemaForMode(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode) error {
	return validateTiDBSchemaForModeWithConfig(ctx, db, mode, currentTiDBAutoEmbeddingRenderConfig())
}

func ValidateTiDBSchemaForAutoEmbeddingConfig(ctx context.Context, db *sql.DB, cfg TiDBAutoEmbeddingConfig) error {
	render, err := tidbAutoEmbeddingRenderConfigFor(cfg)
	if err != nil {
		return err
	}
	return validateTiDBSchemaForModeWithConfig(ctx, db, TiDBEmbeddingModeAuto, render)
}

func ValidateTiDBSchemaForAutoEmbeddingProfile(ctx context.Context, db *sql.DB, profile TiDBAutoEmbeddingProfile) error {
	render, err := tidbAutoEmbeddingRenderConfigForProfile(profile)
	if err != nil {
		return err
	}
	return validateTiDBSchemaForModeWithConfig(ctx, db, TiDBEmbeddingModeAuto, render)
}

func validateTiDBSchemaForModeWithConfig(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) error {
	start := time.Now()
	logger.Info(ctx, "tenant_tidb_schema_validate_started",
		zap.String("mode", string(mode)))
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !IsTiDBCluster(ctx, db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := validateTiDBSchemaMode(mode); err != nil {
		return err
	}
	diffs, err := diffTiDBSchemaForModeWithConfig(ctx, db, mode, cfg)
	if err != nil {
		return err
	}
	if len(diffs) > 0 {
		logger.Warn(ctx, "tenant_tidb_schema_validate_failed",
			zap.String("mode", string(mode)),
			zap.Int("diff_count", len(diffs)),
			zap.Strings("diffs", summarizeTiDBSchemaDiffs(diffs)),
			zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
		return &tidbSchemaDiffError{mode: mode, diffs: diffs}
	}
	logger.Info(ctx, "tenant_tidb_schema_validate_finished",
		zap.String("mode", string(mode)),
		zap.Int("diff_count", 0),
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return nil
}

// EnsureTiDBSchemaForMode repairs known launch-schema drift that can be fixed
// in place, then validates the full TiDB schema contract for the requested mode.
func EnsureTiDBSchemaForMode(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode) error {
	return ensureTiDBSchemaForModeWithConfig(ctx, db, mode, currentTiDBAutoEmbeddingRenderConfig())
}

func EnsureTiDBSchemaForAutoEmbeddingConfig(ctx context.Context, db *sql.DB, cfg TiDBAutoEmbeddingConfig) error {
	render, err := tidbAutoEmbeddingRenderConfigFor(cfg)
	if err != nil {
		return err
	}
	return ensureTiDBSchemaForModeWithConfig(ctx, db, TiDBEmbeddingModeAuto, render)
}

func EnsureTiDBSchemaForAutoEmbeddingProfile(ctx context.Context, db *sql.DB, profile TiDBAutoEmbeddingProfile) error {
	render, err := tidbAutoEmbeddingRenderConfigForProfile(profile)
	if err != nil {
		return err
	}
	return ensureTiDBSchemaForModeWithConfig(ctx, db, TiDBEmbeddingModeAuto, render)
}

func ensureTiDBSchemaForModeWithConfig(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) error {
	start := time.Now()
	attemptedPasses := 0
	logger.Info(ctx, "tenant_tidb_schema_ensure_started",
		zap.String("mode", string(mode)))
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !IsTiDBCluster(ctx, db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := validateTiDBSchemaMode(mode); err != nil {
		return err
	}
	// Path-hash migrations from the legacy schema can need separate passes for
	// adding hash columns, adding the active-upload generated hash column,
	// rebuilding old same-name indexes, and finally widening path columns.
	const maxRepairPasses = 6
	for i := 0; i < maxRepairPasses; i++ {
		attemptedPasses = i + 1
		passStart := time.Now()
		diffs, err := diffTiDBSchemaForModeWithConfig(ctx, db, mode, cfg)
		if err != nil {
			return err
		}
		logger.Info(ctx, "tenant_tidb_schema_ensure_diff_pass_finished",
			zap.String("mode", string(mode)),
			zap.Int("repair_pass", i+1),
			zap.Int("max_repair_passes", maxRepairPasses),
			zap.Int("diff_count", len(diffs)),
			zap.Strings("diffs", summarizeTiDBSchemaDiffs(diffs)),
			zap.Float64("duration_ms", float64(time.Since(passStart).Microseconds())/1000.0))
		if err := BackfillPathHashes(ctx, db); err != nil {
			return fmt.Errorf("backfill path hashes before repair: %w", err)
		}
		if len(diffs) == 0 {
			if err := BackfillStorageRefHashes(ctx, db); err != nil {
				return fmt.Errorf("backfill storage_ref_hash: %w", err)
			}
			logger.Info(ctx, "tenant_tidb_schema_ensure_finished",
				zap.String("mode", string(mode)),
				zap.Int("repair_passes", attemptedPasses),
				zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
			return nil
		}
		repairs := plannedTiDBSchemaRepairs(diffs)
		if len(repairs) == 0 {
			logger.Warn(ctx, "tenant_tidb_schema_ensure_no_safe_repairs",
				zap.String("mode", string(mode)),
				zap.Int("repair_pass", i+1),
				zap.Strings("diffs", summarizeTiDBSchemaDiffs(diffs)))
			// Drift remains but nothing in it is safe to repair automatically.
			break
		}
		logger.Info(ctx, "tenant_tidb_schema_ensure_repair_pass_started",
			zap.String("mode", string(mode)),
			zap.Int("repair_pass", i+1),
			zap.Int("repair_count", len(repairs)),
			zap.Strings("repairs", summarizeSchemaStatements(repairs)))
		if err := applyTiDBSchemaRepairs(ctx, db, repairs); err != nil {
			return err
		}
		if err := BackfillPathHashes(ctx, db); err != nil {
			return fmt.Errorf("backfill path hashes after repair: %w", err)
		}
	}
	if err := validateTiDBSchemaForModeWithConfig(ctx, db, mode, cfg); err != nil {
		return err
	}
	if err := BackfillStorageRefHashes(ctx, db); err != nil {
		return fmt.Errorf("backfill storage_ref_hash: %w", err)
	}
	if err := BackfillPathHashes(ctx, db); err != nil {
		return fmt.Errorf("backfill path hashes: %w", err)
	}
	logger.Info(ctx, "tenant_tidb_schema_ensure_finished",
		zap.String("mode", string(mode)),
		zap.Int("repair_passes", attemptedPasses),
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return nil
}

func validateTiDBSchemaMode(mode TiDBEmbeddingMode) error {
	if mode != TiDBEmbeddingModeAuto && mode != TiDBEmbeddingModeApp {
		return fmt.Errorf("unsupported TiDB embedding mode %q", mode)
	}
	return nil
}

// BackfillStorageRefHashes populates storage_ref_hash for pre-existing storage rows.
func BackfillStorageRefHashes(ctx context.Context, db *sql.DB) error {
	// Legacy tenants: backfill the files table if it exists.
	if _, err := db.ExecContext(ctx, `UPDATE files
		SET storage_ref_hash = LOWER(SHA2(storage_ref, 256))
		WHERE storage_ref_hash = '' AND storage_ref <> ''`); err != nil {
		if !isMissingTableError(err) {
			return err
		}
	}
	_, err := db.ExecContext(ctx, `UPDATE contents
		SET storage_ref_hash = LOWER(SHA2(storage_ref, 256))
		WHERE storage_ref_hash = '' AND storage_ref <> ''`)
	return err
}

// BackfillPathHashes populates path hash columns used to index POSIX-length
// paths without relying on oversized VARCHAR indexes.
func BackfillPathHashes(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `UPDATE file_nodes
		SET path_hash = LOWER(SHA2(path, 256))
		WHERE path_hash = '' AND path <> ''`); err != nil {
		if !isMissingTableError(err) && !isMissingColumnError(err) {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `UPDATE file_nodes
		SET parent_path_hash = LOWER(SHA2(parent_path, 256))
		WHERE parent_path_hash = '' AND parent_path <> ''`); err != nil {
		if !isMissingTableError(err) && !isMissingColumnError(err) {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `UPDATE uploads
		SET target_path_hash = LOWER(SHA2(target_path, 256))
		WHERE target_path_hash = '' AND target_path <> ''`); err != nil {
		if !isMissingTableError(err) && !isMissingColumnError(err) {
			return err
		}
	}
	return nil
}

func initTiDBAutoEmbeddingSchema(ctx context.Context, dsn string) error {
	return initTiDBAutoEmbeddingSchemaWithConfig(ctx, dsn, CurrentTiDBAutoEmbeddingConfig())
}

func initTiDBAutoEmbeddingSchemaWithConfig(ctx context.Context, dsn string, cfg TiDBAutoEmbeddingConfig) error {
	profile, err := TiDBAutoEmbeddingProfileFromConfig(cfg)
	if err != nil {
		return err
	}
	return initTiDBAutoEmbeddingSchemaWithProfile(ctx, dsn, profile)
}

func initTiDBAutoEmbeddingSchemaWithProfile(ctx context.Context, dsn string, profile TiDBAutoEmbeddingProfile) error {
	start := time.Now()
	logger.Info(ctx, "tenant_tidb_schema_init_started",
		zap.String("mode", string(TiDBEmbeddingModeAuto)))
	render, err := tidbAutoEmbeddingRenderConfigForProfile(profile)
	if err != nil {
		return err
	}
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	if !IsTiDBCluster(ctx, db) {
		return fmt.Errorf("provider requires TiDB capabilities (FTS/VECTOR)")
	}
	if err := repairMySQLPathHashSchema(ctx, db); err != nil {
		return err
	}
	if err := ExecSchemaStatementsContext(ctx, db, tidbAutoEmbeddingSchemaStatementsForConfig(render)); err != nil {
		return err
	}
	if err := EnsureTiDBSchemaForAutoEmbeddingProfile(ctx, db, profile); err != nil {
		return err
	}
	logger.Info(ctx, "tenant_tidb_schema_init_finished",
		zap.String("mode", string(TiDBEmbeddingModeAuto)),
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return nil
}

// ValidateTiDBSchemaForModeDSN opens a DSN, validates the schema, and closes.
func ValidateTiDBSchemaForModeDSN(ctx context.Context, dsn string, mode TiDBEmbeddingMode) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	return ValidateTiDBSchemaForMode(ctx, db, mode)
}

func ValidateTiDBSchemaForAutoEmbeddingConfigDSN(ctx context.Context, dsn string, cfg TiDBAutoEmbeddingConfig) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	return ValidateTiDBSchemaForAutoEmbeddingConfig(ctx, db, cfg)
}

func ValidateTiDBSchemaForAutoEmbeddingProfileDSN(ctx context.Context, dsn string, profile TiDBAutoEmbeddingProfile) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	return ValidateTiDBSchemaForAutoEmbeddingProfile(ctx, db, profile)
}

// EnsureTiDBSchemaForModeDSN opens a DSN, repairs known launch-schema drift,
// validates the schema contract, and closes.
func EnsureTiDBSchemaForModeDSN(ctx context.Context, dsn string, mode TiDBEmbeddingMode) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	return EnsureTiDBSchemaForMode(ctx, db, mode)
}

func EnsureTiDBSchemaForAutoEmbeddingConfigDSN(ctx context.Context, dsn string, cfg TiDBAutoEmbeddingConfig) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	return EnsureTiDBSchemaForAutoEmbeddingConfig(ctx, db, cfg)
}

func EnsureTiDBSchemaForAutoEmbeddingProfileDSN(ctx context.Context, dsn string, profile TiDBAutoEmbeddingProfile) error {
	db, err := OpenTiDBSchemaDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = closeTiDBSchemaDB(db) }()
	return EnsureTiDBSchemaForAutoEmbeddingProfile(ctx, db, profile)
}

func OpenTiDBSchemaDB(ctx context.Context, dsn string) (*sql.DB, error) {
	if HasMultiStatements(dsn) {
		return nil, fmt.Errorf("multiStatements is not allowed")
	}
	db, err := mysqlutil.OpenInstrumented(ctx, dsn, mysqlutil.RoleUser)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func closeTiDBSchemaDB(db *sql.DB) error {
	return mysqlutil.CloseInstrumented(db)
}

func detectTiDBEmbeddingModeFromMeta(meta tidbTableMeta) (TiDBEmbeddingMode, error) {
	col, err := meta.requireColumn("embedding")
	if err != nil {
		return TiDBEmbeddingModeUnknown, err
	}
	if normalizeSQLFragment(col.columnType) != fmt.Sprintf("vector(%d)", TiDBAutoEmbeddingDimensions) {
		return TiDBEmbeddingModeUnknown, fmt.Errorf("unsupported embedding column type %q", col.columnType)
	}
	extra := normalizeSQLFragment(col.extra)
	expr := normalizeSQLFragment(col.generationExpression)
	if strings.Contains(extra, "generated") {
		if !strings.Contains(extra, "stored") {
			return TiDBEmbeddingModeUnknown, errors.New("embedding generated column must be stored")
		}
		if !strings.Contains(expr, "embed_text(") {
			return TiDBEmbeddingModeUnknown, errors.New("embedding generated expression must use EMBED_TEXT")
		}
		return TiDBEmbeddingModeAuto, nil
	}
	if expr != "" {
		return TiDBEmbeddingModeUnknown, errors.New("embedding generation expression present without generated column metadata")
	}
	return TiDBEmbeddingModeApp, nil
}

func validateTiDBAutoEmbeddingSemanticTable(meta tidbTableMeta) error {
	if err := validateTiDBSemanticTableBase(meta); err != nil {
		return err
	}
	return schemaDiffsToError(validateTiDBAutoEmbeddingDiffs(meta))
}

func validateTiDBAppEmbeddingSemanticTable(meta tidbTableMeta) error {
	if err := validateTiDBSemanticTableBase(meta); err != nil {
		return err
	}
	return schemaDiffsToError(validateTiDBAppEmbeddingDiffs(meta))
}

func validateTiDBSemanticTableBase(meta tidbTableMeta) error {
	if err := meta.requireColumnType("inode_id", "varchar(64)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("content_text", "longtext"); err != nil {
		return err
	}
	if err := meta.requireColumnType("embedding", fmt.Sprintf("vector(%d)", TiDBAutoEmbeddingDimensions)); err != nil {
		return err
	}
	if err := meta.requireColumnType("embedding_revision", "bigint"); err != nil {
		return err
	}
	if err := meta.requireColumnType("description", "longtext"); err != nil {
		return err
	}
	if err := meta.requireColumnType("description_embedding", fmt.Sprintf("vector(%d)", TiDBAutoEmbeddingDimensions)); err != nil {
		return err
	}
	return meta.requireColumnType("description_embedding_revision", "bigint")
}

func validateTiDBUploadsTableBase(meta tidbTableMeta) error {
	if err := meta.requireColumnType("upload_id", "varchar(64)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("target_path", "text"); err != nil {
		return err
	}
	if err := meta.requireColumnType("target_path_hash", "varchar(64)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("status", "varchar(32)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("storage_encryption_mode", "varchar(16)"); err != nil {
		return err
	}
	if err := meta.requireColumnType("storage_encryption_key_id", "varchar(256)"); err != nil {
		return err
	}
	return meta.requireColumnType("expected_revision", "bigint")
}

func loadTiDBTableMeta(ctx context.Context, db *sql.DB, tableName string) (tidbTableMeta, error) {
	columns, err := loadTiDBColumnMeta(ctx, db, tableName)
	if err != nil {
		return tidbTableMeta{}, fmt.Errorf("load columns: %w", err)
	}
	return tidbTableMeta{tableName: tableName, columns: columns}, nil
}

func loadTiDBColumnMeta(ctx context.Context, db *sql.DB, tableName string) (map[string]tidbColumnMeta, error) {
	rows, err := db.QueryContext(ctx, `SELECT column_name, column_type, extra, generation_expression
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?`, tableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	columns := make(map[string]tidbColumnMeta)
	for rows.Next() {
		var name, columnType string
		var extra, generationExpression sql.NullString
		if err := rows.Scan(&name, &columnType, &extra, &generationExpression); err != nil {
			return nil, err
		}
		columns[strings.ToLower(name)] = tidbColumnMeta{
			columnType:           columnType,
			extra:                extra.String,
			generationExpression: generationExpression.String,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, sql.ErrNoRows
	}
	return columns, nil
}

func (m tidbTableMeta) requireColumn(name string) (tidbColumnMeta, error) {
	col, ok := m.columns[strings.ToLower(name)]
	if !ok {
		return tidbColumnMeta{}, fmt.Errorf("missing %s column", name)
	}
	return col, nil
}

func (m tidbTableMeta) requireColumnType(name, want string) error {
	col, err := m.requireColumn(name)
	if err != nil {
		return err
	}
	if normalizeColumnTypeForCompare(col.columnType) != normalizeColumnTypeForCompare(want) {
		return fmt.Errorf("%s column type = %q, want %s", name, col.columnType, want)
	}
	return nil
}

func loadShowCreateTable(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	var gotTable string
	var createStmt string
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	if err := db.QueryRowContext(ctx, query).Scan(&gotTable, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

func loadTiDBIndexNames(ctx context.Context, db *sql.DB, tableName string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT index_name
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ?`, tableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	indexNames := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		indexNames[strings.ToLower(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return indexNames, nil
}

func normalizeSQLFragment(s string) string {
	return schemaspec.NormalizeSQLFragment(s)
}

func normalizeColumnTypeForCompare(s string) string {
	normalized := normalizeSQLFragment(s)
	switch normalized {
	case "bool", "boolean", "tinyint(1)":
		return "boolean"
	default:
		return normalized
	}
}

func diffTiDBSchemaForModeWithConfig(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) ([]tidbSchemaDiff, error) {
	start := time.Now()
	spec, err := tidbSchemaSpecForModeWithConfig(mode, cfg)
	if err != nil {
		return nil, err
	}
	var diffs []tidbSchemaDiff
	for _, table := range spec.tables {
		tableDiffs, err := diffTiDBTable(ctx, db, table)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, tableDiffs...)
	}
	legacyFilesDiffs, err := diffLegacyTiDBFilesTableIfExistsWithConfig(ctx, db, mode, cfg)
	if err != nil {
		return nil, err
	}
	diffs = append(diffs, legacyFilesDiffs...)
	logger.Info(ctx, "tenant_tidb_schema_diff_finished",
		zap.String("mode", string(mode)),
		zap.Int("table_count", len(spec.tables)),
		zap.Int("diff_count", len(diffs)),
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return diffs, nil
}

func diffLegacyTiDBFilesTableIfExists(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode) ([]tidbSchemaDiff, error) {
	return diffLegacyTiDBFilesTableIfExistsWithConfig(ctx, db, mode, currentTiDBAutoEmbeddingRenderConfig())
}

func diffLegacyTiDBFilesTableIfExistsWithConfig(ctx context.Context, db *sql.DB, mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) ([]tidbSchemaDiff, error) {
	spec, err := legacyTiDBFilesTableSpecForModeWithConfig(mode, cfg)
	if err != nil {
		return nil, err
	}
	meta, err := loadTiDBTableMeta(ctx, db, spec.name)
	if err != nil {
		if isMissingTableError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("load legacy %s table metadata: %w", spec.name, err)
	}
	createStmt, err := loadShowCreateTable(ctx, db, spec.name)
	if err != nil {
		if isMissingTableError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("show create legacy %s: %w", spec.name, err)
	}
	observedIndexes, indexesObserved := loadObservedTiDBIndexes(ctx, db, spec.name, createStmt)
	return diffTiDBTableMetaWithObservedIndexes(spec, meta, createStmt, observedIndexes, indexesObserved), nil
}

func diffTiDBTable(ctx context.Context, db *sql.DB, table tidbTableSpec) ([]tidbSchemaDiff, error) {
	start := time.Now()
	loadColumnsStart := time.Now()
	meta, err := loadTiDBTableMeta(ctx, db, table.name)
	loadColumnsDurationMs := float64(time.Since(loadColumnsStart).Microseconds()) / 1000.0
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			diffs := missingTableAndIndexDiffs(table)
			logger.Info(ctx, "tenant_tidb_schema_diff_table_finished",
				zap.String("table", table.name),
				zap.Bool("table_missing", true),
				zap.Float64("load_columns_ms", loadColumnsDurationMs),
				zap.Float64("show_create_ms", 0),
				zap.Float64("load_indexes_ms", 0),
				zap.Int("diff_count", len(diffs)),
				zap.Strings("diffs", summarizeTiDBSchemaDiffs(diffs)),
				zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
			return diffs, nil
		}
		return nil, fmt.Errorf("load %s table metadata: %w", table.name, err)
	}
	showCreateStart := time.Now()
	createStmt, err := loadShowCreateTable(ctx, db, table.name)
	showCreateDurationMs := float64(time.Since(showCreateStart).Microseconds()) / 1000.0
	if err != nil {
		return nil, fmt.Errorf("show create %s: %w", table.name, err)
	}
	loadIndexesStart := time.Now()
	observedIndexes, indexesObserved := loadObservedTiDBIndexes(ctx, db, table.name, createStmt)
	loadIndexesDurationMs := float64(time.Since(loadIndexesStart).Microseconds()) / 1000.0
	diffs := diffTiDBTableMetaWithObservedIndexes(table, meta, createStmt, observedIndexes, indexesObserved)
	logger.Info(ctx, "tenant_tidb_schema_diff_table_finished",
		zap.String("table", table.name),
		zap.Bool("table_missing", false),
		zap.Bool("indexes_observed", indexesObserved),
		zap.Float64("load_columns_ms", loadColumnsDurationMs),
		zap.Float64("show_create_ms", showCreateDurationMs),
		zap.Float64("load_indexes_ms", loadIndexesDurationMs),
		zap.Int("column_count", len(meta.columns)),
		zap.Int("diff_count", len(diffs)),
		zap.Strings("diffs", summarizeTiDBSchemaDiffs(diffs)),
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return diffs, nil
}

func loadObservedTiDBIndexes(ctx context.Context, db *sql.DB, tableName, createStmt string) (map[string]struct{}, bool) {
	indexNames, statErr := loadTiDBIndexNames(ctx, db, tableName)
	if statErr != nil {
		logger.Warn(ctx, "tenant_tidb_schema_load_index_metadata_failed",
			zap.String("table", tableName),
			zap.Error(statErr))
	}

	merged := make(map[string]struct{})
	if statErr == nil {
		for name := range indexNames {
			merged[name] = struct{}{}
		}
	}

	if observedIndexes, ok := parseObservedTiDBIndexes(createStmt); ok {
		for name := range observedIndexes {
			if _, exists := merged[name]; !exists {
				merged[name] = struct{}{}
			}
		}
	} else if statErr != nil {
		logger.Warn(ctx, "tenant_tidb_schema_parse_show_create_indexes_failed",
			zap.String("table", tableName))
	}

	if len(merged) == 0 && statErr != nil {
		return nil, false
	}
	return merged, true
}

func tidbSchemaSpecForMode(mode TiDBEmbeddingMode) (tidbSchemaSpec, error) {
	return tidbSchemaSpecForModeWithConfig(mode, currentTiDBAutoEmbeddingRenderConfig())
}

func tidbSchemaSpecForModeWithConfig(mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) (tidbSchemaSpec, error) {
	// For app mode, use only the base (required) statements. The optional
	// indexes (FULLTEXT, VECTOR with ADD_COLUMNAR_REPLICA_ON_DEMAND) may be
	// silently skipped on TiDB versions that do not support that syntax, so
	// they must not be part of the enforceable schema contract.
	var stmts []string
	if mode == TiDBEmbeddingModeApp {
		stmts = tidbAppEmbeddingBaseSchemaStatements()
	} else {
		var err error
		stmts, err = initTiDBTenantSchemaStatementsForModeWithConfig(mode, cfg)
		if err != nil {
			return tidbSchemaSpec{}, err
		}
	}
	spec, err := tidbSchemaSpecFromStatements(stmts)
	if err != nil {
		return tidbSchemaSpec{}, err
	}
	for i := range spec.tables {
		if spec.tables[i].name != "semantic" {
			continue
		}
		if mode == TiDBEmbeddingModeAuto {
			if col, ok := spec.tables[i].columns["description_embedding"]; ok {
				col.addSQL = fmt.Sprintf(
					"ALTER TABLE semantic ADD COLUMN description_embedding VECTOR(%d) GENERATED ALWAYS AS (EMBED_TEXT(%s, description, %s)) STORED",
					cfg.dimensions, tidbSQLStringLiteral(cfg.model), tidbSQLStringLiteral(cfg.optionsJSON),
				)
				spec.tables[i].columns["description_embedding"] = col
			}
		}
		spec.tables[i].validate = func(meta tidbTableMeta) []tidbSchemaDiff {
			switch mode {
			case TiDBEmbeddingModeAuto:
				return validateTiDBAutoEmbeddingDiffsWithConfig(meta, cfg)
			case TiDBEmbeddingModeApp:
				return validateTiDBAppEmbeddingDiffs(meta)
			default:
				return []tidbSchemaDiff{{kind: tidbSchemaDiffTableContract, tableName: "semantic", detail: fmt.Sprintf("semantic schema contract: unsupported TiDB embedding mode %q", mode)}}
			}
		}
		break
	}
	return spec, nil
}

func legacyTiDBFilesTableSpecForMode(mode TiDBEmbeddingMode) (tidbTableSpec, error) {
	return legacyTiDBFilesTableSpecForModeWithConfig(mode, currentTiDBAutoEmbeddingRenderConfig())
}

func legacyTiDBFilesTableSpecForModeWithConfig(mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) (tidbTableSpec, error) {
	stmts, err := legacyTiDBFilesSchemaStatementsForModeWithConfig(mode, cfg)
	if err != nil {
		return tidbTableSpec{}, err
	}
	spec, err := tidbSchemaSpecFromStatements(stmts)
	if err != nil {
		return tidbTableSpec{}, err
	}
	for _, table := range spec.tables {
		if table.name != "files" {
			continue
		}
		table.validate = func(meta tidbTableMeta) []tidbSchemaDiff {
			switch mode {
			case TiDBEmbeddingModeAuto:
				return validateTiDBAutoEmbeddingFilesDiffsWithConfig(meta, cfg)
			case TiDBEmbeddingModeApp:
				return validateTiDBAppEmbeddingFilesDiffs(meta)
			default:
				return []tidbSchemaDiff{{kind: tidbSchemaDiffTableContract, tableName: "files", detail: fmt.Sprintf("files legacy schema contract: unsupported TiDB embedding mode %q", mode)}}
			}
		}
		return table, nil
	}
	return tidbTableSpec{}, errors.New("legacy files table spec missing")
}

func legacyTiDBFilesSchemaStatementsForModeWithConfig(mode TiDBEmbeddingMode, cfg tidbAutoEmbeddingRenderConfig) ([]string, error) {
	embeddingColumn := fmt.Sprintf("VECTOR(%d)", cfg.dimensions)
	descriptionEmbeddingColumn := fmt.Sprintf("VECTOR(%d)", cfg.dimensions)
	if mode == TiDBEmbeddingModeAuto {
		modelLiteral := tidbSQLStringLiteral(cfg.model)
		optionsLiteral := tidbSQLStringLiteral(cfg.optionsJSON)
		embeddingColumn = fmt.Sprintf("VECTOR(%d) GENERATED ALWAYS AS (EMBED_TEXT(%s, content_text, %s)) STORED",
			cfg.dimensions, modelLiteral, optionsLiteral)
		descriptionEmbeddingColumn = fmt.Sprintf("VECTOR(%d) GENERATED ALWAYS AS (EMBED_TEXT(%s, description, %s)) STORED",
			cfg.dimensions, modelLiteral, optionsLiteral)
	} else if mode != TiDBEmbeddingModeApp {
		return nil, fmt.Errorf("unsupported TiDB embedding mode %q", mode)
	}
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS files (
			file_id            VARCHAR(64) PRIMARY KEY,
			storage_type       VARCHAR(32) NOT NULL,
			storage_ref        TEXT NOT NULL,
			storage_ref_hash   VARCHAR(64) NOT NULL DEFAULT '',
			storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'legacy',
			storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '',
			content_blob       LONGBLOB,
			content_type       VARCHAR(255),
			size_bytes         BIGINT NOT NULL DEFAULT 0,
			checksum_sha256    VARCHAR(128),
			revision           BIGINT NOT NULL DEFAULT 1,
			status             VARCHAR(32) NOT NULL DEFAULT 'PENDING',
			source_id          VARCHAR(255),
			content_text       LONGTEXT,
			description        LONGTEXT,
			embedding          %s,
			embedding_revision BIGINT,
			description_embedding %s,
			description_embedding_revision BIGINT,
			created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			confirmed_at       DATETIME(3),
			expires_at         DATETIME(3)
		)`, embeddingColumn, descriptionEmbeddingColumn),
		`CREATE INDEX idx_status ON files(status, created_at)`,
		`CREATE INDEX idx_files_storage_ref_hash ON files(storage_ref_hash)`,
	}
	if mode == TiDBEmbeddingModeAuto {
		stmts = append(stmts,
			`ALTER TABLE files
				ADD FULLTEXT INDEX idx_fts_content(content_text)
				WITH PARSER MULTILINGUAL
				ADD_COLUMNAR_REPLICA_ON_DEMAND`,
			`ALTER TABLE files
				ADD FULLTEXT INDEX idx_fts_description(description)
				WITH PARSER MULTILINGUAL
				ADD_COLUMNAR_REPLICA_ON_DEMAND`,
			`ALTER TABLE files
				ADD VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
				ADD_COLUMNAR_REPLICA_ON_DEMAND`,
			`ALTER TABLE files
				ADD VECTOR INDEX idx_files_desc_cosine((VEC_COSINE_DISTANCE(description_embedding)))
				ADD_COLUMNAR_REPLICA_ON_DEMAND`,
		)
	}
	return stmts, nil
}

func tidbSchemaSpecFromStatements(stmts []string) (tidbSchemaSpec, error) {
	tables := make([]tidbTableSpec, 0)
	byName := make(map[string]int)
	for _, stmt := range stmts {
		table, ok, err := parseCreateTableSpec(stmt)
		if err != nil {
			return tidbSchemaSpec{}, err
		}
		if !ok {
			continue
		}
		tables = append(tables, table)
		byName[table.name] = len(tables) - 1
	}
	for _, stmt := range stmts {
		tableName, indexName, createSQL, ok := parseCreateIndexStatement(stmt)
		if !ok {
			tableName, indexName, createSQL, ok = parseAlterTableAddIndexStatement(stmt)
		}
		if !ok {
			continue
		}
		tableIndex, exists := byName[tableName]
		if !exists {
			continue
		}
		if tables[tableIndex].indexes == nil {
			tables[tableIndex].indexes = make(map[string]tidbIndexSpec)
		}
		tables[tableIndex].indexes[indexName] = tidbIndexSpec{createSQL: strings.TrimSpace(createSQL)}
	}
	return tidbSchemaSpec{tables: tables}, nil
}

func parseCreateTableSpec(stmt string) (tidbTableSpec, bool, error) {
	tableName, defs, ok, err := parseCreateTableStatement(stmt)
	if err != nil {
		return tidbTableSpec{}, false, err
	}
	if !ok {
		return tidbTableSpec{}, false, nil
	}
	columns := make(map[string]tidbColumnSpec)
	indexes := make(map[string]tidbIndexSpec)
	var primaryKey tidbPrimaryKeySpec
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		if pkSpec, ok := parsePrimaryKeyDefinition(def); ok {
			primaryKey = pkSpec
			continue
		}
		if indexName, createSQL, ok := parseInlineIndexDefinition(tableName, def); ok {
			indexes[indexName] = tidbIndexSpec{createSQL: createSQL}
			continue
		}
		if isConstraintDefinition(def) {
			continue
		}
		colName, colSpec, ok := parseColumnDefinition(tableName, def)
		if !ok {
			continue
		}
		columns[colName] = colSpec
		if pkCol, ok := parseInlinePrimaryKeyColumn(def); ok {
			primaryKey = tidbPrimaryKeySpec{columns: []string{pkCol}}
		}
	}
	return tidbTableSpec{
		name:            tableName,
		createStatement: strings.TrimSpace(stmt),
		columns:         columns,
		indexes:         indexes,
		primaryKey:      primaryKey,
	}, true, nil
}

func parseCreateTableStatement(stmt string) (tableName string, definitions string, ok bool, err error) {
	return schemaspec.ParseCreateTableStatement(stmt)
}

func splitTopLevelComma(definitions string) []string {
	return schemaspec.SplitTopLevelComma(definitions)
}

func parseInlineIndexDefinition(tableName, def string) (indexName, createSQL string, ok bool) {
	normalized := normalizeSQLFragment(def)
	if strings.HasPrefix(normalized, "unique index ") || strings.HasPrefix(normalized, "unique key ") {
		prefix := "UNIQUE INDEX"
		if strings.HasPrefix(normalized, "unique key ") {
			prefix = "UNIQUE KEY"
		}
		name, cols := parseIndexNameAndColumns(def, prefix)
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s%s", name, tableName, cols), true
	}
	if name, cols, ok := parseConstraintUniqueIndexDefinition(def); ok {
		return name, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s%s", name, tableName, cols), true
	}
	if strings.HasPrefix(normalized, "fulltext index ") || strings.HasPrefix(normalized, "fulltext key ") {
		prefix := "FULLTEXT INDEX"
		if strings.HasPrefix(normalized, "fulltext key ") {
			prefix = "FULLTEXT KEY"
		}
		name, cols := parseIndexNameAndColumns(def, prefix)
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE FULLTEXT INDEX %s ON %s%s", name, tableName, cols), true
	}
	if strings.HasPrefix(normalized, "vector index ") {
		name, cols := parseIndexNameAndColumns(def, "VECTOR INDEX")
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE VECTOR INDEX %s ON %s%s", name, tableName, cols), true
	}
	if strings.HasPrefix(normalized, "index ") || strings.HasPrefix(normalized, "key ") {
		prefix := "INDEX"
		if strings.HasPrefix(normalized, "key ") {
			prefix = "KEY"
		}
		name, cols := parseIndexNameAndColumns(def, prefix)
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE INDEX %s ON %s%s", name, tableName, cols), true
	}
	return "", "", false
}

func parseConstraintUniqueIndexDefinition(def string) (indexName, columns string, ok bool) {
	trimmed := strings.TrimSpace(def)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "CONSTRAINT ") {
		return "", "", false
	}
	rest := strings.TrimSpace(trimmed[len("CONSTRAINT "):])
	name, remainder := splitIdentifierAndRest(rest)
	if name == "" || remainder == "" {
		return "", "", false
	}
	remainder = strings.TrimSpace(remainder)
	upperRemainder := strings.ToUpper(remainder)
	switch {
	case strings.HasPrefix(upperRemainder, "UNIQUE KEY"):
		return parseConstraintUniqueIndexSuffix(name, remainder[len("UNIQUE KEY"):])
	case strings.HasPrefix(upperRemainder, "UNIQUE INDEX"):
		return parseConstraintUniqueIndexSuffix(name, remainder[len("UNIQUE INDEX"):])
	case strings.HasPrefix(upperRemainder, "UNIQUE"):
		return parseConstraintUniqueIndexSuffix(name, remainder[len("UNIQUE"):])
	default:
		return "", "", false
	}
}

func parseConstraintUniqueIndexSuffix(defaultName, suffix string) (indexName, columns string, ok bool) {
	suffix = strings.TrimSpace(suffix)
	if suffix == "" {
		return "", "", false
	}
	name, columnSuffix := splitIdentifierAndSuffix(suffix)
	if name == "" {
		name = defaultName
		columnSuffix = suffix
	}
	cols := parseIndexColumnsSuffix(columnSuffix)
	if cols == "" {
		return "", "", false
	}
	return strings.ToLower(name), cols, true
}

func parseIndexNameAndColumns(def, prefix string) (indexName, columns string) {
	trimmed := strings.TrimSpace(def)
	upper := strings.ToUpper(trimmed)
	p := strings.Index(upper, prefix)
	if p < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(trimmed[p+len(prefix):])
	if rest == "" {
		return "", ""
	}
	name, remainder := splitIdentifierAndRest(rest)
	if name == "" {
		return "", ""
	}
	open := strings.Index(remainder, "(")
	if open < 0 {
		return "", ""
	}
	return strings.ToLower(name), strings.TrimSpace(remainder[open:])
}

func parseIndexColumnsSuffix(s string) string {
	trimmed := strings.TrimSpace(s)
	open := strings.Index(trimmed, "(")
	if open < 0 {
		return ""
	}
	return strings.TrimSpace(trimmed[open:])
}

func parsePrimaryKeyDefinition(def string) (tidbPrimaryKeySpec, bool) {
	normalized := normalizeSQLFragment(def)
	isPrimaryConstraint := strings.HasPrefix(normalized, "primary key")
	isNamedPrimaryConstraint := strings.HasPrefix(normalized, "constraint ") && strings.Contains(normalized, " primary key ")
	if !isPrimaryConstraint && !isNamedPrimaryConstraint {
		return tidbPrimaryKeySpec{}, false
	}
	columns := parseKeyColumnList(def, "PRIMARY KEY")
	if len(columns) == 0 {
		return tidbPrimaryKeySpec{}, false
	}
	return tidbPrimaryKeySpec{columns: columns}, true
}

func parseInlinePrimaryKeyColumn(def string) (string, bool) {
	if !strings.Contains(normalizeSQLFragment(def), " primary key") {
		return "", false
	}
	name, rest := splitIdentifierAndRest(strings.TrimSpace(def))
	if name == "" || rest == "" {
		return "", false
	}
	return strings.ToLower(name), true
}

func parseKeyColumnList(def, token string) []string {
	upper := strings.ToUpper(def)
	pos := strings.Index(upper, token)
	if pos < 0 {
		return nil
	}
	rest := strings.TrimSpace(def[pos+len(token):])
	open := strings.Index(rest, "(")
	close := strings.LastIndex(rest, ")")
	if open < 0 || close <= open {
		return nil
	}
	inner := strings.TrimSpace(rest[open+1 : close])
	if inner == "" {
		return nil
	}
	parts := splitTopLevelComma(inner)
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		name := parseColumnReferenceName(part)
		if name == "" {
			return nil
		}
		columns = append(columns, name)
	}
	return columns
}

func parseColumnReferenceName(def string) string {
	trimmed := strings.TrimSpace(def)
	if trimmed == "" {
		return ""
	}
	if trimmed[0] == '`' {
		end := strings.Index(trimmed[1:], "`")
		if end < 0 {
			return ""
		}
		return strings.ToLower(trimmed[1 : 1+end])
	}
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case ' ', '\t', '\n', '\r', '(':
			return strings.ToLower(trimmed[:i])
		}
	}
	return strings.ToLower(trimmed)
}

func parseColumnDefinition(tableName, def string) (string, tidbColumnSpec, bool) {
	name, rest := splitIdentifierAndRest(strings.TrimSpace(def))
	if name == "" || rest == "" {
		return "", tidbColumnSpec{}, false
	}
	colType := parseColumnType(rest)
	if colType == "" {
		return "", tidbColumnSpec{}, false
	}
	return strings.ToLower(name), tidbColumnSpec{
		columnType: strings.ToLower(strings.TrimSpace(colType)),
		addSQL:     fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, name, strings.TrimSpace(rest)),
		modifySQL:  fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", tableName, name, strings.TrimSpace(rest)),
	}, true
}

func parseColumnType(rest string) string {
	return schemaspec.ParseColumnType(rest)
}

func splitIdentifierAndRest(s string) (identifier string, rest string) {
	return schemaspec.SplitIdentifierAndRest(s)
}

func splitIdentifierAndSuffix(s string) (identifier string, rest string) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return "", ""
	}
	if trimmed[0] == '`' {
		end := strings.Index(trimmed[1:], "`")
		if end < 0 {
			return "", ""
		}
		return trimmed[1 : 1+end], strings.TrimSpace(trimmed[1+end+1:])
	}
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case ' ', '\t', '\n', '\r', '(':
			return trimmed[:i], strings.TrimSpace(trimmed[i:])
		}
	}
	return trimmed, ""
}

func isConstraintDefinition(def string) bool {
	normalized := normalizeSQLFragment(def)
	return strings.HasPrefix(normalized, "primary key") ||
		strings.HasPrefix(normalized, "constraint ") ||
		strings.HasPrefix(normalized, "unique key ") ||
		strings.HasPrefix(normalized, "fulltext ") ||
		strings.HasPrefix(normalized, "vector index ")
}

func parseCreateIndexStatement(stmt string) (tableName, indexName, createSQL string, ok bool) {
	normalized := normalizeSQLFragment(stmt)
	prefix := ""
	switch {
	case strings.HasPrefix(normalized, "create unique index "):
		prefix = "create unique index "
	case strings.HasPrefix(normalized, "create fulltext index "):
		prefix = "create fulltext index "
	case strings.HasPrefix(normalized, "create vector index "):
		prefix = "create vector index "
	case strings.HasPrefix(normalized, "create spatial index "):
		prefix = "create spatial index "
	case strings.HasPrefix(normalized, "create index "):
		prefix = "create index "
	default:
		return "", "", "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(normalized, prefix))
	parts := strings.SplitN(rest, " on ", 2)
	if len(parts) != 2 {
		return "", "", "", false
	}
	nameFields := strings.Fields(parts[0])
	if len(nameFields) == 0 {
		return "", "", "", false
	}
	afterOn := strings.TrimSpace(parts[1])
	if afterOn == "" {
		return "", "", "", false
	}
	tableEnd := strings.IndexAny(afterOn, " (")
	if tableEnd < 0 {
		tableEnd = len(afterOn)
	}
	table := strings.TrimSpace(afterOn[:tableEnd])
	if table == "" {
		return "", "", "", false
	}
	return strings.ToLower(table), strings.ToLower(nameFields[0]), strings.TrimSpace(stmt), true
}

func parseAlterTableAddIndexStatement(stmt string) (tableName, indexName, createSQL string, ok bool) {
	normalized := normalizeSQLFragment(stmt)
	const prefix = "alter table "
	if !strings.HasPrefix(normalized, prefix) {
		return "", "", "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(normalized, prefix))
	if rest == "" {
		return "", "", "", false
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", "", "", false
	}
	table := strings.ToLower(fields[0])

	markers := []string{" add fulltext index ", " add vector index ", " add unique index ", " add unique key ", " add index ", " add key "}
	for _, marker := range markers {
		pos := strings.Index(normalized, marker)
		if pos < 0 {
			continue
		}
		indexPart := strings.TrimSpace(normalized[pos+len(marker):])
		if indexPart == "" {
			return "", "", "", false
		}
		name := indexPart
		if open := strings.Index(name, "("); open >= 0 {
			name = name[:open]
		}
		nameFields := strings.Fields(name)
		if len(nameFields) == 0 {
			return "", "", "", false
		}
		createSQL := strings.TrimSpace(stmt)
		if marker == " add fulltext index " || marker == " add vector index " {
			createSQL = stripColumnarReplicaOnDemand(createSQL)
		}
		return table, strings.ToLower(nameFields[0]), createSQL, true
	}

	return "", "", "", false
}

func stripColumnarReplicaOnDemand(stmt string) string {
	idx := strings.LastIndex(strings.ToLower(stmt), "add_columnar_replica_on_demand")
	if idx < 0 {
		return stmt
	}
	return strings.TrimSpace(stmt[:idx])
}

func diffTiDBTableMeta(table tidbTableSpec, meta tidbTableMeta, createStmt string) []tidbSchemaDiff {
	observedIndexes, ok := parseObservedTiDBIndexes(createStmt)
	return diffTiDBTableMetaWithObservedIndexes(table, meta, createStmt, observedIndexes, ok)
}

func diffTiDBTableMetaWithObservedIndexes(table tidbTableSpec, meta tidbTableMeta, createStmt string, observedIndexes map[string]struct{}, indexesObserved bool) []tidbSchemaDiff {
	var diffs []tidbSchemaDiff
	observedIndexColumns, observedIndexColumnsOK := parseObservedTiDBIndexColumns(createStmt)
	for _, name := range sortedColumnNames(table.columns) {
		spec := table.columns[name]
		col, err := meta.requireColumn(name)
		if err != nil {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffMissingColumn,
				tableName:  table.name,
				columnName: name,
				detail:     fmt.Sprintf("%s schema contract: missing %s column", table.name, name),
				repairSQL:  spec.addSQL,
			})
			continue
		}
		if normalizeColumnTypeForCompare(col.columnType) != normalizeColumnTypeForCompare(spec.columnType) {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffColumnType,
				tableName:  table.name,
				columnName: name,
				detail:     fmt.Sprintf("%s schema contract: %s column type = %q, want %s", table.name, name, col.columnType, spec.columnType),
				repairSQL:  spec.modifySQL,
			})
		}
	}
	if len(table.primaryKey.columns) > 0 {
		actualPrimaryKey, ok := parsePrimaryKeyColumnsFromCreateStatement(createStmt)
		if !ok {
			diffs = append(diffs, tidbSchemaDiff{
				kind:      tidbSchemaDiffTableContract,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: missing primary key constraint", table.name),
			})
		} else if !equalStringSlices(actualPrimaryKey, table.primaryKey.columns) {
			diffs = append(diffs, tidbSchemaDiff{
				kind:      tidbSchemaDiffTableContract,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: primary key columns = (%s), want (%s)", table.name, strings.Join(actualPrimaryKey, ", "), strings.Join(table.primaryKey.columns, ", ")),
			})
		}
	}
	if !indexesObserved {
		if len(table.indexes) > 0 {
			diffs = append(diffs, tidbSchemaDiff{
				kind:      tidbSchemaDiffTableContract,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: unable to inspect indexes", table.name),
			})
		}
	} else {
		for _, name := range sortedIndexNames(table.indexes) {
			spec := table.indexes[name]
			if !hasObservedTiDBIndex(observedIndexes, name) {
				diffs = append(diffs, tidbSchemaDiff{
					kind:      tidbSchemaDiffMissingIndex,
					tableName: table.name,
					detail:    fmt.Sprintf("%s schema contract: missing %s index", table.name, name),
					repairSQL: spec.createSQL,
				})
				continue
			}
			if observedIndexColumnsOK && isPathHashIndexName(table.name, name) {
				observedColumns := observedIndexColumns[strings.ToLower(name)]
				expectedColumns := expectedPathHashIndexColumns(table.name, name)
				if len(expectedColumns) > 0 && len(observedColumns) > 0 && !equalStringSlices(observedColumns, expectedColumns) {
					diffs = append(diffs, tidbSchemaDiff{
						kind:      tidbSchemaDiffMissingIndex,
						tableName: table.name,
						detail:    fmt.Sprintf("%s schema contract: %s index columns = (%s), want (%s)", table.name, name, strings.Join(observedColumns, ", "), strings.Join(expectedColumns, ", ")),
						repairSQL: dropPathHashIndexSQL(table.name, name),
					})
				}
			}
		}
	}
	if table.validate != nil {
		diffs = append(diffs, table.validate(meta)...)
	}
	diffs = append(diffs, legacyUploadActiveTargetPathDiffs(table, meta)...)
	return diffs
}

func legacyUploadActiveTargetPathDiffs(table tidbTableSpec, meta tidbTableMeta) []tidbSchemaDiff {
	if table.name != "uploads" {
		return nil
	}
	if _, ok := meta.columns["active_target_path"]; !ok {
		return nil
	}
	return []tidbSchemaDiff{{
		kind:       tidbSchemaDiffExtraColumn,
		tableName:  "uploads",
		columnName: "active_target_path",
		detail:     "uploads schema contract: legacy active_target_path generated column must be dropped",
		repairSQL:  "ALTER TABLE uploads DROP COLUMN active_target_path",
	}}
}

func hasObservedTiDBIndex(observedIndexes map[string]struct{}, indexName string) bool {
	if len(observedIndexes) == 0 {
		return false
	}
	_, ok := observedIndexes[strings.ToLower(indexName)]
	return ok
}

func parseObservedTiDBIndexColumns(createStmt string) (map[string][]string, bool) {
	_, defs, ok, err := parseCreateTableStatement(createStmt)
	if err != nil || !ok {
		return nil, false
	}
	observed := make(map[string][]string)
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		normalized := normalizeSQLFragment(def)
		var name string
		var columns string
		switch {
		case strings.HasPrefix(normalized, "constraint "):
			var ok bool
			name, columns, ok = parseConstraintUniqueIndexDefinition(def)
			if !ok {
				continue
			}
		case strings.HasPrefix(normalized, "unique key "):
			name, columns = parseIndexNameAndColumns(def, "UNIQUE KEY")
		case strings.HasPrefix(normalized, "unique index "):
			name, columns = parseIndexNameAndColumns(def, "UNIQUE INDEX")
		case strings.HasPrefix(normalized, "fulltext index "):
			name, columns = parseIndexNameAndColumns(def, "FULLTEXT INDEX")
		case strings.HasPrefix(normalized, "fulltext key "):
			name, columns = parseIndexNameAndColumns(def, "FULLTEXT KEY")
		case strings.HasPrefix(normalized, "vector index "):
			name, columns = parseIndexNameAndColumns(def, "VECTOR INDEX")
		case strings.HasPrefix(normalized, "spatial index "):
			name, columns = parseIndexNameAndColumns(def, "SPATIAL INDEX")
		case strings.HasPrefix(normalized, "spatial key "):
			name, columns = parseIndexNameAndColumns(def, "SPATIAL KEY")
		case strings.HasPrefix(normalized, "index "):
			name, columns = parseIndexNameAndColumns(def, "INDEX")
		case strings.HasPrefix(normalized, "key "):
			name, columns = parseIndexNameAndColumns(def, "KEY")
		}
		if name == "" || columns == "" {
			continue
		}
		parsedColumns := parseIndexColumnList(columns)
		if len(parsedColumns) == 0 {
			continue
		}
		observed[strings.ToLower(name)] = parsedColumns
	}
	return observed, true
}

func isPathHashIndexName(tableName, indexName string) bool {
	switch tableName + "." + strings.ToLower(indexName) {
	case "file_nodes.idx_path",
		"file_nodes.idx_parent",
		"uploads.idx_upload_path",
		"uploads.idx_uploads_active":
		return true
	default:
		return false
	}
}

func expectedPathHashIndexColumns(tableName, indexName string) []string {
	switch tableName + "." + strings.ToLower(indexName) {
	case "file_nodes.idx_path":
		return []string{"path_hash"}
	case "file_nodes.idx_parent":
		return []string{"parent_path_hash", "name"}
	case "uploads.idx_upload_path":
		return []string{"target_path_hash", "status"}
	case "uploads.idx_uploads_active":
		return []string{"active_target_path_hash"}
	default:
		return nil
	}
}

func dropPathHashIndexSQL(tableName, indexName string) string {
	switch tableName + "." + strings.ToLower(indexName) {
	case "file_nodes.idx_path":
		return "ALTER TABLE file_nodes DROP INDEX idx_path"
	case "file_nodes.idx_parent":
		return "ALTER TABLE file_nodes DROP INDEX idx_parent"
	case "uploads.idx_upload_path":
		return "ALTER TABLE uploads DROP INDEX idx_upload_path"
	case "uploads.idx_uploads_active":
		return "ALTER TABLE uploads DROP INDEX idx_uploads_active"
	default:
		return ""
	}
}

func parseObservedTiDBIndexes(createStmt string) (map[string]struct{}, bool) {
	_, defs, ok, err := parseCreateTableStatement(createStmt)
	if err != nil || !ok {
		return nil, false
	}
	observed := make(map[string]struct{})
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		normalized := normalizeSQLFragment(def)
		switch {
		case strings.HasPrefix(normalized, "primary key") || strings.Contains(normalized, " primary key"):
			observed["primary"] = struct{}{}
		case strings.HasPrefix(normalized, "constraint "):
			if name, _, ok := parseConstraintUniqueIndexDefinition(def); ok {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "unique key "):
			if name, _ := parseIndexNameAndColumns(def, "UNIQUE KEY"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "unique index "):
			if name, _ := parseIndexNameAndColumns(def, "UNIQUE INDEX"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "fulltext index "):
			if name, _ := parseIndexNameAndColumns(def, "FULLTEXT INDEX"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "fulltext key "):
			if name, _ := parseIndexNameAndColumns(def, "FULLTEXT KEY"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "vector index "):
			if name, _ := parseIndexNameAndColumns(def, "VECTOR INDEX"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "spatial index "):
			if name, _ := parseIndexNameAndColumns(def, "SPATIAL INDEX"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "spatial key "):
			if name, _ := parseIndexNameAndColumns(def, "SPATIAL KEY"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "index "):
			if name, _ := parseIndexNameAndColumns(def, "INDEX"); name != "" {
				observed[name] = struct{}{}
			}
		case strings.HasPrefix(normalized, "key "):
			if name, _ := parseIndexNameAndColumns(def, "KEY"); name != "" {
				observed[name] = struct{}{}
			}
		}
	}
	return observed, true
}

func parseIndexColumnList(def string) []string {
	inner, ok := parseParenthesizedList(def)
	if !ok {
		return nil
	}
	parts := splitTopLevelComma(inner)
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		name := parseColumnReferenceName(part)
		if name == "" {
			return nil
		}
		columns = append(columns, name)
	}
	return columns
}

func parseParenthesizedList(s string) (string, bool) {
	open := strings.Index(s, "(")
	if open < 0 {
		return "", false
	}
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(s[open+1 : i]), true
			}
		}
	}
	return "", false
}

func parsePrimaryKeyColumnsFromCreateStatement(createStmt string) ([]string, bool) {
	_, defs, ok, err := parseCreateTableStatement(createStmt)
	if err != nil || !ok {
		return nil, false
	}
	for _, def := range splitTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		if pkSpec, ok := parsePrimaryKeyDefinition(def); ok {
			return pkSpec.columns, true
		}
		if columnName, ok := parseInlinePrimaryKeyColumn(def); ok {
			return []string{columnName}, true
		}
	}
	return nil, false
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func missingTableDiff(table tidbTableSpec) tidbSchemaDiff {
	detail := fmt.Sprintf("%s schema contract: missing table", table.name)
	if table.createStatement == "" {
		detail = fmt.Sprintf("%s schema contract: missing table and no repair statement available", table.name)
	}
	return tidbSchemaDiff{
		kind:      tidbSchemaDiffMissingTable,
		tableName: table.name,
		detail:    detail,
		repairSQL: table.createStatement,
	}
}

func missingTableAndIndexDiffs(table tidbTableSpec) []tidbSchemaDiff {
	diffs := []tidbSchemaDiff{missingTableDiff(table)}
	for _, name := range sortedIndexNames(table.indexes) {
		spec := table.indexes[name]
		diffs = append(diffs, tidbSchemaDiff{
			kind:      tidbSchemaDiffMissingIndex,
			tableName: table.name,
			detail:    fmt.Sprintf("%s schema contract: missing %s index", table.name, name),
			repairSQL: spec.createSQL,
		})
	}
	return diffs
}

func plannedTiDBSchemaRepairs(diffs []tidbSchemaDiff) []string {
	seen := make(map[string]struct{})
	plans := make([]string, 0, len(diffs))
	deferHashIndexes := hasMissingPathHashColumnDiff(diffs)
	deferPathColumnWidening := deferHashIndexes ||
		hasPathHashIndexColumnMismatchDiff(diffs) ||
		hasLegacyUploadActiveTargetPathDiff(diffs)
	deferActiveUploadHashColumn := hasMissingColumnDiff(diffs, "uploads", "target_path_hash")
	for _, diff := range diffs {
		if diff.repairSQL == "" {
			continue
		}
		if deferPathColumnWidening && isPathColumnWideningRepair(diff) {
			continue
		}
		if deferActiveUploadHashColumn &&
			diff.kind == tidbSchemaDiffMissingColumn &&
			diff.tableName == "uploads" &&
			diff.columnName == "active_target_path_hash" {
			continue
		}
		if deferHashIndexes && isPathHashIndexRepair(diff) {
			continue
		}
		if !isSafeTiDBRepairDiff(diff) {
			continue
		}
		if _, ok := seen[diff.repairSQL]; ok {
			continue
		}
		seen[diff.repairSQL] = struct{}{}
		plans = append(plans, diff.repairSQL)
	}
	return plans
}

func hasPathHashIndexColumnMismatchDiff(diffs []tidbSchemaDiff) bool {
	for _, diff := range diffs {
		if diff.kind == tidbSchemaDiffMissingIndex &&
			isPathHashIndexRepair(diff) &&
			strings.Contains(diff.detail, "index columns") {
			return true
		}
	}
	return false
}

func isPathColumnWideningRepair(diff tidbSchemaDiff) bool {
	if diff.kind != tidbSchemaDiffColumnType {
		return false
	}
	switch diff.tableName + "." + diff.columnName {
	case "file_nodes.path",
		"file_nodes.parent_path",
		"uploads.target_path":
		return true
	default:
		return false
	}
}

func hasMissingColumnDiff(diffs []tidbSchemaDiff, tableName, columnName string) bool {
	for _, diff := range diffs {
		if diff.kind == tidbSchemaDiffMissingColumn &&
			diff.tableName == tableName &&
			diff.columnName == columnName {
			return true
		}
	}
	return false
}

func hasMissingPathHashColumnDiff(diffs []tidbSchemaDiff) bool {
	for _, diff := range diffs {
		if diff.kind != tidbSchemaDiffMissingColumn {
			continue
		}
		switch diff.tableName + "." + diff.columnName {
		case "file_nodes.path_hash",
			"file_nodes.parent_path_hash",
			"uploads.target_path_hash",
			"uploads.active_target_path_hash":
			return true
		}
	}
	return false
}

func isPathHashIndexRepair(diff tidbSchemaDiff) bool {
	if diff.kind != tidbSchemaDiffMissingIndex {
		return false
	}
	switch diff.tableName {
	case "file_nodes":
		return strings.Contains(diff.repairSQL, "idx_path") ||
			strings.Contains(diff.repairSQL, "idx_parent")
	case "uploads":
		return strings.Contains(diff.repairSQL, "idx_upload_path") ||
			strings.Contains(diff.repairSQL, "idx_uploads_active")
	default:
		return false
	}
}

func isSafeTiDBRepairDiff(diff tidbSchemaDiff) bool {
	switch diff.kind {
	case tidbSchemaDiffMissingTable:
		return true
	case tidbSchemaDiffMissingColumn:
		return isSafeAddColumnRepairSQL(diff.repairSQL)
	case tidbSchemaDiffMissingIndex:
		return isSafeAddIndexRepairSQL(diff.repairSQL) || isSafeDropPathHashIndexRepairSQL(diff.repairSQL)
	case tidbSchemaDiffColumnType:
		return isSafeModifyColumnRepairSQL(diff)
	case tidbSchemaDiffExtraColumn:
		return isSafeDropLegacyUploadActiveTargetPathRepairSQL(diff)
	default:
		return false
	}
}

func isSafeAddColumnRepairSQL(sqlText string) bool {
	// STORED GENERATED VECTOR columns whose expression uses EMBED_TEXT are
	// safe to add to existing tables via ALTER TABLE in the correctness sense:
	// TiDB computes the values server-side rather than requiring the client to
	// backfill. Note that this still materializes one EMBED_TEXT call per
	// existing row at ALTER time, which can be slow and carry inference cost
	// for large tables. This covers the description_embedding column introduced
	// for auto-embedding mode.
	normalized := normalizeSQLFragment(sqlText)
	if strings.Contains(normalized, " generated ") &&
		strings.Contains(normalized, " stored") &&
		strings.Contains(normalized, " vector(") &&
		strings.Contains(normalized, "embed_text(") {
		return true
	}
	if normalized == "alter table uploads add column active_target_path_hash varchar(64) as (case when status = 'uploading' then target_path_hash else null end) virtual" {
		return true
	}
	return schemaspec.IsSafeAddColumnRepairSQL(sqlText)
}

func isSafeModifyColumnRepairSQL(diff tidbSchemaDiff) bool {
	n := strings.TrimSuffix(normalizeSQLFragment(diff.repairSQL), ";")
	switch diff.tableName + "." + diff.columnName {
	case "file_nodes.path":
		return n == "alter table file_nodes modify column path text not null"
	case "file_nodes.parent_path":
		return n == "alter table file_nodes modify column parent_path text not null"
	case "uploads.target_path":
		return n == "alter table uploads modify column target_path text not null"
	default:
		return false
	}
}

func hasLegacyUploadActiveTargetPathDiff(diffs []tidbSchemaDiff) bool {
	for _, diff := range diffs {
		if diff.kind == tidbSchemaDiffExtraColumn &&
			diff.tableName == "uploads" &&
			diff.columnName == "active_target_path" {
			return true
		}
	}
	return false
}

func isSafeDropLegacyUploadActiveTargetPathRepairSQL(diff tidbSchemaDiff) bool {
	if diff.tableName != "uploads" || diff.columnName != "active_target_path" {
		return false
	}
	return normalizeSQLFragment(diff.repairSQL) == "alter table uploads drop column active_target_path"
}

func isSafeAddIndexRepairSQL(sqlText string) bool {
	normalized := normalizeSQLFragment(sqlText)
	if strings.HasPrefix(normalized, "create index ") ||
		strings.HasPrefix(normalized, "create unique index ") ||
		strings.HasPrefix(normalized, "create fulltext index ") ||
		strings.HasPrefix(normalized, "create vector index ") ||
		strings.HasPrefix(normalized, "create spatial index ") {
		return true
	}
	if strings.HasPrefix(normalized, "alter table ") {
		if strings.Contains(normalized, " add unique index ") || strings.Contains(normalized, " add unique key ") {
			return true
		}
		if strings.Contains(normalized, " add fulltext index ") || strings.Contains(normalized, " add vector index ") {
			// FULLTEXT and VECTOR indexes are always safe to add on an existing
			// table in auto-embedding mode: TiDB Cloud supports the syntax, and
			// applyTiDBSchemaRepairs will gracefully skip with a warning if the
			// current TiDB version does not.
			return true
		}
		if strings.Contains(normalized, " add index ") || strings.Contains(normalized, " add key ") {
			return true
		}
	}
	return false
}

func isSafeDropPathHashIndexRepairSQL(sqlText string) bool {
	switch normalizeSQLFragment(sqlText) {
	case "alter table file_nodes drop index idx_path",
		"alter table file_nodes drop index idx_parent",
		"alter table uploads drop index idx_upload_path",
		"alter table uploads drop index idx_uploads_active":
		return true
	default:
		return false
	}
}

func applyTiDBSchemaRepairs(ctx context.Context, db *sql.DB, statements []string) error {
	if len(statements) == 0 {
		return nil
	}
	for i, stmt := range statements {
		start := time.Now()
		snippet := schemaStatementSnippet(stmt)
		logger.Info(ctx, "tenant_tidb_schema_repair_statement_started",
			zap.Int("statement_index", i+1),
			zap.Int("statement_count", len(statements)),
			zap.String("statement", snippet))
		if isUniqueIndexRepairSQL(stmt) {
			repair, ok := parseUniqueIndexRepairStatement(stmt)
			if !ok {
				return fmt.Errorf("preflight unique index repair %q: unsupported statement", schemaStatementSnippet(stmt))
			}
			if err := ensureUniqueIndexRepairSafe(ctx, db, repair); err != nil {
				return err
			}
		}
		if isFulltextOrVectorIndexRepairSQL(stmt) && !strings.Contains(normalizeSQLFragment(stmt), "add_columnar_replica_on_demand") {
			stmt += " ADD_COLUMNAR_REPLICA_ON_DEMAND"
			snippet = schemaStatementSnippet(stmt)
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if isIgnorableTiDBSchemaError(err) {
				logger.Info(ctx, "tenant_tidb_schema_repair_statement_skipped_existing",
					zap.Int("statement_index", i+1),
					zap.Int("statement_count", len(statements)),
					zap.String("statement", snippet),
					zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
					zap.Error(err))
				continue
			}
			// FULLTEXT and VECTOR index repairs may fail with optional-feature
			// errors on TiDB versions or configurations that do not support
			// them (e.g. 8200: FULLTEXT index must specify one column name,
			// 1105: FULLTEXT index is not supported). Treat these the same as
			// when the statement was skipped during initial provisioning.
			if isFulltextOrVectorIndexRepairSQL(stmt) && isIgnorableOptionalSchemaError(err) {
				logger.Warn(ctx, "tenant_tidb_schema_repair_optional_index_skipped",
					zap.Int("statement_index", i+1),
					zap.Int("statement_count", len(statements)),
					zap.String("statement", snippet),
					zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
					zap.Error(err))
				continue
			}
			logger.Error(ctx, "tenant_tidb_schema_repair_statement_failed",
				zap.Int("statement_index", i+1),
				zap.Int("statement_count", len(statements)),
				zap.String("statement", snippet),
				zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
				zap.Error(err))
			return fmt.Errorf("apply tidb schema repair %q: %w", snippet, err)
		}
		logger.Info(ctx, "tenant_tidb_schema_repair_statement_finished",
			zap.Int("statement_index", i+1),
			zap.Int("statement_count", len(statements)),
			zap.String("statement", snippet),
			zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	}
	return nil
}

func isUniqueIndexRepairSQL(sqlText string) bool {
	normalized := normalizeSQLFragment(sqlText)
	if strings.HasPrefix(normalized, "create unique index ") {
		return true
	}
	return strings.HasPrefix(normalized, "alter table ") &&
		(strings.Contains(normalized, " add unique index ") || strings.Contains(normalized, " add unique key "))
}

func isFulltextOrVectorIndexRepairSQL(sqlText string) bool {
	normalized := normalizeSQLFragment(sqlText)
	return strings.Contains(normalized, " add fulltext index ") ||
		strings.Contains(normalized, " add vector index ")
}

func parseUniqueIndexRepairStatement(stmt string) (tidbUniqueIndexRepair, bool) {
	if tableName, indexName, columns, ok := parseCreateUniqueIndexRepairStatement(stmt); ok {
		return tidbUniqueIndexRepair{tableName: tableName, indexName: indexName, columns: columns}, true
	}
	if tableName, indexName, columns, ok := parseAlterTableAddUniqueIndexRepairStatement(stmt); ok {
		return tidbUniqueIndexRepair{tableName: tableName, indexName: indexName, columns: columns}, true
	}
	return tidbUniqueIndexRepair{}, false
}

func parseCreateUniqueIndexRepairStatement(stmt string) (tableName, indexName string, columns []string, ok bool) {
	trimmed := strings.TrimSpace(stmt)
	upper := strings.ToUpper(trimmed)
	const prefix = "CREATE UNIQUE INDEX "
	if !strings.HasPrefix(upper, prefix) {
		return "", "", nil, false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	name, remainder := splitIdentifierAndSuffix(rest)
	if name == "" || remainder == "" {
		return "", "", nil, false
	}
	remainder = strings.TrimSpace(remainder)
	upperRemainder := strings.ToUpper(remainder)
	if !strings.HasPrefix(upperRemainder, "ON ") {
		return "", "", nil, false
	}
	afterOn := strings.TrimSpace(remainder[len("ON "):])
	table, columnRemainder := splitIdentifierAndSuffix(afterOn)
	if table == "" || columnRemainder == "" {
		return "", "", nil, false
	}
	parsedColumns := parseIndexColumnList(columnRemainder)
	if len(parsedColumns) == 0 {
		return "", "", nil, false
	}
	return strings.ToLower(table), strings.ToLower(name), parsedColumns, true
}

func parseAlterTableAddUniqueIndexRepairStatement(stmt string) (tableName, indexName string, columns []string, ok bool) {
	trimmed := strings.TrimSpace(stmt)
	upper := strings.ToUpper(trimmed)
	const prefix = "ALTER TABLE "
	if !strings.HasPrefix(upper, prefix) {
		return "", "", nil, false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	table, remainder := splitIdentifierAndRest(rest)
	if table == "" || remainder == "" {
		return "", "", nil, false
	}
	remainder = strings.TrimSpace(remainder)
	normalizedRemainder := normalizeSQLFragment(remainder)
	for _, marker := range []string{"add unique index ", "add unique key "} {
		pos := strings.Index(normalizedRemainder, marker)
		if pos < 0 {
			continue
		}
		afterMarker := strings.TrimSpace(normalizedRemainder[pos+len(marker):])
		name, columnRemainder := splitIdentifierAndSuffix(afterMarker)
		if name == "" || columnRemainder == "" {
			return "", "", nil, false
		}
		parsedColumns := parseIndexColumnList(columnRemainder)
		if len(parsedColumns) == 0 {
			return "", "", nil, false
		}
		return strings.ToLower(table), strings.ToLower(name), parsedColumns, true
	}
	return "", "", nil, false
}

func ensureUniqueIndexRepairSafe(ctx context.Context, db *sql.DB, repair tidbUniqueIndexRepair) error {
	start := time.Now()
	logger.Info(ctx, "tenant_tidb_schema_unique_index_preflight_started",
		zap.String("table", repair.tableName),
		zap.String("index", repair.indexName),
		zap.Strings("columns", repair.columns))
	var exists int
	err := db.QueryRowContext(ctx, buildUniqueIndexDuplicateCheckSQL(repair)).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		logger.Info(ctx, "tenant_tidb_schema_unique_index_preflight_finished",
			zap.String("table", repair.tableName),
			zap.String("index", repair.indexName),
			zap.Bool("duplicates_found", false),
			zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
		return nil
	}
	if err != nil {
		return fmt.Errorf("preflight unique index repair %s on %s: %w", repair.indexName, repair.tableName, err)
	}
	logger.Warn(ctx, "tenant_tidb_schema_unique_index_preflight_finished",
		zap.String("table", repair.tableName),
		zap.String("index", repair.indexName),
		zap.Bool("duplicates_found", true),
		zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0))
	return fmt.Errorf("cannot auto-repair unique index %s on %s: duplicate rows exist for columns (%s)", repair.indexName, repair.tableName, strings.Join(repair.columns, ", "))
}

func summarizeTiDBSchemaDiffs(diffs []tidbSchemaDiff) []string {
	if len(diffs) == 0 {
		return nil
	}
	items := make([]string, 0, len(diffs))
	for _, diff := range diffs {
		item := string(diff.kind) + ": " + diff.detail
		if diff.repairSQL != "" {
			item += " | repair=" + schemaStatementSnippet(diff.repairSQL)
		}
		items = append(items, item)
	}
	return items
}

func summarizeSchemaStatements(statements []string) []string {
	if len(statements) == 0 {
		return nil
	}
	items := make([]string, 0, len(statements))
	for _, stmt := range statements {
		items = append(items, schemaStatementSnippet(stmt))
	}
	return items
}

func buildUniqueIndexDuplicateCheckSQL(repair tidbUniqueIndexRepair) string {
	quotedColumns := make([]string, 0, len(repair.columns))
	nonNullPredicates := make([]string, 0, len(repair.columns))
	for _, column := range repair.columns {
		quoted := quoteSQLIdentifier(column)
		quotedColumns = append(quotedColumns, quoted)
		nonNullPredicates = append(nonNullPredicates, quoted+" IS NOT NULL")
	}
	return fmt.Sprintf("SELECT 1 FROM %s WHERE %s GROUP BY %s HAVING COUNT(*) > 1 LIMIT 1",
		quoteSQLIdentifier(repair.tableName),
		strings.Join(nonNullPredicates, " AND "),
		strings.Join(quotedColumns, ", "))
}

func quoteSQLIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func isIgnorableTiDBSchemaError(err error) bool {
	return schemaspec.IsIgnorableMySQLError(err)
}

func validateTiDBAutoEmbeddingDiffs(meta tidbTableMeta) []tidbSchemaDiff {
	return validateTiDBAutoEmbeddingDiffsWithConfig(meta, currentTiDBAutoEmbeddingRenderConfig())
}

func validateTiDBAutoEmbeddingDiffsWithConfig(meta tidbTableMeta, cfg tidbAutoEmbeddingRenderConfig) []tidbSchemaDiff {
	return validateTiDBAutoEmbeddingTableDiffs(meta, "semantic", cfg)
}

func validateTiDBAutoEmbeddingFilesDiffsWithConfig(meta tidbTableMeta, cfg tidbAutoEmbeddingRenderConfig) []tidbSchemaDiff {
	return validateTiDBAutoEmbeddingTableDiffs(meta, "files", cfg)
}

func validateTiDBAutoEmbeddingTableDiffs(meta tidbTableMeta, tableName string, cfg tidbAutoEmbeddingRenderConfig) []tidbSchemaDiff {
	var diffs []tidbSchemaDiff
	for _, spec := range []struct {
		column        string
		source        string
		allowWritable bool
	}{
		{"embedding", "content_text", false},
		{"description_embedding", "description", true},
	} {
		col, err := meta.requireColumn(spec.column)
		if err != nil {
			return nil
		}
		extra := normalizeSQLFragment(col.extra)
		isStoredGenerated := strings.Contains(extra, "generated") && strings.Contains(extra, "stored")
		if !isStoredGenerated {
			if spec.allowWritable {
				expr := normalizeSQLFragment(col.generationExpression)
				if !strings.Contains(extra, "generated") && expr == "" {
					continue
				}
			}
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffTableContract,
				tableName:  tableName,
				columnName: spec.column,
				detail:     fmt.Sprintf("%s schema contract: %s column must be a stored generated column", tableName, spec.column),
			})
			continue
		}
		expr := normalizeSQLFragment(col.generationExpression)
		checks := []struct {
			pattern string
			errMsg  string
		}{
			{"embed_text(", fmt.Sprintf("%s schema contract: %s generated expression must use EMBED_TEXT", tableName, spec.column)},
			{normalizeSQLFragment(cfg.model), fmt.Sprintf("%s schema contract: %s model contract mismatch", tableName, spec.column)},
			{spec.source, fmt.Sprintf("%s schema contract: generated expression must derive from %s", tableName, spec.source)},
			{normalizeSQLFragment(cfg.optionsJSON), fmt.Sprintf("%s schema contract: %s dimensions option mismatch", tableName, spec.column)},
		}
		for _, check := range checks {
			if !strings.Contains(expr, check.pattern) {
				diffs = append(diffs, tidbSchemaDiff{
					kind:       tidbSchemaDiffTableContract,
					tableName:  tableName,
					columnName: spec.column,
					detail:     check.errMsg,
				})
			}
		}
	}
	return diffs
}

func validateTiDBAppEmbeddingDiffs(meta tidbTableMeta) []tidbSchemaDiff {
	return validateTiDBAppEmbeddingTableDiffs(meta, "semantic")
}

func validateTiDBAppEmbeddingFilesDiffs(meta tidbTableMeta) []tidbSchemaDiff {
	return validateTiDBAppEmbeddingTableDiffs(meta, "files")
}

func validateTiDBAppEmbeddingTableDiffs(meta tidbTableMeta, tableName string) []tidbSchemaDiff {
	var diffs []tidbSchemaDiff
	for _, colName := range []string{"embedding", "description_embedding"} {
		col, err := meta.requireColumn(colName)
		if err != nil {
			return nil
		}
		extra := normalizeSQLFragment(col.extra)
		if strings.Contains(extra, "generated") {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffTableContract,
				tableName:  tableName,
				columnName: colName,
				detail:     fmt.Sprintf("%s schema contract: %s column must be writable in app mode", tableName, colName),
			})
		}
		if expr := normalizeSQLFragment(col.generationExpression); expr != "" {
			diffs = append(diffs, tidbSchemaDiff{
				kind:       tidbSchemaDiffTableContract,
				tableName:  tableName,
				columnName: colName,
				detail:     fmt.Sprintf("%s schema contract: %s column must not define a generation expression in app mode", tableName, colName),
			})
		}
	}
	return diffs
}

func schemaDiffsToError(diffs []tidbSchemaDiff) error {
	if len(diffs) == 0 {
		return nil
	}
	return errors.New(diffs[0].detail)
}

func sortedColumnNames(columns map[string]tidbColumnSpec) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedIndexNames(indexes map[string]tidbIndexSpec) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1146
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "error 1146") ||
		(strings.Contains(msg, "table") && strings.Contains(msg, "doesn't exist")) ||
		(strings.Contains(msg, "relation") && strings.Contains(msg, "does not exist"))
}

func isMissingColumnError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1054
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "error 1054") ||
		strings.Contains(msg, "unknown column") ||
		(strings.Contains(msg, "column") && strings.Contains(msg, "does not exist"))
}
