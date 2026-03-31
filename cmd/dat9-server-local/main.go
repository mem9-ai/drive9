// Command dat9-server-local starts a single-tenant dat9 HTTP server
// backed directly by DAT9_LOCAL_DSN for local validation.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/embedding"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/server"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"go.uber.org/zap"
)

const (
	defaultListenAddr = "127.0.0.1:9009"
	defaultS3Dir      = "/tmp/dat9-local-s3"
)

func main() {
	if len(os.Args) > 2 {
		usage()
	}

	addr := envOr("DAT9_LISTEN_ADDR", defaultListenAddr)
	if len(os.Args) == 2 {
		addr = os.Args[1]
	}

	srvLogger, err := logger.NewServerLogger()
	if err != nil {
		die(fmt.Errorf("create logger: %w", err))
	}
	defer func() { _ = srvLogger.Sync() }()
	logger.Set(srvLogger)

	localDSN := strings.TrimSpace(os.Getenv("DAT9_LOCAL_DSN"))
	if localDSN == "" {
		die(fmt.Errorf("DAT9_LOCAL_DSN is required"))
	}
	localInitSchema := envBool("DAT9_LOCAL_INIT_SCHEMA", true)

	// Local validation should be able to bootstrap a fresh tenant database without
	// going through the multi-tenant provision flow.
	if localInitSchema {
		if err := tenant.InitTiDBTenantSchema(localDSN); err != nil {
			die(fmt.Errorf("init local tenant schema: %w", err))
		}
	}

	store, err := datastore.Open(localDSN)
	if err != nil {
		die(fmt.Errorf("open local datastore: %w", err))
	}
	defer func() { _ = store.Close() }()

	backendOpts, err := buildBackendOptionsFromEnv()
	if err != nil {
		die(err)
	}
	backendOpts.DatabaseAutoEmbedding = shouldUseLocalDatabaseAutoEmbedding(store.DB(), localInitSchema)
	semanticEmbedder, workerOpts, err := buildSemanticWorkerConfigFromEnv()
	if err != nil {
		die(err)
	}
	// Keep the local entrypoint aligned with dat9-server: if only the background
	// embedder is configured, grep reuses it for app-side query embedding.
	if semanticEmbedder != nil && backendOpts.QueryEmbedding.Client == nil {
		backendOpts.QueryEmbedding = backend.QueryEmbeddingOptions{Client: semanticEmbedder}
	}

	s3Dir := envOr("DAT9_S3_DIR", defaultS3Dir)
	// Even in local single-tenant mode we keep the same S3-facing upload code path
	// by backing it with the local mock implementation.
	localS3, err := s3client.NewLocal(s3Dir, publicBaseURL(addr)+"/s3")
	if err != nil {
		die(fmt.Errorf("create local s3: %w", err))
	}

	b, err := backend.NewWithS3ModeAndOptions(store, localS3, true, backendOpts)
	if err != nil {
		die(fmt.Errorf("create local backend: %w", err))
	}
	defer b.Close()

	srv := server.NewWithConfig(server.Config{
		Backend:          b,
		LocalS3:          localS3,
		S3Dir:            s3Dir,
		Logger:           srvLogger,
		SemanticEmbedder: semanticEmbedder,
		SemanticWorkers:  workerOpts,
	})
	defer srv.Close()

	logger.Info(context.Background(), "local_server_mode",
		zap.String("listen_addr", addr),
		zap.String("local_dsn", redactDSN(localDSN)),
		zap.String("s3_dir", s3Dir),
		zap.Bool("local_init_schema", localInitSchema),
		zap.Bool("database_auto_embedding", backendOpts.DatabaseAutoEmbedding))

	// Bind first so we can emit a definitive "started" log only after the socket
	// is actually listening.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		die(fmt.Errorf("listen: %w", err))
	}
	logger.Info(context.Background(), "local_server_started",
		zap.String("listen_addr", addr),
		zap.String("public_url", publicBaseURL(addr)))
	defer func() { _ = ln.Close() }()

	die(http.Serve(ln, srv))
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9-server-local [listen-addr]

environment:
  DAT9_LISTEN_ADDR serve listen address (default: 127.0.0.1:9009)
  DAT9_PUBLIC_URL  externally reachable base URL (optional for loopback listen address)
  DAT9_LOCAL_DSN   local tenant TiDB/MySQL DSN (required)
  DAT9_LOCAL_INIT_SCHEMA initialize tenant schema on startup (default: false)
  DAT9_S3_DIR      local S3 mock root directory (default: /tmp/dat9-local-s3)

  Query embedding (app-side semantic query embedding for grep):
  DAT9_QUERY_EMBED_API_BASE OpenAI-compatible base URL (optional)
  DAT9_QUERY_EMBED_API_KEY  API key for DAT9_QUERY_EMBED_API_BASE (optional)
  DAT9_QUERY_EMBED_MODEL    model name for query embedding (optional)
  DAT9_QUERY_EMBED_DIMENSIONS optional embedding dimensions override
  DAT9_QUERY_EMBED_TIMEOUT_SECONDS embed request timeout seconds (default: 20)

  Async semantic embedding worker:
  DAT9_EMBED_API_BASE OpenAI-compatible base URL for background embedding (optional)
  DAT9_EMBED_API_KEY  API key for DAT9_EMBED_API_BASE (optional)
  DAT9_EMBED_MODEL    model name for background embedding (optional)
  DAT9_EMBED_DIMENSIONS optional embedding dimensions override
  DAT9_EMBED_TIMEOUT_SECONDS embed request timeout seconds (default: 20)
  DAT9_SEMANTIC_WORKERS number of background workers (default: 1)
  DAT9_SEMANTIC_POLL_INTERVAL_MS worker poll interval in milliseconds (default: 200)
  DAT9_SEMANTIC_LEASE_SECONDS task lease duration in seconds (default: 30)
  DAT9_SEMANTIC_RECOVER_INTERVAL_MS recover sweep interval in milliseconds (default: 5000)
  DAT9_SEMANTIC_RETRY_BASE_MS base retry backoff in milliseconds (default: 200)
  DAT9_SEMANTIC_RETRY_MAX_MS max retry backoff in milliseconds (default: 30000)
  DAT9_SEMANTIC_PER_TENANT_CONCURRENCY max concurrent tasks per tenant (default: 1)

  Image extraction (async image -> text for search):
  DAT9_IMAGE_EXTRACT_ENABLED true|false (default: false)
  DAT9_IMAGE_EXTRACT_QUEUE_SIZE buffered task queue size (default: 128)
  DAT9_IMAGE_EXTRACT_WORKERS number of workers (default: 1)
  DAT9_IMAGE_EXTRACT_MAX_BYTES max image bytes processed per task (default: 8388608)
  DAT9_IMAGE_EXTRACT_TIMEOUT_SECONDS extractor timeout seconds (default: 20)
  DAT9_IMAGE_EXTRACT_MAX_TEXT_BYTES max extracted text stored in files.content_text (default: 8192)
  DAT9_IMAGE_EXTRACT_API_BASE OpenAI-compatible base URL (optional)
  DAT9_IMAGE_EXTRACT_API_KEY  API key for DAT9_IMAGE_EXTRACT_API_BASE (optional)
  DAT9_IMAGE_EXTRACT_MODEL    model name for vision extraction (optional)
  DAT9_IMAGE_EXTRACT_PROMPT   custom extraction prompt (optional)
  DAT9_IMAGE_EXTRACT_MAX_TOKENS max model output tokens (default: 256)
`)
	os.Exit(2)
}

func die(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "dat9-server-local: %v\n", err)
	os.Exit(1)
}

func publicBaseURL(listenAddr string) string {
	if v := strings.TrimRight(os.Getenv("DAT9_PUBLIC_URL"), "/"); v != "" {
		return v
	}

	switch {
	case strings.HasPrefix(listenAddr, "127.0.0.1:"),
		strings.HasPrefix(listenAddr, "localhost:"),
		strings.HasPrefix(listenAddr, "[::1]:"):
		return "http://" + listenAddr
	case strings.HasPrefix(listenAddr, "http://"), strings.HasPrefix(listenAddr, "https://"):
		return strings.TrimRight(listenAddr, "/")
	default:
		fmt.Fprintf(os.Stderr, "dat9-server-local: DAT9_PUBLIC_URL is required when listen address is %q (wildcard or non-loopback).\n", listenAddr)
		os.Exit(1)
		return ""
	}
}

var localAutoEmbeddingSchemaValidator = tenant.ValidateTiDBAutoEmbeddingSchema

func shouldUseLocalDatabaseAutoEmbedding(db *sql.DB, schemaInitialized bool) bool {
	if schemaInitialized {
		return true
	}
	if db == nil {
		return false
	}
	return localAutoEmbeddingSchemaValidator(db) == nil
}

func buildBackendOptionsFromEnv() (backend.Options, error) {
	var opts backend.Options

	queryBaseURL := strings.TrimSpace(os.Getenv("DAT9_QUERY_EMBED_API_BASE"))
	queryAPIKey := strings.TrimSpace(os.Getenv("DAT9_QUERY_EMBED_API_KEY"))
	queryModel := strings.TrimSpace(os.Getenv("DAT9_QUERY_EMBED_MODEL"))
	queryConfigured := queryBaseURL != "" || queryAPIKey != "" || queryModel != ""
	if queryConfigured {
		if queryBaseURL == "" || queryAPIKey == "" || queryModel == "" {
			return backend.Options{}, fmt.Errorf("DAT9_QUERY_EMBED_API_BASE, DAT9_QUERY_EMBED_API_KEY and DAT9_QUERY_EMBED_MODEL must be set together")
		}
		queryClient, err := embedding.NewOpenAIClient(embedding.OpenAIClientConfig{
			BaseURL:    queryBaseURL,
			APIKey:     queryAPIKey,
			Model:      queryModel,
			Dimensions: envInt("DAT9_QUERY_EMBED_DIMENSIONS", 0),
			Timeout:    time.Duration(envInt("DAT9_QUERY_EMBED_TIMEOUT_SECONDS", 20)) * time.Second,
		})
		if err != nil {
			return backend.Options{}, fmt.Errorf("init query embedder: %w", err)
		}
		opts.QueryEmbedding = backend.QueryEmbeddingOptions{Client: queryClient}
		logger.Info(context.Background(), "query_embedding_mode_openai_compatible",
			zap.String("model", queryModel), zap.String("base_url", queryBaseURL))
	}

	if !envBool("DAT9_IMAGE_EXTRACT_ENABLED", false) {
		return opts, nil
	}

	async := backend.AsyncImageExtractOptions{
		Enabled:             true,
		QueueSize:           envInt("DAT9_IMAGE_EXTRACT_QUEUE_SIZE", 128),
		Workers:             envInt("DAT9_IMAGE_EXTRACT_WORKERS", 1),
		MaxImageBytes:       envInt64("DAT9_IMAGE_EXTRACT_MAX_BYTES", 8<<20),
		TaskTimeout:         time.Duration(envInt("DAT9_IMAGE_EXTRACT_TIMEOUT_SECONDS", 20)) * time.Second,
		MaxExtractTextBytes: envInt("DAT9_IMAGE_EXTRACT_MAX_TEXT_BYTES", 8<<10),
	}

	baseURL := strings.TrimSpace(os.Getenv("DAT9_IMAGE_EXTRACT_API_BASE"))
	apiKey := strings.TrimSpace(os.Getenv("DAT9_IMAGE_EXTRACT_API_KEY"))
	model := strings.TrimSpace(os.Getenv("DAT9_IMAGE_EXTRACT_MODEL"))
	prompt := strings.TrimSpace(os.Getenv("DAT9_IMAGE_EXTRACT_PROMPT"))
	maxTokens := envInt("DAT9_IMAGE_EXTRACT_MAX_TOKENS", 256)

	configured := baseURL != "" || apiKey != "" || model != ""
	if configured {
		if baseURL == "" || apiKey == "" || model == "" {
			return backend.Options{}, fmt.Errorf("DAT9_IMAGE_EXTRACT_API_BASE, DAT9_IMAGE_EXTRACT_API_KEY and DAT9_IMAGE_EXTRACT_MODEL must be set together")
		}
		extractor, err := backend.NewOpenAIImageTextExtractor(backend.OpenAIImageTextExtractorConfig{
			BaseURL:   baseURL,
			APIKey:    apiKey,
			Model:     model,
			Prompt:    prompt,
			MaxTokens: maxTokens,
			Timeout:   async.TaskTimeout,
		})
		if err != nil {
			return backend.Options{}, fmt.Errorf("init image extractor: %w", err)
		}
		async.Extractor = backend.NewFallbackImageTextExtractor(extractor, backend.NewBasicImageTextExtractor())
		logger.Info(context.Background(), "image_extract_mode_openai_compatible",
			zap.String("model", model), zap.String("base_url", baseURL))
	} else {
		async.Extractor = backend.NewBasicImageTextExtractor()
		logger.Info(context.Background(), "image_extract_mode_basic_fallback")
	}

	opts.AsyncImageExtract = async
	return opts, nil
}

func buildSemanticWorkerConfigFromEnv() (embedding.Client, server.SemanticWorkerOptions, error) {
	var opts server.SemanticWorkerOptions
	baseURL := strings.TrimSpace(os.Getenv("DAT9_EMBED_API_BASE"))
	apiKey := strings.TrimSpace(os.Getenv("DAT9_EMBED_API_KEY"))
	model := strings.TrimSpace(os.Getenv("DAT9_EMBED_MODEL"))
	configured := baseURL != "" || apiKey != "" || model != ""
	if !configured {
		return nil, opts, nil
	}
	if baseURL == "" || apiKey == "" || model == "" {
		return nil, opts, fmt.Errorf("DAT9_EMBED_API_BASE, DAT9_EMBED_API_KEY and DAT9_EMBED_MODEL must be set together")
	}
	client, err := embedding.NewOpenAIClient(embedding.OpenAIClientConfig{
		BaseURL:    baseURL,
		APIKey:     apiKey,
		Model:      model,
		Dimensions: envInt("DAT9_EMBED_DIMENSIONS", 0),
		Timeout:    time.Duration(envInt("DAT9_EMBED_TIMEOUT_SECONDS", 20)) * time.Second,
	})
	if err != nil {
		return nil, opts, fmt.Errorf("init semantic embedder: %w", err)
	}
	opts = server.SemanticWorkerOptions{
		Workers:              envInt("DAT9_SEMANTIC_WORKERS", 1),
		PollInterval:         time.Duration(envInt("DAT9_SEMANTIC_POLL_INTERVAL_MS", 200)) * time.Millisecond,
		LeaseDuration:        time.Duration(envInt("DAT9_SEMANTIC_LEASE_SECONDS", 30)) * time.Second,
		RecoverInterval:      time.Duration(envInt("DAT9_SEMANTIC_RECOVER_INTERVAL_MS", 5000)) * time.Millisecond,
		RetryBaseDelay:       time.Duration(envInt("DAT9_SEMANTIC_RETRY_BASE_MS", 200)) * time.Millisecond,
		RetryMaxDelay:        time.Duration(envInt("DAT9_SEMANTIC_RETRY_MAX_MS", 30000)) * time.Millisecond,
		PerTenantConcurrency: envInt("DAT9_SEMANTIC_PER_TENANT_CONCURRENCY", 1),
	}
	logger.Info(context.Background(), "semantic_embedding_mode_openai_compatible",
		zap.String("model", model), zap.String("base_url", baseURL))
	return client, opts, nil
}

func redactDSN(dsn string) string {
	if at := strings.Index(dsn, "@"); at >= 0 {
		prefix := dsn[:at]
		if colon := strings.Index(prefix, ":"); colon >= 0 {
			return prefix[:colon+1] + "***" + dsn[at:]
		}
	}
	return dsn
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func envInt64(key string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}
