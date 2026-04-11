// Command drive9-server-local starts a single-tenant drive9 HTTP server
// backed directly by DRIVE9_LOCAL_DSN for local validation.
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
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
	"go.uber.org/zap"
)

const (
	defaultListenAddr     = "127.0.0.1:9009"
	defaultS3Dir          = "/tmp/drive9-local-s3"
	envLocalEmbeddingMode = "DRIVE9_LOCAL_EMBEDDING_MODE"
)

func main() {
	startupCtx := context.Background()
	startupStart := time.Now()
	logLocalStartupStep(startupCtx, startupStart, startupStart, "process_start")

	if len(os.Args) > 2 {
		usage()
	}

	addr := envOr("DRIVE9_LISTEN_ADDR", defaultListenAddr)
	if len(os.Args) == 2 {
		addr = os.Args[1]
	}

	srvLogger, err := logger.NewServerLogger()
	if err != nil {
		die(fmt.Errorf("create logger: %w", err))
	}
	defer func() { _ = srvLogger.Sync() }()
	logger.Set(srvLogger)
	logLocalStartupStep(startupCtx, startupStart, time.Now(), "logger_ready")

	localDSN := strings.TrimSpace(os.Getenv("DRIVE9_LOCAL_DSN"))
	if localDSN == "" {
		die(fmt.Errorf("DRIVE9_LOCAL_DSN is required"))
	}
	localInitSchema := envBool("DRIVE9_LOCAL_INIT_SCHEMA", false)
	requestedEmbeddingMode, explicitEmbeddingMode, err := localEmbeddingModeFromEnv()
	if err != nil {
		die(err)
	}

	// Local validation should be able to bootstrap a fresh tenant database without
	// going through the multi-tenant provision flow.
	if localInitSchema {
		stepStart := time.Now()
		initMode := requestedEmbeddingMode
		if !explicitEmbeddingMode {
			initMode = schema.TiDBEmbeddingModeAuto
		}
		if err := localTiDBSchemaInitializer(localDSN, initMode); err != nil {
			die(fmt.Errorf("init local tenant schema: %w", err))
		}
		logLocalStartupStep(startupCtx, startupStart, stepStart, "init_local_tenant_schema",
			zap.String("embedding_mode", string(initMode)))
	}

	stepStart := time.Now()
	store, err := datastore.Open(localDSN)
	if err != nil {
		die(fmt.Errorf("open local datastore: %w", err))
	}
	defer func() { _ = store.Close() }()
	logLocalStartupStep(startupCtx, startupStart, stepStart, "open_local_datastore")

	stepStart = time.Now()
	backendOpts, err := buildBackendOptionsFromEnv()
	if err != nil {
		die(err)
	}
	maxUploadBytes := server.DefaultMaxUploadBytes
	if raw := os.Getenv("DRIVE9_MAX_UPLOAD_BYTES"); raw != "" {
		maxUploadBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || maxUploadBytes <= 0 {
			die(fmt.Errorf("invalid DRIVE9_MAX_UPLOAD_BYTES: must be a positive integer"))
		}
		if maxUploadBytes < 1<<20 {
			die(fmt.Errorf("DRIVE9_MAX_UPLOAD_BYTES too small: minimum 1048576 (1MiB)"))
		}
	}
	backendOpts.MaxUploadBytes = maxUploadBytes
	logLocalStartupStep(startupCtx, startupStart, stepStart, "build_backend_options")

	stepStart = time.Now()
	localEmbeddingMode, err := detectLocalTiDBEmbeddingMode(store.DB(), localInitSchema, requestedEmbeddingMode, explicitEmbeddingMode)
	if err != nil {
		die(fmt.Errorf("detect local embedding mode: %w", err))
	}
	backendOpts.DatabaseAutoEmbedding = localEmbeddingMode == schema.TiDBEmbeddingModeAuto
	logLocalStartupStep(startupCtx, startupStart, stepStart, "detect_local_embedding_mode",
		zap.String("embedding_mode", string(localEmbeddingMode)))

	stepStart = time.Now()
	semanticEmbedder, workerOpts, err := buildSemanticWorkerConfigFromEnv()
	if err != nil {
		die(err)
	}
	logLocalStartupStep(startupCtx, startupStart, stepStart, "build_semantic_worker_config")
	// Keep the local entrypoint aligned with drive9-server: if only the background
	// embedder is configured, grep reuses it for app-side query embedding.
	if semanticEmbedder != nil && backendOpts.QueryEmbedding.Client == nil {
		backendOpts.QueryEmbedding = backend.QueryEmbeddingOptions{Client: semanticEmbedder}
	}

	s3Dir := envOr("DRIVE9_S3_DIR", defaultS3Dir)
	// Even in local single-tenant mode we keep the same S3-facing upload code path
	// by backing it with the local mock implementation.
	stepStart = time.Now()
	localS3, err := s3client.NewLocal(s3Dir, publicBaseURL(addr)+"/s3")
	if err != nil {
		die(fmt.Errorf("create local s3: %w", err))
	}
	logLocalStartupStep(startupCtx, startupStart, stepStart, "create_local_s3")

	stepStart = time.Now()
	b, err := backend.NewWithS3ModeAndOptions(store, localS3, true, backendOpts)
	if err != nil {
		die(fmt.Errorf("create local backend: %w", err))
	}
	defer b.Close()
	logLocalStartupStep(startupCtx, startupStart, stepStart, "create_local_backend")

	stepStart = time.Now()
	srv := server.NewWithConfig(server.Config{
		Backend:          b,
		LocalS3:          localS3,
		S3Dir:            s3Dir,
		MaxUploadBytes:   maxUploadBytes,
		Logger:           srvLogger,
		SemanticEmbedder: semanticEmbedder,
		SemanticWorkers:  workerOpts,
	})
	defer srv.Close()
	logLocalStartupStep(startupCtx, startupStart, stepStart, "create_server")

	audioRuntime := backend.AsyncAudioExtractWillWireRuntime(backendOpts.AsyncAudioExtract)
	logger.Info(startupCtx, "local_server_mode",
		zap.String("listen_addr", addr),
		zap.String("local_dsn", redactDSN(localDSN)),
		zap.String("s3_dir", s3Dir),
		zap.Bool("local_init_schema", localInitSchema),
		zap.String("requested_embedding_mode", localEmbeddingModeLabel(requestedEmbeddingMode, explicitEmbeddingMode)),
		zap.String("embedding_mode", string(localEmbeddingMode)),
		zap.Bool("database_auto_embedding", backendOpts.DatabaseAutoEmbedding),
		zap.Bool("local_audio_extract_runtime", audioRuntime),
		zap.Duration("startup_elapsed", time.Since(startupStart)))

	// Bind first so we can emit a definitive "started" log only after the socket
	// is actually listening.
	stepStart = time.Now()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		die(fmt.Errorf("listen: %w", err))
	}
	logLocalStartupStep(startupCtx, startupStart, stepStart, "listen")
	logger.Info(startupCtx, "local_server_started",
		zap.String("listen_addr", addr),
		zap.String("public_url", publicBaseURL(addr)),
		zap.Duration("startup_elapsed", time.Since(startupStart)))
	defer func() { _ = ln.Close() }()

	die(http.Serve(ln, srv))
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: drive9-server-local [listen-addr]

environment:
  DRIVE9_LISTEN_ADDR serve listen address (default: 127.0.0.1:9009)
  DRIVE9_PUBLIC_URL  externally reachable base URL (optional for loopback listen address)
  DRIVE9_LOCAL_DSN   local tenant TiDB/MySQL DSN (required)
  DRIVE9_LOCAL_INIT_SCHEMA initialize tenant schema on startup (default: false)
  DRIVE9_LOCAL_EMBEDDING_MODE auto|app|detect (default: auto when initing schema, detect otherwise)
  DRIVE9_S3_DIR      local S3 mock root directory (default: /tmp/drive9-local-s3)

  Query embedding (app-side semantic query embedding for grep):
  DRIVE9_QUERY_EMBED_API_BASE OpenAI-compatible base URL (optional)
  DRIVE9_QUERY_EMBED_API_KEY  API key for DRIVE9_QUERY_EMBED_API_BASE (optional)
  DRIVE9_QUERY_EMBED_MODEL    model name for query embedding (optional)
  DRIVE9_QUERY_EMBED_DIMENSIONS optional embedding dimensions override
  DRIVE9_QUERY_EMBED_TIMEOUT_SECONDS embed request timeout seconds (default: 20)

  Async semantic embedding worker:
  DRIVE9_EMBED_API_BASE OpenAI-compatible base URL for background embedding (optional)
  DRIVE9_EMBED_API_KEY  API key for DRIVE9_EMBED_API_BASE (optional)
  DRIVE9_EMBED_MODEL    model name for background embedding (optional)
  DRIVE9_EMBED_DIMENSIONS optional embedding dimensions override
  DRIVE9_EMBED_TIMEOUT_SECONDS embed request timeout seconds (default: 20)
  DRIVE9_SEMANTIC_WORKERS number of background workers (default: 1)
  DRIVE9_SEMANTIC_POLL_INTERVAL_MS worker poll interval in milliseconds (default: 200)
  DRIVE9_SEMANTIC_LEASE_SECONDS task lease duration in seconds (default: 30)
  DRIVE9_SEMANTIC_RECOVER_INTERVAL_MS recover sweep interval in milliseconds (default: 5000)
  DRIVE9_SEMANTIC_RETRY_BASE_MS base retry backoff in milliseconds (default: 200)
  DRIVE9_SEMANTIC_RETRY_MAX_MS max retry backoff in milliseconds (default: 30000)
  DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY max concurrent tasks per tenant (default: 1)

  Image extraction (async image -> text for search):
  DRIVE9_IMAGE_EXTRACT_ENABLED true|false (default: false)
  DRIVE9_IMAGE_EXTRACT_QUEUE_SIZE buffered task queue size (default: 128)
  DRIVE9_IMAGE_EXTRACT_WORKERS number of workers (default: 1)
  DRIVE9_IMAGE_EXTRACT_MAX_BYTES max image bytes processed per task (default: 8388608)
  DRIVE9_IMAGE_EXTRACT_TIMEOUT_SECONDS extractor timeout seconds (default: 20)
  DRIVE9_IMAGE_EXTRACT_MAX_TEXT_BYTES max extracted text stored in files.content_text (default: 8192)
  DRIVE9_IMAGE_EXTRACT_API_BASE OpenAI-compatible base URL (optional)
  DRIVE9_IMAGE_EXTRACT_API_KEY  API key for DRIVE9_IMAGE_EXTRACT_API_BASE (optional)
  DRIVE9_IMAGE_EXTRACT_MODEL    model name for vision extraction (optional)
  DRIVE9_IMAGE_EXTRACT_PROMPT   custom extraction prompt (optional)
  DRIVE9_IMAGE_EXTRACT_MAX_TOKENS max model output tokens (default: 256)

  Async audio transcript extract (TiDB auto-embedding durable tasks):
  DRIVE9_AUDIO_EXTRACT_ENABLED true|false (default: false)
  DRIVE9_AUDIO_EXTRACT_MODE stub|openai (required when enabled)
  DRIVE9_AUDIO_EXTRACT_MAX_BYTES max audio bytes per task (optional; backend default when unset)
  DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS extractor timeout seconds (optional; backend default when unset)
  DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES max transcript bytes stored in files.content_text (optional; backend default when unset)
  DRIVE9_AUDIO_EXTRACT_API_BASE OpenAI-compatible base URL (required for openai mode)
  DRIVE9_AUDIO_EXTRACT_API_KEY  API key for DRIVE9_AUDIO_EXTRACT_API_BASE (required for openai mode)
  DRIVE9_AUDIO_EXTRACT_MODEL    model name for audio transcription (required for openai mode)
  DRIVE9_AUDIO_EXTRACT_PROMPT   optional provider prompt for transcription (openai mode)
`)
	os.Exit(2)
}

func die(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "drive9-server-local: %v\n", err)
	os.Exit(1)
}

func publicBaseURL(listenAddr string) string {
	if v := strings.TrimRight(os.Getenv("DRIVE9_PUBLIC_URL"), "/"); v != "" {
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
		fmt.Fprintf(os.Stderr, "drive9-server-local: DRIVE9_PUBLIC_URL is required when listen address is %q (wildcard or non-loopback).\n", listenAddr)
		os.Exit(1)
		return ""
	}
}

func logLocalStartupStep(ctx context.Context, startupStart, stepStart time.Time, step string, extra ...zap.Field) {
	fields := []zap.Field{
		zap.String("step", step),
		zap.Duration("elapsed", time.Since(stepStart)),
		zap.Duration("startup_elapsed", time.Since(startupStart)),
	}
	fields = append(fields, extra...)
	logger.Info(ctx, "local_server_startup_step", fields...)
}

var (
	localTiDBEmbeddingModeDetector = schema.DetectTiDBEmbeddingMode
	localTiDBSchemaValidator       = schema.ValidateTiDBSchemaForMode
	localTiDBSchemaInitializer     = schema.InitTiDBTenantSchemaForMode
)

func detectLocalTiDBEmbeddingMode(db *sql.DB, schemaInitialized bool, requestedMode schema.TiDBEmbeddingMode, explicitMode bool) (schema.TiDBEmbeddingMode, error) {
	if explicitMode {
		if schemaInitialized {
			return requestedMode, nil
		}
		if db == nil {
			return schema.TiDBEmbeddingModeUnknown, fmt.Errorf("nil db")
		}
		if err := localTiDBSchemaValidator(db, requestedMode); err != nil {
			return schema.TiDBEmbeddingModeUnknown, err
		}
		return requestedMode, nil
	}
	if schemaInitialized {
		return schema.TiDBEmbeddingModeAuto, nil
	}
	if db == nil {
		return schema.TiDBEmbeddingModeUnknown, fmt.Errorf("nil db")
	}
	mode, err := localTiDBEmbeddingModeDetector(db)
	if err != nil {
		return schema.TiDBEmbeddingModeUnknown, err
	}
	if mode != schema.TiDBEmbeddingModeAuto && mode != schema.TiDBEmbeddingModeApp {
		return schema.TiDBEmbeddingModeUnknown, fmt.Errorf("unsupported TiDB embedding mode %q", mode)
	}
	if err := localTiDBSchemaValidator(db, mode); err != nil {
		return schema.TiDBEmbeddingModeUnknown, err
	}
	return mode, nil
}

func localEmbeddingModeFromEnv() (schema.TiDBEmbeddingMode, bool, error) {
	raw := strings.TrimSpace(os.Getenv(envLocalEmbeddingMode))
	switch strings.ToLower(raw) {
	case "":
		return schema.TiDBEmbeddingModeUnknown, false, nil
	case "detect":
		return schema.TiDBEmbeddingModeUnknown, false, nil
	case "auto", string(schema.TiDBEmbeddingModeAuto):
		return schema.TiDBEmbeddingModeAuto, true, nil
	case "app", string(schema.TiDBEmbeddingModeApp):
		return schema.TiDBEmbeddingModeApp, true, nil
	default:
		return schema.TiDBEmbeddingModeUnknown, false, fmt.Errorf("%s must be one of auto, app, or detect", envLocalEmbeddingMode)
	}
}

func localEmbeddingModeLabel(mode schema.TiDBEmbeddingMode, explicit bool) string {
	if !explicit {
		return "detect"
	}
	return string(mode)
}

func buildBackendOptionsFromEnv() (backend.Options, error) {
	var opts backend.Options
	opts.MaxTenantStorageBytes = envInt64("DRIVE9_MAX_TENANT_STORAGE_BYTES", 50*(1<<30))
	if opts.MaxTenantStorageBytes <= 0 {
		return backend.Options{}, fmt.Errorf("DRIVE9_MAX_TENANT_STORAGE_BYTES must be a positive integer")
	}

	queryBaseURL := strings.TrimSpace(os.Getenv("DRIVE9_QUERY_EMBED_API_BASE"))
	queryAPIKey := strings.TrimSpace(os.Getenv("DRIVE9_QUERY_EMBED_API_KEY"))
	queryModel := strings.TrimSpace(os.Getenv("DRIVE9_QUERY_EMBED_MODEL"))
	queryConfigured := queryBaseURL != "" || queryAPIKey != "" || queryModel != ""
	if queryConfigured {
		if queryBaseURL == "" || queryAPIKey == "" || queryModel == "" {
			return backend.Options{}, fmt.Errorf("DRIVE9_QUERY_EMBED_API_BASE, DRIVE9_QUERY_EMBED_API_KEY and DRIVE9_QUERY_EMBED_MODEL must be set together")
		}
		queryClient, err := embedding.NewOpenAIClient(embedding.OpenAIClientConfig{
			BaseURL:    queryBaseURL,
			APIKey:     queryAPIKey,
			Model:      queryModel,
			Dimensions: envInt("DRIVE9_QUERY_EMBED_DIMENSIONS", 0),
			Timeout:    time.Duration(envInt("DRIVE9_QUERY_EMBED_TIMEOUT_SECONDS", 20)) * time.Second,
		})
		if err != nil {
			return backend.Options{}, fmt.Errorf("init query embedder: %w", err)
		}
		opts.QueryEmbedding = backend.QueryEmbeddingOptions{Client: queryClient}
		logger.Info(context.Background(), "query_embedding_mode_openai_compatible",
			zap.String("model", queryModel), zap.String("base_url", queryBaseURL))
	}

	if envBool("DRIVE9_IMAGE_EXTRACT_ENABLED", false) {
		async := backend.AsyncImageExtractOptions{
			Enabled:             true,
			QueueSize:           envInt("DRIVE9_IMAGE_EXTRACT_QUEUE_SIZE", 128),
			Workers:             envInt("DRIVE9_IMAGE_EXTRACT_WORKERS", 1),
			MaxImageBytes:       envInt64("DRIVE9_IMAGE_EXTRACT_MAX_BYTES", 8<<20),
			TaskTimeout:         time.Duration(envInt("DRIVE9_IMAGE_EXTRACT_TIMEOUT_SECONDS", 20)) * time.Second,
			MaxExtractTextBytes: envInt("DRIVE9_IMAGE_EXTRACT_MAX_TEXT_BYTES", 8<<10),
		}

		baseURL := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_API_BASE"))
		apiKey := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_API_KEY"))
		model := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_MODEL"))
		prompt := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_PROMPT"))
		maxTokens := envInt("DRIVE9_IMAGE_EXTRACT_MAX_TOKENS", 256)

		configured := baseURL != "" || apiKey != "" || model != ""
		if configured {
			if baseURL == "" || apiKey == "" || model == "" {
				return backend.Options{}, fmt.Errorf("DRIVE9_IMAGE_EXTRACT_API_BASE, DRIVE9_IMAGE_EXTRACT_API_KEY and DRIVE9_IMAGE_EXTRACT_MODEL must be set together")
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
	}

	audioOpts, err := buildLocalAudioExtractOptionsFromEnv()
	if err != nil {
		return backend.Options{}, err
	}
	if backend.AsyncAudioExtractWillWireRuntime(audioOpts) {
		opts.AsyncAudioExtract = audioOpts
		logger.Info(context.Background(), "local_server_audio_extract_runtime_configured",
			zap.Int64("max_audio_bytes", audioOpts.MaxAudioBytes),
			zap.Duration("task_timeout", audioOpts.TaskTimeout),
			zap.Int("max_extract_text_bytes", audioOpts.MaxExtractTextBytes),
			zap.String("extractor_type", fmt.Sprintf("%T", audioOpts.Extractor)))
	}
	return opts, nil
}

func buildSemanticWorkerConfigFromEnv() (embedding.Client, server.SemanticWorkerOptions, error) {
	var opts server.SemanticWorkerOptions
	baseURL := strings.TrimSpace(os.Getenv("DRIVE9_EMBED_API_BASE"))
	apiKey := strings.TrimSpace(os.Getenv("DRIVE9_EMBED_API_KEY"))
	model := strings.TrimSpace(os.Getenv("DRIVE9_EMBED_MODEL"))
	configured := baseURL != "" || apiKey != "" || model != ""
	if !configured {
		return nil, opts, nil
	}
	if baseURL == "" || apiKey == "" || model == "" {
		return nil, opts, fmt.Errorf("DRIVE9_EMBED_API_BASE, DRIVE9_EMBED_API_KEY and DRIVE9_EMBED_MODEL must be set together")
	}
	client, err := embedding.NewOpenAIClient(embedding.OpenAIClientConfig{
		BaseURL:    baseURL,
		APIKey:     apiKey,
		Model:      model,
		Dimensions: envInt("DRIVE9_EMBED_DIMENSIONS", 0),
		Timeout:    time.Duration(envInt("DRIVE9_EMBED_TIMEOUT_SECONDS", 20)) * time.Second,
	})
	if err != nil {
		return nil, opts, fmt.Errorf("init semantic embedder: %w", err)
	}
	opts = server.SemanticWorkerOptions{
		Workers:              envInt("DRIVE9_SEMANTIC_WORKERS", 1),
		PollInterval:         time.Duration(envInt("DRIVE9_SEMANTIC_POLL_INTERVAL_MS", 200)) * time.Millisecond,
		LeaseDuration:        time.Duration(envInt("DRIVE9_SEMANTIC_LEASE_SECONDS", 30)) * time.Second,
		RecoverInterval:      time.Duration(envInt("DRIVE9_SEMANTIC_RECOVER_INTERVAL_MS", 5000)) * time.Millisecond,
		RetryBaseDelay:       time.Duration(envInt("DRIVE9_SEMANTIC_RETRY_BASE_MS", 200)) * time.Millisecond,
		RetryMaxDelay:        time.Duration(envInt("DRIVE9_SEMANTIC_RETRY_MAX_MS", 30000)) * time.Millisecond,
		PerTenantConcurrency: envInt("DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY", 1),
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
