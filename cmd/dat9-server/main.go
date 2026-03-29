// Command dat9-server starts the dat9 HTTP server.
//
// Usage:
//
//	dat9-server [listen-addr]
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/server"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"go.uber.org/zap"
)

const (
	defaultListenAddr = ":9009"
	defaultS3Dir      = "s3"
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

	metaDSN := os.Getenv("DAT9_META_DSN")
	if metaDSN == "" {
		die(fmt.Errorf("DAT9_META_DSN is required"))
	}

	s3Dir := envOr("DAT9_S3_DIR", defaultS3Dir)
	s3Bucket := os.Getenv("DAT9_S3_BUCKET")
	s3Region := envOr("DAT9_S3_REGION", "us-east-1")
	s3Prefix := os.Getenv("DAT9_S3_PREFIX")
	s3RoleARN := os.Getenv("DAT9_S3_ROLE_ARN")
	backendOptions, err := buildBackendOptionsFromEnv()
	if err != nil {
		die(err)
	}

	store, err := meta.Open(metaDSN)
	if err != nil {
		die(fmt.Errorf("open control-plane store: %w", err))
	}
	defer func() { _ = store.Close() }()

	if s3Bucket == "" {
		if err := os.MkdirAll(s3Dir, 0o755); err != nil {
			die(fmt.Errorf("create s3 dir: %w", err))
		}
		logger.Info(context.Background(), "s3_mode_local", zap.String("dir", s3Dir))
	} else {
		logger.Info(context.Background(), "s3_mode_aws", zap.String("bucket", s3Bucket), zap.String("region", s3Region), zap.String("role", envOr("DAT9_S3_ROLE_ARN", "default-credentials")))
	}

	encryptType := envOr("DAT9_ENCRYPT_TYPE", "local_aes")
	masterHex := os.Getenv("DAT9_MASTER_KEY")
	kmsKey := os.Getenv("DAT9_ENCRYPT_KEY")
	tokenHex := os.Getenv("DAT9_TOKEN_SIGNING_KEY")
	providerType := envOr("DAT9_TENANT_PROVIDER", tenant.ProviderTiDBZero)
	maxUploadBytes := int64(1 << 30)
	if raw := os.Getenv("DAT9_MAX_UPLOAD_BYTES"); raw != "" {
		maxUploadBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || maxUploadBytes <= 0 {
			die(fmt.Errorf("invalid DAT9_MAX_UPLOAD_BYTES: must be a positive integer"))
		}
		if maxUploadBytes < 1<<20 {
			die(fmt.Errorf("DAT9_MAX_UPLOAD_BYTES too small: minimum 1048576 (1MiB)"))
		}
	}
	providerType, err = tenant.NormalizeProvider(providerType)
	if err != nil {
		die(err)
	}

	var provisioner tenant.Provisioner
	var provisionerErr error
	switch providerType {
	case tenant.ProviderTiDBZero:
		provisioner, provisionerErr = tenant.NewZeroProvisionerFromEnv()
	case tenant.ProviderTiDBCloudStarter:
		provisioner, provisionerErr = tenant.NewStarterProvisionerFromEnv()
	case tenant.ProviderDB9:
		provisioner, provisionerErr = tenant.NewDB9ProvisionerFromEnv()
	}
	if provisionerErr != nil {
		logger.Warn(context.Background(), "provisioner_not_configured", zap.String("provider", providerType), zap.Error(provisionerErr))
	}

	var pool *tenant.Pool
	var tokenSecret []byte
	if tokenHex != "" {
		tokenSecret, err = hex.DecodeString(tokenHex)
		if err != nil {
			die(fmt.Errorf("invalid DAT9_TOKEN_SIGNING_KEY: %w", err))
		}
		eKey := masterHex
		eType := encrypt.Type(encryptType)
		if eType == encrypt.TypeKMS {
			eKey = kmsKey
		}
		enc, err := encrypt.New(context.Background(), encrypt.Config{
			Type:   eType,
			Key:    eKey,
			Region: envOr("DAT9_S3_REGION", "us-east-1"),
		})
		if err != nil {
			die(fmt.Errorf("create encryptor: %w", err))
		}

		if err := store.DB().Ping(); err != nil {
			die(fmt.Errorf("control-plane db unavailable: %w", err))
		}

		pool = tenant.NewPool(tenant.PoolConfig{
			S3Dir:          s3Dir,
			PublicURL:      publicBaseURL(addr),
			S3Bucket:       s3Bucket,
			S3Region:       s3Region,
			S3Prefix:       s3Prefix,
			S3RoleARN:      s3RoleARN,
			BackendOptions: backendOptions,
		}, enc)
		defer pool.Close()
	}

	die(server.NewWithConfig(server.Config{
		Meta:           store,
		Pool:           pool,
		Provisioner:    provisioner,
		TokenSecret:    tokenSecret,
		S3Dir:          s3Dir,
		MaxUploadBytes: maxUploadBytes,
		Logger:         srvLogger,
	}).ListenAndServe(addr))
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9-server [listen-addr]

environment:
  DAT9_LISTEN_ADDR serve listen address (default: :9009)
  DAT9_PUBLIC_URL  externally reachable base URL for presigned URLs (required for remote clients)
  DAT9_META_DSN    control-plane MySQL DSN (required)
  DAT9_ENCRYPT_TYPE local_aes|kms
  DAT9_MASTER_KEY  32-byte hex key for local_aes encryptor
  DAT9_ENCRYPT_KEY KMS key id or alias (required for kms)
  DAT9_TOKEN_SIGNING_KEY  32-byte hex key for JWT API key signing
  DAT9_MAX_UPLOAD_BYTES maximum allowed upload size in bytes (default: 1073741824, minimum: 1048576)
  DAT9_TENANT_PROVIDER db9|tidb_zero|tidb_cloud_starter (default for provisioning)
  S3 storage (set DAT9_S3_BUCKET to enable AWS S3, otherwise local mock):
  DAT9_S3_BUCKET   S3 bucket name (enables AWS S3 mode)
  DAT9_S3_REGION   AWS region (default: us-east-1)
  DAT9_S3_PREFIX   S3 key prefix (e.g. "tenants")
  DAT9_S3_ROLE_ARN IAM role ARN to assume via STS (optional)
  DAT9_S3_DIR      local s3 mock root directory (default: ./s3, only used without DAT9_S3_BUCKET)
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
	fmt.Fprintf(os.Stderr, "dat9-server: %v\n", err)
	os.Exit(1)
}

func publicBaseURL(listenAddr string) string {
	if v := strings.TrimRight(os.Getenv("DAT9_PUBLIC_URL"), "/"); v != "" {
		return v
	}

	// Without DAT9_PUBLIC_URL, only allow explicit loopback addresses.
	// Wildcard or non-loopback addresses would produce unreachable presigned URLs.
	switch {
	case strings.HasPrefix(listenAddr, "127.0.0.1:"),
		strings.HasPrefix(listenAddr, "localhost:"),
		strings.HasPrefix(listenAddr, "[::1]:"):
		return "http://" + listenAddr
	case strings.HasPrefix(listenAddr, "http://"), strings.HasPrefix(listenAddr, "https://"):
		return strings.TrimRight(listenAddr, "/")
	default:
		fmt.Fprintf(os.Stderr, "dat9-server: DAT9_PUBLIC_URL is required when listen address is %q (wildcard or non-loopback). Set DAT9_PUBLIC_URL to the externally reachable base URL.\n", listenAddr)
		os.Exit(1)
		return "" // unreachable
	}
}

func buildBackendOptionsFromEnv() (backend.Options, error) {
	var opts backend.Options
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
