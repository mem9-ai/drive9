// Command drive9-server starts the drive9 HTTP server and exposes operational
// schema tooling.
//
// Usage:
//
//	drive9-server [listen-addr]
//	drive9-server schema dump-init-sql [flags]
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/buildinfo"
	"github.com/mem9-ai/dat9/pkg/embedding"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/server"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/db9"
	"github.com/mem9-ai/dat9/pkg/tenant/starter"
	"github.com/mem9-ai/dat9/pkg/tenant/tidbcloudnative"
	"github.com/mem9-ai/dat9/pkg/tenant/tidbzero"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
	accountpb "github.com/tidbcloud/account/idl/pbgen/proto/account"
	serverlessv1 "github.com/tidbcloud/tidb-management-service/api/spec/global/serverless/v1"
	zerov1beta1 "github.com/tidbcloud/tidb-management-service/api/spec/tidb_cloud_open_api/zero/v1beta1"
	mgmtv1 "github.com/tidbcloud/tidb-management-service/api/spec/tidb_mgmt_service/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultListenAddr = ":9009"
	defaultS3Dir      = "s3"
)

type s3Config struct {
	Dir             string
	Bucket          string
	Region          string
	Prefix          string
	RoleARN         string
	Endpoint        string
	ForcePathStyle  bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "-h", "--help", "help":
			usage(os.Stdout, 0)
			return
		case "version":
			if len(os.Args) != 2 {
				usage(os.Stderr, 2)
			}
			_, _ = fmt.Fprint(os.Stdout, versionText())
			return
		case "schema":
			die(runSchemaCommand(os.Args[2:]))
			return
		case "backfill-quota":
			die(runBackfillQuota(os.Args[2:]))
			return
		}
	}
	if len(os.Args) > 2 {
		usage(os.Stderr, 2)
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
	logger.Info(context.Background(), "build_info", buildinfo.Fields("drive9-server")...)

	metaDSN := os.Getenv("DRIVE9_META_DSN")
	if metaDSN == "" {
		die(fmt.Errorf("DRIVE9_META_DSN is required"))
	}

	s3cfg := s3ConfigFromEnv()
	if err := s3cfg.validate(); err != nil {
		die(err)
	}
	backendOptions, err := buildBackendOptionsFromEnv()
	if err != nil {
		die(err)
	}
	semanticEmbedder, semanticWorkerOpts, err := buildSemanticWorkerConfigFromEnv()
	if err != nil {
		die(err)
	}
	if semanticEmbedder != nil && backendOptions.QueryEmbedding.Client == nil {
		backendOptions.QueryEmbedding = backend.QueryEmbeddingOptions{Client: semanticEmbedder}
	}

	store, err := meta.Open(metaDSN)
	if err != nil {
		die(fmt.Errorf("open control-plane store: %w", err))
	}
	defer func() { _ = store.Close() }()

	if s3cfg.Bucket == "" {
		if err := os.MkdirAll(s3cfg.Dir, 0o755); err != nil {
			die(fmt.Errorf("create s3 dir: %w", err))
		}
		logger.Info(context.Background(), "s3_mode_local", zap.String("dir", s3cfg.Dir))
	} else {
		logger.Info(context.Background(), "s3_mode_aws",
			zap.String("bucket", s3cfg.Bucket),
			zap.String("region", s3cfg.Region),
			zap.String("endpoint", s3cfg.Endpoint),
			zap.Bool("path_style", s3cfg.ForcePathStyle),
			zap.String("credentials", s3client.CredentialLogValue(s3cfg.AccessKeyID)),
			zap.String("role", s3client.RoleLogValue(s3cfg.RoleARN)))
	}

	encryptType := envOr("DRIVE9_ENCRYPT_TYPE", "local_aes")
	masterHex := os.Getenv("DRIVE9_MASTER_KEY")
	kmsKey := os.Getenv("DRIVE9_ENCRYPT_KEY")
	tokenHex := os.Getenv("DRIVE9_TOKEN_SIGNING_KEY")
	vaultMKHex := os.Getenv("DRIVE9_VAULT_MASTER_KEY")
	providerType := envOr("DRIVE9_TENANT_PROVIDER", tenant.ProviderTiDBZero)
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
	backendOptions.MaxUploadBytes = maxUploadBytes
	providerType, err = tenant.NormalizeProvider(providerType)
	if err != nil {
		die(err)
	}
	logger.Info(context.Background(), "tenant_provider_selected", zap.String("provider", providerType))

	var provisioner tenant.Provisioner
	var provisionerErr error
	switch providerType {
	case tenant.ProviderTiDBZero:
		provisioner, provisionerErr = tidbzero.NewProvisionerFromEnv()
	case tenant.ProviderTiDBCloudStarter:
		provisioner, provisionerErr = starter.NewProvisionerFromEnv()
	case tenant.ProviderDB9:
		provisioner, provisionerErr = db9.NewProvisionerFromEnv()
	case tenant.ProviderTiDBCloudNative:
		// tidbcloud-native is wired below after the encryptor is created.
	}
	if provisionerErr != nil {
		logger.Warn(context.Background(), "provisioner_not_configured", zap.String("provider", providerType), zap.Error(provisionerErr))
	} else {
		logger.Info(context.Background(), "provisioner_configured", zap.String("provider", providerType))
	}

	var vaultMasterKey []byte
	if vaultMKHex != "" {
		vaultMasterKey, err = hex.DecodeString(vaultMKHex)
		if err != nil {
			die(fmt.Errorf("invalid DRIVE9_VAULT_MASTER_KEY: %w", err))
		}
	}

	var pool *tenant.Pool
	var tokenSecret []byte
	if tokenHex != "" {
		tokenSecret, err = hex.DecodeString(tokenHex)
		if err != nil {
			die(fmt.Errorf("invalid DRIVE9_TOKEN_SIGNING_KEY: %w", err))
		}
		eKey := masterHex
		eType := encrypt.Type(encryptType)
		if eType == encrypt.TypeKMS {
			eKey = kmsKey
		}
		enc, err := encrypt.New(context.Background(), encrypt.Config{
			Type:   eType,
			Key:    eKey,
			Region: s3cfg.Region,
		})
		if err != nil {
			die(fmt.Errorf("create encryptor: %w", err))
		}

		if err := store.DB().Ping(); err != nil {
			die(fmt.Errorf("control-plane db unavailable: %w", err))
		}

		pool = tenant.NewPool(tenant.PoolConfig{
			S3Dir:             s3cfg.Dir,
			PublicURL:         publicBaseURL(addr),
			S3Bucket:          s3cfg.Bucket,
			S3Region:          s3cfg.Region,
			S3Prefix:          s3cfg.Prefix,
			S3RoleARN:         s3cfg.RoleARN,
			S3Endpoint:        s3cfg.Endpoint,
			S3ForcePathStyle:  s3cfg.ForcePathStyle,
			S3AccessKeyID:     s3cfg.AccessKeyID,
			S3SecretAccessKey: s3cfg.SecretAccessKey,
			S3SessionToken:    s3cfg.SessionToken,
			BackendOptions:    backendOptions,
		}, enc)
		defer pool.Close()

		if providerType == tenant.ProviderTiDBCloudNative {
			// Three gRPC targets:
			//   tidb-mgmt-service          → ClusterService (ListClusters) + ZeroInstanceService
			//   serverless-global-service   → ServerlessService (GetEncryptedCloudAdminPwd)
			//   account-provider-grpc       → AccountAPIService (VerifyUserOrgAndProjects)
			mgmtAddr := os.Getenv("DRIVE9_TIDBCLOUD_MGMT_ADDR")
			if mgmtAddr == "" {
				die(fmt.Errorf("DRIVE9_TIDBCLOUD_MGMT_ADDR is required for tidbcloud-native provider"))
			}
			serverlessAddr := os.Getenv("DRIVE9_TIDBCLOUD_SERVERLESS_GLOBAL_ADDR")
			if serverlessAddr == "" {
				die(fmt.Errorf("DRIVE9_TIDBCLOUD_SERVERLESS_GLOBAL_ADDR is required for tidbcloud-native provider"))
			}
			accountAddr := os.Getenv("DRIVE9_TIDBCLOUD_ACCOUNT_ADDR")
			if accountAddr == "" {
				die(fmt.Errorf("DRIVE9_TIDBCLOUD_ACCOUNT_ADDR is required for tidbcloud-native provider"))
			}
			mgmtAddr = normalizeGRPCTarget(mgmtAddr)
			serverlessAddr = normalizeGRPCTarget(serverlessAddr)
			accountAddr = normalizeGRPCTarget(accountAddr)

			mgmtConn, err := newGRPCClientConn(mgmtAddr, true)
			if err != nil {
				die(fmt.Errorf("dial tidb-mgmt-service %s: %w", mgmtAddr, err))
			}
			serverlessConn, err := newGRPCClientConn(serverlessAddr, true)
			if err != nil {
				die(fmt.Errorf("dial serverless-global-service %s: %w", serverlessAddr, err))
			}
			accountConn, err := newGRPCClientConn(accountAddr, true)
			if err != nil {
				die(fmt.Errorf("dial account-provider-grpc %s: %w", accountAddr, err))
			}

			globalClient := tidbcloud.NewGRPCGlobalClient(
				mgmtv1.NewClusterServiceClient(mgmtConn),
				serverlessv1.NewServerlessServiceClient(serverlessConn),
				zerov1beta1.NewZeroInstanceServiceClient(mgmtConn),
			)
			accountClient := tidbcloud.NewGRPCAccountClient(
				accountpb.NewAccountAPIServiceClient(accountConn),
			)

			// Auth0 client-credentials for the cluster proxy JWT middleware.
			// Optional: when unset, proxy requests are sent without a JWT
			// (suitable for dev/staging where Auth0 is disabled on the proxy).
			var auth0Cfg *tidbcloud.ProxyAuth0Config
			if d := os.Getenv("DRIVE9_PROXY_AUTH0_DOMAIN"); d != "" {
				clientID := os.Getenv("DRIVE9_PROXY_AUTH0_CLIENT_ID")
				if clientID == "" {
					die(fmt.Errorf("DRIVE9_PROXY_AUTH0_CLIENT_ID is required when DRIVE9_PROXY_AUTH0_DOMAIN is set"))
				}
				clientSecret := os.Getenv("DRIVE9_PROXY_AUTH0_CLIENT_SECRET")
				if clientSecret == "" {
					die(fmt.Errorf("DRIVE9_PROXY_AUTH0_CLIENT_SECRET is required when DRIVE9_PROXY_AUTH0_DOMAIN is set"))
				}
				audience := os.Getenv("DRIVE9_PROXY_AUTH0_AUDIENCE")
				if audience == "" {
					die(fmt.Errorf("DRIVE9_PROXY_AUTH0_AUDIENCE is required when DRIVE9_PROXY_AUTH0_DOMAIN is set"))
				}
				auth0Cfg = &tidbcloud.ProxyAuth0Config{
					Domain:       d,
					ClientID:     clientID,
					ClientSecret: clientSecret,
					Audience:     audience,
				}
			}

			provisioner = tidbcloudnative.NewProvisioner(globalClient, accountClient, enc, auth0Cfg)
			logger.Info(context.Background(), "provisioner_configured",
				zap.String("provider", providerType),
				zap.String("mgmt_addr", mgmtAddr),
				zap.String("serverless_addr", serverlessAddr),
				zap.String("account_addr", accountAddr))
		}
	}

	if pool != nil {
		pool.SetMetaStore(store)

		// Start the mutation log replay worker for central quota.
		replayWorker := backend.StartMutationReplayWorker(tenant.NewMetaQuotaAdapter(store))
		if replayWorker != nil {
			defer replayWorker.Stop()
		}

		// Start the upload reservation expiry sweep worker.
		expirySweepWorker := backend.StartExpirySweepWorker(store)
		if expirySweepWorker != nil {
			defer expirySweepWorker.Stop()
		}

		// TODO: Run ValidateDurableAsyncExtractRequiresSemanticWorker only when this process
		// can serve tenants that enqueue durable audio_extract_text / img_extract_text
		// (database auto-embedding: tidb_zero, tidb_cloud_starter). pool != nil is too broad
		// for db9-only pools, which never hit that path but still get forced to configure
		// DRIVE9_EMBED_* when async extract is wired on the template (PR #159 review).
		if err := server.ValidateDurableAsyncExtractRequiresSemanticWorker(server.Config{
			Meta:             store,
			Pool:             pool,
			Provisioner:      provisioner,
			TokenSecret:      tokenSecret,
			VaultMasterKey:   vaultMasterKey,
			S3Dir:            s3cfg.Dir,
			MaxUploadBytes:   maxUploadBytes,
			Logger:           srvLogger,
			SemanticEmbedder: semanticEmbedder,
			SemanticWorkers:  semanticWorkerOpts,
		}, backendOptions, false); err != nil {
			die(err)
		}
	}

	die(server.NewWithConfig(server.Config{
		Meta:             store,
		Pool:             pool,
		Provisioner:      provisioner,
		TokenSecret:      tokenSecret,
		VaultMasterKey:   vaultMasterKey,
		VaultIssuerURL:   vaultIssuerURL(addr),
		S3Dir:            s3cfg.Dir,
		MaxUploadBytes:   maxUploadBytes,
		Logger:           srvLogger,
		SemanticEmbedder: semanticEmbedder,
		SemanticWorkers:  semanticWorkerOpts,
	}).ListenAndServe(addr))
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func s3ConfigFromEnv() s3Config {
	dir := strings.TrimSpace(os.Getenv("DRIVE9_S3_DIR"))
	if dir == "" {
		dir = defaultS3Dir
	}
	region := strings.TrimSpace(os.Getenv("DRIVE9_S3_REGION"))
	if region == "" {
		region = "us-east-1"
	}
	return s3Config{
		Dir:             dir,
		Bucket:          strings.TrimSpace(os.Getenv("DRIVE9_S3_BUCKET")),
		Region:          region,
		Prefix:          strings.TrimSpace(os.Getenv("DRIVE9_S3_PREFIX")),
		RoleARN:         strings.TrimSpace(os.Getenv("DRIVE9_S3_ROLE_ARN")),
		Endpoint:        strings.TrimSpace(os.Getenv("DRIVE9_S3_ENDPOINT")),
		ForcePathStyle:  envBool("DRIVE9_S3_FORCE_PATH_STYLE", false),
		AccessKeyID:     strings.TrimSpace(os.Getenv("DRIVE9_S3_ACCESS_KEY_ID")),
		SecretAccessKey: strings.TrimSpace(os.Getenv("DRIVE9_S3_SECRET_ACCESS_KEY")),
		SessionToken:    strings.TrimSpace(os.Getenv("DRIVE9_S3_SESSION_TOKEN")),
	}
}

func (cfg s3Config) validate() error {
	if cfg.Bucket == "" {
		return nil
	}
	return cfg.awsConfig().Validate()
}

func (cfg s3Config) awsConfig() s3client.AWSConfig {
	return s3client.AWSConfig{
		Region:          cfg.Region,
		Bucket:          cfg.Bucket,
		Prefix:          cfg.Prefix,
		RoleARN:         cfg.RoleARN,
		Endpoint:        cfg.Endpoint,
		ForcePathStyle:  cfg.ForcePathStyle,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
	}
}

func versionText() string {
	return buildinfo.String("drive9-server")
}

func usage(out io.Writer, exitCode int) {
	_, _ = fmt.Fprintf(out, `usage:
  drive9-server [listen-addr]
  drive9-server version
  drive9-server schema dump-init-sql --provider <db9|tidb_zero|tidb_cloud_starter>

environment:
  DRIVE9_LISTEN_ADDR serve listen address (default: :9009)
  DRIVE9_PUBLIC_URL  externally reachable base URL for presigned URLs (required for remote clients)
  DRIVE9_META_DSN    control-plane MySQL DSN (required)
  DRIVE9_ENCRYPT_TYPE local_aes|kms
  DRIVE9_MASTER_KEY  32-byte hex key for local_aes encryptor
  DRIVE9_ENCRYPT_KEY KMS key id or alias (required for kms)
  DRIVE9_TOKEN_SIGNING_KEY  32-byte hex key for JWT API key signing
  DRIVE9_VAULT_MASTER_KEY   32-byte hex key for vault DEK wrapping (omit to disable vault)
  DRIVE9_MAX_UPLOAD_BYTES maximum allowed upload size in bytes (default: %d, minimum: 1048576)
  DRIVE9_BENCH_TIMING_LOG_ENABLED true|false to emit benchmark timing logs on successful server hot paths (default: false)
  DRIVE9_QUOTA_SOURCE tenant|server quota enforcement source (default: tenant)
  DRIVE9_TENANT_PROVIDER db9|tidb_zero|tidb_cloud_starter (default for provisioning)
  TiDB Cloud native gRPC:
  DRIVE9_TIDBCLOUD_MGMT_ADDR tidb-mgmt-service gRPC target (required for tidb_cloud_native)
  DRIVE9_TIDBCLOUD_SERVERLESS_GLOBAL_ADDR serverless-global-service gRPC target (required for tidb_cloud_native)
  DRIVE9_TIDBCLOUD_ACCOUNT_ADDR account-provider-grpc target (required for tidb_cloud_native)
  S3 storage (set DRIVE9_S3_BUCKET to enable AWS S3, otherwise local mock):
  DRIVE9_S3_BUCKET   S3 bucket name (enables AWS S3 mode)
  DRIVE9_S3_REGION   AWS region (default: us-east-1)
  DRIVE9_S3_PREFIX   S3 key prefix (e.g. "tenants")
  DRIVE9_S3_ENDPOINT custom S3 endpoint URL for S3-compatible stores such as MinIO (optional)
  DRIVE9_S3_FORCE_PATH_STYLE true|false to force path-style S3 URLs (default: false)
  DRIVE9_S3_ACCESS_KEY_ID static S3 access key id (optional; requires DRIVE9_S3_SECRET_ACCESS_KEY)
  DRIVE9_S3_SECRET_ACCESS_KEY static S3 secret access key (optional; requires DRIVE9_S3_ACCESS_KEY_ID)
  DRIVE9_S3_SESSION_TOKEN static S3 session token (optional; requires DRIVE9_S3_ACCESS_KEY_ID and DRIVE9_S3_SECRET_ACCESS_KEY)
  DRIVE9_S3_ROLE_ARN IAM role ARN to assume via STS (optional)
  DRIVE9_S3_DIR      local s3 mock root directory (default: ./s3, only used without DRIVE9_S3_BUCKET)
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
  DRIVE9_SEMANTIC_TENANT_LIMIT active tenants scanned per round (default: 128)
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
  Audio extraction (async audio -> text for search; MVP durable path is TiDB auto-embedding only):
  Durable audio_extract_text tasks enqueue only for tenants with database auto-embedding
  (tidb_zero / tidb_cloud_starter). For db9-only or other app-managed tenants these vars do
  not enable that semantic_tasks pipeline. When enabled, explicit provider wiring is required:
  DRIVE9_AUDIO_EXTRACT_ENABLED true|false (default: false)
  DRIVE9_AUDIO_EXTRACT_MAX_BYTES max audio bytes processed per task (default: 33554432)
  DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS extractor timeout seconds (default: 120)
  DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES max transcript bytes stored in files.content_text (default: 8192)
  DRIVE9_AUDIO_EXTRACT_API_BASE OpenAI-compatible base URL (required when enabled)
  DRIVE9_AUDIO_EXTRACT_API_KEY  API key for DRIVE9_AUDIO_EXTRACT_API_BASE (required when enabled)
  DRIVE9_AUDIO_EXTRACT_MODEL    model name for audio transcription (required when enabled)
  DRIVE9_AUDIO_EXTRACT_PROMPT   optional provider prompt for transcription

schema tooling:
  dump-init-sql writes the exact init schema SQL to stdout so external systems
  such as tidb_cloud_starter can stay in sync with drive9's schema source of truth.
`, server.DefaultMaxUploadBytes)
	os.Exit(exitCode)
}

func die(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "drive9-server: %v\n", err)
	os.Exit(1)
}

// vaultIssuerURL returns the canonical issuer URL used as the `iss` claim on
// vault grants. DRIVE9_VAULT_ISSUER_URL is the explicit override for sites that
// want the grant issuer to be a different URL than the public object URL (e.g.
// when signed URLs go to a CDN and grant validation happens at a control-plane
// host). When unset, we fall back to the same canonical URL the object plane
// uses, which is what the end-state spec §16 expects (single server identity).
func vaultIssuerURL(listenAddr string) string {
	if v := strings.TrimRight(strings.TrimSpace(os.Getenv("DRIVE9_VAULT_ISSUER_URL")), "/"); v != "" {
		return v
	}
	return publicBaseURL(listenAddr)
}

func publicBaseURL(listenAddr string) string {
	if v := strings.TrimRight(os.Getenv("DRIVE9_PUBLIC_URL"), "/"); v != "" {
		return v
	}

	// Without DRIVE9_PUBLIC_URL, only allow explicit loopback addresses.
	// Wildcard or non-loopback addresses would produce unreachable presigned URLs.
	switch {
	case strings.HasPrefix(listenAddr, "127.0.0.1:"),
		strings.HasPrefix(listenAddr, "localhost:"),
		strings.HasPrefix(listenAddr, "[::1]:"):
		return "http://" + listenAddr
	case strings.HasPrefix(listenAddr, "http://"), strings.HasPrefix(listenAddr, "https://"):
		return strings.TrimRight(listenAddr, "/")
	default:
		fmt.Fprintf(os.Stderr, "drive9-server: DRIVE9_PUBLIC_URL is required when listen address is %q (wildcard or non-loopback). Set DRIVE9_PUBLIC_URL to the externally reachable base URL.\n", listenAddr)
		os.Exit(1)
		return "" // unreachable
	}
}

func buildBackendOptionsFromEnv() (backend.Options, error) {
	var opts backend.Options
	opts.MaxTenantStorageBytes = envInt64("DRIVE9_MAX_TENANT_STORAGE_BYTES", 50*(1<<30))
	if opts.MaxTenantStorageBytes <= 0 {
		return backend.Options{}, fmt.Errorf("DRIVE9_MAX_TENANT_STORAGE_BYTES must be a positive integer")
	}

	// Quota enforcement source: "tenant" (default, per-tenant DB) or "server" (central server DB).
	switch qs := strings.ToLower(strings.TrimSpace(os.Getenv("DRIVE9_QUOTA_SOURCE"))); qs {
	case "", "tenant":
	case "server":
		opts.QuotaSource = backend.QuotaSourceServer
	default:
		return backend.Options{}, fmt.Errorf("DRIVE9_QUOTA_SOURCE must be one of tenant or server, got %q", qs)
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
				logger.Error(context.Background(), "image_extract_mode_invalid_config",
					zap.Bool("base_url_present", baseURL != ""),
					zap.Bool("api_key_present", apiKey != ""),
					zap.Bool("model_present", model != ""))
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

	if envBool("DRIVE9_AUDIO_EXTRACT_ENABLED", false) {
		audioBaseURL := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_API_BASE"))
		audioAPIKey := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_API_KEY"))
		audioModel := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_MODEL"))
		audioPrompt := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_PROMPT"))
		if audioBaseURL == "" || audioAPIKey == "" || audioModel == "" {
			return backend.Options{}, fmt.Errorf("DRIVE9_AUDIO_EXTRACT_API_BASE, DRIVE9_AUDIO_EXTRACT_API_KEY and DRIVE9_AUDIO_EXTRACT_MODEL must be set together when DRIVE9_AUDIO_EXTRACT_ENABLED=true")
		}
		audioTimeout := time.Duration(envInt("DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS", 120)) * time.Second
		audioResponseFormat := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_RESPONSE_FORMAT"))
		audioExtractor, err := backend.NewOpenAIAudioTextExtractor(backend.OpenAIAudioTextExtractorConfig{
			BaseURL:        audioBaseURL,
			APIKey:         audioAPIKey,
			Model:          audioModel,
			Prompt:         audioPrompt,
			ResponseFormat: audioResponseFormat,
			Timeout:        audioTimeout,
		})
		if err != nil {
			return backend.Options{}, fmt.Errorf("init audio extractor: %w", err)
		}
		opts.AsyncAudioExtract = backend.AsyncAudioExtractOptions{
			Enabled:             true,
			MaxAudioBytes:       envInt64("DRIVE9_AUDIO_EXTRACT_MAX_BYTES", 32<<20),
			TaskTimeout:         audioTimeout,
			MaxExtractTextBytes: envInt("DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES", 8<<10),
			Extractor:           audioExtractor,
		}
		logger.Info(context.Background(), "audio_extract_mode_openai_compatible",
			zap.String("model", audioModel), zap.String("base_url", audioBaseURL))
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
		TenantScanLimit:      envInt("DRIVE9_SEMANTIC_TENANT_LIMIT", 128),
		PerTenantConcurrency: envInt("DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY", 1),
	}
	logger.Info(context.Background(), "semantic_embedding_mode_openai_compatible",
		zap.String("model", model), zap.String("base_url", baseURL))
	return client, opts, nil
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

// normalizeGRPCTarget rewrites kubernetes scheme targets to the dns resolver
// so that gRPC can resolve Kubernetes service names correctly.
func normalizeGRPCTarget(target string) string {
	if strings.HasPrefix(target, "kubernetes:///") {
		return "dns:///" + strings.TrimPrefix(target, "kubernetes:///")
	}
	if strings.HasPrefix(target, "kubernetes://") {
		return "dns:///" + strings.TrimPrefix(target, "kubernetes://")
	}
	return target
}

// newGRPCClientConn dials a gRPC target. When insecureMode is true, the
// connection uses no TLS (suitable for cluster-internal traffic).
func newGRPCClientConn(target string, insecureMode bool) (*grpc.ClientConn, error) {
	if insecureMode {
		return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
}
