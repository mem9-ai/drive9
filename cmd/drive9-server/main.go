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
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/buildinfo"
	"github.com/mem9-ai/drive9/pkg/embedding"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/s3client"
	"github.com/mem9-ai/drive9/pkg/server"
	"github.com/mem9-ai/drive9/pkg/slockoauth"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/db9"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
	"github.com/mem9-ai/drive9/pkg/tenant/starter"
	"github.com/mem9-ai/drive9/pkg/tenant/tidbcloudnative"
	"github.com/mem9-ai/drive9/pkg/tenant/tidbzero"
)

const (
	defaultListenAddr = ":9009"
	defaultS3Dir      = "s3"
)

type s3Config struct {
	Dir              string
	Bucket           string
	Region           string
	Prefix           string
	RoleARN          string
	Endpoint         string
	ForcePathStyle   bool
	AccessKeyID      string
	SecretAccessKey  string
	SessionToken     string
	EncryptionPolicy meta.S3EncryptionPolicy
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
			die(schema.ConfigureTiDBAutoEmbeddingFromEnv())
			die(runSchemaCommand(os.Args[2:]))
			return
		}
	}
	if len(os.Args) > 2 {
		usage(os.Stderr, 2)
	}
	autoEmbeddingConfig, err := schema.TiDBAutoEmbeddingConfigFromEnv()
	die(err)
	die(schema.ConfigureTiDBAutoEmbedding(autoEmbeddingConfig))

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
	autoEmbeddingAPIKey := strings.TrimSpace(os.Getenv(schema.EnvTiDBAutoEmbeddingAPIKey))
	autoEmbeddingAPIBase := strings.TrimSpace(os.Getenv(schema.EnvTiDBAutoEmbeddingAPIBase))
	semanticEmbedder, tenantWorkerOpts, err := buildTenantWorkerConfigFromEnv()
	if err != nil {
		die(err)
	}
	if semanticEmbedder != nil && backendOptions.QueryEmbedding.Client == nil {
		backendOptions.QueryEmbedding = backend.QueryEmbeddingOptions{Client: semanticEmbedder}
	}
	backendOptions.AppSemanticTasksEnabled = semanticEmbedder != nil

	// P1-3: Configurable quota cache refresh interval (default 30s).
	// In multi-pod deployments, increasing this reduces per-tenant-per-pod DB reads.
	backend.InitQuotaConfigCacheRefreshInterval(envInt("DRIVE9_QUOTA_CACHE_REFRESH_SECONDS", 0))
	backend.InitQuotaAdmissionCacheTTLs(
		envDuration("DRIVE9_QUOTA_USAGE_CACHE_TTL", 0),
		envDuration("DRIVE9_QUOTA_PENDING_DELTAS_CACHE_TTL", 0),
	)

	store, err := openControlPlaneStoreWithRetry(context.Background(), metaDSN, defaultStartupRetryOptions())
	if err != nil {
		die(fmt.Errorf("open control-plane store: %w", err))
	}
	if raw := os.Getenv("DRIVE9_DEFAULT_STORAGE_QUOTA_BYTES"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			meta.SetDefaultMaxStorageBytes(v)
		}
	}
	defer func() { _ = store.Close() }()

	// Continuously probe the long-lived metadata ("meta") store so a control-plane
	// DB outage after startup is visible via drive9_db_up and logs. Tenant user
	// and user_schema pools are workload pools; probing them would only keep
	// short-lived connections warm and compete with business traffic.
	probeInterval := time.Duration(envInt("DRIVE9_DB_HEALTH_PROBE_INTERVAL_SECONDS", 15)) * time.Second
	probeTimeout := time.Duration(envInt("DRIVE9_DB_HEALTH_PROBE_TIMEOUT_SECONDS", 3)) * time.Second
	metrics.StartDBHealthProbeWithOptions(context.Background(), probeInterval, probeTimeout, dbHealthProbeOptionsFromEnv(), func(role string, up bool, err error) {
		fields := []zap.Field{zap.String("role", role)}
		if up {
			logger.Info(context.Background(), "db_recovered", fields...)
			return
		}
		fields = append(fields, zap.Error(err))
		logger.Error(context.Background(), "db_unavailable", fields...)
	})

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
	aliyunKMSEndpoint := strings.TrimSpace(os.Getenv("DRIVE9_ALIYUN_KMS_ENDPOINT"))
	tokenHex := os.Getenv("DRIVE9_TOKEN_SIGNING_KEY")
	vaultMKHex := os.Getenv("DRIVE9_VAULT_MASTER_KEY")
	providerType := envOr("DRIVE9_TENANT_PROVIDER", tenant.ProviderTiDBZero)
	tenantPoolMaxSize, err := tenantPoolMaxSizeFromEnv()
	if err != nil {
		die(err)
	}
	tenantPoolRefillFreeRatio, err := tenantPoolRefillFreeRatioFromEnv()
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
	backendOptions.MaxUploadBytes = maxUploadBytes
	meta.SetDefaultMaxFileSizeBytes(maxUploadBytes)
	providerType, err = tenant.NormalizeProvider(providerType)
	if err != nil {
		die(err)
	}
	disableDatabaseAutoEmbedding := envBool("DRIVE9_DISABLE_AUTO_EMBEDDING", false)
	if tenant.UsesTiDBAutoEmbedding(providerType) && !disableDatabaseAutoEmbedding {
		die(schema.ValidateTiDBAutoEmbeddingProviderConfig(schema.TiDBAutoEmbeddingProviderConfig{
			Model:   autoEmbeddingConfig.Model,
			APIKey:  autoEmbeddingAPIKey,
			APIBase: autoEmbeddingAPIBase,
		}))
	}
	logger.Info(context.Background(), "tenant_provider_selected", zap.String("provider", providerType))

	var provisioner tenant.Provisioner
	var provisionerErr error
	switch providerType {
	case tenant.ProviderTiDBZero:
		provisioner, provisionerErr = tidbzero.NewProvisionerFromEnv()
	case tenant.ProviderTiDBCloudNative:
		provisioner, provisionerErr = tidbcloudnative.NewProvisionerFromEnv()
	case tenant.ProviderDB9:
		provisioner, provisionerErr = db9.NewProvisionerFromEnv()
	}
	if provisionerErr != nil {
		isWanted := false
		if providerType == tenant.ProviderTiDBCloudNative && os.Getenv("DRIVE9_TIDBCLOUD_NATIVE_API_URL") != "" {
			isWanted = true
		}
		if isWanted {
			logger.Error(context.Background(), "provisioner_failed", zap.String("provider", providerType), zap.Error(provisionerErr))
			os.Exit(1)
		}
		logger.Warn(context.Background(), "provisioner_not_configured", zap.String("provider", providerType), zap.Error(provisionerErr))
	} else {
		logger.Info(context.Background(), "provisioner_configured", zap.String("provider", providerType))
	}

	var legacyStarterProvisioner tenant.Provisioner
	if legacy, err := starter.NewLegacyProvisionerFromEnv(); err != nil {
		if starter.LegacyEnvPresent() {
			logger.Warn(context.Background(), "legacy_starter_provisioner_not_configured", zap.Error(err))
		}
	} else {
		legacyStarterProvisioner = legacy
		logger.Info(context.Background(), "legacy_starter_provisioner_configured")
	}

	var vaultMasterKey []byte
	if vaultMKHex != "" {
		vaultMasterKey, err = hex.DecodeString(vaultMKHex)
		if err != nil {
			die(fmt.Errorf("invalid DRIVE9_VAULT_MASTER_KEY: %w", err))
		}
	}

	var pool *tenant.Pool
	var leaderManager *leader.Manager
	var tokenSecret []byte
	if tokenHex != "" {
		tokenSecret, err = hex.DecodeString(tokenHex)
		if err != nil {
			die(fmt.Errorf("invalid DRIVE9_TOKEN_SIGNING_KEY: %w", err))
		}
		eKey := masterHex
		eType := encrypt.Type(encryptType)
		if strings.EqualFold(string(eType), string(encrypt.TypeKMS)) || strings.EqualFold(string(eType), string(encrypt.TypeAliyunKMS)) || strings.EqualFold(string(eType), string(encrypt.TypeTencentKMS)) {
			eKey = kmsKey
		}
		enc, err := encrypt.New(context.Background(), encrypt.Config{
			Type:              eType,
			Key:               eKey,
			Region:            s3cfg.Region,
			AliyunKMSEndpoint: aliyunKMSEndpoint,
		})
		if err != nil {
			die(fmt.Errorf("create encryptor: %w", err))
		}

		if err := pingControlPlaneDBWithRetry(context.Background(), store, defaultStartupRetryOptions()); err != nil {
			die(fmt.Errorf("control-plane db unavailable: %w", err))
		}

		// Create a leader election manager for gating background schedulers.
		// When disabled (single-pod), IsLeader always returns true and workers
		// start immediately. In multi-pod mode, workers start only on the leader.
		if os.Getenv("DRIVE9_LEADER_DISABLED") == "1" || os.Getenv("DRIVE9_LEADER_DISABLED") == "true" {
			leaderManager = leader.NewManager(store.DB(), leader.WithDisabled())
		} else {
			leaderOpts := []leader.Option{
				leader.WithLockName(envOr("DRIVE9_LEADER_LOCK_NAME", "drive9:leader")),
			}
			if v := envOr("DRIVE9_LEADER_HEARTBEAT_INTERVAL", ""); v != "" {
				if d, err := time.ParseDuration(v); err == nil {
					leaderOpts = append(leaderOpts, leader.WithHeartbeatInterval(d))
				} else {
					logger.Warn(context.Background(), "leader_heartbeat_interval_invalid_falling_back_to_default",
						zap.String("env", "DRIVE9_LEADER_HEARTBEAT_INTERVAL"),
						zap.String("value", v),
						zap.Error(err))
				}
			}
			leaderManager = leader.NewManager(store.DB(), leaderOpts...)
		}
		defer leaderManager.Stop()

		pool = tenant.NewPool(tenant.PoolConfig{
			S3Dir:                        s3cfg.Dir,
			PublicURL:                    publicBaseURL(addr),
			S3Bucket:                     s3cfg.Bucket,
			S3Region:                     s3cfg.Region,
			S3Prefix:                     s3cfg.Prefix,
			S3RoleARN:                    s3cfg.RoleARN,
			S3Endpoint:                   s3cfg.Endpoint,
			S3ForcePathStyle:             s3cfg.ForcePathStyle,
			S3AccessKeyID:                s3cfg.AccessKeyID,
			S3SecretAccessKey:            s3cfg.SecretAccessKey,
			S3SessionToken:               s3cfg.SessionToken,
			S3EncryptionPolicy:           s3cfg.EncryptionPolicy,
			BackendOptions:               backendOptions,
			MaxTenants:                   envInt("DRIVE9_POOL_MAX_TENANTS", 0),
			IdleTimeout:                  envDuration("DRIVE9_POOL_IDLE_TTL", 5*time.Minute),
			IdleReapInterval:             envDuration("DRIVE9_POOL_IDLE_REAP_INTERVAL", 2*time.Minute),
			DisableDatabaseAutoEmbedding: disableDatabaseAutoEmbedding,
			LeaderChecker:                leaderManager,
		}, enc)
		defer pool.Close()

		pool.SetMetaStore(store)
		pool.Start(context.Background())

		// The mutation replay and expiry sweep workers are owned by the server
		// (started/stopped in its leader-gated startLeaderWorkers/stopLeaderWorkers),
		// so they follow leadership transitions alongside the other leader-gated
		// schedulers. main.go no longer registers a competing SetCallbacks pair
		// (that earlier pair was clobbered by the server's own SetCallbacks and
		// never fired).

		// TODO: Run ValidateDurableAsyncExtractRequiresTenantWorker only when this process
		// can serve tenants that enqueue durable audio_extract_text / img_extract_text
		// (database auto-embedding: tidb_zero, tidb_cloud_native). pool != nil is too broad
		// for db9-only pools, which never hit that path but still get forced to configure
		// DRIVE9_EMBED_* when async extract is wired on the template (PR #159 review).
		if err := server.ValidateDurableAsyncExtractRequiresTenantWorker(server.Config{
			Meta:                         store,
			Pool:                         pool,
			Provisioner:                  provisioner,
			LegacyStarterProvisioner:     legacyStarterProvisioner,
			TokenSecret:                  tokenSecret,
			VaultMasterKey:               vaultMasterKey,
			S3Dir:                        s3cfg.Dir,
			MaxUploadBytes:               maxUploadBytes,
			InlineThreshold:              backendOptions.InlineThreshold,
			Logger:                       srvLogger,
			SemanticEmbedder:             semanticEmbedder,
			TenantWorkers:                tenantWorkerOpts,
			TiDBAutoEmbeddingConfig:      autoEmbeddingConfig,
			TiDBAutoEmbeddingAPIKey:      autoEmbeddingAPIKey,
			TiDBAutoEmbeddingAPIBase:     autoEmbeddingAPIBase,
			DisableDatabaseAutoEmbedding: disableDatabaseAutoEmbedding,
		}, backendOptions, false); err != nil {
			die(err)
		}
	}

	slockOAuth, err := slockOAuthFromEnv()
	if err != nil {
		die(err)
	}
	if slockOAuth != nil {
		logger.Info(context.Background(), "slock_oauth_enabled")
	} else {
		logger.Info(context.Background(), "slock_oauth_disabled")
	}

	// Unified tenant outbox notification configuration. The outbox poller reads
	// the central tenant_notify_outbox (always-provisioned meta DB) at 200ms and
	// dispatches by work_mask. See pkg/server/tenant_outbox_poller.go.
	sseNotifyRetention := envDuration("DRIVE9_SSE_NOTIFY_RETENTION", time.Hour)
	podID := strings.TrimSpace(os.Getenv("DRIVE9_POD_ID"))
	podAddr := strings.TrimSpace(os.Getenv("DRIVE9_POD_ADDR"))
	// Default podAddr to the listen addr only if it's a real, non-wildcard
	// address. The pod registry requires a full base URL for peer routing.
	// Wildcard binds like ":9009", "0.0.0.0:9009", or "[::]:9009" are not
	// dialable, so we leave podAddr empty in that case — the podRegistry still
	// reports subscriptions and the leader sweeps stale pods.
	if podAddr == "" && podID != "" {
		if host, port, err := net.SplitHostPort(addr); err == nil && host != "" && host != "0.0.0.0" && host != "::" {
			podAddr = "http://" + net.JoinHostPort(host, port)
		}
	}
	var podNotifySecret []byte
	if raw := strings.TrimSpace(os.Getenv("DRIVE9_POD_NOTIFY_SECRET")); raw != "" {
		podNotifySecret = []byte(raw)
	}
	if podID != "" {
		logger.Info(context.Background(), "tenant_outbox_config",
			zap.String("pod_id", podID),
			zap.String("pod_addr", podAddr),
			zap.Duration("retention", sseNotifyRetention),
			zap.Bool("push_enabled", podNotifySecret != nil))
	}

	die(server.NewWithConfig(server.Config{
		Meta:                            store,
		Pool:                            pool,
		Provisioner:                     provisioner,
		LegacyStarterProvisioner:        legacyStarterProvisioner,
		TokenSecret:                     tokenSecret,
		VaultMasterKey:                  vaultMasterKey,
		VaultIssuerURL:                  vaultIssuerURL(addr),
		PublicURL:                       publicBaseURL(addr),
		S3Dir:                           s3cfg.Dir,
		MaxUploadBytes:                  maxUploadBytes,
		TenantPoolMaxSize:               tenantPoolMaxSize,
		TenantPoolRefillFreeRatio:       tenantPoolRefillFreeRatio,
		InlineThreshold:                 backendOptions.InlineThreshold,
		Logger:                          srvLogger,
		SemanticEmbedder:                semanticEmbedder,
		TenantWorkers:                   tenantWorkerOpts,
		SlockOAuth:                      slockOAuth,
		TiDBAutoEmbeddingConfig:         autoEmbeddingConfig,
		TiDBAutoEmbeddingAPIKey:         autoEmbeddingAPIKey,
		TiDBAutoEmbeddingAPIBase:        autoEmbeddingAPIBase,
		DisableDatabaseAutoEmbedding:    disableDatabaseAutoEmbedding,
		Leader:                          leaderManager,
		TenantOutboxPollInterval:        envDuration("DRIVE9_TENANT_OUTBOX_POLL_INTERVAL_MS", 200*time.Millisecond),
		TenantOutboxCursorFlushInterval: envDuration("DRIVE9_TENANT_OUTBOX_CURSOR_FLUSH_MS", 5000*time.Millisecond),
		TenantShardRefreshInterval:      envDuration("DRIVE9_TENANT_SHARD_REFRESH_MS", 5000*time.Millisecond),
		TenantMaintenanceInterval:       envDuration("DRIVE9_TENANT_MAINTENANCE_INTERVAL_MS", 300000*time.Millisecond),
		SafetyNetScanInterval:           envDuration("DRIVE9_SAFETY_NET_SCAN_INTERVAL_MS", 5*time.Minute),
		SSENotifyRetention:              sseNotifyRetention,
		PodID:                           podID,
		PodAddr:                         podAddr,
		PodNotifySecret:                 podNotifySecret,
	}).ListenAndServe(addr))
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func slockOAuthFromEnv() (*slockoauth.Client, error) {
	origin := strings.TrimSpace(os.Getenv("DRIVE9_SLOCK_ORIGIN"))
	if origin == "" {
		return nil, nil
	}
	cfg := slockoauth.Config{
		Origin:       origin,
		APIOrigin:    strings.TrimSpace(os.Getenv("DRIVE9_SLOCK_API_ORIGIN")),
		ClientID:     strings.TrimSpace(os.Getenv("DRIVE9_SLOCK_CLIENT_ID")),
		ClientSecret: strings.TrimSpace(os.Getenv("DRIVE9_SLOCK_CLIENT_SECRET")),
		PublicURL:    strings.TrimSpace(os.Getenv("DRIVE9_PUBLIC_URL")),
	}
	if cfg.APIOrigin == "" || cfg.ClientID == "" || cfg.ClientSecret == "" || cfg.PublicURL == "" {
		return nil, fmt.Errorf("DRIVE9_SLOCK_ORIGIN enables Slock OAuth; DRIVE9_SLOCK_API_ORIGIN, DRIVE9_SLOCK_CLIENT_ID, DRIVE9_SLOCK_CLIENT_SECRET, and DRIVE9_PUBLIC_URL must also be set")
	}
	return slockoauth.New(cfg)
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
	encryptionPolicy := meta.DefaultS3EncryptionPolicy()
	if mode := strings.TrimSpace(os.Getenv("DRIVE9_S3_ENCRYPTION_MODE")); mode != "" {
		encryptionPolicy.Mode = meta.S3EncryptionMode(mode)
	}
	encryptionPolicy.KMSKeyID = strings.TrimSpace(os.Getenv("DRIVE9_S3_KMS_KEY_ID"))
	encryptionPolicy.BucketKeyEnabled = envBool("DRIVE9_S3_BUCKET_KEY_ENABLED", true)
	return s3Config{
		Dir:              dir,
		Bucket:           strings.TrimSpace(os.Getenv("DRIVE9_S3_BUCKET")),
		Region:           region,
		Prefix:           strings.TrimSpace(os.Getenv("DRIVE9_S3_PREFIX")),
		RoleARN:          strings.TrimSpace(os.Getenv("DRIVE9_S3_ROLE_ARN")),
		Endpoint:         strings.TrimSpace(os.Getenv("DRIVE9_S3_ENDPOINT")),
		ForcePathStyle:   envBool("DRIVE9_S3_FORCE_PATH_STYLE", false),
		AccessKeyID:      strings.TrimSpace(os.Getenv("DRIVE9_S3_ACCESS_KEY_ID")),
		SecretAccessKey:  strings.TrimSpace(os.Getenv("DRIVE9_S3_SECRET_ACCESS_KEY")),
		SessionToken:     strings.TrimSpace(os.Getenv("DRIVE9_S3_SESSION_TOKEN")),
		EncryptionPolicy: encryptionPolicy,
	}
}

func (cfg s3Config) validate() error {
	if err := meta.ValidateGlobalS3EncryptionPolicy(cfg.EncryptionPolicy); err != nil {
		return err
	}
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
  drive9-server schema dump-init-sql --provider <db9|tidb_zero|tidb_cloud_native>

environment:
  DRIVE9_LISTEN_ADDR serve listen address (default: :9009)
  DRIVE9_PUBLIC_URL  externally reachable base URL for presigned URLs (required for remote clients)
  DRIVE9_META_DSN    control-plane MySQL DSN (required)
  DRIVE9_POOL_MAX_TENANTS max cached tenant user DB pools per pod (default: 1024)
  DRIVE9_POOL_IDLE_TTL  idle duration before a cached tenant backend is evicted (default: 5m, 0=disabled)
  DRIVE9_POOL_IDLE_REAP_INTERVAL  how often the idle reaper scans (default: 2m)
  DRIVE9_META_DB_MAX_OPEN_CONNS max open connections for the per-pod meta DB pool (default: 100)
  DRIVE9_META_DB_MAX_IDLE_CONNS max idle connections for the per-pod meta DB pool (default: 20)
  DRIVE9_USER_DB_MAX_OPEN_CONNS max open connections for each cached tenant user DB pool (default: 6)
  DRIVE9_USER_DB_MAX_IDLE_CONNS max idle connections for each cached tenant user DB pool (default: 2)
  DRIVE9_USER_SCHEMA_DB_MAX_OPEN_CONNS max open connections for tenant schema-init DB pools (default: 8)
  DRIVE9_USER_SCHEMA_DB_MAX_IDLE_CONNS max idle connections for tenant schema-init DB pools (default: 2)
  DRIVE9_DB_HEALTH_PROBE_INTERVAL_SECONDS DB health probe interval seconds (default: 15)
  DRIVE9_DB_HEALTH_PROBE_TIMEOUT_SECONDS DB health probe timeout seconds (default: 3)
  DRIVE9_DB_HEALTH_PROBE_META_ENABLED true|false to probe the control-plane DB (default: true)
  DRIVE9_ENCRYPT_TYPE local_aes|kms|aliyun_kms|tencent_kms
  DRIVE9_MASTER_KEY  32-byte hex key for local_aes encryptor
  DRIVE9_ENCRYPT_KEY KMS key id or alias (required for kms/aliyun_kms/tencent_kms)
  DRIVE9_ALIYUN_KMS_ENDPOINT custom Aliyun KMS endpoint, e.g. a VPC endpoint (no https:// prefix)
                             note: aliyun_kms/tencent_kms reads the region from DRIVE9_S3_REGION
                             note: TLS verification is skipped automatically when this is set
  DRIVE9_TOKEN_SIGNING_KEY  32-byte hex key for JWT API key signing
  DRIVE9_VAULT_MASTER_KEY   32-byte hex key for vault DEK wrapping (omit to disable vault)
  DRIVE9_MAX_UPLOAD_BYTES maximum allowed upload size in bytes (default: %d, minimum: 1048576)
  DRIVE9_DEFAULT_STORAGE_QUOTA_BYTES fallback per-tenant total storage limit when no explicit quota is configured (default: %d)
  DRIVE9_LOG_LEVEL debug|info|warn|error (default: info)
  DRIVE9_BENCH_TIMING_LOG_ENABLED true|false to emit benchmark timing logs on successful server hot paths (default: false)
  DRIVE9_OPEN_POOL_TIMING_LOG_ENABLED true|false to emit slow tenant pool and authentication timing logs (default: true)
  DRIVE9_OPEN_POOL_TIMING_SLOW_MS minimum duration in ms for open-pool timing logs (default: 500, 0=all)
  DRIVE9_DB_TRACE_LOG_ENABLED true|false to emit DB operation trace logs with redacted SQL (default: true)
  DRIVE9_DB_SLOW_TRACE_MS minimum DB operation duration in ms for DB trace logs (default: 300, 0=all)
  DRIVE9_QUOTA_USAGE_CACHE_TTL soft small-write central usage cache TTL, e.g. 250ms or 1s
  DRIVE9_QUOTA_PENDING_DELTAS_CACHE_TTL soft small-write in-process pending mutation cache TTL, e.g. 250ms or 1s
  DRIVE9_QUOTA_REPLAY_POLL_MS central quota mutation replay poll interval in milliseconds (default: 1000)
  DRIVE9_QUOTA_REPLAY_MIN_AGE_MS minimum pending mutation age before replay in milliseconds (default: 1000)
  DRIVE9_QUOTA_REPLAY_OBSERVE_MS replay backlog observation interval in milliseconds (default: 10000)
  DRIVE9_DISABLE_AUTO_EMBEDDING true|false disable TiDB database-managed auto-embedding (default: false)
                                set to true when the TiDB Cloud cluster has no supported embedding provider
  DRIVE9_TIDB_AUTO_EMBEDDING_MODEL TiDB EMBED_TEXT model for auto-embedding generated columns
                                   supported provider prefixes include tidbcloud_free/amazon, tidbcloud_free/cohere,
                                   openai, cohere, jina_ai, gemini, huggingface, nvidia_nim, nvidia
                                   (default: tidbcloud_free/amazon/titan-embed-text-v2)
  DRIVE9_TIDB_AUTO_EMBEDDING_DIMENSIONS TiDB EMBED_TEXT vector dimensions
                                        known models use documented defaults; unknown BYOK models require this value
  DRIVE9_TIDB_AUTO_EMBEDDING_API_KEY provider API key for BYOK auto-embedding models
                                    required by openai, cohere, jina_ai, gemini, huggingface, nvidia_nim, nvidia
  DRIVE9_TIDB_AUTO_EMBEDDING_API_BASE provider base endpoint for models that require it
                                     optional for openai models; set it for Azure OpenAI endpoints
  DRIVE9_TENANT_PROVIDER db9|tidb_zero|tidb_cloud_native (default for provisioning)
  DRIVE9_TIDBCLOUD_DEFAULT_SPENDING_LIMIT default TiDB Cloud Cluster spendingLimit.monthly; native defaults to 1000 when unset
  DRIVE9_TIDBCLOUD_NATIVE_API_URL TiDB Cloud Cluster API base URL for tidb_cloud_native
  DRIVE9_TIDBCLOUD_NATIVE_CLOUD_PROVIDER cloud provider for tidb_cloud_native cluster creation, e.g. aws
  DRIVE9_TIDBCLOUD_NATIVE_REGION region for tidb_cloud_native cluster creation, e.g. us-east-1
  DRIVE9_TIDBCLOUD_NATIVE_DEFAULT_DATABASE_NAME default tidb_cloud_native database name (default: tidbcloud_fs)
  DRIVE9_TIDBCLOUD_NATIVE_PUBLIC_KEY optional default TiDB Cloud API public key for tidb_cloud_native create/delete when caller omits it
  DRIVE9_TIDBCLOUD_NATIVE_PRIVATE_KEY optional default TiDB Cloud API private key for tidb_cloud_native create/delete when caller omits it
  DRIVE9_TIDBCLOUD_NATIVE_USE_PRIVATE_ENDPOINT true|false to use TiDB Cloud private endpoint hosts for tidb_cloud_native
  DRIVE9_TIDBCLOUD_PRIVATE_ENDPOINT_HOST_MAP comma-separated public_host=private_host mappings (also accepts public_host:private_host);
                                             when set, disables legacy single-host private endpoint overrides
  DRIVE9_TIDBCLOUD_TENCENT_PRIVATE_ENDPOINT_HOST legacy tencentcloud private endpoint fallback host, used only when host map is unset
  DRIVE9_TIDBCLOUD_ALICLOUD_PRIVATE_ENDPOINT_DOMAIN legacy alicloud private endpoint fallback domain, used only when host map is unset
  DRIVE9_TENANT_POOL_MAX_SIZE maximum admin tenant pool size (default: %d)
  DRIVE9_TENANT_POOL_REFILL_FREE_RATIO claim-triggered refill runs when active free tenants fall below this ratio (default: %.2f)
  DRIVE9_SLOCK_ORIGIN Slock browser origin; when set, enables /v1/auth/slock/*
  DRIVE9_SLOCK_API_ORIGIN Slock API origin (required when DRIVE9_SLOCK_ORIGIN is set)
  DRIVE9_SLOCK_CLIENT_ID Slock connected-app client id (required when DRIVE9_SLOCK_ORIGIN is set)
  DRIVE9_SLOCK_CLIENT_SECRET Slock connected-app client secret (required when DRIVE9_SLOCK_ORIGIN is set)

  SSE cross-pod notification (set DRIVE9_POD_ID to enable; required for large multi-tenant deployments):
  DRIVE9_POD_ID     unique pod identifier; when set, this pod registers in the central
                    pod_registry and reports its SSE subscriber tenant set so peers can
                    route push notifications. The outbox poller runs on all multi-tenant
                    pods regardless; cross-pod HTTP push requires DRIVE9_POD_NOTIFY_SECRET.
  DRIVE9_POD_ADDR   full base URL (e.g. http://10.0.0.5:9009) reachable by other pods for
                    peer push notifications. Defaults to http://<listen-addr> when the
                    listen address is a non-wildcard host; must be set explicitly in
                    multi-host deployments.
  DRIVE9_POD_NOTIFY_SECRET  shared bearer token for the legacy /v1/internal/sse-notify
                    pod-to-pod endpoint (retained for backward compat; the unified
                    outbox poller is the primary cross-pod path).
  DRIVE9_SSE_NOTIFY_RETENTION      how long tenant_notify_outbox rows are kept before
                    leader pruning (default: 1h)
  DRIVE9_TENANT_OUTBOX_POLL_INTERVAL_MS  unified outbox poll interval (default: 200)
  DRIVE9_TENANT_OUTBOX_CURSOR_FLUSH_MS   how often the poller persists its cursor (default: 5000)
  DRIVE9_TENANT_SHARD_REFRESH_MS         shard resolver pod ring refresh interval (default: 5000)
  DRIVE9_TENANT_MAINTENANCE_INTERVAL_MS  piggyback maintenance throttle per tenant (default: 300000)
  DRIVE9_SAFETY_NET_SCAN_INTERVAL_MS     per-pod safety-net scan interval as a Go
                    duration (e.g. 5m, 24h); 0 disables the scan (default: 5m)

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
  DRIVE9_S3_ENCRYPTION_MODE none|sse-s3|sse-kms|dsse-kms (default: none; recommend sse-kms for production)
  DRIVE9_S3_KMS_KEY_ID KMS key ARN/id required for sse-kms/dsse-kms
  DRIVE9_S3_BUCKET_KEY_ENABLED true|false for SSE-KMS bucket keys (default: true)

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
  DRIVE9_SEMANTIC_RETRY_BASE_MS base retry backoff in milliseconds (default: 200)
  DRIVE9_SEMANTIC_RETRY_MAX_MS max retry backoff in milliseconds (default: 30000)
  DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY max concurrent tasks per tenant (default: 1)

  Image extraction (async image -> text for search):
  DRIVE9_IMAGE_EXTRACT_ENABLED true|false (default: false)
  DRIVE9_IMAGE_EXTRACT_QUEUE_SIZE buffered task queue size (default: 128)
  DRIVE9_IMAGE_EXTRACT_WORKERS number of workers (default: 1)
  DRIVE9_IMAGE_EXTRACT_MAX_BYTES max image bytes processed per task (default: 8388608)
  DRIVE9_IMAGE_EXTRACT_TIMEOUT_SECONDS extractor timeout seconds (default: 20)
  DRIVE9_IMAGE_EXTRACT_MAX_TEXT_BYTES max extracted text stored in files.content_text (default: 32768)
  DRIVE9_IMAGE_EXTRACT_API_BASE OpenAI-compatible base URL (optional)
  DRIVE9_IMAGE_EXTRACT_API_KEY  API key for DRIVE9_IMAGE_EXTRACT_API_BASE (optional)
  DRIVE9_IMAGE_EXTRACT_MODEL    model name for vision extraction (optional)
  DRIVE9_IMAGE_EXTRACT_PROMPT   custom extraction prompt (optional)
  DRIVE9_IMAGE_EXTRACT_MAX_TOKENS max model output tokens (default: 4096)

  Audio extraction (async audio -> text for search; MVP durable path is TiDB auto-embedding only):
  Durable audio_extract_text tasks enqueue only for tenants with database auto-embedding
  (tidb_zero / tidb_cloud_native). For db9-only or other app-managed tenants these vars do
  not enable that semantic_tasks pipeline. When enabled, explicit provider wiring is required:
  DRIVE9_AUDIO_EXTRACT_ENABLED true|false (default: false)
  DRIVE9_AUDIO_EXTRACT_MODE     openai|qwen-asr (default: openai)
  DRIVE9_AUDIO_EXTRACT_MAX_BYTES max audio bytes processed per task (default: 33554432 for openai, 7340032 for qwen-asr)
  DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS extractor timeout seconds (default: 120)
  DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES max transcript bytes stored in files.content_text (default: 8192)
  DRIVE9_AUDIO_EXTRACT_API_BASE OpenAI-compatible base URL (required when enabled)
  DRIVE9_AUDIO_EXTRACT_API_KEY  API key for DRIVE9_AUDIO_EXTRACT_API_BASE (required when enabled)
  DRIVE9_AUDIO_EXTRACT_MODEL    model name for audio transcription (required when enabled)
  DRIVE9_AUDIO_EXTRACT_PROMPT   optional provider prompt for transcription

schema tooling:
  dump-init-sql writes the exact init schema SQL to stdout so external systems
  can stay in sync with drive9's schema source of truth.
`, server.DefaultMaxUploadBytes, meta.DefaultMaxStorageBytes(), server.DefaultTenantPoolMaxSize, server.DefaultTenantPoolRefillFreeRatio)
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
	if strings.TrimSpace(os.Getenv("DRIVE9_QUOTA_SOURCE")) != "" {
		return backend.Options{}, fmt.Errorf("DRIVE9_QUOTA_SOURCE has been removed; central quota is now driven by meta-store wiring")
	}
	opts.MaxTenantStorageBytes = envInt64("DRIVE9_MAX_TENANT_STORAGE_BYTES", 50*(1<<30))
	if opts.MaxTenantStorageBytes <= 0 {
		return backend.Options{}, fmt.Errorf("DRIVE9_MAX_TENANT_STORAGE_BYTES must be a positive integer")
	}

	opts.InlineThreshold = envInt64("DRIVE9_INLINE_THRESHOLD", backend.DefaultInlineThreshold)
	if opts.InlineThreshold <= 0 {
		return backend.Options{}, fmt.Errorf("DRIVE9_INLINE_THRESHOLD must be a positive integer")
	}
	opts.TextExtractMaxBytes = envInt64("DRIVE9_TEXT_EXTRACT_MAX_BYTES", backend.DefaultTextExtractMaxBytes)
	if opts.TextExtractMaxBytes <= 0 {
		return backend.Options{}, fmt.Errorf("DRIVE9_TEXT_EXTRACT_MAX_BYTES must be a positive integer")
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

	imageExtract, err := buildImageExtractOptionsFromEnv()
	if err != nil {
		return backend.Options{}, err
	}
	if imageExtract.Enabled {
		opts.AsyncImageExtract = imageExtract
	}
	audioExtract, err := buildAudioExtractOptionsFromEnv()
	if err != nil {
		return backend.Options{}, err
	}
	if audioExtract.Enabled {
		opts.AsyncAudioExtract = audioExtract
	}
	videoExtract, err := buildVideoExtractOptionsFromEnv()
	if err != nil {
		return backend.Options{}, err
	}
	if videoExtract.Enabled {
		opts.AsyncVideoExtract = videoExtract
	}
	return opts, nil
}

func buildImageExtractOptionsFromEnv() (backend.AsyncImageExtractOptions, error) {
	if !envBool("DRIVE9_IMAGE_EXTRACT_ENABLED", false) {
		return backend.AsyncImageExtractOptions{}, nil
	}
	async := backend.AsyncImageExtractOptions{
		Enabled:             true,
		QueueSize:           envInt("DRIVE9_IMAGE_EXTRACT_QUEUE_SIZE", 128),
		Workers:             envInt("DRIVE9_IMAGE_EXTRACT_WORKERS", 1),
		MaxImageBytes:       envInt64("DRIVE9_IMAGE_EXTRACT_MAX_BYTES", 8<<20),
		TaskTimeout:         time.Duration(envInt("DRIVE9_IMAGE_EXTRACT_TIMEOUT_SECONDS", 20)) * time.Second,
		MaxExtractTextBytes: envInt("DRIVE9_IMAGE_EXTRACT_MAX_TEXT_BYTES", backend.DefaultImageExtractMaxTextBytes),
	}

	baseURL := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_API_BASE"))
	apiKey := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_API_KEY"))
	model := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_MODEL"))
	prompt := strings.TrimSpace(os.Getenv("DRIVE9_IMAGE_EXTRACT_PROMPT"))
	maxTokens := envInt("DRIVE9_IMAGE_EXTRACT_MAX_TOKENS", backend.DefaultOpenAIImageExtractMaxTokens)

	configured := baseURL != "" || apiKey != "" || model != ""
	if configured {
		if baseURL == "" || apiKey == "" || model == "" {
			logger.Error(context.Background(), "image_extract_mode_invalid_config",
				zap.Bool("base_url_present", baseURL != ""),
				zap.Bool("api_key_present", apiKey != ""),
				zap.Bool("model_present", model != ""))
			return backend.AsyncImageExtractOptions{}, fmt.Errorf("DRIVE9_IMAGE_EXTRACT_API_BASE, DRIVE9_IMAGE_EXTRACT_API_KEY and DRIVE9_IMAGE_EXTRACT_MODEL must be set together")
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
			return backend.AsyncImageExtractOptions{}, fmt.Errorf("init image extractor: %w", err)
		}
		async.Extractor = backend.NewFallbackImageTextExtractor(extractor, backend.NewBasicImageTextExtractor())
		logger.Info(context.Background(), "image_extract_mode_openai_compatible",
			zap.String("model", model), zap.String("base_url", baseURL))
	} else {
		async.Extractor = backend.NewBasicImageTextExtractor()
		logger.Info(context.Background(), "image_extract_mode_basic_fallback")
	}

	return async, nil
}

func buildAudioExtractOptionsFromEnv() (backend.AsyncAudioExtractOptions, error) {
	if !envBool("DRIVE9_AUDIO_EXTRACT_ENABLED", false) {
		return backend.AsyncAudioExtractOptions{}, nil
	}
	audioMode := strings.ToLower(strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_MODE")))
	if audioMode == "" {
		audioMode = "openai"
	}
	audioBaseURL := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_API_BASE"))
	audioAPIKey := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_API_KEY"))
	audioModel := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_MODEL"))
	audioPrompt := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_PROMPT"))
	if audioBaseURL == "" || audioAPIKey == "" || audioModel == "" {
		return backend.AsyncAudioExtractOptions{}, fmt.Errorf("DRIVE9_AUDIO_EXTRACT_API_BASE, DRIVE9_AUDIO_EXTRACT_API_KEY and DRIVE9_AUDIO_EXTRACT_MODEL must be set together when DRIVE9_AUDIO_EXTRACT_ENABLED=true")
	}
	audioTimeout := time.Duration(envInt("DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS", 120)) * time.Second
	var audioExtractor backend.AudioTextExtractor
	maxAudioBytesDefault := int64(32 << 20)
	switch audioMode {
	case "openai":
		audioResponseFormat := strings.TrimSpace(os.Getenv("DRIVE9_AUDIO_EXTRACT_RESPONSE_FORMAT"))
		extractor, err := backend.NewOpenAIAudioTextExtractor(backend.OpenAIAudioTextExtractorConfig{
			BaseURL:        audioBaseURL,
			APIKey:         audioAPIKey,
			Model:          audioModel,
			Prompt:         audioPrompt,
			ResponseFormat: audioResponseFormat,
			Timeout:        audioTimeout,
		})
		if err != nil {
			return backend.AsyncAudioExtractOptions{}, fmt.Errorf("init openai audio extractor: %w", err)
		}
		audioExtractor = extractor
	case "qwen-asr":
		// Qwen-ASR enforces a 10 MB cap on the base64-encoded data URL.
		// 7 MiB raw audio encodes to about 9.8 MB, leaving room for the prefix.
		maxAudioBytesDefault = 7 << 20
		extractor, err := backend.NewQwenASRAudioTextExtractor(backend.QwenASRAudioTextExtractorConfig{
			BaseURL: audioBaseURL,
			APIKey:  audioAPIKey,
			Model:   audioModel,
			Prompt:  audioPrompt,
			Timeout: audioTimeout,
		})
		if err != nil {
			return backend.AsyncAudioExtractOptions{}, fmt.Errorf("init qwen asr audio extractor: %w", err)
		}
		audioExtractor = extractor
	default:
		return backend.AsyncAudioExtractOptions{}, fmt.Errorf("DRIVE9_AUDIO_EXTRACT_MODE must be %q or %q when DRIVE9_AUDIO_EXTRACT_ENABLED=true (got %q)", "openai", "qwen-asr", audioMode)
	}

	async := backend.AsyncAudioExtractOptions{
		Enabled:             true,
		MaxAudioBytes:       envInt64("DRIVE9_AUDIO_EXTRACT_MAX_BYTES", maxAudioBytesDefault),
		TaskTimeout:         audioTimeout,
		MaxExtractTextBytes: envInt("DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES", 8<<10),
		Extractor:           audioExtractor,
	}
	logger.Info(context.Background(), "audio_extract_mode_configured",
		zap.String("mode", audioMode), zap.String("model", audioModel), zap.String("base_url", audioBaseURL))
	return async, nil
}

func buildVideoExtractOptionsFromEnv() (backend.AsyncVideoExtractOptions, error) {
	// Single-config design: DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST controls
	// everything. Empty/unset = off. "*" = all tenants. Otherwise comma-
	// separated tenant IDs.
	raw := strings.TrimSpace(os.Getenv("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST"))
	allTenants, tenantAllowlist, err := backend.ParseVideoExtractTenantAllowlist(raw)
	if err != nil {
		return backend.AsyncVideoExtractOptions{}, fmt.Errorf("DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST: %w", err)
	}
	// Parser returns nil allowlist + allTenants=false when input is empty
	// or contains only commas/whitespace → off, no runtime init needed.
	if !allTenants && tenantAllowlist == nil {
		return backend.AsyncVideoExtractOptions{}, nil
	}

	videoBaseURL := strings.TrimSpace(os.Getenv("DRIVE9_VIDEO_EXTRACT_API_BASE"))
	videoAPIKey := strings.TrimSpace(os.Getenv("DRIVE9_VIDEO_EXTRACT_API_KEY"))
	videoModel := strings.TrimSpace(os.Getenv("DRIVE9_VIDEO_EXTRACT_MODEL"))
	if videoBaseURL == "" || videoAPIKey == "" || videoModel == "" {
		return backend.AsyncVideoExtractOptions{}, fmt.Errorf("DRIVE9_VIDEO_EXTRACT_API_BASE, DRIVE9_VIDEO_EXTRACT_API_KEY and DRIVE9_VIDEO_EXTRACT_MODEL must be set when DRIVE9_VIDEO_EXTRACT_TENANT_ALLOWLIST is non-empty")
	}
	videoPrompt := strings.TrimSpace(os.Getenv("DRIVE9_VIDEO_EXTRACT_PROMPT"))
	videoTimeout := time.Duration(envInt("DRIVE9_VIDEO_EXTRACT_TIMEOUT_SECONDS", 300)) * time.Second
	ffmpegPath := strings.TrimSpace(os.Getenv("DRIVE9_FFMPEG_PATH"))
	extractor, err := backend.NewOpenAIVideoTextExtractor(backend.OpenAIVideoTextExtractorConfig{
		BaseURL:       videoBaseURL,
		APIKey:        videoAPIKey,
		Model:         videoModel,
		Prompt:        videoPrompt,
		Timeout:       videoTimeout,
		FrameInterval: envInt("DRIVE9_VIDEO_EXTRACT_FRAME_INTERVAL", 5),
		MaxFrames:     envInt("DRIVE9_VIDEO_EXTRACT_MAX_FRAMES", 10),
		FFmpegPath:    ffmpegPath,
	})
	if err != nil {
		return backend.AsyncVideoExtractOptions{}, fmt.Errorf("init video extractor: %w", err)
	}
	async := backend.AsyncVideoExtractOptions{
		Enabled:             true,
		AllTenants:          allTenants,
		MaxVideoBytes:       envInt64("DRIVE9_VIDEO_EXTRACT_MAX_BYTES", 200<<20),
		MaxVideoLLMFiles:    envInt64("DRIVE9_VIDEO_EXTRACT_MAX_FILES", 50),
		TaskTimeout:         videoTimeout,
		MaxExtractTextBytes: envInt("DRIVE9_VIDEO_EXTRACT_MAX_TEXT_BYTES", 32<<10),
		Extractor:           extractor,
		TenantAllowlist:     tenantAllowlist,
	}
	logger.Info(context.Background(), "video_extract_mode_configured",
		zap.String("model", videoModel), zap.String("base_url", videoBaseURL),
		zap.Bool("all_tenants", allTenants), zap.Int("tenant_allowlist_size", len(tenantAllowlist)))
	return async, nil
}

func buildTenantWorkerConfigFromEnv() (embedding.Client, server.TenantWorkerOptions, error) {
	// Warn about deprecated env vars that are no longer parsed.
	for _, deprecated := range []string{"DRIVE9_SEMANTIC_RECOVER_INTERVAL_MS", "DRIVE9_SEMANTIC_TENANT_LIMIT"} {
		if os.Getenv(deprecated) != "" {
			fmt.Fprintf(os.Stderr, "WARNING: %s is deprecated and no longer used; recovery is now kick-driven and tenant scan limit is removed\n", deprecated)
		}
	}
	opts := server.TenantWorkerOptions{
		Workers:              envInt("DRIVE9_SEMANTIC_WORKERS", 1),
		PollInterval:         time.Duration(envInt("DRIVE9_SEMANTIC_POLL_INTERVAL_MS", 200)) * time.Millisecond,
		LeaseDuration:        time.Duration(envInt("DRIVE9_SEMANTIC_LEASE_SECONDS", 30)) * time.Second,
		RetryBaseDelay:       time.Duration(envInt("DRIVE9_SEMANTIC_RETRY_BASE_MS", 200)) * time.Millisecond,
		RetryMaxDelay:        time.Duration(envInt("DRIVE9_SEMANTIC_RETRY_MAX_MS", 30000)) * time.Millisecond,
		PerTenantConcurrency: envInt("DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY", 1),
	}
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

func dbHealthProbeOptionsFromEnv() metrics.DBHealthProbeOptions {
	return metrics.DBHealthProbeOptions{
		ProbeMeta: envBool("DRIVE9_DB_HEALTH_PROBE_META_ENABLED", true),
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

func tenantPoolMaxSizeFromEnv() (int, error) {
	raw := strings.TrimSpace(os.Getenv("DRIVE9_TENANT_POOL_MAX_SIZE"))
	if raw == "" {
		return server.DefaultTenantPoolMaxSize, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("invalid DRIVE9_TENANT_POOL_MAX_SIZE=%q: must be a positive integer", raw)
	}
	return v, nil
}

func tenantPoolRefillFreeRatioFromEnv() (float64, error) {
	raw := strings.TrimSpace(os.Getenv("DRIVE9_TENANT_POOL_REFILL_FREE_RATIO"))
	if raw == "" {
		return server.DefaultTenantPoolRefillFreeRatio, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 || v > 1 || v != v {
		return 0, fmt.Errorf("invalid DRIVE9_TENANT_POOL_REFILL_FREE_RATIO=%q: must be a number in (0,1]", raw)
	}
	return v, nil
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: %v; using %s\n", key, raw, err, fallback)
		return fallback
	}
	if v < 0 {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: duration must be non-negative; using %s\n", key, raw, fallback)
		return fallback
	}
	return v
}
