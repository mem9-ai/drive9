package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/embedding"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/s3client"
	"github.com/mem9-ai/drive9/pkg/slockoauth"
	"github.com/mem9-ai/drive9/pkg/tagutil"
	"github.com/mem9-ai/drive9/pkg/tenant"
	tenantschema "github.com/mem9-ai/drive9/pkg/tenant/schema"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"github.com/mem9-ai/drive9/pkg/traceid"
	"github.com/mem9-ai/drive9/pkg/vault"
	"go.uber.org/zap"
)

type Config struct {
	Meta        *meta.Store
	Pool        *tenant.Pool
	Provisioner tenant.Provisioner
	// LegacyStarterProvisioner is only used for delete/fork compatibility on
	// persisted tidb_cloud_starter tenants. New starter provisioning remains
	// disabled and NormalizeProvider does not accept tidb_cloud_starter.
	LegacyStarterProvisioner tenant.Provisioner
	TokenSecret              []byte
	LocalTenantAPIKey        string
	VaultMasterKey           []byte // 32-byte AES key for vault DEK wrapping; nil disables vault
	VaultIssuerURL           string // canonical server URL placed in grant `iss` claim; empty = server hostname unknown, grants disabled
	PublicURL                string // externally reachable base URL for client responses (e.g., slock callback server_url)
	Backend                  *backend.Dat9Backend
	LocalS3                  *s3client.LocalS3Client
	S3Dir                    string
	MaxUploadBytes           int64
	// TenantPoolMaxSize caps admin tenant pool create/update target size. Values
	// <= 0 are normalized to DefaultTenantPoolMaxSize; the pool is never
	// intentionally uncapped.
	TenantPoolMaxSize int
	// InlineThreshold is the server-wide DB-inline vs S3 cutoff surfaced to
	// clients via /v1/status. When 0, the value is inferred from
	// cfg.Backend.InlineThreshold() (or omitted in responses if no backend).
	InlineThreshold  int64
	Logger           *zap.Logger
	SemanticEmbedder embedding.Client
	TenantWorkers    TenantWorkerOptions
	SlockOAuth       SlockOAuthClient

	TiDBAutoEmbeddingConfig  tenantschema.TiDBAutoEmbeddingConfig
	TiDBAutoEmbeddingAPIKey  string
	TiDBAutoEmbeddingAPIBase string
	// DisableDatabaseAutoEmbedding suppresses runtime writes that would trigger
	// TiDB EMBED_TEXT, but TiDB tenants still use the normal auto schema.
	DisableDatabaseAutoEmbedding bool
	// Leader, when set, gates background schedulers (semantic worker, object GC,
	// tenant delete cleanup, pending tenant reconciler, one-time resume tasks,
	// central-quota mutation replay, upload-reservation expiry sweep, and
	// per-tenant FileGC) to run only on the leader pod. When nil or disabled, all
	// workers start unconditionally (single-pod mode).
	Leader *leader.Manager

	// SSE notification + unified tenant outbox configuration. The unified
	// outbox replaces per-tenant TiDB polling for SSE, semantic, file_gc, and
	// quota work. Every pod polls tenant_notify_outbox (always-provisioned
	// meta DB) and dispatches by work_mask. When Meta is nil (single-tenant
	// mode), these are unused.
	//
	// TenantOutboxPollInterval is the poller tick (default 200ms).
	// TenantOutboxCursorFlushInterval is how often the cursor is persisted
	// (default 5s).
	// TenantShardRefreshInterval is how often the shard resolver refreshes the
	// active pod ring (default 5s).
	// TenantMaintenanceInterval throttles piggybacked fs_events cleanup +
	// observation metrics per tenant (default 5min).
	// SafetyNetScanInterval is how often the leader runs the safety-net scan
	// (default 5min).
	// SSENotifyRetention is how long outbox rows are kept before leader-gated
	// pruning (default 1h).
	//
	// PodID uniquely identifies this pod in the central pod_registry. PodAddr
	// is the internally reachable address (host:port). When both are set, this
	// pod registers itself and reports its SSE subscriber set.
	//
	// PodNotifySecret is retained for backward compatibility (the internal
	// /v1/internal/sse-notify endpoint). Cross-pod push is superseded by the
	// unified outbox poller; the secret still gates the legacy endpoint.
	TenantOutboxPollInterval        time.Duration
	TenantOutboxCursorFlushInterval time.Duration
	TenantShardRefreshInterval      time.Duration
	TenantMaintenanceInterval       time.Duration
	SafetyNetScanInterval           time.Duration
	SSENotifyRetention              time.Duration
	PodID                           string
	PodAddr                         string
	PodNotifySecret                 []byte
}

type SlockOAuthClient interface {
	LoginURL() string
	ExchangeCode(ctx context.Context, code string) (slockoauth.Token, error)
	Userinfo(ctx context.Context, accessToken string) (slockoauth.UserInfo, error)
}

func isBackendQuotaExceeded(err error) bool {
	return errors.Is(err, backend.ErrStorageQuotaExceeded) ||
		errors.Is(err, backend.ErrFileSizeQuotaExceeded) ||
		errors.Is(err, backend.ErrFileCountQuotaExceeded)
}

type autoEmbeddingSchemaProvisioner interface {
	InitSchemaForAutoEmbeddingProfile(context.Context, string, tenantschema.TiDBAutoEmbeddingProfile) error
}

type tenantDatabaseEnsurer interface {
	EnsureDatabase(context.Context, string) error
}

type credentialProvisionRequestValidator interface {
	ValidateCredentialProvisionRequest(tenant.CredentialProvisionRequest) error
}

type provisioningRegionProvider interface {
	ProvisioningCloudProvider() string
	ProvisioningRegion() string
}

type nativeSystemUserProvisioner interface {
	// tenantID is reserved for audit/log naming; current native setup derives SQL principals from the DSN user.
	EnsureSystemUser(ctx context.Context, dsn, tenantID string) (username, password string, err error)
}

func (s *Server) provisionerForTenantProvider(provider string) tenant.Provisioner {
	switch provider {
	case tenant.ProviderTiDBCloudStarterLegacy:
		return s.legacyStarterProvisioner
	case tenant.ProviderTiDBCloudNative, tenant.ProviderTiDBZero, tenant.ProviderDB9:
		return s.provisioner
	default:
		return nil
	}
}

type Server struct {
	fallback                 *backend.Dat9Backend
	meta                     *meta.Store
	pool                     *tenant.Pool
	provisioner              tenant.Provisioner
	legacyStarterProvisioner tenant.Provisioner
	tokenSecret              []byte
	localTenantAPIKey        string
	vaultMK                  *vault.MasterKey
	vaultIssuerURL           string
	publicURL                string
	maxUploadBytes           int64
	tenantPoolMaxSize        int
	inlineThreshold          int64
	metrics                  *serverMetrics
	logger                   *zap.Logger
	mux                      *http.ServeMux
	events                   *eventBuses
	tenantWorker             *tenantWorkerManager
	shardResolver            *semanticShardResolver
	journalCursorSecret      []byte
	objectGCWorker           *objectGCWorker
	slockOAuth               SlockOAuthClient
	tidbAutoEmbedding        tenantAutoEmbeddingDefault
	disableDBAutoEmbed       bool
	forkWorkerCtx            context.Context
	forkWorkerCancel         context.CancelFunc
	forkWorkerWG             sync.WaitGroup
	forkWorkerMu             sync.Mutex
	forkWorkerClosed         bool
	tenantPoolLocks          sync.Map
	tenantPoolCreateLocks    sync.Map
	tenantPoolResumeJobs     sync.Map
	schemaInitErrors         sync.Map
	leader                   *leader.Manager
	// leaderWorkerCtx gates leader-only background schedulers. When leadership
	// changes, this context is cancelled and recreated. Workers that use it
	// (pending tenant reconciler, tenant delete cleanup, one-time resume tasks)
	// stop automatically on cancellation.
	leaderWorkerCtx      context.Context
	leaderWorkerCancel   context.CancelFunc
	leaderWorkerWG       sync.WaitGroup
	leaderWorkerMu       sync.Mutex
	leaderWorkersStarted bool
	// replayWorker and expirySweepWorker are leader-gated central quota
	// workers owned by the server (single callback owner). Started in
	// startLeaderWorkers and stopped in stopLeaderWorkers so they follow
	// leadership transitions rather than being wired separately in main.go.
	replayWorker      *backend.MutationReplayWorker
	expirySweepWorker *backend.ExpirySweepWorker

	// Unified tenant outbox components. The tenantOutboxPoller reads the
	// central tenant_notify_outbox table (in the always-provisioned meta DB)
	// on every pod and dispatches by work_mask: SSE bits wake the local bus;
	// semantic/file_gc/quota bits kick the tenantWorker on the shard owner.
	// The shardResolver determines shard ownership via jump consistent hashing
	// over the active pod ring. The podRegistry maintains this pod's presence
	// and subscriber set in the central DB. All run on every pod (not
	// leader-gated); the leader additionally sweeps stale pods and prunes the
	// outbox.
	podRegistry     *podRegistry
	podNotifySecret []byte
	notifyCancel    context.CancelFunc
	notifyWG        sync.WaitGroup
	// sseNotifyRetention is how long outbox rows are kept before leader pruning.
	sseNotifyRetention time.Duration
	// safetyNetScanInterval is how often the leader runs the safety-net scan.
	safetyNetScanInterval time.Duration
}

type tenantAutoEmbeddingDefault struct {
	config  tenantschema.TiDBAutoEmbeddingConfig
	apiKey  string
	apiBase string
}

var (
	schemaInitRetryWindow    = 10 * time.Minute
	schemaInitInitialBackoff = 2 * time.Second
	schemaInitMaxBackoff     = 30 * time.Second
	pendingTenantStaleAfter  = 10 * time.Minute
	pendingTenantSweepEvery  = time.Minute
)

// DefaultMaxUploadBytes is the server-wide fallback upload size limit.
// Keep callers on this exported constant so the default stays consistent.
const DefaultMaxUploadBytes int64 = 10 * (1 << 30) // 10 GiB

// DefaultTenantPoolMaxSize caps admin tenant pools unless overridden by server
// configuration with a positive value.
const DefaultTenantPoolMaxSize = 200

// TenantStatusResponse is the JSON body of GET /v1/status. Fields are filled
// per authenticated tenant so callers can discover their effective limits
// before initiating uploads. MaxUploadBytes is currently process-wide but the
// shape is per-tenant so future tenant-scoped quotas plug in without a
// protocol change.
type TenantStatusResponse struct {
	Status  string `json:"status"`
	Kind    string `json:"kind,omitempty"`
	Message string `json:"message,omitempty"`

	MaxUploadBytes int64 `json:"max_upload_bytes"`
	// InlineThreshold is the server's DB-inline vs S3 storage cutoff. Clients
	// use it to choose simple PUT vs V2 multipart upload so they stay
	// consistent with server-side IsLargeFile gating. Omitted (zero) by old
	// servers; clients fall back to their compiled-in default.
	InlineThreshold int64 `json:"inline_threshold,omitempty"`
}

const (
	maxBatchStatPaths                           = 256
	maxBatchReadSmallPaths                      = 128
	maxBatchReadSmallBytes                      = 50_000
	defaultBatchReadMaxBody                     = 1 << 20
	maxCredentialProvisionBodyBytes       int64 = 1 << 20
	provisionFailureClusterCleanupTimeout       = 2 * time.Minute
)

func New(b *backend.Dat9Backend) *Server {
	return NewWithConfig(Config{Backend: b})
}

func NewWithConfig(cfg Config) *Server {
	maxUpload := cfg.MaxUploadBytes
	if maxUpload <= 0 {
		maxUpload = DefaultMaxUploadBytes
	}
	logger := cfg.Logger
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	metrics.SetModuleAvailability("vault", false)
	var vaultMK *vault.MasterKey
	if len(cfg.VaultMasterKey) > 0 {
		var err error
		vaultMK, err = vault.NewMasterKey(cfg.VaultMasterKey)
		if err != nil {
			logger.Warn("vault master key invalid, vault disabled", zap.Error(err))
		} else {
			metrics.SetModuleAvailability("vault", true)
		}
	}
	inlineThreshold := cfg.InlineThreshold
	if inlineThreshold <= 0 && cfg.Backend != nil {
		inlineThreshold = cfg.Backend.InlineThreshold()
	}
	if inlineThreshold <= 0 {
		inlineThreshold = backend.DefaultInlineThreshold
	}
	tenantPoolMaxSize := cfg.TenantPoolMaxSize
	if tenantPoolMaxSize <= 0 {
		tenantPoolMaxSize = DefaultTenantPoolMaxSize
	}
	forkWorkerCtx, forkWorkerCancel := context.WithCancel(context.Background())
	s := &Server{
		fallback:                 cfg.Backend,
		meta:                     cfg.Meta,
		pool:                     cfg.Pool,
		tokenSecret:              cfg.TokenSecret,
		localTenantAPIKey:        strings.TrimSpace(cfg.LocalTenantAPIKey),
		vaultMK:                  vaultMK,
		vaultIssuerURL:           strings.TrimSpace(cfg.VaultIssuerURL),
		publicURL:                strings.TrimRight(strings.TrimSpace(cfg.PublicURL), "/"),
		provisioner:              cfg.Provisioner,
		legacyStarterProvisioner: cfg.LegacyStarterProvisioner,
		maxUploadBytes:           maxUpload,
		tenantPoolMaxSize:        tenantPoolMaxSize,
		inlineThreshold:          inlineThreshold,
		metrics:                  newServerMetrics(),
		logger:                   logger,
		events:                   newEventBuses(),
		slockOAuth:               cfg.SlockOAuth,
		tidbAutoEmbedding: tenantAutoEmbeddingDefault{
			config:  defaultTiDBAutoEmbeddingConfig(cfg.TiDBAutoEmbeddingConfig),
			apiKey:  strings.TrimSpace(cfg.TiDBAutoEmbeddingAPIKey),
			apiBase: strings.TrimSpace(cfg.TiDBAutoEmbeddingAPIBase),
		},
		disableDBAutoEmbed:    cfg.DisableDatabaseAutoEmbedding || (cfg.Pool != nil && cfg.Pool.IsAutoEmbeddingDisabled()),
		journalCursorSecret:   newJournalCursorSecret(cfg.TokenSecret),
		forkWorkerCtx:         forkWorkerCtx,
		forkWorkerCancel:      forkWorkerCancel,
		leader:                cfg.Leader,
		podNotifySecret:       cfg.PodNotifySecret,
		sseNotifyRetention:    cfg.SSENotifyRetention,
		safetyNetScanInterval: cfg.SafetyNetScanInterval,
	}
	// Default SSE notify retention.
	if s.sseNotifyRetention <= 0 {
		s.sseNotifyRetention = defaultSSENotifyRetention
	}
	if s.safetyNetScanInterval <= 0 {
		s.safetyNetScanInterval = defaultSafetyNetScanInterval
	}
	mux := http.NewServeMux()

	var business http.Handler = http.HandlerFunc(s.handleBusiness)
	if cfg.Meta != nil && cfg.Pool != nil && len(cfg.TokenSecret) > 0 {
		business = tenantAuthMiddleware(cfg.Meta, cfg.Pool, cfg.TokenSecret, business)
	} else if cfg.Backend != nil {
		business = injectFallbackBackend(cfg.Backend, business)
	}
	mux.Handle("/v1/fs:batch-stat", business)
	mux.Handle("/v1/fs:batch-read-small", business)
	mux.Handle("/v1/fs/", business)
	mux.Handle("/v1/uploads", business)
	mux.Handle("/v1/uploads/", business)
	mux.Handle("/v2/uploads/", business)
	mux.Handle("/v1/tokens", business)
	mux.Handle("/v1/tokens/", business)
	mux.Handle("/v1/tenant", business)
	mux.Handle("/v1/fork", business)
	mux.Handle("/v1/sql", business)
	mux.Handle("/v1/events", business)
	mux.Handle("/v1/journals", business)
	mux.Handle("/v1/journals/", business)
	mux.Handle("/v1/journal-entries", business)
	mux.Handle("/v1/git-workspaces", business)
	mux.Handle("/v1/git-workspaces/", business)
	mux.Handle("/v1/layers", business)
	mux.Handle("/v1/layers/", business)
	mux.Handle("/v1/layer-checkpoints/", business)
	// Vault management API goes through tenant auth.
	mux.Handle("/v1/vault/secrets", business)
	mux.Handle("/v1/vault/secrets/", business)
	mux.Handle("/v1/vault/tokens", business)
	mux.Handle("/v1/vault/tokens/", business)
	mux.Handle("/v1/vault/grants", business)
	mux.Handle("/v1/vault/grants/", business)
	mux.Handle("/v1/vault/audit", business)
	// Vault read (consumption) API: authenticated by capability token, NOT tenant token.
	if s.vaultMK != nil && cfg.Pool != nil && cfg.Meta != nil {
		mux.Handle("/v1/vault/read/", s.capabilityAuthMiddleware(cfg.Meta, cfg.Pool))
		mux.Handle("/v1/vault/read", s.capabilityAuthMiddleware(cfg.Meta, cfg.Pool))
	} else if s.vaultMK != nil && cfg.Backend != nil {
		// Single-tenant fallback: inject local scope and serve directly.
		mux.Handle("/v1/vault/read/", injectFallbackBackend(cfg.Backend, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.handleVaultRead(w, r, strings.TrimPrefix(r.URL.Path, "/v1/vault/read"))
		})))
		mux.Handle("/v1/vault/read", injectFallbackBackend(cfg.Backend, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.handleVaultRead(w, r, "")
		})))
	}
	mux.HandleFunc("/v1/status", s.handleTenantStatus)
	mux.HandleFunc("/v1/provision", s.handleProvision)
	mux.Handle("/v1/quota", s.quotaRootHandler(cfg))
	mux.Handle("/v1/admin/tenant-pool", s.adminTenantPoolHandler())
	mux.Handle("/v1/admin/tenants", s.adminTenantsRootHandler())
	mux.Handle("/v1/admin/tenants/", s.adminTenantsItemHandler())
	mux.HandleFunc("/v1/auth/slock/login", s.handleSlockLogin)
	mux.HandleFunc("/v1/auth/slock/callback", s.handleSlockCallback)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/metrics", s.handleMetrics)
	// Internal pod-to-pod SSE push endpoint. Authenticated via a shared
	// internal bearer secret (not tenant auth). Only registered when a secret
	// is configured; the handler rejects all requests if no secret is set.
	if len(cfg.PodNotifySecret) > 0 {
		mux.HandleFunc(sseNotifyInternalRoute, s.handleInternalSSENotify)
	}

	local := cfg.LocalS3
	if local == nil && cfg.Backend != nil {
		if l, ok := cfg.Backend.S3().(*s3client.LocalS3Client); ok {
			local = l
		}
	}
	if local != nil {
		mux.Handle("/s3/", http.StripPrefix("/s3", local.Handler()))
	} else if cfg.S3Dir != "" && cfg.Pool != nil && cfg.Meta != nil {
		mux.Handle("/s3/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rest := strings.TrimPrefix(r.URL.Path, "/s3/")
			tenantID, sub, ok := strings.Cut(rest, "/")
			if !ok || tenantID == "" {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			b := cfg.Pool.LoadS3Backend(r.Context(), cfg.Meta, tenantID)
			if b == nil || b.S3() == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			localS3, ok := b.S3().(*s3client.LocalS3Client)
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			setRequestMetricTenant(r.Context(), tenantID, "", "", classifyTenantRequest(r))
			subReq := r.Clone(r.Context())
			subURL := *r.URL
			subURL.Path = "/" + sub
			subURL.RawPath = ""
			subReq.URL = &subURL
			localS3.Handler().ServeHTTP(w, subReq)
		}))
	}

	s.mux = mux
	s.tenantWorker = newTenantWorkerManager(cfg.Backend, cfg.Meta, cfg.Pool, cfg.SemanticEmbedder, cfg.TenantWorkers, cfg.TenantMaintenanceInterval)
	if s.tenantWorker != nil {
		// Wire the write-path notifier so freshly enqueued semantic/file_gc/
		// quota work triggers an immediate in-process kick (~0ms latency for
		// same-pod writes) plus a best-effort outbox INSERT for cross-pod
		// delivery. The pool wires each backend's notifier to call
		// SetWorkEnqueuedNotifier which invokes the tenant worker's Kick.
		if cfg.Pool != nil {
			cfg.Pool.SetTenantWorkNotifier(func(tenantID string, workMask int) {
				s.tenantWorker.Kick(tenantID, workMask)
				s.insertTenantNotify(tenantID, workMask)
			})
		}
		if cfg.Backend != nil {
			cfg.Backend.SetWorkEnqueuedNotifier(func(workMask int) {
				s.tenantWorker.Kick(tenantLocalID, workMask)
				s.insertTenantNotify(tenantLocalID, workMask)
			})
		}
	}
	s.objectGCWorker = newObjectGCWorker(cfg.Meta, cfg.Pool)

	// Start unified tenant outbox notification infrastructure. This runs on
	// every pod (not leader-gated) because each pod needs to discover cross-pod
	// work for its own SSE subscribers and sharded tenants. Only enabled in
	// multi-tenant mode (Meta != nil).
	if s.meta != nil {
		s.startNotifyInfrastructure(cfg)
	}

	appManagedTaskTypes := tenantWorkerLogTaskTypesFromTypes(appManagedTenantTaskTypes(cfg.SemanticEmbedder))
	var fallbackTaskTypes, poolAutoTaskTypes []string
	if cfg.Backend != nil {
		fallbackTaskTypes = tenantWorkerLogTaskTypesFromTypes(cfg.Backend.AutoSemanticTaskTypes())
	}
	if cfg.Pool != nil {
		poolAutoTaskTypes = tenantWorkerLogTaskTypesFromTypes(cfg.Pool.AutoSemanticTaskTypes())
	}

	// Gate background schedulers behind the leader manager when configured. When
	// the leader is nil or disabled (single-pod mode), workers start immediately.
	if s.leader != nil {
		s.leader.SetCallbacks(s.onLead, s.onLose)
		s.leader.Start(backgroundWithTrace(context.Background()))
		// In disabled mode, Start calls onLead synchronously, which starts workers.
		// In active mode, if this pod is already leader, onLead fires from the
		// heartbeat goroutine. If not yet leader, workers stay stopped until
		// leadership is gained.
		if !s.leader.IsLeader() {
			logger.Info("server_leader_standby",
				zap.Bool("embedder_configured", cfg.SemanticEmbedder != nil),
				zap.Strings("app_managed_task_types", appManagedTaskTypes),
				zap.Strings("fallback_task_types", fallbackTaskTypes),
				zap.Strings("pool_auto_task_types", poolAutoTaskTypes))
		}
	} else {
		// No leader manager: single-pod mode, start workers immediately.
		s.startLeaderWorkers()
		s.logTenantWorkerStatus(cfg, appManagedTaskTypes, fallbackTaskTypes, poolAutoTaskTypes)
	}
	return s
}

// insertTenantNotify writes a best-effort unified outbox row so other pods
// discover the work via the 200ms poller. Called from the write path after
// the in-process kick. Failures are logged and are safe: the safety-net scan
// recovers any missed work. Uses a non-cancelable background context so a
// client disconnect after the commit doesn't abort the outbox pointer.
func (s *Server) insertTenantNotify(tenantID string, workMask int) {
	if s.meta == nil || tenantID == "" || workMask == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.meta.InsertTenantNotify(ctx, tenantID, workMask); err != nil {
		logger.Warn(ctx, "tenant_notify_outbox_insert_failed",
			zap.String("tenant_id", tenantID),
			zap.Int("work_mask", workMask),
			zap.Error(err))
	}
}

// startNotifyInfrastructure launches the unified tenant outbox components
// that run on every pod (not leader-gated):
//   - shardResolver: refreshes the active pod ring for jump-consistent-hash
//     shard ownership of semantic/file_gc/quota work.
//   - tenantOutboxPoller: reads the central tenant_notify_outbox table at
//     200ms intervals and dispatches by work_mask (SSE → wake local bus;
//     semantic/file_gc/quota → kick tenantWorker on shard owner).
//   - podRegistry: maintains this pod's presence in pod_registry and reports
//     its SSE subscriber tenant set to pod_subscriptions.
//
// Only called in multi-tenant mode (Meta != nil). In single-tenant mode the
// fallback EventBus + tenantWorker workerLoop ticker handle delivery.
func (s *Server) startNotifyInfrastructure(cfg Config) {
	// Create a single context for all notify components. We store only the
	// cancel func (not the context itself) on Server, per coding guidelines.
	notifyCtx, notifyCancel := context.WithCancel(backgroundWithTrace(context.Background()))
	s.notifyCancel = notifyCancel

	// Shard resolver: refresh the active pod ring synchronously before the
	// poller starts so ownsTenant has a valid ring on startup.
	resolver := newSemanticShardResolver(s.meta, cfg.PodID, cfg.TenantShardRefreshInterval)
	resolver.Start(context.Background())
	s.shardResolver = resolver

	// Unified outbox poller: reads the central outbox on every pod and
	// dispatches by work_mask.
	poller := newTenantOutboxPoller(
		s.meta, s.events, s.tenantWorker, resolver.ownsTenantFn(),
		cfg.PodID, cfg.TenantOutboxPollInterval, cfg.TenantOutboxCursorFlushInterval,
	)
	// Initialize the poller cursor synchronously BEFORE starting the goroutine
	// and BEFORE the server accepts live traffic. On restart the cursor is
	// recovered from tenant_outbox_cursor; on first launch it skips to MAX(id).
	poller.initCursor(context.Background())
	s.notifyWG.Add(1)
	go func() {
		defer s.notifyWG.Done()
		poller.run(notifyCtx)
	}()

	// Pod registry: heartbeat + subscription reporting. Created when PodID is
	// set; PodAddr may be empty initially (e.g. when the listen address is not
	// known yet) — the heartbeat is a no-op in that case, but subscription
	// reporting and the leader's stale-pod sweep still function.
	if cfg.PodID != "" {
		reg := newPodRegistry(s.meta, cfg.PodID, cfg.PodAddr, s.events)
		s.podRegistry = reg
		// Synchronous initial registration so this pod is visible in
		// ListActivePods immediately (not after the first heartbeat tick).
		_ = reg.RegisterBeforeStart(context.Background())
		// Start the registry's goroutines. They are tracked by the registry's
		// own WaitGroup and stopped via reg.Stop() in stopNotifyInfrastructure.
		reg.Start(notifyCtx)
	}

	// Start the unified tenant worker on every pod (not leader-gated). Each
	// pod only processes tenants assigned to its shard via jump consistent
	// hashing. This must run on every pod because the shard resolver may
	// assign tenants to non-leader pods — if the worker only ran on the
	// leader, ~(N-1)/N of sharded work (semantic, file_gc) would be silently
	// dropped. In single-tenant mode (no meta/pool) the worker polls the
	// fallback backend directly.
	if s.tenantWorker != nil {
		s.tenantWorker.Start(notifyCtx)
	}

	// Safety-net scan runs on every pod (not leader-gated) so each pod
	// recovers expired leases for its own shard's warm tenants. This
	// complements the per-pod tenant worker — if a kick is lost, the
	// safety-net catches it within 5min for any warm tenant owned by this pod.
	if s.meta != nil && s.pool != nil {
		s.notifyWG.Add(1)
		go func() {
			defer s.notifyWG.Done()
			ticker := time.NewTicker(s.safetyNetScanInterval)
			defer ticker.Stop()
			for {
				select {
				case <-notifyCtx.Done():
					return
				case <-ticker.C:
					s.safetyNetScan(notifyCtx)
				}
			}
		}()
	}
}

// stopNotifyInfrastructure stops the shard resolver, notify poller, and pod
// registry. Called from Close AFTER stopLeaderWorkers so the leader-gated
// stale-pod sweep (which dereferences s.podRegistry) can't race with clearing
// podRegistry. The poller is stopped via notifyCancel; the resolver and
// registry have their own Stop methods that wait for goroutines to exit.
func (s *Server) stopNotifyInfrastructure() {
	if s.notifyCancel != nil {
		s.notifyCancel()
	}
	s.notifyWG.Wait()
	if s.shardResolver != nil {
		s.shardResolver.Stop()
		s.shardResolver = nil
	}
	if s.podRegistry != nil {
		s.podRegistry.Stop()
		s.podRegistry = nil
	}
	// Stop the tenant worker (started in startNotifyInfrastructure on every
	// pod, not leader-gated). Stop after notifyCancel so the worker's context
	// is already cancelled, then wait for goroutines to exit.
	if s.tenantWorker != nil {
		s.tenantWorker.Stop()
	}
	s.notifyCancel = nil
}

func (s *Server) Close() {
	s.forkWorkerMu.Lock()
	if !s.forkWorkerClosed {
		s.forkWorkerClosed = true
		if s.forkWorkerCancel != nil {
			s.forkWorkerCancel()
		}
	}
	s.forkWorkerMu.Unlock()
	s.forkWorkerWG.Wait()
	// Stop the leader manager first so any in-flight onLead/onLose callbacks
	// finish before we tear down local workers. leader.Stop() waits for the
	// heartbeat goroutine to exit, so no further callback can fire after it
	// returns. If this pod currently holds leadership, Stop() invokes onLose,
	// which runs stopLeaderWorkers and stops the leader-gated workers; the
	// stopLeaderWorkers call below is then a no-op. If this pod is a standby,
	// onLose does not fire, and stopLeaderWorkers is a no-op (workers were
	// never started). This ordering prevents the race where Close() stops
	// workers while onLead is still starting them (the callbacks and the local
	// teardown are serialized on leaderWorkerMu, and no callback can outlive
	// leader.Stop()).
	if s.leader != nil {
		s.leader.Stop()
	}
	// In single-pod (no-leader) mode, leader-gated workers were started
	// unconditionally in NewWithConfig; stop them now. In leader mode this is a
	// no-op (already stopped via onLose above) guarded by leaderWorkersStarted.
	// stopLeaderWorkers also stops the semantic and object GC workers in both
	// modes (they are started by startLeaderWorkers), so no separate Stop calls
	// are needed here.
	s.stopLeaderWorkers()
	// Stop SSE cross-pod notification infrastructure AFTER leader-gated workers
	// are stopped, so the stale-pod sweep (which dereferences s.podRegistry)
	// cannot race with clearing podRegistry during shutdown.
	s.stopNotifyInfrastructure()
}

// startLeaderWorkers launches the background schedulers that should run only
// on the leader pod: the fork-worker group (pending tenant reconciler, tenant
// delete cleanup, one-time resume tasks), the semantic and object GC workers,
// and the central-quota mutation replay + expiry sweep workers. The whole start
// (including the worker assignments and Start calls) is serialized under
// leaderWorkerMu and guarded by leaderWorkersStarted, so onLead racing with
// Close()/onLose cannot interleave a start with a stop and leave orphan workers
// running. The mutex is held for the entire transition so that a concurrent
// stopLeaderWorkers observes the fully-started worker set (not a partial one).
func (s *Server) startLeaderWorkers() {
	s.leaderWorkerMu.Lock()
	defer s.leaderWorkerMu.Unlock()
	if s.leaderWorkersStarted {
		return
	}
	s.leaderWorkersStarted = true
	// Create a fresh leaderWorkerCtx for the fork-worker group and one-time
	// resume tasks. These use leaderWorkerCtx (not forkWorkerCtx, which is
	// reserved for API-triggered fork operations that must run on any pod).
	leaderCtx, cancel := context.WithCancel(context.Background())
	s.leaderWorkerCtx = leaderCtx
	s.leaderWorkerCancel = cancel

	if s.meta != nil && s.pool != nil && s.provisioner != nil {
		// One-time resume tasks (run in leader-gated goroutines).
		s.startLeaderGoroutine(leaderCtx, func(workerCtx context.Context) {
			s.resumePendingTenantsWithCtx(workerCtx)
		})
		s.startLeaderGoroutine(leaderCtx, func(workerCtx context.Context) {
			s.resumeProvisioningTenantsWithCtx(workerCtx)
		})
		s.startLeaderGoroutine(leaderCtx, func(workerCtx context.Context) {
			s.resumeDeletingForkTenantsWithCtx(workerCtx)
		})
		// Periodic tenant delete cleanup.
		s.startLeaderGoroutine(leaderCtx, func(workerCtx context.Context) {
			ticker := time.NewTicker(defaultTenantDeletePollInterval)
			defer ticker.Stop()
			s.processTenantDeleteJobs(workerCtx)
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					s.processTenantDeleteJobs(workerCtx)
				}
			}
		})
		// Periodic pending tenant reconciler.
		if pendingTenantSweepEvery > 0 {
			s.startLeaderGoroutine(leaderCtx, func(workerCtx context.Context) {
				ticker := time.NewTicker(pendingTenantSweepEvery)
				defer ticker.Stop()
				for {
					select {
					case <-workerCtx.Done():
						return
					case <-ticker.C:
						s.resumePendingTenantsWithCtx(workerCtx)
					}
				}
			})
		}
	}
	// Central-quota workers: the server owns these as the single leader-callback
	// owner so they start/stop with leadership transitions (previously they were
	// wired in main.go via a second SetCallbacks call that was clobbered by the
	// server's own SetCallbacks).
	if s.meta != nil {
		s.replayWorker = backend.StartMutationReplayWorker(tenant.NewMetaQuotaAdapter(s.meta))
		s.expirySweepWorker = backend.StartExpirySweepWorker(s.meta)
	}
	// Per-tenant FileGC and quota outbox workers are no longer per-backend
	// goroutines — they are driven by kicks through the unified tenant worker.
	// In multi-tenant mode the tenantWorker is started in
	// startNotifyInfrastructure (every pod, not leader-gated). In single-tenant
	// mode (s.meta == nil) startNotifyInfrastructure is never called, so start
	// the worker here instead.
	if s.tenantWorker != nil && s.meta == nil {
		s.tenantWorker.Start(backgroundWithTrace(leaderCtx))
	}
	if s.objectGCWorker != nil {
		s.objectGCWorker.Start(backgroundWithTrace(leaderCtx))
	}
	// Safety-net scan and outbox cleanup are started in
	// startNotifyInfrastructure (every pod) and startLeaderWorkers
	// (leader-only outbox pruning) respectively. See those functions.
	// Periodic tenant notify outbox cleanup (leader-only). Prunes old
	// tenant_notify_outbox rows so the central table doesn't grow unbounded.
	// Only runs in multi-tenant mode (Meta != nil).
	if s.meta != nil {
		s.startLeaderGoroutine(leaderCtx, func(workerCtx context.Context) {
			ticker := time.NewTicker(tenantNotifyCleanupInterval)
			defer ticker.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					s.cleanupTenantNotifyOutbox(workerCtx)
				}
			}
		})
	}
	// Periodic stale pod sweep (leader-only). Marks pods with expired heartbeats
	// as stale and cleans up their subscription rows so the shard resolver stops
	// routing work to dead pods. Only runs when pod registry is configured.
	if s.podRegistry != nil {
		s.startLeaderGoroutine(leaderCtx, func(workerCtx context.Context) {
			ticker := time.NewTicker(stalePodSweepInterval)
			defer ticker.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					s.podRegistry.SweepStalePods(workerCtx)
				}
			}
		})
	}
}

// fsEventsRetention is how long event rows are kept before pruning. Used by
// the piggybacked maintenance in the tenant worker.
const fsEventsRetention = 1 * time.Hour

// defaultSSENotifyRetention is how long tenant_notify_outbox rows are kept
// before the leader prunes them. Matches fs_events retention so the outbox
// doesn't outlive the work it points to.
const defaultSSENotifyRetention = 1 * time.Hour

// defaultSafetyNetScanInterval is how often the leader runs the safety-net scan.
const defaultSafetyNetScanInterval = 5 * time.Minute

// tenantNotifyCleanupInterval is how often the leader prunes old outbox rows.
const tenantNotifyCleanupInterval = 5 * time.Minute

// cleanupTenantNotifyOutbox prunes old tenant_notify_outbox rows from the
// central meta DB. Leader-gated; runs on the tenantNotifyCleanupInterval
// ticker. The outbox is a lightweight pointer table (tenant_id + work_mask),
// not work content — pruning old rows doesn't lose work because the safety-net
// scan recovers any expired leases.
func (s *Server) cleanupTenantNotifyOutbox(ctx context.Context) {
	if ctx.Err() != nil {
		logger.Warn(ctx, "tenant_notify_cleanup_skipped_shutdown", zap.Error(ctx.Err()))
		return
	}
	if s.meta == nil {
		return
	}
	n, err := s.meta.DeleteTenantNotifyBefore(ctx, time.Now().Add(-s.sseNotifyRetention))
	if err != nil {
		if ctx.Err() != nil {
			logger.Warn(ctx, "tenant_notify_cleanup_interrupted_by_shutdown", zap.Error(err))
		} else {
			logger.Warn(ctx, "tenant_notify_cleanup_failed", zap.Error(err))
		}
		return
	}
	if n > 0 {
		logger.Info(ctx, "tenant_notify_outbox_pruned",
			zap.Int64("rows", n),
			zap.Duration("retention", s.sseNotifyRetention))
	}
}

// stopLeaderWorkers stops the background schedulers started by startLeaderWorkers.
// Called when the pod loses leadership or the server shuts down. The whole stop
// (including the worker Stop/wait calls and clearing the worker fields) is
// serialized under leaderWorkerMu and guarded by leaderWorkersStarted, so
// onLose (heartbeat goroutine), Close() (main goroutine), and a concurrent
// startLeaderWorkers cannot interleave. Holding the mutex for the entire
// transition guarantees a concurrent startLeaderWorkers observes the
// fully-stopped state (leaderWorkersStarted == false) and does not leave
// orphan workers that a stop already ran past.
func (s *Server) stopLeaderWorkers() {
	s.leaderWorkerMu.Lock()
	defer s.leaderWorkerMu.Unlock()
	if !s.leaderWorkersStarted {
		return
	}
	s.leaderWorkersStarted = false
	cancel := s.leaderWorkerCancel
	s.leaderWorkerCancel = nil
	replayWorker := s.replayWorker
	s.replayWorker = nil
	expirySweepWorker := s.expirySweepWorker
	s.expirySweepWorker = nil

	if cancel != nil {
		cancel()
	}
	s.leaderWorkerWG.Wait()
	if replayWorker != nil {
		replayWorker.Stop()
	}
	if expirySweepWorker != nil {
		expirySweepWorker.Stop()
	}
	// In multi-tenant mode the tenant worker is started/stopped in
	// startNotifyInfrastructure. In single-tenant mode (s.meta == nil) it is
	// started here in startLeaderWorkers and must be stopped here too.
	if s.tenantWorker != nil && s.meta == nil {
		s.tenantWorker.Stop()
	}
	if s.objectGCWorker != nil {
		s.objectGCWorker.Stop()
	}
}

// startLeaderGoroutine launches a goroutine that runs fn under the leader
// worker context. The goroutine is tracked by leaderWorkerWG and stops when
// leadership is lost (leaderWorkerCtx is cancelled).
func (s *Server) startLeaderGoroutine(ctx context.Context, fn func(context.Context)) {
	s.leaderWorkerWG.Add(1)
	go func() {
		defer s.leaderWorkerWG.Done()
		fn(ctx)
	}()
}

// resumePendingTenantsWithCtx lists pending tenants and reconciles stale ones.
func (s *Server) resumePendingTenantsWithCtx(ctx context.Context) {
	tenants, err := s.meta.ListTenantsByStatus(ctx, meta.TenantPending, 1000)
	if err != nil {
		logger.Error(ctx, "resume_pending_list_failed", zap.Error(err))
		return
	}
	for i := range tenants {
		s.reconcilePendingTenant(ctx, tenants[i])
	}
}

// resumeProvisioningTenantsWithCtx resumes provisioning tenants that were
// interrupted by a previous restart.
func (s *Server) resumeProvisioningTenantsWithCtx(ctx context.Context) {
	tenants, err := s.meta.ListTenantsByStatus(ctx, meta.TenantProvisioning, 1000)
	if err != nil {
		logger.Error(ctx, "resume_provisioning_list_failed", zap.Error(err))
		return
	}
	for i := range tenants {
		t := tenants[i]
		if t.Kind == meta.TenantKindFork {
			logger.Info(ctx, "resume_provisioning_fork",
				zap.String("tenant_id", t.ID),
				zap.String("parent_tenant_id", t.ParentTenantID))
			if t.Provider == tenant.ProviderTiDBCloudNative && t.DBUser == "" {
				logger.Error(ctx, "resume_provisioning_fork_no_credentials",
					zap.String("tenant_id", t.ID),
					zap.String("provider", t.Provider),
					zap.String("cluster_id", t.ClusterID),
					zap.String("branch_id", t.BranchID))
				s.markForkFailed(ctx, t.ID)
				continue
			}
			s.startForkProvision(ctx, t.ID)
			continue
		}
		if t.Provider == tenant.ProviderTiDBCloudNative && t.DBUser == "" {
			// TiDB Cloud native metadata resume is gated by request-scoped customer credentials.
			logger.Warn(ctx, "resume_provisioning_native_no_connection",
				zap.String("tenant_id", t.ID),
				zap.String("provider", t.Provider),
				zap.String("cluster_id", t.ClusterID),
				zap.String("reason", "tidbcloud_credentials_unavailable"))
			if s.metrics != nil {
				s.metrics.recordEvent(t.ID, "tenant_pool_pending_resume",
					"provider", t.Provider,
					"result", "skipped",
					"reason", "tidbcloud_credentials_unavailable")
			}
			continue
		}
		s.startTenantSchemaInitResume(ctx, t)
	}
}

// resumeDeletingForkTenantsWithCtx resumes cleanup for deleting/failed fork tenants.
func (s *Server) resumeDeletingForkTenantsWithCtx(ctx context.Context) {
	for _, status := range []meta.TenantStatus{meta.TenantDeleting, meta.TenantFailed} {
		tenants, err := s.meta.ListTenantsByStatus(ctx, status, 1000)
		if err != nil {
			logger.Error(ctx, "resume_fork_cleanup_list_failed", zap.String("status", string(status)), zap.Error(err))
			continue
		}
		s.resumeForkCleanup(ctx, tenants)
	}
}

// onLead is the leader manager callback invoked when this pod gains leadership.
func (s *Server) onLead() {
	s.startLeaderWorkers()
}

// onLose is the leader manager callback invoked when this pod loses leadership.
func (s *Server) onLose() {
	s.stopLeaderWorkers()
}

// logTenantWorkerStatus logs whether the tenant worker is enabled or disabled.
// Called in single-pod mode (no leader manager) for backward-compatible logging.
// Log-only: the workers are started by startLeaderWorkers (called just before
// this in the no-leader path), so this must NOT call Start again.
func (s *Server) logTenantWorkerStatus(cfg Config, appManagedTaskTypes, fallbackTaskTypes, poolAutoTaskTypes []string) {
	ctx := backgroundWithTrace(context.Background())
	if s.tenantWorker != nil {
		fields := []zap.Field{
			zap.Int("workers", s.tenantWorker.opts.Workers),
			zap.Duration("poll_interval", s.tenantWorker.opts.PollInterval),
			zap.Duration("lease_duration", s.tenantWorker.opts.LeaseDuration),
			zap.Duration("maintenance_interval", s.tenantWorker.opts.MaintenanceInterval),
			zap.String("recovery_mode", "on_kick"),
			zap.Bool("embedder_configured", cfg.SemanticEmbedder != nil),
			zap.Strings("app_managed_task_types", appManagedTaskTypes),
			zap.Strings("fallback_task_types", fallbackTaskTypes),
			zap.Strings("pool_auto_task_types", poolAutoTaskTypes),
			zap.Bool("fallback_image_extract_enabled", cfg.Backend != nil && cfg.Backend.SupportsAsyncImageExtract()),
			zap.Bool("pool_image_extract_enabled", cfg.Pool != nil && cfg.Pool.SupportsAsyncImageExtract()),
		}
		logger.Info(ctx, "server_tenant_worker_enabled", fields...)
	} else {
		logger.Info(ctx, "server_tenant_worker_disabled",
			zap.Bool("embedder_configured", cfg.SemanticEmbedder != nil),
			zap.Strings("app_managed_task_types", appManagedTaskTypes),
			zap.Strings("fallback_task_types", fallbackTaskTypes),
			zap.Strings("pool_auto_task_types", poolAutoTaskTypes),
			zap.Bool("fallback_present", cfg.Backend != nil),
			zap.Bool("fallback_image_extract_enabled", cfg.Backend != nil && cfg.Backend.SupportsAsyncImageExtract()),
			zap.Bool("pool_present", cfg.Pool != nil),
			zap.Bool("pool_image_extract_enabled", cfg.Pool != nil && cfg.Pool.SupportsAsyncImageExtract()))
	}
	if s.objectGCWorker != nil {
		logger.Info(ctx, "server_object_gc_worker_enabled")
	} else {
		logger.Info(ctx, "server_object_gc_worker_disabled")
	}
}

func (s *Server) startTenantSchemaInitResume(ctx context.Context, t meta.Tenant) {
	s.startServerWorker(ctx, func(workerCtx context.Context) {
		schemaProvisioner := s.provisionerForTenantProvider(t.Provider)
		if schemaProvisioner == nil {
			logger.Warn(workerCtx, "resume_schema_init_skipped",
				zap.String("tenant_id", t.ID),
				zap.String("provider", t.Provider),
				zap.String("reason", "provisioner_not_configured"))
			return
		}
		plain, err := s.pool.Decrypt(workerCtx, t.DBPasswordCipher)
		if err != nil {
			logger.Warn(workerCtx, "resume_schema_init_skipped", zap.String("tenant_id", t.ID), zap.Error(err))
			return
		}
		dsn := tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS, t.Provider)
		s.initTenantSchemaAsync(workerCtx, t.ID, dsn, t.Provider, s.schemaInitForTenant(t.ID, t.Provider, schemaProvisioner.InitSchema))
	})
}

func (s *Server) reconcilePendingTenant(ctx context.Context, t meta.Tenant) {
	if t.Provider == tenant.ProviderTiDBCloudNative && strings.TrimSpace(t.ClusterID) != "" && strings.TrimSpace(t.DBUser) == "" {
		logger.Info(ctx, "resume_pending_pool_tenant_skipped",
			zap.String("tenant_id", t.ID),
			zap.String("provider", t.Provider),
			zap.String("cluster_id", t.ClusterID),
			zap.String("reason", "tidbcloud_credentials_unavailable"))
		if s.metrics != nil {
			s.metrics.recordEvent(t.ID, "tenant_pool_pending_resume",
				"provider", t.Provider,
				"result", "skipped",
				"reason", "tidbcloud_credentials_unavailable")
		}
		return
	}
	if !isStalePendingTenant(time.Now().UTC(), t) {
		return
	}
	if pendingTenantConnectionReady(t) {
		updated, err := s.meta.UpdateTenantStatusIf(ctx, t.ID, meta.TenantPending, meta.TenantProvisioning)
		if err != nil {
			logger.Error(ctx, "resume_pending_schema_init_status_update_error",
				zap.String("tenant_id", t.ID),
				zap.Error(err))
			return
		}
		if !updated {
			logger.Info(ctx, "resume_pending_schema_init_skipped",
				zap.String("tenant_id", t.ID),
				zap.String("reason", "status_changed"))
			return
		}
		t.Status = meta.TenantProvisioning
		logger.Info(ctx, "resume_pending_schema_init_started",
			zap.String("tenant_id", t.ID),
			zap.String("provider", t.Provider),
			zap.String("cluster_id", t.ClusterID))
		s.startTenantSchemaInitResume(ctx, t)
		return
	}
	logger.Warn(ctx, "resume_pending_mark_failed",
		zap.String("tenant_id", t.ID),
		zap.String("kind", string(t.Kind)),
		zap.String("provider", t.Provider),
		zap.Time("updated_at", t.UpdatedAt))
	updated, err := s.meta.UpdateTenantStatusIf(ctx, t.ID, meta.TenantPending, meta.TenantFailed)
	if err != nil {
		logger.Error(ctx, "resume_pending_mark_failed_update_error",
			zap.String("tenant_id", t.ID),
			zap.Error(err))
		return
	}
	if !updated {
		logger.Info(ctx, "resume_pending_mark_failed_skipped",
			zap.String("tenant_id", t.ID),
			zap.String("reason", "status_changed"))
	}
}

func pendingTenantConnectionReady(t meta.Tenant) bool {
	return strings.TrimSpace(t.DBHost) != "" &&
		t.DBPort > 0 &&
		strings.TrimSpace(t.DBUser) != "" &&
		len(t.DBPasswordCipher) > 0 &&
		strings.TrimSpace(t.DBName) != ""
}

func isStalePendingTenant(now time.Time, t meta.Tenant) bool {
	if pendingTenantStaleAfter <= 0 {
		return true
	}
	lastTouched := t.UpdatedAt
	if lastTouched.IsZero() || t.CreatedAt.After(lastTouched) {
		lastTouched = t.CreatedAt
	}
	return !lastTouched.IsZero() && now.Sub(lastTouched) >= pendingTenantStaleAfter
}

// backgroundWithTrace creates a background context that inherits the trace ID
// from ctx. Note: pkg/backend has a same-named function with a different
// signature (no args, returns traceid.Background()). This server version
// derives the trace from a caller-supplied context. Both are package-scoped
// so there is no collision, but the naming overlap is intentional — each
// package uses the variant appropriate to its trace ID source.
func backgroundWithTrace(ctx context.Context) context.Context {
	return contextWithTrace(context.Background(), ctx)
}

func (s *Server) startServerWorker(ctx context.Context, fn func(context.Context)) {
	workerCtx := backgroundWithTrace(ctx)
	if s.forkWorkerCtx != nil {
		workerCtx = contextWithTrace(s.forkWorkerCtx, ctx)
	}

	s.forkWorkerMu.Lock()
	if s.forkWorkerClosed {
		s.forkWorkerMu.Unlock()
		logger.Warn(workerCtx, "server_worker_start_after_close")
		return
	}
	s.forkWorkerWG.Add(1)
	s.forkWorkerMu.Unlock()

	go func() {
		defer s.forkWorkerWG.Done()
		fn(workerCtx)
	}()
}

func ensureTrace(ctx context.Context) context.Context {
	if traceid.FromContext(ctx) != "" {
		return ctx
	}
	return traceid.With(ctx, traceid.Generate())
}

func contextWithTrace(parent, traceSource context.Context) context.Context {
	traceID := traceid.FromContext(traceSource)
	if traceID == "" {
		traceID = traceid.Generate()
	}
	return traceid.With(parent, traceID)
}

func tenantDSN(user, password, host string, port int, dbName string, tlsEnabled bool, provider string) string {
	query := "parseTime=true"
	if tlsEnabled {
		query += "&tls=true"
	} else if provider == tenant.ProviderTiDBCloudNative {
		query += "&tls=skip-verify"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", user, password, host, port, dbName, query)
}

func injectFallbackBackend(b *backend.Dat9Backend, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := &TenantScope{
			TenantID:           "local",
			APIKeyID:           "local",
			TokenVersion:       1,
			Backend:            b,
			JournalPermissions: ownerJournalPermissions(),
		}
		setRequestMetricScope(r.Context(), scope, classifyTenantRequest(r))
		next.ServeHTTP(w, r.WithContext(withScope(r.Context(), scope)))
	})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.observe(s.mux, w, r)
}

func (s *Server) ListenAndServe(addr string) error {
	logger.Info(backgroundWithTrace(context.Background()), "server_start", zap.String("addr", addr), zap.Int64("max_upload_bytes", s.maxUploadBytes))
	return http.ListenAndServe(addr, s)
}

func (s *Server) handleBusiness(w http.ResponseWriter, r *http.Request) {
	if scope := ScopeFromContext(r.Context()); scope != nil && scope.IsScoped && !isScopedBusinessRequestAllowed(r) {
		errJSON(w, http.StatusForbidden, "scoped token cannot access endpoint")
		return
	}
	switch {
	case r.URL.Path == "/v1/fs:batch-stat":
		s.handleBatchStat(w, r)
	case r.URL.Path == "/v1/fs:batch-read-small":
		s.handleBatchReadSmall(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/fs/"):
		s.handleFS(w, r)
	case r.URL.Path == "/v1/uploads/initiate":
		s.handleUploads(w, r)
	case r.URL.Path == "/v1/uploads":
		s.handleUploads(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/uploads/"):
		s.handleUploadAction(w, r)
	case strings.HasPrefix(r.URL.Path, "/v2/uploads/"):
		s.handleV2Uploads(w, r)
	case r.URL.Path == "/v1/tokens" || strings.HasPrefix(r.URL.Path, "/v1/tokens/"):
		s.handleTokens(w, r)
	case r.URL.Path == "/v1/tenant":
		s.handleTenantDelete(w, r)
	case r.URL.Path == "/v1/fork":
		s.handleFork(w, r)
	case r.URL.Path == "/v1/sql":
		s.handleSQL(w, r)
	case r.URL.Path == sseEventsRoute:
		s.handleEvents(w, r)
	case r.URL.Path == "/v1/journals" || strings.HasPrefix(r.URL.Path, "/v1/journals/") || r.URL.Path == "/v1/journal-entries":
		s.handleJournal(w, r)
	case r.URL.Path == "/v1/git-workspaces" || strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/"):
		s.handleGitWorkspaces(w, r)
	case r.URL.Path == "/v1/layers" || strings.HasPrefix(r.URL.Path, "/v1/layers/") || strings.HasPrefix(r.URL.Path, "/v1/layer-checkpoints/"):
		s.handleFSLayers(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/vault/secrets"), strings.HasPrefix(r.URL.Path, "/v1/vault/tokens"), strings.HasPrefix(r.URL.Path, "/v1/vault/grants"), strings.HasPrefix(r.URL.Path, "/v1/vault/audit"):
		s.handleVault(w, r)
	default:
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "business_route_not_found", "path", r.URL.Path, "method", r.Method)...)
		errJSON(w, http.StatusNotFound, "not found")
	}
}

// isScopedBusinessRequestAllowed is the dispatcher-level gate for scoped
// tokens. It mirrors the actual downstream dispatch tables (handleFS,
// handleUploads / handleUploadAction, handleV2Uploads) so the gate stays
// in sync with what each handler wires — release-order safety per
// @adversary-1 / @dev-1 reviews (msgs b6f53023, 4619c945, 005e8b0b for
// the C1 read-side; 6e17765f for the POST action priority on C2a write-
// side; 09266f14 for the action-aware upload allowlist on C2b).
//
// Workspace zones coverage as of C2b:
//   - GET/HEAD /v1/fs/* (read-side, action-arm allowlist)
//   - POST /v1/fs:batch-stat / batch-read-small (per-path authorize)
//   - PUT/PATCH/DELETE/POST /v1/fs/* (write-side, action-arm allowlist;
//     chmod stays owner-only)
//   - /v1/uploads* + /v2/uploads/* (action-aware, mirrors actual upload
//     dispatch table; see isScopedV{1,2}UploadRouteAllowed)
//   - /v1/layers* + /v1/layer-checkpoints/* (route-aware, with per-layer
//     base root and per-entry path authorization in fs_layer.go)
//
// chmod (POST /v1/fs/<path>?chmod=1) is explicitly NOT and never will be in
// the scoped allowlist — chmod escalates ACLs and is owner-token-only.
//
// SQL, fork, events, journals, vault are permanently out of scope for
// workspace zones (they don't take a path argument, so the prefix model
// doesn't apply); these stay default-deny here.
//
// The GET branch uses an **action-specific** accept-list (per @adversary-1
// msg 00efe734 / @adversary-2 msg cbedd30a): the chosen action selector
// determines which filter keys the handler actually consumes, and only
// those keys plus the selector itself are admitted.
//
// The POST branch also uses an action-specific accept-list (per @adversary-1
// msg 6e17765f): mixed selectors like `?append=1&copy=1` deny as ambiguous
// rather than silently first-wins. Each action arm allows only the keys
// the corresponding handler reads.
//
// Both upload prefixes route through isScopedV1UploadRouteAllowed /
// isScopedV2UploadRouteAllowed, which enumerate (method, path, action)
// tuples exactly matching the corresponding handler dispatch (no
// prefix-family pass-through — see @adversary-1 msg 09266f14).
func isScopedBusinessRequestAllowed(r *http.Request) bool {
	path := r.URL.Path

	// Batch FS endpoints (always POST). Handlers do per-path AuthorizeFS internally.
	if path == "/v1/fs:batch-stat" || path == "/v1/fs:batch-read-small" {
		return r.Method == http.MethodPost
	}

	// /v1/fs/* — read + write methods admitted (C1 + C2a). Uploads are
	// handled separately below via isScopedV{1,2}UploadRouteAllowed.
	if strings.HasPrefix(path, "/v1/fs/") {
		switch r.Method {
		case http.MethodHead:
			// handleStat — read.
			return true
		case http.MethodGet:
			return isScopedFSGetQueryAllowed(r.URL.Query())
		case http.MethodPut:
			// handleWrite — write. No query params consumed.
			return len(r.URL.Query()) == 0
		case http.MethodPatch:
			// handlePatch — write. No query params consumed.
			return len(r.URL.Query()) == 0
		case http.MethodDelete:
			// handleDelete — delete. Consumes ?recursive and ?kind.
			return queryKeysSubsetOf(r.URL.Query(), []string{"recursive", "kind"})
		case http.MethodPost:
			return isScopedFSPostQueryAllowed(r.URL.Query())
		default:
			return false
		}
	}

	// Uploads (V1 + V2): admitted in PR C2b now that every upload handler
	// authorizes its target path on initiate AND re-authorizes session
	// target path on continuation. Per @adversary-1 review msg 09266f14:
	// the whitelist must be **action-aware**, mirroring the actual
	// downstream dispatch table (handleUploads / handleUploadAction /
	// handleV2Uploads) — release-order safety, same shape as the C1 GET
	// allowlist. Method+path mismatches and unknown actions must be
	// denied here so future routes don't silently inherit the family
	// prefix.
	if strings.HasPrefix(path, "/v1/uploads") {
		return isScopedV1UploadRouteAllowed(r.Method, path)
	}
	if strings.HasPrefix(path, "/v2/uploads/") {
		return isScopedV2UploadRouteAllowed(r.Method, path)
	}

	if path == "/v1/layers" || strings.HasPrefix(path, "/v1/layers/") || strings.HasPrefix(path, "/v1/layer-checkpoints/") {
		return isScopedFSLayerRouteAllowed(r.Method, path, r.URL.Query())
	}

	// SQL, fork, events, journals, vault, status, etc.: still default-deny.
	return false
}

func isScopedFSLayerRouteAllowed(method, path string, query url.Values) bool {
	if path == "/v1/layers" {
		return (method == http.MethodGet || method == http.MethodPost) && len(query) == 0
	}
	if strings.HasPrefix(path, "/v1/layer-checkpoints/") {
		return method == http.MethodGet && len(query) == 0
	}
	if !strings.HasPrefix(path, "/v1/layers/") {
		return false
	}
	rest := strings.TrimPrefix(path, "/v1/layers/")
	if rest == "" {
		return false
	}
	parts := strings.Split(rest, "/")
	switch len(parts) {
	case 1:
		return method == http.MethodGet && len(query) == 0
	case 2:
		switch parts[1] {
		case "diff":
			return method == http.MethodGet && queryKeysSubsetOf(query, []string{"max_seq", "replay", "mode"})
		case "checkpoints", "rollback", "commit":
			return method == http.MethodPost && len(query) == 0
		case "entries":
			if method == http.MethodGet {
				return queryKeysSubsetOf(query, []string{"path", "max_seq"})
			}
			return (method == http.MethodPost || method == http.MethodPut) && len(query) == 0
		case "objects":
			if method == http.MethodGet {
				return queryKeysSubsetOf(query, []string{"path", "max_seq"})
			}
			return (method == http.MethodPost || method == http.MethodPut) && queryKeysSubsetOf(query, []string{"path", "size", "base_revision", "mode"})
		case "events":
			return method == http.MethodGet && queryKeysSubsetOf(query, []string{"since"})
		default:
			return false
		}
	default:
		return false
	}
}

// isScopedV1UploadRouteAllowed mirrors handleUploads (server.go ~1872) and
// handleUploadAction (server.go ~2034) exactly: each row below corresponds
// to one wired handler with its own re-authorize-session-target-path call.
// Any method/path/action combination not in this list is denied.
func isScopedV1UploadRouteAllowed(method, path string) bool {
	// Family root and explicit initiate endpoint.
	if path == "/v1/uploads" {
		// POST → handleUploadInitiate (body.path authorized)
		// GET  → handleUploads list (path query arg authorized as list)
		return method == http.MethodPost || method == http.MethodGet
	}
	if path == "/v1/uploads/initiate" {
		// Same handler as POST /v1/uploads.
		return method == http.MethodPost
	}
	// Per-upload action endpoints: /v1/uploads/<id>[/action]
	rest := strings.TrimPrefix(path, "/v1/uploads/")
	if rest == "" {
		return false
	}
	parts := strings.SplitN(rest, "/", 2)
	uploadID := parts[0]
	if uploadID == "" {
		return false
	}
	action := ""
	if len(parts) > 1 {
		action = strings.Trim(parts[1], "/")
	}
	switch {
	case method == http.MethodPost && strings.HasPrefix(action, "complete"):
		return true
	case (method == http.MethodPost || method == http.MethodGet) && strings.HasPrefix(action, "resume"):
		return true
	case method == http.MethodDelete && action == "":
		return true
	default:
		return false
	}
}

// isScopedV2UploadRouteAllowed mirrors handleV2Uploads (server.go ~2338)
// exactly: only the 5 known V2 POST actions are admitted.
func isScopedV2UploadRouteAllowed(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	rest := strings.TrimPrefix(path, "/v2/uploads/")
	if rest == "" {
		return false
	}
	parts := strings.SplitN(rest, "/", 2)
	seg0 := parts[0]
	action := ""
	if len(parts) > 1 {
		action = strings.Trim(parts[1], "/")
	}
	if seg0 == "" {
		return false
	}
	// /v2/uploads/initiate (no upload_id segment) is its own route.
	if seg0 == "initiate" && action == "" {
		return true
	}
	// Per-upload action: seg0 = upload_id, action = one of the known V2 actions.
	switch action {
	case "presign", "presign-batch", "complete", "abort":
		return true
	default:
		return false
	}
}

// isScopedFSPostQueryAllowed mirrors handleFS's POST dispatcher (server.go
// :691 onward), but rejects mixed selectors as ambiguous (per @adversary-1
// msg 6e17765f) rather than silently first-wins. Each action arm allows
// only the keys the corresponding handler reads. chmod is never allowed
// for scoped tokens.
func isScopedFSPostQueryAllowed(q url.Values) bool {
	// Count how many action selectors are set. Multiple selectors = deny;
	// no selector = deny (no handler matches the dispatcher's else branch
	// for scoped tokens — that's "unknown POST action" which is a 400 for
	// owner today, and we don't want to silently widen for scoped).
	selectors := []string{"append", "copy", "rename", "mkdir", "chmod", "create", "symlink", "hardlink"}
	selectorCount := 0
	var selectorKey string
	for _, k := range selectors {
		if q.Has(k) {
			selectorCount++
			selectorKey = k
		}
	}
	if selectorCount != 1 {
		return false
	}

	switch selectorKey {
	case "append":
		// handleAppend reads only ?append.
		return queryKeysSubsetOf(q, []string{"append"})
	case "copy":
		// handleCopy reads only ?copy (source path is in
		// X-Dat9-Copy-Source header, not query).
		return queryKeysSubsetOf(q, []string{"copy"})
	case "rename":
		// handleRename reads only ?rename (source path is in
		// X-Dat9-Rename-Source header, not query).
		return queryKeysSubsetOf(q, []string{"rename"})
	case "mkdir":
		// handleMkdir reads ?mkdir and ?mode.
		return queryKeysSubsetOf(q, []string{"mkdir", "mode"})
	case "chmod":
		// chmod is owner-only; scoped tokens MUST NEVER reach this arm.
		// Defense in depth — the handler also rejects scoped tokens, but
		// the dispatcher gate makes the policy decision explicit at the
		// allowlist boundary.
		return false
	case "create":
		// handleCreate reads only ?create.
		return queryKeysSubsetOf(q, []string{"create"})
	case "symlink":
		// handleSymlink reads only ?symlink; target is in the JSON body.
		return queryKeysSubsetOf(q, []string{"symlink"})
	case "hardlink":
		// handleHardlink reads only ?hardlink; source path is in
		// X-Dat9-Hardlink-Source.
		return queryKeysSubsetOf(q, []string{"hardlink"})
	default:
		return false
	}
}

// isScopedFSGetQueryAllowed mirrors handleFS's GET dispatcher (server.go:618
// onward): the action is decided by which selector key is present, in
// priority order: stat → grep → find → list → (no selector = handleRead).
// Each branch accepts only the keys its handler actually reads; unknown
// keys for the selected branch deny so a future action key on the same
// path can't silently inherit the C1 allowlist.
func isScopedFSGetQueryAllowed(q url.Values) bool {
	// Note: handleFS GET dispatch order (stat > grep > find > list > read)
	// must match here, otherwise a request like `?stat=1&grep=hello` would
	// allow under one arm and be dispatched to another — pick the action
	// the dispatcher will pick.
	switch {
	case q.Has("stat"):
		// handleStatMetadata reads only ?stat.
		return queryKeysSubsetOf(q, []string{"stat"})
	case q.Has("grep"):
		// handleGrep reads ?grep, ?limit, and optional ?layer.
		return queryKeysSubsetOf(q, []string{"grep", "limit", "layer"})
	case q.Has("find"):
		// handleFind reads ?find, ?name, ?tag, ?newer, ?older,
		// ?minsize, ?maxsize, ?limit, and optional ?layer.
		return queryKeysSubsetOf(q, []string{
			"find", "name", "tag", "newer", "older",
			"minsize", "maxsize", "limit", "layer",
		})
	case q.Has("list"):
		// handleList reads only ?list. (No filter params today.)
		return queryKeysSubsetOf(q, []string{"list"})
	default:
		// No action selector → handleRead, which does not consume query
		// params. Any unknown key on a read request is rejected to prevent
		// future GET-side actions from silently inheriting the allowlist.
		return len(q) == 0
	}
}

// queryKeysSubsetOf returns true iff every key in q is present in allowed.
// allowed is small (<10 strings) so linear scan is fine.
func queryKeysSubsetOf(q url.Values, allowed []string) bool {
	for k := range q {
		ok := false
		for _, a := range allowed {
			if k == a {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleTenantStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_method_not_allowed", "method", r.Method)...)
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.localTenantShimEnabled() {
		s.handleLocalTenantStatus(w, r)
		return
	}
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_not_enabled")...)
		errJSON(w, http.StatusNotFound, "tenant status not enabled")
		return
	}
	tok := bearerToken(r)
	if tok == "" {
		metricEvent(r.Context(), "auth", "result", "missing_token")
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_missing_token")...)
		errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
		return
	}

	resolved, err := s.meta.ResolveByAPIKeyHash(r.Context(), token.HashToken(tok))
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			metricEvent(r.Context(), "auth", "result", "key_not_found")
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_key_not_found")...)
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		metricEvent(r.Context(), "auth", "result", "meta_backend_error")
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_meta_unavailable", "error", err)...)
		errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
		return
	}
	if subtle.ConstantTimeCompare([]byte(token.HashToken(tok)), []byte(resolved.APIKey.JWTHash)) != 1 {
		metricEvent(r.Context(), "auth", "result", "hash_mismatch")
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_hash_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	setRequestMetricTenant(r.Context(), resolved.Tenant.ID, resolved.APIKey.ID, resolved.Tenant.Provider, classifyTenantRequest(r))
	if resolved.APIKey.Status != meta.APIKeyActive {
		metricEvent(r.Context(), "auth", "result", "key_inactive")
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_key_inactive", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "status", resolved.APIKey.Status)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	plain, err := poolDecryptToken(r.Context(), s.pool, resolved.APIKey.JWTCiphertext)
	if err != nil {
		metricEvent(r.Context(), "auth", "result", "decrypt_failed")
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_decrypt_failed", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "error", err)...)
		errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
		return
	}
	if subtle.ConstantTimeCompare([]byte(tok), plain) != 1 {
		metricEvent(r.Context(), "auth", "result", "cipher_mismatch")
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_token_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	claims, err := token.ParseAndVerifyToken(s.tokenSecret, tok)
	if err != nil {
		metricEvent(r.Context(), "auth", "result", "token_invalid")
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_token_invalid", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "error", err)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	if claims.TenantID != resolved.Tenant.ID || claims.TokenVersion != resolved.APIKey.TokenVersion {
		metricEvent(r.Context(), "auth", "result", "claims_mismatch")
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_claims_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "claim_tenant", claims.TenantID, "claim_version", claims.TokenVersion)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}

	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_ok", "tenant_id", resolved.Tenant.ID, "status", resolved.Tenant.Status)...)
	_ = json.NewEncoder(w).Encode(TenantStatusResponse{
		Status:          string(resolved.Tenant.Status),
		Kind:            string(resolved.Tenant.Kind),
		Message:         s.tenantStatusMessage(&resolved.Tenant),
		MaxUploadBytes:  s.maxUploadBytes,
		InlineThreshold: s.inlineThreshold,
	})
}

func (s *Server) tenantStatusMessage(t *meta.Tenant) string {
	if t == nil {
		return ""
	}
	if t.Kind == meta.TenantKindFork && t.Status == meta.TenantProvisioning {
		return forkProvisioningMessage(t)
	}
	if s != nil && (t.Status == meta.TenantProvisioning || t.Status == meta.TenantFailed) {
		if value, ok := s.schemaInitErrors.Load(t.ID); ok {
			if msg, ok := value.(string); ok && msg != "" {
				return "schema init error: " + msg
			}
		}
	}
	return ""
}

func schemaInitStatusErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	if len(msg) > 500 {
		return msg[:500] + "..."
	}
	return msg
}

func backendFromRequest(r *http.Request) *backend.Dat9Backend {
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		return nil
	}
	return scope.Backend
}

func (s *Server) localTenantShimEnabled() bool {
	return s.fallback != nil && s.meta == nil && s.pool == nil && len(s.tokenSecret) == 0 && s.localTenantAPIKey != ""
}

// handleLocalTenantStatus serves drive9-server-local's single-tenant compatibility
// path so e2e scripts can probe tenant status without enabling the multi-tenant
// control plane.
func (s *Server) handleLocalTenantStatus(w http.ResponseWriter, r *http.Request) {
	tok := bearerToken(r)
	if tok == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_missing_token")...)
		errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
		return
	}
	if subtle.ConstantTimeCompare([]byte(tok), []byte(s.localTenantAPIKey)) != 1 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_key_not_found", "tenant_id", "local", "api_key_id", "local")...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	setRequestMetricTenant(r.Context(), "local", "local", "local", classifyTenantRequest(r))
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_ok", "tenant_id", "local", "status", "active")...)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(TenantStatusResponse{
		Status:          "active",
		Kind:            "live",
		MaxUploadBytes:  s.maxUploadBytes,
		InlineThreshold: s.inlineThreshold,
	})
}

func (s *Server) handleFS(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
	if path == "" {
		path = "/"
	}

	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("stat") {
			// HEAD and GET ?stat=1 serve different stat contracts:
			// - GET ?stat=1 (s.handleStatMetadata): enriched JSON metadata
			//   (content_type/semantic_text/tags in addition to core attrs).
			s.handleStatMetadata(w, r, path)
		} else if r.URL.Query().Has("grep") {
			s.handleGrep(w, r, path)
		} else if r.URL.Query().Has("find") {
			s.handleFind(w, r, path)
		} else if r.URL.Query().Has("list") {
			s.handleList(w, r, path)
		} else {
			s.handleRead(w, r, path)
		}
	case http.MethodPut:
		s.handleWrite(w, r, path)
	case http.MethodHead:
		// HEAD (s.handleStat) serves the lightweight header-based stat contract
		// (size/isdir/revision/mtime).
		s.handleStat(w, r, path)
	case http.MethodDelete:
		s.handleDelete(w, r, path)
	case http.MethodPatch:
		s.handlePatch(w, r, path)
	case http.MethodPost:
		if r.URL.Query().Has("append") {
			s.handleAppend(w, r, path)
		} else if r.URL.Query().Has("copy") {
			s.handleCopy(w, r, path)
		} else if r.URL.Query().Has("rename") {
			s.handleRename(w, r, path)
		} else if r.URL.Query().Has("mkdir") {
			s.handleMkdir(w, r, path)
		} else if r.URL.Query().Has("chmod") {
			s.handleChmod(w, r, path)
		} else if r.URL.Query().Has("create") {
			s.handleCreate(w, r, path)
		} else if r.URL.Query().Has("symlink") {
			s.handleSymlink(w, r, path)
		} else if r.URL.Query().Has("hardlink") {
			s.handleHardlink(w, r, path)
		} else {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "fs_unknown_post_action", "path", path)...)
			errJSON(w, http.StatusBadRequest, "unknown POST action")
		}
	default:
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "fs_method_not_allowed", "path", path, "method", r.Method)...)
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpRead, path) {
		return
	}
	start := time.Now()
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "read_missing_scope", "path", path)...)
		metricEvent(r.Context(), "fs_read", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	// Single-pass read plan: one metadata query routes to either inline
	// response (db9) or presigned redirect (S3). No fallback double-stat.
	logger.InfoBenchTiming(r.Context(), "server_read_start", zap.String("path", path))
	planStart := time.Now()
	plan, err := b.ReadPlanCtx(r.Context(), path)
	planDuration := time.Since(planStart)
	if err != nil {
		logger.InfoBenchTiming(r.Context(), "server_read_timing",
			zap.String("path", path),
			zap.Float64("plan_ms", float64(planDuration.Microseconds())/1000.0),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0),
			zap.Error(err))
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "read_not_found", "path", path)...)
			metricEvent(r.Context(), "fs_read", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "read_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_read", "result", "error")
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}

	if plan.PresignURL != "" {
		logger.InfoBenchTiming(r.Context(), "server_read_timing",
			zap.String("path", path),
			zap.String("mode", "redirect"),
			zap.Int64("size", plan.Size),
			zap.Float64("plan_ms", float64(planDuration.Microseconds())/1000.0),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
		logger.Info(r.Context(), "server_event", eventFields(r.Context(), "read_presigned_redirect", "path", path)...)
		metricEvent(r.Context(), "fs_read", "result", "ok")
		recordTenantFileBytes(r.Context(), "fs", "read", "read", plan.Size)
		http.Redirect(w, r, plan.PresignURL, http.StatusFound)
		return
	}

	logger.InfoBenchTiming(r.Context(), "server_read_timing",
		zap.String("path", path),
		zap.String("mode", "inline"),
		zap.Int64("size", plan.Size),
		zap.Int("bytes", len(plan.InlineData)),
		zap.Float64("plan_ms", float64(planDuration.Microseconds())/1000.0),
		zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "read_ok", "path", path, "bytes", len(plan.InlineData))...)
	metricEvent(r.Context(), "fs_read", "result", "ok")
	recordTenantFileBytes(r.Context(), "fs", "read", "read", int64(len(plan.InlineData)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(plan.InlineData)))
	_, _ = w.Write(plan.InlineData)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpList, path) {
		return
	}
	start := time.Now()
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "list_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	logger.InfoBenchTiming(r.Context(), "server_list_start", zap.String("path", path))
	readDirStart := time.Now()
	entries, err := b.ReadDirCtx(r.Context(), path)
	readDirDuration := time.Since(readDirStart)
	if err != nil {
		logger.InfoBenchTiming(r.Context(), "server_list_timing",
			zap.String("path", path),
			zap.Float64("read_dir_ms", float64(readDirDuration.Microseconds())/1000.0),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0),
			zap.Error(err))
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "list_not_found", "path", path)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "list_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "userdb_query", "api", "list", "result", "error")
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	metricEvent(r.Context(), "userdb_query", "api", "list", "result", "ok")
	logger.InfoBenchTiming(r.Context(), "server_list_timing",
		zap.String("path", path),
		zap.Int("entries", len(entries)),
		zap.Float64("read_dir_ms", float64(readDirDuration.Microseconds())/1000.0),
		zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "list_ok", "path", path, "entries", len(entries))...)
	type entry struct {
		Name       string `json:"name"`
		Size       int64  `json:"size"`
		IsDir      bool   `json:"isDir"`
		Mtime      int64  `json:"mtime,omitempty"`
		Revision   int64  `json:"revision,omitempty"`
		Mode       uint32 `json:"mode,omitempty"`
		HasMode    bool   `json:"hasMode"`
		ResourceID string `json:"resource_id,omitempty"`
		Nlink      uint32 `json:"nlink,omitempty"`
	}
	out := make([]entry, 0, len(entries))
	for _, e := range entries {
		var mtime int64
		if !e.ModTime.IsZero() {
			mtime = e.ModTime.Unix()
		}
		hasMode := e.Meta.Content["hasMode"] == "true"
		var nlink uint32
		if raw := e.Meta.Content["nlink"]; raw != "" {
			if parsed, err := strconv.ParseUint(raw, 10, 32); err == nil {
				nlink = uint32(parsed)
			}
		}
		var revision int64
		if raw := e.Meta.Content["revision"]; raw != "" {
			if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
				revision = parsed
			}
		}
		out = append(out, entry{
			Name:       e.Name,
			Size:       e.Size,
			IsDir:      e.IsDir,
			Mtime:      mtime,
			Revision:   revision,
			Mode:       e.Mode,
			HasMode:    hasMode,
			ResourceID: e.Meta.Content["resource_id"],
			Nlink:      nlink,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
}

func (s *Server) handleStatMetadata(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpRead, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "stat_metadata_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Root "/" is an implicit directory with no file_nodes row.
	if path == "/" {
		zero := int64(0)
		logger.Info(r.Context(), "server_event", eventFields(r.Context(), "stat_metadata_ok", "path", path, "is_dir", true)...)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Mtime", "0")
		_ = json.NewEncoder(w).Encode(struct {
			Size         int64             `json:"size"`
			IsDir        bool              `json:"isdir"`
			ResourceID   string            `json:"resource_id"`
			Nlink        uint32            `json:"nlink,omitempty"`
			Revision     int64             `json:"revision"`
			Mtime        *int64            `json:"mtime,omitempty"`
			ContentType  string            `json:"content_type"`
			SemanticText string            `json:"semantic_text"`
			Tags         map[string]string `json:"tags"`
		}{
			IsDir: true,
			Nlink: 2,
			Mtime: &zero,
			Tags:  make(map[string]string),
		})
		return
	}

	nf, err := b.StatNodeCtx(r.Context(), path)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "stat_metadata_not_found", "path", path)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "stat_metadata_failed", "path", path, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}

	tags := make(map[string]string)
	var size int64
	var revision int64
	var mtime *int64
	var contentType string
	var semanticText string
	resourceID := nf.Node.NodeID
	nlink, err := nodeLinkCount(r.Context(), b, nf)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "stat_metadata_refcount_failed", "path", path, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	if nf.File != nil {
		resourceID = nf.File.FileID
		size = nf.File.SizeBytes
		revision = nf.File.Revision
		if nf.File.ConfirmedAt != nil {
			unix := nf.File.ConfirmedAt.Unix()
			mtime = &unix
		} else {
			unix := nf.File.CreatedAt.Unix()
			mtime = &unix
		}
		contentType = nf.File.ContentType
		semanticText = nf.File.ContentText

		tags, err = b.Store().GetFileTags(r.Context(), nf.File.FileID)
		if err != nil {
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "stat_metadata_load_tags_failed", "path", path, "error", err)...)
			errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
			return
		}
	} else {
		unix := nf.Node.CreatedAt.Unix()
		mtime = &unix
	}

	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "stat_metadata_ok", "path", path, "is_dir", nf.Node.IsDirectory)...)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(*mtime, 10))
	_ = json.NewEncoder(w).Encode(struct {
		Size         int64             `json:"size"`
		IsDir        bool              `json:"isdir"`
		ResourceID   string            `json:"resource_id"`
		Nlink        uint32            `json:"nlink,omitempty"`
		Revision     int64             `json:"revision"`
		Mtime        *int64            `json:"mtime,omitempty"`
		ContentType  string            `json:"content_type"`
		SemanticText string            `json:"semantic_text"`
		Tags         map[string]string `json:"tags"`
	}{
		Size:         size,
		IsDir:        nf.Node.IsDirectory,
		ResourceID:   resourceID,
		Nlink:        nlink,
		Revision:     revision,
		Mtime:        mtime,
		ContentType:  contentType,
		SemanticText: semanticText,
		Tags:         tags,
	})
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, path string) {
	timingEnabled := logger.BenchTimingLogEnabled()
	var timingStart time.Time
	var bodyReadDuration time.Duration
	var backendWriteDuration time.Duration
	var responseDuration time.Duration
	if timingEnabled {
		timingStart = time.Now()
	}
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_missing_scope", "path", path)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	expectedRevision, err := parseExpectedRevisionHeader(r.Header.Get("X-Dat9-Expected-Revision"))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid X-Dat9-Expected-Revision header")
		return
	}
	writeTags, err := parseWriteTagsHeader(r.Header)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_invalid_tag_header", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	actualCL := r.ContentLength
	cl := actualCL
	if h := r.Header.Get("X-Dat9-Content-Length"); h != "" {
		parsed, err := strconv.ParseInt(h, 10, 64)
		if err != nil || parsed < 0 {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_invalid_declared_length", "path", path, "header", h)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusBadRequest, "invalid X-Dat9-Content-Length")
			return
		}
		if actualCL > 0 && parsed > 0 && actualCL != parsed {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_length_mismatch", "path", path, "content_length", actualCL, "declared_length", parsed)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusBadRequest, "Content-Length and X-Dat9-Content-Length mismatch")
			return
		}
		cl = parsed
	}
	if cl > s.maxUploadBytes {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_too_large", "path", path, "bytes", cl, "max", s.maxUploadBytes)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload too large: max %d bytes", s.maxUploadBytes))
		return
	}
	if cl > 0 && b.IsLargeFile(cl) {
		if len(writeTags) > 0 {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_large_put_tag_unsupported", "path", path)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusBadRequest, "X-Dat9-Tag is not supported on large-file PUT initiate; send tags in upload complete request")
			return
		}
		partChecksums, err := parsePartChecksumsHeader(r.Header.Get("X-Dat9-Part-Checksums"))
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_bad_checksums_header", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if len(partChecksums) == 0 {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_missing_checksums_header", "path", path)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusBadRequest, "missing X-Dat9-Part-Checksums header")
			return
		}
		description := r.Header.Get("X-Dat9-Description")
		if utf8.RuneCountInString(description) > backend.MaxDescriptionLen {
			errJSON(w, http.StatusBadRequest, fmt.Sprintf("description exceeds %d characters", backend.MaxDescriptionLen))
			return
		}
		plan, err := b.InitiateUploadWithChecksumsIfRevision(r.Context(), path, cl, partChecksums, expectedRevision, description)
		if err != nil {
			if errors.Is(err, backend.ErrUploadTooLarge) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_upload_too_large", "path", path, "error", err)...)
				metricEvent(r.Context(), "fs_write", "result", "error")
				errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
				return
			}
			if isBackendQuotaExceeded(err) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_storage_quota_exceeded", "path", path, "error", err)...)
				metricEvent(r.Context(), "fs_write", "result", "error")
				errJSON(w, http.StatusInsufficientStorage, err.Error())
				return
			}
			if errors.Is(err, backend.ErrPartChecksumCountMismatch) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_checksum_count_mismatch", "path", path, "error", err)...)
				metricEvent(r.Context(), "fs_write", "result", "error")
				errJSON(w, http.StatusBadRequest, err.Error())
				return
			}
			if errors.Is(err, datastore.ErrUploadConflict) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_upload_conflict", "path", path, "error", err)...)
				metricEvent(r.Context(), "fs_write", "result", "conflict")
				errJSON(w, http.StatusConflict, err.Error())
				return
			}
			if errors.Is(err, datastore.ErrRevisionConflict) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_upload_revision_conflict", "path", path, "error", err)...)
				metricEvent(r.Context(), "fs_write", "result", "conflict")
				errJSON(w, http.StatusConflict, err.Error())
				return
			}
			if errJSONInvalidRootDentry(w, err) {
				metricEvent(r.Context(), "fs_write", "result", "error")
				return
			}
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "write_upload_initiate_failed", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSONInternalStorage(w)
			return
		}
		logger.Info(r.Context(), "server_event", eventFields(r.Context(), "write_upload_initiated", "path", path, "parts", len(plan.Parts))...)
		metricEvent(r.Context(), "fs_write", "result", "accepted")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(plan)
		return
	}
	body := http.MaxBytesReader(w, r.Body, s.maxUploadBytes)
	bodyReadStart := time.Now()
	data, err := io.ReadAll(body)
	bodyReadDuration = time.Since(bodyReadStart)
	bodyReadMetricBytes := requestBodyReadMetricBytes(cl, len(data))
	setRequestBodyReadMetric(r.Context(), r.Method, requestRoute(r.URL.Path), bodyReadMetricBytes, bodyReadDuration)
	logSlowWriteBodyRead(r.Context(), r, requestRoute(r.URL.Path), bodyReadMetricBytes, bodyReadDuration)
	if err != nil {
		if timingEnabled {
			logServerWriteTiming(r.Context(), path, 0, expectedRevision, 0, "body_read_error", bodyReadDuration, backendWriteDuration, responseDuration, time.Since(timingStart), err)
		}
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_body_too_large", "path", path, "max", s.maxUploadBytes)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload too large: max %d bytes", s.maxUploadBytes))
			return
		}
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_body_read_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	description := r.Header.Get("X-Dat9-Description")
	if utf8.RuneCountInString(description) > backend.MaxDescriptionLen {
		if timingEnabled {
			logServerWriteTiming(r.Context(), path, len(data), expectedRevision, 0, "validation_error", bodyReadDuration, backendWriteDuration, responseDuration, time.Since(timingStart), nil)
		}
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("description exceeds %d characters", backend.MaxDescriptionLen))
		return
	}
	backendWriteStart := time.Time{}
	if timingEnabled {
		backendWriteStart = time.Now()
	}
	_, committedRevision, err := b.WriteCtxIfRevisionWithTagsResult(r.Context(), path, data, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, expectedRevision, writeTags, description)
	if timingEnabled {
		backendWriteDuration = time.Since(backendWriteStart)
	}
	if err != nil {
		if timingEnabled {
			logServerWriteTiming(r.Context(), path, len(data), expectedRevision, 0, "error", bodyReadDuration, backendWriteDuration, responseDuration, time.Since(timingStart), err)
		}
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_too_large_backend", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if isBackendQuotaExceeded(err) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_storage_quota_exceeded", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_conflict", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			metricEvent(r.Context(), "fs_write", "result", "error")
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "write_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "write_ok", "path", path, "bytes", len(data))...)
	metricEvent(r.Context(), "fs_write", "result", "ok")
	recordTenantFileBytes(r.Context(), "fs", "write", "write", int64(len(data)))
	responseStart := time.Time{}
	if timingEnabled {
		responseStart = time.Now()
	}
	s.publishEvent(r, path, "write")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": committedRevision})
	if timingEnabled {
		responseDuration = time.Since(responseStart)
		logServerWriteTiming(r.Context(), path, len(data), expectedRevision, committedRevision, "ok", bodyReadDuration, backendWriteDuration, responseDuration, time.Since(timingStart), nil)
	}
}

func logServerWriteTiming(ctx context.Context, path string, bytes int, expectedRevision, committedRevision int64, result string, bodyReadDuration, backendWriteDuration, responseDuration, totalDuration time.Duration, err error) {
	fields := []zap.Field{
		zap.String("path", path),
		zap.String("result", result),
		zap.Int("bytes", bytes),
		zap.Int64("expected_revision", expectedRevision),
		zap.Int64("committed_revision", committedRevision),
		zap.Float64("body_read_ms", serverDurationMs(bodyReadDuration)),
		zap.Float64("backend_write_ms", serverDurationMs(backendWriteDuration)),
		zap.Float64("response_ms", serverDurationMs(responseDuration)),
		zap.Float64("total_ms", serverDurationMs(totalDuration)),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	logger.InfoBenchTiming(ctx, "server_write_timing", fields...)
}

func serverDurationMs(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func requestBodyReadMetricBytes(declared int64, observed int) int64 {
	if declared >= 0 {
		return declared
	}
	return int64(observed)
}

const slowWriteBodyReadThreshold = 5 * time.Second

func logSlowWriteBodyRead(ctx context.Context, r *http.Request, route string, bytes int64, d time.Duration) {
	if d < slowWriteBodyReadThreshold {
		return
	}
	fields := eventFields(ctx,
		"write_body_read_slow",
		"method", r.Method,
		"route", route,
		"bytes", bytes,
		"duration_ms", serverDurationMs(d),
		"rate_bucket", bodyReadRateBucket(bytes, d),
	)
	if r.RemoteAddr != "" {
		fields = append(fields, zap.String("remote", r.RemoteAddr))
	}
	logger.Warn(ctx, "server_event", fields...)
}

func bodyReadRateBucket(bytes int64, d time.Duration) string {
	if bytes <= 0 || d <= 0 {
		return "unknown"
	}
	bytesPerSecond := float64(bytes) / d.Seconds()
	switch {
	case bytesPerSecond <= 1<<10:
		return "le_1KiB_s"
	case bytesPerSecond <= 10<<10:
		return "le_10KiB_s"
	case bytesPerSecond <= 100<<10:
		return "le_100KiB_s"
	case bytesPerSecond <= 1<<20:
		return "le_1MiB_s"
	case bytesPerSecond <= 10<<20:
		return "le_10MiB_s"
	default:
		return "gt_10MiB_s"
	}
}

func (s *Server) handlePatch(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_missing_scope", "path", path)...)
		metricEvent(r.Context(), "fs_patch", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	var req struct {
		NewSize          int64  `json:"new_size"`
		DirtyParts       []int  `json:"dirty_parts"`
		PartSize         int64  `json:"part_size,omitempty"`
		ExpectedRevision *int64 `json:"expected_revision,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_bad_body", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_patch", "result", "error")
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.NewSize <= 0 {
		errJSON(w, http.StatusBadRequest, "new_size must be positive")
		return
	}
	if len(req.DirtyParts) == 0 {
		errJSON(w, http.StatusBadRequest, "dirty_parts must not be empty")
		return
	}
	if req.NewSize > s.maxUploadBytes {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_too_large", "path", path, "bytes", req.NewSize, "max", s.maxUploadBytes)...)
		metricEvent(r.Context(), "fs_patch", "result", "error")
		errJSON(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload too large: max %d bytes", s.maxUploadBytes))
		return
	}
	if req.ExpectedRevision != nil && *req.ExpectedRevision < 0 {
		errJSON(w, http.StatusBadRequest, "expected_revision must be >= 0")
		return
	}
	expectedRevision := int64(-1)
	if req.ExpectedRevision != nil {
		expectedRevision = *req.ExpectedRevision
	}

	plan, err := b.InitiatePatchUploadIfRevision(r.Context(), path, req.NewSize, req.DirtyParts, req.PartSize, expectedRevision)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_too_large", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_patch", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if isBackendQuotaExceeded(err) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_storage_quota_exceeded", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_patch", "result", "error")
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_conflict", "path", path)...)
			metricEvent(r.Context(), "fs_patch", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_revision_conflict", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_patch", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_not_found", "path", path)...)
			metricEvent(r.Context(), "fs_patch", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			metricEvent(r.Context(), "fs_patch", "result", "error")
			return
		}
		if errors.Is(err, backend.ErrNotS3Stored) || errors.Is(err, backend.ErrS3NotConfigured) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_unsupported_target", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_patch", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_patch", "result", "error")
		errJSONInternalStorage(w)
		return
	}

	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_initiated", "path", path,
		"dirty_parts", len(plan.UploadParts), "copied_parts", len(plan.CopiedParts))...)
	metricEvent(r.Context(), "fs_patch", "result", "accepted")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request, path string) {
	// Authorize BEFORE generating the upload plan (per @adversary-1 review
	// banked invariant): a plan response leaks "this prefix is writable" to
	// any caller who can hit the endpoint, even if the subsequent PUT is
	// later denied. Putting authorize first ensures denied scoped tokens
	// receive a 403 with no plan-shaped JSON body.
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_missing_scope", "path", path)...)
		metricEvent(r.Context(), "fs_append", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	var req struct {
		AppendSize       int64  `json:"append_size"`
		PartSize         int64  `json:"part_size,omitempty"`
		ExpectedRevision *int64 `json:"expected_revision,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_bad_body", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_append", "result", "error")
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.AppendSize <= 0 {
		errJSON(w, http.StatusBadRequest, "append_size must be positive")
		return
	}
	if req.ExpectedRevision != nil && *req.ExpectedRevision < 0 {
		errJSON(w, http.StatusBadRequest, "expected_revision must be >= 0")
		return
	}

	expectedRevision := int64(-1)
	if req.ExpectedRevision != nil {
		expectedRevision = *req.ExpectedRevision
	}

	plan, err := b.InitiateAppendUploadIfRevision(r.Context(), path, req.AppendSize, req.PartSize, expectedRevision)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_upload_too_large", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_append", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if isBackendQuotaExceeded(err) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_storage_quota_exceeded", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_append", "result", "error")
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_upload_conflict", "path", path)...)
			metricEvent(r.Context(), "fs_append", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_revision_conflict", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_append", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_not_found", "path", path)...)
			metricEvent(r.Context(), "fs_append", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			metricEvent(r.Context(), "fs_append", "result", "error")
			return
		}
		if errors.Is(err, backend.ErrNotS3Stored) || errors.Is(err, backend.ErrS3NotConfigured) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "append_bad_target", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_append", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "append_upload_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_append", "result", "error")
		errJSONInternalStorage(w)
		return
	}

	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "append_upload_initiated", "path", path,
		"dirty_parts", len(plan.UploadParts), "copied_parts", len(plan.CopiedParts), "base_size", plan.BaseSize)...)
	metricEvent(r.Context(), "fs_append", "result", "accepted")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(plan)
}

type batchStatRequest struct {
	Paths []string `json:"paths"`
}

type batchStatResponse struct {
	Results []batchStatResult `json:"results"`
}

type batchStatResult struct {
	Path       string `json:"path"`
	Status     int    `json:"status"`
	Error      string `json:"error,omitempty"`
	Size       int64  `json:"size,omitempty"`
	IsDir      bool   `json:"isDir"`
	Revision   int64  `json:"revision,omitempty"`
	Mtime      int64  `json:"mtime,omitempty"`
	Mode       uint32 `json:"mode,omitempty"`
	HasMode    bool   `json:"hasMode"`
	ResourceID string `json:"resource_id,omitempty"`
	Nlink      uint32 `json:"nlink,omitempty"`
}

type batchReadSmallRequest struct {
	Paths    []string `json:"paths"`
	MaxBytes int64    `json:"max_bytes,omitempty"`
}

type batchReadSmallResponse struct {
	Results []batchReadSmallResult `json:"results"`
}

type batchReadSmallResult struct {
	Path     string `json:"path"`
	Status   int    `json:"status"`
	Error    string `json:"error,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Revision int64  `json:"revision,omitempty"`
	Mtime    int64  `json:"mtime,omitempty"`
}

func (s *Server) handleBatchStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_stat_method_not_allowed", "method", r.Method)...)
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_stat_missing_scope")...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	var req batchStatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_stat_decode_failed", "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if len(req.Paths) > maxBatchStatPaths {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_stat_too_large", "count", len(req.Paths), "limit", maxBatchStatPaths)...)
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("too many paths: %d exceeds limit %d", len(req.Paths), maxBatchStatPaths))
		return
	}

	scope := ScopeFromContext(r.Context())
	results := make([]batchStatResult, len(req.Paths))
	for i, path := range req.Paths {
		// Per-path authorize: a denied path becomes a per-element 403 in
		// the response, but allowed siblings still resolve. This preserves
		// the cp -r preflight contract (PR #434) — one out-of-zone path
		// must not 403 the entire batch.
		if status, msg, allowed := authorizeFSPathForBatch(scope, FSOpRead, path); !allowed {
			results[i] = batchStatResult{Path: path, Status: status, Error: msg}
			continue
		}
		results[i] = s.batchStatOne(r.Context(), b, path)
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "batch_stat_ok", "count", len(results))...)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(batchStatResponse{Results: results})
}

func (s *Server) handleBatchReadSmall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_read_small_method_not_allowed", "method", r.Method)...)
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_read_small_missing_scope")...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	var req batchReadSmallRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, defaultBatchReadMaxBody)).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_read_small_decode_failed", "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if len(req.Paths) > maxBatchReadSmallPaths {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "batch_read_small_too_large", "count", len(req.Paths), "limit", maxBatchReadSmallPaths)...)
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("too many paths: %d exceeds limit %d", len(req.Paths), maxBatchReadSmallPaths))
		return
	}

	maxBytes := req.MaxBytes
	if maxBytes <= 0 || maxBytes > maxBatchReadSmallBytes {
		maxBytes = maxBatchReadSmallBytes
	}
	scope := ScopeFromContext(r.Context())
	results := make([]batchReadSmallResult, len(req.Paths))
	for i, path := range req.Paths {
		// Per-path authorize, same shape as batch-stat above.
		if status, msg, allowed := authorizeFSPathForBatch(scope, FSOpRead, path); !allowed {
			results[i] = batchReadSmallResult{Path: path, Status: status, Error: msg}
			continue
		}
		results[i] = s.batchReadSmallOne(r.Context(), b, path, maxBytes)
	}
	var readBytes int64
	for _, result := range results {
		if result.Status == http.StatusOK {
			readBytes += int64(len(result.Data))
		}
	}
	recordTenantFileBytes(r.Context(), "fs", "batch_read_small", "read", readBytes)
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "batch_read_small_ok", "count", len(results), "max_bytes", maxBytes)...)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(batchReadSmallResponse{Results: results})
}

func (s *Server) batchReadSmallOne(ctx context.Context, b *backend.Dat9Backend, rawPath string, maxBytes int64) batchReadSmallResult {
	result := batchReadSmallResult{Path: rawPath}
	if err := validateBatchStatPath(rawPath); err != nil {
		result.Status = http.StatusBadRequest
		result.Error = err.Error()
		return result
	}

	plan, err := b.ReadInlinePlanCtx(ctx, rawPath)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			result.Status = http.StatusNotFound
			result.Error = err.Error()
			return result
		}
		if errors.Is(err, backend.ErrNotInlineStorage) {
			result.Status = http.StatusRequestEntityTooLarge
			result.Error = "file is not available for inline read"
			return result
		}
		result.Status = http.StatusInternalServerError
		result.Error = err.Error()
		return result
	}
	if plan.Size > maxBytes || int64(len(plan.InlineData)) > maxBytes {
		result.Status = http.StatusRequestEntityTooLarge
		result.Error = fmt.Sprintf("file exceeds batch read-small limit %d bytes", maxBytes)
		return result
	}
	result.Status = http.StatusOK
	result.Data = plan.InlineData
	result.Size = plan.Size
	result.Revision = plan.Revision
	if !plan.Mtime.IsZero() {
		result.Mtime = plan.Mtime.Unix()
	}
	return result
}

func (s *Server) batchStatOne(ctx context.Context, b *backend.Dat9Backend, rawPath string) batchStatResult {
	result := batchStatResult{Path: rawPath}
	path := rawPath
	if path == "" {
		path = "/"
	}
	if err := validateBatchStatPath(path); err != nil {
		result.Status = http.StatusBadRequest
		result.Error = err.Error()
		return result
	}
	if path == "/" {
		result.Status = http.StatusOK
		result.IsDir = true
		result.Nlink = 2
		return result
	}

	nf, err := b.StatNodeLiteCtx(ctx, path)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			result.Status = http.StatusNotFound
			result.Error = err.Error()
			return result
		}
		result.Status = http.StatusInternalServerError
		result.Error = err.Error()
		return result
	}
	result.Status = http.StatusOK
	result.IsDir = nf.Node.IsDirectory
	result.HasMode = nf.HasMode
	result.Mode = nf.Mode
	result.ResourceID = nodeResourceID(nf)
	nlink, nlinkErr := nodeLinkCount(ctx, b, nf)
	if nlinkErr != nil {
		result.Status = http.StatusInternalServerError
		result.Error = nlinkErr.Error()
		return result
	}
	result.Nlink = nlink
	if nf.File != nil {
		result.Size = nf.File.SizeBytes
		result.Revision = nf.File.Revision
		if nf.File.ConfirmedAt != nil {
			result.Mtime = nf.File.ConfirmedAt.Unix()
		} else {
			result.Mtime = nf.File.CreatedAt.Unix()
		}
	} else {
		result.Mtime = nf.Node.CreatedAt.Unix()
	}
	return result
}

func validateBatchStatPath(rawPath string) error {
	if pathutil.IsDir(rawPath) {
		_, err := pathutil.CanonicalizeDir(rawPath)
		return err
	}
	_, err := pathutil.Canonicalize(rawPath)
	return err
}

func nodeResourceID(nf *datastore.NodeWithFile) string {
	if nf == nil {
		return ""
	}
	if nf.File != nil {
		return nf.File.FileID
	}
	if nf.Node.InodeID != "" {
		return nf.Node.InodeID
	}
	return nf.Node.NodeID
}

func nodeLinkCount(ctx context.Context, b *backend.Dat9Backend, nf *datastore.NodeWithFile) (uint32, error) {
	if nf == nil {
		return 0, nil
	}
	if nf.File == nil || nf.File.FileID == "" {
		if nf.Node.IsDirectory {
			return 2, nil
		}
		return 1, nil
	}
	count, err := b.Store().RefCount(ctx, nf.File.FileID)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		count = 1
	}
	if count > int64(^uint32(0)) {
		return ^uint32(0), nil
	}
	return uint32(count), nil
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request, path string) {
	// HEAD must not write a body; for scoped-token deny the status code is
	// still informative (403/400). Go's net/http strips body bytes from HEAD
	// responses, so reusing authorizeFS (which calls errJSON) is safe — the
	// JSON body will not reach the client.
	if !authorizeFS(w, r, FSOpRead, path) {
		return
	}
	start := time.Now()
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "stat_missing_scope", "path", path)...)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Root "/" is an implicit directory that has no file_nodes row.
	// Return a synthetic stat response so clients (WebDAV mount, SDK)
	// can stat the root like any other directory.
	if path == "/" {
		w.Header().Set("Content-Length", "0")
		w.Header().Set("X-Dat9-IsDir", "true")
		w.Header().Set("X-Dat9-Mtime", "0")
		w.Header().Set("X-Dat9-Nlink", "2")
		logger.Info(r.Context(), "server_event", eventFields(r.Context(), "stat_ok", "path", path, "is_dir", true)...)
		w.WriteHeader(http.StatusOK)
		return
	}

	logger.InfoBenchTiming(r.Context(), "server_stat_start", zap.String("path", path))
	statStart := time.Now()
	nf, err := b.StatNodeLiteCtx(r.Context(), path)
	statDuration := time.Since(statStart)
	if err != nil {
		logger.InfoBenchTiming(r.Context(), "server_stat_timing",
			zap.String("path", path),
			zap.Float64("stat_ms", float64(statDuration.Microseconds())/1000.0),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0),
			zap.Error(err))
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "stat_not_found", "path", path)...)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "stat_failed", "path", path, "error", err)...)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var size int64
	if nf.File != nil {
		size = nf.File.SizeBytes
	}
	nlink, err := nodeLinkCount(r.Context(), b, nf)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "stat_refcount_failed", "path", path, "error", err)...)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.InfoBenchTiming(r.Context(), "server_stat_timing",
		zap.String("path", path),
		zap.Bool("is_dir", nf.Node.IsDirectory),
		zap.Int64("size", size),
		zap.Float64("stat_ms", float64(statDuration.Microseconds())/1000.0),
		zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("X-Dat9-IsDir", fmt.Sprintf("%v", nf.Node.IsDirectory))
	if resourceID := nodeResourceID(nf); resourceID != "" {
		w.Header().Set("X-Dat9-Resource-ID", resourceID)
	}
	if nlink > 0 {
		w.Header().Set("X-Dat9-Nlink", strconv.FormatUint(uint64(nlink), 10))
	}
	if nf.File != nil {
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(nf.File.Revision, 10))
		if nf.File.ConfirmedAt != nil {
			w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(nf.File.ConfirmedAt.Unix(), 10))
		} else {
			w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(nf.File.CreatedAt.Unix(), 10))
		}
	} else {
		w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(nf.Node.CreatedAt.Unix(), 10))
	}
	if nf.HasMode {
		w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(nf.Mode), 10))
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "stat_ok", "path", path, "is_dir", nf.Node.IsDirectory)...)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpDelete, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "delete_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	recursive := r.URL.Query().Has("recursive")
	kind := r.URL.Query().Get("kind")
	var err error
	if recursive {
		err = b.RemoveAllCtx(r.Context(), path)
	} else {
		switch kind {
		case "":
			err = b.RemoveCtx(r.Context(), path)
		case "file":
			err = b.RemoveFileCtx(r.Context(), path)
		case "dir":
			err = b.RemoveDirCtx(r.Context(), path)
		default:
			errJSON(w, http.StatusBadRequest, "invalid delete kind")
			return
		}
	}
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "delete_not_found", "path", path, "recursive", recursive, "kind", kind)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "delete_failed", "path", path, "recursive", recursive, "kind", kind, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "delete_ok", "path", path, "recursive", recursive, "kind", kind)...)
	s.publishEvent(r, path, "delete")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCopy(w http.ResponseWriter, r *http.Request, dstPath string) {
	// Copy semantics: src needs read, dst needs write. The source path is
	// in the X-Dat9-Copy-Source HEADER (not the URL) — banked invariant
	// from the cp -r work (PR #434): missing this means the dispatcher
	// could authorize ONLY the dst from the URL and let a scoped token
	// exfiltrate from any zone it doesn't have read on, as long as its dst
	// zone is writable. Both ends MUST authorize.
	srcPath := r.Header.Get("X-Dat9-Copy-Source")
	if srcPath == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "copy_missing_source_header", "dst_path", dstPath)...)
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Copy-Source header")
		return
	}
	if !authorizeFSPair(w, r, FSOpRead, srcPath, FSOpWrite, dstPath) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "copy_missing_scope", "dst_path", dstPath)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.CopyFileCtx(r.Context(), srcPath, dstPath); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "copy_not_found", "src_path", srcPath, "dst_path", dstPath)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "copy_failed", "src_path", srcPath, "dst_path", dstPath, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "copy_ok", "src_path", srcPath, "dst_path", dstPath)...)
	s.publishEvent(r, dstPath, "copy")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleHardlink(w http.ResponseWriter, r *http.Request, dstPath string) {
	srcPath := r.Header.Get("X-Dat9-Hardlink-Source")
	if srcPath == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "hardlink_missing_source_header", "dst_path", dstPath)...)
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Hardlink-Source header")
		return
	}
	if !authorizeFSPair(w, r, FSOpRead, srcPath, FSOpWrite, dstPath) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "hardlink_missing_scope", "dst_path", dstPath)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.HardlinkFileCtx(r.Context(), srcPath, dstPath); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "hardlink_not_found", "src_path", srcPath, "dst_path", dstPath)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrPathConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "hardlink_conflict", "src_path", srcPath, "dst_path", dstPath)...)
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, backend.ErrInvalidHardlinkTarget) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "hardlink_invalid_target", "src_path", srcPath, "dst_path", dstPath, "error", err)...)
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "hardlink_failed", "src_path", srcPath, "dst_path", dstPath, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "hardlink_ok", "src_path", srcPath, "dst_path", dstPath)...)
	s.publishEvent(r, srcPath, "hardlink")
	s.publishEvent(r, dstPath, "hardlink")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request, newPath string) {
	// Rename semantics: src disappears (= delete), dst is the new location
	// (= write). Subtle but important — a scoped token with read+write on
	// the source zone but no delete CAN read & copy the file but must NOT
	// rename it away. See banked invariant.
	oldPath := r.Header.Get("X-Dat9-Rename-Source")
	if oldPath == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "rename_missing_source_header", "new_path", newPath)...)
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Rename-Source header")
		return
	}
	if !authorizeFSPair(w, r, FSOpDelete, oldPath, FSOpWrite, newPath) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "rename_missing_scope", "new_path", newPath)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.RenameCtx(r.Context(), oldPath, newPath); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "rename_not_found", "old_path", oldPath, "new_path", newPath)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrPathConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "rename_conflict", "old_path", oldPath, "new_path", newPath)...)
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "rename_failed", "old_path", oldPath, "new_path", newPath, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "rename_ok", "old_path", oldPath, "new_path", newPath)...)
	s.publishEvent(r, newPath, "rename")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "mkdir_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	mode := uint32(0o755)
	if mStr := r.URL.Query().Get("mode"); mStr != "" {
		if m, err := strconv.ParseUint(mStr, 10, 32); err == nil {
			mode = uint32(m)
		}
	}
	if err := b.MkdirCtx(r.Context(), path, mode); err != nil {
		if errors.Is(err, datastore.ErrPathConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "mkdir_conflict", "path", path, "error", err)...)
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "mkdir_failed", "path", path, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "mkdir_ok", "path", path)...)
	s.publishEvent(r, path, "mkdir")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleChmod(w http.ResponseWriter, r *http.Request, path string) {
	// chmod escalates ACLs; scoped tokens MUST NEVER be allowed to chmod,
	// regardless of zone. The dispatcher already denies scoped tokens on
	// POST /v1/fs/*?chmod, so reaching this handler with a scoped token
	// indicates a defense-in-depth gap somewhere upstream — fail closed.
	if scope := ScopeFromContext(r.Context()); scope != nil && scope.IsScoped {
		errJSON(w, http.StatusForbidden, "chmod is owner-only")
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "chmod_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	var req struct {
		Mode uint32 `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "chmod_bad_body", "path", path, "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if err := b.ChmodCtx(r.Context(), path, req.Mode); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "chmod_failed", "path", path, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "chmod_ok", "path", path)...)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "create_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.CreateCtx(r.Context(), path); err != nil {
		if errors.Is(err, datastore.ErrPathConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "create_conflict", "path", path, "error", err)...)
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "create_failed", "path", path, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "create_ok", "path", path)...)
	s.publishEvent(r, path, "create")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": int64(1)})
}

// Worst-case JSON escaping can expand one target byte to six bytes (\u00XX),
// plus fixed wrapper overhead for {"target":...}.
const maxSymlinkBodyBytes = backend.MaxSymlinkTargetBytes*6 + 64

func (s *Server) handleSymlink(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "symlink_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req struct {
		Target string `json:"target"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSymlinkBodyBytes)).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "symlink_body_too_large", "path", path, "max", maxSymlinkBodyBytes)...)
			errJSON(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "symlink_bad_body", "path", path, "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if err := b.CreateSymlinkCtx(r.Context(), path, req.Target); err != nil {
		if errors.Is(err, backend.ErrInvalidSymlinkTarget) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "symlink_invalid_target", "path", path, "error", err)...)
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "symlink_upload_too_large", "path", path, "error", err)...)
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if isBackendQuotaExceeded(err) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "symlink_storage_quota_exceeded", "path", path, "error", err)...)
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrPathConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "symlink_conflict", "path", path, "error", err)...)
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "symlink_failed", "path", path, "error", err)...)
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "symlink_ok", "path", path)...)
	s.publishEvent(r, path, "symlink")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": int64(1)})
}

func (s *Server) handleUploads(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "uploads_missing_scope")...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if r.Method == http.MethodPost {
		s.handleUploadInitiate(w, r, b)
		return
	}
	if r.Method != http.MethodGet {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "uploads_method_not_allowed", "method", r.Method)...)
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "uploads_missing_path")...)
		errJSON(w, http.StatusBadRequest, "missing path parameter")
		return
	}
	// Listing uploads at a path leaks "an upload is in progress here"
	// metadata. Gate behind `list` op on the target path so scoped tokens
	// can't probe arbitrary paths for upload state.
	if !authorizeFS(w, r, FSOpList, path) {
		return
	}
	status := r.URL.Query().Get("status")
	if status == "" {
		status = string(datastore.UploadUploading)
	}
	uploads, err := b.ListUploads(r.Context(), path, datastore.UploadStatus(status))
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "uploads_list_failed", "path", path, "status", status, "error", err)...)
		metricEvent(r.Context(), "metadb_query", "api", "uploads_list", "result", "error")
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	metricEvent(r.Context(), "metadb_query", "api", "uploads_list", "result", "ok")
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "uploads_list_ok", "path", path, "status", status, "count", len(uploads))...)
	type uploadEntry struct {
		UploadID   string `json:"upload_id"`
		Path       string `json:"path"`
		TotalSize  int64  `json:"total_size"`
		PartsTotal int    `json:"parts_total"`
		Status     string `json:"status"`
		CreatedAt  string `json:"created_at"`
		ExpiresAt  string `json:"expires_at"`
	}
	out := make([]uploadEntry, 0, len(uploads))
	for _, u := range uploads {
		out = append(out, uploadEntry{
			UploadID:   u.UploadID,
			Path:       u.TargetPath,
			TotalSize:  u.TotalSize,
			PartsTotal: u.PartsTotal,
			Status:     string(u.Status),
			CreatedAt:  u.CreatedAt.Format(time.RFC3339Nano),
			ExpiresAt:  u.ExpiresAt.Format(time.RFC3339Nano),
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"uploads": out})
}

func (s *Server) handleUploadInitiate(w http.ResponseWriter, r *http.Request, b *backend.Dat9Backend) {
	var req struct {
		Path             string   `json:"path"`
		TotalSize        int64    `json:"total_size"`
		PartChecksums    []string `json:"part_checksums"`
		ExpectedRevision *int64   `json:"expected_revision,omitempty"`
		Description      string   `json:"description,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_body_too_large", "max", 1<<20)...)
			errJSON(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_bad_body", "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		errJSON(w, http.StatusBadRequest, "missing path")
		return
	}
	// Authorize the target path BEFORE running any of the size / checksum /
	// revision / description validation that would mutate state (and BEFORE
	// the InitiateUploadWithChecksumsIfRevision backend call that creates
	// the session). For scoped tokens this is a "write" op on the target.
	if !authorizeFS(w, r, FSOpWrite, req.Path) {
		return
	}
	if req.TotalSize <= 0 {
		errJSON(w, http.StatusBadRequest, "total_size must be positive")
		return
	}
	if req.TotalSize > s.maxUploadBytes {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_too_large", "path", req.Path, "bytes", req.TotalSize, "max", s.maxUploadBytes)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload too large: max %d bytes", s.maxUploadBytes))
		return
	}
	if req.ExpectedRevision != nil && *req.ExpectedRevision < 0 {
		errJSON(w, http.StatusBadRequest, "expected_revision must be >= 0")
		return
	}
	partChecksums, err := validatePartChecksums(req.PartChecksums)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_bad_checksums", "path", req.Path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(partChecksums) == 0 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_missing_checksums", "path", req.Path)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusBadRequest, "missing part_checksums")
		return
	}
	expectedRevision := int64(-1)
	if req.ExpectedRevision != nil {
		expectedRevision = *req.ExpectedRevision
	}
	if utf8.RuneCountInString(req.Description) > backend.MaxDescriptionLen {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("description exceeds %d characters", backend.MaxDescriptionLen))
		return
	}
	plan, err := b.InitiateUploadWithChecksumsIfRevision(r.Context(), req.Path, req.TotalSize, partChecksums, expectedRevision, req.Description)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_too_large_backend", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if isBackendQuotaExceeded(err) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_storage_quota_exceeded", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, backend.ErrPartChecksumCountMismatch) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_checksum_count_mismatch", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_conflict", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_revision_conflict", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			metricEvent(r.Context(), "fs_write", "result", "error")
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_failed", "path", req.Path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_ok", "path", req.Path, "parts", len(plan.Parts))...)
	metricEvent(r.Context(), "fs_write", "result", "accepted")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleUploadAction(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/uploads/")
	parts := strings.SplitN(rest, "/", 2)
	uploadID := parts[0]
	action := ""
	if len(parts) > 1 {
		action = strings.Trim(parts[1], "/")
	}
	if uploadID == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_action_missing_upload_id", "path", r.URL.Path)...)
		errJSON(w, http.StatusBadRequest, "missing upload ID")
		return
	}
	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(action, "complete"):
		s.handleUploadComplete(w, r, uploadID)
	case (r.Method == http.MethodPost || r.Method == http.MethodGet) && strings.HasPrefix(action, "resume"):
		s.handleUploadResume(w, r, uploadID)
	case r.Method == http.MethodDelete && action == "":
		s.handleUploadAbort(w, r, uploadID)
	default:
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_action_unknown", "upload_id", uploadID, "action", action, "method", r.Method)...)
		errJSON(w, http.StatusBadRequest, "unknown upload action")
	}
}

func (s *Server) handleUploadComplete(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_missing_scope", "upload_id", uploadID)...)
		metricEvent(r.Context(), "upload_complete", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Re-authorize the upload's TargetPath against the CURRENT request
	// scope (NOT the scope that initiated the upload). Banked invariant
	// from C2 review: a scoped token's policy can change between initiate
	// and complete (revoke+reissue with narrower zones is the supported
	// policy-change mechanism). Trusting the initiator would let a
	// since-narrowed token finish a write outside its current scope.
	//
	// authorizeUploadSession also handles ErrNotFound→404 and
	// ErrUploadExpired→410 to preserve the existing client-facing error
	// shape.
	upload, err := authorizeUploadSession(r.Context(), w, ScopeFromContext(r.Context()), b, uploadID, FSOpWrite)
	if err != nil {
		metricEvent(r.Context(), "upload_complete", "result", "error")
		return
	}
	// Owner short-circuit returns (nil, nil) from authorizeUploadSession
	// to preserve the exact pre-C2b error shape (no extra GetUpload on
	// owner hot path). For the post-confirm publishEvent below we need
	// the target path either way — fetch here if the helper didn't.
	if upload == nil {
		upload, err = b.Store().GetUpload(r.Context(), uploadID)
		if err != nil {
			if errors.Is(err, datastore.ErrNotFound) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_not_found", "upload_id", uploadID)...)
				metricEvent(r.Context(), "upload_complete", "result", "error")
				errJSON(w, http.StatusNotFound, err.Error())
				return
			}
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_failed", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "upload_complete", "result", "error")
			errJSONInternalStorage(w)
			return
		}
	}
	tags, err := parseUploadCompleteTags(w, r)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_bad_body", "upload_id", uploadID, "error", err)...)
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := b.ConfirmUploadWithTags(r.Context(), uploadID, tags); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_not_found", "upload_id", uploadID)...)
			metricEvent(r.Context(), "upload_complete", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadNotActive) || errors.Is(err, datastore.ErrPathConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_conflict", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "upload_complete", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, backend.ErrUploadClientProtocol) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_client_protocol_error", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "upload_complete", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_revision_conflict", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "upload_complete", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			metricEvent(r.Context(), "upload_complete", "result", "error")
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "upload_complete", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_ok", "upload_id", uploadID)...)
	metricEvent(r.Context(), "upload_complete", "result", "ok")
	recordTenantFileBytes(r.Context(), "upload", "complete", "write", upload.TotalSize)
	s.publishEvent(r, upload.TargetPath, "upload_complete")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleUploadResume(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_missing_scope", "upload_id", uploadID)...)
		metricEvent(r.Context(), "upload_resume", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Re-authorize the session's TargetPath against the CURRENT scope
	// before mutating/returning a resume plan. See banked invariant on
	// handleUploadComplete.
	if _, err := authorizeUploadSession(r.Context(), w, ScopeFromContext(r.Context()), b, uploadID, FSOpWrite); err != nil {
		metricEvent(r.Context(), "upload_resume", "result", "error")
		return
	}
	partChecksums, err := s.parseResumePartChecksums(w, r, uploadID)
	if err != nil {
		return
	}
	plan, err := b.ResumeUploadWithChecksums(r.Context(), uploadID, partChecksums)
	if err != nil {
		if errors.Is(err, backend.ErrPartChecksumCountMismatch) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_checksum_count_mismatch", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "upload_resume", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_not_found", "upload_id", uploadID)...)
			metricEvent(r.Context(), "upload_resume", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadExpired) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_expired", "upload_id", uploadID)...)
			metricEvent(r.Context(), "upload_resume", "result", "error")
			errJSON(w, http.StatusGone, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadNotActive) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_not_active", "upload_id", uploadID)...)
			metricEvent(r.Context(), "upload_resume", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "upload_resume", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_ok", "upload_id", uploadID, "parts", len(plan.Parts))...)
	metricEvent(r.Context(), "upload_resume", "result", "ok")
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) parseResumePartChecksums(w http.ResponseWriter, r *http.Request, uploadID string) ([]string, error) {
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, "application/json") {
		var req struct {
			PartChecksums []string `json:"part_checksums"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_body_too_large", "upload_id", uploadID, "max", 1<<20)...)
				metricEvent(r.Context(), "upload_resume", "result", "error")
				errJSON(w, http.StatusRequestEntityTooLarge, "request body too large")
				return nil, err
			}
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_bad_body", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "upload_resume", "result", "error")
			errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return nil, err
		}
		partChecksums, err := validatePartChecksums(req.PartChecksums)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_bad_checksums", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "upload_resume", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return nil, err
		}
		if len(partChecksums) == 0 {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_missing_checksums", "upload_id", uploadID)...)
			metricEvent(r.Context(), "upload_resume", "result", "error")
			errJSON(w, http.StatusBadRequest, "missing part_checksums")
			return nil, errors.New("missing part_checksums")
		}
		return partChecksums, nil
	}

	partChecksums, err := parsePartChecksumsHeader(r.Header.Get("X-Dat9-Part-Checksums"))
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_bad_checksums", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "upload_resume", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return nil, err
	}
	if len(partChecksums) == 0 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_resume_missing_checksums", "upload_id", uploadID)...)
		metricEvent(r.Context(), "upload_resume", "result", "error")
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Part-Checksums header")
		return nil, errors.New("missing x-dat9-part-checksums header")
	}
	return partChecksums, nil
}

func parseUploadCompleteTags(w http.ResponseWriter, r *http.Request) (map[string]string, error) {
	// Keep v1 complete backward compatible: legacy clients send an empty body.
	// Tags keys must be unique. Official CLI/SDK callers always satisfy this
	// because they construct tags from map[string]string and reject duplicate
	// --tag keys before issuing requests. Callers that send duplicate JSON object
	// keys are providing invalid input; Go's encoding/json silently keeps the
	// last value for duplicate keys, so callers that need deterministic results
	// must deduplicate before sending.
	var req struct {
		Tags map[string]string `json:"tags,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("invalid request body: %w", err)
	}
	if err := tagutil.ValidateMap(req.Tags); err != nil {
		return nil, err
	}
	return req.Tags, nil
}

func parseWriteTagsHeader(header http.Header) (map[string]string, error) {
	raw := header.Values("X-Dat9-Tag")
	if len(raw) == 0 {
		return nil, nil
	}
	tags := make(map[string]string, len(raw))
	for _, entry := range raw {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid X-Dat9-Tag %q (expected key=value)", entry)
		}
		if key == "" {
			return nil, fmt.Errorf("invalid X-Dat9-Tag %q (empty key)", entry)
		}
		if err := tagutil.ValidateEntry(key, value); err != nil {
			return nil, err
		}
		if _, dup := tags[key]; dup {
			return nil, fmt.Errorf("duplicate X-Dat9-Tag key %q", key)
		}
		tags[key] = value
	}
	return tags, nil
}

func parsePartChecksumsHeader(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	return validatePartChecksums(parts)
}

func validatePartChecksums(parts []string) ([]string, error) {
	out := make([]string, 0, len(parts))
	for i, p := range parts {
		v := strings.TrimSpace(p)
		if v == "" {
			return nil, fmt.Errorf("invalid part checksums: empty value at index %d", i)
		}
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("invalid part checksums: invalid base64 at index %d", i)
		}
		if len(decoded) != 4 {
			return nil, fmt.Errorf("invalid part checksums: decoded length %d at index %d, expected 4", len(decoded), i)
		}
		out = append(out, v)
	}
	return out, nil
}

func parseExpectedRevisionHeader(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return -1, nil
	}
	rev, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || rev < 0 {
		return 0, fmt.Errorf("invalid expected revision")
	}
	return rev, nil
}

func (s *Server) handleUploadAbort(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_abort_missing_scope", "upload_id", uploadID)...)
		metricEvent(r.Context(), "upload_abort", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Re-authorize before AbortUpload mutates session state. Abort uses
	// the `delete` op semantically (the in-progress upload is being torn
	// down — its target path effectively becomes a no-op delete on the
	// session, not a write).
	if _, err := authorizeUploadSession(r.Context(), w, ScopeFromContext(r.Context()), b, uploadID, FSOpDelete); err != nil {
		metricEvent(r.Context(), "upload_abort", "result", "error")
		return
	}
	if err := b.AbortUpload(r.Context(), uploadID); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_abort_not_found", "upload_id", uploadID)...)
			metricEvent(r.Context(), "upload_abort", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "upload_abort_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "upload_abort", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "upload_abort_ok", "upload_id", uploadID)...)
	metricEvent(r.Context(), "upload_abort", "result", "ok")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// --- v2 upload handlers (on-demand presign, adaptive part size) ---

func (s *Server) handleV2Uploads(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v2/uploads/")
	parts := strings.SplitN(rest, "/", 2)
	seg0 := parts[0]
	action := ""
	if len(parts) > 1 {
		action = strings.Trim(parts[1], "/")
	}

	switch {
	case seg0 == "initiate" && r.Method == http.MethodPost:
		s.handleV2UploadInitiate(w, r)
	case seg0 != "" && action == "presign" && r.Method == http.MethodPost:
		s.handleV2PresignPart(w, r, seg0)
	case seg0 != "" && action == "presign-batch" && r.Method == http.MethodPost:
		s.handleV2PresignBatch(w, r, seg0)
	case seg0 != "" && action == "complete" && r.Method == http.MethodPost:
		s.handleV2UploadComplete(w, r, seg0)
	case seg0 != "" && action == "abort" && r.Method == http.MethodPost:
		s.handleV2UploadAbort(w, r, seg0)
	default:
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_uploads_unknown_route", "path", r.URL.Path, "method", r.Method)...)
		errJSON(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleV2UploadInitiate(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_missing_scope")...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req struct {
		Path             string `json:"path"`
		TotalSize        int64  `json:"total_size"`
		ExpectedRevision *int64 `json:"expected_revision,omitempty"`
		Description      string `json:"description,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			errJSON(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_bad_body", "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		errJSON(w, http.StatusBadRequest, "missing path")
		return
	}
	// Authorize the target path BEFORE size/revision checks and BEFORE the
	// backend InitiateUploadV2IfRevision creates session state. Same
	// invariant as V1 handleUploadInitiate.
	if !authorizeFS(w, r, FSOpWrite, req.Path) {
		return
	}
	if req.TotalSize <= 0 {
		errJSON(w, http.StatusBadRequest, "total_size must be positive")
		return
	}
	if req.TotalSize > s.maxUploadBytes {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_too_large", "path", req.Path, "bytes", req.TotalSize, "max", s.maxUploadBytes)...)
		errJSON(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload too large: max %d bytes", s.maxUploadBytes))
		return
	}
	if req.ExpectedRevision != nil && *req.ExpectedRevision < 0 {
		errJSON(w, http.StatusBadRequest, "expected_revision must be >= 0")
		return
	}
	expectedRevision := int64(-1)
	if req.ExpectedRevision != nil {
		expectedRevision = *req.ExpectedRevision
	}
	if utf8.RuneCountInString(req.Description) > backend.MaxDescriptionLen {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("description exceeds %d characters", backend.MaxDescriptionLen))
		return
	}
	plan, err := b.InitiateUploadV2IfRevision(r.Context(), req.Path, req.TotalSize, expectedRevision, req.Description)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_too_large_backend", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "v2_upload_initiate", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if isBackendQuotaExceeded(err) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_storage_quota_exceeded", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "v2_upload_initiate", "result", "error")
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_conflict", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "v2_upload_initiate", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_revision_conflict", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "v2_upload_initiate", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			metricEvent(r.Context(), "v2_upload_initiate", "result", "error")
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_failed", "path", req.Path, "error", err)...)
		metricEvent(r.Context(), "v2_upload_initiate", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_ok", "path", req.Path, "part_size", plan.PartSize, "total_parts", plan.TotalParts)...)
	metricEvent(r.Context(), "v2_upload_initiate", "result", "ok")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleV2PresignPart(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_part_missing_scope", "upload_id", uploadID)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Re-authorize session against current scope before issuing a presigned
	// URL — presign IS the write enablement (gives client direct S3 PUT
	// access), so a since-revoked scoped token cannot be allowed to
	// continue here.
	if _, err := authorizeUploadSession(r.Context(), w, ScopeFromContext(r.Context()), b, uploadID, FSOpWrite); err != nil {
		metricEvent(r.Context(), "v2_presign_part", "result", "error")
		return
	}
	var req struct {
		PartNumber int                      `json:"part_number"`
		Checksum   *backend.PresignChecksum `json:"checksum,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_part_bad_body", "upload_id", uploadID, "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.PartNumber < 1 {
		errJSON(w, http.StatusBadRequest, "part_number must be >= 1")
		return
	}
	u, err := b.PresignPart(r.Context(), uploadID, req.PartNumber, req.Checksum)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "upload not found")
			return
		}
		if errors.Is(err, datastore.ErrUploadExpired) {
			metricEvent(r.Context(), "v2_presign_part", "result", "expired")
			errJSON(w, http.StatusGone, "upload expired")
			return
		}
		if errors.Is(err, datastore.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, "upload is not active")
			return
		}
		if errors.Is(err, backend.ErrUploadClientProtocol) || errors.Is(err, backend.ErrUnsupportedAlgorithm) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_part_client_protocol_error", "upload_id", uploadID, "part_number", req.PartNumber, "error", err)...)
			metricEvent(r.Context(), "v2_presign_part", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_part_failed", "upload_id", uploadID, "part_number", req.PartNumber, "error", err)...)
		metricEvent(r.Context(), "v2_presign_part", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_part_ok", "upload_id", uploadID, "part_number", req.PartNumber)...)
	metricEvent(r.Context(), "v2_presign_part", "result", "ok")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(u)
}

func (s *Server) handleV2PresignBatch(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_batch_missing_scope", "upload_id", uploadID)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Same session re-authorize gate as presign-part.
	if _, err := authorizeUploadSession(r.Context(), w, ScopeFromContext(r.Context()), b, uploadID, FSOpWrite); err != nil {
		metricEvent(r.Context(), "v2_presign_batch", "result", "error")
		return
	}
	var req struct {
		Parts []backend.PresignPartEntry `json:"parts"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_batch_bad_body", "upload_id", uploadID, "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if len(req.Parts) == 0 {
		errJSON(w, http.StatusBadRequest, "parts must not be empty")
		return
	}
	urls, err := b.PresignParts(r.Context(), uploadID, req.Parts)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "upload not found")
			return
		}
		if errors.Is(err, datastore.ErrUploadExpired) {
			metricEvent(r.Context(), "v2_presign_batch", "result", "expired")
			errJSON(w, http.StatusGone, "upload expired")
			return
		}
		if errors.Is(err, datastore.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, "upload is not active")
			return
		}
		if errors.Is(err, backend.ErrUploadClientProtocol) || errors.Is(err, backend.ErrUnsupportedAlgorithm) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_batch_client_protocol_error", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "v2_presign_batch", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_batch_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "v2_presign_batch", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_batch_ok", "upload_id", uploadID, "count", len(urls))...)
	metricEvent(r.Context(), "v2_presign_batch", "result", "ok")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"parts": urls})
}

func (s *Server) handleV2UploadComplete(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_missing_scope", "upload_id", uploadID)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Re-authorize session against current scope before confirming the
	// upload — same invariant as V1 complete: a since-narrowed scoped
	// token must not be able to finish writing outside its current zone,
	// even if it initiated the upload while it still had access.
	if _, err := authorizeUploadSession(r.Context(), w, ScopeFromContext(r.Context()), b, uploadID, FSOpWrite); err != nil {
		metricEvent(r.Context(), "v2_upload_complete", "result", "error")
		return
	}
	// Tags keys must be unique. Official CLI/SDK callers always satisfy this
	// because they construct tags from map[string]string and reject duplicate
	// --tag keys before issuing requests. Callers that send duplicate JSON object
	// keys are providing invalid input; Go's encoding/json silently keeps the
	// last value for duplicate keys, so callers that need deterministic results
	// must deduplicate before sending.
	var req struct {
		Parts []backend.CompletePart `json:"parts"`
		Tags  map[string]string      `json:"tags,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_bad_body", "upload_id", uploadID, "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if len(req.Parts) == 0 {
		errJSON(w, http.StatusBadRequest, "parts must not be empty")
		return
	}
	if err := tagutil.ValidateMap(req.Tags); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	// Fetch upload before confirming so we can publish the target path in the event.
	upload, err := b.Store().GetUpload(r.Context(), uploadID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "upload not found")
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "v2_upload_complete", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	if err := b.ConfirmUploadV2WithTags(r.Context(), uploadID, req.Parts, req.Tags); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "upload not found")
			return
		}
		if errors.Is(err, datastore.ErrUploadExpired) {
			metricEvent(r.Context(), "v2_upload_complete", "result", "expired")
			errJSON(w, http.StatusGone, "upload expired")
			return
		}
		if errors.Is(err, datastore.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, "upload is not active")
			return
		}
		if errors.Is(err, datastore.ErrPathConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_conflict", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "v2_upload_complete", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_revision_conflict", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "v2_upload_complete", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, backend.ErrUploadClientProtocol) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_client_protocol_error", "upload_id", uploadID, "error", err)...)
			metricEvent(r.Context(), "v2_upload_complete", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			metricEvent(r.Context(), "v2_upload_complete", "result", "error")
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "v2_upload_complete", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_ok", "upload_id", uploadID)...)
	metricEvent(r.Context(), "v2_upload_complete", "result", "ok")
	recordTenantFileBytes(r.Context(), "upload", "complete", "write", upload.TotalSize)
	s.publishEvent(r, upload.TargetPath, "upload_complete")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "completed"})
}

func (s *Server) handleV2UploadAbort(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_abort_missing_scope", "upload_id", uploadID)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	// Re-authorize before tearing down. Same `delete` op semantics as
	// V1 handleUploadAbort.
	if _, err := authorizeUploadSession(r.Context(), w, ScopeFromContext(r.Context()), b, uploadID, FSOpDelete); err != nil {
		metricEvent(r.Context(), "v2_upload_abort", "result", "error")
		return
	}
	if err := b.AbortUploadV2(r.Context(), uploadID); err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_abort_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "v2_upload_abort", "result", "error")
		errJSONInternalStorage(w)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_abort_ok", "upload_id", uploadID)...)
	metricEvent(r.Context(), "v2_upload_abort", "result", "ok")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleProvision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_method_not_allowed", "method", r.Method)...)
		metricEvent(r.Context(), "tenant_provision", "result", "error")
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.localTenantShimEnabled() {
		s.handleLocalTenantProvision(w, r)
		return
	}
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_not_enabled")...)
		metricEvent(r.Context(), "tenant_provision", "result", "error")
		errJSON(w, http.StatusNotFound, "provisioning not enabled")
		return
	}
	if s.provisioner == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provisioner_not_configured")...)
		metricEvent(r.Context(), "tenant_provision", "result", "error")
		errJSON(w, http.StatusNotFound, "provisioner not configured")
		return
	}
	provider := s.provisioner.ProviderType()
	provider, err := tenant.NormalizeProvider(provider)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_provider_invalid", "provider", provider, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	var credentialReq *tenant.CredentialProvisionRequest
	var quotaReq *quotaRequest
	if provider == tenant.ProviderTiDBCloudNative {
		decoded, err := decodeCredentialProvisionRequest(w, r)
		if err != nil {
			if !errors.Is(err, tenant.ErrCredentialsRequired) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_invalid_request", "provider", provider, "error", err)...)
				metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
				errJSON(w, http.StatusBadRequest, err.Error())
				return
			}
			if decoded != nil && decoded.Quota != nil {
				errJSON(w, http.StatusBadRequest, "TiDBCloud Mode requires public_key and private_key when quota settings are provided")
				return
			}
			defaultReq := resolveDefaultCredentials(s.provisioner)
			if defaultReq == nil {
				errJSON(w, http.StatusBadRequest, tenant.ErrCredentialsRequired.Error())
				return
			}
			if decoded == nil {
				decoded = &credentialProvisionRequest{}
			}
			decoded.Credential = defaultReq
		}
		if validator, ok := s.provisioner.(credentialProvisionRequestValidator); ok {
			if err := validator.ValidateCredentialProvisionRequest(*decoded.Credential); err != nil {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_invalid_request", "provider", provider, "error", err)...)
				metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
				errJSON(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		credentialReq = decoded.Credential
		quotaReq = decoded.Quota
		if quotaReq != nil {
			if err := s.validateQuotaSetRequest(*quotaReq); err != nil {
				metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
				errJSON(w, http.StatusBadRequest, err.Error())
				return
			}
		}
	} else {
		if err := rejectCredentialProvisionBody(r); err != nil {
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_credential_rejected", "provider", provider, "error", err)...)
			metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if provider == tenant.ProviderTiDBCloudNative && credentialReq != nil {
		poolClaimStarted := time.Now()
		logger.Info(r.Context(), "server_event", eventFields(r.Context(), "provision_tenant_pool_claim_started", "provider", provider, "quota_requested", quotaReq != nil)...)
		if res, pool, claimed, err := s.claimAdminTenantFromPool(r.Context(), *credentialReq, quotaReq); err != nil {
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_tenant_pool_claim_failed", "provider", provider, "duration_ms", durationMillis(poolClaimStarted), "error", err)...)
			metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", provisionTenantPoolClaimMetricResult(err))
			errJSON(w, http.StatusBadGateway, "claim tenant pool tenant failed")
			return
		} else if claimed {
			logger.Info(r.Context(), "server_event", eventFields(r.Context(), "provision_tenant_pool_claim_accepted", "tenant_id", res.TenantID, "provider", res.Provider, "pool_id", pool.PoolID, "organization_id", res.OrganizationID, "duration_ms", durationMillis(poolClaimStarted), "status", res.Status)...)
			setRequestMetricTenant(r.Context(), res.TenantID, res.APIKeyID, res.Provider, classifyTenantRequest(r))
			if res.Status == meta.TenantProvisioning {
				s.startProvisionedTenantSchemaInit(r.Context(), res)
			}
			s.replenishTenantPoolAsync(r.Context(), pool, *credentialReq)
			if quotaReq != nil && quotaReq.TiDBCloudSpendingLimit != nil {
				metricEvent(r.Context(), "tenant_provision", "provider", provider, "quota", "create_time_spending_limit")
			}
			metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "accepted")
			writeProvisionTenantAccepted(w, res)
			logger.Info(r.Context(), "server_event", eventFields(r.Context(), "provision_tenant_pool_create_accepted", "tenant_id", res.TenantID, "provider", res.Provider, "pool_id", pool.PoolID, "organization_id", res.OrganizationID, "duration_ms", durationMillis(poolClaimStarted))...)
			return
		}
		logger.Info(r.Context(), "server_event", eventFields(r.Context(), "provision_tenant_pool_claim_missed", "provider", provider, "duration_ms", durationMillis(poolClaimStarted))...)
	}
	res, err := s.provisionTenant(r.Context(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: credentialReq,
		Quota:                 quotaReq,
	})
	if err != nil {
		var pe *provisionTenantError
		if errors.As(err, &pe) {
			errJSON(w, pe.status, pe.message)
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to provision tenant")
		return
	}
	setRequestMetricTenant(r.Context(), res.TenantID, res.APIKeyID, res.Provider, classifyTenantRequest(r))
	s.startProvisionedTenantSchemaInit(r.Context(), res)
	writeProvisionTenantAccepted(w, res)
}

func provisionTenantPoolClaimMetricResult(err error) string {
	if errors.Is(err, tenant.ErrQuotaPermissionDenied) || errors.Is(err, tenant.ErrQuotaBackendNotFound) {
		return "cluster_error"
	}
	return "error"
}

func writeProvisionTenantAccepted(w http.ResponseWriter, res *provisionTenantResult) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := map[string]string{
		"tenant_id": res.TenantID,
		"api_key":   res.APIKey,
		"status":    string(res.Status),
	}
	if res.Provider == tenant.ProviderTiDBCloudNative {
		if res.CloudProvider != "" {
			response["cloud_provider"] = res.CloudProvider
		}
		if res.Region != "" {
			response["region"] = res.Region
		}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func resolveDefaultCredentials(p tenant.Provisioner) *tenant.CredentialProvisionRequest {
	type defaultCredentialProvider interface {
		DefaultCredentials() (tenant.CredentialProvisionRequest, bool)
	}
	if dp, ok := p.(defaultCredentialProvider); ok {
		if req, ok := dp.DefaultCredentials(); ok {
			return &req
		}
	}
	return nil
}

type credentialProvisionRequest struct {
	Credential *tenant.CredentialProvisionRequest
	Quota      *quotaRequest
}

func decodeCredentialProvisionRequest(w http.ResponseWriter, r *http.Request) (*credentialProvisionRequest, error) {
	var req struct {
		PublicKey  string `json:"public_key"`
		PrivateKey string `json:"private_key"`
		quotaFields
	}
	if r.Body != nil {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCredentialProvisionBodyBytes))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("invalid JSON body: %w", err)
		}
		var extra struct{}
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("invalid JSON body: trailing data")
		}
	}
	out := &credentialProvisionRequest{}
	if req.anySet() {
		out.Quota = &quotaRequest{
			quotaFields: req.quotaFields,
		}
	}
	cred := tenant.CredentialProvisionRequest{
		PublicKey:  strings.TrimSpace(req.PublicKey),
		PrivateKey: strings.TrimSpace(req.PrivateKey),
	}
	if cred.PublicKey == "" && cred.PrivateKey == "" {
		return out, tenant.ErrCredentialsRequired
	}
	if cred.PublicKey == "" || cred.PrivateKey == "" {
		return nil, tenant.ErrPartialCredentials
	}
	out.Credential = &cred
	return out, nil
}

func decodeCredentialRequest(w http.ResponseWriter, r *http.Request, raw any, build func() tenant.CredentialProvisionRequest) (*tenant.CredentialProvisionRequest, error) {
	if r.Body != nil {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCredentialProvisionBodyBytes))
		dec.DisallowUnknownFields()
		if err := dec.Decode(raw); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("invalid JSON body: %w", err)
		}
		var extra struct{}
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("invalid JSON body: trailing data")
		}
	}
	out := build()
	if out.PublicKey == "" && out.PrivateKey == "" {
		return nil, tenant.ErrCredentialsRequired
	}
	if out.PublicKey == "" || out.PrivateKey == "" {
		return nil, tenant.ErrPartialCredentials
	}
	return &out, nil
}

func rejectCredentialProvisionBody(r *http.Request) error {
	if r.Body == nil {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCredentialProvisionBodyBytes))
	if err != nil {
		return nil
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	if len(body) == 0 {
		return nil
	}
	if int64(len(body)) >= maxCredentialProvisionBodyBytes {
		return fmt.Errorf("request body too large")
	}
	var raw struct {
		PublicKey  string `json:"public_key"`
		PrivateKey string `json:"private_key"`
		quotaFields
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if strings.TrimSpace(raw.PublicKey) != "" {
		return fmt.Errorf("tidbcloud public key is not supported for this provider (only tidb_cloud_native)")
	}
	if strings.TrimSpace(raw.PrivateKey) != "" {
		return fmt.Errorf("tidbcloud private key is not supported for this provider (only tidb_cloud_native)")
	}
	if raw.anySet() {
		return fmt.Errorf("quota settings are not supported for this provider (only tidb_cloud_native)")
	}
	return nil
}

type apiKeyIssueSource struct {
	Provider     string
	SubjectKey   string
	MetadataJSON []byte
}

type provisionTenantOptions struct {
	KeyName               string
	TokenVersion          int
	APIKeySource          apiKeyIssueSource
	CredentialProvisioner *tenant.CredentialProvisionRequest
	Quota                 *quotaRequest
}

type provisionTenantResult struct {
	TenantID       string
	APIKey         string
	APIKeyID       string
	Status         meta.TenantStatus
	Provider       string
	TenantDSN      string
	CloudProvider  string
	Region         string
	OrganizationID string
}

type provisionTenantError struct {
	status  int
	message string
	err     error
}

func (e *provisionTenantError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return e.message
}

func (e *provisionTenantError) Unwrap() error { return e.err }

func newProvisionTenantError(status int, message string, err error) *provisionTenantError {
	return &provisionTenantError{status: status, message: message, err: err}
}

func durationMillis(started time.Time) float64 {
	return float64(time.Since(started).Microseconds()) / 1000
}

func logProvisionStage(ctx context.Context, event, tenantID, provider string, started time.Time, kv ...any) {
	fields := []any{
		"tenant_id", tenantID,
		"provider", provider,
		"duration_ms", durationMillis(started),
	}
	fields = append(fields, kv...)
	logger.Info(ctx, "server_event", eventFields(ctx, event, fields...)...)
}

func defaultTiDBAutoEmbeddingConfig(cfg tenantschema.TiDBAutoEmbeddingConfig) tenantschema.TiDBAutoEmbeddingConfig {
	if cfg.Model == "" && cfg.Dimensions == 0 {
		return tenantschema.CurrentTiDBAutoEmbeddingConfig()
	}
	return cfg
}

func (s *Server) defaultAutoEmbeddingProfileForTenant(ctx context.Context, tenantID, provider string, now time.Time) (*meta.TenantAutoEmbeddingProfile, error) {
	if !tenant.UsesTiDBAutoEmbedding(provider) {
		return nil, nil
	}
	schemaProfile, err := tenantschema.TiDBAutoEmbeddingProfileFromConfig(s.tidbAutoEmbedding.config)
	if err != nil {
		return nil, err
	}
	if !s.disableDBAutoEmbed {
		if err := tenantschema.ValidateTiDBAutoEmbeddingProviderConfig(tenantschema.TiDBAutoEmbeddingProviderConfig{
			Model:   schemaProfile.Model,
			APIKey:  s.tidbAutoEmbedding.apiKey,
			APIBase: s.tidbAutoEmbedding.apiBase,
		}); err != nil {
			return nil, err
		}
	}
	var apiKeyCipher []byte
	if s.tidbAutoEmbedding.apiKey != "" {
		cipher, err := s.pool.Encrypt(ctx, []byte(s.tidbAutoEmbedding.apiKey))
		if err != nil {
			return nil, fmt.Errorf("encrypt tenant auto-embedding api key: %w", err)
		}
		apiKeyCipher = cipher
	}
	return &meta.TenantAutoEmbeddingProfile{
		TenantID:     tenantID,
		Model:        schemaProfile.Model,
		Dimensions:   schemaProfile.Dimensions,
		OptionsJSON:  schemaProfile.OptionsJSON,
		APIBase:      s.tidbAutoEmbedding.apiBase,
		APIKeyCipher: apiKeyCipher,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

func (s *Server) copyAutoEmbeddingProfileForFork(ctx context.Context, sourceTenantID, forkTenantID, provider string, now time.Time) error {
	if !tenant.UsesTiDBAutoEmbedding(provider) {
		return nil
	}
	sourceProfile, err := s.meta.EnsureTenantAutoEmbeddingProfile(ctx, sourceTenantID)
	if err != nil {
		return err
	}
	return s.meta.UpsertTenantAutoEmbeddingProfile(ctx, &meta.TenantAutoEmbeddingProfile{
		TenantID:     forkTenantID,
		Model:        sourceProfile.Model,
		Dimensions:   sourceProfile.Dimensions,
		OptionsJSON:  sourceProfile.OptionsJSON,
		APIBase:      sourceProfile.APIBase,
		APIKeyCipher: append([]byte(nil), sourceProfile.APIKeyCipher...),
		CreatedAt:    now,
		UpdatedAt:    now,
	})
}

func (s *Server) applyAutoEmbeddingProviderConfig(ctx context.Context, tenantID, tenantDSN string, profile tenantschema.TiDBAutoEmbeddingProfile) error {
	if s.meta == nil || s.pool == nil {
		return nil
	}
	metaProfile, err := s.meta.EnsureTenantAutoEmbeddingProfile(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("load auto-embedding profile: %w", err)
	}
	if len(metaProfile.APIKeyCipher) == 0 {
		return nil
	}
	apiKey, err := s.pool.Decrypt(ctx, metaProfile.APIKeyCipher)
	if err != nil {
		return fmt.Errorf("decrypt auto-embedding api key: %w", err)
	}
	db, err := sql.Open("mysql", tenantDSN)
	if err != nil {
		return fmt.Errorf("open tenant database for embedding provider config: %w", err)
	}
	defer func() { _ = db.Close() }()
	return tenantschema.ApplyTiDBAutoEmbeddingProviderConfig(ctx, db, tenantschema.TiDBAutoEmbeddingProviderConfig{
		Model:   profile.Model,
		APIKey:  string(apiKey),
		APIBase: metaProfile.APIBase,
	})
}

func (s *Server) autoEmbeddingProfileForTenant(ctx context.Context, tenantID string) (tenantschema.TiDBAutoEmbeddingProfile, error) {
	if s.meta == nil {
		return tenantschema.TiDBAutoEmbeddingProfileFromConfig(s.tidbAutoEmbedding.config)
	}
	profile, err := s.meta.EnsureTenantAutoEmbeddingProfile(ctx, tenantID)
	if err != nil {
		return tenantschema.TiDBAutoEmbeddingProfile{}, err
	}
	return tenantschema.TiDBAutoEmbeddingProfile{
		Model:       profile.Model,
		Dimensions:  profile.Dimensions,
		OptionsJSON: profile.OptionsJSON,
	}, nil
}

func (s *Server) schemaInitForTenant(tenantID, provider string, fallback func(context.Context, string) error) func(context.Context, string) error {
	if !tenant.UsesTiDBAutoEmbedding(provider) {
		return fallback
	}
	provisioner := s.provisionerForTenantProvider(provider)
	if provisioner == nil {
		return fallback
	}
	profileAware, ok := provisioner.(autoEmbeddingSchemaProvisioner)
	if !ok {
		return fallback
	}
	return func(ctx context.Context, dsn string) error {
		if ensurer, ok := provisioner.(tenantDatabaseEnsurer); ok {
			if err := ensurer.EnsureDatabase(ctx, dsn); err != nil {
				return fmt.Errorf("ensure tenant database: %w", err)
			}
		}
		profile, err := s.autoEmbeddingProfileForTenant(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("resolve tenant auto-embedding profile: %w", err)
		}
		if !s.disableDBAutoEmbed {
			if err := s.applyAutoEmbeddingProviderConfig(ctx, tenantID, dsn, profile); err != nil {
				return fmt.Errorf("apply tenant auto-embedding provider config: %w", err)
			}
		}
		return profileAware.InitSchemaForAutoEmbeddingProfile(ctx, dsn, profile)
	}
}

func (s *Server) provisionTenant(ctx context.Context, opts provisionTenantOptions) (*provisionTenantResult, error) {
	rawProvider := s.provisioner.ProviderType()
	provider, err := tenant.NormalizeProvider(rawProvider)
	if err != nil {
		logger.Warn(ctx, "server_event", eventFields(ctx, "provision_provider_invalid", "provider", rawProvider, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", rawProvider, "result", "error")
		return nil, newProvisionTenantError(http.StatusBadRequest, err.Error(), err)
	}
	if provider == tenant.ProviderTiDBCloudNative && opts.CredentialProvisioner == nil {
		if defaultReq := resolveDefaultCredentials(s.provisioner); defaultReq == nil {
			return nil, newProvisionTenantError(http.StatusBadRequest, "public_key and private_key are required", fmt.Errorf("public_key and private_key are required"))
		} else {
			opts.CredentialProvisioner = defaultReq
		}
	}
	tenantID := token.NewID()
	provisionStarted := time.Now()
	logger.Info(ctx, "server_event", eventFields(ctx, "provision_requested", "tenant_id", tenantID, "provider", provider)...)
	setRequestMetricTenant(ctx, tenantID, "", provider, tenantRequestClass{surface: "provision", action: "post"})

	keyName := strings.TrimSpace(opts.KeyName)
	if keyName == "" {
		keyName = "default"
	}
	now := time.Now().UTC()
	stageStarted := time.Now()
	autoProfile, err := s.defaultAutoEmbeddingProfileForTenant(ctx, tenantID, provider, now)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_auto_embedding_profile_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to build tenant auto-embedding profile", err)
	}
	logProvisionStage(ctx, "provision_auto_embedding_profile_built", tenantID, provider, stageStarted, "enabled", autoProfile != nil)
	stageStarted = time.Now()
	if err := s.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         provider,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_insert_tenant_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		metricEvent(ctx, "metadb_query", "api", "insert_tenant", "result", "error")
		return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to persist tenant", err)
	}
	metricEvent(ctx, "metadb_query", "api", "insert_tenant", "result", "ok")
	logProvisionStage(ctx, "provision_tenant_inserted", tenantID, provider, stageStarted, "status", meta.TenantPending)

	if autoProfile != nil {
		stageStarted = time.Now()
		if err := s.meta.UpsertTenantAutoEmbeddingProfile(ctx, autoProfile); err != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "provision_insert_auto_embedding_profile_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
			if uerr := s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed); uerr != nil {
				logger.Error(ctx, "server_event", eventFields(ctx, "provision_mark_failed_update_error", "tenant_id", tenantID, "provider", provider, "error", uerr)...)
			}
			return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to persist tenant auto-embedding profile", err)
		}
		logProvisionStage(ctx, "provision_auto_embedding_profile_inserted", tenantID, provider, stageStarted)
	}

	var cluster *tenant.ClusterInfo
	var provisionCloudCfg *tenant.QuotaCloudConfig
	stageStarted = time.Now()
	provisionMode := "default"
	if provider == tenant.ProviderTiDBCloudNative {
		if opts.Quota != nil {
			provisionMode = "tidb_cloud_native_credentials_quota"
			quotaReq := *opts.Quota
			quotaReq.TenantID = tenantID
			if err := s.validateQuotaSetRequest(quotaReq); err != nil {
				_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
				return nil, newProvisionTenantError(http.StatusBadRequest, err.Error(), err)
			}
			if opts.CredentialProvisioner == nil {
				_ = s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed)
				return nil, newProvisionTenantError(http.StatusBadRequest, tenant.ErrCredentialsRequired.Error(), tenant.ErrCredentialsRequired)
			}
			if quotaProvisioner, ok := s.provisioner.(tenant.CredentialQuotaProvisioner); ok {
				logProvisionStage(ctx, "provision_cluster_provision_started", tenantID, provider, stageStarted, "mode", provisionMode)
				cluster, provisionCloudCfg, err = quotaProvisioner.ProvisionWithCredentialsAndQuota(ctx, tenantID, *opts.CredentialProvisioner, tenant.QuotaUpdateOptions{
					TiDBCloudSpendingLimitMonthly: quotaReq.TiDBCloudSpendingLimit,
				})
			} else {
				err = fmt.Errorf("provisioner does not support create-time quota")
				logger.Error(ctx, "server_event", eventFields(ctx, "provision_create_time_quota_not_supported", "tenant_id", tenantID, "provider", provider, "error", err)...)
				metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
				if uerr := s.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed); uerr != nil {
					logger.Error(ctx, "server_event", eventFields(ctx, "provision_mark_failed_update_error", "tenant_id", tenantID, "provider", provider, "error", uerr)...)
				}
				return nil, newProvisionTenantError(http.StatusInternalServerError, "provisioner does not support create-time quota", err)
			}
		} else if credentialProvisioner, ok := s.provisioner.(tenant.CredentialProvisioner); ok {
			provisionMode = "tidb_cloud_native_credentials"
			logProvisionStage(ctx, "provision_cluster_provision_started", tenantID, provider, stageStarted, "mode", provisionMode)
			cluster, err = credentialProvisioner.ProvisionWithCredentials(ctx, tenantID, *opts.CredentialProvisioner)
		} else {
			err = fmt.Errorf("provisioner does not support request credentials")
		}
	} else {
		provisionMode = "provisioner_default"
		logProvisionStage(ctx, "provision_cluster_provision_started", tenantID, provider, stageStarted, "mode", provisionMode)
		cluster, err = s.provisioner.Provision(ctx, tenantID)
	}
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_cluster_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "cluster_error")
		s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "cluster_provision_error")
		msg := fmt.Sprintf("provision tenant cluster failed: %v", err)
		if strings.Contains(strings.ToLower(err.Error()), "free cluster") {
			msg = "TiDB Cloud free cluster limit reached. Set a monthly Spending Limit to continue. See https://www.pingcap.com/tidb-cloud-starter-pricing-details/#cost-and-limitations"
		} else if strings.Contains(strings.ToLower(err.Error()), "credits") || strings.Contains(strings.ToLower(err.Error()), "payment") {
			msg = "TiDB Cloud payment required. Add a payment method at https://tidbcloud.com/org-settings/billing/payments"
		} else if strings.Contains(strings.ToLower(err.Error()), "instance capacity limit") || strings.Contains(strings.ToLower(err.Error()), "capacity limit") {
			msg = "TiDB Cloud cluster capacity limit reached"
		} else if strings.Contains(strings.ToLower(err.Error()), "status 401") || strings.Contains(strings.ToLower(err.Error()), "invalid tidb cloud") || strings.Contains(strings.ToLower(err.Error()), "unauthorized") {
			msg = "invalid TiDB Cloud API key"
		} else if strings.Contains(strings.ToLower(err.Error()), "cluster limit") {
			msg = "TiDB Cloud cluster limit reached (100 clusters per organization). Contact PingCAP support for assistance."
		}
		return nil, newProvisionTenantError(http.StatusBadGateway, msg, err)
	}
	logProvisionStage(ctx, "provision_cluster_provisioned", tenantID, provider, stageStarted, "mode", provisionMode, "cluster_id", cluster.ClusterID, "organization_id", cluster.OrganizationID)
	cluster.Provider = provider
	if provider == tenant.ProviderTiDBCloudNative {
		stageStarted = time.Now()
		if strings.TrimSpace(cluster.OrganizationID) == "" {
			err := fmt.Errorf("tidbcloud organization label is missing")
			logger.Error(ctx, "server_event", eventFields(ctx, "provision_tidbcloud_org_binding_missing", "tenant_id", tenantID, "provider", provider, "cluster_id", cluster.ClusterID, "error", err)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
			s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "org_binding_missing")
			return nil, newProvisionTenantError(http.StatusBadGateway, "failed to read tidbcloud organization binding", err)
		}
		if err := s.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
			TenantID:       tenantID,
			OrganizationID: cluster.OrganizationID,
			ClusterID:      cluster.ClusterID,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "provision_tidbcloud_org_binding_failed", "tenant_id", tenantID, "provider", provider, "organization_id", cluster.OrganizationID, "cluster_id", cluster.ClusterID, "error", err)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
			s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "org_binding_error")
			return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to persist tidbcloud organization binding", err)
		}
		logProvisionStage(ctx, "provision_tidbcloud_org_binding_persisted", tenantID, provider, stageStarted, "cluster_id", cluster.ClusterID, "organization_id", cluster.OrganizationID)
	}

	stageStarted = time.Now()
	cipherPass, err := s.pool.Encrypt(ctx, []byte(cluster.Password))
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_encrypt_db_password_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "password_encrypt_error")
		return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to encrypt db password", err)
	}
	logProvisionStage(ctx, "provision_db_password_encrypted", tenantID, provider, stageStarted)
	dbtls := true
	if provider == tenant.ProviderTiDBCloudNative {
		v := strings.TrimSpace(strings.ToLower(os.Getenv("DRIVE9_TIDBCLOUD_NATIVE_USE_PRIVATE_ENDPOINT")))
		if v == "1" || v == "true" || v == "yes" {
			dbtls = false
		}
	}
	stageStarted = time.Now()
	if err := s.meta.UpdateTenantConnection(ctx, tenantID, &meta.Tenant{
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           cluster.Username,
		DBPasswordCipher: cipherPass,
		DBName:           cluster.DBName,
		DBTLS:            dbtls,
		Provider:         provider,
		ClusterID:        cluster.ClusterID,
		ClaimURL:         cluster.ClaimURL,
		ClaimExpiresAt:   cluster.ClaimExpiresAt,
	}); err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_update_tenant_connection_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "connection_persist_error")
		return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to persist tenant connection", err)
	}
	logProvisionStage(ctx, "provision_tenant_connection_persisted", tenantID, provider, stageStarted, "cluster_id", cluster.ClusterID, "db_tls", dbtls)
	stageStarted = time.Now()
	if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantProvisioning); err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_update_tenant_status_failed", "tenant_id", tenantID, "provider", provider, "status", meta.TenantProvisioning, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "status_update_error")
		return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to update tenant status", err)
	}
	logProvisionStage(ctx, "provision_tenant_status_updated", tenantID, provider, stageStarted, "status", meta.TenantProvisioning)

	if opts.Quota != nil {
		stageStarted = time.Now()
		quotaReq := *opts.Quota
		quotaReq.TenantID = tenantID
		if err := s.applyQuotaLocalConfig(ctx, tenantID, quotaReq); err != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "provision_quota_update_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
			metricEvent(ctx, "tenant_provision", "provider", provider, "result", "quota_error")
			s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "quota_error")
			return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to set tenant quota", err)
		}
		// The TiDB Cloud spending limit is applied in the create-cluster request and
		// remains cloud-side; list/get quota reads it back from TiDB Cloud.
		if provisionCloudCfg != nil && provisionCloudCfg.TiDBCloudSpendingLimitMonthly != nil {
			metricEvent(ctx, "tenant_provision", "provider", provider, "quota", "create_time_spending_limit")
		}
		logProvisionStage(ctx, "provision_quota_local_config_applied", tenantID, provider, stageStarted, "create_time_spending_limit", provisionCloudCfg != nil && provisionCloudCfg.TiDBCloudSpendingLimitMonthly != nil)
	}

	stageStarted = time.Now()
	apiToken, apiKeyID, err := s.issueOwnerAPIKey(ctx, tenantID, keyName, opts.TokenVersion, opts.APIKeySource)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "provision_insert_api_key_failed", "tenant_id", tenantID, "api_key_id", apiKeyID, "error", err)...)
		metricEvent(ctx, "tenant_provision", "provider", provider, "result", "error")
		metricEvent(ctx, "metadb_query", "api", "insert_api_key", "result", "error")
		s.cleanupProvisionedClusterAfterProvisionFailure(ctx, tenantID, provider, cluster, opts.CredentialProvisioner, "api_key_persist_error")
		return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to persist api key", err)
	}
	logProvisionStage(ctx, "provision_api_key_issued", tenantID, provider, stageStarted, "api_key_id", apiKeyID)

	logger.Info(ctx, "server_event", eventFields(ctx, "provision_accepted", "tenant_id", tenantID, "provider", provider, "duration_ms", durationMillis(provisionStarted))...)
	metricEvent(ctx, "tenant_provision", "provider", provider, "result", "accepted")

	cloudProvider, region := "", ""
	if provider == tenant.ProviderTiDBCloudNative {
		cloudProvider, region = provisioningCloudRegion(s.provisioner)
	}
	return &provisionTenantResult{
		TenantID:       tenantID,
		APIKey:         apiToken,
		APIKeyID:       apiKeyID,
		Status:         meta.TenantProvisioning,
		Provider:       provider,
		TenantDSN:      tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, cluster.DBName, dbtls, provider),
		CloudProvider:  cloudProvider,
		Region:         region,
		OrganizationID: strings.TrimSpace(cluster.OrganizationID),
	}, nil
}

func (s *Server) cleanupProvisionedClusterAfterProvisionFailure(ctx context.Context, tenantID, provider string, cluster *tenant.ClusterInfo, cred *tenant.CredentialProvisionRequest, reason string) {
	cleanupCtx := backgroundWithTrace(ctx)
	if uerr := s.meta.UpdateTenantStatus(cleanupCtx, tenantID, meta.TenantFailed); uerr != nil {
		logger.Error(cleanupCtx, "server_event", eventFields(cleanupCtx, "provision_mark_failed_update_error", "tenant_id", tenantID, "provider", provider, "error", uerr)...)
	}
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return
	}
	t := &meta.Tenant{
		ID:        tenantID,
		Provider:  provider,
		ClusterID: cluster.ClusterID,
		DBHost:    cluster.Host,
		DBPort:    cluster.Port,
		DBUser:    cluster.Username,
		DBName:    cluster.DBName,
	}
	var req tenant.CredentialProvisionRequest
	if provider == tenant.ProviderTiDBCloudNative {
		if cred == nil {
			logger.Warn(cleanupCtx, "server_event", eventFields(cleanupCtx, "provision_cluster_cleanup_skipped_missing_credentials", "tenant_id", tenantID, "provider", provider, "reason", reason, "cluster_id", cluster.ClusterID)...)
			return
		}
		req = *cred
	}
	s.startServerWorker(cleanupCtx, func(workerCtx context.Context) {
		workerCtx, cancel := context.WithTimeout(workerCtx, provisionFailureClusterCleanupTimeout)
		defer cancel()
		if err := s.deprovisionTenantCluster(workerCtx, t, req); err != nil {
			logger.Error(workerCtx, "server_event", eventFields(workerCtx, "provision_cluster_cleanup_failed", "tenant_id", tenantID, "provider", provider, "reason", reason, "cluster_id", cluster.ClusterID, "error", err)...)
			if uerr := s.meta.UpdateTenantClusterReference(workerCtx, tenantID, t); uerr != nil {
				logger.Error(workerCtx, "server_event", eventFields(workerCtx, "provision_cluster_cleanup_reference_persist_failed", "tenant_id", tenantID, "provider", provider, "reason", reason, "cluster_id", cluster.ClusterID, "error", uerr)...)
			}
			return
		}
		if err := s.meta.ClearTenantProvisionMetadata(workerCtx, tenantID); err != nil {
			logger.Error(workerCtx, "server_event", eventFields(workerCtx, "provision_metadata_cleanup_failed", "tenant_id", tenantID, "provider", provider, "reason", reason, "cluster_id", cluster.ClusterID, "error", err)...)
		}
	})
}

func provisioningCloudRegion(provisioner tenant.Provisioner) (string, string) {
	if provisioner == nil {
		return "", ""
	}
	regionProvider, ok := provisioner.(provisioningRegionProvider)
	if !ok {
		return "", ""
	}
	return strings.TrimSpace(regionProvider.ProvisioningCloudProvider()), strings.TrimSpace(regionProvider.ProvisioningRegion())
}

func (s *Server) startProvisionedTenantSchemaInit(ctx context.Context, res *provisionTenantResult) {
	if res == nil || res.TenantID == "" || res.TenantDSN == "" || s.provisioner == nil {
		return
	}
	// Tenant remains in provisioning state until schema initialization succeeds.
	s.startServerWorker(ctx, func(workerCtx context.Context) {
		s.initTenantSchemaAsync(workerCtx, res.TenantID, res.TenantDSN, res.Provider, s.schemaInitForTenant(res.TenantID, res.Provider, s.provisioner.InitSchema))
	})
}

func (s *Server) issueOwnerAPIKey(ctx context.Context, tenantID, keyName string, tokenVersion int, source apiKeyIssueSource) (rawToken, apiKeyID string, err error) {
	if tokenVersion <= 0 {
		tokenVersion, err = newScopedTokenVersion()
		if err != nil {
			return "", "", err
		}
	}
	rawToken, err = token.IssueTokenWithJournalPermissions(s.tokenSecret, tenantID, tokenVersion, time.Time{}, ownerJournalPermissionList())
	if err != nil {
		return "", "", err
	}
	cipherToken, err := s.pool.Encrypt(ctx, []byte(rawToken))
	if err != nil {
		return "", "", err
	}
	now := time.Now().UTC()
	apiKeyID = token.NewID()
	if err := s.meta.InsertAPIKey(ctx, &meta.APIKey{
		ID:                   apiKeyID,
		TenantID:             tenantID,
		KeyName:              keyName,
		JWTCiphertext:        cipherToken,
		JWTHash:              token.HashToken(rawToken),
		TokenVersion:         tokenVersion,
		Status:               meta.APIKeyActive,
		ScopeKind:            meta.APIKeyScopeKindOwner,
		IssuedByProvider:     source.Provider,
		IssuedBySubjectKey:   source.SubjectKey,
		IssuedByMetadataJSON: source.MetadataJSON,
		IssuedAt:             now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}); err != nil {
		return "", "", err
	}
	metricEvent(ctx, "metadb_query", "api", "insert_api_key", "result", "ok")
	return rawToken, apiKeyID, nil
}

// handleLocalTenantProvision serves drive9-server-local's single-tenant
// compatibility path so e2e scripts can obtain one stable API key without
// enabling the multi-tenant provision flow.
func (s *Server) handleLocalTenantProvision(w http.ResponseWriter, r *http.Request) {
	setRequestMetricTenant(r.Context(), "local", "local", "local", classifyTenantRequest(r))
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "provision_requested", "tenant_id", "local", "provider", "local")...)
	metricEvent(r.Context(), "tenant_provision", "provider", "local", "result", "ok")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"tenant_id": "local",
		"api_key":   s.localTenantAPIKey,
		"status":    "provisioning",
	})
}

func (s *Server) initTenantSchemaAsync(ctx context.Context, tenantID, tenantDSN, provider string, schemaInit func(context.Context, string) error) {
	ctx = ensureTrace(ctx)
	ctx = logger.WithContext(ctx, logger.FromContext(ctx).With(
		zap.String("tenant_id", tenantID),
		zap.String("provider", provider),
	))
	logger.Info(ctx, "server_event", eventFields(ctx, "schema_init_started", "tenant_id", tenantID, "provider", provider)...)
	deadline := time.Now().Add(schemaInitRetryWindow)
	backoff := schemaInitInitialBackoff
	attempt := 1
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "schema_init_stopped",
				zap.String("tenant_id", tenantID),
				zap.String("provider", provider),
				zap.Error(ctx.Err()))
			return
		default:
		}
		if !s.tenantSchemaInitStillProvisioning(ctx, tenantID, provider, "before_attempt") {
			return
		}
		err := schemaInit(ctx, tenantDSN)
		if err == nil {
			if !s.tenantSchemaInitStillProvisioning(ctx, tenantID, provider, "before_finalize") {
				return
			}
			err = s.finalizeTenantSchemaInit(ctx, tenantID, tenantDSN, provider)
		}
		if err == nil {
			s.schemaInitErrors.Delete(tenantID)
			logger.Info(ctx, "server_event", eventFields(ctx, "schema_init_ok", "tenant_id", tenantID, "provider", provider, "attempt", attempt)...)
			if s.metrics != nil {
				s.metrics.recordEvent(tenantID, "tenant_schema_init", "provider", provider, "result", "ok")
			}
			updated, err := s.meta.UpdateTenantStatusIf(ctx, tenantID, meta.TenantProvisioning, meta.TenantActive)
			if err != nil {
				logger.Error(ctx, "schema_init_activate_failed", zap.String("tenant_id", tenantID), zap.Error(err))
			} else if !updated {
				logger.Warn(ctx, "schema_init_activate_skipped",
					zap.String("tenant_id", tenantID),
					zap.String("provider", provider),
					zap.String("reason", "status_changed"))
			}
			return
		} else {
			if !s.tenantSchemaInitStillProvisioning(ctx, tenantID, provider, "after_error") {
				return
			}
			s.schemaInitErrors.Store(tenantID, schemaInitStatusErrorMessage(err))
			logger.Error(ctx, "server_event", eventFields(ctx, "schema_init_failed", "tenant_id", tenantID, "provider", provider, "attempt", attempt, "error", err)...)
			if s.metrics != nil {
				s.metrics.recordEvent(tenantID, "tenant_schema_init", "provider", provider, "result", "error")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				if updated, uerr := s.meta.UpdateTenantStatusIf(ctx, tenantID, meta.TenantProvisioning, meta.TenantFailed); uerr != nil {
					logger.Error(ctx, "schema_init_mark_failed_update_error", zap.String("tenant_id", tenantID), zap.Error(uerr))
				} else if !updated {
					logger.Warn(ctx, "schema_init_mark_failed_skipped",
						zap.String("tenant_id", tenantID),
						zap.String("provider", provider),
						zap.String("reason", "status_changed"))
				}
				logger.Error(ctx, "schema_init_retry_exhausted", zap.String("tenant_id", tenantID), zap.Error(err))
				return
			}
			logger.Warn(ctx, "schema_init_attempt_failed",
				zap.String("tenant_id", tenantID),
				zap.String("provider", provider),
				zap.Int("attempt", attempt),
				zap.String("remaining", remaining.Round(time.Second).String()),
				zap.Error(err),
			)
		}
		sleepFor := backoff
		if sleepFor > schemaInitMaxBackoff {
			sleepFor = schemaInitMaxBackoff
		}
		if time.Now().Add(sleepFor).After(deadline) {
			sleepFor = time.Until(deadline)
		}
		if sleepFor > 0 {
			timer := time.NewTimer(sleepFor)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				logger.Info(ctx, "schema_init_stopped",
					zap.String("tenant_id", tenantID),
					zap.String("provider", provider),
					zap.Error(ctx.Err()))
				return
			case <-timer.C:
			}
		}
		backoff *= 2
		attempt++
	}
}

func (s *Server) tenantSchemaInitStillProvisioning(ctx context.Context, tenantID, provider, stage string) bool {
	if s.meta == nil {
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	t, err := s.meta.GetTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			logger.Info(ctx, "server_event", eventFields(ctx, "schema_init_stopped_status_changed",
				"tenant_id", tenantID,
				"provider", provider,
				"stage", stage,
				"status", "missing")...)
			return false
		}
		logger.Warn(ctx, "schema_init_status_lookup_failed",
			zap.String("tenant_id", tenantID),
			zap.String("provider", provider),
			zap.String("stage", stage),
			zap.Error(err))
		return true
	}
	if t.Status != meta.TenantProvisioning {
		logger.Info(ctx, "server_event", eventFields(ctx, "schema_init_stopped_status_changed",
			"tenant_id", tenantID,
			"provider", provider,
			"stage", stage,
			"status", t.Status)...)
		return false
	}
	return true
}

func (s *Server) finalizeTenantSchemaInit(ctx context.Context, tenantID, tenantDSN, provider string) error {
	if provider != tenant.ProviderTiDBCloudNative {
		return nil
	}
	if s.provisioner == nil {
		return fmt.Errorf("server misconfigured: native tenant provider is missing")
	}
	systemUserProvisioner, ok := s.provisioner.(nativeSystemUserProvisioner)
	if !ok {
		return fmt.Errorf("native provisioner does not support system user setup")
	}
	cfg, err := mysql.ParseDSN(tenantDSN)
	if err != nil {
		return fmt.Errorf("parse native tenant DSN for credential finalization: %w", err)
	}
	fromDBUser := strings.TrimSpace(cfg.User)
	if fromDBUser == "" {
		return fmt.Errorf("native tenant DSN has empty username")
	}
	username, password, err := systemUserProvisioner.EnsureSystemUser(ctx, tenantDSN, tenantID)
	if err != nil {
		return fmt.Errorf("ensure native system user: %w", err)
	}
	if username == "" || password == "" {
		return fmt.Errorf("native system user setup returned empty credential")
	}
	cipherPass, err := s.pool.Encrypt(ctx, []byte(password))
	if err != nil {
		return fmt.Errorf("encrypt native system user password: %w", err)
	}
	updated, err := s.meta.UpdateTenantDBCredentialIf(ctx, tenantID, fromDBUser, username, cipherPass)
	if err != nil {
		return fmt.Errorf("persist native system user credential: %w", err)
	}
	if !updated {
		logger.Info(ctx, "native_system_user_credential_update_skipped",
			zap.String("tenant_id", tenantID),
			zap.String("from_db_user", fromDBUser),
			zap.String("db_user", username))
	}
	return nil
}

func errJSON(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func errJSONInvalidRootDentry(w http.ResponseWriter, err error) bool {
	if !errors.Is(err, datastore.ErrInvalidRootDentry) {
		return false
	}
	errJSON(w, http.StatusBadRequest, err.Error())
	return true
}

const internalStorageErrorMessage = "storage backend unavailable; contact support"

func errJSONInternalStorage(w http.ResponseWriter) {
	errJSON(w, http.StatusInternalServerError, internalStorageErrorMessage)
}

func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "sql_method_not_allowed", "method", r.Method)...)
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "sql_missing_scope")...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "sql_bad_json", "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Query == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "sql_empty_query")...)
		errJSON(w, http.StatusBadRequest, "empty query")
		return
	}

	rows, err := b.ExecSQL(r.Context(), req.Query)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "sql_exec_failed", "query_len", len(req.Query), "error", err)...)
		metricEvent(r.Context(), "userdb_query", "api", "sql", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	metricEvent(r.Context(), "userdb_query", "api", "sql", "result", "ok")
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "sql_exec_ok", "query_len", len(req.Query), "rows", len(rows))...)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rows)
}

func (s *Server) handleGrep(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpSearch, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "grep_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	query := r.URL.Query().Get("grep")
	if query == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "grep_empty_query", "path", path)...)
		errJSON(w, http.StatusBadRequest, "empty grep query")
		return
	}
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "grep_invalid_limit", "path", path, "limit", v)...)
			errJSON(w, http.StatusBadRequest, "invalid limit: "+v)
			return
		}
		limit = n
	}
	results, err := b.Grep(r.Context(), query, path, limit)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "grep_failed", "path", path, "query_len", len(query), "limit", limit, "error", err)...)
		metricEvent(r.Context(), "userdb_query", "api", "grep", "result", "error")
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	if layerRef := strings.TrimSpace(r.URL.Query().Get("layer")); layerRef != "" {
		results, err = overlayFSLayerGrep(r.Context(), b, layerRef, query, path, limit, results)
		if err != nil {
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "grep_layer_failed", "path", path, "query_len", len(query), "limit", limit, "layer", layerRef, "error", err)...)
			errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
			return
		}
	}
	metricEvent(r.Context(), "userdb_query", "api", "grep", "result", "ok")
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "grep_ok", "path", path, "query_len", len(query), "limit", limit, "results", len(results))...)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

func (s *Server) handleFind(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpSearch, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "find_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	q := r.URL.Query()
	f := &datastore.FindFilter{PathPrefix: path}
	f.NameGlob = q.Get("name")
	if tag := q.Get("tag"); tag != "" {
		k, v, _ := strings.Cut(tag, "=")
		f.TagKey = k
		f.TagValue = v
	}
	if v := q.Get("newer"); v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "find_invalid_newer", "path", path, "value", v)...)
			errJSON(w, http.StatusBadRequest, "invalid newer date: "+v)
			return
		}
		f.After = &t
	}
	if v := q.Get("older"); v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "find_invalid_older", "path", path, "value", v)...)
			errJSON(w, http.StatusBadRequest, "invalid older date: "+v)
			return
		}
		f.Before = &t
	}
	if v := q.Get("minsize"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "find_invalid_minsize", "path", path, "value", v)...)
			errJSON(w, http.StatusBadRequest, "invalid minsize: "+v)
			return
		}
		f.MinSize = n
	}
	if v := q.Get("maxsize"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "find_invalid_maxsize", "path", path, "value", v)...)
			errJSON(w, http.StatusBadRequest, "invalid maxsize: "+v)
			return
		}
		f.MaxSize = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "find_invalid_limit", "path", path, "value", v)...)
			errJSON(w, http.StatusBadRequest, "invalid limit: "+v)
			return
		}
		f.Limit = n
	}
	results, err := b.Find(r.Context(), f)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "find_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "userdb_query", "api", "find", "result", "error")
		errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
		return
	}
	if layerRef := strings.TrimSpace(q.Get("layer")); layerRef != "" {
		results, err = overlayFSLayerFind(r.Context(), b, layerRef, f, results)
		if err != nil {
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "find_layer_failed", "path", path, "layer", layerRef, "error", err)...)
			errJSON(w, http.StatusInternalServerError, sanitizeClientError(err))
			return
		}
	}
	metricEvent(r.Context(), "userdb_query", "api", "find", "result", "ok")
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "find_ok", "path", path, "results", len(results), "name", f.NameGlob, "tag_key", f.TagKey)...)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

func overlayFSLayerGrep(ctx context.Context, b *backend.Dat9Backend, layerRef, query, pathPrefix string, limit int, base []datastore.SearchResult) ([]datastore.SearchResult, error) {
	layer, entries, err := resolveSearchLayerEntries(ctx, b, layerRef)
	if err != nil {
		return nil, err
	}
	if layer == nil {
		return base, nil
	}
	hiddenExact, hiddenDirs := fsLayerHiddenPaths(entries)
	out := filterLayerHiddenResults(base, hiddenExact, hiddenDirs)
	seen := make(map[string]struct{}, len(out))
	for _, r := range out {
		seen[r.Path] = struct{}{}
	}
	queryFold := strings.ToLower(query)
	for i := range entries {
		entry := &entries[i]
		if _, ok := seen[entry.Path]; ok {
			continue
		}
		if entry.Op != datastore.FSLayerEntryOpUpsert && entry.Op != datastore.FSLayerEntryOpSymlink {
			continue
		}
		if !fsLayerSearchPathMatches(entry.Path, pathPrefix) {
			continue
		}
		matched, err := fsLayerEntryContains(ctx, b, entry, queryFold)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		out = append(out, datastore.SearchResult{
			Path:      entry.Path,
			Name:      pathutil.BaseName(entry.Path),
			SizeBytes: entry.SizeBytes,
		})
		seen[entry.Path] = struct{}{}
		if limit > 0 && len(out) >= limit {
			return out[:limit], nil
		}
	}
	if limit > 0 && len(out) > limit {
		return out[:limit], nil
	}
	return out, nil
}

func fsLayerEntryContains(ctx context.Context, b *backend.Dat9Backend, entry *datastore.FSLayerEntry, queryFold string) (bool, error) {
	if queryFold == "" {
		return true, nil
	}
	if entry.Op == datastore.FSLayerEntryOpSymlink {
		data := []byte(entry.ContentText)
		if len(data) == 0 {
			data = entry.ContentBlob
		}
		return strings.Contains(strings.ToLower(string(data)), queryFold), nil
	}
	rc, err := b.OpenFSLayerEntryData(ctx, entry)
	if err != nil {
		return false, err
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, 64*1024)
	overlap := ""
	overlapBytes := len(queryFold)*4 + 8
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			raw := overlap + string(buf[:n])
			if strings.Contains(strings.ToLower(raw), queryFold) {
				return true, nil
			}
			if len(raw) > overlapBytes {
				overlap = raw[len(raw)-overlapBytes:]
			} else {
				overlap = raw
			}
		}
		if errors.Is(readErr, io.EOF) {
			return false, nil
		}
		if readErr != nil {
			return false, readErr
		}
	}
}

func overlayFSLayerFind(ctx context.Context, b *backend.Dat9Backend, layerRef string, filter *datastore.FindFilter, base []datastore.SearchResult) ([]datastore.SearchResult, error) {
	layer, entries, err := resolveSearchLayerEntries(ctx, b, layerRef)
	if err != nil {
		return nil, err
	}
	if layer == nil {
		return base, nil
	}
	hiddenExact, hiddenDirs := fsLayerHiddenPaths(entries)
	out := filterLayerHiddenResults(base, hiddenExact, hiddenDirs)
	seen := make(map[string]struct{}, len(out))
	for _, r := range out {
		seen[r.Path] = struct{}{}
	}
	for i := range entries {
		entry := &entries[i]
		if entry.Op == datastore.FSLayerEntryOpWhiteout || entry.Op == datastore.FSLayerEntryOpChmod || entry.Op == datastore.FSLayerEntryOpRename {
			continue
		}
		if _, ok := seen[entry.Path]; ok {
			continue
		}
		if !fsLayerFindEntryMatches(entry, filter) {
			continue
		}
		out = append(out, datastore.SearchResult{
			Path:      entry.Path,
			Name:      pathutil.BaseName(entry.Path),
			SizeBytes: entry.SizeBytes,
		})
		seen[entry.Path] = struct{}{}
		if filter != nil && filter.Limit > 0 && len(out) >= filter.Limit {
			return out[:filter.Limit], nil
		}
	}
	if filter != nil && filter.Limit > 0 && len(out) > filter.Limit {
		return out[:filter.Limit], nil
	}
	return out, nil
}

func resolveSearchLayerEntries(ctx context.Context, b *backend.Dat9Backend, layerRef string) (*datastore.FSLayer, []datastore.FSLayerEntry, error) {
	if b == nil || b.Store() == nil {
		return nil, nil, fmt.Errorf("missing backend store")
	}
	layer, err := b.Store().ResolveFSLayerRef(ctx, layerRef)
	if err != nil {
		return nil, nil, err
	}
	switch layer.State {
	case datastore.FSLayerStateActive, datastore.FSLayerStateSealed, datastore.FSLayerStateCommitting:
	default:
		return nil, nil, fmt.Errorf("fs layer %s is %s", layer.LayerID, layer.State)
	}
	entries, err := b.Store().ListFSLayerEntries(ctx, layer.LayerID)
	if err != nil {
		return nil, nil, err
	}
	return layer, entries, nil
}

func fsLayerHiddenPaths(entries []datastore.FSLayerEntry) (map[string]struct{}, []string) {
	exact := make(map[string]struct{})
	var dirs []string
	for i := range entries {
		entry := &entries[i]
		switch entry.Op {
		case datastore.FSLayerEntryOpUpsert, datastore.FSLayerEntryOpSymlink, datastore.FSLayerEntryOpMkdir:
			exact[entry.Path] = struct{}{}
		case datastore.FSLayerEntryOpWhiteout:
			exact[entry.Path] = struct{}{}
			if entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entry.Path, "/") {
				dir := entry.Path
				if !strings.HasSuffix(dir, "/") {
					dir += "/"
				}
				dirs = append(dirs, dir)
			}
		case datastore.FSLayerEntryOpRename:
			exact[entry.Path] = struct{}{}
			target := strings.TrimSpace(entry.ContentText)
			if target == "" && len(entry.ContentBlob) > 0 {
				target = strings.TrimSpace(string(entry.ContentBlob))
			}
			if target != "" {
				exact[target] = struct{}{}
			}
		}
	}
	return exact, dirs
}

func filterLayerHiddenResults(base []datastore.SearchResult, exact map[string]struct{}, dirs []string) []datastore.SearchResult {
	out := make([]datastore.SearchResult, 0, len(base))
	for _, result := range base {
		if _, hidden := exact[result.Path]; hidden {
			continue
		}
		hidden := false
		for _, dir := range dirs {
			if strings.HasPrefix(result.Path, dir) {
				hidden = true
				break
			}
		}
		if !hidden {
			out = append(out, result)
		}
	}
	return out
}

func fsLayerSearchPathMatches(path, prefix string) bool {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || prefix == "/" {
		return strings.HasPrefix(path, "/")
	}
	if strings.HasSuffix(prefix, "/") {
		return path == prefix || strings.HasPrefix(path, prefix)
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func fsLayerFindEntryMatches(entry *datastore.FSLayerEntry, filter *datastore.FindFilter) bool {
	if entry == nil {
		return false
	}
	if filter != nil {
		if !fsLayerSearchPathMatches(entry.Path, filter.PathPrefix) {
			return false
		}
		if filter.TagKey != "" {
			return false
		}
		if filter.NameGlob != "" {
			matched, err := pathpkg.Match(filter.NameGlob, pathutil.BaseName(entry.Path))
			if err != nil || !matched {
				return false
			}
		}
		if filter.After != nil && !entry.UpdatedAt.After(*filter.After) {
			return false
		}
		if filter.Before != nil && !entry.UpdatedAt.Before(*filter.Before) {
			return false
		}
		if filter.MinSize > 0 && entry.SizeBytes < filter.MinSize {
			return false
		}
		if filter.MaxSize > 0 && entry.SizeBytes > filter.MaxSize {
			return false
		}
	}
	return true
}
