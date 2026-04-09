package server

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/embedding"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
	"github.com/mem9-ai/dat9/pkg/traceid"
	"go.uber.org/zap"
)

type Config struct {
	Meta         *meta.Store
	Pool         *tenant.Pool
	Provisioner  tenant.Provisioner
	TokenSecret       []byte
	Backend           *backend.Dat9Backend
	LocalS3           *s3client.LocalS3Client
	S3Dir             string
	MaxUploadBytes    int64
	Logger            *zap.Logger
	SemanticEmbedder  embedding.Client
	SemanticWorkers   SemanticWorkerOptions
}

type Server struct {
	fallback          *backend.Dat9Backend
	meta              *meta.Store
	pool              *tenant.Pool
	provisioner       tenant.Provisioner
	tokenSecret       []byte
	maxUploadBytes    int64
	metrics           *serverMetrics
	logger            *zap.Logger
	mux               *http.ServeMux
	semanticWorker    *semanticWorkerManager
}

var (
	schemaInitRetryWindow    = 10 * time.Minute
	schemaInitInitialBackoff = 2 * time.Second
	schemaInitMaxBackoff     = 30 * time.Second
)

// DefaultMaxUploadBytes is the server-wide fallback upload size limit.
// Keep callers on this exported constant so the default stays consistent.
const DefaultMaxUploadBytes int64 = 10 * (1 << 30) // 10 GiB

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
	s := &Server{
		fallback:       cfg.Backend,
		meta:           cfg.Meta,
		pool:           cfg.Pool,
		tokenSecret:    cfg.TokenSecret,
		provisioner:    cfg.Provisioner,
		maxUploadBytes: maxUpload,
		metrics:        newServerMetrics(),
		logger:         logger,
	}
	mux := http.NewServeMux()

	var business http.Handler = http.HandlerFunc(s.handleBusiness)
	if cfg.Meta != nil && cfg.Pool != nil && len(cfg.TokenSecret) > 0 {
		business = tenantAuthMiddleware(cfg.Meta, cfg.Pool, cfg.TokenSecret, business)
	} else if cfg.Backend != nil {
		business = injectFallbackBackend(cfg.Backend, business)
	}
	mux.Handle("/v1/fs/", business)
	mux.Handle("/v1/uploads", business)
	mux.Handle("/v1/uploads/", business)
	mux.Handle("/v2/uploads/", business)
	mux.Handle("/v1/sql", business)
	mux.HandleFunc("/v1/status", s.handleTenantStatus)
	mux.HandleFunc("/v1/provision", s.handleProvision)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/metrics", s.handleMetrics)

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
			r.URL.Path = "/" + sub
			localS3.Handler().ServeHTTP(w, r)
		}))
	}

	s.mux = mux
	if s.meta != nil && s.pool != nil && s.provisioner != nil {
		s.resumeProvisioningTenants()
	}
	s.semanticWorker = newSemanticWorkerManager(cfg.Backend, cfg.Meta, cfg.Pool, cfg.SemanticEmbedder, cfg.SemanticWorkers)
	if s.semanticWorker != nil {
		logger.Info("server_semantic_workers_enabled",
			zap.Int("workers", s.semanticWorker.opts.Workers),
			zap.Duration("poll_interval", s.semanticWorker.opts.PollInterval),
			zap.Duration("lease_duration", s.semanticWorker.opts.LeaseDuration),
			zap.Duration("recover_interval", s.semanticWorker.opts.RecoverInterval),
			zap.Bool("embedder_configured", cfg.SemanticEmbedder != nil),
			zap.Bool("fallback_image_extract_enabled", cfg.Backend != nil && cfg.Backend.SupportsAsyncImageExtract()),
			zap.Bool("pool_image_extract_enabled", cfg.Pool != nil && cfg.Pool.SupportsAsyncImageExtract()))
		s.semanticWorker.Start(backgroundWithTrace(context.Background()))
	} else {
		logger.Info("server_semantic_workers_disabled",
			zap.Bool("embedder_configured", cfg.SemanticEmbedder != nil),
			zap.Bool("fallback_present", cfg.Backend != nil),
			zap.Bool("fallback_image_extract_enabled", cfg.Backend != nil && cfg.Backend.SupportsAsyncImageExtract()),
			zap.Bool("pool_present", cfg.Pool != nil),
			zap.Bool("pool_image_extract_enabled", cfg.Pool != nil && cfg.Pool.SupportsAsyncImageExtract()))
	}
	return s
}

func (s *Server) Close() {
	if s.semanticWorker != nil {
		s.semanticWorker.Stop()
	}
}

func (s *Server) resumeProvisioningTenants() {
	ctx := backgroundWithTrace(context.Background())
	tenants, err := s.meta.ListTenantsByStatus(ctx, meta.TenantProvisioning, 1000)
	if err != nil {
		logger.Error(ctx, "resume_provisioning_list_failed", zap.Error(err))
		return
	}
	for i := range tenants {
		t := tenants[i]
		go s.resumeTenantSchemaInit(t)
	}
}

func (s *Server) resumeTenantSchemaInit(t meta.Tenant) {
	ctx := backgroundWithTrace(context.Background())
	plain, err := s.pool.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		logger.Warn(ctx, "resume_schema_init_skipped", zap.String("tenant_id", t.ID), zap.Error(err))
		return
	}
	dsn := tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS)
	s.initTenantSchemaAsync(ctx, t.ID, dsn, t.Provider, s.provisioner.InitSchema)
}

func backgroundWithTrace(ctx context.Context) context.Context {
	traceID := traceid.FromContext(ctx)
	if traceID == "" {
		traceID = traceid.Generate()
	}
	return traceid.With(context.Background(), traceID)
}

func tenantDSN(user, password, host string, port int, dbName string, tlsEnabled bool) string {
	query := "parseTime=true"
	if tlsEnabled {
		query += "&tls=true"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", user, password, host, port, dbName, query)
}

func injectFallbackBackend(b *backend.Dat9Backend, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := &TenantScope{TenantID: "local", APIKeyID: "local", TokenVersion: 1, Backend: b}
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
	switch {
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
	case r.URL.Path == "/v1/sql":
		s.handleSQL(w, r)
	default:
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "business_route_not_found", "path", r.URL.Path, "method", r.Method)...)
		errJSON(w, http.StatusNotFound, "not found")
	}
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
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_not_enabled")...)
		errJSON(w, http.StatusNotFound, "tenant status not enabled")
		return
	}
	tok := bearerToken(r)
	if tok == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_missing_token")...)
		errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
		return
	}

	resolved, err := s.meta.ResolveByAPIKeyHash(r.Context(), token.HashToken(tok))
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_key_not_found")...)
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_meta_unavailable", "error", err)...)
		errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
		return
	}
	if subtle.ConstantTimeCompare([]byte(token.HashToken(tok)), []byte(resolved.APIKey.JWTHash)) != 1 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_hash_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	if resolved.APIKey.Status != meta.APIKeyActive {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_key_inactive", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "status", resolved.APIKey.Status)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	plain, err := poolDecryptToken(r.Context(), s.pool, resolved.APIKey.JWTCiphertext)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_decrypt_failed", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "error", err)...)
		errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
		return
	}
	if subtle.ConstantTimeCompare([]byte(tok), plain) != 1 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_token_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	claims, err := token.ParseAndVerifyToken(s.tokenSecret, tok)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_token_invalid", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "error", err)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	if claims.TenantID != resolved.Tenant.ID || claims.TokenVersion != resolved.APIKey.TokenVersion {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_claims_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "claim_tenant", claims.TenantID, "claim_version", claims.TokenVersion)...)
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}

	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "tenant_status_ok", "tenant_id", resolved.Tenant.ID, "status", resolved.Tenant.Status)...)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(resolved.Tenant.Status)})
}

func backendFromRequest(r *http.Request) *backend.Dat9Backend {
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		return nil
	}
	return scope.Backend
}

func (s *Server) handleFS(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
	if path == "" {
		path = "/"
	}

	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("grep") {
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
		s.handleStat(w, r, path)
	case http.MethodDelete:
		s.handleDelete(w, r, path)
	case http.MethodPatch:
		s.handlePatch(w, r, path)
	case http.MethodPost:
		if r.URL.Query().Has("copy") {
			s.handleCopy(w, r, path)
		} else if r.URL.Query().Has("rename") {
			s.handleRename(w, r, path)
		} else if r.URL.Query().Has("mkdir") {
			s.handleMkdir(w, r, path)
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
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "read_missing_scope", "path", path)...)
		metricEvent(r.Context(), "fs_read", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if b.S3() != nil {
		url, err := b.PresignGetObject(r.Context(), path)
		if err == nil {
			logger.Info(r.Context(), "server_event", eventFields(r.Context(), "read_presigned_redirect", "path", path)...)
			metricEvent(r.Context(), "fs_read", "result", "ok")
			http.Redirect(w, r, url, http.StatusFound)
			return
		}
	}

	data, err := b.ReadCtx(r.Context(), path, 0, -1)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "read_not_found", "path", path)...)
			metricEvent(r.Context(), "fs_read", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "read_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_read", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "read_ok", "path", path, "bytes", len(data))...)
	metricEvent(r.Context(), "fs_read", "result", "ok")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "list_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	entries, err := b.ReadDirCtx(r.Context(), path)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "list_not_found", "path", path)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "list_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "userdb_query", "api", "list", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	metricEvent(r.Context(), "userdb_query", "api", "list", "result", "ok")
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "list_ok", "path", path, "entries", len(entries))...)
	type entry struct {
		Name  string `json:"name"`
		Size  int64  `json:"size"`
		IsDir bool   `json:"isDir"`
	}
	out := make([]entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, entry{Name: e.Name, Size: e.Size, IsDir: e.IsDir})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_missing_scope", "path", path)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
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
		plan, err := b.InitiateUploadWithChecksums(r.Context(), path, cl, partChecksums)
		if err != nil {
			if errors.Is(err, backend.ErrUploadTooLarge) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_upload_too_large", "path", path, "error", err)...)
				metricEvent(r.Context(), "fs_write", "result", "error")
				errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
				return
			}
			if errors.Is(err, backend.ErrStorageQuotaExceeded) {
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
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "write_upload_initiate_failed", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusInternalServerError, err.Error())
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
	data, err := io.ReadAll(body)
	if err != nil {
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
	_, err = b.WriteCtx(r.Context(), path, data, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_too_large_backend", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if errors.Is(err, backend.ErrStorageQuotaExceeded) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_storage_quota_exceeded", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "write_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "write_ok", "path", path, "bytes", len(data))...)
	metricEvent(r.Context(), "fs_write", "result", "ok")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handlePatch(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_missing_scope", "path", path)...)
		metricEvent(r.Context(), "fs_patch", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}

	var req struct {
		NewSize    int64 `json:"new_size"`
		DirtyParts []int `json:"dirty_parts"`
		PartSize   int64 `json:"part_size,omitempty"`
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

	plan, err := b.InitiatePatchUpload(r.Context(), path, req.NewSize, req.DirtyParts, req.PartSize)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_too_large", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_patch", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if errors.Is(err, backend.ErrStorageQuotaExceeded) {
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
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_not_found", "path", path)...)
			metricEvent(r.Context(), "fs_patch", "result", "error")
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "file is not S3-stored") || strings.Contains(err.Error(), "S3 not configured") {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "patch_unsupported_target", "path", path, "error", err)...)
			metricEvent(r.Context(), "fs_patch", "result", "error")
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_patch", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "patch_upload_initiated", "path", path,
		"dirty_parts", len(plan.UploadParts), "copied_parts", len(plan.CopiedParts))...)
	metricEvent(r.Context(), "fs_patch", "result", "accepted")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "stat_missing_scope", "path", path)...)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	nf, err := b.Store().Stat(r.Context(), path)
	if err != nil {
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
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("X-Dat9-IsDir", fmt.Sprintf("%v", nf.Node.IsDirectory))
	if nf.File != nil {
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(nf.File.Revision, 10))
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "stat_ok", "path", path, "is_dir", nf.Node.IsDirectory)...)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "delete_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	recursive := r.URL.Query().Has("recursive")
	var err error
	if recursive {
		err = b.RemoveAllCtx(r.Context(), path)
	} else {
		err = b.RemoveCtx(r.Context(), path)
	}
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "delete_not_found", "path", path, "recursive", recursive)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "delete_failed", "path", path, "recursive", recursive, "error", err)...)
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "delete_ok", "path", path, "recursive", recursive)...)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCopy(w http.ResponseWriter, r *http.Request, dstPath string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "copy_missing_scope", "dst_path", dstPath)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	srcPath := r.Header.Get("X-Dat9-Copy-Source")
	if srcPath == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "copy_missing_source_header", "dst_path", dstPath)...)
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Copy-Source header")
		return
	}
	if err := b.CopyFileCtx(r.Context(), srcPath, dstPath); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "copy_not_found", "src_path", srcPath, "dst_path", dstPath)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "copy_failed", "src_path", srcPath, "dst_path", dstPath, "error", err)...)
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "copy_ok", "src_path", srcPath, "dst_path", dstPath)...)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request, newPath string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "rename_missing_scope", "new_path", newPath)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	oldPath := r.Header.Get("X-Dat9-Rename-Source")
	if oldPath == "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "rename_missing_source_header", "new_path", newPath)...)
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Rename-Source header")
		return
	}
	if err := b.RenameCtx(r.Context(), oldPath, newPath); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "rename_not_found", "old_path", oldPath, "new_path", newPath)...)
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "rename_failed", "old_path", oldPath, "new_path", newPath, "error", err)...)
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "rename_ok", "old_path", oldPath, "new_path", newPath)...)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "mkdir_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.MkdirCtx(r.Context(), path, 0o755); err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "mkdir_failed", "path", path, "error", err)...)
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "mkdir_ok", "path", path)...)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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
	status := r.URL.Query().Get("status")
	if status == "" {
		status = string(datastore.UploadUploading)
	}
	uploads, err := b.ListUploads(r.Context(), path, datastore.UploadStatus(status))
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "uploads_list_failed", "path", path, "status", status, "error", err)...)
		metricEvent(r.Context(), "metadb_query", "api", "uploads_list", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
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
		Path          string   `json:"path"`
		TotalSize     int64    `json:"total_size"`
		PartChecksums []string `json:"part_checksums"`
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
	plan, err := b.InitiateUploadWithChecksums(r.Context(), req.Path, req.TotalSize, partChecksums)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_too_large_backend", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if errors.Is(err, backend.ErrStorageQuotaExceeded) {
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
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "upload_initiate_failed", "path", req.Path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
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
	if err := b.ConfirmUpload(r.Context(), uploadID); err != nil {
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
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "upload_complete", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "upload_complete_ok", "upload_id", uploadID)...)
	metricEvent(r.Context(), "upload_complete", "result", "ok")
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
		errJSON(w, http.StatusInternalServerError, err.Error())
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

func (s *Server) handleUploadAbort(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "upload_abort_missing_scope", "upload_id", uploadID)...)
		metricEvent(r.Context(), "upload_abort", "result", "error")
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
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
		errJSON(w, http.StatusInternalServerError, err.Error())
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
		Path      string `json:"path"`
		TotalSize int64  `json:"total_size"`
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
	if req.TotalSize <= 0 {
		errJSON(w, http.StatusBadRequest, "total_size must be positive")
		return
	}
	if req.TotalSize > s.maxUploadBytes {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_too_large", "path", req.Path, "bytes", req.TotalSize, "max", s.maxUploadBytes)...)
		errJSON(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload too large: max %d bytes", s.maxUploadBytes))
		return
	}
	plan, err := b.InitiateUploadV2(r.Context(), req.Path, req.TotalSize)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_too_large_backend", "path", req.Path, "error", err)...)
			metricEvent(r.Context(), "v2_upload_initiate", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if errors.Is(err, backend.ErrStorageQuotaExceeded) {
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
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_initiate_failed", "path", req.Path, "error", err)...)
		metricEvent(r.Context(), "v2_upload_initiate", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
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
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_part_failed", "upload_id", uploadID, "part_number", req.PartNumber, "error", err)...)
		metricEvent(r.Context(), "v2_presign_part", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
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
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_presign_batch_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "v2_presign_batch", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
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
	var req struct {
		Parts []backend.CompletePart `json:"parts"`
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
	if err := b.ConfirmUploadV2(r.Context(), uploadID, req.Parts); err != nil {
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
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "v2_upload_complete", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_complete_ok", "upload_id", uploadID)...)
	metricEvent(r.Context(), "v2_upload_complete", "result", "ok")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "completed"})
}

func (s *Server) handleV2UploadAbort(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_abort_missing_scope", "upload_id", uploadID)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.AbortUploadV2(r.Context(), uploadID); err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "v2_upload_abort_failed", "upload_id", uploadID, "error", err)...)
		metricEvent(r.Context(), "v2_upload_abort", "result", "error")
		errJSON(w, http.StatusInternalServerError, err.Error())
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
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_not_enabled")...)
		metricEvent(r.Context(), "tenant_provision", "result", "error")
		errJSON(w, http.StatusNotFound, "provisioning not enabled")
		return
	}

	// Detect tidbcloud-native provision via headers.
	target, err := tidbcloud.ParseHeaders(r)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_bad_tidbcloud_header", "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if target != nil {
		s.handleNativeProvision(w, r, target)
		return
	}

	if s.provisioner == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provisioner_not_configured")...)
		metricEvent(r.Context(), "tenant_provision", "result", "error")
		errJSON(w, http.StatusNotFound, "provisioner not configured")
		return
	}

	// Reject tidbcloud headers on non-native providers (should not reach here
	// since target == nil, but guard against future changes).
	provider := s.provisioner.ProviderType()
	provider, err = tenant.NormalizeProvider(provider)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "provision_provider_invalid", "provider", provider, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID := token.NewID()
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "provision_requested", "tenant_id", tenantID, "provider", provider)...)
	keyName := "default"

	apiToken, err := token.IssueToken(s.tokenSecret, tenantID, 1)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_issue_token_failed", "tenant_id", tenantID, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to issue token")
		return
	}
	hash := token.HashToken(apiToken)
	now := time.Now().UTC()
	cluster, err := s.provisioner.Provision(r.Context(), tenantID)
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_cluster_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "cluster_error")
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("provision tenant cluster failed: %v", err))
		return
	}
	cluster.Provider = provider

	cipherPass, err := s.pool.Encrypt(r.Context(), []byte(cluster.Password))
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_encrypt_db_password_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to encrypt db password")
		return
	}
	cipherToken, err := s.pool.Encrypt(r.Context(), []byte(apiToken))
	if err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_encrypt_api_key_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to encrypt api key")
		return
	}

	if err := s.meta.InsertTenant(r.Context(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           cluster.Username,
		DBPasswordCipher: cipherPass,
		DBName:           cluster.DBName,
		DBTLS:            true,
		Provider:         provider,
		ClusterID:        cluster.ClusterID,
		ClaimURL:         cluster.ClaimURL,
		ClaimExpiresAt:   cluster.ClaimExpiresAt,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_insert_tenant_failed", "tenant_id", tenantID, "provider", provider, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
		metricEvent(r.Context(), "metadb_query", "api", "insert_tenant", "result", "error")
		errJSON(w, http.StatusInternalServerError, "failed to persist tenant")
		return
	}
	metricEvent(r.Context(), "metadb_query", "api", "insert_tenant", "result", "ok")
	apiKeyID := token.NewID()
	if err := s.meta.InsertAPIKey(r.Context(), &meta.APIKey{
		ID:            apiKeyID,
		TenantID:      tenantID,
		KeyName:       keyName,
		JWTCiphertext: cipherToken,
		JWTHash:       hash,
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "provision_insert_api_key_failed", "tenant_id", tenantID, "api_key_id", apiKeyID, "error", err)...)
		metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "error")
		metricEvent(r.Context(), "metadb_query", "api", "insert_api_key", "result", "error")
		_ = s.meta.UpdateTenantStatus(r.Context(), tenantID, meta.TenantDeleted)
		errJSON(w, http.StatusInternalServerError, "failed to persist api key")
		return
	}
	metricEvent(r.Context(), "metadb_query", "api", "insert_api_key", "result", "ok")

	// Initialize tenant schema asynchronously; tenant remains in provisioning state until success.
	dsn := tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, cluster.DBName, true)
	go s.initTenantSchemaAsync(backgroundWithTrace(r.Context()), tenantID, dsn, provider, s.provisioner.InitSchema)
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "provision_accepted", "tenant_id", tenantID, "provider", provider)...)
	metricEvent(r.Context(), "tenant_provision", "provider", provider, "result", "accepted")

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"api_key": apiToken,
		"status":  string(meta.TenantProvisioning),
	})
}

func (s *Server) initTenantSchemaAsync(ctx context.Context, tenantID, tenantDSN, provider string, schemaInit func(context.Context, string) error) {
	ctx = backgroundWithTrace(ctx)
	logger.Info(ctx, "server_event", eventFields(ctx, "schema_init_started", "tenant_id", tenantID, "provider", provider)...)
	deadline := time.Now().Add(schemaInitRetryWindow)
	backoff := schemaInitInitialBackoff
	attempt := 1
	for {
		if err := schemaInit(ctx, tenantDSN); err == nil {
			logger.Info(ctx, "server_event", eventFields(ctx, "schema_init_ok", "tenant_id", tenantID, "provider", provider, "attempt", attempt)...)
			if s.metrics != nil {
				s.metrics.recordEvent("tenant_schema_init", "provider", provider, "result", "ok")
			}
			if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantActive); err != nil {
				logger.Error(ctx, "schema_init_activate_failed", zap.String("tenant_id", tenantID), zap.Error(err))
			}
			return
		} else {
			logger.Error(ctx, "server_event", eventFields(ctx, "schema_init_failed", "tenant_id", tenantID, "provider", provider, "attempt", attempt, "error", err)...)
			if s.metrics != nil {
				s.metrics.recordEvent("tenant_schema_init", "provider", provider, "result", "error")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				if uerr := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantFailed); uerr != nil {
					logger.Error(ctx, "schema_init_mark_failed_update_error", zap.String("tenant_id", tenantID), zap.Error(uerr))
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
			time.Sleep(sleepFor)
		}
		backoff *= 2
		attempt++
	}
}

func errJSON(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
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
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	metricEvent(r.Context(), "userdb_query", "api", "grep", "result", "ok")
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "grep_ok", "path", path, "query_len", len(query), "limit", limit, "results", len(results))...)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

func (s *Server) handleFind(w http.ResponseWriter, r *http.Request, path string) {
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
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	metricEvent(r.Context(), "userdb_query", "api", "find", "result", "ok")
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "find_ok", "path", path, "results", len(results), "name", f.NameGlob, "tag_key", f.TagKey)...)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}
