package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

type Config struct {
	Meta        *meta.Store
	Pool        *tenant.Pool
	Provisioner tenant.Provisioner
	TokenSecret []byte
	Backend     *backend.Dat9Backend
	LocalS3     *s3client.LocalS3Client
	S3Dir       string
}

type Server struct {
	fallback    *backend.Dat9Backend
	meta        *meta.Store
	pool        *tenant.Pool
	provisioner tenant.Provisioner
	tokenSecret []byte
	mux         *http.ServeMux
}

var (
	schemaInitRetryWindow    = 10 * time.Minute
	schemaInitInitialBackoff = 2 * time.Second
	schemaInitMaxBackoff     = 30 * time.Second
)

func New(b *backend.Dat9Backend) *Server {
	return NewWithConfig(Config{Backend: b})
}

func NewWithConfig(cfg Config) *Server {
	s := &Server{
		fallback:    cfg.Backend,
		meta:        cfg.Meta,
		pool:        cfg.Pool,
		tokenSecret: cfg.TokenSecret,
		provisioner: cfg.Provisioner,
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
	mux.HandleFunc("/v1/status", s.handleTenantStatus)
	mux.HandleFunc("/v1/provision", s.handleProvision)

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
			b := cfg.Pool.LoadS3Backend(cfg.Meta, tenantID)
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
	return s
}

func (s *Server) resumeProvisioningTenants() {
	tenants, err := s.meta.ListTenantsByStatus(meta.TenantProvisioning, 1000)
	if err != nil {
		log.Printf("list provisioning tenants failed: %v", err)
		return
	}
	for i := range tenants {
		t := tenants[i]
		go s.resumeTenantSchemaInit(t)
	}
}

func (s *Server) resumeTenantSchemaInit(t meta.Tenant) {
	plain, err := s.pool.Decrypt(t.DBPasswordCipher)
	if err != nil {
		log.Printf("resume tenant schema init skipped: decrypt db password failed (tenant=%s): %v", t.ID, err)
		return
	}
	dsn := tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS)
	s.initTenantSchemaAsync(t.ID, dsn, t.Provider, s.provisioner.InitSchema)
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
	s.mux.ServeHTTP(w, r)
}

func (s *Server) ListenAndServe(addr string) error {
	log.Printf("dat9 server listening on %s", addr)
	return http.ListenAndServe(addr, s)
}

func (s *Server) handleBusiness(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/fs/"):
		s.handleFS(w, r)
	case r.URL.Path == "/v1/uploads":
		s.handleUploads(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/uploads/"):
		s.handleUploadAction(w, r)
	default:
		errJSON(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleTenantStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "tenant status not enabled")
		return
	}
	tok := bearerToken(r)
	if tok == "" {
		errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
		return
	}

	resolved, err := s.meta.ResolveByAPIKeyHash(tenant.HashToken(tok))
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
		return
	}
	if subtle.ConstantTimeCompare([]byte(tenant.HashToken(tok)), []byte(resolved.APIKey.JWTHash)) != 1 {
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	if resolved.APIKey.Status != meta.APIKeyActive {
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	plain, err := poolDecryptToken(s.pool, resolved.APIKey.JWTCiphertext)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
		return
	}
	if subtle.ConstantTimeCompare([]byte(tok), plain) != 1 {
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	claims, err := tenant.ParseAndVerifyToken(s.tokenSecret, tok)
	if err != nil {
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	if claims.TenantID != resolved.Tenant.ID || claims.TokenVersion != resolved.APIKey.TokenVersion {
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return
	}

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
		if r.URL.Query().Has("list") {
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
	case http.MethodPost:
		if r.URL.Query().Has("copy") {
			s.handleCopy(w, r, path)
		} else if r.URL.Query().Has("rename") {
			s.handleRename(w, r, path)
		} else if r.URL.Query().Has("mkdir") {
			s.handleMkdir(w, r, path)
		} else {
			errJSON(w, http.StatusBadRequest, "unknown POST action")
		}
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if b.S3() != nil {
		url, err := b.PresignGetObject(r.Context(), path)
		if err == nil {
			http.Redirect(w, r, url, http.StatusFound)
			return
		}
	}

	data, err := b.Read(path, 0, -1)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	entries, err := b.ReadDir(path)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
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
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	cl := r.ContentLength
	if h := r.Header.Get("X-Dat9-Content-Length"); h != "" {
		cl, _ = strconv.ParseInt(h, 10, 64)
	}
	if cl > 0 && b.IsLargeFile(cl) {
		plan, err := b.InitiateUpload(r.Context(), path, cl)
		if err != nil {
			if errors.Is(err, datastore.ErrUploadConflict) {
				errJSON(w, http.StatusConflict, err.Error())
				return
			}
			errJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(plan)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	_, err = b.Write(path, data, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	nf, err := b.Store().Stat(path)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
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
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	recursive := r.URL.Query().Has("recursive")
	var err error
	if recursive {
		err = b.RemoveAll(path)
	} else {
		err = b.Remove(path)
	}
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCopy(w http.ResponseWriter, r *http.Request, dstPath string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	srcPath := r.Header.Get("X-Dat9-Copy-Source")
	if srcPath == "" {
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Copy-Source header")
		return
	}
	if err := b.CopyFile(srcPath, dstPath); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request, newPath string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	oldPath := r.Header.Get("X-Dat9-Rename-Source")
	if oldPath == "" {
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Rename-Source header")
		return
	}
	if err := b.Rename(oldPath, newPath); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.Mkdir(path, 0o755); err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleUploads(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		errJSON(w, http.StatusBadRequest, "missing path parameter")
		return
	}
	status := r.URL.Query().Get("status")
	if status == "" {
		status = string(datastore.UploadUploading)
	}
	uploads, err := b.ListUploads(path, datastore.UploadStatus(status))
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
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

func (s *Server) handleUploadAction(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/uploads/")
	parts := strings.SplitN(rest, "/", 2)
	uploadID := parts[0]
	action := ""
	if len(parts) > 1 {
		action = strings.Trim(parts[1], "/")
	}
	if uploadID == "" {
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
		errJSON(w, http.StatusBadRequest, "unknown upload action")
	}
}

func (s *Server) handleUploadComplete(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.ConfirmUpload(r.Context(), uploadID); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadNotActive) || errors.Is(err, datastore.ErrPathConflict) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleUploadResume(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	plan, err := b.ResumeUpload(r.Context(), uploadID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadExpired) {
			errJSON(w, http.StatusGone, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleUploadAbort(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if err := b.AbortUpload(r.Context(), uploadID); err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleProvision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "provisioning not enabled")
		return
	}
	if s.provisioner == nil {
		errJSON(w, http.StatusNotFound, "provisioner not configured")
		return
	}
	provider := s.provisioner.ProviderType()
	provider, err := tenant.NormalizeProvider(provider)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID := tenant.NewID()
	keyName := "default"

	token, err := tenant.IssueToken(s.tokenSecret, tenantID, 1)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to issue token")
		return
	}
	hash := tenant.HashToken(token)
	now := time.Now().UTC()
	cluster, err := s.provisioner.Provision(r.Context(), tenantID)
	if err != nil {
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("provision tenant cluster failed: %v", err))
		return
	}
	cluster.Provider = provider

	cipherPass, err := s.pool.Encrypt([]byte(cluster.Password))
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to encrypt db password")
		return
	}
	cipherToken, err := s.pool.Encrypt([]byte(token))
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to encrypt api key")
		return
	}

	if err := s.meta.InsertTenant(&meta.Tenant{
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
		errJSON(w, http.StatusInternalServerError, "failed to persist tenant")
		return
	}
	apiKeyID := tenant.NewID()
	if err := s.meta.InsertAPIKey(&meta.APIKey{
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
		_ = s.meta.UpdateTenantStatus(tenantID, meta.TenantDeleted)
		errJSON(w, http.StatusInternalServerError, "failed to persist api key")
		return
	}

	// Initialize tenant schema asynchronously; tenant remains in provisioning state until success.
	dsn := tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, cluster.DBName, true)
	go s.initTenantSchemaAsync(tenantID, dsn, provider, s.provisioner.InitSchema)

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"api_key": token,
		"status":  string(meta.TenantProvisioning),
	})
}

func (s *Server) initTenantSchemaAsync(tenantID, tenantDSN, provider string, schemaInit func(context.Context, string) error) {
	deadline := time.Now().Add(schemaInitRetryWindow)
	backoff := schemaInitInitialBackoff
	attempt := 1
	for {
		if err := schemaInit(context.Background(), tenantDSN); err == nil {
			if err := s.meta.UpdateTenantStatus(tenantID, meta.TenantActive); err != nil {
				log.Printf("activate tenant %s failed after schema init: %v", tenantID, err)
			}
			return
		} else {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				if uerr := s.meta.UpdateTenantStatus(tenantID, meta.TenantFailed); uerr != nil {
					log.Printf("mark tenant %s failed after init retries exhausted: update status failed: %v", tenantID, uerr)
				}
				log.Printf("tenant %s marked failed after schema init retries exhausted: %v", tenantID, err)
				return
			}
			log.Printf("init tenant schema failed (tenant=%s provider=%s attempt=%d remaining=%s): %v", tenantID, provider, attempt, remaining.Round(time.Second), err)
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
