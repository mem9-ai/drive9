// Package server implements the dat9 HTTP server.
// All file operations go through /v1/fs/{path}.
package server

import (
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
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

// Config configures the server.
type Config struct {
	// Backend is the fallback backend for local dev mode (no multi-tenant).
	// When Tenants is set, business routes use the tenant backend from context.
	Backend *backend.Dat9Backend

	// Multi-tenant fields. When Tenants is non-nil, auth is enforced.
	Tenants     *tenant.Store
	Pool        *tenant.Pool
	Provisioner tenant.Provisioner

	// AdminKey protects /v1/provision. Required when Tenants is set.
	AdminKey string

	// LocalS3 is set when using local S3 for presigned URL handling.
	LocalS3 *s3client.LocalS3Client
}

type Server struct {
	fallback    *backend.Dat9Backend // nil in multi-tenant mode with no fallback
	tenants     *tenant.Store
	pool        *tenant.Pool
	provisioner tenant.Provisioner
	adminKey    string
	mux         *http.ServeMux
}

func New(cfg Config) *Server {
	s := &Server{
		fallback:    cfg.Backend,
		tenants:     cfg.Tenants,
		pool:        cfg.Pool,
		provisioner: cfg.Provisioner,
		adminKey:    cfg.AdminKey,
	}
	mux := http.NewServeMux()

	// Business routes — require tenant backend
	var businessHandler http.Handler = http.HandlerFunc(s.handleBusiness)
	if cfg.Tenants != nil && cfg.Pool != nil {
		businessHandler = tenantAuthMiddleware(cfg.Tenants, cfg.Pool, businessHandler)
	} else if cfg.Backend != nil {
		// Local dev mode: inject fallback backend for all requests
		businessHandler = injectBackendMiddleware(cfg.Backend, businessHandler)
	}
	mux.Handle("/v1/fs/", businessHandler)
	mux.Handle("/v1/uploads", businessHandler)
	mux.Handle("/v1/uploads/", businessHandler)

	// Control plane routes (no tenant auth, admin key protected)
	mux.HandleFunc("/v1/provision", s.handleProvision)

	// Local S3 presigned URL handler (no auth — URLs are HMAC-signed)
	local := cfg.LocalS3
	if local == nil && cfg.Backend != nil {
		if l, ok := cfg.Backend.S3().(*s3client.LocalS3Client); ok {
			local = l
		}
	}
	if local != nil {
		mux.Handle("/s3/", http.StripPrefix("/s3", local.Handler()))
	}

	s.mux = mux
	return s
}

// injectBackendMiddleware injects a fixed backend into context (local dev mode).
func injectBackendMiddleware(b *backend.Dat9Backend, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(withBackend(r.Context(), b)))
	})
}

// handleBusiness dispatches to the appropriate handler based on path.
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

// backendFromRequest extracts the tenant backend from context.
func backendFromRequest(r *http.Request) *backend.Dat9Backend {
	return BackendFromCtx(r.Context())
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
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
	// Check if this is an S3-stored file — redirect to presigned URL
	if b.S3() != nil {
		url, err := b.PresignGetObject(r.Context(), path)
		if err == nil {
			http.Redirect(w, r, url, http.StatusFound)
			return
		}
		// Not an S3 file or error — fall through to local read
	}

	data, err := b.Read(path, 0, -1)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Write(data)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	entries, err := b.ReadDir(path)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
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
	result := struct {
		Entries []entry `json:"entries"`
	}{Entries: make([]entry, 0, len(entries))}

	for _, e := range entries {
		result.Entries = append(result.Entries, entry{
			Name: e.Name, Size: e.Size, IsDir: e.IsDir,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	// Bifurcate by size. Prefer X-Dat9-Content-Length because Go's net/http
	// normalizes Content-Length to 0 when the request body is http.NoBody.
	cl := r.ContentLength
	if h := r.Header.Get("X-Dat9-Content-Length"); h != "" {
		cl, _ = strconv.ParseInt(h, 10, 64)
	}
	if cl > 0 && b.IsLargeFile(cl) {
		plan, err := b.InitiateUpload(r.Context(), path, cl)
		if err != nil {
			if errors.Is(err, meta.ErrUploadConflict) {
				errJSON(w, http.StatusConflict, err.Error())
				return
			}
			errJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(plan)
		return
	}

	// Small file: proxy through server
	data, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}

	_, err = b.Write(path, data, 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request, path string) {
	b := backendFromRequest(r)
	// Single call to store.Stat to get both FileInfo and revision
	nf, err := b.Store().Stat(path)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
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
	recursive := r.URL.Query().Has("recursive")

	var err error
	if recursive {
		err = b.RemoveAll(path)
	} else {
		err = b.Remove(path)
	}
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCopy(w http.ResponseWriter, r *http.Request, dstPath string) {
	srcPath := r.Header.Get("X-Dat9-Copy-Source")
	if srcPath == "" {
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Copy-Source header")
		return
	}

	if err := backendFromRequest(r).CopyFile(srcPath, dstPath); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request, newPath string) {
	oldPath := r.Header.Get("X-Dat9-Rename-Source")
	if oldPath == "" {
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Rename-Source header")
		return
	}

	if err := backendFromRequest(r).Rename(oldPath, newPath); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request, path string) {
	if err := backendFromRequest(r).Mkdir(path, 0o755); err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleUploads handles GET /v1/uploads?path=...&status=...
func (s *Server) handleUploads(w http.ResponseWriter, r *http.Request) {
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
		status = "UPLOADING"
	}

	uploads, err := backendFromRequest(r).ListUploads(path, meta.UploadStatus(status))
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
	result := make([]uploadEntry, 0, len(uploads))
	for _, u := range uploads {
		result = append(result, uploadEntry{
			UploadID:   u.UploadID,
			Path:       u.TargetPath,
			TotalSize:  u.TotalSize,
			PartsTotal: u.PartsTotal,
			Status:     string(u.Status),
			CreatedAt:  u.CreatedAt.Format("2006-01-02T15:04:05.000Z07:00"),
			ExpiresAt:  u.ExpiresAt.Format("2006-01-02T15:04:05.000Z07:00"),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"uploads": result})
}

// handleUploadAction handles /v1/uploads/{id}/complete, /v1/uploads/{id}/resume, DELETE /v1/uploads/{id}
func (s *Server) handleUploadAction(w http.ResponseWriter, r *http.Request) {
	// Parse: /v1/uploads/{id} or /v1/uploads/{id}/complete or /v1/uploads/{id}/resume
	rest := strings.TrimPrefix(r.URL.Path, "/v1/uploads/")
	parts := strings.SplitN(rest, "/", 2)
	uploadID := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	if uploadID == "" {
		errJSON(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	switch {
	case r.Method == http.MethodPost && action == "complete":
		s.handleUploadComplete(w, r, uploadID)
	case r.Method == http.MethodPost && action == "resume":
		s.handleUploadResume(w, r, uploadID)
	case r.Method == http.MethodDelete && action == "":
		s.handleUploadAbort(w, r, uploadID)
	default:
		errJSON(w, http.StatusBadRequest, "unknown upload action")
	}
}

func (s *Server) handleUploadComplete(w http.ResponseWriter, r *http.Request, uploadID string) {
	if err := backendFromRequest(r).ConfirmUpload(r.Context(), uploadID); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, meta.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, meta.ErrPathConflict) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleUploadResume(w http.ResponseWriter, r *http.Request, uploadID string) {
	plan, err := backendFromRequest(r).ResumeUpload(r.Context(), uploadID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, meta.ErrUploadExpired) {
			errJSON(w, http.StatusGone, err.Error())
			return
		}
		if errors.Is(err, meta.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleUploadAbort(w http.ResponseWriter, r *http.Request, uploadID string) {
	if err := backendFromRequest(r).AbortUpload(r.Context(), uploadID); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleProvision handles POST /v1/provision (admin-key protected).
// Flow: validate admin key → generate API key → provision cluster → init schema → persist tenant → return key.
func (s *Server) handleProvision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.adminKey == "" || s.provisioner == nil || s.tenants == nil {
		errJSON(w, http.StatusNotFound, "provisioning not enabled")
		return
	}
	token := bearerToken(r)
	if token != s.adminKey {
		errJSON(w, http.StatusUnauthorized, "invalid admin key")
		return
	}

	ctx := r.Context()

	// Step 1: Generate API key
	rawKey, prefix, hash, err := tenant.GenerateAPIKey()
	if err != nil {
		log.Printf("provision: generate key: %v", err)
		errJSON(w, http.StatusInternalServerError, "key generation failed")
		return
	}

	// Step 2: Create tenant record in provisioning state
	now := time.Now().UTC()
	t := &tenant.Tenant{
		ID:              hash[:16], // use first 16 chars of hash as tenant ID
		APIKeyPrefix:    prefix,
		APIKeyHash:      hash,
		Status:          tenant.StatusProvisioning,
		ProvisionerType: s.provisioner.ProviderType(),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := s.tenants.Insert(t); err != nil {
		log.Printf("provision: insert tenant: %v", err)
		errJSON(w, http.StatusInternalServerError, "provisioning failed")
		return
	}

	// Step 3: Provision cluster
	info, err := s.provisioner.Provision(ctx)
	if err != nil {
		log.Printf("provision: cluster: %v", err)
		s.tenants.UpdateStatus(t.ID, tenant.StatusDeleted)
		errJSON(w, http.StatusInternalServerError, "cluster provisioning failed")
		return
	}

	// Step 4: Encrypt password and update tenant with cluster info
	passwordEnc, err := s.tenants.EncryptPassword(info.Password)
	if err != nil {
		log.Printf("provision: encrypt password: %v", err)
		s.tenants.UpdateStatus(t.ID, tenant.StatusDeleted)
		errJSON(w, http.StatusInternalServerError, "provisioning failed")
		return
	}
	if err := s.tenants.UpdateClusterInfo(t.ID, info.Host, info.Port, info.Username, passwordEnc, info.DBName); err != nil {
		log.Printf("provision: update cluster info: %v", err)
		s.tenants.UpdateStatus(t.ID, tenant.StatusDeleted)
		errJSON(w, http.StatusInternalServerError, "provisioning failed")
		return
	}

	// Step 5: Init schema on the new cluster
	dsn := tenant.DSN(info.Host, info.Port, info.Username, info.Password, info.DBName)
	if err := s.provisioner.InitSchema(ctx, dsn); err != nil {
		log.Printf("provision: init schema: %v", err)
		s.tenants.UpdateStatus(t.ID, tenant.StatusDeleted)
		errJSON(w, http.StatusInternalServerError, "schema initialization failed")
		return
	}

	// Step 6: Mark tenant active
	if err := s.tenants.UpdateStatus(t.ID, tenant.StatusActive); err != nil {
		log.Printf("provision: activate tenant: %v", err)
		errJSON(w, http.StatusInternalServerError, "provisioning failed")
		return
	}

	// Return the API key (only time it's exposed in plaintext)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"api_key":   rawKey,
		"tenant_id": t.ID,
		"status":    "active",
	})
}

func errJSON(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ListenAndServe starts the server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	log.Printf("dat9 server listening on %s", addr)
	return http.ListenAndServe(addr, s)
}
