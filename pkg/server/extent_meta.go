package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

func (s *Server) handleExtentMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	op := strings.TrimSpace(r.Header.Get("X-Drive9-Extent-Op"))
	if op == "" {
		op = strings.TrimSpace(r.URL.Query().Get("op"))
	}
	if op == "" {
		errJSON(w, http.StatusBadRequest, "missing extent op")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body")
		return
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	body, errno, err := b.Store().RunExtentMetaOp(r.Context(), op, json.RawMessage(raw))
	if err != nil {
		logger.Error(r.Context(), "extent_meta_failed", eventFields(r.Context(), "extent_meta_failed", "op", op, "error", err)...)
		errJSON(w, http.StatusInternalServerError, "extent meta failed")
		return
	}
	if errno == int(syscall.EIO) {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (s *Server) handleExtentBlocks(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/v1/extent/blocks/")
	if key == "" || strings.Contains(key, "..") {
		errJSON(w, http.StatusBadRequest, "invalid block key")
		return
	}
	scope := ScopeFromContext(r.Context())
	full := extentBlockKey(scope.TenantID, key)
	s3 := b.S3()
	if s3 == nil {
		errJSON(w, http.StatusNotImplemented, "object store is not configured")
		return
	}
	switch r.Method {
	case http.MethodPut:
		err := s3.PutObject(r.Context(), full, r.Body, r.ContentLength, s3client.EncryptionOpts{})
		if err != nil {
			errJSON(w, http.StatusBadGateway, "put block failed")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		rc, err := s3.GetObject(r.Context(), full)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer func() { _ = rc.Close() }()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)
	case http.MethodDelete:
		if err := s3.DeleteObject(r.Context(), full); err != nil {
			errJSON(w, http.StatusBadGateway, "delete block failed")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleDataCredential(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if scope.IsScoped {
		errJSON(w, http.StatusForbidden, "scoped token cannot mint data credentials")
		return
	}
	prefix := "t/" + scope.TenantID + "/"
	// Local / mock backends have no STS. The client talks to
	// /v1/extent/blocks with the same API key (fallback ObjectStorage).
	writeJSON(w, http.StatusOK, dataCredentialResponse{
		Scheme:    "drive9",
		Endpoint:  strings.TrimRight(publicBaseURL(r), "/") + "/v1/extent/blocks",
		Prefix:    prefix,
		ExpiresAt: "",
	})
}

type dataCredentialResponse struct {
	Scheme          string `json:"scheme"`
	Endpoint        string `json:"endpoint"`
	Bucket          string `json:"bucket,omitempty"`
	Prefix          string `json:"prefix"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
}

func publicBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	host := r.Host
	if host == "" {
		host = "127.0.0.1"
	}
	return scheme + "://" + host
}

func extentBlockKey(tenantID, key string) string {
	key = strings.TrimPrefix(key, "/")
	prefix := "t/" + tenantID + "/"
	if strings.HasPrefix(key, prefix) {
		return key
	}
	return prefix + key
}

func runExtentBlockGC(ctx context.Context, store *datastore.Store, s3 s3client.S3Client, tenantID string) {
	if store == nil || s3 == nil {
		return
	}
	keys, err := store.ListPendingBlockGC(ctx, 32)
	if err != nil || len(keys) == 0 {
		return
	}
	for _, key := range keys {
		full := extentBlockKey(tenantID, key)
		_ = s3.DeleteObject(ctx, full)
		_ = store.MarkBlockGCDone(ctx, key)
	}
}
