package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

const defaultTenantAPIKeyName = "default"
const tenantAPIKeysPath = "/v1/tenants/keys"

func (s *Server) handleAPIKeys(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "api key management not enabled")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	if scope.APIKeyName != defaultTenantAPIKeyName {
		errJSON(w, http.StatusForbidden, "only default API key may manage API keys")
		return
	}

	sub := strings.TrimPrefix(r.URL.Path, tenantAPIKeysPath)
	if sub == "" || sub == "/" {
		if r.Method == http.MethodGet {
			s.handleAPIKeyList(w, r, scope)
			return
		}
		if r.Method == http.MethodPost {
			s.handleAPIKeyCreate(w, r, scope)
			return
		}
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	keyName, err := url.PathUnescape(strings.TrimPrefix(sub, "/"))
	if err != nil || keyName == "" || strings.Contains(keyName, "/") {
		errJSON(w, http.StatusBadRequest, "invalid key name")
		return
	}
	if r.Method == http.MethodGet {
		s.handleAPIKeyGet(w, r, scope, keyName)
		return
	}
	if r.Method == http.MethodDelete {
		s.handleAPIKeyDelete(w, r, scope, keyName)
		return
	}
	errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
}

func (s *Server) handleAPIKeyList(w http.ResponseWriter, r *http.Request, scope *TenantScope) {
	keys, err := s.meta.ListAPIKeysByTenant(r.Context(), scope.TenantID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to list api keys")
		return
	}
	type apiKeySummary struct {
		KeyID     string     `json:"key_id"`
		KeyName   string     `json:"key_name"`
		Status    string     `json:"status"`
		IssuedAt  time.Time  `json:"issued_at"`
		RevokedAt *time.Time `json:"revoked_at,omitempty"`
	}
	items := make([]apiKeySummary, 0, len(keys))
	for _, key := range keys {
		items = append(items, apiKeySummary{
			KeyID:     key.ID,
			KeyName:   key.KeyName,
			Status:    string(key.Status),
			IssuedAt:  key.IssuedAt,
			RevokedAt: key.RevokedAt,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": items})
}

func (s *Server) handleAPIKeyCreate(w http.ResponseWriter, r *http.Request, scope *TenantScope) {
	var req struct {
		KeyName string `json:"key_name"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	keyName := strings.TrimSpace(req.KeyName)
	if keyName == "" {
		errJSON(w, http.StatusBadRequest, "key_name is required")
		return
	}
	if len(keyName) > 64 {
		errJSON(w, http.StatusBadRequest, "key_name must be <= 64 characters")
		return
	}
	if keyName == defaultTenantAPIKeyName {
		errJSON(w, http.StatusBadRequest, "key_name default is reserved")
		return
	}

	tokenVersion := managedAPITokenVersion()
	apiToken, err := token.IssueToken(s.tokenSecret, scope.TenantID, tokenVersion)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to issue token")
		return
	}
	cipherToken, err := s.pool.Encrypt(r.Context(), []byte(apiToken))
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to encrypt api key")
		return
	}
	now := time.Now().UTC()
	apiKeyID := token.NewID()
	err = s.meta.InsertAPIKey(r.Context(), &meta.APIKey{
		ID:            apiKeyID,
		TenantID:      scope.TenantID,
		KeyName:       keyName,
		JWTCiphertext: cipherToken,
		JWTHash:       token.HashToken(apiToken),
		TokenVersion:  tokenVersion,
		Status:        meta.APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	})
	if err != nil {
		if errors.Is(err, meta.ErrDuplicate) {
			errJSON(w, http.StatusConflict, "api key name already exists")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to persist api key")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"api_key":  apiToken,
		"key_id":   apiKeyID,
		"key_name": keyName,
		"status":   string(meta.APIKeyActive),
	})
}

func (s *Server) handleAPIKeyDelete(w http.ResponseWriter, r *http.Request, scope *TenantScope, keyName string) {
	if keyName == defaultTenantAPIKeyName {
		errJSON(w, http.StatusBadRequest, "default API key cannot be deleted")
		return
	}
	err := s.meta.RevokeAPIKeyByName(r.Context(), scope.TenantID, keyName, time.Now().UTC())
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "api key not found or already revoked")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to revoke api key")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":   "ok",
		"key_name": keyName,
	})
}

func (s *Server) handleAPIKeyGet(w http.ResponseWriter, r *http.Request, scope *TenantScope, keyName string) {
	key, err := s.meta.GetAPIKeyByName(r.Context(), scope.TenantID, keyName)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "api key not found")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to get api key")
		return
	}
	plain, err := s.pool.Decrypt(r.Context(), key.JWTCiphertext)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to decrypt api key")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"api_key":    string(plain),
		"key_id":     key.ID,
		"key_name":   key.KeyName,
		"status":     string(key.Status),
		"issued_at":  key.IssuedAt,
		"revoked_at": key.RevokedAt,
	})
}

func managedAPITokenVersion() int {
	version := int(time.Now().UnixNano() % 2147483647)
	if version <= 1 {
		return 2
	}
	return version
}
