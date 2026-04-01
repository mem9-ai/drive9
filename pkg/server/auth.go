package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

type scopeKey int

const tenantScopeKey scopeKey = iota

type TenantScope struct {
	TenantID     string
	APIKeyID     string
	TokenVersion int
	Backend      *backend.Dat9Backend
}

func ScopeFromContext(ctx context.Context) *TenantScope {
	s, _ := ctx.Value(tenantScopeKey).(*TenantScope)
	return s
}

func withScope(ctx context.Context, scope *TenantScope) context.Context {
	return context.WithValue(ctx, tenantScopeKey, scope)
}

func tenantAuthMiddleware(metaStore *meta.Store, pool *tenant.Pool, tokenSecret []byte, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := bearerToken(r)
		if tok == "" {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_missing_token")...)
			metricEvent(r.Context(), "auth", "result", "missing_token")
			errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		resolved, err := metaStore.ResolveByAPIKeyHash(r.Context(), tenant.HashToken(tok))
		if err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_key_not_found")...)
				metricEvent(r.Context(), "auth", "result", "key_not_found")
				errJSON(w, http.StatusUnauthorized, "invalid API key")
				return
			}
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "auth_backend_unavailable", "error", err)...)
			metricEvent(r.Context(), "auth", "result", "meta_backend_error")
			errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
			return
		}

		if subtle.ConstantTimeCompare([]byte(tenant.HashToken(tok)), []byte(resolved.APIKey.JWTHash)) != 1 {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_hash_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID)...)
			metricEvent(r.Context(), "auth", "result", "hash_mismatch")
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		if resolved.APIKey.Status != meta.APIKeyActive {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_key_inactive", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "status", resolved.APIKey.Status)...)
			metricEvent(r.Context(), "auth", "result", "key_inactive")
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		plain, err := poolDecryptToken(r.Context(), pool, resolved.APIKey.JWTCiphertext)
		if err != nil {
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "auth_decrypt_failed", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "error", err)...)
			metricEvent(r.Context(), "auth", "result", "decrypt_failed")
			errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
			return
		}
		if subtle.ConstantTimeCompare([]byte(tok), plain) != 1 {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_token_cipher_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID)...)
			metricEvent(r.Context(), "auth", "result", "cipher_mismatch")
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		claims, err := tenant.ParseAndVerifyToken(tokenSecret, tok)
		if err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_token_invalid", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "error", err)...)
			metricEvent(r.Context(), "auth", "result", "token_invalid")
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		if claims.TenantID != resolved.Tenant.ID || claims.TokenVersion != resolved.APIKey.TokenVersion {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_claims_mismatch", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID, "claim_tenant", claims.TenantID, "claim_version", claims.TokenVersion)...)
			metricEvent(r.Context(), "auth", "result", "claims_mismatch")
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		switch resolved.Tenant.Status {
		case meta.TenantActive:
		case meta.TenantProvisioning:
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_provisioning", "tenant_id", resolved.Tenant.ID)...)
			metricEvent(r.Context(), "tenant_status", "status", string(meta.TenantProvisioning))
			errJSON(w, http.StatusServiceUnavailable, "tenant is provisioning")
			return
		case meta.TenantFailed:
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_failed", "tenant_id", resolved.Tenant.ID)...)
			metricEvent(r.Context(), "tenant_status", "status", string(meta.TenantFailed))
			errJSON(w, http.StatusServiceUnavailable, "tenant provisioning failed")
			return
		case meta.TenantSuspended, meta.TenantDeleted:
			pool.Invalidate(resolved.Tenant.ID)
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_blocked", "tenant_id", resolved.Tenant.ID, "status", resolved.Tenant.Status)...)
			metricEvent(r.Context(), "tenant_status", "status", string(resolved.Tenant.Status))
			errJSON(w, http.StatusForbidden, "tenant is suspended")
			return
		default:
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "tenant_unavailable", "tenant_id", resolved.Tenant.ID, "status", resolved.Tenant.Status)...)
			metricEvent(r.Context(), "tenant_status", "status", string(resolved.Tenant.Status))
			errJSON(w, http.StatusForbidden, "tenant is unavailable")
			return
		}

		b, release, err := pool.Acquire(r.Context(), &resolved.Tenant)
		if err != nil {
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "backend_load_failed", "tenant_id", resolved.Tenant.ID, "error", err)...)
			metricEvent(r.Context(), "tenant_backend", "result", "load_failed")
			errJSON(w, http.StatusInternalServerError, "backend unavailable")
			return
		}
		defer release()
		metricEvent(r.Context(), "auth", "result", "ok")

		scope := &TenantScope{TenantID: resolved.Tenant.ID, APIKeyID: resolved.APIKey.ID, TokenVersion: resolved.APIKey.TokenVersion, Backend: b}
		next.ServeHTTP(w, r.WithContext(withScope(r.Context(), scope)))
	})
}

func poolDecryptToken(ctx context.Context, pool *tenant.Pool, cipher []byte) ([]byte, error) {
	// Decrypt is tenant-independent and uses pool encryptor shared for API key storage.
	// Keep this helper to avoid exposing raw encryptor in handlers.
	return pool.Decrypt(ctx, cipher)
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		h = r.Header.Get("X-API-Key")
		if h != "" {
			return strings.TrimSpace(h)
		}
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
