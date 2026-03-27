package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/mem9-ai/dat9/pkg/backend"
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
			errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		resolved, err := metaStore.ResolveByAPIKeyHash(tenant.HashToken(tok))
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

		plain, err := poolDecryptToken(pool, resolved.APIKey.JWTCiphertext)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
			return
		}
		if subtle.ConstantTimeCompare([]byte(tok), plain) != 1 {
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		claims, err := tenant.ParseAndVerifyToken(tokenSecret, tok)
		if err != nil {
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		if claims.TenantID != resolved.Tenant.ID || claims.TokenVersion != resolved.APIKey.TokenVersion {
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		switch resolved.Tenant.Status {
		case meta.TenantActive:
		case meta.TenantProvisioning:
			errJSON(w, http.StatusServiceUnavailable, "tenant is provisioning")
			return
		case meta.TenantFailed:
			errJSON(w, http.StatusServiceUnavailable, "tenant provisioning failed")
			return
		case meta.TenantSuspended, meta.TenantDeleted:
			pool.Invalidate(resolved.Tenant.ID)
			errJSON(w, http.StatusForbidden, "tenant is suspended")
			return
		default:
			errJSON(w, http.StatusForbidden, "tenant is unavailable")
			return
		}

		b, err := pool.Get(&resolved.Tenant)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "backend unavailable")
			return
		}

		scope := &TenantScope{TenantID: resolved.Tenant.ID, APIKeyID: resolved.APIKey.ID, TokenVersion: resolved.APIKey.TokenVersion, Backend: b}
		next.ServeHTTP(w, r.WithContext(withScope(r.Context(), scope)))
	})
}

func poolDecryptToken(pool *tenant.Pool, cipher []byte) ([]byte, error) {
	// Decrypt is tenant-independent and uses pool encryptor shared for API key storage.
	// Keep this helper to avoid exposing raw encryptor in handlers.
	return pool.Decrypt(cipher)
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
