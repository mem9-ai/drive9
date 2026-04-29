package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"github.com/mem9-ai/dat9/pkg/tenantctx"
	"github.com/mem9-ai/dat9/pkg/vault"
	"go.uber.org/zap"
)

type scopeKey int

const tenantScopeKey scopeKey = iota

type TenantScope struct {
	TenantID     string
	APIKeyID     string
	TokenVersion int
	Provider     string
	Backend      *backend.Dat9Backend
}

const statusClientClosedRequest = 499

func ScopeFromContext(ctx context.Context) *TenantScope {
	s, _ := ctx.Value(tenantScopeKey).(*TenantScope)
	return s
}

func withScope(ctx context.Context, scope *TenantScope) context.Context {
	ctx = context.WithValue(ctx, tenantScopeKey, scope)
	if scope == nil {
		return ctx
	}
	return tenantctx.WithTenantID(ctx, scope.TenantID)
}

func authPhaseMs(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func isClientCanceled(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled)
}

func logAuthClientCanceled(ctx context.Context, event string, kv ...any) {
	logger.Info(ctx, "server_event", eventFields(ctx, event, kv...)...)
	metricEvent(ctx, "auth", "result", "client_canceled")
}

func writeClientCanceled(w http.ResponseWriter) {
	w.WriteHeader(statusClientClosedRequest)
}

func tenantAuthMiddleware(metaStore *meta.Store, pool *tenant.Pool, tokenSecret []byte, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authStart := time.Now()
		tok := bearerToken(r)
		if tok == "" {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_missing_token")...)
			metricEvent(r.Context(), "auth", "result", "missing_token")
			errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		resolveStart := time.Now()
		resolved, err := metaStore.ResolveByAPIKeyHash(r.Context(), token.HashToken(tok))
		resolveDurationMs := authPhaseMs(resolveStart)
		if err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_key_not_found")...)
				metricEvent(r.Context(), "auth", "result", "key_not_found")
				errJSON(w, http.StatusUnauthorized, "invalid API key")
				return
			}
			if isClientCanceled(r.Context(), err) {
				logAuthClientCanceled(r.Context(), "auth_client_canceled")
				writeClientCanceled(w)
				return
			}
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "auth_backend_unavailable", "error", err)...)
			metricEvent(r.Context(), "auth", "result", "meta_backend_error")
			errJSON(w, http.StatusInternalServerError, "auth backend unavailable")
			return
		}

		if subtle.ConstantTimeCompare([]byte(token.HashToken(tok)), []byte(resolved.APIKey.JWTHash)) != 1 {
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

		decryptStart := time.Now()
		plain, err := poolDecryptToken(r.Context(), pool, resolved.APIKey.JWTCiphertext)
		decryptDurationMs := authPhaseMs(decryptStart)
		if err != nil {
			if isClientCanceled(r.Context(), err) {
				logAuthClientCanceled(r.Context(), "auth_decrypt_client_canceled", "tenant_id", resolved.Tenant.ID, "api_key_id", resolved.APIKey.ID)
				writeClientCanceled(w)
				return
			}
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

		verifyStart := time.Now()
		claims, err := token.ParseAndVerifyToken(tokenSecret, tok)
		verifyDurationMs := authPhaseMs(verifyStart)
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

		acquireStart := time.Now()
		b, release, err := pool.Acquire(r.Context(), &resolved.Tenant)
		acquireDurationMs := authPhaseMs(acquireStart)
		if err != nil {
			if isClientCanceled(r.Context(), err) {
				logAuthClientCanceled(r.Context(), "backend_load_client_canceled", "tenant_id", resolved.Tenant.ID)
				writeClientCanceled(w)
				return
			}
			logger.Error(r.Context(), "server_event", eventFields(r.Context(), "backend_load_failed", "tenant_id", resolved.Tenant.ID, "error", err)...)
			metricEvent(r.Context(), "tenant_backend", "result", "load_failed")
			errJSON(w, http.StatusInternalServerError, "backend unavailable")
			return
		}
		defer release()
		metricEvent(r.Context(), "auth", "result", "ok")
		logger.InfoBenchTiming(r.Context(), "tenant_auth_timing",
			zap.String("path", r.URL.Path),
			zap.String("method", r.Method),
			zap.String("tenant_id", resolved.Tenant.ID),
			zap.String("api_key_id", resolved.APIKey.ID),
			zap.Float64("resolve_api_key_hash_ms", resolveDurationMs),
			zap.Float64("decrypt_token_ms", decryptDurationMs),
			zap.Float64("verify_token_ms", verifyDurationMs),
			zap.Float64("pool_acquire_ms", acquireDurationMs),
			zap.Float64("total_ms", authPhaseMs(authStart)),
		)

		scope := &TenantScope{TenantID: resolved.Tenant.ID, APIKeyID: resolved.APIKey.ID, TokenVersion: resolved.APIKey.TokenVersion, Provider: resolved.Tenant.Provider, Backend: b}
		next.ServeHTTP(w, r.WithContext(withScope(r.Context(), scope)))
	})
}

// capabilityAuthMiddleware returns a handler that resolves the tenant backend
// from a capability token's tenant_id claim. It does NOT do full token verification
// (signature, TTL, revocation) — that is handled by handleVaultRead itself.
// This middleware only exists to load the correct tenant DB so the handler can
// access vault tables.
func (s *Server) capabilityAuthMiddleware(metaStore *meta.Store, pool *tenant.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := bearerToken(r)
		if tok == "" {
			errJSON(w, http.StatusUnauthorized, "missing capability token")
			return
		}

		// Peek at claims to get tenant_id. We only need the payload, not
		// full HMAC verification (handleVaultRead does that).
		// Support both legacy cap tokens (vault_ prefix) and grant tokens (vt_ prefix).
		tenantID, err := peekTokenTenantID(tok)
		if err != nil {
			errJSON(w, http.StatusUnauthorized, "invalid capability token")
			return
		}

		// Look up tenant and load backend. Use a uniform 401 for all
		// failure modes to avoid leaking tenant existence or status to
		// unauthenticated callers (tenant_id is from unverified peek).
		tenant, err := metaStore.GetTenant(r.Context(), tenantID)
		if err != nil || tenant.Status != meta.TenantActive {
			errJSON(w, http.StatusUnauthorized, "invalid capability token")
			return
		}

		b, release, err := pool.Acquire(r.Context(), tenant)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "backend unavailable")
			return
		}
		defer release()

		scope := &TenantScope{TenantID: tenantID, Provider: tenant.Provider, Backend: b}
		sub := strings.TrimPrefix(r.URL.Path, "/v1/vault/read")
		s.handleVaultRead(w, r.WithContext(withScope(r.Context(), scope)), sub)
	})
}

// peekTokenTenantID extracts tenant_id from either a legacy cap token (vault_
// prefix) or a grant token (vt_ prefix). Used by capabilityAuthMiddleware for
// pre-auth tenant routing.
func peekTokenTenantID(raw string) (string, error) {
	if strings.HasPrefix(raw, "vt_") {
		return vault.PeekGrantTenantID(raw)
	}
	return vault.PeekCapTokenTenantID(raw)
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
