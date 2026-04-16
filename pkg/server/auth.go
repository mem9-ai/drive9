package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/tidbcloudnative"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
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

func ScopeFromContext(ctx context.Context) *TenantScope {
	s, _ := ctx.Value(tenantScopeKey).(*TenantScope)
	return s
}

func withScope(ctx context.Context, scope *TenantScope) context.Context {
	return context.WithValue(ctx, tenantScopeKey, scope)
}

func authPhaseMs(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func tenantAuthMiddleware(metaStore *meta.Store, pool *tenant.Pool, tokenSecret []byte, provisioner tenant.Provisioner, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authStart := time.Now()
		// Try tidbcloud-native header auth first.
		if scope, release, handled := nativeAuthScope(w, r, metaStore, pool, provisioner); handled {
			if scope == nil {
				// nativeAuthScope wrote the error response already.
				return
			}
			if release != nil {
				defer release()
			}
			next.ServeHTTP(w, r.WithContext(withScope(r.Context(), scope)))
			return
		}

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
		tenantID, err := peekCapTokenTenantID(tok)
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

func peekCapTokenTenantID(raw string) (string, error) {
	return vault.PeekCapTokenTenantID(raw)
}

func poolDecryptToken(ctx context.Context, pool *tenant.Pool, cipher []byte) ([]byte, error) {
	// Decrypt is tenant-independent and uses pool encryptor shared for API key storage.
	// Keep this helper to avoid exposing raw encryptor in handlers.
	return pool.Decrypt(ctx, cipher)
}

// authorizeNativeTarget asserts the provisioner is a tidbcloudnative.Provisioner
// and verifies/authorizes the resolved target. On failure it writes an HTTP error
// to w and returns (nil, false).
//
// TODO: consider caching VerifyZeroInstance results with a short TTL to avoid
// per-request RPCs on the hot path.
func authorizeNativeTarget(ctx context.Context, w http.ResponseWriter, r *http.Request, provisioner tenant.Provisioner, target *tidbcloud.ResolvedTarget) (*tidbcloudnative.Provisioner, bool) {
	np, ok := provisioner.(*tidbcloudnative.Provisioner)
	if !ok {
		logger.Warn(ctx, "server_event", eventFields(ctx, "native_not_configured")...)
		metricEvent(ctx, "native_auth", "result", "not_configured")
		errJSON(w, http.StatusNotImplemented, fmt.Sprintf("unsupported %s header", tidbcloud.HeaderForTarget(target.Type)))
		return nil, false
	}

	switch target.Type {
	case tidbcloud.TargetZeroInstance:
		zeroInfo, err := np.VerifyZeroInstance(ctx, target.InstanceID)
		if err != nil {
			if tidbcloud.IsNotFound(err) {
				logger.Warn(ctx, "server_event", eventFields(ctx, "native_instance_not_found", "instance_id", target.InstanceID)...)
				metricEvent(ctx, "native_auth", "result", "instance_not_found")
				errJSON(w, http.StatusNotFound, err.Error())
				return nil, false
			}
			logger.Error(ctx, "server_event", eventFields(ctx, "native_verify_instance_failed", "instance_id", target.InstanceID, "error", err)...)
			metricEvent(ctx, "native_auth", "result", "verify_instance_failed")
			errJSON(w, http.StatusBadGateway, fmt.Sprintf("verify instance failed: %v", err))
			return nil, false
		}
		// Stash only non-sensitive zero instance info in the request
		// context so the provision handler can read the expiration
		// timestamp without exposing plaintext credentials.
		if zeroInfo != nil {
			sanitized := *zeroInfo
			sanitized.Username = ""
			sanitized.Password = ""
			*r = *r.WithContext(contextWithZeroInfo(r.Context(), &sanitized))
		}
	case tidbcloud.TargetCluster:
		if authErr := np.Authorize(ctx, r, target.ClusterID); authErr != nil {
			if status, ok := tidbcloud.IsAuthError(authErr); ok {
				logger.Warn(ctx, "server_event", eventFields(ctx, "native_auth_failed", "cluster_id", target.ClusterID, "error", authErr)...)
				metricEvent(ctx, "native_auth", "result", "auth_failed")
				errJSON(w, status, authErr.Error())
				return nil, false
			}
			logger.Error(ctx, "server_event", eventFields(ctx, "native_auth_failed", "cluster_id", target.ClusterID, "error", authErr)...)
			metricEvent(ctx, "native_auth", "result", "auth_failed")
			errJSON(w, http.StatusForbidden, authErr.Error())
			return nil, false
		}
	}

	return np, true
}

// nativeAuthScope checks for tidbcloud-native headers and returns a TenantScope
// if native auth succeeds. Returns (nil, nil, true) if headers were present but auth
// failed (error already written to w). Returns (nil, nil, false) if no native headers.
func nativeAuthScope(w http.ResponseWriter, r *http.Request, metaStore *meta.Store, pool *tenant.Pool, provisioner tenant.Provisioner) (*TenantScope, func(), bool) {
	target, err := tidbcloud.ParseHeaders(r)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "auth_bad_tidbcloud_header", "error", err)...)
		metricEvent(r.Context(), "auth", "result", "bad_tidbcloud_header")
		errJSON(w, http.StatusBadRequest, err.Error())
		return nil, nil, true
	}
	if target == nil {
		return nil, nil, false
	}

	ctx := r.Context()
	if _, ok := authorizeNativeTarget(ctx, w, r, provisioner, target); !ok {
		return nil, nil, true
	}

	tenantID := target.ClusterID
	t, err := metaStore.GetTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			logger.Warn(ctx, "server_event", eventFields(ctx, "auth_native_tenant_not_found", "tenant_id", tenantID)...)
			metricEvent(ctx, "auth", "result", "tenant_not_found")
			errJSON(w, http.StatusNotFound, "tenant not found")
			return nil, nil, true
		}
		logger.Error(ctx, "server_event", eventFields(ctx, "auth_native_meta_error", "tenant_id", tenantID, "error", err)...)
		metricEvent(ctx, "auth", "result", "meta_error")
		errJSON(w, http.StatusInternalServerError, "meta backend unavailable")
		return nil, nil, true
	}

	// If the tenant has an expiration timestamp and it has passed, mark
	// the tenant as deleted so the underlying (destroyed) resources are
	// no longer accessed.
	if t.ClaimExpiresAt != nil && time.Now().After(*t.ClaimExpiresAt) && t.Status != meta.TenantDeleted {
		logger.Info(ctx, "server_event", eventFields(ctx, "tenant_expired_auto_delete",
			"tenant_id", t.ID, "claim_expires_at", t.ClaimExpiresAt.Format(time.RFC3339))...)
		if err := metaStore.UpdateTenantStatus(ctx, t.ID, meta.TenantDeleted); err != nil {
			logger.Error(ctx, "server_event", eventFields(ctx, "tenant_expired_update_status_failed",
				"tenant_id", t.ID, "error", err)...)
			metricEvent(ctx, "tenant_status", "status", "expired_delete_failed")
			errJSON(w, http.StatusInternalServerError, "failed to update expired tenant")
			return nil, nil, true
		}
		pool.Invalidate(t.ID)
		metricEvent(ctx, "tenant_status", "status", "expired_deleted")
		errJSON(w, http.StatusForbidden, "tenant has expired")
		return nil, nil, true
	}

	switch t.Status {
	case meta.TenantActive:
	case meta.TenantProvisioning:
		logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_provisioning", "tenant_id", t.ID)...)
		metricEvent(ctx, "tenant_status", "status", string(meta.TenantProvisioning))
		errJSON(w, http.StatusServiceUnavailable, "tenant is provisioning")
		return nil, nil, true
	case meta.TenantFailed:
		logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_failed", "tenant_id", t.ID)...)
		metricEvent(ctx, "tenant_status", "status", string(meta.TenantFailed))
		errJSON(w, http.StatusServiceUnavailable, "tenant provisioning failed")
		return nil, nil, true
	case meta.TenantSuspended, meta.TenantDeleted:
		pool.Invalidate(t.ID)
		logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_blocked", "tenant_id", t.ID, "status", t.Status)...)
		metricEvent(ctx, "tenant_status", "status", string(t.Status))
		errJSON(w, http.StatusForbidden, "tenant is suspended")
		return nil, nil, true
	default:
		logger.Warn(ctx, "server_event", eventFields(ctx, "tenant_unavailable", "tenant_id", t.ID, "status", t.Status)...)
		metricEvent(ctx, "tenant_status", "status", string(t.Status))
		errJSON(w, http.StatusForbidden, "tenant is unavailable")
		return nil, nil, true
	}

	b, release, err := pool.Acquire(ctx, t)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "backend_load_failed", "tenant_id", t.ID, "error", err)...)
		metricEvent(ctx, "tenant_backend", "result", "load_failed")
		errJSON(w, http.StatusInternalServerError, "backend unavailable")
		return nil, nil, true
	}
	metricEvent(ctx, "auth", "result", "ok_native")

	return &TenantScope{TenantID: t.ID, Backend: b}, release, true
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

// zeroInfoKey is a context key for stashing ZeroInstanceInfo resolved during
// authorizeNativeTarget so the provision handler can read the expiration.
type zeroInfoKeyType struct{}

var zeroInfoKey = zeroInfoKeyType{}

func contextWithZeroInfo(ctx context.Context, info *tidbcloud.ZeroInstanceInfo) context.Context {
	return context.WithValue(ctx, zeroInfoKey, info)
}

func zeroInfoFromContext(ctx context.Context) *tidbcloud.ZeroInstanceInfo {
	v, _ := ctx.Value(zeroInfoKey).(*tidbcloud.ZeroInstanceInfo)
	return v
}
