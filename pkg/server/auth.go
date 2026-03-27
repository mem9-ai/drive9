package server

import (
	"context"
	"net/http"
	"strings"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

type contextKey int

const backendKey contextKey = iota

// BackendFromCtx extracts the tenant-scoped backend from the request context.
// Returns nil if no backend is set (e.g. control plane routes).
func BackendFromCtx(ctx context.Context) *backend.Dat9Backend {
	b, _ := ctx.Value(backendKey).(*backend.Dat9Backend)
	return b
}

func withBackend(ctx context.Context, b *backend.Dat9Backend) context.Context {
	return context.WithValue(ctx, backendKey, b)
}

// tenantAuthMiddleware resolves a Bearer token to a tenant backend.
// Flow: extract token → SHA-256 hash → lookup tenant → check status → pool.Get → inject into context.
func tenantAuthMiddleware(tenants *tenant.Store, pool *tenant.Pool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r)
		if token == "" {
			errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		hash := tenant.HashAPIKey(token)
		t, err := tenants.GetByAPIKeyHash(hash)
		if err != nil {
			// Don't leak whether tenant exists — same error for invalid and not-found
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		switch t.Status {
		case tenant.StatusActive:
			// proceed
		case tenant.StatusProvisioning:
			errJSON(w, http.StatusServiceUnavailable, "tenant is provisioning")
			return
		case tenant.StatusSuspended, tenant.StatusDeleted:
			pool.Invalidate(t.ID)
			errJSON(w, http.StatusForbidden, "tenant is suspended")
			return
		default:
			errJSON(w, http.StatusForbidden, "tenant status: "+string(t.Status))
			return
		}

		b, err := pool.Get(t)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "backend unavailable")
			return
		}

		next.ServeHTTP(w, r.WithContext(withBackend(r.Context(), b)))
	})
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
