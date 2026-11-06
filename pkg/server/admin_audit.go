package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
)

// adminAuditAction returns the stable audit action for a mutating
// control-plane request, or "" when the request is a read / not audited.
//
// Keeping this table next to the route wiring makes new mutating endpoints easy
// to cover: an endpoint without an action is silently unaudited, so add it here
// when the route is added. The finding this file answers was exactly that —
// destructive operations (tenant delete, quota set, object backends, forks) left
// no actor-attributed record.
func adminAuditAction(method, path string) string {
	path = strings.TrimSuffix(path, "/")
	switch {
	case path == "/v1/provision" && method == http.MethodPost:
		return "tenant.create"
	case path == "/v1/quota" && method == http.MethodPost:
		return "quota.set"
	case path == "/v1/tenant" && method == http.MethodDelete:
		return "tenant.delete_self"
	case path == "/v1/fork" && method == http.MethodDelete:
		return "fork.delete"
	case path == "/v1/admin/tenants" && method == http.MethodPost:
		return "tenant.create"
	case strings.HasPrefix(path, "/v1/admin/tenants/"):
		return adminTenantAuditAction(method, strings.TrimPrefix(path, "/v1/admin/tenants/"))
	case path == "/v1/admin/tenant-pool":
		switch method {
		case http.MethodPost:
			return "tenant_pool.create"
		case http.MethodPatch:
			return "tenant_pool.update"
		case http.MethodDelete:
			return "tenant_pool.delete"
		}
	case path == "/v1/admin/object-backends" && method == http.MethodPost:
		return "object_backend.create"
	case strings.HasPrefix(path, "/v1/admin/object-backends/"):
		switch method {
		case http.MethodPatch, http.MethodPut:
			return "object_backend.update"
		case http.MethodDelete:
			return "object_backend.delete"
		}
	}
	return ""
}

// adminTenantAuditAction covers the per-tenant admin routes.
func adminTenantAuditAction(method, rest string) string {
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	switch {
	case len(parts) == 1 && parts[0] != "":
		switch method {
		case http.MethodDelete:
			return "tenant.delete"
		case http.MethodPatch:
			return "tenant.update"
		}
	case len(parts) == 2:
		switch parts[1] {
		case "quota":
			if method == http.MethodPost {
				return "tenant.quota.set"
			}
		case "object-namespace":
			switch method {
			case http.MethodPut:
				return "tenant.object_namespace.update"
			case http.MethodDelete:
				return "tenant.object_namespace.delete"
			}
		}
	}
	return ""
}

// adminAuditActor resolves who performed the request from what this server
// already knows about the caller: the TiDB Cloud organization recorded on the
// request scope, then the tenant/api-key scope, then the public key the caller
// presented. The public key is not a secret and is still worth recording on a
// refused request — that is the audit of an attempt.
func adminAuditActor(r *http.Request) (actorKind, orgID, apiKeyID, publicKey string) {
	tenantID, apiKeyID, _, scopeOrgID := requestMetricScope(r.Context())
	_ = tenantID
	if scopeOrgID != "" {
		return "control_plane", scopeOrgID, apiKeyID, ""
	}
	if apiKeyID != "" {
		return "tenant_api_key", "", apiKeyID, ""
	}
	if key := strings.TrimSpace(r.Header.Get(quotaPublicKeyHeader)); key != "" {
		return "tidbcloud_api_key", "", "", key
	}
	return "unknown", "", "", ""
}

// adminAuditMiddleware emits one structured admin_audit record for every mutating
// control-plane request, refused or not.
//
// It wraps the handler rather than sitting inside auth: a mutation rejected before
// the handler ran (missing or forged key, wrong role, free-plan gate) is exactly
// the attempt an audit trail exists for, and an inner middleware would never see
// it.
func (s *Server) adminAuditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := adminAuditAction(r.Method, r.URL.Path)
		if action == "" {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		recorder, ok := w.(*observedResponseWriter)
		if !ok {
			recorder = &observedResponseWriter{ResponseWriter: w, status: http.StatusOK}
		}
		next.ServeHTTP(recorder, r)

		status := recorder.status
		if status == 0 {
			// The handler answered without an explicit status.
			status = http.StatusOK
		}
		result := "accepted"
		if status >= http.StatusBadRequest {
			result = "failed"
		}
		actorKind, orgID, apiKeyID, publicKey := adminAuditActor(r)
		logger.Info(r.Context(), "admin_audit", eventFields(r.Context(), "admin_audit",
			"action", action,
			"target", r.URL.Path,
			"method", r.Method,
			"status", status,
			"result", result,
			"actor_kind", actorKind,
			"actor_org_id", orgID,
			"actor_api_key_id", apiKeyID,
			"actor_public_key", publicKey,
			"remote", r.RemoteAddr,
			"duration_ms", durationMillis(started),
		)...)
	})
}
