package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/vault"
)

// handleVault dispatches /v1/vault/* requests.
func (s *Server) handleVault(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/vault/")

	// Management API: /v1/vault/secrets, /v1/vault/tokens, /v1/vault/audit
	// Authenticated by tenant token (existing auth middleware).
	if strings.HasPrefix(rest, "secrets") {
		s.handleVaultSecrets(w, r, strings.TrimPrefix(rest, "secrets"))
		return
	}
	if strings.HasPrefix(rest, "tokens") {
		s.handleVaultTokens(w, r, strings.TrimPrefix(rest, "tokens"))
		return
	}
	if rest == "audit" || strings.HasPrefix(rest, "audit") {
		s.handleVaultAudit(w, r)
		return
	}

	// /v1/vault/read/* is routed separately via capabilityAuthMiddleware.
	errJSON(w, http.StatusNotFound, "not found")
}

// vaultStore returns the vault store for the current tenant.
func (s *Server) vaultStore(r *http.Request) (*vault.Store, error) {
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.Backend == nil {
		return nil, fmt.Errorf("no tenant scope")
	}
	if s.vaultMK == nil {
		return nil, fmt.Errorf("vault master key not configured")
	}
	return vault.NewStore(scope.Backend.Store().DB(), s.vaultMK), nil
}

// ---- Management API: /v1/vault/secrets ----

func (s *Server) handleVaultSecrets(w http.ResponseWriter, r *http.Request, sub string) {
	vs, err := s.vaultStore(r)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	scope := ScopeFromContext(r.Context())
	tenantID := scope.TenantID

	// /v1/vault/secrets (no name)
	if sub == "" || sub == "/" {
		switch r.Method {
		case http.MethodPost:
			s.handleVaultSecretCreate(w, r, vs, tenantID)
		case http.MethodGet:
			s.handleVaultSecretList(w, r, vs, tenantID)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	// /v1/vault/secrets/{name}
	name := strings.TrimPrefix(sub, "/")
	switch r.Method {
	case http.MethodGet:
		s.handleVaultSecretGet(w, r, vs, tenantID, name)
	case http.MethodPut:
		s.handleVaultSecretUpdate(w, r, vs, tenantID, name)
	case http.MethodDelete:
		s.handleVaultSecretDelete(w, r, vs, tenantID, name)
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleVaultSecretCreate(w http.ResponseWriter, r *http.Request, vs *vault.Store, tenantID string) {
	var req struct {
		Name       string            `json:"name"`
		SecretType string            `json:"secret_type"`
		Fields     map[string]string `json:"fields"`
		CreatedBy  string            `json:"created_by"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Name == "" {
		errJSON(w, http.StatusBadRequest, "name is required")
		return
	}
	if len(req.Fields) == 0 {
		errJSON(w, http.StatusBadRequest, "fields are required")
		return
	}
	if req.CreatedBy == "" {
		req.CreatedBy = "api"
	}
	secretType := vault.SecretType(req.SecretType)
	if secretType == "" {
		secretType = vault.SecretTypeGeneric
	}

	fields := make(map[string][]byte, len(req.Fields))
	for k, v := range req.Fields {
		fields[k] = []byte(v)
	}

	sec, err := vs.CreateSecret(r.Context(), tenantID, req.Name, req.CreatedBy, secretType, fields)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			errJSON(w, http.StatusConflict, "secret already exists")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to create secret")
		return
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:   tenantID,
		EventType:  "secret.created",
		SecretName: req.Name,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(sec)
}

func (s *Server) handleVaultSecretList(w http.ResponseWriter, r *http.Request, vs *vault.Store, tenantID string) {
	secrets, err := vs.ListSecrets(r.Context(), tenantID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to list secrets")
		return
	}
	if secrets == nil {
		secrets = []*vault.Secret{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"secrets": secrets})
}

func (s *Server) handleVaultSecretGet(w http.ResponseWriter, r *http.Request, vs *vault.Store, tenantID, name string) {
	sec, err := vs.GetSecret(r.Context(), tenantID, name)
	if err != nil {
		if errors.Is(err, vault.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "secret not found")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to get secret")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sec)
}

func (s *Server) handleVaultSecretUpdate(w http.ResponseWriter, r *http.Request, vs *vault.Store, tenantID, name string) {
	var req struct {
		Fields    map[string]string `json:"fields"`
		UpdatedBy string            `json:"updated_by"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if len(req.Fields) == 0 {
		errJSON(w, http.StatusBadRequest, "fields are required")
		return
	}
	if req.UpdatedBy == "" {
		req.UpdatedBy = "api"
	}

	fields := make(map[string][]byte, len(req.Fields))
	for k, v := range req.Fields {
		fields[k] = []byte(v)
	}

	sec, err := vs.UpdateSecret(r.Context(), tenantID, name, req.UpdatedBy, fields)
	if err != nil {
		if errors.Is(err, vault.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "secret not found")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to update secret")
		return
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:   tenantID,
		EventType:  "secret.rotated",
		SecretName: name,
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sec)
}

func (s *Server) handleVaultSecretDelete(w http.ResponseWriter, r *http.Request, vs *vault.Store, tenantID, name string) {
	err := vs.DeleteSecret(r.Context(), tenantID, name)
	if err != nil {
		if errors.Is(err, vault.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "secret not found")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to delete secret")
		return
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:   tenantID,
		EventType:  "secret.deleted",
		SecretName: name,
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ---- Management API: /v1/vault/tokens ----

func (s *Server) handleVaultTokens(w http.ResponseWriter, r *http.Request, sub string) {
	vs, err := s.vaultStore(r)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	scope := ScopeFromContext(r.Context())
	tenantID := scope.TenantID

	if sub == "" || sub == "/" {
		if r.Method == http.MethodPost {
			s.handleVaultTokenIssue(w, r, vs, tenantID)
			return
		}
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// DELETE /v1/vault/tokens/{token_id}
	tokenID := strings.TrimPrefix(sub, "/")
	if r.Method == http.MethodDelete {
		s.handleVaultTokenRevoke(w, r, vs, tenantID, tokenID)
		return
	}
	errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
}

func (s *Server) handleVaultTokenIssue(w http.ResponseWriter, r *http.Request, vs *vault.Store, tenantID string) {
	var req struct {
		AgentID string   `json:"agent_id"`
		TaskID  string   `json:"task_id"`
		Scope   []string `json:"scope"`
		TTLSecs int      `json:"ttl_seconds"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.AgentID == "" {
		errJSON(w, http.StatusBadRequest, "agent_id is required")
		return
	}
	if len(req.Scope) == 0 {
		errJSON(w, http.StatusBadRequest, "scope is required")
		return
	}
	if err := vault.ValidateScope(req.Scope); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	ttl := time.Duration(req.TTLSecs) * time.Second
	if ttl <= 0 {
		ttl = time.Hour // default 1 hour
	}

	tokenStr, capToken, err := vs.IssueCapToken(r.Context(), tenantID, req.AgentID, req.TaskID, req.Scope, ttl)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to issue token")
		return
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:  tenantID,
		EventType: "token.issued",
		TokenID:   capToken.TokenID,
		AgentID:   req.AgentID,
		TaskID:    req.TaskID,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":      tokenStr,
		"token_id":   capToken.TokenID,
		"expires_at": capToken.ExpiresAt,
	})
}

func (s *Server) handleVaultTokenRevoke(w http.ResponseWriter, r *http.Request, vs *vault.Store, tenantID, tokenID string) {
	var req struct {
		RevokedBy string `json:"revoked_by"`
		Reason    string `json:"reason"`
	}
	// Body is optional for DELETE.
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	if req.RevokedBy == "" {
		req.RevokedBy = "api"
	}

	err := vs.RevokeCapToken(r.Context(), tenantID, tokenID, req.RevokedBy, req.Reason)
	if err != nil {
		if errors.Is(err, vault.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "token not found or already revoked")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to revoke token")
		return
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:  tenantID,
		EventType: "token.revoked",
		TokenID:   tokenID,
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ---- Management API: /v1/vault/audit ----

func (s *Server) handleVaultAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vs, err := s.vaultStore(r)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	scope := ScopeFromContext(r.Context())

	secretName := r.URL.Query().Get("secret")
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	events, err := vs.QueryAuditLog(r.Context(), scope.TenantID, secretName, limit)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to query audit log")
		return
	}
	if events == nil {
		events = []*vault.AuditEvent{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"events": events})
}

// ---- Consumption API: /v1/vault/read ----
// Authenticated by capability token (NOT tenant token).

func (s *Server) handleVaultRead(w http.ResponseWriter, r *http.Request, sub string) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract capability token from Authorization header.
	raw := bearerToken(r)
	if raw == "" {
		errJSON(w, http.StatusUnauthorized, "missing capability token")
		return
	}

	// We need a tenant scope to get the DB. For the consumption API,
	// we extract tenant_id from the token itself, but we need the tenant's
	// backend to access the vault DB. Check if tenant scope is already set
	// (management auth middleware), otherwise we need the cap token's tenant_id.
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.Backend == nil {
		errJSON(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenantID := scope.TenantID

	if s.vaultMK == nil {
		errJSON(w, http.StatusInternalServerError, "vault master key not configured")
		return
	}

	vs := vault.NewStore(scope.Backend.Store().DB(), s.vaultMK)

	// Full 4-step verification: HMAC signature → TTL → DB revocation → claims.
	// IMPORTANT: Do NOT write audit events before verification succeeds.
	// The tenant_id comes from an unverified peek and could be forged;
	// writing to tenant audit before proving token authenticity would let
	// an attacker inject events into any tenant's audit log.
	claims, err := vs.VerifyAndResolveCapToken(r.Context(), tenantID, raw)
	if err != nil {
		// Log to server-level observability only — not tenant audit.
		if strings.Contains(err.Error(), "expired") {
			errJSON(w, http.StatusUnauthorized, "token expired")
		} else if strings.Contains(err.Error(), "revoked") {
			errJSON(w, http.StatusUnauthorized, "token revoked")
		} else {
			errJSON(w, http.StatusUnauthorized, "invalid capability token")
		}
		return
	}

	// Parse path: /v1/vault/read, /v1/vault/read/{name}, /v1/vault/read/{name}/{field}
	sub = strings.TrimPrefix(sub, "/")
	if sub == "" {
		// Enumerate: list secrets in scope.
		s.handleVaultReadEnumerate(w, r, vs, claims)
		return
	}

	parts := strings.SplitN(sub, "/", 2)
	secretName := parts[0]
	fieldName := ""
	if len(parts) > 1 {
		fieldName = parts[1]
	}

	if fieldName != "" {
		s.handleVaultReadField(w, r, vs, claims, secretName, fieldName)
	} else {
		s.handleVaultReadSecret(w, r, vs, claims, secretName)
	}
}

func (s *Server) handleVaultReadEnumerate(w http.ResponseWriter, r *http.Request, vs *vault.Store, claims *vault.CapTokenClaims) {
	scopedNames := vault.ScopedSecretNames(claims.Scope)
	// Filter to secrets that actually exist.
	var available []string
	for _, name := range scopedNames {
		_, err := vs.GetSecret(r.Context(), claims.TenantID, name)
		if err == nil {
			available = append(available, name)
		}
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:  claims.TenantID,
		EventType: "secret.list",
		TokenID:   claims.TokenID,
		AgentID:   claims.AgentID,
		TaskID:    claims.TaskID,
		Adapter:   "api",
	})

	w.Header().Set("Content-Type", "application/json")
	if available == nil {
		available = []string{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"secrets": available})
}

func (s *Server) handleVaultReadSecret(w http.ResponseWriter, r *http.Request, vs *vault.Store, claims *vault.CapTokenClaims, secretName string) {
	// Scope check.
	allFields, allowedFields := vault.AllowedFields(claims.Scope, secretName)
	if !allFields && len(allowedFields) == 0 {
		_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
			TenantID:   claims.TenantID,
			EventType:  "secret.denied",
			TokenID:    claims.TokenID,
			AgentID:    claims.AgentID,
			SecretName: secretName,
			Detail:     map[string]string{"reason": "out_of_scope"},
		})
		// Return 404 to avoid leaking secret existence.
		errJSON(w, http.StatusNotFound, "not found")
		return
	}

	fields, err := vs.ReadSecretFields(r.Context(), claims.TenantID, secretName)
	if err != nil {
		if errors.Is(err, vault.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to read secret")
		return
	}

	// Filter fields if scope is field-level.
	if !allFields {
		allowed := make(map[string]bool)
		for _, f := range allowedFields {
			allowed[f] = true
		}
		for k := range fields {
			if !allowed[k] {
				delete(fields, k)
			}
		}
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:   claims.TenantID,
		EventType:  "secret.read",
		TokenID:    claims.TokenID,
		AgentID:    claims.AgentID,
		TaskID:     claims.TaskID,
		SecretName: secretName,
		Adapter:    "api",
	})

	format := r.URL.Query().Get("format")
	switch format {
	case "env":
		w.Header().Set("Content-Type", "text/plain")
		for k, v := range fields {
			fmt.Fprintf(w, "%s=%s\n", strings.ToUpper(k), string(v))
		}
	case "json", "":
		result := make(map[string]string, len(fields))
		for k, v := range fields {
			result[k] = string(v)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	default:
		errJSON(w, http.StatusBadRequest, "unsupported format")
	}
}

func (s *Server) handleVaultReadField(w http.ResponseWriter, r *http.Request, vs *vault.Store, claims *vault.CapTokenClaims, secretName, fieldName string) {
	// Scope check.
	if !vault.CheckScope(claims.Scope, secretName, fieldName) {
		_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
			TenantID:   claims.TenantID,
			EventType:  "secret.denied",
			TokenID:    claims.TokenID,
			AgentID:    claims.AgentID,
			SecretName: secretName,
			FieldName:  fieldName,
			Detail:     map[string]string{"reason": "out_of_scope"},
		})
		errJSON(w, http.StatusNotFound, "not found")
		return
	}

	plaintext, err := vs.ReadSecretField(r.Context(), claims.TenantID, secretName, fieldName)
	if err != nil {
		if errors.Is(err, vault.ErrNotFound) || errors.Is(err, vault.ErrFieldNotFound) {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to read field")
		return
	}

	_ = vs.WriteAuditEvent(r.Context(), &vault.AuditEvent{
		TenantID:   claims.TenantID,
		EventType:  "secret.read",
		TokenID:    claims.TokenID,
		AgentID:    claims.AgentID,
		TaskID:     claims.TaskID,
		SecretName: secretName,
		FieldName:  fieldName,
		Adapter:    "api",
	})

	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write(plaintext)
}
