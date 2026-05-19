package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

type fsTokenScopeRequest struct {
	Prefix string   `json:"prefix"`
	Ops    []string `json:"ops"`
}

type issueScopedTokenRequest struct {
	Subject    string                `json:"subject"`
	TTLSeconds int64                 `json:"ttl_seconds"`
	Scopes     []fsTokenScopeRequest `json:"scopes"`
}

type scopedTokenResponse struct {
	Token     string                `json:"token"`
	TokenID   string                `json:"token_id"`
	Subject   string                `json:"subject"`
	ScopeKind string                `json:"scope_kind"`
	ExpiresAt *time.Time            `json:"expires_at,omitempty"`
	Scopes    []fsTokenScopeRequest `json:"scopes"`
}

const maxScopedTokenTTLSeconds = int64(1<<63-1) / int64(time.Second)

func (s *Server) handleTokens(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "token management not enabled")
		return
	}
	scope, ok := ownerScopeFromRequest(w, r)
	if !ok {
		return
	}

	if r.URL.Path == "/v1/tokens" {
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleScopedTokenIssue(w, r, scope)
		return
	}

	tokenID := strings.TrimPrefix(r.URL.Path, "/v1/tokens/")
	if tokenID == "" || strings.Contains(tokenID, "/") {
		errJSON(w, http.StatusNotFound, "token not found or already revoked")
		return
	}
	if r.Method != http.MethodDelete {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.handleScopedTokenRevoke(w, r, scope, tokenID)
}

func ownerScopeFromRequest(w http.ResponseWriter, r *http.Request) (*TenantScope, bool) {
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return nil, false
	}
	if scope.IsScoped || scope.ScopeKind == meta.APIKeyScopeKindFS {
		errJSON(w, http.StatusForbidden, "scoped token cannot manage tokens")
		return nil, false
	}
	return scope, true
}

func (s *Server) handleScopedTokenIssue(w http.ResponseWriter, r *http.Request, scope *TenantScope) {
	var req issueScopedTokenRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	subject := strings.TrimSpace(req.Subject)
	if subject == "" {
		errJSON(w, http.StatusBadRequest, "subject is required")
		return
	}
	if len(subject) > 64 {
		errJSON(w, http.StatusBadRequest, "subject must be at most 64 bytes")
		return
	}
	if req.TTLSeconds <= 0 {
		errJSON(w, http.StatusBadRequest, "ttl_seconds must be positive")
		return
	}
	if req.TTLSeconds > maxScopedTokenTTLSeconds {
		errJSON(w, http.StatusBadRequest, "ttl_seconds is too large")
		return
	}
	if len(req.Scopes) == 0 {
		errJSON(w, http.StatusBadRequest, "scopes are required")
		return
	}
	validatedScopes, err := validateScopedTokenScopes(req.Scopes)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	tokenVersion, err := newScopedTokenVersion()
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to issue token")
		return
	}
	expiresAt := time.Now().UTC().Add(time.Duration(req.TTLSeconds) * time.Second)
	rawToken, err := token.IssueTokenWithExpiry(s.tokenSecret, scope.TenantID, tokenVersion, expiresAt)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to issue token")
		return
	}
	cipherToken, err := s.pool.Encrypt(r.Context(), []byte(rawToken))
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to encrypt token")
		return
	}

	now := time.Now().UTC()
	apiKeyID := token.NewID()
	if err := s.meta.InsertAPIKey(r.Context(), &meta.APIKey{
		ID:            apiKeyID,
		TenantID:      scope.TenantID,
		KeyName:       subject,
		JWTCiphertext: cipherToken,
		JWTHash:       token.HashToken(rawToken),
		TokenVersion:  tokenVersion,
		Status:        meta.APIKeyActive,
		ScopeKind:     meta.APIKeyScopeKindFS,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		if errors.Is(err, meta.ErrDuplicate) {
			errJSON(w, http.StatusConflict, "token subject already exists")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to persist token")
		return
	}

	for _, scopeReq := range validatedScopes {
		if err := s.meta.InsertAPIKeyFSScope(r.Context(), &meta.APIKeyFSScope{
			TenantID:  scope.TenantID,
			APIKeyID:  apiKeyID,
			Prefix:    scopeReq.Prefix,
			Ops:       scopeReq.Ops,
			CreatedAt: now,
			UpdatedAt: now,
		}); err != nil {
			_ = s.meta.RevokeAPIKey(context.Background(), scope.TenantID, apiKeyID)
			if errors.Is(err, meta.ErrDuplicate) {
				errJSON(w, http.StatusConflict, "duplicate fs scope")
				return
			}
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	rows, err := s.meta.ListAPIKeyFSScopes(r.Context(), scope.TenantID, apiKeyID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to load token scopes")
		return
	}
	resp := scopedTokenResponse{
		Token:     rawToken,
		TokenID:   apiKeyID,
		Subject:   subject,
		ScopeKind: string(meta.APIKeyScopeKindFS),
		ExpiresAt: &expiresAt,
		Scopes:    fsScopeResponses(rows),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleScopedTokenRevoke(w http.ResponseWriter, r *http.Request, scope *TenantScope, tokenID string) {
	if err := s.meta.RevokeAPIKey(r.Context(), scope.TenantID, tokenID); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "token not found or already revoked")
			return
		}
		errJSON(w, http.StatusInternalServerError, "failed to revoke token")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func validateScopedTokenScopes(reqScopes []fsTokenScopeRequest) ([]meta.APIKeyFSScope, error) {
	validated := make([]meta.APIKeyFSScope, 0, len(reqScopes))
	seenPrefix := make(map[string]bool, len(reqScopes))
	for _, scopeReq := range reqScopes {
		prefix, err := canonicalScopePrefix(scopeReq.Prefix)
		if err != nil {
			return nil, err
		}
		if seenPrefix[prefix] {
			return nil, fmt.Errorf("duplicate fs scope prefix %q", prefix)
		}
		seenPrefix[prefix] = true
		ops, err := canonicalScopeOps(scopeReq.Ops)
		if err != nil {
			return nil, err
		}
		validated = append(validated, meta.APIKeyFSScope{Prefix: prefix, Ops: ops})
	}
	return validated, nil
}

func canonicalScopePrefix(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("fs scope prefix is required")
	}
	if strings.TrimSpace(raw) == ":" {
		return "", fmt.Errorf("fs scope prefix is required")
	}
	prefix, err := normalizeFSAuthorizationPath(raw)
	if err != nil {
		return "", fmt.Errorf("invalid fs scope prefix: %w", err)
	}
	return prefix, nil
}

func canonicalScopeOps(raw []string) (string, error) {
	seen := make(map[FSOp]bool)
	for _, part := range raw {
		op := FSOp(strings.TrimSpace(part))
		if op == "" {
			return "", fmt.Errorf("empty fs scope op")
		}
		if !isKnownFSOp(op) {
			return "", fmt.Errorf("unknown fs scope op %q", op)
		}
		seen[op] = true
	}
	if len(seen) == 0 {
		return "", fmt.Errorf("empty fs scope ops")
	}
	if seen[FSOpSearch] && !seen[FSOpRead] {
		return "", fmt.Errorf("search fs scope requires read")
	}
	ordered := make([]string, 0, len(seen))
	for _, op := range []FSOp{FSOpRead, FSOpList, FSOpSearch, FSOpWrite, FSOpDelete} {
		if seen[op] {
			ordered = append(ordered, string(op))
		}
	}
	return strings.Join(ordered, ","), nil
}

func fsScopeResponses(rows []meta.APIKeyFSScope) []fsTokenScopeRequest {
	out := make([]fsTokenScopeRequest, 0, len(rows))
	for _, row := range rows {
		ops := make([]string, 0)
		for _, op := range strings.Split(row.Ops, ",") {
			op = strings.TrimSpace(op)
			if op != "" {
				ops = append(ops, op)
			}
		}
		out = append(out, fsTokenScopeRequest{Prefix: row.Prefix, Ops: ops})
	}
	return out
}

func newScopedTokenVersion() (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt32))
	if err != nil {
		return 0, err
	}
	return int(n.Int64()) + 1, nil
}
