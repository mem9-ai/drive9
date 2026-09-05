package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type fsTokenScopeRequest struct {
	Prefix string   `json:"prefix"`
	Ops    []string `json:"ops"`
}

type issueScopedTokenRequest struct {
	Subject    string                `json:"subject,omitempty"`
	TTLSeconds int64                 `json:"ttl_seconds"`
	Scopes     []fsTokenScopeRequest `json:"scopes"`
}

type scopedTokenResponse struct {
	Token     string                `json:"token"`
	TokenID   string                `json:"token_id,omitempty"`
	Subject   string                `json:"subject,omitempty"`
	ScopeKind string                `json:"scope_kind"`
	ExpiresAt *time.Time            `json:"expires_at,omitempty"`
	Scopes    []fsTokenScopeRequest `json:"scopes"`
}

type revokeScopedTokenByAPIKeyRequest struct {
	APIKey string `json:"api_key"`
}

type requestFingerprintMetadata struct {
	RequestSHA256 string `json:"request_sha256"`
}

const (
	maxScopedTokenTTLSeconds       = int64(1<<63-1) / int64(time.Second)
	scopedTokenIdempotencyProvider = "scoped-token"
)

var (
	errScopedTokenIdempotencyConflict = errors.New("scoped token idempotency conflict")
	errScopedTokenIdempotencyInactive = errors.New("scoped token idempotency key is inactive")
)

func (s *Server) handleTokens(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "token management not enabled")
		return
	}
	scope, ok := ownerScopeFromRequest(w, r, "manage tokens")
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
	if r.URL.Path == "/v1/tokens/revoke" {
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleScopedTokenRevokeByAPIKey(w, r, scope)
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

func ownerScopeFromRequest(w http.ResponseWriter, r *http.Request, action string) (*TenantScope, bool) {
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return nil, false
	}
	if scope.IsScoped || scope.ScopeKind == meta.APIKeyScopeKindFS {
		if strings.TrimSpace(action) == "" {
			action = "perform this operation"
		}
		errJSON(w, http.StatusForbidden, "scoped token cannot "+action)
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
	idempotencyKey, err := validateScopedTokenIdempotencyKey(r.Header.Get("Idempotency-Key"))
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	metadata, err := scopedTokenRequestMetadata(subject, req.TTLSeconds, validatedScopes)
	if err != nil {
		errJSON(w, backendErrorStatus(r.Context(), err), "failed to validate token request")
		return
	}

	var resp *scopedTokenResponse
	replayed := false
	issue := func(ctx context.Context) error {
		if idempotencyKey != "" {
			existing, err := s.meta.GetAPIKeyByIssuer(ctx, scope.TenantID, scopedTokenIdempotencyProvider, idempotencyKey)
			if err == nil {
				if !sameRequestFingerprintMetadata(existing.IssuedByMetadataJSON, metadata) {
					return errScopedTokenIdempotencyConflict
				}
				if existing.Status != meta.APIKeyActive {
					return errScopedTokenIdempotencyInactive
				}
				resp, err = s.scopedTokenResponseForAPIKey(ctx, existing)
				replayed = err == nil
				return err
			}
			if !errors.Is(err, meta.ErrNotFound) {
				return err
			}
		}
		var err error
		resp, err = s.issueScopedToken(ctx, scope.TenantID, subject, req.TTLSeconds, validatedScopes, idempotencyKey, metadata)
		return err
	}
	if idempotencyKey != "" {
		lockSubject := scope.TenantID + "\x00" + idempotencyKey
		err = s.meta.WithExternalBindingLock(r.Context(), scopedTokenIdempotencyProvider, lockSubject, issue)
	} else {
		err = issue(r.Context())
	}
	if errors.Is(err, errScopedTokenIdempotencyConflict) {
		errJSON(w, http.StatusConflict, "idempotency key was already used with a different request")
		return
	}
	if errors.Is(err, errScopedTokenIdempotencyInactive) {
		errJSON(w, http.StatusConflict, "idempotency key belongs to an inactive or expired token")
		return
	}
	if errors.Is(err, meta.ErrDuplicate) {
		errJSON(w, http.StatusConflict, "token already exists")
		return
	}
	if err != nil {
		errJSON(w, backendErrorStatus(r.Context(), err), "failed to issue token")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if replayed {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func validateScopedTokenIdempotencyKey(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	key := strings.TrimSpace(raw)
	if key == "" || key != raw || len(key) > 128 {
		return "", fmt.Errorf("Idempotency-Key must be 1 to 128 non-whitespace characters")
	}
	for _, r := range key {
		allowed := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r == ':'
		if !allowed {
			return "", fmt.Errorf("Idempotency-Key contains an unsupported character")
		}
	}
	return key, nil
}

func sameRequestFingerprintMetadata(left, right []byte) bool {
	var leftMetadata, rightMetadata requestFingerprintMetadata
	if json.Unmarshal(left, &leftMetadata) != nil || json.Unmarshal(right, &rightMetadata) != nil {
		return false
	}
	return leftMetadata.RequestSHA256 != "" && leftMetadata.RequestSHA256 == rightMetadata.RequestSHA256
}

func scopedTokenRequestMetadata(subject string, ttlSeconds int64, scopes []meta.APIKeyFSScope) ([]byte, error) {
	canonicalScopes := append([]meta.APIKeyFSScope(nil), scopes...)
	sortAPIKeyFSScopes(canonicalScopes)
	payload := struct {
		Subject    string               `json:"subject"`
		TTLSeconds int64                `json:"ttl_seconds"`
		Scopes     []meta.APIKeyFSScope `json:"scopes"`
	}{
		Subject:    subject,
		TTLSeconds: ttlSeconds,
		Scopes:     canonicalScopes,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return json.Marshal(requestFingerprintMetadata{RequestSHA256: hex.EncodeToString(sum[:])})
}

func (s *Server) issueScopedToken(
	ctx context.Context,
	tenantID string,
	subject string,
	ttlSeconds int64,
	scopes []meta.APIKeyFSScope,
	idempotencyKey string,
	metadata []byte,
) (*scopedTokenResponse, error) {
	tokenVersion, err := newScopedTokenVersion()
	if err != nil {
		return nil, err
	}
	expiresAt := time.Now().UTC().Add(time.Duration(ttlSeconds) * time.Second).Truncate(time.Second)
	rawToken, err := token.IssueTokenWithExpiry(s.tokenSecret, tenantID, tokenVersion, expiresAt)
	if err != nil {
		return nil, err
	}
	cipherToken, err := s.pool.Encrypt(ctx, []byte(rawToken))
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	apiKeyID := token.NewID()
	key := &meta.APIKey{
		ID:            apiKeyID,
		TenantID:      tenantID,
		KeyName:       subject,
		JWTCiphertext: cipherToken,
		JWTHash:       token.HashToken(rawToken),
		TokenVersion:  tokenVersion,
		Status:        meta.APIKeyActive,
		ScopeKind:     meta.APIKeyScopeKindFS,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if idempotencyKey != "" {
		key.IssuedByProvider = scopedTokenIdempotencyProvider
		key.IssuedBySubjectKey = idempotencyKey
		key.IssuedByMetadataJSON = metadata
	}
	rows := make([]meta.APIKeyFSScope, len(scopes))
	for i, scope := range scopes {
		scope.TenantID = tenantID
		scope.APIKeyID = apiKeyID
		scope.CreatedAt = now
		scope.UpdatedAt = now
		rows[i] = scope
	}
	sortAPIKeyFSScopes(rows)
	if err := s.meta.InsertAPIKeyWithFSScopes(ctx, key, rows); err != nil {
		return nil, err
	}
	return &scopedTokenResponse{
		Token:     rawToken,
		TokenID:   apiKeyID,
		Subject:   subject,
		ScopeKind: string(meta.APIKeyScopeKindFS),
		ExpiresAt: &expiresAt,
		Scopes:    fsScopeResponses(rows),
	}, nil
}

func sortAPIKeyFSScopes(scopes []meta.APIKeyFSScope) {
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].Prefix == scopes[j].Prefix {
			return scopes[i].Ops < scopes[j].Ops
		}
		return scopes[i].Prefix < scopes[j].Prefix
	})
}

func (s *Server) scopedTokenResponseForAPIKey(ctx context.Context, key *meta.APIKey) (*scopedTokenResponse, error) {
	if key == nil || key.ScopeKind != meta.APIKeyScopeKindFS || key.Status != meta.APIKeyActive {
		return nil, errScopedTokenIdempotencyInactive
	}
	rawToken, err := s.pool.Decrypt(ctx, key.JWTCiphertext)
	if err != nil {
		return nil, err
	}
	if token.HashToken(string(rawToken)) != key.JWTHash {
		return nil, fmt.Errorf("replayed scoped token hash mismatch")
	}
	claims, err := token.ParseAndVerifyToken(s.tokenSecret, string(rawToken))
	if err != nil || claims.TenantID != key.TenantID || claims.TokenVersion != key.TokenVersion || claims.ExpiresAt <= 0 {
		return nil, errScopedTokenIdempotencyInactive
	}
	expiresAt := time.Unix(claims.ExpiresAt, 0).UTC()
	rows, err := s.meta.ListAPIKeyFSScopes(ctx, key.TenantID, key.ID)
	if err != nil {
		return nil, err
	}
	return &scopedTokenResponse{
		Token:     string(rawToken),
		TokenID:   key.ID,
		Subject:   key.KeyName,
		ScopeKind: string(meta.APIKeyScopeKindFS),
		ExpiresAt: &expiresAt,
		Scopes:    fsScopeResponses(rows),
	}, nil
}

func (s *Server) handleScopedTokenRevoke(w http.ResponseWriter, r *http.Request, scope *TenantScope, tokenID string) {
	if err := s.meta.RevokeAPIKey(r.Context(), scope.TenantID, tokenID); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "token not found or already revoked")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "failed to revoke token")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleScopedTokenRevokeByAPIKey(w http.ResponseWriter, r *http.Request, scope *TenantScope) {
	var req revokeScopedTokenByAPIKeyRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	apiKey := strings.TrimSpace(req.APIKey)
	if apiKey == "" {
		errJSON(w, http.StatusBadRequest, "api_key is required")
		return
	}
	resolved, err := s.meta.ResolveByAPIKeyHash(r.Context(), token.HashToken(apiKey))
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "token not found or already revoked")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "failed to revoke token")
		return
	}
	if resolved.APIKey.TenantID != scope.TenantID || resolved.APIKey.ScopeKind != meta.APIKeyScopeKindFS {
		errJSON(w, http.StatusNotFound, "token not found or already revoked")
		return
	}
	if err := s.meta.RevokeAPIKey(r.Context(), scope.TenantID, resolved.APIKey.ID); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "token not found or already revoked")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "failed to revoke token")
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
