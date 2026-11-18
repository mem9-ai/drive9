package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// AdminGenerateTokenRequest is the control-plane wire payload for POST /v1/tokens/generate.
// Omitting Scopes generates an owner API key; setting Scopes generates an fs_scoped key
// and requires a positive TTLSeconds.
type AdminGenerateTokenRequest struct {
	TenantID   string         `json:"tenant_id"`
	KeyName    string         `json:"key_name,omitempty"`
	TTLSeconds *int64         `json:"ttl_seconds,omitempty"`
	Scopes     []FSScopeGrant `json:"scopes,omitempty"`
	PublicKey  string         `json:"-"`
	PrivateKey string         `json:"-"`
}

// AdminGenerateTokenResponse is returned by POST /v1/tokens/generate.
type AdminGenerateTokenResponse struct {
	Token     string         `json:"token"`
	TokenID   string         `json:"token_id"`
	TenantID  string         `json:"tenant_id"`
	KeyName   string         `json:"key_name"`
	ScopeKind string         `json:"scope_kind"`
	Status    string         `json:"status"`
	IssuedAt  time.Time      `json:"issued_at"`
	ExpiresAt *time.Time     `json:"expires_at,omitempty"`
	Scopes    []FSScopeGrant `json:"scopes,omitempty"`
}

// AdminGenerateToken mints an owner or fs_scoped API key for one tenant using
// TiDB Cloud organization credentials (POST /v1/tokens/generate).
func (c *Client) AdminGenerateToken(ctx context.Context, req AdminGenerateTokenRequest) (*AdminGenerateTokenResponse, error) {
	req.TenantID = strings.TrimSpace(req.TenantID)
	if req.TenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	req.KeyName = strings.TrimSpace(req.KeyName)
	if len(req.Scopes) == 0 {
		req.Scopes = nil
	} else if req.TTLSeconds == nil || *req.TTLSeconds <= 0 {
		return nil, fmt.Errorf("ttl_seconds is required and must be positive for scoped tokens")
	}
	if req.TTLSeconds != nil && *req.TTLSeconds <= 0 {
		return nil, fmt.Errorf("ttl_seconds must be positive when provided")
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal admin token generate request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/tokens/generate", bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("create admin token generate request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	setQuotaHeaders(httpReq, req.PublicKey, req.PrivateKey)
	// Control-plane requests authenticate with TiDB Cloud headers only; c.do
	// would also attach a tenant Bearer token and trigger 400 ambiguous credentials.
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("admin token generate request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminGenerateTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin token generate response: %w", err)
	}
	return &out, nil
}
