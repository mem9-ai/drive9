package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// FSScopeGrant is one path prefix + operation set for a scoped filesystem token.
type FSScopeGrant struct {
	Prefix string   `json:"prefix"`
	Ops    []string `json:"ops"`
}

// IssueScopedTokenRequest is the wire payload for POST /v1/tokens.
type IssueScopedTokenRequest struct {
	Subject    string         `json:"subject"`
	TTLSeconds int64          `json:"ttl_seconds"`
	Scopes     []FSScopeGrant `json:"scopes"`
}

// IssueScopedTokenResponse is returned by POST /v1/tokens.
type IssueScopedTokenResponse struct {
	Token     string         `json:"token"`
	TokenID   string         `json:"token_id"`
	Subject   string         `json:"subject"`
	ScopeKind string         `json:"scope_kind"`
	ExpiresAt *time.Time     `json:"expires_at,omitempty"`
	Scopes    []FSScopeGrant `json:"scopes"`
}

// IssueScopedToken creates an fs_scoped tenant API token. The caller must use
// an owner API key; scoped tokens are rejected by the server-side dispatcher.
func (c *Client) IssueScopedToken(ctx context.Context, req IssueScopedTokenRequest) (*IssueScopedTokenResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal scoped token issue request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/tokens", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result IssueScopedTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode scoped token issue response: %w", err)
	}
	return &result, nil
}

// RevokeScopedToken revokes one tenant API token by id.
func (c *Client) RevokeScopedToken(ctx context.Context, tokenID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/v1/tokens/"+url.PathEscape(tokenID), nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}
