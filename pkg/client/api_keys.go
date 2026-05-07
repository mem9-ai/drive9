package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// TenantAPIKeySummary holds metadata returned by the tenant API key list API.
type TenantAPIKeySummary struct {
	KeyID     string     `json:"key_id"`
	KeyName   string     `json:"key_name"`
	Status    string     `json:"status"`
	IssuedAt  time.Time  `json:"issued_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// TenantAPIKey holds a tenant API key response that includes plaintext key material.
type TenantAPIKey struct {
	APIKey    string     `json:"api_key"`
	KeyID     string     `json:"key_id"`
	KeyName   string     `json:"key_name"`
	Status    string     `json:"status"`
	IssuedAt  time.Time  `json:"issued_at,omitempty"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

func (c *Client) tenantAPIKeysURL(path string) string {
	if path == "" {
		return c.baseURL + "/v1/tenants/keys"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return c.baseURL + "/v1/tenants/keys" + path
}

// ListTenantAPIKeys lists tenant API key metadata.
func (c *Client) ListTenantAPIKeys(ctx context.Context) ([]TenantAPIKeySummary, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.tenantAPIKeysURL(""), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result struct {
		Keys []TenantAPIKeySummary `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode tenant api key list response: %w", err)
	}
	if result.Keys == nil {
		result.Keys = []TenantAPIKeySummary{}
	}
	return result.Keys, nil
}

// CreateTenantAPIKey creates a named tenant API key.
func (c *Client) CreateTenantAPIKey(ctx context.Context, keyName string) (*TenantAPIKey, error) {
	body, err := json.Marshal(map[string]string{"key_name": keyName})
	if err != nil {
		return nil, fmt.Errorf("marshal tenant api key create request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tenantAPIKeysURL(""), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result TenantAPIKey
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode tenant api key create response: %w", err)
	}
	return &result, nil
}

// GetTenantAPIKey fetches a named tenant API key including plaintext key material.
func (c *Client) GetTenantAPIKey(ctx context.Context, keyName string) (*TenantAPIKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.tenantAPIKeysURL("/"+url.PathEscape(keyName)), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result TenantAPIKey
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode tenant api key get response: %w", err)
	}
	return &result, nil
}

// DeleteTenantAPIKey revokes a named tenant API key.
func (c *Client) DeleteTenantAPIKey(ctx context.Context, keyName string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.tenantAPIKeysURL("/"+url.PathEscape(keyName)), nil)
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
