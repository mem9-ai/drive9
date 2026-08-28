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

// AdminTenantEmbeddingConfig is the effective embedding configuration returned
// by the admin API. APIKey is masked by the server.
type AdminTenantEmbeddingConfig struct {
	Enabled    bool       `json:"enabled"`
	APIBase    *string    `json:"api_base,omitempty"`
	APIKey     *string    `json:"api_key,omitempty"`
	Model      *string    `json:"model,omitempty"`
	Source     string     `json:"source"`
	Generation uint64     `json:"generation,omitempty"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
}

// AdminTenantEmbeddingConfigGetRequest identifies a tenant for embedding-config lookup.
type AdminTenantEmbeddingConfigGetRequest struct {
	TenantID   string
	PublicKey  string
	PrivateKey string
}

// AdminTenantEmbeddingConfigSetRequest replaces a tenant embedding config.
// Provider fields must be present when enabling and omitted when disabling.
type AdminTenantEmbeddingConfigSetRequest struct {
	TenantID   string  `json:"-"`
	PublicKey  string  `json:"-"`
	PrivateKey string  `json:"-"`
	Enabled    bool    `json:"enabled"`
	APIBase    *string `json:"api_base,omitempty"`
	APIKey     *string `json:"api_key,omitempty"`
	Model      *string `json:"model,omitempty"`
}

type adminTenantEmbeddingConfigSetBody struct {
	Enabled bool    `json:"enabled"`
	APIBase *string `json:"api_base,omitempty"`
	APIKey  *string `json:"api_key,omitempty"`
	Model   *string `json:"model,omitempty"`
}

// AdminGetTenantEmbeddingConfig returns the effective embedding configuration
// for one tenant.
func (c *Client) AdminGetTenantEmbeddingConfig(ctx context.Context, query AdminTenantEmbeddingConfigGetRequest) (*AdminTenantEmbeddingConfig, error) {
	path, err := adminTenantEmbeddingConfigPath(query.TenantID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create admin tenant embedding config get request: %w", err)
	}
	setQuotaHeaders(req, query.PublicKey, query.PrivateKey)
	return c.doAdminTenantEmbeddingConfig(req, "get")
}

// AdminSetTenantEmbeddingConfig replaces the embedding configuration for one tenant.
func (c *Client) AdminSetTenantEmbeddingConfig(ctx context.Context, update AdminTenantEmbeddingConfigSetRequest) (*AdminTenantEmbeddingConfig, error) {
	path, err := adminTenantEmbeddingConfigPath(update.TenantID)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(adminTenantEmbeddingConfigSetBody{
		Enabled: update.Enabled,
		APIBase: update.APIBase,
		APIKey:  update.APIKey,
		Model:   update.Model,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal admin tenant embedding config set request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("create admin tenant embedding config set request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setQuotaHeaders(req, update.PublicKey, update.PrivateKey)
	return c.doAdminTenantEmbeddingConfig(req, "set")
}

func adminTenantEmbeddingConfigPath(tenantID string) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return "", fmt.Errorf("tenant ID is required")
	}
	return "/v1/admin/tenants/" + url.PathEscape(tenantID) + "/embedding-config", nil
}

func (c *Client) doAdminTenantEmbeddingConfig(req *http.Request, operation string) (*AdminTenantEmbeddingConfig, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("admin tenant embedding config %s request: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminTenantEmbeddingConfig
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin tenant embedding config %s response: %w", operation, err)
	}
	return &out, nil
}
