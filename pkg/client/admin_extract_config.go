package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// ExtractMediaType identifies the media-specific extraction configuration.
type ExtractMediaType string

const (
	ExtractMediaTypeImage ExtractMediaType = "image"
	ExtractMediaTypeAudio ExtractMediaType = "audio"
	ExtractMediaTypeVideo ExtractMediaType = "video"
	ExtractMediaTypeText  ExtractMediaType = "text"
)

// AdminTenantExtractConfig is the effective extraction configuration returned
// by the admin API. APIKey is masked by the server.
type AdminTenantExtractConfig struct {
	Enabled   bool   `json:"enabled"`
	APIBase   string `json:"api_base,omitempty"`
	APIKey    string `json:"api_key,omitempty"`
	Model     string `json:"model,omitempty"`
	Prompt    string `json:"prompt,omitempty"`
	Source    string `json:"source"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// AdminTenantExtractConfigGetRequest identifies a tenant and media type for
// extract-config lookup.
type AdminTenantExtractConfigGetRequest struct {
	TenantID   string
	MediaType  ExtractMediaType
	PublicKey  string
	PrivateKey string
}

// AdminTenantExtractConfigSetRequest updates a tenant extract config. Nil
// config fields are omitted so partial updates preserve existing values.
type AdminTenantExtractConfigSetRequest struct {
	TenantID   string           `json:"-"`
	MediaType  ExtractMediaType `json:"-"`
	PublicKey  string           `json:"-"`
	PrivateKey string           `json:"-"`
	Enabled    *bool            `json:"enabled,omitempty"`
	APIBase    *string          `json:"api_base,omitempty"`
	APIKey     *string          `json:"api_key,omitempty"`
	Model      *string          `json:"model,omitempty"`
	Prompt     *string          `json:"prompt,omitempty"`
}

// AdminGetTenantExtractConfig returns the effective extract configuration for
// one tenant and media type.
func (c *Client) AdminGetTenantExtractConfig(ctx context.Context, query AdminTenantExtractConfigGetRequest) (*AdminTenantExtractConfig, error) {
	path, err := adminTenantExtractConfigPath(query.TenantID, query.MediaType)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create admin tenant extract config get request: %w", err)
	}
	setQuotaHeaders(req, query.PublicKey, query.PrivateKey)
	return c.doAdminTenantExtractConfig(req, "get")
}

// AdminSetTenantExtractConfig updates the extract configuration for one tenant
// and media type.
func (c *Client) AdminSetTenantExtractConfig(ctx context.Context, update AdminTenantExtractConfigSetRequest) (*AdminTenantExtractConfig, error) {
	path, err := adminTenantExtractConfigPath(update.TenantID, update.MediaType)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(update)
	if err != nil {
		return nil, fmt.Errorf("marshal admin tenant extract config set request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("create admin tenant extract config set request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setQuotaHeaders(req, update.PublicKey, update.PrivateKey)
	return c.doAdminTenantExtractConfig(req, "set")
}

func adminTenantExtractConfigPath(tenantID string, mediaType ExtractMediaType) (string, error) {
	if strings.TrimSpace(tenantID) == "" {
		return "", fmt.Errorf("tenant ID is required")
	}
	if strings.TrimSpace(string(mediaType)) == "" {
		return "", fmt.Errorf("media type is required")
	}
	return "/v1/admin/tenants/" + url.PathEscape(tenantID) + "/extract-config/" + url.PathEscape(string(mediaType)), nil
}

func (c *Client) doAdminTenantExtractConfig(req *http.Request, operation string) (*AdminTenantExtractConfig, error) {
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("admin tenant extract config %s request: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminTenantExtractConfig
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin tenant extract config %s response: %w", operation, err)
	}
	return &out, nil
}
